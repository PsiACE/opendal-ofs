// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use ofs::catalog::{Catalog, VolumeDefinition};
use ofs::filesystem::{AccessModel, Capabilities, OperationId, VolumeId, VolumeModel};
use ofs::managed::{
    D1Config, D1Metadata, ManagedDataFormat, ManagedError, ManagedErrorKind, ManagedFormat,
    ManagedVolume, Metadata, MetadataPlacement, ObjectMetadata,
};
use ofs::sync::{ReplicaState, SyncEngine};
use opendal::Operator;
use url::Url;

use crate::cli::{
    Cli, Command, StatusArgs, SyncArgs, VolumeCommand, VolumeCreateArgs, VolumeGcArgs,
    VolumePackArgs,
};

pub(crate) async fn run(cli: Cli) -> Result<()> {
    match (cli.command, cli.mount_path, cli.backend) {
        (
            Some(Command::Volume {
                command: VolumeCommand::Create(args),
            }),
            None,
            None,
        ) => create_volume(cli.config.as_deref(), args).await,
        (
            Some(Command::Volume {
                command: VolumeCommand::Pack(args),
            }),
            None,
            None,
        ) => pack_volume(cli.config.as_deref(), args).await,
        (
            Some(Command::Volume {
                command: VolumeCommand::Gc(args),
            }),
            None,
            None,
        ) => gc_volume(cli.config.as_deref(), args).await,
        (Some(Command::Sync(args)), None, None) => sync_volume(cli.config.as_deref(), args).await,
        (Some(Command::Status(args)), None, None) => status(cli.config.as_deref(), args),
        (None, Some(mount_path), Some(backend)) => mount(&mount_path, &backend).await,
        (Some(_), _, _) => {
            bail!("a subcommand cannot be combined with Direct Mount arguments; run `ofs --help`")
        }
        (None, _, _) => {
            bail!("provide a volume command or both MOUNT_PATH and BACKEND_URL; run `ofs --help`")
        }
    }
}

async fn gc_volume(config: Option<&Path>, args: VolumeGcArgs) -> Result<()> {
    let volume = open_managed_volume(config, &args.alias).await?;
    let observed = volume
        .observe()
        .await?
        .context("Managed volume has no published namespace to collect")?;
    let sweep = volume.begin_gc(&observed).await?;
    let fixed = volume
        .observe()
        .await?
        .context("Managed namespace disappeared after starting collection")?;
    let collected = volume.collect_unreachable_loose(&fixed, sweep).await?;
    volume.finish_gc(sweep).await?;
    println!(
        "garbage collected {:?}: scanned={} deleted={} bytes={}",
        args.alias, collected.scanned, collected.deleted, collected.deleted_bytes,
    );
    Ok(())
}

async fn pack_volume(config: Option<&Path>, args: VolumePackArgs) -> Result<()> {
    let volume = open_managed_volume(config, &args.alias).await?;
    let observed = volume
        .observe()
        .await?
        .context("Managed volume has no published namespace to pack")?;
    let packed = volume
        .pack_reachable_content(&observed, OperationId::generate())
        .await?;
    let mut retired = 0;
    let mut replacements = 0;
    if let Some(grace_seconds) = args.repack_grace_seconds {
        let fixed = volume
            .observe()
            .await?
            .context("Managed volume has no namespace recovery root for repack")?;
        if let Some(retirement) = volume
            .repack_reachable_content(&fixed, OperationId::generate())
            .await?
        {
            replacements = retirement.replacement_packs().len();
            if grace_seconds > 0 {
                tokio::time::sleep(Duration::from_secs(grace_seconds)).await;
            }
            let current = volume
                .observe()
                .await?
                .context("Managed namespace disappeared during pack retirement")?;
            retired = volume
                .finalize_pack_retirement(&current, retirement)
                .await?
                .len();
        }
    }
    let mut reclaimed = 0;
    if let Some(grace_seconds) = args.reclaim_loose_after_seconds {
        if grace_seconds > 0 {
            tokio::time::sleep(Duration::from_secs(grace_seconds)).await;
        }
        let current = volume
            .observe()
            .await?
            .context("Managed namespace disappeared during loose data reclamation")?;
        reclaimed = volume.reclaim_packed_loose(&current).await?;
    }
    println!(
        "packed {:?}: packs={} content={} logical_bytes={} replacements={} retired={} reclaimed={}",
        args.alias,
        packed.packs.len(),
        packed.reclaimable_loose.len(),
        packed.logical_bytes,
        replacements,
        retired,
        reclaimed,
    );
    Ok(())
}

async fn open_managed_volume(config: Option<&Path>, alias: &str) -> Result<ManagedVolume> {
    let catalog = load_catalog(config)?;
    let definition = catalog
        .get(alias)
        .with_context(|| format!("volume alias {alias:?} is not in the catalog"))?
        .clone();
    if definition.model != VolumeModel::Managed || definition.format_major != 1 {
        bail!("Managed volume maintenance requires a Managed volume using format v1");
    }

    let data = open_operator(&definition.storage)?;
    ManagedDataFormat::read(&data).await?.validate_for_write()?;
    let metadata = open_metadata(data.clone(), definition.metadata.as_ref())?;
    let placement = if definition.metadata.is_some() {
        MetadataPlacement::ExternalD1
    } else {
        MetadataPlacement::ColocatedObject
    };
    let expected = ManagedFormat::v1(
        definition.volume_id,
        placement,
        definition.storage.to_string(),
    )?;
    if metadata.read_format().await? != expected {
        bail!("volume catalog and Managed format v1 binding disagree");
    }
    Ok(match metadata {
        Metadata::Object(_) => ManagedVolume::object(definition.volume_id, data)?,
        Metadata::D1(metadata) => ManagedVolume::d1(definition.volume_id, data, metadata)?,
    })
}

async fn create_volume(config: Option<&Path>, args: VolumeCreateArgs) -> Result<()> {
    if args.model != VolumeModel::Managed {
        bail!(
            "named Direct volumes are not implemented; use `ofs MOUNT_PATH BACKEND_URL` for Direct Mount"
        );
    }

    let mut catalog = match config {
        Some(path) => Catalog::load(path),
        None => Catalog::load_from_env(),
    }
    .context("cannot open the volume catalog; set --config or OFS_CONFIG to a writable path")?;

    let configured = catalog.get(&args.alias).cloned();
    let provisional_id = configured
        .as_ref()
        .map(|definition| definition.volume_id)
        .unwrap_or_else(VolumeId::generate);
    let provisional = VolumeDefinition::new(
        provisional_id,
        args.model,
        args.storage.clone(),
        args.metadata.clone(),
        1,
    )
    .context("volume URLs must be credential-free; supply credentials through provider environment variables")?;
    if configured
        .as_ref()
        .is_some_and(|current| current != &provisional)
    {
        bail!(
            "volume alias {:?} conflicts with its existing configuration",
            args.alias
        );
    }
    let placement = if args.metadata.is_some() {
        MetadataPlacement::ExternalD1
    } else {
        MetadataPlacement::ColocatedObject
    };
    let data = open_operator(&args.storage)?;
    ManagedDataFormat::v1()
        .activate(&data)
        .await
        .map_err(create_format_error)?;
    let metadata = open_metadata(data, args.metadata.as_ref())?;
    let desired = ManagedFormat::v1(provisional_id, placement, args.storage.to_string())?;
    let format = match metadata.create_format(&desired).await {
        Ok(format) => format,
        Err(error) if configured.is_none() && error.kind() == ManagedErrorKind::Conflict => {
            let observed = metadata.read_format().await.map_err(create_format_error)?;
            let expected =
                ManagedFormat::v1(observed.volume_id(), placement, args.storage.to_string())?;
            if observed != expected {
                return Err(create_format_error(error));
            }
            observed
        }
        Err(error) => return Err(create_format_error(error)),
    };
    let definition = VolumeDefinition::new(
        format.volume_id(),
        args.model,
        args.storage,
        args.metadata,
        1,
    )?;
    let created = catalog.create(&args.alias, definition)?;

    catalog
        .save()
        .context("the remote format is ready but the local catalog could not be saved; fix the catalog path and repeat the same command")?;

    let action = if created { "created" } else { "opened" };
    println!("{action} managed volume {:?} with format v1", args.alias);
    Ok(())
}

fn open_metadata(data: Operator, metadata: Option<&Url>) -> Result<Metadata> {
    match metadata {
        None => Ok(Metadata::Object(ObjectMetadata::new(data))),
        Some(url) if url.scheme() == "d1" => Ok(Metadata::D1(D1Metadata::new(d1_config(url)?))),
        Some(_) => bail!("--metadata must use d1://ACCOUNT/DATABASE/STORE"),
    }
}

async fn sync_volume(config: Option<&Path>, args: SyncArgs) -> Result<()> {
    let catalog = load_catalog(config)?;
    let definition = catalog
        .get(&args.alias)
        .with_context(|| format!("volume alias {:?} is not in the catalog", args.alias))?
        .clone();
    if definition.model != VolumeModel::Managed || definition.format_major != 1 {
        bail!("sync requires a Managed volume using format v1");
    }

    let data = open_operator(&definition.storage)?;
    ManagedDataFormat::read(&data).await?.validate_for_write()?;
    let metadata = open_metadata(data.clone(), definition.metadata.as_ref())?;
    let placement = if definition.metadata.is_some() {
        MetadataPlacement::ExternalD1
    } else {
        MetadataPlacement::ColocatedObject
    };
    let expected = ManagedFormat::v1(
        definition.volume_id,
        placement,
        definition.storage.to_string(),
    )?;
    if metadata.read_format().await? != expected {
        bail!("volume catalog and Managed format v1 binding disagree");
    }

    let engine = match metadata {
        Metadata::Object(_) => SyncEngine::object(definition.volume_id, data)?,
        Metadata::D1(metadata) => SyncEngine::d1(definition.volume_id, data, metadata)?,
    };
    let resolutions = args.resolve.into_iter().collect::<Vec<_>>();
    let result = engine
        .sync(&args.replica, &args.state, &resolutions)
        .await?;
    if !result.conflicts.is_empty() {
        bail!(
            "sync retained {} conflict(s); inspect `ofs status` and resolve explicitly",
            result.conflicts.len()
        );
    }
    if result.pending {
        bail!("publication result is unknown; repeat sync to resolve its durable intent");
    }
    println!(
        "synced {:?} at change {}{}",
        args.alias,
        result.common.sequence(),
        if result.published { " (published)" } else { "" }
    );
    Ok(())
}

fn status(config: Option<&Path>, args: StatusArgs) -> Result<()> {
    let state = ReplicaState::load(&args.state)?
        .with_context(|| format!("replica state does not exist: {}", args.state.display()))?;
    let catalog = load_catalog(config)?;
    let (alias, definition) = catalog
        .find_by_id(state.volume)
        .context("replica volume is not in the local catalog")?;
    if definition.model != VolumeModel::Managed {
        bail!("replica state is not bound to a Managed volume");
    }
    let storage = open_operator(&definition.storage)?;
    let storage_capabilities = storage.info().full_capability();
    let metadata_authority = if definition.metadata.is_some() {
        "d1"
    } else {
        "object"
    };
    let capabilities = Capabilities::managed_sync_v1();
    let guarantees = capabilities
        .guarantees()
        .map(|capability| {
            serde_json::json!({
                "name": capability.name.as_str(),
                "guarantee": capability.guarantee,
            })
        })
        .collect::<Vec<_>>();
    let limitations = capabilities
        .limitations()
        .map(|limitation| {
            serde_json::json!({
                "name": limitation.name.as_str(),
                "reason": limitation.reason,
            })
        })
        .collect::<Vec<_>>();
    let value = serde_json::json!({
        "volume": alias,
        "volume_model": model_name(definition.model),
        "access_model": access_name(AccessModel::Sync),
        "replica": display_path(&args.replica),
        "common_sequence": state.common.sequence(),
        "pending": state.pending.is_some(),
        "conflicts": state.conflicts.len(),
        "capabilities": guarantees,
        "limitations": limitations,
        "assembly": {
            "volume": "managed",
            "access": "sync",
            "metadata_authority": metadata_authority,
            "data_operator": "opendal",
            "local_tree_operator": "opendal_fs",
            "custom_layer_order": [],
            "durable_state_owners": ["managed_metadata", "managed_data", "sync_replica"],
        },
        "format": {
            "managed_volume_major": definition.format_major,
            "managed_data_major": 1,
        },
        "data_policy": {
            "foreground_layout": "whole",
            "foreground_placement": "loose",
            "pack_maintenance": "explicit",
        },
        "storage_capabilities": {
            "read": storage_capabilities.read,
            "range_read": storage_capabilities.read,
            "write": storage_capabilities.write,
            "create_only": storage_capabilities.write_with_if_not_exists,
            "compare_and_swap": storage_capabilities.write_with_if_match,
            "stat": storage_capabilities.stat,
            "list": storage_capabilities.list,
        },
    });
    if args.json {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        println!(
            "{}: Managed Sync at change {}, {} pending, {} conflict(s)",
            display_path(&args.replica),
            state.common.sequence(),
            usize::from(state.pending.is_some()),
            state.conflicts.len()
        );
        println!(
            "guarantees: {}; limitations: {}",
            capabilities
                .guarantees()
                .map(|capability| capability.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            capabilities
                .limitations()
                .map(|limitation| limitation.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}

fn load_catalog(config: Option<&Path>) -> Result<Catalog> {
    match config {
        Some(path) => Catalog::load(path),
        None => Catalog::load_from_env(),
    }
    .context("cannot open the volume catalog; set --config or OFS_CONFIG")
}

fn model_name(model: VolumeModel) -> &'static str {
    match model {
        VolumeModel::Direct => "direct",
        VolumeModel::Managed => "managed",
    }
}

fn access_name(model: AccessModel) -> &'static str {
    match model {
        AccessModel::Mount => "mount",
        AccessModel::Sync => "sync",
    }
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn open_operator(url: &Url) -> Result<Operator> {
    let mut arguments = url.query_pairs().into_owned().collect::<Vec<_>>();
    if url.scheme() == "s3" {
        if let Some(bucket) = url.host_str()
            && !arguments.iter().any(|(key, _)| key == "bucket")
        {
            arguments.push(("bucket".into(), bucket.into()));
        }
        let root = url.path().trim_matches('/');
        if !root.is_empty() && !arguments.iter().any(|(key, _)| key == "root") {
            arguments.push(("root".into(), root.into()));
        }
    }
    Operator::via_iter(url.scheme(), arguments).map_err(|_| {
        anyhow!("cannot configure --storage; check its scheme, endpoint, bucket, and root")
    })
}

fn d1_config(url: &Url) -> Result<D1Config> {
    let account = url
        .host_str()
        .context("--metadata needs a D1 account in its host")?;
    let path = url
        .path_segments()
        .context("--metadata needs /DATABASE/STORE")?
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if path.len() != 2 {
        bail!("--metadata path must be /DATABASE/STORE");
    }
    let query = url.query_pairs().collect::<BTreeMap<_, _>>();
    if query.contains_key("token") {
        bail!("remove token from --metadata and set OFS_D1_TOKEN instead");
    }
    let token = env::var("OFS_D1_TOKEN")
        .map_err(|_| anyhow!("set OFS_D1_TOKEN to the D1 API credential and repeat the command"))?;
    let mut config = D1Config::new(account, path[0], path[1], token).map_err(|_| {
        anyhow!("invalid D1 metadata configuration; check account, database, store, and token")
    })?;
    if let Some(api_base) = query.get("api_base") {
        config = config
            .with_api_base(api_base.as_ref())
            .map_err(|_| anyhow!("invalid D1 api_base; provide an absolute Query API base URL"))?;
    }
    Ok(config)
}

fn create_format_error(error: ManagedError) -> anyhow::Error {
    match error.kind() {
        ManagedErrorKind::Invalid => anyhow!(
            "cannot create Managed format v1 because the storage lacks required conditional writes or the format binding is invalid"
        ),
        ManagedErrorKind::Conflict => anyhow!(
            "storage is already bound to another Managed volume; use the matching alias and catalog or choose another storage root"
        ),
        ManagedErrorKind::Corrupt => anyhow!(
            "the existing Managed format is corrupt or unsupported; inspect it before attempting recovery"
        ),
        ManagedErrorKind::Unavailable => anyhow!(
            "Managed metadata is unavailable; check the endpoint and credentials, then repeat the same command"
        ),
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn mount(mount_path: &Path, backend_url: &Url) -> Result<()> {
    use fuse3::MountOptions;
    use fuse3::path::Session;
    use std::env;

    if backend_url.has_host() {
        log::warn!("backend host will be ignored");
    }
    let backend = Operator::via_iter(backend_url.scheme(), backend_url.query_pairs().into_owned())
        .map_err(|_| anyhow!("cannot configure BACKEND_URL; check its scheme and arguments"))?;

    let mut options = MountOptions::default();
    let mut gid: u32 = nix::unistd::getgid().into();
    let mut uid: u32 = nix::unistd::getuid().into();
    if nix::unistd::getuid().is_root() {
        if let Some(sudo_gid) = env::var("SUDO_GID")
            .ok()
            .and_then(|value| value.parse().ok())
        {
            gid = sudo_gid;
        }
        if let Some(sudo_uid) = env::var("SUDO_UID")
            .ok()
            .and_then(|value| value.parse().ok())
        {
            uid = sudo_uid;
        }
    }
    options.gid(gid).uid(uid);
    let filesystem = fuse3_opendal::Filesystem::new(backend, uid, gid);
    let mut handle = if nix::unistd::getuid().is_root() {
        Session::new(options).mount(filesystem, mount_path).await?
    } else {
        Session::new(options)
            .mount_with_unprivileged(filesystem, mount_path)
            .await?
    };
    tokio::select! {
        result = &mut handle => result?,
        _ = tokio::signal::ctrl_c() => handle.unmount().await?,
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
async fn mount(_: &Path, _: &Url) -> Result<()> {
    bail!("Direct Mount is supported on Linux, FreeBSD, and macOS")
}
