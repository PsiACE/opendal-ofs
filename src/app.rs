// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;
use std::env;
use std::num::NonZeroUsize;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use ofs::catalog::{Catalog, VolumeDefinition};
use ofs::filesystem::{OperationId, VolumeId, VolumeModel};
use ofs::managed::{
    D1Config, D1Metadata, FileLayoutPolicy, ManagedErrorKind, ManagedExtension, ManagedFormat,
    ManagedVolume, MetadataPlacement, ObjectMetadata,
};
use ofs::sync::{ReplicaState, SyncEngine};
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer};
use url::Url;

use crate::cli::{
    Cli, Command, FileLayoutArg, MountArgs, StatusArgs, SyncArgs, VolumeCommand, VolumeCreateArgs,
    VolumeGcArgs, VolumePackArgs, VolumeReindexArgs,
};

pub(crate) async fn run(cli: Cli) -> Result<()> {
    let config = cli.config;
    match cli.command {
        Command::Volume {
            command: VolumeCommand::Create(args),
        } => create_volume(&config, args).await,
        Command::Volume {
            command: VolumeCommand::Pack(args),
        } => pack_volume(&config, args).await,
        Command::Volume {
            command: VolumeCommand::Reindex(args),
        } => reindex_volume(&config, args).await,
        Command::Volume {
            command: VolumeCommand::Gc(args),
        } => gc_volume(&config, args).await,
        Command::Mount(args) => mount_volume(&config, args).await,
        Command::Sync(args) => sync_volume(&config, args).await,
        Command::Status(args) => status(&config, args),
    }
}

async fn gc_volume(config: &Path, args: VolumeGcArgs) -> Result<()> {
    let volume =
        open_managed_volume(config, &args.alias, args.runtime.transfer_concurrency).await?;
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

async fn pack_volume(config: &Path, args: VolumePackArgs) -> Result<()> {
    let volume =
        open_managed_volume(config, &args.alias, args.runtime.transfer_concurrency).await?;
    let observed = volume
        .observe()
        .await?
        .context("Managed volume has no published namespace to pack")?;
    let packed = volume
        .pack_reachable_content(&observed, OperationId::generate())
        .await?;
    println!(
        "packed {:?}: packs={} content={} logical_bytes={}",
        args.alias,
        packed.pack_count(),
        packed.content_count(),
        packed.logical_bytes,
    );
    Ok(())
}

async fn reindex_volume(config: &Path, args: VolumeReindexArgs) -> Result<()> {
    let volume =
        open_managed_volume(config, &args.alias, args.runtime.transfer_concurrency).await?;
    let content = volume.rebuild_pack_index().await?;
    println!("rebuilt pack index {:?}: content={content}", args.alias);
    Ok(())
}

async fn open_managed_volume(
    config: &Path,
    alias: &str,
    transfer_concurrency: NonZeroUsize,
) -> Result<ManagedVolume> {
    let catalog = load_catalog(config)?;
    let definition = catalog
        .get(alias)
        .with_context(|| format!("volume alias {alias:?} is not in the catalog"))?
        .clone();
    let settings = definition
        .managed_settings()
        .context("Managed volume maintenance requires a Managed volume")?;
    let data = open_operator(&definition.storage, transfer_concurrency)?;
    let d1 = open_d1_metadata(settings.metadata.as_ref())?;
    let placement = if settings.metadata.is_some() {
        MetadataPlacement::ExternalD1
    } else {
        MetadataPlacement::ColocatedObject
    };
    let expected = managed_format(definition.volume_id, placement)?;
    let data_format = ObjectMetadata::new(data.clone()).read_format().await?;
    if data_format != expected {
        bail!("volume catalog and Managed format v1 binding disagree");
    }
    if let Some(metadata) = &d1
        && metadata.read_format().await? != data_format
    {
        bail!("Managed data root and transactional metadata binding disagree");
    }
    match d1 {
        None => ManagedVolume::object(expected, data),
        Some(metadata) => ManagedVolume::d1(expected, data, metadata),
    }?
    .with_file_layout(settings.file_layout)
    .map_err(Into::into)
}

async fn create_volume(config: &Path, mut args: VolumeCreateArgs) -> Result<()> {
    let transfer_concurrency = args.runtime.transfer_concurrency;
    if args.model == VolumeModel::Managed && args.metadata.is_none() {
        args.metadata = env::var_os("OFS_METADATA_URL")
            .filter(|value| !value.is_empty())
            .map(|value| {
                value
                    .into_string()
                    .map_err(|_| anyhow!("OFS_METADATA_URL is not valid UTF-8"))
                    .and_then(|value| {
                        Url::parse(&value).context("OFS_METADATA_URL is not a valid URL")
                    })
            })
            .transpose()?;
    }
    let mut catalog = Catalog::load(config).context("cannot open the writable volume catalog")?;
    let configured = catalog.get(&args.alias).cloned();
    if args.model == VolumeModel::Direct {
        return create_direct_volume(catalog, configured.as_ref(), args, transfer_concurrency);
    }

    let file_layout = requested_file_layout(
        &args,
        configured
            .as_ref()
            .and_then(VolumeDefinition::managed_settings)
            .map(|settings| settings.file_layout),
    );
    let provisional_id = configured
        .as_ref()
        .map(|definition| definition.volume_id)
        .unwrap_or_else(VolumeId::generate);
    let provisional = VolumeDefinition::managed(
        provisional_id,
        args.storage.clone(),
        args.metadata.clone(),
        file_layout,
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
    let data = open_operator(&args.storage, transfer_concurrency)?;
    let d1 = open_d1_metadata(args.metadata.as_ref())?;
    let desired = managed_format(provisional_id, placement)?;
    let data_metadata = ObjectMetadata::new(data);
    let format = match data_metadata.create_format(&desired).await {
        Ok(format) => format,
        Err(error) if configured.is_none() && error.kind() == ManagedErrorKind::Conflict => {
            let observed = data_metadata.read_format().await?;
            let expected = managed_format(observed.volume_id(), placement)?;
            if observed != expected {
                return Err(error.into());
            }
            observed
        }
        Err(error) => return Err(error.into()),
    };
    if let Some(metadata) = &d1 {
        metadata.create_format(&format).await?;
    }
    let definition =
        VolumeDefinition::managed(format.volume_id(), args.storage, args.metadata, file_layout)?;
    let created = catalog.create(&args.alias, definition)?;

    catalog
        .save()
        .context("the remote format is ready but the local catalog could not be saved; fix the catalog path and repeat the same command")?;

    let action = if created { "created" } else { "opened" };
    println!("{action} managed volume {:?} with format v1", args.alias);
    Ok(())
}

fn create_direct_volume(
    mut catalog: Catalog,
    configured: Option<&VolumeDefinition>,
    args: VolumeCreateArgs,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    if args.metadata.is_some() {
        bail!("--metadata is only valid with --model managed");
    }
    if args.file_layout.is_some() {
        bail!("file layout options are only valid with --model managed");
    }

    let volume_id = configured
        .map(|definition| definition.volume_id)
        .unwrap_or_else(VolumeId::generate);
    open_operator(&args.storage, transfer_concurrency)
        .context("cannot configure the Direct volume storage")?;
    let definition = VolumeDefinition::direct(volume_id, args.storage)
        .context("volume URLs must be credential-free; supply credentials through provider environment variables")?;
    let created = catalog.create(&args.alias, definition)?;
    catalog.save().context(
        "the Direct volume binding could not be saved; fix the catalog path and repeat the command",
    )?;

    let action = if created { "created" } else { "opened" };
    println!("{action} direct volume {:?}", args.alias);
    Ok(())
}

fn requested_file_layout(
    args: &VolumeCreateArgs,
    configured: Option<FileLayoutPolicy>,
) -> FileLayoutPolicy {
    match args.file_layout {
        None => configured.unwrap_or_default(),
        Some(FileLayoutArg::Whole) => FileLayoutPolicy::Whole,
        Some(FileLayoutArg::FastCdc) => FileLayoutPolicy::FastCdcV2020,
    }
}

fn open_d1_metadata(metadata: Option<&Url>) -> Result<Option<D1Metadata>> {
    match metadata {
        None => Ok(None),
        Some(url) if url.scheme() == "d1" => Ok(Some(D1Metadata::new(d1_config(url)?))),
        Some(_) => bail!("--metadata must use d1://ACCOUNT/DATABASE/STORE"),
    }
}

fn managed_format(volume_id: VolumeId, placement: MetadataPlacement) -> Result<ManagedFormat> {
    ManagedFormat::v1(volume_id, placement, [ManagedExtension::FastCdc]).map_err(Into::into)
}

async fn sync_volume(config: &Path, args: SyncArgs) -> Result<()> {
    let transfer_concurrency = args.runtime.transfer_concurrency;
    let volume = open_managed_volume(config, &args.alias, transfer_concurrency).await?;
    let engine = SyncEngine::new(volume).with_transfer_concurrency(transfer_concurrency);
    let result = engine
        .sync(&args.replica, &args.state, &args.resolve)
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

fn status(config: &Path, args: StatusArgs) -> Result<()> {
    let state = ReplicaState::load(&args.state)?
        .with_context(|| format!("replica state does not exist: {}", args.state.display()))?;
    let catalog = load_catalog(config)?;
    let (alias, definition) = catalog
        .find_by_id(state.volume)
        .context("replica volume is not in the local catalog")?;
    definition
        .managed_settings()
        .context("replica state is not bound to a Managed volume")?;
    let value = serde_json::json!({
        "volume": alias,
        "volume_model": "managed",
        "access_model": "sync",
        "common_sequence": state.common.sequence(),
        "pending": state.pending.is_some(),
        "conflicts": state.conflicts.len(),
    });
    if args.json {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        println!(
            "managed sync at change {}, {} pending, {} conflict(s)",
            state.common.sequence(),
            usize::from(state.pending.is_some()),
            state.conflicts.len()
        );
    }
    Ok(())
}

fn load_catalog(config: &Path) -> Result<Catalog> {
    Catalog::load(config).context("cannot open the volume catalog")
}

fn open_operator(url: &Url, transfer_concurrency: NonZeroUsize) -> Result<Operator> {
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
    Operator::via_iter(url.scheme(), arguments)
        .map(|operator| {
            operator
                .layer(ConcurrentLimitLayer::new(transfer_concurrency.get()))
                .layer(RetryLayer::new().with_jitter().with_max_times(4))
        })
        .map_err(|_| {
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

async fn mount_volume(config: &Path, args: MountArgs) -> Result<()> {
    let catalog = load_catalog(config)?;
    let definition = catalog
        .get(&args.alias)
        .with_context(|| format!("volume alias {:?} is not in the catalog", args.alias))?;
    if definition.model() != VolumeModel::Direct {
        bail!(
            "mount currently supports Direct volumes; {:?} is Managed",
            args.alias
        );
    }
    let operator = open_operator(&definition.storage, args.runtime.transfer_concurrency)
        .context("cannot open the Direct volume storage configured in the catalog")?;
    mount(&args.mount_path, operator).await
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn mount(mount_path: &Path, backend: Operator) -> Result<()> {
    use fuse3::MountOptions;
    use fuse3::path::Session;
    use std::env;

    let mut options = MountOptions::default();
    options.read_only(true);
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
async fn mount(_: &Path, _: Operator) -> Result<()> {
    bail!("Direct Mount is supported on Linux, FreeBSD, and macOS")
}
