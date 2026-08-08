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
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use ofs::catalog::{Catalog, VolumeDefinition};
use ofs::filesystem::{AccessModel, OperationId, VolumeId, VolumeModel};
use ofs::managed::{
    D1Config, D1Metadata, FileLayoutPolicy, ManagedError, ManagedErrorKind, ManagedExtension,
    ManagedFormat, ManagedVolume, Metadata, MetadataPlacement, ObjectMetadata,
};
use ofs::sync::{ReplicaState, SyncEngine};
use opendal::Operator;
use opendal::layers::ConcurrentLimitLayer;
use url::Url;

use crate::cli::{
    Cli, Command, FileLayoutArg, MountArgs, StatusArgs, SyncArgs, VolumeCommand, VolumeCreateArgs,
    VolumeGcArgs, VolumePackArgs,
};

const DEFAULT_FASTCDC_MINIMUM_FILE_SIZE: u64 = 1024 * 1024;
const DEFAULT_FASTCDC_MINIMUM_CHUNK_SIZE: u32 = 64 * 1024;
const DEFAULT_FASTCDC_TARGET_CHUNK_SIZE: u32 = 256 * 1024;
const DEFAULT_FASTCDC_MAXIMUM_CHUNK_SIZE: u32 = 1024 * 1024;

pub(crate) async fn run(cli: Cli) -> Result<()> {
    let transfer_concurrency = cli.transfer_concurrency;
    match cli.command {
        Command::Volume {
            command: VolumeCommand::Create(args),
        } => create_volume(cli.config.as_deref(), args, transfer_concurrency).await,
        Command::Volume {
            command: VolumeCommand::Pack(args),
        } => pack_volume(cli.config.as_deref(), args, transfer_concurrency).await,
        Command::Volume {
            command: VolumeCommand::Gc(args),
        } => gc_volume(cli.config.as_deref(), args, transfer_concurrency).await,
        Command::Mount(args) => {
            mount_volume(cli.config.as_deref(), args, transfer_concurrency).await
        }
        Command::Sync(args) => sync_volume(cli.config.as_deref(), args, transfer_concurrency).await,
        Command::Status(args) => status(cli.config.as_deref(), args, transfer_concurrency),
    }
}

async fn gc_volume(
    config: Option<&Path>,
    args: VolumeGcArgs,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    let volume = open_managed_volume(config, &args.alias, transfer_concurrency).await?;
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

async fn pack_volume(
    config: Option<&Path>,
    args: VolumePackArgs,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    let volume = open_managed_volume(config, &args.alias, transfer_concurrency).await?;
    let rebuilt = if args.rebuild_index {
        Some(volume.rebuild_pack_index().await?)
    } else {
        None
    };
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
        "packed {:?}: packs={} content={} logical_bytes={} replacements={} retired={} reclaimed={}{}",
        args.alias,
        packed.packs.len(),
        packed.reclaimable_loose.len(),
        packed.logical_bytes,
        replacements,
        retired,
        reclaimed,
        rebuilt
            .map(|content| format!(" rebuilt_index_content={content}"))
            .unwrap_or_default(),
    );
    Ok(())
}

async fn open_managed_volume(
    config: Option<&Path>,
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
    if settings.format_major != 1 {
        bail!("Managed volume maintenance requires a Managed volume using format v1");
    }

    let data = open_operator(&definition.storage, transfer_concurrency)?;
    let metadata = open_metadata(data.clone(), settings.metadata.as_ref())?;
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
    if let Metadata::D1(metadata) = &metadata
        && metadata.read_format().await? != data_format
    {
        bail!("Managed data root and transactional metadata binding disagree");
    }
    match metadata {
        Metadata::Object(_) => ManagedVolume::object(expected, data),
        Metadata::D1(metadata) => ManagedVolume::d1(expected, data, metadata),
    }?
    .with_file_layout(settings.file_layout)
    .map_err(Into::into)
}

async fn create_volume(
    config: Option<&Path>,
    mut args: VolumeCreateArgs,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
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
    let mut catalog = match config {
        Some(path) => Catalog::load(path),
        None => Catalog::load_from_env(),
    }
    .context("cannot open the volume catalog; set --config or OFS_CONFIG to a writable path")?;
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
    )?;
    let provisional_id = configured
        .as_ref()
        .map(|definition| definition.volume_id)
        .unwrap_or_else(VolumeId::generate);
    let provisional = VolumeDefinition::managed(
        provisional_id,
        args.storage.clone(),
        args.metadata.clone(),
        1,
    )
    .and_then(|definition| definition.with_file_layout(file_layout))
    .context("volume URLs must be credential-free; supply credentials through provider environment variables")?;
    if configured
        .as_ref()
        .is_some_and(|current| !current.has_same_binding(&provisional))
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
    let metadata = open_metadata(data.clone(), args.metadata.as_ref())?;
    let desired = managed_format(provisional_id, placement)?;
    let data_metadata = ObjectMetadata::new(data);
    let format = match data_metadata.create_format(&desired).await {
        Ok(format) => format,
        Err(error) if configured.is_none() && error.kind() == ManagedErrorKind::Conflict => {
            let observed = data_metadata
                .read_format()
                .await
                .map_err(create_format_error)?;
            let expected = managed_format(observed.volume_id(), placement)?;
            if observed != expected {
                return Err(create_format_error(error));
            }
            observed
        }
        Err(error) => return Err(create_format_error(error)),
    };
    if let Metadata::D1(metadata) = &metadata
        && metadata.create_format(&format).await? != format
    {
        bail!("Managed data root and transactional metadata binding disagree");
    }
    let definition = VolumeDefinition::managed(format.volume_id(), args.storage, args.metadata, 1)?
        .with_file_layout(file_layout)?;
    let created = if configured.is_some() {
        catalog.configure_file_layout(&args.alias, file_layout)?;
        false
    } else {
        catalog.create(&args.alias, definition)?
    };

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
    if args.file_layout.is_some()
        || args.fastcdc_minimum_file_size.is_some()
        || args.fastcdc_minimum_chunk_size.is_some()
        || args.fastcdc_target_chunk_size.is_some()
        || args.fastcdc_maximum_chunk_size.is_some()
    {
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
) -> Result<FileLayoutPolicy> {
    let has_fastcdc_sizes = args.fastcdc_minimum_file_size.is_some()
        || args.fastcdc_minimum_chunk_size.is_some()
        || args.fastcdc_target_chunk_size.is_some()
        || args.fastcdc_maximum_chunk_size.is_some();
    if args.file_layout.is_none() && !has_fastcdc_sizes {
        return Ok(configured.unwrap_or_default());
    }
    if matches!(args.file_layout, Some(FileLayoutArg::Whole)) {
        if has_fastcdc_sizes {
            bail!("FastCDC size options require --file-layout fastcdc");
        }
        return Ok(FileLayoutPolicy::Whole);
    }

    let base = match configured {
        Some(FileLayoutPolicy::FastCdcV2020 {
            minimum_file_size,
            minimum_size,
            target_size,
            maximum_size,
        }) => (minimum_file_size, minimum_size, target_size, maximum_size),
        _ => (
            DEFAULT_FASTCDC_MINIMUM_FILE_SIZE,
            DEFAULT_FASTCDC_MINIMUM_CHUNK_SIZE,
            DEFAULT_FASTCDC_TARGET_CHUNK_SIZE,
            DEFAULT_FASTCDC_MAXIMUM_CHUNK_SIZE,
        ),
    };
    let policy = FileLayoutPolicy::FastCdcV2020 {
        minimum_file_size: args.fastcdc_minimum_file_size.unwrap_or(base.0),
        minimum_size: args.fastcdc_minimum_chunk_size.unwrap_or(base.1),
        target_size: args.fastcdc_target_chunk_size.unwrap_or(base.2),
        maximum_size: args.fastcdc_maximum_chunk_size.unwrap_or(base.3),
    };
    policy
        .validate()
        .context("invalid FastCDC file layout configuration")?;
    Ok(policy)
}

fn open_metadata(data: Operator, metadata: Option<&Url>) -> Result<Metadata> {
    match metadata {
        None => Ok(Metadata::Object(ObjectMetadata::new(data))),
        Some(url) if url.scheme() == "d1" => Ok(Metadata::D1(D1Metadata::new(d1_config(url)?))),
        Some(_) => bail!("--metadata must use d1://ACCOUNT/DATABASE/STORE"),
    }
}

fn managed_format(volume_id: VolumeId, placement: MetadataPlacement) -> Result<ManagedFormat> {
    ManagedFormat::v1(
        volume_id,
        placement,
        [ManagedExtension::FastCdc, ManagedExtension::Pack],
    )
    .map_err(Into::into)
}

async fn sync_volume(
    config: Option<&Path>,
    args: SyncArgs,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    let catalog = load_catalog(config)?;
    let definition = catalog
        .get(&args.alias)
        .with_context(|| format!("volume alias {:?} is not in the catalog", args.alias))?
        .clone();
    let settings = definition
        .managed_settings()
        .context("sync requires a Managed volume")?;
    if settings.format_major != 1 {
        bail!("sync requires a Managed volume using format v1");
    }

    let data = open_operator(&definition.storage, transfer_concurrency)?;
    let metadata = open_metadata(data.clone(), settings.metadata.as_ref())?;
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
    if let Metadata::D1(metadata) = &metadata
        && metadata.read_format().await? != data_format
    {
        bail!("Managed data root and transactional metadata binding disagree");
    }

    let volume = match metadata {
        Metadata::Object(_) => ManagedVolume::object(expected, data),
        Metadata::D1(metadata) => ManagedVolume::d1(expected, data, metadata),
    }?
    .with_file_layout(settings.file_layout)?;
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

fn status(
    config: Option<&Path>,
    args: StatusArgs,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    let state = ReplicaState::load(&args.state)?
        .with_context(|| format!("replica state does not exist: {}", args.state.display()))?;
    let catalog = load_catalog(config)?;
    let (alias, definition) = catalog
        .find_by_id(state.volume)
        .context("replica volume is not in the local catalog")?;
    let settings = definition
        .managed_settings()
        .context("replica state is not bound to a Managed volume")?;
    let storage = open_operator(&definition.storage, transfer_concurrency)?;
    let storage_capabilities = storage.info().full_capability();
    let metadata_authority = if settings.metadata.is_some() {
        "d1"
    } else {
        "object"
    };
    let layout_settings = match settings.file_layout {
        FileLayoutPolicy::Whole => serde_json::Value::Null,
        FileLayoutPolicy::FastCdcV2020 {
            minimum_file_size,
            minimum_size,
            target_size,
            maximum_size,
        } => serde_json::json!({
            "algorithm": "fastcdc_v2020",
            "minimum_file_size": minimum_file_size,
            "minimum_chunk_size": minimum_size,
            "target_chunk_size": target_size,
            "maximum_chunk_size": maximum_size,
        }),
    };
    let value = serde_json::json!({
        "volume": alias,
        "volume_model": model_name(definition.model()),
        "access_model": access_name(AccessModel::Sync),
        "replica": display_path(&args.replica),
        "common_sequence": state.common.sequence(),
        "pending": state.pending.is_some(),
        "conflicts": state.conflicts.len(),
        "assembly": {
            "volume": "managed",
            "access": "sync",
            "metadata_authority": metadata_authority,
            "data_operator": "opendal",
            "local_tree_operator": "opendal_fs",
            "runtime": {
                "transfer_concurrency": transfer_concurrency.get(),
            },
            "operator": {
                "layers": [{
                    "name": "opendal.concurrent_limit",
                    "limit": transfer_concurrency.get(),
                }],
                "ofs_custom_layer_order": [],
            },
            "durable_state_owners": ["managed_metadata", "managed_data", "sync_replica"],
        },
        "format": {
            "managed_volume_major": settings.format_major,
            "managed_data_major": 1,
        },
        "data_policy": {
            "foreground_layout": settings.file_layout.name(),
            "layout_settings": layout_settings,
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
            "{}: {} {} at change {}, {} pending, {} conflict(s)",
            display_path(&args.replica),
            model_name(definition.model()),
            access_name(AccessModel::Sync),
            state.common.sequence(),
            usize::from(state.pending.is_some()),
            state.conflicts.len()
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
        .map(|operator| operator.layer(ConcurrentLimitLayer::new(transfer_concurrency.get())))
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

fn create_format_error(error: ManagedError) -> anyhow::Error {
    match error.kind() {
        ManagedErrorKind::UnsupportedFormat => anyhow!(
            "the storage uses an unsupported Managed format, layout, or extension; use a supported client or choose a dedicated empty storage root"
        ),
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

async fn mount_volume(
    config: Option<&Path>,
    args: MountArgs,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
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
    let operator = open_operator(&definition.storage, transfer_concurrency)
        .context("cannot open the Direct volume storage configured in the catalog")?;
    mount(&args.mount_path, operator, args.read_only).await
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn mount(mount_path: &Path, backend: Operator, read_only: bool) -> Result<()> {
    use fuse3::MountOptions;
    use fuse3::path::Session;
    use std::env;

    let mut options = MountOptions::default();
    options.read_only(read_only);
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
async fn mount(_: &Path, _: Operator, _: bool) -> Result<()> {
    bail!("Direct Mount is supported on Linux, FreeBSD, and macOS")
}
