// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::env;
use std::num::NonZeroUsize;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use ofs::catalog::{Catalog, VolumeDefinition};
#[cfg(feature = "managed-branch")]
use ofs::filesystem::BranchName;
use ofs::filesystem::{Volume, VolumeId, VolumeModel};
#[cfg(feature = "managed-branch")]
use ofs::managed::extensions::branch::{BranchInfo, BranchStore, ForkPoint};
use ofs::managed::{
    D1Config, D1Metadata, ManagedErrorKind, ManagedFormat, ManagedVolume, MetadataFormat,
    ObjectMetadata, SegmentGcMaintenance,
};
use ofs::sync::{ReplicaState, SyncEngine};
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer};
use url::Url;

#[cfg(feature = "managed-branch")]
use crate::cli::BranchCommand;
use crate::cli::{
    Cli, Command, MountArgs, StatusArgs, SyncArgs, VolumeCommand, VolumeCreateArgs, VolumeGcArgs,
};

enum MetadataAuthority {
    Object(ObjectMetadata),
    D1(D1Metadata),
}

struct ManagedContext {
    format: ManagedFormat,
    data: Operator,
    metadata: MetadataAuthority,
}

impl MetadataAuthority {
    const fn metadata_format(&self) -> MetadataFormat {
        match self {
            Self::Object(_) => MetadataFormat::ObjectV1,
            Self::D1(_) => MetadataFormat::TransactionalV1,
        }
    }

    async fn create_format(
        &self,
        desired: &ManagedFormat,
    ) -> Result<ManagedFormat, ofs::managed::ManagedError> {
        match self {
            Self::Object(metadata) => metadata.create_format(desired).await,
            Self::D1(metadata) => metadata.create_format(desired).await,
        }
    }

    async fn read_format(&self) -> Result<ManagedFormat, ofs::managed::ManagedError> {
        match self {
            Self::Object(metadata) => metadata.read_format().await,
            Self::D1(metadata) => metadata.read_format().await,
        }
    }

    async fn read_format_optional(
        &self,
    ) -> Result<Option<ManagedFormat>, ofs::managed::ManagedError> {
        match self {
            Self::Object(metadata) => metadata.read_format_optional().await,
            Self::D1(metadata) => metadata.read_format_optional().await,
        }
    }

    fn validate_volume(
        &self,
        format: ManagedFormat,
        data: Operator,
    ) -> Result<(), ofs::managed::ManagedError> {
        match self {
            Self::Object(_) => ManagedVolume::object(format, data).map(|_| ()),
            Self::D1(metadata) => ManagedVolume::d1(format, data, metadata.clone()).map(|_| ()),
        }
    }

    #[cfg(feature = "managed-branch")]
    fn branches(&self, volume_id: VolumeId, data: Operator) -> Result<BranchStore> {
        match self {
            Self::Object(_) => BranchStore::object(volume_id, data).map_err(Into::into),
            Self::D1(metadata) => Ok(BranchStore::d1(volume_id, metadata.clone())),
        }
    }
}

pub(crate) async fn run(cli: Cli) -> Result<()> {
    let config = cli.config;
    match cli.command {
        Command::Volume {
            command: VolumeCommand::Create(args),
        } => create_volume(&config, args).await,
        Command::Volume {
            command: VolumeCommand::Gc(args),
        } => gc_volume(&config, args).await,
        #[cfg(feature = "managed-branch")]
        Command::Branch { command } => branch_command(&config, command).await,
        Command::Mount(args) => mount_volume(&config, args).await,
        Command::Sync(args) => sync_volume(&config, args).await,
        Command::Status(args) => status(&config, args),
    }
}

#[cfg(feature = "managed-branch")]
async fn branch_command(config: &Path, command: BranchCommand) -> Result<()> {
    let (alias, concurrency) = match &command {
        BranchCommand::List(args) => (&args.alias, args.runtime.transfer_concurrency),
        BranchCommand::Show(args) => (&args.alias, args.runtime.transfer_concurrency),
        BranchCommand::Create(args) => (&args.alias, args.runtime.transfer_concurrency),
        BranchCommand::Delete(args) => (&args.alias, args.runtime.transfer_concurrency),
    };
    let branches = open_branch_authority(config, alias, concurrency).await?;
    match command {
        BranchCommand::List(args) => {
            let listed = branches.list().await?;
            if args.json {
                let default = listed
                    .iter()
                    .find(|branch| branch.is_default)
                    .context("Managed branch registry has no default branch")?;
                let entries = listed.iter().map(branch_json).collect::<Vec<_>>();
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "default_branch": default.binding.name.as_str(),
                        "branches": entries,
                    }))?
                );
            } else {
                for branch in listed {
                    println!(
                        "{}{} {} change {}",
                        if branch.is_default { "* " } else { "  " },
                        branch.binding.name,
                        branch.binding.id,
                        branch.cursor.sequence(),
                    );
                }
            }
            Ok(())
        }
        BranchCommand::Show(args) => {
            let name = parse_branch_name(&args.branch)?;
            let branch = branches.get(&name).await?;
            if args.json {
                println!("{}", serde_json::to_string(&branch_json(&branch))?);
            } else {
                println!(
                    "branch {:?} {} at change {}{}",
                    branch.binding.name.as_str(),
                    branch.binding.id,
                    branch.cursor.sequence(),
                    if branch.is_default { " (default)" } else { "" },
                );
            }
            Ok(())
        }
        BranchCommand::Create(args) => {
            let target = parse_branch_name(&args.branch)?;
            let source = match args.from {
                Some(source) => parse_branch_name(&source)?,
                None => branches.default_name().await?,
            };
            let point = args.at.map_or(ForkPoint::Head, ForkPoint::Sequence);
            let created = branches.fork(&source, point, target).await?;
            println!(
                "created branch {:?} {} from {:?} at change {}",
                created.binding.name.as_str(),
                created.binding.id,
                source.as_str(),
                created.cursor.sequence(),
            );
            Ok(())
        }
        BranchCommand::Delete(args) => {
            let name = parse_branch_name(&args.branch)?;
            branches.delete(&name).await?;
            println!("deleted branch {:?}", name.as_str());
            Ok(())
        }
    }
}

#[cfg(feature = "managed-branch")]
fn parse_branch_name(value: &str) -> Result<BranchName> {
    value
        .parse()
        .map_err(|error| anyhow!("invalid branch name {value:?}: {error}"))
}

#[cfg(feature = "managed-branch")]
fn branch_json(branch: &BranchInfo) -> serde_json::Value {
    serde_json::json!({
        "name": branch.binding.name.as_str(),
        "id": branch.binding.id.to_string(),
        "sequence": branch.cursor.sequence(),
        "default": branch.is_default,
        "lifecycle": match branch.lifecycle {
            ofs::managed::extensions::branch::BranchLifecycle::Active => "active",
            ofs::managed::extensions::branch::BranchLifecycle::Sealed => "sealed",
        },
    })
}

async fn gc_volume(config: &Path, args: VolumeGcArgs) -> Result<()> {
    #[cfg(feature = "managed-branch")]
    {
        let ManagedContext {
            format,
            data,
            metadata,
        } = open_managed_context(config, &args.alias, args.runtime.transfer_concurrency).await?;
        if format.requires_extension(ofs::managed::ManagedExtension::BranchV1) {
            let branches = metadata.branches(format.volume_id(), data.clone())?;
            let collected = if args.resume {
                branches.resume_garbage_collect(data).await?
            } else {
                branches.garbage_collect(data).await?
            };
            print_gc_result(&args.alias, collected);
            return Ok(());
        }
    }
    let volume =
        open_managed_volume(config, &args.alias, None, args.runtime.transfer_concurrency).await?;
    let Some(observed) = volume.observe().await? else {
        print_gc_result(&args.alias, SegmentGcMaintenance::default());
        return Ok(());
    };
    let sweep = if args.resume {
        volume.resume_gc(&observed).await?
    } else {
        volume.begin_gc(&observed).await?
    };
    let fixed = volume
        .observe()
        .await?
        .context("Managed namespace disappeared after starting collection")?;
    let collected = volume.collect_unreachable_segments(&fixed, sweep).await?;
    volume.finish_gc(sweep).await?;
    print_gc_result(&args.alias, collected);
    Ok(())
}

fn print_gc_result(alias: &str, collected: SegmentGcMaintenance) {
    println!(
        "garbage collected {:?}: scanned={} deleted={} bytes={}",
        alias, collected.scanned, collected.deleted, collected.deleted_bytes,
    );
}

async fn open_managed_volume(
    config: &Path,
    alias: &str,
    branch: Option<&str>,
    transfer_concurrency: NonZeroUsize,
) -> Result<ManagedVolume> {
    let ManagedContext {
        format,
        data,
        metadata,
    } = open_managed_context(config, alias, transfer_concurrency).await?;
    #[cfg(feature = "managed-branch")]
    if format.requires_extension(ofs::managed::ManagedExtension::BranchV1) {
        let branches = metadata.branches(format.volume_id(), data.clone())?;
        let name = match branch {
            Some(name) => parse_branch_name(name)?,
            None => branches.default_name().await?,
        };
        return ManagedVolume::branch(format, data, branches.bind(&name).await?)
            .map_err(Into::into);
    }
    if branch.is_some() {
        bail!("Managed volume does not enable branch/v1");
    }
    match metadata {
        MetadataAuthority::Object(_) => ManagedVolume::object(format, data),
        MetadataAuthority::D1(metadata) => ManagedVolume::d1(format, data, metadata),
    }
    .map_err(Into::into)
}

#[cfg(feature = "managed-branch")]
async fn open_branch_authority(
    config: &Path,
    alias: &str,
    transfer_concurrency: NonZeroUsize,
) -> Result<BranchStore> {
    let ManagedContext {
        format,
        data,
        metadata,
    } = open_managed_context(config, alias, transfer_concurrency).await?;
    if !format.requires_extension(ofs::managed::ManagedExtension::BranchV1) {
        bail!("Managed volume does not enable branch/v1");
    }
    metadata.branches(format.volume_id(), data)
}

async fn open_managed_context(
    config: &Path,
    alias: &str,
    transfer_concurrency: NonZeroUsize,
) -> Result<ManagedContext> {
    let catalog = load_catalog(config)?;
    let definition = catalog
        .get(alias)
        .with_context(|| format!("volume alias {alias:?} is not in the catalog"))?;
    let settings = definition
        .managed_settings()
        .context("this operation requires a Managed volume")?;
    let data = open_operator(&definition.storage, transfer_concurrency)?;
    let metadata = open_metadata(data.clone(), settings.metadata.as_ref())?;
    let format = metadata.read_format().await?;
    if format.volume_id() != definition.volume_id
        || format.metadata_format() != metadata.metadata_format()
    {
        bail!("volume catalog and Managed format v1 binding disagree");
    }
    Ok(ManagedContext {
        format,
        data,
        metadata,
    })
}

async fn create_volume(config: &Path, mut args: VolumeCreateArgs) -> Result<()> {
    if args.enable.len() > 1 {
        bail!("--enable branch may be specified only once");
    }
    let branch_enabled = !args.enable.is_empty();
    if args.model == VolumeModel::Direct && branch_enabled {
        bail!("--enable branch requires --model managed");
    }
    #[cfg(not(feature = "managed-branch"))]
    if branch_enabled {
        bail!("this ofs build does not include the managed-branch feature");
    }
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
        return create_direct_volume(catalog, configured.as_ref(), args);
    }

    let provisional_id = configured
        .as_ref()
        .map(|definition| definition.volume_id)
        .unwrap_or_else(VolumeId::generate);
    let provisional =
        VolumeDefinition::managed(provisional_id, args.storage.clone(), args.metadata.clone())
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
    let data = open_operator(&args.storage, NonZeroUsize::MIN)?;
    let metadata = open_metadata(data.clone(), args.metadata.as_ref())?;
    let desired = ManagedFormat::v1(provisional_id, metadata.metadata_format());
    #[cfg(feature = "managed-branch")]
    let desired = if branch_enabled {
        desired.with_extension(ofs::managed::ManagedExtension::BranchV1)
    } else {
        desired
    };
    let observed = metadata.read_format_optional().await?;
    let format = match observed {
        Some(observed) => observed,
        None if configured.is_some() => metadata.read_format().await?,
        None => match metadata.create_format(&desired).await {
            Ok(created) => created,
            Err(error) if error.kind() == ManagedErrorKind::Conflict => {
                metadata.read_format().await?
            }
            Err(error) => return Err(error.into()),
        },
    };
    if format.metadata_format() != metadata.metadata_format()
        || configured
            .as_ref()
            .is_some_and(|definition| definition.volume_id != format.volume_id())
    {
        bail!("volume catalog and Managed format v1 binding disagree");
    }
    #[cfg(feature = "managed-branch")]
    if branch_enabled && !format.requires_extension(ofs::managed::ManagedExtension::BranchV1) {
        bail!("existing Managed volume does not enable requested extension branch/v1");
    }
    #[cfg(feature = "managed-branch")]
    if format.requires_extension(ofs::managed::ManagedExtension::BranchV1) {
        metadata
            .branches(format.volume_id(), data.clone())?
            .initialize(BranchName::parse("main").expect("main is a valid branch name"))
            .await?;
    } else {
        metadata.validate_volume(format.clone(), data)?;
    }
    #[cfg(not(feature = "managed-branch"))]
    metadata.validate_volume(format.clone(), data)?;
    let volume_id = format.volume_id();
    let definition = VolumeDefinition::managed(volume_id, args.storage, args.metadata)?;
    let registered = catalog.register(&args.alias, definition)?;

    catalog
        .save()
        .context("the remote format is ready but the local catalog could not be saved; fix the catalog path and repeat the same command")?;

    let action = if registered { "registered" } else { "verified" };
    println!(
        "{action} managed volume alias {:?} for volume {volume_id} with format v1",
        args.alias,
    );
    Ok(())
}

fn create_direct_volume(
    mut catalog: Catalog,
    configured: Option<&VolumeDefinition>,
    args: VolumeCreateArgs,
) -> Result<()> {
    if args.metadata.is_some() {
        bail!("--metadata is only valid with --model managed");
    }
    let volume_id = configured
        .map(|definition| definition.volume_id)
        .unwrap_or_else(VolumeId::generate);
    open_operator(&args.storage, NonZeroUsize::MIN)
        .context("cannot configure the Direct volume storage")?;
    let definition = VolumeDefinition::direct(volume_id, args.storage)
        .context("volume URLs must be credential-free; supply credentials through provider environment variables")?;
    let registered = catalog.register(&args.alias, definition)?;
    catalog.save().context(
        "the Direct volume binding could not be saved; fix the catalog path and repeat the command",
    )?;

    let action = if registered { "registered" } else { "verified" };
    println!(
        "{action} direct volume alias {:?} for volume {volume_id}",
        args.alias
    );
    Ok(())
}

fn open_metadata(data: Operator, metadata: Option<&Url>) -> Result<MetadataAuthority> {
    match metadata {
        None => Ok(MetadataAuthority::Object(ObjectMetadata::new(data))),
        Some(url) if url.scheme() == "d1" => D1Metadata::new(d1_config(url)?)
            .map(MetadataAuthority::D1)
            .map_err(Into::into),
        Some(_) => bail!("--metadata must use d1://ACCOUNT/DATABASE/STORE"),
    }
}

async fn sync_volume(config: &Path, args: SyncArgs) -> Result<()> {
    let transfer_concurrency = args.runtime.transfer_concurrency;
    let volume = open_managed_volume(
        config,
        &args.alias,
        args.branch.as_deref(),
        transfer_concurrency,
    )
    .await?;
    let branch_label = volume
        .authority()
        .branch
        .as_ref()
        .map(|branch| format!(" branch {:?}", branch.name.as_str()))
        .unwrap_or_default();
    let engine = SyncEngine::new(volume, transfer_concurrency);
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
        "synced {:?}{} at change {}{}",
        args.alias,
        branch_label,
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
        "volume_alias": alias,
        "volume_id": state.volume.to_string(),
        "branch_name": state.branch.as_ref().map(|branch| branch.name.as_str()),
        "branch_id": state.branch.as_ref().map(|branch| branch.id.to_string()),
        "volume_model": "managed",
        "access_model": "sync",
        "common_sequence": state.common.sequence(),
        "common_operation": state.common.operation().map(|operation| hex_bytes(operation.as_bytes())),
        "pending": state.pending.is_some(),
        "conflicts": state.conflicts.len(),
    });
    if args.json {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        let branch = state
            .branch
            .as_ref()
            .map(|branch| format!(" branch {:?}", branch.name.as_str()))
            .unwrap_or_default();
        println!(
            "managed sync alias {alias:?} for volume {}{branch} at change {}, {} pending, {} conflict(s)",
            state.volume,
            state.common.sequence(),
            usize::from(state.pending.is_some()),
            state.conflicts.len()
        );
    }
    Ok(())
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn load_catalog(config: &Path) -> Result<Catalog> {
    Catalog::load(config).context("cannot open the volume catalog")
}

fn open_operator(url: &Url, transfer_concurrency: NonZeroUsize) -> Result<Operator> {
    Operator::from_uri(url.as_str())
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
    let api_base = url
        .query_pairs()
        .find_map(|(key, value)| (key == "api_base").then(|| value.into_owned()));
    if url.query_pairs().any(|(key, _)| key == "token") {
        bail!("remove token from --metadata and set OFS_D1_TOKEN instead");
    }
    let token = env::var("OFS_D1_TOKEN")
        .map_err(|_| anyhow!("set OFS_D1_TOKEN to the D1 API credential and repeat the command"))?;
    let mut config = D1Config::new(account, path[0], path[1], token).map_err(|_| {
        anyhow!("invalid D1 metadata configuration; check account, database, store, and token")
    })?;
    if let Some(api_base) = api_base {
        config = config
            .with_api_base(api_base)
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
    let capability = operator.info().full_capability();
    if !capability.read || !capability.stat || !capability.list {
        bail!("Direct Mount requires storage read, stat, and list capabilities");
    }
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
