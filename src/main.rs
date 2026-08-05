// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use uuid::Uuid;

use catalog::{Catalog, MetadataConfig, StorageLocator, VolumeDefinition};
use model::{FormatRecord, MetadataPlacement, VolumeId};
use store::MetadataStore;

mod catalog;
mod cli;
mod d1;
mod model;
mod reconcile;
mod replica;
mod status;
mod store;
mod sync;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let invocation = cli::Invocation::parse();

    logforth::starter_log::stderr().apply();
    match invocation {
        cli::Invocation::Command(command) => execute_command(command).await,
        cli::Invocation::DirectMount(args) => execute_direct_mount(args).await,
    }
}

async fn execute_command(command: cli::Cli) -> Result<()> {
    match command.command {
        cli::Command::Volume(args) => match args.command {
            cli::VolumeCommand::Create(args) => create_volume(command.config, args).await,
        },
        cli::Command::Mount(args) => mount_direct(command.config, args).await,
        cli::Command::Sync(args) => sync_managed(command.config, args).await,
        cli::Command::Status(args) => status_managed(command.config, args).await,
    }
}

async fn mount_direct(
    requested_catalog: Option<std::path::PathBuf>,
    args: cli::MountArgs,
) -> Result<()> {
    let path = catalog::catalog_path(requested_catalog)?;
    let catalog = Catalog::load(&path)?;
    let definition = catalog.get(&args.volume)?;
    if definition.metadata().is_some() {
        bail!("Managed Mount is not available");
    }
    execute_operator_mount(
        store::assemble_operator(definition.storage(), None, None)?,
        args.path,
    )
    .await
}

async fn status_managed(
    requested_catalog: Option<std::path::PathBuf>,
    args: cli::StatusArgs,
) -> Result<()> {
    let paths = replica::ReplicaPaths::resolve(&args.directory, args.state.as_deref())?;
    let state = replica::ReplicaState::load(&paths)?;
    let path = catalog::catalog_path(requested_catalog)?;
    let catalog = Catalog::load(&path)?;
    let (name, definition) = catalog.get_by_id(&state.volume_id)?;
    let metadata = definition
        .metadata()
        .context("replica is not bound to a Managed volume")?;
    let placement = if metadata.external_locator().is_some() {
        "external-d1"
    } else {
        "colocated-object"
    };
    let status_operator = if metadata.external_locator().is_some() {
        None
    } else {
        store::assemble_operator(definition.storage(), None, None).ok()
    };
    let remote = match metadata_store(metadata, status_operator.as_ref(), None) {
        Ok(store) => store.observe(&state.volume_id).await.ok(),
        Err(_) => None,
    };
    let status = status::SyncStatus::inspect(name, placement, &paths, &state, remote.as_ref())?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        println!("{status}");
    }
    Ok(())
}

async fn sync_managed(
    requested_catalog: Option<std::path::PathBuf>,
    args: cli::SyncArgs,
) -> Result<()> {
    let path = catalog::catalog_path(requested_catalog)?;
    let catalog = Catalog::load(&path)?;
    let transfers = args
        .transfer_concurrency
        .unwrap_or_else(|| catalog.transfer_concurrency());
    let definition = catalog.get(&args.volume)?;
    let metadata = definition
        .metadata()
        .context("Direct Sync is not available")?;
    sync::admit(&args.require)?;
    let operator = store::assemble_operator(definition.storage(), None, Some(transfers))?;
    let volume = sync::ManagedVolume {
        metadata: metadata_store(metadata, Some(&operator), None)?,
        data: store::DataStore::new(operator)?,
    };
    let generation = sync::sync_once(
        &volume,
        sync::SyncRequest {
            volume_id: definition.id(),
            local: &args.directory,
            state: args.state.as_deref(),
            resolutions: &args.resolve,
            transfers,
        },
    )
    .await?;
    println!("Managed Sync completed at generation {generation}");
    Ok(())
}

async fn create_volume(
    requested_catalog: Option<std::path::PathBuf>,
    args: cli::VolumeCreateArgs,
) -> Result<()> {
    let path = catalog::catalog_path(requested_catalog)?;
    let mut catalog = Catalog::load(&path)?;
    let storage = StorageLocator::parse(&args.storage)?;
    match args.model {
        cli::VolumeModel::Direct => {
            if args.metadata.is_some() {
                bail!("a Direct volume cannot use --metadata");
            }
            let definition = VolumeDefinition::direct(new_id()?, storage);
            catalog.insert(args.name, definition)?;
            catalog.save(&path)
        }
        cli::VolumeModel::Managed => {
            let metadata = match args.metadata.as_ref() {
                Some(url) => MetadataConfig::external(StorageLocator::parse(url)?),
                None => MetadataConfig::ColocatedObject,
            };
            if let Ok(existing) = catalog.get(&args.name) {
                if existing.storage() != &storage || existing.metadata() != Some(&metadata) {
                    bail!("volume name already refers to a different definition");
                }
                let operator = store::assemble_operator(&storage, Some(&args.storage), None)?;
                store::DataStore::new(operator.clone())?;
                let metadata_store =
                    metadata_store(&metadata, Some(&operator), args.metadata.as_ref())?;
                metadata_store.observe(existing.id()).await?;
                return Ok(());
            }
            let operator = store::assemble_operator(&storage, Some(&args.storage), None)?;
            store::DataStore::new(operator.clone())?;
            let metadata_store =
                metadata_store(&metadata, Some(&operator), args.metadata.as_ref())?;
            let placement = if metadata.external_locator().is_some() {
                MetadataPlacement::ExternalD1
            } else {
                MetadataPlacement::ColocatedObject
            };
            let proposed =
                FormatRecord::new(new_id()?, placement, store::data_store_id(&storage)?)?;
            let observed = metadata_store.initialize(proposed).await?;
            catalog.insert(
                args.name,
                VolumeDefinition::managed(observed.format.volume_id, storage, metadata),
            )?;
            catalog.save(&path)
        }
    }
}

fn metadata_store(
    config: &MetadataConfig,
    operator: Option<&opendal::Operator>,
    current_url: Option<&url::Url>,
) -> Result<Box<dyn MetadataStore>> {
    match config.external_locator() {
        Some(locator) => Ok(Box::new(d1::D1MetadataStore::new(d1::D1Config::resolve(
            locator,
            current_url,
        )?)?)),
        None => Ok(Box::new(store::ObjectMetadataStore::new(
            operator
                .context("colocated metadata requires the Data Store operator")?
                .clone(),
        )?)),
    }
}

fn new_id() -> Result<VolumeId> {
    VolumeId::parse(Uuid::new_v4().to_string())
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn execute_direct_mount(cfg: cli::DirectMountArgs) -> Result<()> {
    use opendal::Operator;

    if cfg.backend.has_host() {
        log::warn!("backend host will be ignored");
    }

    let scheme_str = cfg.backend.scheme();
    let op_args = cfg.backend.query_pairs().into_owned();

    let backend = Operator::via_iter(scheme_str, op_args)
        .map_err(|err| anyhow!("invalid scheme or arguments for {scheme_str}: {err}"))?;

    execute_operator_mount(backend, cfg.mount_path).await
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn execute_operator_mount(
    backend: opendal::Operator,
    mount_path: std::path::PathBuf,
) -> Result<()> {
    use fuse3::MountOptions;
    use fuse3::path::Session;
    use std::env;

    let mut mount_options = MountOptions::default();
    let mut gid = nix::unistd::getgid().into();
    mount_options.gid(gid);
    let mut uid = nix::unistd::getuid().into();
    mount_options.uid(uid);

    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    let mut mount_handle = if nix::unistd::getuid().is_root() {
        if let Some(sudo_gid) = env::var("SUDO_GID")
            .ok()
            .and_then(|gid_str| gid_str.parse::<u32>().ok())
        {
            mount_options.gid(sudo_gid);
            gid = sudo_gid;
        }

        if let Some(sudo_uid) = env::var("SUDO_UID")
            .ok()
            .and_then(|gid_str| gid_str.parse::<u32>().ok())
        {
            mount_options.uid(uid);
            uid = sudo_uid;
        }

        let fs = fuse3_opendal::Filesystem::new(backend, uid, gid);
        Session::new(mount_options).mount(fs, mount_path).await?
    } else {
        let fs = fuse3_opendal::Filesystem::new(backend, uid, gid);
        Session::new(mount_options)
            .mount_with_unprivileged(fs, mount_path)
            .await?
    };

    let handle = &mut mount_handle;
    tokio::select! {
        res = handle => res?,
        _ = tokio::signal::ctrl_c() => {
            mount_handle.unmount().await?
        }
    }

    Ok(())
}
