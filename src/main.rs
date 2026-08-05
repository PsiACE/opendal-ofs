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

use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use uuid::Uuid;

use catalog::{Catalog, MetadataConfig, StorageLocator, VolumeDefinition};
use model::{FormatRecord, MetadataPlacement, VolumeId};
use store::MetadataStore;

mod catalog;
mod cli;
mod model;
mod store;

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
        cli::Command::Mount(args) => {
            let _ = (command.config, args.volume, args.path);
            bail!("volume-oriented mount is not implemented")
        }
        cli::Command::Sync(args) => {
            let _ = (
                command.config,
                args.volume,
                args.directory,
                args.state,
                args.resolve,
                args.transfer_concurrency,
            );
            bail!("Managed Sync is not available in this commit")
        }
        cli::Command::Status(args) => {
            let _ = (command.config, args.directory, args.state, args.json);
            bail!("Managed Sync status is not available in this commit")
        }
    }
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
            if args.metadata.is_some() {
                bail!("external Metadata Store support is not available in this commit");
            }
            let metadata = MetadataConfig::ColocatedObject;
            if let Ok(existing) = catalog.get(&args.name) {
                if existing.storage() != &storage || existing.metadata() != Some(&metadata) {
                    bail!("volume name already refers to a different definition");
                }
                let operator = store::assemble_operator(&storage, Some(&args.storage))?;
                store::DataStore::new(operator.clone())?;
                let object = store::ObjectMetadataStore::new(operator)?;
                object.observe(existing.id()).await?;
                return Ok(());
            }
            let operator = store::assemble_operator(&storage, Some(&args.storage))?;
            store::DataStore::new(operator.clone())?;
            let object = store::ObjectMetadataStore::new(operator)?;
            let proposed = FormatRecord::new(
                new_id()?,
                MetadataPlacement::ColocatedObject,
                store::data_store_id(&storage)?,
            )?;
            let observed = object.initialize(proposed).await?;
            catalog.insert(
                args.name,
                VolumeDefinition::managed(observed.format.volume_id, storage, metadata),
            )?;
            catalog.save(&path)
        }
    }
}

fn new_id() -> Result<VolumeId> {
    VolumeId::parse(Uuid::new_v4().to_string())
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn execute_direct_mount(cfg: cli::DirectMountArgs) -> Result<()> {
    use fuse3::MountOptions;
    use fuse3::path::Session;
    use opendal::Operator;
    use std::env;

    if cfg.backend.has_host() {
        log::warn!("backend host will be ignored");
    }

    let scheme_str = cfg.backend.scheme();
    let op_args = cfg.backend.query_pairs().into_owned();

    let backend = Operator::via_iter(scheme_str, op_args)
        .map_err(|err| anyhow!("invalid scheme or arguments for {scheme_str}: {err}"))?;

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
        Session::new(mount_options)
            .mount(fs, cfg.mount_path)
            .await?
    } else {
        let fs = fuse3_opendal::Filesystem::new(backend, uid, gid);
        Session::new(mount_options)
            .mount_with_unprivileged(fs, cfg.mount_path)
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
