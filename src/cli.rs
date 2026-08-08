// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use ofs::filesystem::VolumeModel;
use url::Url;

#[derive(Debug, Parser)]
#[command(version, about = "OpenDAL filesystem")]
pub(crate) struct Cli {
    /// Volume catalog. OFS_CONFIG provides the same setting.
    #[arg(long, env = "OFS_CONFIG", value_name = "PATH")]
    pub config: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Create or reopen a named volume.
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
    },
    /// Mount a named Direct volume as a read-only online filesystem.
    Mount(MountArgs),
    /// Reconcile and publish a local Managed Sync replica.
    Sync(SyncArgs),
    /// Report the durable state of a local replica.
    Status(StatusArgs),
}

#[derive(Debug, Args)]
pub(crate) struct MountArgs {
    /// Named Direct volume from the local catalog.
    pub alias: String,

    /// Local path where the volume will be mounted.
    pub mount_path: PathBuf,

    #[command(flatten)]
    pub runtime: StorageOptions,
}

#[derive(Debug, Subcommand)]
pub(crate) enum VolumeCommand {
    /// Create or reopen a volume and save its credential-free binding.
    Create(VolumeCreateArgs),
    /// Remove data segments unreachable from the current Managed namespace root.
    Gc(VolumeGcArgs),
}

#[derive(Debug, Args)]
pub(crate) struct VolumeGcArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,

    #[command(flatten)]
    pub runtime: StorageOptions,
}

#[derive(Debug, Args)]
pub(crate) struct VolumeCreateArgs {
    pub alias: String,

    /// Namespace authority model.
    #[arg(long, value_parser = parse_volume_model, value_name = "MODEL")]
    pub model: VolumeModel,

    /// OpenDAL data URL. Credentials are not stored in the catalog.
    #[arg(long, env = "OFS_STORAGE_URL", value_name = "URL")]
    pub storage: Url,

    /// Credential-free D1 metadata URL. Managed volumes also read OFS_METADATA_URL.
    #[arg(long, value_name = "URL")]
    pub metadata: Option<Url>,
}

#[derive(Debug, Args)]
pub(crate) struct SyncArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,

    /// Local directory used as the Sync replica.
    pub replica: PathBuf,

    /// Durable replica state stored outside the replica directory.
    #[arg(long, value_name = "PATH")]
    pub state: PathBuf,

    /// Resolve a retained conflict with the current local candidate. May be repeated.
    #[arg(long, value_name = "RELATIVE_PATH")]
    pub resolve: Vec<String>,

    #[command(flatten)]
    pub runtime: StorageOptions,
}

#[derive(Debug, Args)]
pub(crate) struct StorageOptions {
    /// Maximum concurrency for storage operations in this command.
    #[arg(
        long,
        env = "OFS_TRANSFER_CONCURRENCY",
        default_value = "4",
        value_name = "N"
    )]
    pub transfer_concurrency: NonZeroUsize,
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    /// Durable replica state stored outside the replica directory.
    #[arg(long, value_name = "PATH")]
    pub state: PathBuf,

    /// Emit a machine-readable status object.
    #[arg(long)]
    pub json: bool,
}

fn parse_volume_model(value: &str) -> Result<VolumeModel, String> {
    match value {
        "direct" => Ok(VolumeModel::Direct),
        "managed" => Ok(VolumeModel::Managed),
        _ => Err("expected direct or managed".into()),
    }
}
