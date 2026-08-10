// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
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
    /// Register a local alias, creating the remote volume format when absent.
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
    },
    /// Manage durable branches of a Managed volume.
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
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
    /// Register an alias and save its credential-free volume binding.
    Create(VolumeCreateArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum BranchCommand {
    /// List the branches of a Managed volume.
    List(BranchListArgs),
    /// Show one branch and its current durable position.
    Show(BranchShowArgs),
    /// Fork a new branch from a current or retained position.
    Create(BranchCreateArgs),
    /// Delete a branch without immediately deleting shared data.
    Delete(BranchDeleteArgs),
}

#[derive(Debug, Args)]
pub(crate) struct BranchListArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,

    /// Emit a machine-readable branch list.
    #[arg(long)]
    pub json: bool,

    #[command(flatten)]
    pub runtime: StorageOptions,
}

#[derive(Debug, Args)]
pub(crate) struct BranchShowArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,

    /// Branch to show.
    pub branch: String,

    /// Emit a machine-readable branch description.
    #[arg(long)]
    pub json: bool,

    #[command(flatten)]
    pub runtime: StorageOptions,
}

#[derive(Debug, Args)]
pub(crate) struct BranchCreateArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,

    /// Name of the new branch.
    pub branch: String,

    /// Source branch. Defaults to the volume's default branch.
    #[arg(long, value_name = "BRANCH")]
    pub from: Option<String>,

    /// Retained source sequence. Defaults to the current source position.
    #[arg(long, value_name = "SEQUENCE")]
    pub at: Option<u64>,

    #[command(flatten)]
    pub runtime: StorageOptions,
}

#[derive(Debug, Args)]
pub(crate) struct BranchDeleteArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,

    /// Branch to delete.
    pub branch: String,

    #[command(flatten)]
    pub runtime: StorageOptions,
}

#[derive(Debug, Args)]
pub(crate) struct VolumeCreateArgs {
    pub alias: String,

    /// Namespace authority model.
    #[arg(long, value_parser = parse_volume_model, value_name = "MODEL")]
    pub model: VolumeModel,

    /// Managed volume feature to require.
    #[arg(long, value_enum, value_name = "FEATURE")]
    pub enable: Option<EnableFeature>,

    /// OpenDAL data URL. Credentials are not stored in the catalog.
    #[arg(long, env = "OFS_STORAGE_URL", value_name = "URL")]
    pub storage: Url,

    /// Credential-free D1 metadata URL. Managed volumes also read OFS_METADATA_URL.
    #[arg(long, value_name = "URL")]
    pub metadata: Option<Url>,

    #[command(flatten)]
    pub runtime: StorageOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum EnableFeature {
    /// Durable named branches (`branch/v1`).
    Branch,
}

#[derive(Debug, Args)]
pub(crate) struct SyncArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,

    /// Local directory used as the Sync replica.
    pub replica: PathBuf,

    /// Branch to synchronize. Defaults to the volume's default branch.
    #[arg(long, value_name = "BRANCH")]
    pub branch: Option<String>,

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
