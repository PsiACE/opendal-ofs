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

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(version, about = "OpenDAL filesystem")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Collect immutable data no longer reachable from the Managed namespace.
    Gc(GcArgs),
    /// Reconcile and publish a local Managed Sync replica.
    Sync(SyncArgs),
    /// Report the durable state of a local Managed Sync replica.
    Status(StatusArgs),
    /// Create and inspect filesystem volumes.
    Volume(VolumeArgs),
}

#[derive(Debug, Args)]
pub(crate) struct GcArgs {
    /// OpenDAL storage URL. Provider credentials come from the environment.
    #[arg(env = "OFS_STORAGE_URL", value_name = "VOLUME")]
    pub(crate) storage: String,

    #[command(flatten)]
    pub(crate) resources: ManagedResourceArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ManagedResourceArgs {
    /// Maximum concurrency for storage operations.
    #[arg(
        long,
        env = "OFS_TRANSFER_CONCURRENCY",
        default_value = "4",
        value_name = "N"
    )]
    pub(crate) transfer_concurrency: NonZeroUsize,

    /// Memory target for each external-sort run, in MiB.
    #[arg(
        long,
        env = "OFS_WORK_MEMORY_MIB",
        default_value = "64",
        value_name = "MIB"
    )]
    pub(crate) work_memory_mib: NonZeroUsize,
}

#[derive(Debug, Args)]
pub(crate) struct SyncArgs {
    /// OpenDAL storage URL of an existing Managed volume.
    #[arg(env = "OFS_STORAGE_URL", value_name = "VOLUME")]
    pub(crate) storage: String,

    /// Local directory used as the Sync replica.
    pub(crate) replica: PathBuf,

    /// Durable replica state stored outside the replica directory.
    #[arg(long, value_name = "PATH")]
    pub(crate) state: PathBuf,

    /// Resolve an existing conflict by publishing the current local path.
    #[arg(long, value_name = "RELATIVE-PATH")]
    pub(crate) resolve: Vec<String>,

    /// Require an optional filesystem capability before synchronization starts.
    #[arg(long, value_enum, value_name = "CAPABILITY")]
    pub(crate) require: Vec<Capability>,

    #[command(flatten)]
    pub(crate) resources: ManagedResourceArgs,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum Capability {
    Executable,
    HardLink,
    PortableNames,
    StableRenameIdentity,
    SymbolicLink,
    Xattr,
}

impl Capability {
    pub(crate) const fn available(self) -> bool {
        match self {
            Self::Executable => cfg!(unix),
            Self::PortableNames => true,
            Self::HardLink | Self::StableRenameIdentity | Self::SymbolicLink | Self::Xattr => false,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Executable => "executable",
            Self::HardLink => "hard-link",
            Self::PortableNames => "portable-names",
            Self::StableRenameIdentity => "stable-rename-identity",
            Self::SymbolicLink => "symbolic-link",
            Self::Xattr => "xattr",
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    /// Local directory used as the Sync replica.
    pub(crate) replica: PathBuf,

    /// Durable replica state stored outside the replica directory.
    #[arg(long, value_name = "PATH")]
    pub(crate) state: PathBuf,

    /// Emit machine-readable status.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct VolumeArgs {
    #[command(subcommand)]
    pub(crate) command: VolumeCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum VolumeCommand {
    /// Create a new volume in empty storage.
    Create(VolumeCreateArgs),
}

#[derive(Debug, Args)]
pub(crate) struct VolumeCreateArgs {
    /// OpenDAL storage URL. Provider credentials come from the environment.
    #[arg(env = "OFS_STORAGE_URL", value_name = "VOLUME")]
    pub(crate) storage: String,

    /// Namespace authority model.
    #[arg(long, value_enum, value_name = "MODEL")]
    pub(crate) model: VolumeModel,

    /// Target encoded size of immutable small-file packs, in MiB.
    #[arg(long, value_name = "MIB")]
    pub(crate) pack_target_mib: Option<NonZeroU64>,

    /// Enable a Managed extension; repeat to compose FastCDC with Zstandard.
    #[arg(long = "ext", value_enum, value_name = "EXTENSION")]
    pub(crate) extensions: Vec<ManagedExtension>,

    /// Zstandard compression level used by the zstd extension.
    #[arg(long, default_value = "3", value_name = "LEVEL")]
    pub(crate) zstd_level: i32,

    #[command(flatten)]
    pub(crate) resources: ManagedResourceArgs,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum VolumeModel {
    Managed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum ManagedExtension {
    #[value(name = "fastcdc")]
    FastCdc,
    Zstd,
}
