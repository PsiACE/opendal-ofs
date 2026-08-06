// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use url::Url;

#[derive(Debug, Parser)]
#[command(name = "ofs", version, about)]
pub(crate) struct Cli {
    /// Credential-free volume catalog and client settings.
    #[arg(long, env = "OFS_CONFIG", global = true, value_name = "CATALOG")]
    pub(crate) config: Option<PathBuf>,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Create named volumes.
    Volume(VolumeArgs),
    /// Mount a named volume.
    Mount(MountArgs),
    /// Reconcile a named volume with a native directory.
    Sync(SyncArgs),
    /// Inspect a synchronized native directory.
    Status(StatusArgs),
}

#[derive(Debug, Args)]
pub(crate) struct VolumeArgs {
    #[command(subcommand)]
    pub(crate) command: VolumeCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum VolumeCommand {
    /// Create or reopen a named volume.
    Create(VolumeCreateArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum VolumeModel {
    Direct,
    Managed,
}

#[derive(Debug, Args)]
pub(crate) struct VolumeCreateArgs {
    pub(crate) name: String,
    #[arg(long, value_enum)]
    pub(crate) model: VolumeModel,
    /// OpenDAL URL for immutable data storage.
    #[arg(long)]
    pub(crate) storage: Url,
    /// Optional external Metadata Store URL.
    #[arg(long)]
    pub(crate) metadata: Option<Url>,
}

#[derive(Debug, Args)]
pub(crate) struct MountArgs {
    pub(crate) volume: String,
    pub(crate) path: PathBuf,
}

#[derive(Debug, Args)]
pub(crate) struct SyncArgs {
    pub(crate) volume: String,
    pub(crate) directory: PathBuf,
    /// Durable state outside the synchronized tree.
    #[arg(long)]
    pub(crate) state: Option<PathBuf>,
    /// Publish the current local shape for a retained conflict path.
    #[arg(long, action = clap::ArgAction::Append)]
    pub(crate) resolve: Vec<PathBuf>,
    /// Require a user-visible capability before reconciliation starts.
    #[arg(long, action = clap::ArgAction::Append)]
    pub(crate) require: Vec<String>,
    /// Maximum number of complete content transfer jobs.
    #[arg(long, env = "OFS_SYNC_TRANSFER_CONCURRENCY", value_name = "N")]
    pub(crate) transfer_concurrency: Option<NonZeroUsize>,
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    pub(crate) directory: PathBuf,
    #[arg(long)]
    pub(crate) state: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
}
