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

use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(version, about = "OpenDAL filesystem")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Reconcile and publish a local Managed Sync replica.
    Sync(SyncArgs),
    /// Report the durable state of a local Managed Sync replica.
    Status(StatusArgs),
}

#[derive(Debug, Args)]
pub(crate) struct SyncArgs {
    /// Local directory used as the Sync replica.
    pub(crate) replica: PathBuf,

    /// Durable replica state stored outside the replica directory.
    #[arg(long, value_name = "PATH")]
    pub(crate) state: PathBuf,

    /// OpenDAL storage URL. Provider credentials come from the environment.
    #[arg(long, env = "OFS_STORAGE_URL", value_name = "URL")]
    pub(crate) storage: String,

    /// Create the Managed format if it does not exist.
    #[arg(long)]
    pub(crate) init: bool,

    /// Namespace authority model. Required only with --init.
    #[arg(long, value_parser = ["managed"], value_name = "MODEL")]
    pub(crate) model: Option<String>,

    /// Resolve an existing conflict by publishing the current local path.
    #[arg(long, value_name = "RELATIVE-PATH")]
    pub(crate) resolve: Vec<String>,

    /// Maximum concurrency for storage operations.
    #[arg(
        long,
        env = "OFS_TRANSFER_CONCURRENCY",
        default_value = "4",
        value_name = "N"
    )]
    pub(crate) transfer_concurrency: NonZeroUsize,
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
