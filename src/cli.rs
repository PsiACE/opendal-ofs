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

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(version, about = "OpenDAL filesystem")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Create and inspect filesystem volumes.
    Volume(VolumeArgs),
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
    /// Inspect a volume by its local name.
    Inspect(VolumeInspectArgs),
}

#[derive(Debug, Args)]
pub(crate) struct VolumeCreateArgs {
    /// Local volume name resolved by later commands.
    pub(crate) volume: String,

    /// Namespace authority model.
    #[arg(long, value_enum, value_name = "MODEL")]
    pub(crate) model: VolumeModel,

    /// OpenDAL storage URL. Provider credentials come from the environment.
    #[arg(long, env = "OFS_STORAGE_URL", value_name = "URL")]
    pub(crate) storage: String,

    /// Shared data-segment rotation target.
    #[arg(
        long,
        value_name = "SIZE",
        default_value = "8MiB",
        value_parser = parse_size
    )]
    pub(crate) data_segment_target_size: u64,
}

#[derive(Debug, Args)]
pub(crate) struct VolumeInspectArgs {
    /// Local volume name.
    pub(crate) volume: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum VolumeModel {
    Managed,
}

pub(crate) fn parse_size(value: &str) -> Result<u64, String> {
    let value = value.trim();
    let (digits, multiplier) = if let Some(digits) = value.strip_suffix("GiB") {
        (digits, 1024 * 1024 * 1024)
    } else if let Some(digits) = value.strip_suffix("MiB") {
        (digits, 1024 * 1024)
    } else if let Some(digits) = value.strip_suffix("KiB") {
        (digits, 1024)
    } else if let Some(digits) = value.strip_suffix('B') {
        (digits, 1)
    } else {
        (value, 1)
    };
    let count = digits
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("invalid size {value}"))?;
    count
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size {value} overflows"))
}
