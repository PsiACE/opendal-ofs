// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use ofs::filesystem::VolumeModel;
use url::Url;

#[derive(Debug, Parser)]
#[command(version, about = "OpenDAL filesystem")]
pub(crate) struct Cli {
    /// Volume catalog. OFS_CONFIG provides the same setting.
    #[arg(long, env = "OFS_CONFIG", global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Command>,

    /// FUSE mount path for the Direct Mount compatibility form.
    #[arg(env = "OFS_MOUNT_PATH", index = 1, value_name = "MOUNT_PATH")]
    pub mount_path: Option<PathBuf>,

    /// OpenDAL URL for the Direct Mount compatibility form.
    #[arg(env = "OFS_BACKEND", index = 2, value_name = "BACKEND_URL")]
    pub backend: Option<Url>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Create or inspect a named volume.
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
    },
    /// Reconcile and publish a local Sync replica.
    Sync(SyncArgs),
    /// Report the durable state of a local replica.
    Status(StatusArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum VolumeCommand {
    /// Create or reopen a volume and save its credential-free binding.
    Create(VolumeCreateArgs),
    /// Pack live small whole files through the current Managed namespace root.
    Pack(VolumePackArgs),
    /// Remove loose data unreachable from the current Managed namespace root.
    Gc(VolumeGcArgs),
}

#[derive(Debug, Args)]
pub(crate) struct VolumeGcArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,
}

#[derive(Debug, Args)]
pub(crate) struct VolumePackArgs {
    /// Named Managed volume from the local catalog.
    pub alias: String,

    /// Repack dead entries, then wait this process-local grace period before retiring old packs.
    #[arg(long, env = "OFS_PACK_GRACE_SECONDS", value_name = "SECONDS")]
    pub repack_grace_seconds: Option<u64>,

    /// Wait before reclaiming loose objects that have a verified packed location.
    #[arg(
        long,
        env = "OFS_PACK_RECLAIM_LOOSE_AFTER_SECONDS",
        value_name = "SECONDS"
    )]
    pub reclaim_loose_after_seconds: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct VolumeCreateArgs {
    pub alias: String,

    /// Namespace authority model. This implementation supports managed.
    #[arg(long, value_parser = parse_volume_model, value_name = "MODEL")]
    pub model: VolumeModel,

    /// OpenDAL data URL. Credentials are not stored in the catalog.
    #[arg(long, env = "OFS_STORAGE_URL", value_name = "URL")]
    pub storage: Url,

    /// Credential-free D1 metadata URL. Set its token with OFS_D1_TOKEN.
    #[arg(long, env = "OFS_METADATA_URL", value_name = "URL")]
    pub metadata: Option<Url>,

    /// Foreground file layout. Existing volumes keep their configured value when omitted.
    #[arg(long, env = "OFS_FILE_LAYOUT", value_enum, value_name = "LAYOUT")]
    pub file_layout: Option<FileLayoutArg>,

    /// Smallest file written with FastCDC.
    #[arg(long, env = "OFS_FASTCDC_MINIMUM_FILE_SIZE", value_name = "BYTES")]
    pub fastcdc_minimum_file_size: Option<u64>,

    /// Minimum FastCDC chunk size.
    #[arg(long, env = "OFS_FASTCDC_MINIMUM_CHUNK_SIZE", value_name = "BYTES")]
    pub fastcdc_minimum_chunk_size: Option<u32>,

    /// Target FastCDC chunk size.
    #[arg(long, env = "OFS_FASTCDC_TARGET_CHUNK_SIZE", value_name = "BYTES")]
    pub fastcdc_target_chunk_size: Option<u32>,

    /// Maximum FastCDC chunk size.
    #[arg(long, env = "OFS_FASTCDC_MAXIMUM_CHUNK_SIZE", value_name = "BYTES")]
    pub fastcdc_maximum_chunk_size: Option<u32>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum FileLayoutArg {
    Whole,
    #[value(name = "fastcdc")]
    FastCdc,
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

    /// Resolve this retained conflict with the current local candidate.
    #[arg(long, value_name = "RELATIVE_PATH")]
    pub resolve: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    /// Local directory used as the Sync replica.
    pub replica: PathBuf,

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
