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
use ofs::managed::ManagedMetadata;
use ofs_extras::FileExtensions;

use crate::cli::{VolumeArgs, VolumeCommand, VolumeCreateArgs};

use super::provider::open_operator;

pub(super) async fn run(args: VolumeArgs) -> Result<()> {
    match args.command {
        VolumeCommand::Create(args) => create(args).await,
    }
}

async fn create(args: VolumeCreateArgs) -> Result<()> {
    let crate::cli::VolumeModel::Managed = args.model;
    let metadata = ManagedMetadata::new(
        open_operator(
            &args.storage,
            args.resources.transfer_concurrency,
            args.resources.trace,
        )?,
        args.resources.transfer_concurrency,
        args.resources.work_memory_mib,
    )?;
    let extension_count = |extension| {
        args.extensions
            .iter()
            .filter(|candidate| **candidate == extension)
            .count()
    };
    if args
        .extensions
        .iter()
        .any(|extension| extension_count(*extension) != 1)
    {
        anyhow::bail!("each --ext may be enabled only once");
    }
    let branch = extension_count(crate::cli::ManagedExtension::Branch) == 1;
    let fastcdc = extension_count(crate::cli::ManagedExtension::FastCdc) == 1;
    let zstd = extension_count(crate::cli::ManagedExtension::Zstd) == 1;
    let file_extensions = match (fastcdc, zstd) {
        (false, false) => None,
        (true, false) => Some(FileExtensions::FastCdc),
        (true, true) => Some(FileExtensions::FastCdcZstd {
            level: args.zstd_level,
        }),
        (false, true) => anyhow::bail!("the zstd extension requires fastcdc"),
    };
    if file_extensions.is_some() && args.pack_target_mib.is_some() {
        anyhow::bail!("--pack-target-mib cannot be combined with --ext");
    }
    let pack_target_bytes = args
        .pack_target_mib
        .map(|target| {
            target
                .get()
                .checked_mul(1024 * 1024)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| anyhow::anyhow!("--pack-target-mib overflows"))
        })
        .transpose()?;
    let metadata = if branch {
        ofs_extras::with_branch(metadata, "main")
    } else {
        metadata
    };
    let volume = match file_extensions {
        Some(extensions) => {
            extensions
                .configure(metadata, args.resources.trace)
                .initialize_extension()
                .await?
        }
        None => metadata.initialize(pack_target_bytes).await?,
    };
    println!("created managed volume {}", volume.id());
    Ok(())
}
