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
use ofs_core::{ManagedVolume, VolumeRuntime};
use ofs_extras::{CreateOptions, access, compose};

use crate::cli::{VolumeArgs, VolumeCommand, VolumeCreateArgs, VolumeInspectArgs};
use crate::locator::{VolumeLocator, model_name};

use super::operator::open_storage;

pub(super) async fn run(args: VolumeArgs) -> Result<()> {
    match args.command {
        VolumeCommand::Create(args) => create(args).await,
        VolumeCommand::Inspect(args) => inspect(args).await,
    }
}

async fn create(args: VolumeCreateArgs) -> Result<()> {
    let crate::cli::VolumeModel::Managed = args.model;
    let locator = VolumeLocator::from_env()?;
    locator.create(&args.volume, args.model, &args.storage)?;
    let operator = open_storage(&args.storage)?;
    let options = CreateOptions::new(args.data_segment_target_size).map_err(anyhow::Error::msg)?;
    let layout = options.file_data_layout().map_err(anyhow::Error::msg)?;
    let volume = ManagedVolume::create(
        &operator,
        ofs_core::CreateOptions::new(layout),
        access(),
        VolumeRuntime::standard(),
        "main",
    )
    .await
    .map_err(anyhow::Error::msg)?;
    compose(volume.format()).map_err(anyhow::Error::msg)?;
    println!("created managed volume {} ({})", args.volume, volume.id());
    Ok(())
}

async fn inspect(args: VolumeInspectArgs) -> Result<()> {
    let locator = VolumeLocator::from_env()?;
    let record = locator.resolve(&args.volume)?;
    let operator = open_storage(&record.storage)?;
    let volume = ManagedVolume::open(&operator, access(), VolumeRuntime::standard(), "main")
        .await
        .map_err(anyhow::Error::msg)?;
    let components = compose(volume.format()).map_err(anyhow::Error::msg)?;
    let format = components.format();
    println!("volume {}", args.volume);
    println!("model {}", model_name(record.model));
    println!("layout v0");
    println!("id {}", format.volume_id());
    println!(
        "data-segment-target-size {}B",
        format.file_data_layout().data_segment_target_bytes()
    );
    Ok(())
}
