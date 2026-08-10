// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use anyhow::Result;

use crate::cli::{Cli, Command, VolumeCommand};

mod branch;
mod providers;
mod sync;
mod volume;

use branch::branch_command;
use sync::{status, sync_volume};
use volume::{create_volume, gc_volume};

pub(crate) async fn run(cli: Cli) -> Result<()> {
    let config = cli.config;
    match cli.command {
        Command::Volume {
            command: VolumeCommand::Create(args),
        } => create_volume(&config, args).await,
        Command::Volume {
            command: VolumeCommand::Gc(args),
        } => gc_volume(&config, args).await,
        Command::Branch(args) => branch_command(&config, args).await,
        Command::Sync(args) => sync_volume(&config, args).await,
        Command::Status(args) => status(&config, args),
    }
}
