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

use std::fs;

use anyhow::{Context, Result, bail};
use ofs::managed::ManagedMetadata;
use ofs::sync::{ReplicaState, SyncEngine};

use crate::cli::SyncArgs;

use super::provider::open_operator;

pub(super) async fn run(args: SyncArgs) -> Result<()> {
    validate_options(&args)?;
    let root = fs::canonicalize(&args.replica)
        .with_context(|| format!("cannot open replica directory: {}", args.replica.display()))?;
    if !root.is_dir() {
        bail!("replica is not a directory: {}", args.replica.display());
    }

    let metadata = ManagedMetadata::object(open_operator(
        &args.storage,
        args.transfer_concurrency,
    )?)?;
    if args.init {
        let volume = metadata.initialize().await?;
        let observed = volume.observe().await?;
        ReplicaState::new(root, observed.snapshot)?.save_new(&args.state)?;
        println!("initialized managed sync volume {}", volume.id());
        return Ok(());
    }

    let stored = ReplicaState::load(&args.state)?;
    let volume = match &stored {
        Some(state) => metadata.open(state.volume_id()).await?,
        None => metadata.open_unbound().await?,
    };
    let result = SyncEngine::new(volume.clone())
        .sync(&root, &args.state, &args.resolve)
        .await?;
    if result.conflicts != 0 {
        bail!(
            "sync retained {} conflict(s); inspect `ofs status` and resolve explicitly",
            result.conflicts
        );
    }
    println!(
        "synced managed volume {} at change {}{}",
        volume.id(),
        result.sequence,
        if result.published { " (published)" } else { "" }
    );
    Ok(())
}

fn validate_options(args: &SyncArgs) -> Result<()> {
    match (args.init, args.model.as_deref()) {
        (true, Some("managed")) if args.resolve.is_empty() => Ok(()),
        (false, None) => Ok(()),
        (true, Some("managed")) => bail!("--resolve cannot be used with --init"),
        (true, _) => bail!("--init requires --model managed"),
        (false, Some(_)) => bail!("--model requires --init"),
    }
}
