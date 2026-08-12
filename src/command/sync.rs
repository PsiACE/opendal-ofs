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
    let root = fs::canonicalize(&args.replica)
        .with_context(|| format!("cannot open replica directory: {}", args.replica.display()))?;
    if !root.is_dir() {
        bail!("replica is not a directory: {}", args.replica.display());
    }
    if let Some(capability) = args
        .require
        .iter()
        .find(|capability| !capability.available())
    {
        bail!(
            "required filesystem capability is unavailable: {}",
            capability.name()
        );
    }

    let metadata =
        ManagedMetadata::object(open_operator(&args.storage, args.transfer_concurrency)?)?;
    let stored = ReplicaState::load(&args.state)?;
    let volume = match &stored {
        Some(state) => metadata.open(Some(state.volume_id())).await?,
        None => metadata.open(None).await?,
    };
    let result = SyncEngine::new(volume.clone(), args.transfer_concurrency)
        .sync(&root, &args.state, &args.resolve)
        .await?;
    if !result.conflict_paths.is_empty() {
        let paths = result
            .conflict_paths
            .iter()
            .map(|path| format!("  {path}"))
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "sync retained {} conflict(s); rerun with `--resolve <relative-path>` for each normalized relative path:\n{}",
            result.conflict_paths.len(),
            paths
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
