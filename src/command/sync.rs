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

use anyhow::{Context, Result, anyhow, bail};
use ofs::managed::ManagedMetadata;
use ofs::sync::{ReplicaState, SyncEngine};
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer};

use crate::cli::SyncArgs;

pub(super) async fn run(args: SyncArgs) -> Result<()> {
    validate_options(&args)?;
    let root = fs::canonicalize(&args.replica)
        .with_context(|| format!("cannot open replica directory: {}", args.replica.display()))?;
    if !root.is_dir() {
        bail!("replica is not a directory: {}", args.replica.display());
    }

    let metadata = ManagedMetadata::object(open_operator(&args)?)?;
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
        .sync(&root, &args.state)
        .await?;
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
        (true, Some("managed")) | (false, None) => Ok(()),
        (true, _) => bail!("--init requires --model managed"),
        (false, Some(_)) => bail!("--model requires --init"),
    }
}

fn open_operator(args: &SyncArgs) -> Result<Operator> {
    let concurrency = args.transfer_concurrency.get();
    Operator::from_uri(args.storage.as_str())
        .map(|operator| {
            operator
                .layer(
                    ConcurrentLimitLayer::new(concurrency).with_http_concurrent_limit(concurrency),
                )
                .layer(RetryLayer::new().with_jitter())
        })
        .map_err(|_| {
            anyhow!("cannot configure --storage; check its scheme, endpoint, bucket, and root")
        })
}
