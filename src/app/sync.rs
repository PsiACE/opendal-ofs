// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use anyhow::{Context, Result, bail};
use ofs::sync::{ReplicaState, ReplicaTarget, SyncEngine, SyncVolume};

use crate::cli::{StatusArgs, SyncArgs, TargetOptions};

use super::providers::{initialize_managed_volume, open_managed_volume};

pub(super) async fn sync_volume(args: SyncArgs) -> Result<()> {
    let transfer_concurrency = args.runtime.transfer_concurrency;
    if args.enable.is_some() && !args.init {
        bail!("--enable requires --init");
    }
    let stored = ReplicaState::load(&args.state)?;
    if stored.is_some() && args.init {
        bail!("--init requires a new replica state");
    }
    let target = resolve_target(stored.as_ref(), args.target)?;
    let branch = resolve_branch(stored.as_ref(), args.branch.as_ref())?;
    let expected_volume = stored.as_ref().map(|state| state.volume);
    let volume = if args.init {
        initialize_managed_volume(
            &target,
            expected_volume,
            branch.as_ref(),
            args.enable.is_some(),
            transfer_concurrency,
        )
        .await?
    } else {
        open_managed_volume(
            &target,
            expected_volume,
            branch.as_ref(),
            transfer_concurrency,
        )
        .await?
    };
    let volume_id = volume.id();
    let branch_label = volume
        .authority()
        .branch
        .as_ref()
        .map(|branch| format!(" branch {:?}", branch.name.as_str()))
        .unwrap_or_default();
    let engine = SyncEngine::new(volume, transfer_concurrency);
    let result = engine
        .sync(&args.replica, &args.state, target, &args.resolve)
        .await?;
    if !result.conflicts.is_empty() {
        bail!(
            "sync retained {} conflict(s); inspect `ofs status` and resolve explicitly",
            result.conflicts.len()
        );
    }
    if result.pending {
        bail!("publication result is unknown; repeat sync to resolve its durable intent");
    }
    println!(
        "synced managed volume {}{} at change {}{}",
        volume_id,
        branch_label,
        result.common.sequence(),
        if result.published { " (published)" } else { "" }
    );
    Ok(())
}

pub(super) fn status(args: StatusArgs) -> Result<()> {
    let state = ReplicaState::load(&args.state)?
        .with_context(|| format!("replica state does not exist: {}", args.state.display()))?;
    let value = serde_json::json!({
        "volume_id": state.volume.to_string(),
        "branch_name": state.branch.as_ref().map(|branch| branch.name.as_str()),
        "branch_id": state.branch.as_ref().map(|branch| branch.id.to_string()),
        "volume_model": state.target().model().as_str(),
        "access_model": "sync",
        "capabilities": {
            "portable_names": true,
            "stable_rename_identity": cfg!(unix),
            "executable": cfg!(unix),
            "symbolic_links": false,
            "hard_links": false,
            "remote_durability": "explicit_sync",
            "namespace_publication": "generation_cas",
        },
        "common_sequence": state.common().sequence(),
        "common_operation": state.common().operation().map(|operation| operation.to_string()),
        "pending": state.has_pending(),
        "conflicts": state.conflicts.len(),
    });
    if args.json {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        let branch = state
            .branch
            .as_ref()
            .map(|branch| format!(" branch {:?}", branch.name.as_str()))
            .unwrap_or_default();
        println!(
            "managed sync volume {}{branch} at change {}, {} pending, {} conflict(s)",
            state.volume,
            state.common().sequence(),
            usize::from(state.has_pending()),
            state.conflicts.len()
        );
    }
    Ok(())
}

fn resolve_target(state: Option<&ReplicaState>, options: TargetOptions) -> Result<ReplicaTarget> {
    let TargetOptions {
        model,
        storage,
        metadata,
    } = options;
    match state {
        Some(state) => {
            let current = state.target();
            let supplied = model.is_some() || storage.is_some() || metadata.is_some();
            if !supplied {
                return Ok(current.clone());
            }
            let requested = ReplicaTarget::new(
                model.unwrap_or_else(|| current.model()),
                storage.unwrap_or_else(|| current.storage().clone()),
                metadata.or_else(|| current.metadata().cloned()),
            )?;
            if &requested != current {
                bail!("target options disagree with the existing replica state");
            }
            Ok(requested)
        }
        None => ReplicaTarget::new(
            model.context("--model managed is required for a new replica state")?,
            storage.context("--storage is required for a new replica state")?,
            metadata,
        ),
    }
}

fn resolve_branch(
    state: Option<&ReplicaState>,
    requested: Option<&ofs::filesystem::BranchName>,
) -> Result<Option<ofs::filesystem::BranchName>> {
    let stored = state.and_then(|state| state.branch.as_ref().map(|branch| &branch.name));
    if let (Some(stored), Some(requested)) = (stored, requested)
        && stored != requested
    {
        bail!("--branch disagrees with the existing replica state");
    }
    Ok(stored.cloned().or_else(|| requested.cloned()))
}
