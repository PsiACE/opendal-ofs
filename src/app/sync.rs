// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::path::Path;

use anyhow::{Context, Result, bail};
use ofs::client::catalog::Catalog;
use ofs::sync::{ReplicaState, SyncEngine, SyncVolume};

use crate::cli::{StatusArgs, SyncArgs};

use super::providers::open_managed_volume;

pub(super) async fn sync_volume(config: &Path, args: SyncArgs) -> Result<()> {
    let transfer_concurrency = args.runtime.transfer_concurrency;
    let volume = open_managed_volume(
        config,
        &args.alias,
        args.branch.as_ref(),
        transfer_concurrency,
    )
    .await?;
    let branch_label = volume
        .authority()
        .branch
        .as_ref()
        .map(|branch| format!(" branch {:?}", branch.name.as_str()))
        .unwrap_or_default();
    let engine = SyncEngine::new(volume, transfer_concurrency);
    let result = engine
        .sync(&args.replica, &args.state, &args.resolve)
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
        "synced {:?}{} at change {}{}",
        args.alias,
        branch_label,
        result.common.sequence(),
        if result.published { " (published)" } else { "" }
    );
    Ok(())
}

pub(super) fn status(config: &Path, args: StatusArgs) -> Result<()> {
    let state = ReplicaState::load(&args.state)?
        .with_context(|| format!("replica state does not exist: {}", args.state.display()))?;
    let catalog = Catalog::load(config).context("cannot open the volume catalog")?;
    let (alias, definition) = catalog
        .find_by_id(state.volume)
        .context("replica volume is not in the local catalog")?;
    let value = serde_json::json!({
        "volume_alias": alias,
        "volume_id": state.volume.to_string(),
        "branch_name": state.branch.as_ref().map(|branch| branch.name.as_str()),
        "branch_id": state.branch.as_ref().map(|branch| branch.id.to_string()),
        "volume_model": definition.model.as_str(),
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
            "managed sync alias {alias:?} for volume {}{branch} at change {}, {} pending, {} conflict(s)",
            state.volume,
            state.common().sequence(),
            usize::from(state.has_pending()),
            state.conflicts.len()
        );
    }
    Ok(())
}
