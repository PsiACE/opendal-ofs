// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use anyhow::{Context, Result};
use ofs::managed::ManagedExtension;
use ofs::sync::ReplicaState;

use crate::cli::VolumeGcArgs;

use super::providers::{ManagedContext, open_managed_context};

pub(super) async fn gc_volume(args: VolumeGcArgs) -> Result<()> {
    let state = ReplicaState::load(&args.state)?
        .with_context(|| format!("replica state does not exist: {}", args.state.display()))?;
    let ManagedContext {
        format,
        data,
        metadata,
    } = open_managed_context(
        state.target(),
        Some(state.volume),
        args.runtime.transfer_concurrency,
    )
    .await?;
    let result = if format.requires_extension(ManagedExtension::BranchV1) {
        metadata
            .branches(&format, data)?
            .garbage_collect(args.resume)
            .await?
    } else {
        metadata
            .open_volume(format, data)?
            .garbage_collect(args.resume)
            .await?
    };
    println!(
        "garbage collected volume {}: scanned={} deleted={} bytes={}",
        state.volume, result.scanned, result.deleted, result.deleted_bytes,
    );
    Ok(())
}
