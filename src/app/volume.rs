// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::path::Path;

use anyhow::{Context, Result, bail};
use ofs::client::catalog::{Catalog, VolumeDefinition};
use ofs::filesystem::{BranchName, VolumeId};
use ofs::managed::{ManagedExtension, ManagedFormat};

use crate::cli::{VolumeCreateArgs, VolumeGcArgs};

use super::providers::{ManagedContext, open_managed_context, open_metadata, open_operator};

pub(super) async fn gc_volume(config: &Path, args: VolumeGcArgs) -> Result<()> {
    let ManagedContext {
        format,
        data,
        metadata,
    } = open_managed_context(config, &args.alias, args.runtime.transfer_concurrency).await?;
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
        "garbage collected {:?}: scanned={} deleted={} bytes={}",
        args.alias, result.scanned, result.deleted, result.deleted_bytes,
    );
    Ok(())
}

pub(super) async fn create_volume(config: &Path, args: VolumeCreateArgs) -> Result<()> {
    let branch_enabled = args.enable.is_some();
    let mut catalog = Catalog::load(config).context("cannot open the writable volume catalog")?;
    let configured = catalog.get(&args.alias).cloned();

    let provisional_id = configured
        .as_ref()
        .map(|definition| definition.volume_id)
        .unwrap_or_else(VolumeId::generate);
    let provisional =
        VolumeDefinition::new(
            provisional_id,
            args.model,
            args.storage.clone(),
            args.metadata.clone(),
        )
    .context("volume URLs must be credential-free; supply credentials through provider environment variables")?;
    if configured
        .as_ref()
        .is_some_and(|current| current != &provisional)
    {
        bail!(
            "volume alias {:?} conflicts with its existing configuration",
            args.alias
        );
    }
    let data = open_operator(&args.storage, args.runtime.transfer_concurrency)?;
    let metadata = open_metadata(data.clone(), args.metadata.as_ref())?;
    let desired = ManagedFormat::v1(provisional_id);
    let desired = if branch_enabled {
        desired.with_extension(ManagedExtension::BranchV1)
    } else {
        desired
    };
    let format = if configured.is_some() {
        metadata.read_format().await?
    } else {
        metadata.create_format(&desired).await?
    };
    if configured
        .as_ref()
        .is_some_and(|definition| definition.volume_id != format.volume_id())
    {
        bail!("volume catalog and Managed format v1 binding disagree");
    }
    if branch_enabled && !format.requires_extension(ManagedExtension::BranchV1) {
        bail!("existing Managed volume does not enable requested extension branch/v1");
    }
    if format.requires_extension(ManagedExtension::BranchV1) {
        metadata
            .branches(&format, data.clone())?
            .initialize(BranchName::parse("main").expect("main is a valid branch name"))
            .await?;
    } else {
        metadata.open_volume(format.clone(), data)?;
    }
    let volume_id = format.volume_id();
    let definition = VolumeDefinition::new(volume_id, args.model, args.storage, args.metadata)?;
    let registered = catalog.register(&args.alias, definition)?;

    catalog
        .save()
        .context("the remote format is ready but the local catalog could not be saved; fix the catalog path and repeat the same command")?;

    let action = if registered { "registered" } else { "verified" };
    println!(
        "{action} managed volume alias {:?} for volume {volume_id} with format v1",
        args.alias,
    );
    Ok(())
}
