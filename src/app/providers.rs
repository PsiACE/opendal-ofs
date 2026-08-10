// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Provider construction and Managed volume admission.

use std::env;
use std::num::NonZeroUsize;

use anyhow::{Context, Result, anyhow, bail};
use ofs::filesystem::{BranchName, VolumeId};
use ofs::managed::{D1Config, ManagedExtension, ManagedFormat, ManagedMetadata, ManagedVolume};
use ofs::sync::ReplicaTarget;
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer};
use url::Url;

pub(super) struct ManagedContext {
    pub(super) format: ManagedFormat,
    pub(super) data: Operator,
    pub(super) metadata: ManagedMetadata,
}

pub(super) async fn open_managed_volume(
    target: &ReplicaTarget,
    expected_volume: Option<VolumeId>,
    branch: Option<&BranchName>,
    transfer_concurrency: NonZeroUsize,
) -> Result<ManagedVolume> {
    let ManagedContext {
        format,
        data,
        metadata,
    } = open_managed_context(target, expected_volume, transfer_concurrency).await?;
    volume_from_context(format, data, metadata, branch).await
}

pub(super) async fn initialize_managed_volume(
    target: &ReplicaTarget,
    expected_volume: Option<VolumeId>,
    branch: Option<&BranchName>,
    branch_enabled: bool,
    transfer_concurrency: NonZeroUsize,
) -> Result<ManagedVolume> {
    let data = open_operator(target.storage(), transfer_concurrency)?;
    let metadata = open_metadata(data.clone(), target.metadata())?;
    let provisional_id = expected_volume.unwrap_or_else(VolumeId::generate);
    let desired = ManagedFormat::v1(provisional_id);
    let desired = if branch_enabled {
        desired.with_extension(ManagedExtension::BranchV1)
    } else {
        desired
    };
    let format = metadata.create_format(&desired).await?;
    validate_volume(expected_volume, &format)?;
    if branch_enabled && !format.requires_extension(ManagedExtension::BranchV1) {
        bail!("existing Managed volume does not enable requested extension branch/v1");
    }
    if format.requires_extension(ManagedExtension::BranchV1) {
        metadata
            .branches(&format, data.clone())?
            .initialize(BranchName::parse("main").expect("main is a valid branch name"))
            .await?;
    }
    volume_from_context(format, data, metadata, branch).await
}

async fn volume_from_context(
    format: ManagedFormat,
    data: Operator,
    metadata: ManagedMetadata,
    branch: Option<&BranchName>,
) -> Result<ManagedVolume> {
    if format.requires_extension(ManagedExtension::BranchV1) {
        let branches = metadata.branches(&format, data)?;
        let volume = match branch {
            Some(name) => branches.open(name).await?,
            None => branches.open_default().await?,
        };
        return Ok(volume);
    }
    if branch.is_some() {
        bail!("Managed volume does not enable branch/v1");
    }
    metadata.open_volume(format, data).map_err(Into::into)
}

pub(super) async fn open_managed_context(
    target: &ReplicaTarget,
    expected_volume: Option<VolumeId>,
    transfer_concurrency: NonZeroUsize,
) -> Result<ManagedContext> {
    let data = open_operator(target.storage(), transfer_concurrency)?;
    let metadata = open_metadata(data.clone(), target.metadata())?;
    let format = metadata.read_format().await?;
    validate_volume(expected_volume, &format)?;
    Ok(ManagedContext {
        format,
        data,
        metadata,
    })
}

fn validate_volume(expected: Option<VolumeId>, format: &ManagedFormat) -> Result<()> {
    if expected.is_some_and(|volume| volume != format.volume_id()) {
        bail!("replica state and Managed format belong to different volumes");
    }
    Ok(())
}

pub(super) fn open_metadata(data: Operator, metadata: Option<&Url>) -> Result<ManagedMetadata> {
    match metadata {
        None => ManagedMetadata::object(data).map_err(Into::into),
        Some(url) if url.scheme() == "d1" => {
            ManagedMetadata::d1(d1_config(url)?).map_err(Into::into)
        }
        Some(_) => bail!("--metadata must use d1://ACCOUNT/DATABASE/STORE"),
    }
}

pub(super) fn open_operator(url: &Url, transfer_concurrency: NonZeroUsize) -> Result<Operator> {
    let concurrency = transfer_concurrency.get();
    Operator::from_uri(url.as_str())
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

fn d1_config(url: &Url) -> Result<D1Config> {
    let account = url
        .host_str()
        .context("--metadata needs a D1 account in its host")?;
    let path = url
        .path_segments()
        .context("--metadata needs /DATABASE/STORE")?
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if path.len() != 2 {
        bail!("--metadata path must be /DATABASE/STORE");
    }
    let api_base = url
        .query_pairs()
        .find_map(|(key, value)| (key == "api_base").then(|| value.into_owned()));
    if url.query_pairs().any(|(key, _)| key == "token") {
        bail!("remove token from --metadata and set OFS_D1_TOKEN instead");
    }
    let token = env::var("OFS_D1_TOKEN")
        .map_err(|_| anyhow!("set OFS_D1_TOKEN to the D1 API credential and repeat the command"))?;
    let mut config = D1Config::new(account, path[0], path[1], token).map_err(|_| {
        anyhow!("invalid D1 metadata configuration; check account, database, store, and token")
    })?;
    if let Some(api_base) = api_base {
        config = config
            .with_api_base(api_base)
            .map_err(|_| anyhow!("invalid D1 api_base; provide an absolute Query API base URL"))?;
    }
    Ok(config)
}
