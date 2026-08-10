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
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use ofs::client::catalog::Catalog;
use ofs::filesystem::BranchName;
use ofs::managed::{D1Config, ManagedExtension, ManagedFormat, ManagedMetadata, ManagedVolume};
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer};
use url::Url;

pub(super) struct ManagedContext {
    pub(super) format: ManagedFormat,
    pub(super) data: Operator,
    pub(super) metadata: ManagedMetadata,
}

pub(super) async fn open_managed_volume(
    config: &Path,
    alias: &str,
    branch: Option<&BranchName>,
    transfer_concurrency: NonZeroUsize,
) -> Result<ManagedVolume> {
    let ManagedContext {
        format,
        data,
        metadata,
    } = open_managed_context(config, alias, transfer_concurrency).await?;
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
    config: &Path,
    alias: &str,
    transfer_concurrency: NonZeroUsize,
) -> Result<ManagedContext> {
    let catalog = Catalog::load(config).context("cannot open the volume catalog")?;
    let definition = catalog
        .get(alias)
        .with_context(|| format!("volume alias {alias:?} is not in the catalog"))?;
    let data = open_operator(&definition.storage, transfer_concurrency)?;
    let metadata = open_metadata(data.clone(), definition.metadata.as_ref())?;
    let format = metadata.read_format().await?;
    if format.volume_id() != definition.volume_id {
        bail!("volume catalog and Managed format v1 binding disagree");
    }
    Ok(ManagedContext {
        format,
        data,
        metadata,
    })
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
