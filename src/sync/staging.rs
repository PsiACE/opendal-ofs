// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use opendal::Operator;
use sha2::{Digest as _, Sha256};

use super::local::{
    LocalEntry, LocalKind, LocalTree, NativeIdentity, executable_at, fs_operator,
    native_identity_at, set_executable,
};

const COPY_CHUNK: u64 = 1024 * 1024;
const COPY_CONCURRENCY: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedFile {
    pub size: u64,
    pub source_modified: String,
    pub digest: [u8; 32],
}

/// Immutable input for a later Managed FileVersion builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedTree {
    root: PathBuf,
    files: BTreeMap<String, StagedFile>,
    source_identities: BTreeMap<String, Option<NativeIdentity>>,
}

impl StagedTree {
    pub async fn prepare(tree: &LocalTree, root: impl AsRef<Path>) -> Result<Self> {
        Self::prepare_known(tree, root, &BTreeMap::new()).await
    }

    pub async fn prepare_known(
        tree: &LocalTree,
        root: impl AsRef<Path>,
        known_digests: &BTreeMap<String, [u8; 32]>,
    ) -> Result<Self> {
        let root = root.as_ref();
        prepare_root(tree.root(), root).await?;
        let source = tree.operator()?;
        let staged = fs_operator(root)?;

        for (path, entry) in tree.entries() {
            if entry.kind == LocalKind::Directory {
                staged
                    .create_dir(&format!("{path}/"))
                    .await
                    .with_context(|| format!("create staging directory {path:?}"))?;
            }
        }
        let source_identities = tree
            .entries()
            .iter()
            .map(|(path, entry)| (path.clone(), entry.native_identity))
            .collect();

        let files = stream::iter(
            tree.entries()
                .iter()
                .filter(|(_, expected)| expected.kind == LocalKind::File)
                .map(|(path, expected)| {
                    let source = source.clone();
                    let staged = staged.clone();
                    let source_root = tree.root().to_owned();
                    let staging_root = root.to_owned();
                    let path = path.clone();
                    let expected = expected.clone();
                    let known_digest = known_digests.get(&path).copied();
                    async move {
                        let file = stage_file(
                            &source,
                            &staged,
                            &source_root,
                            &staging_root,
                            &path,
                            &expected,
                            known_digest,
                        )
                        .await?;
                        Ok::<_, anyhow::Error>((path, file))
                    }
                }),
        )
        .buffer_unordered(COPY_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .collect();
        for (path, entry) in tree.entries() {
            require_same_identity(tree.root(), path, entry.kind, entry.native_identity)?;
        }
        Ok(Self {
            root: root.to_owned(),
            files,
            source_identities,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn files(&self) -> &BTreeMap<String, StagedFile> {
        &self.files
    }

    pub(crate) fn source_identities(&self) -> &BTreeMap<String, Option<NativeIdentity>> {
        &self.source_identities
    }
}

async fn stage_file(
    source: &Operator,
    staged: &Operator,
    source_root: &Path,
    staging_root: &Path,
    path: &str,
    expected: &LocalEntry,
    known_digest: Option<[u8; 32]>,
) -> Result<StagedFile> {
    let before = source
        .stat(path)
        .await
        .with_context(|| format!("inspect source file {path:?} before staging"))?;
    require_same(path, expected.size, &expected.modified, &before)?;
    require_same_executable(source_root, path, expected.executable)?;
    require_same_identity(source_root, path, LocalKind::File, expected.native_identity)?;

    let digest = if let Some(digest) = known_digest {
        let copied = tokio::fs::copy(source_root.join(path), staging_root.join(path))
            .await
            .with_context(|| format!("copy unchanged source file {path:?}"))?;
        if copied != expected.size {
            bail!("source file {path:?} returned a short copy; retry sync");
        }
        digest
    } else {
        let reader = source
            .reader(path)
            .await
            .with_context(|| format!("open source file {path:?}"))?;
        let mut writer = staged
            .writer(path)
            .await
            .with_context(|| format!("open staging file {path:?}"))?;
        let mut digest = Sha256::new();
        let mut offset = 0;
        while offset < expected.size {
            let end = expected.size.min(offset + COPY_CHUNK);
            let buffer = reader
                .read(offset..end)
                .await
                .with_context(|| format!("read stable source file {path:?}"))?;
            let bytes = buffer.to_bytes();
            if bytes.len() as u64 != end - offset {
                bail!("source file {path:?} returned a short read; retry sync");
            }
            digest.update(&bytes);
            writer
                .write(bytes)
                .await
                .with_context(|| format!("write staging file {path:?}"))?;
            offset = end;
        }
        writer
            .close()
            .await
            .with_context(|| format!("finish staging file {path:?}"))?;
        digest.finalize().into()
    };

    let after = source
        .stat(path)
        .await
        .with_context(|| format!("inspect source file {path:?} after staging"))?;
    require_same(path, expected.size, &expected.modified, &after)?;
    require_same_executable(source_root, path, expected.executable)?;
    require_same_identity(source_root, path, LocalKind::File, expected.native_identity)?;
    set_executable(&staging_root.join(path), expected.executable)
        .with_context(|| format!("preserve executable bit for {path:?}"))?;
    let staged_metadata = staged
        .stat(path)
        .await
        .with_context(|| format!("verify staging file {path:?}"))?;
    if staged_metadata.content_length() != expected.size {
        bail!("staging file {path:?} is incomplete; retry sync");
    }
    Ok(StagedFile {
        size: expected.size,
        source_modified: expected.modified.clone(),
        digest,
    })
}

fn require_same_identity(
    root: &Path,
    path: &str,
    kind: LocalKind,
    expected: Option<NativeIdentity>,
) -> Result<()> {
    if native_identity_at(root, path, kind)? != expected {
        bail!("source path {path:?} was replaced while preparing publication; retry sync");
    }
    Ok(())
}

fn require_same_executable(root: &Path, path: &str, expected: bool) -> Result<()> {
    if executable_at(root, path, LocalKind::File)? != expected {
        bail!("source file {path:?} changed permissions while preparing publication; retry sync");
    }
    Ok(())
}

async fn prepare_root(source: &Path, staging: &Path) -> Result<()> {
    let source = tokio::fs::canonicalize(source)
        .await
        .context("resolve local replica root")?;
    let parent = staging
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    tokio::fs::create_dir_all(parent)
        .await
        .context("create staging parent")?;
    let parent = tokio::fs::canonicalize(parent)
        .await
        .context("resolve staging parent")?;
    if parent.starts_with(source) {
        bail!("staging directory must be outside the local replica");
    }
    tokio::fs::create_dir(staging)
        .await
        .context("create a fresh staging directory")
}

fn require_same(path: &str, size: u64, modified: &str, observed: &opendal::Metadata) -> Result<()> {
    let observed_modified = observed.last_modified().map(|value| value.to_string());
    if !observed.mode().is_file()
        || observed.content_length() != size
        || observed_modified.as_deref() != Some(modified)
    {
        bail!("source file {path:?} changed while preparing publication; retry sync");
    }
    Ok(())
}
