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
use sha2::{Digest as _, Sha256};

use super::local::{
    LocalKind, LocalTree, NativeFileIdentity, executable_at, fs_operator, native_file_identity_at,
    set_executable,
};

const COPY_CHUNK: u64 = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedFile {
    pub size: u64,
    pub source_modified: String,
    pub digest: [u8; 32],
    pub source_identity: Option<NativeFileIdentity>,
}

/// Immutable input for a later Managed FileVersion builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedTree {
    root: PathBuf,
    files: BTreeMap<String, StagedFile>,
}

impl StagedTree {
    pub async fn prepare(tree: &LocalTree, root: impl AsRef<Path>) -> Result<Self> {
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

        let mut files = BTreeMap::new();
        for (path, expected) in tree.entries() {
            if expected.kind != LocalKind::File {
                continue;
            }
            let before = source
                .stat(path)
                .await
                .with_context(|| format!("inspect source file {path:?} before staging"))?;
            require_same(path, expected.size, &expected.modified, &before)?;
            require_same_executable(tree.root(), path, expected.executable)?;
            require_same_identity(tree.root(), path, expected.native_identity)?;

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

            let after = source
                .stat(path)
                .await
                .with_context(|| format!("inspect source file {path:?} after staging"))?;
            require_same(path, expected.size, &expected.modified, &after)?;
            require_same_executable(tree.root(), path, expected.executable)?;
            require_same_identity(tree.root(), path, expected.native_identity)?;
            set_executable(&root.join(path), expected.executable)
                .with_context(|| format!("preserve executable bit for {path:?}"))?;
            let staged_metadata = staged
                .stat(path)
                .await
                .with_context(|| format!("verify staging file {path:?}"))?;
            if staged_metadata.content_length() != expected.size {
                bail!("staging file {path:?} is incomplete; retry sync");
            }
            files.insert(
                path.clone(),
                StagedFile {
                    size: expected.size,
                    source_modified: expected.modified.clone(),
                    digest: digest.finalize().into(),
                    source_identity: expected.native_identity,
                },
            );
        }
        Ok(Self {
            root: root.to_owned(),
            files,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn files(&self) -> &BTreeMap<String, StagedFile> {
        &self.files
    }
}

fn require_same_identity(
    root: &Path,
    path: &str,
    expected: Option<NativeFileIdentity>,
) -> Result<()> {
    if native_file_identity_at(root, path, LocalKind::File)? != expected {
        bail!("source file {path:?} was replaced while preparing publication; retry sync");
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
