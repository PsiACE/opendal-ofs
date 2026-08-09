// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::local::{
    LocalEntry, LocalKind, LocalTree, NativeIdentity, entry_at, fs_operator, native_identity_at,
    set_executable,
};
use super::path::descendants;
use super::state::PendingIntent;
use crate::filesystem::{FileVersion, OperationId, Volume, VolumeSnapshot};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StagedFile {
    pub size: u64,
    pub digest: [u8; 32],
    content: bool,
    prepared: Option<FileVersion>,
}

impl StagedFile {
    pub(crate) fn prepared(&self) -> Option<&FileVersion> {
        self.prepared.as_ref()
    }
}

/// Immutable input for a later volume file-version builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedTree {
    root: PathBuf,
    logical: LocalTree,
    files: BTreeMap<String, StagedFile>,
}

impl StagedTree {
    pub(crate) async fn prepare_for_publish<V: Volume>(
        tree: &LocalTree,
        root: impl AsRef<Path>,
        known_digests: &BTreeMap<String, [u8; 32]>,
        volume: &V,
        authority: Option<&VolumeSnapshot>,
        concurrency: NonZeroUsize,
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

        let changed = tree
            .entries()
            .iter()
            .filter(|(path, entry)| {
                entry.kind == LocalKind::File && !known_digests.contains_key(*path)
            })
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        let prepared = volume
            .stage_files(&source, &staged, changed.clone(), authority, concurrency)
            .await
            .map_err(anyhow::Error::new)?;
        if prepared.len() != changed.len()
            || changed.iter().any(|path| !prepared.contains_key(path))
        {
            bail!("volume did not prepare every changed file exactly once");
        }

        let mut files = BTreeMap::new();
        for (path, expected) in tree
            .entries()
            .iter()
            .filter(|(_, entry)| entry.kind == LocalKind::File)
        {
            let file = match prepared.get(path) {
                Some(version) => {
                    if version.logical_size != expected.size {
                        bail!("volume returned the wrong size for staged file {path:?}");
                    }
                    let after = source
                        .stat(path)
                        .await
                        .with_context(|| format!("inspect source file {path:?} after staging"))?;
                    require_same(path, expected.size, &expected.modified, &after)?;
                    let attributes =
                        fs::symlink_metadata(tree.root().join(path)).with_context(|| {
                            format!("inspect source file attributes for {path:?} after staging")
                        })?;
                    require_same_file_attributes(path, expected, &attributes)?;
                    set_executable(&root.join(path), expected.executable)
                        .with_context(|| format!("preserve executable bit for {path:?}"))?;
                    let staged_metadata = staged
                        .stat(path)
                        .await
                        .with_context(|| format!("verify staging file {path:?}"))?;
                    if !staged_metadata.is_file()
                        || staged_metadata.content_length() != expected.size
                    {
                        bail!("staging file {path:?} is incomplete; retry sync");
                    }
                    StagedFile {
                        size: version.logical_size,
                        digest: version.logical_digest,
                        content: true,
                        prepared: Some(version.clone()),
                    }
                }
                None => StagedFile {
                    size: expected.size,
                    digest: *known_digests
                        .get(path)
                        .with_context(|| format!("unchanged file {path:?} has no known digest"))?,
                    content: false,
                    prepared: None,
                },
            };
            files.insert(path.clone(), file);
        }
        for (path, entry) in tree.entries() {
            require_same_identity(tree.root(), path, entry.kind, entry.native_identity)?;
        }
        let staged = Self {
            root: root.to_owned(),
            logical: LocalTree::from_entries(root, tree.entries().clone()),
            files,
        };
        Ok(staged)
    }

    pub(crate) fn recover(intent: &PendingIntent) -> Result<Self> {
        let root = &intent.staging;
        if !root.is_dir() {
            bail!("staging cache is missing");
        }
        let staged = Self {
            root: root.to_owned(),
            logical: LocalTree::from_entries(root, intent.entries.clone()),
            files: intent.files.clone(),
        };
        staged.validate()?;
        Ok(staged)
    }

    pub(crate) fn pending(
        &self,
        operation: OperationId,
        renames: BTreeMap<String, String>,
    ) -> PendingIntent {
        PendingIntent {
            operation,
            staging: self.root.clone(),
            renames,
            entries: self.logical.entries().clone(),
            files: self.files.clone(),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn files(&self) -> &BTreeMap<String, StagedFile> {
        &self.files
    }

    pub(crate) fn logical(&self) -> &LocalTree {
        &self.logical
    }

    pub(crate) fn content_path(&self, path: &str) -> Option<PathBuf> {
        self.files
            .get(path)
            .is_some_and(|file| file.content)
            .then(|| self.root.join(path))
    }

    pub(crate) async fn record_materialized_file(
        &mut self,
        path: impl Into<String>,
        digest: [u8; 32],
    ) -> Result<()> {
        let path = path.into();
        let entry = entry_at(&self.root, &path).await?;
        if entry.kind != LocalKind::File {
            bail!("materialized path {path:?} is not a file");
        }
        self.remove_descendants(&path);
        self.files.insert(
            path.clone(),
            StagedFile {
                size: entry.size,
                digest,
                content: true,
                prepared: None,
            },
        );
        self.logical.insert(path, entry);
        Ok(())
    }

    pub(crate) async fn record_materialized_directory(
        &mut self,
        path: impl Into<String>,
    ) -> Result<()> {
        let path = path.into();
        let entry = entry_at(&self.root, &path).await?;
        if entry.kind != LocalKind::Directory {
            bail!("materialized path {path:?} is not a directory");
        }
        self.files.remove(&path);
        self.logical.insert(path, entry);
        Ok(())
    }

    pub(crate) fn apply_remote_attributes(
        &mut self,
        path: &str,
        digest: [u8; 32],
        executable: bool,
    ) -> Result<()> {
        let file = self
            .files
            .get(path)
            .with_context(|| format!("remote attributes reference a missing file {path:?}"))?;
        if file.digest != digest {
            bail!("remote attributes disagree with staged file {path:?}");
        }
        let kind = self
            .logical
            .set_executable(path, executable)
            .with_context(|| format!("remote attributes reference a missing path {path:?}"))?;
        if kind != LocalKind::File {
            bail!("remote attributes reference a directory {path:?}");
        }
        Ok(())
    }

    pub(crate) fn remove_logical_path(&mut self, path: &str) {
        self.logical.remove(path);
        self.files.remove(path);
        self.remove_descendants(path);
    }

    pub(crate) fn matches_source_observation(&self, observed: &LocalTree) -> bool {
        self.logical.entries() == observed.entries()
    }

    fn remove_descendants(&mut self, path: &str) {
        let descendants = descendants(self.logical.entries(), path)
            .map(|(path, _)| path)
            .cloned()
            .collect::<Vec<_>>();
        for descendant in descendants {
            self.logical.remove(&descendant);
            self.files.remove(&descendant);
        }
    }

    fn validate(&self) -> Result<()> {
        let logical_files = self
            .logical
            .entries()
            .iter()
            .filter(|(_, entry)| entry.kind == LocalKind::File)
            .map(|(path, _)| path)
            .collect::<BTreeSet<_>>();
        if logical_files != self.files.keys().collect() {
            bail!("staged state does not describe one complete logical tree");
        }
        for (path, file) in &self.files {
            if self.logical.entries()[path].size != file.size {
                bail!("staged file {path:?} has the wrong size");
            }
            if file.content {
                let metadata = fs::symlink_metadata(self.root.join(path))
                    .with_context(|| format!("inspect staged content {path:?}"))?;
                if !metadata.file_type().is_file() || metadata.len() != file.size {
                    bail!("staged content {path:?} is incomplete");
                }
            }
            if file.prepared.as_ref().is_some_and(|version| {
                !file.content
                    || file.size != version.logical_size
                    || file.digest != version.logical_digest
            }) {
                bail!("prepared file {path:?} disagrees with staged content");
            }
        }
        Ok(())
    }
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

fn require_same_file_attributes(
    path: &str,
    expected: &LocalEntry,
    metadata: &fs::Metadata,
) -> Result<()> {
    if !metadata.file_type().is_file() {
        bail!("source file {path:?} changed kind while preparing publication; retry sync");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let identity = Some(NativeIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        });
        if identity != expected.native_identity {
            bail!("source path {path:?} was replaced while preparing publication; retry sync");
        }
        if (metadata.permissions().mode() & 0o111 != 0) != expected.executable {
            bail!(
                "source file {path:?} changed permissions while preparing publication; retry sync"
            );
        }
    }
    #[cfg(not(unix))]
    if expected.native_identity.is_some() || expected.executable {
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
