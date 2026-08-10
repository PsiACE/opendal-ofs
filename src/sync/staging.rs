// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use opendal::Operator;
use serde::{Deserialize, Serialize};

use super::local::{
    LocalEntry, LocalTree, NativeIdentity, entry_at, fs_operator, native_identity_at,
};
use super::path::descendants;
use super::state::PendingIntent;
use crate::filesystem::{
    FileVersion, FileVersionId, NodeKind, OperationId, Volume, VolumeSnapshot,
};

const TREE_DIR: &str = "tree";
const SEGMENTS_DIR: &str = "segments";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetFile {
    pub id: FileVersionId,
    pub logical_size: u64,
    pub logical_digest: [u8; 32],
}

impl From<&FileVersion> for TargetFile {
    fn from(version: &FileVersion) -> Self {
        Self {
            id: version.id,
            logical_size: version.logical_size,
            logical_digest: version.logical_digest,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetEntry {
    pub local: LocalEntry,
    pub file: Option<TargetFile>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetManifest {
    pub(super) entries: BTreeMap<String, TargetEntry>,
}

impl TargetManifest {
    pub(crate) fn file(&self, path: &str) -> Option<&TargetFile> {
        self.entries.get(path)?.file.as_ref()
    }

    pub(crate) fn select_file(&mut self, path: String, file: TargetFile, executable: bool) {
        self.remove(&path);
        self.entries.insert(
            path,
            TargetEntry {
                local: LocalEntry {
                    kind: NodeKind::RegularFile,
                    size: file.logical_size,
                    modified: String::new(),
                    executable,
                    native_identity: None,
                },
                file: Some(file),
            },
        );
    }

    pub(crate) fn select_directory(&mut self, path: String) {
        self.entries.remove(&path);
        self.entries.insert(
            path,
            TargetEntry {
                local: LocalEntry {
                    kind: NodeKind::Directory,
                    size: 0,
                    modified: String::new(),
                    executable: false,
                    native_identity: None,
                },
                file: None,
            },
        );
    }

    pub(crate) fn select_attributes(
        &mut self,
        path: &str,
        version: TargetFile,
        executable: bool,
    ) -> Result<()> {
        let entry = self
            .entries
            .get_mut(path)
            .with_context(|| format!("remote attributes reference a missing file {path:?}"))?;
        let file = entry
            .file
            .as_mut()
            .with_context(|| format!("remote attributes reference a directory {path:?}"))?;
        if file.logical_size != version.logical_size
            || file.logical_digest != version.logical_digest
        {
            bail!("remote attributes disagree with staged file {path:?}");
        }
        entry.local.executable = executable;
        *file = version;
        Ok(())
    }

    pub(crate) fn remove(&mut self, path: &str) {
        self.entries.remove(path);
        let descendants = descendants(&self.entries, path)
            .map(|(candidate, _)| candidate.clone())
            .collect::<Vec<_>>();
        for descendant in descendants {
            self.entries.remove(&descendant);
        }
    }

    pub(crate) fn same_content(&self, other: &Self) -> bool {
        self.entries.len() == other.entries.len()
            && self.entries.iter().all(|(path, entry)| {
                other.entries.get(path).is_some_and(|desired| {
                    entry.local.kind == desired.local.kind
                        && entry.local.size == desired.local.size
                        && entry.local.executable == desired.local.executable
                        && entry.file.as_ref().map(|file| file.logical_digest)
                            == desired.file.as_ref().map(|file| file.logical_digest)
                })
            })
    }
}

/// Immutable input for a later volume file-version builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedTree {
    pub(super) staging: PathBuf,
    pub(super) root: PathBuf,
    pub(super) source: TargetManifest,
    manifest: Option<TargetManifest>,
    prepared: BTreeMap<FileVersionId, FileVersion>,
}

impl StagedTree {
    pub(crate) async fn prepare_for_publish<V: Volume>(
        tree: &LocalTree,
        root: impl AsRef<Path>,
        known_versions: &BTreeMap<String, FileVersion>,
        volume: &V,
        authority: Option<&VolumeSnapshot>,
        concurrency: NonZeroUsize,
    ) -> Result<Self> {
        let staging = root.as_ref();
        prepare_root(&tree.root, staging).await?;
        let root = staging.join(TREE_DIR);
        let segment_root = staging.join(SEGMENTS_DIR);
        let source = fs_operator(&tree.root)?;
        let segments = fs_operator(&segment_root)?;

        let changed = tree
            .entries
            .iter()
            .filter(|(path, entry)| {
                entry.kind == NodeKind::RegularFile && !known_versions.contains_key(*path)
            })
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        let prepared = volume
            .stage_files(&source, &segments, changed.clone(), authority, concurrency)
            .await
            .map_err(anyhow::Error::new)?;
        if prepared.len() != changed.len()
            || changed.iter().any(|path| !prepared.contains_key(path))
        {
            bail!("volume did not prepare every changed file exactly once");
        }
        let mut entries = BTreeMap::new();
        let mut versions = BTreeMap::new();
        for (path, expected) in &tree.entries {
            let file = match expected.kind {
                NodeKind::Directory => {
                    require_same_identity(
                        &tree.root,
                        path,
                        expected.kind,
                        expected.native_identity,
                    )?;
                    None
                }
                NodeKind::RegularFile => {
                    let version = match prepared.get(path) {
                        Some(version) => {
                            if version.logical_size != expected.size {
                                bail!("volume returned the wrong size for staged file {path:?}");
                            }
                            if entry_at(&tree.root, path).await? != *expected {
                                bail!(
                                    "source file {path:?} changed while preparing publication; retry sync"
                                );
                            }
                            match versions.insert(version.id, version.clone()) {
                                Some(existing) if existing != *version => bail!(
                                    "volume reused a file version identity for different content"
                                ),
                                _ => {}
                            }
                            version
                        }
                        None => {
                            require_same_identity(
                                &tree.root,
                                path,
                                expected.kind,
                                expected.native_identity,
                            )?;
                            known_versions.get(path).with_context(|| {
                                format!("unchanged file {path:?} has no known version")
                            })?
                        }
                    };
                    Some(TargetFile::from(version))
                }
            };
            entries.insert(
                path.clone(),
                TargetEntry {
                    local: expected.clone(),
                    file,
                },
            );
        }
        let source = TargetManifest { entries };
        let staged = Self {
            staging: staging.to_owned(),
            root,
            manifest: None,
            source,
            prepared: versions,
        };
        Ok(staged)
    }

    pub(crate) fn recover(intent: &PendingIntent) -> Result<Self> {
        let staging = &intent.staging;
        let root = staging.join(TREE_DIR);
        if !staging.is_dir() || !staging.join(SEGMENTS_DIR).is_dir() {
            bail!("segment staging is missing");
        }
        match fs::symlink_metadata(&root) {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&root)?,
            Ok(_) => fs::remove_file(&root)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("inspect staged tree"),
        }
        fs::create_dir(&root).context("rebuild staged tree")?;
        let prepared = intent
            .prepared
            .iter()
            .cloned()
            .map(|version| (version.id, version))
            .collect::<BTreeMap<_, _>>();
        if prepared.len() != intent.prepared.len() {
            bail!("pending staging repeats a prepared file version");
        }
        let staged = Self {
            staging: staging.to_owned(),
            root,
            source: intent.source.clone(),
            manifest: None,
            prepared,
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
            staging: self.staging.clone(),
            renames,
            source: self.source.clone(),
            prepared: self.prepared.values().cloned().collect(),
        }
    }

    pub(crate) fn segment_operator(&self) -> Result<Operator> {
        fs_operator(&self.staging.join(SEGMENTS_DIR))
    }

    pub(crate) fn manifest(&self) -> &TargetManifest {
        self.manifest.as_ref().unwrap_or(&self.source)
    }

    pub(crate) fn local_tree(&self) -> LocalTree {
        LocalTree {
            root: self.root.clone(),
            entries: self
                .source
                .entries
                .iter()
                .map(|(path, entry)| (path.clone(), entry.local.clone()))
                .collect(),
        }
    }

    pub(crate) fn resolve_version<'a>(
        &'a self,
        file: &TargetFile,
        authority: Option<&'a VolumeSnapshot>,
    ) -> Result<&'a FileVersion> {
        let version = self
            .prepared
            .get(&file.id)
            .or_else(|| authority.and_then(|snapshot| snapshot.file_versions.get(&file.id)))
            .with_context(|| format!("file version {:?} is unavailable", file.id.as_bytes()))?;
        if TargetFile::from(version) != *file {
            bail!("file version identity disagrees with its logical metadata");
        }
        Ok(version)
    }

    pub(crate) fn replace_manifest(&mut self, manifest: TargetManifest) -> Result<()> {
        validate_manifest(&manifest)?;
        self.manifest = (manifest != self.source).then_some(manifest);
        Ok(())
    }

    pub(crate) fn matches_source_observation(&self, observed: &LocalTree) -> bool {
        self.source.entries.len() == observed.entries.len()
            && self.source.entries.iter().all(|(path, entry)| {
                observed
                    .entries
                    .get(path)
                    .is_some_and(|observed| observed == &entry.local)
            })
    }

    fn validate(&self) -> Result<()> {
        validate_manifest(&self.source)?;
        if let Some(manifest) = &self.manifest {
            validate_manifest(manifest)?;
        }
        Ok(())
    }
}

fn validate_manifest(manifest: &TargetManifest) -> Result<()> {
    for (path, entry) in &manifest.entries {
        if (entry.local.kind == NodeKind::RegularFile) != entry.file.is_some() {
            bail!("staged path {path:?} has inconsistent kind and file state");
        }
        let Some(file) = &entry.file else {
            continue;
        };
        if entry.local.size != file.logical_size {
            bail!("staged file {path:?} disagrees with its volume version");
        }
    }
    Ok(())
}

fn require_same_identity(
    root: &Path,
    path: &str,
    kind: NodeKind,
    expected: Option<NativeIdentity>,
) -> Result<()> {
    if native_identity_at(root, path, kind)? != expected {
        bail!("source path {path:?} was replaced while preparing publication; retry sync");
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
        .context("create a fresh staging directory")?;
    tokio::fs::create_dir(staging.join(TREE_DIR))
        .await
        .context("create the staging tree")?;
    tokio::fs::create_dir(staging.join(SEGMENTS_DIR))
        .await
        .context("create the segment staging directory")
}
