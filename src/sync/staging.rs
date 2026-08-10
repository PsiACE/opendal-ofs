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
use serde::{Deserialize, Serialize};

use super::local::{
    LocalEntry, LocalKind, LocalTree, NativeIdentity, entry_at, fs_operator, native_identity_at,
    set_executable,
};
use super::state::PendingIntent;
use crate::filesystem::{FileVersion, FileVersionId, OperationId, Volume, VolumeSnapshot};

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
    entries: BTreeMap<String, TargetEntry>,
}

impl TargetManifest {
    pub(crate) fn entries(&self) -> &BTreeMap<String, TargetEntry> {
        &self.entries
    }

    pub(crate) fn file(&self, path: &str) -> Option<&TargetFile> {
        self.entries.get(path)?.file.as_ref()
    }

    pub(crate) fn select_file(&mut self, path: String, file: TargetFile, executable: bool) {
        self.remove(&path);
        self.entries.insert(
            path,
            TargetEntry {
                local: LocalEntry {
                    kind: LocalKind::File,
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
                    kind: LocalKind::Directory,
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
        let prefix = format!("{path}/");
        let descendants = self
            .entries
            .range(prefix.clone()..)
            .take_while(|(candidate, _)| candidate.starts_with(&prefix))
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
    root: PathBuf,
    source: TargetManifest,
    manifest: TargetManifest,
    prepared: BTreeMap<FileVersionId, FileVersion>,
    cache: BTreeMap<String, FileVersionId>,
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
                entry.kind == LocalKind::File && !known_versions.contains_key(*path)
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

        let mut entries = tree
            .entries()
            .iter()
            .map(|(path, entry)| {
                (
                    path.clone(),
                    TargetEntry {
                        local: entry.clone(),
                        file: None,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut versions = BTreeMap::new();
        let mut cache = BTreeMap::new();
        for (path, expected) in tree
            .entries()
            .iter()
            .filter(|(_, entry)| entry.kind == LocalKind::File)
        {
            let version = match prepared.get(path) {
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
                    match versions.insert(version.id, version.clone()) {
                        Some(existing) if existing != *version => {
                            bail!("volume reused a file version identity for different content")
                        }
                        _ => {}
                    }
                    cache.insert(path.clone(), version.id);
                    version
                }
                None => known_versions
                    .get(path)
                    .with_context(|| format!("unchanged file {path:?} has no known version"))?,
            };
            entries
                .get_mut(path)
                .expect("a staged file is present in the source observation")
                .file = Some(TargetFile::from(version));
        }
        for (path, entry) in tree.entries() {
            require_same_identity(tree.root(), path, entry.kind, entry.native_identity)?;
        }
        let source = TargetManifest { entries };
        let staged = Self {
            root: root.to_owned(),
            manifest: source.clone(),
            source,
            prepared: versions,
            cache,
        };
        Ok(staged)
    }

    pub(crate) fn recover(intent: &PendingIntent) -> Result<Self> {
        let root = &intent.staging;
        if !root.is_dir() {
            bail!("staging cache is missing");
        }
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
            root: root.to_owned(),
            source: intent.source.clone(),
            manifest: intent.manifest.clone(),
            prepared,
            cache: intent.cache.clone(),
        };
        staged.validate()?;
        Ok(staged)
    }

    pub(crate) fn pending(
        &self,
        operation: OperationId,
        data_finalized: bool,
        renames: BTreeMap<String, String>,
    ) -> PendingIntent {
        PendingIntent {
            operation,
            staging: self.root.clone(),
            data_finalized,
            renames,
            source: self.source.clone(),
            manifest: self.manifest.clone(),
            prepared: self.prepared.values().cloned().collect(),
            cache: self.cache.clone(),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn manifest(&self) -> &TargetManifest {
        &self.manifest
    }

    pub(crate) fn source(&self) -> &TargetManifest {
        &self.source
    }

    pub(crate) fn entries(&self) -> &BTreeMap<String, TargetEntry> {
        self.source.entries()
    }

    pub(crate) fn file(&self, path: &str) -> Option<&TargetFile> {
        self.source.file(path)
    }

    pub(crate) fn local_tree(&self) -> LocalTree {
        LocalTree::from_entries(
            &self.root,
            self.source
                .entries()
                .iter()
                .map(|(path, entry)| (path.clone(), entry.local.clone()))
                .collect(),
        )
    }

    pub(crate) fn prepared_files(&self) -> Result<Vec<(String, FileVersion)>> {
        let mut sources = BTreeMap::new();
        for (path, id) in &self.cache {
            if self.prepared.contains_key(id) {
                sources.entry(*id).or_insert_with(|| path.clone());
            }
        }
        if sources.len() != self.prepared.len() {
            bail!("a prepared file version has no frozen source path");
        }
        Ok(sources
            .into_iter()
            .map(|(id, path)| {
                (
                    path,
                    self.prepared
                        .get(&id)
                        .expect("prepared source identity was checked above")
                        .clone(),
                )
            })
            .collect())
    }

    pub(crate) fn cached(&self, path: &str, version: FileVersionId) -> bool {
        self.cache.get(path) == Some(&version)
    }

    pub(crate) fn content_path(&self, path: &str, version: FileVersionId) -> Option<PathBuf> {
        self.cached(path, version).then(|| self.root.join(path))
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

    pub(crate) async fn replace_manifest(
        &mut self,
        mut manifest: TargetManifest,
        refreshed: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        for path in refreshed {
            let local = entry_at(&self.root, path).await?;
            let entry = manifest
                .entries
                .get_mut(path)
                .with_context(|| format!("materialized path {path:?} is not in target manifest"))?;
            if local.kind != entry.local.kind {
                bail!("materialized path {path:?} has the wrong kind");
            }
            if let Some(file) = &entry.file {
                self.cache.insert(path.clone(), file.id);
            }
            entry.local = local;
        }
        self.manifest = manifest;
        self.cache.retain(|path, version| {
            self.manifest
                .file(path)
                .is_some_and(|file| file.id == *version)
        });
        self.validate()
    }

    pub(crate) fn matches_source_observation(&self, observed: &LocalTree) -> bool {
        self.source.entries().len() == observed.entries().len()
            && self.source.entries().iter().all(|(path, entry)| {
                observed
                    .entries()
                    .get(path)
                    .is_some_and(|observed| observed == &entry.local)
            })
    }

    fn validate(&self) -> Result<()> {
        validate_manifest(&self.source)?;
        validate_manifest(&self.manifest)?;
        for (path, version) in &self.cache {
            let file = self
                .manifest
                .file(path)
                .with_context(|| format!("cached path {path:?} is not a target file"))?;
            if file.id != *version {
                bail!("cached path {path:?} has another file version");
            }
            let metadata = fs::symlink_metadata(self.root.join(path))
                .with_context(|| format!("inspect staged content {path:?}"))?;
            if !metadata.file_type().is_file() || metadata.len() != file.logical_size {
                bail!("staged content {path:?} is incomplete");
            }
        }
        Ok(())
    }
}

fn validate_manifest(manifest: &TargetManifest) -> Result<()> {
    for (path, entry) in manifest.entries() {
        if (entry.local.kind == LocalKind::File) != entry.file.is_some() {
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
