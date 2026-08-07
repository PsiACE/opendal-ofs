// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::local::{
    LocalEntry, LocalKind, LocalTree, NativeIdentity, entry_at, executable_at, fs_operator,
    native_identity_at, set_executable,
};

const COPY_CHUNK: u64 = 1024 * 1024;
const COPY_CONCURRENCY: usize = 8;
const MANIFEST_FORMAT: &str = "ofs-staged-tree";
const MANIFEST_MAJOR: u16 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StagedFile {
    pub size: u64,
    pub source_modified: String,
    pub digest: [u8; 32],
}

/// Immutable input for a later Managed FileVersion builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedTree {
    root: PathBuf,
    logical: LocalTree,
    files: BTreeMap<String, StagedFile>,
    source_identities: BTreeMap<String, Option<NativeIdentity>>,
    content_paths: BTreeSet<String>,
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

        let prepared = stream::iter(
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
                        Ok::<_, anyhow::Error>((path, file, known_digest.is_none()))
                    }
                }),
        )
        .buffer_unordered(COPY_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter();
        let mut files = BTreeMap::new();
        let mut content_paths = BTreeSet::new();
        for (path, file, has_content) in prepared {
            if has_content {
                content_paths.insert(path.clone());
            }
            files.insert(path, file);
        }
        for (path, entry) in tree.entries() {
            require_same_identity(tree.root(), path, entry.kind, entry.native_identity)?;
        }
        let staged = Self {
            root: root.to_owned(),
            logical: LocalTree::from_entries(root, tree.entries().clone()),
            files,
            source_identities,
            content_paths,
        };
        staged.save_manifest()?;
        Ok(staged)
    }

    pub fn load(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let path = Self::manifest_path(root);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(error).context("staged tree manifest is missing");
            }
            Err(error) => return Err(error).context("read staged tree manifest"),
        };
        let stored: StoredStagedTree =
            serde_json::from_slice(&bytes).context("parse staged tree manifest")?;
        if stored.format != MANIFEST_FORMAT || stored.major != MANIFEST_MAJOR {
            bail!("staged tree manifest has an unsupported format");
        }
        let staged = Self {
            root: root.to_owned(),
            logical: LocalTree::from_entries(root, stored.entries),
            files: stored.files,
            source_identities: stored.source_identities,
            content_paths: stored.content_paths,
        };
        staged.validate()?;
        Ok(staged)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn files(&self) -> &BTreeMap<String, StagedFile> {
        &self.files
    }

    pub fn logical(&self) -> &LocalTree {
        &self.logical
    }

    pub fn has_content(&self, path: &str) -> bool {
        self.content_paths.contains(path)
    }

    pub fn content_path(&self, path: &str) -> Option<PathBuf> {
        self.has_content(path).then(|| self.root.join(path))
    }

    pub fn manifest_path(root: &Path) -> PathBuf {
        let parent = root
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let name = root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("staging");
        parent.join(format!(".{name}.ofs-staged-tree.json"))
    }

    pub fn save_manifest(&self) -> Result<()> {
        self.validate()?;
        let path = Self::manifest_path(&self.root);
        let parent = path.parent().unwrap_or(Path::new("."));
        fs::create_dir_all(parent).context("create staged tree manifest directory")?;
        let temporary = parent.join(format!(".ofs-staged-tree-{}.tmp", uuid::Uuid::new_v4()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let stored = StoredStagedTree::from(self);
        let result = (|| -> Result<()> {
            let mut file = options.open(&temporary)?;
            serde_json::to_writer(&mut file, &stored)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &path)?;
            sync_directory(parent)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.context("install staged tree manifest")
    }

    pub fn remove_manifest(root: &Path) -> Result<()> {
        let path = Self::manifest_path(root);
        match fs::remove_file(&path) {
            Ok(()) => sync_directory(path.parent().unwrap_or(Path::new("."))),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("remove staged tree manifest"),
        }
    }

    pub async fn record_materialized_file(
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
                source_modified: entry.modified.clone(),
                digest,
            },
        );
        self.source_identities
            .insert(path.clone(), entry.native_identity);
        self.content_paths.insert(path.clone());
        self.logical.insert(path, entry);
        Ok(())
    }

    pub async fn record_materialized_directory(&mut self, path: impl Into<String>) -> Result<()> {
        let path = path.into();
        let entry = entry_at(&self.root, &path).await?;
        if entry.kind != LocalKind::Directory {
            bail!("materialized path {path:?} is not a directory");
        }
        self.files.remove(&path);
        self.content_paths.remove(&path);
        self.source_identities
            .insert(path.clone(), entry.native_identity);
        self.logical.insert(path, entry);
        Ok(())
    }

    pub fn remove_logical_path(&mut self, path: &str) {
        self.logical.remove(path);
        self.files.remove(path);
        self.source_identities.remove(path);
        self.content_paths.remove(path);
        self.remove_descendants(path);
    }

    pub fn matches_source_observation(&self, observed: &LocalTree) -> bool {
        self.logical.entries() == observed.entries()
            && self.source_identities
                == observed
                    .entries()
                    .iter()
                    .map(|(path, entry)| (path.clone(), entry.native_identity))
                    .collect()
    }

    pub(crate) fn source_identities(&self) -> &BTreeMap<String, Option<NativeIdentity>> {
        &self.source_identities
    }

    fn remove_descendants(&mut self, path: &str) {
        let prefix = format!("{path}/");
        let descendants = self
            .logical
            .entries()
            .keys()
            .filter(|candidate| candidate.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>();
        for descendant in descendants {
            self.logical.remove(&descendant);
            self.files.remove(&descendant);
            self.source_identities.remove(&descendant);
            self.content_paths.remove(&descendant);
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
        if logical_files != self.files.keys().collect()
            || self.logical.entries().keys().collect::<BTreeSet<_>>()
                != self.source_identities.keys().collect()
        {
            bail!("staged tree manifest does not describe one complete logical tree");
        }
        for path in &self.content_paths {
            let file = self
                .files
                .get(path)
                .with_context(|| format!("staged content {path:?} is not a logical file"))?;
            let metadata = fs::symlink_metadata(self.root.join(path))
                .with_context(|| format!("inspect staged content {path:?}"))?;
            if !metadata.file_type().is_file() || metadata.len() != file.size {
                bail!("staged content {path:?} is incomplete");
            }
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredStagedTree {
    format: String,
    major: u16,
    entries: BTreeMap<String, LocalEntry>,
    files: BTreeMap<String, StagedFile>,
    source_identities: BTreeMap<String, Option<NativeIdentity>>,
    content_paths: BTreeSet<String>,
}

impl From<&StagedTree> for StoredStagedTree {
    fn from(staged: &StagedTree) -> Self {
        Self {
            format: MANIFEST_FORMAT.to_owned(),
            major: MANIFEST_MAJOR,
            entries: staged.logical.entries().clone(),
            files: staged.files.clone(),
            source_identities: staged.source_identities.clone(),
            content_paths: staged.content_paths.clone(),
        }
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
    if known_digest.is_none() {
        set_executable(&staging_root.join(path), expected.executable)
            .with_context(|| format!("preserve executable bit for {path:?}"))?;
        let staged_metadata = staged
            .stat(path)
            .await
            .with_context(|| format!("verify staging file {path:?}"))?;
        if staged_metadata.content_length() != expected.size {
            bail!("staging file {path:?} is incomplete; retry sync");
        }
    }
    Ok(StagedFile {
        size: expected.size,
        source_modified: expected.modified.clone(),
        digest,
    })
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().map_err(Into::into)
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> Result<()> {
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
