// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::durable::{JsonFormat, install_json};
use crate::filesystem::{
    AuthorityIdentity, BranchBinding, ChangeCursor, DirectoryRecord, FileVersion, NodeId,
    NodeRecord, OperationId, VolumeId, VolumeSnapshot,
};
use crate::sync::local::LocalEntry;
use crate::sync::path::SnapshotTree;
use crate::sync::staging::TargetManifest;

const STATE_FORMAT: &str = "ofs-sync-replica/1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingIntent {
    pub operation: OperationId,
    pub staging: PathBuf,
    pub data_finalized: bool,
    pub renames: BTreeMap<String, String>,
    pub(crate) source: TargetManifest,
    pub(crate) manifest: Option<TargetManifest>,
    pub(crate) prepared: Vec<FileVersion>,
    pub(crate) cached_paths: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConflictRecord {
    pub path: String,
    pub local_digest: Option<[u8; 32]>,
    pub remote_digest: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaState {
    pub volume: VolumeId,
    pub branch: Option<BranchBinding>,
    pub(crate) authority: Option<VolumeSnapshot>,
    pub(crate) installed: BTreeMap<String, LocalEntry>,
    pub pending: Option<PendingIntent>,
    pub conflicts: Vec<ConflictRecord>,
}

impl ReplicaState {
    pub(crate) fn empty_for(authority: AuthorityIdentity) -> Self {
        Self {
            volume: authority.volume,
            branch: authority.branch,
            authority: None,
            installed: BTreeMap::new(),
            pending: None,
            conflicts: Vec::new(),
        }
    }

    pub fn authority_identity(&self) -> AuthorityIdentity {
        AuthorityIdentity {
            volume: self.volume,
            branch: self.branch.clone(),
        }
    }

    pub fn common(&self) -> ChangeCursor {
        self.authority
            .as_ref()
            .map(|snapshot| snapshot.cursor)
            .unwrap_or(ChangeCursor::Genesis)
    }

    pub(crate) fn at_common(
        identity: AuthorityIdentity,
        authority: &SnapshotTree<'_>,
        installed: BTreeMap<String, LocalEntry>,
    ) -> Result<Self> {
        validate_installed(authority, &installed)?;
        Ok(Self {
            volume: identity.volume,
            branch: identity.branch,
            authority: Some(authority.snapshot.clone()),
            installed,
            pending: None,
            conflicts: Vec::new(),
        })
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let path = path.as_ref();
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("read replica state"),
        };
        let stored: StoredState =
            serde_json::from_slice(&bytes).context("parse replica state JSON")?;
        let mut state: Self = stored.try_into()?;
        if let Some(intent) = &mut state.pending {
            let mut components = intent.staging.components();
            if !matches!(components.next(), Some(Component::Normal(_)))
                || components.next().is_some()
            {
                bail!("replica state contains an invalid pending cache name");
            }
            let cache_name = intent
                .staging
                .file_name()
                .context("replica state contains an invalid pending cache path")?
                .to_owned();
            let parent = path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            intent.staging = parent.join(cache_name);
        }
        Ok(Some(state))
    }

    pub fn install(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        install_json(
            path,
            "ofs-state",
            &StoredState::from(self),
            JsonFormat::Compact,
        )
        .context("install replica state")
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredState {
    format: String,
    volume: VolumeId,
    branch: Option<BranchBinding>,
    authority: Option<StoredSnapshot>,
    installed: BTreeMap<String, LocalEntry>,
    pending: Option<PendingIntent>,
    conflicts: Vec<ConflictRecord>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshot {
    cursor: ChangeCursor,
    root: NodeId,
    nodes: Vec<NodeRecord>,
    directories: Vec<DirectoryRecord>,
    file_versions: Vec<FileVersion>,
}

impl From<&ReplicaState> for StoredState {
    fn from(state: &ReplicaState) -> Self {
        Self {
            format: STATE_FORMAT.into(),
            volume: state.volume,
            branch: state.branch.clone(),
            authority: state.authority.as_ref().map(StoredSnapshot::from),
            installed: state.installed.clone(),
            pending: state.pending.clone().map(|mut intent| {
                intent.staging = intent
                    .staging
                    .file_name()
                    .map(PathBuf::from)
                    .expect("pending cache is a named sibling of replica state");
                intent
            }),
            conflicts: state.conflicts.clone(),
        }
    }
}

impl TryFrom<StoredState> for ReplicaState {
    type Error = anyhow::Error;

    fn try_from(stored: StoredState) -> Result<Self> {
        if stored.format != STATE_FORMAT {
            bail!("replica state format is unsupported");
        }
        let authority = stored
            .authority
            .map(|snapshot| snapshot.into_snapshot(stored.volume))
            .transpose()?;
        if let Some(snapshot) = &authority {
            validate_installed(&SnapshotTree::new(snapshot)?, &stored.installed)?;
        }
        Ok(Self {
            volume: stored.volume,
            branch: stored.branch,
            authority,
            installed: stored.installed,
            pending: stored.pending,
            conflicts: stored.conflicts,
        })
    }
}

impl From<&VolumeSnapshot> for StoredSnapshot {
    fn from(snapshot: &VolumeSnapshot) -> Self {
        Self {
            cursor: snapshot.cursor,
            root: snapshot.root,
            nodes: snapshot.nodes.values().cloned().collect(),
            directories: snapshot.directories.values().cloned().collect(),
            file_versions: snapshot.file_versions.values().cloned().collect(),
        }
    }
}

impl StoredSnapshot {
    fn into_snapshot(self, volume_id: VolumeId) -> Result<VolumeSnapshot> {
        let node_count = self.nodes.len();
        let nodes = self
            .nodes
            .into_iter()
            .map(|record| (record.id, record))
            .collect::<BTreeMap<_, _>>();
        let directory_count = self.directories.len();
        let directories = self
            .directories
            .into_iter()
            .map(|record| (record.node, record))
            .collect::<BTreeMap<_, _>>();
        let version_count = self.file_versions.len();
        let file_versions = self
            .file_versions
            .into_iter()
            .map(|record| (record.id, record))
            .collect::<BTreeMap<_, _>>();
        if nodes.len() != node_count
            || directories.len() != directory_count
            || file_versions.len() != version_count
        {
            bail!("replica authority snapshot repeats a record");
        }
        let snapshot = VolumeSnapshot {
            volume_id,
            cursor: self.cursor,
            root: self.root,
            nodes,
            directories,
            file_versions,
        };
        Ok(snapshot)
    }
}

fn validate_installed(
    tree: &SnapshotTree<'_>,
    installed: &BTreeMap<String, LocalEntry>,
) -> Result<()> {
    if tree.paths.len() != installed.len() {
        bail!("replica base and authority snapshot contain different paths");
    }
    for (path, node) in &tree.paths {
        let saved = installed
            .get(path)
            .with_context(|| format!("replica base is missing {path:?}"))?;
        let record = &tree.snapshot.nodes[node];
        let version = record
            .file_version
            .map(|id| &tree.snapshot.file_versions[&id]);
        if saved.kind != record.kind
            || saved.executable != record.attributes.executable
            || version.is_some_and(|version| saved.size != version.logical_size)
        {
            bail!("replica base disagrees with authority snapshot at {path:?}");
        }
    }
    Ok(())
}
