// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write as _};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::filesystem::{
    AuthorityIdentity, BranchBinding, ChangeCursor, DirectoryRecord, FileVersion, NodeId,
    NodeRecord, OperationId, VolumeId, VolumeSnapshot,
};
use crate::sync::local::NativeIdentity;

const STATE_FORMAT: &str = "ofs-sync-replica";
const STATE_MAJOR: u16 = 1;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstalledEntry {
    pub local_identity: Option<NativeIdentity>,
    pub local_size: Option<u64>,
    pub local_modified: Option<String>,
    pub local_executable: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingIntent {
    pub operation: OperationId,
    pub staging: PathBuf,
    pub renames: BTreeMap<String, String>,
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
    pub(crate) installed: BTreeMap<String, InstalledEntry>,
    pub pending: Option<PendingIntent>,
    pub conflicts: Vec<ConflictRecord>,
}

impl ReplicaState {
    pub(crate) fn empty(volume: VolumeId) -> Self {
        Self::empty_for(AuthorityIdentity::base(volume))
    }

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
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent).context("create replica state directory")?;
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary =
            path.with_extension(format!("ofs-state.{}.{}.tmp", std::process::id(), sequence));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let result = (|| -> Result<()> {
            let mut file = options.open(&temporary)?;
            serde_json::to_writer(&mut file, &StoredState::from(self))?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, path)?;
            sync_directory(parent)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.context("install replica state")
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredState {
    format: String,
    major: u16,
    volume: VolumeId,
    branch: Option<BranchBinding>,
    authority: Option<StoredSnapshot>,
    installed: BTreeMap<String, InstalledEntry>,
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
            major: STATE_MAJOR,
            volume: state.volume,
            branch: state.branch.clone(),
            authority: state.authority.as_ref().map(StoredSnapshot::from),
            installed: state.installed.clone(),
            pending: state.pending.as_ref().map(|intent| PendingIntent {
                operation: intent.operation,
                staging: intent
                    .staging
                    .file_name()
                    .map(PathBuf::from)
                    .expect("pending cache is a named sibling of replica state"),
                renames: intent.renames.clone(),
            }),
            conflicts: state.conflicts.clone(),
        }
    }
}

impl TryFrom<StoredState> for ReplicaState {
    type Error = anyhow::Error;

    fn try_from(stored: StoredState) -> Result<Self> {
        if stored.format != STATE_FORMAT || stored.major != STATE_MAJOR {
            bail!("replica state format is unsupported");
        }
        let authority = stored
            .authority
            .map(|snapshot| snapshot.into_snapshot(stored.volume))
            .transpose()?;
        if let Some(snapshot) = &authority {
            validate_installed(snapshot, &stored.installed)?;
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
        validate_snapshot(&snapshot).context("replica authority snapshot is invalid")?;
        Ok(snapshot)
    }
}

fn validate_installed(
    snapshot: &VolumeSnapshot,
    installed: &BTreeMap<String, InstalledEntry>,
) -> Result<()> {
    let mut expected = BTreeMap::new();
    let mut pending = vec![(String::new(), snapshot.root)];
    while let Some((path, node)) = pending.pop() {
        if !path.is_empty() {
            expected.insert(path.clone(), node);
        }
        if let Some(directory) = snapshot.directories.get(&node) {
            for (name, entry) in &directory.entries {
                let child = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}/{name}")
                };
                pending.push((child, entry.node));
            }
        }
    }
    if expected.len() != installed.len() {
        bail!("replica base and authority snapshot contain different paths");
    }
    for (path, node) in expected {
        let saved = installed
            .get(&path)
            .with_context(|| format!("replica base is missing {path:?}"))?;
        let record = &snapshot.nodes[&node];
        let version = record.file_version.map(|id| &snapshot.file_versions[&id]);
        if saved.local_executable != Some(record.attributes.executable)
            || version.is_some_and(|version| saved.local_size != Some(version.logical_size))
        {
            bail!("replica base disagrees with authority snapshot at {path:?}");
        }
    }
    Ok(())
}

fn validate_snapshot(snapshot: &VolumeSnapshot) -> Result<()> {
    snapshot.validate_structure().map_err(Into::into)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().map_err(Into::into)
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> Result<()> {
    Ok(())
}
