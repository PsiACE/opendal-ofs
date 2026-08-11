// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Cursor;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::filesystem::{ChangeCursor, NodeKind, OperationId, VolumeId, VolumeSnapshot};

use super::SyncError;

const FORMAT: &str = "managed-sync/1";
const MAGIC: &[u8; 8] = b"OFSSTATE";
const MAXIMUM_STATE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaState {
    format: String,
    root: PathBuf,
    common: VolumeSnapshot,
    remote: ChangeCursor,
    native: BTreeMap<String, NativeIdentity>,
    installing: bool,
    pending: Option<PendingPublication>,
    conflicts: Vec<ConflictRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingPublication {
    expected: VolumeSnapshot,
    target: VolumeSnapshot,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConflictRecord {
    pub path: String,
    pub local_digest: Option<[u8; 32]>,
    pub remote_digest: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeIdentity {
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) kind: NodeKind,
}

impl ReplicaState {
    pub fn new(root: PathBuf, common: VolumeSnapshot) -> Result<Self, SyncError> {
        Self::with_native(root, common, BTreeMap::new())
    }

    pub(crate) fn with_native(
        root: PathBuf,
        common: VolumeSnapshot,
        native: BTreeMap<String, NativeIdentity>,
    ) -> Result<Self, SyncError> {
        common.validate()?;
        validate_native(&common, &native)?;
        Ok(Self {
            format: FORMAT.to_owned(),
            root,
            remote: common.cursor,
            common,
            native,
            installing: false,
            pending: None,
            conflicts: Vec::new(),
        })
    }

    pub fn load(path: &Path) -> Result<Option<Self>, SyncError> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(SyncError::io("read replica state", error)),
        };
        let state = decode(&bytes)?;
        if state.format != FORMAT {
            return Err(SyncError::new("replica state format is unsupported"));
        }
        state.common.validate()?;
        if !state.installing {
            validate_native(&state.common, &state.native)?;
        }
        if state.remote.sequence() < state.common.cursor.sequence() {
            return Err(SyncError::new(
                "replica remote cursor is behind its common namespace",
            ));
        }
        Ok(Some(state))
    }

    pub fn save_new(&self, path: &Path) -> Result<(), SyncError> {
        if path.exists() {
            return Err(SyncError::new(format!(
                "cannot attach with an existing replica state: {}",
                path.display()
            )));
        }
        self.save(path)
    }

    pub fn save(&self, path: &Path) -> Result<(), SyncError> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        if let Some(parent) = parent {
            fs::create_dir_all(parent)
                .map_err(|error| SyncError::io("create replica state directory", error))?;
        }
        let temporary = temporary_path(path, OperationId::generate());
        let bytes = encode(self)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| SyncError::io("create replica state", error))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| SyncError::io("persist replica state", error))?;
        fs::rename(&temporary, path)
            .map_err(|error| SyncError::io("install replica state", error))?;
        sync_parent(path)?;
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn volume_id(&self) -> VolumeId {
        self.common.volume_id
    }

    pub const fn common(&self) -> &VolumeSnapshot {
        &self.common
    }

    pub const fn remote_cursor(&self) -> ChangeCursor {
        self.remote
    }

    pub(crate) fn native(&self) -> &BTreeMap<String, NativeIdentity> {
        &self.native
    }

    pub(crate) fn advance(
        &mut self,
        common: VolumeSnapshot,
        native: BTreeMap<String, NativeIdentity>,
    ) -> Result<(), SyncError> {
        common.validate()?;
        validate_native(&common, &native)?;
        self.remote = common.cursor;
        self.common = common;
        self.native = native;
        self.installing = false;
        self.pending = None;
        self.conflicts.clear();
        Ok(())
    }

    pub const fn has_pending(&self) -> bool {
        self.installing || self.pending.is_some()
    }

    pub(crate) const fn has_pending_publication(&self) -> bool {
        self.pending.is_some()
    }

    pub(crate) const fn is_installing(&self) -> bool {
        self.installing
    }

    pub(crate) fn pending_target(&self) -> Option<&VolumeSnapshot> {
        self.pending.as_ref().map(|pending| &pending.target)
    }

    pub(crate) fn pending_expected(&self) -> Option<&VolumeSnapshot> {
        self.pending.as_ref().map(|pending| &pending.expected)
    }

    pub fn conflicts(&self) -> &[ConflictRecord] {
        &self.conflicts
    }

    pub(crate) fn retain_conflicts(
        &mut self,
        conflicts: Vec<ConflictRecord>,
        remote: ChangeCursor,
    ) {
        self.pending = None;
        self.conflicts = conflicts;
        self.remote = remote;
    }

    pub(crate) fn cancel_pending(&mut self, remote: ChangeCursor) {
        self.pending = None;
        self.remote = remote;
    }

    pub(crate) fn begin(
        &mut self,
        expected: VolumeSnapshot,
        target: VolumeSnapshot,
    ) -> Result<(), SyncError> {
        expected.validate()?;
        target.validate()?;
        if expected.volume_id != self.volume_id()
            || target.volume_id != self.volume_id()
            || expected.cursor.sequence() < self.common.cursor.sequence()
            || target.cursor.sequence() != expected.cursor.sequence() + 1
        {
            return Err(SyncError::new("pending publication ancestry is invalid"));
        }
        self.remote = expected.cursor;
        self.pending = Some(PendingPublication { expected, target });
        self.conflicts.clear();
        Ok(())
    }

    pub(crate) fn begin_install(&mut self) {
        self.installing = true;
        self.conflicts.clear();
    }

    pub(crate) fn for_cold_install(
        root: PathBuf,
        target: VolumeSnapshot,
    ) -> Result<Self, SyncError> {
        target.validate()?;
        Ok(Self {
            format: FORMAT.to_owned(),
            root,
            remote: target.cursor,
            common: target,
            native: BTreeMap::new(),
            installing: true,
            pending: None,
            conflicts: Vec::new(),
        })
    }
}

fn validate_native(
    snapshot: &VolumeSnapshot,
    native: &BTreeMap<String, NativeIdentity>,
) -> Result<(), SyncError> {
    let paths = snapshot.paths()?;
    if paths.len() != native.len()
        || paths.iter().any(|(path, node)| {
            native
                .get(path)
                .is_none_or(|identity| identity.kind != snapshot.nodes[node].kind)
        })
        || native
            .values()
            .map(|identity| (identity.device, identity.inode))
            .collect::<BTreeSet<_>>()
            .len()
            != native.len()
    {
        return Err(SyncError::new(
            "replica native identities do not match the common namespace",
        ));
    }
    Ok(())
}

fn encode(state: &ReplicaState) -> Result<Vec<u8>, SyncError> {
    let mut body = Vec::new();
    ciborium::into_writer(state, &mut body)
        .map_err(|_| SyncError::new("replica state cannot be encoded"))?;
    if body.len() > MAXIMUM_STATE_BYTES {
        return Err(SyncError::new("replica state exceeds its size limit"));
    }
    let mut bytes = Vec::with_capacity(MAGIC.len() + body.len() + 32);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<ReplicaState, SyncError> {
    let body = bytes
        .strip_prefix(MAGIC)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| SyncError::new("replica state is invalid"))?;
    if body.len() > MAXIMUM_STATE_BYTES
        || blake3::hash(&bytes[..bytes.len() - 32]).as_bytes() != &bytes[bytes.len() - 32..]
    {
        return Err(SyncError::new("replica state checksum is invalid"));
    }
    let mut input = Cursor::new(body);
    let state = ciborium::from_reader(&mut input)
        .map_err(|_| SyncError::new("replica state is invalid"))?;
    if input.position() != body.len() as u64 {
        return Err(SyncError::new("replica state has trailing bytes"));
    }
    Ok(state)
}

fn temporary_path(path: &Path, operation: OperationId) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state");
    path.with_file_name(format!(".{name}.{operation}.tmp"))
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), SyncError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    File::open(parent.unwrap_or_else(|| Path::new(".")))
        .and_then(|directory| directory.sync_all())
        .map_err(|error| SyncError::io("persist replica state directory", error))
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), SyncError> {
    Ok(())
}
