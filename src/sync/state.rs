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

use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Write as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::{ChangeCursor, Digest, OperationId, VolumeId};
use crate::managed::NamespaceRevision;

const MAGIC: &[u8; 8] = b"OFSSTAT1";
const MAXIMUM_STATE_BYTES: usize = 16 * 1024;

/// A recoverable binding between one local replica and its remote namespace.
///
/// This record deliberately contains no namespace image or per-path identity map.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaState {
    root: PathBuf,
    volume_id: VolumeId,
    common: NamespaceRevision,
    observed: NamespaceRevision,
    phase: SyncPhase,
    conflicts: u64,
    base_expired: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SyncPhase {
    Clean,
    Publishing {
        target: NamespaceRevision,
        operation_id: OperationId,
        maintenance_generation: u64,
    },
    Installing {
        published: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConflictRecord {
    pub path: String,
    pub local_digest: Option<Digest>,
    pub remote_digest: Option<Digest>,
}

impl ReplicaState {
    pub fn new(root: PathBuf, volume_id: VolumeId, common: NamespaceRevision) -> Self {
        Self {
            root,
            volume_id,
            common,
            observed: common,
            phase: SyncPhase::Clean,
            conflicts: 0,
            base_expired: false,
        }
    }

    pub fn load(path: &Path) -> Result<Option<Self>, Error> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(Error::from_io("read replica state", Some(path), error));
            }
        };
        let state = decode(&bytes)?;
        state.validate()?;
        Ok(Some(state))
    }

    pub fn save_new(&self, path: &Path) -> Result<(), Error> {
        self.persist(path, false)
    }

    pub fn save(&self, path: &Path) -> Result<(), Error> {
        self.persist(path, true)
    }

    fn persist(&self, path: &Path, replace: bool) -> Result<(), Error> {
        self.validate()?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        if let Some(parent) = parent {
            fs::create_dir_all(parent).map_err(|error| {
                Error::from_io("create replica state directory", Some(parent), error)
            })?;
        }
        let temporary = temporary_path(path, OperationId::generate());
        let bytes = encode(self)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| Error::from_io("create replica state", Some(&temporary), error))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| Error::from_io("persist replica state", Some(&temporary), error))?;
        if replace {
            fs::rename(&temporary, path)
                .map_err(|error| Error::from_io("install replica state", Some(path), error))?;
        } else {
            fs::hard_link(&temporary, path).map_err(|error| {
                let _ = fs::remove_file(&temporary);
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    Error::invalid(
                        "synchronize replica",
                        format!(
                            "cannot attach with an existing replica state: {}",
                            path.display()
                        ),
                    )
                } else {
                    Error::from_io("install new replica state", Some(path), error)
                }
            })?;
            fs::remove_file(&temporary).map_err(|error| {
                Error::from_io(
                    "remove replica state temporary file",
                    Some(&temporary),
                    error,
                )
            })?;
        }
        sync_parent(path)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.observed.cursor().sequence() < self.common.cursor().sequence() {
            return Err(Error::invalid(
                "synchronize replica",
                "replica remote cursor is behind its common namespace",
            ));
        }
        match self.phase {
            SyncPhase::Clean => Ok(()),
            SyncPhase::Publishing { target, .. }
                if self.observed.cursor().sequence() >= self.common.cursor().sequence()
                    && target.cursor().sequence() == self.observed.cursor().sequence() + 1 =>
            {
                Ok(())
            }
            SyncPhase::Installing { .. }
                if self.observed.cursor().sequence() >= self.common.cursor().sequence() =>
            {
                Ok(())
            }
            _ => Err(Error::corrupt(
                "read replica state",
                "replica recovery references are invalid",
            )),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub const fn common_revision(&self) -> NamespaceRevision {
        self.common
    }

    pub const fn remote_cursor(&self) -> ChangeCursor {
        self.observed.cursor()
    }

    pub const fn conflict_count(&self) -> u64 {
        self.conflicts
    }

    pub const fn has_pending(&self) -> bool {
        !matches!(self.phase, SyncPhase::Clean)
    }

    pub const fn base_expired(&self) -> bool {
        self.base_expired
    }

    pub(crate) const fn pending_publication(
        &self,
    ) -> Option<(NamespaceRevision, NamespaceRevision, OperationId, u64)> {
        match self.phase {
            SyncPhase::Publishing {
                target,
                operation_id,
                maintenance_generation,
            } => Some((self.observed, target, operation_id, maintenance_generation)),
            _ => None,
        }
    }

    pub(crate) const fn installation(&self) -> Option<(NamespaceRevision, bool)> {
        match self.phase {
            SyncPhase::Installing { published } => Some((self.observed, published)),
            _ => None,
        }
    }

    pub(crate) fn advance(&mut self, common: NamespaceRevision) {
        self.common = common;
        self.observed = common;
        self.phase = SyncPhase::Clean;
        self.conflicts = 0;
        self.base_expired = false;
    }

    pub(crate) fn rebase_equivalent(&mut self, common: NamespaceRevision) -> Result<(), Error> {
        if !matches!(self.phase, SyncPhase::Clean)
            || common.cursor().sequence() != self.common.cursor().sequence()
        {
            return Err(Error::corrupt(
                "synchronize replica",
                "equivalent namespace rebase changed the logical cursor",
            ));
        }
        self.common = common;
        self.observed = common;
        self.validate()
    }

    pub(crate) fn begin_publication(
        &mut self,
        expected: NamespaceRevision,
        target: NamespaceRevision,
        operation_id: OperationId,
        maintenance_generation: u64,
    ) -> Result<(), Error> {
        self.phase = SyncPhase::Publishing {
            target,
            operation_id,
            maintenance_generation,
        };
        self.observed = expected;
        self.conflicts = 0;
        self.base_expired = false;
        self.validate()
    }

    pub(crate) fn begin_install(&mut self, target: NamespaceRevision, published: bool) {
        self.phase = SyncPhase::Installing { published };
        self.observed = target;
        self.conflicts = 0;
        self.base_expired = false;
    }

    pub(crate) fn retain_conflicts(
        &mut self,
        conflicts: usize,
        remote: NamespaceRevision,
        base_expired: bool,
    ) {
        self.phase = SyncPhase::Clean;
        self.conflicts = conflicts.try_into().unwrap_or(u64::MAX);
        self.observed = remote;
        self.base_expired = base_expired;
    }

    pub(crate) fn cancel_pending(&mut self, remote: NamespaceRevision) {
        self.phase = SyncPhase::Clean;
        if remote.cursor().sequence() == self.common.cursor().sequence() {
            self.common = remote;
        }
        self.observed = remote;
        self.base_expired = false;
    }

    pub(crate) fn for_cold_install(
        root: PathBuf,
        volume_id: VolumeId,
        target: NamespaceRevision,
    ) -> Self {
        let mut state = Self::new(root, volume_id, target);
        state.begin_install(target, false);
        state
    }
}

fn encode(state: &ReplicaState) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    ciborium::into_writer(state, &mut body)
        .map_err(|_| Error::corrupt("persist replica state", "state cannot be encoded"))?;
    if body.len() > MAXIMUM_STATE_BYTES {
        return Err(Error::corrupt(
            "persist replica state",
            "state exceeds its size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(MAGIC.len() + body.len() + 32);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<ReplicaState, Error> {
    let body = bytes
        .strip_prefix(MAGIC)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| Error::corrupt("read replica state", "state is invalid"))?;
    if body.len() > MAXIMUM_STATE_BYTES
        || blake3::hash(&bytes[..bytes.len() - 32]).as_bytes() != &bytes[bytes.len() - 32..]
    {
        return Err(Error::corrupt(
            "read replica state",
            "state checksum is invalid",
        ));
    }
    let mut input = Cursor::new(body);
    let state = ciborium::from_reader(&mut input)
        .map_err(|_| Error::corrupt("read replica state", "state is invalid"))?;
    if input.position() != body.len() as u64 {
        return Err(Error::corrupt(
            "read replica state",
            "state has trailing bytes",
        ));
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
fn sync_parent(path: &Path) -> Result<(), Error> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::from_io("persist replica state directory", Some(parent), error))
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), Error> {
    Ok(())
}
