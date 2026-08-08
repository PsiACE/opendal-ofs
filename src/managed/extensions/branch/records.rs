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

//! Provider-neutral branch authority records.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::filesystem::{
    BranchBinding, BranchId, BranchName, ChangeCursor, DirectoryEntry, FileVersionId, NodeId,
    NodeKind, OperationId, VolumeId,
};
use crate::managed::format::ExtentMap;
use crate::managed::metadata::namespace::{
    DirectoryPrecondition, DirectoryRecord, FileVersionRecord, NamespaceChange,
    NamespacePublication, NamespaceSnapshot, NodePrecondition, NodeRecord, managed_generation,
    managed_generation_number, validate_publication, validate_snapshot,
};
use crate::managed::{ManagedError, ManagedErrorKind};

pub(crate) type StoredResults = BTreeMap<(BranchId, OperationId), StoredCommittedResult>;

pub(crate) const FORMAT_MAJOR: u16 = 1;
pub(crate) const MAX_TAIL_TRANSACTIONS: usize = 32;
pub(crate) const MAX_TAIL_BYTES: usize = 128 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchLifecycle {
    Active,
    Sealed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchInfo {
    pub binding: BranchBinding,
    pub lifecycle: BranchLifecycle,
    pub cursor: ChangeCursor,
    pub is_default: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForkPoint {
    Head,
    Sequence(u64),
}

/// One replayable namespace change. `payload` uses the shared branch change
/// codec and is interpreted identically by Object and transactional metadata.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredChange {
    pub(crate) origin_branch: [u8; 16],
    pub(crate) operation: [u8; 16],
    pub(crate) parent: StoredCursor,
    pub(crate) cursor: StoredCursor,
    pub(crate) payload: Vec<u8>,
}

impl StoredChange {
    pub(crate) fn prepare(
        branch: BranchId,
        publication: &NamespacePublication,
        base: Option<&NamespaceSnapshot>,
    ) -> Result<(Self, bool), ManagedError> {
        let valid = validate_publication(publication, base)?;
        let delta = StoredDelta::from_change(NamespaceChange::from_publication(publication, base))?;
        Ok((
            Self {
                origin_branch: *branch.as_bytes(),
                operation: *publication.operation.as_bytes(),
                parent: publication.parent.into(),
                cursor: publication.target.cursor.into(),
                payload: encode_value(&delta)?,
            },
            valid,
        ))
    }

    pub(crate) fn apply(
        &self,
        base: Option<NamespaceSnapshot>,
    ) -> Result<NamespaceSnapshot, ManagedError> {
        let delta: StoredDelta = decode_value(&self.payload)?;
        delta
            .into_change(
                OperationId::from_bytes(self.operation),
                self.parent.decode()?,
                self.cursor.decode()?,
            )
            .apply(base)
            .map_err(|_| corrupt("stored branch change is invalid"))
    }

    pub(crate) fn request_digest(&self) -> Result<[u8; 32], ManagedError> {
        Ok(Sha256::digest(encode_value(self)?).into())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredNamespaceState {
    pub(crate) checkpoint: [u8; 32],
    pub(crate) checkpoint_cursor: StoredCursor,
    pub(crate) tail: Vec<StoredChange>,
    pub(crate) previous_history: Option<[u8; 32]>,
}

impl StoredNamespaceState {
    pub(crate) fn cursor(&self) -> Result<ChangeCursor, ManagedError> {
        self.tail.last().map_or_else(
            || self.checkpoint_cursor.decode(),
            |change| change.cursor.decode(),
        )
    }

    pub(crate) fn validate_shape(&self) -> Result<(), ManagedError> {
        if self.tail.len() > MAX_TAIL_TRANSACTIONS
            || self
                .tail
                .iter()
                .map(|change| change.payload.len())
                .sum::<usize>()
                > MAX_TAIL_BYTES
        {
            return Err(corrupt("branch transaction tail exceeds its limit"));
        }
        let mut parent = self.checkpoint_cursor.decode()?;
        for change in &self.tail {
            if change.parent.decode()? != parent {
                return Err(corrupt("branch transaction tail is not consecutive"));
            }
            let cursor = change.cursor.decode()?;
            if cursor.operation() != Some(OperationId::from_bytes(change.operation))
                || parent.sequence().checked_add(1) != Some(cursor.sequence())
            {
                return Err(corrupt("branch transaction cursor is invalid"));
            }
            parent = cursor;
        }
        Ok(())
    }

    pub(crate) fn at_sequence(&self, sequence: u64) -> Option<Self> {
        let checkpoint = self.checkpoint_cursor.sequence;
        if sequence < checkpoint || sequence > self.cursor().ok()?.sequence() {
            return None;
        }
        let mut state = self.clone();
        state.tail.truncate((sequence - checkpoint) as usize);
        Some(state)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredCheckpoint {
    pub(crate) major: u16,
    pub(crate) volume_id: [u8; 16],
    snapshot: StoredSnapshot,
    pub(crate) results: Vec<StoredCommittedResult>,
}

impl StoredCheckpoint {
    pub(crate) fn new(
        snapshot: &NamespaceSnapshot,
        results: StoredResults,
    ) -> Result<Self, ManagedError> {
        validate_snapshot(snapshot)?;
        Ok(Self {
            major: FORMAT_MAJOR,
            volume_id: *snapshot.volume_id.as_bytes(),
            snapshot: StoredSnapshot::from_snapshot(snapshot)?,
            results: results.into_values().collect(),
        })
    }

    pub(crate) fn recover(
        self,
        volume_id: VolumeId,
    ) -> Result<(NamespaceSnapshot, StoredResults), ManagedError> {
        if self.major != FORMAT_MAJOR || self.volume_id != *volume_id.as_bytes() {
            return Err(corrupt("branch checkpoint identity is invalid"));
        }
        let snapshot = self.snapshot.into_snapshot(volume_id)?;
        validate_snapshot(&snapshot)?;
        let mut results = BTreeMap::new();
        for result in self.results {
            result.validate()?;
            let key = (result.origin(), result.operation());
            if results.insert(key, result).is_some() {
                return Err(corrupt("branch checkpoint contains duplicate results"));
            }
        }
        Ok((snapshot, results))
    }

    pub(crate) fn resolve(
        &self,
        branch: BranchId,
        operation: OperationId,
    ) -> Result<Option<&StoredCommittedResult>, ManagedError> {
        self.results
            .iter()
            .find(|result| {
                result.origin_branch == *branch.as_bytes()
                    && result.operation == *operation.as_bytes()
            })
            .map(|result| result.validate().map(|()| result))
            .transpose()
    }
}

pub(crate) fn recover_namespace(
    checkpoint: StoredCheckpoint,
    state: &StoredNamespaceState,
    volume_id: VolumeId,
) -> Result<NamespaceSnapshot, ManagedError> {
    let (mut snapshot, _) = checkpoint.recover(volume_id)?;
    if snapshot.cursor != state.checkpoint_cursor.decode()? {
        return Err(corrupt("branch checkpoint and HEAD disagree"));
    }
    for change in &state.tail {
        snapshot = change.apply(Some(snapshot))?;
    }
    if snapshot.cursor != state.cursor()? {
        return Err(corrupt("branch transaction tail does not reach HEAD"));
    }
    Ok(snapshot)
}

pub(crate) fn recover_retained(
    checkpoint: StoredCheckpoint,
    state: &StoredNamespaceState,
    volume_id: VolumeId,
) -> Result<Vec<NamespaceSnapshot>, ManagedError> {
    let (mut snapshot, _) = checkpoint.recover(volume_id)?;
    if snapshot.cursor != state.checkpoint_cursor.decode()? {
        return Err(corrupt("branch checkpoint and retained state disagree"));
    }
    let mut snapshots = vec![snapshot.clone()];
    for change in &state.tail {
        snapshot = change.apply(Some(snapshot))?;
        snapshots.push(snapshot.clone());
    }
    Ok(snapshots)
}

pub(crate) fn results_for_rotation(
    checkpoint: StoredCheckpoint,
    state: &StoredNamespaceState,
    committed: &StoredChange,
    volume_id: VolumeId,
) -> Result<StoredResults, ManagedError> {
    let (_, mut results) = checkpoint.recover(volume_id)?;
    for change in state.tail.iter().chain(std::iter::once(committed)) {
        let result = StoredCommittedResult::from_change(change)?;
        results.insert((result.origin(), result.operation()), result);
    }
    Ok(results)
}

pub(crate) fn require_request_digest(
    expected: Option<[u8; 32]>,
    observed: [u8; 32],
) -> Result<(), ManagedError> {
    if expected.is_none_or(|expected| expected == observed) {
        Ok(())
    } else {
        Err(ManagedError::new(
            ManagedErrorKind::Conflict,
            "publish Managed branch",
            "operation identity was reused with another payload",
        ))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredCommittedResult {
    pub(crate) origin_branch: [u8; 16],
    pub(crate) operation: [u8; 16],
    pub(crate) cursor: StoredCursor,
    pub(crate) request_sha256: [u8; 32],
}

impl StoredCommittedResult {
    pub(crate) fn from_change(change: &StoredChange) -> Result<Self, ManagedError> {
        let result = Self {
            origin_branch: change.origin_branch,
            operation: change.operation,
            cursor: change.cursor,
            request_sha256: change.request_digest()?,
        };
        result.validate()?;
        Ok(result)
    }

    pub(crate) fn origin(&self) -> BranchId {
        BranchId::from_bytes(self.origin_branch)
    }

    pub(crate) fn operation(&self) -> OperationId {
        OperationId::from_bytes(self.operation)
    }

    pub(crate) fn validate(&self) -> Result<(), ManagedError> {
        if self.cursor.decode()?.operation() != Some(self.operation()) {
            return Err(corrupt("committed branch result cursor is invalid"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredHistory {
    pub(crate) major: u16,
    pub(crate) volume_id: [u8; 16],
    pub(crate) creator_branch: [u8; 16],
    pub(crate) checkpoint: [u8; 32],
    pub(crate) checkpoint_cursor: StoredCursor,
    pub(crate) changes: Vec<StoredChange>,
    pub(crate) previous_history: Option<[u8; 32]>,
}

impl StoredHistory {
    pub(crate) fn new(
        volume_id: VolumeId,
        creator: BranchId,
        state: &StoredNamespaceState,
    ) -> Result<Self, ManagedError> {
        let history = Self {
            major: FORMAT_MAJOR,
            volume_id: *volume_id.as_bytes(),
            creator_branch: *creator.as_bytes(),
            checkpoint: state.checkpoint,
            checkpoint_cursor: state.checkpoint_cursor,
            changes: state.tail.clone(),
            previous_history: state.previous_history,
        };
        history.validate(volume_id)?;
        Ok(history)
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        if self.major != FORMAT_MAJOR || self.volume_id != *volume_id.as_bytes() {
            return Err(corrupt("branch history identity is invalid"));
        }
        StoredNamespaceState {
            checkpoint: self.checkpoint,
            checkpoint_cursor: self.checkpoint_cursor,
            tail: self.changes.clone(),
            previous_history: self.previous_history,
        }
        .validate_shape()
    }

    pub(crate) fn state_at(&self, sequence: u64) -> Option<StoredNamespaceState> {
        StoredNamespaceState {
            checkpoint: self.checkpoint,
            checkpoint_cursor: self.checkpoint_cursor,
            tail: self.changes.clone(),
            previous_history: self.previous_history,
        }
        .at_sequence(sequence)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshot {
    cursor: StoredCursor,
    root: [u8; 16],
    nodes: Vec<StoredNode>,
    directories: Vec<StoredDirectory>,
    file_versions: Vec<StoredFileVersion>,
}

impl StoredSnapshot {
    fn from_snapshot(snapshot: &NamespaceSnapshot) -> Result<Self, ManagedError> {
        Ok(Self {
            cursor: snapshot.cursor.into(),
            root: *snapshot.root.as_bytes(),
            nodes: snapshot
                .nodes
                .values()
                .map(StoredNode::from_record)
                .collect::<Result<_, _>>()?,
            directories: snapshot
                .directories
                .values()
                .map(StoredDirectory::from_record)
                .collect::<Result<_, _>>()?,
            file_versions: snapshot
                .file_versions
                .values()
                .map(StoredFileVersion::from_record)
                .collect(),
        })
    }

    fn into_snapshot(self, volume_id: VolumeId) -> Result<NamespaceSnapshot, ManagedError> {
        Ok(NamespaceSnapshot {
            volume_id,
            cursor: self.cursor.decode()?,
            root: NodeId::from_bytes(self.root),
            nodes: collect_unique(
                self.nodes.into_iter().map(StoredNode::into_record),
                |node| node.id,
                "branch checkpoint contains duplicate nodes",
            )?,
            directories: collect_unique(
                self.directories
                    .into_iter()
                    .map(StoredDirectory::into_record),
                |directory| directory.node,
                "branch checkpoint contains duplicate directories",
            )?,
            file_versions: collect_unique(
                self.file_versions
                    .into_iter()
                    .map(StoredFileVersion::into_record),
                |version| version.id,
                "branch checkpoint contains duplicate file versions",
            )?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDelta {
    volume_id: [u8; 16],
    root: [u8; 16],
    expected_nodes: Vec<StoredNodePrecondition>,
    expected_directories: Vec<StoredDirectoryPrecondition>,
    put_nodes: Vec<StoredNode>,
    remove_nodes: Vec<[u8; 16]>,
    put_directories: Vec<StoredDirectory>,
    remove_directories: Vec<[u8; 16]>,
    put_file_versions: Vec<StoredFileVersion>,
    remove_file_versions: Vec<[u8; 32]>,
}

impl StoredDelta {
    fn from_change(change: NamespaceChange) -> Result<Self, ManagedError> {
        Ok(Self {
            volume_id: *change.volume_id.as_bytes(),
            root: *change.root.as_bytes(),
            expected_nodes: change
                .expected_nodes
                .iter()
                .map(StoredNodePrecondition::from_record)
                .collect::<Result<_, _>>()?,
            expected_directories: change
                .expected_directories
                .iter()
                .map(StoredDirectoryPrecondition::from_record)
                .collect::<Result<_, _>>()?,
            put_nodes: change
                .put_nodes
                .iter()
                .map(StoredNode::from_record)
                .collect::<Result<_, _>>()?,
            remove_nodes: change
                .remove_nodes
                .iter()
                .map(|id| *id.as_bytes())
                .collect(),
            put_directories: change
                .put_directories
                .iter()
                .map(StoredDirectory::from_record)
                .collect::<Result<_, _>>()?,
            remove_directories: change
                .remove_directories
                .iter()
                .map(|id| *id.as_bytes())
                .collect(),
            put_file_versions: change
                .put_file_versions
                .iter()
                .map(StoredFileVersion::from_record)
                .collect(),
            remove_file_versions: change
                .remove_file_versions
                .iter()
                .map(|id| *id.as_bytes())
                .collect(),
        })
    }

    fn into_change(
        self,
        operation: OperationId,
        parent: ChangeCursor,
        cursor: ChangeCursor,
    ) -> NamespaceChange {
        NamespaceChange {
            volume_id: VolumeId::from_bytes(self.volume_id),
            operation,
            parent,
            cursor,
            root: NodeId::from_bytes(self.root),
            expected_nodes: self
                .expected_nodes
                .into_iter()
                .map(StoredNodePrecondition::into_record)
                .collect(),
            expected_directories: self
                .expected_directories
                .into_iter()
                .map(StoredDirectoryPrecondition::into_record)
                .collect(),
            put_nodes: self
                .put_nodes
                .into_iter()
                .map(StoredNode::into_record)
                .collect(),
            remove_nodes: self
                .remove_nodes
                .into_iter()
                .map(NodeId::from_bytes)
                .collect(),
            put_directories: self
                .put_directories
                .into_iter()
                .map(StoredDirectory::into_record)
                .collect(),
            remove_directories: self
                .remove_directories
                .into_iter()
                .map(NodeId::from_bytes)
                .collect(),
            put_file_versions: self
                .put_file_versions
                .into_iter()
                .map(StoredFileVersion::into_record)
                .collect(),
            remove_file_versions: self
                .remove_file_versions
                .into_iter()
                .map(FileVersionId::from_bytes)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNode {
    id: [u8; 16],
    generation: u64,
    kind: StoredNodeKind,
    executable: bool,
    file_version: Option<[u8; 32]>,
}

impl StoredNode {
    fn from_record(record: &NodeRecord) -> Result<Self, ManagedError> {
        Ok(Self {
            id: *record.id.as_bytes(),
            generation: generation_number(&record.generation)?,
            kind: record.kind.into(),
            executable: record.attributes.executable,
            file_version: record.file_version.map(|id| *id.as_bytes()),
        })
    }

    fn into_record(self) -> NodeRecord {
        NodeRecord {
            id: NodeId::from_bytes(self.id),
            generation: managed_generation(self.generation),
            kind: self.kind.into(),
            attributes: crate::filesystem::NodeAttributes {
                executable: self.executable,
            },
            file_version: self.file_version.map(FileVersionId::from_bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredNodeKind {
    Directory,
    RegularFile,
}

impl From<NodeKind> for StoredNodeKind {
    fn from(value: NodeKind) -> Self {
        match value {
            NodeKind::Directory => Self::Directory,
            NodeKind::RegularFile => Self::RegularFile,
        }
    }
}

impl From<StoredNodeKind> for NodeKind {
    fn from(value: StoredNodeKind) -> Self {
        match value {
            StoredNodeKind::Directory => Self::Directory,
            StoredNodeKind::RegularFile => Self::RegularFile,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectory {
    node: [u8; 16],
    generation: u64,
    entries: BTreeMap<String, StoredDirectoryEntry>,
}

impl StoredDirectory {
    fn from_record(record: &DirectoryRecord) -> Result<Self, ManagedError> {
        Ok(Self {
            node: *record.node.as_bytes(),
            generation: generation_number(&record.generation)?,
            entries: record
                .entries
                .iter()
                .map(|(name, entry)| (name.clone(), (*entry).into()))
                .collect(),
        })
    }

    fn into_record(self) -> DirectoryRecord {
        DirectoryRecord {
            node: NodeId::from_bytes(self.node),
            generation: managed_generation(self.generation),
            entries: self
                .entries
                .into_iter()
                .map(|(name, entry)| (name, entry.into()))
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryEntry {
    node: [u8; 16],
    kind: StoredNodeKind,
}

impl From<DirectoryEntry> for StoredDirectoryEntry {
    fn from(value: DirectoryEntry) -> Self {
        Self {
            node: *value.node.as_bytes(),
            kind: value.kind.into(),
        }
    }
}

impl From<StoredDirectoryEntry> for DirectoryEntry {
    fn from(value: StoredDirectoryEntry) -> Self {
        Self {
            node: NodeId::from_bytes(value.node),
            kind: value.kind.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredFileVersion {
    id: [u8; 32],
    logical_size: u64,
    logical_digest: [u8; 32],
    extent_map: ExtentMap,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNodePrecondition {
    node: [u8; 16],
    expected_generation: Option<u64>,
}

impl StoredNodePrecondition {
    fn from_record(record: &NodePrecondition) -> Result<Self, ManagedError> {
        Ok(Self {
            node: *record.node.as_bytes(),
            expected_generation: record
                .expected_generation
                .as_ref()
                .map(generation_number)
                .transpose()?,
        })
    }

    fn into_record(self) -> NodePrecondition {
        NodePrecondition {
            node: NodeId::from_bytes(self.node),
            expected_generation: self.expected_generation.map(managed_generation),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryPrecondition {
    directory: [u8; 16],
    expected_generation: Option<u64>,
}

impl StoredDirectoryPrecondition {
    fn from_record(record: &DirectoryPrecondition) -> Result<Self, ManagedError> {
        Ok(Self {
            directory: *record.directory.as_bytes(),
            expected_generation: record
                .expected_generation
                .as_ref()
                .map(generation_number)
                .transpose()?,
        })
    }

    fn into_record(self) -> DirectoryPrecondition {
        DirectoryPrecondition {
            directory: NodeId::from_bytes(self.directory),
            expected_generation: self.expected_generation.map(managed_generation),
        }
    }
}

fn generation_number(generation: &crate::filesystem::Generation) -> Result<u64, ManagedError> {
    managed_generation_number(generation)
        .ok_or_else(|| corrupt("Managed generation in branch record is invalid"))
}

fn collect_unique<K, V>(
    values: impl IntoIterator<Item = V>,
    key: impl Fn(&V) -> K,
    message: &'static str,
) -> Result<BTreeMap<K, V>, ManagedError>
where
    K: Ord,
{
    let mut output = BTreeMap::new();
    for value in values {
        if output.insert(key(&value), value).is_some() {
            return Err(corrupt(message));
        }
    }
    Ok(output)
}

fn encode_value<T: Serialize>(value: &T) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| corrupt("branch value cannot be encoded"))?;
    Ok(bytes)
}

fn decode_value<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ManagedError> {
    let mut input = std::io::Cursor::new(bytes);
    let value = ciborium::de::from_reader(&mut input)
        .map_err(|_| corrupt("branch value cannot be decoded"))?;
    if input.position() != bytes.len() as u64 {
        return Err(corrupt("branch value has trailing bytes"));
    }
    Ok(value)
}

impl StoredFileVersion {
    fn from_record(record: &FileVersionRecord) -> Self {
        Self {
            id: *record.id.as_bytes(),
            logical_size: record.logical_size,
            logical_digest: record.logical_digest,
            extent_map: record.extent_map.clone(),
        }
    }

    fn into_record(self) -> FileVersionRecord {
        FileVersionRecord {
            id: FileVersionId::from_bytes(self.id),
            logical_size: self.logical_size,
            logical_digest: self.logical_digest,
            extent_map: self.extent_map,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredBranchHead {
    pub(crate) major: u16,
    pub(crate) volume_id: [u8; 16],
    pub(crate) branch_id: [u8; 16],
    pub(crate) lifecycle: BranchLifecycle,
    pub(crate) state: Option<StoredNamespaceState>,
    pub(crate) maintenance_epoch: u64,
    pub(crate) maintenance_active: bool,
}

impl StoredBranchHead {
    pub(crate) fn unborn(volume_id: VolumeId, branch_id: BranchId) -> Self {
        Self {
            major: FORMAT_MAJOR,
            volume_id: *volume_id.as_bytes(),
            branch_id: *branch_id.as_bytes(),
            lifecycle: BranchLifecycle::Active,
            state: None,
            maintenance_epoch: 0,
            maintenance_active: false,
        }
    }

    pub(crate) fn validate(
        &self,
        volume_id: VolumeId,
        branch_id: BranchId,
    ) -> Result<(), ManagedError> {
        if self.major != FORMAT_MAJOR
            || self.volume_id != *volume_id.as_bytes()
            || self.branch_id != *branch_id.as_bytes()
        {
            return Err(corrupt("branch HEAD identity is invalid"));
        }
        if self.maintenance_active && self.maintenance_epoch == 0 {
            return Err(corrupt("branch HEAD maintenance state is invalid"));
        }
        if let Some(state) = &self.state {
            state.validate_shape()?;
        }
        Ok(())
    }

    pub(crate) fn cursor(&self) -> Result<ChangeCursor, ManagedError> {
        self.state
            .as_ref()
            .map_or(Ok(ChangeCursor::Genesis), StoredNamespaceState::cursor)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredBranchRegistry {
    pub(crate) major: u16,
    pub(crate) volume_id: [u8; 16],
    pub(crate) default_branch: [u8; 16],
    pub(crate) branches: BTreeMap<BranchName, [u8; 16]>,
    pub(crate) maintenance_epoch: u64,
    pub(crate) maintenance_active: bool,
}

impl StoredBranchRegistry {
    pub(crate) fn initial(
        volume_id: VolumeId,
        default_name: BranchName,
        default_id: BranchId,
    ) -> Self {
        Self {
            major: FORMAT_MAJOR,
            volume_id: *volume_id.as_bytes(),
            default_branch: *default_id.as_bytes(),
            branches: BTreeMap::from([(default_name, *default_id.as_bytes())]),
            maintenance_epoch: 0,
            maintenance_active: false,
        }
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        let unique_ids = self.branches.values().copied().collect::<BTreeSet<_>>();
        if self.major != FORMAT_MAJOR
            || self.volume_id != *volume_id.as_bytes()
            || unique_ids.len() != self.branches.len()
            || !self
                .branches
                .values()
                .any(|branch| branch == &self.default_branch)
            || self.maintenance_active && self.maintenance_epoch == 0
        {
            return Err(corrupt("branch registry is invalid"));
        }
        Ok(())
    }

    pub(crate) fn branch_id(&self, name: &BranchName) -> Option<BranchId> {
        self.branches.get(name).copied().map(BranchId::from_bytes)
    }

    pub(crate) fn remove_if(&mut self, name: &BranchName, expected: BranchId) -> bool {
        if self.branch_id(name) != Some(expected) {
            return false;
        }
        self.branches.remove(name);
        true
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredCursor {
    pub(crate) sequence: u64,
    pub(crate) operation: Option<[u8; 16]>,
}

impl StoredCursor {
    pub(crate) fn decode(self) -> Result<ChangeCursor, ManagedError> {
        use std::num::NonZeroU64;

        match (self.sequence, self.operation) {
            (0, None) => Ok(ChangeCursor::Genesis),
            (sequence, Some(operation)) => Ok(ChangeCursor::at(
                NonZeroU64::new(sequence).ok_or_else(|| corrupt("branch cursor is invalid"))?,
                OperationId::from_bytes(operation),
            )),
            _ => Err(corrupt("branch cursor is invalid")),
        }
    }
}

impl From<ChangeCursor> for StoredCursor {
    fn from(cursor: ChangeCursor) -> Self {
        Self {
            sequence: cursor.sequence(),
            operation: cursor.operation().map(|operation| *operation.as_bytes()),
        }
    }
}

pub(crate) fn info(
    name: BranchName,
    id: BranchId,
    head: &StoredBranchHead,
    default: BranchId,
) -> Result<BranchInfo, ManagedError> {
    Ok(BranchInfo {
        binding: BranchBinding { name, id },
        lifecycle: head.lifecycle,
        cursor: head.cursor()?,
        is_default: id == default,
    })
}

fn corrupt(message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, "read Managed branch", message)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::filesystem::{NodeAttributes, NodeKind};

    fn cursor(sequence: u64, operation: OperationId) -> ChangeCursor {
        ChangeCursor::at(NonZeroU64::new(sequence).unwrap(), operation)
    }

    fn publication(
        volume: VolumeId,
        parent: ChangeCursor,
        operation: OperationId,
        root: NodeId,
    ) -> NamespacePublication {
        let initial = parent == ChangeCursor::Genesis;
        NamespacePublication {
            operation,
            parent,
            expected_nodes: initial
                .then_some(NodePrecondition {
                    node: root,
                    expected_generation: None,
                })
                .into_iter()
                .collect(),
            expected_directories: initial
                .then_some(DirectoryPrecondition {
                    directory: root,
                    expected_generation: None,
                })
                .into_iter()
                .collect(),
            target: NamespaceSnapshot {
                volume_id: volume,
                cursor: cursor(parent.sequence() + 1, operation),
                root,
                nodes: BTreeMap::from([(
                    root,
                    NodeRecord {
                        id: root,
                        generation: managed_generation(1),
                        kind: NodeKind::Directory,
                        attributes: NodeAttributes::default(),
                        file_version: None,
                    },
                )]),
                directories: BTreeMap::from([(
                    root,
                    DirectoryRecord {
                        node: root,
                        generation: managed_generation(1),
                        entries: BTreeMap::new(),
                    },
                )]),
                file_versions: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn rotated_history_recovers_every_retained_cursor() {
        let volume = VolumeId::from_bytes([1; 16]);
        let branch = BranchId::from_bytes([2; 16]);
        let root = NodeId::from_bytes([3; 16]);
        let first_operation = OperationId::from_bytes([4; 16]);
        let first = publication(volume, ChangeCursor::Genesis, first_operation, root);
        let (first_change, valid) = StoredChange::prepare(branch, &first, None).unwrap();
        assert!(valid);
        let first_result = StoredCommittedResult::from_change(&first_change).unwrap();
        let checkpoint = StoredCheckpoint::new(
            &first.target,
            BTreeMap::from([((branch, first_operation), first_result)]),
        )
        .unwrap();
        let mut state = StoredNamespaceState {
            checkpoint: [5; 32],
            checkpoint_cursor: first.target.cursor.into(),
            tail: Vec::new(),
            previous_history: None,
        };
        let mut snapshot = first.target;
        for byte in 6..=35 {
            let next = publication(
                volume,
                snapshot.cursor,
                OperationId::from_bytes([byte; 16]),
                root,
            );
            let (change, valid) = StoredChange::prepare(branch, &next, Some(&snapshot)).unwrap();
            assert!(valid);
            snapshot = next.target;
            state.tail.push(change);
        }
        let history = StoredHistory::new(volume, branch, &state).unwrap();

        for sequence in 1..=snapshot.cursor.sequence() {
            let retained = history.state_at(sequence).unwrap();
            let recovered = recover_namespace(checkpoint.clone(), &retained, volume).unwrap();
            assert_eq!(recovered.cursor.sequence(), sequence);
        }
    }

    #[test]
    fn committed_results_are_scoped_to_the_origin_branch() {
        let volume = VolumeId::from_bytes([7; 16]);
        let source = BranchId::from_bytes([8; 16]);
        let target = BranchId::from_bytes([9; 16]);
        let root = NodeId::from_bytes([10; 16]);
        let operation = OperationId::from_bytes([11; 16]);
        let publication = publication(volume, ChangeCursor::Genesis, operation, root);
        let (change, valid) = StoredChange::prepare(source, &publication, None).unwrap();
        assert!(valid);
        let checkpoint = StoredCheckpoint::new(
            &publication.target,
            BTreeMap::from([(
                (source, operation),
                StoredCommittedResult::from_change(&change).unwrap(),
            )]),
        )
        .unwrap();

        assert!(checkpoint.resolve(source, operation).unwrap().is_some());
        assert!(checkpoint.resolve(target, operation).unwrap().is_none());
    }

    #[test]
    fn deleting_an_old_incarnation_never_removes_a_recreated_name() {
        let volume = VolumeId::from_bytes([12; 16]);
        let main = BranchId::from_bytes([13; 16]);
        let old = BranchId::from_bytes([14; 16]);
        let replacement = BranchId::from_bytes([15; 16]);
        let main_name = BranchName::parse("main").unwrap();
        let name = BranchName::parse("work").unwrap();
        let mut registry = StoredBranchRegistry::initial(volume, main_name, main);
        registry
            .branches
            .insert(name.clone(), *replacement.as_bytes());

        assert!(!registry.remove_if(&name, old));
        assert_eq!(registry.branch_id(&name), Some(replacement));
    }

    #[test]
    fn one_branch_identity_cannot_be_registered_under_two_names() {
        let volume = VolumeId::from_bytes([16; 16]);
        let branch = BranchId::from_bytes([17; 16]);
        let mut registry =
            StoredBranchRegistry::initial(volume, BranchName::parse("main").unwrap(), branch);
        registry
            .branches
            .insert(BranchName::parse("alias").unwrap(), *branch.as_bytes());

        assert_eq!(
            registry.validate(volume).unwrap_err().kind(),
            ManagedErrorKind::Corrupt,
        );
    }

    #[test]
    fn retained_roots_include_each_diverged_branch() {
        let volume = VolumeId::from_bytes([18; 16]);
        let first_branch = BranchId::from_bytes([19; 16]);
        let second_branch = BranchId::from_bytes([20; 16]);
        let root = NodeId::from_bytes([21; 16]);
        let initial_operation = OperationId::from_bytes([22; 16]);
        let initial = publication(volume, ChangeCursor::Genesis, initial_operation, root);
        let (initial_change, _) = StoredChange::prepare(first_branch, &initial, None).unwrap();
        let checkpoint = StoredCheckpoint::new(
            &initial.target,
            BTreeMap::from([(
                (first_branch, initial_operation),
                StoredCommittedResult::from_change(&initial_change).unwrap(),
            )]),
        )
        .unwrap();
        let base = StoredNamespaceState {
            checkpoint: [23; 32],
            checkpoint_cursor: initial.target.cursor.into(),
            tail: Vec::new(),
            previous_history: None,
        };
        let first_publication = publication(
            volume,
            initial.target.cursor,
            OperationId::from_bytes([24; 16]),
            root,
        );
        let second_publication = publication(
            volume,
            initial.target.cursor,
            OperationId::from_bytes([25; 16]),
            root,
        );
        let (first_change, _) =
            StoredChange::prepare(first_branch, &first_publication, Some(&initial.target)).unwrap();
        let (second_change, _) =
            StoredChange::prepare(second_branch, &second_publication, Some(&initial.target))
                .unwrap();
        let mut first_state = base.clone();
        first_state.tail.push(first_change);
        let mut second_state = base;
        second_state.tail.push(second_change);

        let first_roots = recover_retained(checkpoint.clone(), &first_state, volume).unwrap();
        let second_roots = recover_retained(checkpoint, &second_state, volume).unwrap();
        assert_eq!(first_roots.len(), 2);
        assert_eq!(second_roots.len(), 2);
        assert_ne!(
            first_roots.last().unwrap().cursor,
            second_roots.last().unwrap().cursor
        );
    }
}
