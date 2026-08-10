// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Durable state shared by every Managed namespace authority.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::NamespaceChange;
use super::records::managed_generation_number;
use crate::filesystem::{
    BranchId, ChangeCursor, Generation, NodeKind, OperationId, VolumeError, VolumeId,
    VolumeSnapshot,
};
use crate::managed::error::{conflict, corrupt};

pub(crate) const MAX_TAIL_TRANSACTIONS: usize = 32;
pub(crate) const MAX_TAIL_BYTES: usize = 128 * 1024;
pub(crate) const MAX_CHANGE_SEGMENTS: usize = 8;
const OPERATION_PREFIX_WORDS: usize = 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointRef {
    pub(crate) digest: [u8; 32],
    pub(crate) length: u64,
}

impl CheckpointRef {
    pub(crate) fn from_encoded(bytes: &[u8]) -> Self {
        Self {
            digest: Sha256::digest(bytes).into(),
            length: bytes.len() as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChangeSegmentRef {
    pub(crate) digest: [u8; 32],
    pub(crate) length: u64,
    pub(crate) start: ChangeCursor,
    pub(crate) end: ChangeCursor,
}

impl NamespaceChange {
    pub(crate) fn request_sha256(&self) -> Result<[u8; 32], VolumeError> {
        let mut digest = Sha256::new();
        digest.update(b"OFS1REQ1");
        match self.origin_branch {
            None => digest.update([0]),
            Some(branch) => {
                digest.update([1]);
                digest.update(branch.as_bytes());
            }
        }
        let mutation = &self.mutation;
        digest.update(mutation.volume_id.as_bytes());
        digest.update(mutation.operation.as_bytes());
        hash_cursor(&mut digest, mutation.parent);
        hash_cursor(&mut digest, mutation.cursor);
        digest.update(mutation.root.as_bytes());

        hash_len(&mut digest, mutation.nodes.len())?;
        for change in &mutation.nodes {
            digest.update(change.node.as_bytes());
            hash_optional_generation(&mut digest, change.expected_generation.as_ref())?;
            match &change.target {
                None => digest.update([0]),
                Some(node) => {
                    digest.update([1]);
                    hash_generation(&mut digest, &node.generation)?;
                    hash_kind(&mut digest, node.kind);
                    digest.update([u8::from(node.attributes.executable)]);
                    match node.file_version {
                        None => digest.update([0]),
                        Some(version) => {
                            digest.update([1]);
                            digest.update(version.as_bytes());
                        }
                    }
                }
            }
        }
        hash_len(&mut digest, mutation.directories.len())?;
        for change in &mutation.directories {
            digest.update(change.directory.as_bytes());
            hash_optional_generation(&mut digest, change.expected_generation.as_ref())?;
            match &change.target {
                None => digest.update([0]),
                Some(directory) => {
                    digest.update([1]);
                    hash_generation(&mut digest, &directory.generation)?;
                    hash_len(&mut digest, directory.remove_entries.len())?;
                    for name in &directory.remove_entries {
                        hash_name(&mut digest, name)?;
                    }
                    hash_len(&mut digest, directory.put_entries.len())?;
                    for (name, entry) in &directory.put_entries {
                        hash_name(&mut digest, name)?;
                        digest.update(entry.node.as_bytes());
                        hash_kind(&mut digest, entry.kind);
                    }
                }
            }
        }
        hash_len(&mut digest, mutation.file_versions.len())?;
        for change in &mutation.file_versions {
            digest.update(change.version.as_bytes());
            digest.update([u8::from(change.target.is_some())]);
        }
        Ok(digest.finalize().into())
    }

    pub(crate) fn encoded_len(&self) -> Result<usize, VolumeError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(self, &mut bytes).map_err(|_| {
            corrupt(
                "read Managed namespace",
                "namespace change cannot be encoded",
            )
        })?;
        Ok(bytes.len())
    }
}

fn hash_len(digest: &mut Sha256, length: usize) -> Result<(), VolumeError> {
    let length = u64::try_from(length).map_err(|_| {
        corrupt(
            "read Managed transaction",
            "transaction request length overflows",
        )
    })?;
    digest.update(length.to_be_bytes());
    Ok(())
}

fn hash_cursor(digest: &mut Sha256, cursor: ChangeCursor) {
    match cursor {
        ChangeCursor::Genesis => digest.update([0]),
        ChangeCursor::At {
            sequence,
            operation,
        } => {
            digest.update([1]);
            digest.update(sequence.get().to_be_bytes());
            digest.update(operation.as_bytes());
        }
    }
}

fn hash_generation(digest: &mut Sha256, generation: &Generation) -> Result<(), VolumeError> {
    let generation = managed_generation_number(generation).ok_or_else(|| {
        corrupt(
            "read Managed transaction",
            "transaction generation is invalid",
        )
    })?;
    digest.update(generation.to_be_bytes());
    Ok(())
}

fn hash_optional_generation(
    digest: &mut Sha256,
    generation: Option<&Generation>,
) -> Result<(), VolumeError> {
    match generation {
        None => digest.update([0]),
        Some(generation) => {
            digest.update([1]);
            hash_generation(digest, generation)?;
        }
    }
    Ok(())
}

fn hash_kind(digest: &mut Sha256, kind: NodeKind) {
    digest.update([match kind {
        NodeKind::Directory => 0,
        NodeKind::RegularFile => 1,
    }]);
}

fn hash_name(digest: &mut Sha256, name: &str) -> Result<(), VolumeError> {
    hash_len(digest, name.len())?;
    digest.update(name.as_bytes());
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredNamespaceState {
    pub(crate) checkpoint: CheckpointRef,
    pub(crate) checkpoint_cursor: ChangeCursor,
    pub(crate) tail: Vec<NamespaceChange>,
    pub(crate) segments: Vec<ChangeSegmentRef>,
    pub(crate) operation_prefixes: Vec<u64>,
    pub(crate) outcome: Option<StoredCommittedResult>,
}

impl StoredNamespaceState {
    pub(crate) fn cursor(&self) -> ChangeCursor {
        self.tail
            .last()
            .map_or(self.checkpoint_cursor, NamespaceChange::cursor)
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        if self.tail.len() > MAX_TAIL_TRANSACTIONS
            || self.segments.len() > MAX_CHANGE_SEGMENTS
            || self.operation_prefixes.len() != OPERATION_PREFIX_WORDS
        {
            return Err(corrupt(
                "read Managed namespace",
                "namespace retained state exceeds its limit",
            ));
        }
        let mut previous = None;
        let mut segment_digests = BTreeSet::new();
        for segment in &self.segments {
            if !segment_digests.insert(segment.digest)
                || segment.start.sequence() >= segment.end.sequence()
                || previous.is_some_and(|end| end != segment.start)
            {
                return Err(corrupt(
                    "read Managed namespace",
                    "namespace change segment index is invalid",
                ));
            }
            previous = Some(segment.end);
        }
        if previous.is_some_and(|end| end != self.checkpoint_cursor) {
            return Err(corrupt(
                "read Managed namespace",
                "namespace change segments do not reach the checkpoint",
            ));
        }
        let cursor = validate_change_chain(&self.tail, volume_id, self.checkpoint_cursor)?;
        if let Some(result) = &self.outcome {
            if result.cursor != cursor
                || result
                    .operation()
                    .is_none_or(|operation| !self.maybe_contains(operation))
            {
                return Err(corrupt(
                    "read Managed namespace",
                    "namespace outcome does not describe HEAD",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn at_sequence(&self, sequence: u64) -> Option<Self> {
        let checkpoint = self.checkpoint_cursor.sequence();
        if sequence < checkpoint || sequence > self.cursor().sequence() {
            return None;
        }
        let mut state = self.clone();
        state.tail.truncate((sequence - checkpoint) as usize);
        if state
            .outcome
            .as_ref()
            .is_some_and(|result| result.cursor.sequence() > sequence)
        {
            state.outcome = None;
        }
        Some(state)
    }

    pub(crate) fn record_outcome(&mut self, result: StoredCommittedResult) {
        self.remember(
            result
                .operation()
                .expect("a committed result has an operation"),
        );
        self.outcome = Some(result);
    }

    pub(crate) fn reset_outcomes(&mut self) {
        self.operation_prefixes.fill(0);
        self.outcome = None;
    }

    pub(crate) fn empty_operation_index() -> Vec<u64> {
        vec![0; OPERATION_PREFIX_WORDS]
    }

    pub(crate) fn maybe_contains(&self, operation: OperationId) -> bool {
        let prefix = operation_prefix(operation);
        self.operation_prefixes[prefix / 64] & (1 << (prefix % 64)) != 0
    }

    pub(crate) fn outcome_is_retained(&self) -> bool {
        let Some(result) = &self.outcome else {
            return true;
        };
        !self.tail.is_empty()
            || self
                .segments
                .last()
                .is_some_and(|segment| segment.end == result.cursor)
    }

    fn remember(&mut self, operation: OperationId) {
        let prefix = operation_prefix(operation);
        self.operation_prefixes[prefix / 64] |= 1 << (prefix % 64);
    }

    pub(crate) fn resolve(
        &self,
        origin: Option<BranchId>,
        operation: OperationId,
    ) -> Option<&StoredCommittedResult> {
        self.outcome.as_ref().filter(|result| {
            result.origin_branch == origin && result.operation() == Some(operation)
        })
    }
}

fn operation_prefix(operation: OperationId) -> usize {
    u16::from_be_bytes(
        operation.as_bytes()[..2]
            .try_into()
            .expect("operation has 16 bytes"),
    ) as usize
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredCommittedResult {
    pub(crate) origin_branch: Option<BranchId>,
    pub(crate) cursor: ChangeCursor,
    pub(crate) request_sha256: [u8; 32],
}

impl StoredCommittedResult {
    pub(crate) const fn operation(&self) -> Option<OperationId> {
        self.cursor.operation()
    }

    pub(crate) fn from_change(change: &NamespaceChange, request_sha256: [u8; 32]) -> Self {
        Self {
            origin_branch: change.origin_branch,
            cursor: change.cursor(),
            request_sha256,
        }
    }
}

fn validate_change_chain(
    changes: &[NamespaceChange],
    volume_id: VolumeId,
    mut cursor: ChangeCursor,
) -> Result<ChangeCursor, VolumeError> {
    for change in changes {
        change.validate(volume_id)?;
        if change.parent() != cursor {
            return Err(corrupt(
                "read Managed namespace",
                "change chain is not consecutive",
            ));
        }
        cursor = change.cursor();
    }
    Ok(cursor)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredChangeSegment {
    pub(crate) checkpoint: CheckpointRef,
    pub(crate) changes: Vec<NamespaceChange>,
}

impl StoredChangeSegment {
    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        if self.changes.is_empty() || self.changes.len() > MAX_TAIL_TRANSACTIONS {
            return Err(corrupt(
                "read Managed change segment",
                "change segment size is invalid",
            ));
        }
        validate_change_chain(&self.changes, volume_id, self.changes[0].parent())?;
        Ok(())
    }

    pub(crate) fn reference(&self, digest: [u8; 32], length: u64) -> ChangeSegmentRef {
        ChangeSegmentRef {
            digest,
            length,
            start: self.changes[0].parent(),
            end: self.changes.last().expect("non-empty segment").cursor(),
        }
    }
}

pub(crate) fn recover_namespace(
    mut snapshot: VolumeSnapshot,
    state: &StoredNamespaceState,
    volume_id: VolumeId,
) -> Result<VolumeSnapshot, VolumeError> {
    if snapshot.volume_id != volume_id || snapshot.cursor != state.checkpoint_cursor {
        return Err(corrupt(
            "read Managed namespace",
            "checkpoint identity disagrees with namespace HEAD",
        ));
    }
    super::validate_snapshot(&snapshot)
        .map_err(|_| corrupt("read Managed namespace", "checkpoint namespace is invalid"))?;
    snapshot = apply_changes(snapshot, &state.tail)?;
    if !state.tail.is_empty() {
        super::validate_snapshot_structure(&snapshot)
            .map_err(|_| corrupt("read Managed namespace", "recovered namespace is invalid"))?;
    }
    Ok(snapshot)
}

pub(crate) fn replay_tail_from(
    base: &VolumeSnapshot,
    state: &StoredNamespaceState,
) -> Result<Option<VolumeSnapshot>, VolumeError> {
    super::validate_snapshot(base)?;
    if base.cursor == state.cursor() {
        return Ok(Some(base.clone()));
    }
    let Some(start) = state
        .tail
        .iter()
        .position(|change| change.parent() == base.cursor)
    else {
        return Ok(None);
    };
    let snapshot = apply_changes(base.clone(), &state.tail[start..])?;
    super::validate_snapshot_structure(&snapshot)
        .map_err(|_| corrupt("read Managed namespace", "recovered namespace is invalid"))?;
    Ok(Some(snapshot))
}

pub(super) fn apply_changes(
    mut snapshot: VolumeSnapshot,
    changes: &[NamespaceChange],
) -> Result<VolumeSnapshot, VolumeError> {
    for change in changes {
        snapshot = change.apply(Some(snapshot))?;
    }
    Ok(snapshot)
}

pub(crate) fn require_request_digest(
    expected: Option<[u8; 32]>,
    observed: [u8; 32],
) -> Result<(), VolumeError> {
    if expected.is_none_or(|expected| expected == observed) {
        Ok(())
    } else {
        Err(conflict(
            "publish Managed namespace",
            "operation identity was reused with another payload",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use super::*;
    use crate::filesystem::{
        DirectoryEntry, DirectoryRecord, NodeAttributes, NodeId, NodeRecord, VolumePublication,
    };
    use crate::managed::format::{ContentRef, Extent, ExtentMap, SegmentRef};
    use crate::managed::metadata::namespace::{
        DecodedFileVersion, encode_file_version, managed_generation,
    };

    #[test]
    fn operation_request_sha256_is_interoperable() {
        let volume = VolumeId::from_bytes([1; 16]);
        let branch = BranchId::from_bytes([2; 16]);
        let prior = OperationId::from_bytes([3; 16]);
        let operation = OperationId::from_bytes([4; 16]);
        let root = NodeId::from_bytes([5; 16]);
        let file = NodeId::from_bytes([6; 16]);
        let base = VolumeSnapshot {
            volume_id: volume,
            cursor: ChangeCursor::at(NonZeroU64::MIN, prior),
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
        };
        let decoded = DecodedFileVersion::from_extents(
            1,
            [7; 32],
            ExtentMap {
                extents: vec![Extent {
                    content: ContentRef {
                        digest: [7; 32],
                        length: 1,
                    },
                    segment: SegmentRef {
                        digest: [8; 32],
                        length: 11,
                    },
                    segment_offset: 4,
                }],
            },
        )
        .unwrap();
        let version = encode_file_version(&decoded).unwrap();
        let mut target = base.clone();
        target.cursor = ChangeCursor::at(NonZeroU64::new(2).unwrap(), operation);
        target.nodes.insert(
            file,
            NodeRecord {
                id: file,
                generation: managed_generation(1),
                kind: NodeKind::RegularFile,
                attributes: NodeAttributes { executable: true },
                file_version: Some(version.id),
            },
        );
        target.directories.insert(
            root,
            DirectoryRecord {
                node: root,
                generation: managed_generation(2),
                entries: BTreeMap::from([(
                    "δ.txt".to_owned(),
                    DirectoryEntry {
                        node: file,
                        kind: NodeKind::RegularFile,
                    },
                )]),
            },
        );
        target.file_versions.insert(version.id, version);
        let publication = VolumePublication::between(operation, Some(&base), target).unwrap();
        let change = NamespaceChange::new(publication.mutation().clone(), Some(branch));
        change.validate(volume).unwrap();

        assert_eq!(
            hex::encode(change.request_sha256().unwrap()),
            "627df8be759f08e34a61ffd1af19f5aedb3f50044639621181fad5bebaca088c"
        );
    }
}
