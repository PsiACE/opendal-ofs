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
use crate::filesystem::{
    BranchId, ChangeCursor, OperationId, VolumeError, VolumeId, VolumeSnapshot,
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
