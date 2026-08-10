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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChangeSegmentRef {
    pub(crate) digest: [u8; 32],
    pub(crate) length: u64,
    pub(crate) start: ChangeCursor,
    pub(crate) end: ChangeCursor,
}

impl NamespaceChange {
    pub(crate) fn fingerprint(&self) -> Result<([u8; 32], usize), VolumeError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(self, &mut bytes).map_err(|_| {
            corrupt(
                "read Managed namespace",
                "namespace change cannot be encoded",
            )
        })?;
        let length = bytes.len();
        Ok((Sha256::digest(bytes).into(), length))
    }
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
        let mut parent = self.checkpoint_cursor;
        for change in &self.tail {
            change.validate(volume_id)?;
            if change.parent() != parent {
                return Err(corrupt(
                    "read Managed namespace",
                    "namespace transaction tail is not consecutive",
                ));
            }
            parent = change.cursor();
        }
        let cursor = self.cursor();
        if let Some(result) = &self.outcome {
            result.validate()?;
            if result.cursor != cursor || !self.maybe_contains(result.operation) {
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
        self.remember(result.operation);
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
        self.outcome.as_ref().is_none_or(|result| {
            self.tail
                .iter()
                .any(|change| change.operation() == result.operation)
                || self
                    .segments
                    .iter()
                    .any(|segment| segment.end == result.cursor)
        })
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
        self.outcome
            .as_ref()
            .filter(|result| result.origin_branch == origin && result.operation == operation)
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
    pub(crate) operation: OperationId,
    pub(crate) cursor: ChangeCursor,
    pub(crate) request_sha256: [u8; 32],
}

impl StoredCommittedResult {
    pub(crate) fn from_change(change: &NamespaceChange, request_sha256: [u8; 32]) -> Self {
        Self {
            origin_branch: change.origin_branch,
            operation: change.operation(),
            cursor: change.cursor(),
            request_sha256,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), VolumeError> {
        if self.cursor.operation() != Some(self.operation) {
            return Err(corrupt(
                "read Managed namespace",
                "committed result cursor is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredChangeSegment {
    pub(crate) checkpoint: CheckpointRef,
    pub(crate) changes: Vec<NamespaceChange>,
}

impl StoredChangeSegment {
    pub(crate) fn new(state: &StoredNamespaceState) -> Self {
        Self {
            checkpoint: state.checkpoint,
            changes: state.tail.clone(),
        }
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        if self.changes.is_empty() || self.changes.len() > MAX_TAIL_TRANSACTIONS {
            return Err(corrupt(
                "read Managed change segment",
                "change segment size is invalid",
            ));
        }
        let mut parent = self.start();
        for change in &self.changes {
            change.validate(volume_id)?;
            if change.parent() != parent {
                return Err(corrupt(
                    "read Managed change segment",
                    "change segment is not consecutive",
                ));
            }
            parent = change.cursor();
        }
        Ok(())
    }

    pub(crate) fn cursor(&self) -> ChangeCursor {
        self.changes
            .last()
            .expect("validated change segments are non-empty")
            .cursor()
    }

    pub(crate) fn start(&self) -> ChangeCursor {
        self.changes
            .first()
            .expect("validated change segments are non-empty")
            .parent()
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
    super::validate_snapshot(&snapshot)?;
    for change in &state.tail {
        snapshot = change.apply(Some(snapshot))?;
    }
    if snapshot.cursor != state.cursor() {
        return Err(corrupt(
            "read Managed namespace",
            "transaction tail does not reach namespace HEAD",
        ));
    }
    if !state.tail.is_empty() {
        super::validate_snapshot(&snapshot)
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
    let mut snapshot = base.clone();
    for change in &state.tail[start..] {
        snapshot = change.apply(Some(snapshot))?;
    }
    if snapshot.cursor != state.cursor() {
        return Err(corrupt(
            "read Managed namespace",
            "transaction tail does not reach namespace HEAD",
        ));
    }
    super::validate_snapshot(&snapshot)
        .map_err(|_| corrupt("read Managed namespace", "recovered namespace is invalid"))?;
    Ok(Some(snapshot))
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
