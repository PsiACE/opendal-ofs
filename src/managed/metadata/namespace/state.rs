// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Durable state shared by every Managed namespace authority.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{NamespaceChange, NamespaceSnapshot};
use crate::filesystem::{BranchId, ChangeCursor, OperationId, VolumeId};
use crate::managed::{ManagedError, ManagedErrorKind};

pub(crate) const MAX_TAIL_TRANSACTIONS: usize = 32;
pub(crate) const MAX_TAIL_BYTES: usize = 128 * 1024;

pub(crate) type StoredResults = BTreeMap<(Option<BranchId>, OperationId), StoredCommittedResult>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointRef {
    pub(crate) digest: [u8; 32],
    pub(crate) length: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredChange {
    pub(crate) origin_branch: Option<BranchId>,
    pub(crate) change: NamespaceChange,
}

impl StoredChange {
    pub(crate) fn request_digest(&self) -> Result<[u8; 32], ManagedError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(self, &mut bytes)
            .map_err(|_| corrupt("namespace change cannot be encoded"))?;
        Ok(Sha256::digest(bytes).into())
    }

    pub(crate) fn encoded_len(&self) -> Result<usize, ManagedError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(self, &mut bytes)
            .map_err(|_| corrupt("namespace change cannot be encoded"))?;
        Ok(bytes.len())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredNamespaceState {
    pub(crate) checkpoint: CheckpointRef,
    pub(crate) checkpoint_cursor: ChangeCursor,
    pub(crate) tail: Vec<StoredChange>,
    pub(crate) previous_history: Option<[u8; 32]>,
}

impl StoredNamespaceState {
    pub(crate) fn cursor(&self) -> ChangeCursor {
        self.tail
            .last()
            .map_or(self.checkpoint_cursor, |change| change.change.cursor)
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        if self.tail.len() > MAX_TAIL_TRANSACTIONS {
            return Err(corrupt("namespace transaction tail exceeds its limit"));
        }
        let mut parent = self.checkpoint_cursor;
        for change in &self.tail {
            change.change.validate(volume_id)?;
            if change.change.parent != parent {
                return Err(corrupt("namespace transaction tail is not consecutive"));
            }
            parent = change.change.cursor;
        }
        Ok(())
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) fn at_sequence(&self, sequence: u64) -> Option<Self> {
        let checkpoint = self.checkpoint_cursor.sequence();
        if sequence < checkpoint || sequence > self.cursor().sequence() {
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
    pub(crate) snapshot: NamespaceSnapshot,
    pub(crate) results: Vec<StoredCommittedResult>,
}

impl StoredCheckpoint {
    pub(crate) fn recover(
        self,
        volume_id: VolumeId,
    ) -> Result<(NamespaceSnapshot, StoredResults), ManagedError> {
        if self.snapshot.volume_id != volume_id {
            return Err(corrupt("checkpoint identity is invalid"));
        }
        super::validate_snapshot(&self.snapshot)?;
        let mut results = BTreeMap::new();
        for result in self.results {
            result.validate()?;
            let key = (result.origin_branch, result.operation);
            if results.insert(key, result).is_some() {
                return Err(corrupt("checkpoint contains duplicate results"));
            }
        }
        Ok((self.snapshot, results))
    }

    pub(crate) fn resolve(
        &self,
        origin: Option<BranchId>,
        operation: OperationId,
    ) -> Result<Option<&StoredCommittedResult>, ManagedError> {
        self.results
            .iter()
            .find(|result| result.origin_branch == origin && result.operation == operation)
            .map(|result| result.validate().map(|()| result))
            .transpose()
    }
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
    pub(crate) fn from_change(change: &StoredChange) -> Result<Self, ManagedError> {
        let result = Self {
            origin_branch: change.origin_branch,
            operation: change.change.operation,
            cursor: change.change.cursor,
            request_sha256: change.request_digest()?,
        };
        result.validate()?;
        Ok(result)
    }

    pub(crate) fn validate(&self) -> Result<(), ManagedError> {
        if self.cursor.operation() != Some(self.operation) {
            return Err(corrupt("committed result cursor is invalid"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredHistory {
    pub(crate) volume_id: VolumeId,
    pub(crate) creator_branch: BranchId,
    pub(crate) state: StoredNamespaceState,
}

impl StoredHistory {
    pub(crate) fn new(
        volume_id: VolumeId,
        creator_branch: BranchId,
        state: &StoredNamespaceState,
    ) -> Result<Self, ManagedError> {
        let history = Self {
            volume_id,
            creator_branch,
            state: state.clone(),
        };
        history.validate(volume_id)?;
        Ok(history)
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        if self.volume_id != volume_id {
            return Err(corrupt("namespace history identity is invalid"));
        }
        self.state.validate(volume_id)
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) fn state_at(&self, sequence: u64) -> Option<StoredNamespaceState> {
        self.state.at_sequence(sequence)
    }
}

pub(crate) fn recover_namespace(
    checkpoint: StoredCheckpoint,
    state: &StoredNamespaceState,
    volume_id: VolumeId,
) -> Result<(NamespaceSnapshot, StoredResults), ManagedError> {
    let (mut snapshot, results) = checkpoint.recover(volume_id)?;
    if snapshot.cursor != state.checkpoint_cursor {
        return Err(corrupt("checkpoint and namespace HEAD disagree"));
    }
    for change in &state.tail {
        snapshot = change.change.apply(Some(snapshot))?;
    }
    if snapshot.cursor != state.cursor() {
        return Err(corrupt("transaction tail does not reach namespace HEAD"));
    }
    if !state.tail.is_empty() {
        super::validate_snapshot(&snapshot)
            .map_err(|_| corrupt("recovered namespace is invalid"))?;
    }
    Ok((snapshot, results))
}

pub(crate) fn replay_tail_from(
    base: &NamespaceSnapshot,
    state: &StoredNamespaceState,
) -> Result<Option<NamespaceSnapshot>, ManagedError> {
    super::validate_snapshot(base)?;
    if base.cursor == state.cursor() {
        return Ok(Some(base.clone()));
    }
    let Some(start) = state
        .tail
        .iter()
        .position(|change| change.change.parent == base.cursor)
    else {
        return Ok(None);
    };
    let mut snapshot = base.clone();
    for change in &state.tail[start..] {
        snapshot = change.change.apply(Some(snapshot))?;
    }
    if snapshot.cursor != state.cursor() {
        return Err(corrupt("transaction tail does not reach namespace HEAD"));
    }
    super::validate_snapshot(&snapshot).map_err(|_| corrupt("recovered namespace is invalid"))?;
    Ok(Some(snapshot))
}

pub(crate) fn results_for_rotation(
    mut results: StoredResults,
    state: &StoredNamespaceState,
    committed: &StoredChange,
) -> Result<StoredResults, ManagedError> {
    for change in state.tail.iter().chain(std::iter::once(committed)) {
        let result = StoredCommittedResult::from_change(change)?;
        results.insert((result.origin_branch, result.operation), result);
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
            "publish Managed namespace",
            "operation identity was reused with another payload",
        ))
    }
}

fn corrupt(message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, "read Managed namespace", message)
}
