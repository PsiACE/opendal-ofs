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
use crate::filesystem::{BranchId, ChangeCursor, OperationId, VolumeError, VolumeId};
use crate::managed::error::{conflict, corrupt};

pub(crate) const MAX_TAIL_TRANSACTIONS: usize = 32;
pub(crate) const MAX_TAIL_BYTES: usize = 128 * 1024;

pub(crate) type StoredResults = BTreeMap<(Option<BranchId>, OperationId), StoredCommittedResult>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointRef {
    pub(crate) digest: [u8; 32],
    pub(crate) length: u64,
}

pub(crate) type StoredChange = NamespaceChange;

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
    pub(crate) tail: Vec<StoredChange>,
    pub(crate) previous_history: Option<[u8; 32]>,
}

impl StoredNamespaceState {
    pub(crate) fn cursor(&self) -> ChangeCursor {
        self.tail
            .last()
            .map_or(self.checkpoint_cursor, |change| change.cursor)
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        if self.tail.len() > MAX_TAIL_TRANSACTIONS {
            return Err(corrupt(
                "read Managed namespace",
                "namespace transaction tail exceeds its limit",
            ));
        }
        let mut parent = self.checkpoint_cursor;
        for change in &self.tail {
            change.validate(volume_id)?;
            if change.parent != parent {
                return Err(corrupt(
                    "read Managed namespace",
                    "namespace transaction tail is not consecutive",
                ));
            }
            parent = change.cursor;
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
    ) -> Result<(NamespaceSnapshot, StoredResults), VolumeError> {
        if self.snapshot.volume_id != volume_id {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint identity is invalid",
            ));
        }
        super::validate_snapshot(&self.snapshot)?;
        let mut results = BTreeMap::new();
        for result in self.results {
            result.validate()?;
            let key = (result.origin_branch, result.operation);
            if results.insert(key, result).is_some() {
                return Err(corrupt(
                    "read Managed namespace",
                    "checkpoint contains duplicate results",
                ));
            }
        }
        Ok((self.snapshot, results))
    }

    pub(crate) fn resolve(
        &self,
        origin: Option<BranchId>,
        operation: OperationId,
    ) -> Result<Option<&StoredCommittedResult>, VolumeError> {
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
    pub(crate) fn from_change(change: &StoredChange) -> Result<Self, VolumeError> {
        let result = Self {
            origin_branch: change.origin_branch,
            operation: change.operation,
            cursor: change.cursor,
            request_sha256: change.fingerprint()?.0,
        };
        result.validate()?;
        Ok(result)
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
pub(crate) struct StoredHistory {
    pub(crate) volume_id: VolumeId,
    pub(crate) state: StoredNamespaceState,
}

impl StoredHistory {
    pub(crate) fn new(
        volume_id: VolumeId,
        state: &StoredNamespaceState,
    ) -> Result<Self, VolumeError> {
        let history = Self {
            volume_id,
            state: state.clone(),
        };
        history.validate(volume_id)?;
        Ok(history)
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        if self.volume_id != volume_id {
            return Err(corrupt(
                "read Managed namespace",
                "namespace history identity is invalid",
            ));
        }
        self.state.validate(volume_id)
    }

    pub(crate) fn state_at(&self, sequence: u64) -> Option<StoredNamespaceState> {
        self.state.at_sequence(sequence)
    }
}

pub(crate) fn recover_namespace(
    checkpoint: StoredCheckpoint,
    state: &StoredNamespaceState,
    volume_id: VolumeId,
) -> Result<(NamespaceSnapshot, StoredResults), VolumeError> {
    let (mut snapshot, results) = checkpoint.recover(volume_id)?;
    if snapshot.cursor != state.checkpoint_cursor {
        return Err(corrupt(
            "read Managed namespace",
            "checkpoint and namespace HEAD disagree",
        ));
    }
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
    Ok((snapshot, results))
}

pub(crate) fn replay_tail_from(
    base: &NamespaceSnapshot,
    state: &StoredNamespaceState,
) -> Result<Option<NamespaceSnapshot>, VolumeError> {
    super::validate_snapshot(base)?;
    if base.cursor == state.cursor() {
        return Ok(Some(base.clone()));
    }
    let Some(start) = state
        .tail
        .iter()
        .position(|change| change.parent == base.cursor)
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

pub(crate) fn results_for_rotation(
    mut results: StoredResults,
    state: &StoredNamespaceState,
    committed: &StoredChange,
) -> Result<StoredResults, VolumeError> {
    for change in state.tail.iter().chain(std::iter::once(committed)) {
        let result = StoredCommittedResult::from_change(change)?;
        results.insert((result.origin_branch, result.operation), result);
    }
    Ok(results)
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
