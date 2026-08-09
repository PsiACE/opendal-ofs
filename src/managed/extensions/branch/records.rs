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

use crate::filesystem::{BranchBinding, BranchId, BranchName, ChangeCursor, OperationId, VolumeId};
use crate::managed::metadata::namespace::{
    NamespaceChange, NamespacePublication, NamespaceSnapshot, validate_publication,
    validate_snapshot,
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

/// One replayable shared namespace change, annotated only with its branch of
/// origin so receipts remain branch scoped.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredChange {
    pub(crate) origin_branch: BranchId,
    change: NamespaceChange,
}

impl StoredChange {
    pub(crate) fn prepare(
        branch: BranchId,
        publication: &NamespacePublication,
        base: Option<&NamespaceSnapshot>,
    ) -> Result<(Self, bool), ManagedError> {
        let valid = validate_publication(publication, base)?;
        Ok((
            Self {
                origin_branch: branch,
                change: NamespaceChange::from_publication(publication, base),
            },
            valid,
        ))
    }

    pub(crate) fn apply(
        &self,
        base: Option<NamespaceSnapshot>,
    ) -> Result<NamespaceSnapshot, ManagedError> {
        self.change
            .clone()
            .apply(base)
            .map_err(|_| corrupt("stored branch change is invalid"))
    }

    pub(crate) fn operation(&self) -> OperationId {
        self.change.operation
    }

    pub(crate) fn parent(&self) -> ChangeCursor {
        self.change.parent
    }

    pub(crate) fn cursor(&self) -> ChangeCursor {
        self.change.cursor
    }

    pub(crate) fn encoded_len(&self) -> Result<usize, ManagedError> {
        encode_value(self).map(|bytes| bytes.len())
    }

    pub(crate) fn request_digest(&self) -> Result<[u8; 32], ManagedError> {
        Ok(Sha256::digest(encode_value(self)?).into())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredNamespaceState {
    pub(crate) checkpoint: [u8; 32],
    pub(crate) checkpoint_cursor: ChangeCursor,
    pub(crate) tail: Vec<StoredChange>,
    pub(crate) previous_history: Option<[u8; 32]>,
}

impl StoredNamespaceState {
    pub(crate) fn cursor(&self) -> Result<ChangeCursor, ManagedError> {
        Ok(self
            .tail
            .last()
            .map_or(self.checkpoint_cursor, StoredChange::cursor))
    }

    pub(crate) fn validate_shape(&self) -> Result<(), ManagedError> {
        if self.tail.len() > MAX_TAIL_TRANSACTIONS
            || self.tail.iter().try_fold(0_usize, |total, change| {
                change
                    .encoded_len()
                    .map(|length| total.saturating_add(length))
            })? > MAX_TAIL_BYTES
        {
            return Err(corrupt("branch transaction tail exceeds its limit"));
        }
        let mut parent = self.checkpoint_cursor;
        for change in &self.tail {
            if change.parent() != parent {
                return Err(corrupt("branch transaction tail is not consecutive"));
            }
            let cursor = change.cursor();
            if cursor.operation() != Some(change.operation())
                || parent.sequence().checked_add(1) != Some(cursor.sequence())
            {
                return Err(corrupt("branch transaction cursor is invalid"));
            }
            parent = cursor;
        }
        Ok(())
    }

    pub(crate) fn at_sequence(&self, sequence: u64) -> Option<Self> {
        let checkpoint = self.checkpoint_cursor.sequence();
        if sequence < checkpoint || sequence > self.cursor().ok()?.sequence() {
            return None;
        }
        let mut state = self.clone();
        state.tail.truncate((sequence - checkpoint) as usize);
        Some(state)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StoredCheckpoint {
    pub(crate) snapshot: NamespaceSnapshot,
    pub(crate) results: Vec<StoredCommittedResult>,
}

impl StoredCheckpoint {
    pub(crate) fn new(
        snapshot: &NamespaceSnapshot,
        results: StoredResults,
    ) -> Result<Self, ManagedError> {
        validate_snapshot(snapshot)?;
        Ok(Self {
            snapshot: snapshot.clone(),
            results: results.into_values().collect(),
        })
    }

    pub(crate) fn recover(
        self,
        volume_id: VolumeId,
    ) -> Result<(NamespaceSnapshot, StoredResults), ManagedError> {
        if self.snapshot.volume_id != volume_id {
            return Err(corrupt("branch checkpoint identity is invalid"));
        }
        let snapshot = self.snapshot;
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
            .find(|result| result.origin_branch == branch && result.operation == operation)
            .map(|result| result.validate().map(|()| result))
            .transpose()
    }
}

pub(crate) fn recover_namespace(
    checkpoint: StoredCheckpoint,
    state: &StoredNamespaceState,
    volume_id: VolumeId,
) -> Result<(NamespaceSnapshot, StoredResults), ManagedError> {
    let (mut snapshot, results) = checkpoint.recover(volume_id)?;
    if snapshot.cursor != state.checkpoint_cursor {
        return Err(corrupt("branch checkpoint and HEAD disagree"));
    }
    for change in &state.tail {
        snapshot = change.apply(Some(snapshot))?;
    }
    if snapshot.cursor != state.cursor()? {
        return Err(corrupt("branch transaction tail does not reach HEAD"));
    }
    Ok((snapshot, results))
}

pub(crate) fn recover_retained(
    checkpoint: StoredCheckpoint,
    state: &StoredNamespaceState,
    volume_id: VolumeId,
) -> Result<Vec<NamespaceSnapshot>, ManagedError> {
    let (mut snapshot, _) = checkpoint.recover(volume_id)?;
    if snapshot.cursor != state.checkpoint_cursor {
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
    mut results: StoredResults,
    state: &StoredNamespaceState,
    committed: &StoredChange,
) -> Result<StoredResults, ManagedError> {
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
    pub(crate) origin_branch: BranchId,
    pub(crate) operation: OperationId,
    pub(crate) cursor: ChangeCursor,
    pub(crate) request_sha256: [u8; 32],
}

impl StoredCommittedResult {
    pub(crate) fn from_change(change: &StoredChange) -> Result<Self, ManagedError> {
        let result = Self {
            origin_branch: change.origin_branch,
            operation: change.operation(),
            cursor: change.cursor(),
            request_sha256: change.request_digest()?,
        };
        result.validate()?;
        Ok(result)
    }

    pub(crate) fn origin(&self) -> BranchId {
        self.origin_branch
    }

    pub(crate) fn operation(&self) -> OperationId {
        self.operation
    }

    pub(crate) fn validate(&self) -> Result<(), ManagedError> {
        if self.cursor.operation() != Some(self.operation) {
            return Err(corrupt("committed branch result cursor is invalid"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredHistory {
    pub(crate) major: u16,
    pub(crate) volume_id: VolumeId,
    pub(crate) creator_branch: BranchId,
    pub(crate) checkpoint: [u8; 32],
    pub(crate) checkpoint_cursor: ChangeCursor,
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
            volume_id,
            creator_branch: creator,
            checkpoint: state.checkpoint,
            checkpoint_cursor: state.checkpoint_cursor,
            changes: state.tail.clone(),
            previous_history: state.previous_history,
        };
        history.validate(volume_id)?;
        Ok(history)
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        if self.major != FORMAT_MAJOR || self.volume_id != volume_id {
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

fn encode_value<T: Serialize>(value: &T) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| corrupt("branch value cannot be encoded"))?;
    Ok(bytes)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredBranchHead {
    pub(crate) major: u16,
    pub(crate) volume_id: VolumeId,
    pub(crate) branch_id: BranchId,
    pub(crate) lifecycle: BranchLifecycle,
    pub(crate) state: Option<StoredNamespaceState>,
    pub(crate) maintenance_epoch: u64,
    pub(crate) maintenance_active: bool,
    #[serde(default)]
    pub(crate) maintenance_owner: Option<[u8; 16]>,
}

impl StoredBranchHead {
    pub(crate) fn unborn(volume_id: VolumeId, branch_id: BranchId) -> Self {
        Self {
            major: FORMAT_MAJOR,
            volume_id,
            branch_id,
            lifecycle: BranchLifecycle::Active,
            state: None,
            maintenance_epoch: 0,
            maintenance_active: false,
            maintenance_owner: None,
        }
    }

    pub(crate) fn validate(
        &self,
        volume_id: VolumeId,
        branch_id: BranchId,
    ) -> Result<(), ManagedError> {
        if self.major != FORMAT_MAJOR || self.volume_id != volume_id || self.branch_id != branch_id
        {
            return Err(corrupt("branch HEAD identity is invalid"));
        }
        if self.maintenance_active
            && (self.maintenance_epoch == 0 || self.maintenance_owner.is_none())
        {
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
    pub(crate) volume_id: VolumeId,
    pub(crate) default_branch: BranchId,
    pub(crate) branches: BTreeMap<BranchName, BranchId>,
    pub(crate) maintenance_epoch: u64,
    pub(crate) maintenance_active: bool,
    #[serde(default)]
    pub(crate) maintenance_owner: Option<[u8; 16]>,
}

impl StoredBranchRegistry {
    pub(crate) fn initial(
        volume_id: VolumeId,
        default_name: BranchName,
        default_id: BranchId,
    ) -> Self {
        Self {
            major: FORMAT_MAJOR,
            volume_id,
            default_branch: default_id,
            branches: BTreeMap::from([(default_name, default_id)]),
            maintenance_epoch: 0,
            maintenance_active: false,
            maintenance_owner: None,
        }
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        let unique_ids = self.branches.values().copied().collect::<BTreeSet<_>>();
        if self.major != FORMAT_MAJOR
            || self.volume_id != volume_id
            || unique_ids.len() != self.branches.len()
            || !self
                .branches
                .values()
                .any(|branch| branch == &self.default_branch)
            || self.maintenance_active
                && (self.maintenance_epoch == 0 || self.maintenance_owner.is_none())
        {
            return Err(corrupt("branch registry is invalid"));
        }
        Ok(())
    }

    pub(crate) fn branch_id(&self, name: &BranchName) -> Option<BranchId> {
        self.branches.get(name).copied()
    }

    pub(crate) fn remove_if(&mut self, name: &BranchName, expected: BranchId) -> bool {
        if self.branch_id(name) != Some(expected) {
            return false;
        }
        self.branches.remove(name);
        true
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
    use crate::filesystem::{
        DirectoryPrecondition, DirectoryRecord, NodeAttributes, NodeId, NodeKind, NodePrecondition,
        NodeRecord,
    };
    use crate::managed::metadata::namespace::managed_generation;

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
            checkpoint_cursor: first.target.cursor,
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
            let (recovered, _) = recover_namespace(checkpoint.clone(), &retained, volume).unwrap();
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
        registry.branches.insert(name.clone(), replacement);

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
            .insert(BranchName::parse("alias").unwrap(), branch);

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
            checkpoint_cursor: initial.target.cursor,
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
