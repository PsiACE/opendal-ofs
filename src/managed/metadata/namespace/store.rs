// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! One namespace authority state machine over a bound revision-CAS HEAD.

use super::{
    NamespaceChange, StoredCommittedResult, StoredNamespaceState, recover_namespace,
    replay_tail_from,
};
use crate::filesystem::{
    BranchBinding, BranchId, ChangeCursor, CommitOutcome, OperationId, VolumeError,
    VolumeErrorKind, VolumeId, VolumeSnapshot,
};
use crate::managed::error::{conflict, corrupt, invalid};
use crate::managed::format::{CompressedRecord, V1Record};
use crate::managed::metadata::record::{RecordBackend, Revision};
use opendal::Operator;
use serde::{Deserialize, Serialize};

mod gc;
mod history;
mod publish;
mod receipt;

pub(crate) use gc::{NamespaceGcSweep, RetainedMetadataReads};

#[cfg(test)]
use super::{CheckpointRef, StoredChangeSegment};
#[cfg(test)]
use history::{decode_checkpoint, encode_checkpoint};

const BASE_HEAD_KEY: &str = ".ofs/managed/metadata/v1/head.ofs";
const CHECKPOINT_ROOT: &str = ".ofs/managed/metadata/v1/checkpoints/blake3";
const CHANGE_SEGMENT_ROOT: &str = ".ofs/managed/metadata/v1/changes/blake3";
const OPERATION_ROOT: &str = ".ofs/managed/metadata/v1/operations";
const HEAD_MAGIC: &[u8; 8] = b"OFS1HDZ1";
const CHECKPOINT_MAGIC: &[u8; 8] = b"OFS1CKZ1";
const CHANGE_SEGMENT_RECORD: V1Record = V1Record::new(*b"OFS1CHG1", MAX_CHANGE_SEGMENT_BYTES);
const OPERATION_RECORD: V1Record = V1Record::new(*b"OFS1OPR1", 4096);
const MAX_HEAD_BYTES: usize = 256 * 1024;
pub(crate) const MAX_HEAD_ENCODED_BYTES: usize = MAX_HEAD_BYTES + 64 * 1024;
const MAX_CHECKPOINT_ENCODED_BYTES: usize = 256 * 1024 * 1024;
const MAX_CHECKPOINT_DECODED_BYTES: usize = 256 * 1024 * 1024;
const MAX_CHANGE_SEGMENT_BYTES: usize = 16 * 1024 * 1024;
const HEAD_RECORD: CompressedRecord =
    CompressedRecord::with_u32_length(*HEAD_MAGIC, MAX_HEAD_BYTES, MAX_HEAD_ENCODED_BYTES, true);
const CHECKPOINT_RECORD: CompressedRecord = CompressedRecord::with_u64_length(
    *CHECKPOINT_MAGIC,
    MAX_CHECKPOINT_DECODED_BYTES,
    MAX_CHECKPOINT_ENCODED_BYTES,
    false,
);

#[derive(Clone, Debug)]
pub(crate) struct NamespaceWitness {
    revision: Revision,
    head: StoredHead,
}

#[derive(Clone)]
pub(crate) struct NamespaceStore {
    volume_id: VolumeId,
    data: Operator,
    backend: RecordBackend,
    binding: Option<BranchBinding>,
    head_key: String,
}

impl NamespaceStore {
    pub(crate) fn new(volume_id: VolumeId, operator: Operator, backend: RecordBackend) -> Self {
        Self {
            volume_id,
            data: operator,
            backend,
            binding: None,
            head_key: BASE_HEAD_KEY.to_owned(),
        }
    }

    pub(crate) fn branch(
        volume_id: VolumeId,
        data: Operator,
        backend: RecordBackend,
        binding: BranchBinding,
        head_key: String,
    ) -> Self {
        Self {
            volume_id,
            data,
            backend,
            binding: Some(binding),
            head_key,
        }
    }

    pub(crate) fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub(crate) fn binding(&self) -> Option<&BranchBinding> {
        self.binding.as_ref()
    }

    fn branch_id(&self) -> Option<BranchId> {
        self.binding.as_ref().map(|binding| binding.id)
    }

    pub(crate) async fn observe(
        &self,
        base: Option<&VolumeSnapshot>,
    ) -> Result<Option<(VolumeSnapshot, NamespaceWitness)>, VolumeError> {
        let Some((head, revision)) = self.read_bound_head("read Managed namespace").await? else {
            return Ok(None);
        };
        let Some(state) = &head.state else {
            return Ok(None);
        };
        if let Some(base) = base.filter(|base| base.volume_id == self.volume_id) {
            if let Some(snapshot) = replay_tail_from(base, state)? {
                return Ok(Some((snapshot, NamespaceWitness { revision, head })));
            }
            if let Some(snapshot) = self.replay_retained_from(base, state).await? {
                return Ok(Some((snapshot, NamespaceWitness { revision, head })));
            }
        }
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        let snapshot = recover_namespace(checkpoint, state, self.volume_id)?;
        Ok(Some((snapshot, NamespaceWitness { revision, head })))
    }

    pub(crate) async fn resolve(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, VolumeError> {
        match self.resolve_known(operation, None).await {
            Err(error) if error.kind() == VolumeErrorKind::Unavailable => {
                Ok(CommitOutcome::Unknown)
            }
            result => result,
        }
    }

    pub(crate) async fn read_raw_head(
        &self,
    ) -> Result<Option<(StoredHead, Revision)>, VolumeError> {
        read_head_record(
            &self.backend,
            &self.head_key,
            self.volume_id,
            self.branch_id(),
            "read Managed namespace",
        )
        .await
    }

    async fn read_bound_head(
        &self,
        action: &'static str,
    ) -> Result<Option<(StoredHead, Revision)>, VolumeError> {
        let value = self.read_raw_head().await?;
        if self.branch_id().is_some() {
            let (head, _) = value
                .as_ref()
                .ok_or_else(|| conflict(action, "branch incarnation no longer exists"))?;
            if head.sealed {
                return Err(conflict(action, "branch incarnation no longer exists"));
            }
        }
        Ok(value)
    }
}

pub(crate) async fn read_head_record(
    backend: &RecordBackend,
    key: &str,
    volume_id: VolumeId,
    branch_id: Option<BranchId>,
    action: &'static str,
) -> Result<Option<(StoredHead, Revision)>, VolumeError> {
    let Some((bytes, revision)) = backend.read(key, MAX_HEAD_ENCODED_BYTES, action).await? else {
        return Ok(None);
    };
    let head = decode_head(&bytes)?;
    head.validate(volume_id, branch_id)?;
    Ok(Some((head, revision)))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredHead {
    pub(crate) volume_id: VolumeId,
    pub(crate) branch_id: Option<BranchId>,
    pub(crate) sealed: bool,
    pub(crate) state: Option<StoredNamespaceState>,
    pub(crate) maintenance_epoch: u64,
    pub(crate) maintenance: Option<NamespaceGcSweep>,
}

impl StoredHead {
    pub(crate) const fn unborn(volume_id: VolumeId, branch_id: Option<BranchId>) -> Self {
        Self {
            volume_id,
            branch_id,
            sealed: false,
            state: None,
            maintenance_epoch: 0,
            maintenance: None,
        }
    }

    pub(crate) fn cursor(&self) -> ChangeCursor {
        self.state
            .as_ref()
            .map_or(ChangeCursor::Genesis, StoredNamespaceState::cursor)
    }

    pub(crate) fn validate(
        &self,
        volume_id: VolumeId,
        branch_id: Option<BranchId>,
    ) -> Result<(), VolumeError> {
        if self.volume_id != volume_id || self.branch_id != branch_id {
            return Err(corrupt(
                "read Managed namespace",
                "HEAD identity is invalid",
            ));
        }
        if let Some(state) = &self.state {
            state.validate(volume_id)?;
            if state
                .outcome
                .as_ref()
                .is_some_and(|result| result.origin_branch != branch_id)
            {
                return Err(corrupt(
                    "read Managed namespace",
                    "HEAD outcome authority is invalid",
                ));
            }
        }
        if self.maintenance.as_ref().is_some_and(|maintenance| {
            maintenance.epoch == 0
                || maintenance.epoch != self.maintenance_epoch
                || maintenance.fixed_cursor != self.cursor()
        }) {
            return Err(corrupt(
                "read Managed namespace",
                "namespace maintenance fence is invalid",
            ));
        }
        Ok(())
    }
}

pub(crate) fn encode_head(value: &StoredHead) -> Result<Vec<u8>, VolumeError> {
    HEAD_RECORD
        .encode(value)
        .map_err(|error| invalid("write Managed namespace", error))
}

pub(crate) fn decode_head(bytes: &[u8]) -> Result<StoredHead, VolumeError> {
    HEAD_RECORD
        .decode(bytes)
        .map_err(|error| corrupt("read Managed namespace", error))
}

fn committed_result(change: &NamespaceChange) -> Result<StoredCommittedResult, VolumeError> {
    Ok(StoredCommittedResult::from_change(
        change,
        change.request_digest()?,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use opendal::services;

    use super::*;
    use crate::filesystem::{
        BranchName, DirectoryRecord, NodeAttributes, NodeId, NodeKind, NodeRecord,
        VolumePublication,
    };
    use crate::managed::metadata::namespace::managed_generation;
    use crate::managed::metadata::record::RecordBackend;

    fn checkpoint_snapshot(
        volume_id: VolumeId,
        cursor: ChangeCursor,
        root: NodeId,
    ) -> VolumeSnapshot {
        VolumeSnapshot {
            volume_id,
            cursor,
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
        }
    }

    #[test]
    fn checkpoint_identity_is_durable() {
        let volume_id = VolumeId::from_bytes([1; 16]);
        let operation = OperationId::from_bytes([2; 16]);
        let cursor = ChangeCursor::at(NonZeroU64::MIN, operation);
        let current = checkpoint_snapshot(volume_id, cursor, NodeId::from_bytes([3; 16]));
        let current_bytes = encode_checkpoint(&current).unwrap();
        assert_eq!(decode_checkpoint(&current_bytes).unwrap(), current);
        let mut corrupt = current_bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_checkpoint(&corrupt).unwrap_err().kind(),
            VolumeErrorKind::Corrupt
        );
    }

    #[test]
    fn head_identity_is_durable() {
        let head = StoredHead::unborn(VolumeId::from_bytes([1; 16]), None);
        let bytes = encode_head(&head).unwrap();
        assert_eq!(decode_head(&bytes).unwrap().volume_id, head.volume_id);
        let mut corrupt = bytes;
        *corrupt.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_head(&corrupt).unwrap_err().kind(),
            VolumeErrorKind::Corrupt
        );
    }

    #[tokio::test]
    async fn publication_rejects_another_branch_observation() {
        let operator = Operator::new(services::Memory::default()).unwrap().finish();
        let volume_id = VolumeId::from_bytes([4; 16]);
        let source = BranchId::from_bytes([5; 16]);
        let target = BranchId::from_bytes([6; 16]);
        let store = NamespaceStore::branch(
            volume_id,
            operator.clone(),
            RecordBackend::Object(operator),
            BranchBinding {
                name: BranchName::parse("target").unwrap(),
                id: target,
            },
            "target-head".to_owned(),
        );
        let first = OperationId::from_bytes([1; 16]);
        let base = checkpoint_snapshot(
            volume_id,
            ChangeCursor::at(NonZeroU64::MIN, first),
            NodeId::from_bytes([7; 16]),
        );
        let operation = OperationId::from_bytes([2; 16]);
        let mut next = base.clone();
        next.cursor = ChangeCursor::at(NonZeroU64::new(2).unwrap(), operation);
        let publication = VolumePublication::between(operation, Some(&base), next).unwrap();
        let change = NamespaceChange::new(publication.mutation().clone(), Some(target));
        let witness = NamespaceWitness {
            revision: Revision::Object("shared-etag".to_owned()),
            head: StoredHead::unborn(volume_id, Some(source)),
        };
        assert_eq!(
            store
                .publish(Some((&witness, &base)), change)
                .await
                .unwrap_err()
                .kind(),
            VolumeErrorKind::Invalid
        );
    }

    #[tokio::test]
    async fn retained_changes_catch_up_without_a_checkpoint() {
        let operator = Operator::new(services::Memory::default()).unwrap().finish();
        let volume_id = VolumeId::from_bytes([4; 16]);
        let root = NodeId::from_bytes([5; 16]);
        let backend = RecordBackend::Object(operator.clone());
        let store = NamespaceStore::new(volume_id, operator.clone(), backend);
        let first_operation = OperationId::from_bytes([1; 16]);
        let mut snapshot = checkpoint_snapshot(
            volume_id,
            ChangeCursor::at(NonZeroU64::MIN, first_operation),
            root,
        );
        let retained = snapshot.clone();
        let mut changes = Vec::new();
        for sequence in 2..=33 {
            let operation = OperationId::from_bytes([sequence as u8; 16]);
            let cursor = ChangeCursor::at(NonZeroU64::new(sequence).unwrap(), operation);
            let mut target = snapshot.clone();
            target.cursor = cursor;
            let publication =
                VolumePublication::between(operation, Some(&snapshot), target.clone()).unwrap();
            let change = NamespaceChange::new(publication.mutation().clone(), None);
            changes.push(change);
            snapshot = target;
        }
        let checkpoint = CheckpointRef {
            digest: [6; 32],
            length: 1,
        };
        let segment = StoredChangeSegment {
            checkpoint,
            changes,
        };
        let reference = store.write_change_segment(&segment).await.unwrap();
        let latest = segment.changes.last().unwrap();
        let request_digest = latest.request_digest().unwrap();
        let outcome = StoredCommittedResult::from_change(latest, request_digest);
        let mut state = StoredNamespaceState {
            checkpoint,
            checkpoint_cursor: reference.end,
            tail: Vec::new(),
            segments: vec![reference],
            operation_prefixes: StoredNamespaceState::empty_operation_index(),
            outcome: None,
        };
        state.record_outcome(outcome.clone());
        let caught_up = store
            .replay_retained_from(&retained, &state)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(caught_up.cursor.sequence(), 33);
        let resolved = state
            .resolve(None, OperationId::from_bytes([33; 16]))
            .unwrap();
        assert_eq!(resolved.cursor, outcome.cursor);
        assert_eq!(resolved.request_digest, outcome.request_digest);
    }
}
