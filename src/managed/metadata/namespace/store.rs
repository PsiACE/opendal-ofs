// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! One namespace authority state machine over a bound revision-CAS HEAD.

use futures::future::try_join_all;
use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

use super::{
    ChangeSegmentRef, CheckpointRef, NamespaceChange, StoredChangeSegment, StoredCommittedResult,
    StoredNamespaceState, recover_namespace, replay_tail_from, require_request_digest,
};
use crate::filesystem::{
    BranchBinding, BranchId, ChangeCursor, CommitOutcome, OperationId, VolumeError,
    VolumeErrorKind, VolumeId, VolumeSnapshot,
};
use crate::managed::data::RetainedDataRoots;
use crate::managed::error::{conflict, corrupt, invalid, unavailable};
use crate::managed::format::{CompressedRecord, LowerHex, V1Record};
use crate::managed::metadata::object::{ensure_immutable, read};
use crate::managed::metadata::record::{RecordBackend, Revision};

const BASE_HEAD_KEY: &str = ".ofs/managed/metadata/v1/head.ofs";
const CHECKPOINT_ROOT: &str = ".ofs/managed/metadata/v1/checkpoints/sha256";
const CHANGE_SEGMENT_ROOT: &str = ".ofs/managed/metadata/v1/changes/sha256";
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceGcSweep {
    pub(crate) epoch: u64,
    pub(crate) owner: [u8; 16],
    pub(crate) fixed_cursor: ChangeCursor,
}

#[derive(Default)]
pub(crate) struct RetainedMetadataReads {
    checkpoints: BTreeMap<[u8; 32], CheckpointRef>,
    changes: BTreeMap<[u8; 32], ChangeSegmentRef>,
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
        if let Some(base) = base
            && base.volume_id == self.volume_id
            && let Some(snapshot) = replay_tail_from(base, state)?
        {
            return Ok(Some((snapshot, NamespaceWitness { revision, head })));
        }
        if let Some(base) = base
            && base.volume_id == self.volume_id
            && let Some(snapshot) = self.replay_retained_from(base, state).await?
        {
            return Ok(Some((snapshot, NamespaceWitness { revision, head })));
        }
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        let snapshot = recover_namespace(checkpoint, state, self.volume_id)?;
        Ok(Some((snapshot, NamespaceWitness { revision, head })))
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<(&NamespaceWitness, &VolumeSnapshot)>,
        change: NamespaceChange,
    ) -> Result<CommitOutcome, VolumeError> {
        change.validate(self.volume_id).map_err(|_| {
            invalid(
                "publish Managed namespace",
                "publication belongs to another volume or has invalid ancestry",
            )
        })?;
        let operation = change.operation();
        let cursor = change.cursor();
        let request_digest = change.request_sha256()?;
        let change_bytes = change.encoded_len()?;
        let (mut head, revision, base) = match observed {
            Some((witness, snapshot)) => {
                if witness.head.volume_id != self.volume_id
                    || witness.head.branch_id != self.branch_id()
                {
                    return Err(invalid(
                        "publish Managed namespace",
                        "observation belongs to another authority",
                    ));
                }
                (
                    witness.head.clone(),
                    Some(witness.revision.clone()),
                    Some(snapshot),
                )
            }
            None if self.branch_id().is_some() => {
                let (head, revision) = self
                    .read_bound_head("publish Managed namespace")
                    .await?
                    .expect("a bound branch has a HEAD");
                if head.state.is_some() {
                    return self.outcome_after_race(operation, request_digest).await;
                }
                (head, Some(revision), None)
            }
            None => match self.read_raw_head().await? {
                Some((head, revision)) if head.state.is_none() => (head, Some(revision), None),
                Some(_) => return self.outcome_after_race(operation, request_digest).await,
                None => (StoredHead::unborn(self.volume_id, None), None, None),
            },
        };
        if head.maintenance.is_some() {
            return Ok(CommitOutcome::Conflict {
                observed: head.cursor(),
            });
        }
        if let Some(result) = head
            .state
            .as_ref()
            .and_then(|state| state.resolve(self.branch_id(), operation))
        {
            require_request_digest(Some(request_digest), result.request_sha256)?;
            return Ok(CommitOutcome::Committed(result.cursor));
        }
        let mut validated = change.validate_against(base).map_err(|_| {
            invalid(
                "publish Managed namespace",
                "publication mutation is invalid",
            )
        })?;
        if validated.is_none() {
            if matches!(
                self.resolve_known(operation, Some(request_digest)).await?,
                CommitOutcome::Committed(_)
            ) {
                return Ok(CommitOutcome::Committed(cursor));
            }
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            });
        }
        if head
            .state
            .as_ref()
            .is_some_and(|state| state.maybe_contains(operation))
            && let outcome = self.resolve_known(operation, Some(request_digest)).await?
            && let CommitOutcome::Committed(cursor) = outcome
        {
            return Ok(CommitOutcome::Committed(cursor));
        }

        let displaced_outcome = head
            .state
            .as_ref()
            .filter(|state| !state.outcome_is_retained())
            .and_then(|state| state.outcome.clone());
        let state = match head.state.take() {
            None => {
                let result = StoredCommittedResult::from_change(&change, request_digest);
                let target = change.apply_validated(
                    base.cloned(),
                    validated.take().expect("publication was validated above"),
                );
                let mut state = StoredNamespaceState {
                    checkpoint: self.write_checkpoint(&target).await?,
                    checkpoint_cursor: cursor,
                    tail: Vec::new(),
                    segments: Vec::new(),
                    operation_prefixes: StoredNamespaceState::empty_operation_index(),
                    outcome: None,
                };
                state.record_outcome(result);
                state
            }
            Some(mut current) => {
                let tail_bytes = current.tail.iter().try_fold(0_usize, |total, change| {
                    change
                        .encoded_len()
                        .map(|length| total.saturating_add(length))
                })?;
                if current.tail.len() + 1 >= super::state::MAX_TAIL_TRANSACTIONS
                    || tail_bytes.saturating_add(change_bytes) > super::state::MAX_TAIL_BYTES
                {
                    let mut segments = std::mem::take(&mut current.segments);
                    current.tail.push(change.clone());
                    let segment = StoredChangeSegment {
                        checkpoint: current.checkpoint,
                        changes: current.tail,
                    };
                    segments.push(self.write_change_segment(&segment).await?);
                    let excess = segments
                        .len()
                        .saturating_sub(super::state::MAX_CHANGE_SEGMENTS);
                    for reference in &segments[..excess] {
                        let segment = self.read_change_segment(*reference).await?;
                        let results = segment
                            .changes
                            .iter()
                            .map(committed_result)
                            .collect::<Result<Vec<_>, _>>()?;
                        try_join_all(
                            results
                                .iter()
                                .filter(|result| result.origin_branch == self.branch_id())
                                .map(|result| self.write_operation(result)),
                        )
                        .await?;
                    }
                    segments.drain(..excess);
                    let target = change.apply_validated(
                        base.cloned(),
                        validated.take().expect("publication was validated above"),
                    );
                    let mut next = StoredNamespaceState {
                        checkpoint: self.write_checkpoint(&target).await?,
                        checkpoint_cursor: cursor,
                        tail: Vec::new(),
                        segments,
                        operation_prefixes: current.operation_prefixes,
                        outcome: current.outcome,
                    };
                    next.record_outcome(StoredCommittedResult::from_change(
                        &change,
                        request_digest,
                    ));
                    next
                } else {
                    current.tail.push(change);
                    current.record_outcome(StoredCommittedResult::from_change(
                        current.tail.last().expect("change was appended above"),
                        request_digest,
                    ));
                    current
                }
            }
        };
        if let Some(result) = &displaced_outcome {
            self.write_operation(result).await?;
        }
        head.state = Some(state);
        let bytes = encode_head(&head)?;
        let replaced = match revision {
            Some(revision) => {
                self.backend
                    .replace(
                        &self.head_key,
                        &revision,
                        bytes,
                        "publish Managed namespace",
                    )
                    .await
            }
            None => {
                self.backend
                    .create(&self.head_key, bytes, "publish Managed namespace")
                    .await
            }
        };
        match replaced {
            Ok(true) => Ok(CommitOutcome::Committed(cursor)),
            Ok(false) => self.outcome_after_race(operation, request_digest).await,
            Err(_) => match self.resolve_known(operation, Some(request_digest)).await {
                Ok(CommitOutcome::Committed(cursor)) => Ok(CommitOutcome::Committed(cursor)),
                _ => Ok(CommitOutcome::Unknown),
            },
        }
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

    pub(crate) async fn begin_gc(
        &self,
        resume: bool,
    ) -> Result<(NamespaceGcSweep, Option<VolumeSnapshot>), VolumeError> {
        if self.branch_id().is_some() {
            return Err(invalid(
                "begin Managed data collection",
                "branch collection belongs to its volume control plane",
            ));
        }
        let current = self.read_raw_head().await?;
        let (mut head, revision) = current
            .map(|(head, revision)| (head, Some(revision)))
            .unwrap_or_else(|| (StoredHead::unborn(self.volume_id, None), None));
        let owner = *OperationId::generate().as_bytes();
        if resume {
            let maintenance = head.maintenance.as_mut().ok_or_else(|| {
                conflict(
                    "resume Managed data collection",
                    "no interrupted collection is active",
                )
            })?;
            maintenance.owner = owner;
        } else {
            if head.maintenance.is_some() {
                return Err(conflict(
                    "begin Managed data collection",
                    "another collection is active",
                ));
            }
            head.maintenance_epoch = head.maintenance_epoch.checked_add(1).ok_or_else(|| {
                corrupt(
                    "begin Managed data collection",
                    "maintenance epoch is exhausted",
                )
            })?;
            head.maintenance = Some(NamespaceGcSweep {
                epoch: head.maintenance_epoch,
                owner,
                fixed_cursor: head.cursor(),
            });
        }
        let sweep = head.maintenance.expect("collection was installed above");
        let bytes = encode_head(&head)?;
        let replaced = match revision {
            Some(revision) => {
                self.backend
                    .replace(
                        &self.head_key,
                        &revision,
                        bytes,
                        "begin Managed data collection",
                    )
                    .await?
            }
            None => {
                self.backend
                    .create(&self.head_key, bytes, "begin Managed data collection")
                    .await?
            }
        };
        if !replaced {
            return Err(conflict(
                "begin Managed data collection",
                "namespace authority changed",
            ));
        }
        let Some(state) = &head.state else {
            return Ok((sweep, None));
        };
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        recover_namespace(checkpoint, state, self.volume_id).map(|snapshot| (sweep, Some(snapshot)))
    }

    pub(crate) async fn retain_state_data(
        &self,
        state: &StoredNamespaceState,
        roots: &mut RetainedDataRoots,
        reads: &mut RetainedMetadataReads,
    ) -> Result<(), VolumeError> {
        self.retain_checkpoint(state.checkpoint, roots, reads)
            .await?;
        for change in &state.tail {
            for version in change
                .mutation
                .file_versions
                .iter()
                .filter_map(|change| change.target.as_ref())
            {
                roots.retain_file_version(version)?;
            }
        }
        for reference in &state.segments {
            if let Some(current) = reads.changes.get(&reference.digest) {
                if current != reference {
                    return Err(corrupt(
                        "mark retained data segments",
                        "one change-segment digest has conflicting references",
                    ));
                }
                continue;
            }
            reads.changes.insert(reference.digest, *reference);
            let segment = self.read_change_segment(*reference).await?;
            self.retain_checkpoint(segment.checkpoint, roots, reads)
                .await?;
            for change in segment.changes {
                for version in change
                    .mutation
                    .file_versions
                    .iter()
                    .filter_map(|change| change.target.as_ref())
                {
                    roots.retain_file_version(version)?;
                }
            }
        }
        Ok(())
    }

    async fn retain_checkpoint(
        &self,
        reference: CheckpointRef,
        roots: &mut RetainedDataRoots,
        reads: &mut RetainedMetadataReads,
    ) -> Result<(), VolumeError> {
        if let Some(current) = reads.checkpoints.get(&reference.digest) {
            return if *current == reference {
                Ok(())
            } else {
                Err(corrupt(
                    "mark retained data segments",
                    "one checkpoint digest has conflicting references",
                ))
            };
        }
        reads.checkpoints.insert(reference.digest, reference);
        roots.retain(&self.read_checkpoint(reference).await?)
    }

    pub(crate) async fn finish_gc(&self, sweep: NamespaceGcSweep) -> Result<(), VolumeError> {
        let (mut head, revision) = self.read_raw_head().await?.ok_or_else(|| {
            if self.branch_id().is_some() {
                corrupt(
                    "finish Managed data collection",
                    "registered branch HEAD is missing",
                )
            } else {
                conflict("finish Managed data collection", "namespace disappeared")
            }
        })?;
        if head.maintenance != Some(sweep) {
            return Err(conflict(
                "finish Managed data collection",
                if self.branch_id().is_some() {
                    "branch collection fence changed"
                } else {
                    "collection fence changed"
                },
            ));
        }
        head.maintenance = None;
        if self
            .backend
            .replace(
                &self.head_key,
                &revision,
                encode_head(&head)?,
                "finish Managed data collection",
            )
            .await?
        {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed data collection",
                if self.branch_id().is_some() {
                    "branch HEAD changed"
                } else {
                    "namespace authority changed"
                },
            ))
        }
    }

    async fn resolve_known(
        &self,
        operation: OperationId,
        expected: Option<[u8; 32]>,
    ) -> Result<CommitOutcome, VolumeError> {
        let Some((head, _)) = self.read_bound_head("resolve Managed publication").await? else {
            return Ok(CommitOutcome::Absent);
        };
        let Some(state) = &head.state else {
            return Ok(CommitOutcome::Absent);
        };
        self.resolve_from_state(state, operation, expected).await
    }

    async fn resolve_from_state(
        &self,
        state: &StoredNamespaceState,
        operation: OperationId,
        expected: Option<[u8; 32]>,
    ) -> Result<CommitOutcome, VolumeError> {
        if let Some(result) = state.resolve(self.branch_id(), operation) {
            require_request_digest(expected, result.request_sha256)?;
            return Ok(CommitOutcome::Committed(result.cursor));
        }
        if !state.maybe_contains(operation) {
            return Ok(CommitOutcome::Absent);
        }
        if let Some(result) = state
            .tail
            .iter()
            .rev()
            .find(|change| {
                change.origin_branch == self.branch_id() && change.operation() == operation
            })
            .map(committed_result)
            .transpose()?
        {
            require_request_digest(expected, result.request_sha256)?;
            return Ok(CommitOutcome::Committed(result.cursor));
        }
        let Some(result) = self.read_operation(operation).await? else {
            for reference in state.segments.iter().rev() {
                let segment = self.read_change_segment(*reference).await?;
                if let Some(result) = segment
                    .changes
                    .iter()
                    .rev()
                    .find(|change| {
                        change.origin_branch == self.branch_id() && change.operation() == operation
                    })
                    .map(committed_result)
                    .transpose()?
                {
                    require_request_digest(expected, result.request_sha256)?;
                    return Ok(CommitOutcome::Committed(result.cursor));
                }
            }
            return Ok(CommitOutcome::Absent);
        };
        require_request_digest(expected, result.request_sha256)?;
        Ok(CommitOutcome::Committed(result.cursor))
    }

    async fn read_operation(
        &self,
        operation: OperationId,
    ) -> Result<Option<StoredCommittedResult>, VolumeError> {
        let Some(bytes) = read(
            &self.data,
            &self.operation_key(operation),
            OPERATION_RECORD.maximum_encoded_bytes(),
            "resolve Managed publication",
        )
        .await?
        else {
            return Ok(None);
        };
        let result: StoredCommittedResult = OPERATION_RECORD
            .decode(&bytes)
            .map_err(|error| corrupt("resolve Managed publication", error.message()))?;
        result.validate()?;
        if result.origin_branch != self.branch_id() || result.operation != operation {
            return Err(corrupt(
                "resolve Managed publication",
                "operation receipt identity is invalid",
            ));
        }
        Ok(Some(result))
    }

    async fn write_operation(&self, result: &StoredCommittedResult) -> Result<(), VolumeError> {
        let bytes = OPERATION_RECORD
            .encode(result)
            .map_err(|error| invalid("record Managed publication", error.message()))?;
        ensure_immutable(
            &self.data,
            &self.operation_key(result.operation),
            bytes.into(),
            "record Managed publication",
        )
        .await
    }

    fn operation_key(&self, operation: OperationId) -> String {
        let scope = self.branch_id().map_or_else(
            || "base".to_owned(),
            |branch| LowerHex::encode(branch.as_bytes()),
        );
        format!(
            "{OPERATION_ROOT}/{scope}/{}.ofs",
            LowerHex::encode(operation.as_bytes())
        )
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
        request_digest: [u8; 32],
    ) -> Result<CommitOutcome, VolumeError> {
        let head = match self.read_bound_head("resolve Managed publication").await {
            Err(error) if error.kind() == VolumeErrorKind::Unavailable => {
                return Ok(CommitOutcome::Unknown);
            }
            result => result?,
        };
        let Some((head, _)) = head else {
            return Ok(CommitOutcome::Conflict {
                observed: ChangeCursor::Genesis,
            });
        };
        let observed = head.cursor();
        let resolved = match &head.state {
            Some(state) => {
                self.resolve_from_state(state, operation, Some(request_digest))
                    .await
            }
            None => Ok(CommitOutcome::Absent),
        };
        match resolved {
            Err(error) if error.kind() == VolumeErrorKind::Unavailable => {
                Ok(CommitOutcome::Unknown)
            }
            Err(error) => Err(error),
            Ok(result @ (CommitOutcome::Committed(_) | CommitOutcome::Unknown)) => Ok(result),
            Ok(_) => Ok(CommitOutcome::Conflict { observed }),
        }
    }

    pub(crate) async fn read_checkpoint(
        &self,
        reference: CheckpointRef,
    ) -> Result<VolumeSnapshot, VolumeError> {
        if reference.length > MAX_CHECKPOINT_ENCODED_BYTES as u64 {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint exceeds its encoded size limit",
            ));
        }
        let key = checkpoint_key(reference.digest);
        let bytes = match self.data.read_with(&key).range(0..reference.length).await {
            Ok(bytes) => bytes.to_bytes(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(corrupt("read Managed namespace", "checkpoint is missing"));
            }
            Err(_) => {
                return Err(unavailable(
                    "read Managed namespace",
                    "storage operation failed",
                ));
            }
        };
        if <[u8; 32]>::from(Sha256::digest(&bytes)) != reference.digest {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint identity is invalid",
            ));
        }
        decode_checkpoint(&bytes)
    }

    async fn write_checkpoint(
        &self,
        checkpoint: &VolumeSnapshot,
    ) -> Result<CheckpointRef, VolumeError> {
        let bytes = encode_checkpoint(checkpoint)?;
        let reference = CheckpointRef {
            digest: Sha256::digest(&bytes).into(),
            length: bytes.len() as u64,
        };
        ensure_immutable(
            &self.data,
            &checkpoint_key(reference.digest),
            bytes.into(),
            "checkpoint Managed namespace",
        )
        .await?;
        Ok(reference)
    }

    async fn read_change_segment(
        &self,
        reference: ChangeSegmentRef,
    ) -> Result<StoredChangeSegment, VolumeError> {
        if reference.length > CHANGE_SEGMENT_RECORD.maximum_encoded_bytes() as u64 {
            return Err(corrupt(
                "read Managed change segment",
                "namespace change segment exceeds its size limit",
            ));
        }
        let bytes = self
            .data
            .read_with(&change_segment_key(reference.digest))
            .range(0..reference.length)
            .await
            .map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    corrupt(
                        "read Managed change segment",
                        "namespace change segment is missing",
                    )
                } else {
                    unavailable("read Managed change segment", "storage operation failed")
                }
            })?
            .to_bytes();
        if Sha256::digest(&bytes).as_slice() != reference.digest {
            return Err(corrupt(
                "read Managed change segment",
                "namespace change segment identity is invalid",
            ));
        }
        let segment: StoredChangeSegment = CHANGE_SEGMENT_RECORD
            .decode(&bytes)
            .map_err(|error| corrupt("read Managed change segment", error.message()))?;
        segment.validate(self.volume_id)?;
        if segment.start() != reference.start || segment.cursor() != reference.end {
            return Err(corrupt(
                "read Managed change segment",
                "change segment disagrees with its index",
            ));
        }
        Ok(segment)
    }

    async fn write_change_segment(
        &self,
        segment: &StoredChangeSegment,
    ) -> Result<ChangeSegmentRef, VolumeError> {
        let bytes = CHANGE_SEGMENT_RECORD
            .encode(segment)
            .map_err(|error| invalid("write Managed change segment", error.message()))?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let length = bytes.len() as u64;
        ensure_immutable(
            &self.data,
            &change_segment_key(digest),
            bytes.into(),
            "archive Managed changes",
        )
        .await?;
        Ok(ChangeSegmentRef {
            digest,
            length,
            start: segment.start(),
            end: segment.cursor(),
        })
    }

    pub(crate) async fn state_at_sequence(
        &self,
        current: &StoredNamespaceState,
        sequence: u64,
    ) -> Result<Option<StoredNamespaceState>, VolumeError> {
        if let Some(state) = current.at_sequence(sequence) {
            return Ok(Some(state));
        }
        let Some((position, reference)) =
            current.segments.iter().enumerate().find(|(_, segment)| {
                segment.start.sequence() <= sequence && sequence <= segment.end.sequence()
            })
        else {
            return Ok(None);
        };
        let segment = self.read_change_segment(*reference).await?;
        let length = usize::try_from(sequence - segment.start().sequence())
            .ok()
            .filter(|length| *length <= segment.changes.len())
            .ok_or_else(|| {
                corrupt(
                    "read Managed change segment",
                    "requested sequence is not in the change segment",
                )
            })?;
        let outcome = current
            .outcome
            .clone()
            .filter(|result| result.cursor.sequence() <= sequence);
        Ok(Some(StoredNamespaceState {
            checkpoint: segment.checkpoint,
            checkpoint_cursor: segment.start(),
            tail: segment.changes[..length].to_vec(),
            segments: current.segments[..position].to_vec(),
            operation_prefixes: current.operation_prefixes.clone(),
            outcome,
        }))
    }

    async fn replay_retained_from(
        &self,
        base: &VolumeSnapshot,
        state: &StoredNamespaceState,
    ) -> Result<Option<VolumeSnapshot>, VolumeError> {
        let Some(position) = state.segments.iter().position(|segment| {
            segment.start.sequence() <= base.cursor.sequence()
                && base.cursor.sequence() <= segment.end.sequence()
        }) else {
            return Ok(None);
        };
        let mut snapshot = base.clone();
        for (offset, reference) in state.segments[position..].iter().enumerate() {
            let segment = self.read_change_segment(*reference).await?;
            let start = if snapshot.cursor == segment.start() {
                Some(0)
            } else if snapshot.cursor == segment.cursor() {
                Some(segment.changes.len())
            } else {
                segment
                    .changes
                    .iter()
                    .position(|change| change.parent() == snapshot.cursor)
            };
            let Some(start) = start else {
                return if offset == 0 {
                    Ok(None)
                } else {
                    Err(corrupt(
                        "read Managed change segment",
                        "retained change segments are not consecutive",
                    ))
                };
            };
            for change in &segment.changes[start..] {
                snapshot = change.apply(Some(snapshot))?;
            }
            if snapshot.cursor != reference.end {
                return Err(corrupt(
                    "read Managed change segment",
                    "change segment does not reach its indexed cursor",
                ));
            }
        }
        if snapshot.cursor != state.checkpoint_cursor {
            return Err(corrupt(
                "read Managed change segment",
                "retained changes do not reach the current checkpoint",
            ));
        }
        for change in &state.tail {
            snapshot = change.apply(Some(snapshot))?;
        }
        super::validate_snapshot_structure(&snapshot).map_err(|_| {
            corrupt(
                "read Managed change segment",
                "replayed namespace is invalid",
            )
        })?;
        Ok(Some(snapshot))
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

fn encode_checkpoint(checkpoint: &VolumeSnapshot) -> Result<Vec<u8>, VolumeError> {
    CHECKPOINT_RECORD
        .encode(checkpoint)
        .map_err(|error| invalid("checkpoint Managed namespace", error.message()))
}

fn decode_checkpoint(bytes: &[u8]) -> Result<VolumeSnapshot, VolumeError> {
    CHECKPOINT_RECORD
        .decode(bytes)
        .map_err(|error| corrupt("read Managed namespace", error.message()))
}

pub(crate) fn encode_head(value: &StoredHead) -> Result<Vec<u8>, VolumeError> {
    HEAD_RECORD
        .encode(value)
        .map_err(|error| invalid("write Managed namespace", error.message()))
}

pub(crate) fn decode_head(bytes: &[u8]) -> Result<StoredHead, VolumeError> {
    HEAD_RECORD
        .decode(bytes)
        .map_err(|error| corrupt("read Managed namespace", error.message()))
}

fn committed_result(change: &NamespaceChange) -> Result<StoredCommittedResult, VolumeError> {
    Ok(StoredCommittedResult::from_change(
        change,
        change.request_sha256()?,
    ))
}

pub(crate) fn checkpoint_key(id: [u8; 32]) -> String {
    format!("{CHECKPOINT_ROOT}/{}.ofs", LowerHex::encode(&id))
}

pub(crate) fn change_segment_key(id: [u8; 32]) -> String {
    format!("{CHANGE_SEGMENT_ROOT}/{}.ofs", LowerHex::encode(&id))
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
        let request_digest = latest.request_sha256().unwrap();
        let outcome = StoredCommittedResult::from_change(latest, request_digest);
        let mut state = StoredNamespaceState {
            checkpoint,
            checkpoint_cursor: segment.cursor(),
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
        assert_eq!(resolved.request_sha256, outcome.request_sha256);
    }
}
