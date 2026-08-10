// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! One namespace authority state machine over a bound revision-CAS HEAD.

use std::io::Cursor;

use futures::future::try_join_all;
use opendal::{ErrorKind, Operator};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{
    ChangeSegmentRef, CheckpointRef, NamespaceChange, StoredChangeSegment, StoredCommittedResult,
    StoredNamespaceState, recover_namespace, replay_tail_from, require_request_digest,
    validate_publication,
};
use crate::filesystem::{
    BranchBinding, BranchId, ChangeCursor, CommitOutcome, OperationId, VolumeError,
    VolumeErrorKind, VolumeId, VolumeSnapshot,
};
use crate::managed::error::{conflict, corrupt, invalid, unavailable};
use crate::managed::format::{LowerHex, V1Record};
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
const COMPRESSION_LEVEL: i32 = 3;

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
        let (request_digest, change_bytes) = change.fingerprint()?;
        let (head, revision, base) = match observed {
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
            None => {
                if self.read_raw_head().await?.is_some() {
                    return self.outcome_after_race(operation, request_digest).await;
                }
                (StoredHead::unborn(self.volume_id, None), None, None)
            }
        };
        if let Some(result) = head
            .state
            .as_ref()
            .and_then(|state| state.resolve(self.branch_id(), operation))
        {
            require_request_digest(Some(request_digest), result.request_sha256)?;
            return Ok(CommitOutcome::Committed(result.cursor));
        }
        let mut validated = validate_publication(&change, base)?;
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

        let state = match &head.state {
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
            Some(current) => {
                let tail_bytes = current.tail.iter().try_fold(0_usize, |total, change| {
                    change
                        .fingerprint()
                        .map(|(_, length)| total.saturating_add(length))
                })?;
                if current.tail.len() + 1 >= super::state::MAX_TAIL_TRANSACTIONS
                    || tail_bytes.saturating_add(change_bytes) > super::state::MAX_TAIL_BYTES
                {
                    let mut segments = current.segments.clone();
                    let mut archived = current.clone();
                    archived.tail.push(change.clone());
                    let segment = StoredChangeSegment::new(&archived);
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
                        try_join_all(results.iter().map(|result| self.write_operation(result)))
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
                        operation_prefixes: current.operation_prefixes.clone(),
                        outcome: current.outcome.clone(),
                    };
                    next.record_outcome(StoredCommittedResult::from_change(
                        &change,
                        request_digest,
                    ));
                    next
                } else {
                    let mut next = current.clone();
                    next.tail.push(change);
                    next.record_outcome(StoredCommittedResult::from_change(
                        next.tail.last().expect("change was appended above"),
                        request_digest,
                    ));
                    next
                }
            }
        };
        let mut next = head;
        if let Some(result) = next
            .state
            .as_ref()
            .filter(|state| !state.outcome_is_retained())
            .and_then(|state| state.outcome.as_ref())
        {
            self.write_operation(result).await?;
        }
        next.state = Some(state);
        let bytes = encode_head(&next)?;
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

    async fn resolve_known(
        &self,
        operation: OperationId,
        expected: Option<[u8; 32]>,
    ) -> Result<CommitOutcome, VolumeError> {
        let Some((head, _)) = self.read_bound_head("resolve Managed publication").await? else {
            return Ok(CommitOutcome::Absent);
        };
        let Some(state) = head.state else {
            return Ok(CommitOutcome::Absent);
        };
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
            &bytes,
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
        let resolved = match self.resolve_known(operation, Some(request_digest)).await {
            Err(error) if error.kind() == VolumeErrorKind::Unavailable => CommitOutcome::Unknown,
            result => result?,
        };
        match resolved {
            result @ (CommitOutcome::Committed(_) | CommitOutcome::Unknown) => Ok(result),
            _ => Ok(CommitOutcome::Conflict {
                observed: self
                    .observe(None)
                    .await?
                    .map_or(ChangeCursor::Genesis, |(snapshot, _)| snapshot.cursor),
            }),
        }
    }

    pub(crate) async fn read_checkpoint(
        &self,
        reference: CheckpointRef,
    ) -> Result<VolumeSnapshot, VolumeError> {
        let encoded_length = usize::try_from(reference.length)
            .ok()
            .filter(|length| *length <= MAX_CHECKPOINT_ENCODED_BYTES)
            .ok_or_else(|| {
                corrupt(
                    "read Managed namespace",
                    "checkpoint exceeds its encoded size limit",
                )
            })?;
        let key = checkpoint_key(reference.digest);
        let bytes = match self
            .data
            .read_with(&key)
            .range(0..reference.length)
            .content_length_hint(reference.length)
            .await
        {
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
        if bytes.len() != encoded_length
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != reference.digest
        {
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
            &bytes,
            "checkpoint Managed namespace",
        )
        .await?;
        Ok(reference)
    }

    async fn read_change_segment(
        &self,
        reference: ChangeSegmentRef,
    ) -> Result<StoredChangeSegment, VolumeError> {
        let encoded_length = usize::try_from(reference.length)
            .ok()
            .filter(|length| *length <= CHANGE_SEGMENT_RECORD.maximum_encoded_bytes())
            .ok_or_else(|| {
                corrupt(
                    "read Managed change segment",
                    "namespace change segment exceeds its size limit",
                )
            })?;
        let bytes = self
            .data
            .read_with(&change_segment_key(reference.digest))
            .range(0..reference.length)
            .content_length_hint(reference.length)
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
        if bytes.len() != encoded_length || Sha256::digest(&bytes).as_slice() != reference.digest {
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
        ensure_immutable(
            &self.data,
            &change_segment_key(digest),
            &bytes,
            "archive Managed changes",
        )
        .await?;
        Ok(ChangeSegmentRef {
            digest,
            length: bytes.len() as u64,
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
        super::validate_snapshot(&snapshot).map_err(|_| {
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
        let Some((bytes, revision)) = self
            .backend
            .read(
                &self.head_key,
                MAX_HEAD_ENCODED_BYTES,
                "read Managed namespace",
            )
            .await?
        else {
            return Ok(None);
        };
        let head = decode_head(&bytes)?;
        head.validate(self.volume_id, self.branch_id())?;
        Ok(Some((head, revision)))
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredHead {
    pub(crate) volume_id: VolumeId,
    pub(crate) branch_id: Option<BranchId>,
    pub(crate) sealed: bool,
    pub(crate) state: Option<StoredNamespaceState>,
}

impl StoredHead {
    pub(crate) const fn unborn(volume_id: VolumeId, branch_id: Option<BranchId>) -> Self {
        Self {
            volume_id,
            branch_id,
            sealed: false,
            state: None,
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
        }
        Ok(())
    }
}

fn encode_checkpoint(checkpoint: &VolumeSnapshot) -> Result<Vec<u8>, VolumeError> {
    let mut body = Vec::new();
    ciborium::into_writer(checkpoint, &mut body).map_err(|_| {
        invalid(
            "checkpoint Managed namespace",
            "checkpoint cannot be encoded",
        )
    })?;
    if body.len() > MAX_CHECKPOINT_DECODED_BYTES {
        return Err(invalid(
            "checkpoint Managed namespace",
            "checkpoint exceeds its decoded size limit",
        ));
    }
    let decoded_length = u64::try_from(body.len()).map_err(|_| {
        invalid(
            "checkpoint Managed namespace",
            "checkpoint exceeds its decoded size limit",
        )
    })?;
    let compressed = zstd::bulk::compress(&body, COMPRESSION_LEVEL).map_err(|_| {
        invalid(
            "checkpoint Managed namespace",
            "checkpoint cannot be compressed",
        )
    })?;
    let encoded_length = CHECKPOINT_MAGIC
        .len()
        .saturating_add(8)
        .saturating_add(compressed.len());
    if encoded_length > MAX_CHECKPOINT_ENCODED_BYTES {
        return Err(invalid(
            "checkpoint Managed namespace",
            "checkpoint exceeds its encoded size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(encoded_length);
    bytes.extend_from_slice(CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&decoded_length.to_be_bytes());
    bytes.extend_from_slice(&compressed);
    Ok(bytes)
}

fn decode_checkpoint(bytes: &[u8]) -> Result<VolumeSnapshot, VolumeError> {
    if bytes.len() > MAX_CHECKPOINT_ENCODED_BYTES {
        return Err(corrupt(
            "read Managed namespace",
            "checkpoint exceeds its encoded size limit",
        ));
    }
    let encoded = bytes
        .strip_prefix(CHECKPOINT_MAGIC)
        .ok_or_else(|| corrupt("read Managed namespace", "checkpoint format is invalid"))?;
    let (length, compressed) = encoded
        .split_first_chunk::<8>()
        .ok_or_else(|| corrupt("read Managed namespace", "checkpoint length is missing"))?;
    let decoded_length = usize::try_from(u64::from_be_bytes(*length))
        .ok()
        .filter(|length| *length <= MAX_CHECKPOINT_DECODED_BYTES)
        .ok_or_else(|| {
            corrupt(
                "read Managed namespace",
                "checkpoint decoded size exceeds its limit",
            )
        })?;
    let body = zstd::bulk::decompress(compressed, decoded_length).map_err(|_| {
        corrupt(
            "read Managed namespace",
            "checkpoint compression is invalid",
        )
    })?;
    if body.len() != decoded_length {
        return Err(corrupt(
            "read Managed namespace",
            "checkpoint decoded length does not match",
        ));
    }
    decode_value(&body)
}

pub(crate) fn encode_head(value: &StoredHead) -> Result<Vec<u8>, VolumeError> {
    let mut body = Vec::new();
    ciborium::into_writer(value, &mut body)
        .map_err(|_| invalid("write Managed namespace", "HEAD cannot be encoded"))?;
    if body.len() > MAX_HEAD_BYTES {
        return Err(invalid(
            "write Managed namespace",
            "HEAD exceeds its decoded size limit",
        ));
    }
    let decoded_length = u32::try_from(body.len()).map_err(|_| {
        invalid(
            "write Managed namespace",
            "HEAD exceeds its decoded size limit",
        )
    })?;
    let compressed = zstd::bulk::compress(&body, COMPRESSION_LEVEL)
        .map_err(|_| invalid("write Managed namespace", "HEAD cannot be compressed"))?;
    let mut bytes = Vec::with_capacity(12 + compressed.len() + 32);
    bytes.extend_from_slice(HEAD_MAGIC);
    bytes.extend_from_slice(&decoded_length.to_be_bytes());
    bytes.extend_from_slice(&compressed);
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    if bytes.len() > MAX_HEAD_ENCODED_BYTES {
        return Err(invalid(
            "write Managed namespace",
            "HEAD exceeds its encoded size limit",
        ));
    }
    Ok(bytes)
}

pub(crate) fn decode_head(bytes: &[u8]) -> Result<StoredHead, VolumeError> {
    if bytes.len() > MAX_HEAD_ENCODED_BYTES {
        return Err(corrupt(
            "read Managed namespace",
            "HEAD exceeds its encoded size limit",
        ));
    }
    let encoded = bytes
        .strip_prefix(HEAD_MAGIC)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| corrupt("read Managed namespace", "HEAD format is invalid"))?;
    if Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != &bytes[bytes.len() - 32..] {
        return Err(corrupt(
            "read Managed namespace",
            "HEAD checksum does not match",
        ));
    }
    let (length, compressed) = encoded
        .split_first_chunk::<4>()
        .ok_or_else(|| corrupt("read Managed namespace", "HEAD length is missing"))?;
    let decoded_length = u32::from_be_bytes(*length) as usize;
    if decoded_length > MAX_HEAD_BYTES {
        return Err(corrupt(
            "read Managed namespace",
            "HEAD decoded size exceeds its limit",
        ));
    }
    let body = zstd::bulk::decompress(compressed, decoded_length)
        .map_err(|_| corrupt("read Managed namespace", "HEAD compression is invalid"))?;
    if body.len() != decoded_length {
        return Err(corrupt(
            "read Managed namespace",
            "HEAD decoded length does not match",
        ));
    }
    decode_value(&body)
}

fn committed_result(change: &NamespaceChange) -> Result<StoredCommittedResult, VolumeError> {
    Ok(StoredCommittedResult::from_change(
        change,
        change.fingerprint()?.0,
    ))
}

fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, VolumeError> {
    let mut input = Cursor::new(bytes);
    let value = ciborium::from_reader(&mut input)
        .map_err(|_| corrupt("read Managed namespace", "record cannot be decoded"))?;
    if input.position() != bytes.len() as u64 {
        return Err(corrupt(
            "read Managed namespace",
            "record has trailing bytes",
        ));
    }
    Ok(value)
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
        let request_digest = latest.fingerprint().unwrap().0;
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
