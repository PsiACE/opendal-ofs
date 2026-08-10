// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! One namespace authority state machine over a bound revision-CAS HEAD.

use std::collections::BTreeSet;
use std::io::Cursor;

use opendal::{ErrorKind, Operator};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{
    CheckpointRef, NamespacePublication, NamespaceSnapshot, StoredCheckpoint,
    StoredCommittedResult, StoredHistory, StoredNamespaceState, StoredResults, recover_namespace,
    replay_tail_from, require_request_digest, results_for_rotation, validate_publication,
};
use crate::filesystem::{
    BranchBinding, BranchId, ChangeCursor, CommitOutcome, OperationId, VolumeError,
    VolumeErrorKind, VolumeId,
};
use crate::managed::error::{conflict, corrupt, invalid, unavailable};
use crate::managed::format::{LowerHex, RecordDecodeError, RecordEncodeError, V1Record};
use crate::managed::metadata::object::ensure_immutable;
use crate::managed::metadata::object::read_content_addressed;
use crate::managed::metadata::record::{RecordBackend, Revision};

const BASE_HEAD_KEY: &str = ".ofs/managed/metadata/v1/head.ofs";
const CHECKPOINT_ROOT: &str = ".ofs/managed/metadata/v1/checkpoints/sha256";
const HISTORY_ROOT: &str = ".ofs/managed/metadata/v1/extensions/branch/v1/history/sha256";
const HEAD_MAGIC: &[u8; 8] = b"OFS1HDZ1";
const CHECKPOINT_MAGIC: &[u8; 8] = b"OFS1CKZ1";
const HISTORY_RECORD: V1Record = V1Record::new(*b"OFS1HST1", MAX_HISTORY_BYTES);
const MAX_HEAD_BYTES: usize = 256 * 1024;
const MAX_CHECKPOINT_ENCODED_BYTES: usize = 256 * 1024 * 1024;
const MAX_CHECKPOINT_DECODED_BYTES: usize = 256 * 1024 * 1024;
const MAX_HISTORY_BYTES: usize = 512 * 1024;
const COMPRESSION_LEVEL: i32 = 3;

#[derive(Clone, Debug)]
pub(crate) struct NamespaceObservation {
    pub snapshot: NamespaceSnapshot,
    witness: NamespaceWitness,
}

#[derive(Clone, Debug)]
pub(crate) struct NamespaceWitness {
    pub(crate) revision: Revision,
    pub(crate) head: StoredHead,
    checkpoint_results: Option<StoredResults>,
}

impl NamespaceObservation {
    pub(crate) fn into_parts(self) -> (NamespaceSnapshot, NamespaceWitness) {
        (self.snapshot, self.witness)
    }
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

    pub(crate) async fn observe(&self) -> Result<Option<NamespaceObservation>, VolumeError> {
        self.observe_from_optional(None).await
    }

    pub(crate) async fn observe_from(
        &self,
        base: &NamespaceSnapshot,
    ) -> Result<Option<NamespaceObservation>, VolumeError> {
        self.observe_from_optional(Some(base)).await
    }

    async fn observe_from_optional(
        &self,
        base: Option<&NamespaceSnapshot>,
    ) -> Result<Option<NamespaceObservation>, VolumeError> {
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
            return Ok(Some(NamespaceObservation {
                snapshot,
                witness: NamespaceWitness {
                    revision,
                    head,
                    checkpoint_results: None,
                },
            }));
        }
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        let (snapshot, checkpoint_results) = recover_namespace(checkpoint, state, self.volume_id)?;
        Ok(Some(NamespaceObservation {
            snapshot,
            witness: NamespaceWitness {
                revision,
                head,
                checkpoint_results: Some(checkpoint_results),
            },
        }))
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<(&NamespaceWitness, &NamespaceSnapshot)>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, VolumeError> {
        if publication.target.volume_id != self.volume_id {
            return Err(invalid(
                "publish Managed namespace",
                "publication belongs to another volume",
            ));
        }
        let (head, revision, base, checkpoint_results) = match observed {
            Some((witness, snapshot)) => (
                witness.head.clone(),
                Some(witness.revision.clone()),
                Some(snapshot),
                witness.checkpoint_results.clone(),
            ),
            None if self.branch_id().is_some() => {
                let (head, revision) = self
                    .read_bound_head("publish Managed namespace")
                    .await?
                    .expect("a bound branch has a HEAD");
                if head.state.is_some() {
                    return self.outcome_after_race(publication.operation).await;
                }
                (head, Some(revision), None, None)
            }
            None => {
                if self.read_raw_head().await?.is_some() {
                    return self.outcome_after_race(publication.operation).await;
                }
                (StoredHead::unborn(self.volume_id, None), None, None, None)
            }
        };
        let (valid, change) = validate_publication(publication, base, self.branch_id())?;
        let (request_digest, change_bytes) = change.fingerprint()?;
        if !valid {
            if matches!(
                self.resolve_known(publication.operation, Some(request_digest))
                    .await?,
                CommitOutcome::Committed(_)
            ) {
                return Ok(CommitOutcome::Committed(publication.target.cursor));
            }
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            });
        }

        let state = match (&head.state, checkpoint_results) {
            (None, None) => {
                let result = StoredCommittedResult::from_change(&change)?;
                let checkpoint = StoredCheckpoint {
                    snapshot: publication.target.clone(),
                    results: vec![result],
                };
                StoredNamespaceState {
                    checkpoint: self.write_checkpoint(&checkpoint).await?,
                    checkpoint_cursor: publication.target.cursor,
                    tail: Vec::new(),
                    previous_history: None,
                }
            }
            (Some(current), checkpoint_results) => {
                let tail_bytes = current.tail.iter().try_fold(0_usize, |total, change| {
                    change
                        .fingerprint()
                        .map(|(_, length)| total.saturating_add(length))
                })?;
                if current.tail.len() + 1 >= super::state::MAX_TAIL_TRANSACTIONS
                    || tail_bytes.saturating_add(change_bytes) > super::state::MAX_TAIL_BYTES
                {
                    let checkpoint_results = match checkpoint_results {
                        Some(results) => results,
                        None => {
                            self.read_checkpoint(current.checkpoint)
                                .await?
                                .recover(self.volume_id)?
                                .1
                        }
                    };
                    let previous_history = if self.branch_id().is_some() {
                        let history = StoredHistory::new(self.volume_id, current)?;
                        Some(self.write_history(&history).await?)
                    } else {
                        None
                    };
                    let results = results_for_rotation(checkpoint_results, current, &change)?;
                    let checkpoint = StoredCheckpoint {
                        snapshot: publication.target.clone(),
                        results: results.into_values().collect(),
                    };
                    StoredNamespaceState {
                        checkpoint: self.write_checkpoint(&checkpoint).await?,
                        checkpoint_cursor: publication.target.cursor,
                        tail: Vec::new(),
                        previous_history,
                    }
                } else {
                    let mut next = current.clone();
                    next.tail.push(change);
                    next
                }
            }
            _ => {
                return Err(corrupt(
                    "publish Managed namespace",
                    "observation and HEAD disagree",
                ));
            }
        };
        let mut next = head;
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
            Ok(true) => Ok(CommitOutcome::Committed(publication.target.cursor)),
            Ok(false) => self.outcome_after_race(publication.operation).await,
            Err(_) => match self.resolve(publication.operation).await {
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
        if let Some(change) = state.tail.iter().find(|change| {
            change.origin_branch == self.branch_id() && change.operation == operation
        }) {
            require_request_digest(expected, change.fingerprint()?.0)?;
            return Ok(CommitOutcome::Committed(change.cursor));
        }
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        let Some(result) = checkpoint.resolve(self.branch_id(), operation)? else {
            return Ok(CommitOutcome::Absent);
        };
        require_request_digest(expected, result.request_sha256)?;
        Ok(CommitOutcome::Committed(result.cursor))
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, VolumeError> {
        match self.resolve(operation).await? {
            result @ (CommitOutcome::Committed(_) | CommitOutcome::Unknown) => Ok(result),
            _ => Ok(CommitOutcome::Conflict {
                observed: self
                    .observe()
                    .await?
                    .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor),
            }),
        }
    }

    pub(crate) async fn read_checkpoint(
        &self,
        reference: CheckpointRef,
    ) -> Result<StoredCheckpoint, VolumeError> {
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
        let checkpoint = decode_checkpoint(&bytes)?;
        if checkpoint.snapshot.volume_id != self.volume_id {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint volume is invalid",
            ));
        }
        Ok(checkpoint)
    }

    async fn write_checkpoint(
        &self,
        checkpoint: &StoredCheckpoint,
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
            VolumeErrorKind::Corrupt,
            "immutable checkpoint changed",
        )
        .await?;
        Ok(reference)
    }

    pub(crate) async fn read_history(&self, id: [u8; 32]) -> Result<StoredHistory, VolumeError> {
        let bytes = read_content_addressed(
            &self.data,
            &history_key(id),
            &id,
            "read Managed history",
            "namespace history is missing",
            "namespace history identity is invalid",
        )
        .await?;
        let history: StoredHistory = HISTORY_RECORD
            .decode(&bytes)
            .map_err(history_decode_error)?;
        history.validate(self.volume_id)?;
        Ok(history)
    }

    async fn write_history(&self, history: &StoredHistory) -> Result<[u8; 32], VolumeError> {
        let bytes = HISTORY_RECORD
            .encode(history)
            .map_err(history_encode_error)?;
        let id: [u8; 32] = Sha256::digest(&bytes).into();
        ensure_immutable(
            &self.data,
            &history_key(id),
            &bytes,
            "archive Managed history",
            VolumeErrorKind::Corrupt,
            "immutable namespace history changed",
        )
        .await?;
        Ok(id)
    }

    pub(crate) async fn find_history_state(
        &self,
        mut history_id: Option<[u8; 32]>,
        sequence: u64,
    ) -> Result<Option<StoredNamespaceState>, VolumeError> {
        let mut visited = BTreeSet::new();
        while let Some(id) = history_id {
            if !visited.insert(id) {
                return Err(corrupt(
                    "read Managed history",
                    "namespace history contains a cycle",
                ));
            }
            let history = self.read_history(id).await?;
            if let Some(state) = history.state_at(sequence) {
                return Ok(Some(state));
            }
            history_id = history.state.previous_history;
        }
        Ok(None)
    }

    pub(crate) async fn read_raw_head(
        &self,
    ) -> Result<Option<(StoredHead, Revision)>, VolumeError> {
        let Some((bytes, revision)) = self
            .backend
            .read(&self.head_key, "read Managed namespace")
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

fn encode_checkpoint(checkpoint: &StoredCheckpoint) -> Result<Vec<u8>, VolumeError> {
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

fn decode_checkpoint(bytes: &[u8]) -> Result<StoredCheckpoint, VolumeError> {
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
    Ok(bytes)
}

pub(crate) fn decode_head(bytes: &[u8]) -> Result<StoredHead, VolumeError> {
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

fn history_encode_error(error: RecordEncodeError) -> VolumeError {
    match error {
        RecordEncodeError::Encode => invalid("write Managed history", "record cannot be encoded"),
        RecordEncodeError::TooLarge => {
            invalid("write Managed history", "record exceeds its size limit")
        }
    }
}

fn history_decode_error(error: RecordDecodeError) -> VolumeError {
    match error {
        RecordDecodeError::Envelope => corrupt("read Managed history", "record format is invalid"),
        RecordDecodeError::Checksum => {
            corrupt("read Managed history", "record checksum is invalid")
        }
        RecordDecodeError::Decode => corrupt("read Managed namespace", "record cannot be decoded"),
        RecordDecodeError::TrailingBytes => {
            corrupt("read Managed namespace", "record has trailing bytes")
        }
    }
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

pub(crate) fn history_key(id: [u8; 32]) -> String {
    format!("{HISTORY_ROOT}/{}.ofs", LowerHex::encode(&id))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use super::*;
    use crate::filesystem::{DirectoryRecord, NodeAttributes, NodeId, NodeKind, NodeRecord};
    use crate::managed::metadata::namespace::managed_generation;

    fn checkpoint_snapshot(
        volume_id: VolumeId,
        cursor: ChangeCursor,
        root: NodeId,
    ) -> NamespaceSnapshot {
        NamespaceSnapshot {
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
        let current_bytes = encode_checkpoint(&StoredCheckpoint {
            snapshot: current.clone(),
            results: Vec::new(),
        })
        .unwrap();
        assert_eq!(
            decode_checkpoint(&current_bytes)
                .unwrap()
                .recover(volume_id)
                .unwrap()
                .0,
            current
        );
        let mut corrupt = current_bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_checkpoint(&corrupt).unwrap_err().kind(),
            VolumeErrorKind::Corrupt
        );
    }
}
