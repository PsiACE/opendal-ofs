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

//! Backend-neutral namespace authority plus the Object Metadata constructor.

use std::collections::BTreeMap;
use std::io::Cursor;

use futures::{StreamExt as _, TryStreamExt as _, stream};
use opendal::Operator;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::change::NamespaceChange;
use super::validation::{validate_publication, validate_snapshot};
use super::{
    CheckpointPart, CheckpointRoot, NamespaceGcSweep, NamespacePublication, NamespaceSnapshot,
    PendingCheckpoint,
};
use crate::filesystem::{ChangeCursor, CommitOutcome, OperationId, VolumeId};
use crate::managed::metadata::object::{self, ensure_immutable, read_content_addressed};
use crate::managed::metadata::record::{ObjectRecordBackend, RecordBackend};
use crate::managed::{ManagedError, ManagedErrorKind};

#[cfg(test)]
use crate::filesystem::{
    DirectoryPrecondition, DirectoryRecord, NodeAttributes, NodeId, NodeKind, NodePrecondition,
    NodeRecord,
};

const HEAD_KEY: &str = ".ofs/managed/metadata/v1/head.ofs";
const CHECKPOINT_ROOT: &str = ".ofs/managed/metadata/v1/checkpoints/sha256";
const CHECKPOINT_PART_ROOT: &str = ".ofs/managed/metadata/v1/checkpoint-parts/sha256";
const HEAD_MAGIC: &[u8; 8] = b"OFS1HDZ1";
const FORMAT_MAJOR: u16 = 1;
const MAX_TAIL_TRANSACTIONS: u16 = 32;
const MAX_TAIL_BYTES: usize = 128 * 1024;
const MAX_HEAD_BYTES: usize = 256 * 1024;
const HEAD_COMPRESSION_LEVEL: i32 = 3;
const MAX_CHECKPOINT_READS: usize = 8;
const MAX_CHECKPOINT_WRITES: usize = 8;

#[derive(Clone, Debug)]
pub(crate) struct NamespaceObservation<R = String> {
    pub snapshot: NamespaceSnapshot,
    revision: R,
    authority: Box<ObservationAuthority>,
}

impl<R> NamespaceObservation<R> {
    pub(crate) fn gc_sweep(&self) -> Option<NamespaceGcSweep> {
        self.authority
            .head
            .gc_sweep()
            .expect("observed HEAD has valid maintenance state")
    }
}

#[derive(Clone)]
struct ObservationAuthority {
    head: StoredHead,
    checkpoint: Option<StoredCheckpoint>,
}

impl std::fmt::Debug for ObservationAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObservationAuthority")
            .field("head", &self.head)
            .field("checkpoint_loaded", &self.checkpoint.is_some())
            .field("tail_changes", &self.head.tail.len())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct NamespaceStore<B> {
    pub(crate) volume_id: VolumeId,
    pub(crate) operator: Operator,
    pub(crate) backend: B,
}

pub(crate) type ObjectNamespace = NamespaceStore<ObjectRecordBackend>;

impl NamespaceStore<ObjectRecordBackend> {
    pub(crate) fn new(volume_id: VolumeId, operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.read
            || !capability.write
            || !capability.write_with_if_not_exists
            || !capability.write_with_if_match
        {
            return Err(invalid(
                "open Managed namespace",
                "object metadata requires read, create-only write, and conditional replace",
            ));
        }
        Ok(Self {
            volume_id,
            operator: operator.clone(),
            backend: ObjectRecordBackend::new(operator),
        })
    }
}

#[allow(private_bounds)]
impl<B: RecordBackend> NamespaceStore<B> {
    pub(crate) async fn observe(
        &self,
    ) -> Result<Option<NamespaceObservation<B::Revision>>, ManagedError> {
        let Some((bytes, revision)) = self.read_head().await? else {
            return Ok(None);
        };
        let head = decode_head(&bytes)?;
        self.recover_observation(head, revision).await.map(Some)
    }

    pub(crate) async fn observe_from(
        &self,
        base: &NamespaceSnapshot,
    ) -> Result<Option<NamespaceObservation<B::Revision>>, ManagedError> {
        let Some((bytes, revision)) = self.read_head().await? else {
            return Ok(None);
        };
        let head = decode_head(&bytes)?;
        head.validate(self.volume_id)?;
        if base.volume_id == self.volume_id {
            validate_snapshot(base)?;
            if let Some(snapshot) = replay_tail_from(base, &head)? {
                return Ok(Some(NamespaceObservation {
                    snapshot,
                    revision,
                    authority: Box::new(ObservationAuthority {
                        head,
                        checkpoint: None,
                    }),
                }));
            }
        }
        self.recover_observation(head, revision).await.map(Some)
    }

    async fn recover_observation(
        &self,
        head: StoredHead,
        revision: B::Revision,
    ) -> Result<NamespaceObservation<B::Revision>, ManagedError> {
        let (snapshot, checkpoint) = self.recover(&head).await?;
        Ok(NamespaceObservation {
            snapshot,
            revision,
            authority: Box::new(ObservationAuthority {
                head,
                checkpoint: Some(checkpoint),
            }),
        })
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<&NamespaceObservation<B::Revision>>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        if publication.target.volume_id != self.volume_id {
            return Err(invalid(
                "publish Managed namespace",
                "publication belongs to another volume",
            ));
        }
        if observed.is_some_and(|value| value.gc_sweep().is_some()) {
            return Ok(CommitOutcome::Conflict {
                observed: observed.expect("checked above").snapshot.cursor,
            });
        }
        let base = observed.map(|value| &value.snapshot);
        let stored = NamespaceChange::from_publication(publication, base);
        let encoded_transaction = encode_table_value(&stored, "publish Managed namespace")?;
        let request_sha256 = sha256(&encoded_transaction);
        if !validate_publication(publication, base)? {
            if matches!(
                self.resolve_known(publication.operation, Some(request_sha256))
                    .await?,
                CommitOutcome::Committed(_)
            ) {
                return Ok(CommitOutcome::Committed(publication.target.cursor));
            }
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |state| state.cursor),
            });
        }

        let appended_tail_bytes = observed.map_or(0, |value| value.authority.head.tail_bytes())
            + encoded_transaction.len();
        let checkpoint_due = observed.is_none()
            || observed.is_some_and(|value| {
                value.authority.head.tail.len() + 1 >= usize::from(MAX_TAIL_TRANSACTIONS)
                    || appended_tail_bytes > MAX_TAIL_BYTES
            });
        let (checkpoint, checkpoint_cursor, tail) = if checkpoint_due {
            // The publication target and committed change tail are pinned by
            // the observation used for CAS. Building a checkpoint never rereads
            // the remote checkpoint or HEAD.
            let mut committed = match observed {
                Some(value) => {
                    let checkpoint = match &value.authority.checkpoint {
                        Some(checkpoint) => checkpoint.clone(),
                        None => {
                            self.read_checkpoint(value.authority.head.checkpoint)
                                .await?
                        }
                    };
                    checkpoint.results
                }
                None => BTreeMap::new(),
            };
            if let Some(observed) = observed {
                for transaction in &observed.authority.head.tail {
                    committed.insert(
                        transaction.operation,
                        StoredCommittedResult::from_transaction(transaction)?,
                    );
                }
            }
            committed.insert(
                publication.operation,
                StoredCommittedResult {
                    operation: publication.operation,
                    cursor: stored.cursor,
                    request_sha256,
                },
            );
            let checkpoint_id = self
                .write_checkpoint(&publication.target, &committed)
                .await?;
            (checkpoint_id, publication.target.cursor, Vec::new())
        } else {
            let observed = observed.expect("checkpoint policy has an observation");
            let mut tail = observed.authority.head.tail.clone();
            tail.push(stored.clone());
            (
                observed.authority.head.checkpoint,
                observed.authority.head.checkpoint_cursor,
                tail,
            )
        };
        let head = StoredHead::new(
            self.volume_id,
            stored.cursor,
            checkpoint,
            checkpoint_cursor,
            tail,
        )?
        .with_maintenance(
            observed.map_or(0, |value| value.authority.head.maintenance_epoch),
            observed.and_then(|value| value.authority.head.maintenance_owner),
        );
        let head = encode_head(&head)?;
        let replaced = match observed {
            Some(observed) => {
                self.backend
                    .replace(
                        HEAD_KEY,
                        &observed.revision,
                        head,
                        "publish Managed namespace",
                    )
                    .await
            }
            None => {
                self.backend
                    .create(HEAD_KEY, head, "publish Managed namespace")
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

    pub(crate) async fn begin_gc(
        &self,
        observed: &NamespaceObservation<B::Revision>,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        if observed.snapshot.volume_id != self.volume_id {
            return Err(invalid(
                "begin Managed namespace GC",
                "observation belongs to another volume",
            ));
        }
        if observed.gc_sweep().is_some() {
            return Err(conflict(
                "begin Managed namespace GC",
                "another namespace GC is active",
            ));
        }
        let mut head = observed.authority.head.clone();
        let sweep = head.begin_gc(*OperationId::generate().as_bytes())?;
        if self
            .replace_head(&observed.revision, encode_head(&head)?)
            .await?
        {
            return Ok(sweep);
        }
        Err(conflict(
            "begin Managed namespace GC",
            "namespace authority changed",
        ))
    }

    pub(crate) async fn resume_gc(
        &self,
        observed: &NamespaceObservation<B::Revision>,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        if observed.snapshot.volume_id != self.volume_id {
            return Err(invalid(
                "resume Managed namespace GC",
                "observation belongs to another volume",
            ));
        }
        let mut head = observed.authority.head.clone();
        let sweep = head.resume_gc(*OperationId::generate().as_bytes())?;
        if self
            .replace_head(&observed.revision, encode_head(&head)?)
            .await?
        {
            Ok(sweep)
        } else {
            Err(conflict(
                "resume Managed namespace GC",
                "namespace authority changed",
            ))
        }
    }

    pub(crate) async fn finish_gc(&self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        let observed = self.observe().await?.ok_or_else(|| {
            conflict("finish Managed namespace GC", "namespace authority changed")
        })?;
        if observed.authority.head.maintenance_epoch == sweep.epoch()
            && observed.gc_sweep().is_none()
        {
            return Ok(());
        }
        if observed.gc_sweep() != Some(sweep) {
            return Err(conflict(
                "finish Managed namespace GC",
                "GC sweep token does not match the authority",
            ));
        }
        let mut head = observed.authority.head.clone();
        head.finish_gc(sweep)?;
        if self
            .replace_head(&observed.revision, encode_head(&head)?)
            .await?
        {
            return Ok(());
        }
        let current = self.observe().await?.ok_or_else(|| {
            conflict("finish Managed namespace GC", "namespace authority changed")
        })?;
        if current.authority.head.maintenance_epoch == sweep.epoch() && current.gc_sweep().is_none()
        {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed namespace GC",
                "namespace authority changed",
            ))
        }
    }

    pub(crate) async fn resolve(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        match self.resolve_known(operation, None).await {
            Err(error) if error.kind() == ManagedErrorKind::Unavailable => {
                Ok(CommitOutcome::Unknown)
            }
            outcome => outcome,
        }
    }

    async fn resolve_known(
        &self,
        operation: OperationId,
        expected_sha256: Option<[u8; 32]>,
    ) -> Result<CommitOutcome, ManagedError> {
        let Some(head) = self.read_current_head().await? else {
            return Ok(CommitOutcome::Absent);
        };
        head.validate(self.volume_id)?;
        if let Some(transaction) = head
            .tail
            .iter()
            .find(|transaction| transaction.operation == operation)
        {
            let observed_sha256 = transaction_sha256(transaction, "resolve Managed publication")?;
            require_same_operation(expected_sha256, observed_sha256)?;
            return Ok(CommitOutcome::Committed(transaction.cursor));
        }
        let checkpoint = self.read_checkpoint(head.checkpoint).await?;
        if let Some(result) = checkpoint.results.get(&operation) {
            require_same_operation(expected_sha256, result.request_sha256)?;
            return Ok(CommitOutcome::Committed(result.cursor));
        }
        Ok(CommitOutcome::Absent)
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        let outcome = self.resolve(operation).await?;
        if matches!(
            outcome,
            CommitOutcome::Committed(_) | CommitOutcome::Unknown
        ) {
            return Ok(outcome);
        }
        let observed = self
            .observe()
            .await?
            .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor);
        Ok(CommitOutcome::Conflict { observed })
    }

    async fn recover(
        &self,
        head: &StoredHead,
    ) -> Result<(NamespaceSnapshot, StoredCheckpoint), ManagedError> {
        head.validate(self.volume_id)?;
        self.recover_bounded(head).await
    }

    async fn recover_bounded(
        &self,
        head: &StoredHead,
    ) -> Result<(NamespaceSnapshot, StoredCheckpoint), ManagedError> {
        let checkpoint = self.read_checkpoint(head.checkpoint).await?;
        if checkpoint.snapshot.volume_id != self.volume_id
            || checkpoint.snapshot.cursor != head.checkpoint_cursor
        {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint and HEAD disagree",
            ));
        }
        let mut snapshot = checkpoint.snapshot.clone();
        validate_snapshot(&snapshot)
            .map_err(|_| corrupt("read Managed namespace", "checkpoint is invalid"))?;

        for transaction in &head.tail {
            if transaction.parent != snapshot.cursor {
                return Err(corrupt(
                    "read Managed namespace",
                    "transaction tail is not consecutive",
                ));
            }
            snapshot = apply_transaction(Some(snapshot), transaction)?;
        }
        if snapshot.cursor != head.cursor {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint and transaction tail do not reach HEAD",
            ));
        }
        Ok((snapshot, checkpoint))
    }

    async fn write_checkpoint(
        &self,
        snapshot: &NamespaceSnapshot,
        results: &BTreeMap<OperationId, StoredCommittedResult>,
    ) -> Result<[u8; 32], ManagedError> {
        let results = results.values().cloned().collect::<Vec<_>>();
        let pending = PendingCheckpoint::new(snapshot, &results)?;
        stream::iter(&pending.parts)
            .map(Ok::<_, ManagedError>)
            .try_for_each_concurrent(MAX_CHECKPOINT_WRITES, |part| async move {
                ensure_immutable(
                    &self.operator,
                    &checkpoint_part_key(&part.reference.id),
                    &part.bytes,
                    "publish Managed namespace",
                    ManagedErrorKind::Conflict,
                    "operation identity was reused with another payload",
                )
                .await
            })
            .await?;
        let root = pending.finish();
        let bytes = root.encode()?;
        let id = sha256(&bytes);
        ensure_immutable(
            &self.operator,
            &checkpoint_key(&id),
            &bytes,
            "publish Managed namespace",
            ManagedErrorKind::Conflict,
            "operation identity was reused with another payload",
        )
        .await?;
        Ok(id)
    }

    async fn read_checkpoint(&self, id: [u8; 32]) -> Result<StoredCheckpoint, ManagedError> {
        let bytes = read_content_addressed(
            &self.operator,
            &checkpoint_key(&id),
            &id,
            "read Managed namespace",
            "checkpoint is missing",
            "checkpoint key and content disagree",
        )
        .await?;
        let root = CheckpointRoot::decode(&bytes)?;
        let parts = stream::iter(root.parts.iter().cloned())
            .map(|reference| async move {
                let bytes = object::read(
                    &self.operator,
                    &checkpoint_part_key(&reference.id),
                    "read Managed namespace",
                )
                .await?
                .ok_or_else(|| corrupt("read Managed namespace", "checkpoint part is missing"))?;
                Ok(CheckpointPart { reference, bytes })
            })
            .buffered(MAX_CHECKPOINT_READS)
            .try_collect()
            .await?;
        let (snapshot, results) = root.recover::<StoredCommittedResult>(parts)?;
        let mut indexed = BTreeMap::new();
        for result in results {
            result.validate(result.operation)?;
            if indexed.insert(result.operation, result).is_some() {
                return Err(corrupt(
                    "read Managed operation results",
                    "duplicate operation result",
                ));
            }
        }
        Ok(StoredCheckpoint {
            snapshot,
            results: indexed,
        })
    }

    async fn read_head(&self) -> Result<Option<(Vec<u8>, B::Revision)>, ManagedError> {
        self.backend.read(HEAD_KEY, "read Managed namespace").await
    }

    async fn read_current_head(&self) -> Result<Option<StoredHead>, ManagedError> {
        self.backend
            .read_bytes(HEAD_KEY, "read Managed namespace")
            .await?
            .map(|bytes| decode_head(&bytes))
            .transpose()
    }

    async fn replace_head(
        &self,
        expected_revision: &B::Revision,
        bytes: Vec<u8>,
    ) -> Result<bool, ManagedError> {
        self.backend
            .replace(
                HEAD_KEY,
                expected_revision,
                bytes,
                "publish Managed namespace",
            )
            .await
    }
}

fn checkpoint_key(id: &[u8; 32]) -> String {
    format!("{CHECKPOINT_ROOT}/{}.ofs", hex(id))
}

fn checkpoint_part_key(id: &[u8; 32]) -> String {
    let encoded = hex(id);
    format!("{CHECKPOINT_PART_ROOT}/{encoded}.ofs")
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn encode_head(value: &StoredHead) -> Result<Vec<u8>, ManagedError> {
    let body = encode_table_value(value, "write Managed namespace")?;
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
    let compressed = zstd::bulk::compress(&body, HEAD_COMPRESSION_LEVEL)
        .map_err(|_| invalid("write Managed namespace", "HEAD cannot be compressed"))?;
    let mut bytes = Vec::with_capacity(12 + compressed.len() + 32);
    bytes.extend_from_slice(HEAD_MAGIC);
    bytes.extend_from_slice(&decoded_length.to_be_bytes());
    bytes.extend_from_slice(&compressed);
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

fn decode_head(bytes: &[u8]) -> Result<StoredHead, ManagedError> {
    let encoded = bytes
        .strip_prefix(HEAD_MAGIC)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| corrupt("read Managed namespace", "HEAD format is invalid"))?;
    let expected = bytes
        .get(bytes.len().saturating_sub(32)..)
        .ok_or_else(|| corrupt("read Managed namespace", "HEAD checksum is missing"))?;
    if Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != expected {
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
    decode_table_value(&body, "read Managed namespace")
}

fn encode_table_value<T: Serialize>(
    value: &T,
    action: &'static str,
) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| invalid(action, "namespace change cannot be encoded"))?;
    Ok(bytes)
}

fn decode_table_value<T: DeserializeOwned>(
    bytes: &[u8],
    action: &'static str,
) -> Result<T, ManagedError> {
    let mut input = Cursor::new(bytes);
    let value = ciborium::de::from_reader(&mut input)
        .map_err(|_| corrupt(action, "namespace change cannot be decoded"))?;
    if input.position() != bytes.len() as u64 {
        return Err(corrupt(action, "namespace change has trailing bytes"));
    }
    Ok(value)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn require_same_operation(
    expected: Option<[u8; 32]>,
    observed: [u8; 32],
) -> Result<(), ManagedError> {
    if expected.is_none_or(|expected| expected == observed) {
        Ok(())
    } else {
        Err(conflict(
            "publish Managed namespace",
            "operation identity was reused with another payload",
        ))
    }
}

fn conflict(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Conflict, action, message)
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredHead {
    major: u16,
    volume_id: VolumeId,
    cursor: ChangeCursor,
    checkpoint: [u8; 32],
    checkpoint_cursor: ChangeCursor,
    tail: Vec<NamespaceChange>,
    maintenance_epoch: u64,
    maintenance_state: StoredMaintenanceState,
    #[serde(default)]
    maintenance_owner: Option<[u8; 16]>,
    maintenance_fixed_cursor: Option<ChangeCursor>,
}

impl std::fmt::Debug for StoredHead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoredHead")
            .field("cursor", &self.cursor)
            .field("checkpoint_cursor", &self.checkpoint_cursor)
            .field("tail_changes", &self.tail.len())
            .field("maintenance_epoch", &self.maintenance_epoch)
            .field("maintenance_state", &self.maintenance_state)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredMaintenanceState {
    Idle,
    Sweeping,
}

impl StoredHead {
    fn new(
        volume_id: VolumeId,
        cursor: ChangeCursor,
        checkpoint: [u8; 32],
        checkpoint_cursor: ChangeCursor,
        tail: Vec<NamespaceChange>,
    ) -> Result<Self, ManagedError> {
        let head = Self {
            major: FORMAT_MAJOR,
            volume_id,
            cursor,
            checkpoint,
            checkpoint_cursor,
            tail,
            maintenance_epoch: 0,
            maintenance_state: StoredMaintenanceState::Idle,
            maintenance_owner: None,
            maintenance_fixed_cursor: None,
        };
        head.validate_shape()?;
        Ok(head)
    }

    fn with_maintenance(mut self, epoch: u64, owner: Option<[u8; 16]>) -> Self {
        self.maintenance_epoch = epoch;
        self.maintenance_owner = owner;
        self
    }

    fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        self.validate_shape()?;
        if self.volume_id != volume_id {
            return Err(corrupt(
                "read Managed namespace",
                "HEAD integrity is invalid",
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), ManagedError> {
        let cursor = self.cursor;
        let checkpoint = self.checkpoint_cursor;
        if self.major != FORMAT_MAJOR
            || self.tail.len() > usize::from(MAX_TAIL_TRANSACTIONS)
            || self.tail_bytes() > MAX_TAIL_BYTES
            || checkpoint.sequence().checked_add(self.tail.len() as u64) != Some(cursor.sequence())
            || self.gc_sweep().is_err()
        {
            return Err(corrupt("read Managed namespace", "HEAD shape is invalid"));
        }
        let mut parent = checkpoint;
        for change in &self.tail {
            change.validate(self.volume_id)?;
            if change.parent != parent {
                return Err(corrupt(
                    "read Managed namespace",
                    "HEAD change tail is not consecutive",
                ));
            }
            parent = change.cursor;
        }
        if parent != cursor {
            return Err(corrupt(
                "read Managed namespace",
                "HEAD change tail does not reach its cursor",
            ));
        }
        Ok(())
    }

    fn tail_bytes(&self) -> usize {
        self.tail
            .iter()
            .map(|change| {
                encode_table_value(change, "encode Managed HEAD")
                    .expect("validated change can be encoded")
                    .len()
            })
            .sum()
    }

    fn gc_sweep(&self) -> Result<Option<NamespaceGcSweep>, ManagedError> {
        match (
            self.maintenance_state,
            self.maintenance_owner,
            self.maintenance_fixed_cursor,
        ) {
            (StoredMaintenanceState::Idle, _, None) => Ok(None),
            (StoredMaintenanceState::Sweeping, Some(owner), Some(fixed))
                if self.maintenance_epoch > 0 && fixed == self.cursor =>
            {
                Ok(Some(NamespaceGcSweep::new(
                    self.maintenance_epoch,
                    owner,
                    fixed,
                )))
            }
            _ => Err(corrupt(
                "read Managed namespace",
                "HEAD maintenance state is invalid",
            )),
        }
    }

    fn begin_gc(&mut self, owner: [u8; 16]) -> Result<NamespaceGcSweep, ManagedError> {
        if self.gc_sweep()?.is_some() {
            return Err(conflict(
                "begin Managed namespace GC",
                "another namespace GC is active",
            ));
        }
        self.maintenance_epoch = self.maintenance_epoch.checked_add(1).ok_or_else(|| {
            corrupt(
                "begin Managed namespace GC",
                "maintenance epoch is exhausted",
            )
        })?;
        self.maintenance_state = StoredMaintenanceState::Sweeping;
        self.maintenance_owner = Some(owner);
        self.maintenance_fixed_cursor = Some(self.cursor);
        Ok(NamespaceGcSweep::new(
            self.maintenance_epoch,
            owner,
            self.cursor,
        ))
    }

    fn resume_gc(&mut self, owner: [u8; 16]) -> Result<NamespaceGcSweep, ManagedError> {
        let active = self.gc_sweep()?.ok_or_else(|| {
            conflict(
                "resume Managed namespace GC",
                "no interrupted namespace GC is active",
            )
        })?;
        self.maintenance_owner = Some(owner);
        Ok(NamespaceGcSweep::new(
            active.epoch(),
            owner,
            active.fixed_cursor(),
        ))
    }

    fn finish_gc(&mut self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        if self.gc_sweep()? != Some(sweep) {
            return Err(conflict(
                "finish Managed namespace GC",
                "GC sweep token does not match the authority",
            ));
        }
        self.maintenance_state = StoredMaintenanceState::Idle;
        self.maintenance_fixed_cursor = None;
        Ok(())
    }
}

#[derive(Clone)]
struct StoredCheckpoint {
    snapshot: NamespaceSnapshot,
    results: BTreeMap<OperationId, StoredCommittedResult>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCommittedResult {
    operation: OperationId,
    cursor: ChangeCursor,
    request_sha256: [u8; 32],
}

impl StoredCommittedResult {
    fn from_transaction(transaction: &NamespaceChange) -> Result<Self, ManagedError> {
        Ok(Self {
            operation: transaction.operation,
            cursor: transaction.cursor,
            request_sha256: transaction_sha256(transaction, "checkpoint Managed namespace")?,
        })
    }

    fn validate(&self, operation: OperationId) -> Result<(), ManagedError> {
        if self.cursor.operation() != Some(operation) {
            return Err(corrupt(
                "read Managed operation results",
                "operation result cursor is invalid",
            ));
        }
        Ok(())
    }
}

fn transaction_sha256(
    transaction: &NamespaceChange,
    action: &'static str,
) -> Result<[u8; 32], ManagedError> {
    encode_table_value(transaction, action).map(|bytes| sha256(&bytes))
}

fn apply_transaction(
    base: Option<NamespaceSnapshot>,
    transaction: &NamespaceChange,
) -> Result<NamespaceSnapshot, ManagedError> {
    transaction.clone().apply(base)
}

fn replay_tail_from(
    base: &NamespaceSnapshot,
    head: &StoredHead,
) -> Result<Option<NamespaceSnapshot>, ManagedError> {
    let target = head.cursor;
    if base.cursor == target {
        return Ok(Some(base.clone()));
    }
    let mut start = None;
    for (index, transaction) in head.tail.iter().enumerate() {
        if transaction.parent == base.cursor {
            start = Some(index);
            break;
        }
    }
    let Some(start) = start else {
        return Ok(None);
    };
    let mut snapshot = base.clone();
    for transaction in &head.tail[start..] {
        if transaction.parent != snapshot.cursor {
            return Err(corrupt(
                "read Managed namespace",
                "transaction tail is not consecutive",
            ));
        }
        snapshot = apply_transaction(Some(snapshot), transaction)?;
    }
    if snapshot.cursor != target {
        return Err(corrupt(
            "read Managed namespace",
            "transaction tail does not reach HEAD",
        ));
    }
    Ok(Some(snapshot))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::managed::metadata::namespace::managed_generation;
    use opendal::services::Memory;

    fn root_snapshot(cursor: ChangeCursor) -> NamespaceSnapshot {
        let root = NodeId::from_bytes([3; 16]);
        NamespaceSnapshot {
            volume_id: VolumeId::from_bytes([1; 16]),
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
    fn head_recovers_the_same_gc_sweep_until_it_is_finished() {
        let volume = VolumeId::from_bytes([1; 16]);
        let operation = OperationId::from_bytes([2; 16]);
        let cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), operation);
        let mut head = StoredHead::new(volume, cursor, [4; 32], cursor, Vec::new()).unwrap();

        let sweep = head.begin_gc([5; 16]).unwrap();
        let mut recovered = decode_head(&encode_head(&head).unwrap()).unwrap();
        recovered.validate(volume).unwrap();
        assert_eq!(recovered.gc_sweep().unwrap(), Some(sweep));

        assert_eq!(
            recovered.begin_gc([6; 16]).unwrap_err().kind(),
            ManagedErrorKind::Conflict
        );
        let resumed = recovered.resume_gc([7; 16]).unwrap();
        assert_eq!(resumed.epoch(), sweep.epoch());
        assert_eq!(resumed.fixed_cursor(), sweep.fixed_cursor());
        assert_ne!(resumed, sweep);
        assert_eq!(
            recovered.finish_gc(sweep).unwrap_err().kind(),
            ManagedErrorKind::Conflict
        );

        recovered.finish_gc(resumed).unwrap();
        let idle = decode_head(&encode_head(&recovered).unwrap()).unwrap();
        idle.validate(volume).unwrap();
        assert_eq!(idle.gc_sweep().unwrap(), None);
        assert_eq!(idle.maintenance_epoch, resumed.epoch());
    }

    #[test]
    fn one_interpreter_recovers_checkpoint_and_bounded_tail() {
        let first = OperationId::from_bytes([4; 16]);
        let first_cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), first);
        let first_snapshot = root_snapshot(first_cursor);
        let first_publication = NamespacePublication {
            operation: first,
            parent: ChangeCursor::Genesis,
            expected_nodes: vec![NodePrecondition {
                node: first_snapshot.root,
                expected_generation: None,
            }],
            expected_directories: vec![DirectoryPrecondition {
                directory: first_snapshot.root,
                expected_generation: None,
            }],
            target: first_snapshot.clone(),
        };
        let checkpoint = apply_transaction(
            None,
            &NamespaceChange::from_publication(&first_publication, None),
        )
        .unwrap();
        let second = OperationId::from_bytes([5; 16]);
        let mut target = checkpoint.clone();
        target.cursor = ChangeCursor::at(NonZeroU64::new(2).unwrap(), second);
        let publication = NamespacePublication {
            operation: second,
            parent: first_cursor,
            expected_nodes: Vec::new(),
            expected_directories: Vec::new(),
            target: target.clone(),
        };
        let recovered = apply_transaction(
            Some(checkpoint),
            &NamespaceChange::from_publication(&publication, Some(&first_snapshot)),
        )
        .unwrap();
        assert_eq!(recovered, target);
    }

    #[tokio::test]
    async fn committed_operations_resolve_from_checkpoint_and_inline_tail() {
        let first = OperationId::from_bytes([41; 16]);
        let first_cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), first);
        let first_snapshot = root_snapshot(first_cursor);
        let first_publication = NamespacePublication {
            operation: first,
            parent: ChangeCursor::Genesis,
            expected_nodes: vec![NodePrecondition {
                node: first_snapshot.root,
                expected_generation: None,
            }],
            expected_directories: vec![DirectoryPrecondition {
                directory: first_snapshot.root,
                expected_generation: None,
            }],
            target: first_snapshot.clone(),
        };
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let namespace = ObjectNamespace {
            volume_id: first_snapshot.volume_id,
            operator: operator.clone(),
            backend: ObjectRecordBackend::new(operator),
        };
        assert_eq!(
            namespace.publish(None, &first_publication).await.unwrap(),
            CommitOutcome::Committed(first_cursor)
        );
        assert_eq!(
            namespace.resolve(first).await.unwrap(),
            CommitOutcome::Committed(first_cursor)
        );

        let first_head = namespace.read_current_head().await.unwrap().unwrap();
        let second = OperationId::from_bytes([42; 16]);
        let second_cursor = ChangeCursor::at(NonZeroU64::new(2).unwrap(), second);
        let mut second_snapshot = first_snapshot.clone();
        second_snapshot.cursor = second_cursor;
        let second_publication = NamespacePublication {
            operation: second,
            parent: first_cursor,
            expected_nodes: Vec::new(),
            expected_directories: Vec::new(),
            target: second_snapshot,
        };
        let stored = NamespaceChange::from_publication(&second_publication, Some(&first_snapshot));
        let head = StoredHead::new(
            namespace.volume_id,
            stored.cursor,
            first_head.checkpoint,
            first_head.checkpoint_cursor,
            vec![stored],
        )
        .unwrap();
        namespace
            .operator
            .write(HEAD_KEY, encode_head(&head).unwrap())
            .await
            .unwrap();
        assert_eq!(
            namespace.resolve(second).await.unwrap(),
            CommitOutcome::Committed(second_cursor)
        );
        assert_eq!(
            namespace.resolve(first).await.unwrap(),
            CommitOutcome::Committed(first_cursor)
        );
    }

    #[tokio::test]
    async fn missing_checkpoint_part_is_reported_as_corruption() {
        let operation = OperationId::from_bytes([10; 16]);
        let cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), operation);
        let snapshot = root_snapshot(cursor);
        let publication = NamespacePublication {
            operation,
            parent: ChangeCursor::Genesis,
            expected_nodes: vec![NodePrecondition {
                node: snapshot.root,
                expected_generation: None,
            }],
            expected_directories: vec![DirectoryPrecondition {
                directory: snapshot.root,
                expected_generation: None,
            }],
            target: snapshot.clone(),
        };
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let namespace = ObjectNamespace {
            volume_id: snapshot.volume_id,
            operator: operator.clone(),
            backend: ObjectRecordBackend::new(operator.clone()),
        };
        assert_eq!(
            namespace.publish(None, &publication).await.unwrap(),
            CommitOutcome::Committed(cursor)
        );
        let head = operator.read(HEAD_KEY).await.unwrap().to_bytes();
        let head = decode_head(&head).unwrap();
        let checkpoint = namespace.read_checkpoint(head.checkpoint).await.unwrap();
        let bytes = operator
            .read(&checkpoint_key(&head.checkpoint))
            .await
            .unwrap()
            .to_bytes();
        let root = CheckpointRoot::decode(&bytes).unwrap();
        operator
            .delete(&checkpoint_part_key(&root.parts[0].id))
            .await
            .unwrap();
        assert_eq!(checkpoint.snapshot, snapshot);
        let error = namespace.recover(&head).await.err().unwrap();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
    }
}
