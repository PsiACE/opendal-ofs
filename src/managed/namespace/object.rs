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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::num::NonZeroU64;

use opendal::{ErrorKind, Operator};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::validation::{validate_publication, validate_snapshot};
use super::{
    DirectoryPrecondition, DirectoryRecord, FileVersionLayout, FileVersionRecord,
    NamespacePublication, NamespaceSnapshot, NodePrecondition, NodeRecord, managed_generation,
    managed_generation_number,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, DirectoryEntry, FileVersionId, NodeAttributes, NodeId, NodeKind,
    OperationId, VolumeId,
};
use crate::managed::{ManagedError, ManagedErrorKind};

const HEAD_KEY: &str = ".ofs/managed-sync/head.json";
const TRANSACTION_ROOT: &str = ".ofs/managed-sync/transactions";
const CHECKPOINT_ROOT: &str = ".ofs/managed-sync/checkpoints";
const RESULT_ROOT: &str = ".ofs/managed-sync/results";
const TRANSACTION_MAGIC: &[u8] = b"OFS1TXN\0";
const CHECKPOINT_MAGIC: &[u8] = b"OFS1CHK\0";
const RESULT_MAGIC: &[u8] = b"OFS1RES\0";
const HEAD_MAGIC: &str = "ofs-managed-sync-head";
const FORMAT_MAJOR: u16 = 1;
const MAX_TAIL_TRANSACTIONS: u16 = 32;

#[derive(Clone, Debug)]
pub struct NamespaceObservation {
    pub snapshot: NamespaceSnapshot,
    revision: String,
    authority: Box<ObservationAuthority>,
}

#[derive(Clone, Debug)]
struct ObservationAuthority {
    head: StoredHead,
    committed: BTreeSet<[u8; 16]>,
}

#[derive(Clone)]
pub struct ObjectNamespace {
    volume_id: VolumeId,
    operator: Operator,
}

impl ObjectNamespace {
    pub fn new(volume_id: VolumeId, operator: Operator) -> Result<Self, ManagedError> {
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
            operator,
        })
    }

    pub async fn observe(&self) -> Result<Option<NamespaceObservation>, ManagedError> {
        let Some((bytes, revision)) = self.read_head().await? else {
            return Ok(None);
        };
        let head = decode_head(&bytes)?;
        let recovered = self.recover(&head).await?;
        Ok(Some(NamespaceObservation {
            snapshot: recovered.snapshot,
            revision,
            authority: Box::new(ObservationAuthority {
                head,
                committed: recovered.committed,
            }),
        }))
    }

    pub async fn publish(
        &self,
        observed: Option<&NamespaceObservation>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        if publication.target.volume_id != self.volume_id {
            return Err(invalid(
                "publish Managed namespace",
                "publication belongs to another volume",
            ));
        }
        let base = observed.map(|value| &value.snapshot);
        if !validate_publication(publication, base)? {
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |state| state.cursor),
            });
        }

        let stored = StoredTransaction::from_publication(publication, base);
        let interpreted = apply_transaction(base.cloned(), &stored)?;
        if interpreted != publication.target {
            return Err(invalid(
                "publish Managed namespace",
                "transaction does not reproduce its target",
            ));
        }
        let bytes = encode_cbor(TRANSACTION_MAGIC, &stored, "publish Managed namespace")?;
        let transaction_sha256 = sha256(&bytes);
        self.ensure_transaction(publication.operation, &bytes)
            .await?;
        let mut committed = observed
            .map(|value| value.authority.committed.clone())
            .unwrap_or_default();
        committed.insert(*publication.operation.as_bytes());
        let checkpoint_due = observed.is_none()
            || observed.is_some_and(|value| {
                value.authority.head.tail_transactions + 1 >= MAX_TAIL_TRANSACTIONS
            });
        let (checkpoint, checkpoint_cursor, tail_transactions) = if checkpoint_due {
            let checkpoint = StoredCheckpoint {
                major: FORMAT_MAJOR,
                snapshot: (&publication.target).into(),
                committed: committed.iter().copied().collect(),
            };
            let bytes = encode_cbor(
                CHECKPOINT_MAGIC,
                &checkpoint,
                "checkpoint Managed namespace",
            )?;
            let checkpoint_id = sha256(&bytes);
            self.ensure_immutable(&checkpoint_key(&checkpoint_id), &bytes)
                .await?;
            (checkpoint_id, publication.target.cursor.into(), 0)
        } else {
            let observed = observed.expect("checkpoint policy has an observation");
            (
                observed.authority.head.checkpoint,
                observed.authority.head.checkpoint_cursor,
                observed.authority.head.tail_transactions + 1,
            )
        };
        let head = StoredHead::new(
            self.volume_id,
            stored.cursor,
            stored.operation,
            transaction_sha256,
            checkpoint,
            checkpoint_cursor,
            tail_transactions,
        )?;
        let head = encode_head(&head)?;
        let replaced = match observed {
            Some(observed) => self.replace_head(&observed.revision, head).await,
            None => self.create_head(head).await,
        };
        match replaced {
            Ok(true) => {
                let outcome = CommitOutcome::Committed(publication.target.cursor);
                let _ = self
                    .ensure_result(publication.operation, transaction_sha256, &outcome)
                    .await;
                Ok(outcome)
            }
            Ok(false) => self.outcome_after_race(publication.operation).await,
            Err(_) => match self.resolve(publication.operation).await {
                Ok(CommitOutcome::Committed(cursor)) => Ok(CommitOutcome::Committed(cursor)),
                _ => Ok(CommitOutcome::Unknown),
            },
        }
    }

    pub async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        match self.resolve_known(operation).await {
            Err(error) if error.kind() == ManagedErrorKind::Unavailable => {
                Ok(CommitOutcome::Unknown)
            }
            outcome => outcome,
        }
    }

    async fn resolve_known(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        let result = self.read_result(operation).await?;
        let Some(target) = self.read_transaction(operation).await? else {
            if result.is_some() {
                return Err(corrupt(
                    "resolve Managed publication",
                    "operation result has no transaction",
                ));
            }
            return Ok(CommitOutcome::Absent);
        };
        let transaction_sha256 = sha256(&encode_cbor(
            TRANSACTION_MAGIC,
            &target,
            "resolve Managed publication",
        )?);
        if let Some(result) = result {
            if result.transaction_sha256 != transaction_sha256 {
                return Err(corrupt(
                    "resolve Managed publication",
                    "operation result identifies another transaction",
                ));
            }
            return match result.outcome {
                StoredResultKind::Committed { cursor } => {
                    Ok(CommitOutcome::Committed(cursor.into_cursor()?))
                }
                StoredResultKind::Conflict => Ok(CommitOutcome::Conflict {
                    observed: self
                        .observe()
                        .await?
                        .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor),
                }),
            };
        }
        let Some((bytes, _)) = self.read_head().await? else {
            return Ok(CommitOutcome::Absent);
        };
        let head = decode_head(&bytes)?;
        let recovered = self.recover(&head).await?;
        if recovered.committed.contains(operation.as_bytes()) {
            return Ok(CommitOutcome::Committed(target.cursor.into_cursor()?));
        }
        let parent = target.parent.into_cursor()?;
        if recovered.snapshot.cursor == parent
            || recovered.snapshot.cursor.sequence() <= parent.sequence()
        {
            return Ok(CommitOutcome::Absent);
        }
        Ok(CommitOutcome::Conflict {
            observed: recovered.snapshot.cursor,
        })
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
        let outcome = CommitOutcome::Conflict { observed };
        if let Ok(Some(transaction)) = self.read_transaction(operation).await {
            let bytes = encode_cbor(
                TRANSACTION_MAGIC,
                &transaction,
                "resolve Managed publication",
            )?;
            let _ = self
                .ensure_result(operation, sha256(&bytes), &outcome)
                .await;
        }
        Ok(outcome)
    }

    async fn ensure_transaction(
        &self,
        operation: OperationId,
        expected: &[u8],
    ) -> Result<(), ManagedError> {
        self.ensure_immutable(&transaction_key(operation), expected)
            .await
    }

    async fn ensure_immutable(&self, key: &str, expected: &[u8]) -> Result<(), ManagedError> {
        match self
            .operator
            .write_with(key, expected.to_vec())
            .if_not_exists(true)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error)
                if !matches!(
                    error.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                ) => {}
            Err(_) => {}
        }
        let existing = self
            .operator
            .read(key)
            .await
            .map_err(|_| unavailable("publish Managed namespace"))?;
        if existing.to_bytes().as_ref() == expected {
            Ok(())
        } else {
            Err(ManagedError::new(
                ManagedErrorKind::Conflict,
                "publish Managed namespace",
                "operation identity was reused with another payload",
            ))
        }
    }

    async fn read_transaction(
        &self,
        operation: OperationId,
    ) -> Result<Option<StoredTransaction>, ManagedError> {
        match self.operator.read(&transaction_key(operation)).await {
            Ok(bytes) => {
                let transaction: StoredTransaction = decode_cbor(
                    TRANSACTION_MAGIC,
                    &bytes.to_bytes(),
                    "read Managed transaction",
                )?;
                if transaction.operation != *operation.as_bytes() {
                    return Err(corrupt(
                        "read Managed transaction",
                        "transaction key and operation disagree",
                    ));
                }
                transaction.validate(self.volume_id)?;
                Ok(Some(transaction))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(unavailable("read Managed transaction")),
        }
    }

    async fn required_transaction(
        &self,
        operation: OperationId,
    ) -> Result<StoredTransaction, ManagedError> {
        self.read_transaction(operation)
            .await?
            .ok_or_else(|| corrupt("read Managed namespace", "transaction is missing"))
    }

    async fn recover(&self, head: &StoredHead) -> Result<Recovered, ManagedError> {
        head.validate(self.volume_id)?;
        let checkpoint = self.read_checkpoint(&head.checkpoint).await?;
        if checkpoint.major != FORMAT_MAJOR
            || checkpoint.snapshot.volume_id != *self.volume_id.as_bytes()
            || checkpoint.snapshot.cursor != head.checkpoint_cursor
        {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint and HEAD disagree",
            ));
        }
        let mut snapshot = checkpoint.snapshot.into_snapshot()?;
        validate_snapshot(&snapshot)
            .map_err(|_| corrupt("read Managed namespace", "checkpoint is invalid"))?;
        let committed_count = checkpoint.committed.len();
        let mut committed = checkpoint.committed.into_iter().collect::<BTreeSet<_>>();
        if committed.len() != committed_count {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint repeats an operation result",
            ));
        }

        let latest = OperationId::from_bytes(head.latest_transaction);
        let latest_transaction = self.required_transaction(latest).await?;
        let latest_bytes = encode_cbor(
            TRANSACTION_MAGIC,
            &latest_transaction,
            "read Managed namespace",
        )?;
        if sha256(&latest_bytes) != head.latest_transaction_sha256
            || latest_transaction.cursor != head.cursor
        {
            return Err(corrupt(
                "read Managed namespace",
                "latest transaction and HEAD disagree",
            ));
        }

        let mut tail = Vec::with_capacity(head.tail_transactions.into());
        let mut current = latest_transaction;
        for index in 0..head.tail_transactions {
            if index != 0 {
                current = self
                    .required_transaction(OperationId::from_bytes(
                        current.parent.operation.ok_or_else(|| {
                            corrupt("read Managed namespace", "transaction tail is incomplete")
                        })?,
                    ))
                    .await?;
            }
            tail.push(current.clone());
        }
        tail.reverse();
        for transaction in tail {
            if transaction.parent.into_cursor()? != snapshot.cursor {
                return Err(corrupt(
                    "read Managed namespace",
                    "transaction tail is not consecutive",
                ));
            }
            snapshot = apply_transaction(Some(snapshot), &transaction)?;
            committed.insert(transaction.operation);
        }
        if snapshot.cursor != head.cursor.into_cursor()? {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint and transaction tail do not reach HEAD",
            ));
        }
        Ok(Recovered {
            snapshot,
            committed,
        })
    }

    async fn read_checkpoint(&self, id: &[u8; 32]) -> Result<StoredCheckpoint, ManagedError> {
        let bytes = self
            .operator
            .read(&checkpoint_key(id))
            .await
            .map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    corrupt("read Managed namespace", "checkpoint is missing")
                } else {
                    unavailable("read Managed namespace")
                }
            })?
            .to_bytes();
        if sha256(&bytes) != *id {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint key and content disagree",
            ));
        }
        decode_cbor(CHECKPOINT_MAGIC, &bytes, "read Managed namespace")
    }

    async fn ensure_result(
        &self,
        operation: OperationId,
        transaction_sha256: [u8; 32],
        outcome: &CommitOutcome,
    ) -> Result<(), ManagedError> {
        let outcome = match outcome {
            CommitOutcome::Committed(cursor) => StoredResultKind::Committed {
                cursor: (*cursor).into(),
            },
            CommitOutcome::Conflict { .. } => StoredResultKind::Conflict,
            CommitOutcome::Absent | CommitOutcome::Unknown => return Ok(()),
        };
        let result = StoredResult {
            major: FORMAT_MAJOR,
            operation: *operation.as_bytes(),
            transaction_sha256,
            outcome,
        };
        let bytes = encode_cbor(RESULT_MAGIC, &result, "write Managed operation result")?;
        self.ensure_immutable(&result_key(operation), &bytes).await
    }

    async fn read_result(
        &self,
        operation: OperationId,
    ) -> Result<Option<StoredResult>, ManagedError> {
        match self.operator.read(&result_key(operation)).await {
            Ok(bytes) => {
                let result: StoredResult = decode_cbor(
                    RESULT_MAGIC,
                    &bytes.to_bytes(),
                    "read Managed operation result",
                )?;
                if result.major != FORMAT_MAJOR || result.operation != *operation.as_bytes() {
                    return Err(corrupt(
                        "read Managed operation result",
                        "operation result identity is invalid",
                    ));
                }
                Ok(Some(result))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(unavailable("read Managed operation result")),
        }
    }

    async fn read_head(&self) -> Result<Option<(Vec<u8>, String)>, ManagedError> {
        let reader = match self.operator.reader(HEAD_KEY).await {
            Ok(reader) => reader,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(unavailable("read Managed namespace")),
        };
        let bytes = match reader.read(..).await {
            Ok(bytes) => bytes.to_bytes().to_vec(),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(unavailable("read Managed namespace")),
        };
        let revision = reader
            .metadata()
            .and_then(|metadata| metadata.etag())
            .ok_or_else(|| unavailable("read Managed namespace"))?
            .to_owned();
        Ok(Some((bytes, revision)))
    }

    async fn create_head(&self, bytes: Vec<u8>) -> Result<bool, ManagedError> {
        conditional_result(
            self.operator
                .write_with(HEAD_KEY, bytes)
                .if_not_exists(true)
                .await,
        )
    }

    async fn replace_head(
        &self,
        expected_revision: &str,
        bytes: Vec<u8>,
    ) -> Result<bool, ManagedError> {
        conditional_result(
            self.operator
                .write_with(HEAD_KEY, bytes)
                .if_match(expected_revision)
                .await,
        )
    }
}

fn conditional_result(result: opendal::Result<opendal::Metadata>) -> Result<bool, ManagedError> {
    match result {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(_) => Err(unavailable("publish Managed namespace")),
    }
}

fn transaction_key(operation: OperationId) -> String {
    format!("{TRANSACTION_ROOT}/{}.cbor", hex(operation.as_bytes()))
}

fn result_key(operation: OperationId) -> String {
    format!("{RESULT_ROOT}/{}.cbor", hex(operation.as_bytes()))
}

fn checkpoint_key(id: &[u8; 32]) -> String {
    format!("{CHECKPOINT_ROOT}/{}.cbor", hex(id))
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
    let mut bytes = serde_json::to_vec(value)
        .map_err(|_| invalid("write Managed namespace", "HEAD cannot be encoded"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn decode_head(bytes: &[u8]) -> Result<StoredHead, ManagedError> {
    serde_json::from_slice(bytes).map_err(|_| corrupt("read Managed namespace", "HEAD is invalid"))
}

fn encode_cbor<T: Serialize>(
    magic: &[u8],
    value: &T,
    action: &'static str,
) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::from(magic);
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| invalid(action, "durable record cannot be encoded"))?;
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

fn decode_cbor<T: DeserializeOwned>(
    magic: &[u8],
    bytes: &[u8],
    action: &'static str,
) -> Result<T, ManagedError> {
    let body = bytes
        .strip_prefix(magic)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| corrupt(action, "durable record has the wrong format version"))?;
    let expected = bytes
        .get(bytes.len().saturating_sub(32)..)
        .ok_or_else(|| corrupt(action, "durable record checksum is missing"))?;
    if Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != expected {
        return Err(corrupt(action, "durable record checksum does not match"));
    }
    let mut input = Cursor::new(body);
    let value = ciborium::de::from_reader(&mut input)
        .map_err(|_| corrupt(action, "durable record is invalid"))?;
    if input.position() != body.len() as u64 {
        return Err(corrupt(action, "durable record has trailing bytes"));
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

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "object metadata is unavailable",
    )
}

struct Recovered {
    snapshot: NamespaceSnapshot,
    committed: BTreeSet<[u8; 16]>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredHead {
    magic: String,
    major: u16,
    volume_id: [u8; 16],
    cursor: StoredCursor,
    latest_transaction: [u8; 16],
    latest_transaction_sha256: [u8; 32],
    checkpoint: [u8; 32],
    checkpoint_cursor: StoredCursor,
    tail_transactions: u16,
    checksum: [u8; 32],
}

impl StoredHead {
    fn new(
        volume_id: VolumeId,
        cursor: StoredCursor,
        latest_transaction: [u8; 16],
        latest_transaction_sha256: [u8; 32],
        checkpoint: [u8; 32],
        checkpoint_cursor: StoredCursor,
        tail_transactions: u16,
    ) -> Result<Self, ManagedError> {
        let mut head = Self {
            magic: HEAD_MAGIC.into(),
            major: FORMAT_MAJOR,
            volume_id: *volume_id.as_bytes(),
            cursor,
            latest_transaction,
            latest_transaction_sha256,
            checkpoint,
            checkpoint_cursor,
            tail_transactions,
            checksum: [0; 32],
        };
        head.validate_shape()?;
        head.checksum = head_checksum(&head);
        Ok(head)
    }

    fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        self.validate_shape()?;
        if self.volume_id != *volume_id.as_bytes() || self.checksum != head_checksum(self) {
            return Err(corrupt(
                "read Managed namespace",
                "HEAD integrity is invalid",
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), ManagedError> {
        let cursor = self.cursor.into_cursor()?;
        let checkpoint = self.checkpoint_cursor.into_cursor()?;
        if self.magic != HEAD_MAGIC
            || self.major != FORMAT_MAJOR
            || cursor.operation() != Some(OperationId::from_bytes(self.latest_transaction))
            || self.tail_transactions > MAX_TAIL_TRANSACTIONS
            || checkpoint
                .sequence()
                .checked_add(u64::from(self.tail_transactions))
                != Some(cursor.sequence())
        {
            return Err(corrupt("read Managed namespace", "HEAD shape is invalid"));
        }
        Ok(())
    }
}

fn head_checksum(head: &StoredHead) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"OFS1HEAD\0");
    digest.update(head.magic.as_bytes());
    digest.update(head.major.to_be_bytes());
    digest.update(head.volume_id);
    update_cursor_digest(&mut digest, head.cursor);
    digest.update(head.latest_transaction);
    digest.update(head.latest_transaction_sha256);
    digest.update(head.checkpoint);
    update_cursor_digest(&mut digest, head.checkpoint_cursor);
    digest.update(head.tail_transactions.to_be_bytes());
    digest.finalize().into()
}

fn update_cursor_digest(digest: &mut Sha256, cursor: StoredCursor) {
    digest.update(cursor.sequence.to_be_bytes());
    match cursor.operation {
        Some(operation) => {
            digest.update([1]);
            digest.update(operation);
        }
        None => digest.update([0]),
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredTransaction {
    major: u16,
    volume_id: [u8; 16],
    operation: [u8; 16],
    parent: StoredCursor,
    cursor: StoredCursor,
    root: [u8; 16],
    expected_nodes: Vec<StoredNodePrecondition>,
    expected_directories: Vec<StoredDirectoryPrecondition>,
    put_nodes: Vec<StoredNode>,
    remove_nodes: Vec<[u8; 16]>,
    put_directories: Vec<StoredDirectory>,
    remove_directories: Vec<[u8; 16]>,
    put_file_versions: Vec<StoredFileVersion>,
    remove_file_versions: Vec<[u8; 32]>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCheckpoint {
    major: u16,
    snapshot: StoredSnapshot,
    committed: Vec<[u8; 16]>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredResult {
    major: u16,
    operation: [u8; 16],
    transaction_sha256: [u8; 32],
    outcome: StoredResultKind,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum StoredResultKind {
    Committed { cursor: StoredCursor },
    Conflict,
}

impl StoredTransaction {
    fn from_publication(
        publication: &NamespacePublication,
        base: Option<&NamespaceSnapshot>,
    ) -> Self {
        let empty_nodes = BTreeMap::new();
        let empty_directories = BTreeMap::new();
        let empty_versions = BTreeMap::new();
        let base_nodes = base.map_or(&empty_nodes, |value| &value.nodes);
        let base_directories = base.map_or(&empty_directories, |value| &value.directories);
        let base_versions = base.map_or(&empty_versions, |value| &value.file_versions);
        let target = &publication.target;
        let mut stored = Self {
            major: FORMAT_MAJOR,
            volume_id: *target.volume_id.as_bytes(),
            operation: *publication.operation.as_bytes(),
            parent: publication.parent.into(),
            cursor: target.cursor.into(),
            root: *target.root.as_bytes(),
            expected_nodes: publication
                .expected_nodes
                .iter()
                .map(StoredNodePrecondition::from)
                .collect(),
            expected_directories: publication
                .expected_directories
                .iter()
                .map(StoredDirectoryPrecondition::from)
                .collect(),
            put_nodes: target
                .nodes
                .iter()
                .filter(|(id, record)| base_nodes.get(id) != Some(record))
                .map(|(_, record)| record.into())
                .collect(),
            remove_nodes: base_nodes
                .keys()
                .filter(|id| !target.nodes.contains_key(id))
                .map(|id| *id.as_bytes())
                .collect(),
            put_directories: target
                .directories
                .iter()
                .filter(|(id, record)| base_directories.get(id) != Some(record))
                .map(|(_, record)| record.into())
                .collect(),
            remove_directories: base_directories
                .keys()
                .filter(|id| !target.directories.contains_key(id))
                .map(|id| *id.as_bytes())
                .collect(),
            put_file_versions: target
                .file_versions
                .iter()
                .filter(|(id, record)| base_versions.get(id) != Some(record))
                .map(|(_, record)| record.into())
                .collect(),
            remove_file_versions: base_versions
                .keys()
                .filter(|id| !target.file_versions.contains_key(id))
                .map(|id| *id.as_bytes())
                .collect(),
        };
        stored
            .expected_nodes
            .sort_by_key(|condition| condition.node);
        stored
            .expected_directories
            .sort_by_key(|condition| condition.directory);
        stored
    }

    fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        let parent = self.parent.into_cursor()?;
        let cursor = self.cursor.into_cursor()?;
        if self.major != FORMAT_MAJOR
            || self.volume_id != *volume_id.as_bytes()
            || cursor.operation() != Some(OperationId::from_bytes(self.operation))
            || parent.sequence().checked_add(1) != Some(cursor.sequence())
        {
            return Err(corrupt(
                "read Managed transaction",
                "transaction ancestry is invalid",
            ));
        }
        Ok(())
    }
}

fn apply_transaction(
    base: Option<NamespaceSnapshot>,
    transaction: &StoredTransaction,
) -> Result<NamespaceSnapshot, ManagedError> {
    let volume_id = VolumeId::from_bytes(transaction.volume_id);
    transaction.validate(volume_id)?;
    let parent = transaction.parent.into_cursor()?;
    let validation_base = base.clone();
    let mut target = match base {
        Some(base) if base.volume_id == volume_id && base.cursor == parent => base,
        Some(_) => {
            return Err(corrupt(
                "read Managed transaction",
                "transaction base is invalid",
            ));
        }
        None if parent == ChangeCursor::Genesis => NamespaceSnapshot {
            volume_id,
            cursor: ChangeCursor::Genesis,
            root: NodeId::from_bytes(transaction.root),
            nodes: BTreeMap::new(),
            directories: BTreeMap::new(),
            file_versions: BTreeMap::new(),
        },
        None => {
            return Err(corrupt(
                "read Managed transaction",
                "initial transaction does not begin at genesis",
            ));
        }
    };
    let mut changed_nodes = BTreeSet::new();
    for id in &transaction.remove_nodes {
        if !changed_nodes.insert(*id) || target.nodes.remove(&NodeId::from_bytes(*id)).is_none() {
            return Err(corrupt("read Managed transaction", "node delta is invalid"));
        }
    }
    for node in &transaction.put_nodes {
        if !changed_nodes.insert(node.id) {
            return Err(corrupt("read Managed transaction", "node delta is invalid"));
        }
        let node = node.clone().into_record()?;
        target.nodes.insert(node.id, node);
    }
    let mut changed_directories = BTreeSet::new();
    for id in &transaction.remove_directories {
        if !changed_directories.insert(*id)
            || target
                .directories
                .remove(&NodeId::from_bytes(*id))
                .is_none()
        {
            return Err(corrupt(
                "read Managed transaction",
                "directory delta is invalid",
            ));
        }
    }
    for directory in &transaction.put_directories {
        if !changed_directories.insert(directory.node) {
            return Err(corrupt(
                "read Managed transaction",
                "directory delta is invalid",
            ));
        }
        let directory = directory.clone().into_record()?;
        target.directories.insert(directory.node, directory);
    }
    let mut changed_versions = BTreeSet::new();
    for id in &transaction.remove_file_versions {
        if !changed_versions.insert(*id)
            || target
                .file_versions
                .remove(&FileVersionId::from_bytes(*id))
                .is_none()
        {
            return Err(corrupt(
                "read Managed transaction",
                "file version delta is invalid",
            ));
        }
    }
    for version in &transaction.put_file_versions {
        if !changed_versions.insert(version.id) {
            return Err(corrupt(
                "read Managed transaction",
                "file version delta is invalid",
            ));
        }
        let version = version.clone().into_record()?;
        target.file_versions.insert(version.id, version);
    }
    target.root = NodeId::from_bytes(transaction.root);
    target.cursor = transaction.cursor.into_cursor()?;
    let publication = NamespacePublication {
        operation: OperationId::from_bytes(transaction.operation),
        parent,
        expected_nodes: transaction
            .expected_nodes
            .iter()
            .cloned()
            .map(StoredNodePrecondition::into_record)
            .collect(),
        expected_directories: transaction
            .expected_directories
            .iter()
            .cloned()
            .map(StoredDirectoryPrecondition::into_record)
            .collect(),
        target: target.clone(),
    };
    if !validate_publication(&publication, validation_base.as_ref())
        .map_err(|_| corrupt("read Managed transaction", "transaction is invalid"))?
    {
        return Err(corrupt(
            "read Managed transaction",
            "transaction preconditions are stale",
        ));
    }
    Ok(target)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCursor {
    sequence: u64,
    operation: Option<[u8; 16]>,
}

impl From<ChangeCursor> for StoredCursor {
    fn from(cursor: ChangeCursor) -> Self {
        Self {
            sequence: cursor.sequence(),
            operation: cursor.operation().map(|operation| *operation.as_bytes()),
        }
    }
}

impl StoredCursor {
    fn into_cursor(self) -> Result<ChangeCursor, ManagedError> {
        match (self.sequence, self.operation) {
            (0, None) => Ok(ChangeCursor::Genesis),
            (sequence, Some(operation)) => Ok(ChangeCursor::at(
                NonZeroU64::new(sequence)
                    .ok_or_else(|| corrupt("read Managed namespace", "cursor is invalid"))?,
                OperationId::from_bytes(operation),
            )),
            _ => Err(corrupt("read Managed namespace", "cursor is invalid")),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshot {
    volume_id: [u8; 16],
    cursor: StoredCursor,
    root: [u8; 16],
    nodes: Vec<StoredNode>,
    directories: Vec<StoredDirectory>,
    file_versions: Vec<StoredFileVersion>,
}

impl From<&NamespaceSnapshot> for StoredSnapshot {
    fn from(snapshot: &NamespaceSnapshot) -> Self {
        Self {
            volume_id: *snapshot.volume_id.as_bytes(),
            cursor: snapshot.cursor.into(),
            root: *snapshot.root.as_bytes(),
            nodes: snapshot.nodes.values().map(StoredNode::from).collect(),
            directories: snapshot
                .directories
                .values()
                .map(StoredDirectory::from)
                .collect(),
            file_versions: snapshot
                .file_versions
                .values()
                .map(StoredFileVersion::from)
                .collect(),
        }
    }
}

impl StoredSnapshot {
    fn into_snapshot(self) -> Result<NamespaceSnapshot, ManagedError> {
        let node_count = self.nodes.len();
        let directory_count = self.directories.len();
        let file_version_count = self.file_versions.len();
        let nodes = self
            .nodes
            .into_iter()
            .map(StoredNode::into_record)
            .map(|record| record.map(|record| (record.id, record)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if nodes.len() != node_count {
            return Err(corrupt("read Managed namespace", "duplicate node record"));
        }
        let directories = self
            .directories
            .into_iter()
            .map(StoredDirectory::into_record)
            .map(|record| record.map(|record| (record.node, record)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if directories.len() != directory_count {
            return Err(corrupt(
                "read Managed namespace",
                "duplicate directory record",
            ));
        }
        let file_versions = self
            .file_versions
            .into_iter()
            .map(StoredFileVersion::into_record)
            .map(|record| record.map(|record| (record.id, record)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if file_versions.len() != file_version_count {
            return Err(corrupt(
                "read Managed namespace",
                "duplicate file version record",
            ));
        }
        Ok(NamespaceSnapshot {
            volume_id: VolumeId::from_bytes(self.volume_id),
            cursor: self.cursor.into_cursor()?,
            root: NodeId::from_bytes(self.root),
            nodes,
            directories,
            file_versions,
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNode {
    id: [u8; 16],
    generation: u64,
    kind: StoredNodeKind,
    attributes: StoredNodeAttributes,
    file_version: Option<[u8; 32]>,
}

impl From<&NodeRecord> for StoredNode {
    fn from(node: &NodeRecord) -> Self {
        Self {
            id: *node.id.as_bytes(),
            generation: managed_generation_number(&node.generation)
                .expect("validated Managed node generation"),
            kind: node.kind.into(),
            attributes: node.attributes.into(),
            file_version: node.file_version.map(|version| *version.as_bytes()),
        }
    }
}

impl StoredNode {
    fn into_record(self) -> Result<NodeRecord, ManagedError> {
        Ok(NodeRecord {
            id: NodeId::from_bytes(self.id),
            generation: managed_generation(self.generation),
            kind: self.kind.into(),
            attributes: self.attributes.into(),
            file_version: self.file_version.map(FileVersionId::from_bytes),
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectory {
    node: [u8; 16],
    generation: u64,
    entries: BTreeMap<String, StoredDirectoryEntry>,
}

impl From<&DirectoryRecord> for StoredDirectory {
    fn from(directory: &DirectoryRecord) -> Self {
        Self {
            node: *directory.node.as_bytes(),
            generation: managed_generation_number(&directory.generation)
                .expect("validated Managed directory generation"),
            entries: directory
                .entries
                .iter()
                .map(|(name, entry)| (name.clone(), (*entry).into()))
                .collect(),
        }
    }
}

impl StoredDirectory {
    fn into_record(self) -> Result<DirectoryRecord, ManagedError> {
        Ok(DirectoryRecord {
            node: NodeId::from_bytes(self.node),
            generation: managed_generation(self.generation),
            entries: self
                .entries
                .into_iter()
                .map(|(name, entry)| (name, entry.into()))
                .collect(),
        })
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryEntry {
    node: [u8; 16],
    kind: StoredNodeKind,
}

impl From<DirectoryEntry> for StoredDirectoryEntry {
    fn from(entry: DirectoryEntry) -> Self {
        Self {
            node: *entry.node.as_bytes(),
            kind: entry.kind.into(),
        }
    }
}

impl From<StoredDirectoryEntry> for DirectoryEntry {
    fn from(entry: StoredDirectoryEntry) -> Self {
        Self {
            node: NodeId::from_bytes(entry.node),
            kind: entry.kind.into(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredNodeKind {
    Directory,
    RegularFile,
}

impl From<NodeKind> for StoredNodeKind {
    fn from(kind: NodeKind) -> Self {
        match kind {
            NodeKind::Directory => Self::Directory,
            NodeKind::RegularFile => Self::RegularFile,
        }
    }
}

impl From<StoredNodeKind> for NodeKind {
    fn from(kind: StoredNodeKind) -> Self {
        match kind {
            StoredNodeKind::Directory => Self::Directory,
            StoredNodeKind::RegularFile => Self::RegularFile,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNodeAttributes {
    executable: bool,
}

impl From<NodeAttributes> for StoredNodeAttributes {
    fn from(attributes: NodeAttributes) -> Self {
        Self {
            executable: attributes.executable,
        }
    }
}

impl From<StoredNodeAttributes> for NodeAttributes {
    fn from(attributes: StoredNodeAttributes) -> Self {
        Self {
            executable: attributes.executable,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredFileVersion {
    id: [u8; 32],
    logical_size: u64,
    logical_digest: [u8; 32],
    layout: FileVersionLayout,
}

impl From<&FileVersionRecord> for StoredFileVersion {
    fn from(version: &FileVersionRecord) -> Self {
        Self {
            id: *version.id.as_bytes(),
            logical_size: version.logical_size,
            logical_digest: version.logical_digest,
            layout: version.layout.clone(),
        }
    }
}

impl StoredFileVersion {
    fn into_record(self) -> Result<FileVersionRecord, ManagedError> {
        Ok(FileVersionRecord {
            id: FileVersionId::from_bytes(self.id),
            logical_size: self.logical_size,
            logical_digest: self.logical_digest,
            layout: self.layout,
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNodePrecondition {
    node: [u8; 16],
    expected_generation: Option<u64>,
}

impl From<&NodePrecondition> for StoredNodePrecondition {
    fn from(condition: &NodePrecondition) -> Self {
        Self {
            node: *condition.node.as_bytes(),
            expected_generation: condition.expected_generation.as_ref().map(|value| {
                managed_generation_number(value)
                    .expect("validated Managed node precondition generation")
            }),
        }
    }
}

impl StoredNodePrecondition {
    fn into_record(self) -> NodePrecondition {
        NodePrecondition {
            node: NodeId::from_bytes(self.node),
            expected_generation: self.expected_generation.map(managed_generation),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryPrecondition {
    directory: [u8; 16],
    expected_generation: Option<u64>,
}

impl From<&DirectoryPrecondition> for StoredDirectoryPrecondition {
    fn from(condition: &DirectoryPrecondition) -> Self {
        Self {
            directory: *condition.directory.as_bytes(),
            expected_generation: condition.expected_generation.as_ref().map(|value| {
                managed_generation_number(value)
                    .expect("validated Managed directory precondition generation")
            }),
        }
    }
}

impl StoredDirectoryPrecondition {
    fn into_record(self) -> DirectoryPrecondition {
        DirectoryPrecondition {
            directory: NodeId::from_bytes(self.directory),
            expected_generation: self.expected_generation.map(managed_generation),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn durable_cbor_is_deterministic_and_rejects_corruption() {
        let operation = OperationId::from_bytes([7; 16]);
        let result = StoredResult {
            major: FORMAT_MAJOR,
            operation: *operation.as_bytes(),
            transaction_sha256: [9; 32],
            outcome: StoredResultKind::Committed {
                cursor: ChangeCursor::at(NonZeroU64::new(1).unwrap(), operation).into(),
            },
        };
        let encoded = encode_cbor(RESULT_MAGIC, &result, "test").unwrap();
        assert_eq!(encoded, encode_cbor(RESULT_MAGIC, &result, "test").unwrap());
        assert_eq!(
            hex(&sha256(&encoded)),
            "142474aa1080f5ac4edc2708298130e4ca08a90d800f19c575d4283eede1362c"
        );
        let last = encoded.len() - 1;
        let mut corrupt = encoded;
        corrupt[last] ^= 1;
        assert!(decode_cbor::<StoredResult>(RESULT_MAGIC, &corrupt, "test").is_err());
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
            &StoredTransaction::from_publication(&first_publication, None),
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
            &StoredTransaction::from_publication(&publication, Some(&first_snapshot)),
        )
        .unwrap();
        assert_eq!(recovered, target);
    }
}
