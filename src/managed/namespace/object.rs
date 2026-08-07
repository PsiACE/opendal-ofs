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
use std::num::NonZeroU64;

use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};

use super::validation::{validate_publication, validate_snapshot};
use super::{
    ContentRef, DirectoryEntry, DirectoryPrecondition, DirectoryRecord, FileVersionRecord,
    NamespacePublication, NamespaceSnapshot, NodePrecondition, NodeRecord,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, FileVersionId, NodeId, OperationId, VolumeId,
};
use crate::managed::{ManagedError, ManagedErrorKind};

const HEAD_KEY: &str = ".ofs/managed/head.json";
const TRANSACTION_ROOT: &str = ".ofs/managed/transactions";

#[derive(Clone, Debug)]
pub struct NamespaceObservation {
    pub snapshot: NamespaceSnapshot,
    revision: String,
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
        let head: StoredHead = decode(&bytes, "read Managed namespace")?;
        if head.volume_id != *self.volume_id.as_bytes()
            || head.cursor.operation != Some(head.transaction)
        {
            return Err(corrupt("read Managed namespace", "HEAD is invalid"));
        }
        let transaction = self
            .required_transaction(OperationId::from_bytes(head.transaction))
            .await?;
        if transaction.operation != head.transaction || transaction.target.cursor != head.cursor {
            return Err(corrupt(
                "read Managed namespace",
                "HEAD and transaction disagree",
            ));
        }
        let snapshot = transaction.target.into_snapshot()?;
        if snapshot.volume_id != self.volume_id {
            return Err(corrupt(
                "read Managed namespace",
                "snapshot belongs to another volume",
            ));
        }
        validate_snapshot(&snapshot)
            .map_err(|_| corrupt("read Managed namespace", "snapshot is invalid"))?;
        Ok(Some(NamespaceObservation { snapshot, revision }))
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

        let stored = StoredTransaction::from(publication);
        let bytes = encode(&stored, "publish Managed namespace")?;
        self.ensure_transaction(publication.operation, &bytes)
            .await?;
        let head = StoredHead {
            volume_id: *self.volume_id.as_bytes(),
            cursor: stored.target.cursor,
            transaction: stored.operation,
        };
        let head = encode(&head, "publish Managed namespace")?;
        let replaced = match observed {
            Some(observed) => self.replace_head(&observed.revision, head).await,
            None => self.create_head(head).await,
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

    pub async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        let Some(target) = self.read_transaction(operation).await? else {
            return Ok(CommitOutcome::Absent);
        };
        let Some(observed) = self.observe().await? else {
            return Ok(CommitOutcome::Absent);
        };
        let mut cursor = observed.snapshot.cursor;
        loop {
            let Some(current_operation) = cursor.operation() else {
                return Ok(CommitOutcome::Absent);
            };
            if current_operation == operation {
                return Ok(CommitOutcome::Committed(
                    target.target.cursor.into_cursor()?,
                ));
            }
            if cursor.sequence() <= target.target.cursor.sequence {
                return Ok(CommitOutcome::Absent);
            }
            let current = self.required_transaction(current_operation).await?;
            let current_cursor = current.target.cursor.into_cursor()?;
            if current_cursor != cursor {
                return Err(corrupt(
                    "resolve Managed publication",
                    "transaction ancestry is invalid",
                ));
            }
            cursor = current.parent.into_cursor()?;
        }
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        if let CommitOutcome::Committed(cursor) = self.resolve(operation).await? {
            return Ok(CommitOutcome::Committed(cursor));
        }
        let observed = self
            .observe()
            .await?
            .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor);
        Ok(CommitOutcome::Conflict { observed })
    }

    async fn ensure_transaction(
        &self,
        operation: OperationId,
        expected: &[u8],
    ) -> Result<(), ManagedError> {
        let key = transaction_key(operation);
        match self
            .operator
            .write_with(&key, expected.to_vec())
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
            .read(&key)
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
                let transaction: StoredTransaction =
                    decode(&bytes.to_bytes(), "read Managed transaction")?;
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
    format!("{TRANSACTION_ROOT}/{}.json", hex(operation.as_bytes()))
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

fn encode(value: &impl Serialize, action: &'static str) -> Result<Vec<u8>, ManagedError> {
    serde_json::to_vec(value).map_err(|_| invalid(action, "namespace record cannot be encoded"))
}

fn decode<'a, T: Deserialize<'a>>(
    bytes: &'a [u8],
    action: &'static str,
) -> Result<T, ManagedError> {
    serde_json::from_slice(bytes).map_err(|_| corrupt(action, "namespace record is invalid"))
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

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredHead {
    volume_id: [u8; 16],
    cursor: StoredCursor,
    transaction: [u8; 16],
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredTransaction {
    operation: [u8; 16],
    parent: StoredCursor,
    expected_nodes: Vec<StoredNodePrecondition>,
    expected_directories: Vec<StoredDirectoryPrecondition>,
    target: StoredSnapshot,
}

impl From<&NamespacePublication> for StoredTransaction {
    fn from(publication: &NamespacePublication) -> Self {
        let mut stored = Self {
            operation: *publication.operation.as_bytes(),
            parent: publication.parent.into(),
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
            target: (&publication.target).into(),
        };
        stored
            .expected_nodes
            .sort_by_key(|condition| condition.node);
        stored
            .expected_directories
            .sort_by_key(|condition| condition.directory);
        stored
    }
}

impl StoredTransaction {
    fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        let parent = self.parent.into_cursor()?;
        let target = self.target.cursor.into_cursor()?;
        if self.target.volume_id != *volume_id.as_bytes()
            || target.operation() != Some(OperationId::from_bytes(self.operation))
            || parent.sequence().checked_add(1) != Some(target.sequence())
        {
            return Err(corrupt(
                "read Managed transaction",
                "transaction ancestry is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Deserialize, Serialize)]
struct StoredNode {
    id: [u8; 16],
    generation: u64,
    kind: super::NodeKind,
    attributes: super::NodeAttributes,
    file_version: Option<[u8; 32]>,
}

impl From<&NodeRecord> for StoredNode {
    fn from(node: &NodeRecord) -> Self {
        Self {
            id: *node.id.as_bytes(),
            generation: node.generation,
            kind: node.kind,
            attributes: node.attributes,
            file_version: node.file_version.map(|version| *version.as_bytes()),
        }
    }
}

impl StoredNode {
    fn into_record(self) -> Result<NodeRecord, ManagedError> {
        Ok(NodeRecord {
            id: NodeId::from_bytes(self.id),
            generation: self.generation,
            kind: self.kind,
            attributes: self.attributes,
            file_version: self.file_version.map(FileVersionId::from_bytes),
        })
    }
}

#[derive(Deserialize, Serialize)]
struct StoredDirectory {
    node: [u8; 16],
    generation: u64,
    entries: BTreeMap<String, StoredDirectoryEntry>,
}

impl From<&DirectoryRecord> for StoredDirectory {
    fn from(directory: &DirectoryRecord) -> Self {
        Self {
            node: *directory.node.as_bytes(),
            generation: directory.generation,
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
            generation: self.generation,
            entries: self
                .entries
                .into_iter()
                .map(|(name, entry)| (name, entry.into()))
                .collect(),
        })
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
struct StoredDirectoryEntry {
    node: [u8; 16],
    kind: super::NodeKind,
}

impl From<DirectoryEntry> for StoredDirectoryEntry {
    fn from(entry: DirectoryEntry) -> Self {
        Self {
            node: *entry.node.as_bytes(),
            kind: entry.kind,
        }
    }
}

impl From<StoredDirectoryEntry> for DirectoryEntry {
    fn from(entry: StoredDirectoryEntry) -> Self {
        Self {
            node: NodeId::from_bytes(entry.node),
            kind: entry.kind,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct StoredFileVersion {
    id: [u8; 32],
    logical_size: u64,
    logical_digest: [u8; 32],
    content_digest: [u8; 32],
    content_length: u64,
}

impl From<&FileVersionRecord> for StoredFileVersion {
    fn from(version: &FileVersionRecord) -> Self {
        Self {
            id: *version.id.as_bytes(),
            logical_size: version.logical_size,
            logical_digest: version.logical_digest,
            content_digest: version.content.digest,
            content_length: version.content.logical_length,
        }
    }
}

impl StoredFileVersion {
    fn into_record(self) -> Result<FileVersionRecord, ManagedError> {
        Ok(FileVersionRecord {
            id: FileVersionId::from_bytes(self.id),
            logical_size: self.logical_size,
            logical_digest: self.logical_digest,
            content: ContentRef {
                digest: self.content_digest,
                logical_length: self.content_length,
            },
        })
    }
}

#[derive(Deserialize, Serialize)]
struct StoredNodePrecondition {
    node: [u8; 16],
    expected_generation: Option<u64>,
}

impl From<&NodePrecondition> for StoredNodePrecondition {
    fn from(condition: &NodePrecondition) -> Self {
        Self {
            node: *condition.node.as_bytes(),
            expected_generation: condition.expected_generation,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct StoredDirectoryPrecondition {
    directory: [u8; 16],
    expected_generation: Option<u64>,
}

impl From<&DirectoryPrecondition> for StoredDirectoryPrecondition {
    fn from(condition: &DirectoryPrecondition) -> Self {
        Self {
            directory: *condition.directory.as_bytes(),
            expected_generation: condition.expected_generation,
        }
    }
}
