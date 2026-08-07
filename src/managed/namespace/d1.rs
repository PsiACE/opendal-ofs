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

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::validation::{validate_publication, validate_snapshot};
use super::{
    ContentRef, DirectoryPrecondition, DirectoryRecord, FileVersionRecord, NamespacePublication,
    NamespaceSnapshot, NodePrecondition, NodeRecord, managed_generation, managed_generation_number,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, DirectoryEntry, FileVersionId, NodeAttributes, NodeId, NodeKind,
    OperationId, VolumeId,
};
use crate::managed::metadata::d1::{D1Session, D1Statement, statement};
use crate::managed::{ManagedError, ManagedErrorKind};

const HEADS: &str = "ofs_managed_v1_namespace_heads";
const TRANSACTIONS: &str = "ofs_managed_v1_namespace_transactions";
const RESULTS: &str = "ofs_managed_v1_namespace_results";

#[derive(Clone, Debug)]
pub(crate) struct D1NamespaceObservation {
    pub(crate) snapshot: NamespaceSnapshot,
    revision: u64,
}

#[derive(Clone)]
pub(crate) struct D1Namespace {
    volume_id: VolumeId,
    session: D1Session,
}

impl D1Namespace {
    pub(crate) fn new(volume_id: VolumeId, session: D1Session) -> Self {
        Self { volume_id, session }
    }

    pub(crate) async fn observe(&self) -> Result<Option<D1NamespaceObservation>, ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "SELECT h.revision, t.payload_json FROM {HEADS} h JOIN {TRANSACTIONS} t ON t.store_key = h.store_key AND t.operation_id = h.target_operation WHERE h.store_key = ? AND h.volume_id = ?"
            ),
            vec![self.store_key().into(), self.volume().into()],
        ));
        let results = self.session.query(batch, "read Managed namespace").await?;
        let rows = rows(&results, 3, "read Managed namespace")?;
        let [row] = rows else {
            return if rows.is_empty() {
                Ok(None)
            } else {
                Err(corrupt(
                    "read Managed namespace",
                    "D1 returned duplicate heads",
                ))
            };
        };
        let revision = integer(row, "revision", "read Managed namespace")?;
        let payload = text(row, "payload_json", "read Managed namespace")?;
        let stored: StoredPublication = decode(payload, "read Managed namespace")?;
        stored.validate(self.volume_id)?;
        let snapshot = stored.target.into_snapshot()?;
        if snapshot.volume_id != self.volume_id {
            return Err(corrupt(
                "read Managed namespace",
                "snapshot belongs to another volume",
            ));
        }
        validate_snapshot(&snapshot)
            .map_err(|_| corrupt("read Managed namespace", "snapshot is invalid"))?;
        Ok(Some(D1NamespaceObservation { snapshot, revision }))
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<&D1NamespaceObservation>,
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
        let stored = StoredPublication::from(publication);
        let payload = encode(&stored, "publish Managed namespace")?;
        let operation = hex(publication.operation.as_bytes());
        let target_sequence = sqlite_integer(publication.target.cursor.sequence())?;
        let parent_sequence = sqlite_integer(publication.parent.sequence())?;
        let parent_operation = publication
            .parent
            .operation()
            .map(|value| hex(value.as_bytes()));
        let mut batch = schema_statements();
        batch.extend([
            statement(
                format!(
                    "INSERT OR IGNORE INTO {TRANSACTIONS} (store_key, operation_id, payload_json, parent_sequence, parent_operation, target_sequence) VALUES (?, ?, ?, ?, ?, ?)"
                ),
                vec![
                    self.store_key().into(),
                    operation.clone().into(),
                    payload.clone().into(),
                    parent_sequence.into(),
                    option_text(parent_operation.clone()),
                    target_sequence.into(),
                ],
            ),
            statement(
                format!(
                    "SELECT payload_json FROM {TRANSACTIONS} WHERE store_key = ? AND operation_id = ?"
                ),
                vec![self.store_key().into(), operation.clone().into()],
            ),
        ]);
        batch.push(match observed {
            Some(observed) => statement(
                format!(
                    "UPDATE {HEADS} SET revision = revision + 1, target_sequence = ?, target_operation = ? WHERE store_key = ? AND volume_id = ? AND revision = ? AND target_sequence = ? AND target_operation IS ? AND EXISTS (SELECT 1 FROM {TRANSACTIONS} WHERE store_key = ? AND operation_id = ? AND payload_json = ?) RETURNING revision"
                ),
                vec![
                    target_sequence.into(),
                    operation.clone().into(),
                    self.store_key().into(),
                    self.volume().into(),
                    sqlite_integer(observed.revision)?.into(),
                    parent_sequence.into(),
                    option_text(parent_operation.clone()),
                    self.store_key().into(),
                    operation.clone().into(),
                    payload.clone().into(),
                ],
            ),
            None => statement(
                format!(
                    "INSERT OR IGNORE INTO {HEADS} (store_key, volume_id, revision, target_sequence, target_operation) SELECT ?, ?, 1, ?, ? WHERE ? = 0 AND ? IS NULL AND EXISTS (SELECT 1 FROM {TRANSACTIONS} WHERE store_key = ? AND operation_id = ? AND payload_json = ?) RETURNING revision"
                ),
                vec![
                    self.store_key().into(),
                    self.volume().into(),
                    target_sequence.into(),
                    operation.clone().into(),
                    parent_sequence.into(),
                    option_text(parent_operation.clone()),
                    self.store_key().into(),
                    operation.clone().into(),
                    payload.clone().into(),
                ],
            ),
        });
        batch.extend([
            statement(
                format!(
                    "INSERT OR IGNORE INTO {RESULTS} (store_key, operation_id, target_sequence) SELECT ?, ?, ? FROM {HEADS} WHERE store_key = ? AND volume_id = ? AND target_sequence = ? AND target_operation = ?"
                ),
                vec![
                    self.store_key().into(),
                    operation.clone().into(),
                    target_sequence.into(),
                    self.store_key().into(),
                    self.volume().into(),
                    target_sequence.into(),
                    operation.clone().into(),
                ],
            ),
            statement(
                format!(
                    "SELECT target_sequence FROM {RESULTS} WHERE store_key = ? AND operation_id = ?"
                ),
                vec![self.store_key().into(), operation.into()],
            ),
        ]);

        let results = match self.session.query(batch, "publish Managed namespace").await {
            Ok(results) => results,
            Err(_) => {
                return match self.resolve(publication.operation).await {
                    Ok(CommitOutcome::Committed(cursor)) => Ok(CommitOutcome::Committed(cursor)),
                    _ => Ok(CommitOutcome::Unknown),
                };
            }
        };
        let transaction = rows(&results, 4, "publish Managed namespace")?;
        let [transaction] = transaction else {
            return Err(corrupt(
                "publish Managed namespace",
                "D1 omitted the transaction",
            ));
        };
        if text(transaction, "payload_json", "publish Managed namespace")? != payload {
            return Err(ManagedError::new(
                ManagedErrorKind::Conflict,
                "publish Managed namespace",
                "operation identity was reused with another payload",
            ));
        }
        if !rows(&results, 7, "publish Managed namespace")?.is_empty() {
            return Ok(CommitOutcome::Committed(publication.target.cursor));
        }
        self.outcome_after_race(publication.operation).await
    }

    pub(crate) async fn resolve(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "SELECT target_sequence FROM {RESULTS} WHERE store_key = ? AND operation_id = ?"
            ),
            vec![self.store_key().into(), hex(operation.as_bytes()).into()],
        ));
        let results = self
            .session
            .query(batch, "resolve Managed publication")
            .await?;
        let rows = rows(&results, 3, "resolve Managed publication")?;
        let [row] = rows else {
            return if rows.is_empty() {
                Ok(CommitOutcome::Absent)
            } else {
                Err(corrupt(
                    "resolve Managed publication",
                    "D1 returned duplicate results",
                ))
            };
        };
        let sequence = integer(row, "target_sequence", "resolve Managed publication")?;
        let sequence = NonZeroU64::new(sequence)
            .ok_or_else(|| corrupt("resolve Managed publication", "committed cursor is invalid"))?;
        Ok(CommitOutcome::Committed(ChangeCursor::at(
            sequence, operation,
        )))
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

    fn store_key(&self) -> String {
        self.session.store_key().to_owned()
    }

    fn volume(&self) -> String {
        hex(self.volume_id.as_bytes())
    }
}

fn schema_statements() -> Vec<D1Statement> {
    vec![
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {HEADS} (store_key TEXT PRIMARY KEY, volume_id TEXT NOT NULL, revision INTEGER NOT NULL, target_sequence INTEGER NOT NULL, target_operation TEXT NOT NULL)"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {TRANSACTIONS} (store_key TEXT NOT NULL, operation_id TEXT NOT NULL, payload_json TEXT NOT NULL, parent_sequence INTEGER NOT NULL, parent_operation TEXT, target_sequence INTEGER NOT NULL, PRIMARY KEY (store_key, operation_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {RESULTS} (store_key TEXT NOT NULL, operation_id TEXT NOT NULL, target_sequence INTEGER NOT NULL, PRIMARY KEY (store_key, operation_id))"
            ),
            Vec::new(),
        ),
    ]
}

fn rows<'a>(
    results: &'a [crate::managed::metadata::d1::D1Result],
    index: usize,
    action: &'static str,
) -> Result<&'a [Value], ManagedError> {
    results
        .get(index)
        .map(|result| result.results.as_slice())
        .ok_or_else(|| corrupt(action, "D1 omitted a query result"))
}

fn text<'a>(row: &'a Value, field: &str, action: &'static str) -> Result<&'a str, ManagedError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid namespace row"))
}

fn integer(row: &Value, field: &str, action: &'static str) -> Result<u64, ManagedError> {
    row.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid namespace row"))
}

fn sqlite_integer(value: u64) -> Result<i64, ManagedError> {
    i64::try_from(value).map_err(|_| {
        invalid(
            "publish Managed namespace",
            "change sequence exceeds D1 integer range",
        )
    })
}

fn option_text(value: Option<String>) -> Value {
    value.map_or(Value::Null, Value::String)
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

fn encode(value: &impl Serialize, action: &'static str) -> Result<String, ManagedError> {
    serde_json::to_string(value).map_err(|_| invalid(action, "namespace record cannot be encoded"))
}

fn decode<'a, T: Deserialize<'a>>(value: &'a str, action: &'static str) -> Result<T, ManagedError> {
    serde_json::from_str(value).map_err(|_| corrupt(action, "namespace record is invalid"))
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredPublication {
    operation: [u8; 16],
    parent: StoredCursor,
    expected_nodes: Vec<StoredNodePrecondition>,
    expected_directories: Vec<StoredDirectoryPrecondition>,
    target: StoredSnapshot,
}

impl From<&NamespacePublication> for StoredPublication {
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
        stored.expected_nodes.sort_by_key(|value| value.node);
        stored
            .expected_directories
            .sort_by_key(|value| value.directory);
        stored
    }
}

impl StoredPublication {
    fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        let parent = self.parent.into_cursor()?;
        let target = self.target.cursor.into_cursor()?;
        if self.target.volume_id != *volume_id.as_bytes()
            || target.operation() != Some(OperationId::from_bytes(self.operation))
            || parent.sequence().checked_add(1) != Some(target.sequence())
        {
            return Err(corrupt(
                "read Managed namespace",
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
            operation: cursor.operation().map(|value| *value.as_bytes()),
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
            .map(|record| (record.id, record))
            .collect::<BTreeMap<_, _>>();
        let directories = self
            .directories
            .into_iter()
            .map(StoredDirectory::into_record)
            .map(|record| (record.node, record))
            .collect::<BTreeMap<_, _>>();
        let file_versions = self
            .file_versions
            .into_iter()
            .map(StoredFileVersion::into_record)
            .map(|record| (record.id, record))
            .collect::<BTreeMap<_, _>>();
        if nodes.len() != node_count
            || directories.len() != directory_count
            || file_versions.len() != file_version_count
        {
            return Err(corrupt(
                "read Managed namespace",
                "namespace contains duplicate records",
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
            file_version: node.file_version.map(|value| *value.as_bytes()),
        }
    }
}

impl StoredNode {
    fn into_record(self) -> NodeRecord {
        NodeRecord {
            id: NodeId::from_bytes(self.id),
            generation: managed_generation(self.generation),
            kind: self.kind.into(),
            attributes: self.attributes.into(),
            file_version: self.file_version.map(FileVersionId::from_bytes),
        }
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
    fn into_record(self) -> DirectoryRecord {
        DirectoryRecord {
            node: NodeId::from_bytes(self.node),
            generation: managed_generation(self.generation),
            entries: self
                .entries
                .into_iter()
                .map(|(name, entry)| (name, entry.into()))
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
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
    fn into_record(self) -> FileVersionRecord {
        FileVersionRecord {
            id: FileVersionId::from_bytes(self.id),
            logical_size: self.logical_size,
            logical_digest: self.logical_digest,
            content: ContentRef {
                digest: self.content_digest,
                logical_length: self.content_length,
            },
        }
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
            expected_generation: condition.expected_generation.as_ref().map(|value| {
                managed_generation_number(value)
                    .expect("validated Managed node precondition generation")
            }),
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
            expected_generation: condition.expected_generation.as_ref().map(|value| {
                managed_generation_number(value)
                    .expect("validated Managed directory precondition generation")
            }),
        }
    }
}
