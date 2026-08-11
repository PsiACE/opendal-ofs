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

use opendal::{Buffer, Operator};
use serde::{Deserialize, Serialize};

use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, FileVersion, FileVersionId, Generation,
    NodeAttributes, NodeId, NodeKind, NodeRecord, OperationId, VolumeError, VolumeErrorKind,
    VolumeId, VolumeSnapshot,
};

use super::format::ManagedFormat;
use super::index::{PageRef, read_index, write_index};
use super::object;
use super::record::Record;

const HEAD_KEY: &str = "managed/1/head";
const HEAD_RECORD: Record = Record::new(*b"OFSHEAD1", 64 * 1024);
const COMMIT_RECORD: Record = Record::new(*b"OFSCMIT1", 1024 * 1024);

#[derive(Clone)]
pub struct ManagedVolume {
    format: ManagedFormat,
    operator: Operator,
}

pub struct ManagedObservation {
    pub snapshot: VolumeSnapshot,
    revision: String,
    changes: BTreeMap<ChangeCursor, ChangeRecord>,
    operations: BTreeMap<OperationId, OperationRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    pub(super) namespace_commit: NamespaceCommitRef,
    pub(super) maintenance: Option<GcFence>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NamespaceCommitRef {
    pub(super) digest: [u8; 32],
    pub(super) encoded_length: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GcFence {
    pub(super) owner: OperationId,
    pub(super) namespace_commit: NamespaceCommitRef,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    retained_change_floor: ChangeCursor,
    node_index_root: PageRef,
    directory_entry_index_root: PageRef,
    file_version_index_root: PageRef,
    change_log_root: PageRef,
    operation_result_index_root: PageRef,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct DirectoryKey {
    parent: NodeId,
    name: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ChangeRecord {
    previous: ChangeCursor,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct OperationRecord {
    cursor: ChangeCursor,
}

struct StoredNamespace {
    snapshot: VolumeSnapshot,
    changes: BTreeMap<ChangeCursor, ChangeRecord>,
    operations: BTreeMap<OperationId, OperationRecord>,
}

impl ManagedVolume {
    pub(super) fn new(format: ManagedFormat, operator: Operator) -> Self {
        Self { format, operator }
    }

    pub(super) async fn open(
        format: ManagedFormat,
        operator: Operator,
    ) -> Result<Self, VolumeError> {
        let volume = Self::new(format, operator);
        volume.observe().await?;
        Ok(volume)
    }

    pub const fn id(&self) -> VolumeId {
        self.format.volume_id()
    }

    pub(super) async fn initialize(&self) -> Result<(), VolumeError> {
        let snapshot = empty_snapshot(self.format);
        let namespace_commit = self
            .write_namespace(&snapshot, &BTreeMap::new(), &BTreeMap::new())
            .await?;
        let bytes = HEAD_RECORD.encode(&Head {
            namespace_commit,
            maintenance: None,
        })?;
        if object::create(&self.operator, HEAD_KEY, bytes).await? {
            return Ok(());
        }
        self.observe().await.map(drop)
    }

    pub async fn observe(&self) -> Result<ManagedObservation, VolumeError> {
        let (head, revision) = self.read_head().await?;
        if head.maintenance.is_some() {
            return Err(VolumeError::new(
                VolumeErrorKind::Conflict,
                "open Managed volume: data collection is active",
            ));
        }
        let stored = self.read_namespace(head.namespace_commit).await?;
        Ok(ManagedObservation {
            snapshot: stored.snapshot,
            revision,
            changes: stored.changes,
            operations: stored.operations,
        })
    }

    pub(super) async fn read_head(&self) -> Result<(Head, String), VolumeError> {
        let (bytes, revision) = object::read_with_revision(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .ok_or_else(|| {
            VolumeError::new(
                VolumeErrorKind::Corrupt,
                "open Managed volume: namespace head is missing",
            )
        })?;
        let head: Head = HEAD_RECORD.decode(&bytes)?;
        Ok((head, revision))
    }

    pub async fn publish(
        &self,
        observed: &ManagedObservation,
        target: VolumeSnapshot,
    ) -> Result<(), VolumeError> {
        target.validate()?;
        if target.volume_id != self.id()
            || target.root != self.format.root_node_id()
            || target.cursor.sequence() != observed.snapshot.cursor.sequence() + 1
            || target.cursor.operation().is_none()
        {
            return Err(VolumeError::new(
                VolumeErrorKind::Invalid,
                "publish Managed namespace: publication ancestry is invalid",
            ));
        }
        let operation = target
            .cursor
            .operation()
            .expect("validated publication has an operation identity");
        let mut changes = observed.changes.clone();
        changes.insert(
            target.cursor,
            ChangeRecord {
                previous: observed.snapshot.cursor,
            },
        );
        let mut operations = observed.operations.clone();
        operations.insert(
            operation,
            OperationRecord {
                cursor: target.cursor,
            },
        );
        let namespace_commit = self.write_namespace(&target, &changes, &operations).await?;
        let bytes = HEAD_RECORD.encode(&Head {
            namespace_commit,
            maintenance: None,
        })?;
        if object::replace(&self.operator, HEAD_KEY, &observed.revision, bytes).await? {
            return Ok(());
        }
        let current = self.observe().await?;
        if current.operations.get(&operation).is_some_and(|result| {
            result.cursor == target.cursor && current.snapshot.cursor == target.cursor
        }) {
            Ok(())
        } else {
            Err(VolumeError::new(
                VolumeErrorKind::Conflict,
                "publish Managed namespace: observed generation changed",
            ))
        }
    }

    pub async fn operation_committed(
        &self,
        operation: OperationId,
        observed: &ManagedObservation,
    ) -> Result<bool, VolumeError> {
        Ok(observed.operations.contains_key(&operation))
    }

    pub(crate) fn operator(&self) -> &Operator {
        &self.operator
    }

    pub(super) async fn replace_head(
        &self,
        expected_revision: &str,
        head: &Head,
    ) -> Result<bool, VolumeError> {
        object::replace(
            &self.operator,
            HEAD_KEY,
            expected_revision,
            HEAD_RECORD.encode(head)?,
        )
        .await
    }

    pub(super) async fn snapshot_at(
        &self,
        reference: NamespaceCommitRef,
    ) -> Result<VolumeSnapshot, VolumeError> {
        self.read_namespace(reference)
            .await
            .map(|stored| stored.snapshot)
    }

    async fn write_namespace(
        &self,
        snapshot: &VolumeSnapshot,
        changes: &BTreeMap<ChangeCursor, ChangeRecord>,
        operations: &BTreeMap<OperationId, OperationRecord>,
    ) -> Result<NamespaceCommitRef, VolumeError> {
        snapshot.validate()?;
        let mut directory_entries = BTreeMap::new();
        for (parent, directory) in &snapshot.directories {
            for (name, entry) in &directory.entries {
                directory_entries.insert(
                    DirectoryKey {
                        parent: *parent,
                        name: name.clone(),
                    },
                    *entry,
                );
            }
        }

        let commit = NamespaceCommit {
            volume_id: snapshot.volume_id,
            change_cursor: snapshot.cursor,
            retained_change_floor: ChangeCursor::Genesis,
            node_index_root: write_index(&self.operator, &snapshot.nodes).await?,
            directory_entry_index_root: write_index(&self.operator, &directory_entries).await?,
            file_version_index_root: write_index(&self.operator, &snapshot.file_versions).await?,
            change_log_root: write_index(&self.operator, changes).await?,
            operation_result_index_root: write_index(&self.operator, operations).await?,
        };
        let bytes = COMMIT_RECORD.encode(&commit)?;
        let digest: [u8; 32] = blake3::hash(&bytes).into();
        let reference = NamespaceCommitRef {
            digest,
            encoded_length: bytes
                .len()
                .try_into()
                .map_err(|_| invalid("namespace commit length overflows"))?,
        };
        object::create_immutable(&self.operator, &commit_key(digest), Buffer::from(bytes)).await?;
        Ok(reference)
    }

    async fn read_namespace(
        &self,
        reference: NamespaceCommitRef,
    ) -> Result<StoredNamespace, VolumeError> {
        let length = usize::try_from(reference.encoded_length)
            .ok()
            .filter(|length| *length <= COMMIT_RECORD.maximum_encoded_bytes())
            .ok_or_else(|| corrupt("namespace commit length is invalid"))?;
        let bytes = object::read(&self.operator, &commit_key(reference.digest), length)
            .await?
            .ok_or_else(|| corrupt("namespace commit is missing"))?;
        if bytes.len() != length || blake3::hash(&bytes).as_bytes() != &reference.digest {
            return Err(corrupt("namespace commit does not match its reference"));
        }
        let commit: NamespaceCommit = COMMIT_RECORD.decode(&bytes)?;
        if commit.volume_id != self.id() {
            return Err(corrupt("namespace commit belongs to another volume"));
        }
        let nodes: BTreeMap<NodeId, NodeRecord> =
            read_index(&self.operator, &commit.node_index_root).await?;
        let directory_entries: BTreeMap<DirectoryKey, DirectoryEntry> =
            read_index(&self.operator, &commit.directory_entry_index_root).await?;
        let file_versions: BTreeMap<FileVersionId, FileVersion> =
            read_index(&self.operator, &commit.file_version_index_root).await?;
        let changes = read_index(&self.operator, &commit.change_log_root).await?;
        let operations = read_index(&self.operator, &commit.operation_result_index_root).await?;

        let mut directories = nodes
            .iter()
            .filter(|(_, node)| node.kind == NodeKind::Directory)
            .map(|(id, node)| {
                (
                    *id,
                    DirectoryRecord {
                        node: *id,
                        generation: node.generation.clone(),
                        entries: BTreeMap::new(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        for (key, entry) in directory_entries {
            directories
                .get_mut(&key.parent)
                .ok_or_else(|| corrupt("directory entry parent is missing"))?
                .entries
                .insert(key.name, entry);
        }
        let snapshot = VolumeSnapshot {
            volume_id: commit.volume_id,
            cursor: commit.change_cursor,
            root: self.format.root_node_id(),
            nodes,
            directories,
            file_versions,
        };
        snapshot.validate()?;
        Ok(StoredNamespace {
            snapshot,
            changes,
            operations,
        })
    }
}

fn empty_snapshot(format: ManagedFormat) -> VolumeSnapshot {
    let volume_id = format.volume_id();
    let root = format.root_node_id();
    let generation = Generation::from_bytes(0_u64.to_be_bytes().to_vec());
    VolumeSnapshot {
        volume_id,
        cursor: ChangeCursor::Genesis,
        root,
        nodes: BTreeMap::from([(
            root,
            NodeRecord {
                id: root,
                generation: generation.clone(),
                kind: NodeKind::Directory,
                attributes: NodeAttributes::default(),
                file_version: None,
            },
        )]),
        directories: BTreeMap::from([(
            root,
            DirectoryRecord {
                node: root,
                generation,
                entries: BTreeMap::new(),
            },
        )]),
        file_versions: BTreeMap::new(),
    }
}

fn commit_key(digest: [u8; 32]) -> String {
    let digest = blake3::Hash::from_bytes(digest).to_hex();
    format!("managed/1/objects/commit/{}/{digest}", &digest[..2])
}

fn invalid(message: &'static str) -> VolumeError {
    VolumeError::new(
        VolumeErrorKind::Invalid,
        format!("publish Managed namespace: {message}"),
    )
}

fn corrupt(message: &'static str) -> VolumeError {
    VolumeError::new(
        VolumeErrorKind::Corrupt,
        format!("read Managed namespace: {message}"),
    )
}
