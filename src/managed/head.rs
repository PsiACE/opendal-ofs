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
    ChangeCursor, Digest, DirectoryEntry, DirectoryRecord, FileVersion, Generation, NodeAttributes,
    NodeId, NodeKind, NodeRecord, OperationId, VolumeError, VolumeErrorKind, VolumeId,
    VolumeSnapshot,
};

use super::format::ManagedFormat;
use super::index::{PageRef, read_index, visit_index, write_index, write_index_reusing};
use super::object;
use super::record::Record;

const HEAD_KEY: &str = "managed/1/head";
const HEAD_RECORD: Record = Record::new(*b"OFSHEAD1", 64 * 1024);
const COMMIT_RECORD: Record = Record::new(*b"OFSCMIT1", 1024 * 1024);
const RETAINED_NAMESPACE_COMMITS: usize = 33;

#[derive(Clone)]
pub struct ManagedVolume {
    format: ManagedFormat,
    operator: Operator,
}

pub struct ManagedObservation {
    pub snapshot: VolumeSnapshot,
    namespace_revision: NamespaceRevision,
    revision: String,
    changes: BTreeMap<ChangeCursor, ChangeRecord>,
    operations: BTreeMap<OperationId, OperationRecord>,
    commit: NamespaceCommit,
    directory_entries: BTreeMap<DirectoryKey, DirectoryEntry>,
    objects: IndexObjects,
}

impl ManagedObservation {
    pub const fn revision(&self) -> NamespaceRevision {
        self.namespace_revision
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    pub(super) namespace_commit: NamespaceRevision,
    pub(super) maintenance: Option<GcFence>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceRevision {
    commit: Digest,
    pub(super) encoded_length: u64,
    cursor: ChangeCursor,
}

impl NamespaceRevision {
    pub const fn cursor(self) -> ChangeCursor {
        self.cursor
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GcFence {
    pub(super) owner: OperationId,
    pub(super) namespace_commit: NamespaceRevision,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    retained_change_floor: ChangeCursor,
    previous: Option<NamespaceRevision>,
    node_index_root: PageRef,
    directory_entry_index_root: PageRef,
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
    commit: NamespaceCommit,
    directory_entries: BTreeMap<DirectoryKey, DirectoryEntry>,
    objects: IndexObjects,
}

struct IndexObjects {
    nodes: BTreeMap<crate::filesystem::Digest, u64>,
    directory_entries: BTreeMap<crate::filesystem::Digest, u64>,
    changes: BTreeMap<crate::filesystem::Digest, u64>,
    operations: BTreeMap<crate::filesystem::Digest, u64>,
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
            .write_namespace(
                &snapshot,
                &BTreeMap::new(),
                &BTreeMap::new(),
                ChangeCursor::Genesis,
                None,
            )
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
            namespace_revision: head.namespace_commit,
            revision,
            changes: stored.changes,
            operations: stored.operations,
            commit: stored.commit,
            directory_entries: stored.directory_entries,
            objects: stored.objects,
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

    pub async fn prepare_publication(
        &self,
        observed: &ManagedObservation,
        target: VolumeSnapshot,
    ) -> Result<NamespaceRevision, VolumeError> {
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
        let retained_change_floor = changes
            .keys()
            .rev()
            .nth(RETAINED_NAMESPACE_COMMITS - 1)
            .copied()
            .unwrap_or(ChangeCursor::Genesis);
        changes.retain(|cursor, _| *cursor >= retained_change_floor);
        operations.retain(|_, result| result.cursor >= retained_change_floor);
        self.write_namespace(
            &target,
            &changes,
            &operations,
            retained_change_floor,
            Some(observed),
        )
        .await
    }

    pub async fn commit_publication(
        &self,
        observed: &ManagedObservation,
        target: NamespaceRevision,
    ) -> Result<(), VolumeError> {
        if target.cursor.sequence() != observed.snapshot.cursor.sequence() + 1
            || target.cursor.operation().is_none()
        {
            return Err(VolumeError::new(
                VolumeErrorKind::Invalid,
                "publish Managed namespace: prepared publication ancestry is invalid",
            ));
        }
        let bytes = HEAD_RECORD.encode(&Head {
            namespace_commit: target,
            maintenance: None,
        })?;
        if object::replace(&self.operator, HEAD_KEY, &observed.revision, bytes).await? {
            return Ok(());
        }
        let current = self.observe().await?;
        let operation = target
            .cursor
            .operation()
            .expect("validated publication has an operation identity");
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

    pub async fn snapshot(
        &self,
        revision: NamespaceRevision,
    ) -> Result<VolumeSnapshot, VolumeError> {
        self.read_namespace(revision)
            .await
            .map(|stored| stored.snapshot)
    }

    pub async fn snapshot_if_present(
        &self,
        revision: NamespaceRevision,
    ) -> Result<Option<VolumeSnapshot>, VolumeError> {
        self.read_namespace_if_present(revision)
            .await
            .map(|stored| stored.map(|stored| stored.snapshot))
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

    pub(super) async fn visit_reachable_objects(
        &self,
        reference: NamespaceRevision,
        mut visit: impl FnMut(String, u64) -> Result<(), VolumeError>,
    ) -> Result<(), VolumeError> {
        let mut current = Some(reference);
        for _ in 0..RETAINED_NAMESPACE_COMMITS {
            let Some(reference) = current else {
                break;
            };
            let commit = self.read_commit(reference).await?;
            visit(commit_key(reference.commit), reference.encoded_length)?;

            let containers = visit_index::<NodeId, NodeRecord>(
                &self.operator,
                &commit.node_index_root,
                |_, node| {
                    if let Some(version) = node.file_version
                        && let Some(object) = super::data::whole_object(&FileVersion::new(version))?
                    {
                        visit(super::data::whole_object_key(object.digest), object.length)?;
                    }
                    Ok(())
                },
            )
            .await?;
            visit_containers(containers, &mut visit)?;

            let containers = visit_index::<DirectoryKey, DirectoryEntry>(
                &self.operator,
                &commit.directory_entry_index_root,
                |_, _| Ok(()),
            )
            .await?;
            visit_containers(containers, &mut visit)?;

            let containers = visit_index::<ChangeCursor, ChangeRecord>(
                &self.operator,
                &commit.change_log_root,
                |_, _| Ok(()),
            )
            .await?;
            visit_containers(containers, &mut visit)?;

            let containers = visit_index::<OperationId, OperationRecord>(
                &self.operator,
                &commit.operation_result_index_root,
                |_, _| Ok(()),
            )
            .await?;
            visit_containers(containers, &mut visit)?;
            current = commit.previous;
        }
        Ok(())
    }

    async fn write_namespace(
        &self,
        snapshot: &VolumeSnapshot,
        changes: &BTreeMap<ChangeCursor, ChangeRecord>,
        operations: &BTreeMap<OperationId, OperationRecord>,
        retained_change_floor: ChangeCursor,
        previous: Option<&ManagedObservation>,
    ) -> Result<NamespaceRevision, VolumeError> {
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

        let node_index_root = match previous {
            Some(previous) if previous.snapshot.nodes == snapshot.nodes => {
                previous.commit.node_index_root.clone()
            }
            Some(previous) => {
                write_index_reusing(&self.operator, &snapshot.nodes, &previous.objects.nodes)
                    .await?
            }
            None => write_index(&self.operator, &snapshot.nodes).await?,
        };
        let directory_entry_index_root = match previous {
            Some(previous) if previous.directory_entries == directory_entries => {
                previous.commit.directory_entry_index_root.clone()
            }
            Some(previous) => {
                write_index_reusing(
                    &self.operator,
                    &directory_entries,
                    &previous.objects.directory_entries,
                )
                .await?
            }
            None => write_index(&self.operator, &directory_entries).await?,
        };
        let change_log_root = match previous {
            Some(previous) if previous.changes == *changes => {
                previous.commit.change_log_root.clone()
            }
            Some(previous) => {
                write_index_reusing(&self.operator, changes, &previous.objects.changes).await?
            }
            None => write_index(&self.operator, changes).await?,
        };
        let operation_result_index_root = match previous {
            Some(previous) if previous.operations == *operations => {
                previous.commit.operation_result_index_root.clone()
            }
            Some(previous) => {
                write_index_reusing(&self.operator, operations, &previous.objects.operations)
                    .await?
            }
            None => write_index(&self.operator, operations).await?,
        };

        let commit = NamespaceCommit {
            volume_id: snapshot.volume_id,
            change_cursor: snapshot.cursor,
            retained_change_floor,
            previous: previous.map(|observation| observation.namespace_revision),
            node_index_root,
            directory_entry_index_root,
            change_log_root,
            operation_result_index_root,
        };
        let bytes = COMMIT_RECORD.encode(&commit)?;
        let digest = Digest::from_bytes(blake3::hash(&bytes).into());
        let reference = NamespaceRevision {
            commit: digest,
            encoded_length: bytes
                .len()
                .try_into()
                .map_err(|_| invalid("namespace commit length overflows"))?,
            cursor: snapshot.cursor,
        };
        object::create_immutable(&self.operator, &commit_key(digest), Buffer::from(bytes)).await?;
        Ok(reference)
    }

    async fn read_namespace(
        &self,
        reference: NamespaceRevision,
    ) -> Result<StoredNamespace, VolumeError> {
        let commit = self.read_commit(reference).await?;
        self.read_namespace_indexes(commit).await
    }

    async fn read_namespace_if_present(
        &self,
        reference: NamespaceRevision,
    ) -> Result<Option<StoredNamespace>, VolumeError> {
        let Some(commit) = self.read_commit_if_present(reference).await? else {
            return Ok(None);
        };
        self.read_namespace_indexes(commit).await.map(Some)
    }

    async fn read_namespace_indexes(
        &self,
        commit: NamespaceCommit,
    ) -> Result<StoredNamespace, VolumeError> {
        let (nodes, node_objects): (BTreeMap<NodeId, NodeRecord>, _) =
            read_index(&self.operator, &commit.node_index_root).await?;
        let (directory_entries, directory_entry_objects): (
            BTreeMap<DirectoryKey, DirectoryEntry>,
            _,
        ) = read_index(&self.operator, &commit.directory_entry_index_root).await?;
        let (changes, change_objects) = read_index(&self.operator, &commit.change_log_root).await?;
        let (operations, operation_objects) =
            read_index(&self.operator, &commit.operation_result_index_root).await?;

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
        for (key, entry) in &directory_entries {
            directories
                .get_mut(&key.parent)
                .ok_or_else(|| corrupt("directory entry parent is missing"))?
                .entries
                .insert(key.name.clone(), *entry);
        }
        let file_versions = nodes
            .values()
            .filter_map(|node| node.file_version)
            .map(|id| (id, FileVersion::new(id)))
            .collect();
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
            commit,
            directory_entries,
            objects: IndexObjects {
                nodes: node_objects,
                directory_entries: directory_entry_objects,
                changes: change_objects,
                operations: operation_objects,
            },
        })
    }

    async fn read_commit(
        &self,
        reference: NamespaceRevision,
    ) -> Result<NamespaceCommit, VolumeError> {
        self.read_commit_if_present(reference)
            .await?
            .ok_or_else(|| corrupt("namespace commit is missing"))
    }

    async fn read_commit_if_present(
        &self,
        reference: NamespaceRevision,
    ) -> Result<Option<NamespaceCommit>, VolumeError> {
        let length = usize::try_from(reference.encoded_length)
            .ok()
            .filter(|length| *length <= COMMIT_RECORD.maximum_encoded_bytes())
            .ok_or_else(|| corrupt("namespace commit length is invalid"))?;
        let bytes = object::read(&self.operator, &commit_key(reference.commit), length)
            .await?
            .map_or_else(|| Ok(None), |bytes| Ok(Some(bytes)))?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        if bytes.len() != length || blake3::hash(&bytes).as_bytes() != reference.commit.as_bytes() {
            return Err(corrupt("namespace commit does not match its reference"));
        }
        let commit: NamespaceCommit = COMMIT_RECORD.decode(&bytes)?;
        if commit.volume_id != self.id() {
            return Err(corrupt("namespace commit belongs to another volume"));
        }
        if commit.change_cursor != reference.cursor {
            return Err(corrupt(
                "namespace commit cursor does not match its reference",
            ));
        }
        Ok(Some(commit))
    }
}

fn visit_containers(
    containers: BTreeMap<crate::filesystem::Digest, u64>,
    visit: &mut impl FnMut(String, u64) -> Result<(), VolumeError>,
) -> Result<(), VolumeError> {
    for (digest, length) in containers {
        visit(super::container::object_key(digest), length)?;
    }
    Ok(())
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

fn commit_key(digest: Digest) -> String {
    let digest = blake3::Hash::from_bytes(*digest.as_bytes()).to_hex();
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
