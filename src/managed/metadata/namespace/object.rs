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
use std::io::Cursor;

use futures::{StreamExt as _, TryStreamExt as _, stream};
use opendal::Operator;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::change::NamespaceChange;
use super::validation::{validate_publication, validate_snapshot};
use super::{
    DirectoryPrecondition, DirectoryRecord, FileVersionRecord, NamespaceGcSweep,
    NamespacePublication, NamespaceSnapshot, NodePrecondition, NodeRecord,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, DirectoryEntry, FileVersionId, Generation, NodeId, OperationId,
    VolumeId,
};
use crate::managed::format::sstable::{
    self, Record as TableRecord, RecordGroup as TableRecordGroup, TableRef,
};
use crate::managed::metadata::object::{self, ensure_immutable, read_content_addressed};
use crate::managed::{ManagedError, ManagedErrorKind};

#[cfg(test)]
use crate::filesystem::{NodeAttributes, NodeKind};

const HEAD_KEY: &str = ".ofs/managed/metadata/v1/head.ofs";
const MANIFEST_ROOT: &str = ".ofs/managed/metadata/v1/manifests/sha256";
const SSTABLE_ROOT: &str = ".ofs/managed/metadata/v1/sstables/sha256";
const MANIFEST_MAGIC: &[u8] = b"OFS1MAN\0";
const HEAD_MAGIC: &[u8; 8] = b"OFS1HDZ1";
const FORMAT_MAJOR: u16 = 1;
const MAX_TAIL_TRANSACTIONS: u16 = 32;
const MAX_TAIL_BYTES: usize = 128 * 1024;
const MAX_HEAD_BYTES: usize = 256 * 1024;
const HEAD_COMPRESSION_LEVEL: i32 = 3;
const MAX_CHECKPOINT_UPLOADS: usize = 8;
const MAX_CHECKPOINT_READS: usize = 8;
const NODE_PREFIX: u8 = 1;
const DIRECTORY_PREFIX: u8 = 2;
const DIRECTORY_ENTRY_PREFIX: u8 = 3;
const FILE_VERSION_PREFIX: u8 = 4;
const OPERATION_RESULT_PREFIX: u8 = 5;
const SNAPSHOT_PARTITION_PREFIX: u8 = 1;
const OPERATION_PARTITION_PREFIX: u8 = 2;

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
    manifest: Option<StoredManifest>,
}

impl std::fmt::Debug for ObservationAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObservationAuthority")
            .field("head", &self.head)
            .field(
                "manifest_tables",
                &self.manifest.as_ref().map_or(0, |value| value.tables.len()),
            )
            .field("tail_changes", &self.head.tail.len())
            .finish()
    }
}

#[allow(async_fn_in_trait)]
pub(crate) trait NamespaceHeadBackend: Clone + Send + Sync {
    type Revision: Clone + Send + Sync;

    async fn read(
        &self,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, Self::Revision)>, ManagedError>;
    async fn read_bytes(&self, action: &'static str) -> Result<Option<Vec<u8>>, ManagedError>;
    async fn create(&self, bytes: Vec<u8>, action: &'static str) -> Result<bool, ManagedError>;
    async fn replace(
        &self,
        revision: &Self::Revision,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError>;
}

#[derive(Clone)]
pub(crate) struct NamespaceStore<B> {
    pub(crate) volume_id: VolumeId,
    pub(crate) operator: Operator,
    pub(crate) backend: B,
}

#[derive(Clone)]
pub(crate) struct ObjectHeadBackend {
    operator: Operator,
}

pub(crate) type ObjectNamespace = NamespaceStore<ObjectHeadBackend>;

impl NamespaceHeadBackend for ObjectHeadBackend {
    type Revision = String;

    async fn read(
        &self,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, Self::Revision)>, ManagedError> {
        object::read_with_revision(&self.operator, HEAD_KEY, action).await
    }

    async fn read_bytes(&self, action: &'static str) -> Result<Option<Vec<u8>>, ManagedError> {
        object::read(&self.operator, HEAD_KEY, action).await
    }

    async fn create(&self, bytes: Vec<u8>, action: &'static str) -> Result<bool, ManagedError> {
        object::create(&self.operator, HEAD_KEY, bytes, action).await
    }

    async fn replace(
        &self,
        revision: &Self::Revision,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        object::replace(&self.operator, HEAD_KEY, revision, bytes, action).await
    }
}

impl NamespaceStore<ObjectHeadBackend> {
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
            backend: ObjectHeadBackend { operator },
        })
    }
}

#[allow(private_bounds)]
impl<B: NamespaceHeadBackend> NamespaceStore<B> {
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
                        manifest: None,
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
        let (snapshot, manifest) = self.recover(&head).await?;
        Ok(NamespaceObservation {
            snapshot,
            revision,
            authority: Box::new(ObservationAuthority {
                head,
                manifest: Some(manifest),
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
        let stored = StoredTransaction::from_publication(publication, base);
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
            // the observation used for CAS. Building a manifest never rereads
            // the remote checkpoint or HEAD.
            let mut previous_manifest = None;
            let mut committed = match observed {
                Some(value) => {
                    let manifest = match &value.authority.manifest {
                        Some(manifest) => manifest.clone(),
                        None => self.read_manifest(&value.authority.head.checkpoint).await?,
                    };
                    let committed = self.read_operation_results(&manifest).await?;
                    previous_manifest = Some(manifest);
                    committed
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
                    cursor: stored.cursor,
                    request_sha256,
                },
            );
            let checkpoint = self
                .checkpoint_full(&publication.target, &committed, previous_manifest.as_ref())
                .await?;
            let bytes = encode_cbor(MANIFEST_MAGIC, &checkpoint, "checkpoint Managed namespace")?;
            let checkpoint_id = sha256(&bytes);
            ensure_immutable(
                &self.operator,
                &manifest_key(&checkpoint_id),
                &bytes,
                "publish Managed namespace",
                ManagedErrorKind::Conflict,
                "operation identity was reused with another payload",
            )
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
                    .replace(&observed.revision, head, "publish Managed namespace")
                    .await
            }
            None => self.backend.create(head, "publish Managed namespace").await,
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
        let manifest = self.read_manifest(&head.checkpoint).await?;
        if let Some(result) = self.read_operation_result(&manifest, operation).await? {
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
    ) -> Result<(NamespaceSnapshot, StoredManifest), ManagedError> {
        head.validate(self.volume_id)?;
        self.recover_bounded(head).await
    }

    async fn recover_bounded(
        &self,
        head: &StoredHead,
    ) -> Result<(NamespaceSnapshot, StoredManifest), ManagedError> {
        let checkpoint = self.read_manifest(&head.checkpoint).await?;
        if checkpoint.major != FORMAT_MAJOR
            || checkpoint.volume_id != self.volume_id
            || checkpoint.cursor != head.checkpoint_cursor
        {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint and HEAD disagree",
            ));
        }
        let mut snapshot = self.read_snapshot(checkpoint.clone()).await?;
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

    async fn checkpoint_full(
        &self,
        snapshot: &NamespaceSnapshot,
        committed: &BTreeMap<OperationId, StoredCommittedResult>,
        previous: Option<&StoredManifest>,
    ) -> Result<StoredManifest, ManagedError> {
        let scope = *self.volume_id.as_bytes();
        let paths = snapshot_partition_keys(snapshot)?;
        let mut groups = BTreeMap::<Vec<u8>, Vec<TableRecord>>::new();
        for node in snapshot.nodes.values() {
            let partition = paths.get(&node.id).ok_or_else(|| {
                invalid(
                    "checkpoint Managed namespace",
                    "snapshot node is not reachable from its root",
                )
            })?;
            groups
                .entry(partition.clone())
                .or_default()
                .push(TableRecord {
                    key: typed_key(NODE_PREFIX, node.id.as_bytes()),
                    value: encode_table_value(node, "checkpoint Managed namespace")?,
                });
        }
        for directory in snapshot.directories.values() {
            let partition = paths.get(&directory.node).ok_or_else(|| {
                invalid(
                    "checkpoint Managed namespace",
                    "snapshot directory is not reachable from its root",
                )
            })?;
            groups
                .entry(partition.clone())
                .or_default()
                .push(TableRecord {
                    key: typed_key(DIRECTORY_PREFIX, directory.node.as_bytes()),
                    value: encode_table_value(
                        &directory.generation,
                        "checkpoint Managed namespace",
                    )?,
                });
            for (name, entry) in &directory.entries {
                let partition = child_partition_key(partition, name)?;
                groups.entry(partition).or_default().push(TableRecord {
                    key: directory_entry_key(directory.node, name),
                    value: encode_table_value(entry, "checkpoint Managed namespace")?,
                });
            }
        }
        let mut version_partitions = BTreeMap::<FileVersionId, Vec<u8>>::new();
        for node in snapshot.nodes.values() {
            if let Some(version) = node.file_version {
                let partition = paths.get(&node.id).ok_or_else(|| {
                    invalid(
                        "checkpoint Managed namespace",
                        "snapshot file is not reachable from its root",
                    )
                })?;
                version_partitions
                    .entry(version)
                    .and_modify(|current| {
                        if partition < current {
                            current.clone_from(partition);
                        }
                    })
                    .or_insert_with(|| partition.clone());
            }
        }
        for version in snapshot.file_versions.values() {
            let partition = version_partitions.get(&version.id).ok_or_else(|| {
                invalid(
                    "checkpoint Managed namespace",
                    "snapshot file version is not referenced by a file",
                )
            })?;
            groups
                .entry(partition.clone())
                .or_default()
                .push(TableRecord {
                    key: typed_key(FILE_VERSION_PREFIX, version.id.as_bytes()),
                    value: encode_table_value(version, "checkpoint Managed namespace")?,
                });
        }
        for (operation, result) in committed {
            let mut partition = Vec::with_capacity(1 + operation.as_bytes().len());
            partition.push(OPERATION_PARTITION_PREFIX);
            partition.extend_from_slice(operation.as_bytes());
            groups.entry(partition).or_default().push(TableRecord {
                key: typed_key(OPERATION_RESULT_PREFIX, operation.as_bytes()),
                value: encode_table_value(result, "checkpoint Managed namespace")?,
            });
        }
        let groups = groups
            .into_iter()
            .map(|(partition_key, mut records)| {
                records.sort_by(|left, right| left.key.cmp(&right.key));
                TableRecordGroup {
                    partition_key,
                    records,
                }
            })
            .collect();
        let encoded = sstable::build_set(scope, groups, "checkpoint Managed namespace")?;
        let tables: Vec<TableRef> = stream::iter(encoded)
            .map(|encoded| async move {
                if let Some(existing) = previous
                    .into_iter()
                    .flat_map(|manifest| &manifest.tables)
                    .find(|existing| *existing == &encoded.reference)
                {
                    return Ok(existing.clone());
                }
                ensure_immutable(
                    &self.operator,
                    &sstable_key(&encoded.reference.id),
                    &encoded.bytes,
                    "publish Managed namespace",
                    ManagedErrorKind::Conflict,
                    "operation identity was reused with another payload",
                )
                .await?;
                Ok(encoded.reference)
            })
            .buffered(MAX_CHECKPOINT_UPLOADS)
            .try_collect()
            .await?;
        debug_assert!(
            !tables.is_empty(),
            "a valid namespace contains its root node"
        );
        Ok(StoredManifest {
            major: FORMAT_MAJOR,
            volume_id: snapshot.volume_id,
            cursor: snapshot.cursor,
            root: snapshot.root,
            tables,
        })
    }

    async fn read_snapshot(
        &self,
        checkpoint: StoredManifest,
    ) -> Result<NamespaceSnapshot, ManagedError> {
        validate_tables(&checkpoint.tables)?;
        let mut nodes = BTreeMap::new();
        let mut directories = BTreeMap::new();
        let mut entries = Vec::new();
        let mut file_versions = BTreeMap::new();
        let tables: Vec<Vec<TableRecord>> = stream::iter(checkpoint.tables.iter().cloned())
            .map(|table| {
                self.read_table_range(
                    table,
                    NODE_PREFIX,
                    OPERATION_RESULT_PREFIX,
                    "read Managed namespace",
                )
            })
            .buffered(MAX_CHECKPOINT_READS)
            .try_collect()
            .await?;
        for records in tables {
            for record in records {
                let (&prefix, key) = record
                    .key
                    .split_first()
                    .ok_or_else(|| corrupt("read Managed namespace", "SSTable key is empty"))?;
                match prefix {
                    NODE_PREFIX => {
                        let node: NodeRecord =
                            decode_table_value(&record.value, "read Managed namespace")?;
                        let id = table_node_id(key, "node table key is invalid")?;
                        if node.id != id || nodes.insert(id, node).is_some() {
                            return Err(corrupt(
                                "read Managed namespace",
                                "node table key is invalid",
                            ));
                        }
                    }
                    DIRECTORY_PREFIX => {
                        let generation =
                            decode_table_value(&record.value, "read Managed namespace")?;
                        let node = table_node_id(key, "directory table key is invalid")?;
                        let directory = DirectoryRecord {
                            node,
                            generation,
                            entries: BTreeMap::new(),
                        };
                        if directories.insert(directory.node, directory).is_some() {
                            return Err(corrupt(
                                "read Managed namespace",
                                "duplicate directory record",
                            ));
                        }
                    }
                    DIRECTORY_ENTRY_PREFIX => {
                        let entry = decode_table_value(&record.value, "read Managed namespace")?;
                        let (directory, name) = table_directory_entry_key(key)?;
                        entries.push((directory, name, entry));
                    }
                    FILE_VERSION_PREFIX => {
                        let version: FileVersionRecord =
                            decode_table_value(&record.value, "read Managed namespace")?;
                        let id = table_file_version_id(key)?;
                        if version.id != id || file_versions.insert(id, version).is_some() {
                            return Err(corrupt(
                                "read Managed namespace",
                                "duplicate file version record",
                            ));
                        }
                    }
                    OPERATION_RESULT_PREFIX => continue,
                    _ => {
                        return Err(corrupt(
                            "read Managed namespace",
                            "SSTable key type is invalid",
                        ));
                    }
                }
            }
        }
        for (directory_id, name, stored) in entries {
            let directory = directories.get_mut(&directory_id).ok_or_else(|| {
                corrupt(
                    "read Managed namespace",
                    "entry references a missing directory",
                )
            })?;
            if directory.entries.insert(name, stored).is_some() {
                return Err(corrupt(
                    "read Managed namespace",
                    "duplicate directory entry",
                ));
            }
        }
        Ok(NamespaceSnapshot {
            volume_id: checkpoint.volume_id,
            cursor: checkpoint.cursor,
            root: checkpoint.root,
            nodes,
            directories,
            file_versions,
        })
    }

    async fn read_manifest(&self, id: &[u8; 32]) -> Result<StoredManifest, ManagedError> {
        let bytes = read_content_addressed(
            &self.operator,
            &manifest_key(id),
            id,
            "read Managed namespace",
            "checkpoint is missing",
            "checkpoint key and content disagree",
        )
        .await?;
        decode_cbor(MANIFEST_MAGIC, &bytes, "read Managed namespace")
    }

    async fn read_operation_results(
        &self,
        manifest: &StoredManifest,
    ) -> Result<BTreeMap<OperationId, StoredCommittedResult>, ManagedError> {
        validate_tables(&manifest.tables)?;
        let mut results = BTreeMap::new();
        let tables: Vec<Vec<TableRecord>> = stream::iter(manifest.tables.iter().cloned())
            .map(|table| {
                self.read_table_range(
                    table,
                    OPERATION_RESULT_PREFIX,
                    OPERATION_RESULT_PREFIX + 1,
                    "read Managed operation results",
                )
            })
            .buffered(MAX_CHECKPOINT_READS)
            .try_collect()
            .await?;
        for records in tables {
            for record in records {
                if record.key.first() != Some(&OPERATION_RESULT_PREFIX) {
                    continue;
                }
                let operation = operation_id_from_key(&record.key[1..])?;
                let result: StoredCommittedResult =
                    decode_table_value(&record.value, "read Managed operation results")?;
                result.validate(operation)?;
                if results.insert(operation, result).is_some() {
                    return Err(corrupt(
                        "read Managed operation results",
                        "duplicate operation result",
                    ));
                }
            }
        }
        Ok(results)
    }

    async fn read_table_range(
        &self,
        table: TableRef,
        lower: u8,
        upper: u8,
        action: &'static str,
    ) -> Result<Vec<TableRecord>, ManagedError> {
        let lower = [lower];
        let upper = [upper];
        let blocks = table
            .blocks
            .into_iter()
            .filter(|block| {
                block.last_key.as_slice() >= lower.as_slice()
                    && block.first_key.as_slice() < upper.as_slice()
            })
            .collect();
        Ok(sstable::fetch(
            &self.operator,
            &sstable_key(&table.id),
            *self.volume_id.as_bytes(),
            blocks,
            action,
        )
        .await?
        .into_iter()
        .flat_map(|(_, records)| records)
        .collect())
    }

    async fn read_operation_result(
        &self,
        manifest: &StoredManifest,
        operation: OperationId,
    ) -> Result<Option<StoredCommittedResult>, ManagedError> {
        validate_tables(&manifest.tables)?;
        let key = typed_key(OPERATION_RESULT_PREFIX, operation.as_bytes());
        for table in &manifest.tables {
            let blocks = table
                .blocks
                .iter()
                .filter(|block| {
                    block.first_key.as_slice() <= key.as_slice()
                        && key.as_slice() <= block.last_key.as_slice()
                })
                .cloned()
                .collect();
            for (_, records) in sstable::fetch(
                &self.operator,
                &sstable_key(&table.id),
                *self.volume_id.as_bytes(),
                blocks,
                "resolve Managed publication",
            )
            .await?
            {
                if let Ok(index) = records.binary_search_by(|record| record.key.cmp(&key)) {
                    let result: StoredCommittedResult =
                        decode_table_value(&records[index].value, "resolve Managed publication")?;
                    result.validate(operation)?;
                    return Ok(Some(result));
                }
            }
        }
        Ok(None)
    }

    async fn read_head(&self) -> Result<Option<(Vec<u8>, B::Revision)>, ManagedError> {
        self.backend.read("read Managed namespace").await
    }

    async fn read_current_head(&self) -> Result<Option<StoredHead>, ManagedError> {
        self.backend
            .read_bytes("read Managed namespace")
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
            .replace(expected_revision, bytes, "publish Managed namespace")
            .await
    }
}

fn manifest_key(id: &[u8; 32]) -> String {
    format!("{MANIFEST_ROOT}/{}.ofs", hex(id))
}

fn sstable_key(id: &[u8; 32]) -> String {
    let encoded = hex(id);
    format!("{SSTABLE_ROOT}/{encoded}.sst")
}

fn directory_entry_key(directory: NodeId, name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 16 + name.len());
    key.push(DIRECTORY_ENTRY_PREFIX);
    key.extend_from_slice(directory.as_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

fn snapshot_partition_keys(
    snapshot: &NamespaceSnapshot,
) -> Result<BTreeMap<NodeId, Vec<u8>>, ManagedError> {
    let root = vec![SNAPSHOT_PARTITION_PREFIX];
    let mut paths = BTreeMap::from([(snapshot.root, root)]);
    let mut pending = vec![snapshot.root];
    while let Some(directory_id) = pending.pop() {
        let directory = snapshot.directories.get(&directory_id).ok_or_else(|| {
            invalid(
                "checkpoint Managed namespace",
                "snapshot directory is missing",
            )
        })?;
        let parent = paths
            .get(&directory_id)
            .expect("pending directories have a path")
            .clone();
        for (name, entry) in &directory.entries {
            let path = child_partition_key(&parent, name)?;
            if paths.insert(entry.node, path).is_none()
                && snapshot.directories.contains_key(&entry.node)
            {
                pending.push(entry.node);
            }
        }
    }
    Ok(paths)
}

fn child_partition_key(parent: &[u8], name: &str) -> Result<Vec<u8>, ManagedError> {
    if name.as_bytes().contains(&0) {
        return Err(invalid(
            "checkpoint Managed namespace",
            "snapshot name contains an invalid byte",
        ));
    }
    let mut key = Vec::with_capacity(parent.len() + name.len() + 1);
    key.extend_from_slice(parent);
    key.extend_from_slice(name.as_bytes());
    key.push(0);
    Ok(key)
}

fn typed_key(prefix: u8, body: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + body.len());
    key.push(prefix);
    key.extend_from_slice(body);
    key
}

fn operation_id_from_key(key: &[u8]) -> Result<OperationId, ManagedError> {
    let bytes = key.try_into().map_err(|_| {
        corrupt(
            "read Managed operation results",
            "operation result key is invalid",
        )
    })?;
    Ok(OperationId::from_bytes(bytes))
}

fn table_node_id(key: &[u8], message: &'static str) -> Result<NodeId, ManagedError> {
    let bytes = key
        .try_into()
        .map_err(|_| corrupt("read Managed namespace", message))?;
    Ok(NodeId::from_bytes(bytes))
}

fn table_directory_entry_key(key: &[u8]) -> Result<(NodeId, String), ManagedError> {
    let (directory, name) = key.split_at_checked(16).ok_or_else(|| {
        corrupt(
            "read Managed namespace",
            "directory entry table key is invalid",
        )
    })?;
    let directory = NodeId::from_bytes(directory.try_into().expect("fixed key prefix"));
    let name = String::from_utf8(name.to_vec()).map_err(|_| {
        corrupt(
            "read Managed namespace",
            "directory entry table key is invalid",
        )
    })?;
    Ok((directory, name))
}

fn table_file_version_id(key: &[u8]) -> Result<FileVersionId, ManagedError> {
    let bytes = key.try_into().map_err(|_| {
        corrupt(
            "read Managed namespace",
            "file version table key is invalid",
        )
    })?;
    Ok(FileVersionId::from_bytes(bytes))
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

fn encode_table_value<T: Serialize>(
    value: &T,
    action: &'static str,
) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| invalid(action, "SSTable record cannot be encoded"))?;
    Ok(bytes)
}

fn decode_table_value<T: DeserializeOwned>(
    bytes: &[u8],
    action: &'static str,
) -> Result<T, ManagedError> {
    let mut input = Cursor::new(bytes);
    let value = ciborium::de::from_reader(&mut input)
        .map_err(|_| corrupt(action, "SSTable record cannot be decoded"))?;
    if input.position() != bytes.len() as u64 {
        return Err(corrupt(action, "SSTable record has trailing bytes"));
    }
    Ok(value)
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
    tail: Vec<StoredTransaction>,
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
        tail: Vec<StoredTransaction>,
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

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredTransaction {
    major: u16,
    volume_id: VolumeId,
    operation: OperationId,
    parent: ChangeCursor,
    cursor: ChangeCursor,
    root: NodeId,
    expected_nodes: Vec<NodePrecondition>,
    expected_directories: Vec<DirectoryPrecondition>,
    put_nodes: Vec<NodeRecord>,
    remove_nodes: Vec<NodeId>,
    put_directories: Vec<StoredDirectoryHeader>,
    remove_directories: Vec<NodeId>,
    put_directory_entries: Vec<StoredNamedDirectoryEntry>,
    remove_directory_entries: Vec<StoredDirectoryEntryKey>,
    put_file_versions: Vec<FileVersionRecord>,
    remove_file_versions: Vec<FileVersionId>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredManifest {
    major: u16,
    volume_id: VolumeId,
    cursor: ChangeCursor,
    root: NodeId,
    tables: Vec<TableRef>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCommittedResult {
    cursor: ChangeCursor,
    request_sha256: [u8; 32],
}

impl StoredCommittedResult {
    fn from_transaction(transaction: &StoredTransaction) -> Result<Self, ManagedError> {
        Ok(Self {
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
    transaction: &StoredTransaction,
    action: &'static str,
) -> Result<[u8; 32], ManagedError> {
    encode_table_value(transaction, action).map(|bytes| sha256(&bytes))
}

fn validate_tables(tables: &[TableRef]) -> Result<(), ManagedError> {
    let mut previous_partition_last: Option<&[u8]> = None;
    for table in tables {
        if table.encoded_bytes == 0
            || table.first_partition_key.is_empty()
            || table.first_partition_key > table.last_partition_key
            || previous_partition_last
                .is_some_and(|last| last >= table.first_partition_key.as_slice())
            || table.blocks.is_empty()
            || table.blocks.iter().any(|block| {
                block.records == 0 || block.encoded_bytes == 0 || block.first_key > block.last_key
            })
        {
            return Err(corrupt(
                "read Managed namespace",
                "manifest SSTable references are invalid",
            ));
        }
        previous_partition_last = Some(&table.last_partition_key);
        let mut end = 0_u64;
        let mut previous_last: Option<&[u8]> = None;
        for block in &table.blocks {
            if block.offset < end
                || block.offset.checked_add(block.encoded_bytes).is_none()
                || block.offset + block.encoded_bytes > table.encoded_bytes
                || previous_last.is_some_and(|last| last >= block.first_key.as_slice())
            {
                return Err(corrupt(
                    "read Managed namespace",
                    "manifest SSTable block ranges are invalid",
                ));
            }
            end = block.offset + block.encoded_bytes;
            previous_last = Some(&block.last_key);
        }
    }
    Ok(())
}

impl StoredTransaction {
    fn from_publication(
        publication: &NamespacePublication,
        base: Option<&NamespaceSnapshot>,
    ) -> Self {
        let change = NamespaceChange::from_publication(publication, base);
        Self {
            major: FORMAT_MAJOR,
            volume_id: change.volume_id,
            operation: change.operation,
            parent: change.parent,
            cursor: change.cursor,
            root: change.root,
            expected_nodes: change.expected_nodes,
            expected_directories: change.expected_directories,
            put_nodes: change.put_nodes,
            remove_nodes: change.remove_nodes,
            put_directories: change
                .put_directories
                .iter()
                .map(StoredDirectoryHeader::from)
                .collect(),
            remove_directories: change.remove_directories,
            put_directory_entries: change
                .put_directories
                .iter()
                .flat_map(|directory| {
                    let base = base.and_then(|snapshot| snapshot.directories.get(&directory.node));
                    directory
                        .entries
                        .iter()
                        .filter(move |(name, entry)| {
                            base.and_then(|base| base.entries.get(*name)) != Some(*entry)
                        })
                        .map(move |(name, entry)| StoredNamedDirectoryEntry {
                            directory: directory.node,
                            name: name.clone(),
                            entry: *entry,
                        })
                })
                .collect(),
            remove_directory_entries: change
                .put_directories
                .iter()
                .flat_map(|directory| {
                    base.and_then(|snapshot| snapshot.directories.get(&directory.node))
                        .into_iter()
                        .flat_map(move |base| {
                            base.entries
                                .keys()
                                .filter(move |name| !directory.entries.contains_key(*name))
                                .map(move |name| StoredDirectoryEntryKey {
                                    directory: directory.node,
                                    name: name.clone(),
                                })
                        })
                })
                .collect(),
            put_file_versions: change.put_file_versions,
            remove_file_versions: change.remove_file_versions,
        }
    }

    fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        let parent = self.parent;
        let cursor = self.cursor;
        if self.major != FORMAT_MAJOR
            || self.volume_id != volume_id
            || cursor.operation() != Some(self.operation)
            || parent.sequence().checked_add(1) != Some(cursor.sequence())
        {
            return Err(corrupt(
                "read Managed transaction",
                "transaction ancestry is invalid",
            ));
        }
        Ok(())
    }

    fn to_change(&self, base: Option<&NamespaceSnapshot>) -> Result<NamespaceChange, ManagedError> {
        let volume_id = self.volume_id;
        self.validate(volume_id)?;
        let mut put_directories = BTreeMap::new();
        for header in &self.put_directories {
            let node = header.node;
            if put_directories.contains_key(&node) {
                return Err(corrupt(
                    "read Managed transaction",
                    "transaction repeats a directory header",
                ));
            }
            let mut directory = base
                .and_then(|snapshot| snapshot.directories.get(&node))
                .cloned()
                .unwrap_or_else(|| header.to_record());
            directory.generation = header.generation.clone();
            put_directories.insert(node, directory);
        }
        for removed in &self.remove_directory_entries {
            let directory = put_directories.get_mut(&removed.directory).ok_or_else(|| {
                corrupt(
                    "read Managed transaction",
                    "directory entry removal has no directory header",
                )
            })?;
            if directory.entries.remove(&removed.name).is_none() {
                return Err(corrupt(
                    "read Managed transaction",
                    "directory entry removal is stale",
                ));
            }
        }
        for stored in &self.put_directory_entries {
            let directory = put_directories.get_mut(&stored.directory).ok_or_else(|| {
                corrupt(
                    "read Managed transaction",
                    "directory entry update has no directory header",
                )
            })?;
            directory.entries.insert(stored.name.clone(), stored.entry);
        }
        Ok(NamespaceChange {
            volume_id,
            operation: self.operation,
            parent: self.parent,
            cursor: self.cursor,
            root: self.root,
            expected_nodes: self.expected_nodes.clone(),
            expected_directories: self.expected_directories.clone(),
            put_nodes: self.put_nodes.clone(),
            remove_nodes: self.remove_nodes.clone(),
            put_directories: put_directories.into_values().collect(),
            remove_directories: self.remove_directories.clone(),
            put_file_versions: self.put_file_versions.clone(),
            remove_file_versions: self.remove_file_versions.clone(),
        })
    }
}

fn apply_transaction(
    base: Option<NamespaceSnapshot>,
    transaction: &StoredTransaction,
) -> Result<NamespaceSnapshot, ManagedError> {
    let change = transaction.to_change(base.as_ref())?;
    change.apply(base)
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

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryHeader {
    node: NodeId,
    generation: Generation,
}

impl From<&DirectoryRecord> for StoredDirectoryHeader {
    fn from(directory: &DirectoryRecord) -> Self {
        Self {
            node: directory.node,
            generation: directory.generation.clone(),
        }
    }
}

impl StoredDirectoryHeader {
    fn to_record(&self) -> DirectoryRecord {
        DirectoryRecord {
            node: self.node,
            generation: self.generation.clone(),
            entries: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNamedDirectoryEntry {
    directory: NodeId,
    name: String,
    entry: DirectoryEntry,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryEntryKey {
    directory: NodeId,
    name: String,
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

    #[tokio::test]
    async fn manifest_sstables_round_trip_without_a_whole_snapshot_record() {
        let operation = OperationId::from_bytes([8; 16]);
        let cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), operation);
        let mut snapshot = root_snapshot(cursor);
        snapshot
            .directories
            .get_mut(&snapshot.root)
            .unwrap()
            .entries
            .insert(
                "child".to_owned(),
                DirectoryEntry {
                    node: snapshot.root,
                    kind: NodeKind::Directory,
                },
            );
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let namespace = ObjectNamespace {
            volume_id: snapshot.volume_id,
            operator: operator.clone(),
            backend: ObjectHeadBackend {
                operator: operator.clone(),
            },
        };
        let checkpoint = namespace
            .checkpoint_full(&snapshot, &BTreeMap::new(), None)
            .await
            .unwrap();
        assert_eq!(checkpoint.tables.len(), 1);
        assert!(!checkpoint.tables[0].blocks.is_empty());
        let missing = sstable_key(&checkpoint.tables[0].id);
        operator.delete(&missing).await.unwrap();
        let checkpoint = namespace
            .checkpoint_full(&snapshot, &BTreeMap::new(), None)
            .await
            .unwrap();
        assert!(operator.stat(&missing).await.is_ok());
        let recovered = namespace.read_snapshot(checkpoint).await.unwrap();
        assert_eq!(recovered, snapshot);
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
            backend: ObjectHeadBackend { operator },
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
        let stored =
            StoredTransaction::from_publication(&second_publication, Some(&first_snapshot));
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
    async fn missing_manifest_sstable_is_reported_as_corruption() {
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
            backend: ObjectHeadBackend {
                operator: operator.clone(),
            },
        };
        assert_eq!(
            namespace.publish(None, &publication).await.unwrap(),
            CommitOutcome::Committed(cursor)
        );
        let head = operator.read(HEAD_KEY).await.unwrap().to_bytes();
        let head = decode_head(&head).unwrap();
        let checkpoint = namespace.read_manifest(&head.checkpoint).await.unwrap();
        operator
            .delete(&sstable_key(&checkpoint.tables[0].id))
            .await
            .unwrap();
        let error = namespace.recover(&head).await.err().unwrap();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
    }
}
