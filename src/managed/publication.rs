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

//! Immutable namespace commits, operation receipts, and atomic publication.

use std::cmp::Reverse;

use crate::filesystem::{ChangeCursor, OperationId, VolumeId};
use crate::namespace::Namespace;
use crate::workset::{self, Workspace};
use crate::{Error, ErrorKind};

use super::head::{Head, ManagedObservation, ManagedVolume, NamespaceRevision};
use super::namespace;
use super::object::{GcEpoch, ObjectClass, ObjectRef};
use super::record::Record;
use super::storage;
use super::stream::{self, RecordStreamReader, RecordStreamWriter, StreamKind, StreamRef};

const COMMIT_RECORD: Record = Record::new(*b"OFSCMIT1", 4 * 1024 * 1024);

#[derive(Clone, Debug)]
pub(super) struct NamespaceCommit {
    pub(super) volume_id: VolumeId,
    pub(super) change_cursor: ChangeCursor,
    pub(super) namespace_snapshot: StreamRef,
    pub(super) namespace_changes: Vec<StreamRef>,
    pub(super) operation_results: Vec<StreamRef>,
}
super::wire::tuple_wire!(NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    namespace_snapshot: StreamRef,
    namespace_changes: Vec<StreamRef>,
    operation_results: Vec<StreamRef>,
});

impl NamespaceCommit {
    pub(super) fn genesis(volume_id: VolumeId, namespace_snapshot: StreamRef) -> Self {
        Self {
            volume_id,
            change_cursor: ChangeCursor::GENESIS,
            namespace_snapshot,
            namespace_changes: Vec::new(),
            operation_results: Vec::new(),
        }
    }

    pub(super) fn namespace_streams(&self) -> impl Iterator<Item = StreamRef> + '_ {
        std::iter::once(self.namespace_snapshot).chain(self.namespace_changes.iter().copied())
    }
}

#[derive(Clone, Copy, Debug)]
struct OperationRecord {
    operation_id: OperationId,
    change_cursor: ChangeCursor,
}
super::wire::tuple_wire!(OperationRecord {
    operation_id: OperationId,
    change_cursor: ChangeCursor,
});

impl ManagedVolume {
    pub(crate) async fn prepare_publication(
        &self,
        observed: &ManagedObservation,
        target: &Namespace<StreamRef>,
        operation: OperationId,
    ) -> Result<NamespaceRevision, Error> {
        if target.volume_id != self.id()
            || target.root != self.format.root_node_id()
            || target.cursor.sequence() != observed.namespace.cursor.sequence() + 1
        {
            return Err(Error::invalid(
                "publish Managed namespace",
                "publication ancestry is invalid",
            ));
        }

        let mut commit = observed.commit.clone();
        commit.change_cursor = target.cursor;
        if observed.namespace.cursor == ChangeCursor::GENESIS {
            commit.namespace_snapshot =
                namespace::write_full(self, target, observed.gc_epoch).await?;
            commit.namespace_changes.clear();
        } else if let Some(delta) =
            namespace::write_delta(self, &observed.namespace, target, observed.gc_epoch).await?
        {
            commit.namespace_changes.push(delta);
        } else {
            return Err(Error::invalid(
                "publish Managed namespace",
                "publication contains no namespace change",
            ));
        }

        let operation_record = OperationRecord {
            operation_id: operation,
            change_cursor: target.cursor,
        };
        let operation_stream = stream::write_records(
            &self.operator,
            observed.gc_epoch,
            ObjectClass::OperationResultSegment,
            StreamKind::OPERATION_RESULTS,
            [operation_record],
        )
        .await?;
        commit.operation_results.push(operation_stream);

        // Persisted layout follows data growth, not this caller's resource
        // budget. Once accumulated changes are as large as their snapshot,
        // replace them with one new snapshot. Receipt segments use the same
        // size-tiered rule, so their reference list grows logarithmically.
        if !commit.namespace_changes.is_empty()
            && total_payload(&commit.namespace_changes) >= commit.namespace_snapshot.payload_length
        {
            commit.namespace_snapshot =
                namespace::write_full(self, target, observed.gc_epoch).await?;
            commit.namespace_changes.clear();
        }
        if should_compact_segments(&commit.operation_results) {
            commit.operation_results = copy_operations(self, &commit, observed.gc_epoch, None)
                .await?
                .into_iter()
                .collect();
        }
        write_commit(self, observed.gc_epoch, &commit).await
    }

    pub(crate) async fn commit_publication(
        &self,
        observed: &ManagedObservation,
        target: NamespaceRevision,
        operation: OperationId,
    ) -> Result<(), Error> {
        if target.change_cursor.sequence() != observed.namespace.cursor.sequence() + 1 {
            return Err(Error::invalid(
                "publish Managed namespace",
                "prepared publication ancestry is invalid",
            ));
        }
        let head = Head {
            current_commit: target,
            gc_epoch: observed.gc_epoch,
            minimum_retained_cursor: observed.reclamation_watermark,
        };
        if self.replace_head(&observed.head_revision, &head).await? {
            return Ok(());
        }
        let (current, _) = self.read_head().await?;
        let commit = read_commit(self, current.current_commit).await?;
        if operation_in_commit(self, operation, &commit).await? {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Conflict,
                "publish Managed namespace",
                "observed generation changed",
            ))
        }
    }

    pub(crate) async fn operation_committed(
        &self,
        operation: OperationId,
        observed: &ManagedObservation,
    ) -> Result<bool, Error> {
        operation_in_commit(self, operation, &observed.commit).await
    }
}

async fn operation_in_commit(
    volume: &ManagedVolume,
    operation: OperationId,
    commit: &NamespaceCommit,
) -> Result<bool, Error> {
    for reference in commit.operation_results.iter().rev() {
        namespace::require_stream(
            *reference,
            StreamKind::OPERATION_RESULTS,
            ObjectClass::OperationResultSegment,
        )?;
        let mut reader =
            RecordStreamReader::<OperationRecord>::open(&volume.operator, *reference).await?;
        while let Some(record) = reader.next().await? {
            if record.operation_id == operation {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(super) async fn compact_for_collection(
    volume: &ManagedVolume,
    reference: NamespaceRevision,
    gc_epoch: GcEpoch,
    mut visit: impl FnMut(ObjectRef) -> Result<(), Error>,
) -> Result<NamespaceRevision, Error> {
    let source = read_commit(volume, reference).await?;
    let current = namespace::read(volume, &source, source.change_cursor).await?;
    let namespace_snapshot =
        namespace::write_full_visiting(volume, &current, gc_epoch, &mut visit).await?;
    visit(namespace_snapshot.object)?;
    for result in &source.operation_results {
        visit(result.object)?;
    }
    let commit = NamespaceCommit {
        volume_id: source.volume_id,
        change_cursor: source.change_cursor,
        namespace_snapshot,
        namespace_changes: Vec::new(),
        operation_results: source.operation_results,
    };
    let revision = write_commit(volume, gc_epoch, &commit).await?;
    visit(revision.object)?;
    Ok(revision)
}

fn total_payload(streams: &[StreamRef]) -> u64 {
    streams.iter().fold(0_u64, |total, stream| {
        total.saturating_add(stream.payload_length)
    })
}

fn should_compact_segments(streams: &[StreamRef]) -> bool {
    let Some((base, changes)) = streams.split_first() else {
        return false;
    };
    !changes.is_empty() && total_payload(changes) >= base.payload_length
}

async fn copy_operations(
    volume: &ManagedVolume,
    source: &NamespaceCommit,
    gc_epoch: GcEpoch,
    extra: Option<OperationRecord>,
) -> Result<Option<StreamRef>, Error> {
    let workspace = Workspace::create(volume.workset_options())?;
    let mut records = workspace.writer("operation-results")?;
    let mut count = 0_u64;
    for reference in &source.operation_results {
        namespace::require_stream(
            *reference,
            StreamKind::OPERATION_RESULTS,
            ObjectClass::OperationResultSegment,
        )?;
        let mut reader =
            RecordStreamReader::<OperationRecord>::open(&volume.operator, *reference).await?;
        while let Some(record) = reader.next().await? {
            records.write(&record)?;
            count = count.saturating_add(1);
        }
    }
    if let Some(record) = extra {
        records.write(&record)?;
        count = count.saturating_add(1);
    }
    if count == 0 {
        return Ok(None);
    }
    let sorted = workset::sort(&workspace, &records.finish()?, |record| {
        Reverse(record.change_cursor)
    })?;
    let mut sorted = sorted.reader()?;
    let mut writer = RecordStreamWriter::open(
        &volume.operator,
        gc_epoch,
        ObjectClass::OperationResultSegment,
        StreamKind::OPERATION_RESULTS,
    )
    .await?;
    while let Some(record) = sorted.next()? {
        writer.write(&record).await?;
    }
    writer.close().await.map(Some)
}

pub(super) async fn write_commit(
    volume: &ManagedVolume,
    gc_epoch: GcEpoch,
    commit: &NamespaceCommit,
) -> Result<NamespaceRevision, Error> {
    let mut writer =
        storage::ImmutableWriter::open(&volume.operator, gc_epoch, ObjectClass::NamespaceCommit)
            .await?;
    writer.write(COMMIT_RECORD.encode(commit)?).await?;
    let object = writer.close().await?;
    Ok(NamespaceRevision {
        object,
        change_cursor: commit.change_cursor,
    })
}

pub(super) async fn read_commit(
    volume: &ManagedVolume,
    reference: NamespaceRevision,
) -> Result<NamespaceCommit, Error> {
    if reference.object.class != ObjectClass::NamespaceCommit {
        return Err(Error::corrupt(
            "read Managed namespace",
            "commit reference has the wrong object class",
        ));
    }
    let bytes = storage::read_immutable(
        &volume.operator,
        reference.object,
        COMMIT_RECORD.maximum_encoded_bytes(),
    )
    .await?;
    let commit: NamespaceCommit = COMMIT_RECORD.decode(&bytes)?;
    if commit.volume_id != volume.id()
        || commit.change_cursor != reference.change_cursor
        || commit.namespace_snapshot.kind != StreamKind::NAMESPACE_RECORDS
    {
        return Err(Error::corrupt(
            "read Managed namespace",
            "namespace commit does not match its reference",
        ));
    }
    Ok(commit)
}
