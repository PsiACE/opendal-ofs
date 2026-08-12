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

use crate::filesystem::NamespaceValue;
use crate::filesystem::{ChangeCursor, OperationId};
use crate::namespace::Namespace;
use crate::{Error, ErrorKind};

use super::authority::AuthorityHead;
use super::data::FileDataRef;
use super::head::{ManagedObservation, ManagedVolume, NamespaceRevision};
use super::layout::{
    NamespaceChangeSegment, NamespaceCommit, NamespaceSnapshot, OperationReceipt,
    OperationReceiptSegment, should_merge,
};
use super::namespace;
use super::object::{GcEpoch, ObjectClass, ObjectLocator};
use super::record::Record;
use super::storage;
use super::stream::{self, RecordStreamReader, RecordStreamWriter, StreamKind};

const COMMIT_RECORD: Record = Record::new(*b"OFSCMIT1", 4 * 1024 * 1024);

impl ManagedVolume {
    pub(crate) async fn prepare_publication(
        &self,
        observed: &ManagedObservation,
        target: &Namespace<FileDataRef>,
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
            commit.namespace_snapshot = NamespaceSnapshot {
                change_cursor: target.cursor,
                stream: namespace::write_snapshot(self, target, observed.gc_epoch, |_| Ok(()))
                    .await?,
            };
            commit.namespace_changes.clear();
        } else if let Some(delta_stream) =
            namespace::write_delta(self, &observed.namespace, target, observed.gc_epoch).await?
        {
            let delta = NamespaceChangeSegment::singleton(target.cursor, delta_stream);
            let accumulated = commit
                .namespace_changes
                .iter()
                .try_fold(delta.source_bytes, |total, segment| {
                    total.checked_add(segment.source_bytes)
                })
                .ok_or_else(|| {
                    Error::corrupt("publish Managed namespace", "change bytes overflow")
                })?;
            if accumulated >= commit.namespace_snapshot.stream.payload_length {
                commit.namespace_snapshot = NamespaceSnapshot {
                    change_cursor: target.cursor,
                    stream: namespace::write_snapshot(self, target, observed.gc_epoch, |_| Ok(()))
                        .await?,
                };
                commit.namespace_changes.clear();
            } else {
                append_namespace_change(
                    self,
                    &mut commit.namespace_changes,
                    delta,
                    observed.gc_epoch,
                )
                .await?;
            }
        } else {
            return Err(Error::invalid(
                "publish Managed namespace",
                "publication contains no namespace change",
            ));
        }

        let operation_record = OperationReceipt {
            change_cursor: target.cursor,
            operation_id: operation,
        };
        let operation_stream = stream::write_records(
            &self.operator,
            observed.gc_epoch,
            ObjectClass::OperationReceiptSegment,
            StreamKind::OPERATION_RECEIPTS,
            [operation_record],
        )
        .await?;
        append_operation_receipt(
            self,
            &mut commit.operation_receipts,
            OperationReceiptSegment::singleton(target.cursor, operation_stream),
            observed.gc_epoch,
        )
        .await?;
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
        let head = AuthorityHead::new(target, observed.gc_epoch, observed.reclamation_watermark);
        if self.replace_head(&observed.authority, head).await? {
            return Ok(());
        }
        let current = self.read_authority().await?;
        let commit = read_commit(self, current.head().current_commit()).await?;
        if operation_in_commit(self, operation, target.change_cursor, &commit).await? {
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
        expected_cursor: ChangeCursor,
        observed: &ManagedObservation,
    ) -> Result<bool, Error> {
        operation_in_commit(self, operation, expected_cursor, &observed.commit).await
    }
}

async fn operation_in_commit(
    volume: &ManagedVolume,
    operation: OperationId,
    expected_cursor: ChangeCursor,
    commit: &NamespaceCommit,
) -> Result<bool, Error> {
    Ok(read_operation_receipt(volume, expected_cursor, commit)
        .await?
        .is_some_and(|receipt| receipt.operation_id == operation))
}

async fn read_operation_receipt(
    volume: &ManagedVolume,
    expected_cursor: ChangeCursor,
    commit: &NamespaceCommit,
) -> Result<Option<OperationReceipt>, Error> {
    let Some(segment) = commit.operation_receipts.iter().find(|segment| {
        segment.first_cursor <= expected_cursor && expected_cursor <= segment.last_cursor
    }) else {
        return Ok(None);
    };
    let mut reader =
        RecordStreamReader::<OperationReceipt>::open(&volume.operator, segment.stream).await?;
    let mut previous = None;
    while let Some(record) = reader.next().await? {
        if previous.is_some_and(|previous| previous <= record.change_cursor)
            || record.change_cursor < segment.first_cursor
            || record.change_cursor > segment.last_cursor
        {
            return Err(Error::corrupt(
                "read Managed operation receipt",
                "operation receipts are not newest first",
            ));
        }
        previous = Some(record.change_cursor);
        if record.change_cursor == expected_cursor {
            return Ok(Some(record));
        }
    }
    Err(Error::corrupt(
        "read Managed operation receipt",
        "operation receipt segment does not contain its cursor range",
    ))
}

impl ManagedVolume {
    pub(super) async fn compact_for_collection(
        &self,
        reference: NamespaceRevision,
        gc_epoch: GcEpoch,
        mut visit: impl FnMut(ObjectLocator) -> Result<(), Error> + Send,
    ) -> Result<NamespaceRevision, Error> {
        let source = read_commit(self, reference).await?;
        let current = namespace::read(self, &source, source.change_cursor).await?;
        let mut records = current.reader()?;
        while let Some(record) = records.next()? {
            let Some(node) = record.value else {
                continue;
            };
            let NamespaceValue::RegularFile { content, .. } = node.value else {
                continue;
            };
            if let Some(reference) = content.extension() {
                self.file_access()?
                    .visit_reachable_dyn(&self.access_context, reference, &mut visit)
                    .await?;
            } else {
                visit(content.object_locator())?;
            }
        }
        let namespace_stream =
            namespace::write_snapshot(self, &current, gc_epoch, |_| Ok(())).await?;
        visit(namespace_stream.object.locator)?;
        for receipt in &source.operation_receipts {
            visit(receipt.stream.object.locator)?;
        }
        let commit = NamespaceCommit {
            volume_id: source.volume_id,
            change_cursor: source.change_cursor,
            namespace_snapshot: NamespaceSnapshot {
                change_cursor: source.change_cursor,
                stream: namespace_stream,
            },
            namespace_changes: Vec::new(),
            operation_receipts: source.operation_receipts,
        };
        let revision = write_commit(self, gc_epoch, &commit).await?;
        visit(revision.object.locator)?;
        Ok(revision)
    }
}

async fn merge_operation_receipts(
    volume: &ManagedVolume,
    older: OperationReceiptSegment,
    newer: OperationReceiptSegment,
    gc_epoch: GcEpoch,
) -> Result<OperationReceiptSegment, Error> {
    let mut writer = RecordStreamWriter::open(
        &volume.operator,
        gc_epoch,
        ObjectClass::OperationReceiptSegment,
        StreamKind::OPERATION_RECEIPTS,
    )
    .await?;
    for reference in [newer.stream, older.stream] {
        reference.require(
            StreamKind::OPERATION_RECEIPTS,
            ObjectClass::OperationReceiptSegment,
        )?;
        let mut reader =
            RecordStreamReader::<OperationReceipt>::open(&volume.operator, reference).await?;
        while let Some(record) = reader.next().await? {
            writer.write(&record).await?;
        }
    }
    OperationReceiptSegment::merged(older, newer, writer.close().await?)
}

async fn append_namespace_change(
    volume: &ManagedVolume,
    segments: &mut Vec<NamespaceChangeSegment>,
    mut carry: NamespaceChangeSegment,
    gc_epoch: GcEpoch,
) -> Result<(), Error> {
    while should_merge(
        segments.last().map(|segment| segment.source_bytes),
        carry.source_bytes,
    ) {
        let older = segments.pop().expect("a merge candidate exists");
        carry = namespace::merge_change_segments(volume, older, carry, gc_epoch).await?;
    }
    segments.push(carry);
    Ok(())
}

async fn append_operation_receipt(
    volume: &ManagedVolume,
    segments: &mut Vec<OperationReceiptSegment>,
    mut carry: OperationReceiptSegment,
    gc_epoch: GcEpoch,
) -> Result<(), Error> {
    while should_merge(
        segments.last().map(|segment| segment.source_bytes),
        carry.source_bytes,
    ) {
        let older = segments.pop().expect("a merge candidate exists");
        carry = merge_operation_receipts(volume, older, carry, gc_epoch).await?;
    }
    segments.push(carry);
    Ok(())
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
    if reference.object.locator.class != ObjectClass::NamespaceCommit {
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
    commit.validate(volume.id(), reference.change_cursor)?;
    Ok(commit)
}
