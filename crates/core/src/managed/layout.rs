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

//! Pure wire layout and invariants for one immutable namespace commit.

use crate::Error;
use crate::filesystem::{ChangeCursor, OperationId, VolumeId};

use super::object::ObjectClass;
use super::stream::{StreamKind, StreamRef};

#[derive(Clone, Debug)]
pub(super) struct NamespaceCommit {
    pub(super) volume_id: VolumeId,
    pub(super) change_cursor: ChangeCursor,
    pub(super) namespace_snapshot: NamespaceSnapshot,
    pub(super) namespace_changes: Vec<NamespaceChangeSegment>,
    pub(super) operation_receipts: Vec<OperationReceiptSegment>,
}
super::wire::tuple_wire!(NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    namespace_snapshot: NamespaceSnapshot,
    namespace_changes: Vec<NamespaceChangeSegment>,
    operation_receipts: Vec<OperationReceiptSegment>,
});

#[derive(Clone, Copy, Debug)]
pub(super) struct NamespaceSnapshot {
    pub(super) change_cursor: ChangeCursor,
    pub(super) stream: StreamRef,
}
super::wire::tuple_wire!(NamespaceSnapshot {
    change_cursor: ChangeCursor,
    stream: StreamRef,
});

#[derive(Clone, Copy, Debug)]
pub(super) struct NamespaceChangeSegment {
    pub(super) end_cursor: ChangeCursor,
    /// Sum of the original logical change-stream bytes represented here.
    pub(super) source_bytes: u64,
    pub(super) stream: StreamRef,
}
super::wire::tuple_wire!(NamespaceChangeSegment {
    end_cursor: ChangeCursor,
    source_bytes: u64,
    stream: StreamRef,
});

#[derive(Clone, Copy, Debug)]
pub(super) struct OperationReceiptSegment {
    pub(super) first_cursor: ChangeCursor,
    pub(super) last_cursor: ChangeCursor,
    /// Sum of the original receipt-stream bytes represented here.
    pub(super) source_bytes: u64,
    pub(super) stream: StreamRef,
}
super::wire::tuple_wire!(OperationReceiptSegment {
    first_cursor: ChangeCursor,
    last_cursor: ChangeCursor,
    source_bytes: u64,
    stream: StreamRef,
});

#[derive(Clone, Copy, Debug)]
pub(super) struct OperationReceipt {
    pub(super) change_cursor: ChangeCursor,
    pub(super) operation_id: OperationId,
}
super::wire::tuple_wire!(OperationReceipt {
    change_cursor: ChangeCursor,
    operation_id: OperationId,
});

impl NamespaceCommit {
    pub(super) fn genesis(volume_id: VolumeId, namespace_snapshot: StreamRef) -> Self {
        Self {
            volume_id,
            change_cursor: ChangeCursor::GENESIS,
            namespace_snapshot: NamespaceSnapshot {
                change_cursor: ChangeCursor::GENESIS,
                stream: namespace_snapshot,
            },
            namespace_changes: Vec::new(),
            operation_receipts: Vec::new(),
        }
    }

    pub(super) fn validate(
        &self,
        volume_id: VolumeId,
        reference_cursor: ChangeCursor,
    ) -> Result<(), Error> {
        if self.volume_id != volume_id
            || self.change_cursor != reference_cursor
            || self.namespace_snapshot.change_cursor > self.change_cursor
            || self
                .namespace_snapshot
                .stream
                .require(
                    StreamKind::NAMESPACE_SNAPSHOT,
                    ObjectClass::NamespaceSegment,
                )
                .is_err()
            || !self.valid_namespace_changes()
            || !self.valid_operation_receipts()
        {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace commit does not match its reference",
            ));
        }
        Ok(())
    }

    fn valid_namespace_changes(&self) -> bool {
        let mut previous = self.namespace_snapshot.change_cursor;
        for segment in &self.namespace_changes {
            if segment.source_bytes == 0
                || segment.end_cursor <= previous
                || segment.end_cursor > self.change_cursor
                || segment
                    .stream
                    .require(StreamKind::NAMESPACE_CHANGES, ObjectClass::NamespaceSegment)
                    .is_err()
            {
                return false;
            }
            previous = segment.end_cursor;
        }
        previous == self.change_cursor
    }

    fn valid_operation_receipts(&self) -> bool {
        if self.change_cursor == ChangeCursor::GENESIS {
            return self.operation_receipts.is_empty();
        }
        let mut previous = None::<ChangeCursor>;
        for segment in &self.operation_receipts {
            if segment.source_bytes == 0
                || segment.first_cursor > segment.last_cursor
                || segment.last_cursor > self.change_cursor
                || previous.is_some_and(|cursor| {
                    cursor.sequence().checked_add(1) != Some(segment.first_cursor.sequence())
                })
                || segment
                    .stream
                    .require(
                        StreamKind::OPERATION_RECEIPTS,
                        ObjectClass::OperationReceiptSegment,
                    )
                    .is_err()
            {
                return false;
            }
            previous = Some(segment.last_cursor);
        }
        previous == Some(self.change_cursor)
    }
}

impl NamespaceChangeSegment {
    pub(super) fn singleton(end_cursor: ChangeCursor, stream: StreamRef) -> Self {
        Self {
            end_cursor,
            source_bytes: stream.payload_length,
            stream,
        }
    }

    pub(super) fn merged(older: Self, newer: Self, stream: StreamRef) -> Result<Self, Error> {
        if older.end_cursor >= newer.end_cursor {
            return Err(Error::corrupt(
                "compact Managed changes",
                "namespace change segments are not chronological",
            ));
        }
        Ok(Self {
            end_cursor: newer.end_cursor,
            source_bytes: add_source_bytes(
                older.source_bytes,
                newer.source_bytes,
                "compact Managed changes",
            )?,
            stream,
        })
    }
}

impl OperationReceiptSegment {
    pub(super) fn singleton(cursor: ChangeCursor, stream: StreamRef) -> Self {
        Self {
            first_cursor: cursor,
            last_cursor: cursor,
            source_bytes: stream.payload_length,
            stream,
        }
    }

    pub(super) fn merged(older: Self, newer: Self, stream: StreamRef) -> Result<Self, Error> {
        if older.last_cursor.sequence().checked_add(1) != Some(newer.first_cursor.sequence()) {
            return Err(Error::corrupt(
                "compact Managed operation receipts",
                "operation receipt segments are not adjacent",
            ));
        }
        Ok(Self {
            first_cursor: older.first_cursor,
            last_cursor: newer.last_cursor,
            source_bytes: add_source_bytes(
                older.source_bytes,
                newer.source_bytes,
                "compact Managed operation receipts",
            )?,
            stream,
        })
    }
}

pub(super) fn should_merge(older_source_bytes: Option<u64>, newer_source_bytes: u64) -> bool {
    older_source_bytes.is_some_and(|older| older / 2 < newer_source_bytes)
}

fn add_source_bytes(older: u64, newer: u64, operation: &'static str) -> Result<u64, Error> {
    older
        .checked_add(newer)
        .ok_or_else(|| Error::corrupt(operation, "source bytes overflow"))
}
