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

//! Managed namespace authority and its current visible position.

use opendal::Operator;

use crate::Error;
use crate::filesystem::{ChangeCursor, VolumeId};
use crate::namespace::Namespace;
use crate::workset::WorksetOptions;

use super::data::FileDataRef;
use super::format::ManagedFormat;
use super::layout::NamespaceCommit;
use super::namespace;
use super::object::{GcEpoch, ObjectRef};
use super::publication;
use super::record::Record;
use super::storage;

const HEAD_KEY: &str = "managed/1/head";
const HEAD_RECORD: Record = Record::new(*b"OFSHEAD1", 64 * 1024);

#[derive(Clone)]
pub struct ManagedVolume {
    pub(super) format: ManagedFormat,
    pub(super) operator: Operator,
    pub(super) stream_concurrency: usize,
    pub(super) worksets: WorksetOptions,
}

pub(crate) struct ManagedObservation {
    pub(crate) namespace: Namespace<FileDataRef>,
    pub(super) head_revision: String,
    namespace_revision: NamespaceRevision,
    pub(super) reclamation_watermark: ChangeCursor,
    pub(super) gc_epoch: GcEpoch,
    pub(super) commit: NamespaceCommit,
}

impl ManagedObservation {
    pub(crate) const fn revision(&self) -> NamespaceRevision {
        self.namespace_revision
    }

    pub(crate) const fn maintenance_generation(&self) -> u64 {
        self.gc_epoch.value()
    }

    pub(crate) const fn accepts_prepared(&self, gc_epoch: u64) -> bool {
        gc_epoch == self.gc_epoch.value()
    }

    pub(crate) fn can_read_revision(&self, revision: NamespaceRevision) -> bool {
        let sequence = revision.change_cursor.sequence();
        let current = self.namespace_revision.change_cursor.sequence();
        sequence >= self.reclamation_watermark.sequence() && sequence <= current
    }

    pub(crate) const fn gc_epoch(&self) -> GcEpoch {
        self.gc_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Head {
    pub(super) current_commit: NamespaceRevision,
    pub(super) gc_epoch: GcEpoch,
    pub(super) minimum_retained_cursor: ChangeCursor,
}
super::wire::tuple_wire!(Head {
    current_commit: NamespaceRevision,
    gc_epoch: GcEpoch,
    minimum_retained_cursor: ChangeCursor,
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceRevision {
    pub(super) object: ObjectRef,
    pub(super) change_cursor: ChangeCursor,
}
super::wire::tuple_wire!(NamespaceRevision {
    object: ObjectRef,
    change_cursor: ChangeCursor,
});

impl NamespaceRevision {
    pub const fn cursor(self) -> ChangeCursor {
        self.change_cursor
    }
}

impl ManagedVolume {
    pub(super) const fn new(
        format: ManagedFormat,
        operator: Operator,
        stream_concurrency: usize,
        worksets: WorksetOptions,
    ) -> Self {
        Self {
            format,
            operator,
            stream_concurrency,
            worksets,
        }
    }

    pub const fn id(&self) -> VolumeId {
        self.format.volume_id()
    }

    pub(crate) const fn pack_target_bytes(&self) -> Option<u64> {
        self.format.file_placement().pack_target_bytes()
    }

    pub(super) async fn initialize(&self) -> Result<(), Error> {
        if storage::read_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .is_some()
        {
            return self.observe().await.map(drop);
        }

        let namespace =
            namespace::write_genesis(&self.operator, self.format.root_node_id(), GcEpoch::ZERO)
                .await?;
        let commit = NamespaceCommit::genesis(self.id(), namespace);
        let revision = publication::write_commit(self, GcEpoch::ZERO, &commit).await?;
        let head = Head {
            current_commit: revision,
            gc_epoch: GcEpoch::ZERO,
            minimum_retained_cursor: ChangeCursor::GENESIS,
        };
        if storage::write_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.encode(&head)?,
            storage::ControlCondition::Missing,
        )
        .await?
        {
            Ok(())
        } else {
            self.observe().await.map(drop)
        }
    }

    pub(crate) async fn observe(&self) -> Result<ManagedObservation, Error> {
        let (head, head_revision) = self.read_head().await?;
        let commit = publication::read_commit(self, head.current_commit).await?;
        let namespace = namespace::read(self, &commit, commit.change_cursor).await?;
        Ok(ManagedObservation {
            namespace,
            head_revision,
            namespace_revision: head.current_commit,
            reclamation_watermark: head.minimum_retained_cursor,
            gc_epoch: head.gc_epoch,
            commit,
        })
    }

    pub(super) async fn read_head(&self) -> Result<(Head, String), Error> {
        let control = storage::read_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .ok_or_else(|| Error::corrupt("open Managed volume", "namespace head is missing"))?;
        let head: Head = HEAD_RECORD.decode(&control.bytes)?;
        if head.minimum_retained_cursor.sequence() > head.current_commit.change_cursor.sequence() {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace head retention is invalid",
            ));
        }
        Ok((head, control.revision))
    }

    pub(crate) const fn operator(&self) -> &Operator {
        &self.operator
    }

    pub(crate) const fn workset_options(&self) -> WorksetOptions {
        self.worksets
    }

    pub(super) async fn replace_head(
        &self,
        expected_revision: &str,
        head: &Head,
    ) -> Result<bool, Error> {
        storage::write_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.encode(head)?,
            storage::ControlCondition::Revision(expected_revision),
        )
        .await
    }
}
