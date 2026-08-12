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
use std::sync::Arc;

use crate::Error;
use crate::filesystem::{ChangeCursor, VolumeId};
use crate::namespace::Namespace;
use crate::workset::WorksetOptions;

use super::authority::{AuthorityAccessDyn, AuthorityHead, AuthorityObservation};
use super::data::FileDataRef;
use super::extension::{AccessContext, FileAccessDyn};
use super::format::ManagedFormat;
use super::layout::NamespaceCommit;
use super::namespace;
use super::object::{GcEpoch, ObjectRef};
use super::publication;

#[derive(Clone)]
pub struct ManagedVolume {
    pub(super) format: ManagedFormat,
    pub(super) operator: Operator,
    pub(super) stream_concurrency: usize,
    pub(super) worksets: WorksetOptions,
    pub(super) file_access: Option<Arc<dyn FileAccessDyn>>,
    pub(super) access_context: AccessContext,
    pub(super) authority_access: Arc<dyn AuthorityAccessDyn>,
    pub(super) authority_name: String,
}

pub(crate) struct ManagedObservation {
    pub(crate) namespace: Namespace<FileDataRef>,
    pub(super) authority: AuthorityObservation,
    namespace_revision: NamespaceRevision,
    pub(super) reclamation_watermark: ChangeCursor,
    pub(super) gc_epoch: GcEpoch,
    pub(super) commit: NamespaceCommit,
}

impl ManagedObservation {
    pub(crate) const fn authority_id(&self) -> super::AuthorityId {
        self.authority.id()
    }

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

    /// Immutable namespace commit object.
    pub const fn object(self) -> ObjectRef {
        self.object
    }
}

impl ManagedVolume {
    pub(super) fn new(
        format: ManagedFormat,
        operator: Operator,
        stream_concurrency: usize,
        worksets: WorksetOptions,
        file_access: Option<Arc<dyn FileAccessDyn>>,
        authority_access: Arc<dyn AuthorityAccessDyn>,
        authority_name: String,
    ) -> Self {
        let access_context = AccessContext::new(operator.clone());
        Self {
            format,
            operator,
            stream_concurrency,
            worksets,
            file_access,
            access_context,
            authority_access,
            authority_name,
        }
    }

    pub const fn id(&self) -> VolumeId {
        self.format.volume_id()
    }

    /// Selected namespace authority name.
    pub fn authority_name(&self) -> &str {
        &self.authority_name
    }

    pub(crate) const fn pack_target_bytes(&self) -> Option<u64> {
        self.format.file_placement().pack_target_bytes()
    }

    pub(super) fn file_access(&self) -> Result<&dyn FileAccessDyn, Error> {
        self.file_access.as_deref().ok_or_else(|| {
            Error::unsupported(
                "use Managed file extension",
                "the volume file extension access is unavailable",
            )
        })
    }

    pub(super) async fn initialize(&self) -> Result<(), Error> {
        match self
            .authority_access
            .observe_dyn(&self.access_context, &self.authority_name)
            .await
        {
            Ok(_) => return self.observe().await.map(drop),
            Err(error) if error.kind() == crate::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let namespace =
            namespace::write_genesis(&self.operator, self.format.root_node_id(), GcEpoch::ZERO)
                .await?;
        let commit = NamespaceCommit::genesis(self.id(), namespace);
        let revision = publication::write_commit(self, GcEpoch::ZERO, &commit).await?;
        self.authority_access
            .initialize_dyn(
                &self.access_context,
                AuthorityHead::new(revision, GcEpoch::ZERO, ChangeCursor::GENESIS),
            )
            .await
    }

    pub(crate) async fn observe(&self) -> Result<ManagedObservation, Error> {
        let authority = self.read_authority().await?;
        let head = authority.head();
        let commit = publication::read_commit(self, head.current_commit).await?;
        let namespace = namespace::read(self, &commit, commit.change_cursor).await?;
        Ok(ManagedObservation {
            namespace,
            authority,
            namespace_revision: head.current_commit,
            reclamation_watermark: head.minimum_retained_cursor,
            gc_epoch: head.gc_epoch,
            commit,
        })
    }

    pub(super) async fn read_authority(&self) -> Result<AuthorityObservation, Error> {
        self.authority_access
            .observe_dyn(&self.access_context, &self.authority_name)
            .await
    }

    pub(crate) const fn operator(&self) -> &Operator {
        &self.operator
    }

    pub(crate) const fn workset_options(&self) -> WorksetOptions {
        self.worksets
    }

    pub(super) async fn replace_head(
        &self,
        observed: &AuthorityObservation,
        head: AuthorityHead,
    ) -> Result<bool, Error> {
        self.authority_access
            .compare_exchange_dyn(&self.access_context, &self.authority_name, observed, head)
            .await
    }
}
