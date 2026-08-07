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

//! Concrete composition of the Managed namespace and data plane.

use opendal::Operator;

use super::namespace::{
    ContentRef, D1Namespace, D1NamespaceObservation, FileVersionRecord, NamespaceGcSweep,
    NamespaceObservation, NamespacePublication, NamespaceSnapshot, ObjectNamespace,
};
use super::{
    AuthorityKnownContent, D1Metadata, FileLayoutPolicy, LooseGcMaintenance, ManagedData,
    ManagedError, ManagedErrorKind, PackMaintenance, PackRetirement, SparseExtent,
};
use crate::filesystem::{CommitOutcome, OperationId, VolumeId};
use crate::managed::pack::{PackId, PackLocation, PackReadSession, VerifiedPack};

#[derive(Clone)]
pub struct ManagedVolume {
    namespace: NamespaceAuthority,
    data: ManagedData,
}

#[derive(Clone)]
enum NamespaceAuthority {
    Object(ObjectNamespace),
    D1(D1Namespace),
}

#[derive(Clone, Debug)]
pub struct ManagedObservation {
    authority: AuthorityObservation,
}

/// Reader state shared by one Sync materialization operation.
#[derive(Clone)]
pub(crate) struct ManagedMaterializer {
    data: ManagedData,
    packs: PackReadSession,
}

impl ManagedMaterializer {
    pub(crate) async fn materialize(
        &self,
        version: &FileVersionRecord,
        target: &Operator,
        path: &str,
    ) -> Result<(), ManagedError> {
        self.data
            .read_to_with(version, target, path, &self.packs)
            .await
    }

    pub(crate) async fn pack_locations(
        &self,
        content: ContentRef,
    ) -> Result<Vec<PackLocation>, ManagedError> {
        self.packs.locations(content).await
    }

    pub(crate) async fn read_full_pack(&self, id: PackId) -> Result<VerifiedPack, ManagedError> {
        self.packs.read_full(id).await
    }
}

#[derive(Clone, Debug)]
enum AuthorityObservation {
    Object(NamespaceObservation),
    D1(D1NamespaceObservation),
}

impl ManagedObservation {
    pub fn snapshot(&self) -> &NamespaceSnapshot {
        match &self.authority {
            AuthorityObservation::Object(observed) => &observed.snapshot,
            AuthorityObservation::D1(observed) => &observed.snapshot,
        }
    }

    pub fn gc_sweep(&self) -> Option<NamespaceGcSweep> {
        match &self.authority {
            AuthorityObservation::Object(observed) => observed.gc_sweep(),
            AuthorityObservation::D1(observed) => observed.gc_sweep(),
        }
    }
}

impl ManagedVolume {
    pub fn object(volume_id: VolumeId, data_operator: Operator) -> Result<Self, ManagedError> {
        Ok(Self {
            namespace: NamespaceAuthority::Object(ObjectNamespace::new(
                volume_id,
                data_operator.clone(),
            )?),
            data: ManagedData::new(data_operator)?,
        })
    }

    pub fn d1(
        volume_id: VolumeId,
        data_operator: Operator,
        metadata: D1Metadata,
    ) -> Result<Self, ManagedError> {
        Ok(Self {
            namespace: NamespaceAuthority::D1(D1Namespace::new(volume_id, metadata.session())),
            data: ManagedData::new(data_operator)?,
        })
    }

    pub fn with_file_layout(mut self, policy: FileLayoutPolicy) -> Result<Self, ManagedError> {
        self.data.set_policy(policy)?;
        Ok(self)
    }

    pub(crate) fn materializer(&self) -> Result<ManagedMaterializer, ManagedError> {
        Ok(ManagedMaterializer {
            data: self.data.clone(),
            packs: self.data.read_session()?,
        })
    }

    pub async fn observe(&self) -> Result<Option<ManagedObservation>, ManagedError> {
        match &self.namespace {
            NamespaceAuthority::Object(namespace) => {
                Ok(namespace
                    .observe()
                    .await?
                    .map(|observed| ManagedObservation {
                        authority: AuthorityObservation::Object(observed),
                    }))
            }
            NamespaceAuthority::D1(namespace) => {
                Ok(namespace
                    .observe()
                    .await?
                    .map(|observed| ManagedObservation {
                        authority: AuthorityObservation::D1(observed),
                    }))
            }
        }
    }

    /// Observe the authority, reusing an already verified Sync common base when it is current.
    pub async fn observe_from(
        &self,
        base: Option<&NamespaceSnapshot>,
    ) -> Result<Option<ManagedObservation>, ManagedError> {
        match (&self.namespace, base) {
            (NamespaceAuthority::Object(namespace), Some(base)) => {
                namespace.observe_from(base).await.map(|observed| {
                    observed.map(|observed| ManagedObservation {
                        authority: AuthorityObservation::Object(observed),
                    })
                })
            }
            _ => self.observe().await,
        }
    }

    pub async fn seal_whole_file(
        &self,
        frozen: &Operator,
        path: &str,
    ) -> Result<FileVersionRecord, ManagedError> {
        self.data.seal_whole_file(frozen, path).await
    }

    pub async fn seal_file(
        &self,
        frozen: &Operator,
        path: &str,
    ) -> Result<FileVersionRecord, ManagedError> {
        self.data.seal_file(frozen, path).await
    }

    pub(crate) async fn seal_file_with_known_content(
        &self,
        frozen: &Operator,
        path: &str,
        known: &AuthorityKnownContent,
    ) -> Result<FileVersionRecord, ManagedError> {
        self.data
            .seal_file_with_known_content(frozen, path, known)
            .await
    }

    pub async fn seal_extents(
        &self,
        frozen: &Operator,
        path: &str,
        extents: &[SparseExtent],
    ) -> Result<FileVersionRecord, ManagedError> {
        self.data.seal_extents(frozen, path, extents).await
    }

    pub async fn publish(
        &self,
        observed: Option<&ManagedObservation>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        match (&self.namespace, observed.map(|value| &value.authority)) {
            (NamespaceAuthority::Object(namespace), Some(AuthorityObservation::Object(base))) => {
                namespace.publish(Some(base), publication).await
            }
            (NamespaceAuthority::D1(namespace), Some(AuthorityObservation::D1(base))) => {
                namespace.publish(Some(base), publication).await
            }
            (NamespaceAuthority::Object(namespace), None) => {
                namespace.publish(None, publication).await
            }
            (NamespaceAuthority::D1(namespace), None) => namespace.publish(None, publication).await,
            _ => Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "publish Managed namespace",
                "observation belongs to another metadata authority",
            )),
        }
    }

    pub async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        match &self.namespace {
            NamespaceAuthority::Object(namespace) => namespace.resolve(operation).await,
            NamespaceAuthority::D1(namespace) => namespace.resolve(operation).await,
        }
    }

    /// Fence namespace publication and fix the snapshot used by one GC sweep.
    pub async fn begin_gc(
        &self,
        observed: &ManagedObservation,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        match (&self.namespace, &observed.authority) {
            (NamespaceAuthority::Object(namespace), AuthorityObservation::Object(observed)) => {
                namespace.begin_gc(observed).await
            }
            (NamespaceAuthority::D1(namespace), AuthorityObservation::D1(observed)) => {
                namespace.begin_gc(observed).await
            }
            _ => Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "begin Managed namespace GC",
                "observation belongs to another metadata authority",
            )),
        }
    }

    /// Release the publication fence for the matching GC sweep.
    pub async fn finish_gc(&self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        match &self.namespace {
            NamespaceAuthority::Object(namespace) => namespace.finish_gc(sweep).await,
            NamespaceAuthority::D1(namespace) => namespace.finish_gc(sweep).await,
        }
    }

    /// Delete loose objects unreachable from the snapshot fixed by this sweep.
    pub async fn collect_unreachable_loose(
        &self,
        observed: &ManagedObservation,
        sweep: NamespaceGcSweep,
    ) -> Result<LooseGcMaintenance, ManagedError> {
        if observed.gc_sweep() != Some(sweep) || observed.snapshot().cursor != sweep.fixed_cursor()
        {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "collect unreachable loose data",
                "observation does not hold this active GC sweep",
            ));
        }
        self.data
            .collect_unreachable_loose(observed.snapshot())
            .await
    }

    /// Pack small whole-file content reachable from one fixed namespace observation.
    pub async fn pack_reachable_content(
        &self,
        observed: &ManagedObservation,
        operation: OperationId,
    ) -> Result<PackMaintenance, ManagedError> {
        self.data
            .pack_reachable(observed.snapshot(), operation)
            .await
    }

    /// Repack mixed live/dead packs and publish replacement locations.
    pub async fn repack_reachable_content(
        &self,
        observed: &ManagedObservation,
        operation: OperationId,
    ) -> Result<Option<PackRetirement>, ManagedError> {
        self.data
            .repack_reachable(observed.snapshot(), operation)
            .await
    }

    /// End a process-local grace period and remove retired pack locations.
    pub async fn finalize_pack_retirement(
        &self,
        current: &ManagedObservation,
        retirement: PackRetirement,
    ) -> Result<Vec<super::pack::PackId>, ManagedError> {
        self.data
            .finalize_repack(current.snapshot(), retirement)
            .await
    }

    /// Remove live loose objects only after a packed location is verified.
    pub async fn reclaim_packed_loose(
        &self,
        current: &ManagedObservation,
    ) -> Result<usize, ManagedError> {
        self.data.reclaim_packed_loose(current.snapshot()).await
    }

    pub async fn materialize(
        &self,
        version: &FileVersionRecord,
        target: &Operator,
        path: &str,
    ) -> Result<(), ManagedError> {
        self.materializer()?
            .materialize(version, target, path)
            .await
    }
}
