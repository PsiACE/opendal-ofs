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

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use opendal::Operator;

use super::data::{RetainedDataRoots, SegmentGcMaintenance};
use super::metadata::namespace::{
    NamespaceChange, NamespaceStore, NamespaceWitness, decode_file_version, encode_file_version,
};
use super::{AuthorityKnownContent, ManagedData};
use crate::filesystem::{AuthorityIdentity, CommitOutcome, OperationId, VolumeId};
use crate::filesystem::{
    FileVersion, MaterializeRequest, Volume, VolumeError, VolumeObservation, VolumePublication,
    VolumeSnapshot,
};
use crate::managed::error::{corrupt, invalid};

#[derive(Clone)]
pub struct ManagedVolume {
    namespace: NamespaceStore,
    data: ManagedData,
}

#[derive(Clone, Debug)]
pub struct ManagedObservation {
    witness: NamespaceWitness,
    snapshot: VolumeSnapshot,
}

impl ManagedVolume {
    pub(crate) fn new(
        namespace: NamespaceStore,
        data_operator: Operator,
    ) -> Result<Self, VolumeError> {
        Ok(Self {
            namespace,
            data: ManagedData::new(data_operator)?,
        })
    }

    /// Collect segments unreachable from the fixed base namespace.
    pub async fn garbage_collect(&self, resume: bool) -> Result<SegmentGcMaintenance, VolumeError> {
        let (sweep, snapshot) = self.namespace.begin_gc(resume).await?;
        let mut roots = RetainedDataRoots::default();
        if let Some(snapshot) = snapshot {
            roots.retain(&snapshot)?;
        }
        let result = self.data.collect_unreachable_segments(&roots).await?;
        self.namespace.finish_gc(sweep).await?;
        Ok(result)
    }
}

impl VolumeObservation for ManagedObservation {
    fn snapshot(&self) -> &VolumeSnapshot {
        &self.snapshot
    }
}

impl Volume for ManagedVolume {
    type Observation = ManagedObservation;

    fn id(&self) -> VolumeId {
        self.namespace.volume_id()
    }

    fn authority(&self) -> AuthorityIdentity {
        self.namespace.binding().map_or_else(
            || AuthorityIdentity::base(self.id()),
            |binding| AuthorityIdentity::branch(self.id(), binding.clone()),
        )
    }

    fn initial_generation(&self) -> crate::filesystem::Generation {
        super::metadata::namespace::managed_generation(1)
    }

    fn next_generation(
        &self,
        generation: &crate::filesystem::Generation,
    ) -> Result<crate::filesystem::Generation, VolumeError> {
        super::metadata::namespace::next_managed_generation(generation).ok_or_else(|| {
            invalid(
                "advance filesystem generation",
                "generation is invalid or exhausted",
            )
        })
    }

    async fn observe_from(
        &self,
        base: Option<&VolumeSnapshot>,
    ) -> Result<Option<Self::Observation>, VolumeError> {
        let observed = self.namespace.observe(base).await?;
        Ok(observed.map(|(snapshot, witness)| ManagedObservation { witness, snapshot }))
    }

    async fn stage_files(
        &self,
        source: &Operator,
        segment_staging: &Operator,
        paths: Vec<String>,
        authority: Option<&VolumeSnapshot>,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersion>, VolumeError> {
        let known = if paths.is_empty() {
            AuthorityKnownContent::default()
        } else {
            authority
                .map(authority_known_content)
                .transpose()?
                .unwrap_or_default()
        };
        self.data
            .stage_files(source, segment_staging, paths, &known, concurrency)
            .await?
            .into_iter()
            .map(|(path, version)| Ok((path, encode_file_version(&version)?)))
            .collect()
    }

    async fn finalize_staged_files(
        &self,
        segment_staging: &Operator,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        self.data
            .finalize_staged_files(segment_staging, concurrency)
            .await
    }

    async fn publish(
        &self,
        observed: Option<&Self::Observation>,
        publication: &VolumePublication,
    ) -> Result<CommitOutcome, VolumeError> {
        let observed = observed.map(|observed| (&observed.witness, &observed.snapshot));
        let origin_branch = self.namespace.binding().map(|binding| binding.id);
        self.namespace
            .publish(
                observed,
                NamespaceChange::new(publication.mutation().clone(), origin_branch),
            )
            .await
    }

    async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, VolumeError> {
        self.namespace.resolve(operation).await
    }

    async fn materialize(
        &self,
        target: &Operator,
        segment_staging: Option<&Operator>,
        requests: Vec<MaterializeRequest>,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        let decoded = requests
            .into_iter()
            .map(|request| Ok((request.path, decode_file_version(&request.version)?)))
            .collect::<Result<Vec<_>, VolumeError>>()?;
        self.data
            .materialize(target, segment_staging, decoded, concurrency)
            .await
    }
}

fn authority_known_content(
    snapshot: &VolumeSnapshot,
) -> Result<AuthorityKnownContent, VolumeError> {
    let mut known = AuthorityKnownContent::default();
    let mut visited = BTreeSet::new();
    for id in snapshot.nodes.values().filter_map(|node| node.file_version) {
        if !visited.insert(id) {
            continue;
        }
        let version = snapshot.file_versions.get(&id).ok_or_else(|| {
            corrupt(
                "derive authority-known content",
                "live node references a missing file version",
            )
        })?;
        known.include(&decode_file_version(version)?);
    }
    Ok(known)
}
