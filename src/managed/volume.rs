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

use super::format::ExtentMap;
use super::metadata::namespace::{
    FileVersionRecord, NamespaceChange, NamespaceSnapshot, NamespaceStore, NamespaceWitness,
};
use super::{AuthorityKnownContent, ManagedData};
use crate::filesystem::{AuthorityIdentity, CommitOutcome, OperationId, VolumeId};
use crate::filesystem::{
    FileVersion, MaterializeRequest, Volume, VolumeError, VolumeMutation, VolumeObservation,
    VolumePublication, VolumeSnapshot,
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
    managed_snapshot: NamespaceSnapshot,
    filesystem_snapshot: VolumeSnapshot,
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

    /// Observe the authority, reusing an already verified Sync common base when it is current.
    async fn observe_from(
        &self,
        base: Option<&NamespaceSnapshot>,
    ) -> Result<Option<ManagedObservation>, VolumeError> {
        match base {
            Some(base) => self.namespace.observe_from(base).await?,
            None => self.namespace.observe().await?,
        }
        .map(|observed| {
            let (snapshot, witness) = observed.into_parts();
            managed_observation(snapshot, witness)
        })
        .transpose()
    }

    async fn publish(
        &self,
        observed: Option<&ManagedObservation>,
        mutation: VolumeMutation<FileVersionRecord>,
    ) -> Result<CommitOutcome, VolumeError> {
        let observed = observed.map(|observed| (&observed.witness, &observed.managed_snapshot));
        let origin_branch = self.namespace.binding().map(|binding| binding.id);
        self.namespace
            .publish(observed, NamespaceChange::new(mutation, origin_branch))
            .await
    }
}

impl VolumeObservation for ManagedObservation {
    fn snapshot(&self) -> &VolumeSnapshot {
        &self.filesystem_snapshot
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
        let base = base.map(to_managed_snapshot).transpose()?;
        ManagedVolume::observe_from(self, base.as_ref()).await
    }

    async fn stage_files(
        &self,
        source: &Operator,
        segment_staging: &Operator,
        paths: Vec<String>,
        authority: Option<&VolumeSnapshot>,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersion>, VolumeError> {
        let known = authority
            .map(authority_known_content)
            .transpose()?
            .unwrap_or_default();
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
        ManagedVolume::publish(self, observed, to_managed_mutation(publication.mutation())?).await
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
        known.include(&decode_file_version(version)?)?;
    }
    Ok(known)
}

fn managed_observation(
    snapshot: NamespaceSnapshot,
    witness: NamespaceWitness,
) -> Result<ManagedObservation, VolumeError> {
    let filesystem_snapshot = to_volume_snapshot(&snapshot)?;
    Ok(ManagedObservation {
        witness,
        managed_snapshot: snapshot,
        filesystem_snapshot,
    })
}

fn encode_file_version(version: &FileVersionRecord) -> Result<FileVersion, VolumeError> {
    let mut descriptor = Vec::new();
    ciborium::into_writer(&version.extent_map, &mut descriptor)
        .map_err(|error| invalid("encode Managed file version", error.to_string()))?;
    Ok(FileVersion::from_parts(
        version.id,
        version.logical_size,
        version.logical_digest,
        descriptor,
    ))
}

fn decode_file_version(version: &FileVersion) -> Result<FileVersionRecord, VolumeError> {
    let extent_map: ExtentMap = ciborium::from_reader(version.descriptor())
        .map_err(|error| corrupt("decode Managed file version", error.to_string()))?;
    let decoded =
        FileVersionRecord::from_extents(version.logical_size, version.logical_digest, extent_map)
            .filter(|decoded| decoded.id == version.id)
            .ok_or_else(|| {
                corrupt(
                    "decode Managed file version",
                    "descriptor does not match its filesystem identity",
                )
            })?;
    Ok(decoded)
}

fn to_volume_snapshot(snapshot: &NamespaceSnapshot) -> Result<VolumeSnapshot, VolumeError> {
    Ok(VolumeSnapshot {
        volume_id: snapshot.volume_id,
        cursor: snapshot.cursor,
        root: snapshot.root,
        nodes: snapshot.nodes.clone(),
        directories: snapshot.directories.clone(),
        file_versions: snapshot
            .file_versions
            .iter()
            .map(|(id, version)| Ok((*id, encode_file_version(version)?)))
            .collect::<Result<_, VolumeError>>()?,
    })
}

fn to_managed_snapshot(snapshot: &VolumeSnapshot) -> Result<NamespaceSnapshot, VolumeError> {
    Ok(NamespaceSnapshot {
        volume_id: snapshot.volume_id,
        cursor: snapshot.cursor,
        root: snapshot.root,
        nodes: snapshot.nodes.clone(),
        directories: snapshot.directories.clone(),
        file_versions: snapshot
            .file_versions
            .iter()
            .map(|(id, version)| Ok((*id, decode_file_version(version)?)))
            .collect::<Result<_, VolumeError>>()?,
    })
}

fn to_managed_mutation(
    mutation: &VolumeMutation,
) -> Result<VolumeMutation<FileVersionRecord>, VolumeError> {
    Ok(VolumeMutation {
        volume_id: mutation.volume_id,
        operation: mutation.operation,
        parent: mutation.parent,
        cursor: mutation.cursor,
        root: mutation.root,
        expected_nodes: mutation.expected_nodes.clone(),
        expected_directories: mutation.expected_directories.clone(),
        put_nodes: mutation.put_nodes.clone(),
        remove_nodes: mutation.remove_nodes.clone(),
        put_directories: mutation.put_directories.clone(),
        remove_directories: mutation.remove_directories.clone(),
        put_file_versions: mutation
            .put_file_versions
            .iter()
            .map(decode_file_version)
            .collect::<Result<_, _>>()?,
        remove_file_versions: mutation.remove_file_versions.clone(),
    })
}
