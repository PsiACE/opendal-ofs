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
    FileVersionRecord, NamespacePublication, NamespaceSnapshot, NamespaceStore, NamespaceWitness,
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
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, VolumeError> {
        let base = observed
            .map(|observed| to_managed_snapshot(&observed.filesystem_snapshot))
            .transpose()?;
        let observed = observed.map(|observed| {
            (
                &observed.witness,
                base.as_ref().expect("an observation was decoded above"),
            )
        });
        self.namespace.publish(observed, publication).await
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
        staging: &Operator,
        paths: Vec<String>,
        authority: Option<&VolumeSnapshot>,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersion>, VolumeError> {
        let known = authority
            .map(authority_known_content)
            .transpose()?
            .unwrap_or_default();
        self.data
            .stage_files(source, staging, paths, &known, concurrency)
            .await?
            .into_iter()
            .map(|(path, version)| Ok((path, encode_file_version(&version)?)))
            .collect()
    }

    async fn finalize_staged_files(
        &self,
        staging: &Operator,
        files: Vec<(String, FileVersion)>,
        authority: Option<&VolumeSnapshot>,
    ) -> Result<(), VolumeError> {
        let known = authority
            .map(authority_known_content)
            .transpose()?
            .unwrap_or_default();
        let files = files
            .into_iter()
            .map(|(path, version)| Ok((path, decode_file_version(&version)?)))
            .collect::<Result<Vec<_>, VolumeError>>()?;
        self.data
            .finalize_staged_files(staging, files, &known)
            .await
    }

    async fn publish(
        &self,
        observed: Option<&Self::Observation>,
        publication: &VolumePublication,
    ) -> Result<CommitOutcome, VolumeError> {
        let publication = to_managed_publication(publication)?;
        ManagedVolume::publish(self, observed, &publication).await
    }

    async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, VolumeError> {
        self.namespace.resolve(operation).await
    }

    async fn materialize(
        &self,
        target: &Operator,
        requests: Vec<MaterializeRequest>,
        full_tree: bool,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        let decoded = requests
            .into_iter()
            .map(|request| Ok((request.path, decode_file_version(&request.version)?)))
            .collect::<Result<Vec<_>, VolumeError>>()?;
        self.data
            .materialize(target, decoded, full_tree, concurrency)
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
    let filesystem_snapshot = to_volume_snapshot(snapshot)?;
    Ok(ManagedObservation {
        witness,
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

fn to_volume_snapshot(snapshot: NamespaceSnapshot) -> Result<VolumeSnapshot, VolumeError> {
    Ok(VolumeSnapshot {
        volume_id: snapshot.volume_id,
        cursor: snapshot.cursor,
        root: snapshot.root,
        nodes: snapshot.nodes,
        directories: snapshot.directories,
        file_versions: snapshot
            .file_versions
            .into_iter()
            .map(|(id, version)| Ok((id, encode_file_version(&version)?)))
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

fn to_managed_publication(
    publication: &VolumePublication,
) -> Result<NamespacePublication, VolumeError> {
    Ok(NamespacePublication {
        operation: publication.operation,
        parent: publication.parent,
        expected_nodes: publication.expected_nodes.clone(),
        expected_directories: publication.expected_directories.clone(),
        target: to_managed_snapshot(&publication.target)?,
    })
}
