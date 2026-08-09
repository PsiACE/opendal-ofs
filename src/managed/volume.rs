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

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use opendal::Operator;

#[cfg(feature = "managed-branch")]
use super::extensions::branch::{BoundNamespace, BranchWitness};
use super::format::ExtentMap;
use super::metadata::namespace::{
    FileVersionRecord, NamespaceGcSweep, NamespacePublication, NamespaceSnapshot, NamespaceStore,
    NamespaceWitness,
};
use super::{
    AuthorityKnownContent, D1Metadata, ManagedData, ManagedError, ManagedErrorKind, ManagedFormat,
    MetadataFormat, SegmentGcMaintenance,
};
use crate::filesystem::{AuthorityIdentity, CommitOutcome, OperationId, VolumeId};
use crate::filesystem::{
    FileVersion, MaterializeRequest, Volume, VolumeError, VolumeErrorKind, VolumeObservation,
    VolumePublication, VolumeSnapshot,
};

#[derive(Clone)]
pub struct ManagedVolume {
    volume_id: VolumeId,
    namespace: NamespaceAuthority,
    data: ManagedData,
}

#[derive(Clone)]
enum NamespaceAuthority {
    Base(NamespaceStore),
    #[cfg(feature = "managed-branch")]
    Branch(BoundNamespace),
}

#[derive(Clone, Debug)]
pub struct ManagedObservation {
    authority: AuthorityObservation,
    filesystem_snapshot: VolumeSnapshot,
}

#[derive(Clone, Debug)]
enum AuthorityObservation {
    Base(NamespaceWitness),
    #[cfg(feature = "managed-branch")]
    Branch(Box<BranchWitness>),
}

impl ManagedObservation {
    fn gc_sweep(&self) -> Option<NamespaceGcSweep> {
        match &self.authority {
            AuthorityObservation::Base(witness) => witness.gc_sweep(),
            #[cfg(feature = "managed-branch")]
            AuthorityObservation::Branch(_) => None,
        }
    }
}

impl ManagedVolume {
    pub fn object(format: ManagedFormat, data_operator: Operator) -> Result<Self, ManagedError> {
        if format.metadata_format() != MetadataFormat::ObjectV1
            || !format.required_extensions().is_empty()
        {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "open Managed volume",
                "superblock metadata format is not object/1",
            ));
        }
        let volume_id = format.volume_id();
        Ok(Self {
            volume_id,
            namespace: NamespaceAuthority::Base(NamespaceStore::object(
                volume_id,
                data_operator.clone(),
            )?),
            data: ManagedData::new(data_operator)?,
        })
    }

    #[cfg(feature = "managed-branch")]
    pub fn branch(
        format: ManagedFormat,
        data_operator: Operator,
        namespace: BoundNamespace,
    ) -> Result<Self, ManagedError> {
        if !format.requires_extension(super::ManagedExtension::BranchV1)
            || namespace.volume_id() != format.volume_id()
        {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "open Managed volume",
                "branch namespace does not match the Managed format",
            ));
        }
        Ok(Self {
            volume_id: format.volume_id(),
            namespace: NamespaceAuthority::Branch(namespace),
            data: ManagedData::new(data_operator)?,
        })
    }

    pub fn d1(
        format: ManagedFormat,
        data_operator: Operator,
        metadata: D1Metadata,
    ) -> Result<Self, ManagedError> {
        if format.metadata_format() != MetadataFormat::TransactionalV1
            || !format.required_extensions().is_empty()
        {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "open Managed volume",
                "superblock metadata format is not transactional/1",
            ));
        }
        let volume_id = format.volume_id();
        Ok(Self {
            volume_id,
            namespace: NamespaceAuthority::Base(NamespaceStore::d1(
                volume_id,
                data_operator.clone(),
                metadata,
            )),
            data: ManagedData::new(data_operator)?,
        })
    }

    pub async fn observe(&self) -> Result<Option<ManagedObservation>, ManagedError> {
        match &self.namespace {
            NamespaceAuthority::Base(namespace) => namespace
                .observe()
                .await?
                .map(|observed| {
                    let (snapshot, witness) = observed.into_parts();
                    managed_observation(snapshot, AuthorityObservation::Base(witness))
                })
                .transpose(),
            #[cfg(feature = "managed-branch")]
            NamespaceAuthority::Branch(namespace) => namespace
                .observe()
                .await?
                .map(|observed| {
                    let (snapshot, witness) = observed.into_parts();
                    managed_observation(snapshot, AuthorityObservation::Branch(Box::new(witness)))
                })
                .transpose(),
        }
    }

    /// Observe the authority, reusing an already verified Sync common base when it is current.
    async fn observe_from(
        &self,
        base: Option<&NamespaceSnapshot>,
    ) -> Result<Option<ManagedObservation>, ManagedError> {
        match (&self.namespace, base) {
            (NamespaceAuthority::Base(namespace), Some(base)) => namespace
                .observe_from(base)
                .await?
                .map(|observed| {
                    let (snapshot, witness) = observed.into_parts();
                    managed_observation(snapshot, AuthorityObservation::Base(witness))
                })
                .transpose(),
            #[cfg(feature = "managed-branch")]
            (NamespaceAuthority::Branch(namespace), Some(base)) => namespace
                .observe_from(base)
                .await?
                .map(|observed| {
                    let (snapshot, witness) = observed.into_parts();
                    managed_observation(snapshot, AuthorityObservation::Branch(Box::new(witness)))
                })
                .transpose(),
            _ => self.observe().await,
        }
    }

    async fn publish(
        &self,
        observed: Option<&ManagedObservation>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        let base = observed
            .map(|observed| to_managed_snapshot(&observed.filesystem_snapshot))
            .transpose()?;
        match (&self.namespace, observed.map(|value| &value.authority)) {
            (NamespaceAuthority::Base(namespace), Some(AuthorityObservation::Base(witness))) => {
                namespace
                    .publish(
                        Some((witness, base.as_ref().expect("decoded above"))),
                        publication,
                    )
                    .await
            }
            (NamespaceAuthority::Base(namespace), None) => {
                namespace.publish(None, publication).await
            }
            #[cfg(feature = "managed-branch")]
            (
                NamespaceAuthority::Branch(namespace),
                Some(AuthorityObservation::Branch(witness)),
            ) => {
                namespace
                    .publish(
                        Some((witness, base.as_ref().expect("decoded above"))),
                        publication,
                    )
                    .await
            }
            #[cfg(feature = "managed-branch")]
            (NamespaceAuthority::Branch(namespace), None) => {
                namespace.publish(None, publication).await
            }
            #[cfg(feature = "managed-branch")]
            _ => Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "publish Managed namespace",
                "observation belongs to another metadata authority",
            )),
        }
    }

    async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        match &self.namespace {
            NamespaceAuthority::Base(namespace) => namespace.resolve(operation).await,
            #[cfg(feature = "managed-branch")]
            NamespaceAuthority::Branch(namespace) => namespace.resolve(operation).await,
        }
    }

    /// Fence namespace publication and fix the snapshot used by one GC sweep.
    pub async fn begin_gc(
        &self,
        observed: &ManagedObservation,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        match (&self.namespace, &observed.authority) {
            (NamespaceAuthority::Base(namespace), AuthorityObservation::Base(observed)) => {
                namespace.begin_gc(observed).await
            }
            #[cfg(feature = "managed-branch")]
            (NamespaceAuthority::Branch(_), AuthorityObservation::Branch(_)) => {
                Err(ManagedError::new(
                    ManagedErrorKind::Invalid,
                    "begin Managed namespace GC",
                    "branch GC must be started through its volume control plane",
                ))
            }
            #[cfg(feature = "managed-branch")]
            _ => Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "begin Managed namespace GC",
                "observation belongs to another metadata authority",
            )),
        }
    }

    /// Take ownership of an interrupted namespace GC sweep.
    pub async fn resume_gc(
        &self,
        observed: &ManagedObservation,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        match (&self.namespace, &observed.authority) {
            (NamespaceAuthority::Base(namespace), AuthorityObservation::Base(observed)) => {
                namespace.resume_gc(observed).await
            }
            #[cfg(feature = "managed-branch")]
            (NamespaceAuthority::Branch(_), AuthorityObservation::Branch(_)) => {
                Err(ManagedError::new(
                    ManagedErrorKind::Invalid,
                    "resume Managed namespace GC",
                    "branch GC must be resumed through its volume control plane",
                ))
            }
            #[cfg(feature = "managed-branch")]
            _ => Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "resume Managed namespace GC",
                "observation belongs to another metadata authority",
            )),
        }
    }

    /// Release the publication fence for the matching GC sweep.
    pub async fn finish_gc(&self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        match &self.namespace {
            NamespaceAuthority::Base(namespace) => namespace.finish_gc(sweep).await,
            #[cfg(feature = "managed-branch")]
            NamespaceAuthority::Branch(_) => Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "finish Managed namespace GC",
                "branch GC must be finished through its volume control plane",
            )),
        }
    }

    /// Delete data segments unreachable from the snapshot fixed by this sweep.
    pub async fn collect_unreachable_segments(
        &self,
        sweep: NamespaceGcSweep,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let observed = self.observe().await?.ok_or_else(|| {
            ManagedError::new(
                ManagedErrorKind::Conflict,
                "collect unreachable data segments",
                "namespace authority changed",
            )
        })?;
        if observed.gc_sweep() != Some(sweep)
            || observed.filesystem_snapshot.cursor != sweep.fixed_cursor()
        {
            return Err(ManagedError::new(
                ManagedErrorKind::Conflict,
                "collect unreachable data segments",
                "GC sweep ownership changed",
            ));
        }
        let snapshot = to_managed_snapshot(&observed.filesystem_snapshot)?;
        self.data.collect_unreachable_segments(&snapshot).await
    }
}

impl VolumeObservation for ManagedObservation {
    fn snapshot(&self) -> &VolumeSnapshot {
        &self.filesystem_snapshot
    }
}

async fn materialize_managed_files(
    volume: &ManagedVolume,
    target: &Operator,
    requests: Vec<MaterializeRequest>,
    full_tree: bool,
    concurrency: NonZeroUsize,
) -> Result<(), VolumeError> {
    let decoded = requests
        .into_iter()
        .map(|request| Ok((request.path, decode_file_version(&request.version)?)))
        .collect::<Result<Vec<_>, VolumeError>>()?;
    volume
        .data
        .materialize(target, decoded, full_tree, concurrency)
        .await
        .map_err(Into::into)
}

impl Volume for ManagedVolume {
    type Observation = ManagedObservation;

    fn id(&self) -> VolumeId {
        self.volume_id
    }

    fn authority(&self) -> AuthorityIdentity {
        match &self.namespace {
            #[cfg(feature = "managed-branch")]
            NamespaceAuthority::Branch(namespace) => {
                AuthorityIdentity::branch(self.volume_id, namespace.binding().clone())
            }
            _ => AuthorityIdentity::base(self.volume_id),
        }
    }

    fn initial_generation(&self) -> crate::filesystem::Generation {
        super::metadata::namespace::managed_generation(1)
    }

    fn next_generation(
        &self,
        generation: &crate::filesystem::Generation,
    ) -> Result<crate::filesystem::Generation, VolumeError> {
        super::metadata::namespace::next_managed_generation(generation).ok_or_else(|| {
            VolumeError::new(
                VolumeErrorKind::Invalid,
                "advance filesystem generation: generation is invalid or exhausted",
            )
        })
    }

    async fn observe_from(
        &self,
        base: Option<&VolumeSnapshot>,
    ) -> Result<Option<Self::Observation>, VolumeError> {
        let base = base.map(to_managed_snapshot).transpose()?;
        ManagedVolume::observe_from(self, base.as_ref())
            .await
            .map_err(Into::into)
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
            .map(to_managed_snapshot)
            .transpose()?
            .as_ref()
            .map(AuthorityKnownContent::from_snapshot)
            .transpose()?
            .unwrap_or_default();
        self.data
            .stage_files(source, staging, paths, &known, concurrency)
            .await?
            .into_iter()
            .map(|(path, version)| Ok((path, encode_file_version(&version)?)))
            .collect()
    }

    async fn publish(
        &self,
        observed: Option<&Self::Observation>,
        publication: &VolumePublication,
    ) -> Result<CommitOutcome, VolumeError> {
        let publication = to_managed_publication(publication)?;
        ManagedVolume::publish(self, observed, &publication)
            .await
            .map_err(Into::into)
    }

    async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, VolumeError> {
        ManagedVolume::resolve(self, operation)
            .await
            .map_err(Into::into)
    }

    async fn materialize(
        &self,
        target: &Operator,
        requests: Vec<MaterializeRequest>,
        full_tree: bool,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        materialize_managed_files(self, target, requests, full_tree, concurrency).await
    }
}

fn managed_observation(
    snapshot: NamespaceSnapshot,
    authority: AuthorityObservation,
) -> Result<ManagedObservation, ManagedError> {
    let filesystem_snapshot = to_volume_snapshot(snapshot)?;
    Ok(ManagedObservation {
        authority,
        filesystem_snapshot,
    })
}

fn encode_file_version(version: &FileVersionRecord) -> Result<FileVersion, ManagedError> {
    let mut descriptor = Vec::new();
    ciborium::into_writer(&version.extent_map, &mut descriptor).map_err(|error| {
        ManagedError::new(
            ManagedErrorKind::Invalid,
            "encode Managed file version",
            error.to_string(),
        )
    })?;
    Ok(FileVersion::from_parts(
        version.id,
        version.logical_size,
        version.logical_digest,
        descriptor,
    ))
}

fn decode_file_version(version: &FileVersion) -> Result<FileVersionRecord, ManagedError> {
    let extent_map: ExtentMap = ciborium::from_reader(version.descriptor()).map_err(|error| {
        ManagedError::new(
            ManagedErrorKind::Corrupt,
            "decode Managed file version",
            error.to_string(),
        )
    })?;
    let decoded =
        FileVersionRecord::from_extents(version.logical_size, version.logical_digest, extent_map)
            .filter(|decoded| decoded.id == version.id)
            .ok_or_else(|| {
                ManagedError::new(
                    ManagedErrorKind::Corrupt,
                    "decode Managed file version",
                    "descriptor does not match its filesystem identity",
                )
            })?;
    Ok(decoded)
}

fn to_volume_snapshot(snapshot: NamespaceSnapshot) -> Result<VolumeSnapshot, ManagedError> {
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
            .collect::<Result<_, ManagedError>>()?,
    })
}

fn to_managed_snapshot(snapshot: &VolumeSnapshot) -> Result<NamespaceSnapshot, ManagedError> {
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
            .collect::<Result<_, ManagedError>>()?,
    })
}

fn to_managed_publication(
    publication: &VolumePublication,
) -> Result<NamespacePublication, ManagedError> {
    Ok(NamespacePublication {
        operation: publication.operation,
        parent: publication.parent,
        expected_nodes: publication.expected_nodes.clone(),
        expected_directories: publication.expected_directories.clone(),
        target: to_managed_snapshot(&publication.target)?,
    })
}

impl From<ManagedError> for VolumeError {
    fn from(error: ManagedError) -> Self {
        let kind = match error.kind() {
            ManagedErrorKind::UnsupportedFormat => VolumeErrorKind::UnsupportedFormat,
            ManagedErrorKind::Invalid => VolumeErrorKind::Invalid,
            ManagedErrorKind::Conflict => VolumeErrorKind::Conflict,
            ManagedErrorKind::Corrupt => VolumeErrorKind::Corrupt,
            ManagedErrorKind::Unavailable => VolumeErrorKind::Unavailable,
        };
        Self::new(kind, error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest as _, Sha256};

    use super::*;
    use crate::managed::format::{ContentRef, Extent, SegmentRef};

    #[test]
    fn filesystem_descriptor_round_trips_a_multi_extent_file() {
        let first = b"first ";
        let second = b"second";
        let logical_digest: [u8; 32] = Sha256::digest([first.as_slice(), second].concat()).into();
        let record = FileVersionRecord::from_extents(
            (first.len() + second.len()) as u64,
            logical_digest,
            ExtentMap {
                extents: vec![
                    Extent {
                        logical_offset: 0,
                        content: ContentRef {
                            digest: Sha256::digest(first).into(),
                            length: first.len() as u64,
                        },
                        segment: SegmentRef {
                            digest: [1; 32],
                            length: 128,
                        },
                        segment_offset: 10,
                    },
                    Extent {
                        logical_offset: first.len() as u64,
                        content: ContentRef {
                            digest: Sha256::digest(second).into(),
                            length: second.len() as u64,
                        },
                        segment: SegmentRef {
                            digest: [1; 32],
                            length: 128,
                        },
                        segment_offset: 16,
                    },
                ],
            },
        )
        .unwrap();

        let exposed = encode_file_version(&record).unwrap();
        assert_eq!(decode_file_version(&exposed).unwrap(), record);
    }
}
