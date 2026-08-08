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

use futures::{StreamExt as _, stream};
use opendal::Operator;

use super::namespace::{
    ContentRef, D1Namespace, D1NamespaceObservation, FileVersionLayout, FileVersionRecord,
    NamespaceGcSweep, NamespaceObservation, NamespacePublication, NamespaceSnapshot,
    ObjectNamespace,
};
use super::{
    AuthorityKnownContent, D1Metadata, FileLayoutPolicy, LooseGcMaintenance, ManagedData,
    ManagedError, ManagedErrorKind, ManagedFormat, MetadataPlacement, PackMaintenance,
};
use crate::filesystem::{CommitOutcome, OperationId, VolumeId};
use crate::filesystem::{
    DirectoryRecord as FsDirectoryRecord, FileVersion, MaterializeRequest,
    NodeRecord as FsNodeRecord, Volume, VolumeError, VolumeErrorKind, VolumeObservation,
    VolumePublication, VolumeReader, VolumeSnapshot,
};
use crate::managed::pack::{PackId, PackLocation, PackReadSession, VerifiedPack};

#[derive(Clone)]
pub struct ManagedVolume {
    volume_id: VolumeId,
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

#[derive(Clone, Debug)]
pub struct ManagedVolumeObservation {
    inner: ManagedObservation,
    snapshot: VolumeSnapshot,
}

/// Reader state shared by one Sync materialization operation.
#[derive(Clone)]
pub struct ManagedMaterializer {
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

    pub(crate) async fn read_pack_ranges(
        &self,
        id: PackId,
        entries: &[(ContentRef, PackLocation)],
    ) -> Result<Vec<Vec<u8>>, ManagedError> {
        self.packs.read_ranges(id, entries).await
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
    pub fn object(format: ManagedFormat, data_operator: Operator) -> Result<Self, ManagedError> {
        if format.metadata_placement() != MetadataPlacement::ColocatedObject {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "open Managed volume",
                "superblock metadata placement is not colocated object storage",
            ));
        }
        let volume_id = format.volume_id();
        Ok(Self {
            volume_id,
            namespace: NamespaceAuthority::Object(ObjectNamespace::new(
                volume_id,
                data_operator.clone(),
            )?),
            data: ManagedData::new(data_operator, &format)?,
        })
    }

    pub fn d1(
        format: ManagedFormat,
        data_operator: Operator,
        metadata: D1Metadata,
    ) -> Result<Self, ManagedError> {
        if format.metadata_placement() != MetadataPlacement::ExternalD1 {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "open Managed volume",
                "superblock metadata placement is not external transactional storage",
            ));
        }
        let volume_id = format.volume_id();
        Ok(Self {
            volume_id,
            namespace: NamespaceAuthority::D1(D1Namespace::new(volume_id, metadata.session())),
            data: ManagedData::new(data_operator, &format)?,
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

    /// Rebuild the derived pack placement index from verified pack footers.
    pub async fn rebuild_pack_index(&self) -> Result<usize, ManagedError> {
        self.data.rebuild_pack_index().await
    }
}

impl VolumeObservation for ManagedVolumeObservation {
    fn snapshot(&self) -> &VolumeSnapshot {
        &self.snapshot
    }
}

impl VolumeReader for ManagedMaterializer {
    async fn materialize(
        &self,
        target: &Operator,
        requests: Vec<MaterializeRequest>,
        full_tree: bool,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        let mut packed =
            BTreeMap::<PackId, Vec<(MaterializeRequest, ContentRef, PackLocation)>>::new();
        let mut unpacked = Vec::new();
        for request in requests {
            let managed = decode_file_version(&request.version)?;
            let content = match &managed.layout {
                FileVersionLayout::Whole { content } if content.logical_length > 0 => {
                    Some(*content)
                }
                _ => None,
            };
            let location = match content {
                Some(content) => self
                    .pack_locations(content)
                    .await
                    .ok()
                    .and_then(|locations| locations.into_iter().next()),
                None => None,
            };
            match (content, location) {
                (Some(content), Some(location)) => packed
                    .entry(location.pack)
                    .or_default()
                    .push((request, content, location)),
                _ => unpacked.push((request, managed)),
            }
        }

        let packed_results = stream::iter(packed)
            .map(|(id, requests)| {
                let reader = self.clone();
                let target = target.clone();
                async move {
                    let contents = if full_tree {
                        reader.read_full_pack(id).await.and_then(|pack| {
                            requests
                                .iter()
                                .map(|(_, content, _)| {
                                    pack.content(*content)
                                        .map(ToOwned::to_owned)
                                        .ok_or_else(|| {
                                            ManagedError::new(
                                                ManagedErrorKind::Corrupt,
                                                "materialize Managed files",
                                                "pack index disagrees with verified pack",
                                            )
                                        })
                                })
                                .collect()
                        })
                    } else {
                        let entries = requests
                            .iter()
                            .map(|(_, content, location)| (*content, *location))
                            .collect::<Vec<_>>();
                        reader.read_pack_ranges(id, &entries).await
                    };
                    if let Ok(contents) = contents {
                        for ((request, _, _), bytes) in requests.into_iter().zip(contents) {
                            target.write(&request.path, bytes).await.map_err(|_| {
                                VolumeError::new(
                                    VolumeErrorKind::Unavailable,
                                    format!(
                                        "materialize file {:?}: target write failed",
                                        request.path
                                    ),
                                )
                            })?;
                        }
                    } else {
                        for (request, _, _) in requests {
                            let version = decode_file_version(&request.version)?;
                            reader.materialize(&version, &target, &request.path).await?;
                        }
                    }
                    Ok::<_, VolumeError>(())
                }
            })
            .buffer_unordered(concurrency.get())
            .collect::<Vec<_>>()
            .await;
        for result in packed_results {
            result?;
        }

        let unpacked_results = stream::iter(unpacked)
            .map(|(request, version)| {
                let reader = self.clone();
                let target = target.clone();
                async move {
                    reader
                        .materialize(&version, &target, &request.path)
                        .await
                        .map_err(VolumeError::from)
                }
            })
            .buffer_unordered(concurrency.get())
            .collect::<Vec<_>>()
            .await;
        for result in unpacked_results {
            result?;
        }
        Ok(())
    }
}

impl Volume for ManagedVolume {
    type Observation = ManagedVolumeObservation;
    type Reader = ManagedMaterializer;

    fn id(&self) -> VolumeId {
        self.volume_id
    }

    fn initial_generation(&self) -> crate::filesystem::Generation {
        super::namespace::managed_generation(1)
    }

    fn next_generation(
        &self,
        generation: &crate::filesystem::Generation,
    ) -> Result<crate::filesystem::Generation, VolumeError> {
        super::namespace::next_managed_generation(generation).ok_or_else(|| {
            VolumeError::new(
                VolumeErrorKind::Invalid,
                "advance filesystem generation: generation is invalid or exhausted",
            )
        })
    }

    async fn observe(&self) -> Result<Option<Self::Observation>, VolumeError> {
        let observed = ManagedVolume::observe(self).await?;
        observed.map(volume_observation).transpose()
    }

    async fn observe_from(
        &self,
        base: Option<&VolumeSnapshot>,
    ) -> Result<Option<Self::Observation>, VolumeError> {
        let base = base.map(to_managed_snapshot).transpose()?;
        let observed = ManagedVolume::observe_from(self, base.as_ref()).await?;
        observed.map(volume_observation).transpose()
    }

    async fn stage_files(
        &self,
        source: &Operator,
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
        let staged = stream::iter(paths)
            .map(|path| {
                let volume = self.clone();
                let source = source.clone();
                let known = &known;
                async move {
                    let version = volume
                        .seal_file_with_known_content(&source, &path, known)
                        .await?;
                    Ok::<_, VolumeError>((path, encode_file_version(&version)?))
                }
            })
            .buffer_unordered(concurrency.get())
            .collect::<Vec<_>>()
            .await;
        staged.into_iter().collect()
    }

    async fn publish(
        &self,
        observed: Option<&Self::Observation>,
        publication: &VolumePublication,
    ) -> Result<CommitOutcome, VolumeError> {
        let publication = to_managed_publication(publication)?;
        ManagedVolume::publish(
            self,
            observed.map(|observation| &observation.inner),
            &publication,
        )
        .await
        .map_err(Into::into)
    }

    async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, VolumeError> {
        ManagedVolume::resolve(self, operation)
            .await
            .map_err(Into::into)
    }

    fn reader(&self) -> Result<Self::Reader, VolumeError> {
        self.materializer().map_err(Into::into)
    }
}

fn volume_observation(
    observed: ManagedObservation,
) -> Result<ManagedVolumeObservation, VolumeError> {
    let snapshot = to_volume_snapshot(observed.snapshot())?;
    Ok(ManagedVolumeObservation {
        inner: observed,
        snapshot,
    })
}

fn encode_file_version(version: &FileVersionRecord) -> Result<FileVersion, VolumeError> {
    let mut descriptor = Vec::new();
    ciborium::into_writer(&version.layout, &mut descriptor).map_err(|error| {
        VolumeError::new(
            VolumeErrorKind::Invalid,
            format!("encode Managed file version: {error}"),
        )
    })?;
    Ok(FileVersion::from_parts(
        version.id,
        version.logical_size,
        version.logical_digest,
        descriptor,
    ))
}

fn decode_file_version(version: &FileVersion) -> Result<FileVersionRecord, VolumeError> {
    let layout: FileVersionLayout =
        ciborium::from_reader(version.descriptor()).map_err(|error| {
            VolumeError::new(
                VolumeErrorKind::Corrupt,
                format!("decode Managed file version: {error}"),
            )
        })?;
    let decoded = FileVersionRecord::from_layout(
        version.logical_size,
        version.logical_digest,
        layout,
    )
    .filter(|decoded| decoded.id == version.id)
    .ok_or_else(|| {
        VolumeError::new(
            VolumeErrorKind::Corrupt,
            "decode Managed file version: descriptor does not match its filesystem identity",
        )
    })?;
    Ok(decoded)
}

fn to_volume_snapshot(snapshot: &NamespaceSnapshot) -> Result<VolumeSnapshot, VolumeError> {
    Ok(VolumeSnapshot {
        volume_id: snapshot.volume_id,
        cursor: snapshot.cursor,
        root: snapshot.root,
        nodes: snapshot
            .nodes
            .iter()
            .map(|(id, record)| {
                (
                    *id,
                    FsNodeRecord {
                        id: record.id,
                        generation: record.generation.clone(),
                        kind: record.kind,
                        attributes: record.attributes,
                        file_version: record.file_version,
                    },
                )
            })
            .collect(),
        directories: snapshot
            .directories
            .iter()
            .map(|(id, record)| {
                (
                    *id,
                    FsDirectoryRecord {
                        node: record.node,
                        generation: record.generation.clone(),
                        entries: record.entries.clone(),
                    },
                )
            })
            .collect(),
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
        nodes: snapshot
            .nodes
            .iter()
            .map(|(id, record)| {
                (
                    *id,
                    super::namespace::NodeRecord {
                        id: record.id,
                        generation: record.generation.clone(),
                        kind: record.kind,
                        attributes: record.attributes,
                        file_version: record.file_version,
                    },
                )
            })
            .collect(),
        directories: snapshot
            .directories
            .iter()
            .map(|(id, record)| {
                (
                    *id,
                    super::namespace::DirectoryRecord {
                        node: record.node,
                        generation: record.generation.clone(),
                        entries: record.entries.clone(),
                    },
                )
            })
            .collect(),
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
        expected_nodes: publication
            .expected_nodes
            .iter()
            .map(|precondition| super::namespace::NodePrecondition {
                node: precondition.node,
                expected_generation: precondition.expected_generation.clone(),
            })
            .collect(),
        expected_directories: publication
            .expected_directories
            .iter()
            .map(|precondition| super::namespace::DirectoryPrecondition {
                directory: precondition.directory,
                expected_generation: precondition.expected_generation.clone(),
            })
            .collect(),
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
