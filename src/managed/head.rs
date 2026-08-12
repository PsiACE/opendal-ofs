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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use opendal::Operator;
use serde::{Deserialize, Serialize};

use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, FileFingerprint, FileVersionId, NodeAttributes,
    NodeId, NodeKind, NodeRecord, OperationId, VolumeId, VolumeSnapshot,
};
use crate::{Error, ErrorKind};

use super::data::{FileExtent, FileExtentRecord, FileLayout};
use super::format::ManagedFormat;
use super::object::{self, GcEpoch, ObjectClass, ObjectRef};
use super::record::Record;
use super::stream::{self, StreamKind, StreamRef};

const HEAD_KEY: &str = "managed/1/head";
const HEAD_RECORD: Record = Record::new(*b"OFSHEAD1", 1, 64 * 1024);
const COMMIT_RECORD: Record = Record::new(*b"OFSCMIT1", 1, 4 * 1024 * 1024);
const PROJECTION_RECORD: Record = Record::new(*b"OFSPROJ1", 1, 4 * 1024 * 1024);

#[derive(Clone)]
pub struct ManagedVolume {
    format: ManagedFormat,
    operator: Operator,
    file_versions: Arc<RwLock<BTreeMap<FileVersionId, FileVersionRecord>>>,
    file_extents: Arc<RwLock<BTreeMap<FileVersionId, Vec<FileExtent>>>>,
}

pub struct ManagedObservation {
    pub snapshot: VolumeSnapshot,
    head_revision: String,
    namespace_revision: NamespaceRevision,
    reclamation_watermark: ChangeCursor,
    gc_epoch: GcEpoch,
    operations: BTreeMap<OperationId, OperationRecord>,
    commit: NamespaceCommit,
    node_values: BTreeMap<NodeId, NodeValue>,
    directory_entries: BTreeMap<DirectoryKey, DirectoryEntry>,
}

#[derive(Default)]
pub(crate) struct StagedFileRecords {
    file_versions: Vec<StreamRef>,
    file_extents: Vec<StreamRef>,
}

impl StagedFileRecords {
    pub(crate) fn extend(&mut self, other: Self) {
        self.file_versions.extend(other.file_versions);
        self.file_extents.extend(other.file_extents);
    }
}

impl ManagedObservation {
    pub const fn revision(&self) -> NamespaceRevision {
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
    object: ObjectRef,
    change_cursor: ChangeCursor,
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

#[derive(Clone, Debug)]
struct NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    nodes: Vec<StreamRef>,
    directory_entries: Vec<StreamRef>,
    file_versions: Vec<StreamRef>,
    file_extents: Vec<StreamRef>,
    changes: Vec<StreamRef>,
    operation_results: Vec<StreamRef>,
    projections: Vec<ProjectionRef>,
    extensions: Vec<ExtensionRef>,
}
super::wire::tuple_wire!(NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    nodes: Vec<StreamRef>,
    directory_entries: Vec<StreamRef>,
    file_versions: Vec<StreamRef>,
    file_extents: Vec<StreamRef>,
    changes: Vec<StreamRef>,
    operation_results: Vec<StreamRef>,
    projections: Vec<ProjectionRef>,
    extensions: Vec<ExtensionRef>,
});

#[derive(Clone, Debug)]
struct ProjectionRef {
    kind: String,
    schema_version: u16,
    source: ProjectionSource,
    manifest: ObjectRef,
}
super::wire::tuple_wire!(ProjectionRef {
    kind: String,
    schema_version: u16,
    source: ProjectionSource,
    manifest: ObjectRef,
});

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProjectionSource {
    Namespace {
        volume_id: VolumeId,
        change_cursor: ChangeCursor,
        stream_kinds: Vec<StreamKind>,
    },
    Payloads {
        payload_digests: Vec<super::object::PayloadDigest>,
    },
}

impl Serialize for ProjectionSource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple as _;
        match self {
            Self::Namespace {
                volume_id,
                change_cursor,
                stream_kinds,
            } => {
                let mut tuple = serializer.serialize_tuple(4)?;
                tuple.serialize_element(&0_u8)?;
                tuple.serialize_element(volume_id)?;
                tuple.serialize_element(change_cursor)?;
                tuple.serialize_element(stream_kinds)?;
                tuple.end()
            }
            Self::Payloads { payload_digests } => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&1_u8)?;
                tuple.serialize_element(payload_digests)?;
                tuple.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ProjectionSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ProjectionSourceVisitor;

        impl<'de> serde::de::Visitor<'de> for ProjectionSourceVisitor {
            type Value = ProjectionSource;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Managed projection source array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let tag = next_element(&mut sequence, "projection source tag")?;
                let value = match tag {
                    0_u8 => Self::Value::Namespace {
                        volume_id: next_element(&mut sequence, "source volume identity")?,
                        change_cursor: next_element(&mut sequence, "source change cursor")?,
                        stream_kinds: next_element(&mut sequence, "source stream kinds")?,
                    },
                    1_u8 => Self::Value::Payloads {
                        payload_digests: next_element(&mut sequence, "source payload digests")?,
                    },
                    _ => return Err(serde::de::Error::custom("unknown projection source tag")),
                };
                require_sequence_end(&mut sequence)?;
                Ok(value)
            }
        }

        deserializer.deserialize_seq(ProjectionSourceVisitor)
    }
}

#[derive(Clone, Debug)]
struct ProjectionManifest {
    kind: String,
    schema_version: u16,
    source: ProjectionSource,
    streams: Vec<StreamRef>,
}
super::wire::tuple_wire!(ProjectionManifest {
    kind: String,
    schema_version: u16,
    source: ProjectionSource,
    streams: Vec<StreamRef>,
});

#[derive(Clone, Debug)]
struct ExtensionRef {
    kind: String,
    schema_version: u16,
    manifest: ObjectRef,
}
super::wire::tuple_wire!(ExtensionRef {
    kind: String,
    schema_version: u16,
    manifest: ObjectRef,
});

impl NamespaceCommit {
    fn genesis(volume_id: VolumeId, nodes: StreamRef) -> Self {
        Self {
            volume_id,
            change_cursor: ChangeCursor::Genesis,
            nodes: vec![nodes],
            directory_entries: Vec::new(),
            file_versions: Vec::new(),
            file_extents: Vec::new(),
            changes: Vec::new(),
            operation_results: Vec::new(),
            projections: Vec::new(),
            extensions: Vec::new(),
        }
    }

    fn streams(&self) -> impl Iterator<Item = StreamRef> + '_ {
        self.nodes
            .iter()
            .chain(&self.directory_entries)
            .chain(&self.file_versions)
            .chain(&self.file_extents)
            .chain(&self.changes)
            .chain(&self.operation_results)
            .copied()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectoryKey {
    parent_node_id: NodeId,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NodeValue {
    Directory {
        generation: u64,
        attributes: NodeAttributes,
        directory_generation: u64,
    },
    RegularFile {
        generation: u64,
        attributes: NodeAttributes,
        file_version: FileVersionId,
    },
}

impl Serialize for NodeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple as _;
        match self {
            Self::Directory {
                generation,
                attributes,
                directory_generation,
            } => {
                let mut tuple = serializer.serialize_tuple(4)?;
                tuple.serialize_element(&0_u8)?;
                tuple.serialize_element(generation)?;
                tuple.serialize_element(&attributes.executable)?;
                tuple.serialize_element(directory_generation)?;
                tuple.end()
            }
            Self::RegularFile {
                generation,
                attributes,
                file_version,
            } => {
                let mut tuple = serializer.serialize_tuple(4)?;
                tuple.serialize_element(&1_u8)?;
                tuple.serialize_element(generation)?;
                tuple.serialize_element(&attributes.executable)?;
                tuple.serialize_element(file_version)?;
                tuple.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for NodeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NodeValueVisitor;

        impl<'de> serde::de::Visitor<'de> for NodeValueVisitor {
            type Value = NodeValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Managed node value array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let tag = next_element(&mut sequence, "node value tag")?;
                let generation = next_element(&mut sequence, "node generation")?;
                let executable = next_element(&mut sequence, "node executable attribute")?;
                let value = match tag {
                    0_u8 => Self::Value::Directory {
                        generation,
                        attributes: NodeAttributes { executable },
                        directory_generation: next_element(&mut sequence, "directory generation")?,
                    },
                    1_u8 => Self::Value::RegularFile {
                        generation,
                        attributes: NodeAttributes { executable },
                        file_version: next_element(&mut sequence, "file version")?,
                    },
                    _ => return Err(serde::de::Error::custom("unknown node value tag")),
                };
                require_sequence_end(&mut sequence)?;
                Ok(value)
            }
        }

        deserializer.deserialize_seq(NodeValueVisitor)
    }
}

impl NodeValue {
    const fn generation(&self) -> u64 {
        match self {
            Self::Directory { generation, .. } | Self::RegularFile { generation, .. } => {
                *generation
            }
        }
    }

    const fn directory_generation(&self) -> Option<u64> {
        match self {
            Self::Directory {
                directory_generation,
                ..
            } => Some(*directory_generation),
            Self::RegularFile { .. } => None,
        }
    }

    fn record(&self, file_versions: &BTreeMap<FileVersionId, FileVersionRecord>) -> NodeRecord {
        match self {
            Self::Directory { attributes, .. } => NodeRecord {
                kind: NodeKind::Directory,
                attributes: *attributes,
                file_version: None,
                file_fingerprint: None,
            },
            Self::RegularFile {
                attributes,
                file_version,
                ..
            } => NodeRecord {
                kind: NodeKind::RegularFile,
                attributes: *attributes,
                file_version: Some(*file_version),
                file_fingerprint: file_versions
                    .get(file_version)
                    .map(|record| record.content_fingerprint),
            },
        }
    }
}

#[derive(Debug)]
struct NodeMutation {
    node_id: NodeId,
    change_cursor: ChangeCursor,
    value: Option<NodeValue>,
}
super::wire::tuple_wire!(NodeMutation {
    node_id: NodeId,
    change_cursor: ChangeCursor,
    value: Option<NodeValue>,
});

#[derive(Debug)]
struct DirectoryMutation {
    parent_node_id: NodeId,
    name: String,
    change_cursor: ChangeCursor,
    value: Option<DirectoryEntry>,
}
super::wire::tuple_wire!(DirectoryMutation {
    parent_node_id: NodeId,
    name: String,
    change_cursor: ChangeCursor,
    value: Option<DirectoryEntry>,
});

#[derive(Clone, Copy, Debug)]
pub(super) struct FileVersionRecord {
    pub(super) file_version: FileVersionId,
    pub(super) file_size: u64,
    pub(super) content_fingerprint: FileFingerprint,
}
super::wire::tuple_wire!(FileVersionRecord {
    file_version: FileVersionId,
    file_size: u64,
    content_fingerprint: FileFingerprint,
});

#[derive(Debug)]
struct ChangeRecord {
    change_cursor: ChangeCursor,
    ordinal: u32,
    operation_id: OperationId,
    event: ChangeEvent,
}
super::wire::tuple_wire!(ChangeRecord {
    change_cursor: ChangeCursor,
    ordinal: u32,
    operation_id: OperationId,
    event: ChangeEvent,
});

#[derive(Debug)]
enum ChangeEvent {
    NodeChanged {
        node_id: NodeId,
        generation: u64,
    },
    NodeRemoved {
        node_id: NodeId,
        previous_generation: u64,
    },
    EntryLinked {
        parent_node_id: NodeId,
        name: String,
        node_id: NodeId,
        kind: NodeKind,
        directory_generation: u64,
    },
    EntryUnlinked {
        parent_node_id: NodeId,
        name: String,
        node_id: NodeId,
        directory_generation: u64,
    },
}

impl Serialize for ChangeEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple as _;
        match self {
            Self::NodeChanged {
                node_id,
                generation,
            } => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&0_u8)?;
                tuple.serialize_element(node_id)?;
                tuple.serialize_element(generation)?;
                tuple.end()
            }
            Self::NodeRemoved {
                node_id,
                previous_generation,
            } => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&1_u8)?;
                tuple.serialize_element(node_id)?;
                tuple.serialize_element(previous_generation)?;
                tuple.end()
            }
            Self::EntryLinked {
                parent_node_id,
                name,
                node_id,
                kind,
                directory_generation,
            } => {
                let mut tuple = serializer.serialize_tuple(6)?;
                tuple.serialize_element(&2_u8)?;
                tuple.serialize_element(parent_node_id)?;
                tuple.serialize_element(name)?;
                tuple.serialize_element(node_id)?;
                tuple.serialize_element(kind)?;
                tuple.serialize_element(directory_generation)?;
                tuple.end()
            }
            Self::EntryUnlinked {
                parent_node_id,
                name,
                node_id,
                directory_generation,
            } => {
                let mut tuple = serializer.serialize_tuple(5)?;
                tuple.serialize_element(&3_u8)?;
                tuple.serialize_element(parent_node_id)?;
                tuple.serialize_element(name)?;
                tuple.serialize_element(node_id)?;
                tuple.serialize_element(directory_generation)?;
                tuple.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ChangeEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ChangeEventVisitor;

        impl<'de> serde::de::Visitor<'de> for ChangeEventVisitor {
            type Value = ChangeEvent;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Managed change event array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let tag = next_element(&mut sequence, "change event tag")?;
                let value = match tag {
                    0_u8 => Self::Value::NodeChanged {
                        node_id: next_element(&mut sequence, "node identity")?,
                        generation: next_element(&mut sequence, "node generation")?,
                    },
                    1_u8 => Self::Value::NodeRemoved {
                        node_id: next_element(&mut sequence, "node identity")?,
                        previous_generation: next_element(
                            &mut sequence,
                            "previous node generation",
                        )?,
                    },
                    2_u8 => Self::Value::EntryLinked {
                        parent_node_id: next_element(&mut sequence, "parent node identity")?,
                        name: next_element(&mut sequence, "entry name")?,
                        node_id: next_element(&mut sequence, "node identity")?,
                        kind: next_element(&mut sequence, "node kind")?,
                        directory_generation: next_element(&mut sequence, "directory generation")?,
                    },
                    3_u8 => Self::Value::EntryUnlinked {
                        parent_node_id: next_element(&mut sequence, "parent node identity")?,
                        name: next_element(&mut sequence, "entry name")?,
                        node_id: next_element(&mut sequence, "node identity")?,
                        directory_generation: next_element(&mut sequence, "directory generation")?,
                    },
                    _ => return Err(serde::de::Error::custom("unknown change event tag")),
                };
                require_sequence_end(&mut sequence)?;
                Ok(value)
            }
        }

        deserializer.deserialize_seq(ChangeEventVisitor)
    }
}

fn next_element<'de, A, T>(sequence: &mut A, name: &'static str) -> Result<T, A::Error>
where
    A: serde::de::SeqAccess<'de>,
    T: Deserialize<'de>,
{
    sequence
        .next_element()?
        .ok_or_else(|| serde::de::Error::custom(format!("missing {name}")))
}

fn require_sequence_end<'de, A>(sequence: &mut A) -> Result<(), A::Error>
where
    A: serde::de::SeqAccess<'de>,
{
    if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
        return Err(serde::de::Error::custom("record array has trailing fields"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct OperationRecord {
    operation_id: OperationId,
    change_cursor: ChangeCursor,
}
super::wire::tuple_wire!(OperationRecord {
    operation_id: OperationId,
    change_cursor: ChangeCursor,
});

struct StoredNamespace {
    snapshot: VolumeSnapshot,
    operations: BTreeMap<OperationId, OperationRecord>,
    commit: NamespaceCommit,
    node_values: BTreeMap<NodeId, NodeValue>,
    directory_entries: BTreeMap<DirectoryKey, DirectoryEntry>,
    file_versions: BTreeMap<FileVersionId, FileVersionRecord>,
    file_extents: BTreeMap<FileVersionId, Vec<FileExtent>>,
}

impl ManagedVolume {
    pub(super) fn new(format: ManagedFormat, operator: Operator) -> Self {
        Self {
            format,
            operator,
            file_versions: Arc::new(RwLock::new(BTreeMap::new())),
            file_extents: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub const fn id(&self) -> VolumeId {
        self.format.volume_id()
    }

    pub(super) async fn initialize(&self) -> Result<(), Error> {
        if object::read_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .is_some()
        {
            return self.observe().await.map(drop);
        }
        let root = NodeMutation {
            node_id: self.format.root_node_id(),
            change_cursor: ChangeCursor::Genesis,
            value: Some(NodeValue::Directory {
                generation: 1,
                attributes: NodeAttributes::default(),
                directory_generation: 1,
            }),
        };
        let nodes = stream::write_records(
            &self.operator,
            GcEpoch::ZERO,
            ObjectClass::NodeSegment,
            StreamKind::NODE_MUTATIONS,
            [root],
        )
        .await?;
        let commit = NamespaceCommit::genesis(self.id(), nodes);
        let revision = self.write_commit(GcEpoch::ZERO, &commit).await?;
        let head = Head {
            current_commit: revision,
            gc_epoch: GcEpoch::ZERO,
            minimum_retained_cursor: ChangeCursor::Genesis,
        };
        if object::create_control(&self.operator, HEAD_KEY, HEAD_RECORD.encode(&head)?).await? {
            Ok(())
        } else {
            self.observe().await.map(drop)
        }
    }

    pub async fn observe(&self) -> Result<ManagedObservation, Error> {
        let (head, head_revision) = self.read_head().await?;
        let stored = self.read_namespace(head.current_commit).await?;
        Ok(ManagedObservation {
            snapshot: stored.snapshot,
            head_revision,
            namespace_revision: head.current_commit,
            reclamation_watermark: head.minimum_retained_cursor,
            gc_epoch: head.gc_epoch,
            operations: stored.operations,
            commit: stored.commit,
            node_values: stored.node_values,
            directory_entries: stored.directory_entries,
        })
    }

    pub(super) async fn read_head(&self) -> Result<(Head, String), Error> {
        let (bytes, revision) = object::read_control_with_revision(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .ok_or_else(|| Error::corrupt("open Managed volume", "namespace head is missing"))?;
        let head: Head = HEAD_RECORD.decode(&bytes)?;
        if head.minimum_retained_cursor.sequence() > head.current_commit.change_cursor.sequence() {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace head retention is invalid",
            ));
        }
        Ok((head, revision))
    }

    pub(crate) async fn prepare_publication(
        &self,
        observed: &ManagedObservation,
        target: VolumeSnapshot,
        files: StagedFileRecords,
    ) -> Result<NamespaceRevision, Error> {
        target.validate()?;
        if target.volume_id != self.id()
            || target.root != self.format.root_node_id()
            || target.cursor.sequence() != observed.snapshot.cursor.sequence() + 1
            || target.cursor.operation().is_none()
        {
            return Err(Error::invalid(
                "publish Managed namespace",
                "publication ancestry is invalid",
            ));
        }
        self.write_namespace(observed, &target, files).await
    }

    pub(crate) async fn stage_file_records(
        &self,
        gc_epoch: GcEpoch,
        files: Vec<(FileVersionId, FileFingerprint, FileLayout)>,
    ) -> Result<StagedFileRecords, Error> {
        let mut file_versions = Vec::with_capacity(files.len());
        let mut file_extents = Vec::new();
        for (version, fingerprint, layout) in files {
            file_versions.push(FileVersionRecord {
                file_version: version,
                file_size: fingerprint.logical_length(),
                content_fingerprint: fingerprint,
            });
            file_extents.extend(Self::extent_records(version, layout));
        }
        let mut staged = StagedFileRecords::default();
        append_stream(
            &self.operator,
            gc_epoch,
            &mut staged.file_versions,
            ObjectClass::FileVersionSegment,
            StreamKind::FILE_VERSION_RECORDS,
            file_versions,
        )
        .await?;
        append_stream(
            &self.operator,
            gc_epoch,
            &mut staged.file_extents,
            ObjectClass::FileExtentSegment,
            StreamKind::FILE_EXTENT_RECORDS,
            file_extents,
        )
        .await?;
        Ok(staged)
    }

    pub async fn commit_publication(
        &self,
        observed: &ManagedObservation,
        target: NamespaceRevision,
        operation: OperationId,
    ) -> Result<(), Error> {
        if target.change_cursor.sequence() != observed.snapshot.cursor.sequence() + 1 {
            return Err(Error::invalid(
                "publish Managed namespace",
                "prepared publication ancestry is invalid",
            ));
        }
        let head = Head {
            current_commit: target,
            gc_epoch: observed.gc_epoch,
            minimum_retained_cursor: observed.reclamation_watermark,
        };
        if object::replace_control(
            &self.operator,
            HEAD_KEY,
            &observed.head_revision,
            HEAD_RECORD.encode(&head)?,
        )
        .await?
        {
            return Ok(());
        }
        let current = self.observe().await?;
        if current
            .operations
            .get(&operation)
            .is_some_and(|result| result.change_cursor == target.change_cursor)
        {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Conflict,
                "publish Managed namespace",
                "observed generation changed",
            ))
        }
    }

    pub async fn snapshot(&self, revision: NamespaceRevision) -> Result<VolumeSnapshot, Error> {
        let (head, _) = self.read_head().await?;
        if revision.change_cursor.sequence() < head.minimum_retained_cursor.sequence()
            || revision.change_cursor.sequence() > head.current_commit.change_cursor.sequence()
        {
            return Err(Error::invalid(
                "read Managed namespace",
                "requested change cursor is outside the retained interval",
            ));
        }
        let commit = self.read_commit(head.current_commit).await?;
        self.read_namespace_streams_at(commit, revision.change_cursor)
            .await
            .map(|stored| stored.snapshot)
    }

    pub async fn operation_committed(
        &self,
        operation: OperationId,
        observed: &ManagedObservation,
    ) -> Result<bool, Error> {
        Ok(observed.operations.contains_key(&operation))
    }

    pub(crate) fn operator(&self) -> &Operator {
        &self.operator
    }

    pub(super) async fn replace_head(
        &self,
        expected_revision: &str,
        head: &Head,
    ) -> Result<bool, Error> {
        object::replace_control(
            &self.operator,
            HEAD_KEY,
            expected_revision,
            HEAD_RECORD.encode(head)?,
        )
        .await
    }

    pub(super) async fn visit_reachable_objects(
        &self,
        reference: NamespaceRevision,
        mut visit: impl FnMut(ObjectRef) -> Result<(), Error>,
    ) -> Result<(), Error> {
        visit(reference.object)?;
        let commit = self.read_commit(reference).await?;
        for stream in commit.streams() {
            visit(stream.object)?;
        }
        for reference in &commit.projections {
            visit(reference.manifest)?;
            let projection = self.read_projection_manifest(reference).await?;
            for stream in projection.streams {
                visit(stream.object)?;
            }
        }
        if !commit.extensions.is_empty() {
            return Err(Error::unsupported(
                "collect Managed data",
                "namespace contains an unsupported semantic extension",
            ));
        }
        let stored = self.read_namespace_streams(commit).await?;
        for extents in stored.file_extents.values() {
            for extent in extents {
                visit(extent.shard.object)?;
            }
        }
        Ok(())
    }

    pub(super) async fn compact_for_collection(
        &self,
        reference: NamespaceRevision,
        gc_epoch: GcEpoch,
    ) -> Result<NamespaceRevision, Error> {
        let stored = self.read_namespace(reference).await?;
        let change_cursor = stored.commit.change_cursor;
        let nodes = stored
            .node_values
            .into_iter()
            .map(|(node_id, value)| NodeMutation {
                node_id,
                change_cursor,
                value: Some(value),
            })
            .collect::<Vec<_>>();
        let directory_entries = stored
            .directory_entries
            .into_iter()
            .map(|(key, value)| DirectoryMutation {
                parent_node_id: key.parent_node_id,
                name: key.name,
                change_cursor,
                value: Some(value),
            })
            .collect::<Vec<_>>();
        let live_versions = stored
            .snapshot
            .nodes
            .values()
            .filter_map(|node| node.file_version)
            .collect::<BTreeSet<_>>();
        let file_versions = stored
            .file_versions
            .into_iter()
            .filter_map(|(version, record)| live_versions.contains(&version).then_some(record))
            .collect::<Vec<_>>();
        let file_extents = stored
            .file_extents
            .into_iter()
            .filter(|(version, _)| live_versions.contains(version))
            .flat_map(|(version, extents)| {
                extents.into_iter().map(move |extent| FileExtentRecord {
                    file_version: version,
                    logical_range: extent.logical_range,
                    shard: extent.shard,
                    object_range: extent.object_range,
                })
            })
            .collect::<Vec<_>>();
        let operation_results = stored.operations.into_values().collect::<Vec<_>>();

        let mut commit = NamespaceCommit {
            volume_id: stored.commit.volume_id,
            change_cursor,
            nodes: Vec::new(),
            directory_entries: Vec::new(),
            file_versions: Vec::new(),
            file_extents: Vec::new(),
            changes: Vec::new(),
            operation_results: Vec::new(),
            projections: stored.commit.projections,
            extensions: Vec::new(),
        };
        append_stream(
            &self.operator,
            gc_epoch,
            &mut commit.nodes,
            ObjectClass::NodeSegment,
            StreamKind::NODE_MUTATIONS,
            nodes,
        )
        .await?;
        append_stream(
            &self.operator,
            gc_epoch,
            &mut commit.directory_entries,
            ObjectClass::DirectorySegment,
            StreamKind::DIRECTORY_MUTATIONS,
            directory_entries,
        )
        .await?;
        append_stream(
            &self.operator,
            gc_epoch,
            &mut commit.file_versions,
            ObjectClass::FileVersionSegment,
            StreamKind::FILE_VERSION_RECORDS,
            file_versions,
        )
        .await?;
        append_stream(
            &self.operator,
            gc_epoch,
            &mut commit.file_extents,
            ObjectClass::FileExtentSegment,
            StreamKind::FILE_EXTENT_RECORDS,
            file_extents,
        )
        .await?;
        append_stream(
            &self.operator,
            gc_epoch,
            &mut commit.operation_results,
            ObjectClass::OperationResultSegment,
            StreamKind::OPERATION_RESULTS,
            operation_results,
        )
        .await?;
        self.write_commit(gc_epoch, &commit).await
    }

    async fn write_namespace(
        &self,
        observed: &ManagedObservation,
        target: &VolumeSnapshot,
        files: StagedFileRecords,
    ) -> Result<NamespaceRevision, Error> {
        let cursor = target.cursor;
        let operation = cursor
            .operation()
            .expect("validated target has an operation identity");
        let target_entries = flatten_directories(target);
        let mut node_mutations = Vec::new();
        let mut node_values = observed.node_values.clone();
        let node_ids = observed
            .snapshot
            .nodes
            .keys()
            .chain(target.nodes.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        for node_id in node_ids {
            let previous_record = observed.snapshot.nodes.get(&node_id);
            let target_record = target.nodes.get(&node_id);
            if previous_record == target_record {
                continue;
            }
            let value = match target_record {
                Some(record) => {
                    let generation = observed
                        .node_values
                        .get(&node_id)
                        .map_or(Ok(1), |value| next_generation(value.generation()))?;
                    let value = match record.kind {
                        NodeKind::Directory => {
                            let previous_entries = observed.snapshot.directories.get(&node_id);
                            let target_entries = target.directories.get(&node_id);
                            let directory_generation = observed
                                .node_values
                                .get(&node_id)
                                .and_then(NodeValue::directory_generation)
                                .map_or(Ok(1), |value| {
                                    if previous_entries == target_entries {
                                        Ok(value)
                                    } else {
                                        next_generation(value)
                                    }
                                })?;
                            NodeValue::Directory {
                                generation,
                                attributes: record.attributes,
                                directory_generation,
                            }
                        }
                        NodeKind::RegularFile => NodeValue::RegularFile {
                            generation,
                            attributes: record.attributes,
                            file_version: record.file_version.ok_or_else(|| {
                                Error::invalid(
                                    "publish Managed namespace",
                                    "regular file has no file version",
                                )
                            })?,
                        },
                    };
                    node_values.insert(node_id, value.clone());
                    Some(value)
                }
                None => {
                    node_values.remove(&node_id);
                    None
                }
            };
            node_mutations.push(NodeMutation {
                node_id,
                change_cursor: cursor,
                value,
            });
        }

        let mut directory_mutations = Vec::new();
        let entry_keys = observed
            .directory_entries
            .keys()
            .chain(target_entries.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for key in entry_keys {
            let previous = observed.directory_entries.get(&key);
            let target = target_entries.get(&key);
            if previous == target {
                continue;
            }
            directory_mutations.push(DirectoryMutation {
                parent_node_id: key.parent_node_id,
                name: key.name,
                change_cursor: cursor,
                value: target.copied(),
            });
        }

        let mut changes = Vec::new();
        for mutation in &node_mutations {
            let event = match &mutation.value {
                Some(value) => ChangeEvent::NodeChanged {
                    node_id: mutation.node_id,
                    generation: value.generation(),
                },
                None => ChangeEvent::NodeRemoved {
                    node_id: mutation.node_id,
                    previous_generation: observed.node_values[&mutation.node_id].generation(),
                },
            };
            changes.push(event);
        }
        for mutation in &directory_mutations {
            let directory_generation = node_values
                .get(&mutation.parent_node_id)
                .and_then(NodeValue::directory_generation)
                .or_else(|| {
                    observed
                        .node_values
                        .get(&mutation.parent_node_id)
                        .and_then(NodeValue::directory_generation)
                })
                .ok_or_else(|| {
                    Error::invalid(
                        "publish Managed namespace",
                        "directory mutation parent is invalid",
                    )
                })?;
            let event = match mutation.value {
                Some(entry) => ChangeEvent::EntryLinked {
                    parent_node_id: mutation.parent_node_id,
                    name: mutation.name.clone(),
                    node_id: entry.node_id,
                    kind: entry.kind,
                    directory_generation,
                },
                None => {
                    let previous = observed.directory_entries[&DirectoryKey {
                        parent_node_id: mutation.parent_node_id,
                        name: mutation.name.clone(),
                    }];
                    ChangeEvent::EntryUnlinked {
                        parent_node_id: mutation.parent_node_id,
                        name: mutation.name.clone(),
                        node_id: previous.node_id,
                        directory_generation,
                    }
                }
            };
            changes.push(event);
        }
        let changes = changes
            .into_iter()
            .enumerate()
            .map(|(ordinal, event)| {
                Ok(ChangeRecord {
                    change_cursor: cursor,
                    ordinal: u32::try_from(ordinal).map_err(|_| {
                        Error::invalid("publish Managed namespace", "change ordinal overflows")
                    })?,
                    operation_id: operation,
                    event,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let mut commit = observed.commit.clone();
        commit.change_cursor = cursor;
        commit.file_versions.extend(files.file_versions);
        commit.file_extents.extend(files.file_extents);
        append_stream(
            &self.operator,
            observed.gc_epoch,
            &mut commit.nodes,
            ObjectClass::NodeSegment,
            StreamKind::NODE_MUTATIONS,
            node_mutations,
        )
        .await?;
        append_stream(
            &self.operator,
            observed.gc_epoch,
            &mut commit.directory_entries,
            ObjectClass::DirectorySegment,
            StreamKind::DIRECTORY_MUTATIONS,
            directory_mutations,
        )
        .await?;
        append_stream(
            &self.operator,
            observed.gc_epoch,
            &mut commit.changes,
            ObjectClass::ChangeSegment,
            StreamKind::CHANGE_RECORDS,
            changes,
        )
        .await?;
        append_stream(
            &self.operator,
            observed.gc_epoch,
            &mut commit.operation_results,
            ObjectClass::OperationResultSegment,
            StreamKind::OPERATION_RESULTS,
            [OperationRecord {
                operation_id: operation,
                change_cursor: cursor,
            }],
        )
        .await?;
        self.write_commit(observed.gc_epoch, &commit).await
    }

    async fn write_commit(
        &self,
        gc_epoch: GcEpoch,
        commit: &NamespaceCommit,
    ) -> Result<NamespaceRevision, Error> {
        let object = object::write_immutable(
            &self.operator,
            gc_epoch,
            ObjectClass::NamespaceCommit,
            COMMIT_RECORD.encode(commit)?,
        )
        .await?;
        Ok(NamespaceRevision {
            object,
            change_cursor: commit.change_cursor,
        })
    }

    async fn read_namespace(&self, reference: NamespaceRevision) -> Result<StoredNamespace, Error> {
        let commit = self.read_commit(reference).await?;
        self.read_namespace_streams(commit).await
    }

    async fn read_namespace_streams(
        &self,
        commit: NamespaceCommit,
    ) -> Result<StoredNamespace, Error> {
        let view_cursor = commit.change_cursor;
        self.read_namespace_streams_at(commit, view_cursor).await
    }

    async fn read_namespace_streams_at(
        &self,
        commit: NamespaceCommit,
        view_cursor: ChangeCursor,
    ) -> Result<StoredNamespace, Error> {
        let mut node_values = BTreeMap::new();
        for reference in &commit.nodes {
            require_stream(
                *reference,
                StreamKind::NODE_MUTATIONS,
                ObjectClass::NodeSegment,
            )?;
            stream::visit_records(&self.operator, *reference, |mutation: NodeMutation| {
                if mutation.change_cursor.sequence() > commit.change_cursor.sequence() {
                    return Err(Error::corrupt(
                        "read Managed namespace",
                        "node mutation is newer than its commit",
                    ));
                }
                if mutation.change_cursor.sequence() > view_cursor.sequence() {
                    return Ok(());
                }
                match mutation.value {
                    Some(value) => {
                        node_values.insert(mutation.node_id, value);
                    }
                    None => {
                        node_values.remove(&mutation.node_id);
                    }
                }
                Ok(())
            })
            .await?;
        }
        let mut directory_entries = BTreeMap::new();
        for reference in &commit.directory_entries {
            require_stream(
                *reference,
                StreamKind::DIRECTORY_MUTATIONS,
                ObjectClass::DirectorySegment,
            )?;
            stream::visit_records(&self.operator, *reference, |mutation: DirectoryMutation| {
                if mutation.change_cursor.sequence() > view_cursor.sequence() {
                    return Ok(());
                }
                let key = DirectoryKey {
                    parent_node_id: mutation.parent_node_id,
                    name: mutation.name,
                };
                match mutation.value {
                    Some(value) => {
                        directory_entries.insert(key, value);
                    }
                    None => {
                        directory_entries.remove(&key);
                    }
                }
                Ok(())
            })
            .await?;
        }
        let mut file_versions = BTreeMap::new();
        for reference in &commit.file_versions {
            require_stream(
                *reference,
                StreamKind::FILE_VERSION_RECORDS,
                ObjectClass::FileVersionSegment,
            )?;
            stream::visit_records(&self.operator, *reference, |record: FileVersionRecord| {
                file_versions.insert(record.file_version, record);
                Ok(())
            })
            .await?;
        }
        let mut file_extents = BTreeMap::<FileVersionId, Vec<FileExtent>>::new();
        for reference in &commit.file_extents {
            require_stream(
                *reference,
                StreamKind::FILE_EXTENT_RECORDS,
                ObjectClass::FileExtentSegment,
            )?;
            stream::visit_records(&self.operator, *reference, |record: FileExtentRecord| {
                file_extents
                    .entry(record.file_version)
                    .or_default()
                    .push(FileExtent {
                        logical_range: record.logical_range,
                        shard: record.shard,
                        object_range: record.object_range,
                    });
                Ok(())
            })
            .await?;
        }
        for reference in &commit.changes {
            require_stream(
                *reference,
                StreamKind::CHANGE_RECORDS,
                ObjectClass::ChangeSegment,
            )?;
        }
        let mut operations = BTreeMap::new();
        for reference in &commit.operation_results {
            require_stream(
                *reference,
                StreamKind::OPERATION_RESULTS,
                ObjectClass::OperationResultSegment,
            )?;
            stream::visit_records(&self.operator, *reference, |record: OperationRecord| {
                if record.change_cursor.sequence() <= view_cursor.sequence() {
                    operations.insert(record.operation_id, record);
                }
                Ok(())
            })
            .await?;
        }

        let nodes = node_values
            .iter()
            .map(|(id, value)| (*id, value.record(&file_versions)))
            .collect::<BTreeMap<_, _>>();
        let mut directories = node_values
            .iter()
            .filter(|(_, value)| matches!(value, NodeValue::Directory { .. }))
            .map(|(id, _)| {
                (
                    *id,
                    DirectoryRecord {
                        entries: BTreeMap::new(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        for (key, entry) in &directory_entries {
            directories
                .get_mut(&key.parent_node_id)
                .ok_or_else(|| {
                    Error::corrupt(
                        "read Managed namespace",
                        "directory entry parent is missing",
                    )
                })?
                .entries
                .insert(key.name.clone(), *entry);
        }
        let snapshot = VolumeSnapshot {
            volume_id: commit.volume_id,
            cursor: view_cursor,
            root: self.format.root_node_id(),
            nodes,
            directories,
        };
        snapshot.validate()?;
        self.file_versions
            .write()
            .map_err(|_| Error::unavailable("read Managed namespace", "file cache failed"))?
            .extend(
                file_versions
                    .iter()
                    .map(|(version, record)| (*version, *record)),
            );
        self.file_extents
            .write()
            .map_err(|_| Error::unavailable("read Managed namespace", "file cache failed"))?
            .extend(
                file_extents
                    .iter()
                    .map(|(version, extents)| (*version, extents.clone())),
            );
        Ok(StoredNamespace {
            snapshot,
            operations,
            commit,
            node_values,
            directory_entries,
            file_versions,
            file_extents,
        })
    }

    pub(super) fn file_version_record(
        &self,
        version: FileVersionId,
    ) -> Result<FileVersionRecord, Error> {
        self.file_versions
            .read()
            .map_err(|_| Error::unavailable("read Managed file", "file cache failed"))?
            .get(&version)
            .copied()
            .ok_or_else(|| Error::corrupt("read Managed file", "file version is not indexed"))
    }

    pub(super) fn file_extents(&self, version: FileVersionId) -> Result<Vec<FileExtent>, Error> {
        Ok(self
            .file_extents
            .read()
            .map_err(|_| Error::unavailable("read Managed file", "file cache failed"))?
            .get(&version)
            .cloned()
            .unwrap_or_default())
    }

    async fn read_commit(&self, reference: NamespaceRevision) -> Result<NamespaceCommit, Error> {
        if reference.object.class != ObjectClass::NamespaceCommit {
            return Err(Error::corrupt(
                "read Managed namespace",
                "commit reference has the wrong object class",
            ));
        }
        let bytes = object::read_immutable(
            &self.operator,
            reference.object,
            COMMIT_RECORD.maximum_encoded_bytes(),
        )
        .await?;
        let commit: NamespaceCommit = COMMIT_RECORD.decode(&bytes)?;
        if commit.volume_id != self.id() || commit.change_cursor != reference.change_cursor {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace commit does not match its reference",
            ));
        }
        if !commit.extensions.is_empty() {
            return Err(Error::unsupported(
                "read Managed namespace",
                "namespace contains an unsupported semantic extension",
            ));
        }
        Ok(commit)
    }

    async fn read_projection_manifest(
        &self,
        reference: &ProjectionRef,
    ) -> Result<ProjectionManifest, Error> {
        if reference.manifest.class != ObjectClass::ProjectionManifest {
            return Err(Error::corrupt(
                "read Managed projection",
                "projection manifest has the wrong object class",
            ));
        }
        let bytes = object::read_immutable(
            &self.operator,
            reference.manifest,
            PROJECTION_RECORD.maximum_encoded_bytes(),
        )
        .await?;
        let manifest: ProjectionManifest = PROJECTION_RECORD.decode(&bytes)?;
        if manifest.kind != reference.kind
            || manifest.schema_version != reference.schema_version
            || manifest.source != reference.source
        {
            return Err(Error::corrupt(
                "read Managed projection",
                "projection manifest does not match its reference",
            ));
        }
        Ok(manifest)
    }
}

async fn append_stream<T: Serialize>(
    operator: &Operator,
    gc_epoch: GcEpoch,
    streams: &mut Vec<StreamRef>,
    class: ObjectClass,
    kind: StreamKind,
    records: impl IntoIterator<Item = T>,
) -> Result<(), Error> {
    let records = records.into_iter().collect::<Vec<_>>();
    if records.is_empty() {
        return Ok(());
    }
    streams.push(stream::write_records(operator, gc_epoch, class, kind, records).await?);
    Ok(())
}

fn require_stream(reference: StreamRef, kind: StreamKind, class: ObjectClass) -> Result<(), Error> {
    if reference.kind != kind || reference.object.class != class {
        return Err(Error::corrupt(
            "read Managed namespace",
            "stream reference has the wrong type",
        ));
    }
    Ok(())
}

fn flatten_directories(snapshot: &VolumeSnapshot) -> BTreeMap<DirectoryKey, DirectoryEntry> {
    let mut entries = BTreeMap::new();
    for (parent_node_id, directory) in &snapshot.directories {
        for (name, entry) in &directory.entries {
            entries.insert(
                DirectoryKey {
                    parent_node_id: *parent_node_id,
                    name: name.clone(),
                },
                *entry,
            );
        }
    }
    entries
}

fn next_generation(generation: u64) -> Result<u64, Error> {
    generation
        .checked_add(1)
        .ok_or_else(|| Error::corrupt("publish Managed namespace", "node generation overflows"))
}
