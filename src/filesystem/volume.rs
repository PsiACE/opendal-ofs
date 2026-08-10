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

//! Backend-neutral filesystem view and operations exposed to access models.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use opendal::Operator;
use serde::{Deserialize, Serialize};
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use super::AuthorityIdentity;
use super::{
    ChangeCursor, CommitOutcome, DirectoryEntry, FileVersionId, Generation, NodeAttributes, NodeId,
    NodeKind, OperationId, VolumeId,
};

/// An immutable file version whose durable descriptor is owned by its volume.
///
/// Access models may persist and return `descriptor`, but must not interpret it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileVersion {
    pub id: FileVersionId,
    pub logical_size: u64,
    pub logical_digest: [u8; 32],
    descriptor: Box<[u8]>,
}

impl FileVersion {
    pub fn from_parts(
        id: FileVersionId,
        logical_size: u64,
        logical_digest: [u8; 32],
        descriptor: impl Into<Box<[u8]>>,
    ) -> Self {
        Self {
            id,
            logical_size,
            logical_digest,
            descriptor: descriptor.into(),
        }
    }

    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeRecord {
    pub id: NodeId,
    pub generation: Generation,
    pub kind: NodeKind,
    pub attributes: NodeAttributes,
    pub file_version: Option<FileVersionId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryRecord {
    pub node: NodeId,
    pub generation: Generation,
    pub entries: BTreeMap<String, DirectoryEntry>,
}

/// A backend-neutral, complete filesystem observation.
///
/// `F` is the volume-owned file-version representation. Access frontends use
/// the default opaque [`FileVersion`], while a volume implementation may use
/// its decoded representation internally without copying the namespace model.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeSnapshot {
    pub volume_id: VolumeId,
    pub cursor: ChangeCursor,
    pub root: NodeId,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    pub directories: BTreeMap<NodeId, DirectoryRecord>,
    pub file_versions: BTreeMap<FileVersionId, FileVersion>,
}

impl VolumeSnapshot {
    /// Return every non-root path in this namespace.
    ///
    /// Walking also proves that directories form a tree. Regular files may be
    /// linked from more than one directory.
    pub(crate) fn paths(&self) -> Result<BTreeMap<String, NodeId>, VolumeError> {
        let mut paths = BTreeMap::new();
        let mut pending = vec![(String::new(), self.root)];
        let mut expanded = BTreeSet::new();
        while let Some((path, node)) = pending.pop() {
            if !path.is_empty() && paths.insert(path.clone(), node).is_some() {
                return Err(invalid_snapshot("namespace contains a duplicate path"));
            }
            let record = self
                .nodes
                .get(&node)
                .ok_or_else(|| invalid_snapshot("namespace references a missing node"))?;
            if record.kind != NodeKind::Directory {
                continue;
            }
            if !expanded.insert(node) {
                return Err(invalid_snapshot("namespace directories do not form a tree"));
            }
            let directory = self
                .directories
                .get(&node)
                .ok_or_else(|| invalid_snapshot("namespace references a missing directory"))?;
            for (name, entry) in directory.entries.iter().rev() {
                let child = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}/{name}")
                };
                pending.push((child, entry.node));
            }
        }
        Ok(paths)
    }

    /// Validate the backend-neutral structure shared by all volume formats.
    pub(crate) fn validate_structure(&self) -> Result<(), VolumeError> {
        let root = self
            .nodes
            .get(&self.root)
            .ok_or_else(|| invalid_snapshot("root node is missing"))?;
        if root.kind != NodeKind::Directory || !self.directories.contains_key(&self.root) {
            return Err(invalid_snapshot("root node is not a directory"));
        }

        for (id, node) in &self.nodes {
            if *id != node.id {
                return Err(invalid_snapshot(
                    "node map key does not match its record identity",
                ));
            }
            match node.kind {
                NodeKind::Directory => {
                    if node.file_version.is_some() || !self.directories.contains_key(id) {
                        return Err(invalid_snapshot(
                            "directory node has invalid backing records",
                        ));
                    }
                }
                NodeKind::RegularFile => {
                    let version = node
                        .file_version
                        .ok_or_else(|| invalid_snapshot("file node has no file version"))?;
                    if !self.file_versions.contains_key(&version)
                        || self.directories.contains_key(id)
                    {
                        return Err(invalid_snapshot("file node has invalid backing records"));
                    }
                }
            }
        }

        for (id, directory) in &self.directories {
            if *id != directory.node {
                return Err(invalid_snapshot(
                    "directory map key does not match its record identity",
                ));
            }
            if !self
                .nodes
                .get(id)
                .is_some_and(|node| node.kind == NodeKind::Directory)
            {
                return Err(invalid_snapshot("directory has no directory node"));
            }
            for (name, entry) in &directory.entries {
                if name.is_empty() || name == "." || name == ".." || name.contains('/') {
                    return Err(invalid_snapshot("directory entry name is invalid"));
                }
                let child = self
                    .nodes
                    .get(&entry.node)
                    .ok_or_else(|| invalid_snapshot("directory entry references a missing node"))?;
                if child.kind != entry.kind {
                    return Err(invalid_snapshot(
                        "directory entry kind disagrees with its node",
                    ));
                }
            }
        }

        let paths = self.paths()?;
        validate_portable_paths(paths.keys().map(String::as_str))?;
        let reachable = paths
            .values()
            .copied()
            .chain(std::iter::once(self.root))
            .collect::<BTreeSet<_>>();
        if reachable.len() != self.nodes.len() {
            return Err(invalid_snapshot("namespace contains unreachable nodes"));
        }
        Ok(())
    }
}

const MAX_PORTABLE_COMPONENT_BYTES: usize = 255;
const MAX_PORTABLE_PATH_BYTES: usize = 4096;

pub(crate) fn validate_portable_paths<'a>(
    paths: impl IntoIterator<Item = &'a str>,
) -> Result<(), VolumeError> {
    let mut folded = BTreeSet::new();
    for path in paths {
        if path.is_empty()
            || path.len() > MAX_PORTABLE_PATH_BYTES
            || path.starts_with('/')
            || path.ends_with('/')
            || path.contains("//")
        {
            return Err(invalid_snapshot("path is not portable"));
        }
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        if name.len() > MAX_PORTABLE_COMPONENT_BYTES
            || name == "."
            || name == ".."
            || name.ends_with([' ', '.'])
            || name.chars().any(|character| {
                character.is_control()
                    || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
            })
            || !name.nfc().eq(name.chars())
        {
            return Err(invalid_snapshot("path component is not portable"));
        }
        let folded_name = name.case_fold().nfc().collect::<String>();
        let stem = folded_name.split('.').next().unwrap_or_default();
        if matches!(stem, "con" | "prn" | "aux" | "nul")
            || stem.len() == 4
                && (stem.starts_with("com") || stem.starts_with("lpt"))
                && matches!(stem.as_bytes()[3], b'1'..=b'9')
            || matches!(stem, "com¹" | "com²" | "com³" | "lpt¹" | "lpt²" | "lpt³")
        {
            return Err(invalid_snapshot("path component is reserved"));
        }
        if !folded.insert((parent, folded_name)) {
            return Err(invalid_snapshot(
                "directory contains a case-folding collision",
            ));
        }
    }
    Ok(())
}

fn invalid_snapshot(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Invalid, message)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodePrecondition {
    pub node: NodeId,
    pub expected_generation: Option<Generation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryPrecondition {
    pub directory: NodeId,
    pub expected_generation: Option<Generation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectoryMutation {
    pub(crate) node: NodeId,
    pub(crate) generation: Generation,
    pub(crate) put_entries: BTreeMap<String, DirectoryEntry>,
    pub(crate) remove_entries: Vec<String>,
}

impl DirectoryMutation {
    fn between(target: &DirectoryRecord, base: Option<&DirectoryRecord>) -> Self {
        Self {
            node: target.node,
            generation: target.generation.clone(),
            put_entries: target
                .entries
                .iter()
                .filter(|(name, entry)| {
                    base.and_then(|base| base.entries.get(*name)) != Some(*entry)
                })
                .map(|(name, entry)| (name.clone(), *entry))
                .collect(),
            remove_entries: base
                .into_iter()
                .flat_map(|base| {
                    base.entries
                        .keys()
                        .filter(|name| !target.entries.contains_key(*name))
                        .cloned()
                })
                .collect(),
        }
    }

    pub(crate) fn apply(
        &self,
        base: Option<&DirectoryRecord>,
    ) -> Result<DirectoryRecord, VolumeError> {
        let mut directory = base.cloned().unwrap_or(DirectoryRecord {
            node: self.node,
            generation: self.generation.clone(),
            entries: BTreeMap::new(),
        });
        if directory.node != self.node {
            return Err(invalid_mutation("directory delta identity is invalid"));
        }
        directory.generation = self.generation.clone();
        let mut changed = BTreeSet::new();
        for name in &self.remove_entries {
            if !changed.insert(name.clone()) || directory.entries.remove(name).is_none() {
                return Err(invalid_mutation("directory entry removal is invalid"));
            }
        }
        for (name, entry) in &self.put_entries {
            if !changed.insert(name.clone()) {
                return Err(invalid_mutation("directory entry update is invalid"));
            }
            directory.entries.insert(name.clone(), *entry);
        }
        Ok(directory)
    }
}

/// The changed records in one generation-checked publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VolumeMutation {
    pub(crate) volume_id: VolumeId,
    pub(crate) operation: OperationId,
    pub(crate) parent: ChangeCursor,
    pub(crate) cursor: ChangeCursor,
    pub(crate) root: NodeId,
    pub(crate) expected_nodes: Vec<NodePrecondition>,
    pub(crate) expected_directories: Vec<DirectoryPrecondition>,
    pub(crate) put_nodes: Vec<NodeRecord>,
    pub(crate) remove_nodes: Vec<NodeId>,
    pub(crate) put_directories: Vec<DirectoryMutation>,
    pub(crate) remove_directories: Vec<NodeId>,
    pub(crate) put_file_versions: Vec<FileVersion>,
    pub(crate) remove_file_versions: Vec<FileVersionId>,
}

impl VolumeMutation {
    fn between(
        operation: OperationId,
        base: Option<&VolumeSnapshot>,
        target: &VolumeSnapshot,
    ) -> Self {
        let empty_nodes = BTreeMap::new();
        let empty_directories = BTreeMap::new();
        let empty_versions = BTreeMap::new();
        let base_nodes = base.map_or(&empty_nodes, |snapshot| &snapshot.nodes);
        let base_directories = base.map_or(&empty_directories, |snapshot| &snapshot.directories);
        let base_versions = base.map_or(&empty_versions, |snapshot| &snapshot.file_versions);
        let expected_nodes = changed_keys(base_nodes, &target.nodes)
            .map(|node| NodePrecondition {
                node,
                expected_generation: base_nodes
                    .get(&node)
                    .map(|record| record.generation.clone()),
            })
            .collect();
        let expected_directories = changed_keys(base_directories, &target.directories)
            .map(|directory| DirectoryPrecondition {
                directory,
                expected_generation: base_directories
                    .get(&directory)
                    .map(|record| record.generation.clone()),
            })
            .collect();
        Self {
            volume_id: target.volume_id,
            operation,
            parent: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            cursor: target.cursor,
            root: target.root,
            expected_nodes,
            expected_directories,
            put_nodes: target
                .nodes
                .iter()
                .filter(|(id, record)| base_nodes.get(id) != Some(record))
                .map(|(_, record)| record.clone())
                .collect(),
            remove_nodes: base_nodes
                .keys()
                .filter(|id| !target.nodes.contains_key(id))
                .copied()
                .collect(),
            put_directories: target
                .directories
                .iter()
                .filter(|(id, record)| base_directories.get(id) != Some(record))
                .map(|(id, record)| DirectoryMutation::between(record, base_directories.get(id)))
                .collect(),
            remove_directories: base_directories
                .keys()
                .filter(|id| !target.directories.contains_key(id))
                .copied()
                .collect(),
            put_file_versions: target
                .file_versions
                .iter()
                .filter(|(id, record)| base_versions.get(id) != Some(record))
                .map(|(_, record)| record.clone())
                .collect(),
            remove_file_versions: base_versions
                .keys()
                .filter(|id| !target.file_versions.contains_key(id))
                .copied()
                .collect(),
        }
    }

    pub(crate) fn validate_ancestry(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        if self.volume_id != volume_id
            || self.cursor.operation() != Some(self.operation)
            || self.parent.sequence().checked_add(1) != Some(self.cursor.sequence())
        {
            return Err(invalid_mutation("mutation ancestry is invalid"));
        }
        Ok(())
    }
}

fn changed_keys<'a, K: Copy + Ord, V: PartialEq>(
    base: &'a BTreeMap<K, V>,
    target: &'a BTreeMap<K, V>,
) -> impl Iterator<Item = K> + 'a {
    base.keys()
        .chain(target.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| base.get(key) != target.get(key))
}

fn invalid_mutation(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Invalid, message)
}

/// One generation-checked filesystem publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumePublication {
    pub target: VolumeSnapshot,
    mutation: VolumeMutation,
}

impl VolumePublication {
    pub(crate) fn between(
        operation: OperationId,
        base: Option<&VolumeSnapshot>,
        target: VolumeSnapshot,
    ) -> Result<Self, VolumeError> {
        target.validate_structure()?;
        let parent = base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor);
        if target.cursor.operation() != Some(operation)
            || parent.sequence().checked_add(1) != Some(target.cursor.sequence())
            || base.is_some_and(|base| base.volume_id != target.volume_id)
        {
            return Err(invalid_mutation("publication ancestry is invalid"));
        }
        let mutation = VolumeMutation::between(operation, base, &target);
        Ok(Self { target, mutation })
    }

    pub(crate) fn mutation(&self) -> &VolumeMutation {
        &self.mutation
    }
}

#[derive(Clone, Debug)]
pub struct MaterializeRequest {
    pub path: String,
    pub version: FileVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VolumeErrorKind {
    UnsupportedFormat,
    Invalid,
    Conflict,
    Corrupt,
    Unavailable,
}

/// An actionable error at the Volume boundary.
///
/// The message describes the filesystem operation and omits backend response
/// bodies, credentials, and provider-specific error types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeError {
    kind: VolumeErrorKind,
    message: String,
}

impl VolumeError {
    pub fn new(kind: VolumeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> VolumeErrorKind {
        self.kind
    }
}

impl fmt::Display for VolumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for VolumeError {}

/// An observation retains any private compare-and-swap token needed by a
/// later publication while exposing only a filesystem view to its caller.
pub trait VolumeObservation: Clone + Send + Sync {
    fn snapshot(&self) -> &VolumeSnapshot;
}

/// Authoritative filesystem operations shared by Mount and Sync access.
///
/// Implementations may use an existing object namespace (Direct) or a durable
/// metadata/data representation (Managed). Access-local acknowledgement and
/// replica state do not belong to this interface.
#[allow(async_fn_in_trait)]
pub trait Volume: Clone + Send + Sync {
    type Observation: VolumeObservation;

    fn id(&self) -> VolumeId;

    /// Stable identity of the authority used by this bound volume.
    fn authority(&self) -> AuthorityIdentity {
        AuthorityIdentity::base(self.id())
    }

    fn initial_generation(&self) -> Generation;

    fn next_generation(&self, generation: &Generation) -> Result<Generation, VolumeError>;

    async fn observe_from(
        &self,
        base: Option<&VolumeSnapshot>,
    ) -> Result<Option<Self::Observation>, VolumeError>;

    /// Freeze changed files and prepare every new immutable data object locally.
    ///
    /// `segment_staging` is private to the volume implementation and survives
    /// with the pending intent so that a retry never has to read or reconstruct
    /// data from the live source tree.
    async fn stage_files(
        &self,
        source: &Operator,
        segment_staging: &Operator,
        paths: Vec<String>,
        authority: Option<&VolumeSnapshot>,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersion>, VolumeError>;

    /// Make every locally prepared immutable data object durable.
    ///
    /// Sync persists its pending intent before calling this method and does not
    /// publish namespace metadata until it succeeds. Implementations must make
    /// retries idempotent and must use only `segment_staging`, never the live or
    /// user-visible frozen source tree.
    async fn finalize_staged_files(
        &self,
        segment_staging: &Operator,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError>;

    async fn publish(
        &self,
        observed: Option<&Self::Observation>,
        publication: &VolumePublication,
    ) -> Result<CommitOutcome, VolumeError>;

    async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, VolumeError>;

    async fn materialize(
        &self,
        target: &Operator,
        segment_staging: Option<&Operator>,
        requests: Vec<MaterializeRequest>,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError>;
}
