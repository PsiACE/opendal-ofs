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

use super::AuthorityIdentity;
use super::{
    ChangeCursor, CommitOutcome, DirectoryEntry, FileVersionId, Generation, NodeAttributes, NodeId,
    NodeKind, OperationId, VolumeId,
};

/// An immutable file version whose durable descriptor is owned by its volume.
///
/// Access models may persist and return `descriptor`, but must not interpret it.
#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRecord {
    pub id: NodeId,
    pub generation: Generation,
    pub kind: NodeKind,
    pub attributes: NodeAttributes,
    pub file_version: Option<FileVersionId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeSnapshot<F = FileVersion> {
    pub volume_id: VolumeId,
    pub cursor: ChangeCursor,
    pub root: NodeId,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    pub directories: BTreeMap<NodeId, DirectoryRecord>,
    pub file_versions: BTreeMap<FileVersionId, F>,
}

impl<F> VolumeSnapshot<F> {
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

fn invalid_snapshot(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Invalid, message)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePrecondition {
    pub node: NodeId,
    pub expected_generation: Option<Generation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryPrecondition {
    pub directory: NodeId,
    pub expected_generation: Option<Generation>,
}

/// One generation-checked filesystem publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumePublication<F = FileVersion> {
    pub operation: OperationId,
    pub parent: ChangeCursor,
    pub expected_nodes: Vec<NodePrecondition>,
    pub expected_directories: Vec<DirectoryPrecondition>,
    pub target: VolumeSnapshot<F>,
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

    async fn stage_files(
        &self,
        source: &Operator,
        staging: &Operator,
        paths: Vec<String>,
        authority: Option<&VolumeSnapshot>,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersion>, VolumeError>;

    async fn publish(
        &self,
        observed: Option<&Self::Observation>,
        publication: &VolumePublication,
    ) -> Result<CommitOutcome, VolumeError>;

    async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, VolumeError>;

    async fn materialize(
        &self,
        target: &Operator,
        requests: Vec<MaterializeRequest>,
        full_tree: bool,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError>;
}
