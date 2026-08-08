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

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use opendal::Operator;

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeSnapshot {
    pub volume_id: VolumeId,
    pub cursor: ChangeCursor,
    pub root: NodeId,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    pub directories: BTreeMap<NodeId, DirectoryRecord>,
    pub file_versions: BTreeMap<FileVersionId, FileVersion>,
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
pub struct VolumePublication {
    pub operation: OperationId,
    pub parent: ChangeCursor,
    pub expected_nodes: Vec<NodePrecondition>,
    pub expected_directories: Vec<DirectoryPrecondition>,
    pub target: VolumeSnapshot,
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

/// A read session shared by one materialization operation.
#[allow(async_fn_in_trait)]
pub trait VolumeReader: Clone + Send + Sync {
    async fn materialize(
        &self,
        target: &Operator,
        requests: Vec<MaterializeRequest>,
        full_tree: bool,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError>;
}

/// Authoritative filesystem operations shared by Mount and Sync access.
///
/// Implementations may use an existing object namespace (Direct) or a durable
/// metadata/data representation (Managed). Access-local acknowledgement and
/// replica state do not belong to this interface.
#[allow(async_fn_in_trait)]
pub trait Volume: Clone + Send + Sync {
    type Observation: VolumeObservation;
    type Reader: VolumeReader;

    fn id(&self) -> VolumeId;

    fn initial_generation(&self) -> Generation;

    fn next_generation(&self, generation: &Generation) -> Result<Generation, VolumeError>;

    async fn observe(&self) -> Result<Option<Self::Observation>, VolumeError>;

    async fn observe_from(
        &self,
        base: Option<&VolumeSnapshot>,
    ) -> Result<Option<Self::Observation>, VolumeError>;

    async fn stage_files(
        &self,
        source: &Operator,
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

    fn reader(&self) -> Result<Self::Reader, VolumeError>;
}
