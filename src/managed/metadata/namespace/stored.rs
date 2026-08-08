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

//! Provider-neutral durable records shared by Managed metadata implementations.

use serde::{Deserialize, Serialize};

use super::{
    DirectoryPrecondition, FileVersionRecord, NodePrecondition, NodeRecord, managed_generation,
    managed_generation_number,
};
use crate::filesystem::{DirectoryEntry, FileVersionId, NodeAttributes, NodeId, NodeKind};
use crate::managed::format::ExtentMap;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredNode {
    id: [u8; 16],
    generation: u64,
    kind: StoredNodeKind,
    attributes: StoredNodeAttributes,
    file_version: Option<[u8; 32]>,
}

impl From<&NodeRecord> for StoredNode {
    fn from(node: &NodeRecord) -> Self {
        Self {
            id: *node.id.as_bytes(),
            generation: managed_generation_number(&node.generation)
                .expect("validated Managed node generation"),
            kind: node.kind.into(),
            attributes: node.attributes.into(),
            file_version: node.file_version.map(|version| *version.as_bytes()),
        }
    }
}

impl StoredNode {
    pub(super) fn into_record(self) -> NodeRecord {
        NodeRecord {
            id: NodeId::from_bytes(self.id),
            generation: managed_generation(self.generation),
            kind: self.kind.into(),
            attributes: self.attributes.into(),
            file_version: self.file_version.map(FileVersionId::from_bytes),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredDirectoryEntry {
    node: [u8; 16],
    kind: StoredNodeKind,
}

impl From<DirectoryEntry> for StoredDirectoryEntry {
    fn from(entry: DirectoryEntry) -> Self {
        Self {
            node: *entry.node.as_bytes(),
            kind: entry.kind.into(),
        }
    }
}

impl From<StoredDirectoryEntry> for DirectoryEntry {
    fn from(entry: StoredDirectoryEntry) -> Self {
        Self {
            node: NodeId::from_bytes(entry.node),
            kind: entry.kind.into(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StoredNodeKind {
    Directory,
    RegularFile,
}

impl From<NodeKind> for StoredNodeKind {
    fn from(kind: NodeKind) -> Self {
        match kind {
            NodeKind::Directory => Self::Directory,
            NodeKind::RegularFile => Self::RegularFile,
        }
    }
}

impl From<StoredNodeKind> for NodeKind {
    fn from(kind: StoredNodeKind) -> Self {
        match kind {
            StoredNodeKind::Directory => Self::Directory,
            StoredNodeKind::RegularFile => Self::RegularFile,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredNodeAttributes {
    executable: bool,
}

impl From<NodeAttributes> for StoredNodeAttributes {
    fn from(attributes: NodeAttributes) -> Self {
        Self {
            executable: attributes.executable,
        }
    }
}

impl From<StoredNodeAttributes> for NodeAttributes {
    fn from(attributes: StoredNodeAttributes) -> Self {
        Self {
            executable: attributes.executable,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredFileVersion {
    id: [u8; 32],
    logical_size: u64,
    logical_digest: [u8; 32],
    extent_map: ExtentMap,
}

impl From<&FileVersionRecord> for StoredFileVersion {
    fn from(version: &FileVersionRecord) -> Self {
        Self {
            id: *version.id.as_bytes(),
            logical_size: version.logical_size,
            logical_digest: version.logical_digest,
            extent_map: version.extent_map.clone(),
        }
    }
}

impl StoredFileVersion {
    pub(super) fn into_record(self) -> FileVersionRecord {
        FileVersionRecord {
            id: FileVersionId::from_bytes(self.id),
            logical_size: self.logical_size,
            logical_digest: self.logical_digest,
            extent_map: self.extent_map,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredNodePrecondition {
    node: [u8; 16],
    expected_generation: Option<u64>,
}

impl From<&NodePrecondition> for StoredNodePrecondition {
    fn from(condition: &NodePrecondition) -> Self {
        Self {
            node: *condition.node.as_bytes(),
            expected_generation: condition.expected_generation.as_ref().map(|generation| {
                managed_generation_number(generation)
                    .expect("validated Managed node precondition generation")
            }),
        }
    }
}

impl StoredNodePrecondition {
    pub(super) fn into_record(self) -> NodePrecondition {
        NodePrecondition {
            node: NodeId::from_bytes(self.node),
            expected_generation: self.expected_generation.map(managed_generation),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredDirectoryPrecondition {
    directory: [u8; 16],
    expected_generation: Option<u64>,
}

impl From<&DirectoryPrecondition> for StoredDirectoryPrecondition {
    fn from(condition: &DirectoryPrecondition) -> Self {
        Self {
            directory: *condition.directory.as_bytes(),
            expected_generation: condition.expected_generation.as_ref().map(|generation| {
                managed_generation_number(generation)
                    .expect("validated Managed directory precondition generation")
            }),
        }
    }
}

impl StoredDirectoryPrecondition {
    pub(super) fn into_record(self) -> DirectoryPrecondition {
        DirectoryPrecondition {
            directory: NodeId::from_bytes(self.directory),
            expected_generation: self.expected_generation.map(managed_generation),
        }
    }
}
