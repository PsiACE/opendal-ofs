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

use std::collections::BTreeMap;

use crate::filesystem::{ChangeCursor, FileVersionId, NodeId, OperationId, VolumeId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Directory,
    RegularFile,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct NodeAttributes {
    pub executable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRecord {
    pub id: NodeId,
    pub generation: u64,
    pub kind: NodeKind,
    pub attributes: NodeAttributes,
    pub file_version: Option<FileVersionId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub node: NodeId,
    pub kind: NodeKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryRecord {
    pub node: NodeId,
    pub generation: u64,
    pub entries: BTreeMap<String, DirectoryEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentRef {
    pub digest: [u8; 32],
    pub logical_length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileVersionRecord {
    pub id: FileVersionId,
    pub logical_size: u64,
    pub logical_digest: [u8; 32],
    pub content: ContentRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceSnapshot {
    pub volume_id: VolumeId,
    pub cursor: ChangeCursor,
    pub root: NodeId,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    pub directories: BTreeMap<NodeId, DirectoryRecord>,
    pub file_versions: BTreeMap<FileVersionId, FileVersionRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePrecondition {
    pub node: NodeId,
    pub expected_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryPrecondition {
    pub directory: NodeId,
    pub expected_generation: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespacePublication {
    pub operation: OperationId,
    pub parent: ChangeCursor,
    pub expected_nodes: Vec<NodePrecondition>,
    pub expected_directories: Vec<DirectoryPrecondition>,
    pub target: NamespaceSnapshot,
}
