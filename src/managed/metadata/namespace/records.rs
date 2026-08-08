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

use sha2::{Digest as _, Sha256};

use crate::filesystem::{
    ChangeCursor, DirectoryEntry, FileVersionId, Generation, NodeAttributes, NodeId, NodeKind,
    OperationId, VolumeId,
};
use crate::managed::format::{ContentRef, ExtentMap};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeRecord {
    pub(crate) id: NodeId,
    pub(crate) generation: Generation,
    pub(crate) kind: NodeKind,
    pub(crate) attributes: NodeAttributes,
    pub(crate) file_version: Option<FileVersionId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryRecord {
    pub(crate) node: NodeId,
    pub(crate) generation: Generation,
    pub(crate) entries: BTreeMap<String, DirectoryEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileVersionRecord {
    pub(crate) id: FileVersionId,
    pub(crate) logical_size: u64,
    pub(crate) logical_digest: [u8; 32],
    pub(crate) extent_map: ExtentMap,
}

impl FileVersionRecord {
    pub(crate) fn from_extents(
        logical_size: u64,
        logical_digest: [u8; 32],
        extent_map: ExtentMap,
    ) -> Option<Self> {
        if !extent_map_valid(logical_size, &logical_digest, &extent_map) {
            return None;
        }
        Some(Self {
            id: canonical_file_version_id(logical_size, &logical_digest, &extent_map)?,
            logical_size,
            logical_digest,
            extent_map,
        })
    }

    pub(crate) fn is_valid(&self) -> bool {
        extent_map_valid(self.logical_size, &self.logical_digest, &self.extent_map)
            && canonical_file_version_id(self.logical_size, &self.logical_digest, &self.extent_map)
                == Some(self.id)
    }
}

fn extent_map_valid(size: u64, digest: &[u8; 32], extent_map: &ExtentMap) -> bool {
    if size == 0 {
        let empty: [u8; 32] = Sha256::digest([]).into();
        return *digest == empty && extent_map.extents.is_empty();
    }
    if let [extent] = extent_map.extents.as_slice()
        && extent.logical_offset == 0
        && extent.content.length == size
        && extent.content.digest != *digest
    {
        return false;
    }
    contiguous(
        size,
        extent_map.extents.iter().map(|extent| {
            (extent.content.length != 0
                && extent
                    .segment_offset
                    .checked_add(extent.content.length)
                    .is_some_and(|end| end <= extent.segment.length))
            .then_some((extent.logical_offset, extent.content.length))
        }),
    )
}

fn contiguous(size: u64, spans: impl Iterator<Item = Option<(u64, u64)>>) -> bool {
    let mut next = 0;
    for span in spans {
        let Some((offset, length)) = span else {
            return false;
        };
        if offset != next || length == 0 {
            return false;
        }
        let Some(end) = next.checked_add(length) else {
            return false;
        };
        next = end;
    }
    next == size
}

fn canonical_file_version_id(
    size: u64,
    digest: &[u8; 32],
    extent_map: &ExtentMap,
) -> Option<FileVersionId> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(b"OFS-FILE-V1\0");
    encoded.extend_from_slice(&size.to_be_bytes());
    encoded.extend_from_slice(digest);
    encoded.extend_from_slice(&u64::try_from(extent_map.extents.len()).ok()?.to_be_bytes());
    for extent in &extent_map.extents {
        encoded.extend_from_slice(&extent.logical_offset.to_be_bytes());
        encode_content(&mut encoded, &extent.content);
        encoded.extend_from_slice(&extent.segment.digest);
        encoded.extend_from_slice(&extent.segment.length.to_be_bytes());
        encoded.extend_from_slice(&extent.segment_offset.to_be_bytes());
    }
    Some(FileVersionId::from_bytes(Sha256::digest(encoded).into()))
}

fn encode_content(encoded: &mut Vec<u8>, content: &ContentRef) {
    encoded.extend_from_slice(&content.digest);
    encoded.extend_from_slice(&content.length.to_be_bytes());
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NamespaceSnapshot {
    pub(crate) volume_id: VolumeId,
    pub(crate) cursor: ChangeCursor,
    pub(crate) root: NodeId,
    pub(crate) nodes: BTreeMap<NodeId, NodeRecord>,
    pub(crate) directories: BTreeMap<NodeId, DirectoryRecord>,
    pub(crate) file_versions: BTreeMap<FileVersionId, FileVersionRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodePrecondition {
    pub(crate) node: NodeId,
    pub(crate) expected_generation: Option<Generation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryPrecondition {
    pub(crate) directory: NodeId,
    pub(crate) expected_generation: Option<Generation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NamespacePublication {
    pub(crate) operation: OperationId,
    pub(crate) parent: ChangeCursor,
    pub(crate) expected_nodes: Vec<NodePrecondition>,
    pub(crate) expected_directories: Vec<DirectoryPrecondition>,
    pub(crate) target: NamespaceSnapshot,
}

/// Durable identity of one namespace garbage-collection sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceGcSweep {
    epoch: u64,
    fixed: ChangeCursor,
}

impl NamespaceGcSweep {
    pub fn epoch(self) -> u64 {
        self.epoch
    }

    pub fn fixed_cursor(self) -> ChangeCursor {
        self.fixed
    }

    pub(crate) const fn new(epoch: u64, fixed: ChangeCursor) -> Self {
        Self { epoch, fixed }
    }
}

pub(crate) fn managed_generation(value: u64) -> Generation {
    Generation::from_bytes(value.to_be_bytes().to_vec())
}

pub(crate) fn managed_generation_number(generation: &Generation) -> Option<u64> {
    generation
        .as_bytes()
        .try_into()
        .ok()
        .map(u64::from_be_bytes)
        .filter(|value| *value != 0)
}

pub(crate) fn next_managed_generation(generation: &Generation) -> Option<Generation> {
    managed_generation_number(generation)
        .and_then(|value| value.checked_add(1))
        .map(managed_generation)
}
