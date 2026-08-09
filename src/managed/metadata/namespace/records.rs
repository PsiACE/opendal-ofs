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

use sha2::{Digest as _, Sha256};

use crate::filesystem::{ChangeCursor, FileVersionId, Generation};
use crate::managed::format::{ContentRef, ExtentMap};

pub(crate) type NodeRecord = crate::filesystem::NodeRecord;
pub(crate) type DirectoryRecord = crate::filesystem::DirectoryRecord;

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

pub(crate) type NamespaceSnapshot = crate::filesystem::VolumeSnapshot<FileVersionRecord>;
pub(crate) type NodePrecondition = crate::filesystem::NodePrecondition;
pub(crate) type DirectoryPrecondition = crate::filesystem::DirectoryPrecondition;
pub(crate) type NamespacePublication = crate::filesystem::VolumePublication<FileVersionRecord>;

/// Durable identity of one namespace garbage-collection sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceGcSweep {
    epoch: u64,
    owner: [u8; 16],
    fixed: ChangeCursor,
}

impl NamespaceGcSweep {
    pub fn epoch(self) -> u64 {
        self.epoch
    }

    pub fn fixed_cursor(self) -> ChangeCursor {
        self.fixed
    }

    pub(crate) const fn new(epoch: u64, owner: [u8; 16], fixed: ChangeCursor) -> Self {
        Self {
            epoch,
            owner,
            fixed,
        }
    }

    pub(crate) const fn owner(self) -> [u8; 16] {
        self.owner
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
