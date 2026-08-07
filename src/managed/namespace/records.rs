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

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::filesystem::{
    ChangeCursor, DirectoryEntry, FileVersionId, Generation, NodeAttributes, NodeId, NodeKind,
    OperationId, VolumeId,
};

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContentRef {
    pub digest: [u8; 32],
    pub logical_length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum FileVersionLayout {
    Whole {
        content: ContentRef,
    },
    Chunked {
        chunking: ChunkingSpec,
        chunks: Vec<ChunkSpan>,
    },
    Extents {
        extents: Vec<FileExtent>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkingSpec {
    pub algorithm: ChunkingAlgorithm,
    pub minimum_size: u64,
    pub target_size: u64,
    pub maximum_size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "name")]
pub enum ChunkingAlgorithm {
    Fixed,
    FastCdcV2020 { revision: u32 },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkSpan {
    pub logical_offset: u64,
    pub logical_length: u64,
    pub content: ContentRef,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum FileExtent {
    Hole {
        logical_offset: u64,
        logical_length: u64,
    },
    Data {
        extent: DataExtent,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataExtent {
    pub logical_offset: u64,
    pub logical_length: u64,
    pub data_offset: u64,
    pub content: ContentRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileVersionRecord {
    pub id: FileVersionId,
    pub logical_size: u64,
    pub logical_digest: [u8; 32],
    pub layout: FileVersionLayout,
}

impl FileVersionRecord {
    pub(crate) fn whole(logical_size: u64, logical_digest: [u8; 32]) -> Self {
        let layout = FileVersionLayout::Whole {
            content: ContentRef {
                digest: logical_digest,
                logical_length: logical_size,
            },
        };
        Self {
            id: canonical_file_version_id(logical_size, &logical_digest, &layout)
                .expect("one whole-file entry fits format v1"),
            logical_size,
            logical_digest,
            layout,
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        layout_valid(self.logical_size, &self.logical_digest, &self.layout)
            && canonical_file_version_id(self.logical_size, &self.logical_digest, &self.layout)
                == Some(self.id)
    }
}

fn layout_valid(size: u64, digest: &[u8; 32], layout: &FileVersionLayout) -> bool {
    if size == 0 {
        let empty: [u8; 32] = Sha256::digest([]).into();
        return *digest == empty
            && matches!(
                layout,
                FileVersionLayout::Whole { content }
                    if content.logical_length == 0 && content.digest == empty
            );
    }
    match layout {
        FileVersionLayout::Whole { content } => {
            content.logical_length == size && content.digest == *digest
        }
        FileVersionLayout::Chunked { chunking, chunks } => {
            chunking_valid(chunking)
                && chunk_sizes_valid(chunking, chunks)
                && contiguous(
                    size,
                    chunks.iter().map(|chunk| {
                        (chunk.logical_length == chunk.content.logical_length)
                            .then_some((chunk.logical_offset, chunk.logical_length))
                    }),
                )
        }
        FileVersionLayout::Extents { extents } => contiguous(
            size,
            extents.iter().map(|extent| match extent {
                FileExtent::Hole {
                    logical_offset,
                    logical_length,
                } => Some((*logical_offset, *logical_length)),
                FileExtent::Data { extent: data } => data
                    .data_offset
                    .checked_add(data.logical_length)
                    .filter(|end| *end <= data.content.logical_length)
                    .map(|_| (data.logical_offset, data.logical_length)),
            }),
        ),
    }
}

fn chunking_valid(spec: &ChunkingSpec) -> bool {
    spec.minimum_size > 0
        && spec.minimum_size <= spec.target_size
        && spec.target_size <= spec.maximum_size
        && match spec.algorithm {
            ChunkingAlgorithm::Fixed => {
                spec.minimum_size == spec.target_size && spec.target_size == spec.maximum_size
            }
            ChunkingAlgorithm::FastCdcV2020 { revision } => revision == 1,
        }
}

fn chunk_sizes_valid(spec: &ChunkingSpec, chunks: &[ChunkSpan]) -> bool {
    !chunks.is_empty()
        && chunks.iter().enumerate().all(|(index, chunk)| {
            let last = index + 1 == chunks.len();
            chunk.logical_length <= spec.maximum_size
                && (last || chunk.logical_length >= spec.minimum_size)
                && (!matches!(spec.algorithm, ChunkingAlgorithm::Fixed)
                    || chunk.logical_length == spec.target_size
                    || last && chunk.logical_length < spec.target_size)
        })
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
    layout: &FileVersionLayout,
) -> Option<FileVersionId> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(b"OFS-FILE-V1\0");
    encoded.extend_from_slice(&size.to_be_bytes());
    encoded.extend_from_slice(digest);
    match layout {
        FileVersionLayout::Whole { content } => {
            encoded.push(0);
            encode_content(&mut encoded, content);
        }
        FileVersionLayout::Chunked { chunking, chunks } => {
            encoded.push(1);
            match chunking.algorithm {
                ChunkingAlgorithm::Fixed => encoded.push(0),
                ChunkingAlgorithm::FastCdcV2020 { revision } => {
                    encoded.push(1);
                    encoded.extend_from_slice(&revision.to_be_bytes());
                }
            }
            encoded.extend_from_slice(&chunking.minimum_size.to_be_bytes());
            encoded.extend_from_slice(&chunking.target_size.to_be_bytes());
            encoded.extend_from_slice(&chunking.maximum_size.to_be_bytes());
            encoded.extend_from_slice(&u64::try_from(chunks.len()).ok()?.to_be_bytes());
            for chunk in chunks {
                encoded.extend_from_slice(&chunk.logical_offset.to_be_bytes());
                encoded.extend_from_slice(&chunk.logical_length.to_be_bytes());
                encode_content(&mut encoded, &chunk.content);
            }
        }
        FileVersionLayout::Extents { extents } => {
            encoded.push(2);
            encoded.extend_from_slice(&u64::try_from(extents.len()).ok()?.to_be_bytes());
            for extent in extents {
                match extent {
                    FileExtent::Hole {
                        logical_offset,
                        logical_length,
                    } => {
                        encoded.push(0);
                        encoded.extend_from_slice(&logical_offset.to_be_bytes());
                        encoded.extend_from_slice(&logical_length.to_be_bytes());
                    }
                    FileExtent::Data { extent: data } => {
                        encoded.push(1);
                        encoded.extend_from_slice(&data.logical_offset.to_be_bytes());
                        encoded.extend_from_slice(&data.logical_length.to_be_bytes());
                        encoded.extend_from_slice(&data.data_offset.to_be_bytes());
                        encode_content(&mut encoded, &data.content);
                    }
                }
            }
        }
    }
    Some(FileVersionId::from_bytes(Sha256::digest(encoded).into()))
}

fn encode_content(encoded: &mut Vec<u8>, content: &ContentRef) {
    encoded.extend_from_slice(&content.digest);
    encoded.extend_from_slice(&content.logical_length.to_be_bytes());
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespacePublication {
    pub operation: OperationId,
    pub parent: ChangeCursor,
    pub expected_nodes: Vec<NodePrecondition>,
    pub expected_directories: Vec<DirectoryPrecondition>,
    pub target: NamespaceSnapshot,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(value: &[u8]) -> [u8; 32] {
        Sha256::digest(value).into()
    }

    fn version(
        size: u64,
        logical_digest: [u8; 32],
        layout: FileVersionLayout,
    ) -> FileVersionRecord {
        FileVersionRecord {
            id: canonical_file_version_id(size, &logical_digest, &layout).unwrap(),
            logical_size: size,
            logical_digest,
            layout,
        }
    }

    #[test]
    fn file_version_identity_includes_the_validated_layout() {
        let logical_digest = digest(b"abcd");
        let whole = FileVersionRecord::whole(4, logical_digest);
        let chunked = version(
            4,
            logical_digest,
            FileVersionLayout::Chunked {
                chunking: ChunkingSpec {
                    algorithm: ChunkingAlgorithm::Fixed,
                    minimum_size: 2,
                    target_size: 2,
                    maximum_size: 2,
                },
                chunks: vec![
                    ChunkSpan {
                        logical_offset: 0,
                        logical_length: 2,
                        content: ContentRef {
                            digest: digest(b"ab"),
                            logical_length: 2,
                        },
                    },
                    ChunkSpan {
                        logical_offset: 2,
                        logical_length: 2,
                        content: ContentRef {
                            digest: digest(b"cd"),
                            logical_length: 2,
                        },
                    },
                ],
            },
        );

        assert!(whole.is_valid());
        assert!(chunked.is_valid());
        assert_ne!(whole.id, chunked.id);
    }

    #[test]
    fn empty_and_span_rules_fail_closed() {
        let empty_digest = digest(b"");
        assert!(FileVersionRecord::whole(0, empty_digest).is_valid());

        let empty_chunked = version(
            0,
            empty_digest,
            FileVersionLayout::Chunked {
                chunking: ChunkingSpec {
                    algorithm: ChunkingAlgorithm::Fixed,
                    minimum_size: 1,
                    target_size: 1,
                    maximum_size: 1,
                },
                chunks: Vec::new(),
            },
        );
        assert!(!empty_chunked.is_valid());

        let invalid_extent = version(
            2,
            digest(b"ab"),
            FileVersionLayout::Extents {
                extents: vec![FileExtent::Data {
                    extent: DataExtent {
                        logical_offset: 0,
                        logical_length: 2,
                        data_offset: 1,
                        content: ContentRef {
                            digest: digest(b"ab"),
                            logical_length: 2,
                        },
                    },
                }],
            },
        );
        assert!(!invalid_extent.is_valid());
    }
}
