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
use std::io::Cursor;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::filesystem::{FileVersion, FileVersionId, Generation, VolumeError};
use crate::managed::error::{corrupt, invalid};
use crate::managed::format::{ContentRef, ExtentMap};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DecodedFileVersion {
    pub(crate) id: FileVersionId,
    pub(crate) logical_size: u64,
    pub(crate) logical_digest: [u8; 32],
    pub(crate) extent_map: ExtentMap,
}

impl DecodedFileVersion {
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
    let mut next = 0;
    let mut segment_lengths = BTreeMap::new();
    for extent in &extent_map.extents {
        if extent.logical_offset != next
            || extent.content.length == 0
            || extent
                .segment_offset
                .checked_add(extent.content.length)
                .is_none_or(|end| end > extent.segment.length)
            || segment_lengths
                .insert(extent.segment.digest, extent.segment.length)
                .is_some_and(|length| length != extent.segment.length)
        {
            return false;
        }
        let Some(end) = next.checked_add(extent.content.length) else {
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

pub(crate) fn encode_file_version(
    version: &DecodedFileVersion,
) -> Result<FileVersion, VolumeError> {
    let mut descriptor = Vec::new();
    ciborium::into_writer(&version.extent_map, &mut descriptor)
        .map_err(|error| invalid("encode Managed file version", error.to_string()))?;
    Ok(FileVersion::from_parts(
        version.id,
        version.logical_size,
        version.logical_digest,
        descriptor,
    ))
}

pub(crate) fn decode_file_version(
    version: &FileVersion,
) -> Result<DecodedFileVersion, VolumeError> {
    let descriptor = version.descriptor();
    let mut input = Cursor::new(descriptor);
    let extent_map: ExtentMap = ciborium::from_reader(&mut input)
        .map_err(|error| corrupt("decode Managed file version", error.to_string()))?;
    if input.position() != descriptor.len() as u64 {
        return Err(corrupt(
            "decode Managed file version",
            "descriptor has trailing bytes",
        ));
    }
    DecodedFileVersion::from_extents(version.logical_size, version.logical_digest, extent_map)
        .filter(|decoded| decoded.id == version.id)
        .ok_or_else(|| {
            corrupt(
                "decode Managed file version",
                "descriptor does not match its filesystem identity",
            )
        })
}

pub(crate) fn file_versions_have_consistent_segments<'a>(
    versions: impl IntoIterator<Item = &'a FileVersion>,
) -> bool {
    let mut segment_lengths = BTreeMap::new();
    versions.into_iter().all(|version| {
        decode_file_version(version).is_ok_and(|decoded| {
            decoded.extent_map.extents.iter().all(|extent| {
                segment_lengths
                    .insert(extent.segment.digest, extent.segment.length)
                    .is_none_or(|length| length == extent.segment.length)
            })
        })
    })
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
    use crate::filesystem::VolumeErrorKind;
    use crate::managed::format::{Extent, SegmentRef};

    #[test]
    fn file_version_descriptors_have_one_identity() {
        let empty_digest = Sha256::digest([]).into();
        let empty = DecodedFileVersion::from_extents(
            0,
            empty_digest,
            ExtentMap {
                extents: Vec::new(),
            },
        )
        .unwrap();
        let encoded = encode_file_version(&empty).unwrap();
        let mut descriptor = encoded.descriptor().to_vec();
        descriptor.push(0);
        let trailing = FileVersion::from_parts(
            encoded.id,
            encoded.logical_size,
            encoded.logical_digest,
            descriptor,
        );
        assert_eq!(
            decode_file_version(&trailing).unwrap_err().kind(),
            VolumeErrorKind::Corrupt
        );

        let version = |content, segment_length| {
            let decoded = DecodedFileVersion::from_extents(
                1,
                content,
                ExtentMap {
                    extents: vec![Extent {
                        logical_offset: 0,
                        content: ContentRef {
                            digest: content,
                            length: 1,
                        },
                        segment: SegmentRef {
                            digest: [1; 32],
                            length: segment_length,
                        },
                        segment_offset: 0,
                    }],
                },
            )
            .unwrap();
            encode_file_version(&decoded).unwrap()
        };
        let first = version([2; 32], 1);
        let second = version([3; 32], 2);
        assert!(!file_versions_have_consistent_segments([&first, &second]));
    }
}
