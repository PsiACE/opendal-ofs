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

use std::ops::Range;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite};

use crate::Error;
use crate::filesystem::{FileFingerprint, FileVersionId};

use super::ManagedVolume;
use super::object::{GcEpoch, ObjectClass};
use super::stream::{self, StreamKind, StreamRef};

const SHARD_TARGET_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct FileLayout {
    pub(super) extents: Vec<FileExtent>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct FileExtentRecord {
    pub(super) file_version: FileVersionId,
    pub(super) logical_range: ByteRange,
    pub(super) shard: StreamRef,
    pub(super) object_range: ByteRange,
}
super::wire::tuple_wire!(FileExtentRecord {
    file_version: FileVersionId,
    logical_range: ByteRange,
    shard: StreamRef,
    object_range: ByteRange,
});

#[derive(Clone, Copy, Debug)]
pub(super) struct FileExtent {
    pub(super) logical_range: ByteRange,
    pub(super) shard: StreamRef,
    pub(super) object_range: ByteRange,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ByteRange {
    pub(super) offset: u64,
    pub(super) length: u64,
}
super::wire::tuple_wire!(ByteRange {
    offset: u64,
    length: u64,
});

impl ManagedVolume {
    /// Publish one file layout over independently durable shards.
    pub(crate) async fn publish_data(
        &self,
        source: &mut (impl AsyncRead + Unpin),
        fingerprint: FileFingerprint,
        base_version: Option<FileVersionId>,
        gc_epoch: GcEpoch,
    ) -> Result<FileLayout, Error> {
        let reusable = match base_version {
            Some(base) => self.file_extents(base)?,
            None => Vec::new(),
        };
        let mut extents = Vec::new();
        let mut file_hasher = blake3::Hasher::new();
        let mut logical_offset = 0_u64;
        loop {
            let mut bytes = Vec::new();
            (&mut *source)
                .take(SHARD_TARGET_BYTES)
                .read_to_end(&mut bytes)
                .await
                .map_err(|error| Error::io("read Managed file source", error))?;
            if bytes.is_empty() {
                break;
            }
            file_hasher.update(&bytes);
            let length = bytes.len() as u64;
            let payload_digest =
                super::object::PayloadDigest::from_bytes(blake3::hash(&bytes).into());
            let reusable_shard = reusable
                .iter()
                .find(|extent| {
                    extent.logical_range.offset == logical_offset
                        && extent.logical_range.length == length
                        && extent.object_range.offset == 0
                        && extent.object_range.length == length
                        && extent.shard.payload_length == length
                        && extent.shard.payload_digest.as_bytes() == payload_digest.as_bytes()
                })
                .map(|extent| extent.shard);
            let shard = match reusable_shard {
                Some(shard) => shard,
                None => {
                    stream::write_bytes(
                        self.operator(),
                        gc_epoch,
                        ObjectClass::FileShard,
                        StreamKind::FILE_BYTES,
                        bytes,
                    )
                    .await?
                }
            };
            extents.push(FileExtent {
                logical_range: ByteRange {
                    offset: logical_offset,
                    length,
                },
                shard,
                object_range: ByteRange { offset: 0, length },
            });
            logical_offset = logical_offset
                .checked_add(length)
                .ok_or_else(|| Error::invalid("publish Managed file", "file size overflows"))?;
        }
        let observed = FileFingerprint::new(
            crate::filesystem::Digest::from_bytes(file_hasher.finalize().into()),
            logical_offset,
        );
        if observed != fingerprint {
            return Err(Error::conflict(
                "publish Managed file",
                "local file changed while being published",
            ));
        }
        if logical_offset == 0 && fingerprint.digest().as_bytes() != blake3::hash(&[]).as_bytes() {
            return Err(Error::invalid(
                "publish Managed file",
                "empty file content identity is invalid",
            ));
        }
        Ok(FileLayout { extents })
    }

    /// Read and verify one immutable file version into a destination.
    pub async fn read_data(
        &self,
        version: FileVersionId,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        let record = self.file_version_record(version)?;
        self.read_data_range(version, 0..record.file_size, destination)
            .await
    }

    /// Read and independently verify a selected logical byte range.
    pub async fn read_data_range(
        &self,
        version: FileVersionId,
        range: Range<u64>,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        let record = self.file_version_record(version)?;
        if range.start > range.end || range.end > record.file_size {
            return Err(Error::invalid(
                "read Managed file range",
                "logical byte range is invalid",
            ));
        }
        let mut extents = self.file_extents(version)?;
        extents.sort_by_key(|extent| extent.logical_range.offset);
        let mut expected_offset = 0_u64;
        for extent in extents {
            if extent.logical_range.offset != expected_offset
                || extent
                    .object_range
                    .offset
                    .checked_add(extent.object_range.length)
                    .is_none_or(|end| end > extent.shard.payload_length)
                || extent.logical_range.length != extent.object_range.length
                || extent.shard.kind != StreamKind::FILE_BYTES
                || extent.shard.object.class != ObjectClass::FileShard
            {
                return Err(Error::corrupt(
                    "read Managed file",
                    "file extents do not form a contiguous byte stream",
                ));
            }
            let extent_end = extent.logical_range.offset + extent.logical_range.length;
            if extent.logical_range.offset < range.end && extent_end > range.start {
                let selected_start = range.start.max(extent.logical_range.offset);
                let selected_end = range.end.min(extent_end);
                let object_start =
                    extent.object_range.offset + selected_start - extent.logical_range.offset;
                let object_end =
                    extent.object_range.offset + selected_end - extent.logical_range.offset;
                if object_start == 0
                    && object_end == extent.shard.payload_length
                    && range.start <= extent.logical_range.offset
                    && range.end >= extent_end
                {
                    stream::copy_bytes(self.operator(), extent.shard, destination).await?;
                } else {
                    stream::copy_byte_range(
                        self.operator(),
                        extent.shard,
                        object_start..object_end,
                        destination,
                    )
                    .await?;
                }
            }
            expected_offset = expected_offset
                .checked_add(extent.logical_range.length)
                .ok_or_else(|| Error::corrupt("read Managed file", "file length overflows"))?;
        }
        if expected_offset != record.file_size {
            return Err(Error::corrupt(
                "read Managed file",
                "file extents do not cover the declared file size",
            ));
        }
        Ok(())
    }

    pub(super) fn extent_records(
        version: FileVersionId,
        layout: FileLayout,
    ) -> impl Iterator<Item = FileExtentRecord> {
        layout
            .extents
            .into_iter()
            .map(move |extent| FileExtentRecord {
                file_version: version,
                logical_range: extent.logical_range,
                shard: extent.shard,
                object_range: extent.object_range,
            })
    }
}
