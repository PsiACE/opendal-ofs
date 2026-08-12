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
use crate::filesystem::{ChangeCursor, FileFingerprint, FileVersionId};

use super::ManagedVolume;
use super::object::{self, GcEpoch, ObjectClass, ObjectRef};
use super::record::Record;
use super::stream::{self, StreamKind, StreamRef};

const MANIFEST_RECORD: Record = Record::new(*b"OFSMAN01", 1, 1024 * 1024);
const SHARD_TARGET_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
struct FileManifest {
    file_version: FileVersionId,
    file_size: u64,
    content_fingerprint: FileFingerprint,
    extent_segments: Vec<StreamRef>,
}
super::wire::tuple_wire!(FileManifest {
    file_version: FileVersionId,
    file_size: u64,
    content_fingerprint: FileFingerprint,
    extent_segments: Vec<StreamRef>,
});

#[derive(Clone, Copy, Debug)]
struct FileExtentMutation {
    logical_range: ByteRange,
    change_cursor: ChangeCursor,
    extent: DataExtent,
}
super::wire::tuple_wire!(FileExtentMutation {
    logical_range: ByteRange,
    change_cursor: ChangeCursor,
    extent: DataExtent,
});

#[derive(Clone, Copy, Debug)]
struct DataExtent {
    shard: StreamRef,
    object_range: ByteRange,
}
super::wire::tuple_wire!(DataExtent {
    shard: StreamRef,
    object_range: ByteRange,
});

#[derive(Clone, Copy, Debug)]
struct ByteRange {
    offset: u64,
    length: u64,
}
super::wire::tuple_wire!(ByteRange {
    offset: u64,
    length: u64,
});

impl ManagedVolume {
    /// Publish one immutable file manifest over independently durable shards.
    pub(crate) async fn publish_data(
        &self,
        source: &mut (impl AsyncRead + Unpin),
        version: FileVersionId,
        fingerprint: FileFingerprint,
        base_version: Option<FileVersionId>,
        gc_epoch: GcEpoch,
        change_cursor: ChangeCursor,
    ) -> Result<ObjectRef, Error> {
        let reusable = match base_version {
            Some(base) => self.file_extents(base).await?,
            None => Vec::new(),
        };
        let mut extents = Vec::new();
        let mut file_hasher = blake3::Hasher::new();
        let mut logical_offset = 0_u64;
        loop {
            let mut bytes = Vec::with_capacity(SHARD_TARGET_BYTES as usize);
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
                        && extent.extent.object_range.offset == 0
                        && extent.extent.object_range.length == length
                        && extent.extent.shard.payload_length == length
                        && extent.extent.shard.payload_digest.as_bytes()
                            == payload_digest.as_bytes()
                })
                .map(|extent| extent.extent.shard);
            let shard = match reusable_shard {
                Some(shard) => shard,
                None => {
                    let mut source = std::io::Cursor::new(bytes);
                    stream::write_bytes(
                        self.operator(),
                        gc_epoch,
                        ObjectClass::FileShard,
                        StreamKind::FILE_BYTES,
                        &mut source,
                    )
                    .await?
                }
            };
            extents.push(FileExtentMutation {
                logical_range: ByteRange {
                    offset: logical_offset,
                    length,
                },
                change_cursor,
                extent: DataExtent {
                    shard,
                    object_range: ByteRange { offset: 0, length },
                },
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
        let extent_segments = if extents.is_empty() {
            Vec::new()
        } else {
            vec![
                stream::write_records(
                    self.operator(),
                    gc_epoch,
                    ObjectClass::FileExtentSegment,
                    StreamKind::FILE_EXTENT_MUTATIONS,
                    extents,
                )
                .await?,
            ]
        };
        object::write_immutable(
            self.operator(),
            gc_epoch,
            ObjectClass::FileManifest,
            MANIFEST_RECORD.encode(&FileManifest {
                file_version: version,
                file_size: fingerprint.logical_length(),
                content_fingerprint: fingerprint,
                extent_segments,
            })?,
        )
        .await
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
        let manifest = read_manifest(self, record.manifest).await?;
        if manifest.file_version != version
            || manifest.file_size != record.file_size
            || manifest.content_fingerprint != record.content_fingerprint
        {
            return Err(Error::corrupt(
                "read Managed file",
                "file manifest does not match its version",
            ));
        }
        let mut extents = Vec::new();
        for reference in manifest.extent_segments {
            if reference.kind != StreamKind::FILE_EXTENT_MUTATIONS
                || reference.object.class != ObjectClass::FileExtentSegment
            {
                return Err(Error::corrupt(
                    "read Managed file",
                    "file extent stream has the wrong type",
                ));
            }
            extents.extend(
                stream::read_records::<FileExtentMutation>(self.operator(), reference).await?,
            );
        }
        extents.sort_by_key(|extent| extent.logical_range.offset);
        let mut expected_offset = 0_u64;
        for extent in extents {
            if extent.logical_range.offset != expected_offset
                || extent
                    .extent
                    .object_range
                    .offset
                    .checked_add(extent.extent.object_range.length)
                    .is_none_or(|end| end > extent.extent.shard.payload_length)
                || extent.logical_range.length != extent.extent.object_range.length
                || extent.extent.shard.kind != StreamKind::FILE_BYTES
                || extent.extent.shard.object.class != ObjectClass::FileShard
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
                let object_start = extent.extent.object_range.offset + selected_start
                    - extent.logical_range.offset;
                let object_end =
                    extent.extent.object_range.offset + selected_end - extent.logical_range.offset;
                if object_start == 0
                    && object_end == extent.extent.shard.payload_length
                    && range.start <= extent.logical_range.offset
                    && range.end >= extent_end
                {
                    stream::copy_bytes(self.operator(), extent.extent.shard, destination).await?;
                } else {
                    stream::copy_byte_range(
                        self.operator(),
                        extent.extent.shard,
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
        if expected_offset != manifest.file_size {
            return Err(Error::corrupt(
                "read Managed file",
                "file extents do not cover the declared file size",
            ));
        }
        Ok(())
    }

    async fn file_extents(&self, version: FileVersionId) -> Result<Vec<FileExtentMutation>, Error> {
        let record = self.file_version_record(version)?;
        let manifest = read_manifest(self, record.manifest).await?;
        let mut extents = Vec::new();
        for reference in manifest.extent_segments {
            extents.extend(
                stream::read_records::<FileExtentMutation>(self.operator(), reference).await?,
            );
        }
        Ok(extents)
    }
}

pub(super) async fn visit_manifest_objects(
    volume: &ManagedVolume,
    reference: ObjectRef,
    visit: &mut impl FnMut(ObjectRef) -> Result<(), Error>,
) -> Result<(), Error> {
    let manifest = read_manifest(volume, reference).await?;
    for extent_segment in manifest.extent_segments {
        visit(extent_segment.object)?;
        let extents =
            stream::read_records::<FileExtentMutation>(volume.operator(), extent_segment).await?;
        for extent in extents {
            visit(extent.extent.shard.object)?;
        }
    }
    Ok(())
}

async fn read_manifest(
    volume: &ManagedVolume,
    reference: ObjectRef,
) -> Result<FileManifest, Error> {
    if reference.class != ObjectClass::FileManifest {
        return Err(Error::corrupt(
            "read Managed file",
            "manifest reference has the wrong object class",
        ));
    }
    let bytes = object::read_immutable(
        volume.operator(),
        reference,
        MANIFEST_RECORD.maximum_encoded_bytes(),
    )
    .await?;
    MANIFEST_RECORD.decode(&bytes)
}
