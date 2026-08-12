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

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::Error;
use crate::filesystem::{ChangeCursor, FileVersionId};

use super::ManagedVolume;
use super::object::{self, GcEpoch, ObjectClass, ObjectRef};
use super::record::Record;
use super::stream::{self, StreamKind, StreamRef};

const MANIFEST_RECORD: Record = Record::new(*b"OFSMAN01", 1, 1024 * 1024);

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FileManifest {
    file_version: FileVersionId,
    file_size: u64,
    extent_segments: Vec<StreamRef>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct FileExtentMutation {
    logical_offset: u64,
    length: u64,
    change_cursor: ChangeCursor,
    shard: StreamRef,
    object_offset: u64,
}

impl ManagedVolume {
    /// Publish one immutable file manifest over independently durable shards.
    pub(crate) async fn publish_data(
        &self,
        source: &mut (impl AsyncRead + Unpin),
        version: FileVersionId,
        gc_epoch: GcEpoch,
        change_cursor: ChangeCursor,
    ) -> Result<ObjectRef, Error> {
        let mut extent_segments = Vec::new();
        if version.logical_length() != 0 {
            let shard = stream::write_bytes(
                self.operator(),
                gc_epoch,
                ObjectClass::FileShard,
                StreamKind::FILE_BYTES,
                source,
            )
            .await?;
            if shard.payload_length != version.logical_length()
                || shard.payload_digest.as_bytes() != version.digest().as_bytes()
            {
                return Err(Error::conflict(
                    "publish Managed file",
                    "local file changed while being published",
                ));
            }
            extent_segments.push(
                stream::write_records(
                    self.operator(),
                    gc_epoch,
                    ObjectClass::FileExtentSegment,
                    StreamKind::FILE_EXTENT_MUTATIONS,
                    [FileExtentMutation {
                        logical_offset: 0,
                        length: version.logical_length(),
                        change_cursor,
                        shard,
                        object_offset: 0,
                    }],
                )
                .await?,
            );
        } else if version.digest().as_bytes() != blake3::hash(&[]).as_bytes() {
            return Err(Error::invalid(
                "publish Managed file",
                "empty file content identity is invalid",
            ));
        }
        object::write_immutable(
            self.operator(),
            gc_epoch,
            ObjectClass::FileManifest,
            MANIFEST_RECORD.encode(&FileManifest {
                file_version: version,
                file_size: version.logical_length(),
                extent_segments,
            })?,
        )
        .await
    }

    /// Read and verify one immutable file version into a destination.
    pub(crate) async fn read_data(
        &self,
        version: FileVersionId,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        let record = self.file_version_record(version)?;
        let manifest = read_manifest(self, record.manifest).await?;
        if manifest.file_version != version || manifest.file_size != version.logical_length() {
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
        extents.sort_by_key(|extent| extent.logical_offset);
        let mut expected_offset = 0_u64;
        for extent in extents {
            if extent.logical_offset != expected_offset
                || extent.object_offset != 0
                || extent.length != extent.shard.payload_length
                || extent.shard.kind != StreamKind::FILE_BYTES
                || extent.shard.object.class != ObjectClass::FileShard
            {
                return Err(Error::corrupt(
                    "read Managed file",
                    "file extents do not form a contiguous byte stream",
                ));
            }
            stream::copy_bytes(self.operator(), extent.shard, destination).await?;
            expected_offset = expected_offset
                .checked_add(extent.length)
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
            visit(extent.shard.object)?;
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
