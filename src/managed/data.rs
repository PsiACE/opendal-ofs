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

use std::path::Path;

use blake3::Hasher;
use opendal::Buffer;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::filesystem::{FileVersion, FileVersionId, OperationId, VolumeError, VolumeErrorKind};

use super::ManagedVolume;
use super::object;

const SEGMENT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Segment {
    pub(super) digest: [u8; 32],
    pub(super) length: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    pub(super) segments: Vec<Segment>,
}

impl ManagedVolume {
    /// Inspect a local file without publishing data.
    pub async fn inspect_file(&self, path: &Path) -> Result<FileVersion, VolumeError> {
        let mut file = File::open(path)
            .await
            .map_err(|_| unavailable("inspect local file"))?;
        let mut logical = Hasher::new();
        let mut logical_size = 0_u64;
        let mut segments = Vec::new();
        let mut buffer = vec![0; SEGMENT_BYTES];
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .map_err(|_| unavailable("inspect local file"))?;
            if read == 0 {
                break;
            }
            let bytes = &buffer[..read];
            logical.update(bytes);
            logical_size = logical_size
                .checked_add(read as u64)
                .ok_or_else(|| invalid("inspect local file", "file length overflows"))?;
            segments.push(Segment {
                digest: blake3::hash(bytes).into(),
                length: read as u64,
            });
        }
        let logical_digest: [u8; 32] = logical.finalize().into();
        let descriptor = serde_json::to_vec(&Descriptor { segments })
            .map_err(|_| invalid("inspect local file", "descriptor cannot be encoded"))?;
        Ok(FileVersion::from_parts(
            FileVersionId::from_bytes(logical_digest),
            logical_size,
            logical_digest,
            descriptor,
        ))
    }

    /// Publish every immutable segment before its file version can be committed.
    pub async fn publish_file(
        &self,
        path: &Path,
        version: &FileVersion,
    ) -> Result<(), VolumeError> {
        let descriptor = decode_descriptor(version)?;
        let mut file = File::open(path)
            .await
            .map_err(|_| unavailable("publish local file"))?;
        let mut logical = Hasher::new();
        let mut buffer = vec![0; SEGMENT_BYTES];
        for expected in descriptor.segments {
            let length = usize::try_from(expected.length)
                .ok()
                .filter(|length| *length <= SEGMENT_BYTES && *length > 0)
                .ok_or_else(|| corrupt("publish local file", "segment length is invalid"))?;
            file.read_exact(&mut buffer[..length])
                .await
                .map_err(|_| invalid("publish local file", "file changed while being published"))?;
            let bytes = &buffer[..length];
            if blake3::hash(bytes).as_bytes() != &expected.digest {
                return Err(invalid(
                    "publish local file",
                    "file changed while being published",
                ));
            }
            logical.update(bytes);
            object::create_immutable(
                self.operator(),
                &segment_key(expected.digest),
                Buffer::from(bytes.to_vec()),
            )
            .await?;
        }
        if file
            .read(&mut buffer[..1])
            .await
            .map_err(|_| unavailable("publish local file"))?
            != 0
            || logical.finalize().as_bytes() != &version.logical_digest
        {
            return Err(invalid(
                "publish local file",
                "file changed while being published",
            ));
        }
        Ok(())
    }

    /// Materialize and verify a complete file beside its final destination.
    pub async fn materialize_file(
        &self,
        version: &FileVersion,
        destination: &Path,
    ) -> Result<(), VolumeError> {
        let descriptor = decode_descriptor(version)?;
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|_| unavailable("materialize Managed file"))?;
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file");
        let temporary =
            destination.with_file_name(format!(".{name}.{}.tmp", OperationId::generate()));
        let mut file = File::create(&temporary)
            .await
            .map_err(|_| unavailable("materialize Managed file"))?;
        let mut logical = Hasher::new();
        let mut logical_size = 0_u64;
        for segment in descriptor.segments {
            let bytes = object::read_data(self.operator(), &segment_key(segment.digest)).await?;
            if bytes.len() as u64 != segment.length {
                return Err(corrupt(
                    "materialize Managed file",
                    "segment length is invalid",
                ));
            }
            let mut segment_hash = Hasher::new();
            for chunk in bytes {
                segment_hash.update(&chunk);
                logical.update(&chunk);
                file.write_all(&chunk)
                    .await
                    .map_err(|_| unavailable("materialize Managed file"))?;
                logical_size = logical_size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| corrupt("materialize Managed file", "file length overflows"))?;
            }
            if segment_hash.finalize().as_bytes() != &segment.digest {
                return Err(corrupt(
                    "materialize Managed file",
                    "segment checksum is invalid",
                ));
            }
        }
        if logical_size != version.logical_size
            || logical.finalize().as_bytes() != &version.logical_digest
        {
            return Err(corrupt(
                "materialize Managed file",
                "file checksum is invalid",
            ));
        }
        file.sync_all()
            .await
            .map_err(|_| unavailable("materialize Managed file"))?;
        drop(file);
        tokio::fs::rename(&temporary, destination)
            .await
            .map_err(|_| unavailable("materialize Managed file"))?;
        Ok(())
    }
}

pub(super) fn decode_descriptor(version: &FileVersion) -> Result<Descriptor, VolumeError> {
    let descriptor: Descriptor = serde_json::from_slice(version.descriptor())
        .map_err(|_| corrupt("read Managed file", "file descriptor is invalid"))?;
    let expected_size = descriptor
        .segments
        .iter()
        .try_fold(0_u64, |size, segment| size.checked_add(segment.length))
        .ok_or_else(|| corrupt("read Managed file", "file length overflows"))?;
    if expected_size != version.logical_size
        || descriptor
            .segments
            .iter()
            .any(|segment| segment.length == 0 || segment.length > SEGMENT_BYTES as u64)
    {
        return Err(corrupt(
            "read Managed file",
            "file descriptor is inconsistent",
        ));
    }
    Ok(descriptor)
}

pub(super) fn segment_key(digest: [u8; 32]) -> String {
    format!(
        ".ofs/managed/data/{}",
        blake3::Hash::from_bytes(digest).to_hex()
    )
}

fn invalid(action: &'static str, message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Invalid, format!("{action}: {message}"))
}

fn corrupt(action: &'static str, message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Corrupt, format!("{action}: {message}"))
}

fn unavailable(action: &'static str) -> VolumeError {
    VolumeError::new(
        VolumeErrorKind::Unavailable,
        format!("{action}: storage operation failed"),
    )
}
