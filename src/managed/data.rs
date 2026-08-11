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
use futures::StreamExt as _;
use opendal::{Buffer, ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::filesystem::{
    Digest, FileVersion, FileVersionId, OperationId, VolumeError, VolumeErrorKind,
};

use super::ManagedVolume;

const IO_BUFFER_BYTES: usize = 256 * 1024;
const UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;
const UPLOAD_CONCURRENCY: usize = 4;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FileLayout {
    Empty,
    Whole,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct WholeObject {
    pub(super) digest: [u8; 32],
    pub(super) length: u64,
}

pub(super) struct WholeObjects(Option<WholeObject>);

impl WholeObjects {
    fn into_option(self) -> Option<WholeObject> {
        self.0
    }
}

impl IntoIterator for WholeObjects {
    type Item = WholeObject;
    type IntoIter = std::option::IntoIter<WholeObject>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A zero-or-one iterable object set for collector traversal.
pub(super) struct Descriptor {
    pub(super) segments: WholeObjects,
}

impl ManagedVolume {
    /// Inspect a local file without publishing data.
    pub async fn inspect_file(&self, path: &Path) -> Result<FileVersion, VolumeError> {
        let mut file = File::open(path)
            .await
            .map_err(|_| unavailable("inspect local file"))?;
        let (logical_size, logical_digest) =
            inspect_reader(&mut file, "inspect local file").await?;
        let layout = if logical_size == 0 {
            FileLayout::Empty
        } else {
            FileLayout::Whole
        };
        let descriptor = serde_json::to_vec(&layout)
            .map_err(|_| invalid("inspect local file", "descriptor cannot be encoded"))?;
        Ok(FileVersion::new(
            FileVersionId::new(Digest::from_bytes(logical_digest), logical_size),
            descriptor,
        ))
    }

    /// Publish one immutable whole-file object before its version is committed.
    pub async fn publish_file(
        &self,
        path: &Path,
        version: &FileVersion,
    ) -> Result<(), VolumeError> {
        let descriptor = decode_descriptor(version)?;
        let mut file = File::open(path)
            .await
            .map_err(|_| unavailable("publish local file"))?;
        let Some(object) = descriptor.segments.into_option() else {
            let (length, digest) = inspect_reader(&mut file, "publish local file").await?;
            return verify_local_identity(length, digest, version);
        };

        let key = object_key(object.digest);
        let mut writer = self
            .operator()
            .writer_with(&key)
            .chunk(UPLOAD_PART_BYTES)
            .concurrent(UPLOAD_CONCURRENCY)
            .await
            .map_err(|_| unavailable("publish local file"))?;
        let mut hasher = Hasher::new();
        let mut length = 0_u64;
        let mut buffer = vec![0; IO_BUFFER_BYTES];
        let transfer = async {
            loop {
                let read = file
                    .read(&mut buffer)
                    .await
                    .map_err(|_| unavailable("publish local file"))?;
                if read == 0 {
                    break;
                }
                let bytes = &buffer[..read];
                hasher.update(bytes);
                length = length
                    .checked_add(read as u64)
                    .ok_or_else(|| invalid("publish local file", "file length overflows"))?;
                writer
                    .write(Buffer::from(bytes.to_vec()))
                    .await
                    .map_err(|_| unavailable("publish local file"))?;
            }
            verify_local_identity(length, hasher.finalize().into(), version)
        }
        .await;
        if let Err(error) = transfer {
            let _ = writer.abort().await;
            return Err(error);
        }

        if writer.close().await.is_ok() {
            return Ok(());
        }
        if stream_object(self.operator(), object, None).await.is_ok() {
            Ok(())
        } else {
            Err(unavailable("publish local file"))
        }
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
        let result = async {
            let mut file = File::create(&temporary)
                .await
                .map_err(|_| unavailable("materialize Managed file"))?;
            if let Some(object) = descriptor.segments.into_option() {
                stream_object(self.operator(), object, Some(&mut file)).await?;
            }
            file.sync_all()
                .await
                .map_err(|_| unavailable("materialize Managed file"))?;
            drop(file);
            tokio::fs::rename(&temporary, destination)
                .await
                .map_err(|_| unavailable("materialize Managed file"))?;
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|_| unavailable("materialize Managed file"))?;
            Ok(())
        }
        .await;
        if result.is_err() {
            match tokio::fs::remove_file(&temporary).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(unavailable("clean up Managed file staging")),
            }
        }
        result
    }
}

async fn inspect_reader(
    file: &mut File,
    action: &'static str,
) -> Result<(u64, [u8; 32]), VolumeError> {
    let mut hasher = Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0; IO_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|_| unavailable(action))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| invalid(action, "file length overflows"))?;
    }
    Ok((length, hasher.finalize().into()))
}

async fn stream_object(
    operator: &Operator,
    object: WholeObject,
    mut destination: Option<&mut File>,
) -> Result<(), VolumeError> {
    let key = object_key(object.digest);
    let reader = match operator.reader(&key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(corrupt("read Managed data", "referenced object is missing"));
        }
        Err(_) => return Err(unavailable("read Managed data")),
    };
    let mut stream = reader
        .into_stream(..)
        .await
        .map_err(|_| unavailable("read Managed data"))?;
    let mut hasher = Hasher::new();
    let mut length = 0_u64;
    while let Some(buffer) = stream.next().await {
        let buffer = match buffer {
            Ok(buffer) => buffer,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(corrupt("read Managed data", "referenced object is missing"));
            }
            Err(_) => return Err(unavailable("read Managed data")),
        };
        for chunk in buffer {
            hasher.update(&chunk);
            length = length
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| corrupt("read Managed data", "object length overflows"))?;
            if let Some(file) = destination.as_mut() {
                file.write_all(&chunk)
                    .await
                    .map_err(|_| unavailable("materialize Managed file"))?;
            }
        }
    }
    if length != object.length || hasher.finalize().as_bytes() != &object.digest {
        return Err(corrupt(
            "read Managed data",
            "object content does not match its identity",
        ));
    }
    Ok(())
}

fn verify_local_identity(
    length: u64,
    digest: [u8; 32],
    version: &FileVersion,
) -> Result<(), VolumeError> {
    if length != version.logical_length() || digest != *version.digest().as_bytes() {
        return Err(invalid(
            "publish local file",
            "file changed while being published",
        ));
    }
    Ok(())
}

pub(super) fn decode_descriptor(version: &FileVersion) -> Result<Descriptor, VolumeError> {
    let layout: FileLayout = serde_json::from_slice(version.descriptor())
        .map_err(|_| corrupt("read Managed file", "file descriptor is invalid"))?;
    let digest = *version.digest().as_bytes();
    let segments = match (layout, version.logical_length()) {
        (FileLayout::Empty, 0) if digest == *blake3::hash(&[]).as_bytes() => None,
        (FileLayout::Whole, length) if length > 0 => Some(WholeObject { digest, length }),
        _ => {
            return Err(corrupt(
                "read Managed file",
                "file descriptor is inconsistent",
            ));
        }
    };
    Ok(Descriptor {
        segments: WholeObjects(segments),
    })
}

fn object_key(digest: [u8; 32]) -> String {
    let digest = blake3::Hash::from_bytes(digest).to_hex();
    format!("managed/1/objects/raw/{}/{}", &digest.as_str()[..2], digest)
}

pub(super) fn segment_key(digest: [u8; 32]) -> String {
    object_key(digest)
}

// Keep the metadata helper linked while data reads use bounded streams.
const _: () = {
    let _ = super::object::read_data;
};

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
