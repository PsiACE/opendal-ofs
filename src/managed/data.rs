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
use opendal::{Buffer, ErrorKind as StorageErrorKind, Operator};
use tokio::fs::File;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::filesystem::{Digest, FileVersion, FileVersionId, OperationId};
use crate::{Error, ErrorKind};

use super::ManagedVolume;

const IO_BUFFER_BYTES: usize = 256 * 1024;
const UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(super) struct WholeObject {
    pub(super) digest: [u8; 32],
    pub(super) length: u64,
}

impl ManagedVolume {
    /// Inspect a local file without publishing data.
    pub async fn inspect_file(&self, path: &Path) -> Result<FileVersion, Error> {
        let mut file = File::open(path)
            .await
            .map_err(|error| Error::from_io("inspect local file", Some(path), error))?;
        let (logical_size, logical_digest) =
            inspect_reader(&mut file, path, "inspect local file").await?;
        Ok(FileVersion::new(FileVersionId::new(
            Digest::from_bytes(logical_digest),
            logical_size,
        )))
    }

    /// Publish one immutable whole-file object before its version is committed.
    pub async fn publish_file(&self, path: &Path, version: &FileVersion) -> Result<(), Error> {
        let mut file = File::open(path)
            .await
            .map_err(|error| Error::from_io("publish local file", Some(path), error))?;
        let Some(object) = whole_object(version)? else {
            let (length, digest) = inspect_reader(&mut file, path, "publish local file").await?;
            return verify_local_identity(length, digest, version);
        };

        let key = object_key(object.digest);
        let mut writer = self
            .operator()
            .writer_with(&key)
            .if_not_exists(true)
            .chunk(UPLOAD_PART_BYTES)
            .await
            .map_err(|error| Error::from_storage("publish Managed file", error))?;
        let mut hasher = Hasher::new();
        let mut length = 0_u64;
        let mut buffer = vec![0; IO_BUFFER_BYTES];
        let transfer = async {
            loop {
                let read = file
                    .read(&mut buffer)
                    .await
                    .map_err(|error| Error::from_io("publish local file", Some(path), error))?;
                if read == 0 {
                    break;
                }
                let bytes = &buffer[..read];
                hasher.update(bytes);
                length = length
                    .checked_add(read as u64)
                    .ok_or_else(|| Error::invalid("publish local file", "file length overflows"))?;
                writer
                    .write(Buffer::from(bytes.to_vec()))
                    .await
                    .map_err(|error| Error::from_storage("publish Managed file", error))?;
            }
            verify_local_identity(length, hasher.finalize().into(), version)
        }
        .await;
        if let Err(error) = transfer {
            let _ = writer.abort().await;
            return Err(error);
        }

        let close_error = match writer.close().await {
            Ok(_) => return Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
                ) =>
            {
                let metadata = self
                    .operator()
                    .stat(&key)
                    .await
                    .map_err(|error| Error::from_storage("publish Managed file", error))?;
                if metadata.content_length() == object.length {
                    return Ok(());
                }
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "publish local file",
                    "immutable object has an invalid length",
                ));
            }
            Err(error) => error,
        };
        match stream_object(self.operator(), object, None).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::Corrupt => Err(error),
            Err(_) => Err(Error::from_storage("publish Managed file", close_error)),
        }
    }

    /// Materialize and verify a complete file beside its final destination.
    pub async fn materialize_file(
        &self,
        version: &FileVersion,
        destination: &Path,
    ) -> Result<(), Error> {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| Error::from_io("create replica directory", Some(parent), error))?;
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file");
        let temporary =
            destination.with_file_name(format!(".{name}.{}.tmp", OperationId::generate()));
        let result = async {
            let mut file = File::create(&temporary).await.map_err(|error| {
                Error::from_io("create replica staging file", Some(&temporary), error)
            })?;
            if let Some(object) = whole_object(version)? {
                stream_object(self.operator(), object, Some(&mut file)).await?;
            }
            file.sync_all().await.map_err(|error| {
                Error::from_io("persist replica staging file", Some(&temporary), error)
            })?;
            drop(file);
            tokio::fs::rename(&temporary, destination)
                .await
                .map_err(|error| {
                    Error::from_io("install replica file", Some(destination), error)
                })?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
        Ok(())
    }
}

async fn inspect_reader(
    file: &mut File,
    path: &Path,
    action: &'static str,
) -> Result<(u64, [u8; 32]), Error> {
    let mut hasher = Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0; IO_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| Error::from_io(action, Some(path), error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| Error::invalid(action, "file length overflows"))?;
    }
    Ok((length, hasher.finalize().into()))
}

async fn stream_object(
    operator: &Operator,
    object: WholeObject,
    mut destination: Option<&mut File>,
) -> Result<(), Error> {
    let key = object_key(object.digest);
    let reader = match operator.reader(&key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => {
            return Err(Error::corrupt(
                "read Managed data",
                "referenced object is missing",
            ));
        }
        Err(error) => return Err(Error::from_storage("read Managed data", error)),
    };
    let mut stream = reader
        .into_stream(..)
        .await
        .map_err(|error| Error::from_storage("read Managed data", error))?;
    let mut hasher = Hasher::new();
    let mut length = 0_u64;
    while let Some(buffer) = stream.next().await {
        let buffer = match buffer {
            Ok(buffer) => buffer,
            Err(error) if error.kind() == StorageErrorKind::NotFound => {
                return Err(Error::corrupt(
                    "read Managed data",
                    "referenced object is missing",
                ));
            }
            Err(error) => return Err(Error::from_storage("read Managed data", error)),
        };
        for chunk in buffer {
            hasher.update(&chunk);
            length = length
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| Error::corrupt("read Managed data", "object length overflows"))?;
            if let Some(file) = destination.as_mut() {
                file.write_all(&chunk)
                    .await
                    .map_err(|error| Error::io("write replica staging file", error))?;
            }
        }
    }
    if length != object.length || hasher.finalize().as_bytes() != &object.digest {
        return Err(Error::corrupt(
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
) -> Result<(), Error> {
    if length != version.logical_length() || digest != *version.digest().as_bytes() {
        return Err(Error::invalid(
            "publish local file",
            "file changed while being published",
        ));
    }
    Ok(())
}

pub(super) fn whole_object(version: &FileVersion) -> Result<Option<WholeObject>, Error> {
    let digest = *version.digest().as_bytes();
    match version.logical_length() {
        0 if digest == *blake3::hash(&[]).as_bytes() => Ok(None),
        0 => Err(Error::corrupt(
            "read Managed file",
            "empty file content identity is invalid",
        )),
        length => Ok(Some(WholeObject { digest, length })),
    }
}

fn object_key(digest: [u8; 32]) -> String {
    let digest = blake3::Hash::from_bytes(digest).to_hex();
    format!("managed/1/objects/raw/{}/{}", &digest.as_str()[..2], digest)
}

pub(super) fn whole_object_key(digest: [u8; 32]) -> String {
    object_key(digest)
}
