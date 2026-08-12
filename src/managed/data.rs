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

use blake3::Hasher;
use futures::StreamExt as _;
use opendal::{Buffer, ErrorKind as StorageErrorKind, Operator};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::filesystem::FileVersion;
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
    /// Publish one immutable whole-file object before its version is committed.
    pub(crate) async fn publish_data(
        &self,
        source: &mut (impl AsyncRead + Unpin),
        version: &FileVersion,
    ) -> Result<(), Error> {
        let object = whole_object(version)?;
        let mut writer = if let Some(object) = object {
            let key = object_key(object.digest);
            Some(
                self.operator()
                    .writer_with(&key)
                    .if_not_exists(true)
                    .chunk(UPLOAD_PART_BYTES)
                    .await
                    .map_err(|error| Error::from_storage("publish Managed file", error))?,
            )
        } else {
            None
        };
        let mut hasher = Hasher::new();
        let mut length = 0_u64;
        let mut buffer = vec![0; IO_BUFFER_BYTES];
        let transfer = async {
            loop {
                let read = source
                    .read(&mut buffer)
                    .await
                    .map_err(|error| Error::io("read Managed data source", error))?;
                if read == 0 {
                    break;
                }
                let bytes = &buffer[..read];
                hasher.update(bytes);
                length = length.checked_add(read as u64).ok_or_else(|| {
                    Error::invalid("publish Managed data", "file length overflows")
                })?;
                if let Some(writer) = &mut writer {
                    writer
                        .write(Buffer::from(bytes.to_vec()))
                        .await
                        .map_err(|error| Error::from_storage("publish Managed file", error))?;
                }
            }
            verify_source_identity(length, hasher.finalize().into(), version)
        }
        .await;
        if let Err(error) = transfer {
            if let Some(writer) = &mut writer {
                let _ = writer.abort().await;
            }
            return Err(error);
        }
        let Some(object) = object else {
            return Ok(());
        };
        let key = object_key(object.digest);
        let mut writer = writer.expect("a non-empty Managed file has a data writer");

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
                    "publish Managed data",
                    "immutable object has an invalid length",
                ));
            }
            Err(error) => error,
        };
        let mut sink = tokio::io::sink();
        match stream_object(self.operator(), object, &mut sink).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::Corrupt => Err(error),
            Err(_) => Err(Error::from_storage("publish Managed file", close_error)),
        }
    }

    /// Read and verify one immutable whole-file object into a destination.
    pub(crate) async fn read_data(
        &self,
        version: &FileVersion,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        if let Some(object) = whole_object(version)? {
            stream_object(self.operator(), object, destination).await?;
        }
        Ok(())
    }
}

async fn stream_object(
    operator: &Operator,
    object: WholeObject,
    destination: &mut (impl AsyncWrite + Unpin),
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
            destination
                .write_all(&chunk)
                .await
                .map_err(|error| Error::io("write Managed data destination", error))?;
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

fn verify_source_identity(
    length: u64,
    digest: [u8; 32],
    version: &FileVersion,
) -> Result<(), Error> {
    if length != version.logical_length() || digest != *version.digest().as_bytes() {
        return Err(Error::invalid(
            "publish Managed data",
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
