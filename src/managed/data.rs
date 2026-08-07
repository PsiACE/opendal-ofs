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

//! Whole-file manifests backed by immutable loose objects.

use opendal::{ErrorKind, Operator, Writer};
use sha2::{Digest as _, Sha256};

use super::{ManagedError, ManagedErrorKind};
use crate::managed::namespace::{ContentRef, FileVersionLayout, FileVersionRecord};

const READ_WINDOW: u64 = 4 * 1024 * 1024;
const LOOSE_ROOT: &str = "data/v1/loose/sha256";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Digest([u8; 32]);

impl Digest {
    const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// The concrete Managed v1 data plane.
#[derive(Clone)]
pub(crate) struct ManagedData {
    operator: Operator,
}

impl ManagedData {
    pub(crate) fn new(operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.stat
            || !capability.read
            || !capability.write
            || !capability.write_can_empty
            || !capability.write_with_if_not_exists
        {
            return Err(invalid(
                "open Managed data",
                "data storage requires stat, read, empty write, and create-only write",
            ));
        }
        Ok(Self { operator })
    }

    /// Seal one file that Sync has already frozen against local mutation.
    pub(crate) async fn seal_whole_file(
        &self,
        local: &Operator,
        frozen_path: &str,
    ) -> Result<FileVersionRecord, ManagedError> {
        let metadata = local
            .stat(frozen_path)
            .await
            .map_err(|_| unavailable("read frozen file"))?;
        if !metadata.is_file() {
            return Err(invalid("read frozen file", "input is not a regular file"));
        }
        let size = metadata.content_length();
        let digest = digest_and_copy(local, frozen_path, size, None)
            .await
            .map_err(|_| unavailable("read frozen file"))?;
        let version = whole_file_version(size, digest);
        if size == 0 {
            return Ok(version);
        }
        let FileVersionLayout::Whole { content } = &version.layout else {
            unreachable!("whole-file sealing created another layout")
        };
        let key = loose_key(content);

        match self.operator.writer_with(&key).if_not_exists(true).await {
            Err(error) if already_exists(&error) => {}
            Err(_) => return Err(unavailable("create loose data")),
            Ok(mut writer) => {
                let observed =
                    match digest_and_copy(local, frozen_path, size, Some(&mut writer)).await {
                        Ok(observed) => observed,
                        Err(error) if already_exists(&error) => digest,
                        Err(_) => {
                            let _ = writer.abort().await;
                            return Err(unavailable("write loose data"));
                        }
                    };
                if observed != digest {
                    let _ = writer.abort().await;
                    return Err(invalid(
                        "write loose data",
                        "frozen input changed while it was being sealed",
                    ));
                }
                if let Err(error) = writer.close().await {
                    if !already_exists(&error) {
                        return Err(unavailable("commit loose data"));
                    }
                }
            }
        }

        self.verify(&version).await?;
        Ok(version)
    }

    /// Stream verified content into a caller-owned materialization path.
    pub(crate) async fn read_to(
        &self,
        version: &FileVersionRecord,
        target: &Operator,
        target_path: &str,
    ) -> Result<(), ManagedError> {
        let content = self.verify_metadata(version).await?;
        if version.logical_size == 0 {
            target
                .write(target_path, Vec::<u8>::new())
                .await
                .map_err(|_| unavailable("create materialized file"))?;
            return Ok(());
        }
        let key = loose_key(&content);
        let mut writer = target
            .writer(target_path)
            .await
            .map_err(|_| unavailable("create materialized file"))?;
        let digest = match digest_and_copy(
            &self.operator,
            &key,
            version.logical_size,
            Some(&mut writer),
        )
        .await
        {
            Ok(digest) => digest,
            Err(_) => {
                let _ = writer.abort().await;
                return Err(unavailable("read loose data"));
            }
        };
        if digest.as_bytes() != &content.digest {
            let _ = writer.abort().await;
            return Err(corrupt("read loose data", "content digest does not match"));
        }
        writer
            .close()
            .await
            .map_err(|_| unavailable("commit materialized file"))?;
        Ok(())
    }

    async fn verify(&self, version: &FileVersionRecord) -> Result<(), ManagedError> {
        let content = self.verify_metadata(version).await?;
        if content.logical_length == 0 {
            return Ok(());
        }
        let key = loose_key(&content);
        let digest = digest_and_copy(&self.operator, &key, version.logical_size, None)
            .await
            .map_err(|_| unavailable("verify loose data"))?;
        if digest.as_bytes() != &content.digest {
            return Err(corrupt(
                "verify loose data",
                "content digest does not match its key",
            ));
        }
        Ok(())
    }

    async fn verify_metadata(
        &self,
        version: &FileVersionRecord,
    ) -> Result<ContentRef, ManagedError> {
        if !version.is_valid() {
            return Err(corrupt(
                "read loose data",
                "file manifest identity is invalid",
            ));
        }
        let FileVersionLayout::Whole { content } = &version.layout else {
            return Err(invalid(
                "read loose data",
                "file layout is not supported by the whole-file reader",
            ));
        };
        if content.logical_length == 0 {
            return Ok(*content);
        }
        let metadata = self
            .operator
            .stat(&loose_key(content))
            .await
            .map_err(|_| unavailable("stat loose data"))?;
        if !metadata.is_file() || metadata.content_length() != content.logical_length {
            return Err(corrupt(
                "stat loose data",
                "stored size does not match the file version",
            ));
        }
        Ok(*content)
    }
}

async fn digest_and_copy(
    source: &Operator,
    path: &str,
    size: u64,
    mut target: Option<&mut Writer>,
) -> opendal::Result<Digest> {
    let reader = source.reader(path).await?;
    let mut hash = Sha256::new();
    let mut offset = 0;
    while offset < size {
        let end = (offset + READ_WINDOW).min(size);
        let buffer = reader.read(offset..end).await?;
        if buffer.len() as u64 != end - offset {
            return Err(opendal::Error::new(
                ErrorKind::Unexpected,
                "source returned a short range",
            ));
        }
        for bytes in buffer.clone() {
            hash.update(&bytes);
        }
        if let Some(writer) = target.as_deref_mut() {
            writer.write(buffer).await?;
        }
        offset = end;
    }
    Ok(Digest::from_bytes(hash.finalize().into()))
}

fn whole_file_version(size: u64, digest: Digest) -> FileVersionRecord {
    FileVersionRecord::whole(size, *digest.as_bytes())
}

fn loose_key(content: &ContentRef) -> String {
    let digest = Digest::from_bytes(content.digest).hex();
    format!("{LOOSE_ROOT}/{}/{digest}", &digest[..2])
}

fn already_exists(error: &opendal::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
    )
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "storage operation failed",
    )
}
