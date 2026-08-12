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
use tokio::fs::File;
use tokio::io::AsyncReadExt as _;

use crate::Error;
use crate::filesystem::{Digest, FileFingerprint, OperationId};
use crate::managed::{FileDataRef, GcEpoch, ManagedVolume};

const IO_BUFFER_BYTES: usize = 256 * 1024;

pub(super) async fn inspect_file(path: &Path) -> Result<FileFingerprint, Error> {
    let mut file = File::open(path)
        .await
        .map_err(|error| Error::from_io("inspect local file", Some(path), error))?;
    let mut hasher = Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0; IO_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| Error::from_io("inspect local file", Some(path), error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| Error::invalid("inspect local file", "file length overflows"))?;
    }
    Ok(FileFingerprint::new(
        Digest::from_bytes(hasher.finalize().into()),
        length,
    ))
}

pub(super) async fn publish_file(
    volume: &ManagedVolume,
    path: &Path,
    fingerprint: FileFingerprint,
    gc_epoch: GcEpoch,
) -> Result<crate::managed::FileDataRef, Error> {
    let mut file = File::open(path)
        .await
        .map_err(|error| Error::from_io("publish local file", Some(path), error))?;
    volume
        .publish_data(&mut file, fingerprint, gc_epoch)
        .await
        .map_err(|error| error.with_context("path", path.display()))
}

pub(super) async fn materialize_file(
    volume: &ManagedVolume,
    content: (FileFingerprint, FileDataRef),
    destination: &Path,
    executable: bool,
) -> Result<(), Error> {
    let mut staging = StagedFile::create(destination).await?;
    let result = volume
        .read_data(content, .., staging.writer())
        .await
        .map_err(|error| error.with_context("path", staging.path().display()));
    if let Err(error) = result {
        staging.abort().await;
        return Err(error);
    }
    staging.commit(executable).await
}

/// One durable local-file installation shared by every data placement.
pub(super) struct StagedFile {
    destination: std::path::PathBuf,
    temporary: std::path::PathBuf,
    file: Option<File>,
}

impl StagedFile {
    pub(super) async fn create(destination: &Path) -> Result<Self, Error> {
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
        let file = File::create(&temporary).await.map_err(|error| {
            Error::from_io("create replica staging file", Some(&temporary), error)
        })?;
        Ok(Self {
            destination: destination.to_owned(),
            temporary,
            file: Some(file),
        })
    }

    pub(super) fn writer(&mut self) -> &mut File {
        self.file.as_mut().expect("staging file is open")
    }

    pub(super) fn path(&self) -> &Path {
        &self.temporary
    }

    pub(super) async fn commit(mut self, executable: bool) -> Result<(), Error> {
        let file = self.file.take().expect("staging file is open");
        if let Err(error) = file.sync_all().await {
            drop(file);
            let _ = tokio::fs::remove_file(&self.temporary).await;
            return Err(Error::from_io(
                "persist replica staging file",
                Some(&self.temporary),
                error,
            ));
        }
        drop(file);
        if let Err(error) = tokio::fs::rename(&self.temporary, &self.destination).await {
            let _ = tokio::fs::remove_file(&self.temporary).await;
            return Err(Error::from_io(
                "install replica file",
                Some(&self.destination),
                error,
            ));
        }
        if executable {
            super::local_fs::make_executable(&self.destination)?;
        }
        Ok(())
    }

    pub(super) async fn abort(mut self) {
        drop(self.file.take());
        let _ = tokio::fs::remove_file(&self.temporary).await;
    }
}
