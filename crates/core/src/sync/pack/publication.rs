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

use serde::{Deserialize, Serialize};
use tokio::fs::File;

use crate::Error;
use crate::filesystem::ContentRef;
use crate::managed::extension::{ExtentRef, SegmentRangeRef};
use crate::managed::{FileDataRef, GcEpoch, ManagedVolume, SegmentWriter};
use crate::workset::{Spool, SpoolWriter, Workspace};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PendingFile {
    pub(crate) path: String,
    pub(crate) fingerprint: ContentRef,
}

pub(crate) struct PublicationPlan {
    files: SpoolWriter<PendingFile>,
    body_bytes: u64,
}

impl PublicationPlan {
    pub(crate) fn accepts(target: u64, fingerprint: ContentRef) -> bool {
        fingerprint.length() != 0 && fingerprint.length() <= target
    }

    pub(crate) fn create(workspace: &Workspace) -> Result<Self, Error> {
        Ok(Self {
            files: workspace.writer("pack-publication")?,
            body_bytes: 0,
        })
    }

    pub(crate) fn would_overflow(&self, target: u64, fingerprint: ContentRef) -> bool {
        self.body_bytes
            .checked_add(fingerprint.length())
            .is_none_or(|bytes| bytes > target)
    }

    pub(crate) fn push(&mut self, file: PendingFile) -> Result<(), Error> {
        self.body_bytes = self
            .body_bytes
            .checked_add(file.fingerprint.length())
            .ok_or_else(|| Error::invalid("plan Managed pack", "pack length overflows"))?;
        self.files.write(&file)
    }

    pub(crate) fn finish(self) -> Result<Spool<PendingFile>, Error> {
        self.files.finish()
    }
}

pub(crate) async fn publish(
    volume: &ManagedVolume,
    workspace: &Workspace,
    root: &Path,
    files: &Spool<PendingFile>,
    gc_epoch: GcEpoch,
) -> Result<Spool<(String, FileDataRef)>, Error> {
    let mut pack = SegmentWriter::open(volume.operator(), gc_epoch).await?;
    let result = async {
        let mut source = files.reader()?;
        let mut expected_offset = 0_u64;
        while let Some(file) = source.next()? {
            let path = root.join(&file.path);
            let mut source = File::open(&path)
                .await
                .map_err(|error| Error::from_io("publish local file", Some(&path), error))?;
            let offset = pack
                .write_file(&mut source, file.fingerprint)
                .await
                .map_err(|error| error.with_context("path", path.display()))?;
            if offset != expected_offset {
                return Err(Error::corrupt(
                    "write Managed pack",
                    "pack payload offsets are not contiguous",
                ));
            }
            expected_offset = expected_offset
                .checked_add(file.fingerprint.length())
                .ok_or_else(|| Error::invalid("write Managed pack", "payload length overflows"))?;
        }
        Ok::<_, Error>(())
    }
    .await;
    if let Err(error) = result {
        let _ = pack.abort().await;
        return Err(error);
    }
    let segment = pack.close().await?;
    let mut published = workspace.writer("published-pack-files")?;
    let mut source = files.reader()?;
    let mut offset = 0_u64;
    while let Some(file) = source.next()? {
        published.write(&(
            file.path,
            FileDataRef::single(ExtentRef {
                range: SegmentRangeRef {
                    segment: segment.object.locator,
                    offset,
                    stored: file.fingerprint,
                },
                decoded: Vec::new(),
            }),
        ))?;
        offset = offset
            .checked_add(file.fingerprint.length())
            .ok_or_else(|| Error::invalid("publish Managed pack", "payload length overflows"))?;
    }
    published.finish()
}
