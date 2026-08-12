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

//! Streaming transfer of immutable Managed file data.

use std::ops::{Bound, RangeBounds};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::Error;
use crate::filesystem::FileFingerprint;

use super::ManagedVolume;
use super::PackRangeReader;
use super::data::FileDataRef;
use super::object::GcEpoch;
use super::stream;

impl ManagedVolume {
    /// Publish one file as an immutable byte stream.
    pub(crate) async fn publish_data(
        &self,
        source: &mut (impl AsyncRead + Unpin),
        fingerprint: FileFingerprint,
        gc_epoch: GcEpoch,
    ) -> Result<FileDataRef, Error> {
        let stream = stream::write_byte_stream(
            self.operator(),
            gc_epoch,
            source,
            fingerprint.logical_length(),
            fingerprint.digest(),
        )
        .await?;
        FileDataRef::from_stream(stream, fingerprint)
    }

    /// Read one logical byte range and verify complete-file reads.
    pub(crate) async fn read_data(
        &self,
        content: (FileFingerprint, FileDataRef),
        range: impl RangeBounds<u64>,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        let (fingerprint, content) = content;
        let file_size = fingerprint.logical_length();
        let start = match range.start_bound() {
            Bound::Included(offset) => *offset,
            Bound::Excluded(offset) => offset.checked_add(1).ok_or_else(|| {
                Error::invalid("read Managed file range", "range start overflows")
            })?,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(offset) => offset
                .checked_add(1)
                .ok_or_else(|| Error::invalid("read Managed file range", "range end overflows"))?,
            Bound::Excluded(offset) => *offset,
            Bound::Unbounded => file_size,
        };
        if start > end || end > file_size {
            return Err(Error::invalid(
                "read Managed file range",
                "logical byte range is invalid",
            ));
        }
        if let Some(offset) = content.pack_offset() {
            if start == end {
                return Ok(());
            }
            let physical_start = offset
                .checked_add(start)
                .ok_or_else(|| Error::corrupt("read Managed file", "pack range start overflows"))?;
            let physical_end = offset
                .checked_add(end)
                .ok_or_else(|| Error::corrupt("read Managed file", "pack range end overflows"))?;
            let mut reader = PackRangeReader::open(
                self.operator(),
                content.object_locator(),
                physical_start..physical_end,
            )
            .await?;
            if start == 0 && end == file_size {
                reader.copy_file(fingerprint, destination).await
            } else {
                reader.copy_bytes(end - start, destination).await
            }
        } else {
            stream::copy_byte_stream(
                self.operator(),
                content.stream_ref(fingerprint)?,
                start..end,
                destination,
            )
            .await
        }
    }
}
