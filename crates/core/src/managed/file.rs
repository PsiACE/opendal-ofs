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

//! Streaming transfer through the one ordered-extent file representation.

use std::ops::{Bound, Range, RangeBounds};

use opendal::Operator;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite};

use crate::Error;
use crate::filesystem::ContentRef;

use super::data::FileDataRef;
use super::extension::{ExtentRef, SegmentRangeRef};
use super::format::FilePlacement;
use super::object::{GcEpoch, ObjectClass};
use super::stream;
use super::stream::RecordStreamReader;
use super::{KnownContent, ManagedVolume, SegmentRangeReader};

/// Single-pass reader over one file's inline extent and optional tail.
pub struct FileExtentReader {
    first: Option<ExtentRef>,
    tail: Option<RecordStreamReader<ExtentRef>>,
}

impl FileDataRef {
    /// Open the ordered logical extent stream.
    pub async fn extents(&self, operator: &Operator) -> Result<FileExtentReader, Error> {
        let tail = match self.tail {
            Some(tail) => Some(RecordStreamReader::open(operator, tail).await?),
            None => None,
        };
        Ok(FileExtentReader {
            first: self.first.clone(),
            tail,
        })
    }
}

impl FileExtentReader {
    /// Read the next logical extent.
    pub async fn next(&mut self) -> Result<Option<ExtentRef>, Error> {
        if self.first.is_some() {
            return Ok(self.first.take());
        }
        match self.tail.as_mut() {
            Some(tail) => tail.next().await,
            None => Ok(None),
        }
    }
}

impl ManagedVolume {
    /// Publish one file through the volume's partition and encoding policy.
    pub(crate) async fn publish_data(
        &self,
        source: &mut (impl AsyncRead + Send + Unpin),
        content: ContentRef,
        known: &KnownContent,
        gc_epoch: GcEpoch,
    ) -> Result<FileDataRef, Error> {
        if content.length() == 0 {
            let mut byte = [0_u8; 1];
            if source
                .read(&mut byte)
                .await
                .map_err(|error| Error::io("read empty Managed file", error))?
                != 0
            {
                return Err(Error::conflict(
                    "publish Managed file",
                    "source changed while being published",
                ));
            }
            return Ok(FileDataRef::empty());
        }
        if matches!(self.format.file_placement(), FilePlacement::Extension(_)) {
            return self
                .file_access()?
                .write_dyn(&self.access_context, source, content, known, gc_epoch)
                .await;
        }
        let segment = stream::write_byte_stream(
            self.operator(),
            gc_epoch,
            ObjectClass::DataSegment,
            source,
            content.length(),
            content.digest(),
        )
        .await?;
        Ok(FileDataRef::single(ExtentRef {
            range: SegmentRangeRef {
                segment: segment.object.locator,
                offset: 0,
                stored: content,
            },
            decoded: Vec::new(),
        }))
    }

    /// Read one logical byte range through its ordered extents.
    pub(crate) async fn read_data(
        &self,
        content: (ContentRef, FileDataRef),
        range: impl RangeBounds<u64>,
        destination: &mut (impl AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        let (content, reference) = content;
        let range = logical_range(content.length(), range)?;
        if matches!(self.format.file_placement(), FilePlacement::Extension(_)) {
            return self
                .file_access()?
                .read_dyn(&self.access_context, reference, content, range, destination)
                .await;
        }
        read_identity_extents(self, reference, content, range, destination).await
    }
}

async fn read_identity_extents(
    volume: &ManagedVolume,
    reference: FileDataRef,
    content: ContentRef,
    range: Range<u64>,
    destination: &mut (impl AsyncWrite + Send + Unpin),
) -> Result<(), Error> {
    let mut extents = reference.extents(volume.operator()).await?;
    let mut logical_offset = 0_u64;
    while let Some(extent) = extents.next().await? {
        if !extent.decoded.is_empty() || extent.range.segment.class != ObjectClass::DataSegment {
            return Err(Error::corrupt(
                "read Managed file",
                "core file extent has an unsupported encoding",
            ));
        }
        let extent_end = logical_offset
            .checked_add(extent.range.stored.length())
            .ok_or_else(|| Error::corrupt("read Managed file", "extent range overflows"))?;
        if logical_offset < range.end && range.start < extent_end {
            copy_identity_range(
                volume,
                extent.range,
                range.start.saturating_sub(logical_offset)
                    ..range.end.min(extent_end) - logical_offset,
                destination,
            )
            .await?;
        }
        logical_offset = extent_end;
    }
    if logical_offset != content.length() {
        return Err(Error::corrupt(
            "read Managed file",
            "extents do not cover the logical file",
        ));
    }
    Ok(())
}

async fn copy_identity_range(
    volume: &ManagedVolume,
    reference: SegmentRangeRef,
    range: Range<u64>,
    destination: &mut (impl AsyncWrite + Send + Unpin),
) -> Result<(), Error> {
    let start = reference
        .offset
        .checked_add(range.start)
        .ok_or_else(|| Error::corrupt("read Managed file", "segment range start overflows"))?;
    let end = reference
        .offset
        .checked_add(range.end)
        .ok_or_else(|| Error::corrupt("read Managed file", "segment range end overflows"))?;
    let mut reader =
        SegmentRangeReader::open(volume.operator(), reference.segment, start..end).await?;
    if range.start == 0 && range.end == reference.stored.length() {
        reader.copy_file(reference.stored, destination).await
    } else {
        reader
            .copy_bytes(range.end - range.start, destination)
            .await
    }
}

fn logical_range(file_size: u64, range: impl RangeBounds<u64>) -> Result<Range<u64>, Error> {
    let start = match range.start_bound() {
        Bound::Included(offset) => *offset,
        Bound::Excluded(offset) => offset
            .checked_add(1)
            .ok_or_else(|| Error::invalid("read Managed file range", "range start overflows"))?,
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
    Ok(start..end)
}
