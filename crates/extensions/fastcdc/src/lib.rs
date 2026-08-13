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

//! Streaming FastCDC file layout for Managed volumes.

use std::ops::Range;

use fastcdc::v2020::AsyncStreamCDC;
use ofs_core::filesystem::{ContentRef, Digest};
use ofs_core::managed::extension::{
    AccessContext, ExtensionFormat, ExtensionId, ExtentAccess, ExtentRef, FileAccess,
    FileAccessInfo, FilePartitionExtension, RecordStreamWriter, StreamKind,
};
use ofs_core::managed::{FileDataRef, GcEpoch, KnownContent, ObjectClass};
use ofs_core::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt as _;

/// Stable wire identity of the FastCDC layout extension.
pub const FASTCDC_EXTENSION_ID: ExtensionId = ExtensionId::new(*b"ofs.fastcdc.v1!!");

const MINIMUM_CHUNK_BYTES: u32 = 64 * 1024;
const AVERAGE_CHUNK_BYTES: u32 = 256 * 1024;
const MAXIMUM_CHUNK_BYTES: u32 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Configuration(u32, u32, u32);

impl Configuration {
    const CURRENT: Self = Self(
        MINIMUM_CHUNK_BYTES,
        AVERAGE_CHUNK_BYTES,
        MAXIMUM_CHUNK_BYTES,
    );

    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes)
            .expect("the fixed FastCDC configuration is encodable");
        bytes
    }
}

/// FastCDC v2020 file-layout extension with a stable volume format.
#[derive(Clone, Copy, Debug, Default)]
pub struct FastCdcExtension;

impl FastCdcExtension {
    /// Construct the standard FastCDC layout.
    pub const fn new() -> Self {
        Self
    }
}

impl<A: ExtentAccess> FilePartitionExtension<A> for FastCdcExtension {
    type ExtendedAccess = FastCdcAccess<A>;

    fn extend(&self, inner: A) -> Self::ExtendedAccess {
        FastCdcAccess { inner }
    }
}

/// FastCDC file access composed over one extent encoding.
#[derive(Clone, Debug)]
pub struct FastCdcAccess<A> {
    inner: A,
}

impl<A: ExtentAccess> FileAccess for FastCdcAccess<A> {
    fn info(&self) -> FileAccessInfo {
        FileAccessInfo {
            partitioning: ExtensionFormat {
                id: FASTCDC_EXTENSION_ID,
                configuration: Configuration::CURRENT.encode(),
            },
            decodings: self.inner.info(),
        }
    }

    async fn write(
        &self,
        context: &AccessContext,
        source: &mut (dyn AsyncRead + Send + Unpin),
        content: ContentRef,
        known: &KnownContent,
        gc_epoch: GcEpoch,
    ) -> Result<FileDataRef, Error> {
        let mut first = None;
        let mut tail = None;
        let mut chunker = AsyncStreamCDC::new(
            source,
            MINIMUM_CHUNK_BYTES,
            AVERAGE_CHUNK_BYTES,
            MAXIMUM_CHUNK_BYTES,
        );
        let mut chunks = std::pin::pin!(chunker.as_stream());
        let mut logical_hasher = blake3::Hasher::new();
        let mut logical_length = 0_u64;
        let decoding_count = self.inner.info().len();

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|error| {
                Error::new(
                    ErrorKind::Unavailable,
                    "partition Managed file with FastCDC",
                    "source stream cannot be partitioned",
                )
                .with_source(error)
            })?;
            if chunk.offset != logical_length {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "partition Managed file with FastCDC",
                    "chunk offsets are not contiguous",
                ));
            }
            let chunk_length = u64::try_from(chunk.length).map_err(|_| {
                Error::new(
                    ErrorKind::Invalid,
                    "partition Managed file with FastCDC",
                    "chunk length overflows",
                )
            })?;
            logical_hasher.update(&chunk.data);
            let chunk_content = ContentRef::new(
                Digest::from_bytes(blake3::hash(&chunk.data).into()),
                chunk_length,
            );
            let extent = match known.extent(chunk_content)? {
                Some(extent) => extent,
                None => {
                    let mut bytes = chunk.data.as_slice();
                    self.inner.write(context, &mut bytes, gc_epoch).await?
                }
            };
            if extent.content() != chunk_content || extent.decoded.len() != decoding_count {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "publish Managed FastCDC extent",
                    "extent access returned a different content",
                ));
            }
            if first.is_none() {
                first = Some(extent);
            } else {
                if tail.is_none() {
                    tail = Some(
                        RecordStreamWriter::open(
                            context.operator(),
                            gc_epoch,
                            ObjectClass::FileExtentSegment,
                            StreamKind::FILE_EXTENTS,
                        )
                        .await?,
                    );
                }
                tail.as_mut()
                    .expect("extent tail is open")
                    .write(&extent)
                    .await?;
            }
            logical_length = logical_length.checked_add(chunk_length).ok_or_else(|| {
                Error::new(
                    ErrorKind::Invalid,
                    "partition Managed file with FastCDC",
                    "file length overflows",
                )
            })?;
        }

        let observed = ContentRef::new(
            Digest::from_bytes(logical_hasher.finalize().into()),
            logical_length,
        );
        if observed != content {
            return Err(Error::new(
                ErrorKind::Conflict,
                "publish Managed FastCDC file",
                "source changed while being published",
            ));
        }
        match (first, tail) {
            (None, None) => Ok(FileDataRef::empty()),
            (Some(first), None) => Ok(FileDataRef::single(first)),
            (Some(first), Some(tail)) => FileDataRef::with_tail(first, tail.close().await?),
            (None, Some(_)) => unreachable!("an extent tail requires an inline first extent"),
        }
    }

    async fn read(
        &self,
        context: &AccessContext,
        reference: FileDataRef,
        content: ContentRef,
        range: Range<u64>,
        destination: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        if range.start > range.end || range.end > content.length() {
            return Err(Error::new(
                ErrorKind::Invalid,
                "read Managed FastCDC file",
                "logical byte range is invalid",
            ));
        }
        let mut extents = reference.extents(context.operator()).await?;
        let mut expected_offset = 0_u64;
        let decoding_count = self.inner.info().len();
        while let Some(extent) = extents.next().await? {
            validate_extent(&extent, decoding_count)?;
            let extent_content = extent_content(&extent)?;
            let offset = expected_offset;
            let extent_end = expected_offset
                .checked_add(extent_content.length())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Corrupt,
                        "read Managed FastCDC file",
                        "extent range overflows",
                    )
                })?;
            if offset < range.end && range.start < extent_end {
                let start = range.start.saturating_sub(offset);
                let end = range.end.min(extent_end) - offset;
                self.inner
                    .read(context, extent, start..end, destination)
                    .await?;
            }
            expected_offset = extent_end;
            if expected_offset >= range.end && range.end < content.length() {
                return Ok(());
            }
        }
        if expected_offset != content.length() {
            return Err(Error::new(
                ErrorKind::Corrupt,
                "read Managed FastCDC file",
                "manifest does not cover the logical file",
            ));
        }
        Ok(())
    }
}

fn validate_extent(extent: &ExtentRef, decoding_count: usize) -> Result<(), Error> {
    let length = extent_content(extent)?.length();
    if extent.decoded.len() != decoding_count
        || length == 0
        || length > u64::from(MAXIMUM_CHUNK_BYTES)
    {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "read Managed FastCDC manifest",
            "extent ordering or length is invalid",
        ));
    }
    Ok(())
}

fn extent_content(extent: &ExtentRef) -> Result<ContentRef, Error> {
    Ok(extent.content())
}
