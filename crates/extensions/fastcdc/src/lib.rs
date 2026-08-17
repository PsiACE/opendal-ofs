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

//! FastCDC file partitioner for Managed volumes.

use fastcdc::v2020::{
    AVERAGE_MAX, AVERAGE_MIN, AsyncStreamCDC, MAXIMUM_MAX, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN,
};
use futures::StreamExt as _;
use ofs_core::Error;
use ofs_core::ErrorKind;
use ofs_core::data::{
    ContentReuseLookup, DataSegmentWriter, ExtentCodec, ExtentRunWriter, FilePartitioner,
};
use ofs_core::filesystem::{ContentRef, Digest};
use ofs_core::format::{ExtensionDescriptor, ExtensionId, ExtentMapping, FileRange};
use tokio::io::AsyncReadExt as _;

/// Stable wire identity of the FastCDC layout extension.
pub const FASTCDC_EXTENSION_ID: ExtensionId = ExtensionId::from_bytes(*b"ofs.fastcdc.v1!!");

type Configuration = (u32, u32, u32);

/// FastCDC v2020 partitioner with a stable volume format.
#[derive(Clone, Debug)]
pub struct FastCdcPartitioner {
    configuration: Configuration,
    descriptor: ExtensionDescriptor,
}

impl FastCdcPartitioner {
    pub fn new(minimum: u32, average: u32, maximum: u32) -> Result<Self, Error> {
        if !(MINIMUM_MIN..=MINIMUM_MAX).contains(&minimum)
            || !(AVERAGE_MIN..=AVERAGE_MAX).contains(&average)
            || !(MAXIMUM_MIN..=MAXIMUM_MAX).contains(&maximum)
            || minimum > average
            || average > maximum
        {
            return Err(Error::new(
                ErrorKind::Invalid,
                "configure FastCDC extension",
                "chunk sizes are outside FastCDC's supported ranges or are not ordered minimum <= average <= maximum",
            ));
        }
        Ok(Self {
            configuration: (minimum, average, maximum),
            descriptor: ExtensionDescriptor::encode(
                FASTCDC_EXTENSION_ID,
                &(minimum, average, maximum),
            ),
        })
    }

    pub fn from_descriptor(descriptor: &ExtensionDescriptor) -> Result<Self, Error> {
        let (minimum, average, maximum) = descriptor.decode(FASTCDC_EXTENSION_ID)?;
        Self::new(minimum, average, maximum).map_err(|_| {
            Error::new(
                ErrorKind::Corrupt,
                "open Managed volume",
                "FastCDC chunk sizes are invalid",
            )
        })
    }
}

impl FilePartitioner for FastCdcPartitioner {
    fn descriptor(&self) -> Option<&ExtensionDescriptor> {
        Some(&self.descriptor)
    }

    fn maximum_extent_bytes(&self) -> Option<u64> {
        Some(u64::from(self.configuration.2))
    }

    async fn write_run<C: ExtentCodec>(
        &self,
        codec: &C,
        placement: &mut DataSegmentWriter<'_>,
        source: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        known: &ContentReuseLookup,
        file_offset: u64,
        logical_bytes: u64,
        run: &mut ExtentRunWriter<'_>,
    ) -> Result<ContentRef, Error> {
        let (minimum, average, maximum) = self.configuration;
        if logical_bytes <= u64::from(minimum) {
            let mut bytes = Vec::with_capacity(usize::try_from(logical_bytes).unwrap_or(0));
            source.read_to_end(&mut bytes).await.map_err(|error| {
                Error::new(
                    ErrorKind::Unavailable,
                    "partition Managed file with FastCDC",
                    "source stream cannot be partitioned",
                )
                .with_source(error)
            })?;
            if bytes.len() as u64 != logical_bytes {
                return Err(Error::new(
                    ErrorKind::Conflict,
                    "partition Managed file with FastCDC",
                    "source length changed while it was being published",
                ));
            }
            return write_one_extent(codec, placement, known, run, file_offset, &bytes).await;
        }
        let effective_maximum = maximum.min(
            u32::try_from(logical_bytes)
                .unwrap_or(u32::MAX)
                .max(average),
        );
        let mut chunker = AsyncStreamCDC::new(source, minimum, average, effective_maximum);
        let mut chunks = std::pin::pin!(chunker.as_stream());
        let mut logical_hasher = blake3::Hasher::new();
        let mut logical_length = 0_u64;
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
            let mapping_offset = file_offset.checked_add(logical_length).ok_or_else(|| {
                Error::new(
                    ErrorKind::Invalid,
                    "partition Managed file with FastCDC",
                    "file offset overflows",
                )
            })?;
            write_one_extent(codec, placement, known, run, mapping_offset, &chunk.data).await?;
            logical_length = logical_length.checked_add(chunk_length).ok_or_else(|| {
                Error::new(
                    ErrorKind::Invalid,
                    "partition Managed file with FastCDC",
                    "file length overflows",
                )
            })?;
        }
        if logical_length != logical_bytes {
            return Err(Error::new(
                ErrorKind::Conflict,
                "partition Managed file with FastCDC",
                "source length changed while it was being published",
            ));
        }
        Ok(ContentRef::new(
            Digest::from_bytes(logical_hasher.finalize().into()),
            logical_length,
        ))
    }
}

async fn write_one_extent<C: ExtentCodec>(
    codec: &C,
    placement: &mut DataSegmentWriter<'_>,
    known: &ContentReuseLookup,
    run: &mut ExtentRunWriter<'_>,
    file_offset: u64,
    bytes: &[u8],
) -> Result<ContentRef, Error> {
    let content = ContentRef::new(
        Digest::from_bytes(blake3::hash(bytes).into()),
        bytes.len() as u64,
    );
    if let Some(extent) = known.extent(content)? {
        if content.length() != 0 {
            run.write(ExtentMapping {
                logical_range: FileRange::new(file_offset, content.length())?,
                extent_offset: 0,
                extent,
            })
            .await?;
        }
        return Ok(content);
    }
    let mut source = bytes;
    let extent = codec.encode(placement, &mut source).await?;
    if extent.content() != content {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "publish Managed FastCDC extent",
            "codec returned different content",
        ));
    }
    if content.length() != 0 {
        run.write(ExtentMapping::complete(file_offset, extent)?)
            .await?;
    }
    Ok(content)
}
