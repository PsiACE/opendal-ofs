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

//! Streaming Zstandard extent codec.

use std::ops::Range;

use async_compression::Level;
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use ofs_core::Error;
use ofs_core::ErrorKind;
use ofs_core::data::{ContentHasher, DataSegmentWriter, ExtentCodec, IdentityCodec, RangeReader};
use ofs_core::filesystem::ContentRef;
use ofs_core::format::{ExtensionDescriptor, ExtensionId, ExtentRef};
use tokio::io::{AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader};

/// Stable wire identity of the Zstandard extent encoding.
pub const ZSTD_EXTENSION_ID: ExtensionId = ExtensionId::from_bytes(*b"ofs.ext.zstd.v1!");

const STREAM_BUFFER_BYTES: usize = 256 * 1024;

/// Zstandard extent codec over identity stored bytes.
#[derive(Clone, Debug)]
pub struct ZstdCodec {
    level: i32,
    descriptor: ExtensionDescriptor,
}

impl ZstdCodec {
    pub fn new(level: i32) -> Result<Self, Error> {
        if level < zstd_safe::min_c_level() || level > zstd_safe::max_c_level() {
            return Err(Error::new(
                ErrorKind::Invalid,
                "configure Zstandard extension",
                "compression level is outside the range supported by Zstandard",
            ));
        }
        Ok(Self {
            level,
            descriptor: ExtensionDescriptor::encode(ZSTD_EXTENSION_ID, &level),
        })
    }

    pub fn from_descriptor(descriptor: &ExtensionDescriptor) -> Result<Self, Error> {
        let level = descriptor.decode(ZSTD_EXTENSION_ID)?;
        Self::new(level).map_err(|_| {
            Error::new(
                ErrorKind::Corrupt,
                "open Managed volume",
                "Zstandard compression level is unsupported",
            )
        })
    }
}

impl ExtentCodec for ZstdCodec {
    fn descriptor(&self) -> Option<&ExtensionDescriptor> {
        Some(&self.descriptor)
    }

    fn decoding_count(&self) -> usize {
        1
    }

    fn stored_size_bound(&self, logical_bytes: u64) -> Option<u64> {
        let logical_bytes = usize::try_from(logical_bytes).ok()?;
        u64::try_from(zstd_safe::compress_bound(logical_bytes)).ok()
    }

    fn stored_range(&self, reference: &ExtentRef, range: Range<u64>) -> Result<Range<u64>, Error> {
        let (content, inner) = decode_extent(reference.clone(), &range)?;
        let _ = content;
        IdentityCodec.stored_range(&inner, 0..inner.content().length())
    }

    async fn encode(
        &self,
        placement: &mut DataSegmentWriter<'_>,
        source: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> Result<ExtentRef, Error> {
        let mut content = ContentHasher::default();
        let mut reference = {
            let source = tokio_util::io::InspectReader::new(source, |bytes| content.observe(bytes));
            let mut encoded = ZstdEncoder::with_quality(
                BufReader::with_capacity(STREAM_BUFFER_BYTES, source),
                Level::Precise(self.level),
            );
            IdentityCodec.encode(placement, &mut encoded).await?
        };
        let content = content.complete_content().ok_or_else(|| {
            Error::new(
                ErrorKind::Unavailable,
                "write Zstandard extent",
                "extent source was not consumed completely",
            )
        })?;
        reference.decoding_outputs.push(content);
        Ok(reference)
    }

    async fn decode(
        &self,
        source: &mut RangeReader,
        reference: ExtentRef,
        range: Range<u64>,
        destination: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        let (content, inner) = decode_extent(reference, &range)?;
        let encoded = inner.content();
        let (encoded_reader, mut encoded_writer) = tokio::io::duplex(STREAM_BUFFER_BYTES);
        let feed = async {
            let result = IdentityCodec
                .decode(source, inner, 0..encoded.length(), &mut encoded_writer)
                .await;
            drop(encoded_writer);
            result
        };
        let decode = decode_range(encoded_reader, content, range, destination);
        tokio::try_join!(feed, decode)?;
        Ok(())
    }
}

fn decode_extent(
    reference: ExtentRef,
    range: &Range<u64>,
) -> Result<(ContentRef, ExtentRef), Error> {
    let (content, inner) = reference.into_inner()?;
    if range.start > range.end || range.end > content.length() {
        return Err(Error::new(
            ErrorKind::Invalid,
            "read Zstandard extent",
            "logical byte range is invalid",
        ));
    }
    Ok((content, inner))
}

async fn decode_range(
    encoded: tokio::io::DuplexStream,
    content: ContentRef,
    range: Range<u64>,
    destination: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Error> {
    let decoder = ZstdDecoder::new(BufReader::with_capacity(STREAM_BUFFER_BYTES, encoded));
    let mut content_hash = ContentHasher::default();
    let mut decoded =
        tokio_util::io::InspectReader::new(decoder, |bytes| content_hash.observe(bytes));
    let mut offset = 0_u64;
    let mut bytes = vec![0; STREAM_BUFFER_BYTES];
    loop {
        let read = decoded.read(&mut bytes).await.map_err(|error| {
            Error::new(
                ErrorKind::Corrupt,
                "read Zstandard extent",
                "extent payload cannot be decoded",
            )
            .with_source(error)
        })?;
        if read == 0 {
            break;
        }
        let end = offset.checked_add(read as u64).ok_or_else(|| {
            Error::new(
                ErrorKind::Corrupt,
                "read Zstandard extent",
                "decoded extent length overflows",
            )
        })?;
        let selected_start = range.start.max(offset);
        let selected_end = range.end.min(end);
        if selected_start < selected_end {
            let start = (selected_start - offset) as usize;
            let selected = (selected_end - offset) as usize;
            destination
                .write_all(&bytes[start..selected])
                .await
                .map_err(|error| {
                    Error::new(
                        ErrorKind::Unavailable,
                        "write decoded Zstandard extent",
                        "extent destination is unavailable",
                    )
                    .with_source(error)
                })?;
        }
        offset = end;
    }
    let observed = content_hash.complete_content().ok_or_else(|| {
        Error::new(
            ErrorKind::Corrupt,
            "read Zstandard extent",
            "decoded extent did not reach EOF",
        )
    })?;
    if observed != content {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "read Zstandard extent",
            "decoded extent does not match its content reference",
        ));
    }
    Ok(())
}
