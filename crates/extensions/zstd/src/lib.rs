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

//! Independent streaming Zstandard encoding for Managed extents.

use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_compression::Level;
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use ofs_core::filesystem::{ContentRef, Digest};
use ofs_core::managed::GcEpoch;
use ofs_core::managed::extension::{
    AccessContext, ExtendedExtentAccess, ExtensionFormat, ExtensionId, ExtentAccess,
    ExtentExtension, ExtentRef,
};
use ofs_core::{Error, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader, ReadBuf};

/// Stable wire identity of the Zstandard extent encoding.
pub const ZSTD_EXTENSION_ID: ExtensionId = ExtensionId::new(*b"ofs.ext.zstd.v1!");

const STREAM_BUFFER_BYTES: usize = 256 * 1024;

/// Zstandard extent extension configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZstdExtension {
    level: i32,
}

impl ZstdExtension {
    /// Create an extension using one Zstandard compression level.
    pub const fn new(level: i32) -> Self {
        Self { level }
    }

    /// Return the configured compression level.
    pub const fn level(self) -> i32 {
        self.level
    }
}

impl<A: ExtentAccess> ExtentExtension<A> for ZstdExtension {
    type ExtendedAccess = ZstdExtentAccess<A>;

    fn extend(&self, inner: A) -> Self::ExtendedAccess {
        ZstdExtentAccess {
            inner,
            level: self.level,
        }
    }
}

/// Extent access produced by [`ZstdExtension`].
#[derive(Clone, Debug)]
pub struct ZstdExtentAccess<A> {
    inner: A,
    level: i32,
}

impl<A: ExtentAccess> ExtendedExtentAccess for ZstdExtentAccess<A> {
    type Inner = A;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn info(&self) -> Vec<ExtensionFormat> {
        let mut configuration = Vec::new();
        ciborium::into_writer(&self.level, &mut configuration)
            .expect("encoding a Zstandard level into memory cannot fail");
        let mut extensions = self.inner.info();
        extensions.push(ExtensionFormat {
            id: ZSTD_EXTENSION_ID,
            configuration,
        });
        extensions
    }

    async fn write(
        &self,
        context: &AccessContext,
        source: &mut (dyn AsyncRead + Send + Unpin),
        gc_epoch: GcEpoch,
    ) -> Result<ExtentRef, Error> {
        let content_reader = ContentReader::new(source);
        let mut encoded = ZstdEncoder::with_quality(
            BufReader::with_capacity(STREAM_BUFFER_BYTES, content_reader),
            Level::Precise(self.level),
        );
        let mut reference = self.inner.write(context, &mut encoded, gc_epoch).await?;
        let content = encoded.get_ref().get_ref().content()?;
        reference.decoded.push(content);
        Ok(reference)
    }

    async fn read(
        &self,
        context: &AccessContext,
        reference: ExtentRef,
        range: Range<u64>,
        destination: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        let (content, inner) = reference.into_inner()?;
        if range.start > range.end || range.end > content.length() {
            return Err(Error::new(
                ErrorKind::Invalid,
                "read Zstandard extent",
                "logical byte range is invalid",
            ));
        }
        let encoded = inner.content();
        let (encoded_reader, mut encoded_writer) = tokio::io::duplex(STREAM_BUFFER_BYTES);
        let feed = async {
            let result = self
                .inner
                .read(context, inner, 0..encoded.length(), &mut encoded_writer)
                .await;
            drop(encoded_writer);
            result
        };
        let decode = decode_range(encoded_reader, content, range, destination);
        tokio::try_join!(feed, decode)?;
        Ok(())
    }
}

async fn decode_range(
    encoded: tokio::io::DuplexStream,
    content: ContentRef,
    range: Range<u64>,
    destination: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Error> {
    let mut decoder = ZstdDecoder::new(BufReader::with_capacity(STREAM_BUFFER_BYTES, encoded));
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    let mut bytes = vec![0; STREAM_BUFFER_BYTES];
    loop {
        let read = decoder.read(&mut bytes).await.map_err(|error| {
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
        hasher.update(&bytes[..read]);
        let selected_start = range.start.max(offset);
        let selected_end = range.end.min(end);
        if selected_start < selected_end {
            let start = (selected_start - offset) as usize;
            let end = (selected_end - offset) as usize;
            destination
                .write_all(&bytes[start..end])
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
    if offset != content.length()
        || Digest::from_bytes(hasher.finalize().into()) != content.digest()
    {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "read Zstandard extent",
            "decoded extent does not match its content",
        ));
    }
    Ok(())
}

struct ContentReader<'a> {
    inner: &'a mut (dyn AsyncRead + Send + Unpin),
    hasher: blake3::Hasher,
    length: u64,
    complete: bool,
}

impl<'a> ContentReader<'a> {
    fn new(inner: &'a mut (dyn AsyncRead + Send + Unpin)) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
            length: 0,
            complete: false,
        }
    }

    fn content(&self) -> Result<ContentRef, Error> {
        if !self.complete {
            return Err(Error::new(
                ErrorKind::Unavailable,
                "write Zstandard extent",
                "extent source was not consumed completely",
            ));
        }
        Ok(ContentRef::new(
            Digest::from_bytes(self.hasher.finalize().into()),
            self.length,
        ))
    }
}

impl AsyncRead for ContentReader<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        match Pin::new(&mut *self.inner).poll_read(context, buffer) {
            Poll::Ready(Ok(())) => {
                let read = &buffer.filled()[before..];
                if read.is_empty() {
                    self.complete = true;
                } else {
                    self.hasher.update(read);
                    self.length = self.length.checked_add(read.len() as u64).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "extent length overflows",
                        )
                    })?;
                }
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}
