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
use ofs_core::filesystem::{Digest, FileFingerprint};
use ofs_core::managed::extension::{
    AccessContext, ExtensionFileRef, ExtensionFormat, ExtensionId, ExtentAccess, ExtentRef,
    FileAccess, FileAccessInfo, FileLayoutExtension, RecordStreamReader, RecordStreamWriter,
    StreamKind,
};
use ofs_core::managed::{GcEpoch, ObjectClass, ObjectLocator};
use ofs_core::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt as _;

/// Stable wire identity of the FastCDC layout extension.
pub const FASTCDC_EXTENSION_ID: ExtensionId = ExtensionId::new(*b"ofs.fastcdc.v1!!");
const MANIFEST_KIND: StreamKind = match StreamKind::extension(1024) {
    Some(kind) => kind,
    None => panic!("FastCDC stream kind must be in the extension range"),
};

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

impl<A: ExtentAccess> FileLayoutExtension<A> for FastCdcExtension {
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ManifestExtent(u64, ExtentRef);

impl<A: ExtentAccess> FileAccess for FastCdcAccess<A> {
    fn info(&self) -> FileAccessInfo {
        FileAccessInfo {
            layout: ExtensionFormat {
                id: FASTCDC_EXTENSION_ID,
                name: "fastcdc-v2020".to_owned(),
                revision: 1,
                configuration: Configuration::CURRENT.encode(),
            },
            extents: self.inner.info(),
        }
    }

    async fn write(
        &self,
        context: &AccessContext,
        source: &mut (dyn AsyncRead + Send + Unpin),
        fingerprint: FileFingerprint,
        gc_epoch: GcEpoch,
    ) -> Result<ExtensionFileRef, Error> {
        let mut manifest = RecordStreamWriter::open(
            context.operator(),
            gc_epoch,
            ObjectClass::Extension,
            MANIFEST_KIND,
        )
        .await?;
        let mut chunker = AsyncStreamCDC::new(
            source,
            MINIMUM_CHUNK_BYTES,
            AVERAGE_CHUNK_BYTES,
            MAXIMUM_CHUNK_BYTES,
        );
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
            let chunk_fingerprint = FileFingerprint::new(
                Digest::from_bytes(blake3::hash(&chunk.data).into()),
                chunk_length,
            );
            let mut bytes = chunk.data.as_slice();
            let extent = self.inner.write(context, &mut bytes, gc_epoch).await?;
            if extent.fingerprint() != Some(chunk_fingerprint) {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "publish Managed FastCDC extent",
                    "extent access returned a different fingerprint",
                ));
            }
            manifest
                .write(&ManifestExtent(logical_length, extent))
                .await?;
            logical_length = logical_length.checked_add(chunk_length).ok_or_else(|| {
                Error::new(
                    ErrorKind::Invalid,
                    "partition Managed file with FastCDC",
                    "file length overflows",
                )
            })?;
        }

        let observed = FileFingerprint::new(
            Digest::from_bytes(logical_hasher.finalize().into()),
            logical_length,
        );
        if observed != fingerprint {
            return Err(Error::new(
                ErrorKind::Conflict,
                "publish Managed FastCDC file",
                "source changed while being published",
            ));
        }
        Ok(ExtensionFileRef {
            layout: FASTCDC_EXTENSION_ID,
            root: manifest.close().await?,
        })
    }

    async fn read(
        &self,
        context: &AccessContext,
        reference: ExtensionFileRef,
        fingerprint: FileFingerprint,
        range: Range<u64>,
        destination: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        require_reference(reference)?;
        if range.start > range.end || range.end > fingerprint.logical_length() {
            return Err(Error::new(
                ErrorKind::Invalid,
                "read Managed FastCDC file",
                "logical byte range is invalid",
            ));
        }
        let mut manifest =
            RecordStreamReader::<ManifestExtent>::open(context.operator(), reference.root).await?;
        let mut expected_offset = 0_u64;
        while let Some(ManifestExtent(offset, extent)) = manifest.next().await? {
            validate_extent(offset, expected_offset, &extent)?;
            let extent_fingerprint = extent_fingerprint(&extent)?;
            let extent_end = offset
                .checked_add(extent_fingerprint.logical_length())
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
        }
        if expected_offset != fingerprint.logical_length() {
            return Err(Error::new(
                ErrorKind::Corrupt,
                "read Managed FastCDC file",
                "manifest does not cover the logical file",
            ));
        }
        Ok(())
    }

    async fn visit_reachable(
        &self,
        context: &AccessContext,
        reference: ExtensionFileRef,
        visit: &mut (dyn FnMut(ObjectLocator) -> Result<(), Error> + Send),
    ) -> Result<(), Error> {
        require_reference(reference)?;
        visit(reference.root.object.locator)?;
        let mut manifest =
            RecordStreamReader::<ManifestExtent>::open(context.operator(), reference.root).await?;
        let mut expected_offset = 0_u64;
        while let Some(ManifestExtent(offset, extent)) = manifest.next().await? {
            validate_extent(offset, expected_offset, &extent)?;
            let extent_fingerprint = extent_fingerprint(&extent)?;
            expected_offset = offset
                .checked_add(extent_fingerprint.logical_length())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Corrupt,
                        "visit Managed FastCDC file",
                        "extent range overflows",
                    )
                })?;
            visit(extent.stream.object.locator)?;
        }
        Ok(())
    }
}

fn require_reference(reference: ExtensionFileRef) -> Result<(), Error> {
    if reference.layout != FASTCDC_EXTENSION_ID
        || reference.root.kind != MANIFEST_KIND
        || reference.root.object.locator.class != ObjectClass::Extension
    {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "read Managed FastCDC manifest",
            "file reference has the wrong extension type",
        ));
    }
    Ok(())
}

fn validate_extent(offset: u64, expected_offset: u64, extent: &ExtentRef) -> Result<(), Error> {
    let length = extent_fingerprint(extent)?.logical_length();
    if offset != expected_offset || length == 0 || length > u64::from(MAXIMUM_CHUNK_BYTES) {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "read Managed FastCDC manifest",
            "extent ordering or length is invalid",
        ));
    }
    Ok(())
}

fn extent_fingerprint(extent: &ExtentRef) -> Result<FileFingerprint, Error> {
    extent.fingerprint().ok_or_else(|| {
        Error::new(
            ErrorKind::Corrupt,
            "read Managed FastCDC manifest",
            "extent encoding chain is empty",
        )
    })
}
