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

//! Typed composition points for Managed file-layout and extent extensions.

use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use opendal::Operator;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::Error;
use crate::filesystem::FileFingerprint;

use super::object::{GcEpoch, ObjectClass, ObjectLocator};
use super::stream;

/// Stable identity of one persisted extension type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExtensionId([u8; 16]);

impl ExtensionId {
    /// Identity encoding used by the core extent access.
    pub const IDENTITY: Self = Self(*b"ofs.identity.v1!");

    /// Construct a stable identifier from its 16-byte registered value.
    pub const fn new(value: [u8; 16]) -> Self {
        Self(value)
    }

    /// Return the registered bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for ExtensionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for ExtensionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for ExtensionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IdVisitor;

        impl serde::de::Visitor<'_> for IdVisitor {
            type Value = ExtensionId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a 16-byte extension identifier")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                value
                    .try_into()
                    .map(ExtensionId)
                    .map_err(|_| E::invalid_length(value.len(), &self))
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_bytes(&value)
            }
        }

        deserializer.deserialize_bytes(IdVisitor)
    }
}

/// Self-description stored in the volume format for one active extension.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionFormat {
    /// Stable wire identity.
    pub id: ExtensionId,
    /// Domain name intended for diagnostics and format inspection.
    pub name: String,
    /// Revision of this extension's own records.
    pub revision: u16,
    /// Canonical CBOR configuration interpreted by this extension.
    pub configuration: Vec<u8>,
}

super::wire::tuple_wire!(ExtensionFormat {
    id: ExtensionId,
    name: String,
    revision: u16,
    configuration: Vec<u8>,
});

/// File-layout and extent-encoding description for one volume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileAccessInfo {
    /// Extension that maps logical file offsets to extents.
    pub layout: ExtensionFormat,
    /// Ordered encoding extensions applied independently to each extent.
    pub extents: Vec<ExtensionFormat>,
}

super::wire::tuple_wire!(FileAccessInfo {
    layout: ExtensionFormat,
    extents: Vec<ExtensionFormat>,
});

/// One step in the ordered decoding chain of an extent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtentEncoding {
    /// Extension that decodes this step.
    pub id: ExtensionId,
    /// Fingerprint after this step is decoded.
    pub fingerprint: FileFingerprint,
}

super::wire::tuple_wire!(ExtentEncoding {
    id: ExtensionId,
    fingerprint: FileFingerprint,
});

/// Reference to one independently readable logical extent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentRef {
    /// Decoding steps from the logical representation to the stored stream.
    pub encodings: Vec<ExtentEncoding>,
    /// Encoded immutable stream.
    pub stream: StreamRef,
}

super::wire::tuple_wire!(ExtentRef {
    encodings: Vec<ExtentEncoding>,
    stream: StreamRef,
});

impl ExtentRef {
    /// Fingerprint visible to the caller of the outermost encoding.
    pub fn fingerprint(&self) -> Option<FileFingerprint> {
        self.encodings.first().map(|encoding| encoding.fingerprint)
    }

    /// Consume one expected outer encoding and return the inner reference.
    pub fn into_inner(mut self, id: ExtensionId) -> Result<(FileFingerprint, Self), Error> {
        let Some(encoding) = self.encodings.first().copied() else {
            return Err(Error::new(
                crate::ErrorKind::Corrupt,
                "read Managed extent",
                "extent encoding chain is empty",
            ));
        };
        if encoding.id != id {
            return Err(Error::new(
                crate::ErrorKind::Corrupt,
                "read Managed extent",
                "extent encoding does not match the configured extension",
            ));
        }
        self.encodings.remove(0);
        Ok((encoding.fingerprint, self))
    }
}

/// Root reference emitted by a file-layout extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtensionFileRef {
    /// Layout that interprets the root stream.
    pub layout: ExtensionId,
    /// Self-delimiting layout stream, normally a manifest.
    pub root: StreamRef,
}

super::wire::tuple_wire!(ExtensionFileRef {
    layout: ExtensionId,
    root: StreamRef,
});

/// Storage and concurrency context shared by extension accesses.
#[derive(Clone, Debug)]
pub struct AccessContext {
    operator: Operator,
}

impl AccessContext {
    pub(crate) fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Return the OpenDAL operator configured by the storage boundary.
    pub const fn operator(&self) -> &Operator {
        &self.operator
    }
}

/// Boxed future used only after a typed access is erased at the volume edge.
pub type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Immutable extent access. Encoding extensions wrap this boundary.
pub trait ExtentAccess: Send + Sync + fmt::Debug + Unpin + 'static {
    /// Describe the persisted extent encoding.
    fn info(&self) -> Vec<ExtensionFormat>;

    /// Write one bounded extent from a byte stream.
    fn write<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        gc_epoch: GcEpoch,
    ) -> impl Future<Output = Result<ExtentRef, Error>> + Send + 'a;

    /// Decode one logical extent range into the destination stream.
    fn read<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtentRef,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;
}

/// Typed extension over an extent access.
pub trait ExtentExtension<A: ExtentAccess> {
    /// Resulting statically composed access.
    type ExtendedAccess: ExtentAccess;

    /// Wrap the inner access.
    fn extend(&self, inner: A) -> Self::ExtendedAccess;
}

/// Forwarding surface for extent extensions.
pub trait ExtendedExtentAccess: Send + Sync + fmt::Debug + Unpin + 'static {
    /// Wrapped extent access.
    type Inner: ExtentAccess;

    /// Return the wrapped access.
    fn inner(&self) -> &Self::Inner;

    /// Forward the extent description by default.
    fn info(&self) -> Vec<ExtensionFormat> {
        self.inner().info()
    }

    /// Forward extent publication by default.
    fn write<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        gc_epoch: GcEpoch,
    ) -> impl Future<Output = Result<ExtentRef, Error>> + Send + 'a {
        self.inner().write(context, source, gc_epoch)
    }

    /// Forward extent reads by default.
    fn read<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtentRef,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        self.inner().read(context, reference, range, destination)
    }
}

impl<T: ExtendedExtentAccess> ExtentAccess for T {
    fn info(&self) -> Vec<ExtensionFormat> {
        ExtendedExtentAccess::info(self)
    }

    fn write<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        gc_epoch: GcEpoch,
    ) -> impl Future<Output = Result<ExtentRef, Error>> + Send + 'a {
        ExtendedExtentAccess::write(self, context, source, gc_epoch)
    }

    fn read<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtentRef,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        ExtendedExtentAccess::read(self, context, reference, range, destination)
    }
}

/// Logical file access. Layout extensions are built over an extent access.
pub trait FileAccess: Send + Sync + fmt::Debug + Unpin + 'static {
    /// Describe the persisted layout and extent encoding.
    fn info(&self) -> FileAccessInfo;

    /// Publish one logical file.
    fn write<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        fingerprint: FileFingerprint,
        gc_epoch: GcEpoch,
    ) -> impl Future<Output = Result<ExtensionFileRef, Error>> + Send + 'a;

    /// Read one logical byte range.
    fn read<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        fingerprint: FileFingerprint,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;

    /// Stream every immutable object reachable through a file root.
    fn visit_reachable<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        visit: &'a mut (dyn FnMut(ObjectLocator) -> Result<(), Error> + Send),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;
}

/// Typed file-layout extension over an extent access.
pub trait FileLayoutExtension<A: ExtentAccess> {
    /// Resulting statically composed file access.
    type ExtendedAccess: FileAccess;

    /// Build the layout around the extent access.
    fn extend(&self, inner: A) -> Self::ExtendedAccess;
}

/// Cross-cutting extension over complete logical-file access.
///
/// Flow control and observability extensions use this boundary so publication,
/// range reads, installation, and collection traversal share one call chain.
pub trait FileAccessExtension<A: FileAccess> {
    /// Resulting statically composed access.
    type ExtendedAccess: FileAccess;

    /// Wrap the complete logical-file access.
    fn extend(&self, inner: A) -> Self::ExtendedAccess;
}

/// Default forwarding surface for cross-cutting file access extensions.
pub trait ExtendedFileAccess: Send + Sync + fmt::Debug + Unpin + 'static {
    /// Wrapped logical-file access.
    type Inner: FileAccess;

    /// Return the wrapped access.
    fn inner(&self) -> &Self::Inner;

    /// Forward the persisted access description by default.
    fn info(&self) -> FileAccessInfo {
        self.inner().info()
    }

    /// Forward publication by default.
    fn write<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        fingerprint: FileFingerprint,
        gc_epoch: GcEpoch,
    ) -> impl Future<Output = Result<ExtensionFileRef, Error>> + Send + 'a {
        self.inner().write(context, source, fingerprint, gc_epoch)
    }

    /// Forward logical range reads by default.
    fn read<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        fingerprint: FileFingerprint,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        self.inner()
            .read(context, reference, fingerprint, range, destination)
    }

    /// Forward collection reachability traversal by default.
    fn visit_reachable<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        visit: &'a mut (dyn FnMut(ObjectLocator) -> Result<(), Error> + Send),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        self.inner().visit_reachable(context, reference, visit)
    }
}

impl<T: ExtendedFileAccess> FileAccess for T {
    fn info(&self) -> FileAccessInfo {
        ExtendedFileAccess::info(self)
    }

    fn write<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        fingerprint: FileFingerprint,
        gc_epoch: GcEpoch,
    ) -> impl Future<Output = Result<ExtensionFileRef, Error>> + Send + 'a {
        ExtendedFileAccess::write(self, context, source, fingerprint, gc_epoch)
    }

    fn read<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        fingerprint: FileFingerprint,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        ExtendedFileAccess::read(self, context, reference, fingerprint, range, destination)
    }

    fn visit_reachable<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        visit: &'a mut (dyn FnMut(ObjectLocator) -> Result<(), Error> + Send),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        ExtendedFileAccess::visit_reachable(self, context, reference, visit)
    }
}

/// Object-safe file access used only by `ManagedVolume`.
pub trait FileAccessDyn: Send + Sync + fmt::Debug + Unpin {
    /// Dyn form of [`FileAccess::info`].
    fn info_dyn(&self) -> FileAccessInfo;
    /// Dyn form of [`FileAccess::write`].
    fn write_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        fingerprint: FileFingerprint,
        gc_epoch: GcEpoch,
    ) -> BoxedFuture<'a, Result<ExtensionFileRef, Error>>;
    /// Dyn form of [`FileAccess::read`].
    fn read_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        fingerprint: FileFingerprint,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> BoxedFuture<'a, Result<(), Error>>;
    /// Dyn form of [`FileAccess::visit_reachable`].
    fn visit_reachable_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        visit: &'a mut (dyn FnMut(ObjectLocator) -> Result<(), Error> + Send),
    ) -> BoxedFuture<'a, Result<(), Error>>;
}

impl<A: FileAccess> FileAccessDyn for A {
    fn info_dyn(&self) -> FileAccessInfo {
        self.info()
    }

    fn write_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        fingerprint: FileFingerprint,
        gc_epoch: GcEpoch,
    ) -> BoxedFuture<'a, Result<ExtensionFileRef, Error>> {
        Box::pin(self.write(context, source, fingerprint, gc_epoch))
    }

    fn read_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        fingerprint: FileFingerprint,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> BoxedFuture<'a, Result<(), Error>> {
        Box::pin(self.read(context, reference, fingerprint, range, destination))
    }

    fn visit_reachable_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        visit: &'a mut (dyn FnMut(ObjectLocator) -> Result<(), Error> + Send),
    ) -> BoxedFuture<'a, Result<(), Error>> {
        Box::pin(self.visit_reachable(context, reference, visit))
    }
}

/// Erase a statically composed file access at the volume boundary.
pub fn type_erase(access: impl FileAccess) -> Arc<dyn FileAccessDyn> {
    Arc::new(access)
}

/// Identity extent access backed by the common self-delimiting stream format.
#[derive(Clone, Copy, Debug, Default)]
pub struct IdentityExtentAccess;

impl ExtentAccess for IdentityExtentAccess {
    fn info(&self) -> Vec<ExtensionFormat> {
        vec![ExtensionFormat {
            id: ExtensionId::IDENTITY,
            name: "identity".to_owned(),
            revision: 1,
            configuration: Vec::new(),
        }]
    }

    async fn write(
        &self,
        context: &AccessContext,
        source: &mut (dyn AsyncRead + Send + Unpin),
        gc_epoch: GcEpoch,
    ) -> Result<ExtentRef, Error> {
        let stream = stream::write_unchecked_byte_stream(
            context.operator(),
            gc_epoch,
            ObjectClass::Extension,
            source,
        )
        .await?;
        let fingerprint = FileFingerprint::new(stream.payload_digest, stream.payload_length);
        Ok(ExtentRef {
            encodings: vec![ExtentEncoding {
                id: ExtensionId::IDENTITY,
                fingerprint,
            }],
            stream,
        })
    }

    async fn read(
        &self,
        context: &AccessContext,
        reference: ExtentRef,
        range: Range<u64>,
        destination: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        let (fingerprint, reference) = reference.into_inner(ExtensionId::IDENTITY)?;
        if !reference.encodings.is_empty()
            || reference.stream.object.locator.class != ObjectClass::Extension
            || reference.stream.payload_length != fingerprint.logical_length()
            || reference.stream.payload_digest != fingerprint.digest()
        {
            return Err(Error::corrupt(
                "read Managed extent",
                "extent reference does not use identity encoding",
            ));
        }
        stream::copy_byte_stream(context.operator(), reference.stream, range, destination).await
    }
}

pub use super::stream::{RecordStreamReader, RecordStreamWriter, StreamKind, StreamRef};
