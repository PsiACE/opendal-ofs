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

//! Immutable Managed objects addressed independently from their contents.

use std::fmt;
use std::ops::Range;

use blake3::Hasher;
use opendal::{Buffer, ErrorKind as StorageErrorKind, Operator, Writer};
use serde::{Deserialize, Serialize};

use crate::Error;

const OBJECT_PREFIX: &str = "managed/1/objects/";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct ObjectId([u8; 16]);

impl ObjectId {
    pub(crate) fn generate() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }

    pub(crate) const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub(crate) const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

macro_rules! integrity_value {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub(crate) struct $name([u8; 32]);

        impl $name {
            pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            pub(crate) const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }
        }
    };
}

integrity_value!(ObjectDigest);
integrity_value!(PayloadDigest);
integrity_value!(RangeChecksum);

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct GcEpoch(u64);

impl GcEpoch {
    pub(crate) const ZERO: Self = Self(0);

    pub(crate) const fn value(self) -> u64 {
        self.0
    }

    pub(crate) const fn from_value(value: u64) -> Self {
        Self(value)
    }

    pub(crate) fn next(self) -> Result<Self, Error> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| Error::corrupt("rotate Managed GC epoch", "GC epoch overflows"))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ObjectClass {
    NamespaceCommit,
    NodeSegment,
    DirectorySegment,
    FileVersionSegment,
    ChangeSegment,
    OperationResultSegment,
    FileExtentSegment,
    FileManifest,
    FileShard,
    ProjectionManifest,
    ProjectionSegment,
    ExtensionManifest,
    ExtensionData,
}

impl ObjectClass {
    pub(crate) const fn key_segment(self) -> &'static str {
        match self {
            Self::NamespaceCommit => "namespace-commit",
            Self::NodeSegment => "node-segment",
            Self::DirectorySegment => "directory-segment",
            Self::FileVersionSegment => "file-version-segment",
            Self::ChangeSegment => "change-segment",
            Self::OperationResultSegment => "operation-result-segment",
            Self::FileExtentSegment => "file-extent-segment",
            Self::FileManifest => "file-manifest",
            Self::FileShard => "file-shard",
            Self::ProjectionManifest => "projection-manifest",
            Self::ProjectionSegment => "projection-segment",
            Self::ExtensionManifest => "extension-manifest",
            Self::ExtensionData => "extension-data",
        }
    }

    pub(crate) const fn code(self) -> u8 {
        match self {
            Self::NamespaceCommit => 0,
            Self::NodeSegment => 1,
            Self::DirectorySegment => 2,
            Self::FileVersionSegment => 3,
            Self::ChangeSegment => 4,
            Self::OperationResultSegment => 5,
            Self::FileExtentSegment => 6,
            Self::FileManifest => 7,
            Self::FileShard => 8,
            Self::ProjectionManifest => 9,
            Self::ProjectionSegment => 10,
            Self::ExtensionManifest => 11,
            Self::ExtensionData => 12,
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        [
            Self::NamespaceCommit,
            Self::NodeSegment,
            Self::DirectorySegment,
            Self::FileVersionSegment,
            Self::ChangeSegment,
            Self::OperationResultSegment,
            Self::FileExtentSegment,
            Self::FileManifest,
            Self::FileShard,
            Self::ProjectionManifest,
            Self::ProjectionSegment,
            Self::ExtensionManifest,
            Self::ExtensionData,
        ]
        .into_iter()
        .find(|class| class.key_segment() == value)
    }

    pub(crate) const fn from_code(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::NamespaceCommit),
            1 => Some(Self::NodeSegment),
            2 => Some(Self::DirectorySegment),
            3 => Some(Self::FileVersionSegment),
            4 => Some(Self::ChangeSegment),
            5 => Some(Self::OperationResultSegment),
            6 => Some(Self::FileExtentSegment),
            7 => Some(Self::FileManifest),
            8 => Some(Self::FileShard),
            9 => Some(Self::ProjectionManifest),
            10 => Some(Self::ProjectionSegment),
            11 => Some(Self::ExtensionManifest),
            12 => Some(Self::ExtensionData),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ObjectRef {
    pub(crate) gc_epoch: GcEpoch,
    pub(crate) class: ObjectClass,
    pub(crate) id: ObjectId,
    pub(crate) encoded_length: u64,
    pub(crate) digest: ObjectDigest,
}

impl ObjectRef {
    pub(crate) fn key(self) -> String {
        object_key(self.gc_epoch, self.class, self.id)
    }
}

pub(crate) fn checksum(bytes: &[u8]) -> RangeChecksum {
    RangeChecksum::from_bytes(blake3::hash(bytes).into())
}

pub(crate) async fn read_control(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
) -> Result<Option<Vec<u8>>, Error> {
    let reader = match operator.reader(key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed control object", error)),
    };
    let bytes = match reader.read(..).await {
        Ok(bytes) => bytes.to_vec(),
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed control object", error)),
    };
    if bytes.len() > maximum_bytes {
        return Err(Error::corrupt(
            "read Managed control object",
            "control object exceeds its size limit",
        ));
    }
    Ok(Some(bytes))
}

pub(crate) async fn read_control_with_revision(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
) -> Result<Option<(Vec<u8>, String)>, Error> {
    let reader = match operator.reader(key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed control object", error)),
    };
    let bytes = match reader.read(..).await {
        Ok(bytes) => bytes.to_vec(),
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed control object", error)),
    };
    if bytes.len() > maximum_bytes {
        return Err(Error::corrupt(
            "read Managed control object",
            "control object exceeds its size limit",
        ));
    }
    let revision = reader
        .metadata()
        .and_then(|metadata| metadata.etag())
        .ok_or_else(|| {
            Error::unsupported(
                "read Managed control object",
                "object revision is unavailable",
            )
        })?
        .to_owned();
    Ok(Some((bytes, revision)))
}

pub(crate) async fn create_control(
    operator: &Operator,
    key: &str,
    bytes: Vec<u8>,
) -> Result<bool, Error> {
    match operator.write_with(key, bytes).if_not_exists(true).await {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(Error::from_storage("write Managed control object", error)),
    }
}

pub(crate) async fn replace_control(
    operator: &Operator,
    key: &str,
    expected_revision: &str,
    bytes: Vec<u8>,
) -> Result<bool, Error> {
    match operator
        .write_with(key, bytes)
        .if_match(expected_revision)
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == StorageErrorKind::ConditionNotMatch => Ok(false),
        Err(error) => Err(Error::from_storage("publish Managed control object", error)),
    }
}

pub(crate) async fn write_immutable(
    operator: &Operator,
    gc_epoch: GcEpoch,
    class: ObjectClass,
    bytes: Vec<u8>,
) -> Result<ObjectRef, Error> {
    let id = ObjectId::generate();
    let encoded_length = u64::try_from(bytes.len())
        .map_err(|_| Error::invalid("write Managed object", "object length overflows"))?;
    let digest = ObjectDigest::from_bytes(blake3::hash(&bytes).into());
    let reference = ObjectRef {
        gc_epoch,
        class,
        id,
        encoded_length,
        digest,
    };
    let key = reference.key();
    match operator
        .write_with(&key, Buffer::from(bytes))
        .if_not_exists(true)
        .await
    {
        Ok(_) => Ok(reference),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Err(Error::corrupt(
                "write Managed object",
                "generated object identity already exists",
            ))
        }
        Err(error) => Err(Error::from_storage("write Managed object", error)),
    }
}

pub(crate) struct ImmutableWriter {
    gc_epoch: GcEpoch,
    class: ObjectClass,
    id: ObjectId,
    key: String,
    writer: Writer,
    hasher: Hasher,
    encoded_length: u64,
}

impl ImmutableWriter {
    pub(crate) async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
        chunk_bytes: usize,
    ) -> Result<Self, Error> {
        let id = ObjectId::generate();
        let key = object_key(gc_epoch, class, id);
        let writer = operator
            .writer_with(&key)
            .if_not_exists(true)
            .chunk(chunk_bytes)
            .await
            .map_err(|error| Error::from_storage("open Managed object writer", error))?;
        Ok(Self {
            gc_epoch,
            class,
            id,
            key,
            writer,
            hasher: Hasher::new(),
            encoded_length: 0,
        })
    }

    pub(crate) async fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.encoded_length = self
            .encoded_length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::invalid("write Managed object", "object length overflows"))?;
        self.hasher.update(bytes);
        self.writer
            .write(Buffer::from(bytes.to_vec()))
            .await
            .map_err(|error| Error::from_storage("write Managed object", error))
    }

    pub(crate) async fn close(mut self) -> Result<ObjectRef, Error> {
        self.writer
            .close()
            .await
            .map_err(|error| Error::from_storage("finish Managed object", error))?;
        Ok(ObjectRef {
            gc_epoch: self.gc_epoch,
            class: self.class,
            id: self.id,
            encoded_length: self.encoded_length,
            digest: ObjectDigest::from_bytes(self.hasher.finalize().into()),
        })
    }

    pub(crate) async fn abort(mut self) {
        let _ = self.writer.abort().await;
        let _ = self.key;
    }
}

pub(crate) async fn read_immutable(
    operator: &Operator,
    reference: ObjectRef,
    maximum_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let length = usize::try_from(reference.encoded_length)
        .ok()
        .filter(|length| *length <= maximum_bytes)
        .ok_or_else(|| Error::corrupt("read Managed object", "object length is invalid"))?;
    let key = reference.key();
    let bytes = operator
        .read(&key)
        .await
        .map_err(|error| missing_object("read Managed object", error))?
        .to_vec();
    if bytes.len() != length || blake3::hash(&bytes).as_bytes() != reference.digest.as_bytes() {
        return Err(Error::corrupt(
            "read Managed object",
            "object does not match its reference",
        ));
    }
    Ok(bytes)
}

pub(crate) async fn read_range(
    operator: &Operator,
    reference: ObjectRef,
    range: Range<u64>,
) -> Result<Vec<u8>, Error> {
    if range.start > range.end || range.end > reference.encoded_length {
        return Err(Error::corrupt(
            "read Managed object range",
            "object range is invalid",
        ));
    }
    let expected = usize::try_from(range.end - range.start)
        .map_err(|_| Error::corrupt("read Managed object range", "range length overflows"))?;
    let key = reference.key();
    let bytes = operator
        .read_with(&key)
        .range(range)
        .await
        .map_err(|error| missing_object("read Managed object range", error))?
        .to_vec();
    if bytes.len() != expected {
        return Err(Error::unavailable(
            "read Managed object range",
            "object storage returned an incomplete range",
        ));
    }
    Ok(bytes)
}

fn missing_object(operation: &'static str, error: opendal::Error) -> Error {
    if error.kind() == StorageErrorKind::NotFound {
        Error::corrupt(operation, "referenced object is missing")
    } else {
        Error::from_storage(operation, error)
    }
}

fn object_key(gc_epoch: GcEpoch, class: ObjectClass, id: ObjectId) -> String {
    let prefix = id.as_bytes()[0];
    format!(
        "{OBJECT_PREFIX}{}/{}/{prefix:02x}/{id}",
        gc_epoch.value(),
        class.key_segment(),
    )
}
