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
use crate::filesystem::{Checksum, Digest};

const OBJECT_PREFIX: &str = "managed/1/objects/";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_fixed_bytes(deserializer).map(Self)
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
    NamespaceSegment,
    OperationResultSegment,
    FileData,
}

impl ObjectClass {
    pub(crate) const ALL: [Self; 4] = [
        Self::NamespaceCommit,
        Self::NamespaceSegment,
        Self::OperationResultSegment,
        Self::FileData,
    ];

    pub(crate) const fn key_segment(self) -> &'static str {
        match self {
            Self::NamespaceCommit => "namespace-commit",
            Self::NamespaceSegment => "namespace-segment",
            Self::OperationResultSegment => "operation-result-segment",
            Self::FileData => "file-data",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|class| class.key_segment() == value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObjectRef {
    pub(crate) gc_epoch: GcEpoch,
    pub(crate) class: ObjectClass,
    pub(crate) id: ObjectId,
    pub(crate) encoded_length: u64,
    pub(crate) digest: Digest,
}

super::wire::tuple_wire!(ObjectRef {
    gc_epoch: GcEpoch,
    class: ObjectClass,
    id: ObjectId,
    encoded_length: u64,
    digest: Digest,
});

impl ObjectRef {
    pub(crate) fn key(self) -> String {
        object_key(self.gc_epoch, self.class, self.id)
    }
}

pub(crate) fn checksum(bytes: &[u8]) -> Checksum {
    Checksum::from_bytes(blake3::hash(bytes).into())
}

pub(crate) struct ControlObject {
    pub(crate) bytes: Vec<u8>,
    pub(crate) revision: String,
}

pub(crate) enum ControlCondition<'a> {
    Missing,
    Revision(&'a str),
}

pub(crate) async fn read_control(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
) -> Result<Option<ControlObject>, Error> {
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
    Ok(Some(ControlObject { bytes, revision }))
}

pub(crate) async fn write_control(
    operator: &Operator,
    key: &str,
    bytes: Vec<u8>,
    condition: ControlCondition<'_>,
) -> Result<bool, Error> {
    let write = operator.write_with(key, bytes);
    let result = match condition {
        ControlCondition::Missing => write.if_not_exists(true).await,
        ControlCondition::Revision(revision) => write.if_match(revision).await,
    };
    match result {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(Error::from_storage("publish Managed control object", error)),
    }
}

pub(crate) struct ImmutableWriter {
    gc_epoch: GcEpoch,
    class: ObjectClass,
    id: ObjectId,
    writer: Writer,
    hasher: Hasher,
    encoded_length: u64,
}

impl ImmutableWriter {
    pub(crate) async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
    ) -> Result<Self, Error> {
        let id = ObjectId::generate();
        let key = object_key(gc_epoch, class, id);
        let writer = operator
            .writer_with(&key)
            .if_not_exists(true)
            .await
            .map_err(|error| Error::from_storage("open Managed object writer", error))?;
        Ok(Self {
            gc_epoch,
            class,
            id,
            writer,
            hasher: Hasher::new(),
            encoded_length: 0,
        })
    }

    pub(crate) async fn write(&mut self, bytes: Vec<u8>) -> Result<(), Error> {
        self.encoded_length = self
            .encoded_length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::invalid("write Managed object", "object length overflows"))?;
        self.hasher.update(&bytes);
        self.writer
            .write(Buffer::from(bytes))
            .await
            .map_err(|error| Error::from_storage("write Managed object", error))
    }

    pub(crate) async fn abort(mut self) -> Result<(), Error> {
        self.writer
            .abort()
            .await
            .map_err(|error| Error::from_storage("abort Managed object", error))
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
            digest: Digest::from_bytes(self.hasher.finalize().into()),
        })
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

pub(crate) fn object_key(gc_epoch: GcEpoch, class: ObjectClass, id: ObjectId) -> String {
    let prefix = id.as_bytes()[0];
    format!(
        "{OBJECT_PREFIX}{}/{}/{prefix:02x}/{id}",
        gc_epoch.value(),
        class.key_segment(),
    )
}

fn deserialize_fixed_bytes<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct FixedBytesVisitor<const N: usize>;

    impl<const N: usize> serde::de::Visitor<'_> for FixedBytesVisitor<N> {
        type Value = [u8; N];

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a {N}-byte value")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            value
                .try_into()
                .map_err(|_| E::invalid_length(value.len(), &self))
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_bytes(&value)
        }
    }

    deserializer.deserialize_bytes(FixedBytesVisitor::<N>)
}
