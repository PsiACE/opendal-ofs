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

//! Identity and key layout of immutable Managed objects.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::{Checksum, Digest};

pub(crate) use super::storage::ImmutableWriter;

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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum ObjectClass {
    NamespaceCommit,
    NamespaceSegment,
    OperationResultSegment,
    FileData,
}

impl Serialize for ObjectClass {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(match self {
            Self::NamespaceCommit => 1,
            Self::NamespaceSegment => 2,
            Self::OperationResultSegment => 3,
            Self::FileData => 4,
        })
    }
}

impl<'de> Deserialize<'de> for ObjectClass {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match u8::deserialize(deserializer)? {
            1 => Ok(Self::NamespaceCommit),
            2 => Ok(Self::NamespaceSegment),
            3 => Ok(Self::OperationResultSegment),
            4 => Ok(Self::FileData),
            value => Err(serde::de::Error::custom(format_args!(
                "unknown object class {value}"
            ))),
        }
    }
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
