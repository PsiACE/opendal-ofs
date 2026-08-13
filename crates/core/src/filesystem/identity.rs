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

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! fixed_identity {
    ($(#[$meta:meta])* $name:ident, $length:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; $length]);

        impl $name {
            pub const fn from_bytes(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_bytes(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct IdentityVisitor;

                impl serde::de::Visitor<'_> for IdentityVisitor {
                    type Value = [u8; $length];

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        write!(formatter, "a {}-byte identity", $length)
                    }

                    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        value.try_into().map_err(|_| {
                            E::invalid_length(value.len(), &self)
                        })
                    }

                    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        self.visit_bytes(&value)
                    }
                }

                deserializer.deserialize_bytes(IdentityVisitor).map(Self)
            }
        }
    };
}

fixed_identity!(
    /// Stable identity of one Managed volume.
    VolumeId,
    16
);
fixed_identity!(
    /// Stable identity of one filesystem node.
    NodeId,
    16
);
fixed_identity!(
    /// Content identity used by immutable Managed records and file data.
    Digest,
    32
);
fixed_identity!(
    /// Stable identity of one immutable logical file version.
    FileVersionId,
    16
);
fixed_identity!(
    /// Integrity value used to verify one independently readable record.
    Checksum,
    32
);
fixed_identity!(
    /// Idempotency identity of one publication attempt.
    OperationId,
    16
);

macro_rules! generated_identity {
    ($($name:ident),+ $(,)?) => {
        $(
            impl $name {
                pub fn generate() -> Self {
                    Self::from_bytes(*uuid::Uuid::new_v4().as_bytes())
                }
            }
        )+
    };
}

generated_identity!(VolumeId, NodeId, FileVersionId, OperationId);

/// Stable identity of one logical byte sequence.
///
/// The reference is independent of a file version, path, and physical layout,
/// so the same content can be compared and reused across namespace entries.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContentRef {
    digest: Digest,
    length: u64,
}

impl Serialize for ContentRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        (self.digest, self.length).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ContentRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (digest, length) = Deserialize::deserialize(deserializer)?;
        Ok(Self { digest, length })
    }
}

impl ContentRef {
    pub const fn new(digest: Digest, length: u64) -> Self {
        Self { digest, length }
    }

    pub const fn digest(self) -> Digest {
        self.digest
    }

    pub const fn length(self) -> u64 {
        self.length
    }
}

macro_rules! display_identity {
    ($($name:ident),+ $(,)?) => {
        $(
            impl fmt::Display for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    for byte in self.as_bytes() {
                        write!(formatter, "{byte:02x}")?;
                    }
                    Ok(())
                }
            }
        )+
    };
}

display_identity!(VolumeId, FileVersionId, OperationId);

/// A position in a Managed volume's ordered change stream.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChangeCursor(u64);

impl Serialize for ChangeCursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ChangeCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        u64::deserialize(deserializer).map(Self)
    }
}

impl ChangeCursor {
    pub const GENESIS: Self = Self(0);

    pub const fn sequence(self) -> u64 {
        self.0
    }

    pub const fn from_sequence(sequence: u64) -> Self {
        Self(sequence)
    }
}
