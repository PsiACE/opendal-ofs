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

use crate::Error;
use crate::filesystem::{Digest, FileFingerprint};
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::ser::SerializeTuple as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

use super::extension::ExtensionFileRef;
use super::object::{GcEpoch, ObjectClass, ObjectId, ObjectLocator, ObjectRef};
use super::pack::EntryRef as PackEntryRef;
use super::stream::{STREAM_TAIL_BYTES, StreamKind, StreamRef};

/// Durable reference to one Whole File byte stream.
///
/// Its enclosing namespace record supplies the logical length and payload
/// digest. The variant fixes the object class and stream kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WholeFileRef {
    gc_epoch: GcEpoch,
    object_id: ObjectId,
    object_digest: Digest,
}

super::wire::tuple_wire!(WholeFileRef {
    gc_epoch: GcEpoch,
    object_id: ObjectId,
    object_digest: Digest,
});

/// The one durable file-data reference used by namespace records.
///
/// Whole files name one byte stream. Packed files name one exact range in an
/// immutable pack; the enclosing fingerprint supplies range length and digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileDataRef {
    Whole(WholeFileRef),
    Pack(PackEntryRef),
    Extension(ExtensionFileRef),
}

const WHOLE_FILE: u8 = 1;
const PACK_ENTRY: u8 = 2;
const EXTENSION_FILE: u8 = 3;

impl FileDataRef {
    pub(super) fn from_stream(
        reference: StreamRef,
        fingerprint: FileFingerprint,
    ) -> Result<Self, Error> {
        if reference
            .require(StreamKind::FILE_BYTES, ObjectClass::FileData)
            .is_err()
            || reference.payload_length != fingerprint.logical_length()
            || reference.payload_digest != fingerprint.digest()
        {
            return Err(Error::corrupt(
                "publish Managed file",
                "file data does not match its fingerprint",
            ));
        }
        Ok(Self::Whole(WholeFileRef {
            gc_epoch: reference.object.locator.gc_epoch,
            object_id: reference.object.locator.id,
            object_digest: reference.object.digest,
        }))
    }

    pub(crate) fn stream_ref(self, fingerprint: FileFingerprint) -> Result<StreamRef, Error> {
        let Self::Whole(reference) = self else {
            return Err(Error::corrupt(
                "read Managed file",
                "packed data is not a whole-file stream",
            ));
        };
        let payload_length = fingerprint.logical_length();
        let encoded_length = payload_length
            .checked_add(STREAM_TAIL_BYTES as u64)
            .ok_or_else(|| Error::corrupt("read Managed file", "file length overflows"))?;
        Ok(StreamRef {
            kind: StreamKind::FILE_BYTES,
            object: ObjectRef {
                locator: ObjectLocator {
                    gc_epoch: reference.gc_epoch,
                    class: ObjectClass::FileData,
                    id: reference.object_id,
                },
                encoded_length,
                digest: reference.object_digest,
            },
            payload_length,
            payload_digest: fingerprint.digest(),
        })
    }

    pub(crate) const fn from_pack(locator: ObjectLocator, offset: u64) -> Self {
        Self::Pack(PackEntryRef::new(locator, offset))
    }

    pub(crate) const fn object_locator(self) -> ObjectLocator {
        match self {
            Self::Whole(reference) => ObjectLocator {
                gc_epoch: reference.gc_epoch,
                class: ObjectClass::FileData,
                id: reference.object_id,
            },
            Self::Pack(reference) => reference.locator(),
            Self::Extension(reference) => reference.root.object.locator,
        }
    }

    pub(crate) const fn pack_offset(self) -> Option<u64> {
        match self {
            Self::Whole(_) => None,
            Self::Pack(reference) => Some(reference.offset()),
            Self::Extension(_) => None,
        }
    }

    pub(crate) const fn extension(self) -> Option<ExtensionFileRef> {
        match self {
            Self::Extension(reference) => Some(reference),
            Self::Whole(_) | Self::Pack(_) => None,
        }
    }

    pub(crate) fn validate(self, fingerprint: FileFingerprint) -> Result<(), Error> {
        match self {
            Self::Whole(_) => self.stream_ref(fingerprint).map(drop),
            Self::Pack(reference) => reference
                .offset()
                .checked_add(fingerprint.logical_length())
                .map(drop)
                .ok_or_else(|| Error::corrupt("read Managed file", "pack range overflows")),
            Self::Extension(reference) => {
                if reference.root.object.locator.class != ObjectClass::Extension {
                    return Err(Error::corrupt(
                        "read Managed file",
                        "extension root uses the wrong object class",
                    ));
                }
                Ok(())
            }
        }
    }
}

impl Serialize for FileDataRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tuple = serializer.serialize_tuple(2)?;
        match self {
            Self::Whole(reference) => {
                tuple.serialize_element(&WHOLE_FILE)?;
                tuple.serialize_element(reference)?;
            }
            Self::Pack(reference) => {
                tuple.serialize_element(&PACK_ENTRY)?;
                tuple.serialize_element(reference)?;
            }
            Self::Extension(reference) => {
                tuple.serialize_element(&EXTENSION_FILE)?;
                tuple.serialize_element(reference)?;
            }
        }
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for FileDataRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FileDataRefVisitor;

        impl<'de> Visitor<'de> for FileDataRefVisitor {
            type Value = FileDataRef;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a tagged Managed file-data reference")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let kind: u8 = sequence
                    .next_element()?
                    .ok_or_else(|| A::Error::custom("file-data reference kind is missing"))?;
                let value = match kind {
                    WHOLE_FILE => Self::Value::Whole(
                        sequence
                            .next_element()?
                            .ok_or_else(|| A::Error::custom("whole-file reference is missing"))?,
                    ),
                    PACK_ENTRY => Self::Value::Pack(
                        sequence
                            .next_element()?
                            .ok_or_else(|| A::Error::custom("pack-entry reference is missing"))?,
                    ),
                    EXTENSION_FILE => {
                        Self::Value::Extension(sequence.next_element()?.ok_or_else(|| {
                            A::Error::custom("extension-file reference is missing")
                        })?)
                    }
                    _ => return Err(A::Error::custom("unknown file-data reference kind")),
                };
                if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom("file-data reference has trailing fields"));
                }
                Ok(value)
            }
        }

        deserializer.deserialize_tuple(2, FileDataRefVisitor)
    }
}
