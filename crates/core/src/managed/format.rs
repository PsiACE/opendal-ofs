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
use crate::filesystem::{NodeId, VolumeId};
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::ser::SerializeTuple as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::num::NonZeroU64;

use super::extension::{ExtensionFormat, FileAccessInfo};
use super::record::Record;

pub(crate) const FORMAT_KEY: &str = "managed/1/format";
const MAX_FORMAT_BODY_BYTES: usize = 64 * 1024;

const FORMAT_RECORD: Record = Record::new(*b"OFSFMT01", MAX_FORMAT_BODY_BYTES);
pub(crate) const MAX_FORMAT_BYTES: usize = FORMAT_RECORD.maximum_encoded_bytes();

/// The sole Managed storage format understood by this build.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManagedFormat {
    volume_id: VolumeId,
    root_node_id: NodeId,
    file_placement: FilePlacement,
    authority_extension: Option<ExtensionFormat>,
}

impl ManagedFormat {
    pub(crate) const fn new(
        volume_id: VolumeId,
        root_node_id: NodeId,
        file_placement: FilePlacement,
        authority_extension: Option<ExtensionFormat>,
    ) -> Self {
        Self {
            volume_id,
            root_node_id,
            file_placement,
            authority_extension,
        }
    }

    pub(crate) const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub(crate) const fn root_node_id(&self) -> NodeId {
        self.root_node_id
    }

    pub(crate) const fn file_placement(&self) -> &FilePlacement {
        &self.file_placement
    }

    pub(crate) const fn authority_extension(&self) -> Option<&ExtensionFormat> {
        self.authority_extension.as_ref()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        FORMAT_RECORD.encode(&VolumeFormat {
            volume_id: self.volume_id,
            root_node_id: self.root_node_id,
            file_placement: self.file_placement.clone(),
            authority_extension: self.authority_extension.clone(),
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let format: VolumeFormat = FORMAT_RECORD.decode(bytes)?;
        Ok(Self {
            volume_id: format.volume_id,
            root_node_id: format.root_node_id,
            file_placement: format.file_placement,
            authority_extension: format.authority_extension,
        })
    }
}

/// File-data placement fixed for the lifetime of one volume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FilePlacement {
    Whole,
    Pack { target_bytes: NonZeroU64 },
    Extension(FileAccessInfo),
}

impl Serialize for FilePlacement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tuple = serializer.serialize_tuple(match self {
            Self::Whole => 1,
            Self::Pack { .. } | Self::Extension(_) => 2,
        })?;
        match self {
            Self::Whole => tuple.serialize_element(&1_u8)?,
            Self::Pack { target_bytes } => {
                tuple.serialize_element(&2_u8)?;
                tuple.serialize_element(target_bytes)?;
            }
            Self::Extension(info) => {
                tuple.serialize_element(&3_u8)?;
                tuple.serialize_element(info)?;
            }
        }
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for FilePlacement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilePlacementVisitor;

        impl<'de> Visitor<'de> for FilePlacementVisitor {
            type Value = FilePlacement;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Managed file placement")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let kind: u8 = sequence
                    .next_element()?
                    .ok_or_else(|| A::Error::custom("file placement kind is missing"))?;
                let placement = match kind {
                    1 => FilePlacement::Whole,
                    2 => FilePlacement::Pack {
                        target_bytes: sequence
                            .next_element()?
                            .ok_or_else(|| A::Error::custom("Pack target bytes are missing"))?,
                    },
                    3 => FilePlacement::Extension(sequence.next_element()?.ok_or_else(|| {
                        A::Error::custom("extension access description is missing")
                    })?),
                    _ => return Err(A::Error::custom("unknown file placement kind")),
                };
                if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom("file placement has trailing fields"));
                }
                Ok(placement)
            }
        }

        deserializer.deserialize_seq(FilePlacementVisitor)
    }
}

impl FilePlacement {
    pub(crate) const fn pack_target_bytes(&self) -> Option<u64> {
        match self {
            Self::Whole => None,
            Self::Pack { target_bytes } => Some(target_bytes.get()),
            Self::Extension(_) => None,
        }
    }
}

#[derive(Debug)]
struct VolumeFormat {
    volume_id: VolumeId,
    root_node_id: NodeId,
    file_placement: FilePlacement,
    authority_extension: Option<ExtensionFormat>,
}
super::wire::tuple_wire!(VolumeFormat {
    volume_id: VolumeId,
    root_node_id: NodeId,
    file_placement: FilePlacement,
    authority_extension: Option<ExtensionFormat>,
});
