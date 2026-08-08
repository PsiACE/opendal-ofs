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

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{ManagedError, ManagedErrorKind};
use crate::filesystem::VolumeId;

pub(crate) const SUPERBLOCK_KEY: &str = ".ofs/managed/volume.json";

const MAGIC: &str = "ofs-managed-volume";
const FORMAT: u16 = 1;
const FASTCDC_EXTENSION: &str = "data-fastcdc/1";
const SUPPORTED_EXTENSIONS: &[&str] = &[FASTCDC_EXTENSION];

/// Naming rules fixed by the Managed volume format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NamingPolicy {
    PortableUtf8V1,
}

/// Physical layout of the authoritative Managed metadata store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataPlacement {
    ColocatedObject,
    ExternalD1,
}

/// The single portable superblock shared by metadata and file-data storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedFormat {
    volume_id: VolumeId,
    metadata_placement: MetadataPlacement,
    naming_policy: NamingPolicy,
    extensions: BTreeSet<String>,
}

impl ManagedFormat {
    pub fn v1(
        volume_id: VolumeId,
        metadata_placement: MetadataPlacement,
    ) -> Result<Self, ManagedError> {
        let format = Self {
            volume_id,
            metadata_placement,
            naming_policy: NamingPolicy::PortableUtf8V1,
            extensions: BTreeSet::from([FASTCDC_EXTENSION.to_owned()]),
        };
        format.validate_for_write()?;
        Ok(format)
    }

    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub const fn metadata_placement(&self) -> MetadataPlacement {
        self.metadata_placement
    }

    pub(crate) fn validate_for_read(&self) -> Result<(), ManagedError> {
        if self
            .extensions
            .iter()
            .any(|extension| !SUPPORTED_EXTENSIONS.contains(&extension.as_str()))
        {
            return Err(unsupported("superblock requires an unsupported extension"));
        }
        if !self.extensions.contains(FASTCDC_EXTENSION) {
            return Err(unsupported(
                "superblock omits the required data-fastcdc/1 extension",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_for_write(&self) -> Result<(), ManagedError> {
        self.validate_for_read()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, ManagedError> {
        self.validate_for_write()?;
        serde_json::to_vec(&SuperblockWire::from(self)).map_err(|_| {
            ManagedError::new(
                ManagedErrorKind::Invalid,
                "create Managed volume",
                "superblock cannot be encoded",
            )
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ManagedError> {
        let wire: SuperblockWire = serde_json::from_slice(bytes)
            .map_err(|_| corrupt("superblock is not strict UTF-8 JSON"))?;
        if wire.magic != MAGIC || wire.format != FORMAT {
            return Err(unsupported("superblock format is unsupported"));
        }
        if wire.extensions.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(corrupt(
                "extensions are not strictly ordered or contain duplicates",
            ));
        }
        let format = Self {
            volume_id: decode_volume_id(&wire.volume_id)?,
            metadata_placement: wire.metadata_layout.into(),
            naming_policy: wire.naming_policy.into(),
            extensions: wire.extensions.into_iter().collect(),
        };
        format.validate_for_read()?;
        Ok(format)
    }
}

fn unsupported(message: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::UnsupportedFormat,
        "open Managed volume",
        message,
    )
}

fn corrupt(message: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Corrupt,
        "read Managed superblock",
        message,
    )
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SuperblockWire {
    magic: String,
    format: u16,
    volume_id: String,
    naming_policy: NamingPolicyWire,
    metadata_layout: MetadataLayoutWire,
    data_layout: DataLayoutWire,
    extensions: Vec<String>,
}

impl From<&ManagedFormat> for SuperblockWire {
    fn from(format: &ManagedFormat) -> Self {
        Self {
            magic: MAGIC.to_owned(),
            format: FORMAT,
            volume_id: encode_hex(format.volume_id.as_bytes()),
            naming_policy: format.naming_policy.into(),
            metadata_layout: format.metadata_placement.into(),
            data_layout: DataLayoutWire::ContentAddressedV1,
            extensions: format.extensions.iter().cloned().collect(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
enum NamingPolicyWire {
    #[serde(rename = "portable-utf8/1")]
    PortableUtf8V1,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
enum MetadataLayoutWire {
    #[serde(rename = "object/1")]
    ObjectV1,
    #[serde(rename = "transactional/1")]
    TransactionalV1,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
enum DataLayoutWire {
    #[serde(rename = "content-addressed/1")]
    ContentAddressedV1,
}

impl From<MetadataPlacement> for MetadataLayoutWire {
    fn from(value: MetadataPlacement) -> Self {
        match value {
            MetadataPlacement::ColocatedObject => Self::ObjectV1,
            MetadataPlacement::ExternalD1 => Self::TransactionalV1,
        }
    }
}

impl From<MetadataLayoutWire> for MetadataPlacement {
    fn from(value: MetadataLayoutWire) -> Self {
        match value {
            MetadataLayoutWire::ObjectV1 => Self::ColocatedObject,
            MetadataLayoutWire::TransactionalV1 => Self::ExternalD1,
        }
    }
}

impl From<NamingPolicy> for NamingPolicyWire {
    fn from(value: NamingPolicy) -> Self {
        match value {
            NamingPolicy::PortableUtf8V1 => Self::PortableUtf8V1,
        }
    }
}

impl From<NamingPolicyWire> for NamingPolicy {
    fn from(value: NamingPolicyWire) -> Self {
        match value {
            NamingPolicyWire::PortableUtf8V1 => Self::PortableUtf8V1,
        }
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_volume_id(value: &str) -> Result<VolumeId, ManagedError> {
    if value.len() != 32
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(corrupt("volume identity is not 16-byte lowercase hex"));
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| corrupt("volume identity is not 16-byte lowercase hex"))?;
    }
    Ok(VolumeId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object_format() -> ManagedFormat {
        ManagedFormat::v1(
            VolumeId::from_bytes([1; 16]),
            MetadataPlacement::ColocatedObject,
        )
        .unwrap()
    }

    #[test]
    fn superblock_round_trips_the_required_layout_and_extensions() {
        let format = object_format();
        let encoded = format.encode().unwrap();
        assert_eq!(ManagedFormat::decode(&encoded).unwrap(), format);
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            r#"{"magic":"ofs-managed-volume","format":1,"volume_id":"01010101010101010101010101010101","naming_policy":"portable-utf8/1","metadata_layout":"object/1","data_layout":"content-addressed/1","extensions":["data-fastcdc/1"]}"#
        );
    }

    #[test]
    fn unknown_extension_is_rejected_before_open() {
        let unknown = br#"{"magic":"ofs-managed-volume","format":1,"volume_id":"01010101010101010101010101010101","naming_policy":"portable-utf8/1","metadata_layout":"object/1","data_layout":"content-addressed/1","extensions":["data-future/1"]}"#;
        assert_eq!(
            ManagedFormat::decode(unknown).unwrap_err().kind(),
            ManagedErrorKind::UnsupportedFormat
        );
    }

    #[test]
    fn uppercase_volume_identity_is_rejected() {
        let bytes = br#"{"magic":"ofs-managed-volume","format":1,"volume_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","naming_policy":"portable-utf8/1","metadata_layout":"object/1","data_layout":"content-addressed/1","extensions":[]}"#;
        assert_eq!(
            ManagedFormat::decode(bytes).unwrap_err().kind(),
            ManagedErrorKind::Corrupt
        );
    }

    #[test]
    fn duplicate_extensions_are_rejected() {
        let bytes = br#"{"magic":"ofs-managed-volume","format":1,"volume_id":"01010101010101010101010101010101","naming_policy":"portable-utf8/1","metadata_layout":"object/1","data_layout":"content-addressed/1","extensions":["data-fastcdc/1","data-fastcdc/1"]}"#;
        assert_eq!(
            ManagedFormat::decode(bytes).unwrap_err().kind(),
            ManagedErrorKind::Corrupt
        );
    }
}
