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

use crate::filesystem::VolumeId;
use crate::managed::{ManagedError, ManagedErrorKind};

pub(crate) const SUPERBLOCK_KEY: &str = ".ofs/managed/metadata/v1/superblock.json";

const SPECIFICATION: &str = "managed/1";
const NAMING_POLICY: &str = "portable-utf8/1";
const FILE_VERSION_FORMAT: &str = "extent-map/1";
const DATA_FORMAT: &str = "segment/1";

/// Physical format of the authoritative Managed metadata store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataFormat {
    ObjectV1,
    TransactionalV1,
}

impl MetadataFormat {
    const fn identifier(self) -> &'static str {
        match self {
            Self::ObjectV1 => "object/1",
            Self::TransactionalV1 => "transactional/1",
        }
    }

    fn parse(value: &str) -> Result<Self, ManagedError> {
        match value {
            "object/1" => Ok(Self::ObjectV1),
            "transactional/1" => Ok(Self::TransactionalV1),
            _ => Err(unsupported("metadata format is unsupported")),
        }
    }
}

/// The single portable superblock owned by the selected metadata authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedFormat {
    volume_id: VolumeId,
    metadata_format: MetadataFormat,
}

impl ManagedFormat {
    pub const fn v1(volume_id: VolumeId, metadata_format: MetadataFormat) -> Self {
        Self {
            volume_id,
            metadata_format,
        }
    }

    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub const fn metadata_format(&self) -> MetadataFormat {
        self.metadata_format
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, ManagedError> {
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
        if wire
            .required_extensions
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(corrupt(
                "required extensions are not strictly ordered or contain duplicates",
            ));
        }
        if wire.specification != SPECIFICATION {
            return Err(unsupported("Managed specification is unsupported"));
        }
        if wire.naming_policy != NAMING_POLICY {
            return Err(unsupported("naming policy is unsupported"));
        }
        if wire.file_version_format != FILE_VERSION_FORMAT {
            return Err(unsupported("file version format is unsupported"));
        }
        if wire.data_format != DATA_FORMAT {
            return Err(unsupported("data format is unsupported"));
        }
        if !wire.required_extensions.is_empty() {
            return Err(unsupported("superblock requires an unsupported extension"));
        }
        Ok(Self {
            volume_id: decode_volume_id(&wire.volume_id)?,
            metadata_format: MetadataFormat::parse(&wire.metadata_format)?,
        })
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
    specification: String,
    volume_id: String,
    naming_policy: String,
    metadata_format: String,
    file_version_format: String,
    data_format: String,
    required_extensions: Vec<String>,
}

impl From<&ManagedFormat> for SuperblockWire {
    fn from(format: &ManagedFormat) -> Self {
        Self {
            specification: SPECIFICATION.to_owned(),
            volume_id: encode_hex(format.volume_id.as_bytes()),
            naming_policy: NAMING_POLICY.to_owned(),
            metadata_format: format.metadata_format.identifier().to_owned(),
            file_version_format: FILE_VERSION_FORMAT.to_owned(),
            data_format: DATA_FORMAT.to_owned(),
            required_extensions: Vec::new(),
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
        ManagedFormat::v1(VolumeId::from_bytes([1; 16]), MetadataFormat::ObjectV1)
    }

    #[test]
    fn superblock_round_trips_the_managed_v1_contract() {
        let format = object_format();
        let encoded = format.encode().unwrap();
        assert_eq!(ManagedFormat::decode(&encoded).unwrap(), format);
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            r#"{"specification":"managed/1","volume_id":"01010101010101010101010101010101","naming_policy":"portable-utf8/1","metadata_format":"object/1","file_version_format":"extent-map/1","data_format":"segment/1","required_extensions":[]}"#
        );
    }

    #[test]
    fn transactional_metadata_format_round_trips() {
        let format = ManagedFormat::v1(
            VolumeId::from_bytes([2; 16]),
            MetadataFormat::TransactionalV1,
        );
        let encoded = format.encode().unwrap();
        assert_eq!(ManagedFormat::decode(&encoded).unwrap(), format);
        assert!(
            std::str::from_utf8(&encoded)
                .unwrap()
                .contains(r#""metadata_format":"transactional/1""#)
        );
    }

    #[test]
    fn unknown_required_extension_is_rejected_before_open() {
        let unknown = br#"{"specification":"managed/1","volume_id":"01010101010101010101010101010101","naming_policy":"portable-utf8/1","metadata_format":"object/1","file_version_format":"extent-map/1","data_format":"segment/1","required_extensions":["future/1"]}"#;
        assert_eq!(
            ManagedFormat::decode(unknown).unwrap_err().kind(),
            ManagedErrorKind::UnsupportedFormat
        );
    }

    #[test]
    fn required_extensions_must_be_strictly_ordered() {
        let duplicate = br#"{"specification":"managed/1","volume_id":"01010101010101010101010101010101","naming_policy":"portable-utf8/1","metadata_format":"object/1","file_version_format":"extent-map/1","data_format":"segment/1","required_extensions":["future/1","future/1"]}"#;
        assert_eq!(
            ManagedFormat::decode(duplicate).unwrap_err().kind(),
            ManagedErrorKind::Corrupt
        );
    }

    #[test]
    fn unknown_superblock_field_is_rejected() {
        let unknown = br#"{"specification":"managed/1","volume_id":"01010101010101010101010101010101","naming_policy":"portable-utf8/1","metadata_format":"object/1","file_version_format":"extent-map/1","data_format":"segment/1","required_extensions":[],"policy":"fastcdc"}"#;
        assert_eq!(
            ManagedFormat::decode(unknown).unwrap_err().kind(),
            ManagedErrorKind::Corrupt
        );
    }

    #[test]
    fn uppercase_volume_identity_is_rejected() {
        let bytes = br#"{"specification":"managed/1","volume_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","naming_policy":"portable-utf8/1","metadata_format":"object/1","file_version_format":"extent-map/1","data_format":"segment/1","required_extensions":[]}"#;
        assert_eq!(
            ManagedFormat::decode(bytes).unwrap_err().kind(),
            ManagedErrorKind::Corrupt
        );
    }
}
