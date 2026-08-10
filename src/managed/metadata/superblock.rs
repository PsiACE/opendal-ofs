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

use crate::filesystem::{VolumeError, VolumeId};
use crate::managed::error::{corrupt, invalid, unsupported};
use crate::managed::format::LowerHex;

pub(crate) const SUPERBLOCK_KEY: &str = ".ofs/managed/superblock.json";
pub(crate) const MAX_SUPERBLOCK_BYTES: usize = 64 * 1024;

const FORMAT: &str = "managed/1";

/// A required Managed format capability understood by this build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedExtension {
    BranchV1,
}

impl ManagedExtension {
    const fn identifier(self) -> &'static str {
        match self {
            Self::BranchV1 => "branch/v1",
        }
    }

    fn parse(value: &str) -> Result<Self, VolumeError> {
        match value {
            "branch/v1" => Ok(Self::BranchV1),
            _ => Err(unsupported(
                "open Managed volume",
                "superblock requires an unsupported extension",
            )),
        }
    }
}

/// The single portable superblock owned by the selected metadata authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedFormat {
    volume_id: VolumeId,
    required_extensions: Vec<ManagedExtension>,
}

impl ManagedFormat {
    pub const fn v1(volume_id: VolumeId) -> Self {
        Self {
            volume_id,
            required_extensions: Vec::new(),
        }
    }

    pub fn with_extension(mut self, extension: ManagedExtension) -> Self {
        match self
            .required_extensions
            .binary_search_by(|current| current.identifier().cmp(extension.identifier()))
        {
            Ok(_) => {}
            Err(index) => self.required_extensions.insert(index, extension),
        }
        self
    }

    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub fn requires_extension(&self, extension: ManagedExtension) -> bool {
        self.required_extensions
            .binary_search_by(|current| current.identifier().cmp(extension.identifier()))
            .is_ok()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, VolumeError> {
        let bytes = serde_json::to_vec(&SuperblockWire::from(self))
            .map_err(|_| invalid("create Managed volume", "superblock cannot be encoded"))?;
        if bytes.len() > MAX_SUPERBLOCK_BYTES {
            return Err(invalid(
                "create Managed volume",
                "superblock exceeds its size limit",
            ));
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, VolumeError> {
        if bytes.len() > MAX_SUPERBLOCK_BYTES {
            return Err(corrupt(
                "read Managed superblock",
                "superblock exceeds its size limit",
            ));
        }
        let wire: SuperblockWire = serde_json::from_slice(bytes).map_err(|_| {
            corrupt(
                "read Managed superblock",
                "superblock is not strict UTF-8 JSON",
            )
        })?;
        if wire.format != FORMAT {
            return Err(unsupported(
                "open Managed volume",
                "Managed format version is unsupported",
            ));
        }
        if wire.extensions.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(corrupt(
                "read Managed superblock",
                "required extensions are not strictly ordered or contain duplicates",
            ));
        }
        let required_extensions = wire
            .extensions
            .iter()
            .map(|value| ManagedExtension::parse(value))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            volume_id: decode_volume_id(&wire.volume_id)?,
            required_extensions,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SuperblockWire {
    format: String,
    volume_id: String,
    extensions: Vec<String>,
}

impl From<&ManagedFormat> for SuperblockWire {
    fn from(format: &ManagedFormat) -> Self {
        Self {
            format: FORMAT.to_owned(),
            volume_id: LowerHex::encode(format.volume_id.as_bytes()),
            extensions: format
                .required_extensions
                .iter()
                .map(|extension| extension.identifier().to_owned())
                .collect(),
        }
    }
}

fn decode_volume_id(value: &str) -> Result<VolumeId, VolumeError> {
    let bytes = LowerHex::decode(value)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            corrupt(
                "read Managed superblock",
                "volume identity is not 16-byte lowercase hex",
            )
        })?;
    Ok(VolumeId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::VolumeErrorKind;

    fn base_format() -> ManagedFormat {
        ManagedFormat::v1(VolumeId::from_bytes([1; 16]))
    }

    #[test]
    fn superblock_round_trips() {
        let format = base_format().with_extension(ManagedExtension::BranchV1);
        let encoded = format.encode().unwrap();
        assert_eq!(ManagedFormat::decode(&encoded).unwrap(), format);
        assert_eq!(
            encoded,
            br#"{"format":"managed/1","volume_id":"01010101010101010101010101010101","extensions":["branch/v1"]}"#
        );
    }

    #[test]
    fn unknown_format_is_rejected_before_open() {
        let unknown = br#"{"format":"managed/2","volume_id":"01010101010101010101010101010101","extensions":[]}"#;
        assert_eq!(
            ManagedFormat::decode(unknown).unwrap_err().kind(),
            VolumeErrorKind::UnsupportedFormat
        );
    }

    #[test]
    fn unknown_required_extension_is_rejected_before_open() {
        let unknown = br#"{"format":"managed/1","volume_id":"01010101010101010101010101010101","extensions":["future/v1"]}"#;
        assert_eq!(
            ManagedFormat::decode(unknown).unwrap_err().kind(),
            VolumeErrorKind::UnsupportedFormat
        );
    }

    #[test]
    fn malformed_v1_is_rejected() {
        let malformed: [&[u8]; 3] = [
            br#"{"format":"managed/1","volume_id":"01010101010101010101010101010101","extensions":[],"policy":"fastcdc"}"#,
            br#"{"format":"managed/1","volume_id":"01010101010101010101010101010101","extensions":["branch/v1","branch/v1"]}"#,
            br#"{"format":"managed/1","volume_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","extensions":[]}"#,
        ];
        for bytes in malformed {
            assert_eq!(
                ManagedFormat::decode(bytes).unwrap_err().kind(),
                VolumeErrorKind::Corrupt
            );
        }
    }
}
