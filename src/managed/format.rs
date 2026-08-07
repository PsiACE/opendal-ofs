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
use crate::filesystem::{VolumeId, VolumeModel};

const MAGIC: &str = "ofs-managed-volume";
const MAJOR: u16 = 1;
const MINOR: u16 = 0;
const SUPPORTED_FEATURES: &[&str] = &["whole-file-v1"];

/// Naming rules fixed by the Managed volume format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamingPolicy {
    PortableUtf8,
}

/// Location of the authoritative Managed namespace metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataPlacement {
    ColocatedObject,
    ExternalD1,
}

/// Logical Managed volume format shared by all metadata placements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedFormat {
    volume_id: VolumeId,
    metadata_placement: MetadataPlacement,
    data_root_binding: String,
    naming_policy: NamingPolicy,
    required_reader_features: BTreeSet<String>,
    required_writer_features: BTreeSet<String>,
}

impl ManagedFormat {
    pub fn v1(
        volume_id: VolumeId,
        metadata_placement: MetadataPlacement,
        data_root_binding: impl Into<String>,
    ) -> Result<Self, ManagedError> {
        let format = Self {
            volume_id,
            metadata_placement,
            data_root_binding: data_root_binding.into(),
            naming_policy: NamingPolicy::PortableUtf8,
            required_reader_features: BTreeSet::from(["whole-file-v1".to_owned()]),
            required_writer_features: BTreeSet::from(["whole-file-v1".to_owned()]),
        };
        format.validate_for_write()?;
        Ok(format)
    }

    pub const fn volume_model(&self) -> VolumeModel {
        VolumeModel::Managed
    }

    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub const fn metadata_placement(&self) -> MetadataPlacement {
        self.metadata_placement
    }

    pub fn data_root_binding(&self) -> &str {
        &self.data_root_binding
    }

    pub const fn naming_policy(&self) -> NamingPolicy {
        self.naming_policy
    }

    pub fn required_reader_features(&self) -> &BTreeSet<String> {
        &self.required_reader_features
    }

    pub fn required_writer_features(&self) -> &BTreeSet<String> {
        &self.required_writer_features
    }

    pub fn validate_for_read(&self) -> Result<(), ManagedError> {
        validate_features(&self.required_reader_features)
    }

    pub fn validate_for_write(&self) -> Result<(), ManagedError> {
        if self.data_root_binding.is_empty() {
            return Err(invalid(
                "activate Managed volume",
                "data root binding is empty",
            ));
        }
        self.validate_for_read()?;
        validate_features(&self.required_writer_features)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, ManagedError> {
        self.validate_for_write()?;
        serde_json::to_vec(&FormatWire::from(self)).map_err(|_| {
            ManagedError::new(
                ManagedErrorKind::Invalid,
                "create Managed format",
                "format cannot be encoded",
            )
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ManagedError> {
        let wire: FormatWire = serde_json::from_slice(bytes).map_err(|_| {
            ManagedError::new(
                ManagedErrorKind::Corrupt,
                "read Managed format",
                "format record is not valid JSON",
            )
        })?;
        if wire.magic != MAGIC || wire.major != MAJOR || wire.minor != MINOR {
            return Err(invalid(
                "read Managed format",
                "format version is unsupported",
            ));
        }
        let format = Self {
            volume_id: decode_volume_id(&wire.volume_id)?,
            metadata_placement: wire.metadata_placement.into(),
            data_root_binding: wire.data_root_binding,
            naming_policy: wire.naming_policy.into(),
            required_reader_features: wire.required_reader_features,
            required_writer_features: wire.required_writer_features,
        };
        format.validate_for_read()?;
        Ok(format)
    }
}

fn validate_features(features: &BTreeSet<String>) -> Result<(), ManagedError> {
    if features
        .iter()
        .any(|feature| !SUPPORTED_FEATURES.contains(&feature.as_str()))
    {
        return Err(invalid(
            "activate Managed volume",
            "format requires an unsupported feature",
        ));
    }
    Ok(())
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FormatWire {
    magic: String,
    major: u16,
    minor: u16,
    volume_id: String,
    metadata_placement: MetadataPlacementWire,
    data_root_binding: String,
    naming_policy: NamingPolicyWire,
    required_reader_features: BTreeSet<String>,
    required_writer_features: BTreeSet<String>,
}

impl From<&ManagedFormat> for FormatWire {
    fn from(format: &ManagedFormat) -> Self {
        Self {
            magic: MAGIC.to_owned(),
            major: MAJOR,
            minor: MINOR,
            volume_id: encode_hex(format.volume_id.as_bytes()),
            metadata_placement: format.metadata_placement.into(),
            data_root_binding: format.data_root_binding.clone(),
            naming_policy: format.naming_policy.into(),
            required_reader_features: format.required_reader_features.clone(),
            required_writer_features: format.required_writer_features.clone(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum NamingPolicyWire {
    PortableUtf8,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum MetadataPlacementWire {
    ColocatedObject,
    ExternalD1,
}

impl From<MetadataPlacement> for MetadataPlacementWire {
    fn from(value: MetadataPlacement) -> Self {
        match value {
            MetadataPlacement::ColocatedObject => Self::ColocatedObject,
            MetadataPlacement::ExternalD1 => Self::ExternalD1,
        }
    }
}

impl From<MetadataPlacementWire> for MetadataPlacement {
    fn from(value: MetadataPlacementWire) -> Self {
        match value {
            MetadataPlacementWire::ColocatedObject => Self::ColocatedObject,
            MetadataPlacementWire::ExternalD1 => Self::ExternalD1,
        }
    }
}

impl From<NamingPolicy> for NamingPolicyWire {
    fn from(value: NamingPolicy) -> Self {
        match value {
            NamingPolicy::PortableUtf8 => Self::PortableUtf8,
        }
    }
}

impl From<NamingPolicyWire> for NamingPolicy {
    fn from(value: NamingPolicyWire) -> Self {
        match value {
            NamingPolicyWire::PortableUtf8 => Self::PortableUtf8,
        }
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_volume_id(value: &str) -> Result<VolumeId, ManagedError> {
    if value.len() != 32 {
        return Err(corrupt_volume_id());
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| corrupt_volume_id())?;
    }
    Ok(VolumeId::from_bytes(bytes))
}

fn corrupt_volume_id() -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Corrupt,
        "read Managed format",
        "volume identity is invalid",
    )
}
