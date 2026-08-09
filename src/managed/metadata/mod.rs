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

pub(crate) mod d1;
pub(crate) mod namespace;
pub(crate) mod object;
pub(crate) mod record;
mod superblock;

pub use d1::D1Config;
pub use namespace::NamespaceGcSweep;
pub use superblock::{ManagedExtension, ManagedFormat, MetadataFormat};

use opendal::Operator;
use record::RecordBackend;
use superblock::SUPERBLOCK_KEY;

#[cfg(feature = "managed-branch")]
use super::extensions::branch::BranchStore;
use super::{ManagedError, ManagedVolume};
/// The selected format and sole mutable-record authority for one Managed volume.
pub struct ManagedMetadata {
    format: MetadataFormat,
    backend: RecordBackend,
}

impl ManagedMetadata {
    pub fn object(operator: Operator) -> Result<Self, ManagedError> {
        Ok(Self {
            format: MetadataFormat::ObjectV1,
            backend: RecordBackend::object(operator, "open Managed metadata")?,
        })
    }

    pub fn d1(config: D1Config) -> Result<Self, ManagedError> {
        Ok(Self {
            format: MetadataFormat::TransactionalV1,
            backend: RecordBackend::d1(config)?,
        })
    }

    pub const fn metadata_format(&self) -> MetadataFormat {
        self.format
    }

    pub async fn create_format(
        &self,
        desired: &ManagedFormat,
    ) -> Result<ManagedFormat, ManagedError> {
        self.require_backend_format(desired)?;
        let observed = self
            .backend
            .create_or_read(SUPERBLOCK_KEY, desired.encode()?, "create Managed format")
            .await
            .and_then(|bytes| ManagedFormat::decode(&bytes))?;
        self.require_backend_format(&observed)?;
        Ok(observed)
    }

    pub async fn read_format(&self) -> Result<ManagedFormat, ManagedError> {
        let (bytes, _) = self
            .backend
            .read(SUPERBLOCK_KEY, "read Managed format")
            .await?
            .ok_or_else(|| {
                ManagedError::new(
                    super::ManagedErrorKind::Unavailable,
                    "read Managed format",
                    "Managed format does not exist",
                )
            })?;
        let format = ManagedFormat::decode(&bytes)?;
        self.require_backend_format(&format)?;
        Ok(format)
    }

    pub fn open_volume(
        &self,
        format: ManagedFormat,
        data: Operator,
    ) -> Result<ManagedVolume, ManagedError> {
        self.require_backend_format(&format)?;
        if !format.required_extensions().is_empty() {
            return Err(ManagedError::new(
                super::ManagedErrorKind::Invalid,
                "open Managed volume",
                "base namespace does not accept Managed extensions",
            ));
        }
        let volume_id = format.volume_id();
        ManagedVolume::new(volume_id, data, self.backend.clone())
    }

    #[cfg(feature = "managed-branch")]
    pub fn branches(
        &self,
        format: &ManagedFormat,
        data: Operator,
    ) -> Result<BranchStore, ManagedError> {
        self.require_backend_format(format)?;
        if !format.requires_extension(ManagedExtension::BranchV1) {
            return Err(ManagedError::new(
                super::ManagedErrorKind::Invalid,
                "open Managed branches",
                "Managed format does not enable branch/v1",
            ));
        }
        let volume_id = format.volume_id();
        Ok(BranchStore::new(volume_id, data, self.backend.clone()))
    }

    fn require_backend_format(&self, format: &ManagedFormat) -> Result<(), ManagedError> {
        if format.metadata_format() == self.metadata_format() {
            Ok(())
        } else {
            Err(ManagedError::new(
                super::ManagedErrorKind::Invalid,
                "open Managed metadata",
                "superblock metadata format does not match its authority",
            ))
        }
    }
}
