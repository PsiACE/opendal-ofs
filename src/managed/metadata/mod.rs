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
pub use superblock::{ManagedExtension, ManagedFormat};

use namespace::NamespaceStore;
use opendal::Operator;
use record::RecordBackend;
use superblock::{MAX_SUPERBLOCK_BYTES, SUPERBLOCK_KEY};

use super::ManagedVolume;
use super::error::{invalid, unavailable};
use super::extensions::branch::BranchStore;
use crate::filesystem::VolumeError;
/// The selected format and sole mutable-record authority for one Managed volume.
pub struct ManagedMetadata {
    backend: RecordBackend,
}

impl ManagedMetadata {
    pub fn object(operator: Operator) -> Result<Self, VolumeError> {
        Ok(Self {
            backend: RecordBackend::object(operator, "open Managed metadata")?,
        })
    }

    pub fn d1(config: D1Config) -> Result<Self, VolumeError> {
        Ok(Self {
            backend: RecordBackend::d1(config)?,
        })
    }

    pub async fn create_format(
        &self,
        desired: &ManagedFormat,
    ) -> Result<ManagedFormat, VolumeError> {
        self.backend
            .create_or_read(
                SUPERBLOCK_KEY,
                desired.encode()?,
                MAX_SUPERBLOCK_BYTES,
                "create Managed format",
            )
            .await
            .and_then(|bytes| ManagedFormat::decode(&bytes))
    }

    pub async fn read_format(&self) -> Result<ManagedFormat, VolumeError> {
        let (bytes, _) = self
            .backend
            .read(SUPERBLOCK_KEY, MAX_SUPERBLOCK_BYTES, "read Managed format")
            .await?
            .ok_or_else(|| unavailable("read Managed format", "Managed format does not exist"))?;
        ManagedFormat::decode(&bytes)
    }

    pub fn open_volume(
        &self,
        format: ManagedFormat,
        data: Operator,
    ) -> Result<ManagedVolume, VolumeError> {
        if format.requires_extension(ManagedExtension::BranchV1) {
            return Err(invalid(
                "open Managed volume",
                "base namespace does not accept the branch extension",
            ));
        }
        let volume_id = format.volume_id();
        let namespace = NamespaceStore::new(volume_id, data.clone(), self.backend.clone());
        ManagedVolume::new(namespace, data)
    }

    pub fn branches(
        &self,
        format: &ManagedFormat,
        data: Operator,
    ) -> Result<BranchStore, VolumeError> {
        if !format.requires_extension(ManagedExtension::BranchV1) {
            return Err(invalid(
                "open Managed branches",
                "Managed format does not enable branch/v1",
            ));
        }
        let volume_id = format.volume_id();
        Ok(BranchStore::new(volume_id, data, self.backend.clone()))
    }
}

#[cfg(test)]
mod tests {
    use opendal::services;

    use super::*;
    use crate::filesystem::{VolumeErrorKind, VolumeId};

    fn memory() -> Operator {
        Operator::new(services::Memory::default())
            .expect("memory operator must build")
            .finish()
    }

    fn object_metadata(operator: Operator) -> ManagedMetadata {
        ManagedMetadata {
            backend: RecordBackend::Object(operator),
        }
    }

    #[tokio::test]
    async fn existing_remote_format_wins_during_alias_creation() {
        let data = memory();
        let metadata = object_metadata(data);
        let current = ManagedFormat::v1(VolumeId::from_bytes([1; 16]))
            .with_extension(ManagedExtension::BranchV1);
        metadata.create_format(&current).await.unwrap();

        let provisional = ManagedFormat::v1(VolumeId::from_bytes([2; 16]));
        assert_eq!(metadata.create_format(&provisional).await.unwrap(), current);
    }

    #[test]
    fn open_rejects_extension_mismatches() {
        let data = memory();
        let metadata = object_metadata(data.clone());
        let base = ManagedFormat::v1(VolumeId::from_bytes([1; 16]));
        let branched = base.clone().with_extension(ManagedExtension::BranchV1);

        assert_eq!(
            metadata.branches(&base, data.clone()).err().unwrap().kind(),
            VolumeErrorKind::Invalid
        );
        assert_eq!(
            metadata
                .open_volume(branched, data.clone())
                .err()
                .unwrap()
                .kind(),
            VolumeErrorKind::Invalid
        );
    }
}
