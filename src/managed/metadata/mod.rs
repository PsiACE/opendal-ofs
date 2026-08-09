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
#[doc(hidden)]
pub mod record;
mod superblock;

pub use d1::D1Config;
pub(crate) use d1::D1Metadata;
pub use namespace::NamespaceGcSweep;
pub use superblock::{ManagedExtension, ManagedFormat, MetadataFormat};

use object::ObjectMetadata;
use opendal::Operator;

#[cfg(feature = "managed-branch")]
use super::extensions::branch::{BoundNamespace, BranchStore};
use super::{ManagedError, ManagedVolume};
/// The metadata authority selected for one Managed volume.
///
/// Backend selection ends here. Callers open formats, volumes, and optional
/// extensions without repeating Object/D1 dispatch throughout the access
/// model.
pub struct ManagedMetadata(MetadataBackend);

enum MetadataBackend {
    Object(ObjectMetadata),
    D1(D1Metadata),
}

impl ManagedMetadata {
    pub const fn object(operator: Operator) -> Self {
        Self(MetadataBackend::Object(ObjectMetadata::new(operator)))
    }

    pub fn d1(config: D1Config) -> Result<Self, ManagedError> {
        D1Metadata::new(config).map(MetadataBackend::D1).map(Self)
    }

    pub const fn metadata_format(&self) -> MetadataFormat {
        match &self.0 {
            MetadataBackend::Object(_) => MetadataFormat::ObjectV1,
            MetadataBackend::D1(_) => MetadataFormat::TransactionalV1,
        }
    }

    pub async fn create_format(
        &self,
        desired: &ManagedFormat,
    ) -> Result<ManagedFormat, ManagedError> {
        self.require_backend_format(desired)?;
        match &self.0 {
            MetadataBackend::Object(metadata) => metadata.create_format(desired).await,
            MetadataBackend::D1(metadata) => metadata.create_format(desired).await,
        }
    }

    pub async fn read_format(&self) -> Result<ManagedFormat, ManagedError> {
        let format = self.read_format_optional().await?.ok_or_else(|| {
            ManagedError::new(
                super::ManagedErrorKind::Unavailable,
                "read Managed format",
                "Managed format does not exist",
            )
        })?;
        Ok(format)
    }

    async fn read_format_optional(&self) -> Result<Option<ManagedFormat>, ManagedError> {
        let format = match &self.0 {
            MetadataBackend::Object(metadata) => metadata.read_format_optional().await,
            MetadataBackend::D1(metadata) => metadata.read_format_optional().await,
        }?;
        if let Some(format) = &format {
            self.require_backend_format(format)?;
        }
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
        match &self.0 {
            MetadataBackend::Object(_) => ManagedVolume::object(volume_id, data),
            MetadataBackend::D1(metadata) => ManagedVolume::d1(volume_id, data, metadata.clone()),
        }
    }

    #[cfg(feature = "managed-branch")]
    pub fn open_branch_volume(
        &self,
        format: ManagedFormat,
        data: Operator,
        namespace: BoundNamespace,
    ) -> Result<ManagedVolume, ManagedError> {
        self.require_backend_format(&format)?;
        if !format.requires_extension(ManagedExtension::BranchV1)
            || namespace.volume_id() != format.volume_id()
        {
            return Err(ManagedError::new(
                super::ManagedErrorKind::Invalid,
                "open Managed volume",
                "branch namespace does not match the Managed format",
            ));
        }
        ManagedVolume::branch(format.volume_id(), data, namespace)
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
        match &self.0 {
            MetadataBackend::Object(_) => BranchStore::object(volume_id, data),
            MetadataBackend::D1(metadata) => Ok(BranchStore::d1(volume_id, metadata.clone())),
        }
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

fn require_same_format(
    desired: &ManagedFormat,
    observed: ManagedFormat,
) -> Result<ManagedFormat, ManagedError> {
    if &observed == desired {
        Ok(observed)
    } else {
        Err(ManagedError::new(
            super::ManagedErrorKind::Conflict,
            "create Managed format",
            "metadata is bound to another Managed volume",
        ))
    }
}
