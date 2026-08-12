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

//! Managed volume authority and durable storage format.

pub mod authority;
mod data;
pub mod extension;
mod file;
mod format;
mod gc;
mod head;
mod layout;
mod namespace;
mod object;
mod pack;
mod publication;
mod record;
mod storage;
mod stream;
mod wire;

pub use authority::{
    AuthorityAccess, AuthorityExtension, AuthorityFuture, AuthorityHead, AuthorityId,
    AuthorityObservation, AuthorityRoot, AuthorityRoots, CollectionFence, DefaultAuthorityAccess,
};
pub(crate) use data::FileDataRef;
pub(crate) use format::ManagedFormat;
pub use gc::GcOutcome;
pub(crate) use head::ManagedObservation;
pub use head::{ManagedVolume, NamespaceRevision};
pub use object::{GcEpoch, ObjectClass, ObjectId, ObjectLocator, ObjectRef};
pub(crate) use pack::{
    ENTRY_BYTES as PACK_ENTRY_BYTES, RangeReader as PackRangeReader,
    TRAILER_BYTES as PACK_TRAILER_BYTES, Writer as PackWriter,
};

use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::sync::Arc;

use opendal::Operator;

use crate::filesystem::{NodeId, VolumeId};
use crate::workset::WorksetOptions;
use crate::{Error, ErrorKind};
use authority::{AuthorityAccessDyn, DEFAULT_AUTHORITY};
use extension::{FileAccess, FileAccessDyn, FileAccessInfo};
use format::{FORMAT_KEY, MAX_FORMAT_BYTES};

/// Object Metadata authority for one Managed volume.
#[derive(Clone)]
pub struct ManagedMetadata {
    operator: Operator,
    stream_concurrency: usize,
    worksets: WorksetOptions,
    file_access: Option<Arc<dyn FileAccessDyn>>,
    authority_access: Arc<dyn AuthorityAccessDyn>,
    authority_name: String,
}

impl ManagedMetadata {
    pub fn new(
        operator: Operator,
        stream_concurrency: NonZeroUsize,
        work_memory_mib: NonZeroUsize,
    ) -> Result<Self, Error> {
        let capability = operator.info().full_capability();
        if !(capability.read
            && capability.write
            && capability.write_with_if_not_exists
            && capability.write_with_if_match)
        {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "open Managed metadata",
                "storage lacks conditional record operations",
            ));
        }
        Ok(Self {
            operator,
            stream_concurrency: stream_concurrency.get(),
            worksets: WorksetOptions::new(work_memory_mib, stream_concurrency)?,
            file_access: None,
            authority_access: Arc::new(DefaultAuthorityAccess),
            authority_name: DEFAULT_AUTHORITY.to_owned(),
        })
    }

    /// Configure one statically composed namespace authority extension.
    pub fn with_authority_extension(
        mut self,
        access: impl AuthorityAccess,
        name: impl Into<String>,
    ) -> Self {
        self.authority_access = Arc::new(access);
        self.authority_name = name.into();
        self
    }

    /// Configure one statically composed file extension access.
    pub fn with_file_extension(mut self, access: impl FileAccess) -> Self {
        self.file_access = Some(extension::type_erase(access));
        self
    }

    /// Read the file extension description recorded by an existing volume.
    pub async fn file_extension(&self) -> Result<Option<FileAccessInfo>, Error> {
        let format = self.read_format().await?;
        Ok(match format.file_placement() {
            format::FilePlacement::Extension(info) => Some(info.clone()),
            format::FilePlacement::Whole | format::FilePlacement::Pack { .. } => None,
        })
    }

    /// Read the namespace authority extension recorded by an existing volume.
    pub async fn authority_extension(&self) -> Result<Option<extension::ExtensionFormat>, Error> {
        Ok(self.read_format().await?.authority_extension().cloned())
    }

    /// Create the Managed superblock once, or return the existing format.
    pub async fn initialize(
        &self,
        pack_target_bytes: Option<NonZeroU64>,
    ) -> Result<ManagedVolume, Error> {
        let file_placement = match pack_target_bytes {
            Some(target_bytes) => format::FilePlacement::Pack { target_bytes },
            None => format::FilePlacement::Whole,
        };
        let desired = ManagedFormat::new(
            VolumeId::generate(),
            NodeId::generate(),
            file_placement,
            self.authority_access.info_dyn(),
        );
        let encoded = desired.encode()?;
        let format = if storage::write_control(
            &self.operator,
            FORMAT_KEY,
            encoded,
            storage::ControlCondition::Missing,
        )
        .await?
        {
            desired
        } else {
            let existing = self.read_format().await?;
            if existing.file_placement() != desired.file_placement() {
                return Err(Error::conflict(
                    "create Managed volume",
                    "volume already uses a different file placement",
                ));
            }
            existing
        };
        let volume = ManagedVolume::new(
            format,
            self.operator.clone(),
            self.stream_concurrency,
            self.worksets,
            self.file_access.clone(),
            self.authority_access.clone(),
            self.authority_name.clone(),
        );
        volume.initialize().await?;
        Ok(volume)
    }

    /// Create a volume using the configured file extension access.
    pub async fn initialize_extension(&self) -> Result<ManagedVolume, Error> {
        let access = self.file_access.as_ref().ok_or_else(|| {
            Error::invalid(
                "create Managed volume",
                "a file extension access was not configured",
            )
        })?;
        let desired = ManagedFormat::new(
            VolumeId::generate(),
            NodeId::generate(),
            format::FilePlacement::Extension(access.info_dyn()),
            self.authority_access.info_dyn(),
        );
        let format = if storage::write_control(
            &self.operator,
            FORMAT_KEY,
            desired.encode()?,
            storage::ControlCondition::Missing,
        )
        .await?
        {
            desired
        } else {
            let existing = self.read_format().await?;
            if existing.file_placement() != desired.file_placement() {
                return Err(Error::conflict(
                    "create Managed volume",
                    "volume already uses a different file extension access",
                ));
            }
            existing
        };
        let volume = ManagedVolume::new(
            format,
            self.operator.clone(),
            self.stream_concurrency,
            self.worksets,
            self.file_access.clone(),
            self.authority_access.clone(),
            self.authority_name.clone(),
        );
        volume.initialize().await?;
        Ok(volume)
    }

    async fn read_format(&self) -> Result<ManagedFormat, Error> {
        let control = storage::read_control(&self.operator, FORMAT_KEY, MAX_FORMAT_BYTES)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    "open Managed volume",
                    "Managed format does not exist",
                )
            })?;
        ManagedFormat::decode(&control.bytes)
    }

    /// Open the authority, optionally checking a replica's recorded volume identity.
    pub async fn open(&self, expected: Option<VolumeId>) -> Result<ManagedVolume, Error> {
        let format = self.read_format().await?;
        if expected.is_some_and(|expected| format.volume_id() != expected) {
            return Err(Error::invalid(
                "open Managed volume",
                "replica state belongs to a different volume",
            ));
        }
        match (format.file_placement(), self.file_access.as_ref()) {
            (format::FilePlacement::Extension(expected), Some(access))
                if access.info_dyn() == *expected => {}
            (format::FilePlacement::Extension(_), _) => {
                return Err(Error::unsupported(
                    "open Managed volume",
                    "the volume file extension access is not configured",
                ));
            }
            (format::FilePlacement::Whole | format::FilePlacement::Pack { .. }, None) => {}
            (format::FilePlacement::Whole | format::FilePlacement::Pack { .. }, Some(_)) => {
                return Err(Error::invalid(
                    "open Managed volume",
                    "a file extension access was configured for a core file layout",
                ));
            }
        }
        if format.authority_extension() != self.authority_access.info_dyn().as_ref() {
            return Err(Error::unsupported(
                "open Managed volume",
                "the volume namespace authority extension is not configured",
            ));
        }
        Ok(ManagedVolume::new(
            format,
            self.operator.clone(),
            self.stream_concurrency,
            self.worksets,
            self.file_access.clone(),
            self.authority_access.clone(),
            self.authority_name.clone(),
        ))
    }
}
