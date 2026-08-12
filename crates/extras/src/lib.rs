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

//! Optional Managed extensions.

use ofs_core::filesystem::VolumeId;
use ofs_core::managed::extension::{
    ExtensionId, ExtentExtension as _, FileAccessInfo, FileLayoutExtension as _,
    IdentityExtentAccess,
};
use ofs_core::managed::{
    AuthorityExtension as _, DefaultAuthorityAccess, ManagedMetadata, ManagedVolume,
};
use ofs_core::{Error, ErrorKind};
use ofs_ext_branch::BranchExtension;
pub use ofs_ext_branch::{BRANCH_EXTENSION_ID, BranchManager};
use ofs_ext_fastcdc::{FASTCDC_EXTENSION_ID, FastCdcExtension};
use ofs_ext_zstd::{ZSTD_EXTENSION_ID, ZstdExtension};

/// Supported file-extension composition selected when a volume is created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileExtensions {
    /// FastCDC layout over identity extents.
    FastCdc,
    /// FastCDC layout over independently compressed Zstandard extents.
    FastCdcZstd { level: i32 },
}

/// Configure the Branch namespace authority extension.
pub fn with_branch(metadata: ManagedMetadata, name: impl Into<String>) -> ManagedMetadata {
    metadata.with_authority_extension(BranchExtension::new().extend(DefaultAuthorityAccess), name)
}

impl FileExtensions {
    /// Configure a metadata handle with this statically composed access.
    pub fn configure(self, metadata: ManagedMetadata) -> ManagedMetadata {
        match self {
            Self::FastCdc => {
                metadata.with_file_extension(FastCdcExtension::new().extend(IdentityExtentAccess))
            }
            Self::FastCdcZstd { level } => metadata.with_file_extension(
                FastCdcExtension::new()
                    .extend(ZstdExtension::new(level).extend(IdentityExtentAccess)),
            ),
        }
    }
}

/// Configure the extension access described by an existing volume.
pub async fn configure_existing(
    metadata: ManagedMetadata,
    authority: impl Into<String>,
) -> Result<ManagedMetadata, Error> {
    let file = metadata.file_extension().await?;
    let authority_extension = metadata.authority_extension().await?;
    let metadata = match file {
        Some(info) => detect(&info)?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unsupported,
                    "open Managed volume",
                    "the volume uses an unavailable file extension composition",
                )
            })?
            .configure(metadata),
        None => metadata,
    };
    match authority_extension {
        Some(extension) if extension.id == BRANCH_EXTENSION_ID => {
            Ok(with_branch(metadata, authority))
        }
        Some(_) => Err(Error::new(
            ErrorKind::Unsupported,
            "open Managed volume",
            "the volume uses an unavailable namespace authority extension",
        )),
        None => Ok(metadata),
    }
}

/// Configure and open an existing volume.
pub async fn open(
    metadata: ManagedMetadata,
    expected: Option<VolumeId>,
    authority: impl Into<String>,
) -> Result<ManagedVolume, Error> {
    configure_existing(metadata, authority)
        .await?
        .open(expected)
        .await
}

fn detect(info: &FileAccessInfo) -> Result<Option<FileExtensions>, Error> {
    if info.layout.id != FASTCDC_EXTENSION_ID {
        return Ok(None);
    }
    let identities = info
        .extents
        .iter()
        .map(|extension| extension.id)
        .collect::<Vec<ExtensionId>>();
    if identities == [ExtensionId::IDENTITY] {
        return Ok(Some(FileExtensions::FastCdc));
    }
    if identities == [ZSTD_EXTENSION_ID, ExtensionId::IDENTITY] {
        let mut input = info.extents[0].configuration.as_slice();
        let level = ciborium::from_reader(&mut input).map_err(|_| {
            Error::new(
                ErrorKind::Corrupt,
                "open Managed volume",
                "Zstandard extension configuration is invalid",
            )
        })?;
        if !input.is_empty() {
            return Err(Error::new(
                ErrorKind::Corrupt,
                "open Managed volume",
                "Zstandard extension configuration has trailing bytes",
            ));
        }
        return Ok(Some(FileExtensions::FastCdcZstd { level }));
    }
    Ok(None)
}
