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

//! Create and open a Managed volume from its experimental v0 format.

use opendal::Operator;

use crate::Error;
use crate::ErrorKind;
use crate::filesystem::{NodeId, VolumeId};
use crate::format::{FORMAT_KEY, FORMAT_RECORD, FileDataLayout, VolumeFormat};
use crate::storage::ControlRecord;

const FORMAT: ControlRecord<VolumeFormat> = ControlRecord::new(FORMAT_KEY, FORMAT_RECORD);

/// User choices that become a persisted `VolumeFormat`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateOptions {
    file_data_layout: FileDataLayout,
}

impl CreateOptions {
    pub fn new(file_data_layout: FileDataLayout) -> Self {
        Self { file_data_layout }
    }

    pub const fn file_data_layout(&self) -> &FileDataLayout {
        &self.file_data_layout
    }
}

/// Opened Managed volume facade for layout v0.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedVolume {
    format: VolumeFormat,
}

impl ManagedVolume {
    pub const fn format(&self) -> &VolumeFormat {
        &self.format
    }

    pub const fn id(&self) -> VolumeId {
        self.format.volume_id()
    }

    /// Create a volume in empty storage, or reopen it when the same layout exists.
    pub async fn create(operator: &Operator, options: CreateOptions) -> Result<Self, Error> {
        require_control_capabilities(operator)?;
        let format = VolumeFormat::new(
            VolumeId::generate(),
            NodeId::generate(),
            options.file_data_layout,
            None,
        );
        if FORMAT.write(operator, &format, None).await? {
            return Ok(Self { format });
        }
        let existing = Self::open(operator).await?;
        if existing.format.file_data_layout() != format.file_data_layout()
            || existing.format.authority() != format.authority()
        {
            return Err(Error::conflict(
                "create Managed volume",
                "storage already contains a different volume layout",
            ));
        }
        Ok(existing)
    }

    /// Read the volume format from storage.
    pub async fn open(operator: &Operator) -> Result<Self, Error> {
        require_control_capabilities(operator)?;
        let observed = FORMAT.read(operator).await?.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "open Managed volume",
                "volume format is missing",
            )
        })?;
        Ok(Self {
            format: observed.value,
        })
    }
}

fn require_control_capabilities(operator: &Operator) -> Result<(), Error> {
    let capability = operator.info().full_capability();
    if capability.read && capability.write && capability.write_with_if_not_exists {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::Unsupported,
        "open Managed volume",
        "storage lacks conditional create for the volume format",
    ))
}
