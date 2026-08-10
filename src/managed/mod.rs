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

mod data;
mod error;
mod format;
mod head;
mod history;
mod object;
mod record;

pub use format::ManagedFormat;
pub use head::{ManagedObservation, ManagedVolume};

use opendal::Operator;

use crate::filesystem::{VolumeError, VolumeId};
use error::{invalid, unavailable};
use format::{MAX_SUPERBLOCK_BYTES, SUPERBLOCK_KEY};

/// Object Metadata authority for one Managed volume.
#[derive(Clone)]
pub struct ManagedMetadata {
    operator: Operator,
}

impl ManagedMetadata {
    pub fn object(operator: Operator) -> Result<Self, VolumeError> {
        let capability = operator.info().full_capability();
        if !(capability.read
            && capability.write
            && capability.write_with_if_not_exists
            && capability.write_with_if_match)
        {
            return Err(invalid(
                "open Managed metadata",
                "storage lacks conditional record operations",
            ));
        }
        Ok(Self { operator })
    }

    /// Create the Managed superblock once, or return the existing format.
    pub async fn initialize(&self) -> Result<ManagedVolume, VolumeError> {
        let desired = ManagedFormat::v1(VolumeId::generate());
        let encoded = desired.encode()?;
        let format = if object::create(&self.operator, SUPERBLOCK_KEY, encoded).await? {
            desired
        } else {
            self.read_format().await?
        };
        let volume = ManagedVolume::new(format, self.operator.clone());
        volume.initialize().await?;
        Ok(volume)
    }

    pub async fn read_format(&self) -> Result<ManagedFormat, VolumeError> {
        let bytes = object::read(&self.operator, SUPERBLOCK_KEY, MAX_SUPERBLOCK_BYTES)
            .await?
            .ok_or_else(|| unavailable("open Managed volume", "Managed format does not exist"))?;
        ManagedFormat::decode(&bytes)
    }

    /// Open the authority only when it belongs to the replica's recorded volume.
    pub async fn open(&self, expected: VolumeId) -> Result<ManagedVolume, VolumeError> {
        let format = self.read_format().await?;
        if format.volume_id() != expected {
            return Err(invalid(
                "open Managed volume",
                "replica state belongs to a different volume",
            ));
        }
        ManagedVolume::open(format, self.operator.clone()).await
    }

    pub async fn open_unbound(&self) -> Result<ManagedVolume, VolumeError> {
        let format = self.read_format().await?;
        ManagedVolume::open(format, self.operator.clone()).await
    }
}
