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
mod file;
mod format;
mod gc;
mod head;
mod layout;
mod namespace;
mod object;
mod publication;
mod record;
mod storage;
mod stream;
mod wire;

pub(crate) use data::FileDataRef;
pub(crate) use format::ManagedFormat;
pub use gc::GcOutcome;
pub(crate) use head::ManagedObservation;
pub use head::{ManagedVolume, NamespaceRevision};
pub(crate) use object::GcEpoch;

use std::num::NonZeroUsize;

use opendal::Operator;

use crate::filesystem::{NodeId, VolumeId};
use crate::workset::WorksetOptions;
use crate::{Error, ErrorKind};
use format::{FORMAT_KEY, MAX_FORMAT_BYTES};

/// Object Metadata authority for one Managed volume.
#[derive(Clone)]
pub struct ManagedMetadata {
    operator: Operator,
    stream_concurrency: usize,
    worksets: WorksetOptions,
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
        })
    }

    /// Create the Managed superblock once, or return the existing format.
    pub async fn initialize(&self) -> Result<ManagedVolume, Error> {
        let desired = ManagedFormat::new(VolumeId::generate(), NodeId::generate());
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
            self.read_format().await?
        };
        let volume = ManagedVolume::new(
            format,
            self.operator.clone(),
            self.stream_concurrency,
            self.worksets,
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
        Ok(ManagedVolume::new(
            format,
            self.operator.clone(),
            self.stream_concurrency,
            self.worksets,
        ))
    }
}
