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

//! Immutable data segments and file extent materialization.

use std::collections::BTreeSet;
use std::sync::Arc;

use opendal::{Buffer, ErrorKind, Operator};
use sha2::{Digest as _, Sha256};
use tokio::sync::OnceCell;

use super::error::{corrupt, invalid, unavailable};
use crate::filesystem::VolumeError;
use crate::managed::format::{ContentRef, LowerHex, SegmentRef, V1Record};

mod gc;
mod read;
mod stage;

pub(crate) use gc::RetainedDataRoots;
pub use gc::SegmentGcMaintenance;
pub(crate) use stage::AuthorityKnownContent;

const SEGMENT_ROOT: &str = ".ofs/managed/data/v1/segments/sha256";
const STAGING_PLAN_KEY: &str = "plan.ofs";
const STAGING_PLAN_RECORD: V1Record = V1Record::new(*b"OFS1DSP1", 64 * 1024 * 1024);
// Placement policy. These values are not part of the durable format.
const TARGET_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

/// The Managed v1 data plane.
#[derive(Clone)]
pub(crate) struct ManagedData {
    operator: Operator,
    cached: Arc<OnceCell<Operator>>,
}

impl ManagedData {
    pub(crate) fn new(operator: Operator) -> Result<Self, VolumeError> {
        let capability = operator.info().full_capability();
        if !capability.read || !capability.write || !capability.write_with_if_not_exists {
            return Err(invalid(
                "open Managed data",
                "data storage requires read, write, and create-only write",
            ));
        }
        Ok(Self {
            operator,
            cached: Arc::new(OnceCell::new()),
        })
    }
}

fn buffer_content_ref(bytes: &Buffer) -> ContentRef {
    let mut digest = Sha256::new();
    for chunk in bytes.clone() {
        digest.update(&chunk);
    }
    ContentRef {
        digest: digest.finalize().into(),
        length: bytes.len() as u64,
    }
}

async fn read_staging_plan(staging: &Operator) -> Result<BTreeSet<SegmentRef>, VolumeError> {
    let bytes = staging.read(STAGING_PLAN_KEY).await.map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            corrupt("finalize Managed files", "staged segment plan is missing")
        } else {
            unavailable("finalize Managed files", "storage operation failed")
        }
    })?;
    STAGING_PLAN_RECORD
        .decode(&bytes.to_bytes())
        .map_err(|_| corrupt("finalize Managed files", "staged segment plan is invalid"))
}

fn verify_complete_segment(reference: SegmentRef, bytes: &Buffer) -> Result<(), VolumeError> {
    if bytes.len() as u64 != reference.length
        || buffer_content_ref(bytes).digest != reference.digest
    {
        return Err(corrupt(
            "read data segment",
            "segment does not match its reference",
        ));
    }
    Ok(())
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        digest: Sha256::digest(bytes).into(),
        length: bytes.len() as u64,
    }
}

fn segment_key(reference: SegmentRef) -> String {
    let digest = LowerHex::encode(&reference.digest);
    format!("{SEGMENT_ROOT}/{}/{}.seg", &digest[..2], digest)
}

fn referenced_segment_error(action: &'static str, error: opendal::Error) -> VolumeError {
    if error.kind() == ErrorKind::NotFound {
        corrupt(action, "file version references a missing data segment")
    } else {
        unavailable(action, "storage operation failed")
    }
}
