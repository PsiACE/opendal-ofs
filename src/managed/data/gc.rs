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

//! Reachability roots and the native OpenDAL segment sweep.

use std::collections::BTreeMap;

use futures::TryStreamExt as _;

use super::{ManagedData, SEGMENT_ROOT};
use crate::filesystem::{FileVersion, VolumeError, VolumeSnapshot};
use crate::managed::error::{corrupt, unavailable};
use crate::managed::format::{LowerHex, SegmentRef};
use crate::managed::metadata::namespace::decode_file_version;

const ACTION: &str = "collect unreachable data segments";

/// Data segments removed by one explicit reachability sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentGcMaintenance {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
}

/// Immutable segments retained by one or more fixed namespace roots.
#[derive(Default)]
pub(crate) struct RetainedDataRoots(BTreeMap<[u8; 32], u64>);

impl RetainedDataRoots {
    pub(crate) fn retain(&mut self, snapshot: &VolumeSnapshot) -> Result<(), VolumeError> {
        for version in snapshot.file_versions.values() {
            self.retain_file_version(version)?;
        }
        Ok(())
    }

    pub(crate) fn retain_file_version(&mut self, version: &FileVersion) -> Result<(), VolumeError> {
        let version = decode_file_version(version)?;
        for extent in version.extent_map.extents {
            if self
                .0
                .insert(extent.segment.digest, extent.segment.length)
                .is_some_and(|length| length != extent.segment.length)
            {
                return Err(corrupt(
                    "mark retained data segments",
                    "one segment digest has conflicting physical lengths",
                ));
            }
        }
        Ok(())
    }
}

impl ManagedData {
    pub(crate) async fn collect_unreachable_segments(
        &self,
        roots: &RetainedDataRoots,
    ) -> Result<SegmentGcMaintenance, VolumeError> {
        let capability = self.operator.info().full_capability();
        if !capability.list || !capability.delete {
            return Err(unavailable(ACTION, "data storage requires list and delete"));
        }
        let mut result = SegmentGcMaintenance::default();
        let mut deleter = self.operator.deleter().await.map_err(storage_error)?;
        let mut entries = self
            .operator
            .lister_with(&format!("{SEGMENT_ROOT}/"))
            .recursive(true)
            .await
            .map_err(storage_error)?;
        while let Some(entry) = entries.try_next().await.map_err(storage_error)? {
            if !entry.metadata().is_file() {
                continue;
            }
            let Some(reference) =
                segment_ref_from_key(entry.path(), entry.metadata().content_length())
            else {
                continue;
            };
            result.scanned += 1;
            if let Some(length) = roots.0.get(&reference.digest) {
                if *length != reference.length {
                    return Err(corrupt(
                        "collect unreachable data segments",
                        "live segment has an unexpected physical length",
                    ));
                }
                continue;
            }
            deleter.delete(entry.path()).await.map_err(storage_error)?;
            result.deleted += 1;
            result.deleted_bytes = result
                .deleted_bytes
                .checked_add(reference.length)
                .ok_or_else(|| {
                    corrupt(
                        "collect unreachable data segments",
                        "deleted byte count overflows",
                    )
                })?;
        }
        deleter.close().await.map_err(storage_error)?;
        Ok(result)
    }
}

fn storage_error(_: opendal::Error) -> VolumeError {
    unavailable(ACTION, "storage operation failed")
}

fn segment_ref_from_key(path: &str, length: u64) -> Option<SegmentRef> {
    let relative = path.strip_prefix(SEGMENT_ROOT)?.strip_prefix('/')?;
    let (shard, name) = relative.split_once('/')?;
    let digest = name.strip_suffix(".seg")?;
    if shard.len() != 2 || digest.len() != 64 || !digest.starts_with(shard) {
        return None;
    }
    let digest: [u8; 32] = LowerHex::decode(digest)?.try_into().ok()?;
    Some(SegmentRef { digest, length })
}
