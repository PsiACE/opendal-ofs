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

use std::collections::BTreeMap;

use futures::TryStreamExt as _;

use crate::filesystem::{OperationId, VolumeError, VolumeErrorKind, VolumeSnapshot};

use super::ManagedVolume;
use super::data::{decode_descriptor, segment_key};
use super::head::GcFence;

const DATA_PREFIX: &str = "managed/1/objects/raw/";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOutcome {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
}

impl ManagedVolume {
    pub async fn collect_unreachable(&self, resume: bool) -> Result<GcOutcome, VolumeError> {
        let capability = self.operator().info().full_capability();
        if !capability.list || !capability.delete {
            return Err(VolumeError::new(
                VolumeErrorKind::Invalid,
                "collect Managed data: storage lacks list or delete",
            ));
        }
        let (fence, snapshot) = self.begin_gc(resume).await?;
        let live = live_objects(&snapshot)?;
        let outcome = self.sweep(&live).await?;
        self.finish_gc(fence).await?;
        Ok(outcome)
    }

    async fn begin_gc(&self, resume: bool) -> Result<(GcFence, VolumeSnapshot), VolumeError> {
        let (mut head, revision) = self.read_head().await?;
        let owner = OperationId::generate();
        let fence = match (resume, head.maintenance) {
            (false, None) => GcFence {
                owner,
                namespace_commit: head.namespace_commit,
            },
            (false, Some(_)) => {
                return Err(conflict(
                    "begin Managed data collection: another collection is active",
                ));
            }
            (true, Some(active)) if active.namespace_commit == head.namespace_commit => GcFence {
                owner,
                namespace_commit: active.namespace_commit,
            },
            (true, Some(_)) => {
                return Err(corrupt(
                    "resume Managed data collection: fence cursor is invalid",
                ));
            }
            (true, None) => {
                return Err(conflict(
                    "resume Managed data collection: no interrupted collection is active",
                ));
            }
        };
        head.maintenance = Some(fence);
        if !self.replace_head(&revision, &head).await? {
            return Err(conflict(
                "begin Managed data collection: namespace authority changed",
            ));
        }
        let snapshot = self.snapshot_at(head.namespace_commit).await?;
        Ok((fence, snapshot))
    }

    async fn sweep(&self, live: &BTreeMap<String, u64>) -> Result<GcOutcome, VolumeError> {
        let mut outcome = GcOutcome::default();
        let mut lister = self
            .operator()
            .lister_with(DATA_PREFIX)
            .recursive(true)
            .await
            .map_err(|_| unavailable("list Managed data objects"))?;
        let mut deleter = self
            .operator()
            .deleter()
            .await
            .map_err(|_| unavailable("open Managed data deleter"))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|_| unavailable("list Managed data objects"))?
        {
            if !entry.metadata().is_file() || !valid_object_key(entry.path()) {
                continue;
            }
            outcome.scanned += 1;
            let length = entry.metadata().content_length();
            if let Some(expected) = live.get(entry.path()) {
                if *expected != length {
                    return Err(corrupt("live Managed object length is invalid"));
                }
                continue;
            }
            deleter
                .delete(entry.path())
                .await
                .map_err(|_| unavailable("delete Managed data object"))?;
            outcome.deleted += 1;
            outcome.deleted_bytes = outcome
                .deleted_bytes
                .checked_add(length)
                .ok_or_else(|| corrupt("deleted Managed data byte count overflows"))?;
        }
        deleter
            .close()
            .await
            .map_err(|_| unavailable("finish Managed data deletion"))?;
        Ok(outcome)
    }

    async fn finish_gc(&self, fence: GcFence) -> Result<(), VolumeError> {
        let (mut head, revision) = self.read_head().await?;
        if head.maintenance != Some(fence) || head.namespace_commit != fence.namespace_commit {
            return Err(conflict(
                "finish Managed data collection: collection fence changed",
            ));
        }
        head.maintenance = None;
        if self.replace_head(&revision, &head).await? {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed data collection: namespace authority changed",
            ))
        }
    }
}

fn live_objects(snapshot: &VolumeSnapshot) -> Result<BTreeMap<String, u64>, VolumeError> {
    let mut live = BTreeMap::new();
    for version in snapshot.file_versions.values() {
        for segment in decode_descriptor(version)?.segments {
            let key = segment_key(segment.digest);
            if live
                .insert(key, segment.length)
                .is_some_and(|length| length != segment.length)
            {
                return Err(corrupt("one Managed object has conflicting lengths"));
            }
        }
    }
    Ok(live)
}

fn valid_object_key(path: &str) -> bool {
    path.strip_prefix(DATA_PREFIX).is_some_and(|suffix| {
        let Some((prefix, digest)) = suffix.split_once('/') else {
            return false;
        };
        prefix.len() == 2
            && digest.len() == 64
            && prefix == &digest[..2]
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

fn conflict(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Conflict, message)
}

fn corrupt(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Corrupt, message)
}

fn unavailable(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Unavailable, message)
}
