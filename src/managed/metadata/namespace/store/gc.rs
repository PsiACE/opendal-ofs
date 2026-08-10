// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Namespace collection fencing and reachability discovery.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{NamespaceStore, StoredHead, encode_head};
use crate::filesystem::{ChangeCursor, OperationId, VolumeError, VolumeSnapshot};
use crate::managed::data::RetainedDataRoots;
use crate::managed::error::{conflict, corrupt, invalid};
use crate::managed::metadata::namespace::{
    ChangeSegmentRef, CheckpointRef, StoredNamespaceState, recover_namespace,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceGcSweep {
    pub(crate) epoch: u64,
    pub(crate) owner: [u8; 16],
    pub(crate) fixed_cursor: ChangeCursor,
}

#[derive(Default)]
pub(crate) struct RetainedMetadataReads {
    checkpoints: BTreeMap<[u8; 32], CheckpointRef>,
    changes: BTreeMap<[u8; 32], ChangeSegmentRef>,
}

impl NamespaceStore {
    pub(crate) async fn begin_gc(
        &self,
        resume: bool,
    ) -> Result<(NamespaceGcSweep, Option<VolumeSnapshot>), VolumeError> {
        if self.branch_id().is_some() {
            return Err(invalid(
                "begin Managed data collection",
                "branch collection belongs to its volume control plane",
            ));
        }
        let current = self.read_raw_head().await?;
        let (mut head, revision) = current
            .map(|(head, revision)| (head, Some(revision)))
            .unwrap_or_else(|| (StoredHead::unborn(self.volume_id, None), None));
        let owner = *OperationId::generate().as_bytes();
        if resume {
            let maintenance = head.maintenance.as_mut().ok_or_else(|| {
                conflict(
                    "resume Managed data collection",
                    "no interrupted collection is active",
                )
            })?;
            maintenance.owner = owner;
        } else {
            if head.maintenance.is_some() {
                return Err(conflict(
                    "begin Managed data collection",
                    "another collection is active",
                ));
            }
            head.maintenance_epoch = head.maintenance_epoch.checked_add(1).ok_or_else(|| {
                corrupt(
                    "begin Managed data collection",
                    "maintenance epoch is exhausted",
                )
            })?;
            head.maintenance = Some(NamespaceGcSweep {
                epoch: head.maintenance_epoch,
                owner,
                fixed_cursor: head.cursor(),
            });
        }
        let sweep = head.maintenance.expect("collection was installed above");
        let bytes = encode_head(&head)?;
        let replaced = match revision {
            Some(revision) => {
                self.backend
                    .replace(
                        &self.head_key,
                        &revision,
                        bytes,
                        "begin Managed data collection",
                    )
                    .await?
            }
            None => {
                self.backend
                    .create(&self.head_key, bytes, "begin Managed data collection")
                    .await?
            }
        };
        if !replaced {
            return Err(conflict(
                "begin Managed data collection",
                "namespace authority changed",
            ));
        }
        let Some(state) = &head.state else {
            return Ok((sweep, None));
        };
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        recover_namespace(checkpoint, state, self.volume_id).map(|snapshot| (sweep, Some(snapshot)))
    }

    pub(crate) async fn retain_state_data(
        &self,
        state: &StoredNamespaceState,
        roots: &mut RetainedDataRoots,
        reads: &mut RetainedMetadataReads,
    ) -> Result<(), VolumeError> {
        self.retain_checkpoint(state.checkpoint, roots, reads)
            .await?;
        for change in &state.tail {
            for version in change
                .mutation
                .file_versions
                .iter()
                .filter_map(|change| change.target.as_ref())
            {
                roots.retain_file_version(version)?;
            }
        }
        for reference in &state.segments {
            if let Some(current) = reads.changes.get(&reference.digest) {
                if current != reference {
                    return Err(corrupt(
                        "mark retained data segments",
                        "one change-segment digest has conflicting references",
                    ));
                }
                continue;
            }
            reads.changes.insert(reference.digest, *reference);
            let segment = self.read_change_segment(*reference).await?;
            self.retain_checkpoint(segment.checkpoint, roots, reads)
                .await?;
            for change in segment.changes {
                for version in change
                    .mutation
                    .file_versions
                    .iter()
                    .filter_map(|change| change.target.as_ref())
                {
                    roots.retain_file_version(version)?;
                }
            }
        }
        Ok(())
    }

    async fn retain_checkpoint(
        &self,
        reference: CheckpointRef,
        roots: &mut RetainedDataRoots,
        reads: &mut RetainedMetadataReads,
    ) -> Result<(), VolumeError> {
        if let Some(current) = reads.checkpoints.get(&reference.digest) {
            return if *current == reference {
                Ok(())
            } else {
                Err(corrupt(
                    "mark retained data segments",
                    "one checkpoint digest has conflicting references",
                ))
            };
        }
        reads.checkpoints.insert(reference.digest, reference);
        roots.retain(&self.read_checkpoint(reference).await?)
    }

    pub(crate) async fn finish_gc(&self, sweep: NamespaceGcSweep) -> Result<(), VolumeError> {
        let (mut head, revision) = self.read_raw_head().await?.ok_or_else(|| {
            if self.branch_id().is_some() {
                corrupt(
                    "finish Managed data collection",
                    "registered branch HEAD is missing",
                )
            } else {
                conflict("finish Managed data collection", "namespace disappeared")
            }
        })?;
        if head.maintenance != Some(sweep) {
            return Err(conflict(
                "finish Managed data collection",
                "collection fence changed",
            ));
        }
        head.maintenance = None;
        if self
            .backend
            .replace(
                &self.head_key,
                &revision,
                encode_head(&head)?,
                "finish Managed data collection",
            )
            .await?
        {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed data collection",
                "namespace authority changed",
            ))
        }
    }
}
