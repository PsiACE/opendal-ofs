// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Collection fencing across every registered branch namespace.

use super::{BRANCH_CAS_ATTEMPTS, BranchStore};
use crate::filesystem::{BranchBinding, BranchId, OperationId, VolumeError};
use crate::managed::ManagedData;
use crate::managed::data::{RetainedDataRoots, SegmentGcMaintenance};
use crate::managed::error::{conflict, corrupt};
use crate::managed::metadata::namespace::{
    NamespaceGcSweep, RetainedMetadataReads, StoredNamespaceState,
};

impl BranchStore {
    /// Collect data unreachable from every current and retained branch position.
    pub async fn garbage_collect(&self, resume: bool) -> Result<SegmentGcMaintenance, VolumeError> {
        let (mut registry, revision) = self.registry().await?;
        let owner = *OperationId::generate().as_bytes();
        if resume {
            if registry.maintenance_owner.is_none() {
                return Err(conflict(
                    "resume Managed data collection",
                    "no interrupted collection is active",
                ));
            }
        } else {
            if registry.maintenance_owner.is_some() {
                return Err(conflict(
                    "begin Managed data collection",
                    "another collection is active",
                ));
            }
            registry.maintenance_epoch =
                registry.maintenance_epoch.checked_add(1).ok_or_else(|| {
                    corrupt(
                        "begin Managed data collection",
                        "maintenance epoch is exhausted",
                    )
                })?;
        }
        registry.maintenance_owner = Some(owner);
        if !self
            .replace_registry(&revision, &registry, "begin Managed data collection")
            .await?
        {
            return Err(conflict(
                "begin Managed data collection",
                "branch registry changed",
            ));
        }

        let mut roots = RetainedDataRoots::default();
        let mut reads = RetainedMetadataReads::default();
        let mut sweeps = Vec::with_capacity(registry.branches.len());
        for (name, id) in &registry.branches {
            let namespace = self.namespace(BranchBinding {
                name: name.clone(),
                id: *id,
            });
            let (sweep, state) = self
                .lock_head(*id, registry.maintenance_epoch, owner)
                .await?;
            if let Some(state) = &state {
                namespace
                    .retain_state_data(state, &mut roots, &mut reads)
                    .await?;
            }
            sweeps.push((namespace, sweep));
        }
        let (mut current, revision) = self.registry().await?;
        if current.maintenance_owner != Some(owner)
            || current.maintenance_epoch != registry.maintenance_epoch
            || current.branches != registry.branches
        {
            return Err(conflict(
                "mark retained data segments",
                "branch registry collection fence changed",
            ));
        }

        let result = ManagedData::new(self.data.clone())?
            .collect_unreachable_segments(&roots)
            .await?;
        for (namespace, sweep) in sweeps {
            namespace.finish_gc(sweep).await?;
        }
        current.maintenance_owner = None;
        if !self
            .replace_registry(&revision, &current, "finish Managed data collection")
            .await?
        {
            return Err(conflict(
                "finish Managed data collection",
                "branch registry changed",
            ));
        }
        Ok(result)
    }

    async fn lock_head(
        &self,
        id: BranchId,
        epoch: u64,
        owner: [u8; 16],
    ) -> Result<(NamespaceGcSweep, Option<StoredNamespaceState>), VolumeError> {
        for _ in 0..BRANCH_CAS_ATTEMPTS {
            let (mut head, revision) = self.read_head(id).await?.ok_or_else(|| {
                corrupt(
                    "begin Managed data collection",
                    "registered branch HEAD is missing",
                )
            })?;
            if head.maintenance_epoch > epoch
                || head
                    .maintenance
                    .is_some_and(|maintenance| maintenance.epoch != epoch)
            {
                return Err(conflict(
                    "begin Managed data collection",
                    "branch collection fence changed",
                ));
            }
            let sweep = NamespaceGcSweep {
                epoch,
                owner,
                fixed_cursor: head.cursor(),
            };
            head.maintenance_epoch = epoch;
            head.maintenance = Some(sweep);
            if self
                .replace_head(id, &revision, &head, "begin Managed data collection")
                .await?
            {
                return Ok((sweep, head.state));
            }
        }
        Err(conflict(
            "begin Managed data collection",
            "branch HEAD kept changing",
        ))
    }
}
