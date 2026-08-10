// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Branch registry records. Namespace state is shared with the base authority.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::filesystem::{BranchBinding, BranchId, BranchName, ChangeCursor, VolumeError, VolumeId};
use crate::managed::error::corrupt;
use crate::managed::metadata::namespace::StoredHead;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchLifecycle {
    Active,
    Sealed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchInfo {
    pub binding: BranchBinding,
    pub lifecycle: BranchLifecycle,
    pub cursor: ChangeCursor,
    pub is_default: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForkPoint {
    Head,
    Sequence(u64),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredBranchRegistry {
    pub(crate) volume_id: VolumeId,
    pub(crate) default_branch: BranchId,
    pub(crate) branches: BTreeMap<BranchName, BranchId>,
    pub(crate) maintenance_epoch: u64,
    pub(crate) maintenance_owner: Option<[u8; 16]>,
}

impl StoredBranchRegistry {
    pub(crate) fn initial(
        volume_id: VolumeId,
        default_name: BranchName,
        default_id: BranchId,
    ) -> Self {
        Self {
            volume_id,
            default_branch: default_id,
            branches: BTreeMap::from([(default_name, default_id)]),
            maintenance_epoch: 0,
            maintenance_owner: None,
        }
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        let unique_ids = self.branches.values().copied().collect::<BTreeSet<_>>();
        if self.volume_id != volume_id
            || unique_ids.len() != self.branches.len()
            || !self.branches.values().any(|id| *id == self.default_branch)
            || self.maintenance_owner.is_some() && self.maintenance_epoch == 0
        {
            return Err(corrupt("read Managed branch", "branch registry is invalid"));
        }
        Ok(())
    }

    pub(crate) fn default_binding(&self) -> BranchBinding {
        self.branches
            .iter()
            .find(|(_, id)| **id == self.default_branch)
            .map(|(name, id)| BranchBinding {
                name: name.clone(),
                id: *id,
            })
            .expect("validated branch registries contain the default branch")
    }
}

pub(crate) fn info(
    name: BranchName,
    id: BranchId,
    head: &StoredHead,
    default: BranchId,
) -> BranchInfo {
    BranchInfo {
        binding: BranchBinding { name, id },
        lifecycle: if head.sealed {
            BranchLifecycle::Sealed
        } else {
            BranchLifecycle::Active
        },
        cursor: head.cursor(),
        is_default: id == default,
    }
}
