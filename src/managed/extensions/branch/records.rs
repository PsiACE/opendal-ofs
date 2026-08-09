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

use crate::filesystem::{BranchBinding, BranchId, BranchName, ChangeCursor, VolumeId};
use crate::managed::metadata::namespace::StoredHead;
use crate::managed::{ManagedError, ManagedErrorKind};

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
    pub(crate) maintenance_active: bool,
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
            maintenance_active: false,
            maintenance_owner: None,
        }
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        let unique_ids = self.branches.values().copied().collect::<BTreeSet<_>>();
        if self.volume_id != volume_id
            || unique_ids.len() != self.branches.len()
            || !self.branches.values().any(|id| *id == self.default_branch)
            || self.maintenance_active
                && (self.maintenance_epoch == 0 || self.maintenance_owner.is_none())
        {
            return Err(corrupt("branch registry is invalid"));
        }
        Ok(())
    }

    pub(crate) fn branch_id(&self, name: &BranchName) -> Option<BranchId> {
        self.branches.get(name).copied()
    }

    pub(crate) fn default_binding(&self) -> Option<BranchBinding> {
        self.branches
            .iter()
            .find(|(_, id)| **id == self.default_branch)
            .map(|(name, id)| BranchBinding {
                name: name.clone(),
                id: *id,
            })
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

fn corrupt(message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, "read Managed branch", message)
}
