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

//! Transactional storage for the Managed branch control plane.

use std::collections::{BTreeMap, BTreeSet};

use opendal::Operator;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::D1BoundNamespace;
use super::checkpoint::{CheckpointPart, CheckpointRoot, PendingCheckpoint};
use super::namespace::BranchNamespaceStore;
use super::records::{
    BranchInfo, BranchLifecycle, ForkPoint, StoredBranchHead, StoredBranchRegistry,
    StoredCheckpoint, StoredHistory, StoredNamespaceState, info,
};
use crate::filesystem::{BranchBinding, BranchId, BranchName, OperationId, VolumeId};
use crate::managed::metadata::d1::{D1Result, D1Session, D1Statement, statement};
use crate::managed::metadata::namespace::NamespaceSnapshot;
use crate::managed::{
    D1Metadata, ManagedData, ManagedError, ManagedErrorKind, SegmentGcMaintenance,
};

const REGISTRY: &str = "ofs_managed_branch_v1_registry";
const HEADS: &str = "ofs_managed_branch_v1_heads";
const CHECKPOINTS: &str = "ofs_managed_branch_v1_checkpoints";
const CHECKPOINT_PARTS: &str = "ofs_managed_branch_v1_checkpoint_parts";
const HISTORY: &str = "ofs_managed_branch_v1_history";
const SCHEMA_RESULTS: usize = 5;
// D1 imposes this value boundary. It is a provider constraint, not a separate
// branch-format policy; checkpoint contents are split before reaching it.
const MAX_D1_VALUE_BYTES: usize = 2_000_000;
const MAX_REGISTRY_BYTES: usize = MAX_D1_VALUE_BYTES;
const MAX_HEAD_BYTES: usize = MAX_D1_VALUE_BYTES;
const MAX_CHECKPOINT_ROOT_BYTES: usize = MAX_D1_VALUE_BYTES;
const MAX_CHECKPOINT_PART_BYTES: usize = MAX_D1_VALUE_BYTES;
const MAX_HISTORY_BYTES: usize = MAX_D1_VALUE_BYTES;
const CHECKPOINT_READ_PAGE: usize = 32;
const METADATA_PAGE_SIZE: usize = 1000;
const MAX_DELETE_IDS_BYTES: usize = 96 * 1024;

#[derive(Clone)]
pub struct D1BranchStore {
    volume_id: VolumeId,
    session: D1Session,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct D1BranchGcFence {
    epoch: u64,
    owner: [u8; 16],
}

impl D1BranchGcFence {
    fn owns_registry(self, registry: &StoredBranchRegistry) -> bool {
        registry.maintenance_epoch == self.epoch && registry.maintenance_owner == Some(self.owner)
    }

    fn owns_head(self, head: &StoredBranchHead) -> bool {
        head.maintenance_epoch == self.epoch && head.maintenance_owner == Some(self.owner)
    }
}

impl BranchNamespaceStore for D1BranchStore {
    type Revision = (u64, u64);

    fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    async fn current_head(
        &self,
        binding: &BranchBinding,
        action: &'static str,
    ) -> Result<(StoredBranchHead, Self::Revision), ManagedError> {
        let (registry, registry_revision) = self.registry().await?;
        if registry.maintenance_active {
            return Err(conflict(action, "branch maintenance is active"));
        }
        if registry.branch_id(&binding.name) != Some(binding.id) {
            return Err(conflict(action, "branch incarnation no longer exists"));
        }
        let (head, head_revision) = self
            .read_head(binding.id)
            .await?
            .ok_or_else(|| conflict(action, "branch incarnation no longer exists"))?;
        if head.lifecycle != BranchLifecycle::Active {
            return Err(conflict(action, "branch is sealed for deletion"));
        }
        Ok((head, (head_revision, registry_revision)))
    }

    async fn replace_head(
        &self,
        branch: BranchId,
        revision: &Self::Revision,
        head: &StoredBranchHead,
    ) -> Result<bool, ManagedError> {
        D1BranchStore::replace_head(
            self,
            branch,
            revision.0,
            revision.1,
            head,
            "publish Managed branch",
        )
        .await
    }

    async fn read_checkpoint(&self, id: [u8; 32]) -> Result<StoredCheckpoint, ManagedError> {
        D1BranchStore::read_checkpoint(self, id).await
    }

    async fn write_checkpoint(
        &self,
        checkpoint: &StoredCheckpoint,
    ) -> Result<[u8; 32], ManagedError> {
        D1BranchStore::write_checkpoint(self, checkpoint).await
    }

    async fn write_history(&self, history: &StoredHistory) -> Result<[u8; 32], ManagedError> {
        D1BranchStore::write_history(self, history).await
    }
}

struct D1GcRoots {
    snapshots: Vec<NamespaceSnapshot>,
    heads: BTreeSet<String>,
    checkpoints: BTreeSet<String>,
    checkpoint_parts: BTreeSet<String>,
    histories: BTreeSet<String>,
}

impl D1BranchStore {
    pub fn new(volume_id: VolumeId, metadata: D1Metadata) -> Self {
        Self {
            volume_id,
            session: metadata.session(),
        }
    }

    pub async fn initialize(&self, default_name: BranchName) -> Result<BranchInfo, ManagedError> {
        if let Some((registry, _)) = self.read_registry().await? {
            return self.initialize_existing(default_name, registry).await;
        }

        let branch_id = BranchId::generate();
        let head = StoredBranchHead::unborn(self.volume_id, branch_id);
        let registry =
            StoredBranchRegistry::initial(self.volume_id, default_name.clone(), branch_id);
        let head_json = encode(&head, MAX_HEAD_BYTES, "initialize Managed branches")?;
        let registry_json = encode(&registry, MAX_REGISTRY_BYTES, "initialize Managed branches")?;
        let mut batch = schema_statements();
        batch.extend([
            statement(
                format!(
                    "INSERT OR IGNORE INTO {HEADS} (store_key, branch_id, revision, lifecycle, record_json) VALUES (?, ?, 1, 'active', ?)"
                ),
                vec![
                    self.store_key().into(),
                    branch_id.to_string().into(),
                    head_json.into(),
                ],
            ),
            statement(
                format!(
                    "INSERT OR IGNORE INTO {REGISTRY} (store_key, volume_id, revision, maintenance_epoch, maintenance_state, maintenance_owner, record_json) VALUES (?, ?, 1, 0, 'idle', NULL, ?)"
                ),
                vec![
                    self.store_key().into(),
                    self.volume_id.to_string().into(),
                    registry_json.into(),
                ],
            ),
            statement(
                format!(
                    "SELECT revision, volume_id, maintenance_epoch, maintenance_state, maintenance_owner, record_json FROM {REGISTRY} WHERE store_key = ?"
                ),
                vec![self.store_key().into()],
            ),
        ]);
        let results = self
            .session
            .query(batch, "initialize Managed branches")
            .await?;
        let rows = rows(&results, SCHEMA_RESULTS + 2, "initialize Managed branches")?;
        let observed = decode_registry_row(rows, self.volume_id, "initialize Managed branches")?
            .ok_or_else(|| unavailable("initialize Managed branches"))?;
        self.initialize_existing(default_name, observed.0).await
    }

    async fn initialize_existing(
        &self,
        default_name: BranchName,
        registry: StoredBranchRegistry,
    ) -> Result<BranchInfo, ManagedError> {
        let default = BranchId::from_bytes(registry.default_branch);
        if registry.branch_id(&default_name) != Some(default) {
            return Err(conflict(
                "initialize Managed branches",
                "the volume has another default branch",
            ));
        }
        self.get(&default_name).await
    }

    pub async fn list(&self) -> Result<Vec<BranchInfo>, ManagedError> {
        let (registry, _) = self.registry().await?;
        let default = BranchId::from_bytes(registry.default_branch);
        let mut branches = Vec::with_capacity(registry.branches.len());
        for (name, id) in registry.branches {
            let id = BranchId::from_bytes(id);
            let (head, _) = self.read_head(id).await?.ok_or_else(|| {
                corrupt("list Managed branches", "registered branch HEAD is missing")
            })?;
            branches.push(info(name, id, &head, default)?);
        }
        Ok(branches)
    }

    pub async fn default_name(&self) -> Result<BranchName, ManagedError> {
        let (registry, _) = self.registry().await?;
        let mut names = registry
            .branches
            .into_iter()
            .filter_map(|(name, id)| (id == registry.default_branch).then_some(name));
        let name = names
            .next()
            .ok_or_else(|| corrupt("read Managed branches", "default branch is missing"))?;
        if names.next().is_some() {
            return Err(corrupt(
                "read Managed branches",
                "default branch identity is ambiguous",
            ));
        }
        Ok(name)
    }

    pub async fn get(&self, name: &BranchName) -> Result<BranchInfo, ManagedError> {
        let (registry, _) = self.registry().await?;
        let id = registry
            .branch_id(name)
            .ok_or_else(|| not_found("show Managed branch"))?;
        let (head, _) = self
            .read_head(id)
            .await?
            .ok_or_else(|| corrupt("show Managed branch", "registered branch HEAD is missing"))?;
        info(
            name.clone(),
            id,
            &head,
            BranchId::from_bytes(registry.default_branch),
        )
    }

    pub async fn bind(&self, name: &BranchName) -> Result<D1BoundNamespace, ManagedError> {
        let branch = self.get(name).await?;
        if branch.lifecycle != BranchLifecycle::Active {
            return Err(conflict(
                "bind Managed branch",
                "branch is sealed for deletion",
            ));
        }
        Ok(D1BoundNamespace {
            store: self.clone(),
            binding: branch.binding,
        })
    }

    pub async fn fork(
        &self,
        source_name: &BranchName,
        point: ForkPoint,
        target_name: BranchName,
    ) -> Result<BranchInfo, ManagedError> {
        let (mut registry, registry_revision) = self.registry().await?;
        if registry.maintenance_active {
            return Err(conflict(
                "fork Managed branch",
                "branch maintenance is active",
            ));
        }
        if registry.branch_id(&target_name).is_some() {
            return Err(conflict(
                "fork Managed branch",
                "target branch already exists",
            ));
        }
        let source_id = registry
            .branch_id(source_name)
            .ok_or_else(|| not_found("fork Managed branch"))?;
        let (source, source_revision) = self
            .read_head(source_id)
            .await?
            .ok_or_else(|| corrupt("fork Managed branch", "source branch HEAD is missing"))?;
        if source.lifecycle != BranchLifecycle::Active {
            return Err(conflict(
                "fork Managed branch",
                "source branch is sealed for deletion",
            ));
        }

        let target_state = match point {
            ForkPoint::Head => source.state.clone(),
            ForkPoint::Sequence(0) => None,
            ForkPoint::Sequence(sequence) => match &source.state {
                None => return Err(position_not_retained()),
                Some(state) => match state.at_sequence(sequence) {
                    Some(state) => Some(state),
                    None => self
                        .state_from_history(state.previous_history, sequence)
                        .await?
                        .map(Some)
                        .ok_or_else(position_not_retained)?,
                },
            },
        };
        let target_id = BranchId::generate();
        let target = StoredBranchHead {
            major: source.major,
            volume_id: source.volume_id,
            branch_id: *target_id.as_bytes(),
            lifecycle: BranchLifecycle::Active,
            state: target_state,
            maintenance_epoch: source.maintenance_epoch,
            maintenance_active: source.maintenance_active,
            maintenance_owner: source.maintenance_owner,
        };
        target.validate(self.volume_id, target_id)?;
        registry
            .branches
            .insert(target_name.clone(), *target_id.as_bytes());
        let registry_json = encode(&registry, MAX_REGISTRY_BYTES, "fork Managed branch")?;
        let head_json = encode(&target, MAX_HEAD_BYTES, "fork Managed branch")?;
        let next_registry_revision = checked_revision(registry_revision, "fork Managed branch")?;
        let mut batch = schema_statements();
        batch.extend([
            statement(
                format!(
                    "INSERT OR IGNORE INTO {HEADS} (store_key, branch_id, revision, lifecycle, record_json) SELECT ?, ?, 1, 'active', ? WHERE EXISTS (SELECT 1 FROM {REGISTRY} WHERE store_key = ? AND revision = ? AND maintenance_state = 'idle') AND EXISTS (SELECT 1 FROM {HEADS} WHERE store_key = ? AND branch_id = ? AND revision = ? AND lifecycle = 'active') RETURNING revision"
                ),
                vec![
                    self.store_key().into(),
                    target_id.to_string().into(),
                    head_json.into(),
                    self.store_key().into(),
                    sqlite_integer(registry_revision, "fork Managed branch")?.into(),
                    self.store_key().into(),
                    source_id.to_string().into(),
                    sqlite_integer(source_revision, "fork Managed branch")?.into(),
                ],
            ),
            statement(
                format!(
                    "UPDATE {REGISTRY} SET revision = revision + 1, record_json = ? WHERE store_key = ? AND revision = ? AND maintenance_state = 'idle' AND EXISTS (SELECT 1 FROM {HEADS} WHERE store_key = ? AND branch_id = ? AND revision = 1 AND lifecycle = 'active') RETURNING revision"
                ),
                vec![
                    registry_json.into(),
                    self.store_key().into(),
                    sqlite_integer(registry_revision, "fork Managed branch")?.into(),
                    self.store_key().into(),
                    target_id.to_string().into(),
                ],
            ),
        ]);
        let results = match self.session.query(batch, "fork Managed branch").await {
            Ok(results) => results,
            Err(error) => {
                return match self.get(&target_name).await {
                    Ok(current) if current.binding.id == target_id => Ok(current),
                    Ok(_) => Err(conflict(
                        "fork Managed branch",
                        "target branch was created concurrently",
                    )),
                    Err(observed) if observed.kind() == ManagedErrorKind::Invalid => Err(error),
                    Err(observed) => Err(observed),
                };
            }
        };
        let changed = rows(&results, SCHEMA_RESULTS + 1, "fork Managed branch")?;
        if changed.len() == 1
            && integer(&changed[0], "revision", "fork Managed branch")? == next_registry_revision
        {
            return info(
                target_name,
                target_id,
                &target,
                BranchId::from_bytes(registry.default_branch),
            );
        }
        if changed.len() > 1 {
            return Err(corrupt(
                "fork Managed branch",
                "D1 returned duplicate registry rows",
            ));
        }
        self.resolve_fork(target_name, target_id).await
    }

    async fn resolve_fork(
        &self,
        target_name: BranchName,
        target_id: BranchId,
    ) -> Result<BranchInfo, ManagedError> {
        let current = self.get(&target_name).await?;
        if current.binding.id == target_id {
            Ok(current)
        } else {
            Err(conflict(
                "fork Managed branch",
                "target branch was created concurrently",
            ))
        }
    }

    pub async fn delete(&self, name: &BranchName) -> Result<(), ManagedError> {
        let (mut registry, registry_revision) = self.registry().await?;
        if registry.maintenance_active {
            return Err(conflict(
                "delete Managed branch",
                "branch maintenance is active",
            ));
        }
        let branch_id = registry
            .branch_id(name)
            .ok_or_else(|| not_found("delete Managed branch"))?;
        if registry.default_branch == *branch_id.as_bytes() {
            return Err(conflict(
                "delete Managed branch",
                "default branch cannot be deleted",
            ));
        }
        let (mut head, head_revision) = self
            .read_head(branch_id)
            .await?
            .ok_or_else(|| corrupt("delete Managed branch", "registered branch HEAD is missing"))?;
        registry.branches.remove(name);
        head.lifecycle = BranchLifecycle::Sealed;
        let registry_json = encode(&registry, MAX_REGISTRY_BYTES, "delete Managed branch")?;
        let head_json = encode(&head, MAX_HEAD_BYTES, "delete Managed branch")?;
        let mut batch = schema_statements();
        batch.extend([
            statement(
                format!(
                    "UPDATE {REGISTRY} SET revision = revision + 1, record_json = ? WHERE store_key = ? AND revision = ? AND maintenance_state = 'idle' AND EXISTS (SELECT 1 FROM {HEADS} WHERE store_key = ? AND branch_id = ? AND revision = ? AND lifecycle = 'active') RETURNING revision"
                ),
                vec![
                    registry_json.into(),
                    self.store_key().into(),
                    sqlite_integer(registry_revision, "delete Managed branch")?.into(),
                    self.store_key().into(),
                    branch_id.to_string().into(),
                    sqlite_integer(head_revision, "delete Managed branch")?.into(),
                ],
            ),
            statement(
                format!(
                    "UPDATE {HEADS} SET revision = revision + 1, lifecycle = 'sealed', record_json = ? WHERE store_key = ? AND branch_id = ? AND revision = ? AND lifecycle = 'active' AND EXISTS (SELECT 1 FROM {REGISTRY} WHERE store_key = ? AND revision = ? AND maintenance_state = 'idle') RETURNING revision"
                ),
                vec![
                    head_json.into(),
                    self.store_key().into(),
                    branch_id.to_string().into(),
                    sqlite_integer(head_revision, "delete Managed branch")?.into(),
                    self.store_key().into(),
                    sqlite_integer(
                        checked_revision(registry_revision, "delete Managed branch")?,
                        "delete Managed branch",
                    )?
                    .into(),
                ],
            ),
        ]);
        let results = match self.session.query(batch, "delete Managed branch").await {
            Ok(results) => results,
            Err(error) => {
                return match self.get(name).await {
                    Err(observed) if observed.kind() == ManagedErrorKind::Invalid => Ok(()),
                    Ok(current) if current.binding.id != branch_id => Ok(()),
                    Ok(_) => Err(error),
                    Err(observed) => Err(observed),
                };
            }
        };
        let sealed = rows(&results, SCHEMA_RESULTS + 1, "delete Managed branch")?;
        if sealed.len() == 1 {
            return Ok(());
        }
        if sealed.len() > 1 {
            return Err(corrupt(
                "delete Managed branch",
                "D1 returned duplicate branch HEADs",
            ));
        }
        match self.get(name).await {
            Err(error) if error.kind() == ManagedErrorKind::Invalid => Ok(()),
            Ok(current) if current.binding.id != branch_id => Ok(()),
            _ => Err(conflict(
                "delete Managed branch",
                "branch authority changed",
            )),
        }
    }

    async fn begin_gc(&self) -> Result<D1BranchGcFence, ManagedError> {
        let (mut registry, registry_revision) = self.registry().await?;
        if registry.maintenance_active {
            return Err(conflict(
                "begin Managed branch GC",
                "another branch GC is active",
            ));
        }
        registry.maintenance_epoch = registry
            .maintenance_epoch
            .checked_add(1)
            .ok_or_else(|| invalid("begin Managed branch GC", "maintenance epoch is exhausted"))?;
        registry.maintenance_active = true;
        let owner = *OperationId::generate().as_bytes();
        registry.maintenance_owner = Some(owner);
        let registry_json = encode(&registry, MAX_REGISTRY_BYTES, "begin Managed branch GC")?;
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {REGISTRY} SET revision = revision + 1, maintenance_epoch = ?, maintenance_state = 'sweeping', maintenance_owner = ?, record_json = ? WHERE store_key = ? AND revision = ? AND maintenance_state = 'idle' RETURNING maintenance_epoch"
            ),
            vec![
                sqlite_integer(registry.maintenance_epoch, "begin Managed branch GC")?.into(),
                encode_owner(owner).into(),
                registry_json.into(),
                self.store_key().into(),
                sqlite_integer(registry_revision, "begin Managed branch GC")?.into(),
            ],
        ));
        let results = self.session.query(batch, "begin Managed branch GC").await?;
        let changed = rows(&results, SCHEMA_RESULTS, "begin Managed branch GC")?;
        if let [row] = changed {
            let fence = D1BranchGcFence {
                epoch: integer(row, "maintenance_epoch", "begin Managed branch GC")?,
                owner,
            };
            self.fix_gc_heads(&registry, fence, "begin Managed branch GC")
                .await?;
            return Ok(fence);
        }
        if !changed.is_empty() {
            return Err(corrupt(
                "begin Managed branch GC",
                "D1 returned duplicate registries",
            ));
        }
        let (current, _) = self.registry().await?;
        if current.maintenance_active {
            Err(conflict(
                "begin Managed branch GC",
                "another branch GC is active",
            ))
        } else {
            Err(conflict(
                "begin Managed branch GC",
                "branch registry changed",
            ))
        }
    }

    async fn fix_gc_heads(
        &self,
        registry: &StoredBranchRegistry,
        fence: D1BranchGcFence,
        action: &'static str,
    ) -> Result<(), ManagedError> {
        for branch in registry.branches.values() {
            let branch_id = BranchId::from_bytes(*branch);
            let (mut head, revision) = self
                .read_head(branch_id)
                .await?
                .ok_or_else(|| corrupt(action, "registered branch HEAD is missing"))?;
            if head.lifecycle != BranchLifecycle::Active {
                return Err(corrupt(action, "registered branch HEAD is not active"));
            }
            if head.maintenance_active && fence.owns_head(&head) {
                continue;
            }
            head.maintenance_epoch = fence.epoch;
            head.maintenance_active = true;
            head.maintenance_owner = Some(fence.owner);
            if !self
                .replace_gc_head(branch_id, revision, &head, fence, true, action)
                .await?
            {
                return Err(conflict(action, "GC fence changed while fixing roots"));
            }
        }
        let (current, _) = self.registry().await?;
        if current.maintenance_active && fence.owns_registry(&current) {
            Ok(())
        } else {
            Err(conflict(action, "GC fence changed while fixing roots"))
        }
    }

    async fn clear_gc_heads(
        &self,
        registry: &StoredBranchRegistry,
        fence: D1BranchGcFence,
    ) -> Result<(), ManagedError> {
        for branch in registry.branches.values() {
            let branch_id = BranchId::from_bytes(*branch);
            let Some((mut head, revision)) = self.read_head(branch_id).await? else {
                continue;
            };
            if !head.maintenance_active || !fence.owns_head(&head) {
                continue;
            }
            head.maintenance_active = false;
            if !self
                .replace_gc_head(
                    branch_id,
                    revision,
                    &head,
                    fence,
                    false,
                    "finish Managed branch GC",
                )
                .await?
            {
                return Err(conflict(
                    "finish Managed branch GC",
                    "GC fence changed while releasing roots",
                ));
            }
        }
        Ok(())
    }

    async fn replace_gc_head(
        &self,
        branch_id: BranchId,
        revision: u64,
        head: &StoredBranchHead,
        fence: D1BranchGcFence,
        active: bool,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        let encoded = encode(head, MAX_HEAD_BYTES, action)?;
        let state = if active { "sweeping" } else { "idle" };
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {HEADS} SET revision = revision + 1, record_json = ? WHERE store_key = ? AND branch_id = ? AND revision = ? AND lifecycle = 'active' AND EXISTS (SELECT 1 FROM {REGISTRY} WHERE store_key = ? AND maintenance_epoch = ? AND maintenance_owner = ? AND maintenance_state = ?) RETURNING revision"
            ),
            vec![
                encoded.into(),
                self.store_key().into(),
                branch_id.to_string().into(),
                sqlite_integer(revision, action)?.into(),
                self.store_key().into(),
                sqlite_integer(fence.epoch, action)?.into(),
                encode_owner(fence.owner).into(),
                state.into(),
            ],
        ));
        let results = self.session.query(batch, action).await?;
        let changed = rows(&results, SCHEMA_RESULTS, action)?;
        if changed.len() > 1 {
            return Err(corrupt(action, "D1 returned duplicate branch HEADs"));
        }
        Ok(changed.len() == 1)
    }

    async fn finish_gc(&self, fence: D1BranchGcFence) -> Result<(), ManagedError> {
        let (mut registry, registry_revision) = self.registry().await?;
        if !registry.maintenance_active && fence.owns_registry(&registry) {
            return self.clear_gc_heads(&registry, fence).await;
        }
        if !registry.maintenance_active || !fence.owns_registry(&registry) {
            return Err(conflict(
                "finish Managed branch GC",
                "GC fence does not match the registry",
            ));
        }
        registry.maintenance_active = false;
        let registry_json = encode(&registry, MAX_REGISTRY_BYTES, "finish Managed branch GC")?;
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {REGISTRY} SET revision = revision + 1, maintenance_state = 'idle', record_json = ? WHERE store_key = ? AND revision = ? AND maintenance_epoch = ? AND maintenance_owner = ? AND maintenance_state = 'sweeping' RETURNING revision"
            ),
            vec![
                registry_json.into(),
                self.store_key().into(),
                sqlite_integer(registry_revision, "finish Managed branch GC")?.into(),
                sqlite_integer(fence.epoch, "finish Managed branch GC")?.into(),
                encode_owner(fence.owner).into(),
            ],
        ));
        let results = self
            .session
            .query(batch, "finish Managed branch GC")
            .await?;
        let changed = rows(&results, SCHEMA_RESULTS, "finish Managed branch GC")?;
        if changed.len() == 1 {
            return self.clear_gc_heads(&registry, fence).await;
        }
        if changed.len() > 1 {
            return Err(corrupt(
                "finish Managed branch GC",
                "D1 returned duplicate registries",
            ));
        }
        let (current, _) = self.registry().await?;
        if !current.maintenance_active && fence.owns_registry(&current) {
            self.clear_gc_heads(&current, fence).await
        } else {
            Err(conflict(
                "finish Managed branch GC",
                "branch registry changed",
            ))
        }
    }

    /// Collect data unreachable from every current and retained branch state.
    /// A failed mark or sweep deliberately leaves the fence active so the same
    /// command can resume without allowing publication against partial roots.
    pub async fn garbage_collect(
        &self,
        data_operator: Operator,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let data = ManagedData::new(data_operator)?;
        let fence = self.begin_gc().await?;
        self.collect_with_fence(fence, &data).await
    }

    /// Resume a failed GC after the process that owned its active fence has
    /// stopped. Calling this concurrently with that process is unsafe because
    /// both collectors would operate from independently marked data roots.
    pub async fn resume_garbage_collect(
        &self,
        data_operator: Operator,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let data = ManagedData::new(data_operator)?;
        let (registry, registry_revision) = self.registry().await?;
        if !registry.maintenance_active {
            return Err(conflict(
                "resume Managed branch GC",
                "no interrupted branch GC is active",
            ));
        }
        let fence = D1BranchGcFence {
            epoch: registry.maintenance_epoch,
            owner: *OperationId::generate().as_bytes(),
        };
        let mut resumed = registry;
        resumed.maintenance_owner = Some(fence.owner);
        let registry_json = encode(&resumed, MAX_REGISTRY_BYTES, "resume Managed branch GC")?;
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {REGISTRY} SET revision = revision + 1, maintenance_owner = ?, record_json = ? WHERE store_key = ? AND revision = ? AND maintenance_epoch = ? AND maintenance_state = 'sweeping' RETURNING revision"
            ),
            vec![
                encode_owner(fence.owner).into(),
                registry_json.into(),
                self.store_key().into(),
                sqlite_integer(registry_revision, "resume Managed branch GC")?.into(),
                sqlite_integer(fence.epoch, "resume Managed branch GC")?.into(),
            ],
        ));
        let results = self
            .session
            .query(batch, "resume Managed branch GC")
            .await?;
        let changed = rows(&results, SCHEMA_RESULTS, "resume Managed branch GC")?;
        if changed.len() != 1 {
            return if changed.is_empty() {
                Err(conflict(
                    "resume Managed branch GC",
                    "branch registry changed",
                ))
            } else {
                Err(corrupt(
                    "resume Managed branch GC",
                    "D1 returned duplicate registries",
                ))
            };
        }
        self.fix_gc_heads(&resumed, fence, "resume Managed branch GC")
            .await?;
        self.collect_with_fence(fence, &data).await
    }

    async fn collect_with_fence(
        &self,
        fence: D1BranchGcFence,
        data: &ManagedData,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let roots = self.gc_roots(fence).await?;
        let collected = data
            .collect_unreachable_segments_from(&roots.snapshots)
            .await?;
        self.sweep_metadata(fence, &roots).await?;
        self.finish_gc(fence).await?;
        Ok(collected)
    }

    async fn gc_roots(&self, fence: D1BranchGcFence) -> Result<D1GcRoots, ManagedError> {
        let (registry, registry_revision) = self.registry().await?;
        if !registry.maintenance_active || !fence.owns_registry(&registry) {
            return Err(conflict(
                "mark Managed branch GC roots",
                "GC fence does not match the registry",
            ));
        }
        let mut roots = D1GcRoots {
            snapshots: Vec::new(),
            heads: BTreeSet::new(),
            checkpoints: BTreeSet::new(),
            checkpoint_parts: BTreeSet::new(),
            histories: BTreeSet::new(),
        };
        for branch in registry.branches.values() {
            let branch_id = BranchId::from_bytes(*branch);
            roots.heads.insert(branch_id.to_string());
            let (head, _) = self.read_head(branch_id).await?.ok_or_else(|| {
                corrupt(
                    "mark Managed branch GC roots",
                    "registered branch HEAD is missing",
                )
            })?;
            if head.lifecycle != BranchLifecycle::Active {
                return Err(corrupt(
                    "mark Managed branch GC roots",
                    "registered branch HEAD is not active",
                ));
            }
            if !head.maintenance_active || !fence.owns_head(&head) {
                return Err(conflict(
                    "mark Managed branch GC roots",
                    "branch HEAD is not fixed by this GC fence",
                ));
            }
            let Some(state) = head.state else {
                continue;
            };
            roots.checkpoints.insert(encode_id(state.checkpoint));
            roots.checkpoint_parts.extend(
                self.read_checkpoint_root(state.checkpoint)
                    .await?
                    .parts
                    .into_iter()
                    .map(|part| encode_id(part.id)),
            );
            roots
                .snapshots
                .extend(self.snapshots_for_state(&state).await?);
            let mut history_id = state.previous_history;
            let mut chain = BTreeSet::new();
            while let Some(id) = history_id {
                visit_history(&mut chain, id, "mark Managed branch GC roots")?;
                roots.histories.insert(encode_id(id));
                let history = self.read_history(id).await?;
                roots.checkpoints.insert(encode_id(history.checkpoint));
                roots.checkpoint_parts.extend(
                    self.read_checkpoint_root(history.checkpoint)
                        .await?
                        .parts
                        .into_iter()
                        .map(|part| encode_id(part.id)),
                );
                let historical = StoredNamespaceState {
                    checkpoint: history.checkpoint,
                    checkpoint_cursor: history.checkpoint_cursor,
                    tail: history.changes.clone(),
                    previous_history: history.previous_history,
                };
                roots
                    .snapshots
                    .extend(self.snapshots_for_state(&historical).await?);
                history_id = history.previous_history;
            }
        }
        // A lifecycle or publication implementation that forgot the volume
        // fence must fail closed rather than feed an incomplete live set to GC.
        let (after, after_revision) = self.registry().await?;
        if !after.maintenance_active
            || after.maintenance_epoch != fence.epoch
            || after.maintenance_owner != Some(fence.owner)
            || after_revision != registry_revision
            || after.branches != registry.branches
        {
            return Err(conflict(
                "mark Managed branch GC roots",
                "branch roots changed during collection",
            ));
        }
        Ok(roots)
    }

    async fn sweep_metadata(
        &self,
        fence: D1BranchGcFence,
        roots: &D1GcRoots,
    ) -> Result<(), ManagedError> {
        let heads = self
            .metadata_ids(format!(
                "SELECT branch_id AS metadata_id FROM {HEADS} WHERE store_key = ? AND branch_id > ? ORDER BY branch_id LIMIT {METADATA_PAGE_SIZE}"
            ))
            .await?;
        let checkpoints = self
            .metadata_ids(format!(
                "SELECT DISTINCT checkpoint_id AS metadata_id FROM {CHECKPOINTS} WHERE store_key = ? AND checkpoint_id > ? ORDER BY checkpoint_id LIMIT {METADATA_PAGE_SIZE}"
            ))
            .await?;
        let histories = self
            .metadata_ids(format!(
                "SELECT history_id AS metadata_id FROM {HISTORY} WHERE store_key = ? AND history_id > ? ORDER BY history_id LIMIT {METADATA_PAGE_SIZE}"
            ))
            .await?;
        let checkpoint_parts = self
            .metadata_ids(format!(
                "SELECT part_id AS metadata_id FROM {CHECKPOINT_PARTS} WHERE store_key = ? AND part_id > ? ORDER BY part_id LIMIT {METADATA_PAGE_SIZE}"
            ))
            .await?;

        self.delete_metadata(HEADS, "branch_id", heads.difference(&roots.heads), fence)
            .await?;
        self.delete_metadata(
            CHECKPOINTS,
            "checkpoint_id",
            checkpoints.difference(&roots.checkpoints),
            fence,
        )
        .await?;
        self.delete_metadata(
            CHECKPOINT_PARTS,
            "part_id",
            checkpoint_parts.difference(&roots.checkpoint_parts),
            fence,
        )
        .await?;
        self.delete_metadata(
            HISTORY,
            "history_id",
            histories.difference(&roots.histories),
            fence,
        )
        .await
    }

    async fn metadata_ids(&self, sql: String) -> Result<BTreeSet<String>, ManagedError> {
        let action = "scan Managed branch GC metadata";
        let mut ids = BTreeSet::new();
        let mut after = String::new();
        loop {
            let mut batch = schema_statements();
            batch.push(statement(
                sql.clone(),
                vec![self.store_key().into(), after.clone().into()],
            ));
            let results = self.session.query(batch, action).await?;
            let page = rows(&results, SCHEMA_RESULTS, action)?;
            for row in page {
                let id = text(row, "metadata_id", action)?;
                if id <= after.as_str() || !ids.insert(id.to_owned()) {
                    return Err(corrupt(action, "D1 returned unordered branch metadata"));
                }
                after = id.to_owned();
            }
            if page.len() < METADATA_PAGE_SIZE {
                return Ok(ids);
            }
        }
    }

    async fn delete_metadata<'a>(
        &self,
        table: &'static str,
        key: &'static str,
        ids: impl Iterator<Item = &'a String>,
        fence: D1BranchGcFence,
    ) -> Result<(), ManagedError> {
        let action = "sweep Managed branch GC metadata";
        let ids = ids.cloned().collect::<Vec<_>>();
        for chunk in ids.chunks(METADATA_PAGE_SIZE) {
            let encoded = serde_json::to_string(chunk)
                .map_err(|_| invalid(action, "branch metadata IDs cannot be encoded"))?;
            if encoded.len() > MAX_DELETE_IDS_BYTES {
                return Err(invalid(
                    action,
                    "branch metadata deletion exceeds D1 request limits",
                ));
            }
            let mut batch = schema_statements();
            batch.extend([
                statement(
                    format!(
                        "DELETE FROM {table} WHERE store_key = ? AND {key} IN (SELECT value FROM json_each(?)) AND EXISTS (SELECT 1 FROM {REGISTRY} WHERE store_key = ? AND maintenance_epoch = ? AND maintenance_owner = ? AND maintenance_state = 'sweeping')"
                    ),
                    vec![
                        self.store_key().into(),
                        encoded.into(),
                        self.store_key().into(),
                        sqlite_integer(fence.epoch, action)?.into(),
                        encode_owner(fence.owner).into(),
                    ],
                ),
                statement(
                    format!(
                        "SELECT maintenance_epoch, maintenance_owner FROM {REGISTRY} WHERE store_key = ? AND maintenance_epoch = ? AND maintenance_owner = ? AND maintenance_state = 'sweeping'"
                    ),
                    vec![
                        self.store_key().into(),
                        sqlite_integer(fence.epoch, action)?.into(),
                        encode_owner(fence.owner).into(),
                    ],
                ),
            ]);
            let results = self.session.query(batch, action).await?;
            let fence_rows = rows(&results, SCHEMA_RESULTS + 1, action)?;
            let [row] = fence_rows else {
                return if fence_rows.is_empty() {
                    Err(conflict(action, "GC fence changed during metadata sweep"))
                } else {
                    Err(corrupt(action, "D1 returned duplicate registries"))
                };
            };
            if integer(row, "maintenance_epoch", action)? != fence.epoch
                || text(row, "maintenance_owner", action)? != encode_owner(fence.owner)
            {
                return Err(conflict(action, "GC fence changed during metadata sweep"));
            }
        }
        Ok(())
    }

    async fn snapshots_for_state(
        &self,
        state: &StoredNamespaceState,
    ) -> Result<Vec<NamespaceSnapshot>, ManagedError> {
        state.validate_shape()?;
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        let (mut snapshot, _) = checkpoint.recover(self.volume_id)?;
        if snapshot.cursor != state.checkpoint_cursor.decode()? {
            return Err(corrupt(
                "mark Managed branch GC roots",
                "branch checkpoint and retained state disagree",
            ));
        }
        let mut snapshots = vec![snapshot.clone()];
        for change in &state.tail {
            snapshot = change.apply(Some(snapshot))?;
            snapshots.push(snapshot.clone());
        }
        Ok(snapshots)
    }

    async fn state_from_history(
        &self,
        mut history_id: Option<[u8; 32]>,
        sequence: u64,
    ) -> Result<Option<StoredNamespaceState>, ManagedError> {
        let mut chain = BTreeSet::new();
        while let Some(id) = history_id {
            visit_history(&mut chain, id, "read Managed branch history")?;
            let history = self.read_history(id).await?;
            if let Some(state) = history.state_at(sequence) {
                return Ok(Some(state));
            }
            history_id = history.previous_history;
        }
        Ok(None)
    }

    async fn read_checkpoint_root(&self, id: [u8; 32]) -> Result<CheckpointRoot, ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "SELECT record_json FROM {CHECKPOINTS} WHERE store_key = ? AND checkpoint_id = ?"
            ),
            vec![self.store_key().into(), encode_id(id).into()],
        ));
        let results = self.session.query(batch, "read Managed branch").await?;
        let rows = rows(&results, SCHEMA_RESULTS, "read Managed branch")?;
        let [row] = rows else {
            return if rows.is_empty() {
                Err(corrupt(
                    "read Managed branch",
                    "branch checkpoint is missing",
                ))
            } else {
                Err(corrupt(
                    "read Managed branch",
                    "D1 returned duplicate branch checkpoints",
                ))
            };
        };
        let encoded = text(row, "record_json", "read Managed branch")?;
        if Sha256::digest(encoded.as_bytes()).as_slice() != id {
            return Err(corrupt(
                "read Managed branch",
                "branch checkpoint identity is invalid",
            ));
        }
        let root: CheckpointRoot =
            decode(encoded, MAX_CHECKPOINT_ROOT_BYTES, "read Managed branch")?;
        if root.volume_id != *self.volume_id.as_bytes() {
            return Err(corrupt(
                "read Managed branch",
                "branch checkpoint volume is invalid",
            ));
        }
        Ok(root)
    }

    async fn read_checkpoint(&self, id: [u8; 32]) -> Result<StoredCheckpoint, ManagedError> {
        let root = self.read_checkpoint_root(id).await?;
        let mut decoded = BTreeMap::<String, CheckpointPart>::new();
        for page in root.parts.chunks(CHECKPOINT_READ_PAGE) {
            let ids = serde_json::to_string(
                &page
                    .iter()
                    .map(|part| encode_id(part.id))
                    .collect::<Vec<_>>(),
            )
            .map_err(|_| corrupt("read Managed branch", "checkpoint part page is invalid"))?;
            let mut batch = schema_statements();
            batch.push(statement(
                format!(
                    "SELECT part_id, record_json FROM {CHECKPOINT_PARTS} WHERE store_key = ? AND part_id IN (SELECT value FROM json_each(?))"
                ),
                vec![self.store_key().into(), ids.into()],
            ));
            let results = self.session.query(batch, "read Managed branch").await?;
            for row in rows(&results, SCHEMA_RESULTS, "read Managed branch")? {
                let part_id = text(row, "part_id", "read Managed branch")?.to_owned();
                let encoded = text(row, "record_json", "read Managed branch")?;
                let expected = page
                    .iter()
                    .find(|part| encode_id(part.id) == part_id)
                    .ok_or_else(|| corrupt("read Managed branch", "unexpected checkpoint part"))?;
                let bytes = decode_blob(encoded, "read Managed branch")?;
                if bytes.len() < 32
                    || Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != expected.id
                {
                    return Err(corrupt(
                        "read Managed branch",
                        "branch checkpoint part identity is invalid",
                    ));
                }
                let part = CheckpointPart {
                    reference: expected.clone(),
                    bytes,
                };
                if decoded.insert(part_id, part).is_some() {
                    return Err(corrupt(
                        "read Managed branch",
                        "D1 returned duplicate checkpoint parts",
                    ));
                }
            }
        }
        let parts = root
            .parts
            .iter()
            .map(|part| {
                decoded.remove(&encode_id(part.id)).ok_or_else(|| {
                    corrupt("read Managed branch", "branch checkpoint part is missing")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        root.recover(parts)
    }

    async fn write_checkpoint(
        &self,
        checkpoint: &StoredCheckpoint,
    ) -> Result<[u8; 32], ManagedError> {
        let pending = PendingCheckpoint::from_checkpoint(checkpoint)?;
        for part in &pending.parts {
            let encoded = encode_blob(&part.bytes, "checkpoint Managed branch")?;
            let id = part.reference.id;
            let mut batch = schema_statements();
            batch.extend([
                statement(
                    format!(
                        "INSERT OR IGNORE INTO {CHECKPOINT_PARTS} (store_key, part_id, record_json) VALUES (?, ?, ?)"
                    ),
                    vec![self.store_key().into(), encode_id(id).into(), encoded.clone().into()],
                ),
                statement(
                    format!(
                        "SELECT record_json FROM {CHECKPOINT_PARTS} WHERE store_key = ? AND part_id = ?"
                    ),
                    vec![self.store_key().into(), encode_id(id).into()],
                ),
            ]);
            let results = self
                .session
                .query(batch, "checkpoint Managed branch")
                .await?;
            let observed = rows(&results, SCHEMA_RESULTS + 1, "checkpoint Managed branch")?;
            let [row] = observed else {
                return Err(corrupt(
                    "checkpoint Managed branch",
                    "D1 omitted immutable branch checkpoint part",
                ));
            };
            if text(row, "record_json", "checkpoint Managed branch")? != encoded {
                return Err(corrupt(
                    "checkpoint Managed branch",
                    "immutable branch checkpoint part changed",
                ));
            }
        }
        let root = pending.finish();
        let encoded = encode(
            &root,
            MAX_CHECKPOINT_ROOT_BYTES,
            "checkpoint Managed branch",
        )?;
        let id: [u8; 32] = Sha256::digest(encoded.as_bytes()).into();
        let mut batch = schema_statements();
        batch.extend([
            statement(
                format!(
                    "INSERT OR IGNORE INTO {CHECKPOINTS} (store_key, checkpoint_id, record_json) VALUES (?, ?, ?)"
                ),
                vec![self.store_key().into(), encode_id(id).into(), encoded.clone().into()],
            ),
            statement(
                format!(
                    "SELECT record_json FROM {CHECKPOINTS} WHERE store_key = ? AND checkpoint_id = ?"
                ),
                vec![self.store_key().into(), encode_id(id).into()],
            ),
        ]);
        let results = self
            .session
            .query(batch, "checkpoint Managed branch")
            .await?;
        let observed = rows(&results, SCHEMA_RESULTS + 1, "checkpoint Managed branch")?;
        let [row] = observed else {
            return Err(corrupt(
                "checkpoint Managed branch",
                "D1 omitted immutable branch checkpoint root",
            ));
        };
        if text(row, "record_json", "checkpoint Managed branch")? != encoded {
            return Err(corrupt(
                "checkpoint Managed branch",
                "immutable branch checkpoint root changed",
            ));
        }
        Ok(id)
    }

    async fn read_history(&self, id: [u8; 32]) -> Result<StoredHistory, ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!("SELECT record_json FROM {HISTORY} WHERE store_key = ? AND history_id = ?"),
            vec![self.store_key().into(), encode_id(id).into()],
        ));
        let results = self.session.query(batch, "read Managed branch").await?;
        let rows = rows(&results, SCHEMA_RESULTS, "read Managed branch")?;
        let [row] = rows else {
            return if rows.is_empty() {
                Err(corrupt("read Managed branch", "branch history is missing"))
            } else {
                Err(corrupt(
                    "read Managed branch",
                    "D1 returned duplicate branch histories",
                ))
            };
        };
        let encoded = text(row, "record_json", "read Managed branch")?;
        if Sha256::digest(encoded.as_bytes()).as_slice() != id {
            return Err(corrupt(
                "read Managed branch",
                "branch history identity is invalid",
            ));
        }
        let history: StoredHistory = decode(encoded, MAX_HISTORY_BYTES, "read Managed branch")?;
        history.validate(self.volume_id)?;
        Ok(history)
    }

    async fn write_history(&self, history: &StoredHistory) -> Result<[u8; 32], ManagedError> {
        let encoded = encode(history, MAX_HISTORY_BYTES, "archive Managed branch history")?;
        let id: [u8; 32] = Sha256::digest(encoded.as_bytes()).into();
        let mut batch = schema_statements();
        batch.extend([
            statement(
                format!(
                    "INSERT OR IGNORE INTO {HISTORY} (store_key, history_id, record_json) VALUES (?, ?, ?)"
                ),
                vec![
                    self.store_key().into(),
                    encode_id(id).into(),
                    encoded.clone().into(),
                ],
            ),
            statement(
                format!(
                    "SELECT record_json FROM {HISTORY} WHERE store_key = ? AND history_id = ?"
                ),
                vec![self.store_key().into(), encode_id(id).into()],
            ),
        ]);
        let results = self
            .session
            .query(batch, "archive Managed branch history")
            .await?;
        let rows = rows(
            &results,
            SCHEMA_RESULTS + 1,
            "archive Managed branch history",
        )?;
        let [row] = rows else {
            return Err(corrupt(
                "archive Managed branch history",
                "D1 omitted immutable branch history",
            ));
        };
        if text(row, "record_json", "archive Managed branch history")? != encoded {
            return Err(corrupt(
                "archive Managed branch history",
                "immutable branch history changed",
            ));
        }
        Ok(id)
    }

    async fn replace_head(
        &self,
        branch_id: BranchId,
        head_revision: u64,
        registry_revision: u64,
        head: &StoredBranchHead,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        let encoded = encode(head, MAX_HEAD_BYTES, action)?;
        let lifecycle = match head.lifecycle {
            BranchLifecycle::Active => "active",
            BranchLifecycle::Sealed => "sealed",
        };
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {HEADS} SET revision = revision + 1, lifecycle = ?, record_json = ? WHERE store_key = ? AND branch_id = ? AND revision = ? AND lifecycle = 'active' AND EXISTS (SELECT 1 FROM {REGISTRY} WHERE store_key = ? AND revision = ? AND maintenance_state = 'idle') RETURNING revision"
            ),
            vec![
                lifecycle.into(),
                encoded.into(),
                self.store_key().into(),
                branch_id.to_string().into(),
                sqlite_integer(head_revision, action)?.into(),
                self.store_key().into(),
                sqlite_integer(registry_revision, action)?.into(),
            ],
        ));
        let results = self.session.query(batch, action).await?;
        let changed = rows(&results, SCHEMA_RESULTS, action)?;
        if changed.len() > 1 {
            return Err(corrupt(action, "D1 returned duplicate branch HEADs"));
        }
        Ok(changed.len() == 1)
    }

    async fn registry(&self) -> Result<(StoredBranchRegistry, u64), ManagedError> {
        self.read_registry()
            .await?
            .ok_or_else(|| corrupt("read Managed branches", "branch registry is missing"))
    }

    async fn read_registry(&self) -> Result<Option<(StoredBranchRegistry, u64)>, ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "SELECT revision, volume_id, maintenance_epoch, maintenance_state, maintenance_owner, record_json FROM {REGISTRY} WHERE store_key = ?"
            ),
            vec![self.store_key().into()],
        ));
        let results = self.session.query(batch, "read Managed branches").await?;
        decode_registry_row(
            rows(&results, SCHEMA_RESULTS, "read Managed branches")?,
            self.volume_id,
            "read Managed branches",
        )
    }

    async fn read_head(
        &self,
        branch_id: BranchId,
    ) -> Result<Option<(StoredBranchHead, u64)>, ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "SELECT revision, lifecycle, record_json FROM {HEADS} WHERE store_key = ? AND branch_id = ?"
            ),
            vec![self.store_key().into(), branch_id.to_string().into()],
        ));
        let results = self.session.query(batch, "read Managed branch").await?;
        let rows = rows(&results, SCHEMA_RESULTS, "read Managed branch")?;
        let [row] = rows else {
            return if rows.is_empty() {
                Ok(None)
            } else {
                Err(corrupt(
                    "read Managed branch",
                    "D1 returned duplicate branch HEADs",
                ))
            };
        };
        let head: StoredBranchHead = decode(
            text(row, "record_json", "read Managed branch")?,
            MAX_HEAD_BYTES,
            "read Managed branch",
        )?;
        head.validate(self.volume_id, branch_id)?;
        if lifecycle(row, "read Managed branch")? != head.lifecycle {
            return Err(corrupt(
                "read Managed branch",
                "branch HEAD lifecycle columns disagree",
            ));
        }
        Ok(Some((
            head,
            integer(row, "revision", "read Managed branch")?,
        )))
    }

    fn store_key(&self) -> String {
        self.session.store_key().to_owned()
    }
}

fn schema_statements() -> Vec<D1Statement> {
    vec![
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {REGISTRY} (store_key TEXT PRIMARY KEY, volume_id TEXT NOT NULL, revision INTEGER NOT NULL, maintenance_epoch INTEGER NOT NULL, maintenance_state TEXT NOT NULL CHECK (maintenance_state IN ('idle', 'sweeping')), maintenance_owner TEXT, record_json TEXT NOT NULL)"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {HEADS} (store_key TEXT NOT NULL, branch_id TEXT NOT NULL, revision INTEGER NOT NULL, lifecycle TEXT NOT NULL CHECK (lifecycle IN ('active', 'sealed')), record_json TEXT NOT NULL, PRIMARY KEY (store_key, branch_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {CHECKPOINTS} (store_key TEXT NOT NULL, checkpoint_id TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (store_key, checkpoint_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {CHECKPOINT_PARTS} (store_key TEXT NOT NULL, part_id TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (store_key, part_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {HISTORY} (store_key TEXT NOT NULL, history_id TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (store_key, history_id))"
            ),
            Vec::new(),
        ),
    ]
}

fn decode_registry_row(
    rows: &[Value],
    volume_id: VolumeId,
    action: &'static str,
) -> Result<Option<(StoredBranchRegistry, u64)>, ManagedError> {
    let [row] = rows else {
        return if rows.is_empty() {
            Ok(None)
        } else {
            Err(corrupt(action, "D1 returned duplicate registries"))
        };
    };
    if text(row, "volume_id", action)? != volume_id.to_string() {
        return Err(corrupt(action, "branch registry volume is invalid"));
    }
    let registry: StoredBranchRegistry = decode(
        text(row, "record_json", action)?,
        MAX_REGISTRY_BYTES,
        action,
    )?;
    registry.validate(volume_id)?;
    let maintenance = text(row, "maintenance_state", action)?;
    let epoch = integer(row, "maintenance_epoch", action)?;
    let owner = match row.get("maintenance_owner") {
        None | Some(Value::Null) => None,
        Some(Value::String(owner)) => Some(decode_owner(owner, action)?),
        _ => return Err(corrupt(action, "branch registry owner is invalid")),
    };
    if registry.maintenance_epoch != epoch
        || registry.maintenance_active != (maintenance == "sweeping")
        || registry.maintenance_owner != owner
        || !matches!(maintenance, "idle" | "sweeping")
    {
        return Err(corrupt(
            action,
            "branch registry maintenance columns disagree",
        ));
    }
    Ok(Some((registry, integer(row, "revision", action)?)))
}

fn lifecycle(row: &Value, action: &'static str) -> Result<BranchLifecycle, ManagedError> {
    match text(row, "lifecycle", action)? {
        "active" => Ok(BranchLifecycle::Active),
        "sealed" => Ok(BranchLifecycle::Sealed),
        _ => Err(corrupt(action, "branch HEAD lifecycle is invalid")),
    }
}

fn rows<'a>(
    results: &'a [D1Result],
    index: usize,
    action: &'static str,
) -> Result<&'a [Value], ManagedError> {
    results
        .get(index)
        .map(|result| result.results.as_slice())
        .ok_or_else(|| corrupt(action, "D1 omitted a query result"))
}

fn text<'a>(row: &'a Value, field: &str, action: &'static str) -> Result<&'a str, ManagedError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid branch row"))
}

fn integer(row: &Value, field: &str, action: &'static str) -> Result<u64, ManagedError> {
    row.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid branch row"))
}

fn checked_revision(value: u64, action: &'static str) -> Result<u64, ManagedError> {
    value
        .checked_add(1)
        .filter(|value| i64::try_from(*value).is_ok())
        .ok_or_else(|| invalid(action, "branch revision is exhausted"))
}

fn sqlite_integer(value: u64, action: &'static str) -> Result<i64, ManagedError> {
    i64::try_from(value).map_err(|_| invalid(action, "branch counter exceeds D1 integer range"))
}

fn encode(
    value: &impl Serialize,
    maximum: usize,
    action: &'static str,
) -> Result<String, ManagedError> {
    let encoded = serde_json::to_string(value)
        .map_err(|_| invalid(action, "branch record cannot be encoded"))?;
    if encoded.len() > maximum {
        return Err(invalid(action, "branch record exceeds its size limit"));
    }
    Ok(encoded)
}

fn decode<T: DeserializeOwned>(
    encoded: &str,
    maximum: usize,
    action: &'static str,
) -> Result<T, ManagedError> {
    if encoded.len() > maximum {
        return Err(corrupt(action, "branch record exceeds its size limit"));
    }
    serde_json::from_str(encoded).map_err(|_| corrupt(action, "branch record cannot be decoded"))
}

fn encode_id(id: [u8; 32]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn encode_blob(bytes: &[u8], action: &'static str) -> Result<String, ManagedError> {
    if bytes.len().saturating_mul(2) > MAX_CHECKPOINT_PART_BYTES {
        return Err(invalid(
            action,
            "branch checkpoint SSTable exceeds the D1 value limit",
        ));
    }
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn decode_blob(encoded: &str, action: &'static str) -> Result<Vec<u8>, ManagedError> {
    if encoded.len() > MAX_CHECKPOINT_PART_BYTES || encoded.len() % 2 != 0 {
        return Err(corrupt(
            action,
            "branch checkpoint SSTable encoding is invalid",
        ));
    }
    (0..encoded.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&encoded[index..index + 2], 16)
                .map_err(|_| corrupt(action, "branch checkpoint SSTable encoding is invalid"))
        })
        .collect()
}

fn encode_owner(owner: [u8; 16]) -> String {
    owner.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_owner(encoded: &str, action: &'static str) -> Result<[u8; 16], ManagedError> {
    if encoded.len() != 32 {
        return Err(corrupt(action, "branch registry owner is invalid"));
    }
    let mut owner = [0; 16];
    for (index, byte) in owner.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16)
            .map_err(|_| corrupt(action, "branch registry owner is invalid"))?;
    }
    Ok(owner)
}

fn position_not_retained() -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Invalid,
        "fork Managed branch",
        "requested branch position is not retained",
    )
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn conflict(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Conflict, action, message)
}

fn not_found(action: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, "branch does not exist")
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "D1 branch metadata is unavailable",
    )
}

fn visit_history(
    chain: &mut BTreeSet<[u8; 32]>,
    id: [u8; 32],
    action: &'static str,
) -> Result<(), ManagedError> {
    if chain.insert(id) {
        Ok(())
    } else {
        Err(corrupt(action, "branch history contains a cycle"))
    }
}
