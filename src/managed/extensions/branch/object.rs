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

use std::collections::BTreeSet;
use std::io::Cursor;

use opendal::Operator;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};

use super::record_set::{CheckpointRoot, PartRef, PendingCheckpoint};
use super::records::{
    BranchInfo, BranchLifecycle, ForkPoint, MAX_TAIL_BYTES, MAX_TAIL_TRANSACTIONS,
    StoredBranchHead, StoredBranchRegistry, StoredCheckpoint, StoredCommittedResult, StoredHistory,
    StoredNamespaceState, info, recover_namespace, recover_retained, require_request_digest,
    results_for_rotation,
};
use crate::filesystem::{
    BranchBinding, BranchId, BranchName, ChangeCursor, CommitOutcome, OperationId, VolumeId,
};
use crate::managed::metadata::namespace::{NamespacePublication, NamespaceSnapshot};
use crate::managed::metadata::object;
use crate::managed::{ManagedData, ManagedError, ManagedErrorKind, SegmentGcMaintenance};

const ROOT: &str = ".ofs/managed/metadata/v1/extensions/branch/v1";
const REGISTRY_KEY: &str = ".ofs/managed/metadata/v1/extensions/branch/v1/registry.ofs";
const REGISTRY_MAGIC: &[u8; 8] = b"OFS1BRG1";
const HEAD_MAGIC: &[u8; 8] = b"OFS1BRH1";
const CHECKPOINT_MAGIC: &[u8; 8] = b"OFS1BRC1";
const CHECKPOINT_PART_MAGIC: &[u8; 8] = b"OFS1BRP1";
const HISTORY_MAGIC: &[u8; 8] = b"OFS1BRY1";
// The registry remains the small mutable branch authority. Its actual backend
// write is the limit; unlike checkpoint data, it has no format-level byte cap.
const MAX_REGISTRY_BYTES: usize = usize::MAX;
const MAX_HEAD_BYTES: usize = 256 * 1024;
const MAX_CHECKPOINT_ROOT_BYTES: usize = 4 * 1024 * 1024;
const MAX_CHECKPOINT_PART_BYTES: usize = 2 * 1024 * 1024;
const MAX_HISTORY_BYTES: usize = 512 * 1024;

#[derive(Clone)]
pub struct ObjectBranchStore {
    volume_id: VolumeId,
    operator: Operator,
}

#[derive(Clone)]
pub struct ObjectBoundNamespace {
    store: ObjectBranchStore,
    binding: BranchBinding,
}

#[derive(Clone, Debug)]
pub struct ObjectBranchObservation {
    pub(crate) snapshot: NamespaceSnapshot,
    revision: String,
    head: StoredBranchHead,
    checkpoint: StoredCheckpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObjectBranchGcFence {
    epoch: u64,
    owner: [u8; 16],
}

impl ObjectBranchGcFence {
    fn owns_registry(self, registry: &StoredBranchRegistry) -> bool {
        registry.maintenance_epoch == self.epoch && registry.maintenance_owner == Some(self.owner)
    }

    fn owns_head(self, head: &StoredBranchHead) -> bool {
        head.maintenance_epoch == self.epoch && head.maintenance_owner == Some(self.owner)
    }
}

struct ObjectGcRoots {
    snapshots: Vec<NamespaceSnapshot>,
    heads: BTreeSet<String>,
    checkpoints: BTreeSet<String>,
    checkpoint_parts: BTreeSet<String>,
    histories: BTreeSet<String>,
}

impl ObjectBoundNamespace {
    pub fn binding(&self) -> &BranchBinding {
        &self.binding
    }

    pub fn volume_id(&self) -> VolumeId {
        self.store.volume_id
    }

    pub(crate) async fn observe(&self) -> Result<Option<ObjectBranchObservation>, ManagedError> {
        let (head, revision) = self.current_head("read Managed branch").await?;
        let Some(state) = &head.state else {
            return Ok(None);
        };
        let checkpoint = self.store.read_checkpoint(state.checkpoint).await?;
        let snapshot = recover_namespace(checkpoint.clone(), state, self.store.volume_id)?;
        Ok(Some(ObjectBranchObservation {
            snapshot,
            revision,
            head,
            checkpoint,
        }))
    }

    pub(crate) async fn observe_from(
        &self,
        _base: &NamespaceSnapshot,
    ) -> Result<Option<ObjectBranchObservation>, ManagedError> {
        self.observe().await
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<&ObjectBranchObservation>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        let branch_id = self.binding.id;
        if publication.target.volume_id != self.store.volume_id {
            return Err(invalid(
                "publish Managed branch",
                "publication belongs to another volume",
            ));
        }
        let (head, revision, base, checkpoint) = match observed {
            Some(observed) => {
                observed.head.validate(self.store.volume_id, branch_id)?;
                if observed.head.lifecycle != BranchLifecycle::Active
                    || observed.head.maintenance_active
                {
                    return Err(conflict(
                        "publish Managed branch",
                        "branch is sealed for deletion",
                    ));
                }
                (
                    observed.head.clone(),
                    observed.revision.clone(),
                    Some(&observed.snapshot),
                    Some(observed.checkpoint.clone()),
                )
            }
            None => {
                let (head, revision) = self.current_head("publish Managed branch").await?;
                if head.state.is_some() {
                    return self.outcome_after_race(publication.operation).await;
                }
                (head, revision, None, None)
            }
        };
        let (change, valid) = super::records::StoredChange::prepare(branch_id, publication, base)?;
        if !valid {
            if matches!(
                self.resolve_known(publication.operation, Some(change.request_digest()?))
                    .await?,
                CommitOutcome::Committed(_)
            ) {
                return Ok(CommitOutcome::Committed(publication.target.cursor));
            }
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            });
        }
        let request_digest = change.request_digest()?;
        if let CommitOutcome::Committed(cursor) = self
            .resolve_known(publication.operation, Some(request_digest))
            .await?
        {
            return Ok(CommitOutcome::Committed(cursor));
        }

        let state = match (&head.state, checkpoint) {
            (None, None) => {
                let mut results = std::collections::BTreeMap::new();
                let result = StoredCommittedResult::from_change(&change)?;
                results.insert((branch_id, publication.operation), result);
                let checkpoint = StoredCheckpoint::new(&publication.target, results)?;
                let checkpoint_id = self.store.write_checkpoint(&checkpoint).await?;
                StoredNamespaceState {
                    checkpoint: checkpoint_id,
                    checkpoint_cursor: publication.target.cursor.into(),
                    tail: Vec::new(),
                    previous_history: None,
                }
            }
            (Some(current), Some(checkpoint)) => {
                let appended_bytes = current
                    .tail
                    .iter()
                    .map(|change| change.payload.len())
                    .sum::<usize>()
                    + change.payload.len();
                if current.tail.len() + 1 >= MAX_TAIL_TRANSACTIONS
                    || appended_bytes > MAX_TAIL_BYTES
                {
                    let history = StoredHistory::new(self.store.volume_id, branch_id, current)?;
                    let history_id = self.store.write_history(&history).await?;
                    let results =
                        results_for_rotation(checkpoint, current, &change, self.store.volume_id)?;
                    let checkpoint = StoredCheckpoint::new(&publication.target, results)?;
                    let checkpoint_id = self.store.write_checkpoint(&checkpoint).await?;
                    StoredNamespaceState {
                        checkpoint: checkpoint_id,
                        checkpoint_cursor: publication.target.cursor.into(),
                        tail: Vec::new(),
                        previous_history: Some(history_id),
                    }
                } else {
                    let mut next = current.clone();
                    next.tail.push(change);
                    next
                }
            }
            _ => {
                return Err(corrupt(
                    "publish Managed branch",
                    "branch observation and checkpoint disagree",
                ));
            }
        };
        let next = StoredBranchHead {
            major: head.major,
            volume_id: head.volume_id,
            branch_id: head.branch_id,
            lifecycle: head.lifecycle,
            state: Some(state),
            maintenance_epoch: head.maintenance_epoch,
            maintenance_active: head.maintenance_active,
            maintenance_owner: head.maintenance_owner,
        };
        let bytes = encode(HEAD_MAGIC, &next, MAX_HEAD_BYTES, "publish Managed branch")?;
        match self
            .store
            .replace(
                &head_key(branch_id),
                &revision,
                bytes,
                "publish Managed branch",
            )
            .await
        {
            Ok(true) => Ok(CommitOutcome::Committed(publication.target.cursor)),
            Ok(false) => self.outcome_after_race(publication.operation).await,
            Err(_) => match self.resolve(publication.operation).await {
                Ok(CommitOutcome::Committed(cursor)) => Ok(CommitOutcome::Committed(cursor)),
                _ => Ok(CommitOutcome::Unknown),
            },
        }
    }

    pub(crate) async fn resolve(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        match self.resolve_known(operation, None).await {
            Err(error) if error.kind() == ManagedErrorKind::Unavailable => {
                Ok(CommitOutcome::Unknown)
            }
            result => result,
        }
    }

    async fn resolve_known(
        &self,
        operation: OperationId,
        expected: Option<[u8; 32]>,
    ) -> Result<CommitOutcome, ManagedError> {
        let (head, _) = self
            .current_head("resolve Managed branch publication")
            .await?;
        let Some(state) = head.state else {
            return Ok(CommitOutcome::Absent);
        };
        if let Some(change) = state.tail.iter().find(|change| {
            change.origin_branch == *self.binding.id.as_bytes()
                && change.operation == *operation.as_bytes()
        }) {
            require_request_digest(expected, change.request_digest()?)?;
            return Ok(CommitOutcome::Committed(change.cursor.decode()?));
        }
        let checkpoint = self.store.read_checkpoint(state.checkpoint).await?;
        let Some(result) = checkpoint.resolve(self.binding.id, operation)? else {
            return Ok(CommitOutcome::Absent);
        };
        require_request_digest(expected, result.request_sha256)?;
        Ok(CommitOutcome::Committed(result.cursor.decode()?))
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        match self.resolve(operation).await? {
            result @ (CommitOutcome::Committed(_) | CommitOutcome::Unknown) => Ok(result),
            _ => Ok(CommitOutcome::Conflict {
                observed: self
                    .observe()
                    .await?
                    .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor),
            }),
        }
    }

    async fn current_head(
        &self,
        action: &'static str,
    ) -> Result<(StoredBranchHead, String), ManagedError> {
        let (head, revision) = self
            .store
            .read_head(self.binding.id)
            .await?
            .ok_or_else(|| conflict(action, "branch incarnation no longer exists"))?;
        if head.lifecycle != BranchLifecycle::Active {
            return Err(conflict(action, "branch is sealed for deletion"));
        }
        if head.maintenance_active {
            return Err(conflict(action, "branch maintenance is active"));
        }
        Ok((head, revision))
    }
}

impl ObjectBranchStore {
    pub fn new(volume_id: VolumeId, operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.read
            || !capability.write
            || !capability.write_with_if_not_exists
            || !capability.write_with_if_match
        {
            return Err(invalid(
                "open Managed branches",
                "object branch metadata requires read, create-only write, and conditional replace",
            ));
        }
        Ok(Self {
            volume_id,
            operator,
        })
    }

    /// Idempotently create the first unborn branch. The head is prepared
    /// before the registry, making the registry the branch-existence authority.
    pub async fn initialize(&self, default_name: BranchName) -> Result<BranchInfo, ManagedError> {
        if let Some((registry, _)) = self.read_registry().await? {
            let observed_name = registry
                .branches
                .iter()
                .find_map(|(name, id)| (*id == registry.default_branch).then_some(name));
            if observed_name != Some(&default_name) {
                return Err(conflict(
                    "initialize Managed branches",
                    "the volume has another default branch",
                ));
            }
            return self.get(&default_name).await;
        }

        let branch_id = BranchId::generate();
        let head = StoredBranchHead::unborn(self.volume_id, branch_id);
        let encoded_head = encode(HEAD_MAGIC, &head, MAX_HEAD_BYTES, "create Managed branch")?;
        let _ = self
            .create(&head_key(branch_id), encoded_head, "create Managed branch")
            .await?;
        let registry =
            StoredBranchRegistry::initial(self.volume_id, default_name.clone(), branch_id);
        let encoded_registry = encode(
            REGISTRY_MAGIC,
            &registry,
            MAX_REGISTRY_BYTES,
            "initialize Managed branches",
        )?;
        if !self
            .create(
                REGISTRY_KEY,
                encoded_registry,
                "initialize Managed branches",
            )
            .await?
        {
            return self.initialize_existing(default_name).await;
        }
        info(default_name, branch_id, &head, branch_id)
    }

    async fn initialize_existing(
        &self,
        default_name: BranchName,
    ) -> Result<BranchInfo, ManagedError> {
        let (registry, _) = self
            .read_registry()
            .await?
            .ok_or_else(|| unavailable("initialize Managed branches"))?;
        let expected = registry.branch_id(&default_name);
        if expected != Some(BranchId::from_bytes(registry.default_branch)) {
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
        for (name, bytes) in registry.branches {
            let id = BranchId::from_bytes(bytes);
            let (head, _) = self.read_head(id).await?.ok_or_else(|| {
                corrupt("list Managed branches", "registered branch HEAD is missing")
            })?;
            branches.push(info(name, id, &head, default)?);
        }
        Ok(branches)
    }

    pub async fn default_name(&self) -> Result<BranchName, ManagedError> {
        let (registry, _) = self.registry().await?;
        registry
            .branches
            .into_iter()
            .find_map(|(name, id)| (id == registry.default_branch).then_some(name))
            .ok_or_else(|| corrupt("read default Managed branch", "default branch is missing"))
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
        head.validate(self.volume_id, id)?;
        info(
            name.clone(),
            id,
            &head,
            BranchId::from_bytes(registry.default_branch),
        )
    }

    pub async fn bind(&self, name: &BranchName) -> Result<ObjectBoundNamespace, ManagedError> {
        let branch = self.get(name).await?;
        if branch.lifecycle != BranchLifecycle::Active {
            return Err(conflict(
                "bind Managed branch",
                "branch is sealed for deletion",
            ));
        }
        Ok(ObjectBoundNamespace {
            store: self.clone(),
            binding: branch.binding,
        })
    }

    pub async fn delete(&self, name: &BranchName) -> Result<(), ManagedError> {
        let (registry, _) = self.registry().await?;
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
            return Err(invalid(
                "delete Managed branch",
                "default branch cannot be deleted",
            ));
        }

        loop {
            let Some((mut head, revision)) = self.read_head(branch_id).await? else {
                return Err(corrupt(
                    "delete Managed branch",
                    "registered branch HEAD is missing",
                ));
            };
            if head.lifecycle == BranchLifecycle::Sealed {
                break;
            }
            if head.maintenance_active {
                return Err(conflict(
                    "delete Managed branch",
                    "branch maintenance is active",
                ));
            }
            head.lifecycle = BranchLifecycle::Sealed;
            let bytes = encode(HEAD_MAGIC, &head, MAX_HEAD_BYTES, "delete Managed branch")?;
            match self
                .replace(
                    &head_key(branch_id),
                    &revision,
                    bytes,
                    "delete Managed branch",
                )
                .await
            {
                Ok(true) => break,
                Ok(false) => continue,
                Err(error) => {
                    if self
                        .read_head(branch_id)
                        .await?
                        .is_some_and(|(head, _)| head.lifecycle == BranchLifecycle::Sealed)
                    {
                        break;
                    }
                    return Err(error);
                }
            }
        }

        loop {
            let (mut registry, revision) = self.registry().await?;
            match registry.branch_id(name) {
                None => return Ok(()),
                Some(current) if current != branch_id => return Ok(()),
                Some(_) => {}
            }
            if registry.maintenance_active {
                return Err(conflict(
                    "delete Managed branch",
                    "branch maintenance is active",
                ));
            }
            if !registry.remove_if(name, branch_id) {
                return Ok(());
            }
            let bytes = encode(
                REGISTRY_MAGIC,
                &registry,
                MAX_REGISTRY_BYTES,
                "delete Managed branch",
            )?;
            match self
                .replace(REGISTRY_KEY, &revision, bytes, "delete Managed branch")
                .await
            {
                Ok(true) => return Ok(()),
                Ok(false) => continue,
                Err(error) => {
                    let (current, _) = self.registry().await?;
                    if current.branch_id(name) != Some(branch_id) {
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn begin_gc(&self) -> Result<ObjectBranchGcFence, ManagedError> {
        let (registry, fence) = loop {
            let (mut registry, revision) = self.registry().await?;
            if registry.maintenance_active {
                return Err(conflict(
                    "begin Managed branch GC",
                    "branch maintenance is already active",
                ));
            }
            registry.maintenance_epoch =
                registry.maintenance_epoch.checked_add(1).ok_or_else(|| {
                    invalid("begin Managed branch GC", "maintenance epoch is exhausted")
                })?;
            registry.maintenance_active = true;
            let fence = ObjectBranchGcFence {
                epoch: registry.maintenance_epoch,
                owner: *OperationId::generate().as_bytes(),
            };
            registry.maintenance_owner = Some(fence.owner);
            let bytes = encode(
                REGISTRY_MAGIC,
                &registry,
                MAX_REGISTRY_BYTES,
                "begin Managed branch GC",
            )?;
            match self
                .replace(REGISTRY_KEY, &revision, bytes, "begin Managed branch GC")
                .await
            {
                Ok(true) => break (registry, fence),
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        };

        self.fix_gc_heads(&registry, fence, "begin Managed branch GC")
            .await?;
        Ok(fence)
    }

    async fn fix_gc_heads(
        &self,
        registry: &StoredBranchRegistry,
        fence: ObjectBranchGcFence,
        action: &'static str,
    ) -> Result<(), ManagedError> {
        let mut fixed = registry.clone();
        'registry: loop {
            for (name, branch) in fixed.branches.clone() {
                let branch_id = BranchId::from_bytes(branch);
                loop {
                    let (mut head, revision) = self
                        .read_head(branch_id)
                        .await?
                        .ok_or_else(|| corrupt(action, "registered branch HEAD is missing"))?;
                    if head.lifecycle == BranchLifecycle::Sealed {
                        fixed = self
                            .remove_sealed_registration(name, branch_id, fence, action)
                            .await?;
                        continue 'registry;
                    }
                    if head.maintenance_active && fence.owns_head(&head) {
                        break;
                    }
                    head.maintenance_epoch = fence.epoch;
                    head.maintenance_active = true;
                    head.maintenance_owner = Some(fence.owner);
                    let bytes = encode(HEAD_MAGIC, &head, MAX_HEAD_BYTES, action)?;
                    match self
                        .replace(&head_key(branch_id), &revision, bytes, action)
                        .await
                    {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(error) => return Err(error),
                    }
                }
            }

            let (observed, _) = self.registry().await?;
            if !observed.maintenance_active || !fence.owns_registry(&observed) {
                return Err(conflict(action, "GC fence changed while fixing roots"));
            }
            if observed.branches != fixed.branches {
                return Err(conflict(
                    action,
                    "branch registry changed while fixing roots",
                ));
            }
            return Ok(());
        }
    }

    async fn remove_sealed_registration(
        &self,
        name: BranchName,
        branch_id: BranchId,
        fence: ObjectBranchGcFence,
        action: &'static str,
    ) -> Result<StoredBranchRegistry, ManagedError> {
        loop {
            let (mut registry, revision) = self.registry().await?;
            if !registry.maintenance_active || !fence.owns_registry(&registry) {
                return Err(conflict(action, "GC fence changed while fixing roots"));
            }
            if !remove_sealed_incarnation(&mut registry, &name, branch_id, action)? {
                return Ok(registry);
            }
            let bytes = encode(REGISTRY_MAGIC, &registry, MAX_REGISTRY_BYTES, action)?;
            match object::replace(&self.operator, REGISTRY_KEY, &revision, bytes, action).await {
                Ok(true) => return Ok(registry),
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
    }

    async fn gc_roots(&self, fence: ObjectBranchGcFence) -> Result<ObjectGcRoots, ManagedError> {
        let (registry, registry_revision) = self.registry().await?;
        if !registry.maintenance_active || !fence.owns_registry(&registry) {
            return Err(conflict(
                "mark Managed branch GC roots",
                "GC fence does not match the registry",
            ));
        }
        let mut roots = ObjectGcRoots {
            snapshots: Vec::new(),
            heads: BTreeSet::new(),
            checkpoints: BTreeSet::new(),
            checkpoint_parts: BTreeSet::new(),
            histories: BTreeSet::new(),
        };
        for branch in registry.branches.values() {
            let branch_id = BranchId::from_bytes(*branch);
            roots.heads.insert(head_key(branch_id));
            let (head, _) = self.read_head(branch_id).await?.ok_or_else(|| {
                corrupt(
                    "mark Managed branch GC roots",
                    "registered branch HEAD is missing",
                )
            })?;
            if head.lifecycle != BranchLifecycle::Active
                || !head.maintenance_active
                || !fence.owns_head(&head)
            {
                return Err(conflict(
                    "mark Managed branch GC roots",
                    "branch HEAD is not fixed by this GC fence",
                ));
            }
            let Some(state) = head.state else {
                continue;
            };
            roots.checkpoints.insert(checkpoint_key(state.checkpoint));
            roots.checkpoint_parts.extend(
                self.read_checkpoint_root(state.checkpoint)
                    .await?
                    .parts
                    .into_iter()
                    .map(|part| checkpoint_part_key(part.id)),
            );
            roots
                .snapshots
                .extend(self.snapshots_for_state(&state).await?);
            let mut history_id = state.previous_history;
            let mut chain = BTreeSet::new();
            while let Some(id) = history_id {
                visit_history(&mut chain, id, "mark Managed branch GC roots")?;
                roots.histories.insert(history_key(id));
                let history = self.read_history(id).await?;
                roots.checkpoints.insert(checkpoint_key(history.checkpoint));
                roots.checkpoint_parts.extend(
                    self.read_checkpoint_root(history.checkpoint)
                        .await?
                        .parts
                        .into_iter()
                        .map(|part| checkpoint_part_key(part.id)),
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

    async fn finish_gc(&self, fence: ObjectBranchGcFence) -> Result<(), ManagedError> {
        let branches = loop {
            let (mut registry, revision) = self.registry().await?;
            if !registry.maintenance_active {
                if !fence.owns_registry(&registry) {
                    return Err(conflict(
                        "finish Managed branch GC",
                        "GC fence does not match the registry",
                    ));
                }
                break registry.branches;
            }
            if !fence.owns_registry(&registry) {
                return Err(conflict(
                    "finish Managed branch GC",
                    "GC fence does not match the registry",
                ));
            }
            registry.maintenance_active = false;
            let branches = registry.branches.clone();
            let bytes = encode(
                REGISTRY_MAGIC,
                &registry,
                MAX_REGISTRY_BYTES,
                "finish Managed branch GC",
            )?;
            match self
                .replace(REGISTRY_KEY, &revision, bytes, "finish Managed branch GC")
                .await
            {
                Ok(true) => break branches,
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        };

        for branch in branches.values() {
            let branch_id = BranchId::from_bytes(*branch);
            loop {
                let Some((mut head, revision)) = self.read_head(branch_id).await? else {
                    break;
                };
                if !head.maintenance_active || !fence.owns_head(&head) {
                    break;
                }
                head.maintenance_active = false;
                let bytes = encode(
                    HEAD_MAGIC,
                    &head,
                    MAX_HEAD_BYTES,
                    "finish Managed branch GC",
                )?;
                match self
                    .replace(
                        &head_key(branch_id),
                        &revision,
                        bytes,
                        "finish Managed branch GC",
                    )
                    .await
                {
                    Ok(true) => break,
                    Ok(false) => continue,
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    /// Mark every retained branch position, sweep the shared data plane once,
    /// and release the volume fence only after deletion succeeds.
    pub async fn garbage_collect(
        &self,
        data_operator: Operator,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let fence = self.begin_gc().await?;
        self.collect_with_fence(data_operator, fence).await
    }

    /// Resume an interrupted collection after the caller has established that
    /// the collector which owns the active fence is no longer running.
    pub async fn resume_garbage_collect(
        &self,
        data_operator: Operator,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let (registry, revision) = self.registry().await?;
        if !registry.maintenance_active {
            return Err(conflict(
                "resume Managed branch GC",
                "branch maintenance is not active",
            ));
        }
        let fence = ObjectBranchGcFence {
            epoch: registry.maintenance_epoch,
            owner: *OperationId::generate().as_bytes(),
        };
        let mut resumed = registry.clone();
        resumed.maintenance_owner = Some(fence.owner);
        let bytes = encode(
            REGISTRY_MAGIC,
            &resumed,
            MAX_REGISTRY_BYTES,
            "resume Managed branch GC",
        )?;
        if !self
            .replace(REGISTRY_KEY, &revision, bytes, "resume Managed branch GC")
            .await?
        {
            return Err(conflict(
                "resume Managed branch GC",
                "branch registry changed",
            ));
        }
        self.fix_gc_heads(&resumed, fence, "resume Managed branch GC")
            .await?;
        self.collect_with_fence(data_operator, fence).await
    }

    async fn collect_with_fence(
        &self,
        data_operator: Operator,
        fence: ObjectBranchGcFence,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let capability = self.operator.info().full_capability();
        if !capability.list || !capability.delete {
            return Err(invalid(
                "garbage collect Managed branches",
                "object branch garbage collection requires list and delete",
            ));
        }
        let roots = self.gc_roots(fence).await?;
        let data = ManagedData::new(data_operator)?;
        let maintenance = data
            .collect_unreachable_segments_from(&roots.snapshots)
            .await?;
        self.sweep_metadata(fence, &roots).await?;
        self.finish_gc(fence).await?;
        Ok(maintenance)
    }

    async fn sweep_metadata(
        &self,
        fence: ObjectBranchGcFence,
        roots: &ObjectGcRoots,
    ) -> Result<(), ManagedError> {
        self.ensure_gc_fence(fence, "sweep Managed branch GC metadata")
            .await?;
        let heads = self
            .metadata_candidates(&format!("{ROOT}/heads/"), 16)
            .await?;
        let checkpoints = self
            .metadata_candidates(&format!("{ROOT}/checkpoints/sha256/"), 32)
            .await?;
        let checkpoint_parts = self
            .metadata_candidates(&format!("{ROOT}/checkpoint-parts/sha256/"), 32)
            .await?;
        let histories = self
            .metadata_candidates(&format!("{ROOT}/history/sha256/"), 32)
            .await?;
        let unreachable = heads
            .difference(&roots.heads)
            .chain(checkpoints.difference(&roots.checkpoints))
            .chain(checkpoint_parts.difference(&roots.checkpoint_parts))
            .chain(histories.difference(&roots.histories))
            .cloned()
            .collect::<Vec<_>>();
        self.ensure_gc_fence(fence, "sweep Managed branch GC metadata")
            .await?;
        self.operator
            .delete_iter(unreachable.iter().map(String::as_str))
            .await
            .map_err(|_| unavailable("sweep Managed branch GC metadata"))?;
        self.ensure_gc_fence(fence, "sweep Managed branch GC metadata")
            .await
    }

    async fn metadata_candidates(
        &self,
        prefix: &str,
        identity_bytes: usize,
    ) -> Result<BTreeSet<String>, ManagedError> {
        let entries = self
            .operator
            .list_with(prefix)
            .recursive(true)
            .await
            .map_err(|_| unavailable("scan Managed branch GC metadata"))?;
        Ok(entries
            .into_iter()
            .filter(|entry| entry.metadata().is_file())
            .filter_map(|entry| {
                canonical_metadata_key(entry.path(), prefix, identity_bytes)
                    .then(|| entry.path().to_owned())
            })
            .collect())
    }

    async fn ensure_gc_fence(
        &self,
        fence: ObjectBranchGcFence,
        action: &'static str,
    ) -> Result<(), ManagedError> {
        let (registry, _) = self.registry().await?;
        if registry.maintenance_active && fence.owns_registry(&registry) {
            Ok(())
        } else {
            Err(conflict(action, "GC fence changed during metadata sweep"))
        }
    }

    async fn snapshots_for_state(
        &self,
        state: &StoredNamespaceState,
    ) -> Result<Vec<NamespaceSnapshot>, ManagedError> {
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        recover_retained(checkpoint, state, self.volume_id)
    }

    pub async fn fork(
        &self,
        source: &BranchName,
        point: ForkPoint,
        target: BranchName,
    ) -> Result<BranchInfo, ManagedError> {
        let (mut registry, revision) = self.registry().await?;
        if registry.maintenance_active {
            return Err(conflict(
                "fork Managed branch",
                "branch maintenance is active",
            ));
        }
        if registry.branches.contains_key(&target) {
            return Err(conflict(
                "fork Managed branch",
                "target branch already exists",
            ));
        }
        let source_id = registry
            .branch_id(source)
            .ok_or_else(|| not_found("fork Managed branch"))?;
        let (source_head, _) = self
            .read_head(source_id)
            .await?
            .ok_or_else(|| corrupt("fork Managed branch", "source branch HEAD is missing"))?;
        if source_head.lifecycle != BranchLifecycle::Active || source_head.maintenance_active {
            return Err(conflict(
                "fork Managed branch",
                "source branch is sealed for deletion",
            ));
        }
        let state = match point {
            ForkPoint::Head => source_head.state.clone(),
            ForkPoint::Sequence(0) => None,
            ForkPoint::Sequence(sequence) => {
                let current = source_head
                    .state
                    .as_ref()
                    .ok_or_else(|| position_not_retained("fork Managed branch"))?;
                if let Some(state) = current.at_sequence(sequence) {
                    Some(state)
                } else {
                    self.find_history_state(current.previous_history, sequence)
                        .await?
                        .ok_or_else(|| position_not_retained("fork Managed branch"))?
                        .into()
                }
            }
        };
        let target_id = BranchId::generate();
        let target_head = StoredBranchHead {
            major: source_head.major,
            volume_id: source_head.volume_id,
            branch_id: *target_id.as_bytes(),
            lifecycle: BranchLifecycle::Active,
            state,
            maintenance_epoch: 0,
            maintenance_active: false,
            maintenance_owner: None,
        };
        let bytes = encode(
            HEAD_MAGIC,
            &target_head,
            MAX_HEAD_BYTES,
            "fork Managed branch",
        )?;
        if !self
            .create(&head_key(target_id), bytes, "fork Managed branch")
            .await?
        {
            return Err(conflict(
                "fork Managed branch",
                "target branch identity already exists",
            ));
        }
        registry
            .branches
            .insert(target.clone(), *target_id.as_bytes());
        let bytes = encode(
            REGISTRY_MAGIC,
            &registry,
            MAX_REGISTRY_BYTES,
            "fork Managed branch",
        )?;
        match self
            .replace(REGISTRY_KEY, &revision, bytes, "fork Managed branch")
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Err(conflict(
                    "fork Managed branch",
                    "branch registry changed concurrently",
                ));
            }
            Err(error) => {
                return match self.get(&target).await {
                    Ok(current) if current.binding.id == target_id => Ok(current),
                    Ok(_) => Err(conflict(
                        "fork Managed branch",
                        "target branch was created concurrently",
                    )),
                    Err(observed) if observed.kind() == ManagedErrorKind::Invalid => Err(error),
                    Err(observed) => Err(observed),
                };
            }
        }
        info(
            target,
            target_id,
            &target_head,
            BranchId::from_bytes(registry.default_branch),
        )
    }

    async fn find_history_state(
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
        let bytes = object::read_content_addressed(
            &self.operator,
            &checkpoint_key(id),
            &id,
            "read Managed branch",
            "branch checkpoint is missing",
            "branch checkpoint identity is invalid",
        )
        .await?;
        let root: CheckpointRoot = decode(
            CHECKPOINT_MAGIC,
            &bytes,
            MAX_CHECKPOINT_ROOT_BYTES,
            "read Managed branch",
        )?;
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
        let mut parts = Vec::with_capacity(root.parts.len());
        for reference in &root.parts {
            let bytes = object::read_content_addressed(
                &self.operator,
                &checkpoint_part_key(reference.id),
                &reference.id,
                "read Managed branch",
                "branch checkpoint part is missing",
                "branch checkpoint part identity is invalid",
            )
            .await?;
            parts.push(decode(
                CHECKPOINT_PART_MAGIC,
                &bytes,
                MAX_CHECKPOINT_PART_BYTES,
                "read Managed branch",
            )?);
        }
        root.recover(parts)
    }

    async fn write_checkpoint(
        &self,
        checkpoint: &StoredCheckpoint,
    ) -> Result<[u8; 32], ManagedError> {
        let pending = PendingCheckpoint::from_checkpoint(checkpoint)?;
        let mut references = Vec::with_capacity(pending.parts.len());
        for part in &pending.parts {
            let bytes = encode(
                CHECKPOINT_PART_MAGIC,
                part,
                MAX_CHECKPOINT_PART_BYTES,
                "checkpoint Managed branch",
            )?;
            let id: [u8; 32] = Sha256::digest(&bytes).into();
            self.ensure_immutable(
                &checkpoint_part_key(id),
                &bytes,
                "checkpoint Managed branch",
            )
            .await?;
            references.push(PartRef {
                id,
                records: part.records.len() as u32,
            });
        }
        let root = pending.finish(references)?;
        let bytes = encode(
            CHECKPOINT_MAGIC,
            &root,
            MAX_CHECKPOINT_ROOT_BYTES,
            "checkpoint Managed branch",
        )?;
        let id: [u8; 32] = Sha256::digest(&bytes).into();
        self.ensure_immutable(&checkpoint_key(id), &bytes, "checkpoint Managed branch")
            .await?;
        Ok(id)
    }

    async fn read_history(&self, id: [u8; 32]) -> Result<StoredHistory, ManagedError> {
        let bytes = object::read_content_addressed(
            &self.operator,
            &history_key(id),
            &id,
            "read Managed branch",
            "branch history is missing",
            "branch history identity is invalid",
        )
        .await?;
        let history: StoredHistory = decode(
            HISTORY_MAGIC,
            &bytes,
            MAX_HISTORY_BYTES,
            "read Managed branch",
        )?;
        history.validate(self.volume_id)?;
        Ok(history)
    }

    async fn write_history(&self, history: &StoredHistory) -> Result<[u8; 32], ManagedError> {
        let bytes = encode(
            HISTORY_MAGIC,
            history,
            MAX_HISTORY_BYTES,
            "archive Managed branch history",
        )?;
        let id: [u8; 32] = Sha256::digest(&bytes).into();
        self.ensure_immutable(&history_key(id), &bytes, "archive Managed branch history")
            .await?;
        Ok(id)
    }

    async fn ensure_immutable(
        &self,
        key: &str,
        expected: &[u8],
        action: &'static str,
    ) -> Result<(), ManagedError> {
        object::ensure_immutable(
            &self.operator,
            key,
            expected,
            action,
            ManagedErrorKind::Corrupt,
            "immutable branch object changed",
        )
        .await
    }

    async fn registry(&self) -> Result<(StoredBranchRegistry, String), ManagedError> {
        self.read_registry()
            .await?
            .ok_or_else(|| corrupt("read Managed branches", "branch registry is missing"))
    }

    async fn read_registry(&self) -> Result<Option<(StoredBranchRegistry, String)>, ManagedError> {
        let Some((bytes, revision)) =
            object::read_with_revision(&self.operator, REGISTRY_KEY, "read Managed branches")
                .await?
        else {
            return Ok(None);
        };
        let registry: StoredBranchRegistry = decode(
            REGISTRY_MAGIC,
            &bytes,
            MAX_REGISTRY_BYTES,
            "read Managed branches",
        )?;
        registry.validate(self.volume_id)?;
        Ok(Some((registry, revision)))
    }

    async fn read_head(
        &self,
        branch_id: BranchId,
    ) -> Result<Option<(StoredBranchHead, String)>, ManagedError> {
        let Some((bytes, revision)) =
            object::read_with_revision(&self.operator, &head_key(branch_id), "read Managed branch")
                .await?
        else {
            return Ok(None);
        };
        let head: StoredBranchHead =
            decode(HEAD_MAGIC, &bytes, MAX_HEAD_BYTES, "read Managed branch")?;
        head.validate(self.volume_id, branch_id)?;
        Ok(Some((head, revision)))
    }

    async fn create(
        &self,
        key: &str,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        object::create(&self.operator, key, bytes, action).await
    }

    async fn replace(
        &self,
        key: &str,
        expected_revision: &str,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        object::replace(&self.operator, key, expected_revision, bytes, action).await
    }
}

fn remove_sealed_incarnation(
    registry: &mut StoredBranchRegistry,
    name: &BranchName,
    branch_id: BranchId,
    action: &'static str,
) -> Result<bool, ManagedError> {
    if registry.branch_id(name) != Some(branch_id) {
        return Ok(false);
    }
    if registry.default_branch == *branch_id.as_bytes() {
        return Err(corrupt(action, "default branch HEAD is sealed"));
    }
    Ok(registry.remove_if(name, branch_id))
}

fn head_key(branch: BranchId) -> String {
    format!("{ROOT}/heads/{branch}.ofs")
}

fn checkpoint_key(id: [u8; 32]) -> String {
    format!("{ROOT}/checkpoints/sha256/{}.ofs", hex(&id))
}

fn checkpoint_part_key(id: [u8; 32]) -> String {
    format!("{ROOT}/checkpoint-parts/sha256/{}.ofs", hex(&id))
}

fn history_key(id: [u8; 32]) -> String {
    format!("{ROOT}/history/sha256/{}.ofs", hex(&id))
}

fn canonical_metadata_key(path: &str, prefix: &str, identity_bytes: usize) -> bool {
    let Some(identity) = path
        .strip_prefix(prefix)
        .and_then(|path| path.strip_suffix(".ofs"))
    else {
        return false;
    };
    identity.len() == identity_bytes * 2
        && identity
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn encode<T: Serialize>(
    magic: &[u8; 8],
    value: &T,
    maximum: usize,
    action: &'static str,
) -> Result<Vec<u8>, ManagedError> {
    let mut body = Vec::new();
    ciborium::ser::into_writer(value, &mut body)
        .map_err(|_| invalid(action, "branch record cannot be encoded"))?;
    if body.len() > maximum {
        return Err(invalid(action, "branch record exceeds its size limit"));
    }
    let mut bytes = Vec::with_capacity(magic.len() + body.len() + 32);
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&body);
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

fn decode<T: DeserializeOwned>(
    magic: &[u8; 8],
    bytes: &[u8],
    maximum: usize,
    action: &'static str,
) -> Result<T, ManagedError> {
    let body = bytes
        .strip_prefix(magic)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| corrupt(action, "branch record format is invalid"))?;
    if body.len() > maximum
        || Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != &bytes[bytes.len() - 32..]
    {
        return Err(corrupt(action, "branch record checksum is invalid"));
    }
    let mut input = Cursor::new(body);
    let value = ciborium::de::from_reader(&mut input)
        .map_err(|_| corrupt(action, "branch record cannot be decoded"))?;
    if input.position() != body.len() as u64 {
        return Err(corrupt(action, "branch record has trailing bytes"));
    }
    Ok(value)
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn conflict(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Conflict, action, message)
}

fn position_not_retained(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Invalid,
        action,
        "branch position is not retained",
    )
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
        "object branch metadata is unavailable",
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use opendal::services::Memory;

    use crate::filesystem::{DirectoryEntry, NodeAttributes, NodeKind};
    use crate::managed::format::ExtentMap;
    use crate::managed::metadata::namespace::{
        DirectoryRecord, FileVersionRecord, NamespaceSnapshot, NodeRecord, managed_generation,
    };

    fn checkpoint_snapshot(entries: usize) -> NamespaceSnapshot {
        let volume_id = VolumeId::from_bytes([9; 16]);
        let root = crate::filesystem::NodeId::from_bytes([8; 16]);
        let file = crate::filesystem::NodeId::from_bytes([6; 16]);
        let operation = OperationId::from_bytes([7; 16]);
        let cursor = ChangeCursor::at(std::num::NonZeroU64::new(1).unwrap(), operation);
        let version = FileVersionRecord::from_extents(
            0,
            Sha256::digest([]).into(),
            ExtentMap {
                extents: Vec::new(),
            },
        )
        .unwrap();
        let entries = (0..entries)
            .map(|index| {
                (
                    format!("{index:08}-{}", "x".repeat(96)),
                    DirectoryEntry {
                        node: file,
                        kind: NodeKind::RegularFile,
                    },
                )
            })
            .collect();
        NamespaceSnapshot {
            volume_id,
            cursor,
            root,
            nodes: BTreeMap::from([
                (
                    root,
                    NodeRecord {
                        id: root,
                        generation: managed_generation(1),
                        kind: NodeKind::Directory,
                        attributes: NodeAttributes::default(),
                        file_version: None,
                    },
                ),
                (
                    file,
                    NodeRecord {
                        id: file,
                        generation: managed_generation(1),
                        kind: NodeKind::RegularFile,
                        attributes: NodeAttributes::default(),
                        file_version: Some(version.id),
                    },
                ),
            ]),
            directories: BTreeMap::from([(
                root,
                DirectoryRecord {
                    node: root,
                    generation: managed_generation(1),
                    entries,
                },
            )]),
            file_versions: BTreeMap::from([(version.id, version)]),
        }
    }

    #[tokio::test]
    async fn checkpoint_parts_round_trip_and_a_missing_part_is_corrupt() {
        let snapshot = checkpoint_snapshot(5_000);
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let store = ObjectBranchStore {
            volume_id: snapshot.volume_id,
            operator: operator.clone(),
        };
        let checkpoint = StoredCheckpoint::new(&snapshot, BTreeMap::new()).unwrap();
        let id = store.write_checkpoint(&checkpoint).await.unwrap();
        let root = store.read_checkpoint_root(id).await.unwrap();
        assert!(root.parts.len() > 1);
        let recovered = store
            .read_checkpoint(id)
            .await
            .unwrap()
            .recover(snapshot.volume_id)
            .unwrap()
            .0;
        assert_eq!(recovered, snapshot);

        operator
            .delete(&checkpoint_part_key(root.parts[0].id))
            .await
            .unwrap();
        assert_eq!(
            store.read_checkpoint(id).await.unwrap_err().kind(),
            ManagedErrorKind::Corrupt
        );
    }

    #[test]
    fn durable_takeover_revokes_the_previous_gc_owner() {
        let volume = VolumeId::from_bytes([1; 16]);
        let branch = BranchId::from_bytes([2; 16]);
        let main = BranchName::parse("main").unwrap();
        let old = ObjectBranchGcFence {
            epoch: 1,
            owner: [3; 16],
        };
        let current = ObjectBranchGcFence {
            epoch: 1,
            owner: [4; 16],
        };
        let mut registry = StoredBranchRegistry::initial(volume, main, branch);
        registry.maintenance_epoch = current.epoch;
        registry.maintenance_active = true;
        registry.maintenance_owner = Some(current.owner);

        let bytes = encode(
            REGISTRY_MAGIC,
            &registry,
            MAX_REGISTRY_BYTES,
            "test branch GC",
        )
        .unwrap();
        let recovered: StoredBranchRegistry =
            decode(REGISTRY_MAGIC, &bytes, MAX_REGISTRY_BYTES, "test branch GC").unwrap();
        assert!(!old.owns_registry(&recovered));
        assert!(current.owns_registry(&recovered));
    }

    #[test]
    fn registry_authority_has_no_format_level_one_mib_limit() {
        let volume = VolumeId::from_bytes([1; 16]);
        let default = BranchId::from_bytes([2; 16]);
        let mut registry =
            StoredBranchRegistry::initial(volume, BranchName::parse("main").unwrap(), default);
        for index in 0_u64..40_000 {
            let mut id = [0; 16];
            id[..8].copy_from_slice(&index.to_be_bytes());
            id[8..].copy_from_slice(&index.to_be_bytes());
            registry
                .branches
                .insert(BranchName::parse(format!("branch-{index:08}")).unwrap(), id);
        }

        let bytes = encode(
            REGISTRY_MAGIC,
            &registry,
            MAX_REGISTRY_BYTES,
            "test large branch registry",
        )
        .unwrap();
        assert!(bytes.len() > 1024 * 1024);
        let recovered: StoredBranchRegistry = decode(
            REGISTRY_MAGIC,
            &bytes,
            MAX_REGISTRY_BYTES,
            "test large branch registry",
        )
        .unwrap();
        assert_eq!(recovered.branches.len(), registry.branches.len());
    }

    #[test]
    fn repeated_history_identity_is_a_cycle() {
        let mut chain = BTreeSet::new();
        visit_history(&mut chain, [1; 32], "test branch history").unwrap();
        assert_eq!(
            visit_history(&mut chain, [1; 32], "test branch history")
                .unwrap_err()
                .kind(),
            ManagedErrorKind::Corrupt
        );
    }

    #[test]
    fn sealed_cleanup_is_exact_and_preserves_a_replacement() {
        let volume = VolumeId::from_bytes([1; 16]);
        let default = BranchId::from_bytes([2; 16]);
        let old = BranchId::from_bytes([3; 16]);
        let replacement = BranchId::from_bytes([4; 16]);
        let main = BranchName::parse("main").unwrap();
        let name = BranchName::parse("work").unwrap();
        let mut registry = StoredBranchRegistry::initial(volume, main.clone(), default);

        registry.branches.insert(name.clone(), *old.as_bytes());
        assert!(remove_sealed_incarnation(&mut registry, &name, old, "test branch GC").unwrap());
        assert_eq!(registry.branch_id(&name), None);

        registry
            .branches
            .insert(name.clone(), *replacement.as_bytes());
        assert!(!remove_sealed_incarnation(&mut registry, &name, old, "test branch GC").unwrap());
        assert_eq!(registry.branch_id(&name), Some(replacement));

        assert_eq!(
            remove_sealed_incarnation(&mut registry, &main, default, "test branch GC")
                .unwrap_err()
                .kind(),
            ManagedErrorKind::Corrupt
        );
    }
}
