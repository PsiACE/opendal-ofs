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

use super::records::{
    BranchInfo, BranchLifecycle, ForkPoint, StoredBranchHead, StoredBranchRegistry, StoredChange,
    StoredCheckpoint, StoredCommittedResult, StoredHistory, StoredNamespaceState, info,
    require_request_digest,
};
use crate::filesystem::{
    BranchBinding, BranchId, BranchName, ChangeCursor, CommitOutcome, OperationId, VolumeId,
};
use crate::managed::metadata::d1::{D1Result, D1Session, D1Statement, statement};
use crate::managed::metadata::namespace::{NamespacePublication, NamespaceSnapshot};
use crate::managed::{
    D1Metadata, ManagedData, ManagedError, ManagedErrorKind, SegmentGcMaintenance,
};

const REGISTRY: &str = "ofs_managed_branch_v1_registry";
const HEADS: &str = "ofs_managed_branch_v1_heads";
const CHECKPOINTS: &str = "ofs_managed_branch_v1_checkpoints";
const HISTORY: &str = "ofs_managed_branch_v1_history";
const SCHEMA_RESULTS: usize = 4;
const MAX_REGISTRY_BYTES: usize = 1024 * 1024;
const MAX_HEAD_BYTES: usize = 1024 * 1024;
const MAX_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;
const MAX_HISTORY_BYTES: usize = 1024 * 1024;
const CHECKPOINT_PART_BYTES: usize = 512 * 1024;
const METADATA_PAGE_SIZE: usize = 1000;
const MAX_DELETE_IDS_BYTES: usize = 96 * 1024;

#[derive(Clone)]
pub struct D1BranchStore {
    volume_id: VolumeId,
    session: D1Session,
}

#[derive(Clone)]
pub struct D1BoundNamespace {
    store: D1BranchStore,
    binding: BranchBinding,
}

#[derive(Clone, Debug)]
pub struct D1BranchObservation {
    pub(crate) snapshot: NamespaceSnapshot,
    registry_revision: u64,
    head_revision: u64,
    head: StoredBranchHead,
    checkpoint: StoredCheckpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct D1BranchGcFence {
    epoch: u64,
}

impl D1BoundNamespace {
    pub fn binding(&self) -> &BranchBinding {
        &self.binding
    }

    pub fn volume_id(&self) -> VolumeId {
        self.store.volume_id
    }

    pub(crate) async fn observe(&self) -> Result<Option<D1BranchObservation>, ManagedError> {
        let authority = self.current_head("read Managed branch").await?;
        let Some(state) = &authority.head.state else {
            return Ok(None);
        };
        let checkpoint = self.store.read_checkpoint(state.checkpoint).await?;
        let (mut snapshot, _) = checkpoint.clone().recover(self.store.volume_id)?;
        if snapshot.cursor != state.checkpoint_cursor.decode()? {
            return Err(corrupt(
                "read Managed branch",
                "branch checkpoint and HEAD disagree",
            ));
        }
        for change in &state.tail {
            snapshot = change.apply(Some(snapshot))?;
        }
        if snapshot.cursor != state.cursor()? {
            return Err(corrupt(
                "read Managed branch",
                "branch tail does not reach HEAD",
            ));
        }
        Ok(Some(D1BranchObservation {
            snapshot,
            registry_revision: authority.registry_revision,
            head_revision: authority.head_revision,
            head: authority.head,
            checkpoint,
        }))
    }

    pub(crate) async fn observe_from(
        &self,
        _base: &NamespaceSnapshot,
    ) -> Result<Option<D1BranchObservation>, ManagedError> {
        self.observe().await
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<&D1BranchObservation>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        let branch_id = self.binding.id;
        let (head, head_revision, registry_revision, base, checkpoint) = match observed {
            Some(observed) => (
                observed.head.clone(),
                observed.head_revision,
                observed.registry_revision,
                Some(&observed.snapshot),
                Some(observed.checkpoint.clone()),
            ),
            None => {
                let authority = self.current_head("publish Managed branch").await?;
                if authority.head.state.is_some() {
                    return self.outcome_after_race(publication.operation).await;
                }
                (
                    authority.head,
                    authority.head_revision,
                    authority.registry_revision,
                    None,
                    None,
                )
            }
        };
        let (change, valid) = StoredChange::prepare(branch_id, publication, base)?;
        let request_digest = change.request_digest()?;
        if !valid {
            if matches!(
                self.resolve_known(publication.operation, Some(request_digest))
                    .await?,
                CommitOutcome::Committed(_)
            ) {
                return Ok(CommitOutcome::Committed(publication.target.cursor));
            }
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            });
        }
        if let CommitOutcome::Committed(cursor) = self
            .resolve_known(publication.operation, Some(request_digest))
            .await?
        {
            return Ok(CommitOutcome::Committed(cursor));
        }

        let state = match (&head.state, checkpoint) {
            (None, None) => {
                let mut results = BTreeMap::new();
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
                if current.tail.len() + 1 >= super::records::MAX_TAIL_TRANSACTIONS
                    || appended_bytes > super::records::MAX_TAIL_BYTES
                {
                    let history = StoredHistory::new(self.store.volume_id, branch_id, current)?;
                    let history_id = self.store.write_history(&history).await?;
                    let (_, mut results) = checkpoint.recover(self.store.volume_id)?;
                    for prior in &current.tail {
                        let result = StoredCommittedResult::from_change(prior)?;
                        results.insert((result.origin(), result.operation()), result);
                    }
                    let result = StoredCommittedResult::from_change(&change)?;
                    results.insert((result.origin(), result.operation()), result);
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
        };
        match self
            .store
            .replace_head(
                branch_id,
                head_revision,
                registry_revision,
                &next,
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
        let authority = self
            .current_head("resolve Managed branch publication")
            .await?;
        let Some(state) = authority.head.state else {
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
        let (_, results) = checkpoint.recover(self.store.volume_id)?;
        let Some(result) = results.get(&(self.binding.id, operation)) else {
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

    async fn current_head(&self, action: &'static str) -> Result<D1HeadAuthority, ManagedError> {
        let (registry, registry_revision) = self.store.registry().await?;
        if registry.maintenance_active {
            return Err(conflict(action, "branch maintenance is active"));
        }
        if registry.branch_id(&self.binding.name) != Some(self.binding.id) {
            return Err(conflict(action, "branch incarnation no longer exists"));
        }
        let (head, head_revision) = self
            .store
            .read_head(self.binding.id)
            .await?
            .ok_or_else(|| conflict(action, "branch incarnation no longer exists"))?;
        if head.lifecycle != BranchLifecycle::Active {
            return Err(conflict(action, "branch is sealed for deletion"));
        }
        Ok(D1HeadAuthority {
            registry_revision,
            head_revision,
            head,
        })
    }
}

struct D1HeadAuthority {
    registry_revision: u64,
    head_revision: u64,
    head: StoredBranchHead,
}

struct D1GcRoots {
    snapshots: Vec<NamespaceSnapshot>,
    heads: BTreeSet<String>,
    checkpoints: BTreeSet<String>,
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
                    "INSERT OR IGNORE INTO {REGISTRY} (store_key, volume_id, revision, maintenance_epoch, maintenance_state, record_json) VALUES (?, ?, 1, 0, 'idle', ?)"
                ),
                vec![
                    self.store_key().into(),
                    self.volume_id.to_string().into(),
                    registry_json.into(),
                ],
            ),
            statement(
                format!(
                    "SELECT revision, volume_id, maintenance_epoch, maintenance_state, record_json FROM {REGISTRY} WHERE store_key = ?"
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
        let registry_json = encode(&registry, MAX_REGISTRY_BYTES, "begin Managed branch GC")?;
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {REGISTRY} SET revision = revision + 1, maintenance_epoch = ?, maintenance_state = 'sweeping', record_json = ? WHERE store_key = ? AND revision = ? AND maintenance_state = 'idle' RETURNING maintenance_epoch"
            ),
            vec![
                sqlite_integer(registry.maintenance_epoch, "begin Managed branch GC")?.into(),
                registry_json.into(),
                self.store_key().into(),
                sqlite_integer(registry_revision, "begin Managed branch GC")?.into(),
            ],
        ));
        let results = self.session.query(batch, "begin Managed branch GC").await?;
        let changed = rows(&results, SCHEMA_RESULTS, "begin Managed branch GC")?;
        if let [row] = changed {
            return Ok(D1BranchGcFence {
                epoch: integer(row, "maintenance_epoch", "begin Managed branch GC")?,
            });
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

    async fn finish_gc(&self, fence: D1BranchGcFence) -> Result<(), ManagedError> {
        let (mut registry, registry_revision) = self.registry().await?;
        if !registry.maintenance_active && registry.maintenance_epoch == fence.epoch {
            return Ok(());
        }
        if !registry.maintenance_active || registry.maintenance_epoch != fence.epoch {
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
                "UPDATE {REGISTRY} SET revision = revision + 1, maintenance_state = 'idle', record_json = ? WHERE store_key = ? AND revision = ? AND maintenance_epoch = ? AND maintenance_state = 'sweeping' RETURNING revision"
            ),
            vec![
                registry_json.into(),
                self.store_key().into(),
                sqlite_integer(registry_revision, "finish Managed branch GC")?.into(),
                sqlite_integer(fence.epoch, "finish Managed branch GC")?.into(),
            ],
        ));
        let results = self
            .session
            .query(batch, "finish Managed branch GC")
            .await?;
        let changed = rows(&results, SCHEMA_RESULTS, "finish Managed branch GC")?;
        if changed.len() == 1 {
            return Ok(());
        }
        if changed.len() > 1 {
            return Err(corrupt(
                "finish Managed branch GC",
                "D1 returned duplicate registries",
            ));
        }
        let (current, _) = self.registry().await?;
        if !current.maintenance_active && current.maintenance_epoch == fence.epoch {
            Ok(())
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
        let (registry, _) = self.registry().await?;
        if !registry.maintenance_active {
            return Err(conflict(
                "resume Managed branch GC",
                "no interrupted branch GC is active",
            ));
        }
        let fence = D1BranchGcFence {
            epoch: registry.maintenance_epoch,
        };
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
        if !registry.maintenance_active || registry.maintenance_epoch != fence.epoch {
            return Err(conflict(
                "mark Managed branch GC roots",
                "GC fence does not match the registry",
            ));
        }
        let mut roots = D1GcRoots {
            snapshots: Vec::new(),
            heads: BTreeSet::new(),
            checkpoints: BTreeSet::new(),
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
            let Some(state) = head.state else {
                continue;
            };
            roots.checkpoints.insert(encode_id(state.checkpoint));
            roots
                .snapshots
                .extend(self.snapshots_for_state(&state).await?);
            let mut history_id = state.previous_history;
            let mut chain = BTreeSet::new();
            while let Some(id) = history_id {
                if !chain.insert(id) {
                    return Err(corrupt(
                        "mark Managed branch GC roots",
                        "branch history contains a cycle",
                    ));
                }
                roots.histories.insert(encode_id(id));
                let history = self.read_history(id).await?;
                roots.checkpoints.insert(encode_id(history.checkpoint));
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
                        "DELETE FROM {table} WHERE store_key = ? AND {key} IN (SELECT value FROM json_each(?)) AND EXISTS (SELECT 1 FROM {REGISTRY} WHERE store_key = ? AND maintenance_epoch = ? AND maintenance_state = 'sweeping')"
                    ),
                    vec![
                        self.store_key().into(),
                        encoded.into(),
                        self.store_key().into(),
                        sqlite_integer(fence.epoch, action)?.into(),
                    ],
                ),
                statement(
                    format!(
                        "SELECT maintenance_epoch FROM {REGISTRY} WHERE store_key = ? AND maintenance_epoch = ? AND maintenance_state = 'sweeping'"
                    ),
                    vec![
                        self.store_key().into(),
                        sqlite_integer(fence.epoch, action)?.into(),
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
            if integer(row, "maintenance_epoch", action)? != fence.epoch {
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
        while let Some(id) = history_id {
            let history = self.read_history(id).await?;
            if let Some(state) = history.state_at(sequence) {
                return Ok(Some(state));
            }
            history_id = history.previous_history;
        }
        Ok(None)
    }

    async fn read_checkpoint(&self, id: [u8; 32]) -> Result<StoredCheckpoint, ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "SELECT part_index, part_count, total_bytes, record_part FROM {CHECKPOINTS} WHERE store_key = ? AND checkpoint_id = ? ORDER BY part_index"
            ),
            vec![self.store_key().into(), encode_id(id).into()],
        ));
        let results = self.session.query(batch, "read Managed branch").await?;
        decode_checkpoint_rows(
            rows(&results, SCHEMA_RESULTS, "read Managed branch")?,
            id,
            "read Managed branch",
        )
    }

    async fn write_checkpoint(
        &self,
        checkpoint: &StoredCheckpoint,
    ) -> Result<[u8; 32], ManagedError> {
        let encoded = encode(
            checkpoint,
            MAX_CHECKPOINT_BYTES,
            "checkpoint Managed branch",
        )?;
        let id: [u8; 32] = Sha256::digest(encoded.as_bytes()).into();
        let parts = Self::checkpoint_parts(&encoded);
        let part_count = parts.len() as u64;
        let total_bytes = encoded.len() as u64;
        let mut batch = schema_statements();
        for (index, part) in parts.iter().enumerate() {
            batch.push(statement(
                format!(
                    "INSERT OR IGNORE INTO {CHECKPOINTS} (store_key, checkpoint_id, part_index, part_count, total_bytes, record_part) VALUES (?, ?, ?, ?, ?, ?)"
                ),
                vec![
                    self.store_key().into(),
                    encode_id(id).into(),
                    (index as u64).into(),
                    part_count.into(),
                    total_bytes.into(),
                    (*part).to_owned().into(),
                ],
            ));
        }
        batch.push(statement(
            format!(
                "SELECT part_index, part_count, total_bytes, record_part FROM {CHECKPOINTS} WHERE store_key = ? AND checkpoint_id = ? ORDER BY part_index"
            ),
            vec![self.store_key().into(), encode_id(id).into()],
        ));
        let results = self
            .session
            .query(batch, "checkpoint Managed branch")
            .await?;
        let observed = decode_checkpoint_rows(
            rows(
                &results,
                SCHEMA_RESULTS + parts.len(),
                "checkpoint Managed branch",
            )?,
            id,
            "checkpoint Managed branch",
        )?;
        let observed = encode(&observed, MAX_CHECKPOINT_BYTES, "checkpoint Managed branch")?;
        if observed != encoded {
            return Err(corrupt(
                "checkpoint Managed branch",
                "immutable branch checkpoint changed",
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
                "SELECT revision, volume_id, maintenance_epoch, maintenance_state, record_json FROM {REGISTRY} WHERE store_key = ?"
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

    /// Split one immutable checkpoint into rows below D1's 2 MB row limit.
    fn checkpoint_parts(bytes: &str) -> Vec<&str> {
        let mut parts = Vec::new();
        let mut start = 0;
        while start < bytes.len() {
            let mut end = (start + CHECKPOINT_PART_BYTES).min(bytes.len());
            while !bytes.is_char_boundary(end) {
                end -= 1;
            }
            parts.push(&bytes[start..end]);
            start = end;
        }
        parts
    }
}

fn schema_statements() -> Vec<D1Statement> {
    vec![
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {REGISTRY} (store_key TEXT PRIMARY KEY, volume_id TEXT NOT NULL, revision INTEGER NOT NULL, maintenance_epoch INTEGER NOT NULL, maintenance_state TEXT NOT NULL CHECK (maintenance_state IN ('idle', 'sweeping')), record_json TEXT NOT NULL)"
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
                "CREATE TABLE IF NOT EXISTS {CHECKPOINTS} (store_key TEXT NOT NULL, checkpoint_id TEXT NOT NULL, part_index INTEGER NOT NULL, part_count INTEGER NOT NULL, total_bytes INTEGER NOT NULL, record_part TEXT NOT NULL, PRIMARY KEY (store_key, checkpoint_id, part_index))"
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

fn decode_checkpoint_rows(
    rows: &[Value],
    id: [u8; 32],
    action: &'static str,
) -> Result<StoredCheckpoint, ManagedError> {
    if rows.is_empty() {
        return Err(corrupt(action, "branch checkpoint is missing"));
    }
    let part_count = integer(&rows[0], "part_count", action)?;
    let total_bytes = integer(&rows[0], "total_bytes", action)?;
    if part_count == 0
        || part_count as usize != rows.len()
        || total_bytes as usize > MAX_CHECKPOINT_BYTES
    {
        return Err(corrupt(action, "branch checkpoint parts are invalid"));
    }
    let mut encoded = String::with_capacity(total_bytes as usize);
    for (index, row) in rows.iter().enumerate() {
        if integer(row, "part_index", action)? != index as u64
            || integer(row, "part_count", action)? != part_count
            || integer(row, "total_bytes", action)? != total_bytes
        {
            return Err(corrupt(action, "branch checkpoint parts are invalid"));
        }
        encoded.push_str(text(row, "record_part", action)?);
    }
    if encoded.len() as u64 != total_bytes || Sha256::digest(encoded.as_bytes()).as_slice() != id {
        return Err(corrupt(action, "branch checkpoint identity is invalid"));
    }
    decode(&encoded, MAX_CHECKPOINT_BYTES, action)
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
    if registry.maintenance_epoch != epoch
        || registry.maintenance_active != (maintenance == "sweeping")
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
