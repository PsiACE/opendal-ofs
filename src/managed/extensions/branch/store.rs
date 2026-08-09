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

//! Branch authority over native revision-CAS records.

use std::collections::BTreeSet;
use std::io::Cursor;

use super::records::{BranchInfo, ForkPoint, StoredBranchRegistry, info};
use crate::filesystem::{BranchBinding, BranchId, BranchName, OperationId, VolumeId};
use crate::managed::data::RetainedDataRoots;
use crate::managed::metadata::namespace::{
    NamespaceStore, StoredHead, StoredNamespaceState, checkpoint_key, decode_head, encode_head,
    history_key,
};
use crate::managed::metadata::record::{RecordBackend, Revision};
use crate::managed::{ManagedData, ManagedError, ManagedErrorKind, SegmentGcMaintenance};
use opendal::Operator;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};

const ROOT: &str = ".ofs/managed/metadata/v1/extensions/branch/v1";
const REGISTRY_KEY: &str = ".ofs/managed/metadata/v1/extensions/branch/v1/registry.ofs";
const REGISTRY_MAGIC: &[u8; 8] = b"OFS1BRG1";

#[derive(Clone)]
pub struct BranchStore {
    pub(crate) volume_id: VolumeId,
    pub(crate) backend: RecordBackend,
    data: Operator,
}

/// A branch incarnation bound to the shared Managed namespace state machine.
pub struct BoundNamespace(pub(crate) NamespaceStore);

impl BoundNamespace {
    pub fn binding(&self) -> &BranchBinding {
        self.0
            .binding()
            .expect("a bound branch namespace has a branch identity")
    }

    pub fn volume_id(&self) -> VolumeId {
        self.0.volume_id()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchGcFence {
    epoch: u64,
    owner: [u8; 16],
}

impl BranchGcFence {
    fn owns_registry(self, registry: &StoredBranchRegistry) -> bool {
        registry.maintenance_epoch == self.epoch && registry.maintenance_owner == Some(self.owner)
    }

    fn owns_head(self, head: &StoredHead) -> bool {
        head.maintenance_epoch == self.epoch && head.maintenance_owner == Some(self.owner)
    }
}

struct GcRoots {
    data: RetainedDataRoots,
    heads: BTreeSet<String>,
    checkpoints: BTreeSet<String>,
    histories: BTreeSet<String>,
}

enum CasMutation<T> {
    Return(T),
    Replace(T),
}

enum CasFailure {
    BeforeReplace(ManagedError),
    Replace(ManagedError),
}

impl From<CasFailure> for ManagedError {
    fn from(failure: CasFailure) -> Self {
        match failure {
            CasFailure::BeforeReplace(error) | CasFailure::Replace(error) => error,
        }
    }
}

impl BranchStore {
    fn namespace(&self, binding: BranchBinding) -> NamespaceStore {
        let id = binding.id;
        NamespaceStore::branch(
            self.volume_id,
            self.data.clone(),
            self.backend.clone(),
            binding,
            head_key(id),
        )
    }

    async fn mutate_registry<T>(
        &self,
        action: &'static str,
        mut mutate: impl FnMut(&mut StoredBranchRegistry) -> Result<CasMutation<T>, ManagedError>,
    ) -> Result<T, CasFailure> {
        loop {
            let (mut registry, revision) =
                self.registry().await.map_err(CasFailure::BeforeReplace)?;
            let result = match mutate(&mut registry).map_err(CasFailure::BeforeReplace)? {
                CasMutation::Return(result) => return Ok(result),
                CasMutation::Replace(result) => result,
            };
            let bytes =
                encode(REGISTRY_MAGIC, &registry, action).map_err(CasFailure::BeforeReplace)?;
            match self
                .backend
                .replace(REGISTRY_KEY, &revision, bytes, action)
                .await
            {
                Ok(true) => return Ok(result),
                Ok(false) => continue,
                Err(error) => return Err(CasFailure::Replace(error)),
            }
        }
    }

    async fn mutate_head<T>(
        &self,
        branch: BranchId,
        action: &'static str,
        mut mutate: impl FnMut(&mut StoredHead) -> Result<CasMutation<T>, ManagedError>,
    ) -> Result<Option<T>, CasFailure> {
        loop {
            let Some((mut head, revision)) = self
                .read_head(branch)
                .await
                .map_err(CasFailure::BeforeReplace)?
            else {
                return Ok(None);
            };
            let result = match mutate(&mut head).map_err(CasFailure::BeforeReplace)? {
                CasMutation::Return(result) => return Ok(Some(result)),
                CasMutation::Replace(result) => result,
            };
            let bytes = encode_head(&head).map_err(CasFailure::BeforeReplace)?;
            match self
                .backend
                .replace(&head_key(branch), &revision, bytes, action)
                .await
            {
                Ok(true) => return Ok(Some(result)),
                Ok(false) => continue,
                Err(error) => return Err(CasFailure::Replace(error)),
            }
        }
    }

    pub(crate) fn new(volume_id: VolumeId, data: Operator, backend: RecordBackend) -> Self {
        Self {
            volume_id,
            backend,
            data,
        }
    }

    /// Idempotently create the first unborn branch. The head is prepared
    /// before the registry, making the registry the branch-existence authority.
    pub async fn initialize(&self, default_name: BranchName) -> Result<BranchInfo, ManagedError> {
        if let Some((registry, _)) = self.read_registry().await? {
            return self.initialized(&registry, default_name).await;
        }

        let branch_id = BranchId::generate();
        let head = StoredHead::unborn(self.volume_id, Some(branch_id));
        let encoded_head = encode_head(&head)?;
        let _ = self
            .backend
            .create(&head_key(branch_id), encoded_head, "create Managed branch")
            .await?;
        let registry =
            StoredBranchRegistry::initial(self.volume_id, default_name.clone(), branch_id);
        let encoded_registry = encode(REGISTRY_MAGIC, &registry, "initialize Managed branches")?;
        if !self
            .backend
            .create(
                REGISTRY_KEY,
                encoded_registry,
                "initialize Managed branches",
            )
            .await?
        {
            let (registry, _) = self
                .read_registry()
                .await?
                .ok_or_else(|| unavailable("initialize Managed branches"))?;
            return self.initialized(&registry, default_name).await;
        }
        Ok(info(default_name, branch_id, &head, branch_id))
    }

    async fn initialized(
        &self,
        registry: &StoredBranchRegistry,
        default_name: BranchName,
    ) -> Result<BranchInfo, ManagedError> {
        if registry.default_binding().map(|binding| binding.name) != Some(default_name.clone()) {
            return Err(conflict(
                "initialize Managed branches",
                "the volume has another default branch",
            ));
        }
        self.registered_branch(registry, &default_name, "initialize Managed branches")
            .await
    }

    pub async fn list(&self) -> Result<Vec<BranchInfo>, ManagedError> {
        let (registry, _) = self.registry().await?;
        let default = registry.default_branch;
        let mut branches = Vec::with_capacity(registry.branches.len());
        for (name, id) in registry.branches {
            let (head, _) = self.read_head(id).await?.ok_or_else(|| {
                corrupt("list Managed branches", "registered branch HEAD is missing")
            })?;
            branches.push(info(name, id, &head, default));
        }
        Ok(branches)
    }

    pub async fn default_name(&self) -> Result<BranchName, ManagedError> {
        let (registry, _) = self.registry().await?;
        registry
            .default_binding()
            .map(|binding| binding.name)
            .ok_or_else(|| corrupt("read default Managed branch", "default branch is missing"))
    }

    pub async fn get(&self, name: &BranchName) -> Result<BranchInfo, ManagedError> {
        let (registry, _) = self.registry().await?;
        self.registered_branch(&registry, name, "show Managed branch")
            .await
    }

    async fn registered_branch(
        &self,
        registry: &StoredBranchRegistry,
        name: &BranchName,
        action: &'static str,
    ) -> Result<BranchInfo, ManagedError> {
        let id = registry.branch_id(name).ok_or_else(|| not_found(action))?;
        let (head, _) = self
            .read_head(id)
            .await?
            .ok_or_else(|| corrupt(action, "registered branch HEAD is missing"))?;
        Ok(info(name.clone(), id, &head, registry.default_branch))
    }

    pub async fn bind(&self, name: &BranchName) -> Result<BoundNamespace, ManagedError> {
        let (registry, _) = self.registry().await?;
        let id = registry
            .branch_id(name)
            .ok_or_else(|| not_found("bind Managed branch"))?;
        Ok(BoundNamespace(self.namespace(BranchBinding {
            name: name.clone(),
            id,
        })))
    }

    pub async fn bind_default(&self) -> Result<BoundNamespace, ManagedError> {
        let (registry, _) = self.registry().await?;
        let binding = registry
            .default_binding()
            .ok_or_else(|| corrupt("bind default Managed branch", "default branch is missing"))?;
        Ok(BoundNamespace(self.namespace(binding)))
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
        if registry.default_branch == branch_id {
            return Err(invalid(
                "delete Managed branch",
                "default branch cannot be deleted",
            ));
        }

        let sealed = self
            .mutate_head(branch_id, "delete Managed branch", |head| {
                if head.sealed {
                    return Ok(CasMutation::Return(()));
                }
                if head.maintenance_active {
                    return Err(conflict(
                        "delete Managed branch",
                        "branch maintenance is active",
                    ));
                }
                head.sealed = true;
                Ok(CasMutation::Replace(()))
            })
            .await;
        match sealed {
            Ok(Some(())) => {}
            Ok(None) => {
                return Err(corrupt(
                    "delete Managed branch",
                    "registered branch HEAD is missing",
                ));
            }
            Err(CasFailure::Replace(error)) => {
                if !self
                    .read_head(branch_id)
                    .await?
                    .is_some_and(|(head, _)| head.sealed)
                {
                    return Err(error);
                }
            }
            Err(error) => return Err(error.into()),
        }

        let removed = self
            .mutate_registry("delete Managed branch", |registry| {
                if registry.branch_id(name) != Some(branch_id) {
                    return Ok(CasMutation::Return(()));
                }
                if registry.maintenance_active {
                    return Err(conflict(
                        "delete Managed branch",
                        "branch maintenance is active",
                    ));
                }
                registry.branches.remove(name);
                Ok(CasMutation::Replace(()))
            })
            .await;
        match removed {
            Ok(()) => Ok(()),
            Err(CasFailure::Replace(error)) => {
                let (current, _) = self.registry().await?;
                if current.branch_id(name) == Some(branch_id) {
                    Err(error)
                } else {
                    Ok(())
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn begin_gc(&self) -> Result<BranchGcFence, ManagedError> {
        let (registry, fence) = self
            .mutate_registry("begin Managed branch GC", |registry| {
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
                let fence = BranchGcFence {
                    epoch: registry.maintenance_epoch,
                    owner: *OperationId::generate().as_bytes(),
                };
                registry.maintenance_owner = Some(fence.owner);
                Ok(CasMutation::Replace((registry.clone(), fence)))
            })
            .await
            .map_err(ManagedError::from)?;

        self.fix_gc_heads(&registry, fence, "begin Managed branch GC")
            .await?;
        Ok(fence)
    }

    async fn fix_gc_heads(
        &self,
        registry: &StoredBranchRegistry,
        fence: BranchGcFence,
        action: &'static str,
    ) -> Result<(), ManagedError> {
        for (name, branch_id) in &registry.branches {
            let sealed = self
                .mutate_head(*branch_id, action, |head| {
                    if head.sealed {
                        return Ok(CasMutation::Return(true));
                    }
                    if head.maintenance_active && fence.owns_head(head) {
                        return Ok(CasMutation::Return(false));
                    }
                    head.maintenance_epoch = fence.epoch;
                    head.maintenance_active = true;
                    head.maintenance_owner = Some(fence.owner);
                    head.maintenance_fixed_cursor = Some(head.cursor());
                    Ok(CasMutation::Replace(false))
                })
                .await
                .map_err(ManagedError::from)?
                .ok_or_else(|| corrupt(action, "registered branch HEAD is missing"))?;
            if !sealed {
                continue;
            }
            self.mutate_registry(action, |registry| {
                if !registry.maintenance_active || !fence.owns_registry(registry) {
                    return Err(conflict(action, "GC fence changed while fixing roots"));
                }
                if registry.branch_id(name) != Some(*branch_id) {
                    return Ok(CasMutation::Return(()));
                }
                if registry.default_branch == *branch_id {
                    return Err(corrupt(action, "default branch HEAD is sealed"));
                }
                registry.branches.remove(name);
                Ok(CasMutation::Replace(()))
            })
            .await
            .map_err(ManagedError::from)?;
        }
        self.ensure_gc_fence(fence, action).await
    }

    async fn gc_roots(&self, fence: BranchGcFence) -> Result<GcRoots, ManagedError> {
        let (registry, _) = self.registry().await?;
        if !registry.maintenance_active || !fence.owns_registry(&registry) {
            return Err(conflict(
                "mark Managed branch GC roots",
                "GC fence does not match the registry",
            ));
        }
        let mut roots = GcRoots {
            data: RetainedDataRoots::default(),
            heads: BTreeSet::new(),
            checkpoints: BTreeSet::new(),
            histories: BTreeSet::new(),
        };
        for (name, branch) in &registry.branches {
            let branch_id = *branch;
            let namespace = self.namespace(BranchBinding {
                name: name.clone(),
                id: branch_id,
            });
            roots.heads.insert(head_key(branch_id));
            let (head, _) = self.read_head(branch_id).await?.ok_or_else(|| {
                corrupt(
                    "mark Managed branch GC roots",
                    "registered branch HEAD is missing",
                )
            })?;
            if head.sealed || !head.maintenance_active || !fence.owns_head(&head) {
                return Err(conflict(
                    "mark Managed branch GC roots",
                    "branch HEAD is not fixed by this GC fence",
                ));
            }
            let Some(state) = head.state else {
                continue;
            };
            self.retain_state_roots(&namespace, &mut roots, &state)
                .await?;
            let mut history_id = state.previous_history;
            let mut chain = BTreeSet::new();
            while let Some(id) = history_id {
                if !chain.insert(id) {
                    return Err(corrupt(
                        "mark Managed branch GC roots",
                        "branch history contains a cycle",
                    ));
                }
                if !roots.histories.insert(history_key(id)) {
                    break;
                }
                let history = namespace.read_history(id).await?;
                self.retain_state_roots(&namespace, &mut roots, &history.state)
                    .await?;
                history_id = history.state.previous_history;
            }
        }
        self.ensure_gc_fence(fence, "mark Managed branch GC roots")
            .await?;
        Ok(roots)
    }

    async fn finish_gc(&self, fence: BranchGcFence) -> Result<(), ManagedError> {
        let branches = self
            .mutate_registry("finish Managed branch GC", |registry| {
                if !fence.owns_registry(registry) {
                    return Err(conflict(
                        "finish Managed branch GC",
                        "GC fence does not match the registry",
                    ));
                }
                let branches = registry.branches.clone();
                if !registry.maintenance_active {
                    return Ok(CasMutation::Return(branches));
                }
                registry.maintenance_active = false;
                Ok(CasMutation::Replace(branches))
            })
            .await
            .map_err(ManagedError::from)?;

        for branch in branches.values() {
            let branch_id = *branch;
            self.mutate_head(branch_id, "finish Managed branch GC", |head| {
                if !head.maintenance_active || !fence.owns_head(head) {
                    return Ok(CasMutation::Return(()));
                }
                head.maintenance_active = false;
                head.maintenance_fixed_cursor = None;
                Ok(CasMutation::Replace(()))
            })
            .await
            .map_err(ManagedError::from)?;
        }
        Ok(())
    }

    /// Mark every retained branch position, sweep the shared data plane once,
    /// and release the volume fence only after deletion succeeds.
    pub async fn garbage_collect(&self) -> Result<SegmentGcMaintenance, ManagedError> {
        let fence = self.begin_gc().await?;
        self.collect_with_fence(fence).await
    }

    /// Resume an interrupted collection after the caller has established that
    /// the collector which owns the active fence is no longer running.
    pub async fn resume_garbage_collect(&self) -> Result<SegmentGcMaintenance, ManagedError> {
        let (registry, revision) = self.registry().await?;
        if !registry.maintenance_active {
            return Err(conflict(
                "resume Managed branch GC",
                "branch maintenance is not active",
            ));
        }
        let fence = BranchGcFence {
            epoch: registry.maintenance_epoch,
            owner: *OperationId::generate().as_bytes(),
        };
        let mut resumed = registry.clone();
        resumed.maintenance_owner = Some(fence.owner);
        let bytes = encode(REGISTRY_MAGIC, &resumed, "resume Managed branch GC")?;
        if !self
            .backend
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
        self.collect_with_fence(fence).await
    }

    async fn collect_with_fence(
        &self,
        fence: BranchGcFence,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let roots = self.gc_roots(fence).await?;
        let data = ManagedData::new(self.data.clone())?;
        let maintenance = data.collect_unreachable_segments_from(&roots.data).await?;
        self.sweep_metadata(fence, &roots).await?;
        self.finish_gc(fence).await?;
        Ok(maintenance)
    }

    async fn sweep_metadata(
        &self,
        fence: BranchGcFence,
        roots: &GcRoots,
    ) -> Result<(), ManagedError> {
        self.ensure_gc_fence(fence, "sweep Managed branch GC metadata")
            .await?;
        let head_prefix = format!("{ROOT}/heads/");
        let checkpoint_prefix = ".ofs/managed/metadata/v1/checkpoints/sha256/";
        let history_prefix = format!("{ROOT}/history/sha256/");
        let unreachable_heads = self
            .backend
            .list(&format!("{ROOT}/"), "scan Managed branch GC metadata")
            .await?
            .into_iter()
            .filter(|key| {
                canonical_metadata_key(key, &head_prefix, 16) && !roots.heads.contains(key)
            })
            .collect::<Vec<_>>();
        let mut unreachable_objects = Vec::new();
        for (prefix, retained) in [
            (checkpoint_prefix, &roots.checkpoints),
            (history_prefix.as_str(), &roots.histories),
        ] {
            unreachable_objects.extend(
                self.data
                    .list_with(prefix)
                    .recursive(true)
                    .await
                    .map_err(|_| unavailable("scan Managed branch GC metadata"))?
                    .into_iter()
                    .filter(|entry| {
                        entry.metadata().is_file()
                            && canonical_metadata_key(entry.path(), prefix, 32)
                            && !retained.contains(entry.path())
                    })
                    .map(|entry| entry.path().to_owned()),
            );
        }
        self.ensure_gc_fence(fence, "sweep Managed branch GC metadata")
            .await?;
        self.backend
            .delete(unreachable_heads, "sweep Managed branch GC metadata")
            .await?;
        self.data
            .delete_iter(unreachable_objects.iter().map(String::as_str))
            .await
            .map_err(|_| unavailable("sweep Managed branch GC metadata"))?;
        self.ensure_gc_fence(fence, "sweep Managed branch GC metadata")
            .await
    }

    async fn ensure_gc_fence(
        &self,
        fence: BranchGcFence,
        action: &'static str,
    ) -> Result<(), ManagedError> {
        let (registry, _) = self.registry().await?;
        if registry.maintenance_active && fence.owns_registry(&registry) {
            Ok(())
        } else {
            Err(conflict(action, "GC fence changed during metadata sweep"))
        }
    }

    async fn retain_state_roots(
        &self,
        namespace: &NamespaceStore,
        roots: &mut GcRoots,
        state: &StoredNamespaceState,
    ) -> Result<(), ManagedError> {
        roots.checkpoints.insert(checkpoint_key(state.checkpoint));
        namespace
            .visit_retained(state, |snapshot| roots.data.retain(snapshot))
            .await
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
        if source_head.sealed || source_head.maintenance_active {
            return Err(conflict(
                "fork Managed branch",
                "source branch is sealed for deletion",
            ));
        }
        let state = match (point, source_head.state) {
            (ForkPoint::Head, state) => state,
            (ForkPoint::Sequence(0), _) => None,
            (ForkPoint::Sequence(sequence), Some(current)) => {
                if let Some(state) = current.at_sequence(sequence) {
                    Some(state)
                } else {
                    self.namespace(BranchBinding {
                        name: source.clone(),
                        id: source_id,
                    })
                    .find_history_state(current.previous_history, sequence)
                    .await?
                    .ok_or_else(|| position_not_retained("fork Managed branch"))?
                    .into()
                }
            }
            (ForkPoint::Sequence(_), None) => {
                return Err(position_not_retained("fork Managed branch"));
            }
        };
        let target_id = BranchId::generate();
        let target_head = StoredHead {
            branch_id: Some(target_id),
            state,
            ..StoredHead::unborn(self.volume_id, Some(target_id))
        };
        let bytes = encode_head(&target_head)?;
        if !self
            .backend
            .create(&head_key(target_id), bytes, "fork Managed branch")
            .await?
        {
            return Err(conflict(
                "fork Managed branch",
                "target branch identity already exists",
            ));
        }
        registry.branches.insert(target.clone(), target_id);
        let bytes = encode(REGISTRY_MAGIC, &registry, "fork Managed branch")?;
        match self
            .backend
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
        Ok(info(
            target,
            target_id,
            &target_head,
            registry.default_branch,
        ))
    }

    async fn registry(&self) -> Result<(StoredBranchRegistry, Revision), ManagedError> {
        self.read_registry()
            .await?
            .ok_or_else(|| corrupt("read Managed branches", "branch registry is missing"))
    }

    async fn read_registry(
        &self,
    ) -> Result<Option<(StoredBranchRegistry, Revision)>, ManagedError> {
        let Some((bytes, revision)) = self
            .backend
            .read(REGISTRY_KEY, "read Managed branches")
            .await?
        else {
            return Ok(None);
        };
        let registry: StoredBranchRegistry =
            decode(REGISTRY_MAGIC, &bytes, "read Managed branches")?;
        registry.validate(self.volume_id)?;
        Ok(Some((registry, revision)))
    }

    async fn read_head(
        &self,
        branch_id: BranchId,
    ) -> Result<Option<(StoredHead, Revision)>, ManagedError> {
        let Some((bytes, revision)) = self
            .backend
            .read(&head_key(branch_id), "read Managed branch")
            .await?
        else {
            return Ok(None);
        };
        let head = decode_head(&bytes)?;
        head.validate(self.volume_id, Some(branch_id))?;
        Ok(Some((head, revision)))
    }
}

fn head_key(branch: BranchId) -> String {
    format!("{ROOT}/heads/{branch}.ofs")
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

fn encode<T: Serialize>(
    magic: &[u8; 8],
    value: &T,
    action: &'static str,
) -> Result<Vec<u8>, ManagedError> {
    let mut body = Vec::new();
    ciborium::ser::into_writer(value, &mut body)
        .map_err(|_| invalid(action, "branch record cannot be encoded"))?;
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
    action: &'static str,
) -> Result<T, ManagedError> {
    let body = bytes
        .strip_prefix(magic)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| corrupt(action, "branch record format is invalid"))?;
    if Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != &bytes[bytes.len() - 32..] {
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
