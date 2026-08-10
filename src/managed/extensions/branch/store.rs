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

mod gc;

use super::records::{BranchInfo, ForkPoint, StoredBranchRegistry, info};
use crate::filesystem::{BranchBinding, BranchId, BranchName, VolumeError, VolumeId};
use crate::managed::ManagedVolume;
use crate::managed::error::{conflict, corrupt, invalid, unavailable};
use crate::managed::format::V1Record;
use crate::managed::metadata::namespace::{
    NamespaceStore, StoredHead, encode_head, read_head_record,
};
use crate::managed::metadata::record::{RecordBackend, Revision};
use futures::{StreamExt, TryStreamExt, stream};
use opendal::Operator;
use std::num::NonZeroUsize;

const ROOT: &str = ".ofs/managed/metadata/v1/extensions/branch/v1";
const REGISTRY_KEY: &str = ".ofs/managed/metadata/v1/extensions/branch/v1/registry.ofs";
const MAX_REGISTRY_BODY_BYTES: usize = 4 * 1024 * 1024;
const REGISTRY_RECORD: V1Record = V1Record::new(*b"OFS1BRG1", MAX_REGISTRY_BODY_BYTES);
const BRANCH_CAS_ATTEMPTS: usize = 8;

#[derive(Clone)]
pub struct BranchStore {
    volume_id: VolumeId,
    backend: RecordBackend,
    data: Operator,
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

    pub(crate) fn new(volume_id: VolumeId, data: Operator, backend: RecordBackend) -> Self {
        Self {
            volume_id,
            backend,
            data,
        }
    }

    /// Idempotently create the first unborn branch. The head is prepared
    /// before the registry, making the registry the branch-existence authority.
    pub async fn initialize(&self, default_name: BranchName) -> Result<BranchInfo, VolumeError> {
        if let Some((registry, _)) = self.read_registry().await? {
            return self.initialized(&registry, default_name).await;
        }

        let branch_id = BranchId::generate();
        let head = StoredHead::unborn(self.volume_id, Some(branch_id));
        let encoded_head = encode_head(&head)?;
        if !self
            .backend
            .create(&head_key(branch_id), encoded_head, "create Managed branch")
            .await?
        {
            return Err(conflict(
                "initialize Managed branches",
                "default branch identity already exists",
            ));
        }
        let registry =
            StoredBranchRegistry::initial(self.volume_id, default_name.clone(), branch_id);
        let encoded_registry = REGISTRY_RECORD
            .encode(&registry)
            .map_err(|error| invalid("initialize Managed branches", error.message()))?;
        if !self
            .backend
            .create(
                REGISTRY_KEY,
                encoded_registry,
                "initialize Managed branches",
            )
            .await?
        {
            let (registry, _) = self.read_registry().await?.ok_or_else(|| {
                unavailable(
                    "initialize Managed branches",
                    "object branch metadata is unavailable",
                )
            })?;
            return self.initialized(&registry, default_name).await;
        }
        Ok(info(default_name, branch_id, &head, branch_id))
    }

    async fn initialized(
        &self,
        registry: &StoredBranchRegistry,
        default_name: BranchName,
    ) -> Result<BranchInfo, VolumeError> {
        if registry.default_binding().name != default_name {
            return Err(conflict(
                "initialize Managed branches",
                "the volume has another default branch",
            ));
        }
        self.registered_branch(registry, &default_name, "initialize Managed branches")
            .await
    }

    pub async fn list(&self, concurrency: NonZeroUsize) -> Result<Vec<BranchInfo>, VolumeError> {
        let (registry, _) = self.registry().await?;
        let default = registry.default_branch;
        stream::iter(registry.branches)
            .map(|(name, id)| async move {
                let (head, _) = self.read_head(id).await?.ok_or_else(|| {
                    corrupt("list Managed branches", "registered branch HEAD is missing")
                })?;
                Ok(info(name, id, &head, default))
            })
            .buffered(concurrency.get())
            .try_collect::<Vec<_>>()
            .await
    }

    pub async fn get(&self, name: &BranchName) -> Result<BranchInfo, VolumeError> {
        let (registry, _) = self.registry().await?;
        self.registered_branch(&registry, name, "show Managed branch")
            .await
    }

    async fn registered_branch(
        &self,
        registry: &StoredBranchRegistry,
        name: &BranchName,
        action: &'static str,
    ) -> Result<BranchInfo, VolumeError> {
        let id = registry
            .branches
            .get(name)
            .copied()
            .ok_or_else(|| invalid(action, "branch does not exist"))?;
        let (head, _) = self
            .read_head(id)
            .await?
            .ok_or_else(|| corrupt(action, "registered branch HEAD is missing"))?;
        Ok(info(name.clone(), id, &head, registry.default_branch))
    }

    pub async fn open(&self, name: &BranchName) -> Result<ManagedVolume, VolumeError> {
        let (registry, _) = self.registry().await?;
        let id = registry
            .branches
            .get(name)
            .copied()
            .ok_or_else(|| invalid("open Managed branch", "branch does not exist"))?;
        let namespace = self.namespace(BranchBinding {
            name: name.clone(),
            id,
        });
        ManagedVolume::new(namespace, self.data.clone())
    }

    pub async fn open_default(&self) -> Result<ManagedVolume, VolumeError> {
        let (registry, _) = self.registry().await?;
        let binding = registry.default_binding();
        let namespace = self.namespace(binding);
        ManagedVolume::new(namespace, self.data.clone())
    }

    pub async fn delete(&self, name: &BranchName) -> Result<(), VolumeError> {
        let (mut registry, mut registry_revision) = self.registry().await?;
        if registry.maintenance_owner.is_some() {
            return Err(conflict(
                "delete Managed branch",
                "data collection is active",
            ));
        }
        let branch_id = registry
            .branches
            .get(name)
            .copied()
            .ok_or_else(|| invalid("delete Managed branch", "branch does not exist"))?;
        if registry.default_branch == branch_id {
            return Err(invalid(
                "delete Managed branch",
                "default branch cannot be deleted",
            ));
        }

        let mut sealed = false;
        for _ in 0..BRANCH_CAS_ATTEMPTS {
            let Some((mut head, revision)) = self.read_head(branch_id).await? else {
                return Err(corrupt(
                    "delete Managed branch",
                    "registered branch HEAD is missing",
                ));
            };
            if head.sealed {
                sealed = true;
                break;
            }
            if head.maintenance.is_some() {
                return Err(conflict(
                    "delete Managed branch",
                    "data collection is active",
                ));
            }
            head.sealed = true;
            match self
                .replace_head(branch_id, &revision, &head, "delete Managed branch")
                .await
            {
                Ok(true) => {
                    sealed = true;
                    break;
                }
                Ok(false) => tokio::task::yield_now().await,
                Err(error) => {
                    if self
                        .read_head(branch_id)
                        .await?
                        .is_some_and(|(head, _)| head.sealed)
                    {
                        sealed = true;
                        break;
                    }
                    return Err(error);
                }
            }
        }
        if !sealed {
            return Err(conflict(
                "delete Managed branch",
                "branch HEAD changed concurrently",
            ));
        }

        for _ in 0..BRANCH_CAS_ATTEMPTS {
            if registry.branches.get(name).copied() != Some(branch_id) {
                return Ok(());
            }
            if registry.maintenance_owner.is_some() {
                return Err(conflict(
                    "delete Managed branch",
                    "data collection is active",
                ));
            }
            registry.branches.remove(name);
            match self
                .replace_registry(&registry_revision, &registry, "delete Managed branch")
                .await
            {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    (registry, registry_revision) = self.registry().await?;
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    let (current, _) = self.registry().await?;
                    return if current.branches.get(name).copied() == Some(branch_id) {
                        Err(error)
                    } else {
                        Ok(())
                    };
                }
            }
        }
        Err(conflict(
            "delete Managed branch",
            "branch registry changed concurrently",
        ))
    }

    pub async fn fork(
        &self,
        source: Option<BranchName>,
        point: ForkPoint,
        target: BranchName,
    ) -> Result<(BranchInfo, BranchName), VolumeError> {
        let (mut registry, revision) = self.registry().await?;
        if registry.maintenance_owner.is_some() {
            return Err(conflict("fork Managed branch", "data collection is active"));
        }
        if registry.branches.contains_key(&target) {
            return Err(conflict(
                "fork Managed branch",
                "target branch already exists",
            ));
        }
        let source = match source {
            Some(source) => source,
            None => registry.default_binding().name,
        };
        let source_id = registry
            .branches
            .get(&source)
            .copied()
            .ok_or_else(|| invalid("fork Managed branch", "branch does not exist"))?;
        let (source_head, _) = self
            .read_head(source_id)
            .await?
            .ok_or_else(|| corrupt("fork Managed branch", "source branch HEAD is missing"))?;
        if source_head.sealed {
            return Err(conflict(
                "fork Managed branch",
                "source branch is sealed for deletion",
            ));
        }
        let state = match (point, source_head.state) {
            (ForkPoint::Head, state) => state,
            (ForkPoint::Sequence(0), _) => None,
            (ForkPoint::Sequence(sequence), Some(current)) => self
                .namespace(BranchBinding {
                    name: source.clone(),
                    id: source_id,
                })
                .state_at_sequence(&current, sequence)
                .await?
                .ok_or_else(|| invalid("fork Managed branch", "branch position is not retained"))?
                .into(),
            (ForkPoint::Sequence(_), None) => {
                return Err(invalid(
                    "fork Managed branch",
                    "branch position is not retained",
                ));
            }
        };
        let state = state.map(|mut state| {
            state.reset_outcomes();
            state
        });
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
        match self
            .replace_registry(&revision, &registry, "fork Managed branch")
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
                let (current, _) = self.registry().await?;
                return match current.branches.get(&target).copied() {
                    Some(id) if id == target_id => Ok((
                        info(target, target_id, &target_head, current.default_branch),
                        source,
                    )),
                    Some(_) => Err(conflict(
                        "fork Managed branch",
                        "target branch was created concurrently",
                    )),
                    None => Err(error),
                };
            }
        }
        Ok((
            info(target, target_id, &target_head, registry.default_branch),
            source,
        ))
    }
    async fn replace_registry(
        &self,
        revision: &Revision,
        registry: &StoredBranchRegistry,
        action: &'static str,
    ) -> Result<bool, VolumeError> {
        let bytes = REGISTRY_RECORD
            .encode(registry)
            .map_err(|error| invalid(action, error.message()))?;
        self.backend
            .replace(REGISTRY_KEY, revision, bytes, action)
            .await
    }

    async fn replace_head(
        &self,
        id: BranchId,
        revision: &Revision,
        head: &StoredHead,
        action: &'static str,
    ) -> Result<bool, VolumeError> {
        self.backend
            .replace(&head_key(id), revision, encode_head(head)?, action)
            .await
    }

    async fn registry(&self) -> Result<(StoredBranchRegistry, Revision), VolumeError> {
        self.read_registry()
            .await?
            .ok_or_else(|| corrupt("read Managed branches", "branch registry is missing"))
    }

    async fn read_registry(&self) -> Result<Option<(StoredBranchRegistry, Revision)>, VolumeError> {
        let Some((bytes, revision)) = self
            .backend
            .read(
                REGISTRY_KEY,
                REGISTRY_RECORD.maximum_encoded_bytes(),
                "read Managed branches",
            )
            .await?
        else {
            return Ok(None);
        };
        let registry: StoredBranchRegistry = REGISTRY_RECORD
            .decode(&bytes)
            .map_err(|error| corrupt("read Managed branches", error.message()))?;
        registry.validate(self.volume_id)?;
        Ok(Some((registry, revision)))
    }

    async fn read_head(
        &self,
        branch_id: BranchId,
    ) -> Result<Option<(StoredHead, Revision)>, VolumeError> {
        read_head_record(
            &self.backend,
            &head_key(branch_id),
            self.volume_id,
            Some(branch_id),
            "read Managed branch",
        )
        .await
    }
}

fn head_key(branch: BranchId) -> String {
    format!("{ROOT}/heads/{branch}.ofs")
}
