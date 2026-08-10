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

use super::records::{BranchInfo, ForkPoint, StoredBranchRegistry, info};
use crate::filesystem::{
    BranchBinding, BranchId, BranchName, VolumeError, VolumeErrorKind, VolumeId,
};
use crate::managed::ManagedVolume;
use crate::managed::error::{conflict, corrupt, invalid, unavailable};
use crate::managed::format::{RecordDecodeError, RecordEncodeError, V1Record};
use crate::managed::metadata::namespace::{
    MAX_HEAD_ENCODED_BYTES, NamespaceStore, StoredHead, decode_head, encode_head,
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
    pub(crate) volume_id: VolumeId,
    pub(crate) backend: RecordBackend,
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
        let _ = self
            .backend
            .create(&head_key(branch_id), encoded_head, "create Managed branch")
            .await?;
        let registry =
            StoredBranchRegistry::initial(self.volume_id, default_name.clone(), branch_id);
        let encoded_registry = REGISTRY_RECORD
            .encode(&registry)
            .map_err(|error| encode_error("initialize Managed branches", error))?;
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
        if registry.default_binding().map(|binding| binding.name) != Some(default_name.clone()) {
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
        let mut branches = stream::iter(registry.branches)
            .map(|(name, id)| async move {
                let (head, _) = self.read_head(id).await?.ok_or_else(|| {
                    corrupt("list Managed branches", "registered branch HEAD is missing")
                })?;
                Ok(info(name, id, &head, default))
            })
            .buffer_unordered(concurrency.get())
            .try_collect::<Vec<_>>()
            .await?;
        branches.sort_by(|left, right| left.binding.name.cmp(&right.binding.name));
        Ok(branches)
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
            .branch_id(name)
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
            .branch_id(name)
            .ok_or_else(|| invalid("open Managed branch", "branch does not exist"))?;
        let namespace = self.namespace(BranchBinding {
            name: name.clone(),
            id,
        });
        ManagedVolume::new(namespace, self.data.clone())
    }

    pub async fn open_default(&self) -> Result<ManagedVolume, VolumeError> {
        let (registry, _) = self.registry().await?;
        let binding = registry
            .default_binding()
            .ok_or_else(|| corrupt("open default Managed branch", "default branch is missing"))?;
        let namespace = self.namespace(binding);
        ManagedVolume::new(namespace, self.data.clone())
    }

    pub async fn delete(&self, name: &BranchName) -> Result<(), VolumeError> {
        let (registry, _) = self.registry().await?;
        let branch_id = registry
            .branch_id(name)
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
            head.sealed = true;
            let bytes = encode_head(&head)?;
            match self
                .backend
                .replace(
                    &head_key(branch_id),
                    &revision,
                    bytes,
                    "delete Managed branch",
                )
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
            let (mut registry, revision) = self.registry().await?;
            if registry.branch_id(name) != Some(branch_id) {
                return Ok(());
            }
            registry.branches.remove(name);
            let bytes = REGISTRY_RECORD
                .encode(&registry)
                .map_err(|error| encode_error("delete Managed branch", error))?;
            match self
                .backend
                .replace(REGISTRY_KEY, &revision, bytes, "delete Managed branch")
                .await
            {
                Ok(true) => return Ok(()),
                Ok(false) => tokio::task::yield_now().await,
                Err(error) => {
                    let (current, _) = self.registry().await?;
                    return if current.branch_id(name) == Some(branch_id) {
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
        if registry.branches.contains_key(&target) {
            return Err(conflict(
                "fork Managed branch",
                "target branch already exists",
            ));
        }
        let source = match source {
            Some(source) => source,
            None => registry
                .default_binding()
                .map(|binding| binding.name)
                .ok_or_else(|| corrupt("fork Managed branch", "default branch is missing"))?,
        };
        let source_id = registry
            .branch_id(&source)
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
        let bytes = REGISTRY_RECORD
            .encode(&registry)
            .map_err(|error| encode_error("fork Managed branch", error))?;
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
                    Ok(current) if current.binding.id == target_id => Ok((current, source)),
                    Ok(_) => Err(conflict(
                        "fork Managed branch",
                        "target branch was created concurrently",
                    )),
                    Err(observed) if observed.kind() == VolumeErrorKind::Invalid => Err(error),
                    Err(observed) => Err(observed),
                };
            }
        }
        Ok((
            info(target, target_id, &target_head, registry.default_branch),
            source,
        ))
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
            .map_err(|error| decode_error("read Managed branches", error))?;
        registry.validate(self.volume_id)?;
        Ok(Some((registry, revision)))
    }

    async fn read_head(
        &self,
        branch_id: BranchId,
    ) -> Result<Option<(StoredHead, Revision)>, VolumeError> {
        let Some((bytes, revision)) = self
            .backend
            .read(
                &head_key(branch_id),
                MAX_HEAD_ENCODED_BYTES,
                "read Managed branch",
            )
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

fn encode_error(action: &'static str, _: RecordEncodeError) -> VolumeError {
    invalid(action, "branch record cannot be encoded")
}

fn decode_error(action: &'static str, error: RecordDecodeError) -> VolumeError {
    let message = match error {
        RecordDecodeError::Envelope => "branch record format is invalid",
        RecordDecodeError::Checksum => "branch record checksum is invalid",
        RecordDecodeError::Decode => "branch record cannot be decoded",
        RecordDecodeError::TrailingBytes => "branch record has trailing bytes",
    };
    corrupt(action, message)
}
