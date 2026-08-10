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

use std::io::Cursor;

use super::records::{BranchInfo, ForkPoint, StoredBranchRegistry, info};
use crate::filesystem::{BranchBinding, BranchId, BranchName, VolumeId};
use crate::managed::metadata::namespace::{NamespaceStore, StoredHead, decode_head, encode_head};
use crate::managed::metadata::record::{RecordBackend, Revision};
use crate::managed::{ManagedError, ManagedErrorKind, ManagedVolume};
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

    pub async fn open(&self, name: &BranchName) -> Result<ManagedVolume, ManagedError> {
        let (registry, _) = self.registry().await?;
        let id = registry
            .branch_id(name)
            .ok_or_else(|| not_found("open Managed branch"))?;
        let namespace = self.namespace(BranchBinding {
            name: name.clone(),
            id,
        });
        ManagedVolume::new(namespace, self.data.clone())
    }

    pub async fn open_default(&self) -> Result<ManagedVolume, ManagedError> {
        let (registry, _) = self.registry().await?;
        let binding = registry
            .default_binding()
            .ok_or_else(|| corrupt("open default Managed branch", "default branch is missing"))?;
        let namespace = self.namespace(binding);
        ManagedVolume::new(namespace, self.data.clone())
    }

    pub async fn delete(&self, name: &BranchName) -> Result<(), ManagedError> {
        let (registry, _) = self.registry().await?;
        let branch_id = registry
            .branch_id(name)
            .ok_or_else(|| not_found("delete Managed branch"))?;
        if registry.default_branch == branch_id {
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
            if head.sealed {
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
                Ok(true) => break,
                Ok(false) => continue,
                Err(error) => {
                    if self
                        .read_head(branch_id)
                        .await?
                        .is_some_and(|(head, _)| head.sealed)
                    {
                        break;
                    }
                    return Err(error);
                }
            }
        }

        loop {
            let (mut registry, revision) = self.registry().await?;
            if registry.branch_id(name) != Some(branch_id) {
                return Ok(());
            }
            registry.branches.remove(name);
            let bytes = encode(REGISTRY_MAGIC, &registry, "delete Managed branch")?;
            match self
                .backend
                .replace(REGISTRY_KEY, &revision, bytes, "delete Managed branch")
                .await
            {
                Ok(true) => return Ok(()),
                Ok(false) => continue,
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
    }

    pub async fn fork(
        &self,
        source: Option<BranchName>,
        point: ForkPoint,
        target: BranchName,
    ) -> Result<(BranchInfo, BranchName), ManagedError> {
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
            .ok_or_else(|| not_found("fork Managed branch"))?;
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
                    Ok(current) if current.binding.id == target_id => Ok((current, source)),
                    Ok(_) => Err(conflict(
                        "fork Managed branch",
                        "target branch was created concurrently",
                    )),
                    Err(observed) if observed.kind() == ManagedErrorKind::Invalid => Err(error),
                    Err(observed) => Err(observed),
                };
            }
        }
        Ok((
            info(target, target_id, &target_head, registry.default_branch),
            source,
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
