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

//! Streaming branch registry and namespace authority extension.

use std::num::NonZeroUsize;

use futures::{StreamExt as _, TryStreamExt as _};
use ofs_core::authority::{
    AuthorityHead, AuthorityId, AuthorityObservation, AuthorityRoot, AuthorityRoots,
    AuthoritySelector, AuthorityStore, CollectionFence,
};
use ofs_core::format::{ExtensionDescriptor, ExtensionId, RecordCodec, StreamKind, StreamRef};
use ofs_core::format::{GcEpoch, ObjectClass};
use ofs_core::storage::{ControlRecord, RecordStreamReader, RecordStreamWriter};
use ofs_core::{Error, ErrorKind};
use opendal::Operator;
use serde::{Deserialize, Serialize};

/// Stable wire identity of the Branch authority extension.
pub const BRANCH_EXTENSION_ID: ExtensionId = ExtensionId::from_bytes(*b"ofs.branch.v1!!!");

const HEAD_KEY: &str = "managed/0/ext/branch/head";
const HEAD_RECORD: ControlRecord<RegistryHead> =
    ControlRecord::new(HEAD_KEY, RecordCodec::new(*b"OFSBRH01", 64 * 1024));
const REVISION_RECORD: RecordCodec = RecordCodec::new(*b"OFSBRV01", 64 * 1024);
const REGISTRY_KIND: StreamKind = match StreamKind::extension(1025) {
    Some(kind) => kind,
    None => panic!("Branch stream kind must be in the extension range"),
};
const DEFAULT_AUTHORITY: &str = "main";

impl BranchAuthorityStore {
    /// Reconstruct the Branch authority described by a volume.
    pub fn from_format(format: &ExtensionDescriptor) -> Result<Self, Error> {
        if !format.require(BRANCH_EXTENSION_ID)?.is_empty() {
            return Err(Error::new(
                ErrorKind::Corrupt,
                "open Managed volume",
                "Branch configuration is not empty",
            ));
        }
        Ok(Self)
    }
}

/// Branch registry authority backend.
#[derive(Clone, Copy, Debug)]
pub struct BranchAuthorityStore;

impl AuthorityStore for BranchAuthorityStore {
    fn info(&self) -> Option<ExtensionDescriptor> {
        Some(ExtensionDescriptor::empty(BRANCH_EXTENSION_ID))
    }

    async fn initialize(
        &self,
        operator: &Operator,
        multipart_part_bytes: NonZeroUsize,
        initial: AuthorityHead,
    ) -> Result<(), Error> {
        if read_head(operator).await?.is_some() {
            self.observe(operator, DEFAULT_AUTHORITY).await?;
            return Ok(());
        }
        let root = AuthorityRoot {
            id: AuthorityId::generate(),
            name: DEFAULT_AUTHORITY.to_owned(),
            head: initial,
        };
        let registry = write_registry(
            operator,
            initial.gc_epoch,
            multipart_part_bytes,
            futures::stream::iter([Ok(root)]),
        )
        .await?;
        let desired = RegistryHead {
            registry,
            gc_epoch: initial.gc_epoch,
        };
        let _ = HEAD_RECORD.write(operator, &desired, None).await?;
        self.observe(operator, DEFAULT_AUTHORITY).await.map(drop)
    }

    async fn observe(
        &self,
        operator: &Operator,
        name: &str,
    ) -> Result<AuthorityObservation, Error> {
        validate_name(name)?;
        let observed = require_head(operator).await?;
        let root = find_root(operator, observed.head.registry, name)
            .await?
            .ok_or_else(|| not_found(name))?;
        Ok(AuthorityObservation {
            id: root.id,
            head: root.head,
            revision: REVISION_RECORD.encode(&RegistryRevision {
                etag: observed.etag,
                expected: root,
            })?,
        })
    }

    async fn compare_exchange(
        &self,
        operator: &Operator,
        multipart_part_bytes: NonZeroUsize,
        name: &str,
        observed: &AuthorityObservation,
        next: AuthorityHead,
    ) -> Result<bool, Error> {
        validate_name(name)?;
        let revision: RegistryRevision = REVISION_RECORD.decode(&observed.revision)?;
        if revision.expected.name != name
            || revision.expected.id != observed.id
            || revision.expected.head != observed.head
        {
            return Err(Error::new(
                ErrorKind::Invalid,
                "publish Managed branch",
                "branch observation does not match the selected authority",
            ));
        }
        let current = require_head(operator).await?;
        if current.etag != revision.etag {
            return Ok(false);
        }
        let registry = rewrite_registry(
            operator,
            current.head.registry,
            current.head.gc_epoch,
            multipart_part_bytes,
            name,
            |root| {
                root.ok_or_else(|| not_found(name))?;
                Ok(Some(AuthorityRoot {
                    id: observed.id,
                    name: name.to_owned(),
                    head: next,
                }))
            },
        )
        .await?;
        HEAD_RECORD
            .write(
                operator,
                &RegistryHead {
                    registry,
                    gc_epoch: current.head.gc_epoch,
                },
                Some(&current.etag),
            )
            .await
            .map_err(branch_context)
    }

    async fn begin_collection(
        &self,
        operator: &Operator,
        _multipart_part_bytes: NonZeroUsize,
    ) -> Result<(CollectionFence, AuthorityRoots), Error> {
        let observed = require_head(operator).await?;
        let epoch = observed
            .head
            .gc_epoch
            .next()
            .map_err(|error| error.with_context("extension", "branch"))?;
        let rotated = RegistryHead {
            registry: observed.head.registry,
            gc_epoch: epoch,
        };
        if !HEAD_RECORD
            .write(operator, &rotated, Some(&observed.etag))
            .await
            .map_err(branch_context)?
        {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed branches",
                "branch registry changed while rotating the collection epoch",
            ));
        }
        let current = require_head(operator).await?;
        let roots = registry_roots(operator.clone(), current.head.registry).await?;
        Ok((
            CollectionFence {
                epoch,
                revision: current.etag.into_bytes(),
            },
            roots,
        ))
    }

    async fn finish_collection(
        &self,
        operator: &Operator,
        multipart_part_bytes: NonZeroUsize,
        fence: CollectionFence,
        roots: &mut AuthorityRoots,
    ) -> Result<bool, Error> {
        let registry = write_registry(operator, fence.epoch, multipart_part_bytes, roots).await?;
        let etag = std::str::from_utf8(&fence.revision).map_err(|_| {
            Error::new(
                ErrorKind::Corrupt,
                "collect Managed branches",
                "branch collection fence is invalid",
            )
        })?;
        HEAD_RECORD
            .write(
                operator,
                &RegistryHead {
                    registry,
                    gc_epoch: fence.epoch,
                },
                Some(etag),
            )
            .await
            .map_err(branch_context)
    }
}

/// Management operations that use the same streamed registry pipeline.
#[derive(Clone, Debug)]
pub struct BranchManager {
    operator: Operator,
    multipart_part_bytes: NonZeroUsize,
}

impl BranchManager {
    /// Bind branch management to an already-configured OpenDAL operator.
    pub fn new(operator: Operator, multipart_part_bytes: NonZeroUsize) -> Self {
        Self {
            operator,
            multipart_part_bytes,
        }
    }

    /// Create a branch from an existing authority position.
    pub async fn create(&self, name: &str, source: &str) -> Result<(), Error> {
        validate_name(name)?;
        validate_name(source)?;
        let current = require_head(&self.operator).await?;
        let source = find_root(&self.operator, current.head.registry, source)
            .await?
            .ok_or_else(|| not_found(source))?;
        if self
            .rewrite(current, name, |existing| {
                if existing.is_some() {
                    return Err(Error::new(
                        ErrorKind::Conflict,
                        "create Managed branch",
                        "branch already exists",
                    ));
                }
                Ok(Some(AuthorityRoot {
                    id: AuthorityId::generate(),
                    name: name.to_owned(),
                    head: source.head,
                }))
            })
            .await?
        {
            Ok(())
        } else {
            Err(registry_conflict("create Managed branch"))
        }
    }

    /// Delete one non-default branch.
    pub async fn delete(&self, name: &str) -> Result<(), Error> {
        validate_name(name)?;
        if name == DEFAULT_AUTHORITY {
            return Err(Error::new(
                ErrorKind::Invalid,
                "delete Managed branch",
                "the default branch cannot be deleted",
            ));
        }
        let current = require_head(&self.operator).await?;
        if self
            .rewrite(current, name, |existing| {
                existing.ok_or_else(|| not_found(name))?;
                Ok(None)
            })
            .await?
        {
            Ok(())
        } else {
            Err(registry_conflict("delete Managed branch"))
        }
    }

    /// Open a forward-only stream of live branches.
    pub async fn list(&self) -> Result<AuthorityRoots, Error> {
        let head = require_head(&self.operator).await?;
        registry_roots(self.operator.clone(), head.head.registry).await
    }

    async fn rewrite(
        &self,
        current: ObservedHead,
        name: &str,
        replace: impl FnOnce(Option<AuthorityRoot>) -> Result<Option<AuthorityRoot>, Error>,
    ) -> Result<bool, Error> {
        let registry = rewrite_registry(
            &self.operator,
            current.head.registry,
            current.head.gc_epoch,
            self.multipart_part_bytes,
            name,
            replace,
        )
        .await?;
        HEAD_RECORD
            .write(
                &self.operator,
                &RegistryHead {
                    registry,
                    gc_epoch: current.head.gc_epoch,
                },
                Some(&current.etag),
            )
            .await
            .map_err(branch_context)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RegistryHead {
    registry: StreamRef,
    gc_epoch: GcEpoch,
}

struct ObservedHead {
    head: RegistryHead,
    etag: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RegistryRevision {
    etag: String,
    expected: AuthorityRoot,
}

async fn registry_roots(operator: Operator, reference: StreamRef) -> Result<AuthorityRoots, Error> {
    require_registry(reference)?;
    let reader = RecordStreamReader::<AuthorityRoot>::open(&operator, reference).await?;
    Ok(
        futures::stream::try_unfold((reader, None), |(mut reader, mut previous)| async move {
            let Some(root) = reader.next().await? else {
                return Ok(None);
            };
            validate_order(&mut previous, &root.name)?;
            Ok(Some((root, (reader, previous))))
        })
        .boxed(),
    )
}

async fn rewrite_registry(
    operator: &Operator,
    source: StreamRef,
    epoch: GcEpoch,
    multipart_part_bytes: NonZeroUsize,
    target: &str,
    replace: impl FnOnce(Option<AuthorityRoot>) -> Result<Option<AuthorityRoot>, Error>,
) -> Result<StreamRef, Error> {
    let mut reader = registry_roots(operator.clone(), source).await?;
    let mut writer = RecordStreamWriter::open(
        operator,
        epoch,
        ObjectClass::Extension,
        REGISTRY_KIND,
        multipart_part_bytes,
    )
    .await?;
    let mut replace = Some(replace);
    let mut applied = false;
    while let Some(root) = reader.try_next().await? {
        if !applied && target < root.name.as_str() {
            if let Some(replacement) = replace.take().expect("replacement is pending")(None)? {
                writer.write(&replacement).await?;
            }
            applied = true;
        }
        if root.name == target {
            if let Some(replacement) = replace.take().expect("replacement is pending")(Some(root))?
            {
                writer.write(&replacement).await?;
            }
            applied = true;
        } else {
            writer.write(&root).await?;
        }
    }
    if !applied && let Some(replacement) = replace.expect("replacement is pending")(None)? {
        writer.write(&replacement).await?;
    }
    writer.close().await
}

async fn find_root(
    operator: &Operator,
    reference: StreamRef,
    name: &str,
) -> Result<Option<AuthorityRoot>, Error> {
    let mut roots = registry_roots(operator.clone(), reference).await?;
    while let Some(root) = roots.try_next().await? {
        if root.name == name {
            return Ok(Some(root));
        }
        if root.name.as_str() > name {
            break;
        }
    }
    Ok(None)
}

async fn write_registry(
    operator: &Operator,
    epoch: GcEpoch,
    multipart_part_bytes: NonZeroUsize,
    mut roots: impl futures::TryStream<Ok = AuthorityRoot, Error = Error> + Unpin,
) -> Result<StreamRef, Error> {
    let mut writer = RecordStreamWriter::open(
        operator,
        epoch,
        ObjectClass::Extension,
        REGISTRY_KIND,
        multipart_part_bytes,
    )
    .await?;
    let mut previous = None;
    while let Some(root) = roots.try_next().await? {
        validate_order(&mut previous, &root.name)?;
        writer.write(&root).await?;
    }
    writer.close().await
}

fn require_registry(reference: StreamRef) -> Result<(), Error> {
    reference.require(REGISTRY_KIND, ObjectClass::Extension)
}

fn validate_order(previous: &mut Option<String>, current: &str) -> Result<(), Error> {
    if previous
        .as_deref()
        .is_some_and(|previous| previous >= current)
    {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "read Managed branches",
            "branch registry is not ordered by name",
        ));
    }
    *previous = Some(current.to_owned());
    Ok(())
}

fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 255
        || name.contains('/')
        || name == "."
        || name == ".."
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::new(
            ErrorKind::Invalid,
            "select Managed branch",
            "branch name must be one portable ASCII component",
        ));
    }
    Ok(())
}

fn not_found(name: &str) -> Error {
    Error::new(
        ErrorKind::NotFound,
        "open Managed branch",
        "branch does not exist",
    )
    .with_context("branch", name)
}

fn registry_conflict(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Conflict,
        operation,
        "branch registry changed concurrently",
    )
}

async fn require_head(operator: &Operator) -> Result<ObservedHead, Error> {
    read_head(operator).await?.ok_or_else(|| {
        Error::new(
            ErrorKind::NotFound,
            "open Managed branches",
            "branch registry does not exist",
        )
    })
}

async fn read_head(operator: &Operator) -> Result<Option<ObservedHead>, Error> {
    let Some(observed) = HEAD_RECORD.read(operator).await.map_err(branch_context)? else {
        return Ok(None);
    };
    let etag = observed.revision.ok_or_else(|| {
        Error::new(
            ErrorKind::Unsupported,
            "open Managed branches",
            "branch registry revision is missing",
        )
    })?;
    Ok(Some(ObservedHead {
        head: observed.value,
        etag,
    }))
}

/// Authority selector that names roots through the Branch registry.
#[derive(Clone, Debug)]
pub struct BranchSelector {
    descriptor: ExtensionDescriptor,
}

impl Default for BranchSelector {
    fn default() -> Self {
        Self {
            descriptor: ExtensionDescriptor::empty(BRANCH_EXTENSION_ID),
        }
    }
}

impl AuthoritySelector for BranchSelector {
    type Store = BranchAuthorityStore;

    fn descriptor(&self) -> Option<&ExtensionDescriptor> {
        Some(&self.descriptor)
    }

    fn validate_name(&self, name: &str) -> Result<(), Error> {
        validate_name(name)
    }

    fn store(&self) -> Self::Store {
        BranchAuthorityStore
    }
}

fn branch_context(error: Error) -> Error {
    error.with_context("extension", "branch")
}
