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

use std::fmt;

use ofs_core::managed::extension::{
    AccessContext, ExtensionFormat, ExtensionId, RecordStreamReader, RecordStreamWriter,
    StreamKind, StreamRef,
};
use ofs_core::managed::{
    AuthorityAccess, AuthorityExtension, AuthorityFuture, AuthorityHead, AuthorityId,
    AuthorityObservation, AuthorityRoot, AuthorityRoots, CollectionFence, GcEpoch, ObjectClass,
};
use ofs_core::{Error, ErrorKind};
use opendal::{ErrorKind as StorageErrorKind, Operator};
use serde::{Deserialize, Serialize};

/// Stable wire identity of the Branch authority extension.
pub const BRANCH_EXTENSION_ID: ExtensionId = ExtensionId::new(*b"ofs.branch.v1!!!");

const HEAD_KEY: &str = "managed/1/ext/branch/head";
const HEAD_MAGIC: &[u8; 8] = b"OFSBRH01";
const MAXIMUM_HEAD_BYTES: usize = 64 * 1024;
const REGISTRY_KIND: StreamKind = match StreamKind::extension(1025) {
    Some(kind) => kind,
    None => panic!("Branch stream kind must be in the extension range"),
};
const DEFAULT_AUTHORITY: &str = "main";

/// Branch namespace authority extension.
#[derive(Clone, Copy, Debug, Default)]
pub struct BranchExtension;

impl BranchExtension {
    /// Construct the Branch authority extension.
    pub const fn new() -> Self {
        Self
    }
}

impl<A: AuthorityAccess> AuthorityExtension<A> for BranchExtension {
    type ExtendedAccess = BranchAuthorityAccess<A>;

    fn extend(&self, inner: A) -> Self::ExtendedAccess {
        BranchAuthorityAccess { inner }
    }
}

/// Branch registry composed over the core authority access.
#[derive(Clone, Debug)]
pub struct BranchAuthorityAccess<A> {
    inner: A,
}

impl<A: AuthorityAccess> AuthorityAccess for BranchAuthorityAccess<A> {
    fn info(&self) -> Option<ExtensionFormat> {
        let _ = &self.inner;
        Some(format())
    }

    async fn initialize(
        &self,
        context: &AccessContext,
        initial: AuthorityHead,
    ) -> Result<(), Error> {
        if read_head(context.operator()).await?.is_some() {
            self.observe(context, DEFAULT_AUTHORITY).await?;
            return Ok(());
        }
        let root = AuthorityRoot {
            id: AuthorityId::generate(),
            name: DEFAULT_AUTHORITY.to_owned(),
            head: initial,
        };
        let registry = write_registry(context.operator(), initial.gc_epoch(), [root]).await?;
        let desired = RegistryHead {
            registry,
            gc_epoch: initial.gc_epoch(),
        };
        let _ = write_head(context.operator(), &desired, HeadCondition::Missing).await?;
        self.observe(context, DEFAULT_AUTHORITY).await.map(drop)
    }

    async fn observe(
        &self,
        context: &AccessContext,
        name: &str,
    ) -> Result<AuthorityObservation, Error> {
        validate_name(name)?;
        let observed = require_head(context.operator()).await?;
        let root = find_root(context.operator(), observed.head.registry, name)
            .await?
            .ok_or_else(|| not_found(name))?;
        Ok(AuthorityObservation::new(
            root.id,
            root.head,
            encode_revision(&RegistryRevision {
                etag: observed.etag,
                expected: root,
            })?,
        ))
    }

    async fn compare_exchange(
        &self,
        context: &AccessContext,
        name: &str,
        observed: &AuthorityObservation,
        next: AuthorityHead,
    ) -> Result<bool, Error> {
        validate_name(name)?;
        let revision = decode_revision(observed.revision())?;
        if revision.expected.name != name
            || revision.expected.id != observed.id()
            || revision.expected.head != observed.head()
        {
            return Err(Error::new(
                ErrorKind::Invalid,
                "publish Managed branch",
                "branch observation does not match the selected authority",
            ));
        }
        let current = require_head(context.operator()).await?;
        if current.etag != revision.etag {
            return Ok(false);
        }
        let registry = rewrite_registry(
            context.operator(),
            current.head.registry,
            current.head.gc_epoch,
            Rewrite::Replace {
                expected: &revision.expected,
                next: AuthorityRoot {
                    id: observed.id(),
                    name: name.to_owned(),
                    head: next,
                },
            },
        )
        .await?;
        write_head(
            context.operator(),
            &RegistryHead {
                registry,
                gc_epoch: current.head.gc_epoch,
            },
            HeadCondition::Revision(&current.etag),
        )
        .await
    }

    async fn begin_collection(
        &self,
        context: &AccessContext,
    ) -> Result<(CollectionFence, Box<dyn AuthorityRoots>), Error> {
        let observed = require_head(context.operator()).await?;
        let epoch = observed
            .head
            .gc_epoch
            .next()
            .map_err(|error| error.with_context("extension", "branch"))?;
        let rotated = RegistryHead {
            registry: observed.head.registry,
            gc_epoch: epoch,
        };
        if !write_head(
            context.operator(),
            &rotated,
            HeadCondition::Revision(&observed.etag),
        )
        .await?
        {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed branches",
                "branch registry changed while rotating the collection epoch",
            ));
        }
        let current = require_head(context.operator()).await?;
        let roots = RegistryRoots::open(context.operator().clone(), current.head.registry).await?;
        Ok((
            CollectionFence::new(epoch, current.etag.into_bytes()),
            Box::new(roots),
        ))
    }

    async fn finish_collection(
        &self,
        context: &AccessContext,
        fence: CollectionFence,
        roots: &mut dyn AuthorityRoots,
    ) -> Result<bool, Error> {
        let mut writer = RecordStreamWriter::open(
            context.operator(),
            fence.epoch(),
            ObjectClass::Extension,
            REGISTRY_KIND,
        )
        .await?;
        let mut previous = None;
        while let Some(root) = roots.next().await? {
            validate_order(&mut previous, &root.name)?;
            writer.write(&root).await?;
        }
        let registry = writer.close().await?;
        let etag = std::str::from_utf8(fence.revision()).map_err(|_| {
            Error::new(
                ErrorKind::Corrupt,
                "collect Managed branches",
                "branch collection fence is invalid",
            )
        })?;
        write_head(
            context.operator(),
            &RegistryHead {
                registry,
                gc_epoch: fence.epoch(),
            },
            HeadCondition::Revision(etag),
        )
        .await
    }
}

/// Management operations that use the same streamed registry pipeline.
#[derive(Clone, Debug)]
pub struct BranchManager {
    context: AccessContext,
}

impl BranchManager {
    /// Bind branch management to an already-configured OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self {
            context: AccessContext::new(operator),
        }
    }

    /// Create a branch from an existing authority position.
    pub async fn create(&self, name: &str, source: &str) -> Result<(), Error> {
        validate_name(name)?;
        validate_name(source)?;
        let current = require_head(self.context.operator()).await?;
        let source = find_root(self.context.operator(), current.head.registry, source)
            .await?
            .ok_or_else(|| not_found(source))?;
        if find_root(self.context.operator(), current.head.registry, name)
            .await?
            .is_some()
        {
            return Err(Error::new(
                ErrorKind::Conflict,
                "create Managed branch",
                "branch already exists",
            ));
        }
        let registry = rewrite_registry(
            self.context.operator(),
            current.head.registry,
            current.head.gc_epoch,
            Rewrite::Insert(AuthorityRoot {
                id: AuthorityId::generate(),
                name: name.to_owned(),
                head: source.head,
            }),
        )
        .await?;
        if write_head(
            self.context.operator(),
            &RegistryHead {
                registry,
                gc_epoch: current.head.gc_epoch,
            },
            HeadCondition::Revision(&current.etag),
        )
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
        let current = require_head(self.context.operator()).await?;
        let expected = find_root(self.context.operator(), current.head.registry, name)
            .await?
            .ok_or_else(|| not_found(name))?;
        let registry = rewrite_registry(
            self.context.operator(),
            current.head.registry,
            current.head.gc_epoch,
            Rewrite::Delete(&expected),
        )
        .await?;
        if write_head(
            self.context.operator(),
            &RegistryHead {
                registry,
                gc_epoch: current.head.gc_epoch,
            },
            HeadCondition::Revision(&current.etag),
        )
        .await?
        {
            Ok(())
        } else {
            Err(registry_conflict("delete Managed branch"))
        }
    }

    /// Open a forward-only stream of live branches.
    pub async fn list(&self) -> Result<Box<dyn AuthorityRoots>, Error> {
        let head = require_head(self.context.operator()).await?;
        Ok(Box::new(
            RegistryRoots::open(self.context.operator().clone(), head.head.registry).await?,
        ))
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

struct RegistryRoots {
    reader: RecordStreamReader<AuthorityRoot>,
}

impl RegistryRoots {
    async fn open(operator: Operator, reference: StreamRef) -> Result<Self, Error> {
        require_registry(reference)?;
        Ok(Self {
            reader: RecordStreamReader::open(&operator, reference).await?,
        })
    }
}

impl AuthorityRoots for RegistryRoots {
    fn next(&mut self) -> AuthorityFuture<'_, Result<Option<AuthorityRoot>, Error>> {
        Box::pin(self.reader.next())
    }
}

enum Rewrite<'a> {
    Insert(AuthorityRoot),
    Replace {
        expected: &'a AuthorityRoot,
        next: AuthorityRoot,
    },
    Delete(&'a AuthorityRoot),
}

async fn rewrite_registry(
    operator: &Operator,
    source: StreamRef,
    epoch: GcEpoch,
    mut rewrite: Rewrite<'_>,
) -> Result<StreamRef, Error> {
    require_registry(source)?;
    let mut reader = RecordStreamReader::<AuthorityRoot>::open(operator, source).await?;
    let mut writer =
        RecordStreamWriter::open(operator, epoch, ObjectClass::Extension, REGISTRY_KIND).await?;
    let target = match &rewrite {
        Rewrite::Insert(root) => root.name.clone(),
        Rewrite::Replace { expected, .. } | Rewrite::Delete(expected) => expected.name.clone(),
    };
    let mut applied = false;
    let mut previous = None;
    while let Some(root) = reader.next().await? {
        validate_order(&mut previous, &root.name)?;
        if !applied
            && target < root.name
            && let Rewrite::Insert(root) = &rewrite
        {
            writer.write(root).await?;
            applied = true;
        }
        if root.name == target {
            match &mut rewrite {
                Rewrite::Insert(_) => {
                    return Err(Error::new(
                        ErrorKind::Conflict,
                        "update Managed branches",
                        "branch already exists",
                    ));
                }
                Rewrite::Replace { expected, next } => {
                    if &root != *expected {
                        return Err(registry_conflict("publish Managed branch"));
                    }
                    writer.write(next).await?;
                }
                Rewrite::Delete(expected) => {
                    if &root != *expected {
                        return Err(registry_conflict("delete Managed branch"));
                    }
                }
            }
            applied = true;
        } else {
            writer.write(&root).await?;
        }
    }
    if !applied {
        if let Rewrite::Insert(root) = rewrite {
            writer.write(&root).await?;
        } else {
            return Err(Error::new(
                ErrorKind::NotFound,
                "update Managed branches",
                "branch does not exist",
            ));
        }
    }
    writer.close().await
}

async fn find_root(
    operator: &Operator,
    reference: StreamRef,
    name: &str,
) -> Result<Option<AuthorityRoot>, Error> {
    require_registry(reference)?;
    let mut reader = RecordStreamReader::<AuthorityRoot>::open(operator, reference).await?;
    let mut previous = None;
    while let Some(root) = reader.next().await? {
        validate_order(&mut previous, &root.name)?;
        match root.name.as_str().cmp(name) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(Some(root)),
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }
    Ok(None)
}

async fn write_registry(
    operator: &Operator,
    epoch: GcEpoch,
    roots: impl IntoIterator<Item = AuthorityRoot>,
) -> Result<StreamRef, Error> {
    let mut writer =
        RecordStreamWriter::open(operator, epoch, ObjectClass::Extension, REGISTRY_KIND).await?;
    let mut previous = None;
    for root in roots {
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

fn format() -> ExtensionFormat {
    ExtensionFormat {
        id: BRANCH_EXTENSION_ID,
        name: "branch".to_owned(),
        revision: 1,
        configuration: Vec::new(),
    }
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
    let metadata = match operator.stat(HEAD_KEY).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage_error("read Managed branch head", error)),
    };
    if metadata.content_length() > MAXIMUM_HEAD_BYTES as u64 {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "read Managed branch head",
            "branch head exceeds its size limit",
        ));
    }
    let bytes = operator
        .read(HEAD_KEY)
        .await
        .map_err(|error| storage_error("read Managed branch head", error))?;
    let etag = metadata
        .etag()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unsupported,
                "read Managed branch head",
                "object revision is unavailable",
            )
        })?
        .to_owned();
    Ok(Some(ObservedHead {
        head: decode_envelope(&bytes.to_vec())?,
        etag,
    }))
}

enum HeadCondition<'a> {
    Missing,
    Revision(&'a str),
}

async fn write_head(
    operator: &Operator,
    head: &RegistryHead,
    condition: HeadCondition<'_>,
) -> Result<bool, Error> {
    let bytes = encode_envelope(head)?;
    let write = operator.write_with(HEAD_KEY, bytes);
    let result = match condition {
        HeadCondition::Missing => write.if_not_exists(true).await,
        HeadCondition::Revision(etag) => write.if_match(etag).await,
    };
    match result {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(storage_error("write Managed branch head", error)),
    }
}

fn encode_envelope<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    ciborium::into_writer(value, &mut body).map_err(|_| {
        Error::new(
            ErrorKind::Invalid,
            "encode Managed branch head",
            "branch head cannot be encoded",
        )
    })?;
    let mut bytes = Vec::with_capacity(HEAD_MAGIC.len() + body.len() + 32);
    bytes.extend_from_slice(HEAD_MAGIC);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    Ok(bytes)
}

fn decode_envelope<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    if bytes.len() > MAXIMUM_HEAD_BYTES || bytes.len() < HEAD_MAGIC.len() + 32 {
        return Err(invalid_envelope());
    }
    let body_end = bytes.len() - 32;
    if &bytes[..HEAD_MAGIC.len()] != HEAD_MAGIC
        || blake3::hash(&bytes[..body_end]).as_bytes() != &bytes[body_end..]
    {
        return Err(invalid_envelope());
    }
    let mut body = &bytes[HEAD_MAGIC.len()..body_end];
    let value = ciborium::from_reader(&mut body).map_err(|_| invalid_envelope())?;
    if !body.is_empty() {
        return Err(invalid_envelope());
    }
    Ok(value)
}

fn invalid_envelope() -> Error {
    Error::new(
        ErrorKind::Corrupt,
        "read Managed branch head",
        "branch head envelope is invalid",
    )
}

fn encode_revision(revision: &RegistryRevision) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    ciborium::into_writer(revision, &mut bytes).map_err(|_| {
        Error::new(
            ErrorKind::Invalid,
            "observe Managed branch",
            "branch revision cannot be encoded",
        )
    })?;
    Ok(bytes)
}

fn decode_revision(bytes: &[u8]) -> Result<RegistryRevision, Error> {
    let mut input = bytes;
    let revision = ciborium::from_reader(&mut input).map_err(|_| {
        Error::new(
            ErrorKind::Corrupt,
            "publish Managed branch",
            "branch revision is invalid",
        )
    })?;
    if !input.is_empty() {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "publish Managed branch",
            "branch revision has trailing bytes",
        ));
    }
    Ok(revision)
}

fn storage_error(operation: &'static str, source: opendal::Error) -> Error {
    let kind = match source.kind() {
        StorageErrorKind::NotFound => ErrorKind::NotFound,
        StorageErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        StorageErrorKind::Unsupported => ErrorKind::Unsupported,
        StorageErrorKind::ConditionNotMatch | StorageErrorKind::AlreadyExists => {
            ErrorKind::Conflict
        }
        _ => ErrorKind::Unavailable,
    };
    Error::new(kind, operation, "object storage operation failed").with_source(source)
}

impl fmt::Display for BranchExtension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("branch")
    }
}
