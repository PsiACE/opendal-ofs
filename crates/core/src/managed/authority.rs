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

//! Typed composition point for namespace authority extensions.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::ChangeCursor;

use super::extension::{AccessContext, ExtensionFormat};
use super::head::NamespaceRevision;
use super::object::GcEpoch;
use super::storage;

pub(crate) const DEFAULT_AUTHORITY: &str = "main";
const HEAD_KEY: &str = "managed/1/head";
const MAXIMUM_HEAD_BYTES: usize = 64 * 1024;

/// Stable identity of one namespace authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AuthorityId([u8; 16]);

impl AuthorityId {
    /// Generate a new authority identity.
    pub fn generate() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }
}

/// Current namespace and reclamation position of one authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorityHead {
    pub(crate) current_commit: NamespaceRevision,
    pub(crate) gc_epoch: GcEpoch,
    pub(crate) minimum_retained_cursor: ChangeCursor,
}

super::wire::tuple_wire!(AuthorityHead {
    current_commit: NamespaceRevision,
    gc_epoch: GcEpoch,
    minimum_retained_cursor: ChangeCursor,
});

impl AuthorityHead {
    /// Construct an authority position.
    pub const fn new(
        current_commit: NamespaceRevision,
        gc_epoch: GcEpoch,
        minimum_retained_cursor: ChangeCursor,
    ) -> Self {
        Self {
            current_commit,
            gc_epoch,
            minimum_retained_cursor,
        }
    }

    /// Current namespace revision.
    pub const fn current_commit(self) -> NamespaceRevision {
        self.current_commit
    }

    /// Epoch used for new immutable objects.
    pub const fn gc_epoch(self) -> GcEpoch {
        self.gc_epoch
    }

    /// Oldest namespace cursor still retained by this authority.
    pub const fn minimum_retained_cursor(self) -> ChangeCursor {
        self.minimum_retained_cursor
    }
}

/// One observed authority position and its opaque conditional revision.
#[derive(Clone, Debug)]
pub struct AuthorityObservation {
    id: AuthorityId,
    head: AuthorityHead,
    revision: Vec<u8>,
}

impl AuthorityObservation {
    /// Construct an observation returned by an authority extension.
    pub fn new(id: AuthorityId, head: AuthorityHead, revision: Vec<u8>) -> Self {
        Self { id, head, revision }
    }

    /// Stable identity of the selected authority.
    pub const fn id(&self) -> AuthorityId {
        self.id
    }

    /// Observed namespace and reclamation position.
    pub const fn head(&self) -> AuthorityHead {
        self.head
    }

    /// Opaque conditional revision owned by the authority implementation.
    pub fn revision(&self) -> &[u8] {
        &self.revision
    }
}

/// One live root consumed and replaced during collection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityRoot {
    /// Stable authority identity.
    pub id: AuthorityId,
    /// Display and selection name.
    pub name: String,
    /// Namespace and reclamation position.
    pub head: AuthorityHead,
}

/// Opaque fence established before collection roots are streamed.
#[derive(Clone, Debug)]
pub struct CollectionFence {
    epoch: GcEpoch,
    revision: Vec<u8>,
}

impl CollectionFence {
    /// Construct a fence owned by an authority extension.
    pub fn new(epoch: GcEpoch, revision: Vec<u8>) -> Self {
        Self { epoch, revision }
    }

    /// Epoch reserved for compacted roots and concurrent publications.
    pub const fn epoch(&self) -> GcEpoch {
        self.epoch
    }

    /// Opaque conditional revision owned by the authority extension.
    pub fn revision(&self) -> &[u8] {
        &self.revision
    }
}

/// Boxed future used by the erased authority boundary.
pub type AuthorityFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Forward-only stream of authority roots.
pub trait AuthorityRoots: Send {
    /// Read the next root.
    fn next(&mut self) -> AuthorityFuture<'_, Result<Option<AuthorityRoot>, Error>>;
}

/// Namespace authority access. Authority extensions wrap this boundary.
pub trait AuthorityAccess: Send + Sync + fmt::Debug + Unpin + 'static {
    /// Describe the persisted authority extension, or `None` for the core authority.
    fn info(&self) -> Option<ExtensionFormat>;

    /// Initialize the first authority exactly once.
    fn initialize<'a>(
        &'a self,
        context: &'a AccessContext,
        initial: AuthorityHead,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;

    /// Observe one selected authority.
    fn observe<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
    ) -> impl Future<Output = Result<AuthorityObservation, Error>> + Send + 'a;

    /// Conditionally replace one selected authority.
    fn compare_exchange<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
        observed: &'a AuthorityObservation,
        next: AuthorityHead,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a;

    /// Rotate the global collection epoch and stream an exact root set.
    fn begin_collection<'a>(
        &'a self,
        context: &'a AccessContext,
    ) -> impl Future<Output = Result<(CollectionFence, Box<dyn AuthorityRoots>), Error>> + Send + 'a;

    /// Publish compacted roots if the collection fence is still current.
    fn finish_collection<'a>(
        &'a self,
        context: &'a AccessContext,
        fence: CollectionFence,
        roots: &'a mut dyn AuthorityRoots,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a;
}

/// Typed namespace-authority extension.
pub trait AuthorityExtension<A: AuthorityAccess> {
    /// Resulting statically composed access.
    type ExtendedAccess: AuthorityAccess;

    /// Wrap the inner authority access.
    fn extend(&self, inner: A) -> Self::ExtendedAccess;
}

/// Default forwarding surface for namespace authority extensions.
pub trait ExtendedAuthorityAccess: Send + Sync + fmt::Debug + Unpin + 'static {
    /// Wrapped authority access.
    type Inner: AuthorityAccess;

    /// Return the wrapped access.
    fn inner(&self) -> &Self::Inner;

    /// Forward the authority description by default.
    fn info(&self) -> Option<ExtensionFormat> {
        self.inner().info()
    }

    /// Forward initialization by default.
    fn initialize<'a>(
        &'a self,
        context: &'a AccessContext,
        initial: AuthorityHead,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        self.inner().initialize(context, initial)
    }

    /// Forward observation by default.
    fn observe<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
    ) -> impl Future<Output = Result<AuthorityObservation, Error>> + Send + 'a {
        self.inner().observe(context, name)
    }

    /// Forward conditional publication by default.
    fn compare_exchange<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
        observed: &'a AuthorityObservation,
        next: AuthorityHead,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        self.inner().compare_exchange(context, name, observed, next)
    }

    /// Forward collection fencing by default.
    fn begin_collection<'a>(
        &'a self,
        context: &'a AccessContext,
    ) -> impl Future<Output = Result<(CollectionFence, Box<dyn AuthorityRoots>), Error>> + Send + 'a
    {
        self.inner().begin_collection(context)
    }

    /// Forward compacted root publication by default.
    fn finish_collection<'a>(
        &'a self,
        context: &'a AccessContext,
        fence: CollectionFence,
        roots: &'a mut dyn AuthorityRoots,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        self.inner().finish_collection(context, fence, roots)
    }
}

impl<T: ExtendedAuthorityAccess> AuthorityAccess for T {
    fn info(&self) -> Option<ExtensionFormat> {
        ExtendedAuthorityAccess::info(self)
    }

    fn initialize<'a>(
        &'a self,
        context: &'a AccessContext,
        initial: AuthorityHead,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        ExtendedAuthorityAccess::initialize(self, context, initial)
    }

    fn observe<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
    ) -> impl Future<Output = Result<AuthorityObservation, Error>> + Send + 'a {
        ExtendedAuthorityAccess::observe(self, context, name)
    }

    fn compare_exchange<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
        observed: &'a AuthorityObservation,
        next: AuthorityHead,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        ExtendedAuthorityAccess::compare_exchange(self, context, name, observed, next)
    }

    fn begin_collection<'a>(
        &'a self,
        context: &'a AccessContext,
    ) -> impl Future<Output = Result<(CollectionFence, Box<dyn AuthorityRoots>), Error>> + Send + 'a
    {
        ExtendedAuthorityAccess::begin_collection(self, context)
    }

    fn finish_collection<'a>(
        &'a self,
        context: &'a AccessContext,
        fence: CollectionFence,
        roots: &'a mut dyn AuthorityRoots,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        ExtendedAuthorityAccess::finish_collection(self, context, fence, roots)
    }
}

/// Object-safe authority access used only by `ManagedVolume`.
pub trait AuthorityAccessDyn: Send + Sync + fmt::Debug + Unpin {
    fn info_dyn(&self) -> Option<ExtensionFormat>;
    fn initialize_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        initial: AuthorityHead,
    ) -> AuthorityFuture<'a, Result<(), Error>>;
    fn observe_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
    ) -> AuthorityFuture<'a, Result<AuthorityObservation, Error>>;
    fn compare_exchange_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
        observed: &'a AuthorityObservation,
        next: AuthorityHead,
    ) -> AuthorityFuture<'a, Result<bool, Error>>;
    fn begin_collection_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
    ) -> AuthorityFuture<'a, Result<(CollectionFence, Box<dyn AuthorityRoots>), Error>>;
    fn finish_collection_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        fence: CollectionFence,
        roots: &'a mut dyn AuthorityRoots,
    ) -> AuthorityFuture<'a, Result<bool, Error>>;
}

impl<A: AuthorityAccess> AuthorityAccessDyn for A {
    fn info_dyn(&self) -> Option<ExtensionFormat> {
        self.info()
    }

    fn initialize_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        initial: AuthorityHead,
    ) -> AuthorityFuture<'a, Result<(), Error>> {
        Box::pin(self.initialize(context, initial))
    }

    fn observe_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
    ) -> AuthorityFuture<'a, Result<AuthorityObservation, Error>> {
        Box::pin(self.observe(context, name))
    }

    fn compare_exchange_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        name: &'a str,
        observed: &'a AuthorityObservation,
        next: AuthorityHead,
    ) -> AuthorityFuture<'a, Result<bool, Error>> {
        Box::pin(self.compare_exchange(context, name, observed, next))
    }

    fn begin_collection_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
    ) -> AuthorityFuture<'a, Result<(CollectionFence, Box<dyn AuthorityRoots>), Error>> {
        Box::pin(self.begin_collection(context))
    }

    fn finish_collection_dyn<'a>(
        &'a self,
        context: &'a AccessContext,
        fence: CollectionFence,
        roots: &'a mut dyn AuthorityRoots,
    ) -> AuthorityFuture<'a, Result<bool, Error>> {
        Box::pin(self.finish_collection(context, fence, roots))
    }
}

/// Core single-authority implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultAuthorityAccess;

impl AuthorityAccess for DefaultAuthorityAccess {
    fn info(&self) -> Option<ExtensionFormat> {
        None
    }

    async fn initialize(
        &self,
        context: &AccessContext,
        initial: AuthorityHead,
    ) -> Result<(), Error> {
        if storage::read_control(context.operator(), HEAD_KEY, MAXIMUM_HEAD_BYTES)
            .await?
            .is_some()
        {
            self.observe(context, DEFAULT_AUTHORITY).await?;
            return Ok(());
        }
        storage::write_control(
            context.operator(),
            HEAD_KEY,
            encode_head(initial)?,
            storage::ControlCondition::Missing,
        )
        .await?;
        Ok(())
    }

    async fn observe(
        &self,
        context: &AccessContext,
        name: &str,
    ) -> Result<AuthorityObservation, Error> {
        require_default(name)?;
        let control = storage::read_control(context.operator(), HEAD_KEY, MAXIMUM_HEAD_BYTES)
            .await?
            .ok_or_else(|| {
                Error::new(
                    crate::ErrorKind::NotFound,
                    "open Managed volume",
                    "namespace head is missing",
                )
            })?;
        let head = decode_head(&control.bytes)?;
        Ok(AuthorityObservation::new(
            AuthorityId([0; 16]),
            head,
            control.revision.into_bytes(),
        ))
    }

    async fn compare_exchange(
        &self,
        context: &AccessContext,
        name: &str,
        observed: &AuthorityObservation,
        next: AuthorityHead,
    ) -> Result<bool, Error> {
        require_default(name)?;
        let revision = std::str::from_utf8(observed.revision())
            .map_err(|_| Error::corrupt("publish Managed namespace", "head revision is invalid"))?;
        storage::write_control(
            context.operator(),
            HEAD_KEY,
            encode_head(next)?,
            storage::ControlCondition::Revision(revision),
        )
        .await
    }

    async fn begin_collection(
        &self,
        context: &AccessContext,
    ) -> Result<(CollectionFence, Box<dyn AuthorityRoots>), Error> {
        let observed = self.observe(context, DEFAULT_AUTHORITY).await?;
        let mut rotated = observed.head();
        rotated.gc_epoch = rotated.gc_epoch.next()?;
        if !self
            .compare_exchange(context, DEFAULT_AUTHORITY, &observed, rotated)
            .await?
        {
            return Err(Error::conflict(
                "collect Managed objects",
                "namespace authority changed while rotating the GC epoch",
            ));
        }
        let current = self.observe(context, DEFAULT_AUTHORITY).await?;
        Ok((
            CollectionFence::new(rotated.gc_epoch, current.revision().to_vec()),
            Box::new(OneRoot(Some(AuthorityRoot {
                id: current.id(),
                name: DEFAULT_AUTHORITY.to_owned(),
                head: current.head(),
            }))),
        ))
    }

    async fn finish_collection(
        &self,
        context: &AccessContext,
        fence: CollectionFence,
        roots: &mut dyn AuthorityRoots,
    ) -> Result<bool, Error> {
        let root = roots.next().await?.ok_or_else(|| {
            Error::corrupt(
                "collect Managed objects",
                "compacted authority root is missing",
            )
        })?;
        if root.name != DEFAULT_AUTHORITY || roots.next().await?.is_some() {
            return Err(Error::corrupt(
                "collect Managed objects",
                "compacted authority root set is invalid",
            ));
        }
        let observed = AuthorityObservation::new(root.id, root.head, fence.revision);
        self.compare_exchange(context, DEFAULT_AUTHORITY, &observed, root.head)
            .await
    }
}

struct OneRoot(Option<AuthorityRoot>);

impl AuthorityRoots for OneRoot {
    fn next(&mut self) -> AuthorityFuture<'_, Result<Option<AuthorityRoot>, Error>> {
        let root = self.0.take();
        Box::pin(async move { Ok(root) })
    }
}

fn require_default(name: &str) -> Result<(), Error> {
    if name == DEFAULT_AUTHORITY {
        Ok(())
    } else {
        Err(Error::new(
            crate::ErrorKind::NotFound,
            "open Managed authority",
            "the selected authority does not exist",
        ))
    }
}

fn encode_head(head: AuthorityHead) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    ciborium::into_writer(&head, &mut bytes)
        .map_err(|_| Error::invalid("encode Managed authority", "authority cannot be encoded"))?;
    Ok(bytes)
}

fn decode_head(bytes: &[u8]) -> Result<AuthorityHead, Error> {
    let mut input = bytes;
    let head: AuthorityHead = ciborium::from_reader(&mut input)
        .map_err(|_| Error::corrupt("read Managed authority", "authority is invalid"))?;
    if !input.is_empty()
        || head.minimum_retained_cursor.sequence() > head.current_commit.cursor().sequence()
    {
        return Err(Error::corrupt(
            "read Managed authority",
            "authority position is invalid",
        ));
    }
    Ok(head)
}
