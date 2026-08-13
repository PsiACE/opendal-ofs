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

use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

use crate::workset::{self, Spool, Workspace};
use crate::{Error, ErrorKind};

use super::ManagedVolume;
use super::authority::{AuthorityHead, AuthorityRoot, AuthorityRoots};
use super::object::{GcEpoch, OBJECT_PREFIX, ObjectLocator};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOutcome {
    pub scanned: u64,
    pub deleted: u64,
    pub deleted_bytes: u64,
}

impl ManagedVolume {
    /// Rotate the upload epoch, compact live metadata, then merge a streamed
    /// inventory against the streamed reachability set.
    pub async fn collect_unreachable(&self) -> Result<GcOutcome, Error> {
        let (fence, mut roots) = self
            .authority_access
            .begin_collection_dyn(&self.access_context)
            .await?;
        let collection_epoch = fence.epoch();
        crate::fault::check("after-gc-epoch-rotation")?;

        let workspace = Workspace::create(self.workset_options())?;
        let mut records = workspace.writer("gc-reachable")?;
        let mut compacted = workspace.writer("gc-authority-roots")?;
        while let Some(root) = roots.next().await? {
            let collection_commit = self
                .compact_for_collection(
                    &workspace,
                    root.head.current_commit(),
                    collection_epoch,
                    |reference| records.write(&reference),
                )
                .await?;
            compacted.write(&AuthorityRoot {
                id: root.id,
                name: root.name,
                head: AuthorityHead::new(
                    collection_commit,
                    collection_epoch,
                    collection_commit.cursor(),
                ),
            })?;
        }
        let mut compacted = SpoolAuthorityRoots(compacted.finish()?.reader()?);
        if !self
            .authority_access
            .finish_collection_dyn(&self.access_context, fence, &mut compacted)
            .await?
        {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed objects",
                "namespace authority changed while publishing compacted roots",
            ));
        }

        let marks = workset::sort(&workspace, &records.finish()?, |identity| *identity)?;
        let candidates = self.inventory_workset(&workspace, collection_epoch).await?;
        self.sweep_worksets(&marks, &candidates).await
    }

    async fn inventory_workset(
        &self,
        workspace: &Workspace,
        current_epoch: GcEpoch,
    ) -> Result<Spool<ObjectRecord>, Error> {
        let mut records = workspace.writer("gc-inventory")?;
        let mut lister = self
            .operator()
            .lister_with(OBJECT_PREFIX)
            .recursive(true)
            .await
            .map_err(|error| Error::from_storage("list Managed objects", error))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|error| Error::from_storage("list Managed objects", error))?
        {
            if !entry.metadata().is_file() {
                continue;
            }
            let identity = ObjectLocator::parse_key(entry.path()).ok_or_else(|| {
                Error::corrupt("collect Managed objects", "object key is invalid")
            })?;
            if identity.gc_epoch.value() >= current_epoch.value() {
                continue;
            }
            records.write(&ObjectRecord {
                identity,
                length: entry.metadata().content_length(),
            })?;
        }
        workset::sort(workspace, &records.finish()?, |record| record.identity)
    }

    async fn sweep_worksets(
        &self,
        marks: &Spool<ObjectLocator>,
        candidates: &Spool<ObjectRecord>,
    ) -> Result<GcOutcome, Error> {
        let mut marks = marks.reader()?;
        let mut mark = marks.next()?;
        let mut candidates = candidates.reader()?;
        let mut outcome = GcOutcome::default();
        let mut deleter = self
            .operator()
            .deleter()
            .await
            .map_err(|error| Error::from_storage("open Managed object deleter", error))?;

        while let Some(candidate) = candidates.next()? {
            outcome.scanned = outcome.scanned.checked_add(1).ok_or_else(|| {
                Error::corrupt("collect Managed objects", "scanned object count overflows")
            })?;
            while mark.as_ref().is_some_and(|mark| *mark < candidate.identity) {
                mark = next_unique_mark(&mut marks, mark.take())?;
            }
            if let Some(reachable) = mark.as_ref()
                && *reachable == candidate.identity
            {
                continue;
            }
            deleter
                .delete(candidate.identity.key())
                .await
                .map_err(|error| Error::from_storage("delete Managed object", error))?;
            outcome.deleted = outcome.deleted.checked_add(1).ok_or_else(|| {
                Error::corrupt("collect Managed objects", "deleted object count overflows")
            })?;
            outcome.deleted_bytes = outcome
                .deleted_bytes
                .checked_add(candidate.length)
                .ok_or_else(|| {
                    Error::corrupt("collect Managed objects", "deleted byte count overflows")
                })?;
        }
        deleter
            .close()
            .await
            .map_err(|error| Error::from_storage("finish Managed object deletion", error))?;
        Ok(outcome)
    }
}

struct SpoolAuthorityRoots(workset::SpoolReader<AuthorityRoot>);

impl AuthorityRoots for SpoolAuthorityRoots {
    fn next(
        &mut self,
    ) -> super::authority::AuthorityFuture<'_, Result<Option<AuthorityRoot>, Error>> {
        let root = self.0.next();
        Box::pin(async move { root })
    }
}

fn next_unique_mark(
    reader: &mut workset::SpoolReader<ObjectLocator>,
    previous: Option<ObjectLocator>,
) -> Result<Option<ObjectLocator>, Error> {
    let mut next = reader.next()?;
    while let (Some(previous), Some(current)) = (previous, next) {
        if previous != current {
            return Ok(Some(current));
        }
        next = reader.next()?;
    }
    Ok(next)
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct ObjectRecord {
    identity: ObjectLocator,
    length: u64,
}
