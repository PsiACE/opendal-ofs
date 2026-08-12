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
use super::object::{GcEpoch, ObjectClass, ObjectId, ObjectRef};

const OBJECT_PREFIX: &str = "managed/1/objects/";

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
        let (mut head, revision) = self.read_head().await?;
        let previous_commit = head.current_commit;
        let collection_epoch = head.gc_epoch.next()?;
        head.gc_epoch = collection_epoch;
        if !self.replace_head(&revision, &head).await? {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed objects",
                "namespace authority changed while rotating the GC epoch",
            ));
        }
        crate::fault::check("after-gc-epoch-rotation")?;

        let (mut rotated, revision) = self.read_head().await?;
        if rotated.gc_epoch != collection_epoch || rotated.current_commit != previous_commit {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed objects",
                "namespace authority changed before metadata compaction",
            ));
        }
        let collection_commit = self
            .compact_for_collection(previous_commit, collection_epoch)
            .await?;
        rotated.current_commit = collection_commit;
        if !self.replace_head(&revision, &rotated).await? {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed objects",
                "namespace authority changed while publishing compacted metadata",
            ));
        }

        let workspace = Workspace::create(self.workset_options())?;
        let marks = self
            .reachable_workset(&workspace, collection_commit)
            .await?;
        let candidates = self.inventory_workset(&workspace, collection_epoch).await?;
        let outcome = self.sweep_worksets(&marks, &candidates).await?;
        self.advance_reclamation_watermark(collection_commit.cursor())
            .await?;
        Ok(outcome)
    }

    async fn reachable_workset(
        &self,
        workspace: &Workspace,
        commit: super::NamespaceRevision,
    ) -> Result<Spool<ObjectRecord>, Error> {
        let mut records = workspace.writer("gc-reachable")?;
        self.visit_reachable_objects(commit, |reference| {
            records.write(&ObjectRecord::from_ref(reference))
        })
        .await?;
        workset::sort(workspace, &records.finish()?, |record| record.identity)
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
            let identity = ObjectIdentity::parse(entry.path()).ok_or_else(|| {
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
        marks: &Spool<ObjectRecord>,
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
            while mark
                .as_ref()
                .is_some_and(|mark| mark.identity < candidate.identity)
            {
                mark = next_unique_mark(&mut marks, mark.take())?;
            }
            if let Some(reachable) = mark.as_ref()
                && reachable.identity == candidate.identity
            {
                if reachable.length != candidate.length {
                    return Err(Error::corrupt(
                        "collect Managed objects",
                        "reachable object length changed",
                    ));
                }
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

    async fn advance_reclamation_watermark(
        &self,
        completed: crate::filesystem::ChangeCursor,
    ) -> Result<(), Error> {
        for _ in 0..8 {
            let (mut head, revision) = self.read_head().await?;
            if head.minimum_retained_cursor.sequence() >= completed.sequence() {
                return Ok(());
            }
            head.minimum_retained_cursor = completed;
            if self.replace_head(&revision, &head).await? {
                return Ok(());
            }
        }
        Err(Error::new(
            ErrorKind::Conflict,
            "collect Managed objects",
            "namespace kept changing while publishing the reclamation watermark",
        ))
    }
}

fn next_unique_mark(
    reader: &mut workset::SpoolReader<ObjectRecord>,
    previous: Option<ObjectRecord>,
) -> Result<Option<ObjectRecord>, Error> {
    let mut next = reader.next()?;
    while let (Some(previous), Some(current)) = (previous, next) {
        if previous.identity != current.identity {
            return Ok(Some(current));
        }
        if previous.length != current.length {
            return Err(Error::corrupt(
                "collect Managed objects",
                "one reachable object has conflicting lengths",
            ));
        }
        next = reader.next()?;
    }
    Ok(next)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct ObjectIdentity {
    gc_epoch: GcEpoch,
    class: ObjectClass,
    id: ObjectId,
}

impl ObjectIdentity {
    const fn from_ref(reference: ObjectRef) -> Self {
        Self {
            gc_epoch: reference.gc_epoch,
            class: reference.class,
            id: reference.id,
        }
    }

    fn parse(path: &str) -> Option<Self> {
        let mut parts = path.strip_prefix(OBJECT_PREFIX)?.split('/');
        let gc_epoch = GcEpoch::from_value(parts.next()?.parse().ok()?);
        let class = ObjectClass::parse(parts.next()?)?;
        let prefix = parts.next()?;
        let encoded = parts.next()?;
        if parts.next().is_some() || prefix.len() != 2 || encoded.len() != 32 {
            return None;
        }
        let mut id = [0_u8; 16];
        for (index, byte) in id.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16).ok()?;
        }
        (format!("{:02x}", id[0]) == prefix).then_some(Self {
            gc_epoch,
            class,
            id: ObjectId::from_bytes(id),
        })
    }

    fn key(self) -> String {
        super::object::object_key(self.gc_epoch, self.class, self.id)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct ObjectRecord {
    identity: ObjectIdentity,
    length: u64,
}

impl ObjectRecord {
    const fn from_ref(reference: ObjectRef) -> Self {
        Self {
            identity: ObjectIdentity::from_ref(reference),
            length: reference.encoded_length,
        }
    }
}
