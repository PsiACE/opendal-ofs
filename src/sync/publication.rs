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

//! Durable file publication followed by atomic namespace publication.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use crate::Error;
use crate::filesystem::{NamespaceValue, OperationId};
use crate::managed::{FileDataRef, ManagedObservation, ManagedVolume, NamespaceRevision};
use crate::namespace::Namespace;
use crate::workset::{self, Spool, SpoolWriter, Workspace};
use futures::stream::{FuturesUnordered, StreamExt as _};

use super::SyncEngine;
use super::pack::{self, PendingFile, PublicationPlan};
use super::state::ReplicaState;
use super::transfer::publish_file;

impl SyncEngine {
    pub(super) async fn prepare_and_commit(
        &self,
        state_path: &Path,
        state: &mut ReplicaState,
        observed: &ManagedObservation,
        target: &Namespace<FileDataRef>,
    ) -> Result<NamespaceRevision, Error> {
        let operation = OperationId::generate();
        let revision = self
            .volume
            .prepare_publication(observed, target, operation)
            .await?;
        state.begin_publication(
            observed.revision(),
            revision,
            operation,
            observed.maintenance_generation(),
        )?;
        super::state_file::persist(state, state_path, true)?;
        crate::fault::check("before-publish")?;
        self.volume
            .commit_publication(observed, revision, operation)
            .await?;
        crate::fault::check("after-publish")?;
        Ok(revision)
    }

    pub(super) async fn publish_files(
        &self,
        root: &Path,
        observed: &ManagedObservation,
        target: &Namespace<Option<FileDataRef>>,
    ) -> Result<Namespace<FileDataRef>, Error> {
        let workspace = Workspace::create(self.volume.workset_options())?;
        let mut completed = workspace.writer("completed-file-publications")?;
        let mut publications = FuturesUnordered::<PublicationFuture>::new();
        let mut pack = None;
        let pack_target = self.volume.pack_target_bytes();
        let mut target_reader = target.reader()?;
        while let Some(record) = target_reader.next()? {
            let Some(node) = record.value.as_ref() else {
                return Err(Error::corrupt(
                    "publish Managed files",
                    "current namespace contains a tombstone",
                ));
            };
            let NamespaceValue::RegularFile {
                fingerprint,
                content: None,
                ..
            } = &node.value
            else {
                continue;
            };
            let pending = PendingFile {
                path: record.path,
                fingerprint: *fingerprint,
            };
            if pack_target.is_some_and(|target| PublicationPlan::accepts(target, *fingerprint)) {
                let target = pack_target.expect("Pack placement has a target");
                if pack
                    .as_ref()
                    .is_some_and(|plan: &PublicationPlan| plan.would_overflow(target, *fingerprint))
                {
                    schedule_pack(
                        &mut publications,
                        pack.take().expect("non-empty Pack plan"),
                        self.volume.clone(),
                        workspace.clone(),
                        root.to_owned(),
                        observed.gc_epoch(),
                    )?;
                }
                let plan = match pack.as_mut() {
                    Some(plan) => plan,
                    None => pack.insert(PublicationPlan::create(&workspace)?),
                };
                plan.push(pending)?;
            } else {
                schedule_file(
                    &mut publications,
                    self.volume.clone(),
                    root.to_owned(),
                    pending,
                    observed.gc_epoch(),
                );
            }
            if publications.len() >= self.transfer_concurrency {
                let publication = publications
                    .next()
                    .await
                    .expect("a file publication remains")?;
                record_publication(&mut completed, publication)?;
            }
        }
        if let Some(pack) = pack {
            schedule_pack(
                &mut publications,
                pack,
                self.volume.clone(),
                workspace.clone(),
                root.to_owned(),
                observed.gc_epoch(),
            )?;
        }
        while let Some(publication) = publications.next().await {
            record_publication(&mut completed, publication?)?;
        }

        let completed = workset::sort(
            &workspace,
            &completed.finish()?,
            |(path, _): &(String, FileDataRef)| path.clone(),
        )?;
        let mut completed_reader = completed.reader()?;
        let mut target_reader = target.reader()?;
        let mut output = workspace.writer("published-namespace")?;
        while let Some(record) = target_reader.next()? {
            let node = record
                .value
                .as_ref()
                .expect("current namespace was validated before publication");
            let content = match &node.value {
                NamespaceValue::Directory { .. } => None,
                NamespaceValue::RegularFile {
                    content: Some(reference),
                    ..
                } => Some(*reference),
                NamespaceValue::RegularFile { content: None, .. } => {
                    let Some((path, reference)) = completed_reader.next()? else {
                        return Err(Error::corrupt(
                            "publish Managed files",
                            "published file result is missing",
                        ));
                    };
                    if path != record.path {
                        return Err(Error::corrupt(
                            "publish Managed files",
                            "published file result does not match its namespace path",
                        ));
                    }
                    Some(reference)
                }
            };
            output.write(&record.map_content(|_| content.expect("regular file content")))?;
        }
        if completed_reader.next()?.is_some() {
            return Err(Error::corrupt(
                "publish Managed files",
                "published file result has no namespace entry",
            ));
        }
        Ok(Namespace {
            volume_id: target.volume_id,
            cursor: target.cursor,
            root: target.root,
            entries: output.finish()?,
        })
    }
}

enum PublishedFiles {
    One(String, FileDataRef),
    Pack(Spool<(String, FileDataRef)>),
}

type PublicationFuture = Pin<Box<dyn Future<Output = Result<PublishedFiles, Error>> + Send>>;

fn schedule_file(
    publications: &mut FuturesUnordered<PublicationFuture>,
    volume: ManagedVolume,
    root: PathBuf,
    file: PendingFile,
    gc_epoch: crate::managed::GcEpoch,
) {
    publications.push(Box::pin(async move {
        let content =
            publish_file(&volume, &root.join(&file.path), file.fingerprint, gc_epoch).await?;
        Ok(PublishedFiles::One(file.path, content))
    }));
}

fn schedule_pack(
    publications: &mut FuturesUnordered<PublicationFuture>,
    plan: PublicationPlan,
    volume: ManagedVolume,
    workspace: Workspace,
    root: PathBuf,
    gc_epoch: crate::managed::GcEpoch,
) -> Result<(), Error> {
    let files = plan.finish()?;
    publications.push(Box::pin(async move {
        pack::publish(&volume, &workspace, &root, &files, gc_epoch)
            .await
            .map(PublishedFiles::Pack)
    }));
    Ok(())
}

fn record_publication(
    completed: &mut SpoolWriter<(String, FileDataRef)>,
    publication: PublishedFiles,
) -> Result<(), Error> {
    match publication {
        PublishedFiles::One(path, content) => completed.write(&(path, content)),
        PublishedFiles::Pack(files) => {
            let mut files = files.reader()?;
            while let Some(file) = files.next()? {
                completed.write(&file)?;
            }
            Ok(())
        }
    }
}
