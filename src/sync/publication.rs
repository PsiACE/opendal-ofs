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

use std::path::Path;

use futures::TryStreamExt as _;

use crate::Error;
use crate::filesystem::{NamespaceValue, OperationId};
use crate::managed::{FileDataRef, ManagedObservation, NamespaceRevision};
use crate::namespace::Namespace;
use crate::workset::{self, Workspace};

use super::SyncEngine;
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
        let publications = target
            .entries
            .stream()?
            .try_filter_map(|record| async move {
                let Some(node) = record.value.as_ref() else {
                    return Err(Error::corrupt(
                        "publish Managed files",
                        "current namespace contains a tombstone",
                    ));
                };
                let publication = match &node.value {
                    NamespaceValue::RegularFile {
                        fingerprint,
                        content: None,
                        ..
                    } => Some((record.path, *fingerprint)),
                    NamespaceValue::Directory { .. }
                    | NamespaceValue::RegularFile {
                        content: Some(_), ..
                    } => None,
                };
                Ok(publication)
            })
            .map_ok(|(path, fingerprint)| async move {
                let content = publish_file(
                    &self.volume,
                    &root.join(&path),
                    fingerprint,
                    observed.gc_epoch(),
                )
                .await?;
                Ok::<_, Error>((path, content))
            })
            .try_buffer_unordered(self.transfer_concurrency);
        futures::pin_mut!(publications);
        while let Some(publication) = publications.try_next().await? {
            completed.write(&publication)?;
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
