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
use crate::managed::{ManagedObservation, NamespaceRevision, StreamRef};
use crate::namespace::Namespace;
use crate::workset::Workspace;

use super::SyncEngine;
use super::state::ReplicaState;
use super::transfer::publish_file;

impl SyncEngine {
    pub(super) async fn prepare_and_commit(
        &self,
        state_path: &Path,
        state: &mut ReplicaState,
        observed: &ManagedObservation,
        target: &Namespace<StreamRef>,
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
        state.save(state_path)?;
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
        target: &Namespace<Option<StreamRef>>,
    ) -> Result<Namespace<StreamRef>, Error> {
        let workspace = Workspace::create(self.volume.workset_options())?;
        let mut output = workspace.writer("published-namespace")?;
        let publications = target
            .entries
            .stream()?
            .map_ok(|record| async move {
                let Some(node) = record.value.as_ref() else {
                    return Err(Error::corrupt(
                        "publish Managed files",
                        "current namespace contains a tombstone",
                    ));
                };
                let content = match &node.value {
                    NamespaceValue::Directory { .. } => None,
                    NamespaceValue::RegularFile {
                        fingerprint,
                        content,
                        ..
                    } => match content {
                        Some(reference) => Some(*reference),
                        None => Some(
                            publish_file(
                                &self.volume,
                                &root.join(&record.path),
                                *fingerprint,
                                observed.gc_epoch(),
                            )
                            .await?,
                        ),
                    },
                };
                Ok::<_, Error>(record.map_content(|_| content.expect("regular file content")))
            })
            .try_buffered(self.transfer_concurrency);
        futures::pin_mut!(publications);
        while let Some(record) = publications.try_next().await? {
            output.write(&record)?;
        }
        Ok(Namespace {
            volume_id: target.volume_id,
            cursor: target.cursor,
            root: target.root,
            entries: output.finish()?,
        })
    }
}
