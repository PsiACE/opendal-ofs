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

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::path::Path;

use futures::TryStreamExt as _;
use serde::de::DeserializeOwned;

use crate::Error;
use crate::filesystem::{ChangeCursor, NamespaceValue, OperationId};
use crate::managed::{ManagedObservation, ManagedVolume, NamespaceRevision, StreamRef};
use crate::workset::{Namespace, Workspace};

use super::ReplicaState;
use super::install::{install, repair};
use super::reconcile::{changed_paths, reconcile};
use super::scan::{ScannedTree, scan};
use super::transfer::publish_file;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncOutcome {
    pub conflict_paths: Vec<String>,
    pub published: bool,
    pub sequence: u64,
}

pub struct SyncEngine {
    transfer_concurrency: usize,
    volume: ManagedVolume,
}

impl SyncEngine {
    pub const fn new(volume: ManagedVolume, transfer_concurrency: NonZeroUsize) -> Self {
        Self {
            transfer_concurrency: transfer_concurrency.get(),
            volume,
        }
    }

    pub async fn sync(
        &self,
        root: &Path,
        state_path: &Path,
        resolve_paths: &[String],
    ) -> Result<SyncOutcome, Error> {
        let resolved = resolve_paths.iter().cloned().collect::<BTreeSet<_>>();
        if resolved.len() != resolve_paths.len() {
            return Err(Error::invalid(
                "synchronize replica",
                "a conflict resolution path was provided more than once",
            ));
        }
        let root = std::fs::canonicalize(root)
            .map_err(|error| Error::from_io("open replica directory", Some(root), error))?;
        if !root.is_dir() {
            return Err(Error::invalid(
                "synchronize replica",
                "replica path is not a directory",
            ));
        }

        let observed = self.volume.observe().await?;
        let stored = ReplicaState::load(state_path)?;
        if let Some(state) = &stored {
            if state.root() != root {
                return Err(Error::invalid(
                    "synchronize replica",
                    "replica state belongs to a different local directory",
                ));
            }
            if state.volume_id() != self.volume.id() {
                return Err(Error::invalid(
                    "synchronize replica",
                    "replica state belongs to a different volume",
                ));
            }
        }
        if let Some((target, published)) = stored.as_ref().and_then(ReplicaState::installation) {
            return self
                .recover_install(
                    &root,
                    state_path,
                    stored.expect("checked interrupted installation state"),
                    observed,
                    target,
                    published,
                )
                .await;
        }

        let mut state = match stored {
            Some(state) => state,
            None if observed.namespace.cursor == ChangeCursor::GENESIS
                && !directory_is_empty(&root)? =>
            {
                if !resolved.is_empty() {
                    return Err(Error::invalid(
                        "synchronize replica",
                        "--resolve requires an unresolved conflict in replica state",
                    ));
                }
                let state = ReplicaState::new(root.clone(), self.volume.id(), observed.revision());
                state.save_new(state_path)?;
                state
            }
            None => {
                if !resolved.is_empty() {
                    return Err(Error::invalid(
                        "synchronize replica",
                        "--resolve requires an unresolved conflict in replica state",
                    ));
                }
                require_empty(&root)?;
                let mut state = ReplicaState::for_cold_install(
                    root.clone(),
                    self.volume.id(),
                    observed.revision(),
                );
                state.save_new(state_path)?;
                install::<StreamRef>(
                    &root,
                    None,
                    &observed.namespace,
                    &self.volume,
                    self.transfer_concurrency,
                )
                .await?;
                state.advance(observed.revision());
                state.save(state_path)?;
                return Ok(SyncOutcome {
                    conflict_paths: Vec::new(),
                    published: false,
                    sequence: observed.namespace.cursor.sequence(),
                });
            }
        };
        if state.pending_publication().is_some() {
            return self
                .recover_pending(&root, state_path, state, observed)
                .await;
        }

        if state.common_revision() != observed.revision()
            && state.common_revision().cursor().sequence()
                == observed.revision().cursor().sequence()
        {
            state.rebase_equivalent(observed.revision())?;
            state.save(state_path)?;
        }
        if !observed.can_read_revision(state.common_revision()) {
            return self
                .conservative_rebase(&root, state_path, state, observed, &resolved)
                .await;
        }

        let common_revision = state.common_revision();
        let loaded_base = if observed.revision() == common_revision {
            None
        } else {
            Some(self.volume.namespace(common_revision).await?)
        };
        let base = loaded_base.as_ref().unwrap_or(&observed.namespace);
        let local = scan(
            &root,
            base,
            self.transfer_concurrency,
            self.volume.workset_options(),
        )
        .await?;
        let remote_changed = observed.revision() != common_revision;
        match (local, remote_changed) {
            (ScannedTree::Unchanged, false) => Ok(SyncOutcome {
                conflict_paths: Vec::new(),
                published: false,
                sequence: base.cursor.sequence(),
            }),
            (ScannedTree::Changed(local), false) => {
                if !resolved.is_empty() {
                    return Err(Error::invalid(
                        "synchronize replica",
                        "--resolve requires a current local and remote conflict",
                    ));
                }
                let target = self.publish_files(&root, &observed, &local).await?;
                let revision = self
                    .prepare_and_commit(state_path, &mut state, &observed, &target)
                    .await?;
                state.advance(revision);
                state.save(state_path)?;
                Ok(SyncOutcome {
                    conflict_paths: Vec::new(),
                    published: true,
                    sequence: revision.cursor().sequence(),
                })
            }
            (ScannedTree::Unchanged, true) => {
                if !resolved.is_empty() {
                    return Err(Error::invalid(
                        "synchronize replica",
                        "--resolve requires a current local and remote conflict",
                    ));
                }
                self.install_and_advance(
                    &root,
                    state_path,
                    state,
                    &observed.namespace,
                    observed.revision(),
                    Some(base),
                    false,
                )
                .await
            }
            (ScannedTree::Changed(local), true) => {
                let plan = reconcile(
                    base,
                    &local,
                    &observed.namespace,
                    &resolved,
                    self.volume.workset_options(),
                )?;
                if !plan.conflicts.is_empty() {
                    let conflict_paths = plan
                        .conflicts
                        .into_iter()
                        .map(|conflict| conflict.path)
                        .collect::<Vec<_>>();
                    state.retain_conflicts(conflict_paths.len(), observed.revision(), false);
                    state.save(state_path)?;
                    return Ok(SyncOutcome {
                        conflict_paths,
                        published: false,
                        sequence: base.cursor.sequence(),
                    });
                }
                let (target, revision) = if plan.publish {
                    let target = self.publish_files(&root, &observed, &plan.target).await?;
                    let revision = self
                        .prepare_and_commit(state_path, &mut state, &observed, &target)
                        .await?;
                    (target, revision)
                } else {
                    (observed.namespace.clone(), observed.revision())
                };
                self.install_and_advance(
                    &root,
                    state_path,
                    state,
                    &target,
                    revision,
                    Some(&local),
                    plan.publish,
                )
                .await
            }
        }
    }

    async fn prepare_and_commit(
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

    async fn recover_install(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        observed: ManagedObservation,
        target: NamespaceRevision,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        if !observed.can_read_revision(target) {
            state.begin_install(observed.revision(), false);
            state.save(state_path)?;
            repair(
                root,
                &observed.namespace,
                &self.volume,
                self.transfer_concurrency,
            )
            .await?;
            state.advance(observed.revision());
            state.save(state_path)?;
            return Ok(SyncOutcome {
                conflict_paths: Vec::new(),
                published: false,
                sequence: observed.namespace.cursor.sequence(),
            });
        }
        let loaded = if observed.revision() == target {
            None
        } else {
            Some(self.volume.namespace(target).await?)
        };
        let target_namespace = loaded.as_ref().unwrap_or(&observed.namespace);
        state.begin_install(target, published);
        state.save(state_path)?;
        repair(
            root,
            target_namespace,
            &self.volume,
            self.transfer_concurrency,
        )
        .await?;
        state.advance(target);
        state.save(state_path)?;
        if observed.revision() != target {
            return self
                .install_and_advance(
                    root,
                    state_path,
                    state,
                    &observed.namespace,
                    observed.revision(),
                    Some(target_namespace),
                    published,
                )
                .await;
        }
        Ok(SyncOutcome {
            conflict_paths: Vec::new(),
            published,
            sequence: target.cursor().sequence(),
        })
    }

    async fn conservative_rebase(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        observed: ManagedObservation,
        resolved: &BTreeSet<String>,
    ) -> Result<SyncOutcome, Error> {
        let local = match scan(
            root,
            &observed.namespace,
            self.transfer_concurrency,
            self.volume.workset_options(),
        )
        .await?
        {
            ScannedTree::Unchanged => {
                state.advance(observed.revision());
                state.save(state_path)?;
                return Ok(SyncOutcome {
                    conflict_paths: Vec::new(),
                    published: false,
                    sequence: observed.namespace.cursor.sequence(),
                });
            }
            ScannedTree::Changed(namespace) => namespace,
        };
        let ambiguous = changed_paths(&observed.namespace, &local)?;
        if ambiguous.is_empty() {
            state.advance(observed.revision());
            state.save(state_path)?;
            return Ok(SyncOutcome {
                conflict_paths: Vec::new(),
                published: false,
                sequence: observed.namespace.cursor.sequence(),
            });
        }
        if resolved != &ambiguous {
            let conflict_paths = ambiguous.into_iter().collect::<Vec<_>>();
            state.retain_conflicts(conflict_paths.len(), observed.revision(), true);
            state.save(state_path)?;
            return Ok(SyncOutcome {
                conflict_paths,
                published: false,
                sequence: state.common_revision().cursor().sequence(),
            });
        }
        let target = self.publish_files(root, &observed, &local).await?;
        let revision = self
            .prepare_and_commit(state_path, &mut state, &observed, &target)
            .await?;
        state.advance(revision);
        state.save(state_path)?;
        Ok(SyncOutcome {
            conflict_paths: Vec::new(),
            published: true,
            sequence: revision.cursor().sequence(),
        })
    }

    async fn install_and_advance<C: DeserializeOwned>(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        target: &Namespace<StreamRef>,
        target_revision: NamespaceRevision,
        current: Option<&Namespace<C>>,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        state.begin_install(target_revision, published);
        state.save(state_path)?;
        install(
            root,
            current,
            target,
            &self.volume,
            self.transfer_concurrency,
        )
        .await?;
        state.advance(target_revision);
        state.save(state_path)?;
        Ok(SyncOutcome {
            conflict_paths: Vec::new(),
            published,
            sequence: target_revision.cursor().sequence(),
        })
    }

    async fn recover_pending(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        observed: ManagedObservation,
    ) -> Result<SyncOutcome, Error> {
        let (expected, target, operation, maintenance_generation) = state
            .pending_publication()
            .expect("pending state has publication references");
        if observed.revision() == target
            || self
                .volume
                .operation_committed(operation, &observed)
                .await?
        {
            return self
                .recover_install(root, state_path, state, observed, target, true)
                .await;
        }
        if !observed.accepts_prepared(maintenance_generation) {
            state.cancel_pending(observed.revision());
            state.save(state_path)?;
            return Err(Error::invalid(
                "synchronize replica",
                "pending publication was invalidated by data collection; repeat sync to prepare it again",
            ));
        }
        if observed.revision() != expected {
            state.cancel_pending(observed.revision());
            state.save(state_path)?;
            return Err(Error::invalid(
                "synchronize replica",
                "pending publication conflicted with a newer remote change; repeat sync to reconcile",
            ));
        }
        crate::fault::check("before-publish")?;
        self.volume
            .commit_publication(&observed, target, operation)
            .await?;
        crate::fault::check("after-publish")?;
        let committed = self.volume.observe().await?;
        self.recover_install(root, state_path, state, committed, target, true)
            .await
    }

    async fn publish_files(
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

fn require_empty(root: &Path) -> Result<(), Error> {
    if !directory_is_empty(root)? {
        return Err(Error::invalid(
            "synchronize replica",
            "a replica without state must use an empty local directory",
        ));
    }
    Ok(())
}

fn directory_is_empty(root: &Path) -> Result<bool, Error> {
    let mut entries = std::fs::read_dir(root)
        .map_err(|error| Error::from_io("read replica directory", Some(root), error))?;
    Ok(entries.next().is_none())
}
