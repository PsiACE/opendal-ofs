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

use futures::{StreamExt as _, TryStreamExt as _};

use crate::Error;
use crate::filesystem::{ChangeCursor, NodeKind, VolumeSnapshot};
use crate::managed::{ManagedObservation, ManagedVolume, NamespaceRevision};

use super::ReplicaState;
use super::install::{install, repair};
use super::reconcile::{changed_paths, reconcile};
use super::scan::{ScannedTree, scan};
use super::transfer::{inspect_file, publish_file};

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
            None if observed.snapshot.cursor == ChangeCursor::Genesis
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
                install(
                    &root,
                    None,
                    &observed.snapshot,
                    &self.volume,
                    self.transfer_concurrency,
                )
                .await?;
                state.advance(observed.revision());
                state.save(state_path)?;
                return Ok(SyncOutcome {
                    conflict_paths: Vec::new(),
                    published: false,
                    sequence: observed.snapshot.cursor.sequence(),
                });
            }
        };
        if state.pending_publication().is_some() {
            return self
                .recover_pending(&root, state_path, state, observed)
                .await;
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
            Some(self.volume.snapshot(common_revision).await?)
        };
        let base = loaded_base.as_ref().unwrap_or(&observed.snapshot);
        let local = scan(&root, base).await?;
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
                self.publish_target_files(&root, &observed.snapshot, &local)
                    .await?;
                let target = self.volume.prepare_publication(&observed, local).await?;
                state.begin_publication(
                    observed.revision(),
                    target,
                    observed.maintenance_generation(),
                )?;
                state.save(state_path)?;
                test_interrupt("before-publish")?;
                self.volume.commit_publication(&observed, target).await?;
                test_interrupt("after-publish")?;
                state.advance(target);
                state.save(state_path)?;
                Ok(SyncOutcome {
                    conflict_paths: Vec::new(),
                    published: true,
                    sequence: target.cursor().sequence(),
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
                    &observed.snapshot,
                    observed.revision(),
                    Some(base),
                    false,
                )
                .await
            }
            (ScannedTree::Changed(local), true) => {
                let plan = reconcile(base, &local, &observed.snapshot, &resolved)?;
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
                let target_revision = if plan.publish {
                    self.publish_target_files(&root, &observed.snapshot, &plan.target)
                        .await?;
                    let target = self
                        .volume
                        .prepare_publication(&observed, plan.target.clone())
                        .await?;
                    state.begin_publication(
                        observed.revision(),
                        target,
                        observed.maintenance_generation(),
                    )?;
                    state.save(state_path)?;
                    test_interrupt("before-publish")?;
                    self.volume.commit_publication(&observed, target).await?;
                    test_interrupt("after-publish")?;
                    target
                } else {
                    observed.revision()
                };
                self.install_and_advance(
                    &root,
                    state_path,
                    state,
                    &plan.target,
                    target_revision,
                    Some(&local),
                    plan.publish,
                )
                .await
            }
        }
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
                &observed.snapshot,
                &self.volume,
                self.transfer_concurrency,
            )
            .await?;
            state.advance(observed.revision());
            state.save(state_path)?;
            return Ok(SyncOutcome {
                conflict_paths: Vec::new(),
                published: false,
                sequence: observed.snapshot.cursor.sequence(),
            });
        }
        let loaded_snapshot = if observed.revision() == target {
            None
        } else {
            Some(self.volume.snapshot(target).await?)
        };
        let snapshot = loaded_snapshot.as_ref().unwrap_or(&observed.snapshot);
        state.begin_install(target, published);
        state.save(state_path)?;
        repair(root, snapshot, &self.volume, self.transfer_concurrency).await?;
        state.advance(target);
        state.save(state_path)?;
        if observed.revision() != target {
            return self
                .install_and_advance(
                    root,
                    state_path,
                    state,
                    &observed.snapshot,
                    observed.revision(),
                    Some(snapshot),
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
        let local = match scan(root, &observed.snapshot).await? {
            ScannedTree::Unchanged => {
                state.advance(observed.revision());
                state.save(state_path)?;
                return Ok(SyncOutcome {
                    conflict_paths: Vec::new(),
                    published: false,
                    sequence: observed.snapshot.cursor.sequence(),
                });
            }
            ScannedTree::Changed(snapshot) => snapshot,
        };
        let ambiguous = changed_paths(&observed.snapshot, &local)?;
        if ambiguous.is_empty() {
            state.advance(observed.revision());
            state.save(state_path)?;
            return Ok(SyncOutcome {
                conflict_paths: Vec::new(),
                published: false,
                sequence: observed.snapshot.cursor.sequence(),
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

        self.publish_target_files(root, &observed.snapshot, &local)
            .await?;
        let target = self.volume.prepare_publication(&observed, local).await?;
        state.begin_publication(
            observed.revision(),
            target,
            observed.maintenance_generation(),
        )?;
        state.save(state_path)?;
        test_interrupt("before-publish")?;
        self.volume.commit_publication(&observed, target).await?;
        test_interrupt("after-publish")?;
        state.advance(target);
        state.save(state_path)?;
        Ok(SyncOutcome {
            conflict_paths: Vec::new(),
            published: true,
            sequence: target.cursor().sequence(),
        })
    }

    async fn install_and_advance(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        target: &VolumeSnapshot,
        target_revision: NamespaceRevision,
        current: Option<&VolumeSnapshot>,
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
        let (expected, target, maintenance_generation) = state
            .pending_publication()
            .expect("pending state has publication references");
        if observed.revision() == target {
            return self
                .recover_install(root, state_path, state, observed, target, true)
                .await;
        }
        let operation = target
            .cursor()
            .operation()
            .expect("pending target has an operation identity");
        if self
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
        test_interrupt("before-publish")?;
        self.volume.commit_publication(&observed, target).await?;
        test_interrupt("after-publish")?;
        let committed = self.volume.observe().await?;
        self.recover_install(root, state_path, state, committed, target, true)
            .await
    }

    async fn publish_target_files(
        &self,
        root: &Path,
        common: &VolumeSnapshot,
        target: &VolumeSnapshot,
    ) -> Result<(), Error> {
        let mut new_versions = BTreeSet::new();
        let mut files = Vec::new();
        for (path, node_id) in target.paths()? {
            let node = &target.nodes[&node_id];
            if node.kind != NodeKind::RegularFile {
                continue;
            }
            let version = node
                .file_version
                .and_then(|id| target.file_versions.get(&id))
                .ok_or_else(|| {
                    Error::corrupt("publish Managed files", "pending file has no file version")
                })?;
            if common.file_versions.get(&version.id) == Some(version) {
                continue;
            }
            let publish = new_versions.insert(version.id);
            files.push((path, version, publish));
        }

        futures::stream::iter(files)
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(
                self.transfer_concurrency,
                |(path, version, publish)| async move {
                    let path = root.join(path);
                    if publish {
                        publish_file(&self.volume, &path, version).await?;
                    } else if inspect_file(&path).await? != *version {
                        return Err(Error::conflict(
                            "publish Managed files",
                            "local file changed while being published",
                        ));
                    }
                    Ok(())
                },
            )
            .await
    }
}

#[cfg(debug_assertions)]
fn test_interrupt(point: &str) -> Result<(), Error> {
    if std::env::var("OFS_INTERNAL_TEST_INTERRUPT").as_deref() == Ok(point) {
        return Err(Error::invalid(
            "synchronize replica",
            "internal test interrupted replica synchronization",
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
const fn test_interrupt(_point: &str) -> Result<(), Error> {
    Ok(())
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
