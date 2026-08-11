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

use crate::filesystem::{ChangeCursor, NodeKind, VolumeSnapshot};
use crate::managed::{ManagedObservation, ManagedVolume, NamespaceRevision};

use super::install::{install, repair};
use super::reconcile::{changed_paths, reconcile};
use super::scan::scan;
use super::{ReplicaState, SyncError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncOutcome {
    pub conflicts: usize,
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
    ) -> Result<SyncOutcome, SyncError> {
        let resolved = resolve_paths.iter().cloned().collect::<BTreeSet<_>>();
        if resolved.len() != resolve_paths.len() {
            return Err(SyncError::new(
                "a conflict resolution path was provided more than once",
            ));
        }
        let root = std::fs::canonicalize(root)
            .map_err(|error| SyncError::io("open replica directory", error))?;
        if !root.is_dir() {
            return Err(SyncError::new("replica path is not a directory"));
        }

        let observed = self.volume.observe().await?;
        let stored = ReplicaState::load(state_path)?;
        if let Some(state) = &stored {
            if state.root() != root {
                return Err(SyncError::new(
                    "replica state belongs to a different local directory",
                ));
            }
            if state.volume_id() != self.volume.id() {
                return Err(SyncError::new(
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
                    return Err(SyncError::new(
                        "--resolve requires an unresolved conflict in replica state",
                    ));
                }
                let state = ReplicaState::new(root.clone(), self.volume.id(), observed.revision());
                state.save_new(state_path)?;
                state
            }
            None => {
                if !resolved.is_empty() {
                    return Err(SyncError::new(
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
                    conflicts: 0,
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

        if !observed.retains(state.common_revision()) {
            return self
                .conservative_rebase(&root, state_path, state, observed, &resolved)
                .await;
        }
        let base = self.volume.snapshot(state.common_revision()).await?;
        let local = scan(&root, &base, &self.volume).await?;
        let local_changed = local.snapshot.cursor != base.cursor;
        let remote_changed = observed.revision() != state.common_revision();
        match (local_changed, remote_changed) {
            (false, false) => Ok(SyncOutcome {
                conflicts: 0,
                published: false,
                sequence: base.cursor.sequence(),
            }),
            (true, false) => {
                if !resolved.is_empty() {
                    return Err(SyncError::new(
                        "--resolve requires a current local and remote conflict",
                    ));
                }
                self.publish_target_files(&root, &observed.snapshot, &local.snapshot)
                    .await?;
                let target = self
                    .volume
                    .prepare_publication(&observed, local.snapshot)
                    .await?;
                state.begin_publication(observed.revision(), target)?;
                state.save(state_path)?;
                test_interrupt("before-publish")?;
                self.volume.commit_publication(&observed, target).await?;
                test_interrupt("after-publish")?;
                state.advance(target);
                state.save(state_path)?;
                Ok(SyncOutcome {
                    conflicts: 0,
                    published: true,
                    sequence: target.cursor().sequence(),
                })
            }
            (false, true) => {
                if !resolved.is_empty() {
                    return Err(SyncError::new(
                        "--resolve requires a current local and remote conflict",
                    ));
                }
                self.install_and_advance(
                    &root,
                    state_path,
                    state,
                    &observed.snapshot,
                    observed.revision(),
                    Some(&base),
                    false,
                )
                .await
            }
            (true, true) => {
                let plan = reconcile(&base, &local.snapshot, &observed.snapshot, &resolved)?;
                if !plan.conflicts.is_empty() {
                    let conflicts = plan.conflicts.len();
                    state.retain_conflicts(conflicts, observed.revision(), false);
                    state.save(state_path)?;
                    return Ok(SyncOutcome {
                        conflicts,
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
                    state.begin_publication(observed.revision(), target)?;
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
                    Some(&local.snapshot),
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
    ) -> Result<SyncOutcome, SyncError> {
        if !observed.retains(target) {
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
                conflicts: 0,
                published: false,
                sequence: observed.snapshot.cursor.sequence(),
            });
        }
        let snapshot = self.volume.snapshot(target).await?;
        state.begin_install(target, published);
        state.save(state_path)?;
        repair(root, &snapshot, &self.volume, self.transfer_concurrency).await?;
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
                    Some(&snapshot),
                    published,
                )
                .await;
        }
        Ok(SyncOutcome {
            conflicts: 0,
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
    ) -> Result<SyncOutcome, SyncError> {
        let local = scan(root, &observed.snapshot, &self.volume).await?;
        let ambiguous = changed_paths(&observed.snapshot, &local.snapshot)?;
        if ambiguous.is_empty() {
            state.advance(observed.revision());
            state.save(state_path)?;
            return Ok(SyncOutcome {
                conflicts: 0,
                published: false,
                sequence: observed.snapshot.cursor.sequence(),
            });
        }
        if resolved != &ambiguous {
            let conflicts = ambiguous.len();
            state.retain_conflicts(conflicts, observed.revision(), true);
            state.save(state_path)?;
            return Ok(SyncOutcome {
                conflicts,
                published: false,
                sequence: state.common_revision().cursor().sequence(),
            });
        }

        self.publish_target_files(root, &observed.snapshot, &local.snapshot)
            .await?;
        let target = self
            .volume
            .prepare_publication(&observed, local.snapshot)
            .await?;
        state.begin_publication(observed.revision(), target)?;
        state.save(state_path)?;
        test_interrupt("before-publish")?;
        self.volume.commit_publication(&observed, target).await?;
        test_interrupt("after-publish")?;
        state.advance(target);
        state.save(state_path)?;
        Ok(SyncOutcome {
            conflicts: 0,
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
    ) -> Result<SyncOutcome, SyncError> {
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
            conflicts: 0,
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
    ) -> Result<SyncOutcome, SyncError> {
        let (expected, target) = state
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
        if !observed.accepts_prepared(target) {
            state.cancel_pending(observed.revision());
            state.save(state_path)?;
            return Err(SyncError::new(
                "pending publication was invalidated by data collection; repeat sync to prepare it again",
            ));
        }
        if observed.revision() != expected {
            state.cancel_pending(observed.revision());
            state.save(state_path)?;
            return Err(SyncError::new(
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
    ) -> Result<(), SyncError> {
        futures::stream::iter(target.paths()?)
            .map(Ok::<_, SyncError>)
            .try_for_each_concurrent(self.transfer_concurrency, |(path, node_id)| async move {
                let node = &target.nodes[&node_id];
                if node.kind != NodeKind::RegularFile {
                    return Ok(());
                }
                let version = node
                    .file_version
                    .and_then(|id| target.file_versions.get(&id))
                    .ok_or_else(|| SyncError::new("pending file has no file version"))?;
                if common.file_versions.get(&version.id) == Some(version) {
                    return Ok(());
                }
                self.volume.publish_file(&root.join(path), version).await?;
                Ok(())
            })
            .await
    }
}

#[cfg(debug_assertions)]
fn test_interrupt(point: &str) -> Result<(), SyncError> {
    if std::env::var("OFS_INTERNAL_TEST_INTERRUPT").as_deref() == Ok(point) {
        return Err(SyncError::new(
            "internal test interrupted replica synchronization",
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
const fn test_interrupt(_point: &str) -> Result<(), SyncError> {
    Ok(())
}

fn require_empty(root: &Path) -> Result<(), SyncError> {
    if !directory_is_empty(root)? {
        return Err(SyncError::new(
            "a replica without state must use an empty local directory",
        ));
    }
    Ok(())
}

fn directory_is_empty(root: &Path) -> Result<bool, SyncError> {
    let mut entries =
        std::fs::read_dir(root).map_err(|error| SyncError::io("read replica directory", error))?;
    Ok(entries.next().is_none())
}
