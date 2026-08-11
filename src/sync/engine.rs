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
use std::path::Path;

use crate::filesystem::{NodeKind, VolumeSnapshot};
use crate::managed::ManagedVolume;

use super::install::{install, repair};
use super::reconcile::reconcile;
use super::scan::{scan, scan_native};
use super::{ReplicaState, SyncError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncOutcome {
    pub conflicts: usize,
    pub published: bool,
    pub sequence: u64,
}

pub struct SyncEngine {
    volume: ManagedVolume,
}

impl SyncEngine {
    pub const fn new(volume: ManagedVolume) -> Self {
        Self { volume }
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
        if stored.as_ref().is_some_and(ReplicaState::is_installing) {
            return self
                .recover_install(
                    &root,
                    state_path,
                    stored.expect("checked interrupted installation state"),
                    observed,
                )
                .await;
        }
        let mut state = match stored {
            Some(state) => state,
            None if observed.snapshot.cursor == crate::filesystem::ChangeCursor::Genesis
                && !directory_is_empty(&root)? =>
            {
                if !resolved.is_empty() {
                    return Err(SyncError::new(
                        "--resolve requires an unresolved conflict in replica state",
                    ));
                }
                let state = ReplicaState::new(root.clone(), observed.snapshot.clone())?;
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
                let mut state =
                    ReplicaState::for_cold_install(root.clone(), observed.snapshot.clone())?;
                state.save_new(state_path)?;
                install(&root, None, &observed.snapshot, &self.volume).await?;
                state.advance(observed.snapshot.clone(), scan_native(&root)?)?;
                state.save(state_path)?;
                return Ok(SyncOutcome {
                    conflicts: 0,
                    published: false,
                    sequence: observed.snapshot.cursor.sequence(),
                });
            }
        };
        if state.has_pending_publication() {
            return self
                .recover_pending(&root, state_path, state, observed)
                .await;
        }

        let local = scan(&root, state.common(), state.native(), &self.volume).await?;
        let local_changed = local.snapshot.cursor != state.common().cursor;
        let remote_changed = observed.snapshot.cursor != state.common().cursor;
        match (local_changed, remote_changed) {
            (false, false) => Ok(SyncOutcome {
                conflicts: 0,
                published: false,
                sequence: state.common().cursor.sequence(),
            }),
            (true, false) => {
                if !resolved.is_empty() {
                    return Err(SyncError::new(
                        "--resolve requires a current local and remote conflict",
                    ));
                }
                state.begin(observed.snapshot.clone(), local.snapshot.clone())?;
                state.save(state_path)?;
                self.publish_target_files(&root, &observed.snapshot, &local.snapshot)
                    .await?;
                self.volume
                    .publish(&observed, local.snapshot.clone())
                    .await?;
                test_interrupt("after-publish")?;
                state.advance(local.snapshot, local.native)?;
                state.save(state_path)?;
                Ok(SyncOutcome {
                    conflicts: 0,
                    published: true,
                    sequence: state.common().cursor.sequence(),
                })
            }
            (false, true) => {
                if !resolved.is_empty() {
                    return Err(SyncError::new(
                        "--resolve requires a current local and remote conflict",
                    ));
                }
                let current = state.common().clone();
                self.install_and_advance(
                    &root,
                    state_path,
                    state,
                    &observed.snapshot,
                    Some(&current),
                    false,
                )
                .await
            }
            (true, true) => {
                let plan = reconcile(
                    state.common(),
                    &local.snapshot,
                    &observed.snapshot,
                    &resolved,
                )?;
                if !plan.conflicts.is_empty() {
                    let conflicts = plan.conflicts.len();
                    state.retain_conflicts(plan.conflicts, observed.snapshot.cursor);
                    state.save(state_path)?;
                    return Ok(SyncOutcome {
                        conflicts,
                        published: false,
                        sequence: state.common().cursor.sequence(),
                    });
                }
                if plan.publish {
                    state.begin(observed.snapshot.clone(), plan.target.clone())?;
                    state.save(state_path)?;
                    self.publish_target_files(&root, &observed.snapshot, &plan.target)
                        .await?;
                    self.volume.publish(&observed, plan.target.clone()).await?;
                    test_interrupt("after-publish")?;
                }
                self.install_and_advance(
                    &root,
                    state_path,
                    state,
                    &plan.target,
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
        observed: crate::managed::ManagedObservation,
    ) -> Result<SyncOutcome, SyncError> {
        repair(root, &observed.snapshot, &self.volume).await?;
        let published = state.has_pending_publication();
        state.advance(observed.snapshot, scan_native(root)?)?;
        state.save(state_path)?;
        Ok(SyncOutcome {
            conflicts: 0,
            published,
            sequence: state.common().cursor.sequence(),
        })
    }

    async fn install_and_advance(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        target: &VolumeSnapshot,
        current: Option<&VolumeSnapshot>,
        published: bool,
    ) -> Result<SyncOutcome, SyncError> {
        state.begin_install();
        state.save(state_path)?;
        install(root, current, target, &self.volume).await?;
        state.advance(target.clone(), scan_native(root)?)?;
        state.save(state_path)?;
        Ok(SyncOutcome {
            conflicts: 0,
            published,
            sequence: state.common().cursor.sequence(),
        })
    }

    async fn repair_and_advance(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        target: VolumeSnapshot,
    ) -> Result<SyncOutcome, SyncError> {
        state.begin_install();
        state.save(state_path)?;
        repair(root, &target, &self.volume).await?;
        state.advance(target, scan_native(root)?)?;
        state.save(state_path)?;
        Ok(SyncOutcome {
            conflicts: 0,
            published: true,
            sequence: state.common().cursor.sequence(),
        })
    }

    async fn recover_pending(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        observed: crate::managed::ManagedObservation,
    ) -> Result<SyncOutcome, SyncError> {
        let target = state
            .pending_target()
            .expect("pending state has a target")
            .clone();
        let expected = state
            .pending_expected()
            .expect("pending state has an expected snapshot")
            .clone();
        if observed.snapshot.cursor == target.cursor {
            return self
                .repair_and_advance(root, state_path, state, observed.snapshot)
                .await;
        }
        if observed.snapshot.cursor != expected.cursor {
            let operation = target
                .cursor
                .operation()
                .expect("pending target has an operation identity");
            if self
                .volume
                .operation_committed(operation, &observed)
                .await?
            {
                return self
                    .repair_and_advance(root, state_path, state, observed.snapshot)
                    .await;
            }
            state.cancel_pending(observed.snapshot.cursor);
            state.save(state_path)?;
            return Err(SyncError::new(
                "pending publication conflicted with a newer remote change; repeat sync to reconcile",
            ));
        }
        self.publish_target_files(root, &expected, &target).await?;
        self.volume.publish(&observed, target.clone()).await?;
        test_interrupt("after-publish")?;
        self.repair_and_advance(root, state_path, state, target)
            .await
    }

    async fn publish_target_files(
        &self,
        root: &Path,
        common: &VolumeSnapshot,
        target: &VolumeSnapshot,
    ) -> Result<(), SyncError> {
        for (path, node_id) in target.paths()? {
            let node = &target.nodes[&node_id];
            if node.kind != NodeKind::RegularFile {
                continue;
            }
            let version = node
                .file_version
                .and_then(|id| target.file_versions.get(&id))
                .ok_or_else(|| SyncError::new("pending file has no file version"))?;
            if common.file_versions.get(&version.id) == Some(version) {
                continue;
            }
            let observed = self.volume.inspect_file(&root.join(&path)).await?;
            if observed != *version {
                return Err(SyncError::new(
                    "local file changed after its publication was prepared",
                ));
            }
            self.volume.publish_file(&root.join(path), version).await?;
        }
        Ok(())
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
