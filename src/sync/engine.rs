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

use super::install::install;
use super::reconcile::reconcile;
use super::scan::scan;
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
        let Some(mut state) = ReplicaState::load(state_path)? else {
            if !resolved.is_empty() {
                return Err(SyncError::new(
                    "--resolve requires an unresolved conflict in replica state",
                ));
            }
            require_empty(&root)?;
            install(&root, None, &observed.snapshot, &self.volume).await?;
            ReplicaState::new(root, observed.snapshot.clone())?.save_new(state_path)?;
            return Ok(SyncOutcome {
                conflicts: 0,
                published: false,
                sequence: observed.snapshot.cursor.sequence(),
            });
        };
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

        if state.has_pending() {
            return self
                .recover_pending(&root, state_path, state, observed)
                .await;
        }

        let local = scan(&root, state.common(), &self.volume).await?;
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
                state.advance(local.snapshot);
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
                install(
                    &root,
                    Some(state.common()),
                    &observed.snapshot,
                    &self.volume,
                )
                .await?;
                state.advance(observed.snapshot);
                state.save(state_path)?;
                Ok(SyncOutcome {
                    conflicts: 0,
                    published: false,
                    sequence: state.common().cursor.sequence(),
                })
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
                    state.retain_conflicts(plan.conflicts);
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
                }
                install(&root, Some(&local.snapshot), &plan.target, &self.volume).await?;
                state.advance(plan.target);
                state.save(state_path)?;
                Ok(SyncOutcome {
                    conflicts: 0,
                    published: plan.publish,
                    sequence: state.common().cursor.sequence(),
                })
            }
        }
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
            install(root, Some(state.common()), &target, &self.volume).await?;
            state.advance(target);
            state.save(state_path)?;
            return Ok(SyncOutcome {
                conflicts: 0,
                published: true,
                sequence: state.common().cursor.sequence(),
            });
        }
        if observed.snapshot.cursor != expected.cursor {
            return Err(SyncError::new(
                "pending publication outcome is unknown after the remote volume advanced",
            ));
        }
        self.publish_target_files(root, &expected, &target).await?;
        self.volume.publish(&observed, target.clone()).await?;
        install(root, Some(state.common()), &target, &self.volume).await?;
        state.advance(target);
        state.save(state_path)?;
        Ok(SyncOutcome {
            conflicts: 0,
            published: true,
            sequence: state.common().cursor.sequence(),
        })
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

fn require_empty(root: &Path) -> Result<(), SyncError> {
    let mut entries = std::fs::read_dir(root)
        .map_err(|error| SyncError::io("read cold replica directory", error))?;
    if entries.next().is_some() {
        return Err(SyncError::new(
            "a replica without state must use an empty local directory",
        ));
    }
    Ok(())
}
