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

//! Recovery paths for interrupted or no-longer-readable Sync state.

use std::collections::BTreeSet;
use std::path::Path;

use crate::Error;
use crate::managed::{ManagedObservation, NamespaceRevision};

use super::ReplicaState;
use super::engine::{SyncEngine, SyncOutcome};
use super::install::repair;
use super::reconcile::changed_paths;
use super::scan::{ScannedTree, scan};

impl SyncEngine {
    pub(super) async fn recover_install(
        &self,
        root: &Path,
        state_path: &Path,
        mut state: ReplicaState,
        observed: ManagedObservation,
        target: NamespaceRevision,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        let published = published && observed.can_read_revision(target);
        let current = observed.revision();
        state.begin_install(current, published);
        super::state_file::persist(&state, state_path, true)?;
        repair(
            root,
            &observed.namespace,
            &self.volume,
            self.transfer_concurrency,
        )
        .await?;
        state.advance(current);
        super::state_file::persist(&state, state_path, true)?;
        Ok(SyncOutcome {
            conflict_paths: Vec::new(),
            published,
            sequence: current.cursor().sequence(),
        })
    }

    pub(super) async fn recover_pending(
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
                .operation_committed(operation, target.cursor(), &observed)
                .await?
        {
            return self
                .recover_install(root, state_path, state, observed, target, true)
                .await;
        }
        if !observed.accepts_prepared(maintenance_generation) {
            state.cancel_pending(observed.revision());
            super::state_file::persist(&state, state_path, true)?;
            return Err(Error::invalid(
                "synchronize replica",
                "pending publication was invalidated by data collection; repeat sync to prepare it again",
            ));
        }
        if observed.revision() != expected {
            state.cancel_pending(observed.revision());
            super::state_file::persist(&state, state_path, true)?;
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

    pub(super) async fn conservative_rebase(
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
                super::state_file::persist(&state, state_path, true)?;
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
            super::state_file::persist(&state, state_path, true)?;
            return Ok(SyncOutcome {
                conflict_paths: Vec::new(),
                published: false,
                sequence: observed.namespace.cursor.sequence(),
            });
        }
        if resolved != &ambiguous {
            let conflict_paths = ambiguous.into_iter().collect::<Vec<_>>();
            state.retain_conflicts(conflict_paths.len(), observed.revision(), true);
            super::state_file::persist(&state, state_path, true)?;
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
        super::state_file::persist(&state, state_path, true)?;
        Ok(SyncOutcome {
            conflict_paths: Vec::new(),
            published: true,
            sequence: revision.cursor().sequence(),
        })
    }
}
