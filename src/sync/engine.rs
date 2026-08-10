// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Volume-independent Sync orchestration.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::path::Path;

use super::install::{apply_target, fresh_sibling, install_staged_changes, remove_tree};
use super::path::SnapshotTree;
use super::{ConflictRecord, LocalTree, ReplicaState, StagedTree, build_publication, reconcile};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, FileVersionId, NodeKind, OperationId, Volume, VolumeObservation,
};
use anyhow::{Context, Result, bail};

#[derive(Clone, Debug)]
pub struct SyncResult {
    pub common: ChangeCursor,
    pub conflicts: Vec<ConflictRecord>,
    pub pending: bool,
    pub published: bool,
}

#[derive(Clone)]
pub struct SyncEngine<V> {
    volume: V,
    transfer_concurrency: NonZeroUsize,
}

impl<V: Volume> SyncEngine<V> {
    pub fn new(volume: V, transfer_concurrency: NonZeroUsize) -> Self {
        Self {
            volume,
            transfer_concurrency,
        }
    }

    pub async fn sync(
        &self,
        replica_path: impl AsRef<Path>,
        state_path: impl AsRef<Path>,
        resolve_paths: &[String],
    ) -> Result<SyncResult> {
        super::local::require_native_capabilities()?;
        let replica_path = replica_path.as_ref();
        let state_path = state_path.as_ref();
        let requested = resolve_paths.iter().cloned().collect::<BTreeSet<_>>();
        if requested.len() != resolve_paths.len() {
            bail!("a conflict resolution path was provided more than once");
        }
        let volume_id = self.volume.id();
        let authority_identity = self.volume.authority();
        let mut state = ReplicaState::load(state_path)?
            .unwrap_or_else(|| ReplicaState::empty_for(authority_identity.clone()));
        if state.volume != volume_id {
            bail!("replica state belongs to another volume");
        }
        if state.branch != authority_identity.branch {
            bail!("replica state belongs to another branch incarnation");
        }
        tokio::fs::create_dir_all(replica_path)
            .await
            .context("create local replica")?;

        let mut resolved_commit = None;
        let prior_staging = if let Some(pending) = state.pending.as_ref() {
            let committed = match self.volume.resolve(pending.operation).await? {
                CommitOutcome::Committed(committed) => Some(committed),
                CommitOutcome::Unknown => {
                    return Ok(result(&state, false));
                }
                CommitOutcome::Absent | CommitOutcome::Conflict { .. } => None,
            };
            let staged = match StagedTree::recover(pending) {
                Ok(staged)
                    if staged.matches_source_observation(&LocalTree::scan(replica_path).await?) =>
                {
                    Some(staged)
                }
                _ => None,
            };
            let staging_path = pending.staging.clone();
            if staged.is_none() {
                let _ = remove_tree(&staging_path);
                if committed.is_none() {
                    state.pending = None;
                    state.install(state_path)?;
                }
            }
            if let Some(committed) = committed {
                state.pending = None;
                resolved_commit = Some(committed);
            }
            staged
        } else {
            None
        };
        let observed = self.volume.observe_from(state.authority.as_ref()).await?;
        let remote = observed.as_ref().map(|value| value.snapshot());
        if remote.is_none() && state.common() != ChangeCursor::Genesis {
            bail!("authoritative namespace disappeared after this replica was initialized");
        }
        if let Some(committed) = resolved_commit {
            let remote = remote.context("committed publication has no authoritative namespace")?;
            if remote.cursor.sequence() < committed.sequence()
                || remote.cursor.sequence() == committed.sequence() && remote.cursor != committed
            {
                bail!("authoritative namespace is behind the committed publication");
            }
        }
        let base = state
            .authority
            .as_ref()
            .map(SnapshotTree::new)
            .transpose()?;
        let remote_tree = remote.map(SnapshotTree::new).transpose()?;

        let (local, staging_path, mut staged) = match prior_staging {
            Some(staged) => (staged.local_tree(), staged.staging.clone(), staged),
            None => {
                let local = LocalTree::scan(replica_path).await?;
                if resolve_paths.is_empty()
                    && state.conflicts.is_empty()
                    && let Some(tree) = remote_tree.as_ref()
                    && tree.snapshot.cursor == state.common()
                    && local.entries == state.installed
                {
                    return Ok(result(&state, false));
                }
                let staging_path = fresh_sibling(state_path, "publish");
                let known_versions = known_local_versions(&local, &state, base.as_ref());
                let staged = StagedTree::prepare_for_publish(
                    &local,
                    &staging_path,
                    &known_versions,
                    base.as_ref().map(|tree| tree.snapshot),
                    &self.volume,
                    remote,
                    self.transfer_concurrency,
                )
                .await?;
                (local, staging_path, staged)
            }
        };
        let operation = OperationId::generate();
        let mut publish = remote.is_none() && !local.entries.is_empty();
        let mut conflicts = Vec::new();
        let mut local_renames = BTreeMap::new();
        let mut target_update = None;
        let mut resolved = BTreeSet::new();

        if let Some(remote_tree) = remote_tree.as_ref() {
            let mut plan = reconcile(&state, &staged, base.as_ref(), remote_tree)?;
            publish |= plan.publish;
            local_renames = std::mem::take(&mut plan.renames);
            for conflict in std::mem::take(&mut plan.conflicts) {
                if requested.contains(&conflict.path) {
                    resolved.insert(conflict.path.clone());
                    publish = true;
                } else {
                    conflicts.push(conflict);
                }
            }
            target_update = Some(plan);
        }
        if resolved != requested {
            let missing = requested.difference(&resolved).collect::<Vec<_>>();
            bail!("no unresolved conflict exists for {missing:?}");
        }
        if !conflicts.is_empty() {
            state.conflicts = conflicts;
            state.pending = None;
            state.install(state_path)?;
            remove_tree(&staging_path)?;
            return Ok(result(&state, false));
        }

        if target_update.is_some() || publish {
            let pending = staged.pending(operation, local_renames.clone());
            let changed = state.pending.as_ref() != Some(&pending) || !state.conflicts.is_empty();
            state.pending = Some(pending);
            state.conflicts.clear();
            if changed {
                state.install(state_path)?;
            }
        }

        if publish {
            let segment_staging = staged.segment_operator()?;
            self.volume
                .finalize_staged_files(&segment_staging, self.transfer_concurrency)
                .await?;
        }

        if let Some(plan) = target_update {
            apply_target(
                &self.volume,
                &mut staged,
                replica_path,
                remote,
                plan,
                self.transfer_concurrency,
            )
            .await?;
        }

        if !publish {
            state.conflicts.clear();
            if let Some(remote_tree) = remote_tree.as_ref() {
                let remote_advanced = remote_tree.snapshot.cursor != state.common();
                if remote_advanced && matching_local(replica_path, &local).await?.is_none() {
                    bail!("local replica changed while remote state was being installed");
                }
                if remote_advanced {
                    install_staged_changes(replica_path, &staged, &staged.source)?;
                }
                let installed = LocalTree::scan(replica_path).await?;
                state = ReplicaState::at_common(
                    state.authority_identity(),
                    remote_tree,
                    installed.entries,
                )?;
            } else {
                state.pending = None;
            }
            state.install(state_path)?;
            remove_tree(&staging_path)?;
            return Ok(result(&state, resolved_commit.is_some()));
        }

        let requires_materialization = !staged.source.same_content(staged.manifest());

        let publication = build_publication(
            &self.volume,
            operation,
            remote_tree.as_ref(),
            &staged,
            &local_renames,
        )?;
        match self.volume.publish(observed.as_ref(), &publication).await? {
            CommitOutcome::Committed(committed) if committed == publication.target.cursor => {
                let Some(observed_local) = matching_local(replica_path, &local).await? else {
                    return Ok(result(&state, false));
                };
                if requires_materialization {
                    install_staged_changes(replica_path, &staged, &staged.source)?;
                }
                let committed = SnapshotTree::new(&publication.target)?;
                let installed = if requires_materialization {
                    LocalTree::scan(replica_path).await?
                } else {
                    observed_local
                };
                state = ReplicaState::at_common(
                    state.authority_identity(),
                    &committed,
                    installed.entries,
                )?;
                state.install(state_path)?;
                remove_tree(&staging_path)?;
                Ok(result(&state, true))
            }
            CommitOutcome::Absent | CommitOutcome::Conflict { .. } | CommitOutcome::Unknown => {
                Ok(result(&state, false))
            }
            CommitOutcome::Committed(_) => {
                bail!("volume returned a commit cursor that does not match the publication")
            }
        }
    }
}

fn result(state: &ReplicaState, published: bool) -> SyncResult {
    SyncResult {
        common: state.common(),
        conflicts: state.conflicts.clone(),
        pending: state.pending.is_some(),
        published,
    }
}

async fn matching_local(root: &Path, expected: &LocalTree) -> Result<Option<LocalTree>> {
    let observed = LocalTree::scan(root).await?;
    Ok((observed.entries == expected.entries).then_some(observed))
}

fn known_local_versions<'a>(
    local: &'a LocalTree,
    state: &ReplicaState,
    tree: Option<&SnapshotTree<'_>>,
) -> BTreeMap<&'a str, FileVersionId> {
    let Some(tree) = tree else {
        return BTreeMap::new();
    };
    local
        .entries
        .iter()
        .filter_map(|(path, entry)| {
            let base = state.installed.get(path)?;
            if entry.kind == NodeKind::RegularFile && base == entry {
                let version = tree.get(path)?.file?;
                Some((path.as_str(), version.id))
            } else {
                None
            }
        })
        .collect()
}
