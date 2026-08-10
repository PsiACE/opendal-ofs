// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Volume-independent Sync orchestration.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use super::local::{entry_at, fs_operator, set_executable};
use super::path::SnapshotTree;
use super::reconcile::ReconcilePlan;
use super::{
    ConflictRecord, LocalKind, LocalTree, ReplicaState, StagedTree, TargetManifest,
    build_publication, reconcile,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, FileVersion, MaterializeRequest, OperationId, Volume,
    VolumeObservation,
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
        let prior_staging = if let Some(pending) = state.pending.clone() {
            match self.volume.resolve(pending.operation).await? {
                CommitOutcome::Committed(committed) => {
                    let staged = StagedTree::recover(&pending).ok();
                    let staged = match staged {
                        Some(staged)
                            if staged.matches_source_observation(
                                &LocalTree::scan(replica_path).await?,
                            ) =>
                        {
                            Some((staged, pending.operation, pending.data_finalized))
                        }
                        _ => {
                            let _ = remove_tree(&pending.staging);
                            None
                        }
                    };
                    state.pending = None;
                    resolved_commit = Some(committed);
                    staged
                }
                CommitOutcome::Unknown => {
                    return Ok(result(&state, false));
                }
                CommitOutcome::Absent | CommitOutcome::Conflict { .. } => {
                    match StagedTree::recover(&pending) {
                        Ok(staged) => Some((staged, pending.operation, pending.data_finalized)),
                        Err(_) => {
                            state.pending = None;
                            state.install(state_path)?;
                            let _ = remove_tree(&pending.staging);
                            None
                        }
                    }
                }
            }
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

        let (local, staging_path, mut staged, operation, mut data_finalized) = match prior_staging {
            Some((staged, operation, data_finalized)) => (
                staged.local_tree(),
                staged.root().to_owned(),
                staged,
                operation,
                data_finalized,
            ),
            None => {
                let local = LocalTree::scan(replica_path).await?;
                if resolve_paths.is_empty()
                    && state.conflicts.is_empty()
                    && let Some(tree) = remote_tree.as_ref()
                    && tree.snapshot().cursor == state.common()
                    && local.entries() == &state.installed
                {
                    return Ok(result(&state, false));
                }
                let staging_path = fresh_sibling(state_path, "publish");
                let known_versions = known_local_versions(&local, &state, base.as_ref());
                let staged = StagedTree::prepare_for_publish(
                    &local,
                    &staging_path,
                    &known_versions,
                    &self.volume,
                    remote,
                    self.transfer_concurrency,
                )
                .await?;
                let operation = OperationId::generate();
                (local, staging_path, staged, operation, false)
            }
        };
        let source_manifest = staged.source().clone();
        let mut publish = remote.is_none() && !local.entries().is_empty();
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
            state.pending = Some(staged.pending(operation, data_finalized, local_renames.clone()));
            state.conflicts.clear();
            state.install(state_path)?;
        }

        if publish && !data_finalized {
            let staging = fs_operator(staged.root())?;
            self.volume
                .finalize_staged_files(&staging, staged.prepared_files()?, remote)
                .await?;
            data_finalized = true;
            state.pending = Some(staged.pending(operation, data_finalized, local_renames.clone()));
            state.install(state_path)?;
        }

        if let Some(plan) = target_update {
            apply_target(
                &self.volume,
                &mut staged,
                replica_path,
                remote,
                plan,
                state.installed.is_empty() && local.entries().is_empty(),
                self.transfer_concurrency,
            )
            .await?;
        }

        if !publish {
            state.conflicts.clear();
            if let Some(remote_tree) = remote_tree.as_ref() {
                let remote_advanced = remote_tree.snapshot().cursor != state.common();
                if remote_advanced && !matches_local(replica_path, &local).await? {
                    bail!("local replica changed while remote state was being installed");
                }
                if remote_advanced {
                    if state.installed.is_empty() && local.entries().is_empty() {
                        install_staged_tree(replica_path, &staging_path)?;
                    } else {
                        install_staged_changes(replica_path, &staged, &source_manifest)?;
                    }
                }
                state = state_from_snapshot(remote_tree, replica_path, &state).await?;
            } else {
                state.pending = None;
            }
            state.install(state_path)?;
            remove_tree(&staging_path)?;
            return Ok(result(&state, resolved_commit.is_some()));
        }

        let requires_materialization = !source_manifest.same_content(staged.manifest());

        let publication = build_publication(
            &self.volume,
            operation,
            remote_tree.as_ref(),
            &staged,
            &local_renames,
        )?;
        state.pending = Some(staged.pending(operation, data_finalized, local_renames));
        state.conflicts.clear();
        state.install(state_path)?;
        match self.volume.publish(observed.as_ref(), &publication).await? {
            CommitOutcome::Committed(committed) if committed == publication.target.cursor => {
                let unchanged = matches_local(replica_path, &local).await?;
                if !unchanged {
                    return Ok(result(&state, false));
                }
                if requires_materialization {
                    install_staged_changes(replica_path, &staged, &source_manifest)?;
                }
                let committed = SnapshotTree::new(&publication.target)?;
                state = state_from_snapshot(&committed, replica_path, &state).await?;
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

async fn matches_local(root: &Path, expected: &LocalTree) -> Result<bool> {
    Ok(LocalTree::scan(root).await?.entries() == expected.entries())
}

async fn apply_target<V: Volume>(
    volume: &V,
    staged: &mut StagedTree,
    source_root: &Path,
    authority: Option<&crate::filesystem::VolumeSnapshot>,
    plan: ReconcilePlan,
    full_tree: bool,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    let ReconcilePlan {
        target: manifest,
        mut materialize,
        reuse,
        refresh,
        ..
    } = plan;
    let root = staged.root().to_owned();
    let target = fs_operator(&root)?;
    let removed = staged
        .manifest()
        .entries()
        .iter()
        .rev()
        .filter(|(path, entry)| {
            manifest
                .entries()
                .get(*path)
                .is_none_or(|desired| desired.local.kind != entry.local.kind)
        })
        .map(|(path, entry)| (path.clone(), entry.local.kind))
        .collect::<Vec<_>>();
    for (path, kind) in removed {
        let target_path = match kind {
            LocalKind::Directory => format!("{path}/"),
            LocalKind::File => path,
        };
        target.delete(&target_path).await?;
    }
    for path in &refresh {
        if manifest
            .entries()
            .get(path)
            .is_some_and(|entry| entry.local.kind == LocalKind::Directory)
        {
            target.create_dir(&format!("{path}/")).await?;
        }
    }

    for (path, source) in reuse {
        if !reuse_local_file(
            source_root,
            &root,
            staged.source(),
            &manifest,
            &source,
            &path,
        )
        .await?
        {
            materialize.insert(path);
        }
    }

    let requests = materialize
        .iter()
        .filter(|path| {
            manifest
                .file(path)
                .is_some_and(|file| !staged.cached(path, file.id))
        })
        .map(|path| -> Result<_> {
            let file = manifest
                .file(path)
                .with_context(|| format!("materialization path {path:?} is not a target file"))?;
            Ok(MaterializeRequest {
                path: path.clone(),
                version: staged.resolve_version(file, authority)?.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    volume
        .materialize(&target, requests, full_tree, transfer_concurrency)
        .await?;
    for path in &materialize {
        let executable = manifest
            .entries()
            .get(path)
            .with_context(|| format!("materialization path {path:?} is not in target manifest"))?
            .local
            .executable;
        set_executable(&root.join(path), executable)?;
    }
    staged.replace_manifest(manifest, &refresh).await
}

async fn reuse_local_file(
    source_root: &Path,
    staging_root: &Path,
    source: &TargetManifest,
    target: &TargetManifest,
    source_path: &str,
    target_path: &str,
) -> Result<bool> {
    let Some(source_entry) = source.entries().get(source_path) else {
        return Ok(false);
    };
    let (Some(source_file), Some(target_file)) =
        (source.file(source_path), target.file(target_path))
    else {
        return Ok(false);
    };
    if source_file.logical_digest != target_file.logical_digest
        || source_entry.local.size != target_file.logical_size
    {
        return Ok(false);
    }
    let Ok(observed) = entry_at(source_root, source_path).await else {
        return Ok(false);
    };
    if observed != source_entry.local {
        return Ok(false);
    }

    let destination = staging_root.join(target_path);
    tokio::fs::create_dir_all(destination.parent().unwrap_or(staging_root)).await?;
    let copied = match tokio::fs::copy(source_root.join(source_path), &destination).await {
        Ok(copied) => copied,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("reuse local file for remote rename"),
    };
    let source_unchanged = entry_at(source_root, source_path)
        .await
        .is_ok_and(|observed| observed == source_entry.local);
    if copied != target_file.logical_size || !source_unchanged {
        let _ = tokio::fs::remove_file(destination).await;
        return Ok(false);
    }
    set_executable(&destination, target.entries()[target_path].local.executable)?;
    Ok(true)
}

async fn state_from_snapshot(
    tree: &SnapshotTree<'_>,
    replica: &Path,
    previous: &ReplicaState,
) -> Result<ReplicaState> {
    let local = LocalTree::scan(replica).await?;
    ReplicaState::at_common(
        previous.authority_identity(),
        tree.snapshot().clone(),
        local.entries().clone(),
    )
}

fn known_local_versions(
    local: &LocalTree,
    state: &ReplicaState,
    tree: Option<&SnapshotTree<'_>>,
) -> BTreeMap<String, FileVersion> {
    let Some(tree) = tree else {
        return BTreeMap::new();
    };
    local
        .entries()
        .iter()
        .filter_map(|(path, entry)| {
            let base = state.installed.get(path)?;
            if entry.kind == LocalKind::File && base == entry {
                let version = tree.get(path)?.file?;
                Some((path.clone(), version.clone()))
            } else {
                None
            }
        })
        .collect()
}

fn install_staged_changes(
    replica: &Path,
    staged: &StagedTree,
    before: &TargetManifest,
) -> Result<()> {
    let removals = before
        .entries()
        .iter()
        .rev()
        .filter(|(path, entry)| {
            staged
                .manifest()
                .entries()
                .get(*path)
                .is_none_or(|desired| desired.local.kind != entry.local.kind)
        })
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    for path in removals {
        let target = replica.join(path);
        match fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&target)?,
            Ok(_) => fs::remove_file(&target)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("inspect obsolete replica path"),
        }
        sync_parent(&target)?;
    }

    for (path, entry) in staged.manifest().entries() {
        if entry.local.kind == LocalKind::Directory {
            fs::create_dir_all(replica.join(path))?;
        }
    }
    for (path, entry) in staged.manifest().entries() {
        if entry.local.kind != LocalKind::File {
            continue;
        }
        let same_content = before.entries().get(path).is_some_and(|existing| {
            existing.local.kind == LocalKind::File
                && existing.local.size == entry.local.size
                && before.file(path).map(|file| file.logical_digest)
                    == staged.manifest().file(path).map(|file| file.logical_digest)
        });
        let destination = replica.join(path);
        if !same_content {
            let source = staged
                .content_path(
                    path,
                    staged
                        .manifest()
                        .file(path)
                        .expect("a target file has a file version")
                        .id,
                )
                .with_context(|| format!("changed path {path:?} has no durable staged content"))?;
            let parent = destination.parent().unwrap_or(replica);
            fs::create_dir_all(parent)?;
            let temporary = parent.join(format!(".ofs-install-{}", uuid::Uuid::new_v4()));
            let result = (|| -> Result<()> {
                let copied = fs::copy(&source, &temporary)?;
                if copied != entry.local.size {
                    bail!("staged path {path:?} returned a short copy")
                }
                set_executable(&temporary, entry.local.executable)?;
                fs::File::open(&temporary)?.sync_all()?;
                fs::rename(&temporary, &destination)?;
                sync_parent(&destination)
            })();
            if result.is_err() {
                let _ = fs::remove_file(&temporary);
            }
            result.with_context(|| format!("install staged path {path:?}"))?;
        } else {
            set_executable(&destination, entry.local.executable)?;
        }
    }
    Ok(())
}

fn install_staged_tree(replica: &Path, staging: &Path) -> Result<()> {
    let backup = fresh_sibling(replica, "backup");
    let existed = replica.exists();
    if existed {
        fs::rename(replica, &backup).context("move prior replica aside")?;
    }
    if let Err(error) = fs::rename(staging, replica) {
        if existed {
            let _ = fs::rename(&backup, replica);
        }
        return Err(error).context("install staged replica tree");
    }
    sync_parent(replica)?;
    if existed {
        fs::remove_dir_all(backup).context("remove replaced replica tree")?;
    }
    Ok(())
}

fn fresh_sibling(path: &Path, purpose: &str) -> PathBuf {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("replica");
    parent.join(format!(".{name}.ofs-{purpose}-{}", uuid::Uuid::new_v4()))
}

fn remove_tree(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path).context("remove completed sync staging")?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    fs::File::open(parent)?.sync_all().map_err(Into::into)
}

#[cfg(not(unix))]
fn sync_parent(_: &Path) -> Result<()> {
    Ok(())
}
