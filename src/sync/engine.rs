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

use anyhow::{Context, Result, bail};
use opendal::Operator;

use super::local::{fs_operator, set_executable};
use super::path::{SnapshotTree, subtree};
use super::{
    ConflictRecord, LocalKind, LocalTree, RemoteEdit, ReplicaState, StagedTree, build_publication,
    reconcile,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, FileVersion, MaterializeRequest, OperationId, Volume,
    VolumeObservation,
};

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
                            Some(staged)
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
                        Ok(staged) => Some(staged),
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

        let (local, staging_path, mut staged) = match prior_staging {
            Some(staged) => (staged.logical().clone(), staged.root().to_owned(), staged),
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
                let known_digests = known_local_digests(&local, &state, base.as_ref());
                let staged = StagedTree::prepare_for_publish(
                    &local,
                    &staging_path,
                    &known_digests,
                    &self.volume,
                    remote,
                    self.transfer_concurrency,
                )
                .await?;
                (local, staging_path, staged)
            }
        };
        let source_files = staged.files().clone();
        let mut publish = remote.is_none() && !local.entries().is_empty();
        let mut install_remote = false;
        let mut conflicts = Vec::new();
        let mut local_renames = BTreeMap::new();
        let requested = resolve_paths.iter().cloned().collect::<BTreeSet<_>>();
        if requested.len() != resolve_paths.len() {
            bail!("a conflict resolution path was provided more than once");
        }
        let mut resolved = BTreeSet::new();

        if let Some(remote_tree) = remote_tree.as_ref() {
            let remote = remote_tree.snapshot();
            let plan = reconcile(&state, &staged, base.as_ref(), remote_tree)?;
            install_remote |= remote.cursor != state.common();
            let target = fs_operator(staged.root())?;
            publish |= plan.publish;
            local_renames = plan.renames;
            let mut installs = Vec::new();
            for edit in plan.edits {
                match edit {
                    RemoteEdit::InstallFile {
                        path,
                        version,
                        digest,
                        executable,
                    } => {
                        if staged
                            .logical()
                            .entries()
                            .get(&path)
                            .is_some_and(|entry| entry.kind == LocalKind::Directory)
                        {
                            remove_staged_path(&mut staged, &target, &path).await?;
                        }
                        let file = remote
                            .file_versions
                            .get(&version)
                            .context("reconciliation references a missing remote file version")?
                            .clone();
                        installs.push((path, file, digest, executable));
                    }
                    RemoteEdit::InstallDirectory { path } => {
                        if staged.logical().entries().contains_key(&path) {
                            remove_staged_path(&mut staged, &target, &path).await?;
                        }
                        target.create_dir(&format!("{path}/")).await?;
                        staged.record_materialized_directory(path).await?;
                        install_remote = true;
                    }
                    RemoteEdit::Remove { path } => {
                        remove_staged_path(&mut staged, &target, &path).await?;
                        install_remote = true;
                    }
                    RemoteEdit::SetExecutable {
                        path,
                        digest,
                        executable,
                    } => {
                        staged.apply_remote_attributes(&path, digest, executable)?;
                        install_remote = true;
                    }
                }
            }
            for conflict in plan.conflicts {
                if requested.contains(&conflict.path) {
                    resolved.insert(conflict.path.clone());
                    publish = true;
                } else {
                    conflicts.push(conflict);
                }
            }
            let full_tree = state.installed.is_empty() && local.entries().is_empty();
            let installed = materialize_files(
                &self.volume,
                &target,
                staged.root(),
                installs,
                full_tree,
                self.transfer_concurrency,
            )
            .await?;
            for installed in installed {
                let (path, digest, _) = installed;
                staged
                    .record_materialized_file(path.clone(), digest)
                    .await?;
                install_remote = true;
            }
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

        if !publish {
            state.conflicts.clear();
            if let Some(remote_tree) = remote_tree.as_ref() {
                if install_remote && !matches_local(replica_path, &local).await? {
                    bail!("local replica changed while remote state was being installed");
                }
                if install_remote {
                    install_staged_changes(replica_path, &staged, &local, &source_files)?;
                }
                state = state_from_snapshot(remote_tree, replica_path, &state).await?;
            } else {
                state.pending = None;
            }
            state.install(state_path)?;
            remove_tree(&staging_path)?;
            return Ok(result(&state, resolved_commit.is_some()));
        }

        let operation = OperationId::generate();
        let requires_materialization = !same_content(&local, &source_files, &staged);
        let prepared = staged
            .files()
            .iter()
            .map(|(path, staged_file)| {
                let remote_version = remote_tree
                    .as_ref()
                    .and_then(|tree| tree.get(path))
                    .and_then(|entry| entry.file);
                let version = match remote_version {
                    Some(version) if staged_file.digest == version.logical_digest => version,
                    _ => staged_file.prepared().with_context(|| {
                        format!("staged file {path:?} has no prepared volume version")
                    })?,
                };
                Ok((path.clone(), version.clone()))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;

        let publication = build_publication(
            &self.volume,
            operation,
            remote_tree.as_ref(),
            base.as_ref(),
            staged.logical(),
            &prepared,
            &local_renames,
        )?;
        state.pending = Some(staged.pending(operation, local_renames));
        state.conflicts.clear();
        state.install(state_path)?;
        match self.volume.publish(observed.as_ref(), &publication).await? {
            CommitOutcome::Committed(committed) if committed == publication.target.cursor => {
                let unchanged = matches_local(replica_path, &local).await?;
                if !unchanged {
                    return Ok(result(&state, false));
                }
                if requires_materialization {
                    install_staged_changes(replica_path, &staged, &local, &source_files)?;
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

async fn remove_staged_path(staged: &mut StagedTree, target: &Operator, path: &str) -> Result<()> {
    let removed = subtree(staged.logical().entries(), path)
        .rev()
        .map(|(path, entry)| (path.clone(), entry.kind))
        .collect::<Vec<_>>();
    for (path, kind) in removed {
        let target_path = match kind {
            LocalKind::Directory => format!("{path}/"),
            LocalKind::File => path,
        };
        target.delete(&target_path).await?;
    }
    staged.remove_logical_path(path);
    Ok(())
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

fn known_local_digests(
    local: &LocalTree,
    state: &ReplicaState,
    tree: Option<&SnapshotTree<'_>>,
) -> BTreeMap<String, [u8; 32]> {
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
                Some((path.clone(), version.logical_digest))
            } else {
                None
            }
        })
        .collect()
}

fn same_content(
    before: &LocalTree,
    before_files: &BTreeMap<String, super::staging::StagedFile>,
    after: &StagedTree,
) -> bool {
    before.entries().len() == after.logical().entries().len()
        && before.entries().iter().all(|(path, entry)| {
            after.logical().entries().get(path).is_some_and(|desired| {
                entry.kind == desired.kind
                    && entry.size == desired.size
                    && entry.executable == desired.executable
                    && before_files.get(path).map(|file| file.digest)
                        == after.files().get(path).map(|file| file.digest)
            })
        })
}

fn install_staged_changes(
    replica: &Path,
    staged: &StagedTree,
    before: &LocalTree,
    before_files: &BTreeMap<String, super::staging::StagedFile>,
) -> Result<()> {
    let removals = before
        .entries()
        .iter()
        .rev()
        .filter(|(path, entry)| {
            staged
                .logical()
                .entries()
                .get(*path)
                .is_none_or(|desired| desired.kind != entry.kind)
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

    for (path, entry) in staged.logical().entries() {
        if entry.kind == LocalKind::Directory {
            fs::create_dir_all(replica.join(path))?;
        }
    }
    for (path, entry) in staged.logical().entries() {
        if entry.kind != LocalKind::File {
            continue;
        }
        let same_content = before.entries().get(path).is_some_and(|existing| {
            existing.kind == LocalKind::File
                && existing.size == entry.size
                && before_files.get(path).map(|file| file.digest)
                    == staged.files().get(path).map(|file| file.digest)
        });
        let destination = replica.join(path);
        if !same_content {
            let source = staged
                .content_path(path)
                .with_context(|| format!("changed path {path:?} has no durable staged content"))?;
            let parent = destination.parent().unwrap_or(replica);
            fs::create_dir_all(parent)?;
            let temporary = parent.join(format!(".ofs-install-{}", uuid::Uuid::new_v4()));
            let result = (|| -> Result<()> {
                let copied = fs::copy(&source, &temporary)?;
                if copied != entry.size {
                    bail!("staged path {path:?} returned a short copy")
                }
                set_executable(&temporary, entry.executable)?;
                fs::File::open(&temporary)?.sync_all()?;
                fs::rename(&temporary, &destination)?;
                sync_parent(&destination)
            })();
            if result.is_err() {
                let _ = fs::remove_file(&temporary);
            }
            result.with_context(|| format!("install staged path {path:?}"))?;
        } else {
            set_executable(&destination, entry.executable)?;
        }
    }
    Ok(())
}

async fn materialize_files<V: Volume>(
    volume: &V,
    target: &Operator,
    root: &Path,
    files: Vec<(String, FileVersion, [u8; 32], bool)>,
    full_tree: bool,
    transfer_concurrency: NonZeroUsize,
) -> Result<Vec<(String, [u8; 32], bool)>> {
    let requests = files
        .iter()
        .map(|(path, version, _, _)| MaterializeRequest {
            path: path.clone(),
            version: version.clone(),
        })
        .collect();
    volume
        .materialize(target, requests, full_tree, transfer_concurrency)
        .await?;
    for (path, _, _, executable) in &files {
        set_executable(&root.join(path), *executable)?;
    }
    let installed = files
        .into_iter()
        .map(|(path, _, digest, executable)| (path, digest, executable))
        .collect();
    Ok(installed)
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
