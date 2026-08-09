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
use opendal::{Operator, services};

use super::local::{NativeIdentity, fs_operator, set_executable};
use super::{
    BaseEntry, ConflictRecord, LocalKind, LocalTree, PendingIntent, ReconcileAction, ReplicaState,
    StagedTree, build_publication, reconcile, snapshot_paths,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, FileVersion, MaterializeRequest, NodeKind, OperationId, Volume,
    VolumeObservation, VolumeSnapshot,
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
    pub fn new(volume: V) -> Self {
        Self {
            volume,
            transfer_concurrency: NonZeroUsize::new(4).expect("default concurrency is non-zero"),
        }
    }

    pub fn with_transfer_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.transfer_concurrency = concurrency;
        self
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
        if state.authority_identity() != authority_identity {
            bail!("replica state belongs to another branch incarnation");
        }
        tokio::fs::create_dir_all(replica_path)
            .await
            .context("create local replica")?;

        let prior_staging = if let Some(pending) = state.pending.clone() {
            match self.volume.resolve(pending.operation).await? {
                CommitOutcome::Committed(committed) => {
                    let observed = self
                        .volume
                        .observe_from(None)
                        .await?
                        .context("committed publication has no authoritative namespace")?;
                    let staged = pending
                        .staging
                        .is_dir()
                        .then(|| StagedTree::load(&pending.staging))
                        .transpose()
                        .ok()
                        .flatten();
                    let target_is_live = if observed.snapshot().cursor == committed {
                        match staged.as_ref() {
                            Some(staged) => staged
                                .matches_source_observation(&LocalTree::scan(replica_path).await?),
                            None => false,
                        }
                    } else {
                        false
                    };
                    let safe_to_install = target_is_live
                        || committed_tree_is_safe(
                            &state,
                            replica_path,
                            observed.snapshot(),
                            state_path,
                        )
                        .await?;
                    if !target_is_live && !safe_to_install {
                        state.pending = None;
                        state.install(state_path)?;
                        let _ = remove_tree(&pending.staging);
                        None
                    } else {
                        let materialized = !target_is_live;
                        if materialized {
                            materialize_tree(
                                &self.volume,
                                replica_path,
                                observed.snapshot(),
                                self.transfer_concurrency,
                            )
                            .await?;
                        }
                        state = advance_common_base(
                            observed.snapshot(),
                            replica_path,
                            state_path,
                            &state,
                        )
                        .await?;
                        let _ = remove_tree(&pending.staging);
                        return Ok(result(&state, true));
                    }
                }
                CommitOutcome::Unknown => {
                    return Ok(result(&state, false));
                }
                CommitOutcome::Absent | CommitOutcome::Conflict { .. } => {
                    if pending.staging.is_dir() && StagedTree::load(&pending.staging).is_ok() {
                        Some(pending.staging)
                    } else {
                        state.pending = None;
                        state.install(state_path)?;
                        let _ = remove_tree(&pending.staging);
                        None
                    }
                }
            }
        } else {
            None
        };
        let observed = self.volume.observe_from(state.authority.as_ref()).await?;
        let remote = observed.as_ref().map(|value| value.snapshot());
        if remote.is_none() && state.common != ChangeCursor::Genesis {
            bail!("authoritative namespace disappeared after this replica was initialized");
        }

        let (local, staging_path, mut staged) = match prior_staging.as_ref() {
            Some(path) => {
                let staged = StagedTree::load(path)?;
                (staged.logical().clone(), staged.root().to_owned(), staged)
            }
            None => {
                let local = LocalTree::scan(replica_path).await?;
                if resolve_paths.is_empty()
                    && state.conflicts.is_empty()
                    && remote.is_some_and(|snapshot| {
                        snapshot.cursor == state.common && local_matches_state(&local, &state)
                    })
                {
                    return Ok(result(&state, false));
                }
                let staging_path = fresh_sibling(state_path, "publish");
                let known_digests = known_local_digests(&local, &state);
                let staged =
                    StagedTree::prepare_known(&local, &staging_path, &known_digests).await?;
                (local, staging_path, staged)
            }
        };
        let frozen_input = FrozenTree::from_parts(&local, &staged);
        let mut known_digests = staged
            .files()
            .iter()
            .map(|(path, file)| (path.clone(), file.digest))
            .collect::<BTreeMap<_, _>>();
        let mut publish = remote.is_none() && !local.entries().is_empty();
        let mut install_remote = false;
        let mut staged_full_tree = false;
        let mut conflicts = Vec::new();
        let mut local_renames = BTreeMap::new();
        let requested = resolve_paths.iter().cloned().collect::<BTreeSet<_>>();
        if requested.len() != resolve_paths.len() {
            bail!("a conflict resolution path was provided more than once");
        }
        let mut resolved = BTreeSet::new();

        if let Some(remote) = remote {
            let plan = reconcile(&state, &staged, remote)?;
            install_remote |= remote.cursor != state.common;
            let target = fs_operator(staged.root())?;
            let (merge_remote_directories, publish_local_directories) =
                directory_changes(&state, &local, remote)?;
            publish |= publish_local_directories;
            install_remote |= merge_remote_directories;
            if merge_remote_directories {
                create_remote_directories(&mut staged, &target, &state, remote).await?;
            }
            local_renames = plan.local_renames;
            let mut installs = Vec::new();
            for action in plan.actions {
                match action {
                    ReconcileAction::KeepLocal => {}
                    ReconcileAction::PublishLocal => publish = true,
                    ReconcileAction::PublishRename => publish = true,
                    ReconcileAction::InstallRemote {
                        path,
                        node,
                        version,
                        digest,
                        ..
                    } => {
                        let file = remote
                            .file_versions
                            .get(&version)
                            .context("reconciliation references a missing remote file version")?
                            .clone();
                        let executable = remote
                            .nodes
                            .get(&node)
                            .context("reconciliation references a missing remote node")?
                            .attributes
                            .executable;
                        installs.push((path, file, digest, executable));
                    }
                    ReconcileAction::DeleteLocal { path } => {
                        target.delete(&path).await?;
                        staged.remove_logical_path(&path);
                        known_digests.remove(&path);
                        install_remote = true;
                    }
                    ReconcileAction::Conflict(conflict) if requested.contains(&conflict.path) => {
                        resolved.insert(conflict.path.clone());
                        publish = true;
                    }
                    ReconcileAction::Conflict(conflict) => conflicts.push(conflict),
                    ReconcileAction::Unsupported { path, reason } => {
                        bail!("cannot reconcile {path:?}: {reason}")
                    }
                }
            }
            let full_tree = state.base.is_empty() && local.entries().is_empty();
            staged_full_tree = full_tree;
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
                known_digests.insert(path, digest);
                install_remote = true;
            }
            if merge_remote_directories {
                delete_absent_directories(&mut staged, &target, &state, remote).await?;
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
            if let Some(remote) = remote {
                if install_remote && !matches_frozen(replica_path, &frozen_input).await? {
                    bail!("local replica changed while remote state was being installed");
                }
                if install_remote {
                    if staged_full_tree {
                        StagedTree::remove_manifest(&staging_path)?;
                        install_tree(replica_path, &staging_path)?;
                    } else {
                        install_staged_changes(replica_path, &staged, &frozen_input)?;
                    }
                }
                state = state_from_snapshot(remote, replica_path, &state).await?;
            } else {
                state.pending = None;
            }
            state.install(state_path)?;
            remove_tree(&staging_path)?;
            return Ok(result(&state, false));
        }

        let operation = OperationId::generate();
        staged.save_manifest()?;
        state.pending = Some(PendingIntent {
            operation,
            staging: staging_path.clone(),
            renames: local_renames,
        });
        state.conflicts.clear();
        state.install(state_path)?;

        let merged = staged.logical().clone();
        let merged_input = FrozenTree(
            merged
                .entries()
                .iter()
                .map(|(path, entry)| {
                    (
                        path.clone(),
                        FrozenEntry {
                            kind: entry.kind,
                            size: entry.size,
                            digest: known_digests.get(path).copied(),
                            executable: entry.executable,
                            modified: entry.modified.clone(),
                            native_identity: entry.native_identity,
                        },
                    )
                })
                .collect(),
        );
        let requires_materialization = !merged_input.same_content(&frozen_input);
        let frozen = fs_operator(staged.root())?;
        let remote_files = remote.map(snapshot_files).transpose()?.unwrap_or_default();
        let mut prepared = BTreeMap::new();
        let mut changed = Vec::new();
        for (path, _) in merged
            .entries()
            .iter()
            .filter(|(_, entry)| entry.kind == LocalKind::File)
        {
            match (known_digests.get(path), remote_files.get(path)) {
                (Some(digest), Some(version)) if *digest == version.logical_digest => {
                    prepared.insert(path.clone(), version.clone());
                }
                _ => changed.push(path.clone()),
            }
        }
        prepared.extend(
            self.volume
                .stage_files(&frozen, changed, remote, self.transfer_concurrency)
                .await?,
        );

        let mut publication_state = state.clone();
        publication_state.common = remote.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor);
        let publication = build_publication(
            &self.volume,
            volume_id,
            operation,
            remote,
            &publication_state,
            &merged,
            &prepared,
        )?;
        match self.volume.publish(observed.as_ref(), &publication).await? {
            CommitOutcome::Committed(committed) if committed == publication.target.cursor => {
                let unchanged = matches_frozen(replica_path, &frozen_input).await?;
                if !unchanged {
                    return Ok(result(&state, false));
                }
                let materialized = requires_materialization;
                if materialized {
                    install_staged_changes(replica_path, &staged, &frozen_input)?;
                }
                state = advance_common_base(&publication.target, replica_path, state_path, &state)
                    .await?;
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

async fn advance_common_base(
    snapshot: &VolumeSnapshot,
    replica: &Path,
    state_path: &Path,
    previous: &ReplicaState,
) -> Result<ReplicaState> {
    let state = state_from_snapshot(snapshot, replica, previous).await?;
    state.install(state_path)?;
    Ok(state)
}

fn result(state: &ReplicaState, published: bool) -> SyncResult {
    SyncResult {
        common: state.common,
        conflicts: state.conflicts.clone(),
        pending: state.pending.is_some(),
        published,
    }
}

#[derive(Eq, PartialEq)]
struct FrozenTree(BTreeMap<String, FrozenEntry>);

#[derive(Eq, PartialEq)]
struct FrozenEntry {
    kind: LocalKind,
    size: u64,
    digest: Option<[u8; 32]>,
    executable: bool,
    modified: String,
    native_identity: Option<NativeIdentity>,
}

impl FrozenTree {
    fn from_parts(local: &LocalTree, staged: &StagedTree) -> Self {
        Self(
            local
                .entries()
                .iter()
                .map(|(path, entry)| {
                    let digest = staged.files().get(path).map(|file| file.digest);
                    (
                        path.clone(),
                        FrozenEntry {
                            kind: entry.kind,
                            size: entry.size,
                            digest,
                            executable: entry.executable,
                            modified: entry.modified.clone(),
                            native_identity: entry.native_identity,
                        },
                    )
                })
                .collect(),
        )
    }

    fn same_content(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self.0.iter().all(|(path, entry)| {
                other.0.get(path).is_some_and(|other| {
                    entry.kind == other.kind
                        && entry.size == other.size
                        && entry.digest == other.digest
                        && entry.executable == other.executable
                })
            })
    }

    fn matches_observation(&self, observed: &LocalTree) -> bool {
        self.0.len() == observed.entries().len()
            && self.0.iter().all(|(path, expected)| {
                observed.entries().get(path).is_some_and(|entry| {
                    expected.kind == entry.kind
                        && expected.size == entry.size
                        && expected.executable == entry.executable
                        && expected.modified == entry.modified
                        && expected.native_identity == entry.native_identity
                })
            })
    }
}

async fn committed_tree_is_safe(
    state: &ReplicaState,
    replica: &Path,
    committed: &VolumeSnapshot,
    anchor: &Path,
) -> Result<bool> {
    let local = LocalTree::scan(replica).await?;
    let staging_path = fresh_sibling(anchor, "recovery-compare");
    let staged = StagedTree::prepare(&local, &staging_path).await?;
    let safe = match reconcile(state, &staged, committed) {
        Ok(plan) => {
            let files_are_safe = plan.actions.iter().all(|action| {
                matches!(
                    action,
                    ReconcileAction::KeepLocal
                        | ReconcileAction::InstallRemote { .. }
                        | ReconcileAction::DeleteLocal { .. }
                )
            });
            let directories_are_safe = directory_changes(state, &local, committed)
                .map(|(_, publish_local)| !publish_local)
                .unwrap_or(false);
            files_are_safe && directories_are_safe
        }
        Err(_) => false,
    };
    remove_tree(&staging_path)?;
    Ok(safe)
}

async fn matches_frozen(root: &Path, expected: &FrozenTree) -> Result<bool> {
    Ok(expected.matches_observation(&LocalTree::scan(root).await?))
}

fn directory_changes(
    state: &ReplicaState,
    local: &LocalTree,
    remote: &VolumeSnapshot,
) -> Result<(bool, bool)> {
    let base_kinds = state
        .base
        .iter()
        .map(|(path, entry)| {
            (
                path.clone(),
                if entry.digest.is_some() {
                    LocalKind::File
                } else {
                    LocalKind::Directory
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let local_kinds = local
        .entries()
        .iter()
        .map(|(path, entry)| (path.clone(), entry.kind))
        .collect::<BTreeMap<_, _>>();
    let remote_kinds = snapshot_paths(remote)?
        .into_iter()
        .map(|(path, node)| {
            let kind = match remote.nodes[&node].kind {
                NodeKind::Directory => LocalKind::Directory,
                NodeKind::RegularFile => LocalKind::File,
            };
            (path, kind)
        })
        .collect::<BTreeMap<_, _>>();
    let paths = base_kinds
        .keys()
        .chain(local_kinds.keys())
        .chain(remote_kinds.keys())
        .collect::<BTreeSet<_>>();
    for path in paths {
        let base = base_kinds.get(path);
        let local = local_kinds.get(path);
        let remote = remote_kinds.get(path);
        let has_directory = [base, local, remote]
            .into_iter()
            .flatten()
            .any(|kind| *kind == LocalKind::Directory);
        let has_file = [base, local, remote]
            .into_iter()
            .flatten()
            .any(|kind| *kind == LocalKind::File);
        if local != remote && has_directory && has_file {
            bail!("cannot reconcile {path:?}: file and directory changes overlap");
        }
    }

    for (path, entry) in &state.base {
        if entry.digest.is_some() {
            continue;
        }
        let local_kept = local_kinds.get(path) == Some(&LocalKind::Directory);
        let remote_kept = remote_kinds.get(path) == Some(&LocalKind::Directory);
        if !remote_kept && local_kept && local_subtree_changed(state, local, path) {
            bail!("cannot reconcile {path:?}: remote directory deletion overlaps local changes");
        }
        if !local_kept && remote_kept && remote_subtree_changed(state, remote, path)? {
            bail!("cannot reconcile {path:?}: local directory deletion overlaps remote changes");
        }
    }

    let base = directories(&base_kinds);
    let local = directories(&local_kinds);
    let remote = directories(&remote_kinds);
    if local == remote {
        Ok((false, false))
    } else {
        Ok((remote != base, local != base))
    }
}

fn local_subtree_changed(state: &ReplicaState, local: &LocalTree, directory: &str) -> bool {
    state
        .base
        .keys()
        .chain(local.entries().keys())
        .filter(|path| path_is_within(path, directory))
        .any(
            |path| match (state.base.get(path), local.entries().get(path)) {
                (Some(base), Some(current)) => {
                    let base_kind = if base.digest.is_some() {
                        LocalKind::File
                    } else {
                        LocalKind::Directory
                    };
                    current.kind != base_kind
                        || current.kind == LocalKind::File
                            && (base.local_identity != current.native_identity
                                || base.local_size != Some(current.size)
                                || base.local_modified.as_deref()
                                    != Some(current.modified.as_str())
                                || base.local_executable != Some(current.executable))
                }
                (None, None) => false,
                _ => true,
            },
        )
}

fn remote_subtree_changed(
    state: &ReplicaState,
    remote: &VolumeSnapshot,
    directory: &str,
) -> Result<bool> {
    let remote_paths = snapshot_paths(remote)?;
    let paths = state
        .base
        .keys()
        .chain(remote_paths.keys())
        .filter(|path| path_is_within(path, directory))
        .collect::<BTreeSet<_>>();
    for path in paths {
        let changed = match (state.base.get(path), remote_paths.get(path)) {
            (Some(base), Some(node)) => {
                let record = remote
                    .nodes
                    .get(node)
                    .context("remote path references a missing node")?;
                let remote_digest = match record.file_version {
                    Some(version) => Some(
                        remote
                            .file_versions
                            .get(&version)
                            .context("remote node references a missing file version")?
                            .logical_digest,
                    ),
                    None => None,
                };
                let remote_directory_generation = remote
                    .directories
                    .get(node)
                    .map(|directory| &directory.generation);
                base.node != *node
                    || base.generation != record.generation
                    || base.digest != remote_digest
                    || base.directory_generation.as_ref() != remote_directory_generation
            }
            (None, None) => false,
            _ => true,
        };
        if changed {
            return Ok(true);
        }
    }
    Ok(false)
}

fn path_is_within(path: &str, directory: &str) -> bool {
    path == directory
        || path
            .strip_prefix(directory)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn directories(kinds: &BTreeMap<String, LocalKind>) -> BTreeSet<String> {
    kinds
        .iter()
        .filter(|(_, kind)| **kind == LocalKind::Directory)
        .map(|(path, _)| path.clone())
        .collect()
}

async fn create_remote_directories(
    staged: &mut StagedTree,
    target: &Operator,
    state: &ReplicaState,
    remote: &VolumeSnapshot,
) -> Result<()> {
    for (path, node) in snapshot_paths(remote)? {
        if remote.nodes[&node].kind == NodeKind::Directory && !state.base.contains_key(&path) {
            target.create_dir(&format!("{path}/")).await?;
            staged.record_materialized_directory(path).await?;
        }
    }
    Ok(())
}

async fn delete_absent_directories(
    staged: &mut StagedTree,
    target: &Operator,
    state: &ReplicaState,
    remote: &VolumeSnapshot,
) -> Result<()> {
    let remote = snapshot_paths(remote)?
        .into_iter()
        .filter(|(_, node)| remote.nodes[node].kind == NodeKind::Directory)
        .map(|(path, _)| path)
        .collect::<BTreeSet<_>>();
    let mut removed = state
        .base
        .iter()
        .filter(|(path, entry)| entry.digest.is_none() && !remote.contains(*path))
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    removed.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
    for path in removed {
        target.delete(&format!("{path}/")).await?;
        staged.remove_logical_path(&path);
    }
    Ok(())
}

async fn state_from_snapshot(
    snapshot: &VolumeSnapshot,
    replica: &Path,
    previous: &ReplicaState,
) -> Result<ReplicaState> {
    let local = LocalTree::scan(replica).await?;
    let mut state = ReplicaState::empty(snapshot.volume_id);
    state.branch = previous.branch.clone();
    state.common = snapshot.cursor;
    state.authority = Some(snapshot.clone());
    for (path, node) in snapshot_paths(snapshot)? {
        let record = &snapshot.nodes[&node];
        let digest = record
            .file_version
            .map(|version| snapshot.file_versions[&version].logical_digest);
        let local_kind = match record.kind {
            NodeKind::Directory => LocalKind::Directory,
            NodeKind::RegularFile => LocalKind::File,
        };
        let local_entry = local.entries().get(&path);
        if local_entry.is_some_and(|entry| entry.kind != local_kind) {
            bail!("installed local path {path:?} has the wrong kind");
        }
        let directory_generation = snapshot
            .directories
            .get(&node)
            .map(|directory| directory.generation.clone());
        state.base.insert(
            path,
            BaseEntry {
                node,
                generation: record.generation.clone(),
                directory_generation,
                digest,
                local_identity: local_entry.and_then(|entry| entry.native_identity),
                local_size: local_entry.map(|entry| entry.size),
                local_modified: local_entry.map(|entry| entry.modified.clone()),
                local_executable: local_entry.map(|entry| entry.executable),
            },
        );
    }
    Ok(state)
}

fn local_matches_state(local: &LocalTree, state: &ReplicaState) -> bool {
    local.entries().len() == state.base.len()
        && local.entries().iter().all(|(path, entry)| {
            state.base.get(path).is_some_and(|base| {
                base.local_identity == entry.native_identity
                    && base.local_size == Some(entry.size)
                    && base.local_modified.as_deref() == Some(entry.modified.as_str())
                    && base.local_executable == Some(entry.executable)
                    && base.digest.is_some() == (entry.kind == LocalKind::File)
            })
        })
}

fn known_local_digests(local: &LocalTree, state: &ReplicaState) -> BTreeMap<String, [u8; 32]> {
    local
        .entries()
        .iter()
        .filter_map(|(path, entry)| {
            let base = state.base.get(path)?;
            if entry.kind == LocalKind::File
                && base.local_identity == entry.native_identity
                && base.local_size == Some(entry.size)
                && base.local_modified.as_deref() == Some(entry.modified.as_str())
                && base.local_executable == Some(entry.executable)
            {
                base.digest.map(|digest| (path.clone(), digest))
            } else {
                None
            }
        })
        .collect()
}

fn install_staged_changes(replica: &Path, staged: &StagedTree, before: &FrozenTree) -> Result<()> {
    let after = FrozenTree::from_parts(staged.logical(), staged);
    let mut removals = before
        .0
        .keys()
        .filter(|path| !after.0.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    removals.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
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
        let desired = after.0.get(path).context("staged file is missing")?;
        let same_content = before.0.get(path).is_some_and(|existing| {
            existing.kind == LocalKind::File
                && existing.size == desired.size
                && existing.digest == desired.digest
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

async fn materialize_tree<V: Volume>(
    volume: &V,
    replica: &Path,
    snapshot: &VolumeSnapshot,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    let staging = fresh_sibling(replica, "materialize");
    fs::create_dir(&staging).context("create materialization tree")?;
    let root = staging
        .to_str()
        .context("materialization path is not valid Unicode")?;
    let target = Operator::new(services::Fs::default().root(root))?.finish();
    let result = async {
        let mut files = Vec::new();
        for (path, node) in snapshot_paths(snapshot)? {
            let record = &snapshot.nodes[&node];
            match record.kind {
                NodeKind::Directory => target.create_dir(&format!("{path}/")).await?,
                NodeKind::RegularFile => {
                    let version = record
                        .file_version
                        .context("file node has no file version")?;
                    let version = snapshot
                        .file_versions
                        .get(&version)
                        .context("file version is missing")?
                        .clone();
                    files.push((path, version, record.attributes.executable));
                }
            }
        }
        materialize_files(
            volume,
            &target,
            &staging,
            files
                .into_iter()
                .map(|(path, version, executable)| {
                    let digest = version.logical_digest;
                    (path, version, digest, executable)
                })
                .collect(),
            true,
            transfer_concurrency,
        )
        .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    install_tree(replica, &staging)
}

fn install_tree(replica: &Path, staging: &Path) -> Result<()> {
    let backup = fresh_sibling(replica, "backup");
    let existed = replica.exists();
    if existed {
        fs::rename(replica, &backup).context("move prior replica aside")?;
    }
    if let Err(error) = fs::rename(staging, replica) {
        if existed {
            let _ = fs::rename(&backup, replica);
        }
        return Err(error).context("install materialized replica");
    }
    sync_parent(replica)?;
    if existed {
        fs::remove_dir_all(backup).context("remove prior replica tree")?;
    }
    Ok(())
}

fn snapshot_files(snapshot: &VolumeSnapshot) -> Result<BTreeMap<String, FileVersion>> {
    snapshot_paths(snapshot)?
        .into_iter()
        .filter_map(|(path, node)| {
            snapshot.nodes[&node]
                .file_version
                .map(|version| (path, version))
        })
        .map(|(path, version)| {
            snapshot
                .file_versions
                .get(&version)
                .cloned()
                .map(|record| (path, record))
                .context("file version is missing")
        })
        .collect()
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
    StagedTree::remove_manifest(path)?;
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
