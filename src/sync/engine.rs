// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Object-backed Managed Sync orchestration.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use opendal::{Operator, services};

use super::local::fs_operator;
use super::{
    BaseEntry, ConflictRecord, LocalKind, LocalTree, PendingIntent, ReconcileAction, ReplicaState,
    StagedTree, build_publication, reconcile,
};
use crate::filesystem::{ChangeCursor, CommitOutcome, Generation, NodeId, OperationId, VolumeId};
use crate::managed::ManagedVolume;
use crate::managed::namespace::{FileVersionRecord, NamespaceSnapshot, NodeKind};

#[derive(Clone, Debug)]
pub struct SyncResult {
    pub volume: VolumeId,
    pub common: ChangeCursor,
    pub conflicts: Vec<ConflictRecord>,
    pub pending: bool,
    pub published: bool,
    pub materialized: bool,
}

#[derive(Clone)]
pub struct SyncEngine {
    volume_id: VolumeId,
    volume: ManagedVolume,
}

impl SyncEngine {
    pub fn object(volume_id: VolumeId, data_operator: Operator) -> Result<Self> {
        Ok(Self {
            volume_id,
            volume: ManagedVolume::object(volume_id, data_operator)?,
        })
    }

    pub async fn sync(
        &self,
        replica_path: impl AsRef<Path>,
        state_path: impl AsRef<Path>,
        resolve_paths: &[String],
    ) -> Result<SyncResult> {
        let replica_path = replica_path.as_ref();
        let state_path = state_path.as_ref();
        tokio::fs::create_dir_all(replica_path)
            .await
            .context("create local replica")?;
        let mut state =
            ReplicaState::load(state_path)?.unwrap_or_else(|| ReplicaState::empty(self.volume_id));
        if state.volume != self.volume_id {
            bail!("replica state belongs to another volume");
        }

        let prior_staging = if let Some(pending) = &state.pending {
            match self.volume.resolve(pending.operation).await? {
                CommitOutcome::Committed(_) => {
                    let observed = self
                        .volume
                        .observe()
                        .await?
                        .context("committed publication has no authoritative namespace")?;
                    let safe_to_install = committed_tree_is_safe(
                        &state,
                        replica_path,
                        &pending.staging,
                        observed.snapshot(),
                        state_path,
                    )
                    .await?;
                    if safe_to_install {
                        materialize_tree(&self.volume, replica_path, observed.snapshot()).await?;
                    }
                    state = state_from_snapshot(observed.snapshot())?;
                    state.install(state_path)?;
                    return Ok(result(&state, true, safe_to_install));
                }
                CommitOutcome::Unknown => return Ok(result(&state, false, false)),
                CommitOutcome::Absent | CommitOutcome::Conflict { .. } => {
                    if !pending.staging.is_dir() {
                        bail!("pending publication staging is missing; restore it before recovery");
                    }
                    Some(pending.staging.clone())
                }
            }
        } else {
            None
        };
        if prior_staging.is_some() {
            state.pending = None;
        }

        let observed = self.volume.observe().await?;
        let remote = observed.as_ref().map(|value| value.snapshot());
        if remote.is_none() && state.common != ChangeCursor::Genesis {
            bail!("authoritative namespace disappeared after this replica was initialized");
        }

        let source = prior_staging.as_deref().unwrap_or(replica_path);
        let local = LocalTree::scan(source).await?;
        let staging_path = fresh_sibling(state_path, "publish");
        let staged = StagedTree::prepare(&local, &staging_path).await?;
        let frozen_input = FrozenTree::from_parts(&local, &staged);
        let mut known_digests = staged
            .files()
            .iter()
            .map(|(path, file)| (path.clone(), file.digest))
            .collect::<BTreeMap<_, _>>();
        let mut publish = remote.is_none() && !local.entries().is_empty();
        let mut install_remote = false;
        let mut conflicts = Vec::new();
        let requested = resolve_paths.iter().cloned().collect::<BTreeSet<_>>();
        if requested.len() != resolve_paths.len() {
            bail!("a conflict resolution path was provided more than once");
        }
        let mut resolved = BTreeSet::new();

        if let Some(remote) = remote {
            let plan = reconcile(&state, &staged, remote)?;
            if plan.base != state.common || plan.remote != remote.cursor {
                bail!("reconciliation plan does not match its fixed inputs");
            }
            install_remote |= remote.cursor != state.common;
            let target = fs_operator(staged.root())?;
            let (merge_remote_directories, publish_local_directories) =
                directory_changes(&state, &local, remote)?;
            publish |= publish_local_directories;
            install_remote |= merge_remote_directories;
            if merge_remote_directories {
                create_remote_directories(&target, remote).await?;
            }
            for action in plan.actions {
                match action {
                    ReconcileAction::KeepLocal { .. } => {}
                    ReconcileAction::PublishLocal { .. } => publish = true,
                    ReconcileAction::InstallRemote {
                        path,
                        version,
                        digest,
                        ..
                    } => {
                        let file = remote
                            .file_versions
                            .get(&version)
                            .context("reconciliation references a missing remote file version")?;
                        self.volume.materialize(file, &target, &path).await?;
                        known_digests.insert(path, digest);
                        install_remote = true;
                    }
                    ReconcileAction::DeleteLocal { path } => {
                        target.delete(&path).await?;
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
            if merge_remote_directories {
                delete_absent_directories(&target, &local, remote).await?;
            }
        }
        if resolved != requested {
            let missing = requested.difference(&resolved).collect::<Vec<_>>();
            bail!("no unresolved conflict exists for {missing:?}");
        }
        if !conflicts.is_empty() {
            state.conflicts = conflicts;
            state.install(state_path)?;
            if let Some(old) = prior_staging {
                remove_tree(&old)?;
            }
            remove_tree(&staging_path)?;
            return Ok(result(&state, false, false));
        }

        if !publish {
            state.conflicts.clear();
            if let Some(remote) = remote {
                if install_remote {
                    if matches_frozen(replica_path, &frozen_input, state_path).await? {
                        materialize_tree(&self.volume, replica_path, remote).await?;
                    } else {
                        install_remote = false;
                    }
                }
                state = state_from_snapshot(remote)?;
            } else {
                state.pending = None;
            }
            state.install(state_path)?;
            if let Some(old) = prior_staging {
                remove_tree(&old)?;
            }
            remove_tree(&staging_path)?;
            return Ok(result(&state, false, install_remote));
        }

        let operation = OperationId::generate();
        state.pending = Some(PendingIntent {
            operation,
            base: remote.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            staging: staging_path.clone(),
        });
        state.conflicts.clear();
        state.install(state_path)?;
        if let Some(old) = &prior_staging {
            remove_tree(old)?;
        }

        let merged = LocalTree::scan(staged.root()).await?;
        let frozen = fs_operator(staged.root())?;
        let remote_files = remote.map(snapshot_files).transpose()?.unwrap_or_default();
        let mut prepared = BTreeMap::new();
        for (path, entry) in merged.entries() {
            if entry.kind != LocalKind::File {
                continue;
            }
            let version = match (known_digests.get(path), remote_files.get(path)) {
                (Some(digest), Some(version)) if *digest == version.logical_digest => {
                    version.clone()
                }
                _ => self.volume.seal_whole_file(&frozen, path).await?,
            };
            prepared.insert(path.clone(), version);
        }

        let mut publication_state = state.clone();
        publication_state.common = remote.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor);
        let publication = build_publication(
            self.volume_id,
            operation,
            remote,
            &publication_state,
            &merged,
            &prepared,
        )?;
        match self.volume.publish(observed.as_ref(), &publication).await? {
            CommitOutcome::Committed(_) => {
                let unchanged = matches_frozen(replica_path, &frozen_input, state_path).await?;
                if unchanged {
                    materialize_tree(&self.volume, replica_path, &publication.target).await?;
                }
                state = state_from_snapshot(&publication.target)?;
                state.install(state_path)?;
                if let Some(old) = prior_staging {
                    remove_tree(&old)?;
                }
                remove_tree(&staging_path)?;
                Ok(result(&state, true, unchanged))
            }
            CommitOutcome::Absent | CommitOutcome::Conflict { .. } | CommitOutcome::Unknown => {
                Ok(result(&state, false, false))
            }
        }
    }
}

fn result(state: &ReplicaState, published: bool, materialized: bool) -> SyncResult {
    SyncResult {
        volume: state.volume,
        common: state.common,
        conflicts: state.conflicts.clone(),
        pending: state.pending.is_some(),
        published,
        materialized,
    }
}

#[derive(Eq, PartialEq)]
struct FrozenTree(BTreeMap<String, FrozenEntry>);

#[derive(Eq, PartialEq)]
struct FrozenEntry {
    kind: LocalKind,
    size: u64,
    digest: Option<[u8; 32]>,
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
                        },
                    )
                })
                .collect(),
        )
    }
}

async fn same_tree(left: &Path, right: &Path, anchor: &Path) -> Result<bool> {
    Ok(freeze_tree(left, anchor).await? == freeze_tree(right, anchor).await?)
}

async fn committed_tree_is_safe(
    state: &ReplicaState,
    replica: &Path,
    original_staging: &Path,
    committed: &NamespaceSnapshot,
    anchor: &Path,
) -> Result<bool> {
    if original_staging.is_dir() && same_tree(replica, original_staging, anchor).await? {
        return Ok(true);
    }
    let local = LocalTree::scan(replica).await?;
    let staging_path = fresh_sibling(anchor, "recovery-compare");
    let staged = StagedTree::prepare(&local, &staging_path).await?;
    let safe = match reconcile(state, &staged, committed) {
        Ok(plan) => {
            let files_are_safe = plan.actions.iter().all(|action| {
                matches!(
                    action,
                    ReconcileAction::KeepLocal { .. }
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

async fn matches_frozen(root: &Path, expected: &FrozenTree, anchor: &Path) -> Result<bool> {
    Ok(&freeze_tree(root, anchor).await? == expected)
}

async fn freeze_tree(root: &Path, anchor: &Path) -> Result<FrozenTree> {
    let local = LocalTree::scan(root).await?;
    let staging_path = fresh_sibling(anchor, "compare");
    let staged = StagedTree::prepare(&local, &staging_path).await?;
    let frozen = FrozenTree::from_parts(&local, &staged);
    remove_tree(&staging_path)?;
    Ok(frozen)
}

fn directory_changes(
    state: &ReplicaState,
    local: &LocalTree,
    remote: &NamespaceSnapshot,
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

    let base = directories(&base_kinds);
    let local = directories(&local_kinds);
    let remote = directories(&remote_kinds);
    if local == remote {
        Ok((false, false))
    } else if local == base {
        Ok((true, false))
    } else if remote == base {
        Ok((false, true))
    } else {
        bail!("local and remote directory changes overlap; resolve them before syncing")
    }
}

fn directories(kinds: &BTreeMap<String, LocalKind>) -> BTreeSet<String> {
    kinds
        .iter()
        .filter(|(_, kind)| **kind == LocalKind::Directory)
        .map(|(path, _)| path.clone())
        .collect()
}

async fn create_remote_directories(target: &Operator, remote: &NamespaceSnapshot) -> Result<()> {
    for (path, node) in snapshot_paths(remote)? {
        if remote.nodes[&node].kind == NodeKind::Directory {
            target.create_dir(&format!("{path}/")).await?;
        }
    }
    Ok(())
}

async fn delete_absent_directories(
    target: &Operator,
    local: &LocalTree,
    remote: &NamespaceSnapshot,
) -> Result<()> {
    let remote = snapshot_paths(remote)?
        .into_iter()
        .filter(|(_, node)| remote.nodes[node].kind == NodeKind::Directory)
        .map(|(path, _)| path)
        .collect::<BTreeSet<_>>();
    let mut removed = local
        .entries()
        .iter()
        .filter(|(path, entry)| entry.kind == LocalKind::Directory && !remote.contains(*path))
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    removed.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
    for path in removed {
        target.delete(&format!("{path}/")).await?;
    }
    Ok(())
}

fn state_from_snapshot(snapshot: &NamespaceSnapshot) -> Result<ReplicaState> {
    let mut state = ReplicaState::empty(snapshot.volume_id);
    state.common = snapshot.cursor;
    for (path, node) in snapshot_paths(snapshot)? {
        let record = &snapshot.nodes[&node];
        let digest = record
            .file_version
            .map(|version| snapshot.file_versions[&version].logical_digest);
        state.base.insert(
            path,
            BaseEntry {
                node,
                generation: Generation::from_bytes(record.generation.to_be_bytes().to_vec()),
                digest,
            },
        );
    }
    Ok(state)
}

async fn materialize_tree(
    volume: &ManagedVolume,
    replica: &Path,
    snapshot: &NamespaceSnapshot,
) -> Result<()> {
    let staging = fresh_sibling(replica, "materialize");
    fs::create_dir(&staging).context("create materialization tree")?;
    let root = staging
        .to_str()
        .context("materialization path is not valid Unicode")?;
    let target = Operator::new(services::Fs::default().root(root))?.finish();
    let result = async {
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
                        .context("file version is missing")?;
                    volume.materialize(version, &target, &path).await?;
                    set_executable(&staging.join(&path), record.attributes.executable)?;
                }
            }
        }
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

fn snapshot_files(snapshot: &NamespaceSnapshot) -> Result<BTreeMap<String, FileVersionRecord>> {
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

fn snapshot_paths(snapshot: &NamespaceSnapshot) -> Result<BTreeMap<String, NodeId>> {
    let mut paths = BTreeMap::new();
    let mut pending = vec![(String::new(), snapshot.root)];
    while let Some((path, node)) = pending.pop() {
        if !path.is_empty() {
            paths.insert(path.clone(), node);
        }
        let record = snapshot.nodes.get(&node).context("node is missing")?;
        if record.kind != NodeKind::Directory {
            continue;
        }
        let directory = snapshot
            .directories
            .get(&node)
            .context("directory record is missing")?;
        for (name, entry) in directory.entries.iter().rev() {
            let child = if path.is_empty() {
                name.clone()
            } else {
                format!("{path}/{name}")
            };
            pending.push((child, entry.node));
        }
    }
    Ok(paths)
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

fn set_executable(path: &Path, executable: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mut permissions = fs::metadata(path)?.permissions();
        let mode = permissions.mode();
        permissions.set_mode(if executable {
            mode | 0o111
        } else {
            mode & !0o111
        });
        fs::set_permissions(path, permissions)?;
    }
    #[cfg(not(unix))]
    let _ = (path, executable);
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
