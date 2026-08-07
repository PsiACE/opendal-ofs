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
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use futures::{StreamExt as _, stream};
use opendal::{Operator, services};

use super::local::{NativeIdentity, fs_operator, set_executable};
use super::{
    BaseEntry, ConflictRecord, LocalKind, LocalTree, PendingIntent, ReconcileAction, ReplicaState,
    StagedTree, build_publication, reconcile,
};
use crate::filesystem::{
    ChangeCursor, NodeId, NodeKind, OperationId, PublicationProgress, VolumeId,
};
use crate::managed::namespace::{FileVersionRecord, NamespaceSnapshot};
use crate::managed::{D1Metadata, FileLayoutPolicy, ManagedVolume};

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
    transfer_concurrency: NonZeroUsize,
}

impl SyncEngine {
    pub fn object(volume_id: VolumeId, data_operator: Operator) -> Result<Self> {
        Ok(Self {
            volume_id,
            volume: ManagedVolume::object(volume_id, data_operator)?,
            transfer_concurrency: NonZeroUsize::new(4).expect("default concurrency is non-zero"),
        })
    }

    pub fn d1(volume_id: VolumeId, data_operator: Operator, metadata: D1Metadata) -> Result<Self> {
        Ok(Self {
            volume_id,
            volume: ManagedVolume::d1(volume_id, data_operator, metadata)?,
            transfer_concurrency: NonZeroUsize::new(4).expect("default concurrency is non-zero"),
        })
    }

    pub fn with_file_layout(mut self, policy: FileLayoutPolicy) -> Result<Self> {
        self.volume = self.volume.with_file_layout(policy)?;
        Ok(self)
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
        tokio::fs::create_dir_all(replica_path)
            .await
            .context("create local replica")?;
        let mut state =
            ReplicaState::load(state_path)?.unwrap_or_else(|| ReplicaState::empty(self.volume_id));
        if state.volume != self.volume_id {
            bail!("replica state belongs to another volume");
        }

        let prior_staging = if let Some(pending) = state.pending.clone() {
            let progress = PublicationProgress::prepared(pending.base)
                .record_outcome(self.volume.resolve(pending.operation).await?)
                .context("resolve prepared publication")?;
            match progress {
                PublicationProgress::Published { committed } => {
                    let observed = self
                        .volume
                        .observe()
                        .await?
                        .context("committed publication has no authoritative namespace")?;
                    let target_is_live = observed.snapshot().cursor == committed
                        && pending.staging.is_dir()
                        && same_tree(replica_path, &pending.staging, state_path).await?;
                    let safe_to_install = target_is_live
                        || committed_tree_is_safe(
                            &state,
                            replica_path,
                            observed.snapshot(),
                            state_path,
                        )
                        .await?;
                    if !target_is_live && !safe_to_install {
                        return Ok(result(&state, false, false));
                    }
                    let materialized = !target_is_live;
                    if materialized {
                        if pending.staging.is_dir() {
                            apply_snapshot_attributes(&pending.staging, observed.snapshot())?;
                            install_tree(replica_path, &pending.staging)?;
                        } else {
                            materialize_tree(
                                &self.volume,
                                replica_path,
                                observed.snapshot(),
                                self.transfer_concurrency,
                            )
                            .await?;
                        }
                    }
                    let progress = progress
                        .record_install(observed.snapshot().cursor)
                        .context("record installed publication")?;
                    state = advance_common_base(
                        progress,
                        observed.snapshot(),
                        replica_path,
                        state_path,
                    )
                    .await?;
                    return Ok(result(&state, true, materialized));
                }
                PublicationProgress::Unknown { .. } => {
                    return Ok(result(&state, false, false));
                }
                PublicationProgress::Retry { .. } => {
                    if !pending.staging.is_dir() {
                        bail!("pending publication staging is missing; restore it before recovery");
                    }
                    Some(pending.staging)
                }
                _ => bail!("resolved publication entered an invalid state"),
            }
        } else {
            None
        };
        let observed = self.volume.observe_from(state.authority.as_ref()).await?;
        let remote = observed.as_ref().map(|value| value.snapshot());
        if remote.is_none() && state.common != ChangeCursor::Genesis {
            bail!("authoritative namespace disappeared after this replica was initialized");
        }

        let source = prior_staging.as_deref().unwrap_or(replica_path);
        let local = LocalTree::scan(source).await?;
        if prior_staging.is_none()
            && resolve_paths.is_empty()
            && state.conflicts.is_empty()
            && remote.is_some_and(|snapshot| {
                snapshot.cursor == state.common && local_matches_state(&local, &state)
            })
        {
            return Ok(result(&state, false, false));
        }
        let staging_path = fresh_sibling(state_path, "publish");
        let known_digests = local
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
            .collect::<BTreeMap<_, _>>();
        let staged = StagedTree::prepare_known(&local, &staging_path, &known_digests).await?;
        let frozen_input = FrozenTree::from_parts(&local, &staged);
        let mut known_digests = staged
            .files()
            .iter()
            .map(|(path, file)| (path.clone(), file.digest))
            .collect::<BTreeMap<_, _>>();
        let mut publish = remote.is_none() && !local.entries().is_empty();
        let mut install_remote = false;
        let mut conflicts = Vec::new();
        let mut local_renames = BTreeMap::new();
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
            local_renames = plan.local_renames;
            let mut installs = Vec::new();
            for action in plan.actions {
                match action {
                    ReconcileAction::KeepLocal { .. } => {}
                    ReconcileAction::PublishLocal { .. } => publish = true,
                    ReconcileAction::PublishRename { .. } => publish = true,
                    ReconcileAction::InstallRemote {
                        path,
                        version,
                        digest,
                        ..
                    } => {
                        let file = remote
                            .file_versions
                            .get(&version)
                            .context("reconciliation references a missing remote file version")?
                            .clone();
                        installs.push((path, file, digest));
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
            let materializer = self.volume.materializer()?;
            let installed = stream::iter(installs)
                .map(|(path, file, digest)| {
                    let materializer = materializer.clone();
                    let target = target.clone();
                    async move {
                        materializer.materialize(&file, &target, &path).await?;
                        Ok::<_, crate::managed::ManagedError>((path, digest))
                    }
                })
                .buffer_unordered(self.transfer_concurrency.get())
                .collect::<Vec<_>>()
                .await;
            for installed in installed {
                let (path, digest) = installed?;
                known_digests.insert(path, digest);
                install_remote = true;
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
            state.pending = None;
            state.install(state_path)?;
            if let Some(old) = prior_staging {
                remove_tree(&old)?;
            }
            remove_tree(&staging_path)?;
            return Ok(result(&state, false, false));
        }

        if !publish {
            state.conflicts.clear();
            let mut staging_installed = false;
            if let Some(remote) = remote {
                if install_remote {
                    if matches_frozen(replica_path, &frozen_input).await? {
                        apply_snapshot_attributes(staged.root(), remote)?;
                        install_tree(replica_path, &staging_path)?;
                        staging_installed = true;
                    } else {
                        install_remote = false;
                    }
                }
                state = state_from_snapshot(remote, replica_path).await?;
            } else {
                state.pending = None;
            }
            state.install(state_path)?;
            if let Some(old) = prior_staging {
                remove_tree(&old)?;
            }
            if !staging_installed {
                remove_tree(&staging_path)?;
            }
            return Ok(result(&state, false, install_remote));
        }

        let operation = OperationId::generate();
        state.pending = Some(PendingIntent {
            operation,
            base: remote.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            staging: staging_path.clone(),
            renames: local_renames,
        });
        state.conflicts.clear();
        state.install(state_path)?;
        if let Some(old) = &prior_staging {
            remove_tree(old)?;
        }

        let merged = LocalTree::scan(staged.root()).await?;
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
        let files = merged
            .entries()
            .iter()
            .filter(|(_, entry)| entry.kind == LocalKind::File)
            .map(|(path, _)| {
                let reusable = match (known_digests.get(path), remote_files.get(path)) {
                    (Some(digest), Some(version)) if *digest == version.logical_digest => {
                        Some(version.clone())
                    }
                    _ => None,
                };
                (path.clone(), reusable)
            })
            .collect::<Vec<_>>();
        let sealed = stream::iter(files)
            .map(|(path, reusable)| {
                let volume = self.volume.clone();
                let frozen = frozen.clone();
                async move {
                    let version = match reusable {
                        Some(version) => version,
                        None => volume.seal_file(&frozen, &path).await?,
                    };
                    Ok::<_, anyhow::Error>((path, version))
                }
            })
            .buffer_unordered(self.transfer_concurrency.get())
            .collect::<Vec<_>>()
            .await;
        let mut prepared = BTreeMap::new();
        for sealed in sealed {
            let (path, version) = sealed?;
            prepared.insert(path, version);
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
        let progress = PublicationProgress::prepared(publication.parent)
            .record_outcome(self.volume.publish(observed.as_ref(), &publication).await?)
            .context("record publication outcome")?;
        match progress {
            PublicationProgress::Published { .. } => {
                let unchanged = matches_frozen(replica_path, &frozen_input).await?;
                if !unchanged {
                    return Ok(result(&state, false, false));
                }
                let materialized = requires_materialization;
                if materialized {
                    apply_snapshot_attributes(staged.root(), &publication.target)?;
                    install_tree(replica_path, &staging_path)?;
                }
                let progress = progress
                    .record_install(publication.target.cursor)
                    .context("record installed publication")?;
                state =
                    advance_common_base(progress, &publication.target, replica_path, state_path)
                        .await?;
                if let Some(old) = prior_staging {
                    remove_tree(&old)?;
                }
                if !materialized {
                    remove_tree(&staging_path)?;
                }
                Ok(result(&state, true, materialized))
            }
            PublicationProgress::Retry { .. } | PublicationProgress::Unknown { .. } => {
                Ok(result(&state, false, false))
            }
            _ => bail!("publication entered an invalid state"),
        }
    }
}

async fn advance_common_base(
    progress: PublicationProgress,
    snapshot: &NamespaceSnapshot,
    replica: &Path,
    state_path: &Path,
) -> Result<ReplicaState> {
    let state = state_from_snapshot(snapshot, replica).await?;
    let progress = progress
        .record_common_base(state.common)
        .context("advance publication common base")?;
    let _complete = progress
        .record_intent_clear()
        .context("clear completed publication intent")?;
    state.install(state_path)?;
    Ok(state)
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

async fn same_tree(left: &Path, right: &Path, anchor: &Path) -> Result<bool> {
    Ok(freeze_tree(left, anchor)
        .await?
        .same_content(&freeze_tree(right, anchor).await?))
}

async fn committed_tree_is_safe(
    state: &ReplicaState,
    replica: &Path,
    committed: &NamespaceSnapshot,
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

async fn matches_frozen(root: &Path, expected: &FrozenTree) -> Result<bool> {
    Ok(expected.matches_observation(&LocalTree::scan(root).await?))
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

async fn state_from_snapshot(snapshot: &NamespaceSnapshot, replica: &Path) -> Result<ReplicaState> {
    let local = LocalTree::scan(replica).await?;
    let mut state = ReplicaState::empty(snapshot.volume_id);
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

fn apply_snapshot_attributes(root: &Path, snapshot: &NamespaceSnapshot) -> Result<()> {
    for (path, node) in snapshot_paths(snapshot)? {
        let record = &snapshot.nodes[&node];
        if record.kind == NodeKind::RegularFile {
            set_executable(&root.join(path), record.attributes.executable)?;
        }
    }
    Ok(())
}

async fn materialize_tree(
    volume: &ManagedVolume,
    replica: &Path,
    snapshot: &NamespaceSnapshot,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    let staging = fresh_sibling(replica, "materialize");
    fs::create_dir(&staging).context("create materialization tree")?;
    let root = staging
        .to_str()
        .context("materialization path is not valid Unicode")?;
    let target = Operator::new(services::Fs::default().root(root))?.finish();
    let materializer = volume.materializer()?;
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
        let installed = stream::iter(files)
            .map(|(path, version, executable)| {
                let materializer = materializer.clone();
                let target = target.clone();
                let staging = staging.clone();
                async move {
                    materializer.materialize(&version, &target, &path).await?;
                    set_executable(&staging.join(&path), executable)?;
                    Ok::<_, anyhow::Error>(())
                }
            })
            .buffer_unordered(transfer_concurrency.get())
            .collect::<Vec<_>>()
            .await;
        for installed in installed {
            installed?;
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

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    fs::File::open(parent)?.sync_all().map_err(Into::into)
}

#[cfg(not(unix))]
fn sync_parent(_: &Path) -> Result<()> {
    Ok(())
}
