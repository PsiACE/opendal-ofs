// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One foreground Managed Sync reconciliation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use futures::{StreamExt, TryStreamExt, stream};
use uuid::Uuid;

use crate::model::{
    CommitRecord, Cursor, Manifest, NamespaceChange, Node, NodeKind, OperationId, VolumeId,
};
use crate::replica::{
    CommonBase, Conflict, ConflictKind, PendingMaterialization, PendingPublication, ReplicaLock,
    ReplicaPaths, ReplicaState,
};
use crate::store::{DataStore, MetadataStore, PublicationOutcome};

pub(crate) const CAPABILITIES: &[&str] = &[
    "atomic-snapshot",
    "change-feed",
    "conditional-publication",
    "conflict-retention",
    "idempotent-publication",
    "immutable-data",
    "local-replica",
    "offline-write",
    "portable-names",
    "stable-node-id",
];

pub(crate) fn admit(required: &[String]) -> Result<()> {
    for capability in required {
        if !CAPABILITIES.contains(&capability.as_str()) {
            bail!("required Managed Sync capability {capability:?} is unavailable");
        }
    }
    Ok(())
}

pub(crate) struct ManagedVolume {
    pub(crate) metadata: Box<dyn MetadataStore>,
    pub(crate) data: DataStore,
}

pub(crate) struct SyncRequest<'a> {
    pub(crate) volume_id: &'a VolumeId,
    pub(crate) local: &'a Path,
    pub(crate) state: Option<&'a Path>,
    pub(crate) resolutions: &'a [PathBuf],
    pub(crate) transfers: NonZeroUsize,
}

pub(crate) async fn sync_once(volume: &ManagedVolume, request: SyncRequest<'_>) -> Result<u64> {
    let paths = ReplicaPaths::resolve(request.local, request.state)?;
    let _lock = ReplicaLock::acquire(&paths)?;
    let mut state = ReplicaState::load_or_new(request.volume_id, &paths)?;
    recover(volume, &paths, &mut state, request.transfers).await?;
    if state.publication.is_none() {
        crate::replica::clear_staging(&paths)?;
    }

    let observed = volume.metadata.observe(request.volume_id).await?;
    let remote = remote_manifest(
        volume,
        &state,
        &observed.head.cursor,
        &observed.head.checkpoint,
    )
    .await?;

    if state.common.is_none() {
        let local = crate::replica::scan(&paths, None, false)?;
        if !local.entries.is_empty() && !remote.entries.is_empty() {
            bail!("local and remote trees are both non-empty without a common base");
        }
        if local.entries.is_empty() {
            materialize(
                volume,
                &paths,
                &mut state,
                observed.head.cursor,
                remote,
                request.transfers,
            )
            .await?;
            return Ok(state.common.as_ref().unwrap().cursor.generation);
        }
    }

    let base = state
        .common
        .as_ref()
        .map(|value| &value.manifest)
        .cloned()
        .unwrap_or_default();
    let resolved = resolution_paths(request.resolutions)?;
    if !state.conflicts.is_empty() {
        let unresolved = state
            .conflicts
            .iter()
            .map(|conflict| conflict.path.as_str())
            .filter(|path| !resolved.contains(*path))
            .collect::<Vec<_>>();
        if !unresolved.is_empty() {
            bail!(
                "unresolved Managed Sync conflicts: {}",
                unresolved.join(", ")
            );
        }
        if state
            .conflicts
            .iter()
            .any(|conflict| conflict.remote_cursor != observed.head.cursor)
        {
            bail!("remote generation changed after conflict resolution was selected");
        }
    }

    let stable_local = crate::replica::scan(&paths, Some(&base), true)?;
    if state.conflicts.is_empty()
        && state
            .common
            .as_ref()
            .is_some_and(|common| common.cursor == observed.head.cursor)
        && stable_local == remote
    {
        return Ok(observed.head.cursor.generation);
    }
    let (target, conflicts) = merge(
        &base,
        &stable_local,
        &remote,
        &observed.head.cursor,
        &resolved,
    );
    if !conflicts.is_empty() {
        state.conflicts = conflicts;
        state.save(&paths)?;
        crate::replica::clear_staging(&paths)?;
        bail!("Managed Sync conflict; inspect status and rerun with --resolve PATH");
    }
    state.conflicts.clear();

    if target == remote {
        materialize(
            volume,
            &paths,
            &mut state,
            observed.head.cursor,
            target,
            request.transfers,
        )
        .await?;
        return Ok(state.common.as_ref().unwrap().cursor.generation);
    }

    let changes = diff(&remote, &target);
    let operation = OperationId::parse(Uuid::new_v4().to_string())?;
    let pending = PendingPublication {
        operation: operation.clone(),
        parent: observed.head.cursor.clone(),
        source: stable_local.clone(),
        target: target.clone(),
        changes: changes.clone(),
    };
    crate::replica::sync_staging(&paths)?;
    state.publication = Some(pending);
    state.save(&paths)?;
    upload_changes(volume, &paths, &changes, request.transfers).await?;
    if !publication_source_unchanged(&paths, &stable_local)? {
        state.publication = None;
        state.save(&paths)?;
        crate::replica::clear_staging(&paths)?;
        bail!("local tree changed while publication was prepared; rerun sync");
    }
    let commit = CommitRecord::new(
        request.volume_id.clone(),
        observed.head.cursor.clone(),
        operation,
        changes,
    )?;
    match volume.metadata.publish(&observed, commit).await? {
        PublicationOutcome::Committed(cursor) | PublicationOutcome::AlreadyCommitted(cursor) => {
            if !publication_source_unchanged(&paths, &stable_local)? {
                bail!("publication committed but the local tree changed; rerun sync");
            }
            state.publication = None;
            state.save(&paths)?;
            materialize(
                volume,
                &paths,
                &mut state,
                cursor,
                target,
                request.transfers,
            )
            .await?;
            Ok(state.common.as_ref().unwrap().cursor.generation)
        }
        PublicationOutcome::Conflict(actual) => {
            state.publication = None;
            state.save(&paths)?;
            crate::replica::clear_staging(&paths)?;
            bail!(
                "publication was stale at generation {}; rerun sync to reconcile",
                actual.head.cursor.generation
            )
        }
        PublicationOutcome::Unknown => {
            bail!("publication result is unknown; rerun sync to resolve the recorded operation")
        }
    }
}

async fn recover(
    volume: &ManagedVolume,
    paths: &ReplicaPaths,
    state: &mut ReplicaState,
    transfers: NonZeroUsize,
) -> Result<()> {
    if let Some(pending) = state.publication.clone() {
        match volume
            .metadata
            .resolve(&state.volume_id, &pending.operation)
            .await?
        {
            Some(cursor) => {
                if publication_source_unchanged(paths, &pending.source)? {
                    state.publication = None;
                    state.save(paths)?;
                    materialize(volume, paths, state, cursor, pending.target, transfers).await?;
                } else {
                    state.publication = None;
                    state.save(paths)?;
                    crate::replica::clear_staging(paths)?;
                }
            }
            None => {
                let observed = volume.metadata.observe(&state.volume_id).await?;
                if observed.head.cursor == pending.parent {
                    if !publication_source_unchanged(paths, &pending.source)? {
                        state.publication = None;
                        state.save(paths)?;
                        crate::replica::clear_staging(paths)?;
                        return Ok(());
                    }
                    upload_changes(volume, paths, &pending.changes, transfers).await?;
                    let commit = CommitRecord::new(
                        state.volume_id.clone(),
                        pending.parent,
                        pending.operation,
                        pending.changes,
                    )?;
                    match volume.metadata.publish(&observed, commit).await? {
                        PublicationOutcome::Committed(cursor)
                        | PublicationOutcome::AlreadyCommitted(cursor) => {
                            if publication_source_unchanged(paths, &pending.source)? {
                                state.publication = None;
                                state.save(paths)?;
                                materialize(
                                    volume,
                                    paths,
                                    state,
                                    cursor,
                                    pending.target,
                                    transfers,
                                )
                                .await?;
                            } else {
                                state.publication = None;
                                state.save(paths)?;
                                crate::replica::clear_staging(paths)?;
                            }
                        }
                        PublicationOutcome::Unknown => {
                            bail!("recorded publication result remains unknown")
                        }
                        PublicationOutcome::Conflict(_) => {
                            state.publication = None;
                            state.save(paths)?;
                        }
                    }
                } else {
                    state.publication = None;
                    state.save(paths)?;
                }
            }
        }
    }
    if let Some(pending) = state.materialization.clone() {
        materialize(
            volume,
            paths,
            state,
            pending.target,
            pending.manifest,
            transfers,
        )
        .await?;
    }
    Ok(())
}

fn publication_source_unchanged(paths: &ReplicaPaths, source: &Manifest) -> Result<bool> {
    Ok(crate::replica::scan(paths, Some(source), false)? == *source)
}

async fn remote_manifest(
    volume: &ManagedVolume,
    state: &ReplicaState,
    target: &Cursor,
    checkpoint: &Cursor,
) -> Result<Manifest> {
    let (mut cursor, mut manifest) = match &state.common {
        Some(common) if common.cursor.generation <= target.generation => {
            (common.cursor.clone(), common.manifest.clone())
        }
        _ => {
            let value = volume
                .metadata
                .checkpoint(&state.volume_id, checkpoint)
                .await?;
            (value.cursor, value.manifest)
        }
    };
    for commit in volume
        .metadata
        .changes(&state.volume_id, &cursor, target)
        .await?
    {
        if commit.parent != cursor {
            bail!("Managed change log is not consecutive");
        }
        manifest = manifest.apply(&commit.changes)?;
        cursor = commit.cursor;
    }
    if cursor != *target {
        bail!("Managed change log did not reach its fixed target cursor");
    }
    Ok(manifest)
}

fn diff(parent: &Manifest, target: &Manifest) -> Vec<NamespaceChange> {
    let mut changes = Vec::new();
    for (path, node) in &parent.entries {
        if !target.entries.contains_key(path) {
            changes.push(NamespaceChange::Remove {
                path: path.clone(),
                removed: node.id.clone(),
            });
        }
    }
    for (path, node) in &target.entries {
        let previous = parent.entries.get(path);
        if previous != Some(node) {
            changes.push(NamespaceChange::Put {
                path: path.clone(),
                node: node.clone(),
                replaces: previous.map(|value| value.id.clone()),
            });
        }
    }
    changes
}

fn merge(
    base: &Manifest,
    local: &Manifest,
    remote: &Manifest,
    cursor: &Cursor,
    resolved: &BTreeSet<String>,
) -> (Manifest, Vec<Conflict>) {
    let base_identities = identities(base);
    let local_identities = identities(local);
    let remote_identities = identities(remote);
    let mut identity_choices = BTreeMap::<String, Option<Node>>::new();
    let mut conflicts = Vec::new();
    for (id, (base_path, base_node)) in &base_identities {
        let local_value = local_identities.get(id);
        let remote_value = remote_identities.get(id);
        let local_path = local_value.map(|(path, _)| path.as_str());
        let remote_path = remote_value.map(|(path, _)| path.as_str());
        let local_relocated = local_path != Some(base_path.as_str());
        let remote_relocated = remote_path != Some(base_path.as_str());
        let divergent_location = local_relocated && remote_relocated && local_path != remote_path;
        let rename_vs_edit = local_path.is_some()
            && local_relocated
            && remote_path == Some(base_path.as_str())
            && remote_value.is_some_and(|(_, node)| node != base_node)
            || remote_path.is_some()
                && remote_relocated
                && local_path == Some(base_path.as_str())
                && local_value.is_some_and(|(_, node)| node != base_node);
        if !divergent_location && !rename_vs_edit {
            continue;
        }
        for path in [Some(base_path.as_str()), local_path, remote_path]
            .into_iter()
            .flatten()
        {
            identity_choices.insert(path.to_owned(), local.entries.get(path).cloned());
        }
        if !resolved.contains(base_path) {
            conflicts.push(Conflict {
                path: base_path.clone(),
                kind: ConflictKind::DivergentRename,
                base: Some(base_node.clone()),
                local: local_value.map(|(_, node)| node.clone()),
                remote: remote_value.map(|(_, node)| node.clone()),
                remote_cursor: cursor.clone(),
            });
        }
    }
    let paths = base
        .entries
        .keys()
        .chain(local.entries.keys())
        .chain(remote.entries.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut entries = BTreeMap::new();
    for path in paths {
        let base_node = base.entries.get(&path);
        let local_node = local.entries.get(&path);
        let remote_node = remote.entries.get(&path);
        let selected = if let Some(node) = identity_choices.get(&path) {
            node.as_ref()
        } else if resolved.contains(&path) {
            local_node
        } else if local_node == base_node {
            remote_node
        } else if remote_node == base_node || local_node == remote_node {
            local_node
        } else {
            conflicts.push(Conflict {
                path: path.clone(),
                kind: conflict_kind(base_node, local_node, remote_node),
                base: base_node.cloned(),
                local: local_node.cloned(),
                remote: remote_node.cloned(),
                remote_cursor: cursor.clone(),
            });
            local_node
        };
        if let Some(node) = selected {
            entries.insert(path, node.clone());
        }
    }
    (Manifest { entries }, conflicts)
}

fn identities(manifest: &Manifest) -> BTreeMap<crate::model::NodeId, (String, Node)> {
    manifest
        .entries
        .iter()
        .map(|(path, node)| (node.id.clone(), (path.clone(), node.clone())))
        .collect()
}

fn conflict_kind(base: Option<&Node>, local: Option<&Node>, remote: Option<&Node>) -> ConflictKind {
    match (base, local, remote) {
        (Some(_), None, Some(_)) | (Some(_), Some(_), None) => ConflictKind::DeleteVsModify,
        (_, Some(a), Some(b))
            if std::mem::discriminant(&a.kind) != std::mem::discriminant(&b.kind) =>
        {
            ConflictKind::IncompatibleTypeReplacement
        }
        (_, Some(a), Some(b)) if a.id == b.id => ConflictKind::SameNodeModified,
        _ => ConflictKind::DivergentRename,
    }
}

async fn upload_changes(
    volume: &ManagedVolume,
    paths: &ReplicaPaths,
    changes: &[NamespaceChange],
    concurrency: NonZeroUsize,
) -> Result<()> {
    let mut files = BTreeMap::new();
    for change in changes {
        let content = match change {
            NamespaceChange::Put {
                node:
                    Node {
                        kind: NodeKind::File { content, .. },
                        ..
                    },
                ..
            } => content,
            NamespaceChange::Put { .. } | NamespaceChange::Remove { .. } => continue,
        };
        if let Some(existing) = files.insert(content.sha256.clone(), content.clone())
            && existing != *content
        {
            bail!("one content digest has inconsistent publication metadata");
        }
    }
    stream::iter(files.into_values())
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(concurrency.get(), |content| async move {
            let staged = paths.staged(&content.sha256);
            if staged.exists() {
                volume
                    .data
                    .put_file(&staged, &content.sha256, content.size)
                    .await?;
            } else {
                volume.data.verify(&content).await?;
            }
            Ok(())
        })
        .await
}

async fn materialize(
    volume: &ManagedVolume,
    paths: &ReplicaPaths,
    state: &mut ReplicaState,
    cursor: Cursor,
    target: Manifest,
    transfers: NonZeroUsize,
) -> Result<()> {
    let mut durable_directories = BTreeSet::new();
    let mut installs = Vec::new();
    match &state.materialization {
        Some(pending) if pending.target == cursor && pending.manifest == target => {}
        Some(_) => bail!("materialization intent does not match its requested target"),
        None => {
            state.materialization = Some(PendingMaterialization {
                target: cursor.clone(),
                manifest: target.clone(),
            });
            state.save(paths)?;
        }
    }
    let current = crate::replica::scan(paths, Some(&target), false)?;
    stage_materialization_files(volume, paths, &current, &target, transfers).await?;
    let mut removals = current
        .entries
        .keys()
        .filter(|path| !target.entries.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    removals.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
    for relative in removals {
        let path = paths.local.join(relative);
        if let Some(parent) = path.parent() {
            durable_directories.insert(parent.to_owned());
        }
        remove_path(&path)?;
    }
    let mut directories = target
        .entries
        .iter()
        .filter(|(_, node)| matches!(node.kind, NodeKind::Directory))
        .collect::<Vec<_>>();
    directories.sort_by_key(|(path, _)| path.matches('/').count());
    for (relative, _) in directories {
        let path = paths.local.join(relative);
        if path.exists() && !path.is_dir() {
            remove_path(&path)?;
        }
        if !path.exists() {
            if let Some(parent) = path.parent() {
                durable_directories.insert(parent.to_owned());
            }
        }
        std::fs::create_dir_all(path)?;
    }
    for (relative, node) in &target.entries {
        let NodeKind::File {
            content,
            executable,
        } = &node.kind
        else {
            continue;
        };
        if current.entries.get(relative) == Some(node) {
            continue;
        }
        let path = paths.local.join(relative);
        if path.exists() && !path.is_file() {
            remove_path(&path)?;
        }
        let parent = path
            .parent()
            .context("materialization path has no parent")?;
        std::fs::create_dir_all(parent)?;
        durable_directories.insert(parent.to_owned());
        let temporary = parent.join(format!(".ofs-apply-{}", node.id.as_str()));
        installs.push(StagedInstall {
            source: paths.staged(&content.sha256),
            temporary,
            target: path,
            executable: *executable,
        });
    }
    install_staged_files(installs, transfers).await?;
    let mut durable_directories = durable_directories.into_iter().collect::<Vec<_>>();
    durable_directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in durable_directories {
        crate::replica::sync_directory(&directory)?;
    }
    let observed = crate::replica::scan(paths, Some(&target), false)?;
    if observed != target {
        bail!("materialized tree does not match its target manifest");
    }
    state.common = Some(CommonBase {
        cursor,
        manifest: target,
    });
    state.materialization = None;
    state.conflicts.clear();
    crate::replica::clear_staging(paths)?;
    state.save(paths)
}

async fn stage_materialization_files(
    volume: &ManagedVolume,
    paths: &ReplicaPaths,
    current: &Manifest,
    target: &Manifest,
    transfers: NonZeroUsize,
) -> Result<()> {
    let mut contents = BTreeMap::new();
    for (relative, node) in &target.entries {
        if current.entries.get(relative) == Some(node) {
            continue;
        }
        let NodeKind::File { content, .. } = &node.kind else {
            continue;
        };
        if let Some(existing) = contents.insert(content.sha256.clone(), content.clone())
            && existing != *content
        {
            bail!("one content digest has inconsistent materialization metadata");
        }
    }
    if contents.is_empty() {
        return Ok(());
    }
    crate::replica::prepare_staging(paths)?;
    stream::iter(contents.into_values())
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(transfers.get(), |content| async move {
            if crate::replica::staged_content_matches(paths, &content)? {
                return Ok(());
            }
            crate::replica::discard_staged_content(paths, &content)?;
            volume
                .data
                .fetch(&content, &paths.staged(&content.sha256))
                .await
        })
        .await?;
    crate::replica::sync_staging(paths)
}

fn install_staged_file(
    staged: &Path,
    temporary: &Path,
    target: &Path,
    executable: bool,
) -> Result<()> {
    if temporary.exists() {
        remove_path(temporary)?;
    }
    let mut input = File::open(staged)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)?;
    std::io::copy(&mut input, &mut output)?;
    output.flush()?;
    output.sync_all()?;
    drop(output);
    set_executable(temporary, executable)?;
    std::fs::rename(temporary, target)?;
    Ok(())
}

struct StagedInstall {
    source: PathBuf,
    temporary: PathBuf,
    target: PathBuf,
    executable: bool,
}

async fn install_staged_files(installs: Vec<StagedInstall>, transfers: NonZeroUsize) -> Result<()> {
    stream::iter(installs)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(transfers.get(), |install| async move {
            tokio::task::spawn_blocking(move || {
                install_staged_file(
                    &install.source,
                    &install.temporary,
                    &install.target,
                    install.executable,
                )
            })
            .await
            .context("join staged file installation")?
        })
        .await
}

fn remove_path(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_dir() {
        std::fs::remove_dir(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_executable(path: &Path, executable: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path)?.permissions();
    let mode = if executable {
        permissions.mode() | 0o111
    } else {
        permissions.mode() & !0o111
    };
    permissions.set_mode(mode);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path, _executable: bool) -> Result<()> {
    Ok(())
}

fn resolution_paths(paths: &[PathBuf]) -> Result<BTreeSet<String>> {
    paths
        .iter()
        .map(|path| {
            let value = path
                .to_str()
                .context("resolution path must be UTF-8")?
                .replace('\\', "/");
            if value.is_empty()
                || value.starts_with('/')
                || value.split('/').any(|part| part == "..")
            {
                bail!("resolution path must be a portable relative path");
            }
            Ok(value)
        })
        .collect()
}
