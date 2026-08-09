// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

use super::path::{descendants, subtree};
use super::{ConflictRecord, ReplicaState, StagedTree};
use crate::filesystem::NodeKind;
use crate::filesystem::{FileVersionId, Generation, NodeId, VolumeSnapshot};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReconcilePlan {
    pub actions: Vec<ReconcileAction>,
    pub local_renames: BTreeMap<String, String>,
    pub merge_remote_directories: bool,
    pub publish_local_directories: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReconcileAction {
    KeepLocal,
    InstallRemote {
        path: String,
        version: FileVersionId,
        digest: [u8; 32],
        executable: bool,
    },
    DeleteLocal {
        path: String,
    },
    ApplyRemoteAttributes {
        path: String,
        digest: [u8; 32],
        executable: bool,
    },
    PublishLocal,
    PublishRename,
    Conflict(ConflictRecord),
    Unsupported {
        path: String,
        reason: &'static str,
    },
}

#[derive(Clone)]
struct RemoteFile {
    node: NodeId,
    generation: Generation,
    version: FileVersionId,
    digest: [u8; 32],
    executable: bool,
}

#[derive(Clone)]
struct RemoteDirectory {
    node: NodeId,
    generation: Generation,
    directory_generation: Generation,
}

/// Reconcile staged local file content with one authoritative volume snapshot.
///
/// The result contains decisions only. Installing or publishing data is a
/// separate operation, so this function performs no I/O.
pub(crate) fn reconcile(
    replica: &ReplicaState,
    local: &StagedTree,
    remote: &VolumeSnapshot,
    base_path_ids: &BTreeMap<String, NodeId>,
    remote_path_ids: &BTreeMap<String, NodeId>,
) -> Result<ReconcilePlan> {
    if replica.volume != remote.volume_id {
        bail!("replica state and remote namespace belong to different volumes");
    }
    if replica.common.sequence() > remote.cursor.sequence() {
        bail!("replica base cursor is ahead of the remote namespace");
    }

    let base_paths = replica
        .authority
        .as_ref()
        .map(|snapshot| remote_paths(snapshot, base_path_ids))
        .transpose()?
        .unwrap_or_default();
    let remote_paths = remote_paths(remote, remote_path_ids)?;
    let remote_files = remote_paths
        .iter()
        .filter_map(|(path, value)| match value {
            RemotePath::File(file) => Some((path.clone(), file.clone())),
            RemotePath::Directory(_) => None,
        })
        .collect::<BTreeMap<_, _>>();
    let local_digests = local
        .files()
        .iter()
        .map(|(path, file)| (path.clone(), file.digest))
        .collect::<BTreeMap<_, _>>();
    validate_directory_deletions(replica, local, &base_paths, &remote_paths)?;

    let mut actions = Vec::new();
    let mut handled = BTreeSet::new();
    reconcile_remote_renames(
        &base_paths,
        local,
        &remote_files,
        &mut handled,
        &mut actions,
    );
    let local_renames = reconcile_local_renames(
        replica,
        &base_paths,
        local,
        &remote_paths,
        &mut handled,
        &mut actions,
    )?;

    let mut paths = base_paths.keys().cloned().collect::<BTreeSet<_>>();
    paths.extend(local.logical().entries().keys().cloned());
    paths.extend(remote_paths.keys().cloned());
    let mut merge_remote_directories = false;
    let mut publish_local_directories = false;
    for path in paths {
        if handled.contains(&path) {
            continue;
        }
        let base = base_paths.get(&path);
        let local_digest = local_digests.get(&path).copied();
        let remote_path = remote_paths.get(&path);
        let remote_digest = remote_path.and_then(RemotePath::digest);
        let base_kind = base.map(RemotePath::kind);
        let local_kind = local.logical().entries().get(&path).map(|entry| entry.kind);
        let remote_kind = remote_path.map(RemotePath::kind);
        let base_directory = base_kind == Some(super::LocalKind::Directory);
        let local_directory = local_kind == Some(super::LocalKind::Directory);
        let remote_directory = remote_kind == Some(super::LocalKind::Directory);
        if local_directory != remote_directory {
            merge_remote_directories |= remote_directory != base_directory;
            publish_local_directories |= local_directory != base_directory;
        }
        if local_kind != remote_kind {
            if local_kind != base_kind && remote_kind != base_kind {
                actions.push(ReconcileAction::Unsupported {
                    path,
                    reason: "local and remote path types changed incompatibly",
                });
            } else if local_kind == base_kind {
                match (local_kind, remote_path) {
                    (Some(super::LocalKind::File), None | Some(RemotePath::Directory(_))) => {
                        actions.push(ReconcileAction::DeleteLocal { path });
                    }
                    (_, Some(RemotePath::File(file))) => {
                        actions.push(install_remote(path, file.clone()));
                    }
                    _ => {}
                }
            } else {
                actions.push(ReconcileAction::PublishLocal);
            }
            continue;
        }
        if local_kind != Some(super::LocalKind::File) {
            continue;
        }
        let local_executable = local
            .logical()
            .entries()
            .get(&path)
            .map(|entry| entry.executable);
        let base_executable = base.and_then(RemotePath::executable);
        let remote_executable = remote_path.and_then(RemotePath::executable);

        let base_digest = base.and_then(RemotePath::digest);
        let local_changed = local_digest != base_digest || local_executable != base_executable;
        let remote_changed = remote_digest != base_digest
            || remote_executable != base_executable
            || match (base, remote_path) {
                (Some(base), Some(RemotePath::File(file))) => base.node() != file.node,
                (None, Some(_)) | (Some(_), None) => true,
                _ => false,
            };

        let action = if let Some(digest) = local_digest
            && Some(digest) == remote_digest
        {
            match (base_executable, local_executable, remote_executable) {
                (_, Some(local), Some(remote)) if local == remote => {
                    Some(ReconcileAction::KeepLocal)
                }
                (Some(base), Some(local), Some(remote)) if local == base => {
                    Some(ReconcileAction::ApplyRemoteAttributes {
                        path,
                        digest,
                        executable: remote,
                    })
                }
                (Some(base), Some(_), Some(remote)) if remote == base => {
                    Some(ReconcileAction::PublishLocal)
                }
                _ => Some(ReconcileAction::Conflict(ConflictRecord {
                    path,
                    local_digest,
                    remote_digest,
                })),
            }
        } else {
            match (local_changed, remote_changed) {
                (false, false) if local_digest.is_some() => Some(ReconcileAction::KeepLocal),
                (false, false) => None,
                (true, false) => Some(ReconcileAction::PublishLocal),
                (false, true) => Some(remote_action(path, remote_path)),
                (true, true) if local_digest == remote_digest => {
                    local_digest.map(|_| ReconcileAction::KeepLocal)
                }
                (true, true) => Some(ReconcileAction::Conflict(ConflictRecord {
                    path,
                    local_digest,
                    remote_digest,
                })),
            }
        };
        if let Some(action) = action {
            actions.push(action);
        }
    }

    Ok(ReconcilePlan {
        actions,
        local_renames,
        merge_remote_directories,
        publish_local_directories,
    })
}

fn validate_directory_deletions(
    replica: &ReplicaState,
    local: &StagedTree,
    base: &BTreeMap<String, RemotePath>,
    remote: &BTreeMap<String, RemotePath>,
) -> Result<()> {
    for (path, entry) in base {
        if entry.kind() != super::LocalKind::Directory {
            continue;
        }
        let local_kept = local
            .logical()
            .entries()
            .get(path)
            .is_some_and(|entry| entry.kind == super::LocalKind::Directory);
        let remote_kept = remote
            .get(path)
            .is_some_and(|entry| entry.kind() == super::LocalKind::Directory);
        if !remote_kept && local_kept && local_subtree_changed(replica, local, base, path) {
            bail!("cannot reconcile {path:?}: remote directory deletion overlaps local changes");
        }
        if !local_kept && remote_kept && remote_subtree_changed(base, remote, path) {
            bail!("cannot reconcile {path:?}: local directory deletion overlaps remote changes");
        }
    }

    Ok(())
}

fn local_subtree_changed(
    replica: &ReplicaState,
    local: &StagedTree,
    base: &BTreeMap<String, RemotePath>,
    directory: &str,
) -> bool {
    let paths = subtree(&replica.installed, directory)
        .map(|(path, _)| path)
        .chain(subtree(local.logical().entries(), directory).map(|(path, _)| path))
        .collect::<BTreeSet<_>>();
    paths.into_iter().any(|path| {
        match (
            replica.installed.get(path),
            local.logical().entries().get(path),
        ) {
            (Some(installed), Some(current)) => {
                current.kind != base[path].kind()
                    || current.kind == super::LocalKind::File
                        && (installed.local_identity != current.native_identity
                            || installed.local_size != Some(current.size)
                            || installed.local_modified.as_deref()
                                != Some(current.modified.as_str())
                            || installed.local_executable != Some(current.executable))
            }
            (None, None) => false,
            _ => true,
        }
    })
}

fn remote_subtree_changed(
    base: &BTreeMap<String, RemotePath>,
    remote: &BTreeMap<String, RemotePath>,
    directory: &str,
) -> bool {
    let paths = subtree(base, directory)
        .map(|(path, _)| path)
        .chain(subtree(remote, directory).map(|(path, _)| path))
        .collect::<BTreeSet<_>>();
    paths
        .into_iter()
        .any(|path| match (base.get(path), remote.get(path)) {
            (Some(base), Some(remote)) => !remote_matches_base(remote, base),
            (None, None) => false,
            _ => true,
        })
}

fn remote_action(path: String, remote: Option<&RemotePath>) -> ReconcileAction {
    match remote {
        Some(RemotePath::File(file)) => ReconcileAction::InstallRemote {
            path,
            version: file.version,
            digest: file.digest,
            executable: file.executable,
        },
        None => ReconcileAction::DeleteLocal { path },
        Some(RemotePath::Directory(_)) => ReconcileAction::Unsupported {
            path,
            reason: "directory installation belongs to namespace reconciliation",
        },
    }
}

fn reconcile_remote_renames(
    base: &BTreeMap<String, RemotePath>,
    local: &StagedTree,
    remote: &BTreeMap<String, RemoteFile>,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) {
    let remote_by_node = unique_remote_nodes(remote);
    for (old_path, base) in base {
        let Some(base_digest) = base.digest() else {
            continue;
        };
        let Some(Some(new_path)) = remote_by_node.get(&base.node()) else {
            continue;
        };
        if new_path == old_path {
            continue;
        }
        handled.insert(old_path.clone());
        handled.insert(new_path.clone());
        let old_local = local_file(local, old_path);
        let new_local = local_file(local, new_path);
        let renamed = remote[new_path].clone();
        let base_executable = base
            .executable()
            .expect("a file digest has file attributes");
        if old_local == Some((base_digest, base_executable)) && new_local.is_none() {
            actions.push(ReconcileAction::DeleteLocal {
                path: old_path.clone(),
            });
            actions.push(install_remote(new_path.clone(), renamed));
        } else if old_local.is_none() && new_local.is_some_and(|file| file.0 == renamed.digest) {
            if new_local.is_some_and(|file| file.1 == renamed.executable) {
                actions.push(ReconcileAction::KeepLocal);
            } else {
                actions.push(ReconcileAction::ApplyRemoteAttributes {
                    path: new_path.clone(),
                    digest: renamed.digest,
                    executable: renamed.executable,
                });
            }
        } else {
            actions.push(ReconcileAction::Unsupported {
                path: old_path.clone(),
                reason: "remote rename overlaps a local change and requires reconciliation",
            });
        }
    }
}

fn reconcile_local_renames(
    replica: &ReplicaState,
    base_paths: &BTreeMap<String, RemotePath>,
    local: &StagedTree,
    remote: &BTreeMap<String, RemotePath>,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) -> Result<BTreeMap<String, String>> {
    let local_paths = local.logical().entries();
    let mut renames = replica
        .pending
        .as_ref()
        .map(|intent| intent.renames.clone())
        .unwrap_or_default();
    let mut base_by_identity = BTreeMap::new();
    for (path, entry) in &replica.installed {
        if let Some(identity) = entry.local_identity {
            base_by_identity
                .entry(identity)
                .and_modify(|value: &mut Option<String>| *value = None)
                .or_insert_with(|| Some(path.clone()));
        }
    }
    let mut local_by_identity = BTreeMap::new();
    for (path, entry) in local_paths {
        if let Some(identity) = entry.native_identity {
            local_by_identity
                .entry(identity)
                .and_modify(|value: &mut Option<String>| *value = None)
                .or_insert_with(|| Some(path.clone()));
        }
    }
    for (identity, from) in base_by_identity {
        let (Some(from), Some(Some(path))) = (from, local_by_identity.get(&identity)) else {
            continue;
        };
        if !local_paths.contains_key(&from) && !replica.installed.contains_key(path) {
            renames.insert(from, path.clone());
        }
    }

    let targets = renames.values().collect::<BTreeSet<_>>();
    if targets.len() != renames.len() {
        bail!("remembered local renames contain more than one source for a target");
    }
    validate_subtree_renames(base_paths, local_paths, &renames)?;

    for (from, path) in &renames {
        if handled.contains(from) && handled.contains(path) {
            continue;
        }
        let base = base_paths
            .get(from)
            .with_context(|| format!("remembered rename source {from:?} is not in the base"))?;
        if !local_paths.contains_key(path) {
            bail!("remembered rename target {path:?} is not staged");
        }
        if local_paths.contains_key(from) || replica.installed.contains_key(path) {
            bail!("remembered rename {from:?} to {path:?} no longer describes the local tree");
        }
        let remote_source = remote.get(from);
        let remote_target = remote.get(path);
        if remote_source.is_some_and(|entry| remote_matches_base(entry, base))
            && remote_target.is_none()
        {
            handled.insert(from.clone());
            handled.insert(path.clone());
            actions.push(ReconcileAction::PublishRename);
        } else if remote_target.is_some_and(|entry| entry.node() == base.node())
            && remote_source.is_none()
            && remote_target.is_some_and(|entry| local_matches_remote(local, path, entry))
        {
            handled.insert(from.clone());
            handled.insert(path.clone());
            actions.push(ReconcileAction::KeepLocal);
        } else {
            handled.insert(from.clone());
            handled.insert(path.clone());
            actions.push(ReconcileAction::Conflict(ConflictRecord {
                path: path.clone(),
                local_digest: local.files().get(path).map(|file| file.digest),
                remote_digest: remote_target.or(remote_source).and_then(RemotePath::digest),
            }));
        }
    }

    reject_unidentified_moves(replica, base_paths, local, handled, actions);
    Ok(renames)
}

fn validate_subtree_renames(
    base: &BTreeMap<String, RemotePath>,
    local: &BTreeMap<String, super::local::LocalEntry>,
    renames: &BTreeMap<String, String>,
) -> Result<()> {
    for (from, path) in renames {
        let entry = base
            .get(from)
            .with_context(|| format!("remembered rename source {from:?} is not in the base"))?;
        if matches!(entry, RemotePath::File(_)) {
            continue;
        }
        let target_prefix = format!("{path}/");
        for (child_from, _) in descendants(base, from) {
            let suffix = &child_from[from.len() + 1..];
            let child_path = format!("{target_prefix}{suffix}");
            if local.contains_key(&child_path)
                && renames.get(child_from).map(String::as_str) != Some(child_path.as_str())
            {
                bail!("local directory subtree was copied without stable identity");
            }
        }
        let source_prefix = format!("{from}/");
        for (child_from, child_path) in renames {
            let Some(suffix) = child_from.strip_prefix(&source_prefix) else {
                continue;
            };
            if child_path != &format!("{target_prefix}{suffix}") {
                bail!("local directory rename overlaps another local move");
            }
        }
    }
    Ok(())
}

fn remote_matches_base(remote: &RemotePath, base: &RemotePath) -> bool {
    if remote.node() != base.node() || remote.generation() != base.generation() {
        return false;
    }
    match remote {
        RemotePath::File(file) => base.digest() == Some(file.digest),
        RemotePath::Directory(directory) => match base {
            RemotePath::Directory(base) => {
                base.directory_generation == directory.directory_generation
            }
            RemotePath::File(_) => false,
        },
    }
}

fn local_matches_remote(local: &StagedTree, path: &str, remote: &RemotePath) -> bool {
    match remote {
        RemotePath::File(file) => local_file(local, path) == Some((file.digest, file.executable)),
        RemotePath::Directory(_) => !local.files().contains_key(path),
    }
}

fn local_file(local: &StagedTree, path: &str) -> Option<([u8; 32], bool)> {
    let file = local.files().get(path)?;
    let entry = local.logical().entries().get(path)?;
    (entry.kind == super::LocalKind::File).then_some((file.digest, entry.executable))
}

fn reject_unidentified_moves(
    replica: &ReplicaState,
    base: &BTreeMap<String, RemotePath>,
    local: &StagedTree,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) {
    let local_paths = local.logical().entries();
    let deleted = replica
        .installed
        .iter()
        .filter(|(path, _)| !local_paths.contains_key(*path))
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let added = local_paths
        .keys()
        .filter(|path| !replica.installed.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    let deleted = deleted
        .into_iter()
        .filter(|path| !handled.contains(path))
        .collect::<Vec<_>>();
    let added = added
        .into_iter()
        .filter(|path| !handled.contains(path))
        .collect::<Vec<_>>();
    if deleted.is_empty() || added.is_empty() {
        return;
    }
    let identities_are_reliable = deleted.iter().all(|path| {
        replica
            .installed
            .get(path)
            .is_some_and(|entry| entry.local_identity.is_some())
    }) && added.iter().all(|path| {
        local_paths
            .get(path)
            .is_some_and(|entry| entry.native_identity.is_some())
    });
    let crosses_devices = deleted.iter().any(|from| {
        matches!(base[from], RemotePath::Directory(_))
            && added.iter().any(|path| {
                !local.files().contains_key(path)
                    && replica.installed[from]
                        .local_identity
                        .zip(local_paths[path].native_identity)
                        .is_some_and(|(from, to)| from.device != to.device)
            })
    });
    if identities_are_reliable && !crosses_devices {
        return;
    }
    let suspects = deleted.into_iter().chain(added).collect::<BTreeSet<_>>();
    handled.extend(suspects.iter().cloned());
    for path in suspects {
        actions.push(ReconcileAction::Unsupported {
            path,
            reason: "local move lacks a stable same-filesystem identity",
        });
    }
}

fn unique_remote_nodes(remote: &BTreeMap<String, RemoteFile>) -> BTreeMap<NodeId, Option<String>> {
    let mut nodes = BTreeMap::new();
    for (path, file) in remote {
        nodes
            .entry(file.node)
            .and_modify(|current| *current = None)
            .or_insert_with(|| Some(path.clone()));
    }
    nodes
}

fn install_remote(path: String, file: RemoteFile) -> ReconcileAction {
    ReconcileAction::InstallRemote {
        path,
        version: file.version,
        digest: file.digest,
        executable: file.executable,
    }
}

#[derive(Clone)]
enum RemotePath {
    Directory(RemoteDirectory),
    File(RemoteFile),
}

impl RemotePath {
    fn kind(&self) -> super::LocalKind {
        match self {
            Self::Directory(_) => super::LocalKind::Directory,
            Self::File(_) => super::LocalKind::File,
        }
    }

    fn node(&self) -> NodeId {
        match self {
            Self::Directory(directory) => directory.node,
            Self::File(file) => file.node,
        }
    }

    fn generation(&self) -> &Generation {
        match self {
            Self::Directory(directory) => &directory.generation,
            Self::File(file) => &file.generation,
        }
    }

    fn digest(&self) -> Option<[u8; 32]> {
        match self {
            Self::Directory(_) => None,
            Self::File(file) => Some(file.digest),
        }
    }

    fn executable(&self) -> Option<bool> {
        match self {
            Self::Directory(_) => None,
            Self::File(file) => Some(file.executable),
        }
    }
}

fn remote_paths(
    snapshot: &VolumeSnapshot,
    path_ids: &BTreeMap<String, NodeId>,
) -> Result<BTreeMap<String, RemotePath>> {
    let mut paths = BTreeMap::new();
    for (path, node) in path_ids {
        let record = snapshot
            .nodes
            .get(node)
            .context("remote namespace references a missing node")?;
        match record.kind {
            NodeKind::RegularFile => {
                let version_id = record
                    .file_version
                    .context("remote file has no file version")?;
                let version = snapshot
                    .file_versions
                    .get(&version_id)
                    .context("remote file version is missing")?;
                paths.insert(
                    path.clone(),
                    RemotePath::File(RemoteFile {
                        node: *node,
                        generation: record.generation.clone(),
                        version: version_id,
                        digest: version.logical_digest,
                        executable: record.attributes.executable,
                    }),
                );
            }
            NodeKind::Directory => {
                let directory = snapshot
                    .directories
                    .get(node)
                    .context("remote directory record is missing")?;
                paths.insert(
                    path.clone(),
                    RemotePath::Directory(RemoteDirectory {
                        node: *node,
                        generation: record.generation.clone(),
                        directory_generation: directory.generation.clone(),
                    }),
                );
            }
        }
    }
    Ok(paths)
}
