// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

use super::path::{SnapshotEntry, SnapshotTree, descendants, subtree};
use super::{ConflictRecord, ReplicaState, StagedTree};
use crate::filesystem::{FileVersionId, NodeId, NodeKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReconcilePlan {
    pub publish: bool,
    pub edits: Vec<RemoteEdit>,
    pub conflicts: Vec<ConflictRecord>,
    pub renames: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RemoteEdit {
    InstallFile {
        path: String,
        version: FileVersionId,
        digest: [u8; 32],
        executable: bool,
    },
    InstallDirectory {
        path: String,
    },
    Remove {
        path: String,
    },
    SetExecutable {
        path: String,
        digest: [u8; 32],
        executable: bool,
    },
}

/// Reconcile staged local file content with one authoritative volume snapshot.
///
/// The result contains decisions only. Installing or publishing data is a
/// separate operation, so this function performs no I/O.
pub(crate) fn reconcile(
    replica: &ReplicaState,
    local: &StagedTree,
    base: Option<&SnapshotTree<'_>>,
    remote: &SnapshotTree<'_>,
) -> Result<ReconcilePlan> {
    if replica.volume != remote.snapshot().volume_id {
        bail!("replica state and remote namespace belong to different volumes");
    }
    if replica.common().sequence() > remote.snapshot().cursor.sequence() {
        bail!("replica base cursor is ahead of the remote namespace");
    }

    validate_directory_deletions(replica, local, base, remote)?;

    let mut edits = Vec::new();
    let mut conflicts = Vec::new();
    let mut publish = false;
    let mut handled = BTreeSet::new();
    if let Some(base) = base {
        reconcile_remote_renames(base, local, remote, &mut handled, &mut edits)?;
    }
    let renames = reconcile_local_renames(
        replica,
        base,
        local,
        remote,
        &mut handled,
        &mut conflicts,
        &mut publish,
    )?;

    let mut paths = base
        .into_iter()
        .flat_map(|tree| tree.paths().keys().cloned())
        .collect::<BTreeSet<_>>();
    paths.extend(local.logical().entries().keys().cloned());
    paths.extend(remote.paths().keys().cloned());
    for path in paths {
        if handled.contains(&path) {
            continue;
        }
        let base_entry = base.and_then(|tree| tree.get(&path));
        let local_digest = local.files().get(&path).map(|file| file.digest);
        let remote_entry = remote.get(&path);
        let remote_digest = remote_entry.and_then(digest);
        let base_kind = base_entry.map(kind);
        let local_kind = local.logical().entries().get(&path).map(|entry| entry.kind);
        let remote_kind = remote_entry.map(kind);
        if local_kind != remote_kind {
            if local_kind != base_kind && remote_kind != base_kind {
                bail!(
                    "cannot reconcile {path:?}: local and remote path types changed incompatibly"
                );
            } else if local_kind == base_kind {
                match remote_entry {
                    Some(entry) if kind(entry) == super::LocalKind::File => {
                        edits.push(install_file(path, entry));
                    }
                    Some(_) => edits.push(RemoteEdit::InstallDirectory { path }),
                    None => edits.push(RemoteEdit::Remove { path }),
                }
            } else {
                publish = true;
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
        let base_executable = base_entry.and_then(executable);
        let remote_executable = remote_entry.and_then(executable);

        let base_digest = base_entry.and_then(digest);
        let local_changed = local_digest != base_digest || local_executable != base_executable;
        let remote_changed = remote_digest != base_digest
            || remote_executable != base_executable
            || match (base_entry, remote_entry) {
                (Some(base), Some(remote)) => base.node.id != remote.node.id,
                (None, Some(_)) | (Some(_), None) => true,
                _ => false,
            };

        if let Some(digest) = local_digest
            && Some(digest) == remote_digest
        {
            match (base_executable, local_executable, remote_executable) {
                (_, Some(local), Some(remote)) if local == remote => {}
                (Some(base), Some(local), Some(remote)) if local == base => {
                    edits.push(RemoteEdit::SetExecutable {
                        path,
                        digest,
                        executable: remote,
                    });
                }
                (Some(base), Some(_), Some(remote)) if remote == base => {
                    publish = true;
                }
                _ => conflicts.push(ConflictRecord {
                    path,
                    local_digest,
                    remote_digest,
                }),
            }
        } else {
            match (local_changed, remote_changed) {
                (false, false) => {}
                (true, false) => publish = true,
                (false, true) => match remote_entry {
                    Some(entry) => edits.push(install_file(path, entry)),
                    None => edits.push(RemoteEdit::Remove { path }),
                },
                (true, true) if local_digest == remote_digest => {}
                (true, true) => conflicts.push(ConflictRecord {
                    path,
                    local_digest,
                    remote_digest,
                }),
            }
        }
    }

    Ok(ReconcilePlan {
        publish,
        edits,
        conflicts,
        renames,
    })
}

fn validate_directory_deletions(
    replica: &ReplicaState,
    local: &StagedTree,
    base: Option<&SnapshotTree<'_>>,
    remote: &SnapshotTree<'_>,
) -> Result<()> {
    let Some(base) = base else {
        return Ok(());
    };
    for path in base.paths().keys() {
        if base
            .get(path)
            .is_none_or(|entry| kind(entry) != super::LocalKind::Directory)
        {
            continue;
        }
        let local_kept = local
            .logical()
            .entries()
            .get(path)
            .is_some_and(|entry| entry.kind == super::LocalKind::Directory);
        let remote_kept = remote
            .get(path)
            .is_some_and(|entry| kind(entry) == super::LocalKind::Directory);
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
    base: &SnapshotTree<'_>,
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
                current.kind != kind(base.get(path).expect("installed path is in the base"))
                    || current.kind == super::LocalKind::File && installed != current
            }
            (None, None) => false,
            _ => true,
        }
    })
}

fn remote_subtree_changed(
    base: &SnapshotTree<'_>,
    remote: &SnapshotTree<'_>,
    directory: &str,
) -> bool {
    let paths = subtree(base.paths(), directory)
        .map(|(path, _)| path)
        .chain(subtree(remote.paths(), directory).map(|(path, _)| path))
        .collect::<BTreeSet<_>>();
    paths
        .into_iter()
        .any(|path| match (base.get(path), remote.get(path)) {
            (Some(base), Some(remote)) => !remote_matches_base(remote, base),
            (None, None) => false,
            _ => true,
        })
}

fn reconcile_remote_renames(
    base: &SnapshotTree<'_>,
    local: &StagedTree,
    remote: &SnapshotTree<'_>,
    handled: &mut BTreeSet<String>,
    edits: &mut Vec<RemoteEdit>,
) -> Result<()> {
    let remote_by_node = unique_remote_nodes(remote);
    for old_path in base.paths().keys() {
        let base_entry = base.get(old_path).expect("indexed base path is valid");
        let Some(base_digest) = digest(base_entry) else {
            continue;
        };
        let Some(Some(new_path)) = remote_by_node.get(&base_entry.node.id) else {
            continue;
        };
        if new_path == old_path {
            continue;
        }
        handled.insert(old_path.clone());
        handled.insert(new_path.clone());
        let old_local = local_file(local, old_path);
        let new_local = local_file(local, new_path);
        let renamed = remote
            .get(new_path)
            .expect("indexed remote rename is valid");
        let renamed_digest = digest(renamed).expect("remote rename is a file");
        let renamed_executable = executable(renamed).expect("remote rename is a file");
        let base_executable = executable(base_entry).expect("a file digest has file attributes");
        if old_local == Some((base_digest, base_executable)) && new_local.is_none() {
            edits.push(RemoteEdit::Remove {
                path: old_path.clone(),
            });
            edits.push(install_file(new_path.clone(), renamed));
        } else if old_local.is_none() && new_local.is_some_and(|file| file.0 == renamed_digest) {
            if new_local.is_some_and(|file| file.1 != renamed_executable) {
                edits.push(RemoteEdit::SetExecutable {
                    path: new_path.clone(),
                    digest: renamed_digest,
                    executable: renamed_executable,
                });
            }
        } else {
            bail!(
                "cannot reconcile {old_path:?}: remote rename overlaps a local change and requires reconciliation"
            );
        }
    }
    Ok(())
}

fn reconcile_local_renames(
    replica: &ReplicaState,
    base: Option<&SnapshotTree<'_>>,
    local: &StagedTree,
    remote: &SnapshotTree<'_>,
    handled: &mut BTreeSet<String>,
    conflicts: &mut Vec<ConflictRecord>,
    publish: &mut bool,
) -> Result<BTreeMap<String, String>> {
    let local_paths = local.logical().entries();
    let mut renames = replica
        .pending
        .as_ref()
        .map(|intent| intent.renames.clone())
        .unwrap_or_default();
    let mut base_by_identity = BTreeMap::new();
    for (path, entry) in &replica.installed {
        if let Some(identity) = entry.native_identity {
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
    validate_subtree_renames(base, local_paths, &renames)?;

    for (from, path) in &renames {
        if handled.contains(from) && handled.contains(path) {
            continue;
        }
        let base_entry = base
            .and_then(|tree| tree.get(from))
            .with_context(|| format!("remembered rename source {from:?} is not in the base"))?;
        if !local_paths.contains_key(path) {
            bail!("remembered rename target {path:?} is not staged");
        }
        if local_paths.contains_key(from) || replica.installed.contains_key(path) {
            bail!("remembered rename {from:?} to {path:?} no longer describes the local tree");
        }
        let remote_source = remote.get(from);
        let remote_target = remote.get(path);
        if remote_source.is_some_and(|entry| remote_matches_base(entry, base_entry))
            && remote_target.is_none()
        {
            handled.insert(from.clone());
            handled.insert(path.clone());
            *publish = true;
        } else if remote_target.is_some_and(|entry| entry.node.id == base_entry.node.id)
            && remote_source.is_none()
            && remote_target.is_some_and(|entry| local_matches_remote(local, path, entry))
        {
            handled.insert(from.clone());
            handled.insert(path.clone());
        } else {
            handled.insert(from.clone());
            handled.insert(path.clone());
            conflicts.push(ConflictRecord {
                path: path.clone(),
                local_digest: local.files().get(path).map(|file| file.digest),
                remote_digest: remote_target.or(remote_source).and_then(digest),
            });
        }
    }

    reject_unidentified_moves(replica, base, local, handled)?;
    Ok(renames)
}

fn validate_subtree_renames(
    base: Option<&SnapshotTree<'_>>,
    local: &BTreeMap<String, super::local::LocalEntry>,
    renames: &BTreeMap<String, String>,
) -> Result<()> {
    for (from, path) in renames {
        let entry = base
            .and_then(|tree| tree.get(from))
            .with_context(|| format!("remembered rename source {from:?} is not in the base"))?;
        if kind(entry) == super::LocalKind::File {
            continue;
        }
        let target_prefix = format!("{path}/");
        for (child_from, _) in
            descendants(base.expect("a remembered rename has a base").paths(), from)
        {
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

fn remote_matches_base(remote: SnapshotEntry<'_>, base: SnapshotEntry<'_>) -> bool {
    if remote.node.id != base.node.id || remote.node.generation != base.node.generation {
        return false;
    }
    match (remote.file, remote.directory, base.file, base.directory) {
        (Some(remote), None, Some(base), None) => base.logical_digest == remote.logical_digest,
        (None, Some(remote), None, Some(base)) => base.generation == remote.generation,
        _ => false,
    }
}

fn local_matches_remote(local: &StagedTree, path: &str, remote: SnapshotEntry<'_>) -> bool {
    match remote.node.kind {
        NodeKind::RegularFile => local_file(local, path) == digest(remote).zip(executable(remote)),
        NodeKind::Directory => !local.files().contains_key(path),
    }
}

fn local_file(local: &StagedTree, path: &str) -> Option<([u8; 32], bool)> {
    let file = local.files().get(path)?;
    let entry = local.logical().entries().get(path)?;
    (entry.kind == super::LocalKind::File).then_some((file.digest, entry.executable))
}

fn reject_unidentified_moves(
    replica: &ReplicaState,
    base: Option<&SnapshotTree<'_>>,
    local: &StagedTree,
    handled: &mut BTreeSet<String>,
) -> Result<()> {
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
        return Ok(());
    }
    let identities_are_reliable = deleted.iter().all(|path| {
        replica
            .installed
            .get(path)
            .is_some_and(|entry| entry.native_identity.is_some())
    }) && added.iter().all(|path| {
        local_paths
            .get(path)
            .is_some_and(|entry| entry.native_identity.is_some())
    });
    let crosses_devices = deleted.iter().any(|from| {
        base.and_then(|tree| tree.get(from))
            .is_some_and(|entry| kind(entry) == super::LocalKind::Directory)
            && added.iter().any(|path| {
                !local.files().contains_key(path)
                    && replica.installed[from]
                        .native_identity
                        .zip(local_paths[path].native_identity)
                        .is_some_and(|(from, to)| from.device != to.device)
            })
    });
    if identities_are_reliable && !crosses_devices {
        return Ok(());
    }
    let suspects = deleted.into_iter().chain(added).collect::<BTreeSet<_>>();
    handled.extend(suspects.iter().cloned());
    if let Some(path) = suspects.into_iter().next() {
        bail!("cannot reconcile {path:?}: local move lacks a stable same-filesystem identity");
    }
    Ok(())
}

fn unique_remote_nodes(remote: &SnapshotTree<'_>) -> BTreeMap<NodeId, Option<String>> {
    let mut nodes = BTreeMap::new();
    for path in remote.paths().keys() {
        let entry = remote.get(path).expect("indexed remote path is valid");
        if entry.node.kind != NodeKind::RegularFile {
            continue;
        }
        nodes
            .entry(entry.node.id)
            .and_modify(|current| *current = None)
            .or_insert_with(|| Some(path.clone()));
    }
    nodes
}

fn install_file(path: String, entry: SnapshotEntry<'_>) -> RemoteEdit {
    let file = entry.file.expect("remote file has a validated version");
    RemoteEdit::InstallFile {
        path,
        version: entry.node.file_version.expect("remote file has a version"),
        digest: file.logical_digest,
        executable: entry.node.attributes.executable,
    }
}

fn kind(entry: SnapshotEntry<'_>) -> super::LocalKind {
    match entry.node.kind {
        NodeKind::Directory => super::LocalKind::Directory,
        NodeKind::RegularFile => super::LocalKind::File,
    }
}

fn digest(entry: SnapshotEntry<'_>) -> Option<[u8; 32]> {
    entry.file.map(|file| file.logical_digest)
}

fn executable(entry: SnapshotEntry<'_>) -> Option<bool> {
    entry.file.map(|_| entry.node.attributes.executable)
}
