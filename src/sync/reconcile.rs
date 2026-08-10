// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};

use super::path::{SnapshotEntry, SnapshotTree, subtree};
use super::{ConflictRecord, ReplicaState, StagedTree, TargetManifest};
use crate::filesystem::NodeKind;

mod renames;

use renames::{reconcile_local_renames, reconcile_remote_renames};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReconcilePlan {
    pub publish: bool,
    pub target: TargetManifest,
    pub edits: BTreeMap<String, TargetEdit>,
    pub conflicts: Vec<ConflictRecord>,
    pub renames: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TargetEdit {
    Materialize,
    Reuse(String),
    Directory,
}

impl ReconcilePlan {
    fn select_file(&mut self, path: String, entry: SnapshotEntry<'_>, edit: TargetEdit) {
        self.target.select_file(
            path.clone(),
            entry.file.expect("remote file has a version"),
            entry.node.attributes.executable,
        );
        self.edits.insert(path, edit);
    }

    fn select_directory(&mut self, path: String) {
        self.target.select_directory(path.clone());
        self.edits.insert(path, TargetEdit::Directory);
    }
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
    if replica.volume != remote.snapshot.volume_id {
        bail!("replica state and remote namespace belong to different volumes");
    }
    if replica.common().sequence() > remote.snapshot.cursor.sequence() {
        bail!("replica base cursor is ahead of the remote namespace");
    }

    validate_directory_deletions(replica, local, base, remote)?;

    let mut plan = ReconcilePlan {
        publish: false,
        target: local.source.clone(),
        edits: BTreeMap::new(),
        conflicts: Vec::new(),
        renames: BTreeMap::new(),
    };
    let mut handled = BTreeSet::new();
    if let Some(base) = base {
        reconcile_remote_renames(base, local, remote, &mut handled, &mut plan)?;
    }
    plan.renames = reconcile_local_renames(
        replica,
        base,
        local,
        remote,
        &mut handled,
        &mut plan.conflicts,
        &mut plan.publish,
    )?;

    let mut paths = base
        .into_iter()
        .flat_map(|tree| tree.paths.keys().cloned())
        .collect::<BTreeSet<_>>();
    paths.extend(local.source.entries.keys().cloned());
    paths.extend(remote.paths.keys().cloned());
    for path in paths {
        if handled.contains(&path) {
            continue;
        }
        let base_entry = base.and_then(|tree| tree.get(&path));
        let local_digest = local.source.file(&path).map(|file| file.logical_digest);
        let remote_entry = remote.get(&path);
        let remote_digest = remote_entry.and_then(digest);
        let base_kind = base_entry.map(|entry| entry.node.kind);
        let local_kind = local
            .source
            .entries
            .get(&path)
            .map(|entry| entry.local.kind);
        let remote_kind = remote_entry.map(|entry| entry.node.kind);
        if local_kind != remote_kind {
            if local_kind != base_kind && remote_kind != base_kind {
                bail!(
                    "cannot reconcile {path:?}: local and remote path types changed incompatibly"
                );
            } else if local_kind == base_kind {
                match remote_entry {
                    Some(entry) if entry.node.kind == NodeKind::RegularFile => {
                        plan.select_file(path, entry, TargetEdit::Materialize);
                    }
                    Some(_) => plan.select_directory(path),
                    None => plan.target.remove(&path),
                }
            } else {
                plan.publish = true;
            }
            continue;
        }
        if local_kind != Some(NodeKind::RegularFile) {
            continue;
        }
        let local_executable = local
            .source
            .entries
            .get(&path)
            .map(|entry| entry.local.executable);
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
                    let version = remote_entry
                        .and_then(|entry| entry.file)
                        .expect("remote file has a version");
                    plan.target.select_attributes(&path, version, remote)?;
                }
                (Some(base), Some(_), Some(remote)) if remote == base => {
                    plan.publish = true;
                }
                _ => plan.conflicts.push(ConflictRecord {
                    path,
                    local_digest,
                    remote_digest,
                }),
            }
        } else {
            match (local_changed, remote_changed) {
                (false, false) => {}
                (true, false) => plan.publish = true,
                (false, true) => match remote_entry {
                    Some(entry) => plan.select_file(path, entry, TargetEdit::Materialize),
                    None => plan.target.remove(&path),
                },
                (true, true) if local_digest == remote_digest => {}
                (true, true) => plan.conflicts.push(ConflictRecord {
                    path,
                    local_digest,
                    remote_digest,
                }),
            }
        }
    }

    Ok(plan)
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
    for path in base.paths.keys() {
        if base
            .get(path)
            .is_none_or(|entry| entry.node.kind != NodeKind::Directory)
        {
            continue;
        }
        let local_kept = local
            .source
            .entries
            .get(path)
            .is_some_and(|entry| entry.local.kind == NodeKind::Directory);
        let remote_kept = remote
            .get(path)
            .is_some_and(|entry| entry.node.kind == NodeKind::Directory);
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
        .chain(subtree(&local.source.entries, directory).map(|(path, _)| path))
        .collect::<BTreeSet<_>>();
    paths.into_iter().any(|path| {
        match (replica.installed.get(path), local.source.entries.get(path)) {
            (Some(installed), Some(current)) => {
                current.local.kind
                    != base
                        .get(path)
                        .expect("installed path is in the base")
                        .node
                        .kind
                    || current.local.kind == NodeKind::RegularFile && installed != &current.local
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
    let paths = subtree(&base.paths, directory)
        .map(|(path, _)| path)
        .chain(subtree(&remote.paths, directory).map(|(path, _)| path))
        .collect::<BTreeSet<_>>();
    paths
        .into_iter()
        .any(|path| match (base.get(path), remote.get(path)) {
            (Some(base), Some(remote)) => !remote_matches_base(remote, base),
            (None, None) => false,
            _ => true,
        })
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

fn digest(entry: SnapshotEntry<'_>) -> Option<[u8; 32]> {
    entry.file.map(|file| file.logical_digest)
}

fn executable(entry: SnapshotEntry<'_>) -> Option<bool> {
    entry.file.map(|_| entry.node.attributes.executable)
}
