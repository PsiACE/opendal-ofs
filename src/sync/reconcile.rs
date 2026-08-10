// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

use super::path::{SnapshotEntry, SnapshotTree, subtree};
use super::staging::{TargetEntry, TargetFile};
use super::{ConflictRecord, ReplicaState, StagedTree, TargetManifest};
use crate::filesystem::{NodeId, NodeKind};

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
            TargetFile::from(entry.file.expect("remote file has a version")),
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
                        .expect("remote file has a version")
                        .into();
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

fn reconcile_remote_renames(
    base: &SnapshotTree<'_>,
    local: &StagedTree,
    remote: &SnapshotTree<'_>,
    handled: &mut BTreeSet<String>,
    plan: &mut ReconcilePlan,
) -> Result<()> {
    let remote_by_node = unique_remote_nodes(remote);
    for old_path in base.paths.keys() {
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
            plan.target.remove(old_path);
            plan.select_file(
                new_path.clone(),
                renamed,
                if renamed_digest == base_digest {
                    TargetEdit::Reuse(old_path.clone())
                } else {
                    TargetEdit::Materialize
                },
            );
        } else if old_local.is_none() && new_local.is_some_and(|file| file.0 == renamed_digest) {
            if new_local.is_some_and(|file| file.1 != renamed_executable) {
                plan.target.select_attributes(
                    new_path,
                    TargetFile::from(renamed.file.expect("remote rename is a file")),
                    renamed_executable,
                )?;
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
    let local_paths = &local.source.entries;
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
        if let Some(identity) = entry.local.native_identity {
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

    let renames = compact_renames(renames)?;
    validate_subtree_renames(replica, base, local_paths, &renames)?;

    for (from, path) in &renames {
        for (source, target) in
            rename_subtree(base.expect("a remembered rename has a base"), from, path)?
        {
            if handled.contains(&source) && handled.contains(&target) {
                continue;
            }
            let base_entry = base
                .and_then(|tree| tree.get(&source))
                .expect("validated rename source is in the base");
            let remote_source = remote.get(&source);
            let remote_target = remote.get(&target);
            if remote_source.is_some_and(|entry| remote_matches_base(entry, base_entry))
                && remote_target.is_none()
            {
                handled.insert(source);
                handled.insert(target);
                *publish = true;
            } else if remote_target.is_some_and(|entry| entry.node.id == base_entry.node.id)
                && remote_source.is_none()
                && remote_target.is_some_and(|entry| local_matches_remote(local, &target, entry))
            {
                handled.insert(source);
                handled.insert(target);
            } else {
                handled.insert(source);
                handled.insert(target.clone());
                conflicts.push(ConflictRecord {
                    path: target.clone(),
                    local_digest: local.source.file(&target).map(|file| file.logical_digest),
                    remote_digest: remote_target.or(remote_source).and_then(digest),
                });
            }
        }
    }

    reject_unidentified_moves(replica, base, local, handled)?;
    Ok(renames)
}

fn compact_renames(renames: BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    let mut roots = BTreeMap::new();
    for (source, target) in renames {
        if let Some((ancestor, mapped)) = covering_mapping(&roots, &source) {
            if mapped_path(ancestor, mapped, &source) != target {
                bail!("local directory rename overlaps another local move");
            }
            continue;
        }
        roots.insert(source, target);
    }
    let mut targets = BTreeMap::new();
    for (source, target) in &roots {
        if covering_mapping(&targets, target).is_some() {
            bail!("remembered local renames overlap at their targets");
        }
        if targets.insert(target.clone(), source.clone()).is_some() {
            bail!("remembered local renames contain more than one source for a target");
        }
    }
    Ok(roots)
}

fn validate_subtree_renames(
    replica: &ReplicaState,
    base: Option<&SnapshotTree<'_>>,
    local: &BTreeMap<String, TargetEntry>,
    renames: &BTreeMap<String, String>,
) -> Result<()> {
    for (from, path) in renames {
        let tree = base.context("remembered local rename has no common base")?;
        tree.get(from)
            .with_context(|| format!("remembered rename source {from:?} is not in the base"))?;
        for (source, target) in rename_subtree(tree, from, path)? {
            let installed = replica
                .installed
                .get(&source)
                .with_context(|| format!("remembered rename source {source:?} is not installed"))?;
            let current = local
                .get(&target)
                .with_context(|| format!("remembered rename target {target:?} is not staged"))?;
            if local.contains_key(&source) || replica.installed.contains_key(&target) {
                bail!(
                    "remembered rename {source:?} to {target:?} no longer describes the local tree"
                );
            }
            let current = &current.local;
            if installed.native_identity.is_none()
                || installed.native_identity != current.native_identity
                || installed.kind != current.kind
            {
                bail!("local directory subtree was copied without stable identity");
            }
        }
    }
    Ok(())
}

fn rename_subtree<'a>(
    base: &'a SnapshotTree<'a>,
    source: &'a str,
    target: &'a str,
) -> Result<impl Iterator<Item = (String, String)> + 'a> {
    if base.get(source).is_none() {
        bail!("remembered rename source {source:?} is not in the base");
    }
    Ok(subtree(&base.paths, source)
        .map(move |(path, _)| (path.clone(), mapped_path(source, target, path))))
}

fn covering_mapping<'a>(
    mappings: &'a BTreeMap<String, String>,
    path: &str,
) -> Option<(&'a str, &'a str)> {
    let mut parent = path;
    while let Some((next, _)) = parent.rsplit_once('/') {
        parent = next;
        if let Some((source, target)) = mappings.get_key_value(parent) {
            return Some((source, target));
        }
    }
    None
}

fn mapped_path(source: &str, target: &str, path: &str) -> String {
    format!("{target}{}", &path[source.len()..])
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
        NodeKind::Directory => local.source.file(path).is_none(),
    }
}

fn local_file(local: &StagedTree, path: &str) -> Option<([u8; 32], bool)> {
    let file = local.source.file(path)?;
    let entry = local.source.entries.get(path)?;
    (entry.local.kind == NodeKind::RegularFile)
        .then_some((file.logical_digest, entry.local.executable))
}

fn reject_unidentified_moves(
    replica: &ReplicaState,
    base: Option<&SnapshotTree<'_>>,
    local: &StagedTree,
    handled: &mut BTreeSet<String>,
) -> Result<()> {
    let local_paths = &local.source.entries;
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
            .is_some_and(|entry| entry.local.native_identity.is_some())
    });
    let crosses_devices = deleted.iter().any(|from| {
        base.and_then(|tree| tree.get(from))
            .is_some_and(|entry| entry.node.kind == NodeKind::Directory)
            && added.iter().any(|path| {
                local.source.file(path).is_none()
                    && replica.installed[from]
                        .native_identity
                        .zip(local_paths[path].local.native_identity)
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
    for path in remote.paths.keys() {
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

fn digest(entry: SnapshotEntry<'_>) -> Option<[u8; 32]> {
    entry.file.map(|file| file.logical_digest)
}

fn executable(entry: SnapshotEntry<'_>) -> Option<bool> {
    entry.file.map(|_| entry.node.attributes.executable)
}
