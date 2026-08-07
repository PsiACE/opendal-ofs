// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

use super::{ConflictRecord, ReplicaState, StagedTree};
use crate::filesystem::NodeKind;
use crate::filesystem::{ChangeCursor, FileVersionId, Generation, NodeId};
use crate::managed::namespace::{FileVersionRecord, NamespaceSnapshot};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcilePlan {
    pub base: ChangeCursor,
    pub remote: ChangeCursor,
    pub actions: Vec<ReconcileAction>,
    pub local_renames: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileAction {
    KeepLocal {
        path: String,
        digest: Option<[u8; 32]>,
    },
    InstallRemote {
        path: String,
        node: NodeId,
        version: FileVersionId,
        digest: [u8; 32],
    },
    DeleteLocal {
        path: String,
    },
    PublishLocal {
        path: String,
        digest: Option<[u8; 32]>,
    },
    PublishRename {
        from: String,
        path: String,
        node: NodeId,
    },
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
}

#[derive(Clone)]
struct RemoteDirectory {
    node: NodeId,
    generation: Generation,
    directory_generation: Generation,
}

/// Reconcile staged local file content with one authoritative Managed snapshot.
///
/// The result contains decisions only. Installing or publishing data is a
/// separate operation, so this function performs no I/O.
pub fn reconcile(
    replica: &ReplicaState,
    local: &StagedTree,
    remote: &NamespaceSnapshot,
) -> Result<ReconcilePlan> {
    if replica.volume != remote.volume_id {
        bail!("replica state and remote namespace belong to different volumes");
    }
    if replica.common.sequence() > remote.cursor.sequence() {
        bail!("replica base cursor is ahead of the remote namespace");
    }

    let remote_paths = remote_paths(remote)?;
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

    let mut actions = Vec::new();
    let mut handled = BTreeSet::new();
    reconcile_remote_renames(
        replica,
        &local_digests,
        &remote_files,
        &mut handled,
        &mut actions,
    );
    let local_renames =
        reconcile_local_renames(replica, local, &remote_paths, &mut handled, &mut actions)?;

    let mut paths = replica.base.keys().cloned().collect::<BTreeSet<_>>();
    paths.extend(local_digests.keys().cloned());
    paths.extend(remote_paths.keys().cloned());
    for path in paths {
        if handled.contains(&path) {
            continue;
        }
        let base = replica.base.get(&path);
        let local_digest = local_digests.get(&path).copied();
        let remote_path = remote_paths.get(&path);
        let remote_digest = remote_path.and_then(RemotePath::digest);

        if base.is_some_and(|entry| entry.digest.is_none())
            || matches!(remote_path, Some(RemotePath::Directory(_)))
        {
            if local_digest.is_some() {
                actions.push(ReconcileAction::Unsupported {
                    path,
                    reason: "file and directory changes require namespace reconciliation",
                });
            }
            continue;
        }

        let base_digest = base.and_then(|entry| entry.digest);
        let local_changed = local_digest != base_digest;
        let remote_changed = remote_digest != base_digest
            || match (base, remote_path) {
                (Some(base), Some(RemotePath::File(file))) => base.node != file.node,
                (None, Some(_)) | (Some(_), None) => true,
                _ => false,
            };

        let action = if local_digest.is_some() && local_digest == remote_digest {
            Some(ReconcileAction::KeepLocal {
                path,
                digest: local_digest,
            })
        } else {
            match (local_changed, remote_changed) {
                (false, false) if local_digest.is_some() => Some(ReconcileAction::KeepLocal {
                    path,
                    digest: local_digest,
                }),
                (false, false) => None,
                (true, false) => Some(ReconcileAction::PublishLocal {
                    path,
                    digest: local_digest,
                }),
                (false, true) => Some(remote_action(path, remote_path)),
                (true, true) if local_digest == remote_digest => {
                    local_digest.map(|digest| ReconcileAction::KeepLocal {
                        path,
                        digest: Some(digest),
                    })
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
        base: replica.common,
        remote: remote.cursor,
        actions,
        local_renames,
    })
}

fn remote_action(path: String, remote: Option<&RemotePath>) -> ReconcileAction {
    match remote {
        Some(RemotePath::File(file)) => ReconcileAction::InstallRemote {
            path,
            node: file.node,
            version: file.version,
            digest: file.digest,
        },
        None => ReconcileAction::DeleteLocal { path },
        Some(RemotePath::Directory(_)) => ReconcileAction::Unsupported {
            path,
            reason: "directory installation belongs to namespace reconciliation",
        },
    }
}

fn reconcile_remote_renames(
    replica: &ReplicaState,
    local: &BTreeMap<String, [u8; 32]>,
    remote: &BTreeMap<String, RemoteFile>,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) {
    let remote_by_node = unique_remote_nodes(remote);
    for (old_path, base) in &replica.base {
        let Some(base_digest) = base.digest else {
            continue;
        };
        let Some(Some(new_path)) = remote_by_node.get(&base.node) else {
            continue;
        };
        if new_path == old_path {
            continue;
        }
        handled.insert(old_path.clone());
        handled.insert(new_path.clone());
        let old_local = local.get(old_path).copied();
        let new_local = local.get(new_path).copied();
        let renamed = remote[new_path].clone();
        if old_local == Some(base_digest) && new_local.is_none() {
            actions.push(ReconcileAction::DeleteLocal {
                path: old_path.clone(),
            });
            actions.push(install_remote(new_path.clone(), renamed));
        } else if old_local.is_none() && new_local == Some(renamed.digest) {
            actions.push(ReconcileAction::KeepLocal {
                path: new_path.clone(),
                digest: new_local,
            });
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
    local: &StagedTree,
    remote: &BTreeMap<String, RemotePath>,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) -> Result<BTreeMap<String, String>> {
    let local_paths = local.source_identities();
    let mut renames = replica
        .pending
        .as_ref()
        .map(|intent| intent.renames.clone())
        .unwrap_or_default();
    let mut base_by_identity = BTreeMap::new();
    for (path, entry) in &replica.base {
        if let Some(identity) = entry.local_identity {
            base_by_identity
                .entry(identity)
                .and_modify(|value: &mut Option<String>| *value = None)
                .or_insert_with(|| Some(path.clone()));
        }
    }
    let mut local_by_identity = BTreeMap::new();
    for (path, identity) in local_paths {
        if let Some(identity) = identity {
            local_by_identity
                .entry(*identity)
                .and_modify(|value: &mut Option<String>| *value = None)
                .or_insert_with(|| Some(path.clone()));
        }
    }
    for (identity, from) in base_by_identity {
        let (Some(from), Some(Some(path))) = (from, local_by_identity.get(&identity)) else {
            continue;
        };
        if !local_paths.contains_key(&from) && !replica.base.contains_key(path) {
            renames.insert(from, path.clone());
        }
    }

    let targets = renames.values().collect::<BTreeSet<_>>();
    if targets.len() != renames.len() {
        bail!("remembered local renames contain more than one source for a target");
    }
    validate_subtree_renames(replica, local_paths, &renames)?;

    for (from, path) in &renames {
        if handled.contains(from) && handled.contains(path) {
            continue;
        }
        let base = replica
            .base
            .get(from)
            .with_context(|| format!("remembered rename source {from:?} is not in the base"))?;
        if !local_paths.contains_key(path) {
            bail!("remembered rename target {path:?} is not staged");
        }
        if local_paths.contains_key(from) || replica.base.contains_key(path) {
            bail!("remembered rename {from:?} to {path:?} no longer describes the local tree");
        }
        let remote_source = remote.get(from);
        let remote_target = remote.get(path);
        if remote_source.is_some_and(|entry| remote_matches_base(entry, base))
            && remote_target.is_none()
        {
            handled.insert(from.clone());
            handled.insert(path.clone());
            actions.push(ReconcileAction::PublishRename {
                from: from.clone(),
                path: path.clone(),
                node: base.node,
            });
        } else if remote_target.is_some_and(|entry| entry.node() == base.node)
            && remote_source.is_none()
            && remote_target.is_some_and(|entry| local_matches_remote(local, path, entry))
        {
            handled.insert(from.clone());
            handled.insert(path.clone());
            actions.push(ReconcileAction::KeepLocal {
                path: path.clone(),
                digest: local.files().get(path).map(|file| file.digest),
            });
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

    reject_unidentified_moves(replica, local, handled, actions);
    Ok(renames)
}

fn validate_subtree_renames(
    replica: &ReplicaState,
    local: &BTreeMap<String, Option<super::local::NativeIdentity>>,
    renames: &BTreeMap<String, String>,
) -> Result<()> {
    for (from, path) in renames {
        if replica.base[from].digest.is_some() {
            continue;
        }
        let source_prefix = format!("{from}/");
        let target_prefix = format!("{path}/");
        for child_from in replica
            .base
            .keys()
            .filter(|candidate| candidate.starts_with(&source_prefix))
        {
            let suffix = &child_from[source_prefix.len()..];
            let child_path = format!("{target_prefix}{suffix}");
            if local.contains_key(&child_path)
                && renames.get(child_from).map(String::as_str) != Some(child_path.as_str())
            {
                bail!("local directory subtree was copied without stable identity");
            }
        }
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

fn remote_matches_base(remote: &RemotePath, base: &super::BaseEntry) -> bool {
    if remote.node() != base.node || remote.generation() != &base.generation {
        return false;
    }
    match remote {
        RemotePath::File(file) => base.digest == Some(file.digest),
        RemotePath::Directory(directory) => {
            base.digest.is_none()
                && base.directory_generation.as_ref() == Some(&directory.directory_generation)
        }
    }
}

fn local_matches_remote(local: &StagedTree, path: &str, remote: &RemotePath) -> bool {
    match remote {
        RemotePath::File(file) => local
            .files()
            .get(path)
            .is_some_and(|local| local.digest == file.digest),
        RemotePath::Directory(_) => !local.files().contains_key(path),
    }
}

fn reject_unidentified_moves(
    replica: &ReplicaState,
    local: &StagedTree,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) {
    let local_paths = local.source_identities();
    let deleted = replica
        .base
        .iter()
        .filter(|(path, _)| !local_paths.contains_key(*path))
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let added = local_paths
        .keys()
        .filter(|path| !replica.base.contains_key(*path))
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
            .base
            .get(path)
            .is_some_and(|entry| entry.local_identity.is_some())
    }) && added
        .iter()
        .all(|path| local_paths.get(path).is_some_and(Option::is_some));
    let crosses_devices = deleted.iter().any(|from| {
        replica.base[from].digest.is_none()
            && added.iter().any(|path| {
                !local.files().contains_key(path)
                    && replica.base[from]
                        .local_identity
                        .zip(local_paths[path])
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
        node: file.node,
        version: file.version,
        digest: file.digest,
    }
}

#[derive(Clone)]
enum RemotePath {
    Directory(RemoteDirectory),
    File(RemoteFile),
}

impl RemotePath {
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
}

fn remote_paths(snapshot: &NamespaceSnapshot) -> Result<BTreeMap<String, RemotePath>> {
    let mut paths = BTreeMap::new();
    let mut pending = vec![(String::new(), snapshot.root)];
    let mut expanded = BTreeSet::new();
    while let Some((path, node)) = pending.pop() {
        let record = snapshot
            .nodes
            .get(&node)
            .context("remote namespace references a missing node")?;
        match record.kind {
            NodeKind::RegularFile => {
                let version_id = record
                    .file_version
                    .context("remote file has no file version")?;
                let version: &FileVersionRecord = snapshot
                    .file_versions
                    .get(&version_id)
                    .context("remote file version is missing")?;
                if !path.is_empty() {
                    paths.insert(
                        path,
                        RemotePath::File(RemoteFile {
                            node,
                            generation: record.generation.clone(),
                            version: version_id,
                            digest: version.logical_digest,
                        }),
                    );
                }
            }
            NodeKind::Directory => {
                if !expanded.insert(node) {
                    bail!("remote namespace is not a directory tree");
                }
                let directory = snapshot
                    .directories
                    .get(&node)
                    .context("remote directory record is missing")?;
                if !path.is_empty() {
                    paths.insert(
                        path.clone(),
                        RemotePath::Directory(RemoteDirectory {
                            node,
                            generation: record.generation.clone(),
                            directory_generation: directory.generation.clone(),
                        }),
                    );
                }
                for (name, entry) in directory.entries.iter().rev() {
                    let child = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{path}/{name}")
                    };
                    pending.push((child, entry.node));
                }
            }
        }
    }
    Ok(paths)
}
