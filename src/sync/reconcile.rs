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
use crate::filesystem::{ChangeCursor, FileVersionId, NodeId};
use crate::managed::namespace::{FileVersionRecord, NamespaceSnapshot, NodeKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcilePlan {
    pub base: ChangeCursor,
    pub remote: ChangeCursor,
    pub actions: Vec<ReconcileAction>,
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
    Conflict(ConflictRecord),
    Unsupported {
        path: String,
        reason: &'static str,
    },
}

#[derive(Clone, Copy)]
struct RemoteFile {
    node: NodeId,
    version: FileVersionId,
    digest: [u8; 32],
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
            RemotePath::File(file) => Some((path.clone(), *file)),
            RemotePath::Directory => None,
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
    reject_possible_local_renames(
        replica,
        &local_digests,
        &remote_files,
        &mut handled,
        &mut actions,
    );

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
            || matches!(remote_path, Some(RemotePath::Directory))
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
        Some(RemotePath::Directory) => ReconcileAction::Unsupported {
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
        let renamed = remote[new_path];
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

fn reject_possible_local_renames(
    replica: &ReplicaState,
    local: &BTreeMap<String, [u8; 32]>,
    remote: &BTreeMap<String, RemoteFile>,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) {
    let mut suspects = BTreeSet::new();
    for (new_path, digest) in local {
        if replica.base.contains_key(new_path) || handled.contains(new_path) {
            continue;
        }
        let possible_sources = replica.base.iter().filter(|(old_path, base)| {
            base.digest == Some(*digest)
                && !local.contains_key(*old_path)
                && remote
                    .get(*old_path)
                    .is_some_and(|file| file.node == base.node && file.digest == *digest)
        });
        let sources = possible_sources
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        if sources.is_empty() {
            continue;
        }
        suspects.insert(new_path.clone());
        suspects.extend(sources);
    }
    handled.extend(suspects.iter().cloned());
    for path in suspects {
        actions.push(ReconcileAction::Unsupported {
            path,
            reason: "possible local rename lacks a stable local identity",
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

#[derive(Clone, Copy)]
enum RemotePath {
    Directory,
    File(RemoteFile),
}

impl RemotePath {
    fn digest(&self) -> Option<[u8; 32]> {
        match self {
            Self::Directory => None,
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
                if !path.is_empty() {
                    paths.insert(path.clone(), RemotePath::Directory);
                }
                let directory = snapshot
                    .directories
                    .get(&node)
                    .context("remote directory record is missing")?;
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
