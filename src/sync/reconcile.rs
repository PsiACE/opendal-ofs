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
use crate::filesystem::{ChangeCursor, FileVersionId, NodeId};
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
        digest: [u8; 32],
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
    let local_renames =
        reconcile_local_renames(replica, local, &remote_files, &mut handled, &mut actions)?;

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

fn reconcile_local_renames(
    replica: &ReplicaState,
    local: &StagedTree,
    remote: &BTreeMap<String, RemoteFile>,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) -> Result<BTreeMap<String, String>> {
    let files = local.files();
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
    for (path, file) in files {
        if let Some(identity) = file.source_identity {
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
        if !files.contains_key(&from) && !replica.base.contains_key(path) {
            renames.insert(from, path.clone());
        }
    }

    let targets = renames.values().collect::<BTreeSet<_>>();
    if targets.len() != renames.len() {
        bail!("remembered local renames contain more than one source for a target");
    }

    for (from, path) in &renames {
        if handled.contains(from) && handled.contains(path) {
            continue;
        }
        let base = replica
            .base
            .get(from)
            .with_context(|| format!("remembered rename source {from:?} is not in the base"))?;
        let base_digest = base
            .digest
            .with_context(|| format!("remembered rename source {from:?} is not a file"))?;
        let digest = files
            .get(path)
            .with_context(|| format!("remembered rename target {path:?} is not staged"))?
            .digest;
        if files.contains_key(from) || replica.base.contains_key(path) {
            bail!("remembered rename {from:?} to {path:?} no longer describes the local tree");
        }
        let remote_source = remote.get(from);
        let remote_target = remote.get(path);
        if remote_source.is_some_and(|file| file.node == base.node && file.digest == base_digest)
            && remote_target.is_none()
        {
            handled.insert(from.clone());
            handled.insert(path.clone());
            actions.push(ReconcileAction::PublishRename {
                from: from.clone(),
                path: path.clone(),
                node: base.node,
                digest,
            });
        } else if remote_target.is_some_and(|file| file.node == base.node)
            && remote_source.is_none()
            && remote_target.is_some_and(|file| file.digest == digest)
        {
            handled.insert(from.clone());
            handled.insert(path.clone());
            actions.push(ReconcileAction::KeepLocal {
                path: path.clone(),
                digest: Some(digest),
            });
        } else {
            handled.insert(from.clone());
            handled.insert(path.clone());
            actions.push(ReconcileAction::Conflict(ConflictRecord {
                path: path.clone(),
                local_digest: Some(digest),
                remote_digest: remote_target.or(remote_source).map(|file| file.digest),
            }));
        }
    }

    reject_unidentified_moves(replica, files, handled, actions);
    Ok(renames)
}

fn reject_unidentified_moves(
    replica: &ReplicaState,
    local: &BTreeMap<String, super::StagedFile>,
    handled: &mut BTreeSet<String>,
    actions: &mut Vec<ReconcileAction>,
) {
    let deleted = replica
        .base
        .iter()
        .filter(|(path, entry)| entry.digest.is_some() && !local.contains_key(*path))
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let added = local
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
    }) && added.iter().all(|path| {
        local
            .get(path)
            .is_some_and(|file| file.source_identity.is_some())
    });
    if identities_are_reliable {
        return;
    }
    let suspects = deleted.into_iter().chain(added).collect::<BTreeSet<_>>();
    handled.extend(suspects.iter().cloned());
    for path in suspects {
        actions.push(ReconcileAction::Unsupported {
            path,
            reason: "local move lacks a stable native file identity",
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
