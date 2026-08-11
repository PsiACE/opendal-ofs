// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, Generation, NodeId, NodeKind, OperationId,
    VolumeSnapshot,
};

use super::{ConflictRecord, SyncError};

pub(crate) struct ReconcilePlan {
    pub(crate) target: VolumeSnapshot,
    pub(crate) conflicts: Vec<ConflictRecord>,
    pub(crate) publish: bool,
}

#[derive(Clone, Copy)]
enum Source {
    Local,
    Remote,
}

pub(crate) fn reconcile(
    common: &VolumeSnapshot,
    local: &VolumeSnapshot,
    remote: &VolumeSnapshot,
    resolved: &BTreeSet<String>,
) -> Result<ReconcilePlan, SyncError> {
    common.validate()?;
    local.validate()?;
    remote.validate()?;
    if common.volume_id != local.volume_id
        || common.volume_id != remote.volume_id
        || common.cursor.sequence() > remote.cursor.sequence()
    {
        return Err(SyncError::new("reconciliation ancestry is invalid"));
    }

    let common_paths = common.paths()?;
    let local_paths = local.paths()?;
    let remote_paths = remote.paths()?;
    let directory_conflicts = directory_conflicts(
        common,
        local,
        remote,
        &common_paths,
        &local_paths,
        &remote_paths,
    );
    let mut conflicts = Vec::new();
    let mut resolved_conflicts = BTreeSet::new();
    let mut force_local = Vec::new();
    let mut blocked = Vec::new();
    for path in directory_conflicts {
        if resolved.contains(&path) {
            resolved_conflicts.insert(path.clone());
            force_local.push(path);
        } else {
            conflicts.push(conflict(&path, local, remote, &local_paths, &remote_paths));
            blocked.push(path);
        }
    }

    let paths = common_paths
        .keys()
        .chain(local_paths.keys())
        .chain(remote_paths.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut selected = BTreeMap::new();
    for path in paths {
        if covered_by(&blocked, &path) {
            continue;
        }
        if covered_by(&force_local, &path) {
            if let Some(node) = local_paths.get(&path) {
                selected.insert(path, (Source::Local, *node));
            }
            continue;
        }

        let base = common_paths.get(&path).copied();
        let local_entry = local_paths.get(&path).copied();
        let remote_entry = remote_paths.get(&path).copied();
        let local_changed = !same_entry(common, base, local, local_entry);
        let remote_changed = !same_entry(common, base, remote, remote_entry);
        let choice = match (local_changed, remote_changed) {
            (false, false) | (false, true) => remote_entry.map(|node| (Source::Remote, node)),
            (true, false) => local_entry.map(|node| (Source::Local, node)),
            (true, true) if same_entry(local, local_entry, remote, remote_entry) => {
                remote_entry.map(|node| (Source::Remote, node))
            }
            (true, true) if resolved.contains(&path) => {
                resolved_conflicts.insert(path.clone());
                local_entry.map(|node| (Source::Local, node))
            }
            (true, true) => {
                conflicts.push(conflict(&path, local, remote, &local_paths, &remote_paths));
                None
            }
        };
        if let Some(choice) = choice {
            selected.insert(path, choice);
        }
    }

    if resolved_conflicts != *resolved {
        let missing = resolved
            .difference(&resolved_conflicts)
            .cloned()
            .collect::<Vec<_>>();
        return Err(SyncError::new(format!(
            "no unresolved conflict exists for {missing:?}"
        )));
    }
    if !conflicts.is_empty() {
        conflicts.sort_by(|left, right| left.path.cmp(&right.path));
        conflicts.dedup_by(|left, right| left.path == right.path);
        return Ok(ReconcilePlan {
            target: remote.clone(),
            conflicts,
            publish: false,
        });
    }

    let mut target = build_target(local, remote, selected)?;
    if same_namespace(&target, remote) {
        target = remote.clone();
        return Ok(ReconcilePlan {
            target,
            conflicts,
            publish: false,
        });
    }
    let sequence = remote
        .cursor
        .sequence()
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .ok_or_else(|| SyncError::new("Managed change sequence overflows"))?;
    target.cursor = ChangeCursor::at(sequence, OperationId::generate());
    target.validate()?;
    Ok(ReconcilePlan {
        target,
        conflicts,
        publish: true,
    })
}

fn directory_conflicts(
    common: &VolumeSnapshot,
    local: &VolumeSnapshot,
    remote: &VolumeSnapshot,
    common_paths: &BTreeMap<String, NodeId>,
    local_paths: &BTreeMap<String, NodeId>,
    remote_paths: &BTreeMap<String, NodeId>,
) -> Vec<String> {
    common_paths
        .iter()
        .filter(|(_, node)| common.nodes[node].kind == NodeKind::Directory)
        .filter_map(|(path, common_node)| {
            let local_kept = local_paths
                .get(path)
                .is_some_and(|node| local.nodes[node].kind == NodeKind::Directory);
            let remote_kept = remote_paths
                .get(path)
                .is_some_and(|node| remote.nodes[node].kind == NodeKind::Directory);
            let local_overlap = !local_kept
                && remote_kept
                && subtree_changed(common, remote, common_paths, remote_paths, path);
            let remote_overlap = !remote_kept
                && local_kept
                && subtree_changed(common, local, common_paths, local_paths, path);
            (local_overlap || remote_overlap).then(|| {
                let _ = common_node;
                path.clone()
            })
        })
        .collect()
}

fn subtree_changed(
    common: &VolumeSnapshot,
    side: &VolumeSnapshot,
    common_paths: &BTreeMap<String, NodeId>,
    side_paths: &BTreeMap<String, NodeId>,
    directory: &str,
) -> bool {
    common_paths
        .keys()
        .chain(side_paths.keys())
        .filter(|path| is_descendant(directory, path))
        .any(|path| {
            !same_entry(
                common,
                common_paths.get(path).copied(),
                side,
                side_paths.get(path).copied(),
            )
        })
}

fn covered_by(prefixes: &[String], path: &str) -> bool {
    prefixes
        .iter()
        .any(|prefix| path == prefix || is_descendant(prefix, path))
}

fn is_descendant(directory: &str, path: &str) -> bool {
    path.strip_prefix(directory)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

fn same_entry(
    left: &VolumeSnapshot,
    left_id: Option<NodeId>,
    right: &VolumeSnapshot,
    right_id: Option<NodeId>,
) -> bool {
    let (Some(left_id), Some(right_id)) = (left_id, right_id) else {
        return left_id.is_none() && right_id.is_none();
    };
    let left_node = &left.nodes[&left_id];
    let right_node = &right.nodes[&right_id];
    left_id == right_id
        && left_node.kind == right_node.kind
        && left_node.attributes == right_node.attributes
        && match (left_node.file_version, right_node.file_version) {
            (Some(left_version), Some(right_version)) => {
                left.file_versions[&left_version] == right.file_versions[&right_version]
            }
            (None, None) => true,
            _ => false,
        }
}

fn conflict(
    path: &str,
    local: &VolumeSnapshot,
    remote: &VolumeSnapshot,
    local_paths: &BTreeMap<String, NodeId>,
    remote_paths: &BTreeMap<String, NodeId>,
) -> ConflictRecord {
    ConflictRecord {
        path: path.to_owned(),
        local_digest: digest(local, local_paths.get(path).copied()),
        remote_digest: digest(remote, remote_paths.get(path).copied()),
    }
}

fn digest(snapshot: &VolumeSnapshot, node: Option<NodeId>) -> Option<[u8; 32]> {
    let node = &snapshot.nodes[&node?];
    let version = snapshot.file_versions.get(&node.file_version?)?;
    Some(*version.digest().as_bytes())
}

fn build_target(
    local: &VolumeSnapshot,
    remote: &VolumeSnapshot,
    selected: BTreeMap<String, (Source, NodeId)>,
) -> Result<VolumeSnapshot, SyncError> {
    let next_generation = Generation::from_bytes(
        remote
            .cursor
            .sequence()
            .checked_add(1)
            .ok_or_else(|| SyncError::new("Managed change sequence overflows"))?
            .to_be_bytes()
            .to_vec(),
    );
    let mut nodes = BTreeMap::new();
    let mut file_versions = BTreeMap::new();
    nodes.insert(remote.root, remote.nodes[&remote.root].clone());
    for (source, node_id) in selected.values() {
        let snapshot = match source {
            Source::Local => local,
            Source::Remote => remote,
        };
        let node = snapshot.nodes[node_id].clone();
        if let Some(version_id) = node.file_version {
            file_versions
                .entry(version_id)
                .or_insert_with(|| snapshot.file_versions[&version_id].clone());
        }
        nodes.insert(*node_id, node);
    }

    let mut directory_entries = BTreeMap::<NodeId, BTreeMap<String, DirectoryEntry>>::new();
    directory_entries.insert(remote.root, BTreeMap::new());
    for (path, (_, node_id)) in &selected {
        if nodes[node_id].kind == NodeKind::Directory {
            directory_entries.entry(*node_id).or_default();
        }
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        let parent_id = if parent.is_empty() {
            remote.root
        } else {
            selected
                .get(parent)
                .map(|(_, node)| *node)
                .ok_or_else(|| SyncError::new(format!("merged path {path:?} has no parent")))?
        };
        if nodes[&parent_id].kind != NodeKind::Directory {
            return Err(SyncError::new(format!(
                "merged path {path:?} has a non-directory parent"
            )));
        }
        directory_entries.entry(parent_id).or_default().insert(
            name.to_owned(),
            DirectoryEntry {
                node: *node_id,
                kind: nodes[node_id].kind,
            },
        );
    }

    let mut directories = BTreeMap::new();
    for (node, entries) in directory_entries {
        let generation = local
            .directories
            .get(&node)
            .filter(|directory| directory.entries == entries)
            .or_else(|| {
                remote
                    .directories
                    .get(&node)
                    .filter(|directory| directory.entries == entries)
            })
            .map_or_else(
                || next_generation.clone(),
                |directory| directory.generation.clone(),
            );
        directories.insert(
            node,
            DirectoryRecord {
                node,
                generation,
                entries,
            },
        );
    }

    let target = VolumeSnapshot {
        volume_id: remote.volume_id,
        cursor: remote.cursor,
        root: remote.root,
        nodes,
        directories,
        file_versions,
    };
    target.validate()?;
    Ok(target)
}

fn same_namespace(left: &VolumeSnapshot, right: &VolumeSnapshot) -> bool {
    left.root == right.root
        && left.nodes == right.nodes
        && left.directories == right.directories
        && left.file_versions == right.file_versions
}
