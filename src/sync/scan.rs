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
use std::path::{Path, PathBuf};

use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, FileVersion, FileVersionId, Generation,
    NodeAttributes, NodeId, NodeKind, NodeRecord, OperationId, VolumeSnapshot,
};
use crate::managed::ManagedVolume;

use super::SyncError;

pub(crate) struct ScannedTree {
    pub(crate) snapshot: VolumeSnapshot,
    pub(crate) changed_files: Vec<(PathBuf, FileVersion)>,
}

#[derive(Clone, Copy)]
struct LocalEntry {
    kind: NodeKind,
    executable: bool,
}

pub(crate) async fn scan(
    root: &Path,
    base: &VolumeSnapshot,
    volume: &ManagedVolume,
) -> Result<ScannedTree, SyncError> {
    let local = scan_paths(root)?;
    let base_paths = base.paths()?;
    let next_sequence = base
        .cursor
        .sequence()
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .ok_or_else(|| SyncError::new("Managed change sequence overflows"))?;
    let next_generation = Generation::from_bytes(next_sequence.get().to_be_bytes().to_vec());

    let mut ids = BTreeMap::new();
    ids.insert(String::new(), base.root);
    for (path, entry) in &local {
        let node = base_paths
            .get(path)
            .copied()
            .filter(|node| base.nodes[node].kind == entry.kind)
            .unwrap_or_else(NodeId::generate);
        ids.insert(path.clone(), node);
    }

    let mut file_versions = BTreeMap::new();
    let mut file_by_path = BTreeMap::<String, FileVersionId>::new();
    let mut prepared = BTreeMap::<String, FileVersion>::new();
    for (path, entry) in &local {
        if entry.kind != NodeKind::RegularFile {
            continue;
        }
        let version = volume.inspect_file(&root.join(path)).await?;
        file_by_path.insert(path.clone(), version.id);
        file_versions
            .entry(version.id)
            .or_insert_with(|| version.clone());
        prepared.insert(path.clone(), version);
    }

    let mut directories = BTreeMap::new();
    for (path, node) in &ids {
        let kind = if path.is_empty() {
            NodeKind::Directory
        } else {
            local[path].kind
        };
        if kind != NodeKind::Directory {
            continue;
        }
        let mut entries = BTreeMap::new();
        for (child_path, child) in &local {
            let (parent, name) = child_path.rsplit_once('/').unwrap_or(("", child_path));
            if parent == path {
                entries.insert(
                    name.to_owned(),
                    DirectoryEntry {
                        node: ids[child_path],
                        kind: child.kind,
                    },
                );
            }
        }
        let generation = base
            .directories
            .get(node)
            .filter(|record| record.entries == entries)
            .map_or_else(
                || next_generation.clone(),
                |record| record.generation.clone(),
            );
        directories.insert(
            *node,
            DirectoryRecord {
                node: *node,
                generation,
                entries,
            },
        );
    }

    let mut nodes = BTreeMap::new();
    for (path, node) in &ids {
        let (kind, attributes, file_version) = if path.is_empty() {
            (NodeKind::Directory, NodeAttributes::default(), None)
        } else {
            let local = local[path];
            (
                local.kind,
                NodeAttributes {
                    executable: local.executable,
                },
                file_by_path.get(path).copied(),
            )
        };
        let generation = base
            .nodes
            .get(node)
            .filter(|record| {
                record.kind == kind
                    && record.attributes == attributes
                    && record.file_version == file_version
            })
            .map_or_else(
                || next_generation.clone(),
                |record| record.generation.clone(),
            );
        nodes.insert(
            *node,
            NodeRecord {
                id: *node,
                generation,
                kind,
                attributes,
                file_version,
            },
        );
    }

    let mut snapshot = VolumeSnapshot {
        volume_id: base.volume_id,
        cursor: base.cursor,
        root: base.root,
        nodes,
        directories,
        file_versions,
    };
    snapshot.validate()?;
    if same_namespace(&snapshot, base) {
        return Ok(ScannedTree {
            snapshot: base.clone(),
            changed_files: Vec::new(),
        });
    }

    let operation = OperationId::generate();
    snapshot.cursor = ChangeCursor::at(next_sequence, operation);
    let changed_files = prepared
        .into_iter()
        .filter(|(_, version)| base.file_versions.get(&version.id) != Some(version))
        .map(|(path, version)| (root.join(path), version))
        .collect();
    Ok(ScannedTree {
        snapshot,
        changed_files,
    })
}

fn same_namespace(left: &VolumeSnapshot, right: &VolumeSnapshot) -> bool {
    left.root == right.root
        && left.nodes == right.nodes
        && left.directories == right.directories
        && left.file_versions == right.file_versions
}

fn scan_paths(root: &Path) -> Result<BTreeMap<String, LocalEntry>, SyncError> {
    let mut entries = BTreeMap::new();
    let mut pending = vec![(root.to_owned(), String::new())];
    let mut file_identities = BTreeSet::new();
    while let Some((directory, parent)) = pending.pop() {
        let children = std::fs::read_dir(&directory)
            .map_err(|error| SyncError::io("scan local directory", error))?;
        for child in children {
            let child = child.map_err(|error| SyncError::io("scan local directory", error))?;
            let name = child
                .file_name()
                .into_string()
                .map_err(|_| SyncError::new("local directory contains a non-Unicode name"))?;
            let path = if parent.is_empty() {
                name
            } else {
                format!("{parent}/{name}")
            };
            let metadata = std::fs::symlink_metadata(child.path())
                .map_err(|error| SyncError::io("inspect local path", error))?;
            let entry = local_entry(&metadata, &mut file_identities)?;
            if entry.kind == NodeKind::Directory {
                pending.push((child.path(), path.clone()));
            }
            entries.insert(path, entry);
        }
    }
    Ok(entries)
}

#[cfg(unix)]
fn local_entry(
    metadata: &std::fs::Metadata,
    file_identities: &mut BTreeSet<(u64, u64)>,
) -> Result<LocalEntry, SyncError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let kind = if metadata.is_dir() {
        NodeKind::Directory
    } else if metadata.is_file() {
        if metadata.nlink() > 1 || !file_identities.insert((metadata.dev(), metadata.ino())) {
            return Err(SyncError::new(
                "local replica contains a hard-linked file, which Managed Sync does not support",
            ));
        }
        NodeKind::RegularFile
    } else {
        return Err(SyncError::new(
            "local replica contains a symbolic link or special file",
        ));
    };
    Ok(LocalEntry {
        kind,
        executable: kind == NodeKind::RegularFile && metadata.permissions().mode() & 0o111 != 0,
    })
}

#[cfg(not(unix))]
fn local_entry(
    _metadata: &std::fs::Metadata,
    _file_identities: &mut BTreeSet<(u64, u64)>,
) -> Result<LocalEntry, SyncError> {
    Err(SyncError::new(
        "Managed Sync native identity is not implemented on this platform",
    ))
}
