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
use std::path::Path;

use futures::StreamExt as _;

use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, FileVersionId, Generation, NodeAttributes,
    NodeId, NodeKind, NodeRecord, OperationId, VolumeSnapshot,
};
use crate::managed::ManagedVolume;

use super::SyncError;
pub(crate) struct ScannedTree {
    pub(crate) snapshot: VolumeSnapshot,
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

    let mut inspected = futures::stream::iter(
        local
            .iter()
            .filter(|(_, entry)| entry.kind == NodeKind::RegularFile),
    )
    .map(|(path, _)| async move {
        Ok::<_, SyncError>((path, volume.inspect_file(&root.join(path)).await?))
    })
    .buffer_unordered(32);
    let mut file_versions = BTreeMap::new();
    let mut file_by_path = BTreeMap::<String, FileVersionId>::new();
    while let Some(result) = inspected.next().await {
        let (path, version) = result?;
        file_by_path.insert(path.clone(), version.id);
        file_versions
            .entry(version.id)
            .or_insert_with(|| version.clone());
    }

    let mut ids = BTreeMap::from([(String::new(), base.root)]);
    let mut used = BTreeSet::from([base.root]);
    for (path, entry) in &local {
        if let Some(node) = base_paths
            .get(path)
            .copied()
            .filter(|node| base.nodes[node].kind == entry.kind)
        {
            ids.insert(path.clone(), node);
            used.insert(node);
        }
    }
    reuse_unique_identities(
        &local,
        &file_by_path,
        base,
        &base_paths,
        &mut ids,
        &mut used,
    );
    for path in local.keys() {
        ids.entry(path.clone()).or_insert_with(NodeId::generate);
    }

    let mut entries_by_directory = ids
        .iter()
        .filter(|(path, _)| path.is_empty() || local[*path].kind == NodeKind::Directory)
        .map(|(_, node)| (*node, BTreeMap::new()))
        .collect::<BTreeMap<_, _>>();
    for (child_path, child) in &local {
        let (parent, name) = child_path.rsplit_once('/').unwrap_or(("", child_path));
        entries_by_directory
            .get_mut(&ids[parent])
            .expect("a local entry's parent is a directory")
            .insert(
                name.to_owned(),
                DirectoryEntry {
                    node: ids[child_path],
                    kind: child.kind,
                },
            );
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
        let entries = entries_by_directory
            .remove(node)
            .expect("every local directory has an entry set");
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
        });
    }

    let operation = OperationId::generate();
    snapshot.cursor = ChangeCursor::at(next_sequence, operation);
    Ok(ScannedTree { snapshot })
}

fn reuse_unique_identities(
    local: &BTreeMap<String, LocalEntry>,
    file_by_path: &BTreeMap<String, FileVersionId>,
    base: &VolumeSnapshot,
    base_paths: &BTreeMap<String, NodeId>,
    ids: &mut BTreeMap<String, NodeId>,
    used: &mut BTreeSet<NodeId>,
) {
    let local_signatures = local_signatures(local, file_by_path);
    let base_signatures = snapshot_signatures(base, base_paths);
    let mut local_by_signature = BTreeMap::<_, Vec<&String>>::new();
    for (path, signature) in &local_signatures {
        if !ids.contains_key(path) {
            local_by_signature.entry(*signature).or_default().push(path);
        }
    }
    let mut base_by_signature = BTreeMap::<_, Vec<NodeId>>::new();
    for (path, signature) in &base_signatures {
        let node = base_paths[path];
        if !path.is_empty() && !used.contains(&node) {
            base_by_signature.entry(*signature).or_default().push(node);
        }
    }
    for (signature, paths) in local_by_signature {
        let Some(nodes) = base_by_signature.get(&signature) else {
            continue;
        };
        if let ([path], [node]) = (paths.as_slice(), nodes.as_slice()) {
            ids.insert((*path).clone(), *node);
            used.insert(*node);
        }
    }
}

fn local_signatures(
    local: &BTreeMap<String, LocalEntry>,
    files: &BTreeMap<String, FileVersionId>,
) -> BTreeMap<String, crate::filesystem::Digest> {
    let children = child_paths(local.keys());
    let mut signatures = BTreeMap::new();
    for (path, entry) in local.iter().rev() {
        let child_signatures = children
            .get(path.as_str())
            .into_iter()
            .flatten()
            .map(|(name, child)| (*name, signatures[*child]))
            .collect::<Vec<_>>();
        signatures.insert(
            path.clone(),
            entry_signature(
                entry.kind,
                entry.executable,
                files.get(path).copied(),
                &child_signatures,
            ),
        );
    }
    signatures
}

fn snapshot_signatures(
    snapshot: &VolumeSnapshot,
    paths: &BTreeMap<String, NodeId>,
) -> BTreeMap<String, crate::filesystem::Digest> {
    let children = child_paths(paths.keys());
    let mut signatures = BTreeMap::new();
    for (path, node_id) in paths.iter().rev() {
        let node = &snapshot.nodes[node_id];
        let child_signatures = children
            .get(path.as_str())
            .into_iter()
            .flatten()
            .map(|(name, child)| (*name, signatures[*child]))
            .collect::<Vec<_>>();
        signatures.insert(
            path.clone(),
            entry_signature(
                node.kind,
                node.attributes.executable,
                node.file_version,
                &child_signatures,
            ),
        );
    }
    signatures
}

fn child_paths<'a>(
    paths: impl Iterator<Item = &'a String>,
) -> BTreeMap<&'a str, Vec<(&'a str, &'a str)>> {
    let mut children = BTreeMap::<_, Vec<_>>::new();
    for path in paths {
        if path.is_empty() {
            continue;
        }
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        children
            .entry(parent)
            .or_default()
            .push((name, path.as_str()));
    }
    children
}

fn entry_signature(
    kind: NodeKind,
    executable: bool,
    file: Option<FileVersionId>,
    children: &[(&str, crate::filesystem::Digest)],
) -> crate::filesystem::Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[match kind {
        NodeKind::Directory => 0,
        NodeKind::RegularFile => 1,
    }]);
    hasher.update(&[u8::from(executable)]);
    if let Some(file) = file {
        hasher.update(file.digest().as_bytes());
        hasher.update(&file.logical_length().to_be_bytes());
    }
    for (name, signature) in children {
        hasher.update(&(name.len() as u64).to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update(signature.as_bytes());
    }
    crate::filesystem::Digest::from_bytes(hasher.finalize().into())
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
    metadata: &std::fs::Metadata,
    _file_identities: &mut BTreeSet<(u64, u64)>,
) -> Result<LocalEntry, SyncError> {
    let kind = if metadata.is_dir() {
        NodeKind::Directory
    } else if metadata.is_file() {
        NodeKind::RegularFile
    } else {
        return Err(SyncError::new(
            "local replica contains a symbolic link or special file",
        ));
    };
    Ok(LocalEntry {
        kind,
        executable: false,
    })
}
