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

use super::transfer::inspect_file;
use crate::Error;
use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, FileFingerprint, FileVersionId, NodeAttributes,
    NodeId, NodeKind, NodeRecord, OperationId, VolumeSnapshot,
};

pub(crate) enum ScannedTree {
    Unchanged,
    Changed(VolumeSnapshot),
}

#[derive(Clone, Copy)]
struct LocalEntry {
    kind: NodeKind,
    executable: bool,
}

pub(crate) async fn scan(root: &Path, base: &VolumeSnapshot) -> Result<ScannedTree, Error> {
    let local = scan_paths(root)?;
    let base_paths = base.paths()?;

    let mut inspected = futures::stream::iter(
        local
            .iter()
            .filter(|(_, entry)| entry.kind == NodeKind::RegularFile),
    )
    .map(|(path, _)| async move { Ok::<_, Error>((path, inspect_file(&root.join(path)).await?)) })
    .buffer_unordered(32);
    let mut file_by_path = BTreeMap::<String, FileFingerprint>::new();
    while let Some(result) = inspected.next().await {
        let (path, version) = result?;
        file_by_path.insert(path.clone(), version);
    }

    if same_local_namespace(&local, &file_by_path, base, &base_paths) {
        return Ok(ScannedTree::Unchanged);
    }

    let next_sequence = base
        .cursor
        .sequence()
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .ok_or_else(|| Error::corrupt("scan replica", "Managed change sequence overflows"))?;
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
        directories.insert(*node, DirectoryRecord { entries });
    }

    let mut nodes = BTreeMap::new();
    for (path, node) in &ids {
        let (kind, attributes, file_version, file_fingerprint) = if path.is_empty() {
            (NodeKind::Directory, NodeAttributes::default(), None, None)
        } else {
            let local = local[path];
            let fingerprint = file_by_path.get(path).copied();
            let version = fingerprint.map(|fingerprint| {
                base_paths
                    .get(path)
                    .and_then(|node| base.nodes.get(node))
                    .filter(|node| node.file_fingerprint == Some(fingerprint))
                    .and_then(|node| node.file_version)
                    .unwrap_or_else(FileVersionId::generate)
            });
            (
                local.kind,
                NodeAttributes {
                    executable: local.executable,
                },
                version,
                fingerprint,
            )
        };
        nodes.insert(
            *node,
            NodeRecord {
                kind,
                attributes,
                file_version,
                file_fingerprint,
            },
        );
    }

    let mut snapshot = VolumeSnapshot {
        volume_id: base.volume_id,
        cursor: base.cursor,
        root: base.root,
        nodes,
        directories,
    };
    snapshot.validate()?;
    let operation = OperationId::generate();
    snapshot.cursor = ChangeCursor::at(next_sequence, operation);
    Ok(ScannedTree::Changed(snapshot))
}

fn same_local_namespace(
    local: &BTreeMap<String, LocalEntry>,
    files: &BTreeMap<String, FileFingerprint>,
    base: &VolumeSnapshot,
    base_paths: &BTreeMap<String, NodeId>,
) -> bool {
    if local.len() != base_paths.len()
        || base.nodes[&base.root].attributes != NodeAttributes::default()
    {
        return false;
    }
    local.iter().all(|(path, entry)| {
        let Some(node) = base_paths.get(path).and_then(|node| base.nodes.get(node)) else {
            return false;
        };
        node.kind == entry.kind
            && node.attributes
                == NodeAttributes {
                    executable: entry.executable,
                }
            && node.file_fingerprint == files.get(path).copied()
    })
}

fn reuse_unique_identities(
    local: &BTreeMap<String, LocalEntry>,
    file_by_path: &BTreeMap<String, FileFingerprint>,
    base: &VolumeSnapshot,
    base_paths: &BTreeMap<String, NodeId>,
    ids: &mut BTreeMap<String, NodeId>,
    used: &mut BTreeSet<NodeId>,
) {
    let has_unassigned_local = local.keys().any(|path| !ids.contains_key(path));
    let has_unused_base = base_paths
        .iter()
        .any(|(path, node)| !path.is_empty() && !used.contains(node));
    if !has_unassigned_local || !has_unused_base {
        return;
    }

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
    files: &BTreeMap<String, FileFingerprint>,
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
                node.file_fingerprint,
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
    file: Option<FileFingerprint>,
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

fn scan_paths(root: &Path) -> Result<BTreeMap<String, LocalEntry>, Error> {
    let mut entries = BTreeMap::new();
    let mut pending = vec![(root.to_owned(), String::new())];
    let mut file_identities = BTreeSet::new();
    while let Some((directory, parent)) = pending.pop() {
        let children = std::fs::read_dir(&directory)
            .map_err(|error| Error::from_io("scan local directory", Some(&directory), error))?;
        for child in children {
            let child = child
                .map_err(|error| Error::from_io("scan local directory", Some(&directory), error))?;
            let name = child.file_name().into_string().map_err(|_| {
                Error::invalid(
                    "synchronize replica",
                    "local directory contains a non-Unicode name",
                )
            })?;
            let path = if parent.is_empty() {
                name
            } else {
                format!("{parent}/{name}")
            };
            let child_path = child.path();
            let metadata = std::fs::symlink_metadata(&child_path)
                .map_err(|error| Error::from_io("inspect local path", Some(&child_path), error))?;
            let entry = local_entry(&metadata, &mut file_identities)?;
            if entry.kind == NodeKind::Directory {
                pending.push((child_path, path.clone()));
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
) -> Result<LocalEntry, Error> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let kind = if metadata.is_dir() {
        NodeKind::Directory
    } else if metadata.is_file() {
        if metadata.nlink() > 1 || !file_identities.insert((metadata.dev(), metadata.ino())) {
            return Err(Error::unsupported(
                "scan replica",
                "local replica contains a hard-linked file, which Managed Sync does not support",
            ));
        }
        NodeKind::RegularFile
    } else {
        return Err(Error::unsupported(
            "scan replica",
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
) -> Result<LocalEntry, Error> {
    let kind = if metadata.is_dir() {
        NodeKind::Directory
    } else if metadata.is_file() {
        NodeKind::RegularFile
    } else {
        return Err(Error::unsupported(
            "scan replica",
            "local replica contains a symbolic link or special file",
        ));
    };
    Ok(LocalEntry {
        kind,
        executable: false,
    })
}
