// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use anyhow::{Context, Result, bail};

use super::path::SnapshotTree;
use super::{LocalKind, LocalTree};
use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryPrecondition, DirectoryRecord, FileVersion,
    NodeAttributes, NodeId, NodeKind, NodePrecondition, NodeRecord, OperationId, Volume,
    VolumePublication, VolumeSnapshot,
};

/// Build one complete volume namespace target from a stable local observation.
///
/// Rename identity belongs to reconciliation. This builder preserves identity
/// at an unchanged path, but never treats equal content as proof of a rename.
pub(crate) fn build_publication<V: Volume>(
    volume_api: &V,
    operation: OperationId,
    authoritative: Option<&SnapshotTree<'_>>,
    base: Option<&SnapshotTree<'_>>,
    local: &LocalTree,
    prepared: &BTreeMap<String, FileVersion>,
    renames: &BTreeMap<String, String>,
) -> Result<VolumePublication> {
    let volume = volume_api.id();
    let authoritative_snapshot = authoritative.map(SnapshotTree::snapshot);
    let empty_paths = BTreeMap::new();
    let old_paths = authoritative.map_or(&empty_paths, SnapshotTree::paths);
    let parent = authoritative_snapshot.map_or(ChangeCursor::Genesis, |state| state.cursor);
    if authoritative_snapshot.is_some_and(|state| state.volume_id != volume) {
        bail!("namespace and requested volume disagree");
    }
    reject_unresolved_renames(
        local,
        old_paths,
        base,
        authoritative_snapshot,
        renames,
        prepared,
    )?;

    let rename_sources = renames
        .iter()
        .map(|(from, path)| (path.clone(), from.clone()))
        .collect::<BTreeMap<_, _>>();

    let old_nodes = authoritative_snapshot.map(|state| &state.nodes);
    let old_directories = authoritative_snapshot.map(|state| &state.directories);
    let root = authoritative_snapshot.map_or_else(NodeId::generate, |state| state.root);
    let mut identities = BTreeMap::from([(String::new(), root)]);
    let mut used = BTreeSet::from([root]);
    for (path, entry) in local.entries() {
        let kind = node_kind(entry.kind);
        let identity = old_paths
            .get(path)
            .or_else(|| {
                rename_sources
                    .get(path)
                    .and_then(|from| old_paths.get(from))
            })
            .and_then(|id| old_nodes.and_then(|nodes| nodes.get(id)))
            .filter(|node| node.kind == kind)
            .map(|node| node.id)
            .unwrap_or_else(|| fresh_node(&mut used, old_nodes));
        used.insert(identity);
        identities.insert(path.clone(), identity);
    }

    let mut nodes = BTreeMap::new();
    let mut file_versions = BTreeMap::new();
    for (path, kind) in std::iter::once(("", NodeKind::Directory)).chain(
        local
            .entries()
            .iter()
            .map(|(path, entry)| (path.as_str(), node_kind(entry.kind))),
    ) {
        let id = identities[path];
        let file_version = if kind == NodeKind::RegularFile {
            let version = prepared
                .get(path)
                .with_context(|| format!("local file {path:?} has no prepared file version"))?;
            let size = local.entries()[path].size;
            if version.logical_size != size {
                bail!("prepared file version for {path:?} does not match the local file");
            }
            match file_versions.insert(version.id, version.clone()) {
                Some(existing) if existing != *version => {
                    bail!("prepared file version identity is reused with different content")
                }
                _ => {}
            }
            Some(version.id)
        } else {
            None
        };
        let attributes = local
            .entries()
            .get(path)
            .map_or_else(NodeAttributes::default, |entry| NodeAttributes {
                executable: entry.executable,
            });
        let body = (kind, attributes, file_version);
        let generation = next_node_generation(volume_api, id, body, old_nodes)?;
        nodes.insert(
            id,
            NodeRecord {
                id,
                generation,
                kind,
                attributes: body.1,
                file_version,
            },
        );
    }

    let entries = directory_entries(local, &identities)?;
    let mut directories = BTreeMap::new();
    for (path, kind) in std::iter::once(("", NodeKind::Directory)).chain(
        local
            .entries()
            .iter()
            .map(|(path, entry)| (path.as_str(), node_kind(entry.kind))),
    ) {
        if kind != NodeKind::Directory {
            continue;
        }
        let node = identities[path];
        let current = entries.get(path).cloned().unwrap_or_default();
        let generation = next_directory_generation(volume_api, node, &current, old_directories)?;
        directories.insert(
            node,
            DirectoryRecord {
                node,
                generation,
                entries: current,
            },
        );
    }

    let cursor = ChangeCursor::at(
        NonZeroU64::new(
            parent
                .sequence()
                .checked_add(1)
                .context("namespace cursor overflow")?,
        )
        .context("namespace cursor cannot be zero")?,
        operation,
    );
    let target = VolumeSnapshot {
        volume_id: volume,
        cursor,
        root,
        nodes,
        directories,
        file_versions,
    };
    Ok(VolumePublication {
        operation,
        parent,
        expected_nodes: node_preconditions(authoritative_snapshot, &target),
        expected_directories: directory_preconditions(authoritative_snapshot, &target),
        target,
    })
}

fn reject_unresolved_renames(
    local: &LocalTree,
    old_paths: &BTreeMap<String, NodeId>,
    base: Option<&SnapshotTree<'_>>,
    authoritative: Option<&VolumeSnapshot>,
    renames: &BTreeMap<String, String>,
    prepared: &BTreeMap<String, FileVersion>,
) -> Result<()> {
    let Some(authoritative) = authoritative else {
        return Ok(());
    };
    let Some(base) = base else {
        return Ok(());
    };
    for (path, entry) in local.entries() {
        if old_paths.contains_key(path) || entry.kind != LocalKind::File {
            continue;
        }
        if renames.values().any(|target| target == path) {
            continue;
        }
        let digest = prepared
            .get(path)
            .with_context(|| format!("local file {path:?} has no prepared file version"))?
            .logical_digest;
        let possible = base.paths().iter().any(|(old_path, node)| {
            !local.entries().contains_key(old_path)
                && authoritative.nodes.contains_key(node)
                && base
                    .get(old_path)
                    .and_then(|entry| entry.file)
                    .is_some_and(|version| version.logical_digest == digest)
        });
        if possible {
            bail!(
                "local path {path:?} may be a rename; reconcile it before building a publication"
            );
        }
    }
    Ok(())
}

fn directory_entries(
    local: &LocalTree,
    identities: &BTreeMap<String, NodeId>,
) -> Result<BTreeMap<String, BTreeMap<String, DirectoryEntry>>> {
    let mut directories = BTreeMap::<String, BTreeMap<String, DirectoryEntry>>::new();
    directories.insert(String::new(), BTreeMap::new());
    for (path, entry) in local.entries() {
        let (parent, name) = split_path(path)?;
        if name.is_empty() {
            bail!("local path {path:?} is invalid");
        }
        if !parent.is_empty()
            && !local
                .entries()
                .get(parent)
                .is_some_and(|entry| entry.kind == LocalKind::Directory)
        {
            bail!("local path {path:?} has no directory parent");
        }
        let kind = node_kind(entry.kind);
        directories.entry(parent.to_owned()).or_default().insert(
            name.to_owned(),
            DirectoryEntry {
                node: identities[path],
                kind,
            },
        );
        if kind == NodeKind::Directory {
            directories.entry(path.clone()).or_default();
        }
    }
    Ok(directories)
}

fn node_kind(kind: LocalKind) -> NodeKind {
    match kind {
        LocalKind::Directory => NodeKind::Directory,
        LocalKind::File => NodeKind::RegularFile,
    }
}

fn split_path(path: &str) -> Result<(&str, &str)> {
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    if path.starts_with('/')
        || path.ends_with('/')
        || path.contains("//")
        || name == "."
        || name == ".."
    {
        bail!("local path {path:?} is not canonical");
    }
    Ok((parent, name))
}

fn fresh_node(used: &mut BTreeSet<NodeId>, old: Option<&BTreeMap<NodeId, NodeRecord>>) -> NodeId {
    loop {
        let node = NodeId::generate();
        if !used.contains(&node) && old.is_none_or(|nodes| !nodes.contains_key(&node)) {
            return node;
        }
    }
}

fn next_node_generation<V: Volume>(
    volume: &V,
    id: NodeId,
    body: (
        NodeKind,
        NodeAttributes,
        Option<crate::filesystem::FileVersionId>,
    ),
    old: Option<&BTreeMap<NodeId, NodeRecord>>,
) -> Result<crate::filesystem::Generation> {
    let Some(previous) = old.and_then(|nodes| nodes.get(&id)) else {
        return Ok(volume.initial_generation());
    };
    if (previous.kind, previous.attributes, previous.file_version) == body {
        Ok(previous.generation.clone())
    } else {
        volume
            .next_generation(&previous.generation)
            .context("node generation overflow")
    }
}

fn next_directory_generation<V: Volume>(
    volume: &V,
    id: NodeId,
    entries: &BTreeMap<String, DirectoryEntry>,
    old: Option<&BTreeMap<NodeId, DirectoryRecord>>,
) -> Result<crate::filesystem::Generation> {
    let Some(previous) = old.and_then(|directories| directories.get(&id)) else {
        return Ok(volume.initial_generation());
    };
    if previous.entries == *entries {
        Ok(previous.generation.clone())
    } else {
        volume
            .next_generation(&previous.generation)
            .context("directory generation overflow")
    }
}

fn node_preconditions(
    authoritative: Option<&VolumeSnapshot>,
    target: &VolumeSnapshot,
) -> Vec<NodePrecondition> {
    let empty = BTreeMap::new();
    let old = authoritative.map_or(&empty, |state| &state.nodes);
    old.keys()
        .chain(target.nodes.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|id| old.get(id) != target.nodes.get(id))
        .map(|node| NodePrecondition {
            node,
            expected_generation: old.get(&node).map(|record| record.generation.clone()),
        })
        .collect()
}

fn directory_preconditions(
    authoritative: Option<&VolumeSnapshot>,
    target: &VolumeSnapshot,
) -> Vec<DirectoryPrecondition> {
    let empty = BTreeMap::new();
    let old = authoritative.map_or(&empty, |state| &state.directories);
    old.keys()
        .chain(target.directories.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|id| old.get(id) != target.directories.get(id))
        .map(|directory| DirectoryPrecondition {
            directory,
            expected_generation: old.get(&directory).map(|record| record.generation.clone()),
        })
        .collect()
}
