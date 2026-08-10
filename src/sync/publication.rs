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

use super::LocalKind;
use super::path::SnapshotTree;
use super::staging::{StagedTree, TargetManifest};
use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, NodeAttributes, NodeId, NodeKind, NodeRecord,
    OperationId, Volume, VolumePublication, VolumeSnapshot,
};

/// Build one complete volume namespace target from a stable local observation.
///
/// Rename identity belongs to reconciliation. This builder preserves identity
/// at an unchanged path, but never treats equal content as proof of a rename.
pub(crate) fn build_publication<V: Volume>(
    volume_api: &V,
    operation: OperationId,
    authoritative: Option<&SnapshotTree<'_>>,
    staged: &StagedTree,
    renames: &BTreeMap<String, String>,
) -> Result<VolumePublication> {
    let target_manifest = staged.manifest();
    let volume = volume_api.id();
    let authoritative_snapshot = authoritative.map(SnapshotTree::snapshot);
    let empty_paths = BTreeMap::new();
    let old_paths = authoritative.map_or(&empty_paths, SnapshotTree::paths);
    let parent = authoritative_snapshot.map_or(ChangeCursor::Genesis, |state| state.cursor);
    if authoritative_snapshot.is_some_and(|state| state.volume_id != volume) {
        bail!("namespace and requested volume disagree");
    }
    let rename_sources = renames
        .iter()
        .map(|(from, path)| (path.clone(), from.clone()))
        .collect::<BTreeMap<_, _>>();

    let old_nodes = authoritative_snapshot.map(|state| &state.nodes);
    let old_directories = authoritative_snapshot.map(|state| &state.directories);
    let root = authoritative_snapshot.map_or_else(NodeId::generate, |state| state.root);
    let mut identities = BTreeMap::from([(String::new(), root)]);
    let mut used = BTreeSet::from([root]);
    for (path, entry) in target_manifest.entries() {
        let kind = node_kind(entry.local.kind);
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
        target_manifest
            .entries()
            .iter()
            .map(|(path, entry)| (path.as_str(), node_kind(entry.local.kind))),
    ) {
        let id = identities[path];
        let file_version = if kind == NodeKind::RegularFile {
            let file = target_manifest
                .file(path)
                .with_context(|| format!("target file {path:?} has no volume version"))?;
            let version = staged
                .resolve_version(file, authoritative_snapshot)
                .with_context(|| format!("resolve target file version for {path:?}"))?
                .clone();
            let size = target_manifest.entries()[path].local.size;
            if version.logical_size != size {
                bail!("target file version for {path:?} does not match the local file");
            }
            match file_versions.insert(version.id, version.clone()) {
                Some(existing) if existing != version => {
                    bail!("target file version identity is reused with different content")
                }
                _ => {}
            }
            Some(version.id)
        } else {
            None
        };
        let attributes =
            target_manifest
                .entries()
                .get(path)
                .map_or_else(NodeAttributes::default, |entry| NodeAttributes {
                    executable: entry.local.executable,
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

    let entries = directory_entries(target_manifest, &identities)?;
    let mut directories = BTreeMap::new();
    for (path, kind) in std::iter::once(("", NodeKind::Directory)).chain(
        target_manifest
            .entries()
            .iter()
            .map(|(path, entry)| (path.as_str(), node_kind(entry.local.kind))),
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
    VolumePublication::between(operation, authoritative_snapshot, target).map_err(Into::into)
}

fn directory_entries(
    target: &TargetManifest,
    identities: &BTreeMap<String, NodeId>,
) -> Result<BTreeMap<String, BTreeMap<String, DirectoryEntry>>> {
    let mut directories = BTreeMap::<String, BTreeMap<String, DirectoryEntry>>::new();
    directories.insert(String::new(), BTreeMap::new());
    for (path, entry) in target.entries() {
        let (parent, name) = split_path(path)?;
        if name.is_empty() {
            bail!("local path {path:?} is invalid");
        }
        if !parent.is_empty()
            && !target
                .entries()
                .get(parent)
                .is_some_and(|entry| entry.local.kind == LocalKind::Directory)
        {
            bail!("local path {path:?} has no directory parent");
        }
        let kind = node_kind(entry.local.kind);
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
