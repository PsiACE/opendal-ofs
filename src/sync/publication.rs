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

use super::SyncVolume;
use super::path::SnapshotTree;
use super::staging::StagedTree;
use crate::filesystem::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, NodeAttributes, NodeId, NodeKind, NodeRecord,
    OperationId, VolumePublication, VolumeSnapshot,
};

/// Build one complete volume namespace target from a stable local observation.
///
/// Rename identity belongs to reconciliation. This builder preserves identity
/// at an unchanged path, but never treats equal content as proof of a rename.
pub(crate) fn build_publication<V: SyncVolume>(
    volume_api: &V,
    operation: OperationId,
    authoritative: Option<&SnapshotTree<'_>>,
    staged: &StagedTree,
    renames: &BTreeMap<String, String>,
) -> Result<VolumePublication> {
    let target_manifest = staged.manifest();
    let volume = volume_api.id();
    let authoritative_snapshot = authoritative.map(|tree| tree.snapshot);
    let empty_paths = BTreeMap::new();
    let old_paths = authoritative.map_or(&empty_paths, |tree| &tree.paths);
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
    for (path, entry) in &target_manifest.entries {
        let kind = entry.local.kind;
        let identity = old_paths
            .get(path)
            .or_else(|| rename_source(&rename_sources, path).and_then(|from| old_paths.get(&from)))
            .and_then(|id| old_nodes.and_then(|nodes| nodes.get(id)))
            .filter(|node| node.kind == kind)
            .map(|node| node.id)
            .unwrap_or_else(|| fresh_node(&mut used, old_nodes));
        used.insert(identity);
        identities.insert(path.clone(), identity);
    }

    let mut nodes = BTreeMap::new();
    let mut file_versions = BTreeMap::new();
    let mut directory_entries = BTreeMap::<String, BTreeMap<String, DirectoryEntry>>::new();
    directory_entries.insert(String::new(), BTreeMap::new());
    for (path, kind) in std::iter::once(("", NodeKind::Directory)).chain(
        target_manifest
            .entries
            .iter()
            .map(|(path, entry)| (path.as_str(), entry.local.kind)),
    ) {
        let id = identities[path];
        let file_version = if kind == NodeKind::RegularFile {
            let file = target_manifest
                .file(path)
                .with_context(|| format!("target file {path:?} has no volume version"))?;
            let size = target_manifest.entries[path].local.size;
            let version = staged
                .resolve_version(file, size, authoritative_snapshot)
                .with_context(|| format!("resolve target file version for {path:?}"))?
                .clone();
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
                .entries
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
        if path.is_empty() {
            continue;
        }
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        if !parent.is_empty()
            && !target_manifest
                .entries
                .get(parent)
                .is_some_and(|entry| entry.local.kind == NodeKind::Directory)
        {
            bail!("local path {path:?} has no directory parent");
        }
        directory_entries
            .entry(parent.to_owned())
            .or_default()
            .insert(name.to_owned(), DirectoryEntry { node: id, kind });
        if kind == NodeKind::Directory {
            directory_entries.entry(path.to_owned()).or_default();
        }
    }

    let mut directories = BTreeMap::new();
    for (path, current) in directory_entries {
        let node = identities[&path];
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

fn rename_source(renames: &BTreeMap<String, String>, path: &str) -> Option<String> {
    if let Some(source) = renames.get(path) {
        return Some(source.clone());
    }
    let mut parent = path;
    while let Some((next, _)) = parent.rsplit_once('/') {
        parent = next;
        if let Some(source) = renames.get(parent) {
            return Some(format!("{source}{}", &path[parent.len()..]));
        }
    }
    None
}

fn fresh_node(used: &mut BTreeSet<NodeId>, old: Option<&BTreeMap<NodeId, NodeRecord>>) -> NodeId {
    loop {
        let node = NodeId::generate();
        if !used.contains(&node) && old.is_none_or(|nodes| !nodes.contains_key(&node)) {
            return node;
        }
    }
}

fn next_node_generation<V: SyncVolume>(
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

fn next_directory_generation<V: SyncVolume>(
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
