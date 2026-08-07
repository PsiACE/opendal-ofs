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

use super::records::{
    DirectoryPrecondition, DirectoryRecord, NamespacePublication, NamespaceSnapshot,
    NodePrecondition, NodeRecord, managed_generation, managed_generation_number,
    next_managed_generation,
};
use crate::filesystem::{ChangeCursor, FileVersionId, NodeAttributes, NodeId, NodeKind};
use crate::managed::{ManagedError, ManagedErrorKind};

pub(super) fn validate_publication(
    publication: &NamespacePublication,
    base: Option<&NamespaceSnapshot>,
) -> Result<bool, ManagedError> {
    validate_snapshot(&publication.target)?;
    if publication.target.cursor.operation() != Some(publication.operation)
        || publication.parent.sequence().checked_add(1)
            != Some(publication.target.cursor.sequence())
        || base.is_some_and(|state| {
            state.volume_id != publication.target.volume_id || state.cursor != publication.parent
        })
        || base.is_none() && publication.parent != ChangeCursor::Genesis
    {
        return Err(invalid(
            "publish Managed namespace",
            "publication ancestry is invalid",
        ));
    }

    let empty_nodes = BTreeMap::new();
    let empty_directories = BTreeMap::new();
    let nodes = base.map_or(&empty_nodes, |state| &state.nodes);
    let directories = base.map_or(&empty_directories, |state| &state.directories);
    if !preconditions_match_nodes(nodes, &publication.expected_nodes)?
        || !preconditions_match_directories(directories, &publication.expected_directories)?
    {
        return Ok(false);
    }
    validate_generations(publication, nodes, directories)?;
    if let Some(base) = base {
        for (id, version) in &base.file_versions {
            if let Some(next) = publication.target.file_versions.get(id)
                && next != version
            {
                return Err(invalid(
                    "publish Managed namespace",
                    "an immutable file version changed",
                ));
            }
        }
    }
    Ok(true)
}

pub(super) fn validate_snapshot(snapshot: &NamespaceSnapshot) -> Result<(), ManagedError> {
    let root = snapshot
        .nodes
        .get(&snapshot.root)
        .filter(|node| node.kind == NodeKind::Directory)
        .ok_or_else(|| invalid("read Managed namespace", "root directory is missing"))?;
    if root.id != snapshot.root || !snapshot.directories.contains_key(&snapshot.root) {
        return Err(invalid(
            "read Managed namespace",
            "root directory is invalid",
        ));
    }
    for (id, node) in &snapshot.nodes {
        if *id != node.id || managed_generation_number(&node.generation).is_none() {
            return Err(invalid("read Managed namespace", "node record is invalid"));
        }
        match node.kind {
            NodeKind::Directory if node.file_version.is_none() => {}
            NodeKind::RegularFile
                if node
                    .file_version
                    .is_some_and(|version| snapshot.file_versions.contains_key(&version)) => {}
            _ => return Err(invalid("read Managed namespace", "node content is invalid")),
        }
    }
    for (id, directory) in &snapshot.directories {
        if *id != directory.node
            || managed_generation_number(&directory.generation).is_none()
            || !snapshot
                .nodes
                .get(id)
                .is_some_and(|node| node.kind == NodeKind::Directory)
        {
            return Err(invalid(
                "read Managed namespace",
                "directory record is invalid",
            ));
        }
        for (name, entry) in &directory.entries {
            if name.is_empty()
                || name == "."
                || name == ".."
                || name.contains('/')
                || !snapshot
                    .nodes
                    .get(&entry.node)
                    .is_some_and(|node| node.kind == entry.kind)
            {
                return Err(invalid(
                    "read Managed namespace",
                    "directory entry is invalid",
                ));
            }
        }
    }
    for (id, version) in &snapshot.file_versions {
        if *id != version.id
            || version.logical_size != version.content.logical_length
            || version.logical_digest != version.content.digest
        {
            return Err(invalid("read Managed namespace", "file version is invalid"));
        }
    }
    Ok(())
}

fn preconditions_match_nodes(
    current: &BTreeMap<NodeId, NodeRecord>,
    expected: &[NodePrecondition],
) -> Result<bool, ManagedError> {
    let mut unique = BTreeSet::new();
    for condition in expected {
        if !unique.insert(condition.node) {
            return Err(invalid(
                "publish Managed namespace",
                "duplicate node precondition",
            ));
        }
        if current.get(&condition.node).map(|node| &node.generation)
            != condition.expected_generation.as_ref()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn preconditions_match_directories(
    current: &BTreeMap<NodeId, DirectoryRecord>,
    expected: &[DirectoryPrecondition],
) -> Result<bool, ManagedError> {
    let mut unique = BTreeSet::new();
    for condition in expected {
        if !unique.insert(condition.directory) {
            return Err(invalid(
                "publish Managed namespace",
                "duplicate directory precondition",
            ));
        }
        if current
            .get(&condition.directory)
            .map(|directory| &directory.generation)
            != condition.expected_generation.as_ref()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_generations(
    publication: &NamespacePublication,
    nodes: &BTreeMap<NodeId, NodeRecord>,
    directories: &BTreeMap<NodeId, DirectoryRecord>,
) -> Result<(), ManagedError> {
    let node_conditions = publication
        .expected_nodes
        .iter()
        .map(|condition| condition.node)
        .collect::<BTreeSet<_>>();
    for id in nodes.keys().chain(publication.target.nodes.keys()) {
        let current = nodes.get(id);
        let target = publication.target.nodes.get(id);
        let changed = current.map(node_body) != target.map(node_body);
        let expected = match (current, target, changed) {
            (None, Some(_), _) => managed_generation(1),
            (Some(node), Some(_), false) => node.generation.clone(),
            (Some(node), Some(_), true) => next_managed_generation(&node.generation)
                .ok_or_else(|| invalid("publish Managed namespace", "node generation overflow"))?,
            (Some(_), None, _) => {
                if !node_conditions.contains(id) {
                    return Err(invalid(
                        "publish Managed namespace",
                        "changed node lacks a precondition",
                    ));
                }
                continue;
            }
            (None, None, _) => continue,
        };
        if target.is_some_and(|node| node.generation != expected)
            || changed && !node_conditions.contains(id)
        {
            return Err(invalid(
                "publish Managed namespace",
                "node generation transition is invalid",
            ));
        }
    }

    let directory_conditions = publication
        .expected_directories
        .iter()
        .map(|condition| condition.directory)
        .collect::<BTreeSet<_>>();
    for id in directories
        .keys()
        .chain(publication.target.directories.keys())
    {
        let current = directories.get(id);
        let target = publication.target.directories.get(id);
        let changed = current.map(|item| &item.entries) != target.map(|item| &item.entries);
        let expected = match (current, target, changed) {
            (None, Some(_), _) => managed_generation(1),
            (Some(directory), Some(_), false) => directory.generation.clone(),
            (Some(directory), Some(_), true) => next_managed_generation(&directory.generation)
                .ok_or_else(|| {
                    invalid("publish Managed namespace", "directory generation overflow")
                })?,
            (Some(_), None, _) => {
                if !directory_conditions.contains(id) {
                    return Err(invalid(
                        "publish Managed namespace",
                        "changed directory lacks a precondition",
                    ));
                }
                continue;
            }
            (None, None, _) => continue,
        };
        if target.is_some_and(|directory| directory.generation != expected)
            || changed && !directory_conditions.contains(id)
        {
            return Err(invalid(
                "publish Managed namespace",
                "directory generation transition is invalid",
            ));
        }
    }
    Ok(())
}

fn node_body(node: &NodeRecord) -> (NodeId, NodeKind, NodeAttributes, Option<FileVersionId>) {
    (node.id, node.kind, node.attributes, node.file_version)
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}
