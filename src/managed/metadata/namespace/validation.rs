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

pub(crate) fn validate_publication(
    publication: &NamespacePublication,
    base: Option<&NamespaceSnapshot>,
) -> Result<bool, ManagedError> {
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
    validate_transition(
        &publication.expected_nodes,
        &publication.expected_directories,
        &publication.target,
        base,
    )
}

pub(crate) fn validate_transition(
    expected_nodes: &[NodePrecondition],
    expected_directories: &[DirectoryPrecondition],
    target: &NamespaceSnapshot,
    base: Option<&NamespaceSnapshot>,
) -> Result<bool, ManagedError> {
    validate_snapshot(target)?;
    let empty_nodes = BTreeMap::new();
    let empty_directories = BTreeMap::new();
    let nodes = base.map_or(&empty_nodes, |state| &state.nodes);
    let directories = base.map_or(&empty_directories, |state| &state.directories);
    if !preconditions_match_nodes(nodes, expected_nodes)?
        || !preconditions_match_directories(directories, expected_directories)?
    {
        return Ok(false);
    }
    validate_generations(
        target,
        expected_nodes,
        expected_directories,
        nodes,
        directories,
    )?;
    if let Some(base) = base {
        for (id, version) in &base.file_versions {
            if let Some(next) = target.file_versions.get(id)
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

pub(crate) fn validate_snapshot(snapshot: &NamespaceSnapshot) -> Result<(), ManagedError> {
    snapshot
        .validate_structure()
        .map_err(|_| invalid("read Managed namespace", "namespace structure is invalid"))?;
    for node in snapshot.nodes.values() {
        if managed_generation_number(&node.generation).is_none() {
            return Err(invalid("read Managed namespace", "node record is invalid"));
        }
    }
    for directory in snapshot.directories.values() {
        if managed_generation_number(&directory.generation).is_none() {
            return Err(invalid(
                "read Managed namespace",
                "directory record is invalid",
            ));
        }
    }
    for (id, version) in &snapshot.file_versions {
        if *id != version.id || !version.is_valid() {
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
    target: &NamespaceSnapshot,
    expected_nodes: &[NodePrecondition],
    expected_directories: &[DirectoryPrecondition],
    nodes: &BTreeMap<NodeId, NodeRecord>,
    directories: &BTreeMap<NodeId, DirectoryRecord>,
) -> Result<(), ManagedError> {
    let node_conditions = expected_nodes
        .iter()
        .map(|condition| condition.node)
        .collect::<BTreeSet<_>>();
    for id in nodes.keys().chain(target.nodes.keys()) {
        let current = nodes.get(id);
        let next = target.nodes.get(id);
        let changed = current.map(node_body) != next.map(node_body);
        let expected = match (current, next, changed) {
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
        if next.is_some_and(|node| node.generation != expected)
            || changed && !node_conditions.contains(id)
        {
            return Err(invalid(
                "publish Managed namespace",
                "node generation transition is invalid",
            ));
        }
    }

    let directory_conditions = expected_directories
        .iter()
        .map(|condition| condition.directory)
        .collect::<BTreeSet<_>>();
    for id in directories.keys().chain(target.directories.keys()) {
        let current = directories.get(id);
        let next = target.directories.get(id);
        let changed = current.map(|item| &item.entries) != next.map(|item| &item.entries);
        let expected = match (current, next, changed) {
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
        if next.is_some_and(|directory| directory.generation != expected)
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::filesystem::{DirectoryEntry, OperationId, VolumeId};
    use crate::managed::format::{ContentRef, Extent, ExtentMap, SegmentRef};
    use crate::managed::metadata::namespace::FileVersionRecord;

    const ROOT: NodeId = NodeId::from_bytes([1; 16]);
    const FILE: NodeId = NodeId::from_bytes([2; 16]);

    fn operation(byte: u8) -> OperationId {
        OperationId::from_bytes([byte; 16])
    }

    fn cursor(sequence: u64, byte: u8) -> ChangeCursor {
        ChangeCursor::at(NonZeroU64::new(sequence).unwrap(), operation(byte))
    }

    fn file_version(size: u64, digest: [u8; 32]) -> FileVersionRecord {
        FileVersionRecord::from_extents(
            size,
            digest,
            ExtentMap {
                extents: vec![Extent {
                    logical_offset: 0,
                    content: ContentRef {
                        digest,
                        length: size,
                    },
                    segment: SegmentRef {
                        digest: [9; 32],
                        length: size + 66,
                    },
                    segment_offset: 10,
                }],
            },
        )
        .unwrap()
    }

    fn base_snapshot() -> NamespaceSnapshot {
        let version = file_version(3, [3; 32]);
        NamespaceSnapshot {
            volume_id: VolumeId::from_bytes([7; 16]),
            cursor: cursor(1, 1),
            root: ROOT,
            nodes: BTreeMap::from([
                (
                    ROOT,
                    NodeRecord {
                        id: ROOT,
                        generation: managed_generation(1),
                        kind: NodeKind::Directory,
                        attributes: NodeAttributes::default(),
                        file_version: None,
                    },
                ),
                (
                    FILE,
                    NodeRecord {
                        id: FILE,
                        generation: managed_generation(1),
                        kind: NodeKind::RegularFile,
                        attributes: NodeAttributes::default(),
                        file_version: Some(version.id),
                    },
                ),
            ]),
            directories: BTreeMap::from([(
                ROOT,
                DirectoryRecord {
                    node: ROOT,
                    generation: managed_generation(1),
                    entries: BTreeMap::from([(
                        "old".to_owned(),
                        DirectoryEntry {
                            node: FILE,
                            kind: NodeKind::RegularFile,
                        },
                    )]),
                },
            )]),
            file_versions: BTreeMap::from([(version.id, version)]),
        }
    }

    fn publication(base: &NamespaceSnapshot, target: NamespaceSnapshot) -> NamespacePublication {
        NamespacePublication {
            operation: operation(2),
            parent: base.cursor,
            expected_nodes: Vec::new(),
            expected_directories: Vec::new(),
            target,
        }
    }

    #[test]
    fn rename_preserves_file_identity_and_advances_only_the_directory() {
        let base = base_snapshot();
        let mut target = base.clone();
        target.cursor = cursor(2, 2);
        let directory = target.directories.get_mut(&ROOT).unwrap();
        let entry = directory.entries.remove("old").unwrap();
        directory.entries.insert("new".to_owned(), entry);
        directory.generation = managed_generation(2);
        let mut publication = publication(&base, target);
        publication
            .expected_directories
            .push(DirectoryPrecondition {
                directory: ROOT,
                expected_generation: Some(managed_generation(1)),
            });

        assert_eq!(
            publication.target.directories[&ROOT].entries["new"].node,
            FILE
        );
        assert_eq!(
            publication.target.nodes[&FILE].generation,
            base.nodes[&FILE].generation
        );
        assert!(validate_publication(&publication, Some(&base)).unwrap());
    }

    #[test]
    fn file_content_or_attributes_require_the_next_node_generation() {
        let base = base_snapshot();
        let mut content_changed = base.clone();
        content_changed.cursor = cursor(2, 2);
        let version = file_version(4, [4; 32]);
        content_changed.nodes.get_mut(&FILE).unwrap().file_version = Some(version.id);
        content_changed.file_versions = BTreeMap::from([(version.id, version)]);

        let mut attributes_changed = base.clone();
        attributes_changed.cursor = cursor(2, 2);
        attributes_changed
            .nodes
            .get_mut(&FILE)
            .unwrap()
            .attributes
            .executable = true;

        for target in [content_changed, attributes_changed] {
            let mut publication = publication(&base, target);
            publication.expected_nodes.push(NodePrecondition {
                node: FILE,
                expected_generation: Some(managed_generation(1)),
            });
            assert!(validate_publication(&publication, Some(&base)).is_err());

            publication.target.nodes.get_mut(&FILE).unwrap().generation = managed_generation(2);
            assert!(validate_publication(&publication, Some(&base)).unwrap());
        }
    }

    #[test]
    fn stale_node_or_directory_precondition_is_a_conflict() {
        let base = base_snapshot();
        let mut target = base.clone();
        target.cursor = cursor(2, 2);

        let mut stale_node = publication(&base, target.clone());
        stale_node.expected_nodes.push(NodePrecondition {
            node: FILE,
            expected_generation: Some(managed_generation(2)),
        });
        assert!(!validate_publication(&stale_node, Some(&base)).unwrap());

        let mut stale_directory = publication(&base, target);
        stale_directory
            .expected_directories
            .push(DirectoryPrecondition {
                directory: ROOT,
                expected_generation: Some(managed_generation(2)),
            });
        assert!(!validate_publication(&stale_directory, Some(&base)).unwrap());
    }
}
