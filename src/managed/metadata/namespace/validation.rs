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
    DirectoryRecord, NamespacePublication, NamespaceSnapshot, NodeRecord, managed_generation,
    managed_generation_number, next_managed_generation,
};
use crate::filesystem::{
    ChangeCursor, FileVersionId, Generation, NodeAttributes, NodeId, NodeKind,
};
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
    let target = &publication.target;
    validate_snapshot(target)?;
    let empty_nodes = BTreeMap::new();
    let empty_directories = BTreeMap::new();
    let nodes = base.map_or(&empty_nodes, |state| &state.nodes);
    let directories = base.map_or(&empty_directories, |state| &state.directories);
    let Some(node_conditions) = match_preconditions(
        nodes,
        publication
            .expected_nodes
            .iter()
            .map(|condition| (condition.node, condition.expected_generation.as_ref())),
        |record| &record.generation,
        "duplicate node precondition",
    )?
    else {
        return Ok(false);
    };
    let Some(directory_conditions) = match_preconditions(
        directories,
        publication
            .expected_directories
            .iter()
            .map(|condition| (condition.directory, condition.expected_generation.as_ref())),
        |record| &record.generation,
        "duplicate directory precondition",
    )?
    else {
        return Ok(false);
    };
    validate_generations(
        target,
        &node_conditions,
        &directory_conditions,
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

pub(super) fn match_preconditions<'a, K, V>(
    current: &BTreeMap<K, V>,
    expected: impl IntoIterator<Item = (K, Option<&'a Generation>)>,
    generation: impl Fn(&V) -> &Generation,
    duplicate: &'static str,
) -> Result<Option<BTreeSet<K>>, ManagedError>
where
    K: Copy + Ord,
{
    let mut unique = BTreeSet::new();
    for (key, expected_generation) in expected {
        if !unique.insert(key) {
            return Err(invalid("publish Managed namespace", duplicate));
        }
        if current.get(&key).map(&generation) != expected_generation {
            return Ok(None);
        }
    }
    Ok(Some(unique))
}

fn validate_generations(
    target: &NamespaceSnapshot,
    node_conditions: &BTreeSet<NodeId>,
    directory_conditions: &BTreeSet<NodeId>,
    nodes: &BTreeMap<NodeId, NodeRecord>,
    directories: &BTreeMap<NodeId, DirectoryRecord>,
) -> Result<(), ManagedError> {
    for id in nodes.keys().chain(target.nodes.keys()) {
        let current = nodes.get(id);
        let next = target.nodes.get(id);
        validate_node_generation(current, next, node_conditions.contains(id))?;
    }

    for id in directories.keys().chain(target.directories.keys()) {
        let current = directories.get(id);
        let next = target.directories.get(id);
        validate_directory_generation(current, next, directory_conditions.contains(id))?;
    }
    Ok(())
}

pub(super) fn validate_node_generation(
    current: Option<&NodeRecord>,
    next: Option<&NodeRecord>,
    has_precondition: bool,
) -> Result<(), ManagedError> {
    validate_generation(
        current.map(|record| &record.generation),
        next.map(|record| &record.generation),
        current.map(node_body) != next.map(node_body),
        has_precondition,
    )
}

pub(super) fn validate_directory_generation(
    current: Option<&DirectoryRecord>,
    next: Option<&DirectoryRecord>,
    has_precondition: bool,
) -> Result<(), ManagedError> {
    validate_generation(
        current.map(|record| &record.generation),
        next.map(|record| &record.generation),
        current.map(|record| &record.entries) != next.map(|record| &record.entries),
        has_precondition,
    )
}

fn validate_generation(
    current: Option<&Generation>,
    next: Option<&Generation>,
    changed: bool,
    has_precondition: bool,
) -> Result<(), ManagedError> {
    let expected = match (current, next, changed) {
        (None, Some(_), _) => managed_generation(1),
        (Some(generation), Some(_), false) => generation.clone(),
        (Some(generation), Some(_), true) => next_managed_generation(generation)
            .ok_or_else(|| invalid("publish Managed namespace", "record generation overflow"))?,
        (Some(_), None, _) if has_precondition => return Ok(()),
        (Some(_), None, _) => {
            return Err(invalid(
                "publish Managed namespace",
                "changed record lacks a precondition",
            ));
        }
        (None, None, _) => return Ok(()),
    };
    if next.is_some_and(|generation| *generation != expected) || changed && !has_precondition {
        return Err(invalid(
            "publish Managed namespace",
            "record generation transition is invalid",
        ));
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
    use crate::filesystem::{
        DirectoryEntry, DirectoryPrecondition, NodePrecondition, OperationId, VolumeId,
    };
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
