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
    DirectoryRecord, NamespaceSnapshot, NodeRecord, managed_generation, managed_generation_number,
    next_managed_generation,
};
use super::{StoredChange, ValidatedChange};
use crate::filesystem::{FileVersionId, Generation, NodeAttributes, NodeId, NodeKind, VolumeError};
use crate::managed::error::invalid;

pub(crate) fn validate_publication(
    change: &StoredChange,
    base: Option<&NamespaceSnapshot>,
) -> Result<Option<ValidatedChange>, VolumeError> {
    change.validate_against(base).map_err(|_| {
        invalid(
            "publish Managed namespace",
            "publication mutation is invalid",
        )
    })
}

pub(crate) fn validate_snapshot(snapshot: &NamespaceSnapshot) -> Result<(), VolumeError> {
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
) -> Result<Option<BTreeSet<K>>, VolumeError>
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

pub(super) fn validate_node_generation(
    current: Option<&NodeRecord>,
    next: Option<&NodeRecord>,
    has_precondition: bool,
) -> Result<(), VolumeError> {
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
) -> Result<(), VolumeError> {
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
) -> Result<(), VolumeError> {
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::filesystem::{ChangeCursor, OperationId, VolumeId};

    const ROOT: NodeId = NodeId::from_bytes([1; 16]);

    fn cursor(n: u64) -> ChangeCursor {
        ChangeCursor::at(
            NonZeroU64::new(n).unwrap(),
            OperationId::from_bytes([n as u8; 16]),
        )
    }

    fn base_snapshot() -> NamespaceSnapshot {
        NamespaceSnapshot {
            volume_id: VolumeId::from_bytes([7; 16]),
            cursor: cursor(1),
            root: ROOT,
            nodes: BTreeMap::from([(
                ROOT,
                NodeRecord {
                    id: ROOT,
                    generation: managed_generation(1),
                    kind: NodeKind::Directory,
                    attributes: NodeAttributes::default(),
                    file_version: None,
                },
            )]),
            directories: BTreeMap::from([(
                ROOT,
                DirectoryRecord {
                    node: ROOT,
                    generation: managed_generation(1),
                    entries: BTreeMap::new(),
                },
            )]),
            file_versions: BTreeMap::new(),
        }
    }

    #[test]
    fn stale_precondition_is_a_conflict() {
        let base = base_snapshot();
        let mut target = base.clone();
        target.cursor = cursor(2);
        let publication = crate::filesystem::VolumePublication::between(
            OperationId::from_bytes([2; 16]),
            Some(&base),
            target,
        )
        .unwrap();
        let mut change = StoredChange::new(publication.mutation().clone(), None);
        change.mutation.expected_nodes = vec![crate::filesystem::NodePrecondition {
            node: ROOT,
            expected_generation: Some(managed_generation(2)),
        }];
        assert!(
            validate_publication(&change, Some(&base))
                .unwrap()
                .is_none()
        );
    }
}
