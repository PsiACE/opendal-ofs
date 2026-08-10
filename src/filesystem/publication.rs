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

//! Generation-checked filesystem publications.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{
    ChangeCursor, DirectoryEntry, DirectoryRecord, FileVersion, FileVersionId, Generation, NodeId,
    NodeRecord, OperationId, VolumeError, VolumeErrorKind, VolumeId, VolumeSnapshot,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeChange {
    pub node: NodeId,
    pub expected_generation: Option<Generation>,
    pub target: Option<NodeRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectoryChange {
    pub directory: NodeId,
    pub expected_generation: Option<Generation>,
    pub target: Option<DirectoryMutation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileVersionChange {
    pub version: FileVersionId,
    pub target: Option<FileVersion>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectoryMutation {
    pub(crate) generation: Generation,
    pub(crate) put_entries: BTreeMap<String, DirectoryEntry>,
    pub(crate) remove_entries: Vec<String>,
}

impl DirectoryMutation {
    fn between(target: &DirectoryRecord, base: Option<&DirectoryRecord>) -> Self {
        Self {
            generation: target.generation.clone(),
            put_entries: target
                .entries
                .iter()
                .filter(|(name, entry)| {
                    base.and_then(|base| base.entries.get(*name)) != Some(*entry)
                })
                .map(|(name, entry)| (name.clone(), *entry))
                .collect(),
            remove_entries: base
                .into_iter()
                .flat_map(|base| {
                    base.entries
                        .keys()
                        .filter(|name| !target.entries.contains_key(*name))
                        .cloned()
                })
                .collect(),
        }
    }

    pub(crate) fn validate_against(
        &self,
        base: Option<&DirectoryRecord>,
    ) -> Result<bool, VolumeError> {
        if self.remove_entries.iter().any(|name| {
            self.put_entries.contains_key(name)
                || base.is_none_or(|directory| !directory.entries.contains_key(name))
        }) {
            return Err(invalid_mutation("directory entry removal is invalid"));
        }
        Ok(base.is_none()
            || !self.remove_entries.is_empty()
            || self.put_entries.iter().any(|(name, entry)| {
                base.and_then(|directory| directory.entries.get(name)) != Some(entry)
            }))
    }

    pub(crate) fn apply(&self, node: NodeId, base: Option<DirectoryRecord>) -> DirectoryRecord {
        let mut directory = base.unwrap_or(DirectoryRecord {
            node,
            generation: self.generation.clone(),
            entries: BTreeMap::new(),
        });
        directory.generation = self.generation.clone();
        for name in &self.remove_entries {
            directory.entries.remove(name);
        }
        for (name, entry) in &self.put_entries {
            directory.entries.insert(name.clone(), *entry);
        }
        directory
    }
}

/// The changed records in one generation-checked publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VolumeMutation {
    pub(crate) volume_id: VolumeId,
    pub(crate) operation: OperationId,
    pub(crate) parent: ChangeCursor,
    pub(crate) cursor: ChangeCursor,
    pub(crate) root: NodeId,
    pub(crate) nodes: Vec<NodeChange>,
    pub(crate) directories: Vec<DirectoryChange>,
    pub(crate) file_versions: Vec<FileVersionChange>,
}

impl VolumeMutation {
    fn between(
        operation: OperationId,
        base: Option<&VolumeSnapshot>,
        target: &VolumeSnapshot,
    ) -> Self {
        let empty_nodes = BTreeMap::new();
        let empty_directories = BTreeMap::new();
        let empty_versions = BTreeMap::new();
        let base_nodes = base.map_or(&empty_nodes, |snapshot| &snapshot.nodes);
        let base_directories = base.map_or(&empty_directories, |snapshot| &snapshot.directories);
        let base_versions = base.map_or(&empty_versions, |snapshot| &snapshot.file_versions);
        let nodes = changed_keys(base_nodes, &target.nodes)
            .map(|node| NodeChange {
                node,
                expected_generation: base_nodes
                    .get(&node)
                    .map(|record| record.generation.clone()),
                target: target.nodes.get(&node).cloned(),
            })
            .collect();
        let directories = changed_keys(base_directories, &target.directories)
            .map(|directory| DirectoryChange {
                directory,
                expected_generation: base_directories
                    .get(&directory)
                    .map(|record| record.generation.clone()),
                target: target.directories.get(&directory).map(|record| {
                    DirectoryMutation::between(record, base_directories.get(&directory))
                }),
            })
            .collect();
        let file_versions = changed_keys(base_versions, &target.file_versions)
            .map(|version| FileVersionChange {
                version,
                target: target.file_versions.get(&version).cloned(),
            })
            .collect();
        Self {
            volume_id: target.volume_id,
            operation,
            parent: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            cursor: target.cursor,
            root: target.root,
            nodes,
            directories,
            file_versions,
        }
    }

    pub(crate) fn validate_ancestry(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        if self.volume_id != volume_id
            || self.cursor.operation() != Some(self.operation)
            || self.parent.sequence().checked_add(1) != Some(self.cursor.sequence())
        {
            return Err(invalid_mutation("mutation ancestry is invalid"));
        }
        Ok(())
    }
}

fn changed_keys<'a, K: Copy + Ord, V: PartialEq>(
    base: &'a BTreeMap<K, V>,
    target: &'a BTreeMap<K, V>,
) -> impl Iterator<Item = K> + 'a {
    base.keys()
        .chain(target.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| base.get(key) != target.get(key))
}

fn invalid_mutation(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Invalid, message)
}

/// One generation-checked filesystem publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumePublication {
    pub target: VolumeSnapshot,
    mutation: VolumeMutation,
}

impl VolumePublication {
    pub(crate) fn between(
        operation: OperationId,
        base: Option<&VolumeSnapshot>,
        target: VolumeSnapshot,
    ) -> Result<Self, VolumeError> {
        target.validate_structure()?;
        let parent = base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor);
        if target.cursor.operation() != Some(operation)
            || parent.sequence().checked_add(1) != Some(target.cursor.sequence())
            || base.is_some_and(|base| base.volume_id != target.volume_id)
        {
            return Err(invalid_mutation("publication ancestry is invalid"));
        }
        let mutation = VolumeMutation::between(operation, base, &target);
        Ok(Self { target, mutation })
    }

    pub(crate) fn mutation(&self) -> &VolumeMutation {
        &self.mutation
    }
}

/// The authoritative result of a generation-checked publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    /// The mutation is visible at this change-stream position.
    Committed(ChangeCursor),
    /// Recovery proved that the operation did not commit.
    Absent,
    /// An observed precondition no longer matches authoritative state.
    Conflict { observed: ChangeCursor },
    /// The caller must retain its intent and resolve the original operation.
    Unknown,
}
