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

use serde::{Deserialize, Serialize};

use super::validation::{
    match_preconditions, validate_directory_generation, validate_node_generation,
};
use super::{
    DirectoryPrecondition, DirectoryRecord, FileVersionRecord, NamespacePublication,
    NamespaceSnapshot, NodePrecondition, NodeRecord,
};
use crate::filesystem::{
    ChangeCursor, DirectoryEntry, FileVersionId, Generation, NodeId, OperationId, VolumeId,
};
use crate::managed::{ManagedError, ManagedErrorKind};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceChange {
    pub(crate) volume_id: VolumeId,
    pub(crate) operation: OperationId,
    pub(crate) parent: ChangeCursor,
    pub(crate) cursor: ChangeCursor,
    pub(crate) root: NodeId,
    pub(crate) expected_nodes: Vec<NodePrecondition>,
    pub(crate) expected_directories: Vec<DirectoryPrecondition>,
    pub(crate) put_nodes: Vec<NodeRecord>,
    pub(crate) remove_nodes: Vec<NodeId>,
    put_directories: Vec<DirectoryDelta>,
    pub(crate) remove_directories: Vec<NodeId>,
    pub(crate) put_file_versions: Vec<FileVersionRecord>,
    pub(crate) remove_file_versions: Vec<FileVersionId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DirectoryDelta {
    node: NodeId,
    generation: Generation,
    put_entries: BTreeMap<String, DirectoryEntry>,
    remove_entries: Vec<String>,
}

impl DirectoryDelta {
    fn between(target: &DirectoryRecord, base: Option<&DirectoryRecord>) -> Self {
        Self {
            node: target.node,
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

    fn apply(&self, base: Option<&DirectoryRecord>) -> Result<DirectoryRecord, ManagedError> {
        let mut directory = base.cloned().unwrap_or(DirectoryRecord {
            node: self.node,
            generation: self.generation.clone(),
            entries: BTreeMap::new(),
        });
        if directory.node != self.node {
            return Err(corrupt("directory delta identity is invalid"));
        }
        directory.generation = self.generation.clone();
        let mut changed = BTreeSet::new();
        for name in &self.remove_entries {
            if !changed.insert(name.clone()) || directory.entries.remove(name).is_none() {
                return Err(corrupt("directory entry removal is invalid"));
            }
        }
        for (name, entry) in &self.put_entries {
            if !changed.insert(name.clone()) {
                return Err(corrupt("directory entry update is invalid"));
            }
            directory.entries.insert(name.clone(), *entry);
        }
        Ok(directory)
    }
}

impl NamespaceChange {
    pub(crate) fn from_publication(
        publication: &NamespacePublication,
        base: Option<&NamespaceSnapshot>,
    ) -> Self {
        let empty_nodes = BTreeMap::new();
        let empty_directories = BTreeMap::new();
        let empty_versions = BTreeMap::new();
        let base_nodes = base.map_or(&empty_nodes, |snapshot| &snapshot.nodes);
        let base_directories = base.map_or(&empty_directories, |snapshot| &snapshot.directories);
        let base_versions = base.map_or(&empty_versions, |snapshot| &snapshot.file_versions);
        let target = &publication.target;
        let mut expected_nodes = publication.expected_nodes.clone();
        let mut expected_directories = publication.expected_directories.clone();
        expected_nodes.sort_by_key(|condition| condition.node);
        expected_directories.sort_by_key(|condition| condition.directory);

        Self {
            volume_id: target.volume_id,
            operation: publication.operation,
            parent: publication.parent,
            cursor: target.cursor,
            root: target.root,
            expected_nodes,
            expected_directories,
            put_nodes: target
                .nodes
                .iter()
                .filter(|(id, record)| base_nodes.get(id) != Some(record))
                .map(|(_, record)| record.clone())
                .collect(),
            remove_nodes: base_nodes
                .keys()
                .filter(|id| !target.nodes.contains_key(id))
                .copied()
                .collect(),
            put_directories: target
                .directories
                .iter()
                .filter(|(id, record)| base_directories.get(id) != Some(record))
                .map(|(id, record)| DirectoryDelta::between(record, base_directories.get(id)))
                .collect(),
            remove_directories: base_directories
                .keys()
                .filter(|id| !target.directories.contains_key(id))
                .copied()
                .collect(),
            put_file_versions: target
                .file_versions
                .iter()
                .filter(|(id, record)| base_versions.get(id) != Some(record))
                .map(|(_, record)| record.clone())
                .collect(),
            remove_file_versions: base_versions
                .keys()
                .filter(|id| !target.file_versions.contains_key(id))
                .copied()
                .collect(),
        }
    }

    pub(crate) fn apply(
        &self,
        base: Option<NamespaceSnapshot>,
    ) -> Result<NamespaceSnapshot, ManagedError> {
        self.validate(self.volume_id)?;
        let mut target = match base {
            Some(base) if base.volume_id == self.volume_id && base.cursor == self.parent => base,
            Some(_) => return Err(corrupt("transaction base is invalid")),
            None if self.parent == ChangeCursor::Genesis => NamespaceSnapshot {
                volume_id: self.volume_id,
                cursor: ChangeCursor::Genesis,
                root: self.root,
                nodes: BTreeMap::new(),
                directories: BTreeMap::new(),
                file_versions: BTreeMap::new(),
            },
            None => return Err(corrupt("initial transaction does not begin at genesis")),
        };

        let expected_nodes = match_preconditions(
            &target.nodes,
            self.expected_nodes
                .iter()
                .map(|condition| (condition.node, condition.expected_generation.as_ref())),
            |record| &record.generation,
            "duplicate node precondition",
        )
        .map_err(|_| corrupt("transaction is invalid"))?
        .ok_or_else(|| corrupt("transaction preconditions are stale"))?;
        apply_records(
            &mut target.nodes,
            self.remove_nodes.iter().copied(),
            self.put_nodes.iter().cloned(),
            |record| record.id,
            |id, current, next| {
                validate_node_generation(current, next, expected_nodes.contains(&id))
            },
            "node delta is invalid",
        )?;
        let put_directories = self
            .put_directories
            .iter()
            .map(|delta| {
                let base = target.directories.get(&delta.node);
                delta.apply(base)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let expected_directories = match_preconditions(
            &target.directories,
            self.expected_directories
                .iter()
                .map(|condition| (condition.directory, condition.expected_generation.as_ref())),
            |record| &record.generation,
            "duplicate directory precondition",
        )
        .map_err(|_| corrupt("transaction is invalid"))?
        .ok_or_else(|| corrupt("transaction preconditions are stale"))?;
        apply_records(
            &mut target.directories,
            self.remove_directories.iter().copied(),
            put_directories,
            |record| record.node,
            |id, current, next| {
                validate_directory_generation(current, next, expected_directories.contains(&id))
            },
            "directory delta is invalid",
        )?;
        apply_records(
            &mut target.file_versions,
            self.remove_file_versions.iter().copied(),
            self.put_file_versions.iter().cloned(),
            |record| record.id,
            |_, current, next| {
                if next.is_some_and(|next| {
                    !next.is_valid() || current.is_some_and(|current| current != next)
                }) {
                    Err(corrupt("file version delta is invalid"))
                } else {
                    Ok(())
                }
            },
            "file version delta is invalid",
        )?;
        target.root = self.root;
        target.cursor = self.cursor;
        Ok(target)
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        if self.volume_id != volume_id
            || self.cursor.operation() != Some(self.operation)
            || self.parent.sequence().checked_add(1) != Some(self.cursor.sequence())
        {
            return Err(corrupt("transaction ancestry is invalid"));
        }
        Ok(())
    }
}

fn apply_records<K, V>(
    current: &mut BTreeMap<K, V>,
    removed: impl IntoIterator<Item = K>,
    put: impl IntoIterator<Item = V>,
    key: impl Fn(&V) -> K,
    validate: impl Fn(K, Option<&V>, Option<&V>) -> Result<(), ManagedError>,
    invalid_delta: &'static str,
) -> Result<(), ManagedError>
where
    K: Copy + Ord,
{
    let mut changed = BTreeSet::new();
    for id in removed {
        if !changed.insert(id) || !current.contains_key(&id) {
            return Err(corrupt(invalid_delta));
        }
        validate(id, current.get(&id), None).map_err(|_| corrupt("transaction is invalid"))?;
        current.remove(&id);
    }
    for record in put {
        let id = key(&record);
        if !changed.insert(id) {
            return Err(corrupt(invalid_delta));
        }
        validate(id, current.get(&id), Some(&record))
            .map_err(|_| corrupt("transaction is invalid"))?;
        current.insert(id, record);
    }
    Ok(())
}

fn corrupt(message: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Corrupt,
        "read Managed transaction",
        message,
    )
}
