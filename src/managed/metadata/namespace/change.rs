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

use super::validation::validate_publication;
use super::{
    DirectoryPrecondition, DirectoryRecord, FileVersionRecord, NamespacePublication,
    NamespaceSnapshot, NodePrecondition, NodeRecord,
};
use crate::filesystem::{ChangeCursor, FileVersionId, NodeId, OperationId, VolumeId};
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
    pub(crate) put_directories: Vec<DirectoryRecord>,
    pub(crate) remove_directories: Vec<NodeId>,
    pub(crate) put_file_versions: Vec<FileVersionRecord>,
    pub(crate) remove_file_versions: Vec<FileVersionId>,
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
                .map(|(_, record)| record.clone())
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
        self,
        base: Option<NamespaceSnapshot>,
    ) -> Result<NamespaceSnapshot, ManagedError> {
        let validation_base = base.clone();
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

        apply_records(
            &mut target.nodes,
            self.remove_nodes,
            self.put_nodes,
            |record| record.id,
            "node delta is invalid",
        )?;
        apply_records(
            &mut target.directories,
            self.remove_directories,
            self.put_directories,
            |record| record.node,
            "directory delta is invalid",
        )?;
        apply_records(
            &mut target.file_versions,
            self.remove_file_versions,
            self.put_file_versions,
            |record| record.id,
            "file version delta is invalid",
        )?;
        target.root = self.root;
        target.cursor = self.cursor;

        let publication = NamespacePublication {
            operation: self.operation,
            parent: self.parent,
            expected_nodes: self.expected_nodes,
            expected_directories: self.expected_directories,
            target: target.clone(),
        };
        if !validate_publication(&publication, validation_base.as_ref())
            .map_err(|_| corrupt("transaction is invalid"))?
        {
            return Err(corrupt("transaction preconditions are stale"));
        }
        Ok(target)
    }
}

fn apply_records<K, V>(
    current: &mut BTreeMap<K, V>,
    removed: Vec<K>,
    put: Vec<V>,
    key: impl Fn(&V) -> K,
    invalid_delta: &'static str,
) -> Result<(), ManagedError>
where
    K: Copy + Ord,
{
    let mut changed = BTreeSet::new();
    for id in removed {
        if !changed.insert(id) || current.remove(&id).is_none() {
            return Err(corrupt(invalid_delta));
        }
    }
    for record in put {
        if !changed.insert(key(&record)) {
            return Err(corrupt(invalid_delta));
        }
        current.insert(key(&record), record);
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
