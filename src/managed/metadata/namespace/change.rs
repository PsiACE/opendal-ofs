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

use super::decode_file_version;
use super::validation::{
    match_preconditions, validate_directory_generation, validate_node_generation,
};
use crate::filesystem::{
    BranchId, ChangeCursor, DirectoryRecord, FileVersion, OperationId, VolumeError, VolumeId,
    VolumeMutation, VolumeSnapshot,
};
use crate::managed::error::corrupt;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceChange {
    pub(crate) origin_branch: Option<BranchId>,
    pub(crate) mutation: VolumeMutation,
}

pub(crate) struct ValidatedChange {
    put_directories: Vec<DirectoryRecord>,
}

impl NamespaceChange {
    pub(crate) fn new(mutation: VolumeMutation, origin_branch: Option<BranchId>) -> Self {
        Self {
            origin_branch,
            mutation,
        }
    }

    pub(crate) const fn operation(&self) -> OperationId {
        self.mutation.operation
    }

    pub(crate) const fn parent(&self) -> ChangeCursor {
        self.mutation.parent
    }

    pub(crate) const fn cursor(&self) -> ChangeCursor {
        self.mutation.cursor
    }

    pub(crate) fn apply(
        &self,
        base: Option<VolumeSnapshot>,
    ) -> Result<VolumeSnapshot, VolumeError> {
        let Some(validated) = self.validate_against(base.as_ref()).map_err(|_| {
            corrupt(
                "read Managed transaction",
                "transaction transition is invalid",
            )
        })?
        else {
            return Err(corrupt(
                "read Managed transaction",
                "transaction preconditions are stale",
            ));
        };
        Ok(self.apply_validated(base, validated))
    }

    pub(crate) fn apply_validated(
        &self,
        base: Option<VolumeSnapshot>,
        validated: ValidatedChange,
    ) -> VolumeSnapshot {
        let mut target = base.unwrap_or_else(|| VolumeSnapshot {
            volume_id: self.mutation.volume_id,
            cursor: ChangeCursor::Genesis,
            root: self.mutation.root,
            nodes: BTreeMap::new(),
            directories: BTreeMap::new(),
            file_versions: BTreeMap::new(),
        });
        for id in &self.mutation.remove_nodes {
            target.nodes.remove(id);
        }
        target.nodes.extend(
            self.mutation
                .put_nodes
                .iter()
                .cloned()
                .map(|record| (record.id, record)),
        );
        for id in &self.mutation.remove_directories {
            target.directories.remove(id);
        }
        target.directories.extend(
            validated
                .put_directories
                .into_iter()
                .map(|record| (record.node, record)),
        );
        for id in &self.mutation.remove_file_versions {
            target.file_versions.remove(id);
        }
        target.file_versions.extend(
            self.mutation
                .put_file_versions
                .iter()
                .cloned()
                .map(|record| (record.id, record)),
        );
        target.root = self.mutation.root;
        target.cursor = self.mutation.cursor;
        target
    }

    pub(crate) fn validate_against(
        &self,
        base: Option<&VolumeSnapshot>,
    ) -> Result<Option<ValidatedChange>, VolumeError> {
        if base.is_some_and(|base| {
            base.volume_id != self.mutation.volume_id || base.cursor != self.mutation.parent
        }) || base.is_none() && self.mutation.parent != ChangeCursor::Genesis
        {
            return Err(corrupt(
                "read Managed transaction",
                "transaction base is invalid",
            ));
        }
        let empty_nodes = BTreeMap::new();
        let empty_directories = BTreeMap::new();
        let empty_versions = BTreeMap::new();
        let nodes = base.map_or(&empty_nodes, |snapshot| &snapshot.nodes);
        let directories = base.map_or(&empty_directories, |snapshot| &snapshot.directories);
        let versions = base.map_or(&empty_versions, |snapshot| &snapshot.file_versions);
        let Some(expected_nodes) = match_preconditions(
            nodes,
            self.mutation
                .expected_nodes
                .iter()
                .map(|condition| (condition.node, condition.expected_generation.as_ref())),
            |record| &record.generation,
            "duplicate node precondition",
        )?
        else {
            return Ok(None);
        };
        validate_records(
            nodes,
            self.mutation.remove_nodes.iter().copied(),
            self.mutation.put_nodes.iter(),
            |record| record.id,
            |id, current, next| {
                validate_node_generation(current, next, expected_nodes.contains(&id))
            },
        )?;
        let put_directories = self
            .mutation
            .put_directories
            .iter()
            .map(|delta| delta.apply(directories.get(&delta.node)))
            .collect::<Result<Vec<_>, _>>()?;
        let Some(expected_directories) = match_preconditions(
            directories,
            self.mutation
                .expected_directories
                .iter()
                .map(|condition| (condition.directory, condition.expected_generation.as_ref())),
            |record| &record.generation,
            "duplicate directory precondition",
        )?
        else {
            return Ok(None);
        };
        validate_records(
            directories,
            self.mutation.remove_directories.iter().copied(),
            put_directories.iter(),
            |record| record.node,
            |id, current, next| {
                validate_directory_generation(current, next, expected_directories.contains(&id))
            },
        )?;
        validate_records(
            versions,
            self.mutation.remove_file_versions.iter().copied(),
            self.mutation.put_file_versions.iter(),
            |record| record.id,
            |_, current, next| {
                if next.is_some_and(|next| {
                    decode_file_version(next).is_err()
                        || current.is_some_and(|current: &FileVersion| current != next)
                }) {
                    Err(corrupt(
                        "read Managed transaction",
                        "file version delta is invalid",
                    ))
                } else {
                    Ok(())
                }
            },
        )?;
        Ok(Some(ValidatedChange { put_directories }))
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        self.mutation.validate_ancestry(volume_id).map_err(|_| {
            corrupt(
                "read Managed transaction",
                "transaction ancestry is invalid",
            )
        })
    }
}

fn validate_records<'a, K, V: 'a>(
    current: &BTreeMap<K, V>,
    removed: impl IntoIterator<Item = K>,
    put: impl IntoIterator<Item = &'a V>,
    key: impl Fn(&V) -> K,
    validate: impl Fn(K, Option<&V>, Option<&V>) -> Result<(), VolumeError>,
) -> Result<(), VolumeError>
where
    K: Copy + Ord,
{
    let mut changed = BTreeSet::new();
    for id in removed {
        if !changed.insert(id) || !current.contains_key(&id) {
            return Err(corrupt(
                "read Managed transaction",
                "transaction delta is invalid",
            ));
        }
        validate(id, current.get(&id), None)?;
    }
    for record in put {
        let id = key(record);
        if !changed.insert(id) {
            return Err(corrupt(
                "read Managed transaction",
                "transaction delta is invalid",
            ));
        }
        validate(id, current.get(&id), Some(record))?;
    }
    Ok(())
}
