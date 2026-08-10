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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::validation::{validate_directory_generation, validate_node_generation};
use super::{decode_file_version, file_versions_have_consistent_segments};
use crate::filesystem::{
    BranchId, ChangeCursor, DirectoryRecord, OperationId, VolumeError, VolumeId, VolumeMutation,
    VolumeSnapshot,
};
use crate::managed::error::corrupt;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceChange {
    pub(crate) origin_branch: Option<BranchId>,
    pub(crate) mutation: VolumeMutation,
}

pub(super) struct ValidatedChange {
    directories: Vec<Option<DirectoryRecord>>,
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

    pub(super) fn apply_validated(
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
        for change in &self.mutation.nodes {
            match &change.target {
                Some(record) => target.nodes.insert(change.node, record.clone()),
                None => target.nodes.remove(&change.node),
            };
        }
        for (change, record) in self.mutation.directories.iter().zip(validated.directories) {
            match record {
                Some(record) => target.directories.insert(change.directory, record),
                None => target.directories.remove(&change.directory),
            };
        }
        for change in &self.mutation.file_versions {
            match &change.target {
                Some(record) => target.file_versions.insert(change.version, record.clone()),
                None => target.file_versions.remove(&change.version),
            };
        }
        target.root = self.mutation.root;
        target.cursor = self.mutation.cursor;
        target
    }

    pub(super) fn validate_against(
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
        for change in &self.mutation.nodes {
            let current = nodes.get(&change.node);
            if current.map(|record| &record.generation) != change.expected_generation.as_ref() {
                return Ok(None);
            }
            if current.is_none() && change.target.is_none() {
                return Err(corrupt(
                    "read Managed transaction",
                    "node removal is invalid",
                ));
            }
            validate_node_generation(current, change.target.as_ref())?;
        }
        let mut validated_directories = Vec::with_capacity(self.mutation.directories.len());
        for change in &self.mutation.directories {
            let current = directories.get(&change.directory);
            if current.map(|record| &record.generation) != change.expected_generation.as_ref() {
                return Ok(None);
            }
            let target = change
                .target
                .as_ref()
                .map(|delta| delta.apply(change.directory, current))
                .transpose()?;
            if current.is_none() && target.is_none() {
                return Err(corrupt(
                    "read Managed transaction",
                    "directory removal is invalid",
                ));
            }
            validate_directory_generation(current, target.as_ref())?;
            validated_directories.push(target);
        }
        for change in &self.mutation.file_versions {
            match (&change.target, versions.get(&change.version)) {
                (None, None) => {
                    return Err(corrupt(
                        "read Managed transaction",
                        "file version removal is invalid",
                    ));
                }
                (Some(target), Some(current)) if target != current => {
                    return Err(corrupt(
                        "read Managed transaction",
                        "file version replacement is invalid",
                    ));
                }
                (Some(_), _) | (None, Some(_)) => {}
            }
        }
        if !self.mutation.file_versions.is_empty() {
            let mut target_versions: BTreeMap<_, _> = versions
                .iter()
                .map(|(id, version)| (*id, version))
                .collect();
            for change in &self.mutation.file_versions {
                match &change.target {
                    Some(target) => {
                        target_versions.insert(change.version, target);
                    }
                    None => {
                        target_versions.remove(&change.version);
                    }
                }
            }
            if !file_versions_have_consistent_segments(target_versions.into_values()) {
                return Err(corrupt(
                    "read Managed transaction",
                    "file version delta is invalid",
                ));
            }
        }
        Ok(Some(ValidatedChange {
            directories: validated_directories,
        }))
    }

    pub(crate) fn validate(&self, volume_id: VolumeId) -> Result<(), VolumeError> {
        self.mutation.validate_ancestry(volume_id).map_err(|_| {
            corrupt(
                "read Managed transaction",
                "transaction ancestry is invalid",
            )
        })?;
        let mutation = &self.mutation;
        let ordered = strictly_ordered_by(&mutation.nodes, |left, right| left.node < right.node)
            && strictly_ordered_by(&mutation.directories, |left, right| {
                left.directory < right.directory
            })
            && strictly_ordered_by(&mutation.file_versions, |left, right| {
                left.version < right.version
            })
            && mutation.directories.iter().all(|change| {
                change.target.as_ref().is_none_or(|directory| {
                    strictly_ordered_by(&directory.remove_entries, |left, right| left < right)
                })
            })
            && mutation.nodes.iter().all(|change| {
                change
                    .target
                    .as_ref()
                    .is_none_or(|node| node.id == change.node)
            })
            && mutation.file_versions.iter().all(|change| {
                change
                    .target
                    .as_ref()
                    .is_none_or(|version| version.id == change.version)
            });
        if !ordered {
            return Err(corrupt(
                "read Managed transaction",
                "transaction effects are not strictly ordered",
            ));
        }
        if mutation
            .file_versions
            .iter()
            .filter_map(|change| change.target.as_ref())
            .any(|version| decode_file_version(version).is_err())
        {
            return Err(corrupt(
                "read Managed transaction",
                "transaction file version is invalid",
            ));
        }
        Ok(())
    }
}

fn strictly_ordered_by<T>(values: &[T], before: impl Fn(&T, &T) -> bool) -> bool {
    values.windows(2).all(|pair| before(&pair[0], &pair[1]))
}
