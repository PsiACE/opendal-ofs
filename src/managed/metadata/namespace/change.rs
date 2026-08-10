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
use sha2::{Digest as _, Sha256};

use super::records::managed_generation_number;
use super::validation::{validate_generation, validate_node_generation};
use super::{decode_file_version, file_versions_have_consistent_segments};
use crate::filesystem::{
    BranchId, ChangeCursor, Generation, NodeKind, OperationId, VolumeError, VolumeId,
    VolumeMutation, VolumeSnapshot,
};
use crate::managed::error::corrupt;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceChange {
    pub(crate) origin_branch: Option<BranchId>,
    pub(crate) mutation: VolumeMutation,
}

pub(super) struct ValidatedChange(());

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

    pub(crate) fn request_sha256(&self) -> Result<[u8; 32], VolumeError> {
        let mut digest = Sha256::new();
        digest.update(b"OFS1REQ1");
        match self.origin_branch {
            None => digest.update([0]),
            Some(branch) => {
                digest.update([1]);
                digest.update(branch.as_bytes());
            }
        }
        let mutation = &self.mutation;
        digest.update(mutation.volume_id.as_bytes());
        digest.update(mutation.operation.as_bytes());
        hash_cursor(&mut digest, mutation.parent);
        hash_cursor(&mut digest, mutation.cursor);
        digest.update(mutation.root.as_bytes());

        hash_len(&mut digest, mutation.nodes.len())?;
        for change in &mutation.nodes {
            digest.update(change.node.as_bytes());
            hash_optional_generation(&mut digest, change.expected_generation.as_ref())?;
            match &change.target {
                None => digest.update([0]),
                Some(node) => {
                    digest.update([1]);
                    hash_generation(&mut digest, &node.generation)?;
                    hash_kind(&mut digest, node.kind);
                    digest.update([u8::from(node.attributes.executable)]);
                    match node.file_version {
                        None => digest.update([0]),
                        Some(version) => {
                            digest.update([1]);
                            digest.update(version.as_bytes());
                        }
                    }
                }
            }
        }
        hash_len(&mut digest, mutation.directories.len())?;
        for change in &mutation.directories {
            digest.update(change.directory.as_bytes());
            hash_optional_generation(&mut digest, change.expected_generation.as_ref())?;
            match &change.target {
                None => digest.update([0]),
                Some(directory) => {
                    digest.update([1]);
                    hash_generation(&mut digest, &directory.generation)?;
                    hash_len(&mut digest, directory.remove_entries.len())?;
                    for name in &directory.remove_entries {
                        hash_name(&mut digest, name)?;
                    }
                    hash_len(&mut digest, directory.put_entries.len())?;
                    for (name, entry) in &directory.put_entries {
                        hash_name(&mut digest, name)?;
                        digest.update(entry.node.as_bytes());
                        hash_kind(&mut digest, entry.kind);
                    }
                }
            }
        }
        hash_len(&mut digest, mutation.file_versions.len())?;
        for change in &mutation.file_versions {
            digest.update(change.version.as_bytes());
            digest.update([u8::from(change.target.is_some())]);
        }
        Ok(digest.finalize().into())
    }

    pub(crate) fn encoded_len(&self) -> Result<usize, VolumeError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(self, &mut bytes).map_err(|_| {
            corrupt(
                "read Managed namespace",
                "namespace change cannot be encoded",
            )
        })?;
        Ok(bytes.len())
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
        _validated: ValidatedChange,
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
        for change in &self.mutation.directories {
            match &change.target {
                Some(delta) => {
                    let current = target.directories.remove(&change.directory);
                    target
                        .directories
                        .insert(change.directory, delta.apply(change.directory, current));
                }
                None => {
                    target.directories.remove(&change.directory);
                }
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
        for change in &self.mutation.directories {
            let current = directories.get(&change.directory);
            if current.map(|record| &record.generation) != change.expected_generation.as_ref() {
                return Ok(None);
            }
            if current.is_none() && change.target.is_none() {
                return Err(corrupt(
                    "read Managed transaction",
                    "directory removal is invalid",
                ));
            }
            let (generation, changed) = match &change.target {
                Some(delta) => (Some(&delta.generation), delta.validate_against(current)?),
                None => (None, true),
            };
            validate_generation(
                current.map(|directory| &directory.generation),
                generation,
                changed,
            )?;
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
        Ok(Some(ValidatedChange(())))
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

fn hash_len(digest: &mut Sha256, length: usize) -> Result<(), VolumeError> {
    let length = u64::try_from(length).map_err(|_| {
        corrupt(
            "read Managed transaction",
            "transaction request length overflows",
        )
    })?;
    digest.update(length.to_be_bytes());
    Ok(())
}

fn hash_cursor(digest: &mut Sha256, cursor: ChangeCursor) {
    match cursor {
        ChangeCursor::Genesis => digest.update([0]),
        ChangeCursor::At {
            sequence,
            operation,
        } => {
            digest.update([1]);
            digest.update(sequence.get().to_be_bytes());
            digest.update(operation.as_bytes());
        }
    }
}

fn hash_generation(digest: &mut Sha256, generation: &Generation) -> Result<(), VolumeError> {
    let generation = managed_generation_number(generation).ok_or_else(|| {
        corrupt(
            "read Managed transaction",
            "transaction generation is invalid",
        )
    })?;
    digest.update(generation.to_be_bytes());
    Ok(())
}

fn hash_optional_generation(
    digest: &mut Sha256,
    generation: Option<&Generation>,
) -> Result<(), VolumeError> {
    match generation {
        None => digest.update([0]),
        Some(generation) => {
            digest.update([1]);
            hash_generation(digest, generation)?;
        }
    }
    Ok(())
}

fn hash_kind(digest: &mut Sha256, kind: NodeKind) {
    digest.update([match kind {
        NodeKind::Directory => 0,
        NodeKind::RegularFile => 1,
    }]);
}

fn hash_name(digest: &mut Sha256, name: &str) -> Result<(), VolumeError> {
    hash_len(digest, name.len())?;
    digest.update(name.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use super::*;
    use crate::filesystem::{
        DirectoryEntry, DirectoryRecord, NodeAttributes, NodeId, NodeRecord, VolumePublication,
    };
    use crate::managed::format::{ContentRef, Extent, ExtentMap, SegmentRef};
    use crate::managed::metadata::namespace::{
        DecodedFileVersion, encode_file_version, managed_generation,
    };

    #[test]
    fn operation_request_sha256_is_interoperable() {
        let volume = VolumeId::from_bytes([1; 16]);
        let branch = BranchId::from_bytes([2; 16]);
        let prior = OperationId::from_bytes([3; 16]);
        let operation = OperationId::from_bytes([4; 16]);
        let root = NodeId::from_bytes([5; 16]);
        let file = NodeId::from_bytes([6; 16]);
        let base = VolumeSnapshot {
            volume_id: volume,
            cursor: ChangeCursor::at(NonZeroU64::MIN, prior),
            root,
            nodes: BTreeMap::from([(
                root,
                NodeRecord {
                    id: root,
                    generation: managed_generation(1),
                    kind: NodeKind::Directory,
                    attributes: NodeAttributes::default(),
                    file_version: None,
                },
            )]),
            directories: BTreeMap::from([(
                root,
                DirectoryRecord {
                    node: root,
                    generation: managed_generation(1),
                    entries: BTreeMap::new(),
                },
            )]),
            file_versions: BTreeMap::new(),
        };
        let decoded = DecodedFileVersion::from_extents(
            1,
            [7; 32],
            ExtentMap {
                extents: vec![Extent {
                    content: ContentRef {
                        digest: [7; 32],
                        length: 1,
                    },
                    segment: SegmentRef {
                        digest: [8; 32],
                        length: 11,
                    },
                    segment_offset: 4,
                }],
            },
        )
        .unwrap();
        let version = encode_file_version(&decoded).unwrap();
        let mut target = base.clone();
        target.cursor = ChangeCursor::at(NonZeroU64::new(2).unwrap(), operation);
        target.nodes.insert(
            file,
            NodeRecord {
                id: file,
                generation: managed_generation(1),
                kind: NodeKind::RegularFile,
                attributes: NodeAttributes { executable: true },
                file_version: Some(version.id),
            },
        );
        target.directories.insert(
            root,
            DirectoryRecord {
                node: root,
                generation: managed_generation(2),
                entries: BTreeMap::from([(
                    "δ.txt".to_owned(),
                    DirectoryEntry {
                        node: file,
                        kind: NodeKind::RegularFile,
                    },
                )]),
            },
        );
        target.file_versions.insert(version.id, version);
        let publication = VolumePublication::between(operation, Some(&base), target).unwrap();
        let change = NamespaceChange::new(publication.mutation().clone(), Some(branch));
        change.validate(volume).unwrap();

        assert_eq!(
            hex::encode(change.request_sha256().unwrap()),
            "627df8be759f08e34a61ffd1af19f5aedb3f50044639621181fad5bebaca088c"
        );
    }
}
