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

//! Branch checkpoint records stored in the shared Managed SSTable format.
//!
//! The storage providers own only the immutable-part I/O.  Record boundaries,
//! paging, reconstruction, and validation stay here so Object and D1 cannot
//! acquire subtly different checkpoint formats.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::records::{
    FORMAT_MAJOR, StoredCheckpoint, StoredCommittedResult, StoredDirectory, StoredDirectoryEntry,
    StoredFileVersion, StoredNode, StoredSnapshot,
};
use crate::managed::format::{Extent, ExtentMap, sstable};
use crate::managed::{ManagedError, ManagedErrorKind};

/// Keeps D1 values comfortably below its row/request limits while producing
/// reasonably sized immutable Object writes. This is a target, not a limit on
/// the complete checkpoint or on a single natural record.
const TARGET_PART_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointRoot {
    pub(crate) major: u16,
    pub(crate) volume_id: [u8; 16],
    pub(crate) cursor: super::records::StoredCursor,
    pub(crate) root: [u8; 16],
    pub(crate) parts: Vec<sstable::TableRef>,
}

#[derive(Clone, Debug)]
pub(crate) struct CheckpointPart {
    pub(crate) reference: sstable::TableRef,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub(crate) enum CheckpointRecord {
    Node(StoredNode),
    Directory {
        node: [u8; 16],
        generation: u64,
    },
    DirectoryEntry {
        directory: [u8; 16],
        name: String,
        entry: StoredDirectoryEntry,
    },
    FileVersion {
        id: [u8; 32],
        logical_size: u64,
        logical_digest: [u8; 32],
        extents: u64,
    },
    FileExtent {
        file_version: [u8; 32],
        ordinal: u64,
        extent: Extent,
    },
    Receipt(StoredCommittedResult),
}

pub(crate) struct PendingCheckpoint {
    major: u16,
    volume_id: [u8; 16],
    cursor: super::records::StoredCursor,
    root: [u8; 16],
    pub(crate) parts: Vec<CheckpointPart>,
}

impl PendingCheckpoint {
    pub(crate) fn from_checkpoint(checkpoint: &StoredCheckpoint) -> Result<Self, ManagedError> {
        let mut records = Vec::new();
        records.extend(
            checkpoint
                .snapshot
                .nodes
                .iter()
                .cloned()
                .map(CheckpointRecord::Node),
        );
        for directory in &checkpoint.snapshot.directories {
            records.push(CheckpointRecord::Directory {
                node: directory.node,
                generation: directory.generation,
            });
            records.extend(directory.entries.iter().map(|(name, entry)| {
                CheckpointRecord::DirectoryEntry {
                    directory: directory.node,
                    name: name.clone(),
                    entry: *entry,
                }
            }));
        }
        for version in &checkpoint.snapshot.file_versions {
            records.push(CheckpointRecord::FileVersion {
                id: version.id,
                logical_size: version.logical_size,
                logical_digest: version.logical_digest,
                extents: version.extent_map.extents.len() as u64,
            });
            records.extend(version.extent_map.extents.iter().enumerate().map(
                |(ordinal, extent)| CheckpointRecord::FileExtent {
                    file_version: version.id,
                    ordinal: ordinal as u64,
                    extent: *extent,
                },
            ));
        }
        records.extend(
            checkpoint
                .results
                .iter()
                .cloned()
                .map(CheckpointRecord::Receipt),
        );

        let mut batches = Vec::new();
        let mut current = Vec::<sstable::Record>::new();
        let mut current_bytes = 0_usize;
        for (ordinal, record) in records.into_iter().enumerate() {
            let mut value = Vec::new();
            ciborium::into_writer(&record, &mut value)
                .map_err(|_| invalid("checkpoint record cannot be encoded"))?;
            let record_bytes = value.len();
            if !current.is_empty() && current_bytes.saturating_add(record_bytes) > TARGET_PART_BYTES
            {
                batches.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current_bytes = current_bytes.saturating_add(record_bytes);
            current.push(sstable::Record {
                key: (ordinal as u64).to_be_bytes().to_vec(),
                value,
            });
        }
        if !current.is_empty() {
            batches.push(current);
        }
        if batches.is_empty() {
            return Err(invalid("branch checkpoint has no records"));
        }
        let mut parts = Vec::with_capacity(batches.len());
        for records in batches {
            let partition_key = records
                .first()
                .expect("checkpoint batch is not empty")
                .key
                .clone();
            let tables = sstable::build_set(
                checkpoint.volume_id,
                vec![sstable::RecordGroup {
                    partition_key,
                    records,
                }],
                "checkpoint Managed branch",
            )?;
            if tables.len() != 1 {
                return Err(invalid("checkpoint batch produced more than one SSTable"));
            }
            let table = tables.into_iter().next().expect("one table was produced");
            parts.push(CheckpointPart {
                reference: table.reference,
                bytes: table.bytes,
            });
        }
        Ok(Self {
            major: checkpoint.major,
            volume_id: checkpoint.volume_id,
            cursor: checkpoint.snapshot.cursor,
            root: checkpoint.snapshot.root,
            parts,
        })
    }

    pub(crate) fn finish(self) -> CheckpointRoot {
        CheckpointRoot {
            major: self.major,
            volume_id: self.volume_id,
            cursor: self.cursor,
            root: self.root,
            parts: self.parts.into_iter().map(|part| part.reference).collect(),
        }
    }
}

impl CheckpointRoot {
    pub(crate) fn recover(
        &self,
        parts: Vec<CheckpointPart>,
    ) -> Result<StoredCheckpoint, ManagedError> {
        if self.major != FORMAT_MAJOR || self.parts.is_empty() || self.parts.len() != parts.len() {
            return Err(corrupt("branch checkpoint root is invalid"));
        }

        let mut nodes = Vec::new();
        let mut directories = BTreeMap::<[u8; 16], StoredDirectory>::new();
        let mut file_versions = BTreeMap::<[u8; 32], PendingFileVersion>::new();
        let mut results = Vec::new();
        for (reference, part) in self.parts.iter().zip(parts) {
            if reference != &part.reference {
                return Err(corrupt("branch checkpoint part is invalid"));
            }
            for record in sstable::decode(
                reference,
                &part.bytes,
                self.volume_id,
                "read Managed branch",
            )? {
                let record: CheckpointRecord = ciborium::from_reader(record.value.as_slice())
                    .map_err(|_| corrupt("branch checkpoint record is invalid"))?;
                match record {
                    CheckpointRecord::Node(node) => nodes.push(node),
                    CheckpointRecord::Directory { node, generation } => {
                        if directories
                            .insert(
                                node,
                                StoredDirectory {
                                    node,
                                    generation,
                                    entries: BTreeMap::new(),
                                },
                            )
                            .is_some()
                        {
                            return Err(corrupt(
                                "branch checkpoint contains duplicate directories",
                            ));
                        }
                    }
                    CheckpointRecord::DirectoryEntry {
                        directory,
                        name,
                        entry,
                    } => {
                        let directory = directories.get_mut(&directory).ok_or_else(|| {
                            corrupt("branch checkpoint entry precedes its directory")
                        })?;
                        if directory.entries.insert(name, entry).is_some() {
                            return Err(corrupt(
                                "branch checkpoint contains duplicate directory entries",
                            ));
                        }
                    }
                    CheckpointRecord::FileVersion {
                        id,
                        logical_size,
                        logical_digest,
                        extents,
                    } => {
                        if file_versions
                            .insert(
                                id,
                                PendingFileVersion {
                                    logical_size,
                                    logical_digest,
                                    expected_extents: extents,
                                    extents: Vec::new(),
                                },
                            )
                            .is_some()
                        {
                            return Err(corrupt(
                                "branch checkpoint contains duplicate file versions",
                            ));
                        }
                    }
                    CheckpointRecord::FileExtent {
                        file_version,
                        ordinal,
                        extent,
                    } => {
                        let version = file_versions.get_mut(&file_version).ok_or_else(|| {
                            corrupt("branch checkpoint extent precedes its file version")
                        })?;
                        if ordinal != version.extents.len() as u64 {
                            return Err(corrupt("branch checkpoint extent order is invalid"));
                        }
                        version.extents.push(extent);
                    }
                    CheckpointRecord::Receipt(result) => results.push(result),
                }
            }
        }
        let file_versions = file_versions
            .into_iter()
            .map(|(id, version)| {
                if version.expected_extents != version.extents.len() as u64 {
                    return Err(corrupt("branch checkpoint extent count is invalid"));
                }
                Ok(StoredFileVersion {
                    id,
                    logical_size: version.logical_size,
                    logical_digest: version.logical_digest,
                    extent_map: ExtentMap {
                        extents: version.extents,
                    },
                })
            })
            .collect::<Result<Vec<_>, ManagedError>>()?;
        Ok(StoredCheckpoint {
            major: self.major,
            volume_id: self.volume_id,
            snapshot: StoredSnapshot {
                cursor: self.cursor,
                root: self.root,
                nodes,
                directories: directories.into_values().collect(),
                file_versions,
            },
            results,
        })
    }
}

struct PendingFileVersion {
    logical_size: u64,
    logical_digest: [u8; 32],
    expected_extents: u64,
    extents: Vec<Extent>,
}

fn invalid(message: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Invalid,
        "checkpoint Managed branch",
        message,
    )
}

fn corrupt(message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, "read Managed branch", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_missing_part() {
        let root = CheckpointRoot {
            major: FORMAT_MAJOR,
            volume_id: [1; 16],
            cursor: super::super::records::StoredCursor {
                sequence: 0,
                operation: None,
            },
            root: [2; 16],
            parts: vec![sstable::TableRef {
                id: [3; 32],
                encoded_bytes: 1,
                first_partition_key: vec![0],
                last_partition_key: vec![0],
                blocks: Vec::new(),
            }],
        };
        assert!(root.recover(Vec::new()).is_err());
    }
}
