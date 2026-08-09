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

//! Namespace checkpoints stored in one shared content-addressed part format.
//!
//! The storage providers own only the immutable-part I/O.  Record boundaries,
//! paging, reconstruction, and validation stay here so Object and D1 cannot
//! acquire subtly different checkpoint formats.

use std::collections::BTreeMap;
use std::io::Cursor;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::filesystem::{ChangeCursor, NodeId, VolumeId};
use crate::managed::metadata::namespace::{
    DirectoryRecord, FileVersionRecord, NamespaceSnapshot, NodeRecord,
};
use crate::managed::{ManagedError, ManagedErrorKind};

const FORMAT_MAJOR: u16 = 1;
const ROOT_MAGIC: &[u8; 8] = b"OFS1CKP1";
const PART_MAGIC: &[u8; 8] = b"OFS1CPP1";
const MAX_ROOT_BYTES: usize = 4 * 1024 * 1024;

/// Keeps D1 values comfortably below its row/request limits while producing
/// reasonably sized immutable Object writes. This is a target, not a limit on
/// the complete checkpoint or on a single natural record.
const TARGET_PART_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointRoot {
    pub(crate) major: u16,
    pub(crate) volume_id: VolumeId,
    pub(crate) cursor: ChangeCursor,
    pub(crate) root: NodeId,
    pub(crate) parts: Vec<CheckpointPartRef>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointPartRef {
    pub(crate) id: [u8; 32],
    pub(crate) encoded_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct CheckpointPart {
    pub(crate) reference: CheckpointPartRef,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "type")]
pub(crate) enum CheckpointRecord<R> {
    Node(NodeRecord),
    Directory(DirectoryRecord),
    FileVersion(FileVersionRecord),
    Receipt(R),
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct EncodedPart<'a, R> {
    major: u16,
    volume_id: VolumeId,
    records: &'a [CheckpointRecord<R>],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecodedPart<R> {
    major: u16,
    volume_id: VolumeId,
    records: Vec<CheckpointRecord<R>>,
}

pub(crate) struct PendingCheckpoint {
    major: u16,
    volume_id: VolumeId,
    cursor: ChangeCursor,
    root: NodeId,
    pub(crate) parts: Vec<CheckpointPart>,
}

impl PendingCheckpoint {
    pub(crate) fn new<R: Clone + Serialize>(
        snapshot: &NamespaceSnapshot,
        results: &[R],
    ) -> Result<Self, ManagedError> {
        let mut records = Vec::new();
        records.extend(snapshot.nodes.values().cloned().map(CheckpointRecord::Node));
        records.extend(
            snapshot
                .directories
                .values()
                .cloned()
                .map(CheckpointRecord::Directory),
        );
        records.extend(
            snapshot
                .file_versions
                .values()
                .cloned()
                .map(CheckpointRecord::FileVersion),
        );
        records.extend(results.iter().cloned().map(CheckpointRecord::Receipt));

        let mut batches = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0_usize;
        for record in records {
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
            current.push(record);
        }
        if !current.is_empty() {
            batches.push(current);
        }
        if batches.is_empty() {
            return Err(invalid("checkpoint has no records"));
        }
        let mut parts = Vec::with_capacity(batches.len());
        for records in batches {
            parts.push(CheckpointPart::encode(snapshot.volume_id, &records)?);
        }
        Ok(Self {
            major: FORMAT_MAJOR,
            volume_id: snapshot.volume_id,
            cursor: snapshot.cursor,
            root: snapshot.root,
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
    pub(crate) fn encode(&self) -> Result<Vec<u8>, ManagedError> {
        let mut body = Vec::new();
        ciborium::into_writer(self, &mut body)
            .map_err(|_| invalid("checkpoint root cannot be encoded"))?;
        if body.len() > MAX_ROOT_BYTES {
            return Err(invalid("checkpoint root exceeds its size limit"));
        }
        let mut bytes = Vec::with_capacity(ROOT_MAGIC.len() + body.len() + 32);
        bytes.extend_from_slice(ROOT_MAGIC);
        bytes.extend_from_slice(&body);
        let checksum: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend_from_slice(&checksum);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ManagedError> {
        let body = bytes
            .strip_prefix(ROOT_MAGIC)
            .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
            .ok_or_else(|| corrupt("checkpoint root format is invalid"))?;
        if body.len() > MAX_ROOT_BYTES {
            return Err(corrupt("checkpoint root exceeds its size limit"));
        }
        let expected = bytes
            .get(bytes.len().saturating_sub(32)..)
            .ok_or_else(|| corrupt("checkpoint root checksum is missing"))?;
        if Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != expected {
            return Err(corrupt("checkpoint root checksum does not match"));
        }
        let mut input = Cursor::new(body);
        let root =
            ciborium::from_reader(&mut input).map_err(|_| corrupt("checkpoint root is invalid"))?;
        if input.position() != body.len() as u64 {
            return Err(corrupt("checkpoint root has trailing bytes"));
        }
        Ok(root)
    }

    pub(crate) fn recover<R: DeserializeOwned>(
        &self,
        parts: Vec<CheckpointPart>,
    ) -> Result<(NamespaceSnapshot, Vec<R>), ManagedError> {
        if self.major != FORMAT_MAJOR || self.parts.is_empty() || self.parts.len() != parts.len() {
            return Err(corrupt("checkpoint root is invalid"));
        }

        let mut nodes = BTreeMap::new();
        let mut directories = BTreeMap::new();
        let mut file_versions = BTreeMap::new();
        let mut results = Vec::new();
        for (reference, part) in self.parts.iter().zip(parts) {
            if reference != &part.reference {
                return Err(corrupt("checkpoint part is invalid"));
            }
            for record in part.decode::<R>(self.volume_id)? {
                let repeated = match record {
                    CheckpointRecord::Node(node) => nodes.insert(node.id, node).is_some(),
                    CheckpointRecord::Directory(directory) => {
                        directories.insert(directory.node, directory).is_some()
                    }
                    CheckpointRecord::FileVersion(version) => {
                        file_versions.insert(version.id, version).is_some()
                    }
                    CheckpointRecord::Receipt(result) => {
                        results.push(result);
                        false
                    }
                };
                if repeated {
                    return Err(corrupt("checkpoint contains a duplicate record"));
                }
            }
        }
        Ok((
            NamespaceSnapshot {
                volume_id: self.volume_id,
                cursor: self.cursor,
                root: self.root,
                nodes,
                directories,
                file_versions,
            },
            results,
        ))
    }
}

impl CheckpointPart {
    fn encode<R: Serialize>(
        volume_id: VolumeId,
        records: &[CheckpointRecord<R>],
    ) -> Result<Self, ManagedError> {
        let mut bytes = Vec::from(PART_MAGIC);
        ciborium::into_writer(
            &EncodedPart {
                major: FORMAT_MAJOR,
                volume_id,
                records,
            },
            &mut bytes,
        )
        .map_err(|_| invalid("checkpoint part cannot be encoded"))?;
        let id: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend_from_slice(&id);
        Ok(Self {
            reference: CheckpointPartRef {
                id,
                encoded_bytes: bytes.len() as u64,
            },
            bytes,
        })
    }

    fn decode<R: DeserializeOwned>(
        &self,
        volume_id: VolumeId,
    ) -> Result<Vec<CheckpointRecord<R>>, ManagedError> {
        let body = self
            .bytes
            .strip_prefix(PART_MAGIC)
            .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
            .ok_or_else(|| corrupt("checkpoint part format is invalid"))?;
        if self.bytes.len() as u64 != self.reference.encoded_bytes
            || Sha256::digest(&self.bytes[..self.bytes.len() - 32]).as_slice() != self.reference.id
            || !self.bytes.ends_with(&self.reference.id)
        {
            return Err(corrupt("checkpoint part identity is invalid"));
        }
        let mut input = Cursor::new(body);
        let part: DecodedPart<R> =
            ciborium::from_reader(&mut input).map_err(|_| corrupt("checkpoint part is invalid"))?;
        if input.position() != body.len() as u64
            || part.major != FORMAT_MAJOR
            || part.volume_id != volume_id
            || part.records.is_empty()
        {
            return Err(corrupt("checkpoint part is invalid"));
        }
        Ok(part.records)
    }
}

fn invalid(message: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Invalid,
        "checkpoint Managed namespace",
        message,
    )
}

fn corrupt(message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, "read Managed namespace", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_missing_part() {
        let root = CheckpointRoot {
            major: FORMAT_MAJOR,
            volume_id: VolumeId::from_bytes([1; 16]),
            cursor: ChangeCursor::Genesis,
            root: NodeId::from_bytes([2; 16]),
            parts: vec![CheckpointPartRef {
                id: [3; 32],
                encoded_bytes: 1,
            }],
        };
        assert!(root.recover::<()>(Vec::new()).is_err());
    }
}
