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

//! Backend-neutral filesystem snapshots and their structural validation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use super::{
    ChangeCursor, DirectoryEntry, FileVersionId, Generation, NodeAttributes, NodeId, NodeKind,
    VolumeError, VolumeErrorKind, VolumeId,
};

/// An immutable file version whose durable descriptor is owned by its volume.
///
/// Access models may persist and return `descriptor`, but must not interpret it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileVersion {
    pub id: FileVersionId,
    pub logical_size: u64,
    pub logical_digest: [u8; 32],
    descriptor: Box<[u8]>,
}

impl FileVersion {
    pub fn from_parts(
        id: FileVersionId,
        logical_size: u64,
        logical_digest: [u8; 32],
        descriptor: impl Into<Box<[u8]>>,
    ) -> Self {
        Self {
            id,
            logical_size,
            logical_digest,
            descriptor: descriptor.into(),
        }
    }

    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeRecord {
    pub id: NodeId,
    pub generation: Generation,
    pub kind: NodeKind,
    pub attributes: NodeAttributes,
    pub file_version: Option<FileVersionId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryRecord {
    pub node: NodeId,
    pub generation: Generation,
    pub entries: BTreeMap<String, DirectoryEntry>,
}

/// A backend-neutral, complete filesystem observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeSnapshot {
    pub volume_id: VolumeId,
    pub cursor: ChangeCursor,
    pub root: NodeId,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    pub directories: BTreeMap<NodeId, DirectoryRecord>,
    pub file_versions: BTreeMap<FileVersionId, FileVersion>,
}

impl VolumeSnapshot {
    /// Return every non-root path in this namespace.
    ///
    /// Walking also proves that directories form a tree. Regular files may be
    /// linked from more than one directory.
    fn paths(&self) -> Result<BTreeMap<String, NodeId>, VolumeError> {
        let mut paths = BTreeMap::new();
        let mut pending = vec![(String::new(), self.root)];
        let mut expanded = BTreeSet::new();
        while let Some((path, node)) = pending.pop() {
            let record = &self.nodes[&node];
            if record.kind == NodeKind::Directory {
                if !expanded.insert(node) {
                    return Err(invalid_snapshot("namespace directories do not form a tree"));
                }
                for (name, entry) in self.directories[&node].entries.iter().rev() {
                    let child = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{path}/{name}")
                    };
                    pending.push((child, entry.node));
                }
            }
            if !path.is_empty() {
                paths.insert(path, node);
            }
        }
        Ok(paths)
    }

    /// Validate the backend-neutral structure shared by all volume formats.
    pub(crate) fn validated_paths(&self) -> Result<BTreeMap<String, NodeId>, VolumeError> {
        let root = self
            .nodes
            .get(&self.root)
            .ok_or_else(|| invalid_snapshot("root node is missing"))?;
        if root.kind != NodeKind::Directory || !self.directories.contains_key(&self.root) {
            return Err(invalid_snapshot("root node is not a directory"));
        }

        for (id, node) in &self.nodes {
            if *id != node.id {
                return Err(invalid_snapshot(
                    "node map key does not match its record identity",
                ));
            }
            match node.kind {
                NodeKind::Directory => {
                    if node.file_version.is_some() || !self.directories.contains_key(id) {
                        return Err(invalid_snapshot(
                            "directory node has invalid backing records",
                        ));
                    }
                }
                NodeKind::RegularFile => {
                    let version = node
                        .file_version
                        .ok_or_else(|| invalid_snapshot("file node has no file version"))?;
                    if !self.file_versions.contains_key(&version)
                        || self.directories.contains_key(id)
                    {
                        return Err(invalid_snapshot("file node has invalid backing records"));
                    }
                }
            }
        }

        for (id, directory) in &self.directories {
            if *id != directory.node {
                return Err(invalid_snapshot(
                    "directory map key does not match its record identity",
                ));
            }
            if !self
                .nodes
                .get(id)
                .is_some_and(|node| node.kind == NodeKind::Directory)
            {
                return Err(invalid_snapshot("directory has no directory node"));
            }
            for (name, entry) in &directory.entries {
                if name.is_empty() || name == "." || name == ".." || name.contains('/') {
                    return Err(invalid_snapshot("directory entry name is invalid"));
                }
                let child = self
                    .nodes
                    .get(&entry.node)
                    .ok_or_else(|| invalid_snapshot("directory entry references a missing node"))?;
                if child.kind != entry.kind {
                    return Err(invalid_snapshot(
                        "directory entry kind disagrees with its node",
                    ));
                }
            }
        }

        if self
            .file_versions
            .iter()
            .any(|(id, version)| *id != version.id)
        {
            return Err(invalid_snapshot(
                "file-version map key does not match its record identity",
            ));
        }

        let paths = self.paths()?;
        validate_portable_paths(paths.keys().map(String::as_str))?;
        let reachable = paths
            .values()
            .copied()
            .chain(std::iter::once(self.root))
            .collect::<BTreeSet<_>>();
        if reachable.len() != self.nodes.len() {
            return Err(invalid_snapshot("namespace contains unreachable nodes"));
        }
        Ok(paths)
    }

    pub(crate) fn validate_structure(&self) -> Result<(), VolumeError> {
        self.validated_paths().map(drop)
    }
}

const MAX_PORTABLE_COMPONENT_BYTES: usize = 255;
const MAX_PORTABLE_PATH_BYTES: usize = 4096;

pub(crate) fn validate_portable_paths<'a>(
    paths: impl IntoIterator<Item = &'a str>,
) -> Result<(), VolumeError> {
    let mut folded = BTreeSet::new();
    for path in paths {
        if path.is_empty()
            || path.len() > MAX_PORTABLE_PATH_BYTES
            || path.starts_with('/')
            || path.ends_with('/')
            || path.contains("//")
        {
            return Err(invalid_snapshot("path is not portable"));
        }
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        if name.len() > MAX_PORTABLE_COMPONENT_BYTES
            || name == "."
            || name == ".."
            || name.ends_with([' ', '.'])
            || name.chars().any(|character| {
                character.is_control()
                    || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
            })
            || !name.nfc().eq(name.chars())
        {
            return Err(invalid_snapshot("path component is not portable"));
        }
        let folded_name = name.case_fold().nfc().collect::<String>();
        let stem = folded_name.split('.').next().unwrap_or_default();
        if matches!(stem, "con" | "prn" | "aux" | "nul")
            || stem.len() == 4
                && (stem.starts_with("com") || stem.starts_with("lpt"))
                && matches!(stem.as_bytes()[3], b'1'..=b'9')
            || matches!(stem, "com¹" | "com²" | "com³" | "lpt¹" | "lpt²" | "lpt³")
        {
            return Err(invalid_snapshot("path component is reserved"));
        }
        if !folded.insert((parent, folded_name)) {
            return Err(invalid_snapshot(
                "directory contains a case-folding collision",
            ));
        }
    }
    Ok(())
}

fn invalid_snapshot(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Invalid, message)
}
