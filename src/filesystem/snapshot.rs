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
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use super::{
    ChangeCursor, DirectoryEntry, FileVersionId, Generation, NodeAttributes, NodeId, NodeKind,
    VolumeError, VolumeErrorKind, VolumeId,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileVersion {
    pub id: FileVersionId,
}

impl FileVersion {
    pub const fn new(id: FileVersionId) -> Self {
        Self { id }
    }

    pub const fn logical_length(&self) -> u64 {
        self.id.logical_length()
    }

    pub const fn digest(&self) -> super::Digest {
        self.id.digest()
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

/// A complete, backend-neutral observation of a Managed namespace.
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
    /// Return every non-root path and its stable node identity.
    pub fn paths(&self) -> Result<BTreeMap<String, NodeId>, VolumeError> {
        self.validate()?;
        let mut paths = BTreeMap::new();
        let mut pending = vec![(String::new(), self.root)];
        while let Some((path, node_id)) = pending.pop() {
            if self.nodes[&node_id].kind == NodeKind::Directory {
                for (name, entry) in self.directories[&node_id].entries.iter().rev() {
                    let child = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{path}/{name}")
                    };
                    pending.push((child, entry.node));
                }
            }
            if !path.is_empty() {
                paths.insert(path, node_id);
            }
        }
        Ok(paths)
    }

    pub fn validate(&self) -> Result<(), VolumeError> {
        let root = self
            .nodes
            .get(&self.root)
            .ok_or_else(|| invalid("root node is missing"))?;
        if root.kind != NodeKind::Directory || !self.directories.contains_key(&self.root) {
            return Err(invalid("root node is not a directory"));
        }

        for (id, node) in &self.nodes {
            if *id != node.id {
                return Err(invalid("node key does not match its identity"));
            }
            match node.kind {
                NodeKind::Directory
                    if node.file_version.is_none() && self.directories.contains_key(id) => {}
                NodeKind::RegularFile
                    if node
                        .file_version
                        .is_some_and(|version| self.file_versions.contains_key(&version))
                        && !self.directories.contains_key(id) => {}
                _ => return Err(invalid("node has invalid backing records")),
            }
        }

        let mut paths = Vec::new();
        let mut reachable = BTreeSet::new();
        let mut expanded = BTreeSet::new();
        let mut pending = vec![(String::new(), self.root)];
        while let Some((path, node_id)) = pending.pop() {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| invalid("directory entry references a missing node"))?;
            reachable.insert(node_id);
            if node.kind == NodeKind::Directory {
                if !expanded.insert(node_id) {
                    return Err(invalid("namespace directories do not form a tree"));
                }
                let directory = self
                    .directories
                    .get(&node_id)
                    .ok_or_else(|| invalid("directory has no backing record"))?;
                if directory.node != node_id {
                    return Err(invalid("directory key does not match its identity"));
                }
                for (name, entry) in directory.entries.iter().rev() {
                    let child = self
                        .nodes
                        .get(&entry.node)
                        .ok_or_else(|| invalid("directory entry references a missing node"))?;
                    if entry.kind != child.kind {
                        return Err(invalid("directory entry kind disagrees with its node"));
                    }
                    let child_path = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{path}/{name}")
                    };
                    pending.push((child_path, entry.node));
                }
            }
            if !path.is_empty() {
                paths.push(path);
            }
        }

        if reachable.len() != self.nodes.len() {
            return Err(invalid("namespace contains unreachable nodes"));
        }
        if self
            .file_versions
            .iter()
            .any(|(id, version)| *id != version.id)
        {
            return Err(invalid("file-version key does not match its identity"));
        }
        validate_portable_paths(paths.iter().map(String::as_str))
    }
}

const MAX_COMPONENT_BYTES: usize = 255;
const MAX_PATH_BYTES: usize = 4096;

fn validate_portable_paths<'a>(
    paths: impl IntoIterator<Item = &'a str>,
) -> Result<(), VolumeError> {
    let mut folded = BTreeSet::new();
    for path in paths {
        if path.is_empty()
            || path.len() > MAX_PATH_BYTES
            || path.starts_with('/')
            || path.ends_with('/')
            || path.contains("//")
        {
            return Err(invalid("path is not portable"));
        }
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        if name.len() > MAX_COMPONENT_BYTES
            || name == "."
            || name == ".."
            || name.ends_with([' ', '.'])
            || name.chars().any(|character| {
                character.is_control()
                    || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
            })
            || !name.nfc().eq(name.chars())
        {
            return Err(invalid("path component is not portable"));
        }
        let folded_name = name.case_fold().nfc().collect::<String>();
        let stem = folded_name.split('.').next().unwrap_or_default();
        if matches!(stem, "con" | "prn" | "aux" | "nul")
            || stem.len() == 4
                && (stem.starts_with("com") || stem.starts_with("lpt"))
                && matches!(stem.as_bytes()[3], b'1'..=b'9')
            || matches!(stem, "com¹" | "com²" | "com³" | "lpt¹" | "lpt²" | "lpt³")
        {
            return Err(invalid("path component is reserved"));
        }
        if !folded.insert((parent, folded_name)) {
            return Err(invalid("directory contains a case-folding collision"));
        }
    }
    Ok(())
}

fn invalid(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Invalid, message)
}
