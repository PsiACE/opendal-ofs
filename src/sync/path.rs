// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Component-aware operations over canonical Sync paths.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};

use crate::filesystem::{
    DirectoryRecord, FileVersion, NodeId, NodeKind, NodeRecord, VolumeSnapshot,
};

/// One validated, path-sorted view over an immutable namespace snapshot.
pub(crate) struct SnapshotTree<'a> {
    snapshot: &'a VolumeSnapshot,
    paths: BTreeMap<String, NodeId>,
}

#[derive(Clone, Copy)]
pub(crate) struct SnapshotEntry<'a> {
    pub node: &'a NodeRecord,
    pub directory: Option<&'a DirectoryRecord>,
    pub file: Option<&'a FileVersion>,
}

impl<'a> SnapshotTree<'a> {
    pub(crate) fn new(snapshot: &'a VolumeSnapshot) -> Result<Self> {
        let paths = snapshot.paths()?;
        for (path, id) in &paths {
            let node = snapshot
                .nodes
                .get(id)
                .with_context(|| format!("snapshot path {path:?} references a missing node"))?;
            match node.kind {
                NodeKind::Directory if snapshot.directories.contains_key(id) => {}
                NodeKind::RegularFile
                    if node
                        .file_version
                        .is_some_and(|version| snapshot.file_versions.contains_key(&version)) => {}
                NodeKind::Directory => bail!("snapshot directory {path:?} has no record"),
                NodeKind::RegularFile => bail!("snapshot file {path:?} has no version"),
            }
        }
        Ok(Self { snapshot, paths })
    }

    pub(crate) fn snapshot(&self) -> &'a VolumeSnapshot {
        self.snapshot
    }

    pub(crate) fn paths(&self) -> &BTreeMap<String, NodeId> {
        &self.paths
    }

    pub(crate) fn get(&self, path: &str) -> Option<SnapshotEntry<'a>> {
        let id = self.paths.get(path)?;
        let node = &self.snapshot.nodes[id];
        Some(SnapshotEntry {
            node,
            directory: self.snapshot.directories.get(id),
            file: node
                .file_version
                .map(|version| &self.snapshot.file_versions[&version]),
        })
    }
}

/// Returns the entries below `directory`, excluding the directory itself.
///
/// Sync paths are canonical relative paths separated by `/`. Their descendants
/// therefore occupy one contiguous `BTreeMap` range between `directory/` and
/// `directory0`, because `0` immediately follows `/` in ASCII ordering.
pub(crate) fn descendants<'a, V>(
    paths: &'a BTreeMap<String, V>,
    directory: &str,
) -> impl DoubleEndedIterator<Item = (&'a String, &'a V)> {
    paths.range(format!("{directory}/")..format!("{directory}0"))
}

/// Returns `path` and every entry below it, in path order.
pub(crate) fn subtree<'a, V>(
    paths: &'a BTreeMap<String, V>,
    path: &str,
) -> impl DoubleEndedIterator<Item = (&'a String, &'a V)> {
    paths
        .get_key_value(path)
        .into_iter()
        .chain(descendants(paths, path))
}
