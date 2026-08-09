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

use anyhow::Result;

use crate::filesystem::{NodeId, VolumeSnapshot};

/// Builds the one path index reused throughout a Sync pass.
pub(crate) fn snapshot_paths(snapshot: &VolumeSnapshot) -> Result<BTreeMap<String, NodeId>> {
    snapshot.paths().map_err(Into::into)
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
