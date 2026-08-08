// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

use crate::filesystem::{NodeId, NodeKind, VolumeSnapshot};

pub(crate) fn snapshot_paths(snapshot: &VolumeSnapshot) -> Result<BTreeMap<String, NodeId>> {
    let mut paths = BTreeMap::new();
    let mut pending = vec![(String::new(), snapshot.root)];
    let mut expanded = BTreeSet::new();
    while let Some((path, node)) = pending.pop() {
        if !path.is_empty() && paths.insert(path.clone(), node).is_some() {
            bail!("authoritative namespace contains a duplicate path");
        }
        let record = snapshot
            .nodes
            .get(&node)
            .context("authoritative namespace references a missing node")?;
        if record.kind != NodeKind::Directory {
            continue;
        }
        if !expanded.insert(node) {
            bail!("authoritative namespace is not a directory tree");
        }
        let directory = snapshot
            .directories
            .get(&node)
            .context("authoritative namespace references a missing directory")?;
        for (name, entry) in directory.entries.iter().rev() {
            let child = if path.is_empty() {
                name.clone()
            } else {
                format!("{path}/{name}")
            };
            pending.push((child, entry.node));
        }
    }
    Ok(paths)
}
