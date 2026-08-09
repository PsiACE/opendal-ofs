// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;

use anyhow::Result;

use crate::filesystem::{NodeId, VolumeSnapshot};

pub(crate) fn snapshot_paths(snapshot: &VolumeSnapshot) -> Result<BTreeMap<String, NodeId>> {
    snapshot.paths().map_err(Into::into)
}
