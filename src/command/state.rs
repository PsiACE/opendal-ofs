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

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use ofs::filesystem::{ChangeCursor, OperationId, VolumeId};
use serde::{Deserialize, Serialize};

const FORMAT: &str = "managed-sync/1";

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplicaState {
    format: String,
    pub(super) root: PathBuf,
    pub(super) volume_id: VolumeId,
    pub(super) cursor: ChangeCursor,
}

impl ReplicaState {
    pub(super) fn new(root: PathBuf, volume_id: VolumeId) -> Self {
        Self {
            format: FORMAT.to_owned(),
            root,
            volume_id,
            cursor: ChangeCursor::Genesis,
        }
    }

    pub(super) fn load(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)
            .with_context(|| format!("cannot read replica state: {}", path.display()))?;
        let state: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("replica state is invalid: {}", path.display()))?;
        if state.format != FORMAT {
            bail!("replica state format is unsupported: {}", path.display());
        }
        Ok(state)
    }

    pub(super) fn save_new(&self, path: &Path) -> Result<()> {
        if path.exists() {
            bail!("--init requires a new replica state: {}", path.display());
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        if let Some(parent) = parent {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "cannot create replica state directory: {}",
                    parent.display()
                )
            })?;
        }
        let temporary = temporary_path(path, OperationId::generate());
        let bytes = serde_json::to_vec(self).context("cannot encode replica state")?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| format!("cannot create replica state: {}", temporary.display()))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("cannot persist replica state: {}", temporary.display()))?;
        fs::rename(&temporary, path)
            .with_context(|| format!("cannot install replica state: {}", path.display()))?;
        sync_parent(path)?;
        Ok(())
    }
}

fn temporary_path(path: &Path, operation: OperationId) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state");
    path.with_file_name(format!(".{name}.{operation}.tmp"))
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    File::open(parent.unwrap_or_else(|| Path::new(".")))
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("cannot persist replica state: {}", path.display()))
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<()> {
    Ok(())
}
