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

use std::path::Path;

use crate::filesystem::{NodeKind, VolumeSnapshot};
use crate::managed::ManagedVolume;

use super::SyncError;

pub(crate) async fn install(
    root: &Path,
    current: Option<&VolumeSnapshot>,
    target: &VolumeSnapshot,
    volume: &ManagedVolume,
) -> Result<(), SyncError> {
    let target_paths = target.paths()?;
    let current_paths = current.map(VolumeSnapshot::paths).transpose()?;

    if let Some(current_paths) = &current_paths {
        let mut removed = current_paths
            .keys()
            .filter(|path| !target_paths.contains_key(*path))
            .collect::<Vec<_>>();
        removed.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
        for path in removed {
            remove_path(&root.join(path))?;
        }
    }

    let mut directories = target_paths
        .iter()
        .filter(|(_, node)| target.nodes[node].kind == NodeKind::Directory)
        .collect::<Vec<_>>();
    directories.sort_by_key(|(path, _)| path.matches('/').count());
    for (path, _) in directories {
        let destination = root.join(path);
        if destination.exists() && !destination.is_dir() {
            remove_path(&destination)?;
        }
        std::fs::create_dir_all(&destination)
            .map_err(|error| SyncError::io("create replica directory", error))?;
    }

    for (path, node_id) in &target_paths {
        let node = &target.nodes[node_id];
        if node.kind != NodeKind::RegularFile {
            continue;
        }
        let unchanged = current_paths.as_ref().is_some_and(|paths| {
            paths.get(path).is_some_and(|current_id| {
                let current_node =
                    &current.expect("current paths require a snapshot").nodes[current_id];
                current_node.kind == NodeKind::RegularFile
                    && current_node.file_version == node.file_version
                    && current_node.attributes == node.attributes
            })
        });
        if unchanged {
            continue;
        }
        let destination = root.join(path);
        if destination.exists() && destination.is_dir() {
            std::fs::remove_dir_all(&destination)
                .map_err(|error| SyncError::io("replace replica directory", error))?;
        }
        let version = node
            .file_version
            .and_then(|id| target.file_versions.get(&id))
            .ok_or_else(|| SyncError::new("remote file has no file version"))?;
        volume.materialize_file(version, &destination).await?;
        set_executable(&destination, node.attributes.executable)?;
    }
    Ok(())
}

fn remove_path(path: &Path) -> Result<(), SyncError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(SyncError::io("inspect replica path", error)),
    };
    if metadata.is_dir() {
        std::fs::remove_dir_all(path)
            .map_err(|error| SyncError::io("remove replica directory", error))
    } else {
        std::fs::remove_file(path).map_err(|error| SyncError::io("remove replica file", error))
    }
}

#[cfg(unix)]
fn set_executable(path: &Path, executable: bool) -> Result<(), SyncError> {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = std::fs::metadata(path)
        .map_err(|error| SyncError::io("read replica permissions", error))?
        .permissions();
    let mode = permissions.mode();
    permissions.set_mode(if executable {
        mode | 0o111
    } else {
        mode & !0o111
    });
    std::fs::set_permissions(path, permissions)
        .map_err(|error| SyncError::io("write replica permissions", error))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path, _executable: bool) -> Result<(), SyncError> {
    Err(SyncError::new(
        "Managed Sync executable attributes are not implemented on this platform",
    ))
}
