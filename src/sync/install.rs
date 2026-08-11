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

use std::collections::BTreeSet;
use std::fs::File;
use std::path::{Path, PathBuf};

use futures::{StreamExt as _, TryStreamExt as _};

use crate::filesystem::{NodeKind, VolumeSnapshot};
use crate::managed::ManagedVolume;

use super::SyncError;

pub(crate) async fn install(
    root: &Path,
    current: Option<&VolumeSnapshot>,
    target: &VolumeSnapshot,
    volume: &ManagedVolume,
    transfer_concurrency: usize,
) -> Result<(), SyncError> {
    apply(root, current, target, volume, transfer_concurrency, false).await
}

/// Repair an interrupted installation from the current authoritative snapshot.
pub(crate) async fn repair(
    root: &Path,
    target: &VolumeSnapshot,
    volume: &ManagedVolume,
    transfer_concurrency: usize,
) -> Result<(), SyncError> {
    apply(root, None, target, volume, transfer_concurrency, true).await
}

async fn apply(
    root: &Path,
    current: Option<&VolumeSnapshot>,
    target: &VolumeSnapshot,
    volume: &ManagedVolume,
    transfer_concurrency: usize,
    authoritative: bool,
) -> Result<(), SyncError> {
    let target_paths = target.paths()?;
    let current_paths = current.map(VolumeSnapshot::paths).transpose()?;
    let mut durability = Durability::default();

    let mut removed = if authoritative {
        actual_paths(root)?
            .into_iter()
            .filter(|path| {
                path.to_str()
                    .is_none_or(|path| !target_paths.contains_key(path))
            })
            .map(|path| root.join(path))
            .collect::<Vec<_>>()
    } else {
        current_paths
            .iter()
            .flat_map(|paths| paths.keys())
            .filter(|path| !target_paths.contains_key(*path))
            .map(|path| root.join(path))
            .collect::<Vec<_>>()
    };
    removed.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for destination in removed {
        remove_path(&destination)?;
        durability.changed_parent(&destination);
        test_interrupt()?;
    }

    let mut directories = target_paths
        .iter()
        .filter(|(_, node)| target.nodes[node].kind == NodeKind::Directory)
        .collect::<Vec<_>>();
    directories.sort_by_key(|(path, _)| path.matches('/').count());
    for (path, _) in directories {
        let destination = root.join(path);
        match path_metadata(&destination)? {
            Some(metadata) if metadata.is_dir() => {}
            Some(_) => {
                remove_path(&destination)?;
                durability.changed_parent(&destination);
                test_interrupt()?;
                create_directory(&destination, &mut durability)?;
                test_interrupt()?;
            }
            None => {
                create_directory(&destination, &mut durability)?;
                test_interrupt()?;
            }
        }
    }

    let mut files = Vec::new();
    for (path, node_id) in &target_paths {
        let node = &target.nodes[node_id];
        if node.kind != NodeKind::RegularFile {
            continue;
        }
        let destination = root.join(path);
        let version = node
            .file_version
            .and_then(|id| target.file_versions.get(&id))
            .ok_or_else(|| SyncError::new("remote file has no file version"))?;
        let unchanged = if authoritative {
            local_file_matches(volume, &destination, version, node.attributes.executable).await?
        } else {
            current_paths.as_ref().is_some_and(|paths| {
                paths.get(path).is_some_and(|current_id| {
                    let current_node =
                        &current.expect("current paths require a snapshot").nodes[current_id];
                    current_node.kind == NodeKind::RegularFile
                        && current_node.file_version == node.file_version
                        && current_node.attributes == node.attributes
                })
            })
        };
        if unchanged {
            continue;
        }
        if path_metadata(&destination)?.is_some_and(|metadata| metadata.is_dir()) {
            std::fs::remove_dir_all(&destination)
                .map_err(|error| SyncError::io("replace replica directory", error))?;
            durability.changed_parent(&destination);
            test_interrupt()?;
        }
        durability.changed_parent(&destination);
        files.push((destination, version.clone(), node.attributes.executable));
    }
    futures::stream::iter(files)
        .map(Ok::<_, SyncError>)
        .try_for_each_concurrent(
            transfer_concurrency,
            |(destination, version, executable)| async move {
                volume.materialize_file(&version, &destination).await?;
                if executable {
                    set_executable(&destination, true)?;
                    sync_file(&destination)?;
                }
                test_interrupt()?;
                Ok(())
            },
        )
        .await?;
    durability.sync()?;
    Ok(())
}

async fn local_file_matches(
    volume: &ManagedVolume,
    path: &Path,
    expected: &crate::filesystem::FileVersion,
    executable: bool,
) -> Result<bool, SyncError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(SyncError::io("inspect replica file", error)),
    };
    if !supported_regular_file(&metadata) || is_executable(&metadata) != executable {
        return Ok(false);
    }
    Ok(volume.inspect_file(path).await? == *expected)
}

fn actual_paths(root: &Path) -> Result<Vec<PathBuf>, SyncError> {
    let mut paths = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let children = std::fs::read_dir(&directory)
            .map_err(|error| SyncError::io("scan interrupted installation", error))?;
        for child in children {
            let child =
                child.map_err(|error| SyncError::io("scan interrupted installation", error))?;
            let path = child.path();
            let metadata = std::fs::symlink_metadata(&path)
                .map_err(|error| SyncError::io("inspect interrupted installation", error))?;
            if metadata.is_dir() {
                pending.push(path.clone());
            }
            paths.push(
                path.strip_prefix(root)
                    .expect("walked installation path is below its root")
                    .to_owned(),
            );
        }
    }
    Ok(paths)
}

fn create_directory(path: &Path, durability: &mut Durability) -> Result<(), SyncError> {
    let mut missing = Vec::new();
    let mut candidate = path;
    while !candidate.exists() {
        missing.push(candidate.to_owned());
        let Some(parent) = candidate.parent() else {
            break;
        };
        candidate = parent;
    }
    std::fs::create_dir_all(path)
        .map_err(|error| SyncError::io("create replica directory", error))?;
    for directory in missing {
        durability.directories.insert(directory.clone());
        durability.changed_parent(&directory);
    }
    Ok(())
}

#[derive(Default)]
struct Durability {
    directories: BTreeSet<PathBuf>,
}

impl Durability {
    fn changed_parent(&mut self, path: &Path) {
        if let Some(parent) = path.parent() {
            self.directories.insert(parent.to_owned());
        }
    }

    fn sync(self) -> Result<(), SyncError> {
        let mut directories = self.directories.into_iter().collect::<Vec<_>>();
        directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for directory in directories {
            if path_metadata(&directory)?.is_none_or(|metadata| !metadata.is_dir()) {
                continue;
            }
            File::open(directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| SyncError::io("persist replica directory", error))?;
        }
        Ok(())
    }
}

fn sync_file(path: &Path) -> Result<(), SyncError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| SyncError::io("persist replica file attributes", error))
}

#[cfg(debug_assertions)]
fn test_interrupt() -> Result<(), SyncError> {
    if std::env::var_os("OFS_INTERNAL_TEST_INTERRUPT").as_deref() == Some("during-install".as_ref())
    {
        return Err(SyncError::new(
            "internal test interrupted replica installation",
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
const fn test_interrupt() -> Result<(), SyncError> {
    Ok(())
}

fn remove_path(path: &Path) -> Result<(), SyncError> {
    let Some(metadata) = path_metadata(path)? else {
        return Ok(());
    };
    if metadata.is_dir() {
        std::fs::remove_dir_all(path)
            .map_err(|error| SyncError::io("remove replica directory", error))
    } else {
        std::fs::remove_file(path).map_err(|error| SyncError::io("remove replica file", error))
    }
}

fn path_metadata(path: &Path) -> Result<Option<std::fs::Metadata>, SyncError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(SyncError::io("inspect replica path", error)),
    }
}

#[cfg(unix)]
fn supported_regular_file(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.is_file() && metadata.nlink() == 1
}

#[cfg(not(unix))]
fn supported_regular_file(metadata: &std::fs::Metadata) -> bool {
    metadata.is_file()
}

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
const fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    false
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
