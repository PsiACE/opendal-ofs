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

//! Native local-filesystem operations used while installing a namespace.

use std::cmp::Reverse;
#[cfg(any(unix, windows))]
use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::FileFingerprint;
use crate::workset::{self, SpoolWriter, Workspace};

use super::transfer::inspect_file;

pub(super) async fn file_matches(
    path: &Path,
    expected: FileFingerprint,
    executable: bool,
) -> Result<bool, Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::from_io("inspect replica file", Some(path), error)),
    };
    if !supported_regular_file(&metadata) || is_executable(&metadata) != executable {
        return Ok(false);
    }
    if metadata.len() != expected.logical_length() {
        return Ok(false);
    }
    Ok(inspect_file(path).await? == expected)
}

pub(super) fn scan_paths(
    root: &Path,
    actual: &mut SpoolWriter<String>,
    removed: &mut SpoolWriter<StoredPath>,
) -> Result<(), Error> {
    let root_entries = std::fs::read_dir(root)
        .map_err(|error| Error::from_io("scan interrupted installation", Some(root), error))?;
    let mut pending = vec![root_entries];
    while let Some(children) = pending.last_mut() {
        let Some(child) = children.next() else {
            pending.pop();
            continue;
        };
        let child = child
            .map_err(|error| Error::from_io("scan interrupted installation", Some(root), error))?;
        let path = child.path();
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
            Error::from_io("inspect interrupted installation", Some(&path), error)
        })?;
        let relative = path
            .strip_prefix(root)
            .expect("walked installation path is below its root");
        match portable_replica_path(relative) {
            Some(path) => {
                actual.write(&path)?;
            }
            None => {
                removed.write(&StoredPath::from_path(relative)?)?;
            }
        }
        if metadata.is_dir() {
            pending.push(std::fs::read_dir(&path).map_err(|error| {
                Error::from_io("scan interrupted installation", Some(&path), error)
            })?);
        }
    }
    Ok(())
}

fn portable_replica_path(path: &Path) -> Option<String> {
    let path = path.to_str()?;
    #[cfg(windows)]
    let path = path.replace('\\', "/");
    Some(path.to_owned())
}

pub(super) fn create_directory(
    path: &Path,
    durability: &mut DirectoryDurability,
) -> Result<(), Error> {
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
        .map_err(|error| Error::from_io("create replica directory", Some(path), error))?;
    for directory in missing {
        durability.record(&directory)?;
        durability.changed_parent(&directory)?;
    }
    Ok(())
}

pub(super) struct DirectoryDurability {
    directories: SpoolWriter<StoredPath>,
}

impl DirectoryDurability {
    pub(super) fn create(workspace: &Workspace) -> Result<Self, Error> {
        Ok(Self {
            directories: workspace.writer("durability")?,
        })
    }

    fn record(&mut self, path: &Path) -> Result<(), Error> {
        self.directories.write(&StoredPath::from_path(path)?)?;
        Ok(())
    }

    pub(super) fn changed_parent(&mut self, path: &Path) -> Result<(), Error> {
        if let Some(parent) = path.parent() {
            self.record(parent)?;
        }
        Ok(())
    }

    pub(super) fn sync(self, workspace: &Workspace) -> Result<(), Error> {
        let directories = workset::sort(workspace, &self.directories.finish()?, |path| {
            Reverse(path.clone())
        })?;
        let mut directories = directories.reader()?;
        let mut previous = None;
        while let Some(directory) = directories.next()? {
            if previous.as_ref() == Some(&directory) {
                continue;
            }
            previous = Some(directory.clone());
            let directory = directory.to_path_buf();
            if path_metadata(&directory)?.is_none_or(|metadata| !metadata.is_dir()) {
                continue;
            }
            File::open(&directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    Error::from_io("persist replica directory", Some(&directory), error)
                })?;
        }
        Ok(())
    }
}

pub(super) fn remove_path(path: &Path) -> Result<(), Error> {
    let Some(metadata) = path_metadata(path)? else {
        return Ok(());
    };
    if metadata.is_dir() {
        std::fs::remove_dir_all(path)
            .map_err(|error| Error::from_io("remove replica directory", Some(path), error))
    } else {
        std::fs::remove_file(path)
            .map_err(|error| Error::from_io("remove replica file", Some(path), error))
    }
}

pub(super) fn remove_replaced_directory(path: &Path) -> Result<(), Error> {
    std::fs::remove_dir_all(path)
        .map_err(|error| Error::from_io("replace replica directory", Some(path), error))
}

pub(super) fn path_metadata(path: &Path) -> Result<Option<std::fs::Metadata>, Error> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::from_io("inspect replica path", Some(path), error)),
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
pub(super) fn make_executable(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = std::fs::metadata(path)
        .map_err(|error| Error::from_io("read replica permissions", Some(path), error))?
        .permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    std::fs::set_permissions(path, permissions)
        .map_err(|error| Error::from_io("write replica permissions", Some(path), error))?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::from_io("persist replica file attributes", Some(path), error))
}

#[cfg(not(unix))]
pub(super) fn make_executable(_path: &Path) -> Result<(), Error> {
    Err(Error::unsupported(
        "install replica",
        "Managed Sync executable attributes are not implemented on this platform",
    ))
}

#[cfg(unix)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct StoredPath(Vec<u8>);

#[cfg(unix)]
impl StoredPath {
    pub(super) fn from_path(path: &Path) -> Result<Self, Error> {
        use std::os::unix::ffi::OsStrExt as _;

        Ok(Self(path.as_os_str().as_bytes().to_vec()))
    }

    pub(super) fn to_path_buf(&self) -> PathBuf {
        use std::os::unix::ffi::OsStringExt as _;

        PathBuf::from(OsString::from_vec(self.0.clone()))
    }
}

#[cfg(windows)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct StoredPath(Vec<u16>);

#[cfg(windows)]
impl StoredPath {
    pub(super) fn from_path(path: &Path) -> Result<Self, Error> {
        use std::os::windows::ffi::OsStrExt as _;

        Ok(Self(
            path.as_os_str()
                .encode_wide()
                .map(|unit| {
                    if unit == b'\\' as u16 {
                        b'/' as u16
                    } else {
                        unit
                    }
                })
                .collect(),
        ))
    }

    pub(super) fn to_path_buf(&self) -> PathBuf {
        use std::os::windows::ffi::OsStringExt as _;

        PathBuf::from(OsString::from_wide(&self.0))
    }
}

#[cfg(not(any(unix, windows)))]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct StoredPath(String);

#[cfg(not(any(unix, windows)))]
impl StoredPath {
    pub(super) fn from_path(path: &Path) -> Result<Self, Error> {
        path.to_str()
            .map(|path| Self(path.to_owned()))
            .ok_or_else(|| {
                Error::unsupported("record replica path", "platform path is not Unicode")
            })
    }

    pub(super) fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.0)
    }
}
