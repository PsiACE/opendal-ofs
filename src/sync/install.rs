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

use std::cmp::Reverse;
#[cfg(any(unix, windows))]
use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};

use futures::TryStreamExt as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::{FileFingerprint, NamespaceValue, NodeKind};
use crate::managed::{ManagedVolume, StreamRef};
use crate::workset::{self, Namespace, Spool, SpoolWriter, Workspace};

use super::transfer::{inspect_file, materialize_file};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FileInstallation {
    destination: StoredPath,
    fingerprint: FileFingerprint,
    content: StreamRef,
    executable: bool,
}

pub(crate) async fn install<C: DeserializeOwned>(
    root: &Path,
    current: Option<&Namespace<C>>,
    target: &Namespace<StreamRef>,
    volume: &ManagedVolume,
    transfer_concurrency: usize,
) -> Result<(), Error> {
    apply(root, current, target, volume, transfer_concurrency, false).await
}

/// Repair an interrupted installation from the current authoritative snapshot.
pub(crate) async fn repair(
    root: &Path,
    target: &Namespace<StreamRef>,
    volume: &ManagedVolume,
    transfer_concurrency: usize,
) -> Result<(), Error> {
    apply::<StreamRef>(root, None, target, volume, transfer_concurrency, true).await
}

async fn apply<C: DeserializeOwned>(
    root: &Path,
    current: Option<&Namespace<C>>,
    target: &Namespace<StreamRef>,
    volume: &ManagedVolume,
    transfer_concurrency: usize,
    authoritative: bool,
) -> Result<(), Error> {
    let workspace = Workspace::create()?;
    let removals = if authoritative {
        repair_removals(root, target, &workspace)?
    } else {
        namespace_removals(current, target, &workspace)?
    };
    let mut durability = Durability::create(&workspace)?;
    let removals = workset::sort(&workspace, &removals, |path| Reverse(path.clone()))?;
    let mut removals = removals.reader()?;
    while let Some(path) = removals.next()? {
        let destination = root.join(path.to_path_buf());
        remove_path(&destination)?;
        durability.changed_parent(&destination)?;
        test_interrupt()?;
    }

    let mut directories = target.reader()?;
    while let Some(record) = directories.next()? {
        let Some(node) = record.value else {
            continue;
        };
        if record.path.is_empty() || node.kind() != NodeKind::Directory {
            continue;
        }
        let destination = root.join(&record.path);
        match path_metadata(&destination)? {
            Some(metadata) if metadata.is_dir() => {}
            Some(_) => {
                remove_path(&destination)?;
                durability.changed_parent(&destination)?;
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

    let transfer_concurrency = transfer_concurrency.max(1);
    let mut file_installations = workspace.writer("file-installations")?;
    let mut current_reader = current.map(Namespace::reader).transpose()?;
    let mut current_record = current_reader
        .as_mut()
        .map(|reader| reader.next())
        .transpose()?
        .flatten();
    let mut target_reader = target.reader()?;
    while let Some(record) = target_reader.next()? {
        while current_record
            .as_ref()
            .is_some_and(|current| current.path < record.path)
        {
            current_record = current_reader
                .as_mut()
                .expect("current record requires a reader")
                .next()?;
        }
        let matching_current = current_record
            .as_ref()
            .filter(|current| current.path == record.path);
        let Some(node) = record.value else {
            continue;
        };
        let NamespaceValue::RegularFile {
            version,
            fingerprint,
            content,
        } = node.value
        else {
            continue;
        };
        let destination = root.join(&record.path);
        let unchanged = if authoritative {
            local_file_matches(&destination, fingerprint, node.attributes.executable).await?
        } else {
            matching_current.is_some_and(|current| {
                current.value.as_ref().is_some_and(|current| {
                    current.attributes == node.attributes
                        && current
                            .file()
                            .is_some_and(|(current_version, _, _)| current_version == version)
                })
            })
        };
        if unchanged {
            continue;
        }
        if path_metadata(&destination)?.is_some_and(|metadata| metadata.is_dir()) {
            std::fs::remove_dir_all(&destination).map_err(|error| {
                Error::from_io("replace replica directory", Some(&destination), error)
            })?;
            durability.changed_parent(&destination)?;
            test_interrupt()?;
        }
        durability.changed_parent(&destination)?;
        file_installations.write(&FileInstallation {
            destination: StoredPath::from_path(&destination)?,
            fingerprint,
            content,
            executable: node.attributes.executable,
        })?;
    }
    file_installations
        .finish()?
        .stream()?
        .try_for_each_concurrent(transfer_concurrency, |installation| async move {
            let destination = installation.destination.to_path_buf();
            materialize_file(
                volume,
                (installation.fingerprint, installation.content),
                &destination,
            )
            .await?;
            if installation.executable {
                set_executable(&destination, true)?;
                sync_file(&destination)?;
            }
            test_interrupt()?;
            Ok(())
        })
        .await?;
    durability.sync(&workspace)?;
    Ok(())
}

async fn local_file_matches(
    path: &Path,
    expected: crate::filesystem::FileFingerprint,
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
    Ok(inspect_file(path).await? == expected)
}

fn namespace_removals<C: DeserializeOwned>(
    current: Option<&Namespace<C>>,
    target: &Namespace<StreamRef>,
    workspace: &Workspace,
) -> Result<Spool<StoredPath>, Error> {
    let mut removed = workspace.writer("removed")?;
    let Some(current) = current else {
        return removed.finish();
    };
    let mut current = current.reader()?;
    let mut target = target.reader()?;
    let mut left = current.next()?;
    let mut right = target.next()?;
    while let Some(record) = left.as_ref() {
        match right.as_ref().map(|target| record.path.cmp(&target.path)) {
            None | Some(std::cmp::Ordering::Less) => {
                if !record.path.is_empty() {
                    removed.write(&StoredPath::from_path(Path::new(&record.path))?)?;
                }
                left = current.next()?;
            }
            Some(std::cmp::Ordering::Equal) => {
                left = current.next()?;
                right = target.next()?;
            }
            Some(std::cmp::Ordering::Greater) => {
                right = target.next()?;
            }
        }
    }
    removed.finish()
}

fn repair_removals(
    root: &Path,
    target: &Namespace<StreamRef>,
    workspace: &Workspace,
) -> Result<Spool<StoredPath>, Error> {
    let mut actual = workspace.writer("actual-paths")?;
    let mut removed = workspace.writer("removed")?;
    scan_actual_paths(root, &mut actual, &mut removed)?;
    let actual = workset::sort(workspace, &actual.finish()?, String::clone)?;
    let mut actual = actual.reader()?;
    let mut target = target.reader()?;
    let mut left = actual.next()?;
    let mut right = target.next()?;
    while let Some(path) = left.as_ref() {
        match right.as_ref().map(|target| path.cmp(&target.path)) {
            None | Some(std::cmp::Ordering::Less) => {
                removed.write(&StoredPath::from_path(Path::new(path))?)?;
                left = actual.next()?;
            }
            Some(std::cmp::Ordering::Equal) => {
                left = actual.next()?;
                right = target.next()?;
            }
            Some(std::cmp::Ordering::Greater) => {
                right = target.next()?;
            }
        }
    }
    removed.finish()
}

fn scan_actual_paths(
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

fn create_directory(path: &Path, durability: &mut Durability) -> Result<(), Error> {
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

struct Durability {
    directories: SpoolWriter<StoredPath>,
}

impl Durability {
    fn create(workspace: &Workspace) -> Result<Self, Error> {
        Ok(Self {
            directories: workspace.writer("durability")?,
        })
    }

    fn record(&mut self, path: &Path) -> Result<(), Error> {
        self.directories.write(&StoredPath::from_path(path)?)?;
        Ok(())
    }

    fn changed_parent(&mut self, path: &Path) -> Result<(), Error> {
        if let Some(parent) = path.parent() {
            self.record(parent)?;
        }
        Ok(())
    }

    fn sync(self, workspace: &Workspace) -> Result<(), Error> {
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

#[cfg(unix)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct StoredPath(Vec<u8>);

#[cfg(unix)]
impl StoredPath {
    fn from_path(path: &Path) -> Result<Self, Error> {
        use std::os::unix::ffi::OsStrExt as _;

        Ok(Self(path.as_os_str().as_bytes().to_vec()))
    }

    fn to_path_buf(&self) -> PathBuf {
        use std::os::unix::ffi::OsStringExt as _;

        PathBuf::from(OsString::from_vec(self.0.clone()))
    }
}

#[cfg(windows)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct StoredPath(Vec<u16>);

#[cfg(windows)]
impl StoredPath {
    fn from_path(path: &Path) -> Result<Self, Error> {
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

    fn to_path_buf(&self) -> PathBuf {
        use std::os::windows::ffi::OsStringExt as _;

        PathBuf::from(OsString::from_wide(&self.0))
    }
}

#[cfg(not(any(unix, windows)))]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct StoredPath(String);

#[cfg(not(any(unix, windows)))]
impl StoredPath {
    fn from_path(path: &Path) -> Result<Self, Error> {
        path.to_str()
            .map(|path| Self(path.to_owned()))
            .ok_or_else(|| {
                Error::unsupported("record replica path", "platform path is not Unicode")
            })
    }

    fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.0)
    }
}

fn sync_file(path: &Path) -> Result<(), Error> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::from_io("persist replica file attributes", Some(path), error))
}

#[cfg(debug_assertions)]
fn test_interrupt() -> Result<(), Error> {
    if std::env::var_os("OFS_INTERNAL_TEST_INTERRUPT").as_deref() == Some("during-install".as_ref())
    {
        return Err(Error::invalid(
            "synchronize replica",
            "internal test interrupted replica installation",
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
const fn test_interrupt() -> Result<(), Error> {
    Ok(())
}

fn remove_path(path: &Path) -> Result<(), Error> {
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

fn path_metadata(path: &Path) -> Result<Option<std::fs::Metadata>, Error> {
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
fn set_executable(path: &Path, executable: bool) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = std::fs::metadata(path)
        .map_err(|error| Error::from_io("read replica permissions", Some(path), error))?
        .permissions();
    let mode = permissions.mode();
    permissions.set_mode(if executable {
        mode | 0o111
    } else {
        mode & !0o111
    });
    std::fs::set_permissions(path, permissions)
        .map_err(|error| Error::from_io("write replica permissions", Some(path), error))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path, _executable: bool) -> Result<(), Error> {
    Err(Error::unsupported(
        "install replica",
        "Managed Sync executable attributes are not implemented on this platform",
    ))
}
