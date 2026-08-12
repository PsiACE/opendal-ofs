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
use std::path::Path;

use futures::stream::{FuturesUnordered, StreamExt as _};
use serde::de::DeserializeOwned;

use crate::Error;
use crate::filesystem::NamespaceValue;
use crate::managed::{ManagedVolume, StreamRef};
use crate::namespace::Namespace;
use crate::workset::{self, Spool, Workspace};

use super::local_fs::{self, DirectoryDurability, StoredPath};
use super::transfer::materialize_file;

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
    let workspace = Workspace::create(volume.workset_options())?;
    let removals = if authoritative {
        repair_removals(root, target, &workspace)?
    } else {
        namespace_removals(current, target, &workspace)?
    };
    let mut durability = DirectoryDurability::create(&workspace)?;
    let removals = workset::sort(&workspace, &removals, |path| Reverse(path.clone()))?;
    let mut removals = removals.reader()?;
    while let Some(path) = removals.next()? {
        let destination = root.join(path.to_path_buf());
        local_fs::remove_path(&destination)?;
        durability.changed_parent(&destination)?;
        crate::fault::check("during-install")?;
    }

    let mut file_installations = FuturesUnordered::new();
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
        let destination = root.join(&record.path);
        let NamespaceValue::RegularFile {
            version,
            fingerprint,
            content,
        } = node.value
        else {
            if record.path.is_empty() {
                continue;
            }
            match local_fs::path_metadata(&destination)? {
                Some(metadata) if metadata.is_dir() => {}
                Some(_) => {
                    local_fs::remove_path(&destination)?;
                    durability.changed_parent(&destination)?;
                    crate::fault::check("during-install")?;
                    local_fs::create_directory(&destination, &mut durability)?;
                    crate::fault::check("during-install")?;
                }
                None => {
                    local_fs::create_directory(&destination, &mut durability)?;
                    crate::fault::check("during-install")?;
                }
            }
            continue;
        };
        if !authoritative
            && matching_current.is_some_and(|current| {
                current.value.as_ref().is_some_and(|current| {
                    current.attributes == node.attributes
                        && current
                            .file()
                            .is_some_and(|(current_version, _, _)| current_version == version)
                })
            })
        {
            continue;
        }
        let executable = node.attributes.executable;
        file_installations.push(install_file(
            volume,
            destination,
            fingerprint,
            content,
            executable,
            authoritative,
        ));
        if file_installations.len() >= transfer_concurrency
            && let Some(destination) = file_installations
                .next()
                .await
                .expect("a file installation remains")?
        {
            durability.changed_parent(&destination)?;
        }
    }
    while let Some(installation) = file_installations.next().await {
        if let Some(destination) = installation? {
            durability.changed_parent(&destination)?;
        }
    }
    durability.sync(&workspace)?;
    Ok(())
}

async fn install_file(
    volume: &ManagedVolume,
    destination: std::path::PathBuf,
    fingerprint: crate::filesystem::FileFingerprint,
    content: StreamRef,
    executable: bool,
    authoritative: bool,
) -> Result<Option<std::path::PathBuf>, Error> {
    if authoritative && local_fs::file_matches(&destination, fingerprint, executable).await? {
        return Ok(None);
    }
    if local_fs::path_metadata(&destination)?.is_some_and(|metadata| metadata.is_dir()) {
        local_fs::remove_replaced_directory(&destination)?;
        crate::fault::check("during-install")?;
    }
    materialize_file(volume, (fingerprint, content), &destination).await?;
    if executable {
        local_fs::make_executable(&destination)?;
    }
    crate::fault::check("during-install")?;
    Ok(Some(destination))
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
    local_fs::scan_paths(root, &mut actual, &mut removed)?;
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
