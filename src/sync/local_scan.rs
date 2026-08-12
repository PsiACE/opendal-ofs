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

//! Read native filesystem facts into a path-ordered record stream.

use std::fs::ReadDir;
use std::path::{Path, PathBuf};

use futures::stream::{FuturesUnordered, StreamExt as _};
use serde::{Deserialize, Serialize};
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use crate::Error;
use crate::filesystem::{FileFingerprint, NodeKind, validate_portable_path};
use crate::workset;

use super::transfer::inspect_file;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct LocalRecord {
    pub(super) path: String,
    pub(super) kind: NodeKind,
    pub(super) executable: bool,
    pub(super) fingerprint: Option<FileFingerprint>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PortableName {
    parent: String,
    folded: String,
}

struct DirectoryScan {
    path: PathBuf,
    relative: String,
    children: ReadDir,
}

pub(super) async fn scan(
    workspace: &workset::Workspace,
    root: &Path,
    concurrency: usize,
) -> Result<workset::Spool<LocalRecord>, Error> {
    let metadata = std::fs::symlink_metadata(root)
        .map_err(|error| Error::from_io("inspect local path", Some(root), error))?;
    if !metadata.is_dir() {
        return Err(Error::invalid(
            "scan replica",
            "local replica root is not a directory",
        ));
    }

    let mut records = workspace.writer("local-paths")?;
    records.write(&LocalRecord {
        path: String::new(),
        kind: NodeKind::Directory,
        executable: false,
        fingerprint: None,
    })?;
    let mut portable_names = workspace.writer("portable-names")?;
    let children = std::fs::read_dir(root)
        .map_err(|error| Error::from_io("scan local directory", Some(root), error))?;
    let mut directories = vec![DirectoryScan {
        path: root.to_owned(),
        relative: String::new(),
        children,
    }];
    let mut inspections = FuturesUnordered::new();

    while !directories.is_empty() {
        let next = {
            let directory = directories.last_mut().expect("directory scan exists");
            match directory.children.next() {
                Some(child) => Some((directory.path.clone(), directory.relative.clone(), child)),
                None => None,
            }
        };
        let Some((directory, parent, child)) = next else {
            directories.pop();
            continue;
        };
        let child = child
            .map_err(|error| Error::from_io("scan local directory", Some(&directory), error))?;
        let name = child.file_name().into_string().map_err(|_| {
            Error::invalid(
                "synchronize replica",
                "local directory contains a non-Unicode name",
            )
        })?;
        let path = if parent.is_empty() {
            name.clone()
        } else {
            format!("{parent}/{name}")
        };
        validate_portable_path(&path)?;
        portable_names.write(&PortableName {
            parent: parent.clone(),
            folded: name.case_fold().nfc().collect(),
        })?;

        let child_path = child.path();
        let metadata = std::fs::symlink_metadata(&child_path)
            .map_err(|error| Error::from_io("inspect local path", Some(&child_path), error))?;
        let (kind, executable) = local_entry(&metadata)?;
        let record = LocalRecord {
            path: path.clone(),
            kind,
            executable,
            fingerprint: None,
        };
        if kind == NodeKind::RegularFile {
            inspections.push(inspect_local_file(child_path.clone(), record));
            if inspections.len() >= concurrency {
                let record = inspections
                    .next()
                    .await
                    .expect("a local file inspection remains")?;
                records.write(&record)?;
            }
        } else {
            records.write(&record)?;
        }
        if kind == NodeKind::Directory {
            let children = std::fs::read_dir(&child_path).map_err(|error| {
                Error::from_io("scan local directory", Some(&child_path), error)
            })?;
            directories.push(DirectoryScan {
                path: child_path,
                relative: path,
                children,
            });
        }
    }
    while let Some(record) = inspections.next().await {
        records.write(&record?)?;
    }

    validate_portable_names(workspace, portable_names.finish()?)?;
    let records = records.finish()?;
    workset::sort(workspace, &records, |record: &LocalRecord| {
        record.path.clone()
    })
}

async fn inspect_local_file(path: PathBuf, mut record: LocalRecord) -> Result<LocalRecord, Error> {
    record.fingerprint = Some(inspect_file(&path).await?);
    Ok(record)
}

fn validate_portable_names(
    workspace: &workset::Workspace,
    names: workset::Spool<PortableName>,
) -> Result<(), Error> {
    let names = workset::sort(workspace, &names, |name: &PortableName| {
        (name.parent.clone(), name.folded.clone())
    })?;
    let mut reader = names.reader()?;
    let mut previous = None;
    while let Some(name) = reader.next()? {
        let key = (name.parent, name.folded);
        if previous.as_ref() == Some(&key) {
            return Err(Error::invalid(
                "synchronize replica",
                "directory contains a case-folding collision",
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

#[cfg(unix)]
fn local_entry(metadata: &std::fs::Metadata) -> Result<(NodeKind, bool), Error> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if metadata.is_dir() {
        return Ok((NodeKind::Directory, false));
    }
    if metadata.is_file() {
        if metadata.nlink() > 1 {
            return Err(Error::unsupported(
                "scan replica",
                "local replica contains a hard-linked file, which Managed Sync does not support",
            ));
        }
        return Ok((
            NodeKind::RegularFile,
            metadata.permissions().mode() & 0o111 != 0,
        ));
    }
    Err(Error::unsupported(
        "scan replica",
        "local replica contains a symbolic link or special file",
    ))
}

#[cfg(not(unix))]
fn local_entry(metadata: &std::fs::Metadata) -> Result<(NodeKind, bool), Error> {
    if metadata.is_dir() {
        Ok((NodeKind::Directory, false))
    } else if metadata.is_file() {
        Ok((NodeKind::RegularFile, false))
    } else {
        Err(Error::unsupported(
            "scan replica",
            "local replica contains a symbolic link or special file",
        ))
    }
}
