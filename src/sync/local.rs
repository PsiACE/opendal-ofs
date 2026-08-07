// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use opendal::{EntryMode, Operator, services};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalKind {
    Directory,
    File,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NativeFileIdentity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalEntry {
    pub kind: LocalKind,
    pub size: u64,
    pub modified: String,
    pub executable: bool,
    pub native_identity: Option<NativeFileIdentity>,
}

/// One stable, path-sorted observation of an ordinary directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalTree {
    root: PathBuf,
    entries: BTreeMap<String, LocalEntry>,
}

impl LocalTree {
    pub async fn scan(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let operator = fs_operator(root)?;
        let listed = operator
            .list_with("")
            .recursive(true)
            .await
            .context("scan local replica through OpenDAL fs")?;
        let mut entries = BTreeMap::new();
        for entry in listed {
            let path = entry.path().trim_end_matches('/');
            if path.is_empty() {
                continue;
            }
            let metadata = entry.metadata();
            let kind = match metadata.mode() {
                EntryMode::DIR => LocalKind::Directory,
                EntryMode::FILE => LocalKind::File,
                _ => bail!(
                    "local path {path:?} is a symbolic link or special file; remove it before sync"
                ),
            };
            let modified = metadata
                .last_modified()
                .context("local filesystem did not report modification time")?
                .to_string();
            entries.insert(
                path.to_owned(),
                LocalEntry {
                    kind,
                    size: metadata.content_length(),
                    modified,
                    executable: executable_at(root, path, kind)?,
                    native_identity: native_file_identity_at(root, path, kind)?,
                },
            );
        }
        Ok(Self {
            root: root.to_owned(),
            entries,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn entries(&self) -> &BTreeMap<String, LocalEntry> {
        &self.entries
    }

    pub(crate) fn operator(&self) -> Result<Operator> {
        fs_operator(&self.root)
    }
}

pub(crate) fn native_file_identity_at(
    root: &Path,
    path: &str,
    kind: LocalKind,
) -> Result<Option<NativeFileIdentity>> {
    if kind != LocalKind::File {
        return Ok(None);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = fs::symlink_metadata(root.join(path))
            .with_context(|| format!("inspect local identity for {path:?}"))?;
        if !metadata.file_type().is_file() {
            bail!("local path {path:?} is not a regular file; remove it before sync");
        }
        Ok(Some(NativeFileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        }))
    }
    #[cfg(not(unix))]
    {
        let _ = (root, path);
        Ok(None)
    }
}

pub(crate) fn executable_at(root: &Path, path: &str, kind: LocalKind) -> Result<bool> {
    if kind != LocalKind::File {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let metadata = fs::symlink_metadata(root.join(path))
            .with_context(|| format!("inspect local permissions for {path:?}"))?;
        if metadata.file_type().is_symlink() {
            bail!("local path {path:?} is a symbolic link; remove it before sync");
        }
        Ok(metadata.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        let _ = (root, path);
        Ok(false)
    }
}

pub(crate) fn set_executable(path: &Path, executable: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mut permissions = fs::metadata(path)?.permissions();
        let mode = permissions.mode();
        permissions.set_mode(if executable {
            mode | 0o111
        } else {
            mode & !0o111
        });
        fs::set_permissions(path, permissions)?;
    }
    #[cfg(not(unix))]
    let _ = (path, executable);
    Ok(())
}

pub(crate) fn fs_operator(root: &Path) -> Result<Operator> {
    let root = root
        .to_str()
        .context("local replica path is not valid Unicode")?;
    Ok(Operator::new(services::Fs::default().root(root))
        .context("configure OpenDAL fs for local replica")?
        .finish())
}
