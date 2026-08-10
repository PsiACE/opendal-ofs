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
use opendal::{Operator, services};
use serde::{Deserialize, Serialize};

use crate::filesystem::{NodeKind, validate_portable_paths};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeIdentity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalEntry {
    pub kind: NodeKind,
    pub size: u64,
    pub modified: String,
    pub executable: bool,
    pub native_identity: Option<NativeIdentity>,
}

/// One stable, path-sorted observation of an ordinary directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalTree {
    pub(super) root: PathBuf,
    pub(super) entries: BTreeMap<String, LocalEntry>,
}

impl LocalTree {
    pub(crate) async fn scan(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        root.to_str()
            .context("local replica path is not valid Unicode")?;
        let mut pending = vec![(root.to_owned(), String::new())];
        let mut entries = BTreeMap::new();
        let mut file_identities = BTreeMap::new();
        while let Some((directory, parent)) = pending.pop() {
            let mut children = tokio::fs::read_dir(&directory)
                .await
                .with_context(|| format!("scan local directory {parent:?}"))?;
            while let Some(child) = children
                .next_entry()
                .await
                .with_context(|| format!("scan local directory {parent:?}"))?
            {
                let name = child.file_name().into_string().map_err(|_| {
                    anyhow::anyhow!("local directory {parent:?} contains a non-Unicode name")
                })?;
                let path = if parent.is_empty() {
                    name
                } else {
                    format!("{parent}/{name}")
                };
                let (entry, link_count) = local_entry(
                    &path,
                    tokio::fs::symlink_metadata(child.path())
                        .await
                        .with_context(|| format!("inspect local path {path:?}"))?,
                )?;
                if entry.kind == NodeKind::Directory {
                    pending.push((child.path(), path.clone()));
                } else {
                    if link_count > 1 {
                        bail!(
                            "local path {path:?} is a hard link; Sync does not publish hard-linked files"
                        );
                    }
                    if let Some(identity) = entry.native_identity
                        && let Some(other) = file_identities.insert(identity, path.clone())
                    {
                        bail!(
                            "local paths {other:?} and {path:?} are hard links; Sync does not publish hard-linked files"
                        );
                    }
                }
                entries.insert(path, entry);
            }
        }
        validate_portable_paths(entries.keys().map(String::as_str))
            .map_err(anyhow::Error::new)
            .context("local replica contains a non-portable path")?;
        Ok(Self {
            root: root.to_owned(),
            entries,
        })
    }
}

pub(crate) async fn entry_at(root: &Path, path: &str) -> Result<LocalEntry> {
    local_entry(
        path,
        tokio::fs::symlink_metadata(root.join(path))
            .await
            .with_context(|| format!("inspect materialized path {path:?}"))?,
    )
    .map(|(entry, _)| entry)
}

pub(crate) fn native_identity_at(
    root: &Path,
    path: &str,
    kind: NodeKind,
) -> Result<Option<NativeIdentity>> {
    let (entry, _) = local_entry(
        path,
        fs::symlink_metadata(root.join(path))
            .with_context(|| format!("inspect local attributes for {path:?}"))?,
    )?;
    if entry.kind != kind {
        bail!("local path {path:?} changed kind while it was being inspected; retry sync");
    }
    Ok(entry.native_identity)
}

fn local_entry(path: &str, metadata: fs::Metadata) -> Result<(LocalEntry, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let kind = match metadata.file_type() {
            kind if kind.is_dir() => NodeKind::Directory,
            kind if kind.is_file() => NodeKind::RegularFile,
            _ => bail!(
                "local path {path:?} is a symbolic link or special file; remove it before sync"
            ),
        };
        Ok((
            LocalEntry {
                kind,
                size: metadata.len(),
                modified: format!("{}.{:09}", metadata.mtime(), metadata.mtime_nsec()),
                executable: kind == NodeKind::RegularFile
                    && metadata.permissions().mode() & 0o111 != 0,
                native_identity: Some(NativeIdentity {
                    device: metadata.dev(),
                    inode: metadata.ino(),
                }),
            },
            metadata.nlink(),
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = (path, metadata);
        bail!(
            "Managed Sync requires native file identity and executable attributes on this platform"
        )
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
    {
        let _ = (path, executable);
        bail!("Managed Sync requires executable attribute support on this platform");
    }
    Ok(())
}

pub(crate) fn require_native_capabilities() -> Result<()> {
    #[cfg(not(unix))]
    bail!(
        "Managed Sync is unavailable because this platform cannot preserve native identity and executable attributes"
    );
    #[cfg(unix)]
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
