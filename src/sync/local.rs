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
use futures::TryStreamExt as _;
use opendal::{EntryMode, Operator, services};
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
        let operator = fs_operator(root)?;
        let mut listed = operator
            .lister_with("")
            .recursive(true)
            .await
            .context("scan local replica through OpenDAL fs")?;
        let mut entries = BTreeMap::new();
        let mut file_identities = BTreeMap::new();
        while let Some(entry) = listed
            .try_next()
            .await
            .context("scan local replica through OpenDAL fs")?
        {
            let path = entry.path().trim_end_matches('/');
            if path.is_empty() {
                continue;
            }
            let metadata = entry.metadata();
            let kind = match metadata.mode() {
                EntryMode::DIR => NodeKind::Directory,
                EntryMode::FILE => NodeKind::RegularFile,
                _ => bail!(
                    "local path {path:?} is a symbolic link or special file; remove it before sync"
                ),
            };
            let modified = metadata
                .last_modified()
                .context("local filesystem did not report modification time")?
                .to_string();
            let (native_identity, executable, link_count) = native_attributes_at(root, path, kind)?;
            if kind == NodeKind::RegularFile {
                if link_count > 1 {
                    bail!(
                        "local path {path:?} is a hard link; Sync does not publish hard-linked files"
                    );
                }
                if let Some(identity) = native_identity
                    && let Some(other) = file_identities.insert(identity, path.to_owned())
                {
                    bail!(
                        "local paths {other:?} and {path:?} are hard links; Sync does not publish hard-linked files"
                    );
                }
            }
            entries.insert(
                path.to_owned(),
                LocalEntry {
                    kind,
                    size: metadata.content_length(),
                    modified,
                    executable,
                    native_identity,
                },
            );
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
    let metadata = fs_operator(root)?
        .stat(path)
        .await
        .with_context(|| format!("inspect materialized path {path:?}"))?;
    let kind = match metadata.mode() {
        EntryMode::DIR => NodeKind::Directory,
        EntryMode::FILE => NodeKind::RegularFile,
        _ => bail!("materialized path {path:?} is a symbolic link or special file"),
    };
    let (native_identity, executable, _) = native_attributes_at(root, path, kind)?;
    Ok(LocalEntry {
        kind,
        size: metadata.content_length(),
        modified: metadata
            .last_modified()
            .context("local filesystem did not report modification time")?
            .to_string(),
        executable,
        native_identity,
    })
}

pub(crate) fn native_identity_at(
    root: &Path,
    path: &str,
    kind: NodeKind,
) -> Result<Option<NativeIdentity>> {
    native_attributes_at(root, path, kind).map(|(identity, _, _)| identity)
}

fn native_attributes_at(
    root: &Path,
    path: &str,
    kind: NodeKind,
) -> Result<(Option<NativeIdentity>, bool, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let metadata = fs::symlink_metadata(root.join(path))
            .with_context(|| format!("inspect local attributes for {path:?}"))?;
        let expected = match kind {
            NodeKind::Directory => metadata.file_type().is_dir(),
            NodeKind::RegularFile => metadata.file_type().is_file(),
        };
        if !expected {
            bail!("local path {path:?} changed kind while it was being inspected; retry sync");
        }
        Ok((
            Some(NativeIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            }),
            kind == NodeKind::RegularFile && metadata.permissions().mode() & 0o111 != 0,
            metadata.nlink(),
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = (root, path, kind);
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
