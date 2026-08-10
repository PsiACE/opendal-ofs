// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Materializing and atomically installing a reconciled local tree.

use std::collections::BTreeSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use super::local::{entry_at, fs_operator, set_executable};
use super::reconcile::{ReconcilePlan, TargetEdit};
use super::staging::{StagedTree, TargetManifest};
use crate::filesystem::{MaterializeRequest, NodeKind, Volume, VolumeSnapshot};

pub(super) async fn apply_target<V: Volume>(
    volume: &V,
    staged: &mut StagedTree,
    source_root: &Path,
    authority: Option<&VolumeSnapshot>,
    plan: ReconcilePlan,
    transfer_concurrency: NonZeroUsize,
) -> Result<()> {
    let ReconcilePlan {
        target: manifest,
        edits,
        ..
    } = plan;
    let root = staged.root.clone();
    let target = fs_operator(&root)?;
    for (path, kind) in removal_roots(staged.manifest(), &manifest) {
        match kind {
            NodeKind::Directory => target.delete_with(path).recursive(true).await?,
            NodeKind::RegularFile => target.delete(path).await?,
        }
    }
    for (path, edit) in &edits {
        if matches!(edit, TargetEdit::Directory) {
            target.create_dir(&format!("{path}/")).await?;
        }
    }

    let mut materialize = BTreeSet::new();
    for (path, edit) in &edits {
        match edit {
            TargetEdit::Materialize => {
                materialize.insert(path.clone());
            }
            TargetEdit::Reuse(source)
                if !reuse_local_file(
                    source_root,
                    &root,
                    &staged.source,
                    &manifest,
                    source,
                    path,
                )
                .await? =>
            {
                materialize.insert(path.clone());
            }
            TargetEdit::Reuse(_) | TargetEdit::Directory => {}
        }
    }

    let requests =
        materialize
            .iter()
            .map(|path| -> Result<_> {
                let entry = manifest.entries.get(path).with_context(|| {
                    format!("materialization path {path:?} is not a target file")
                })?;
                let file = entry.file.as_ref().with_context(|| {
                    format!("materialization path {path:?} is not a target file")
                })?;
                Ok(MaterializeRequest {
                    path: path.clone(),
                    version: staged
                        .resolve_version(file, entry.local.size, authority)?
                        .clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
    let segment_staging = staged.segment_operator()?;
    volume
        .materialize(
            &target,
            Some(&segment_staging),
            requests,
            transfer_concurrency,
        )
        .await?;
    for path in &materialize {
        let executable = manifest
            .entries
            .get(path)
            .with_context(|| format!("materialization path {path:?} is not in target manifest"))?
            .local
            .executable;
        set_executable(&root.join(path), executable)?;
    }
    staged.replace_manifest(manifest);
    Ok(())
}

async fn reuse_local_file(
    source_root: &Path,
    staging_root: &Path,
    source: &TargetManifest,
    target: &TargetManifest,
    source_path: &str,
    target_path: &str,
) -> Result<bool> {
    let Some(source_entry) = source.entries.get(source_path) else {
        return Ok(false);
    };
    let Some(source_file) = source.file(source_path) else {
        return Ok(false);
    };
    let Some(target_entry) = target.entries.get(target_path) else {
        return Ok(false);
    };
    let Some(target_file) = &target_entry.file else {
        return Ok(false);
    };
    if source_file.logical_digest != target_file.logical_digest
        || source_entry.local.size != target_entry.local.size
    {
        return Ok(false);
    }
    let Ok(observed) = entry_at(source_root, source_path).await else {
        return Ok(false);
    };
    if observed != source_entry.local {
        return Ok(false);
    }

    let destination = staging_root.join(target_path);
    tokio::fs::create_dir_all(destination.parent().unwrap_or(staging_root)).await?;
    let copied = match tokio::fs::copy(source_root.join(source_path), &destination).await {
        Ok(copied) => copied,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("reuse local file for remote rename"),
    };
    let source_unchanged = entry_at(source_root, source_path)
        .await
        .is_ok_and(|observed| observed == source_entry.local);
    if copied != target_entry.local.size || !source_unchanged {
        let _ = tokio::fs::remove_file(destination).await;
        return Ok(false);
    }
    set_executable(&destination, target_entry.local.executable)?;
    Ok(true)
}

pub(super) fn install_staged_changes(
    replica: &Path,
    staged: &StagedTree,
    before: &TargetManifest,
) -> Result<()> {
    for (path, _) in removal_roots(before, staged.manifest()) {
        let target = replica.join(path);
        match fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&target)?,
            Ok(_) => fs::remove_file(&target)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("inspect obsolete replica path"),
        }
        sync_parent(&target)?;
    }

    for (path, entry) in &staged.manifest().entries {
        if entry.local.kind == NodeKind::Directory {
            fs::create_dir_all(replica.join(path))?;
        }
    }
    for (path, entry) in &staged.manifest().entries {
        if entry.local.kind != NodeKind::RegularFile {
            continue;
        }
        let same_content = before.entries.get(path).is_some_and(|existing| {
            existing.local.kind == NodeKind::RegularFile
                && existing.local.size == entry.local.size
                && before.file(path).map(|file| file.logical_digest)
                    == staged.manifest().file(path).map(|file| file.logical_digest)
        });
        let destination = replica.join(path);
        if !same_content {
            let source = staged.root.join(path);
            let parent = destination.parent().unwrap_or(replica);
            fs::create_dir_all(parent)?;
            let temporary = parent.join(format!(".ofs-install-{}", uuid::Uuid::new_v4()));
            let result = (|| -> Result<()> {
                let installed = match fs::rename(&source, &temporary) {
                    Ok(()) => fs::metadata(&temporary)?.len(),
                    Err(error) if error.kind() == std::io::ErrorKind::CrossesDevices => {
                        fs::copy(&source, &temporary)?
                    }
                    Err(error) => return Err(error.into()),
                };
                if installed != entry.local.size {
                    bail!("staged path {path:?} has an unexpected length")
                }
                set_executable(&temporary, entry.local.executable)?;
                fs::File::open(&temporary)?.sync_all()?;
                fs::rename(&temporary, &destination)?;
                sync_parent(&destination)
            })();
            if result.is_err() {
                let _ = fs::remove_file(&temporary);
            }
            result.with_context(|| format!("install staged path {path:?}"))?;
        } else {
            set_executable(&destination, entry.local.executable)?;
        }
    }
    Ok(())
}

fn removal_roots<'a>(
    before: &'a TargetManifest,
    after: &TargetManifest,
) -> Vec<(&'a str, NodeKind)> {
    let mut roots = Vec::<(&str, NodeKind)>::new();
    for (path, entry) in &before.entries {
        if after
            .entries
            .get(path)
            .is_some_and(|desired| desired.local.kind == entry.local.kind)
            || roots.last().is_some_and(|(parent, _)| {
                path.strip_prefix(parent)
                    .is_some_and(|suffix| suffix.starts_with('/'))
            })
        {
            continue;
        }
        roots.push((path, entry.local.kind));
    }
    roots
}

pub(super) fn fresh_sibling(path: &Path, purpose: &str) -> PathBuf {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("replica");
    parent.join(format!(".{name}.ofs-{purpose}-{}", uuid::Uuid::new_v4()))
}

pub(super) fn remove_tree(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path).context("remove completed sync staging")?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    fs::File::open(parent)?.sync_all().map_err(Into::into)
}

#[cfg(not(unix))]
fn sync_parent(_: &Path) -> Result<()> {
    Ok(())
}
