// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One foreground Managed Sync reconciliation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use futures::{StreamExt, TryStreamExt, stream};
use uuid::Uuid;

use crate::model::{
    CommitRecord, Cursor, Manifest, NamespaceChange, Node, NodeKind, OperationId, VolumeId,
};
use crate::replica::{
    CommonBase, PendingMaterialization, PendingPublication, ReplicaLock, ReplicaPaths, ReplicaState,
};
use crate::store::{DataStore, MetadataStore, PublicationOutcome};

pub(crate) const CAPABILITIES: &[&str] = &[
    "atomic-snapshot",
    "change-feed",
    "conditional-publication",
    "conflict-retention",
    "idempotent-publication",
    "immutable-data",
    "local-replica",
    "offline-write",
    "portable-names",
    "stable-node-id",
];

pub(crate) fn admit(required: &[String]) -> Result<()> {
    for capability in required {
        if !CAPABILITIES.contains(&capability.as_str()) {
            bail!("required Managed Sync capability {capability:?} is unavailable");
        }
    }
    Ok(())
}

pub(crate) struct ManagedVolume {
    pub(crate) metadata: Box<dyn MetadataStore>,
    pub(crate) data: DataStore,
}

pub(crate) struct SyncRequest<'a> {
    pub(crate) volume_id: &'a VolumeId,
    pub(crate) local: &'a Path,
    pub(crate) state: Option<&'a Path>,
    pub(crate) resolutions: &'a [PathBuf],
    pub(crate) transfers: NonZeroUsize,
}

pub(crate) async fn sync_once(volume: &ManagedVolume, request: SyncRequest<'_>) -> Result<u64> {
    let paths = ReplicaPaths::resolve(request.local, request.state)?;
    let _lock = ReplicaLock::acquire(&paths)?;
    let mut state = ReplicaState::load_or_new(request.volume_id, &paths)?;
    recover(volume, &paths, &mut state, request.transfers).await?;
    if state.publication.is_none() {
        crate::replica::clear_staging(&paths)?;
    }

    let observed = volume.metadata.observe(request.volume_id).await?;
    let remote = remote_manifest(
        volume,
        &state,
        &observed.head.cursor,
        &observed.head.checkpoint,
    )
    .await?;

    if state.common.is_none() {
        let local = crate::replica::scan(&paths, None, false)?;
        if !local.entries.is_empty() && !remote.entries.is_empty() {
            bail!("local and remote trees are both non-empty without a common base");
        }
        if local.entries.is_empty() {
            materialize(
                volume,
                &paths,
                &mut state,
                observed.head.cursor,
                remote,
                request.transfers,
            )
            .await?;
            return Ok(state.common.as_ref().unwrap().cursor.generation);
        }
    }

    let base = state
        .common
        .as_ref()
        .map(|value| &value.manifest)
        .cloned()
        .unwrap_or_default();
    let resolved = resolution_paths(request.resolutions)?;
    if !state.conflicts.is_empty() {
        let unresolved = state
            .conflicts
            .iter()
            .map(|conflict| conflict.path.as_str())
            .filter(|path| !resolved.contains(*path))
            .collect::<Vec<_>>();
        if !unresolved.is_empty() {
            bail!(
                "unresolved Managed Sync conflicts: {}",
                unresolved.join(", ")
            );
        }
        if state
            .conflicts
            .iter()
            .any(|conflict| conflict.remote_cursor != observed.head.cursor)
        {
            bail!("remote generation changed after conflict resolution was selected");
        }
    }

    let stable_local = crate::replica::scan(&paths, Some(&base), true)?;
    if state.conflicts.is_empty()
        && state
            .common
            .as_ref()
            .is_some_and(|common| common.cursor == observed.head.cursor)
        && stable_local == remote
    {
        return Ok(observed.head.cursor.generation);
    }
    let (target, conflicts) = crate::reconcile::merge(
        &base,
        &stable_local,
        &remote,
        &observed.head.cursor,
        &resolved,
    );
    if !conflicts.is_empty() {
        state.conflicts = conflicts;
        state.save(&paths)?;
        crate::replica::clear_staging(&paths)?;
        bail!("Managed Sync conflict; inspect status and rerun with --resolve PATH");
    }
    state.conflicts.clear();

    if target == remote {
        materialize(
            volume,
            &paths,
            &mut state,
            observed.head.cursor,
            target,
            request.transfers,
        )
        .await?;
        return Ok(state.common.as_ref().unwrap().cursor.generation);
    }

    let changes = crate::reconcile::diff(&remote, &target);
    let operation = OperationId::parse(Uuid::new_v4().to_string())?;
    let pending = PendingPublication {
        operation: operation.clone(),
        parent: observed.head.cursor.clone(),
        source: stable_local.clone(),
        target: target.clone(),
        changes: changes.clone(),
    };
    crate::replica::sync_staging(&paths)?;
    state.publication = Some(pending);
    state.save(&paths)?;
    upload_changes(volume, &paths, &changes, request.transfers).await?;
    if !publication_source_unchanged(&paths, &stable_local)? {
        state.publication = None;
        state.save(&paths)?;
        crate::replica::clear_staging(&paths)?;
        bail!("local tree changed while publication was prepared; rerun sync");
    }
    let commit = CommitRecord::new(
        request.volume_id.clone(),
        observed.head.cursor.clone(),
        operation,
        changes,
    )?;
    let cursor = match volume.metadata.publish(&observed, commit).await? {
        PublicationOutcome::Committed(cursor) => cursor,
        PublicationOutcome::AlreadyCommitted(cursor) => cursor,
        PublicationOutcome::Conflict(actual) => {
            state.publication = None;
            state.save(&paths)?;
            crate::replica::clear_staging(&paths)?;
            bail!(
                "publication was stale at generation {}; rerun sync to reconcile",
                actual.head.cursor.generation
            )
        }
        PublicationOutcome::Unknown => {
            bail!("publication result is unknown; rerun sync to resolve the recorded operation")
        }
    };
    if !publication_source_unchanged(&paths, &stable_local)? {
        bail!("publication committed but the local tree changed; rerun sync");
    }
    state.publication = None;
    state.save(&paths)?;
    materialize(
        volume,
        &paths,
        &mut state,
        cursor,
        target,
        request.transfers,
    )
    .await?;
    Ok(state.common.as_ref().unwrap().cursor.generation)
}

async fn recover(
    volume: &ManagedVolume,
    paths: &ReplicaPaths,
    state: &mut ReplicaState,
    transfers: NonZeroUsize,
) -> Result<()> {
    if let Some(pending) = state.publication.clone() {
        match volume
            .metadata
            .resolve(&state.volume_id, &pending.operation)
            .await?
        {
            Some(cursor) => {
                if publication_source_unchanged(paths, &pending.source)? {
                    state.publication = None;
                    state.save(paths)?;
                    materialize(volume, paths, state, cursor, pending.target, transfers).await?;
                } else {
                    state.publication = None;
                    state.save(paths)?;
                    crate::replica::clear_staging(paths)?;
                }
            }
            None => {
                let observed = volume.metadata.observe(&state.volume_id).await?;
                if observed.head.cursor == pending.parent {
                    if !publication_source_unchanged(paths, &pending.source)? {
                        state.publication = None;
                        state.save(paths)?;
                        crate::replica::clear_staging(paths)?;
                        return Ok(());
                    }
                    upload_changes(volume, paths, &pending.changes, transfers).await?;
                    let commit = CommitRecord::new(
                        state.volume_id.clone(),
                        pending.parent,
                        pending.operation,
                        pending.changes,
                    )?;
                    let cursor = match volume.metadata.publish(&observed, commit).await? {
                        PublicationOutcome::Committed(cursor) => cursor,
                        PublicationOutcome::AlreadyCommitted(cursor) => cursor,
                        PublicationOutcome::Unknown => {
                            bail!("recorded publication result remains unknown")
                        }
                        PublicationOutcome::Conflict(_) => {
                            state.publication = None;
                            state.save(paths)?;
                            return Ok(());
                        }
                    };
                    if publication_source_unchanged(paths, &pending.source)? {
                        state.publication = None;
                        state.save(paths)?;
                        materialize(volume, paths, state, cursor, pending.target, transfers)
                            .await?;
                    } else {
                        state.publication = None;
                        state.save(paths)?;
                        crate::replica::clear_staging(paths)?;
                    }
                } else {
                    state.publication = None;
                    state.save(paths)?;
                }
            }
        }
    }
    if let Some(pending) = state.materialization.clone() {
        materialize(
            volume,
            paths,
            state,
            pending.target,
            pending.manifest,
            transfers,
        )
        .await?;
    }
    Ok(())
}

fn publication_source_unchanged(paths: &ReplicaPaths, source: &Manifest) -> Result<bool> {
    Ok(crate::replica::scan(paths, Some(source), false)? == *source)
}

async fn remote_manifest(
    volume: &ManagedVolume,
    state: &ReplicaState,
    target: &Cursor,
    checkpoint: &Cursor,
) -> Result<Manifest> {
    let (mut cursor, mut manifest) = match &state.common {
        Some(common) if common.cursor.generation <= target.generation => {
            (common.cursor.clone(), common.manifest.clone())
        }
        _ => {
            let value = volume
                .metadata
                .checkpoint(&state.volume_id, checkpoint)
                .await?;
            (value.cursor, value.manifest)
        }
    };
    for commit in volume
        .metadata
        .changes(&state.volume_id, &cursor, target)
        .await?
    {
        if commit.parent != cursor {
            bail!("Managed change log is not consecutive");
        }
        manifest = manifest.apply(&commit.changes)?;
        cursor = commit.cursor;
    }
    if cursor != *target {
        bail!("Managed change log did not reach its fixed target cursor");
    }
    Ok(manifest)
}

async fn upload_changes(
    volume: &ManagedVolume,
    paths: &ReplicaPaths,
    changes: &[NamespaceChange],
    concurrency: NonZeroUsize,
) -> Result<()> {
    let mut files = BTreeMap::new();
    for change in changes {
        let content = match change {
            NamespaceChange::Put {
                node:
                    Node {
                        kind: NodeKind::File { content, .. },
                        ..
                    },
                ..
            } => content,
            NamespaceChange::Put { .. } | NamespaceChange::Remove { .. } => continue,
        };
        if let Some(existing) = files.insert(content.sha256.clone(), content.clone())
            && existing != *content
        {
            bail!("one content digest has inconsistent publication metadata");
        }
    }
    stream::iter(files.into_values())
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(concurrency.get(), |content| async move {
            let staged = paths.staged(&content.sha256);
            if staged.exists() {
                volume
                    .data
                    .put_file(&staged, &content.sha256, content.size)
                    .await?;
            } else {
                volume.data.verify(&content).await?;
            }
            Ok(())
        })
        .await
}

async fn materialize(
    volume: &ManagedVolume,
    paths: &ReplicaPaths,
    state: &mut ReplicaState,
    cursor: Cursor,
    target: Manifest,
    transfers: NonZeroUsize,
) -> Result<()> {
    let mut durable_directories = BTreeSet::new();
    let mut installs = Vec::new();
    match &state.materialization {
        Some(pending) if pending.target == cursor && pending.manifest == target => {}
        Some(_) => bail!("materialization intent does not match its requested target"),
        None => {
            state.materialization = Some(PendingMaterialization {
                target: cursor.clone(),
                manifest: target.clone(),
            });
            state.save(paths)?;
        }
    }
    let current = crate::replica::scan(paths, Some(&target), false)?;
    stage_materialization_files(volume, paths, &current, &target, transfers).await?;
    let mut removals = current
        .entries
        .keys()
        .filter(|path| !target.entries.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    removals.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
    for relative in removals {
        let path = paths.local.join(relative);
        if let Some(parent) = path.parent() {
            durable_directories.insert(parent.to_owned());
        }
        remove_path(&path)?;
    }
    let mut directories = target
        .entries
        .iter()
        .filter(|(_, node)| matches!(node.kind, NodeKind::Directory))
        .collect::<Vec<_>>();
    directories.sort_by_key(|(path, _)| path.matches('/').count());
    for (relative, _) in directories {
        let path = paths.local.join(relative);
        if path.exists() && !path.is_dir() {
            remove_path(&path)?;
        }
        if !path.exists() {
            if let Some(parent) = path.parent() {
                durable_directories.insert(parent.to_owned());
            }
        }
        std::fs::create_dir_all(path)?;
    }
    for (relative, node) in &target.entries {
        let NodeKind::File {
            content,
            executable,
        } = &node.kind
        else {
            continue;
        };
        if current.entries.get(relative) == Some(node) {
            continue;
        }
        let path = paths.local.join(relative);
        if path.exists() && !path.is_file() {
            remove_path(&path)?;
        }
        let parent = path
            .parent()
            .context("materialization path has no parent")?;
        std::fs::create_dir_all(parent)?;
        durable_directories.insert(parent.to_owned());
        let temporary = parent.join(format!(".ofs-apply-{}", node.id.as_str()));
        installs.push(StagedInstall {
            source: paths.staged(&content.sha256),
            temporary,
            target: path,
            executable: *executable,
        });
    }
    install_staged_files(installs, transfers).await?;
    sync_durable_directories(durable_directories)?;
    let observed = crate::replica::scan(paths, Some(&target), false)?;
    if observed != target {
        bail!("materialized tree does not match its target manifest");
    }
    state.common = Some(CommonBase {
        cursor,
        manifest: target,
    });
    state.materialization = None;
    state.conflicts.clear();
    crate::replica::clear_staging(paths)?;
    state.save(paths)
}

fn sync_durable_directories(directories: BTreeSet<PathBuf>) -> Result<()> {
    let mut directories = directories.into_iter().collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        match std::fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() => crate::replica::sync_directory(&directory)?,
            Ok(_) => bail!(
                "materialization durability path is not a directory: {}",
                directory.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A deeper removal records both the removed directory and its
                // surviving parent. The parent fsync makes the removal durable.
            }
            Err(error) => {
                return Err(error).context(format!(
                    "inspect materialization durability path {}",
                    directory.display()
                ));
            }
        }
    }
    Ok(())
}

async fn stage_materialization_files(
    volume: &ManagedVolume,
    paths: &ReplicaPaths,
    current: &Manifest,
    target: &Manifest,
    transfers: NonZeroUsize,
) -> Result<()> {
    let mut contents = BTreeMap::new();
    for (relative, node) in &target.entries {
        if current.entries.get(relative) == Some(node) {
            continue;
        }
        let NodeKind::File { content, .. } = &node.kind else {
            continue;
        };
        if let Some(existing) = contents.insert(content.sha256.clone(), content.clone())
            && existing != *content
        {
            bail!("one content digest has inconsistent materialization metadata");
        }
    }
    if contents.is_empty() {
        return Ok(());
    }
    crate::replica::prepare_staging(paths)?;
    stream::iter(contents.into_values())
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(transfers.get(), |content| async move {
            if crate::replica::staged_content_matches(paths, &content)? {
                return Ok(());
            }
            crate::replica::discard_staged_content(paths, &content)?;
            volume
                .data
                .fetch(&content, &paths.staged(&content.sha256))
                .await
        })
        .await?;
    crate::replica::sync_staging(paths)
}

fn install_staged_file(
    staged: &Path,
    temporary: &Path,
    target: &Path,
    executable: bool,
) -> Result<()> {
    if temporary.exists() {
        remove_path(temporary)?;
    }
    let mut input = File::open(staged)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)?;
    std::io::copy(&mut input, &mut output)?;
    output.flush()?;
    output.sync_all()?;
    drop(output);
    set_executable(temporary, executable)?;
    std::fs::rename(temporary, target)?;
    Ok(())
}

struct StagedInstall {
    source: PathBuf,
    temporary: PathBuf,
    target: PathBuf,
    executable: bool,
}

async fn install_staged_files(installs: Vec<StagedInstall>, transfers: NonZeroUsize) -> Result<()> {
    stream::iter(installs)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(transfers.get(), |install| async move {
            tokio::task::spawn_blocking(move || {
                install_staged_file(
                    &install.source,
                    &install.temporary,
                    &install.target,
                    install.executable,
                )
            })
            .await
            .context("join staged file installation")?
        })
        .await
}

fn remove_path(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_dir() {
        std::fs::remove_dir(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_executable(path: &Path, executable: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path)?.permissions();
    let mode = if executable {
        permissions.mode() | 0o111
    } else {
        permissions.mode() & !0o111
    };
    permissions.set_mode(mode);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path, _executable: bool) -> Result<()> {
    Ok(())
}

fn resolution_paths(paths: &[PathBuf]) -> Result<BTreeSet<String>> {
    paths
        .iter()
        .map(|path| {
            let value = path
                .to_str()
                .context("resolution path must be UTF-8")?
                .replace('\\', "/");
            if value.is_empty()
                || value.starts_with('/')
                || value.split('/').any(|part| part == "..")
            {
                bail!("resolution path must be a portable relative path");
            }
            Ok(value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentRef, NodeId};
    use crate::reconcile::{diff, merge};
    use crate::replica::ConflictKind;

    fn directory(id: &str) -> Node {
        Node {
            id: NodeId::parse(id).unwrap(),
            kind: NodeKind::Directory,
        }
    }

    fn file(id: &str, digest_byte: char) -> Node {
        let sha256 = digest_byte.to_string().repeat(64);
        Node {
            id: NodeId::parse(id).unwrap(),
            kind: NodeKind::File {
                content: ContentRef { sha256, size: 1 },
                executable: false,
            },
        }
    }

    fn manifest(entries: Vec<(&str, Node)>) -> Manifest {
        Manifest {
            entries: entries
                .into_iter()
                .map(|(path, node)| (path.to_owned(), node))
                .collect(),
        }
    }

    fn cursor() -> Cursor {
        Cursor {
            generation: 1,
            operation: OperationId::parse("test-operation").unwrap(),
        }
    }

    #[test]
    fn established_empty_replicas_coalesce_nested_directory_creation() {
        let base = Manifest::default();
        let local = manifest(vec![
            (".agents", directory("local-agents")),
            (".agents/skills", directory("local-skills")),
            (".agents/skills/a.md", file("local-file", 'a')),
        ]);
        let remote = manifest(vec![
            (".agents", directory("remote-agents")),
            (".agents/skills", directory("remote-skills")),
            (".agents/skills/b.md", file("remote-file", 'b')),
        ]);

        let (target, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert!(conflicts.is_empty());
        assert_eq!(target.entries[".agents"].id, remote.entries[".agents"].id);
        assert_eq!(
            target.entries[".agents/skills"].id,
            remote.entries[".agents/skills"].id
        );
        assert!(target.entries.contains_key(".agents/skills/a.md"));
        assert!(target.entries.contains_key(".agents/skills/b.md"));
        target.validate().unwrap();
        let changes = diff(&remote, &target);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            NamespaceChange::Put { path, replaces: None, .. }
                if path == ".agents/skills/a.md"
        ));
    }

    #[test]
    fn upgrade_replicas_coalesce_one_new_public_directory() {
        let agents = directory("shared-agents");
        let base = manifest(vec![(".agents", agents.clone())]);
        let local = manifest(vec![
            (".agents", agents.clone()),
            (".agents/memory", directory("local-memory")),
            (".agents/memory/a.md", file("local-memory-file", 'a')),
        ]);
        let remote = manifest(vec![
            (".agents", agents),
            (".agents/memory", directory("remote-memory")),
            (".agents/memory/b.md", file("remote-memory-file", 'b')),
        ]);

        let (target, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert!(conflicts.is_empty());
        assert_eq!(
            target.entries[".agents/memory"].id,
            remote.entries[".agents/memory"].id
        );
        assert!(target.entries.contains_key(".agents/memory/a.md"));
        assert!(target.entries.contains_key(".agents/memory/b.md"));
        target.validate().unwrap();
    }

    #[test]
    fn replicas_coalesce_a_public_directory_recreated_after_deletion() {
        let agents = directory("shared-agents");
        let skills = directory("shared-skills");
        // Both replicas have already caught up to the generation where
        // `.agents/skills/shared` was deleted.
        let base = manifest(vec![
            (".agents", agents.clone()),
            (".agents/skills", skills.clone()),
        ]);
        let local = manifest(vec![
            (".agents", agents.clone()),
            (".agents/skills", skills.clone()),
            (".agents/skills/shared", directory("local-recreated-shared")),
            (
                ".agents/skills/shared/a.md",
                file("local-recreated-file", 'a'),
            ),
        ]);
        let remote = manifest(vec![
            (".agents", agents),
            (".agents/skills", skills),
            (
                ".agents/skills/shared",
                directory("remote-recreated-shared"),
            ),
            (
                ".agents/skills/shared/b.md",
                file("remote-recreated-file", 'b'),
            ),
        ]);

        let (target, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert!(conflicts.is_empty());
        assert_eq!(
            target.entries[".agents/skills/shared"].id,
            remote.entries[".agents/skills/shared"].id
        );
        assert!(target.entries.contains_key(".agents/skills/shared/a.md"));
        assert!(target.entries.contains_key(".agents/skills/shared/b.md"));
        target.validate().unwrap();
    }

    #[test]
    fn a_rename_and_an_unrelated_new_directory_do_not_coalesce() {
        let old = directory("existing-directory");
        let base = manifest(vec![("old", old.clone())]);
        let local = manifest(vec![("shared", old.clone())]);
        let remote = manifest(vec![("old", old), ("shared", directory("new-directory"))]);

        let (_, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, "shared");
        assert_eq!(conflicts[0].kind, ConflictKind::DivergentRename);
    }

    #[test]
    fn coalesced_directories_do_not_hide_same_file_conflicts() {
        let base = Manifest::default();
        let local = manifest(vec![
            (".agents", directory("local-agents")),
            (".agents/config.toml", file("local-config", 'a')),
        ]);
        let remote = manifest(vec![
            (".agents", directory("remote-agents")),
            (".agents/config.toml", file("remote-config", 'b')),
        ]);

        let (_, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, ".agents/config.toml");
        assert_eq!(conflicts[0].kind, ConflictKind::DivergentRename);
    }

    #[test]
    fn durability_skips_a_directory_removed_during_materialization() {
        let root = tempfile::tempdir().unwrap();
        let removed = root.path().join("removed");
        std::fs::create_dir(&removed).unwrap();
        std::fs::remove_dir(&removed).unwrap();
        let directories = [root.path().to_owned(), removed].into_iter().collect();

        sync_durable_directories(directories).unwrap();
    }
}
