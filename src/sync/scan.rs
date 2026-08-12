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

use std::cmp::Ordering;
use std::fs::ReadDir;
use std::path::{Path, PathBuf};

use futures::stream::{FuturesUnordered, StreamExt as _};
use serde::{Deserialize, Serialize};
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use super::transfer::inspect_file;
use crate::Error;
use crate::filesystem::{
    ChangeCursor, FileFingerprint, FileVersionId, NamespaceNode, NamespaceRecord, NamespaceValue,
    NodeAttributes, NodeId, NodeKind, validate_portable_path,
};
use crate::managed::{self, StreamRef};
use crate::workset;

pub(crate) enum ScannedTree {
    Unchanged,
    Changed(workset::Namespace<Option<StreamRef>>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalRecord {
    path: String,
    kind: NodeKind,
    executable: bool,
    fingerprint: Option<FileFingerprint>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PortableName {
    parent: String,
    folded: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalRenameCandidate {
    path: String,
    executable: bool,
    fingerprint: FileFingerprint,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BaseRenameCandidate {
    path: String,
    fingerprint: FileFingerprint,
    node: NamespaceNode<StreamRef>,
}

struct DirectoryScan {
    path: PathBuf,
    relative: String,
    children: ReadDir,
}

pub(crate) async fn scan(
    root: &Path,
    base: &workset::Namespace<managed::StreamRef>,
    concurrency: usize,
) -> Result<ScannedTree, Error> {
    let workspace = workset::Workspace::create()?;
    let local = scan_local(&workspace, root, concurrency).await?;
    let next_cursor = base
        .cursor
        .sequence()
        .checked_add(1)
        .map(ChangeCursor::from_sequence);
    let record_cursor = next_cursor.unwrap_or(base.cursor);

    let mut output = workspace.writer("scanned-namespace")?;
    let mut local_candidates = workspace.writer("local-rename-candidates")?;
    let mut base_candidates = workspace.writer("base-rename-candidates")?;
    let mut local_reader = local.reader()?;
    let mut base_reader = base.reader()?;
    let mut local_head = local_reader.next()?;
    let mut base_head = base_reader.next()?;
    let mut changed = false;
    let mut root_seen = false;

    while local_head.is_some() || base_head.is_some() {
        let ordering = match (&local_head, &base_head) {
            (Some(local), Some(base)) => local.path.cmp(&base.path),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => break,
        };
        match ordering {
            Ordering::Less => {
                let local = local_head.take().expect("local scan record exists");
                local_head = local_reader.next()?;
                changed = true;
                queue_new_local(&mut output, &mut local_candidates, local, record_cursor)?;
            }
            Ordering::Greater => {
                let base_record = base_head.take().expect("base namespace record exists");
                base_head = base_reader.next()?;
                changed = true;
                queue_removed_base(&mut base_candidates, base_record)?;
            }
            Ordering::Equal => {
                let local = local_head.take().expect("local scan record exists");
                let base_record = base_head.take().expect("base namespace record exists");
                local_head = local_reader.next()?;
                base_head = base_reader.next()?;
                let path = local.path.clone();
                let base_node = base_record.value.ok_or_else(|| {
                    Error::corrupt("scan replica", "base namespace contains a deletion")
                })?;
                if path.is_empty() {
                    if base_node.node_id != base.root || base_node.kind() != NodeKind::Directory {
                        return Err(Error::corrupt(
                            "scan replica",
                            "base namespace root is invalid",
                        ));
                    }
                    root_seen = true;
                }
                if local.kind == base_node.kind() {
                    changed |= !same_base_entry(&local, &base_node);
                    let node = reuse_same_path(&local, base_node)?;
                    output.write(&NamespaceRecord {
                        path,
                        change_cursor: record_cursor,
                        value: Some(node),
                    })?;
                } else {
                    changed = true;
                    queue_removed_node(&mut base_candidates, path.clone(), base_node)?;
                    queue_new_local(&mut output, &mut local_candidates, local, record_cursor)?;
                }
            }
        }
    }
    if !root_seen {
        return Err(Error::corrupt(
            "scan replica",
            "base namespace root is missing",
        ));
    }

    let local_candidates = workset::sort(
        &workspace,
        &local_candidates.finish()?,
        |candidate: &LocalRenameCandidate| (candidate.fingerprint, candidate.path.clone()),
    )?;
    let base_candidates = workset::sort(
        &workspace,
        &base_candidates.finish()?,
        |candidate: &BaseRenameCandidate| (candidate.fingerprint, candidate.path.clone()),
    )?;
    resolve_file_renames(
        &local_candidates,
        &base_candidates,
        &mut output,
        record_cursor,
    )?;

    if !changed {
        return Ok(ScannedTree::Unchanged);
    }
    let cursor = next_cursor
        .ok_or_else(|| Error::corrupt("scan replica", "Managed change sequence overflows"))?;
    let entries = workset::sort(
        &workspace,
        &output.finish()?,
        |record: &NamespaceRecord<Option<StreamRef>>| record.path.clone(),
    )?;
    Ok(ScannedTree::Changed(workset::Namespace {
        volume_id: base.volume_id,
        cursor,
        root: base.root,
        entries,
    }))
}

async fn scan_local(
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
            if inspections.len() >= concurrency.max(1) {
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

fn reuse_same_path(
    local: &LocalRecord,
    base: NamespaceNode<StreamRef>,
) -> Result<NamespaceNode<Option<StreamRef>>, Error> {
    let attributes = NodeAttributes {
        executable: local.executable,
    };
    match base.value {
        NamespaceValue::Directory { generation } => Ok(NamespaceNode {
            node_id: base.node_id,
            generation: if base.attributes == attributes {
                base.generation
            } else {
                next_generation(base.generation)?
            },
            attributes,
            value: NamespaceValue::Directory { generation },
        }),
        NamespaceValue::RegularFile {
            version,
            fingerprint,
            content,
        } => {
            let local_fingerprint = local
                .fingerprint
                .expect("a local regular file has a fingerprint");
            let unchanged_content = fingerprint == local_fingerprint;
            Ok(NamespaceNode {
                node_id: base.node_id,
                generation: if unchanged_content && base.attributes == attributes {
                    base.generation
                } else {
                    next_generation(base.generation)?
                },
                attributes,
                value: if unchanged_content {
                    NamespaceValue::RegularFile {
                        version,
                        fingerprint,
                        content: Some(content),
                    }
                } else {
                    NamespaceValue::RegularFile {
                        version: FileVersionId::generate(),
                        fingerprint: local_fingerprint,
                        content: None,
                    }
                },
            })
        }
    }
}

fn same_base_entry(local: &LocalRecord, node: &NamespaceNode<StreamRef>) -> bool {
    if node.kind() != local.kind || node.attributes.executable != local.executable {
        return false;
    }
    match &node.value {
        NamespaceValue::Directory { .. } => local.kind == NodeKind::Directory,
        NamespaceValue::RegularFile { fingerprint, .. } => {
            local.kind == NodeKind::RegularFile && local.fingerprint == Some(*fingerprint)
        }
    }
}

fn queue_new_local(
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<StreamRef>>>,
    candidates: &mut workset::SpoolWriter<LocalRenameCandidate>,
    local: LocalRecord,
    cursor: ChangeCursor,
) -> Result<(), Error> {
    match local.kind {
        NodeKind::Directory => output.write(&NamespaceRecord {
            path: local.path,
            change_cursor: cursor,
            value: Some(new_directory()),
        }),
        NodeKind::RegularFile => candidates.write(&LocalRenameCandidate {
            path: local.path,
            executable: local.executable,
            fingerprint: local
                .fingerprint
                .expect("a local regular file has a fingerprint"),
        }),
    }
}

fn queue_removed_base(
    candidates: &mut workset::SpoolWriter<BaseRenameCandidate>,
    record: NamespaceRecord<StreamRef>,
) -> Result<(), Error> {
    let node = record
        .value
        .ok_or_else(|| Error::corrupt("scan replica", "base namespace contains a deletion"))?;
    queue_removed_node(candidates, record.path, node)
}

fn queue_removed_node(
    candidates: &mut workset::SpoolWriter<BaseRenameCandidate>,
    path: String,
    node: NamespaceNode<StreamRef>,
) -> Result<(), Error> {
    let Some((_, fingerprint, _)) = node.file() else {
        return Ok(());
    };
    candidates.write(&BaseRenameCandidate {
        path,
        fingerprint,
        node,
    })
}

fn resolve_file_renames(
    local: &workset::Spool<LocalRenameCandidate>,
    base: &workset::Spool<BaseRenameCandidate>,
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<StreamRef>>>,
    cursor: ChangeCursor,
) -> Result<(), Error> {
    let mut local_reader = local.reader()?;
    let mut base_reader = base.reader()?;
    let mut local_head = local_reader.next()?;
    let mut base_head = base_reader.next()?;
    while local_head.is_some() || base_head.is_some() {
        let ordering = match (&local_head, &base_head) {
            (Some(local), Some(base)) => local.fingerprint.cmp(&base.fingerprint),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => break,
        };
        match ordering {
            Ordering::Less => {
                let fingerprint = local_head
                    .as_ref()
                    .expect("local rename candidate exists")
                    .fingerprint;
                if let Some(local) = take_local_group(
                    fingerprint,
                    &mut local_reader,
                    &mut local_head,
                    output,
                    cursor,
                )? {
                    write_new_file(output, local, cursor)?;
                }
            }
            Ordering::Greater => {
                let fingerprint = base_head
                    .as_ref()
                    .expect("base rename candidate exists")
                    .fingerprint;
                take_base_group(fingerprint, &mut base_reader, &mut base_head)?;
            }
            Ordering::Equal => {
                let fingerprint = local_head
                    .as_ref()
                    .expect("local rename candidate exists")
                    .fingerprint;
                let local = take_local_group(
                    fingerprint,
                    &mut local_reader,
                    &mut local_head,
                    output,
                    cursor,
                )?;
                let base = take_base_group(fingerprint, &mut base_reader, &mut base_head)?;
                match (local, base) {
                    (Some(local), Some(base)) => write_renamed_file(output, local, base, cursor)?,
                    (Some(local), None) => write_new_file(output, local, cursor)?,
                    (None, _) => {}
                }
            }
        }
    }
    Ok(())
}

fn take_local_group(
    fingerprint: FileFingerprint,
    reader: &mut workset::SpoolReader<LocalRenameCandidate>,
    head: &mut Option<LocalRenameCandidate>,
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<StreamRef>>>,
    cursor: ChangeCursor,
) -> Result<Option<LocalRenameCandidate>, Error> {
    let first = head.take().expect("local rename candidate exists");
    *head = reader.next()?;
    if head
        .as_ref()
        .is_none_or(|candidate| candidate.fingerprint != fingerprint)
    {
        return Ok(Some(first));
    }
    write_new_file(output, first, cursor)?;
    while head
        .as_ref()
        .is_some_and(|candidate| candidate.fingerprint == fingerprint)
    {
        let candidate = head.take().expect("local rename candidate exists");
        write_new_file(output, candidate, cursor)?;
        *head = reader.next()?;
    }
    Ok(None)
}

fn take_base_group(
    fingerprint: FileFingerprint,
    reader: &mut workset::SpoolReader<BaseRenameCandidate>,
    head: &mut Option<BaseRenameCandidate>,
) -> Result<Option<BaseRenameCandidate>, Error> {
    let first = head.take().expect("base rename candidate exists");
    *head = reader.next()?;
    if head
        .as_ref()
        .is_none_or(|candidate| candidate.fingerprint != fingerprint)
    {
        return Ok(Some(first));
    }
    while head
        .as_ref()
        .is_some_and(|candidate| candidate.fingerprint == fingerprint)
    {
        *head = reader.next()?;
    }
    Ok(None)
}

fn write_new_file(
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<StreamRef>>>,
    local: LocalRenameCandidate,
    cursor: ChangeCursor,
) -> Result<(), Error> {
    output.write(&NamespaceRecord {
        path: local.path,
        change_cursor: cursor,
        value: Some(NamespaceNode {
            node_id: NodeId::generate(),
            generation: 1,
            attributes: NodeAttributes {
                executable: local.executable,
            },
            value: NamespaceValue::RegularFile {
                version: FileVersionId::generate(),
                fingerprint: local.fingerprint,
                content: None,
            },
        }),
    })
}

fn write_renamed_file(
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<StreamRef>>>,
    local: LocalRenameCandidate,
    base: BaseRenameCandidate,
    cursor: ChangeCursor,
) -> Result<(), Error> {
    let BaseRenameCandidate { node, .. } = base;
    let NamespaceValue::RegularFile {
        version,
        fingerprint,
        content,
    } = node.value
    else {
        return Err(Error::corrupt(
            "scan replica",
            "rename candidate is not a regular file",
        ));
    };
    let attributes = NodeAttributes {
        executable: local.executable,
    };
    let generation = if attributes == node.attributes {
        node.generation
    } else {
        next_generation(node.generation)?
    };
    output.write(&NamespaceRecord {
        path: local.path,
        change_cursor: cursor,
        value: Some(NamespaceNode {
            node_id: node.node_id,
            generation,
            attributes,
            value: NamespaceValue::RegularFile {
                version,
                fingerprint,
                content: Some(content),
            },
        }),
    })
}

fn new_directory() -> NamespaceNode<Option<StreamRef>> {
    NamespaceNode {
        node_id: NodeId::generate(),
        generation: 1,
        attributes: NodeAttributes::default(),
        value: NamespaceValue::Directory { generation: 1 },
    }
}

fn next_generation(generation: u64) -> Result<u64, Error> {
    generation
        .checked_add(1)
        .ok_or_else(|| Error::corrupt("scan replica", "node generation overflows"))
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
