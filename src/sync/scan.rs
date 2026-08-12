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

//! Merge a path-ordered local scan with the last common namespace.

use std::cmp::Ordering;
use std::path::Path;

use crate::Error;
use crate::filesystem::{
    ChangeCursor, FileVersionId, NamespaceNode, NamespaceRecord, NamespaceValue, NodeAttributes,
    NodeId, NodeKind,
};
use crate::managed::{self, StreamRef};
use crate::namespace::Namespace;
use crate::workset;

use super::local_scan::{self, LocalRecord};
use super::rename::RenameCandidates;

pub(crate) enum ScannedTree {
    Unchanged,
    Changed(Namespace<Option<StreamRef>>),
}

pub(crate) async fn scan(
    root: &Path,
    base: &Namespace<managed::StreamRef>,
    concurrency: usize,
    worksets: workset::WorksetOptions,
) -> Result<ScannedTree, Error> {
    let workspace = workset::Workspace::create(worksets)?;
    let local = local_scan::scan(&workspace, root, concurrency).await?;
    let next_cursor = base
        .cursor
        .sequence()
        .checked_add(1)
        .map(ChangeCursor::from_sequence);
    let record_cursor = next_cursor.unwrap_or(base.cursor);

    let mut output = workspace.writer("scanned-namespace")?;
    let mut renames = RenameCandidates::new(&workspace)?;
    let mut changed_directories = workspace.writer("changed-directories")?;
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
                write_parent(&mut changed_directories, &local.path)?;
                write_new_local(&mut output, &mut renames, local, record_cursor)?;
            }
            Ordering::Greater => {
                let base_record = base_head.take().expect("base namespace record exists");
                base_head = base_reader.next()?;
                changed = true;
                write_parent(&mut changed_directories, &base_record.path)?;
                renames.removed_record(base_record)?;
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
                    write_parent(&mut changed_directories, &path)?;
                    renames.removed_node(path.clone(), base_node)?;
                    write_new_local(&mut output, &mut renames, local, record_cursor)?;
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

    let renames = renames.resolve(&workspace, record_cursor)?;
    if !changed {
        return Ok(ScannedTree::Unchanged);
    }
    let cursor = next_cursor
        .ok_or_else(|| Error::corrupt("scan replica", "Managed change sequence overflows"))?;
    let entries = merge_path_records(&workspace, &output.finish()?, &renames)?;
    let changed_directories =
        workset::sort(&workspace, &changed_directories.finish()?, String::clone)?;
    let entries = advance_directory_generations(&workspace, base, &entries, &changed_directories)?;
    Ok(ScannedTree::Changed(Namespace {
        volume_id: base.volume_id,
        cursor,
        root: base.root,
        entries,
    }))
}

fn merge_path_records(
    workspace: &workset::Workspace,
    main: &workset::Spool<NamespaceRecord<Option<StreamRef>>>,
    renames: &workset::Spool<NamespaceRecord<Option<StreamRef>>>,
) -> Result<workset::Spool<NamespaceRecord<Option<StreamRef>>>, Error> {
    let mut main = main.reader()?;
    let mut renames = renames.reader()?;
    let mut left = main.next()?;
    let mut right = renames.next()?;
    let mut output = workspace.writer("scanned-namespace-ordered")?;
    while left.is_some() || right.is_some() {
        let ordering = match (&left, &right) {
            (Some(left), Some(right)) => left.path.cmp(&right.path),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => break,
        };
        let record = match ordering {
            Ordering::Less => {
                let record = left.take().expect("main namespace record exists");
                left = main.next()?;
                record
            }
            Ordering::Greater => {
                let record = right.take().expect("rename namespace record exists");
                right = renames.next()?;
                record
            }
            Ordering::Equal => {
                return Err(Error::corrupt(
                    "scan replica",
                    "one path has conflicting scan records",
                ));
            }
        };
        output.write(&record)?;
    }
    output.finish()
}

fn write_new_local(
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<StreamRef>>>,
    renames: &mut RenameCandidates,
    local: LocalRecord,
    cursor: ChangeCursor,
) -> Result<(), Error> {
    match local.kind {
        NodeKind::Directory => output.write(&NamespaceRecord {
            path: local.path,
            change_cursor: cursor,
            value: Some(new_directory()),
        }),
        NodeKind::RegularFile => renames.local(local),
    }
}

fn write_parent(output: &mut workset::SpoolWriter<String>, path: &str) -> Result<(), Error> {
    if path.is_empty() {
        return Ok(());
    }
    output.write(
        &path
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent)
            .to_owned(),
    )
}

fn advance_directory_generations(
    workspace: &workset::Workspace,
    base: &Namespace<StreamRef>,
    target: &workset::Spool<NamespaceRecord<Option<StreamRef>>>,
    changed_directories: &workset::Spool<String>,
) -> Result<workset::Spool<NamespaceRecord<Option<StreamRef>>>, Error> {
    let mut base = base.reader()?;
    let mut target = target.reader()?;
    let mut changed = changed_directories.reader()?;
    let mut base_record = base.next()?;
    let mut changed_path = changed.next()?;
    let mut output = workspace.writer("directory-generations")?;

    while let Some(mut record) = target.next()? {
        while base_record
            .as_ref()
            .is_some_and(|base| base.path < record.path)
        {
            base_record = base.next()?;
        }
        while changed_path
            .as_ref()
            .is_some_and(|changed| changed < &record.path)
        {
            let previous_changed = changed_path.take();
            changed_path = changed.next()?;
            while changed_path == previous_changed {
                changed_path = changed.next()?;
            }
        }
        if changed_path.as_deref() == Some(record.path.as_str())
            && let (Some(base), Some(node)) = (base_record.as_ref(), record.value.as_mut())
            && base.path == record.path
            && base
                .value
                .as_ref()
                .is_some_and(|base| base.node_id == node.node_id)
            && let NamespaceValue::Directory { generation } = &mut node.value
        {
            *generation = next_generation(*generation)?;
        }
        output.write(&record)?;
    }
    output.finish()
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

fn new_directory() -> NamespaceNode<Option<StreamRef>> {
    NamespaceNode {
        node_id: NodeId::generate(),
        generation: 1,
        attributes: NodeAttributes::default(),
        value: NamespaceValue::Directory { generation: 1 },
    }
}

pub(super) fn next_generation(generation: u64) -> Result<u64, Error> {
    generation
        .checked_add(1)
        .ok_or_else(|| Error::corrupt("scan replica", "node generation overflows"))
}
