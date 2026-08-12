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

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use serde::de::DeserializeOwned;

use crate::Error;
use crate::filesystem::{
    NamespaceNode, NamespaceRecord, NodeKind, OperationId, validate_portable_path,
};
use crate::managed::StreamRef;
use crate::workset::{Namespace, SpoolReader, Workspace};

use super::ConflictRecord;

pub(crate) struct ReconcilePlan {
    pub(crate) target: Namespace<Option<StreamRef>>,
    pub(crate) conflicts: Vec<ConflictRecord>,
    pub(crate) publish: bool,
}

pub(crate) fn changed_paths(
    base: &Namespace<StreamRef>,
    side: &Namespace<Option<StreamRef>>,
) -> Result<BTreeSet<String>, Error> {
    require_same_volume(base, side)?;
    let mut base = OrderedRecords::open(base)?;
    let mut side = OrderedRecords::open(side)?;
    let mut changed = BTreeSet::new();
    while let Some(path) = next_path(&base, &side, &EmptyRecords) {
        let base_record = base.take(&path)?;
        let side_record = side.take(&path)?;
        if !same_entry(base_record.as_ref(), side_record.as_ref()) {
            changed.insert(path);
        }
    }
    Ok(changed)
}

pub(crate) fn reconcile(
    common: &Namespace<StreamRef>,
    local: &Namespace<Option<StreamRef>>,
    remote: &Namespace<StreamRef>,
    resolved: &BTreeSet<String>,
) -> Result<ReconcilePlan, Error> {
    require_same_volume(common, local)?;
    require_same_volume(common, remote)?;
    if common.cursor.sequence() > remote.cursor.sequence() {
        return Err(Error::corrupt(
            "reconcile replica",
            "reconciliation ancestry is invalid",
        ));
    }

    let directory_conflicts = directory_conflicts(common, local, remote)?;
    let workspace = Workspace::create()?;
    let mut target = workspace.writer("reconciled-namespace")?;
    let mut common_records = OrderedRecords::open(common)?;
    let mut local_records = OrderedRecords::open(local)?;
    let mut remote_records = OrderedRecords::open(remote)?;
    let mut directory_conflicts = directory_conflicts.into_iter().peekable();
    let mut active_directories = Vec::<(String, bool)>::new();
    let mut conflicts = Vec::new();
    let mut resolved_conflicts = BTreeSet::new();
    let mut differs_from_remote = false;

    while let Some(path) = next_path(&common_records, &local_records, &remote_records) {
        active_directories
            .retain(|(directory, _)| path == *directory || is_descendant(directory, &path));
        while directory_conflicts
            .peek()
            .is_some_and(|directory| directory == &path)
        {
            let directory = directory_conflicts
                .next()
                .expect("peeked directory conflict");
            let is_resolved = resolved.contains(&directory);
            if is_resolved {
                resolved_conflicts.insert(directory.clone());
            }
            active_directories.push((directory, is_resolved));
        }

        let common_record = common_records.take(&path)?;
        let local_record = local_records.take(&path)?;
        let remote_record = remote_records.take(&path)?;
        let remote_comparison = remote_record.clone();
        let blocked = active_directories
            .iter()
            .any(|(_, is_resolved)| !is_resolved);
        let forced_local = !blocked && !active_directories.is_empty();

        let selected = if blocked {
            if active_directories
                .iter()
                .any(|(directory, is_resolved)| directory == &path && !is_resolved)
            {
                conflicts.push(conflict(
                    &path,
                    local_record.as_ref(),
                    remote_record.as_ref(),
                ));
            }
            None
        } else if forced_local {
            local_record.and_then(live_local_record)
        } else {
            let local_changed = !same_entry(common_record.as_ref(), local_record.as_ref());
            let remote_changed = !same_entry(common_record.as_ref(), remote_record.as_ref());
            match (local_changed, remote_changed) {
                (false, false) | (false, true) => remote_record.and_then(live_remote_record),
                (true, false) => local_record.and_then(live_local_record),
                (true, true) if same_entry(local_record.as_ref(), remote_record.as_ref()) => {
                    remote_record.and_then(live_remote_record)
                }
                (true, true) if resolved.contains(&path) => {
                    resolved_conflicts.insert(path.clone());
                    local_record.and_then(live_local_record)
                }
                (true, true) => {
                    conflicts.push(conflict(
                        &path,
                        local_record.as_ref(),
                        remote_record.as_ref(),
                    ));
                    None
                }
            }
        };

        if !same_entry(selected.as_ref(), remote_comparison.as_ref()) {
            differs_from_remote = true;
        }
        if let Some(record) = selected {
            target.write(&record)?;
        }
    }

    if resolved_conflicts != *resolved {
        let missing = resolved
            .difference(&resolved_conflicts)
            .cloned()
            .collect::<Vec<_>>();
        return Err(Error::invalid(
            "synchronize replica",
            format!("no unresolved conflict exists for {missing:?}"),
        ));
    }

    conflicts.sort_by(|left, right| left.path.cmp(&right.path));
    conflicts.dedup_by(|left, right| left.path == right.path);
    if !conflicts.is_empty() {
        return Ok(ReconcilePlan {
            target: map_remote(remote, &workspace)?,
            conflicts,
            publish: false,
        });
    }
    if !differs_from_remote {
        return Ok(ReconcilePlan {
            target: map_remote(remote, &workspace)?,
            conflicts,
            publish: false,
        });
    }

    let sequence = remote
        .cursor
        .sequence()
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .ok_or_else(|| Error::corrupt("reconcile replica", "Managed change sequence overflows"))?;
    Ok(ReconcilePlan {
        target: Namespace {
            volume_id: remote.volume_id,
            cursor: crate::filesystem::ChangeCursor::at(sequence, OperationId::generate()),
            root: remote.root,
            entries: target.finish()?,
        },
        conflicts,
        publish: true,
    })
}

fn directory_conflicts(
    common: &Namespace<StreamRef>,
    local: &Namespace<Option<StreamRef>>,
    remote: &Namespace<StreamRef>,
) -> Result<Vec<String>, Error> {
    let mut common = OrderedRecords::open(common)?;
    let mut local = OrderedRecords::open(local)?;
    let mut remote = OrderedRecords::open(remote)?;
    let mut pending = Vec::<DirectoryWatch>::new();
    let mut conflicts = Vec::new();

    while let Some(path) = next_path(&common, &local, &remote) {
        let mut retained = Vec::with_capacity(pending.len());
        for watch in pending.drain(..) {
            if path == watch.path || is_descendant(&watch.path, &path) {
                retained.push(watch);
            } else if watch.changed {
                conflicts.push(watch.path);
            }
        }
        pending = retained;

        let common_record = common.take(&path)?;
        let local_record = local.take(&path)?;
        let remote_record = remote.take(&path)?;
        let local_changed = !same_entry(common_record.as_ref(), local_record.as_ref());
        let remote_changed = !same_entry(common_record.as_ref(), remote_record.as_ref());
        for watch in &mut pending {
            if is_descendant(&watch.path, &path) {
                watch.changed |= match watch.side {
                    WatchedSide::Local => local_changed,
                    WatchedSide::Remote => remote_changed,
                };
            }
        }

        if kind(common_record.as_ref()) == Some(NodeKind::Directory) {
            let local_kept = kind(local_record.as_ref()) == Some(NodeKind::Directory);
            let remote_kept = kind(remote_record.as_ref()) == Some(NodeKind::Directory);
            if !local_kept && remote_kept {
                pending.push(DirectoryWatch {
                    path: path.clone(),
                    side: WatchedSide::Remote,
                    changed: false,
                });
            } else if local_kept && !remote_kept {
                pending.push(DirectoryWatch {
                    path,
                    side: WatchedSide::Local,
                    changed: false,
                });
            }
        }
    }
    conflicts.extend(
        pending
            .into_iter()
            .filter(|watch| watch.changed)
            .map(|watch| watch.path),
    );
    conflicts.sort();
    conflicts.dedup();
    Ok(conflicts)
}

#[derive(Clone, Copy)]
enum WatchedSide {
    Local,
    Remote,
}

struct DirectoryWatch {
    path: String,
    side: WatchedSide,
    changed: bool,
}

struct OrderedRecords<C> {
    reader: SpoolReader<NamespaceRecord<C>>,
    current: Option<NamespaceRecord<C>>,
    previous_path: Option<String>,
}

impl<C: DeserializeOwned> OrderedRecords<C> {
    fn open(namespace: &Namespace<C>) -> Result<Self, Error> {
        let mut records = Self {
            reader: namespace.reader()?,
            current: None,
            previous_path: None,
        };
        records.advance()?;
        Ok(records)
    }

    fn path(&self) -> Option<&str> {
        self.current.as_ref().map(|record| record.path.as_str())
    }

    fn take(&mut self, path: &str) -> Result<Option<NamespaceRecord<C>>, Error> {
        if self.path() != Some(path) {
            return Ok(None);
        }
        let record = self.current.take();
        self.advance()?;
        Ok(record)
    }

    fn advance(&mut self) -> Result<(), Error> {
        let next = self.reader.next()?;
        if let Some(record) = &next {
            validate_portable_path(&record.path)?;
            if self
                .previous_path
                .as_ref()
                .is_some_and(|previous| previous >= &record.path)
            {
                return Err(Error::corrupt(
                    "read filesystem namespace",
                    "namespace paths are not strictly ordered",
                ));
            }
            self.previous_path = Some(record.path.clone());
        }
        self.current = next;
        Ok(())
    }
}

struct EmptyRecords;

trait RecordPath {
    fn record_path(&self) -> Option<&str>;
}

impl<C: DeserializeOwned> RecordPath for OrderedRecords<C> {
    fn record_path(&self) -> Option<&str> {
        self.path()
    }
}

impl RecordPath for EmptyRecords {
    fn record_path(&self) -> Option<&str> {
        None
    }
}

fn next_path(
    first: &impl RecordPath,
    second: &impl RecordPath,
    third: &impl RecordPath,
) -> Option<String> {
    [
        first.record_path(),
        second.record_path(),
        third.record_path(),
    ]
    .into_iter()
    .flatten()
    .min()
    .map(str::to_owned)
}

fn same_entry<L, R>(left: Option<&NamespaceRecord<L>>, right: Option<&NamespaceRecord<R>>) -> bool {
    match (
        left.and_then(|record| record.value.as_ref()),
        right.and_then(|record| record.value.as_ref()),
    ) {
        (None, None) => true,
        (Some(left), Some(right)) => same_node(left, right),
        _ => false,
    }
}

fn same_node<L, R>(left: &NamespaceNode<L>, right: &NamespaceNode<R>) -> bool {
    left.node_id == right.node_id
        && left.attributes == right.attributes
        && match (left.file(), right.file()) {
            (None, None) => true,
            (Some((left, _, _)), Some((right, _, _))) => left == right,
            _ => false,
        }
}

fn kind<C>(record: Option<&NamespaceRecord<C>>) -> Option<NodeKind> {
    record.and_then(|record| record.value.as_ref().map(NamespaceNode::kind))
}

fn live_local_record(
    record: NamespaceRecord<Option<StreamRef>>,
) -> Option<NamespaceRecord<Option<StreamRef>>> {
    record.value.is_some().then_some(record)
}

fn live_remote_record(
    record: NamespaceRecord<StreamRef>,
) -> Option<NamespaceRecord<Option<StreamRef>>> {
    record.value.is_some().then(|| record.map_content(Some))
}

fn conflict<L, R>(
    path: &str,
    local: Option<&NamespaceRecord<L>>,
    remote: Option<&NamespaceRecord<R>>,
) -> ConflictRecord {
    ConflictRecord {
        path: path.to_owned(),
        local_digest: digest(local),
        remote_digest: digest(remote),
    }
}

fn digest<C>(record: Option<&NamespaceRecord<C>>) -> Option<crate::filesystem::Digest> {
    let (_, fingerprint, _) = record?.value.as_ref()?.file()?;
    Some(fingerprint.digest())
}

fn map_remote(
    remote: &Namespace<StreamRef>,
    workspace: &Workspace,
) -> Result<Namespace<Option<StreamRef>>, Error> {
    let mut source = OrderedRecords::open(remote)?;
    let mut entries = workspace.writer("remote-namespace")?;
    while let Some(path) = source.path().map(str::to_owned) {
        if let Some(record) = source.take(&path)?.and_then(live_remote_record) {
            entries.write(&record)?;
        }
    }
    Ok(Namespace {
        volume_id: remote.volume_id,
        cursor: remote.cursor,
        root: remote.root,
        entries: entries.finish()?,
    })
}

fn require_same_volume<L, R>(left: &Namespace<L>, right: &Namespace<R>) -> Result<(), Error> {
    if left.volume_id != right.volume_id || left.root != right.root {
        return Err(Error::corrupt(
            "reconcile replica",
            "reconciliation namespaces belong to different volumes",
        ));
    }
    Ok(())
}

fn is_descendant(directory: &str, path: &str) -> bool {
    if directory.is_empty() {
        return !path.is_empty();
    }
    path.strip_prefix(directory)
        .is_some_and(|suffix| suffix.starts_with('/'))
}
