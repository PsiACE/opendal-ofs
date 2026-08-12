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

//! Encoding, merging, and validation of path-ordered namespace streams.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use futures::StreamExt as _;
use opendal::Operator;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::{
    ChangeCursor, NamespaceNode, NamespaceRecord, NamespaceValue, NodeId, validate_portable_path,
};
use crate::namespace::Namespace;
use crate::workset::{MergeRuns, Spool, Workspace};

use super::data::FileDataRef;
use super::head::{ManagedVolume, NamespaceRevision};
use super::layout::{NamespaceChangeSegment, NamespaceCommit};
use super::object::{GcEpoch, ObjectClass, ObjectLocator};
use super::publication;
use super::stream::{self, RecordStreamReader, RecordStreamWriter, StreamKind, StreamRef};

pub(super) async fn write_genesis(
    operator: &Operator,
    root_node_id: NodeId,
    gc_epoch: GcEpoch,
) -> Result<StreamRef, Error> {
    let root = NamespaceRecord::<FileDataRef> {
        path: String::new(),
        value: Some(NamespaceNode {
            node_id: root_node_id,
            generation: 1,
            attributes: Default::default(),
            value: NamespaceValue::Directory { generation: 1 },
        }),
    };
    stream::write_records(
        operator,
        gc_epoch,
        ObjectClass::NamespaceSegment,
        StreamKind::NAMESPACE_SNAPSHOT,
        [root],
    )
    .await
}

impl ManagedVolume {
    pub(crate) async fn namespace(
        &self,
        revision: NamespaceRevision,
    ) -> Result<Namespace<FileDataRef>, Error> {
        let (head, _) = self.read_head().await?;
        if revision.change_cursor.sequence() < head.minimum_retained_cursor.sequence()
            || revision.change_cursor.sequence() > head.current_commit.change_cursor.sequence()
        {
            return Err(Error::invalid(
                "read Managed namespace",
                "requested change cursor is outside the retained interval",
            ));
        }
        let reference = if revision.change_cursor == head.minimum_retained_cursor
            && revision.object.locator.gc_epoch < head.current_commit.object.locator.gc_epoch
        {
            head.current_commit
        } else {
            revision
        };
        let commit = publication::read_commit(self, reference).await?;
        read(self, &commit, revision.change_cursor).await
    }
}

pub(super) async fn write_snapshot(
    volume: &ManagedVolume,
    namespace: &Namespace<FileDataRef>,
    gc_epoch: GcEpoch,
    mut visit_file: impl FnMut(ObjectLocator) -> Result<(), Error>,
) -> Result<StreamRef, Error> {
    let mut source = namespace.reader()?;
    let mut writer = RecordStreamWriter::open(
        &volume.operator,
        gc_epoch,
        ObjectClass::NamespaceSegment,
        StreamKind::NAMESPACE_SNAPSHOT,
    )
    .await?;
    while let Some(record) = source.next()? {
        if let Some(NamespaceNode {
            value: NamespaceValue::RegularFile { content, .. },
            ..
        }) = record.value.as_ref()
        {
            visit_file(content.object_locator())?;
        }
        writer.write(&record).await?;
    }
    writer.close().await
}

pub(super) async fn write_delta(
    volume: &ManagedVolume,
    previous: &Namespace<FileDataRef>,
    target: &Namespace<FileDataRef>,
    gc_epoch: GcEpoch,
) -> Result<Option<StreamRef>, Error> {
    let mut previous = previous.reader()?;
    let mut target_reader = target.reader()?;
    let mut left = previous.next()?;
    let mut right = target_reader.next()?;
    let mut writer = None;
    while left.is_some() || right.is_some() {
        let ordering = match (&left, &right) {
            (Some(left), Some(right)) => left.path.cmp(&right.path),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => break,
        };
        let (path, value) = match ordering {
            std::cmp::Ordering::Less => {
                let record = left.take().expect("left namespace record exists");
                left = previous.next()?;
                (record.path, None)
            }
            std::cmp::Ordering::Greater => {
                let record = right.take().expect("right namespace record exists");
                right = target_reader.next()?;
                (record.path, record.value)
            }
            std::cmp::Ordering::Equal => {
                let old = left.take().expect("left namespace record exists");
                let new = right.take().expect("right namespace record exists");
                left = previous.next()?;
                right = target_reader.next()?;
                if old.value == new.value {
                    continue;
                }
                (new.path, new.value)
            }
        };
        if writer.is_none() {
            writer = Some(
                RecordStreamWriter::open(
                    &volume.operator,
                    gc_epoch,
                    ObjectClass::NamespaceSegment,
                    StreamKind::NAMESPACE_CHANGES,
                )
                .await?,
            );
        }
        writer
            .as_mut()
            .expect("namespace delta writer is open")
            .write(&NamespaceRecord { path, value })
            .await?;
    }
    match writer {
        Some(writer) => writer.close().await.map(Some),
        None => Ok(None),
    }
}

pub(super) async fn merge_change_segments(
    volume: &ManagedVolume,
    older: NamespaceChangeSegment,
    newer: NamespaceChangeSegment,
    gc_epoch: GcEpoch,
) -> Result<NamespaceChangeSegment, Error> {
    for reference in [older.stream, newer.stream] {
        reference.require(StreamKind::NAMESPACE_CHANGES, ObjectClass::NamespaceSegment)?;
    }
    let mut left =
        RecordStreamReader::<NamespaceRecord<FileDataRef>>::open(&volume.operator, older.stream)
            .await?;
    let mut right =
        RecordStreamReader::<NamespaceRecord<FileDataRef>>::open(&volume.operator, newer.stream)
            .await?;
    let mut left_head = left.next().await?;
    let mut right_head = right.next().await?;
    let mut previous_left = None::<String>;
    let mut previous_right = None::<String>;
    let mut writer = RecordStreamWriter::open(
        &volume.operator,
        gc_epoch,
        ObjectClass::NamespaceSegment,
        StreamKind::NAMESPACE_CHANGES,
    )
    .await?;
    while left_head.is_some() || right_head.is_some() {
        if let Some(record) = left_head.as_ref() {
            require_increasing_path(&mut previous_left, &record.path)?;
        }
        if let Some(record) = right_head.as_ref() {
            require_increasing_path(&mut previous_right, &record.path)?;
        }
        let ordering = match (&left_head, &right_head) {
            (Some(left), Some(right)) => left.path.cmp(&right.path),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => break,
        };
        let selected = match ordering {
            Ordering::Less => {
                let record = left_head.take().expect("left change exists");
                previous_left = Some(record.path.clone());
                left_head = left.next().await?;
                record
            }
            Ordering::Greater => {
                let record = right_head.take().expect("right change exists");
                previous_right = Some(record.path.clone());
                right_head = right.next().await?;
                record
            }
            Ordering::Equal => {
                let left_record = left_head.take().expect("left change exists");
                let right_record = right_head.take().expect("right change exists");
                previous_left = Some(left_record.path.clone());
                previous_right = Some(right_record.path.clone());
                left_head = left.next().await?;
                right_head = right.next().await?;
                right_record
            }
        };
        writer.write(&selected).await?;
    }
    NamespaceChangeSegment::merged(older, newer, writer.close().await?)
}

pub(super) async fn read(
    volume: &ManagedVolume,
    commit: &NamespaceCommit,
    view_cursor: ChangeCursor,
) -> Result<Namespace<FileDataRef>, Error> {
    if view_cursor == commit.namespace_snapshot.change_cursor {
        return read_snapshot(volume, commit, view_cursor).await;
    }
    if view_cursor != commit.change_cursor {
        return Err(Error::corrupt(
            "read Managed namespace",
            "commit cursor does not match the requested view",
        ));
    }
    if commit.namespace_changes.is_empty() {
        return Err(Error::corrupt(
            "read Managed namespace",
            "namespace commit has no change stream for its cursor",
        ));
    }
    if commit.namespace_snapshot.change_cursor > view_cursor {
        return Err(Error::corrupt(
            "read Managed namespace",
            "snapshot is newer than the requested view",
        ));
    }
    let workspace = Workspace::create(volume.worksets)?;
    let mut streams = MergeRuns::new(workspace.merge_fan_in());
    streams.push(
        download_snapshot(
            volume,
            &workspace,
            commit.namespace_snapshot.stream,
            commit.namespace_snapshot.change_cursor,
        )
        .await?,
        |group| merge_group(&workspace, group, view_cursor),
    )?;
    let downloads = futures::stream::iter(commit.namespace_changes.iter().copied())
        .map(|segment| download_changes(volume, &workspace, segment, commit.change_cursor))
        .buffer_unordered(volume.stream_concurrency);
    futures::pin_mut!(downloads);
    while let Some(stream) = downloads.next().await {
        streams.push(stream?, |group| merge_group(&workspace, group, view_cursor))?;
    }
    let merged = streams
        .finish(|group| merge_group(&workspace, group, view_cursor))?
        .ok_or_else(|| Error::corrupt("read Managed namespace", "namespace has no streams"))?;
    finish_view(volume, commit, view_cursor, &workspace, merged)
}

async fn read_snapshot(
    volume: &ManagedVolume,
    commit: &NamespaceCommit,
    view_cursor: ChangeCursor,
) -> Result<Namespace<FileDataRef>, Error> {
    let reference = commit.namespace_snapshot.stream;
    reference.require(
        StreamKind::NAMESPACE_SNAPSHOT,
        ObjectClass::NamespaceSegment,
    )?;
    let workspace = Workspace::create(volume.worksets)?;
    let mut output = workspace.writer("namespace")?;
    let mut remote =
        RecordStreamReader::<NamespaceRecord<FileDataRef>>::open(&volume.operator, reference)
            .await?;
    let mut previous = None::<String>;
    let mut root_seen = false;
    while let Some(record) = remote.next().await? {
        validate_record(volume, &record, &mut previous, &mut root_seen)?;
        output.write(&record)?;
    }
    finish_namespace(volume, commit, view_cursor, output.finish()?, root_seen)
}

fn finish_view(
    volume: &ManagedVolume,
    commit: &NamespaceCommit,
    view_cursor: ChangeCursor,
    workspace: &Workspace,
    merged: Spool<VersionedRecord>,
) -> Result<Namespace<FileDataRef>, Error> {
    let mut output = workspace.writer("namespace")?;
    let mut records = merged.reader()?;
    let mut previous_output = None::<String>;
    let mut root_seen = false;
    while let Some(record) = records.next()? {
        let Some(node) = record.value else {
            continue;
        };
        let record = NamespaceRecord {
            path: record.path,
            value: Some(node),
        };
        validate_record(volume, &record, &mut previous_output, &mut root_seen)?;
        output.write(&record)?;
    }
    finish_namespace(volume, commit, view_cursor, output.finish()?, root_seen)
}

fn finish_namespace(
    volume: &ManagedVolume,
    commit: &NamespaceCommit,
    view_cursor: ChangeCursor,
    entries: Spool<NamespaceRecord<FileDataRef>>,
    root_seen: bool,
) -> Result<Namespace<FileDataRef>, Error> {
    if !root_seen {
        return Err(Error::corrupt(
            "read Managed namespace",
            "namespace root is missing",
        ));
    }
    Ok(Namespace {
        volume_id: commit.volume_id,
        cursor: view_cursor,
        root: volume.format.root_node_id(),
        entries,
    })
}

async fn download_snapshot(
    volume: &ManagedVolume,
    workspace: &Workspace,
    reference: StreamRef,
    cursor: ChangeCursor,
) -> Result<Spool<VersionedRecord>, Error> {
    reference.require(
        StreamKind::NAMESPACE_SNAPSHOT,
        ObjectClass::NamespaceSegment,
    )?;
    let mut remote =
        RecordStreamReader::<NamespaceRecord<FileDataRef>>::open(&volume.operator, reference)
            .await?;
    let mut local = workspace.writer("namespace-input")?;
    let mut previous = None::<String>;
    while let Some(record) = remote.next().await? {
        require_increasing_path(&mut previous, &record.path)?;
        previous = Some(record.path.clone());
        local.write(&VersionedRecord {
            path: record.path,
            change_cursor: cursor,
            value: record.value,
        })?;
    }
    local.finish()
}

async fn download_changes(
    volume: &ManagedVolume,
    workspace: &Workspace,
    segment: NamespaceChangeSegment,
    commit_cursor: ChangeCursor,
) -> Result<Spool<VersionedRecord>, Error> {
    let reference = segment.stream;
    reference.require(StreamKind::NAMESPACE_CHANGES, ObjectClass::NamespaceSegment)?;
    if segment.end_cursor > commit_cursor || segment.source_bytes == 0 {
        return Err(Error::corrupt(
            "read Managed namespace",
            "namespace change descriptor is invalid",
        ));
    }
    let mut remote =
        RecordStreamReader::<NamespaceRecord<FileDataRef>>::open(&volume.operator, reference)
            .await?;
    let mut local = workspace.writer("namespace-input")?;
    let mut previous = None::<String>;
    while let Some(record) = remote.next().await? {
        require_increasing_path(&mut previous, &record.path)?;
        previous = Some(record.path.clone());
        local.write(&VersionedRecord {
            path: record.path,
            change_cursor: segment.end_cursor,
            value: record.value,
        })?;
    }
    local.finish()
}

fn merge_group(
    workspace: &Workspace,
    streams: &[Spool<VersionedRecord>],
    view_cursor: ChangeCursor,
) -> Result<Spool<VersionedRecord>, Error> {
    let mut readers = streams
        .iter()
        .map(Spool::reader)
        .collect::<Result<Vec<_>, Error>>()?;
    let mut heap = BinaryHeap::new();
    for (source, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = reader.next()? {
            heap.push(NamespaceItem { record, source });
        }
    }
    let mut output = workspace.writer("namespace-merge")?;

    while let Some(first) = heap.pop() {
        let path = first.record.path.clone();
        let mut selected = None::<VersionedRecord>;
        let mut item = Some(first);
        loop {
            let NamespaceItem { record, source } = item.take().expect("namespace item exists");
            if record.change_cursor <= view_cursor {
                match selected.as_ref() {
                    Some(current) if current.change_cursor == record.change_cursor => {
                        if current.value != record.value {
                            return Err(Error::corrupt(
                                "read Managed namespace",
                                "one change cursor has conflicting namespace records",
                            ));
                        }
                    }
                    Some(current) if current.change_cursor > record.change_cursor => {}
                    _ => selected = Some(record),
                }
            }
            if let Some(record) = readers[source].next()? {
                heap.push(NamespaceItem { record, source });
            }
            if heap.peek().is_none_or(|next| next.record.path != path) {
                break;
            }
            item = heap.pop();
        }
        if let Some(record) = selected {
            output.write(&record)?;
        }
    }
    output.finish()
}

struct NamespaceItem {
    record: VersionedRecord,
    source: usize,
}

impl PartialEq for NamespaceItem {
    fn eq(&self, other: &Self) -> bool {
        self.record.path == other.record.path && self.source == other.source
    }
}

impl Eq for NamespaceItem {}

impl PartialOrd for NamespaceItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NamespaceItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .path
            .cmp(&self.record.path)
            .then_with(|| other.source.cmp(&self.source))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct VersionedRecord {
    path: String,
    change_cursor: ChangeCursor,
    value: Option<NamespaceNode<FileDataRef>>,
}

fn validate_record(
    volume: &ManagedVolume,
    record: &NamespaceRecord<FileDataRef>,
    previous: &mut Option<String>,
    root_seen: &mut bool,
) -> Result<(), Error> {
    require_increasing_path(previous, &record.path)?;
    let node = record
        .value
        .as_ref()
        .ok_or_else(|| Error::corrupt("read Managed namespace", "snapshot contains a deletion"))?;
    validate_portable_path(&record.path)?;
    validate_content(node)?;
    if record.path.is_empty() {
        if node.node_id != volume.format.root_node_id()
            || !matches!(node.value, NamespaceValue::Directory { .. })
        {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace root is invalid",
            ));
        }
        *root_seen = true;
    }
    *previous = Some(record.path.clone());
    Ok(())
}

fn require_increasing_path(previous: &mut Option<String>, path: &str) -> Result<(), Error> {
    if previous
        .as_ref()
        .is_some_and(|previous| previous.as_str() >= path)
    {
        return Err(Error::corrupt(
            "read Managed namespace",
            "namespace stream is not strictly path ordered",
        ));
    }
    Ok(())
}

fn validate_content(node: &NamespaceNode<FileDataRef>) -> Result<(), Error> {
    if let NamespaceValue::RegularFile {
        fingerprint,
        content,
        ..
    } = node.value
        && content.validate(fingerprint).is_err()
    {
        return Err(Error::corrupt(
            "read Managed namespace",
            "file content does not match its namespace record",
        ));
    }
    Ok(())
}
