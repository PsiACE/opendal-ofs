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

use crate::Error;
use crate::filesystem::{
    ChangeCursor, NamespaceNode, NamespaceRecord, NamespaceValue, NodeId, validate_portable_path,
};
use crate::namespace::Namespace;
use crate::workset::{MergeRuns, Spool, Workspace};

use super::head::{ManagedVolume, NamespaceRevision};
use super::object::{GcEpoch, ObjectClass, ObjectRef};
use super::publication::{self, NamespaceCommit};
use super::stream::{self, RecordStreamReader, RecordStreamWriter, StreamKind, StreamRef};

pub(super) async fn write_genesis(
    operator: &Operator,
    root_node_id: NodeId,
    gc_epoch: GcEpoch,
) -> Result<StreamRef, Error> {
    let root = NamespaceRecord::<StreamRef> {
        path: String::new(),
        change_cursor: ChangeCursor::GENESIS,
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
        StreamKind::NAMESPACE_RECORDS,
        [root],
    )
    .await
}

impl ManagedVolume {
    pub(crate) async fn namespace(
        &self,
        revision: NamespaceRevision,
    ) -> Result<Namespace<StreamRef>, Error> {
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
            && revision.object.gc_epoch < head.current_commit.object.gc_epoch
        {
            head.current_commit
        } else {
            revision
        };
        let commit = publication::read_commit(self, reference).await?;
        read(self, &commit, revision.change_cursor).await
    }
}

pub(super) async fn write_full(
    volume: &ManagedVolume,
    namespace: &Namespace<StreamRef>,
    gc_epoch: GcEpoch,
) -> Result<StreamRef, Error> {
    write_full_visiting(volume, namespace, gc_epoch, |_| Ok(())).await
}

pub(super) async fn write_full_visiting(
    volume: &ManagedVolume,
    namespace: &Namespace<StreamRef>,
    gc_epoch: GcEpoch,
    mut visit_file: impl FnMut(ObjectRef) -> Result<(), Error>,
) -> Result<StreamRef, Error> {
    let mut source = namespace.reader()?;
    let mut writer = RecordStreamWriter::open(
        &volume.operator,
        gc_epoch,
        ObjectClass::NamespaceSegment,
        StreamKind::NAMESPACE_RECORDS,
    )
    .await?;
    while let Some(record) = source.next()? {
        if let Some(NamespaceNode {
            value: NamespaceValue::RegularFile { content, .. },
            ..
        }) = record.value.as_ref()
        {
            visit_file(content.object)?;
        }
        writer.write(&record).await?;
    }
    writer.close().await
}

pub(super) async fn write_delta(
    volume: &ManagedVolume,
    previous: &Namespace<StreamRef>,
    target: &Namespace<StreamRef>,
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
                    StreamKind::NAMESPACE_RECORDS,
                )
                .await?,
            );
        }
        writer
            .as_mut()
            .expect("namespace delta writer is open")
            .write(&NamespaceRecord {
                path,
                change_cursor: target.cursor,
                value,
            })
            .await?;
    }
    match writer {
        Some(writer) => writer.close().await.map(Some),
        None => Ok(None),
    }
}

pub(super) async fn read(
    volume: &ManagedVolume,
    commit: &NamespaceCommit,
    view_cursor: ChangeCursor,
) -> Result<Namespace<StreamRef>, Error> {
    let workspace = Workspace::create(volume.worksets)?;
    let mut output = workspace.writer("namespace")?;
    let mut streams = MergeRuns::new(workspace.merge_fan_in());
    let downloads = futures::stream::iter(commit.namespace_streams())
        .map(|reference| download(volume, &workspace, reference, commit.change_cursor))
        .buffer_unordered(volume.stream_concurrency);
    futures::pin_mut!(downloads);
    while let Some(stream) = downloads.next().await {
        streams.push(stream?, |group| merge_group(&workspace, group, view_cursor))?;
    }
    let merged = streams
        .finish(|group| merge_group(&workspace, group, view_cursor))?
        .ok_or_else(|| Error::corrupt("read Managed namespace", "namespace has no streams"))?;
    let mut records = merged.reader()?;
    let mut previous_output = None::<String>;
    let mut root_seen = false;
    while let Some(record) = records.next()? {
        if record.change_cursor > view_cursor {
            continue;
        }
        let Some(node) = record.value.as_ref() else {
            continue;
        };
        validate_portable_path(&record.path)?;
        if previous_output
            .as_ref()
            .is_some_and(|previous| previous >= &record.path)
        {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace view is not strictly path ordered",
            ));
        }
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
            root_seen = true;
        }
        previous_output = Some(record.path.clone());
        output.write(&record)?;
    }
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
        entries: output.finish()?,
    })
}

async fn download(
    volume: &ManagedVolume,
    workspace: &Workspace,
    reference: StreamRef,
    commit_cursor: ChangeCursor,
) -> Result<Spool<NamespaceRecord<StreamRef>>, Error> {
    require_stream(
        reference,
        StreamKind::NAMESPACE_RECORDS,
        ObjectClass::NamespaceSegment,
    )?;
    let mut remote =
        RecordStreamReader::<NamespaceRecord<StreamRef>>::open(&volume.operator, reference).await?;
    let mut local = workspace.writer("namespace-input")?;
    let mut previous = None::<String>;
    while let Some(record) = remote.next().await? {
        if previous
            .as_ref()
            .is_some_and(|previous| previous >= &record.path)
        {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace stream is not strictly path ordered",
            ));
        }
        if record.change_cursor.sequence() > commit_cursor.sequence() {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace record is newer than its commit",
            ));
        }
        previous = Some(record.path.clone());
        local.write(&record)?;
    }
    local.finish()
}

fn merge_group(
    workspace: &Workspace,
    streams: &[Spool<NamespaceRecord<StreamRef>>],
    view_cursor: ChangeCursor,
) -> Result<Spool<NamespaceRecord<StreamRef>>, Error> {
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
        let mut selected = None::<NamespaceRecord<StreamRef>>;
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
    record: NamespaceRecord<StreamRef>,
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

fn validate_content(node: &NamespaceNode<StreamRef>) -> Result<(), Error> {
    if let NamespaceValue::RegularFile {
        fingerprint,
        content,
        ..
    } = node.value
        && (content.kind != StreamKind::FILE_BYTES
            || content.object.class != ObjectClass::FileData
            || content.payload_length != fingerprint.logical_length()
            || content.payload_digest != fingerprint.digest())
    {
        return Err(Error::corrupt(
            "read Managed namespace",
            "file content does not match its namespace record",
        ));
    }
    Ok(())
}

pub(super) fn require_stream(
    reference: StreamRef,
    kind: StreamKind,
    class: ObjectClass,
) -> Result<(), Error> {
    if reference.kind != kind || reference.object.class != class {
        return Err(Error::corrupt(
            "read Managed namespace",
            "stream reference has the wrong type",
        ));
    }
    Ok(())
}
