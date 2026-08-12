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

use crate::filesystem::{
    ChangeCursor, NamespaceNode, NamespaceRecord, NamespaceValue, OperationId, VolumeId,
    validate_portable_path,
};
use crate::workset::{Namespace, Spool, WorksetOptions, Workspace, balanced_merge};
use crate::{Error, ErrorKind};
use futures::StreamExt as _;
use opendal::Operator;

use super::format::ManagedFormat;
use super::object::{self, GcEpoch, ObjectClass, ObjectRef};
use super::record::Record;
use super::stream::{self, RecordStreamReader, RecordStreamWriter, StreamKind, StreamRef};

const HEAD_KEY: &str = "managed/1/head";
const HEAD_RECORD: Record = Record::new(*b"OFSHEAD1", 64 * 1024);
const COMMIT_RECORD: Record = Record::new(*b"OFSCMIT1", 4 * 1024 * 1024);

#[derive(Clone)]
pub struct ManagedVolume {
    format: ManagedFormat,
    operator: Operator,
    stream_concurrency: usize,
    worksets: WorksetOptions,
}

pub(crate) struct ManagedObservation {
    pub(crate) namespace: Namespace<StreamRef>,
    head_revision: String,
    namespace_revision: NamespaceRevision,
    reclamation_watermark: ChangeCursor,
    gc_epoch: GcEpoch,
    commit: NamespaceCommit,
}

impl ManagedObservation {
    pub(crate) const fn revision(&self) -> NamespaceRevision {
        self.namespace_revision
    }

    pub(crate) const fn maintenance_generation(&self) -> u64 {
        self.gc_epoch.value()
    }

    pub(crate) const fn accepts_prepared(&self, gc_epoch: u64) -> bool {
        gc_epoch == self.gc_epoch.value()
    }

    pub(crate) fn can_read_revision(&self, revision: NamespaceRevision) -> bool {
        let sequence = revision.change_cursor.sequence();
        let current = self.namespace_revision.change_cursor.sequence();
        sequence >= self.reclamation_watermark.sequence() && sequence <= current
    }

    pub(crate) const fn gc_epoch(&self) -> GcEpoch {
        self.gc_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Head {
    pub(super) current_commit: NamespaceRevision,
    pub(super) gc_epoch: GcEpoch,
    pub(super) minimum_retained_cursor: ChangeCursor,
}
super::wire::tuple_wire!(Head {
    current_commit: NamespaceRevision,
    gc_epoch: GcEpoch,
    minimum_retained_cursor: ChangeCursor,
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceRevision {
    object: ObjectRef,
    change_cursor: ChangeCursor,
}
super::wire::tuple_wire!(NamespaceRevision {
    object: ObjectRef,
    change_cursor: ChangeCursor,
});

impl NamespaceRevision {
    pub const fn cursor(self) -> ChangeCursor {
        self.change_cursor
    }
}

#[derive(Clone, Debug)]
struct NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    namespace: Vec<StreamRef>,
    operation_results: Vec<StreamRef>,
}
super::wire::tuple_wire!(NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    namespace: Vec<StreamRef>,
    operation_results: Vec<StreamRef>,
});

impl NamespaceCommit {
    fn genesis(volume_id: VolumeId, namespace: StreamRef) -> Self {
        Self {
            volume_id,
            change_cursor: ChangeCursor::GENESIS,
            namespace: vec![namespace],
            operation_results: Vec::new(),
        }
    }

    fn streams(&self) -> impl Iterator<Item = StreamRef> + '_ {
        self.namespace
            .iter()
            .chain(&self.operation_results)
            .copied()
    }
}

#[derive(Clone, Copy, Debug)]
struct OperationRecord {
    operation_id: OperationId,
    change_cursor: ChangeCursor,
}
super::wire::tuple_wire!(OperationRecord {
    operation_id: OperationId,
    change_cursor: ChangeCursor,
});

impl ManagedVolume {
    pub(super) const fn new(
        format: ManagedFormat,
        operator: Operator,
        stream_concurrency: usize,
        worksets: WorksetOptions,
    ) -> Self {
        Self {
            format,
            operator,
            stream_concurrency,
            worksets,
        }
    }

    pub const fn id(&self) -> VolumeId {
        self.format.volume_id()
    }

    pub(super) async fn initialize(&self) -> Result<(), Error> {
        if object::read_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .is_some()
        {
            return self.observe().await.map(drop);
        }

        let root = NamespaceRecord::<StreamRef> {
            path: String::new(),
            change_cursor: ChangeCursor::GENESIS,
            value: Some(NamespaceNode {
                node_id: self.format.root_node_id(),
                generation: 1,
                attributes: Default::default(),
                value: NamespaceValue::Directory { generation: 1 },
            }),
        };
        let namespace = stream::write_records(
            &self.operator,
            GcEpoch::ZERO,
            ObjectClass::NamespaceSegment,
            StreamKind::NAMESPACE_RECORDS,
            [root],
        )
        .await?;
        let commit = NamespaceCommit::genesis(self.id(), namespace);
        let revision = self.write_commit(GcEpoch::ZERO, &commit).await?;
        let head = Head {
            current_commit: revision,
            gc_epoch: GcEpoch::ZERO,
            minimum_retained_cursor: ChangeCursor::GENESIS,
        };
        if object::write_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.encode(&head)?,
            object::ControlCondition::Missing,
        )
        .await?
        {
            Ok(())
        } else {
            self.observe().await.map(drop)
        }
    }

    pub(crate) async fn observe(&self) -> Result<ManagedObservation, Error> {
        let (head, head_revision) = self.read_head().await?;
        let commit = self.read_commit(head.current_commit).await?;
        let namespace = self
            .read_namespace_streams(&commit, commit.change_cursor)
            .await?;
        Ok(ManagedObservation {
            namespace,
            head_revision,
            namespace_revision: head.current_commit,
            reclamation_watermark: head.minimum_retained_cursor,
            gc_epoch: head.gc_epoch,
            commit,
        })
    }

    pub(super) async fn read_head(&self) -> Result<(Head, String), Error> {
        let control = object::read_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .ok_or_else(|| Error::corrupt("open Managed volume", "namespace head is missing"))?;
        let head: Head = HEAD_RECORD.decode(&control.bytes)?;
        if head.minimum_retained_cursor.sequence() > head.current_commit.change_cursor.sequence() {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace head retention is invalid",
            ));
        }
        Ok((head, control.revision))
    }

    pub(crate) async fn prepare_publication(
        &self,
        observed: &ManagedObservation,
        target: &Namespace<StreamRef>,
        operation: OperationId,
    ) -> Result<NamespaceRevision, Error> {
        if target.volume_id != self.id()
            || target.root != self.format.root_node_id()
            || target.cursor.sequence() != observed.namespace.cursor.sequence() + 1
        {
            return Err(Error::invalid(
                "publish Managed namespace",
                "publication ancestry is invalid",
            ));
        }

        let mut commit = observed.commit.clone();
        commit.change_cursor = target.cursor;
        let compact = commit.namespace.len() >= self.stream_concurrency
            || commit.operation_results.len() >= self.stream_concurrency;
        if compact {
            commit.namespace = vec![self.write_full_namespace(target, observed.gc_epoch).await?];
        } else if let Some(delta) = self
            .write_namespace_delta(&observed.namespace, target, observed.gc_epoch)
            .await?
        {
            commit.namespace.push(delta);
        } else {
            return Err(Error::invalid(
                "publish Managed namespace",
                "publication contains no namespace change",
            ));
        }

        let operation_record = OperationRecord {
            operation_id: operation,
            change_cursor: target.cursor,
        };
        if compact {
            commit.operation_results = self
                .copy_operations(&commit, observed.gc_epoch, Some(operation_record))
                .await?
                .into_iter()
                .collect();
        } else {
            let operation_stream = stream::write_records(
                &self.operator,
                observed.gc_epoch,
                ObjectClass::OperationResultSegment,
                StreamKind::OPERATION_RESULTS,
                [operation_record],
            )
            .await?;
            commit.operation_results.push(operation_stream);
        }
        self.write_commit(observed.gc_epoch, &commit).await
    }

    pub(crate) async fn commit_publication(
        &self,
        observed: &ManagedObservation,
        target: NamespaceRevision,
        operation: OperationId,
    ) -> Result<(), Error> {
        if target.change_cursor.sequence() != observed.namespace.cursor.sequence() + 1 {
            return Err(Error::invalid(
                "publish Managed namespace",
                "prepared publication ancestry is invalid",
            ));
        }
        let head = Head {
            current_commit: target,
            gc_epoch: observed.gc_epoch,
            minimum_retained_cursor: observed.reclamation_watermark,
        };
        if object::write_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.encode(&head)?,
            object::ControlCondition::Revision(&observed.head_revision),
        )
        .await?
        {
            return Ok(());
        }
        let current = self.observe().await?;
        if self.operation_committed(operation, &current).await? {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Conflict,
                "publish Managed namespace",
                "observed generation changed",
            ))
        }
    }

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
        let commit = self.read_commit(reference).await?;
        self.read_namespace_streams(&commit, revision.change_cursor)
            .await
    }

    pub(crate) async fn operation_committed(
        &self,
        operation: OperationId,
        observed: &ManagedObservation,
    ) -> Result<bool, Error> {
        for reference in &observed.commit.operation_results {
            require_stream(
                *reference,
                StreamKind::OPERATION_RESULTS,
                ObjectClass::OperationResultSegment,
            )?;
            let mut reader =
                RecordStreamReader::<OperationRecord>::open(&self.operator, *reference).await?;
            while let Some(record) = reader.next().await? {
                if record.operation_id == operation {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub(crate) const fn operator(&self) -> &Operator {
        &self.operator
    }

    pub(crate) const fn workset_options(&self) -> WorksetOptions {
        self.worksets
    }

    pub(super) async fn replace_head(
        &self,
        expected_revision: &str,
        head: &Head,
    ) -> Result<bool, Error> {
        object::write_control(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.encode(head)?,
            object::ControlCondition::Revision(expected_revision),
        )
        .await
    }

    pub(super) async fn visit_reachable_objects(
        &self,
        reference: NamespaceRevision,
        mut visit: impl FnMut(ObjectRef) -> Result<(), Error>,
    ) -> Result<(), Error> {
        visit(reference.object)?;
        let commit = self.read_commit(reference).await?;
        for reference in commit.streams() {
            visit(reference.object)?;
        }
        for reference in &commit.namespace {
            require_stream(
                *reference,
                StreamKind::NAMESPACE_RECORDS,
                ObjectClass::NamespaceSegment,
            )?;
            let mut reader =
                RecordStreamReader::<NamespaceRecord<StreamRef>>::open(&self.operator, *reference)
                    .await?;
            while let Some(record) = reader.next().await? {
                if let Some(NamespaceNode {
                    value: NamespaceValue::RegularFile { content, .. },
                    ..
                }) = record.value
                {
                    visit(content.object)?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn compact_for_collection(
        &self,
        reference: NamespaceRevision,
        gc_epoch: GcEpoch,
    ) -> Result<NamespaceRevision, Error> {
        let source = self.read_commit(reference).await?;
        let namespace = self
            .read_namespace_streams(&source, source.change_cursor)
            .await?;
        let mut commit = NamespaceCommit {
            volume_id: source.volume_id,
            change_cursor: source.change_cursor,
            namespace: vec![self.write_full_namespace(&namespace, gc_epoch).await?],
            operation_results: Vec::new(),
        };
        if let Some(operations) = self.copy_operations(&source, gc_epoch, None).await? {
            commit.operation_results.push(operations);
        }
        self.write_commit(gc_epoch, &commit).await
    }

    async fn copy_operations(
        &self,
        source: &NamespaceCommit,
        gc_epoch: GcEpoch,
        extra: Option<OperationRecord>,
    ) -> Result<Option<StreamRef>, Error> {
        let mut writer = None;
        for reference in &source.operation_results {
            require_stream(
                *reference,
                StreamKind::OPERATION_RESULTS,
                ObjectClass::OperationResultSegment,
            )?;
            let mut reader =
                RecordStreamReader::<OperationRecord>::open(&self.operator, *reference).await?;
            while let Some(record) = reader.next().await? {
                if writer.is_none() {
                    writer = Some(
                        RecordStreamWriter::open(
                            &self.operator,
                            gc_epoch,
                            ObjectClass::OperationResultSegment,
                            StreamKind::OPERATION_RESULTS,
                        )
                        .await?,
                    );
                }
                writer
                    .as_mut()
                    .expect("operation writer is open")
                    .write(&record)
                    .await?;
            }
        }
        if let Some(record) = extra {
            if writer.is_none() {
                writer = Some(
                    RecordStreamWriter::open(
                        &self.operator,
                        gc_epoch,
                        ObjectClass::OperationResultSegment,
                        StreamKind::OPERATION_RESULTS,
                    )
                    .await?,
                );
            }
            writer
                .as_mut()
                .expect("operation writer is open")
                .write(&record)
                .await?;
        }
        match writer {
            Some(writer) => writer.close().await.map(Some),
            None => Ok(None),
        }
    }

    async fn write_full_namespace(
        &self,
        namespace: &Namespace<StreamRef>,
        gc_epoch: GcEpoch,
    ) -> Result<StreamRef, Error> {
        let mut source = namespace.reader()?;
        let mut writer = RecordStreamWriter::open(
            &self.operator,
            gc_epoch,
            ObjectClass::NamespaceSegment,
            StreamKind::NAMESPACE_RECORDS,
        )
        .await?;
        while let Some(record) = source.next()? {
            writer.write(&record).await?;
        }
        writer.close().await
    }

    async fn write_namespace_delta(
        &self,
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
                        &self.operator,
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

    async fn read_namespace_streams(
        &self,
        commit: &NamespaceCommit,
        view_cursor: ChangeCursor,
    ) -> Result<Namespace<StreamRef>, Error> {
        let workspace = Workspace::create(self.worksets)?;
        let mut output = workspace.writer("namespace")?;
        let mut streams = Vec::with_capacity(commit.namespace.len());
        let downloads = futures::stream::iter(commit.namespace.iter().copied())
            .map(|reference| {
                self.download_namespace_stream(&workspace, reference, commit.change_cursor)
            })
            .buffer_unordered(self.stream_concurrency);
        futures::pin_mut!(downloads);
        while let Some(stream) = downloads.next().await {
            streams.push(stream?);
        }
        let merged = merge_namespace_streams(&workspace, streams, view_cursor)?;
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
                if node.node_id != self.format.root_node_id()
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
            root: self.format.root_node_id(),
            entries: output.finish()?,
        })
    }

    async fn download_namespace_stream(
        &self,
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
            RecordStreamReader::<NamespaceRecord<StreamRef>>::open(&self.operator, reference)
                .await?;
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

    async fn write_commit(
        &self,
        gc_epoch: GcEpoch,
        commit: &NamespaceCommit,
    ) -> Result<NamespaceRevision, Error> {
        let mut writer =
            object::ImmutableWriter::open(&self.operator, gc_epoch, ObjectClass::NamespaceCommit)
                .await?;
        writer.write(COMMIT_RECORD.encode(commit)?).await?;
        let object = writer.close().await?;
        Ok(NamespaceRevision {
            object,
            change_cursor: commit.change_cursor,
        })
    }

    async fn read_commit(&self, reference: NamespaceRevision) -> Result<NamespaceCommit, Error> {
        if reference.object.class != ObjectClass::NamespaceCommit {
            return Err(Error::corrupt(
                "read Managed namespace",
                "commit reference has the wrong object class",
            ));
        }
        let bytes = object::read_immutable(
            &self.operator,
            reference.object,
            COMMIT_RECORD.maximum_encoded_bytes(),
        )
        .await?;
        let commit: NamespaceCommit = COMMIT_RECORD.decode(&bytes)?;
        if commit.volume_id != self.id()
            || commit.change_cursor != reference.change_cursor
            || commit.namespace.is_empty()
        {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace commit does not match its reference",
            ));
        }
        Ok(commit)
    }
}

fn merge_namespace_streams(
    workspace: &Workspace,
    streams: Vec<Spool<NamespaceRecord<StreamRef>>>,
    view_cursor: ChangeCursor,
) -> Result<Spool<NamespaceRecord<StreamRef>>, Error> {
    balanced_merge(streams, |left, right| {
        merge_namespace_pair(workspace, left, right, view_cursor)
    })?
    .ok_or_else(|| Error::corrupt("read Managed namespace", "namespace has no streams"))
}

fn merge_namespace_pair(
    workspace: &Workspace,
    left: &Spool<NamespaceRecord<StreamRef>>,
    right: &Spool<NamespaceRecord<StreamRef>>,
    view_cursor: ChangeCursor,
) -> Result<Spool<NamespaceRecord<StreamRef>>, Error> {
    let mut readers = [left, right]
        .into_iter()
        .map(Spool::reader)
        .collect::<Result<Vec<_>, Error>>()?;
    let mut heads = readers
        .iter_mut()
        .map(|reader| reader.next())
        .collect::<Result<Vec<_>, Error>>()?;
    let mut output = workspace.writer("namespace-merge")?;

    while let Some(path) = heads
        .iter()
        .filter_map(|record| record.as_ref().map(|record| record.path.as_str()))
        .min()
        .map(str::to_owned)
    {
        let mut selected = None::<NamespaceRecord<StreamRef>>;
        for (index, head) in heads.iter_mut().enumerate() {
            if head.as_ref().is_none_or(|record| record.path != path) {
                continue;
            }
            let record = head.take().expect("matching namespace record exists");
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
            *head = readers[index].next()?;
        }
        if let Some(record) = selected {
            output.write(&record)?;
        }
    }
    output.finish()
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

fn require_stream(reference: StreamRef, kind: StreamKind, class: ObjectClass) -> Result<(), Error> {
    if reference.kind != kind || reference.object.class != class {
        return Err(Error::corrupt(
            "read Managed namespace",
            "stream reference has the wrong type",
        ));
    }
    Ok(())
}
