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

//! Bounded temporary record streams used while reconciling a namespace.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read as _, Write as _};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;
use crate::filesystem::{ChangeCursor, NamespaceRecord, NodeId, OperationId, VolumeId};

// SlateDB uses a 64 MiB L0 flush budget by default. The workset uses the same
// order of magnitude for run generation, but this is a local resource policy
// and never enters the Managed wire format.
const SORT_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const MERGE_FAN_IN: usize = 32;

struct WorkspaceInner {
    path: PathBuf,
}

impl Drop for WorkspaceInner {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Clone)]
pub(crate) struct Workspace {
    inner: Arc<WorkspaceInner>,
}

#[derive(Clone)]
pub(crate) struct Namespace<C> {
    pub(crate) volume_id: VolumeId,
    pub(crate) cursor: ChangeCursor,
    pub(crate) root: NodeId,
    pub(crate) entries: Spool<NamespaceRecord<C>>,
}

impl<C: DeserializeOwned> Namespace<C> {
    pub(crate) fn reader(&self) -> Result<SpoolReader<NamespaceRecord<C>>, Error> {
        self.entries.reader()
    }
}

impl Workspace {
    pub(crate) fn create() -> Result<Self, Error> {
        let path = std::env::temp_dir().join(format!("ofs-sync-{}", OperationId::generate()));
        fs::create_dir(&path)
            .map_err(|error| Error::from_io("create Sync workspace", Some(&path), error))?;
        Ok(Self {
            inner: Arc::new(WorkspaceInner { path }),
        })
    }

    pub(crate) fn writer<T>(&self, stem: &str) -> Result<SpoolWriter<T>, Error> {
        let path = self
            .inner
            .path
            .join(format!("{stem}-{}", OperationId::generate()));
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| Error::from_io("create Sync record stream", Some(&path), error))?;
        Ok(SpoolWriter {
            workspace: Some(self.clone()),
            path,
            writer: Some(BufWriter::new(file)),
            marker: PhantomData,
        })
    }
}

struct SpoolFile {
    _workspace: Workspace,
    path: PathBuf,
}

impl Drop for SpoolFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) struct Spool<T> {
    file: Arc<SpoolFile>,
    marker: PhantomData<T>,
}

impl<T> Clone for Spool<T> {
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            marker: PhantomData,
        }
    }
}

impl<T: DeserializeOwned> Spool<T> {
    pub(crate) fn reader(&self) -> Result<SpoolReader<T>, Error> {
        SpoolReader::open(self.file.clone())
    }

    pub(crate) fn stream(
        &self,
    ) -> Result<impl futures::Stream<Item = Result<T, Error>> + use<T>, Error> {
        let reader = self.reader()?;
        Ok(futures::stream::try_unfold(
            reader,
            |mut reader| async move {
                match reader.next()? {
                    Some(record) => Ok(Some((record, reader))),
                    None => Ok(None),
                }
            },
        ))
    }
}

pub(crate) struct SpoolWriter<T> {
    workspace: Option<Workspace>,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    marker: PhantomData<T>,
}

impl<T> Drop for SpoolWriter<T> {
    fn drop(&mut self) {
        drop(self.writer.take());
        if self.workspace.is_some() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl<T: Serialize> SpoolWriter<T> {
    pub(crate) fn write(&mut self, value: &T) -> Result<usize, Error> {
        let mut bytes = Vec::new();
        ciborium::into_writer(value, &mut bytes)
            .map_err(|_| Error::invalid("write Sync record stream", "record cannot be encoded"))?;
        self.write_frame(&bytes)
    }

    fn write_frame(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let length = u32::try_from(bytes.len())
            .map_err(|_| Error::invalid("write Sync record stream", "record is too large"))?;
        let writer = self.writer.as_mut().expect("unfinished Sync record writer");
        writer
            .write_all(&length.to_le_bytes())
            .and_then(|()| writer.write_all(bytes))
            .map_err(|error| Error::from_io("write Sync record stream", Some(&self.path), error))?;
        Ok(bytes.len() + size_of::<u32>())
    }

    pub(crate) fn finish(mut self) -> Result<Spool<T>, Error> {
        let mut writer = self.writer.take().expect("unfinished Sync record writer");
        writer.flush().map_err(|error| {
            Error::from_io("finish Sync record stream", Some(&self.path), error)
        })?;
        drop(writer);
        let workspace = self
            .workspace
            .take()
            .expect("unfinished Sync record writer");
        Ok(Spool {
            file: Arc::new(SpoolFile {
                _workspace: workspace,
                path: self.path.clone(),
            }),
            marker: PhantomData,
        })
    }
}

pub(crate) struct SpoolReader<T> {
    file: Arc<SpoolFile>,
    reader: BufReader<File>,
    pending_length: Option<usize>,
    marker: PhantomData<T>,
}

impl<T: DeserializeOwned> SpoolReader<T> {
    fn open(spool: Arc<SpoolFile>) -> Result<Self, Error> {
        let file = File::open(&spool.path)
            .map_err(|error| Error::from_io("open Sync record stream", Some(&spool.path), error))?;
        Ok(Self {
            file: spool,
            reader: BufReader::new(file),
            pending_length: None,
            marker: PhantomData,
        })
    }

    pub(crate) fn next(&mut self) -> Result<Option<T>, Error> {
        let Some(bytes) = self.next_frame()? else {
            return Ok(None);
        };
        decode_record(&bytes).map(Some)
    }

    fn next_frame(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let Some(frame_bytes) = self.peek_frame_bytes()? else {
            return Ok(None);
        };
        let length = frame_bytes - size_of::<u32>();
        self.pending_length = None;
        let mut bytes = vec![0; length];
        self.reader.read_exact(&mut bytes).map_err(|error| {
            Error::from_io("read Sync record stream", Some(&self.file.path), error)
        })?;
        Ok(Some(bytes))
    }

    fn peek_frame_bytes(&mut self) -> Result<Option<usize>, Error> {
        if let Some(length) = self.pending_length {
            return length
                .checked_add(size_of::<u32>())
                .map(Some)
                .ok_or_else(|| {
                    Error::corrupt("read Sync record stream", "record length overflows")
                });
        }
        let mut length = [0_u8; size_of::<u32>()];
        let first = self.reader.read(&mut length[..1]).map_err(|error| {
            Error::from_io("read Sync record stream", Some(&self.file.path), error)
        })?;
        if first == 0 {
            return Ok(None);
        }
        self.reader.read_exact(&mut length[1..]).map_err(|error| {
            Error::from_io("read Sync record stream", Some(&self.file.path), error)
        })?;
        let length = u32::from_le_bytes(length) as usize;
        self.pending_length = Some(length);
        length
            .checked_add(size_of::<u32>())
            .map(Some)
            .ok_or_else(|| Error::corrupt("read Sync record stream", "record length overflows"))
    }
}

fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    let mut input = std::io::Cursor::new(bytes);
    let value = ciborium::from_reader(&mut input)
        .map_err(|_| Error::corrupt("read Sync record stream", "record is invalid"))?;
    if input.position() != bytes.len() as u64 {
        return Err(Error::corrupt(
            "read Sync record stream",
            "record has trailing bytes",
        ));
    }
    Ok(value)
}

pub(crate) fn sort<T, K>(
    workspace: &Workspace,
    source: &Spool<T>,
    key: impl Fn(&T) -> K + Copy,
) -> Result<Spool<T>, Error>
where
    T: DeserializeOwned + Serialize,
    K: Ord,
{
    let mut source = source.reader()?;
    let mut runs = Vec::new();
    loop {
        let mut records = Vec::new();
        let mut encoded_bytes = 0_usize;
        while encoded_bytes < SORT_MEMORY_BYTES {
            let Some(frame_bytes) = source.peek_frame_bytes()? else {
                break;
            };
            if encoded_bytes != 0 && frame_bytes > SORT_MEMORY_BYTES - encoded_bytes {
                break;
            }
            let bytes = source
                .next_frame()?
                .expect("peeked Sync record remains available");
            let record = decode_record::<T>(&bytes)?;
            records.push(RecordItem {
                bytes,
                key: key(&record),
                source: 0,
            });
            encoded_bytes += frame_bytes;
        }
        if records.is_empty() {
            break;
        }
        records.sort_by(|left, right| left.key.cmp(&right.key));
        let mut run = workspace.writer("sort-run")?;
        for record in records {
            run.write_frame(&record.bytes)?;
        }
        runs.push(run.finish()?);
    }

    if runs.is_empty() {
        return workspace.writer("sorted")?.finish();
    }
    while runs.len() > 1 {
        let mut merged = Vec::new();
        let mut inputs = runs.into_iter();
        loop {
            let mut group = inputs.by_ref().take(MERGE_FAN_IN).collect::<Vec<_>>();
            if group.is_empty() {
                break;
            }
            if group.len() == 1 {
                merged.push(group.pop().expect("one sort run remains"));
            } else {
                merged.push(merge_runs(workspace, &group, key)?);
            }
        }
        runs = merged;
    }
    Ok(runs.pop().expect("one sorted run remains"))
}

struct RecordItem<K> {
    bytes: Vec<u8>,
    key: K,
    source: usize,
}

impl<K: Ord> PartialEq for RecordItem<K> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source == other.source
    }
}

impl<K: Ord> Eq for RecordItem<K> {}

impl<K: Ord> PartialOrd for RecordItem<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord> Ord for RecordItem<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source.cmp(&self.source))
    }
}

fn merge_runs<T, K>(
    workspace: &Workspace,
    runs: &[Spool<T>],
    key: impl Fn(&T) -> K + Copy,
) -> Result<Spool<T>, Error>
where
    T: DeserializeOwned + Serialize,
    K: Ord,
{
    let mut readers = runs
        .iter()
        .map(Spool::reader)
        .collect::<Result<Vec<_>, Error>>()?;
    let mut heap = BinaryHeap::new();
    for (source, reader) in readers.iter_mut().enumerate() {
        if let Some(bytes) = reader.next_frame()? {
            let record = decode_record::<T>(&bytes)?;
            heap.push(RecordItem {
                bytes,
                key: key(&record),
                source,
            });
        }
    }
    let mut output = workspace.writer("merge-run")?;
    while let Some(item) = heap.pop() {
        output.write_frame(&item.bytes)?;
        let source = item.source;
        drop(item);
        if let Some(bytes) = readers[source].next_frame()? {
            let record = decode_record::<T>(&bytes)?;
            heap.push(RecordItem {
                bytes,
                key: key(&record),
                source,
            });
        }
    }
    output.finish()
}
