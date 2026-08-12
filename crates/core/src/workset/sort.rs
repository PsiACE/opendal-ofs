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
use std::collections::BinaryHeap;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;

use super::spool::decode_record;
use super::{Spool, Workspace};

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
    let fan_in = workspace.merge_fan_in();
    let mut runs = MergeRuns::new(fan_in);
    loop {
        let mut records = Vec::new();
        let mut encoded_bytes = 0_usize;
        while encoded_bytes < workspace.sort_run_target_bytes() {
            let Some(frame_bytes) = source.peek_frame_bytes()? else {
                break;
            };
            if encoded_bytes != 0 && frame_bytes > workspace.sort_run_target_bytes() - encoded_bytes
            {
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
        runs.push(run.finish()?, |group| merge_runs(workspace, group, key))?;
    }
    drop(source);

    runs.finish(|runs| merge_runs(workspace, runs, key))?
        .map_or_else(|| workspace.writer("sorted")?.finish(), Ok)
}

/// Incrementally compacts equivalent sorted inputs without retaining one
/// descriptor per input. The merge arity follows the caller's I/O budget;
/// arity two is the natural degenerate merge that still reduces run count.
pub(crate) struct MergeRuns<T> {
    fan_in: usize,
    levels: Vec<Vec<T>>,
}

impl<T> MergeRuns<T> {
    pub(crate) fn new(fan_in: usize) -> Self {
        Self {
            fan_in,
            levels: Vec::new(),
        }
    }

    pub(crate) fn push(
        &mut self,
        mut input: T,
        mut merge: impl FnMut(&[T]) -> Result<T, Error>,
    ) -> Result<(), Error> {
        let mut level = 0;
        loop {
            if level == self.levels.len() {
                self.levels.push(Vec::new());
            }
            self.levels[level].push(input);
            if self.levels[level].len() < self.fan_in {
                return Ok(());
            }
            input = merge(&std::mem::take(&mut self.levels[level]))?;
            level += 1;
        }
    }

    pub(crate) fn finish(
        self,
        merge: impl FnMut(&[T]) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        merge_all(
            self.levels.into_iter().flatten().collect(),
            self.fan_in,
            merge,
        )
    }
}

pub(crate) fn merge_all<T>(
    mut inputs: Vec<T>,
    fan_in: usize,
    mut merge: impl FnMut(&[T]) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    while inputs.len() > 1 {
        let mut output = Vec::with_capacity(inputs.len().div_ceil(fan_in));
        let mut remaining = inputs.into_iter();
        loop {
            let group = remaining.by_ref().take(fan_in).collect::<Vec<_>>();
            if group.is_empty() {
                break;
            }
            if group.len() == 1 {
                output.extend(group);
            } else {
                output.push(merge(&group)?);
            }
        }
        inputs = output;
    }
    Ok(inputs.pop())
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
