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

//! Immutable data segments and file extent materialization.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use fastcdc::v2020::AsyncStreamCDC;
use foyer::HybridCacheBuilder;
use futures::{StreamExt, TryStreamExt, stream};
use opendal::layers::{FoyerKey, FoyerLayer, FoyerValue};
use opendal::{Buffer, ErrorKind, Operator};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, OnceCell, mpsc, oneshot};

use super::{ManagedError, ManagedErrorKind};
use crate::filesystem::NodeKind;
use crate::managed::format::{ContentRef, Extent, ExtentMap, SegmentRef};
use crate::managed::metadata::namespace::{FileVersionRecord, NamespaceSnapshot};
use crate::managed::metadata::object::ensure_immutable;

const SEGMENT_ROOT: &str = ".ofs/managed/data/v1/segments/sha256";
const REQUEST_EQUIVALENT_BYTES: u64 = 4 * 1024;
const RANGE_FETCH_GAP: usize = 512 * 1024;
// Placement policy. These values are not part of the durable format.
const TARGET_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
const MATERIALIZE_WINDOW_BYTES: u64 = TARGET_SEGMENT_SIZE;
const MATERIALIZE_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MATERIALIZE_BATCH_BYTES: u64 = MATERIALIZE_CACHE_BYTES as u64;
const FASTCDC_MINIMUM_FILE_SIZE: u64 = 1024 * 1024;
const FASTCDC_MINIMUM_SIZE: u32 = 64 * 1024;
const FASTCDC_TARGET_SIZE: u32 = 256 * 1024;
const FASTCDC_MAXIMUM_SIZE: u32 = 1024 * 1024;

/// Data segments removed by one namespace-fenced garbage-collection sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentGcMaintenance {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
}

/// Unique immutable data segments retained across one or more namespace positions.
#[derive(Default)]
pub(crate) struct RetainedDataRoots(BTreeMap<[u8; 32], u64>);

impl RetainedDataRoots {
    pub(crate) fn retain(&mut self, snapshot: &NamespaceSnapshot) -> Result<(), ManagedError> {
        visit_reachable_file_versions(snapshot, "mark retained data segments", |version| {
            for extent in &version.extent_map.extents {
                match self.0.insert(extent.segment.digest, extent.segment.length) {
                    Some(length) if length != extent.segment.length => {
                        return Err(corrupt(
                            "mark retained data segments",
                            "one segment digest has conflicting physical lengths",
                        ));
                    }
                    _ => {}
                }
            }
            Ok(())
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StoredContent {
    segment: SegmentRef,
    offset: u64,
}

/// Physical locations already referenced by one fixed authority snapshot.
#[derive(Clone, Debug, Default)]
pub(crate) struct AuthorityKnownContent(BTreeMap<ContentRef, StoredContent>);

impl AuthorityKnownContent {
    pub(crate) fn include(&mut self, version: &FileVersionRecord) -> Result<(), ManagedError> {
        if !version.is_valid() {
            return Err(corrupt(
                "derive authority-known content",
                "live node references an invalid file version",
            ));
        }
        for extent in &version.extent_map.extents {
            self.0.entry(extent.content).or_insert(StoredContent {
                segment: extent.segment,
                offset: extent.segment_offset,
            });
        }
        Ok(())
    }

    fn get(&self, content: &ContentRef) -> Option<StoredContent> {
        self.0.get(content).copied()
    }
}

#[derive(Clone, Copy)]
struct FastCdcSizes {
    minimum: u32,
    target: u32,
    maximum: u32,
}

#[derive(Debug)]
struct PreparedFile {
    logical_size: u64,
    logical_digest: [u8; 32],
    extents: Vec<PreparedExtent>,
}

#[derive(Debug)]
struct PreparedExtent {
    logical_offset: u64,
    content: ContentRef,
}

struct PreparedChunk {
    extent: PreparedExtent,
    bytes: Vec<u8>,
}

struct PreparedStream {
    path: String,
    chunks: mpsc::Receiver<PreparedChunk>,
    completion: oneshot::Receiver<Result<(u64, [u8; 32]), ManagedError>>,
}

#[derive(Debug)]
struct SealedSegment {
    reference: SegmentRef,
    bytes: Vec<u8>,
    locations: BTreeMap<ContentRef, StoredContent>,
}

type DemandKey = (u64, u64, ContentRef);
type SegmentDemand = BTreeMap<DemandKey, usize>;

struct MaterializationPlan {
    segments: BTreeMap<SegmentRef, SegmentReadPlan>,
}

struct MaterializationContext {
    target: Operator,
    cached: Operator,
    plans: MaterializationPlan,
}

enum SegmentReadPlan {
    Complete {
        demands: BTreeSet<DemandKey>,
        verified: OnceCell<()>,
    },
    Ranged {
        demands: BTreeSet<DemandKey>,
        state: Mutex<RangeReadState>,
    },
}

struct RangeReadState {
    bytes: Option<BTreeMap<DemandKey, Buffer>>,
    remaining: usize,
}

/// The Managed v1 data plane.
#[derive(Clone)]
pub(crate) struct ManagedData {
    operator: Operator,
    cached: Arc<OnceCell<Operator>>,
}

impl ManagedData {
    pub(crate) fn new(operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.read || !capability.write || !capability.write_with_if_not_exists {
            return Err(invalid(
                "open Managed data",
                "data storage requires read, write, and create-only write",
            ));
        }
        Ok(Self {
            operator,
            cached: Arc::new(OnceCell::new()),
        })
    }

    /// Freeze a set of files into as few immutable segments as placement policy permits.
    pub(crate) async fn stage_files(
        &self,
        source: &Operator,
        staging: &Operator,
        paths: Vec<String>,
        known: &AuthorityKnownContent,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersionRecord>, ManagedError> {
        let mut paths = paths;
        paths.sort();
        if paths.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("stage Managed files", "input path is repeated"));
        }
        // Keep file production concurrent but consume the bounded streams in path order.
        // Segment placement is then stable without retaining complete files in memory.
        let mut producer_tasks = Vec::with_capacity(paths.len());
        let mut prepared_streams = Vec::with_capacity(paths.len());
        for path in paths {
            let source = source.clone();
            let staging = staging.clone();
            let producer_path = path.clone();
            let (sender, chunks) = mpsc::channel(2);
            let (complete, completion) = oneshot::channel();
            producer_tasks.push(async move {
                let result = stream_file(&source, &staging, &producer_path, &sender).await;
                drop(sender);
                let _ = complete.send(result);
                Ok::<(), ManagedError>(())
            });
            prepared_streams.push(PreparedStream {
                path,
                chunks,
                completion,
            });
        }
        let producers = stream::iter(producer_tasks)
            .buffer_unordered(concurrency.get())
            .try_collect::<Vec<_>>();

        let collect = async {
            let mut files = Vec::with_capacity(prepared_streams.len());
            let mut new_content = BTreeMap::<ContentRef, Vec<u8>>::new();
            let mut pending_bytes = 0_u64;
            let mut created = BTreeMap::new();
            for mut prepared in prepared_streams {
                let mut extents = Vec::new();
                while let Some(PreparedChunk { extent, bytes }) = prepared.chunks.recv().await {
                    if known.get(&extent.content).is_none()
                        && !created.contains_key(&extent.content)
                    {
                        match new_content.entry(extent.content) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                entry.insert(bytes);
                                pending_bytes = pending_bytes
                                    .checked_add(extent.content.length)
                                    .ok_or_else(|| {
                                        invalid(
                                            "stage Managed files",
                                            "pending segment bytes overflow",
                                        )
                                    })?;
                            }
                            std::collections::btree_map::Entry::Occupied(entry)
                                if entry.get() != &bytes =>
                            {
                                return Err(corrupt(
                                    "stage Managed files",
                                    "equal content references contain different bytes",
                                ));
                            }
                            std::collections::btree_map::Entry::Occupied(_) => {}
                        }
                    }
                    extents.push(extent);

                    while pending_bytes >= TARGET_SEGMENT_SIZE {
                        let contents = take_segment_contents(&mut new_content)?;
                        pending_bytes -= contents.keys().map(|content| content.length).sum::<u64>();
                        created.extend(self.create_segment(seal_segment(contents)?).await?);
                    }
                }
                let (logical_size, logical_digest) = prepared
                    .completion
                    .await
                    .map_err(|_| unavailable("stage Managed files"))??;
                files.push((
                    prepared.path,
                    PreparedFile {
                        logical_size,
                        logical_digest,
                        extents,
                    },
                ));
            }

            while !new_content.is_empty() {
                let contents = take_segment_contents(&mut new_content)?;
                created.extend(self.create_segment(seal_segment(contents)?).await?);
            }

            files
                .into_iter()
                .map(|(path, file)| {
                    let extent_map = ExtentMap {
                        extents: file
                            .extents
                            .into_iter()
                            .map(|extent| {
                                let stored = known
                                    .get(&extent.content)
                                    .or_else(|| created.get(&extent.content).copied())
                                    .ok_or_else(|| {
                                        corrupt(
                                            "stage Managed files",
                                            "prepared content has no segment location",
                                        )
                                    })?;
                                Ok(Extent {
                                    logical_offset: extent.logical_offset,
                                    content: extent.content,
                                    segment: stored.segment,
                                    segment_offset: stored.offset,
                                })
                            })
                            .collect::<Result<Vec<_>, ManagedError>>()?,
                    };
                    let version = FileVersionRecord::from_extents(
                        file.logical_size,
                        file.logical_digest,
                        extent_map,
                    )
                    .ok_or_else(|| {
                        invalid(
                            "stage Managed files",
                            "generated file extent map is invalid",
                        )
                    })?;
                    Ok((path, version))
                })
                .collect()
        };

        let (_, files) = futures::try_join!(producers, collect)?;
        Ok(files)
    }

    async fn create_segment(
        &self,
        segment: SealedSegment,
    ) -> Result<BTreeMap<ContentRef, StoredContent>, ManagedError> {
        let key = segment_key(segment.reference);
        ensure_immutable(
            &self.operator,
            &key,
            &segment.bytes,
            "create data segment",
            ManagedErrorKind::Corrupt,
            "immutable data segment changed",
        )
        .await?;
        Ok(segment.locations)
    }

    pub(crate) async fn materialize(
        &self,
        target: &Operator,
        requests: Vec<(String, FileVersionRecord)>,
        full_tree: bool,
        concurrency: NonZeroUsize,
    ) -> Result<(), ManagedError> {
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        let mut batch_bytes = 0_u64;
        for request in requests {
            let size = request.1.logical_size;
            if !batch.is_empty() && batch_bytes.saturating_add(size) > MATERIALIZE_BATCH_BYTES {
                batches.push(std::mem::take(&mut batch));
                batch_bytes = 0;
            }
            batch_bytes = batch_bytes.saturating_add(size);
            batch.push(request);
        }
        if !batch.is_empty() {
            batches.push(batch);
        }

        for requests in batches {
            let plans = MaterializationPlan::new(
                &requests,
                full_tree,
                self.operator.info().full_capability().stat,
            );
            let cached = if plans
                .segments
                .values()
                .any(|plan| matches!(plan, SegmentReadPlan::Complete { .. }))
            {
                self.cached_operator().await?
            } else {
                self.operator.clone()
            };
            let context = Arc::new(MaterializationContext {
                target: target.clone(),
                cached,
                plans,
            });
            stream::iter(requests)
                .map(|(path, version)| {
                    let context = context.clone();
                    async move {
                        self.materialize_file(&context, path, version, concurrency)
                            .await
                    }
                })
                .buffer_unordered(concurrency.get())
                .try_collect::<Vec<_>>()
                .await?;
        }
        Ok(())
    }

    async fn cached_operator(&self) -> Result<Operator, ManagedError> {
        self.cached
            .get_or_try_init(|| async {
                let cache = HybridCacheBuilder::new()
                    .with_flush_on_close(false)
                    .memory(MATERIALIZE_CACHE_BYTES)
                    .with_shards(1)
                    .with_weighter(|_: &FoyerKey, value: &FoyerValue| value.0.len())
                    .storage()
                    .build()
                    .await
                    .map_err(|_| unavailable("open materialization segment cache"))?;
                Ok(self
                    .operator
                    .clone()
                    .layer(FoyerLayer::new(cache).with_size_limit(..=MATERIALIZE_CACHE_BYTES)))
            })
            .await
            .cloned()
    }

    async fn materialize_file(
        &self,
        context: &MaterializationContext,
        path: String,
        version: FileVersionRecord,
        concurrency: NonZeroUsize,
    ) -> Result<(), ManagedError> {
        let mut writer = context
            .target
            .writer(&path)
            .await
            .map_err(|_| unavailable("write materialized file"))?;
        let mut logical = Sha256::new();
        let mut written = 0_u64;
        let extents = &version.extent_map.extents;
        let mut start = 0;
        while start < extents.len() {
            let end = extent_window_end(extents, start);
            let fetched = match self
                .read_extent_window(context, &extents[start..end], concurrency)
                .await
            {
                Ok(fetched) => fetched,
                Err(error) => {
                    let _ = writer.abort().await;
                    return Err(error);
                }
            };
            for bytes in fetched {
                let Some(next_written) = written.checked_add(bytes.len() as u64) else {
                    let _ = writer.abort().await;
                    return Err(corrupt(
                        "materialize Managed files",
                        "logical file length overflows",
                    ));
                };
                written = next_written;
                for chunk in bytes.clone() {
                    logical.update(&chunk);
                }
                if writer.write(bytes).await.is_err() {
                    let _ = writer.abort().await;
                    return Err(unavailable("write materialized file"));
                }
            }
            start = end;
        }
        if written != version.logical_size
            || <[u8; 32]>::from(logical.finalize()) != version.logical_digest
        {
            let _ = writer.abort().await;
            return Err(corrupt(
                "materialize Managed files",
                "logical digest does not match the file version",
            ));
        }
        writer
            .close()
            .await
            .map_err(|_| unavailable("write materialized file"))?;
        Ok(())
    }

    async fn read_extent_window(
        &self,
        context: &MaterializationContext,
        extents: &[Extent],
        concurrency: NonZeroUsize,
    ) -> Result<Vec<Buffer>, ManagedError> {
        let mut reads = BTreeMap::<SegmentRef, Vec<(usize, Extent)>>::new();
        for (index, extent) in extents.iter().copied().enumerate() {
            reads
                .entry(extent.segment)
                .or_default()
                .push((index, extent));
        }

        let fetched = stream::iter(reads)
            .map(|(segment, extents)| async move {
                context.plans.segments[&segment]
                    .read_extents(&self.operator, &context.cached, segment, &extents)
                    .await
            })
            .buffer_unordered(concurrency.get())
            .try_collect::<Vec<Vec<_>>>()
            .await?;
        let mut ordered = vec![None; extents.len()];
        for (index, bytes) in fetched.into_iter().flatten() {
            ordered[index] = Some(bytes);
        }
        Ok(ordered
            .into_iter()
            .map(|bytes| bytes.expect("every planned extent produces one result"))
            .collect())
    }

    pub(crate) async fn collect_unreachable_segments(
        &self,
        snapshot: &NamespaceSnapshot,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let mut roots = RetainedDataRoots::default();
        roots.retain(snapshot)?;
        self.collect_unreachable_segments_from(&roots).await
    }

    pub(crate) async fn collect_unreachable_segments_from(
        &self,
        roots: &RetainedDataRoots,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let capability = self.operator.info().full_capability();
        if !capability.list || !capability.delete {
            return Err(unavailable("collect unreachable data segments"));
        }
        let mut result = SegmentGcMaintenance::default();
        let mut deleter = self
            .operator
            .deleter()
            .await
            .map_err(|_| unavailable("delete unreachable data segments"))?;
        let mut entries = self
            .operator
            .lister_with(&format!("{SEGMENT_ROOT}/"))
            .recursive(true)
            .await
            .map_err(|_| unavailable("list data segments"))?;
        while let Some(entry) = entries
            .try_next()
            .await
            .map_err(|_| unavailable("list data segments"))?
        {
            if !entry.metadata().is_file() {
                continue;
            }
            let Some(reference) =
                segment_ref_from_key(entry.path(), entry.metadata().content_length())
            else {
                continue;
            };
            result.scanned += 1;
            if let Some(length) = roots.0.get(&reference.digest) {
                if *length != reference.length {
                    return Err(corrupt(
                        "collect unreachable data segments",
                        "live segment has an unexpected physical length",
                    ));
                }
                continue;
            }
            let deleted_bytes = result
                .deleted_bytes
                .checked_add(reference.length)
                .ok_or_else(|| {
                    corrupt(
                        "collect unreachable data segments",
                        "deleted byte count exceeds format v1",
                    )
                })?;
            deleter
                .delete(entry.path())
                .await
                .map_err(|_| unavailable("delete unreachable data segments"))?;
            result.deleted += 1;
            result.deleted_bytes = deleted_bytes;
        }
        deleter
            .close()
            .await
            .map_err(|_| unavailable("delete unreachable data segments"))?;
        Ok(result)
    }
}

impl MaterializationPlan {
    fn new(
        requests: &[(String, FileVersionRecord)],
        full_tree: bool,
        cache_complete_segments: bool,
    ) -> Self {
        let mut segments = BTreeMap::<SegmentRef, SegmentDemand>::new();
        let oversized = requests
            .iter()
            .map(|(_, version)| version.logical_size)
            .fold(0_u64, u64::saturating_add)
            > MATERIALIZE_BATCH_BYTES;
        let mut windows = BTreeMap::<SegmentRef, usize>::new();
        for (_, version) in requests {
            for extent in &version.extent_map.extents {
                *segments
                    .entry(extent.segment)
                    .or_default()
                    .entry(demand_key(extent))
                    .or_default() += 1;
            }
            if oversized {
                let mut start = 0;
                while start < version.extent_map.extents.len() {
                    let end = extent_window_end(&version.extent_map.extents, start);
                    for segment in version.extent_map.extents[start..end]
                        .iter()
                        .map(|extent| extent.segment)
                        .collect::<BTreeSet<_>>()
                    {
                        *windows.entry(segment).or_default() += 1;
                    }
                    start = end;
                }
            }
        }
        Self {
            segments: segments
                .into_iter()
                .map(|(segment, demands)| {
                    let plan = if cache_complete_segments
                        && usize::try_from(segment.length)
                            .is_ok_and(|length| length <= MATERIALIZE_CACHE_BYTES)
                        && (prefer_complete_segment(segment, &demands, full_tree)
                            || windows.get(&segment).is_some_and(|count| *count > 1))
                    {
                        SegmentReadPlan::Complete {
                            demands: demands.keys().copied().collect(),
                            verified: OnceCell::new(),
                        }
                    } else {
                        let remaining = demands.values().sum();
                        SegmentReadPlan::Ranged {
                            demands: demands.keys().copied().collect(),
                            state: Mutex::new(RangeReadState {
                                bytes: None,
                                remaining,
                            }),
                        }
                    };
                    (segment, plan)
                })
                .collect(),
        }
    }
}

impl SegmentReadPlan {
    async fn read_extents(
        &self,
        operator: &Operator,
        cached: &Operator,
        segment: SegmentRef,
        extents: &[(usize, Extent)],
    ) -> Result<Vec<(usize, Buffer)>, ManagedError> {
        match self {
            Self::Complete { demands, verified } => {
                let bytes = cached
                    .read(&segment_key(segment))
                    .await
                    .map_err(|error| referenced_segment_error("read data segment", error))?;
                verified
                    .get_or_try_init(|| async { verify_complete_demands(segment, &bytes, demands) })
                    .await?;
                extents
                    .iter()
                    .map(|(index, extent)| {
                        slice_extent(&bytes, 0, extent).map(|bytes| (*index, bytes))
                    })
                    .collect()
            }
            Self::Ranged { demands, state } => {
                let mut state = state.lock().await;
                if state.bytes.is_none() {
                    let reader = operator
                        .reader_with(&segment_key(segment))
                        .gap(RANGE_FETCH_GAP)
                        .content_length_hint(segment.length)
                        .await
                        .map_err(|error| referenced_segment_error("read data segment", error))?;
                    let fetched = reader
                        .fetch(
                            demands
                                .iter()
                                .map(|(offset, length, _)| *offset..*offset + *length)
                                .collect(),
                        )
                        .await
                        .map_err(|error| referenced_segment_error("read data segment", error))?;
                    let mut bytes = BTreeMap::new();
                    for (demand, buffer) in demands.iter().copied().zip(fetched) {
                        verify_range_demand(&buffer, demand)?;
                        bytes.insert(demand, buffer);
                    }
                    state.bytes = Some(bytes);
                }
                let bytes = state
                    .bytes
                    .as_ref()
                    .expect("range bytes are initialized above");
                let fetched = extents
                    .iter()
                    .map(|(index, extent)| (*index, bytes[&demand_key(extent)].clone()))
                    .collect();
                state.remaining = state
                    .remaining
                    .checked_sub(extents.len())
                    .expect("each planned extent is consumed once");
                if state.remaining == 0 {
                    state.bytes = None;
                }
                Ok(fetched)
            }
        }
    }
}

fn extent_window_end(extents: &[Extent], start: usize) -> usize {
    let mut end = start;
    let mut bytes = 0_u64;
    while end < extents.len()
        && (end == start
            || bytes.saturating_add(extents[end].content.length) <= MATERIALIZE_WINDOW_BYTES)
    {
        bytes = bytes.saturating_add(extents[end].content.length);
        end += 1;
    }
    end
}

fn demand_key(extent: &Extent) -> DemandKey {
    (extent.segment_offset, extent.content.length, extent.content)
}

fn verify_complete_demands(
    segment: SegmentRef,
    bytes: &Buffer,
    demands: &BTreeSet<DemandKey>,
) -> Result<(), ManagedError> {
    verify_complete_segment(segment, bytes)?;
    for demand @ (offset, length, _) in demands {
        let start = usize::try_from(*offset)
            .map_err(|_| corrupt("read data segment", "extent offset exceeds this process"))?;
        let length = usize::try_from(*length)
            .map_err(|_| corrupt("read data segment", "extent length exceeds this process"))?;
        let end = start
            .checked_add(length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| corrupt("read data segment", "extent exceeds data segment"))?;
        verify_range_demand(&bytes.slice(start..end), *demand)?;
    }
    Ok(())
}

fn verify_range_demand(bytes: &Buffer, demand: DemandKey) -> Result<(), ManagedError> {
    let (_, length, content) = demand;
    if bytes.len() as u64 != length {
        return Err(corrupt(
            "read data segment",
            "segment range returned an unexpected length",
        ));
    }
    if buffer_content_ref(bytes) != content {
        return Err(corrupt(
            "read data segment",
            "extent bytes do not match their content reference",
        ));
    }
    Ok(())
}

fn slice_extent(bytes: &Buffer, range_start: u64, extent: &Extent) -> Result<Buffer, ManagedError> {
    let start = extent
        .segment_offset
        .checked_sub(range_start)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| corrupt("read data segment", "extent range is invalid"))?;
    let length = usize::try_from(extent.content.length)
        .map_err(|_| corrupt("read data segment", "extent length exceeds this process"))?;
    let end = start
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| corrupt("read data segment", "extent exceeds fetched range"))?;
    Ok(bytes.slice(start..end))
}

fn buffer_content_ref(bytes: &Buffer) -> ContentRef {
    let mut digest = Sha256::new();
    for chunk in bytes.clone() {
        digest.update(&chunk);
    }
    ContentRef {
        digest: digest.finalize().into(),
        length: bytes.len() as u64,
    }
}

fn prefer_complete_segment(segment: SegmentRef, demands: &SegmentDemand, full_tree: bool) -> bool {
    let mut requests = 0_u64;
    let mut transferred = 0_u64;
    let mut span: Option<Range<u64>> = None;
    for (offset, length, _) in demands.keys() {
        let range = *offset..*offset + *length;
        match span.as_mut() {
            Some(current) if range.start.saturating_sub(current.end) <= RANGE_FETCH_GAP as u64 => {
                current.end = current.end.max(range.end);
            }
            Some(current) => {
                requests += 1;
                transferred = transferred.saturating_add(current.end - current.start);
                *current = range;
            }
            None => span = Some(range),
        }
    }
    if let Some(current) = span {
        requests += 1;
        transferred = transferred.saturating_add(current.end - current.start);
    }
    // A cold Foyer miss performs one stat and one full read. Sparse ranges are
    // preferable unless the complete read removes at least one remote request.
    if requests <= 2 {
        return false;
    }

    let saved_requests = requests - 2;
    let byte_budget = REQUEST_EQUIVALENT_BYTES * if full_tree { 4 } else { 1 };
    segment.length.saturating_sub(transferred) <= saved_requests.saturating_mul(byte_budget)
}

async fn stream_file(
    source: &Operator,
    staging: &Operator,
    path: &str,
    sender: &mpsc::Sender<PreparedChunk>,
) -> Result<(u64, [u8; 32]), ManagedError> {
    let metadata = source
        .stat(path)
        .await
        .map_err(|_| unavailable("read frozen file"))?;
    if !metadata.is_file() {
        return Err(invalid("read frozen file", "input is not a regular file"));
    }
    let size = metadata.content_length();
    if size == 0 {
        staging
            .write(path, Vec::<u8>::new())
            .await
            .map_err(|_| unavailable("write frozen file"))?;
        return Ok((size, Sha256::digest([]).into()));
    }
    if size < FASTCDC_MINIMUM_FILE_SIZE {
        let bytes = source
            .read(path)
            .await
            .map_err(|_| unavailable("read frozen file"))?
            .to_bytes()
            .to_vec();
        if bytes.len() as u64 != size {
            return Err(invalid(
                "read frozen file",
                "frozen input changed while it was being staged",
            ));
        }
        staging
            .write(path, bytes.clone())
            .await
            .map_err(|_| unavailable("write frozen file"))?;
        let content = content_ref(&bytes);
        sender
            .send(PreparedChunk {
                extent: PreparedExtent {
                    logical_offset: 0,
                    content,
                },
                bytes,
            })
            .await
            .map_err(|_| unavailable("stage Managed files"))?;
        return Ok((size, content.digest));
    }

    let digest = stream_fastcdc(
        source,
        staging,
        path,
        size,
        FastCdcSizes {
            minimum: FASTCDC_MINIMUM_SIZE,
            target: FASTCDC_TARGET_SIZE,
            maximum: FASTCDC_MAXIMUM_SIZE,
        },
        sender,
    )
    .await?;
    Ok((size, digest))
}

async fn stream_fastcdc(
    source: &Operator,
    staging: &Operator,
    path: &str,
    size: u64,
    sizes: FastCdcSizes,
    sender: &mpsc::Sender<PreparedChunk>,
) -> Result<[u8; 32], ManagedError> {
    let reader = source
        .reader(path)
        .await
        .map_err(|_| unavailable("read frozen file"))?
        .into_bytes_stream(..)
        .await
        .map_err(|_| unavailable("read frozen file"))?;
    let writer = staging
        .writer(path)
        .await
        .map_err(|_| unavailable("write frozen file"))?;
    let reader: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Vec<u8>>> + Send>> =
        Box::pin(stream::try_unfold(
            (Box::pin(reader), Some(writer)),
            |(mut reader, mut writer)| async move {
                match reader.next().await {
                    Some(Ok(buffer)) => {
                        writer
                            .as_mut()
                            .expect("writer remains open while input has data")
                            .write(buffer.clone())
                            .await
                            .map_err(std::io::Error::other)?;
                        Ok(Some((buffer.to_vec(), (reader, writer))))
                    }
                    Some(Err(error)) => Err(std::io::Error::other(error)),
                    None => {
                        writer
                            .take()
                            .expect("writer closes exactly once")
                            .close()
                            .await
                            .map_err(std::io::Error::other)?;
                        Ok(None)
                    }
                }
            },
        ));
    let reader = reader.into_async_read();
    let mut chunker = AsyncStreamCDC::new(reader, sizes.minimum, sizes.target, sizes.maximum);
    let chunks = chunker.as_stream();
    futures::pin_mut!(chunks);
    let mut logical = Sha256::new();
    let mut observed = 0_u64;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| unavailable("chunk frozen file"))?;
        if chunk.offset != observed {
            return Err(invalid(
                "read frozen file",
                "frozen input changed while it was being staged",
            ));
        }
        logical.update(&chunk.data);
        let content = content_ref(&chunk.data);
        observed = observed
            .checked_add(content.length)
            .ok_or_else(|| invalid("read frozen file", "frozen input length overflows"))?;
        sender
            .send(PreparedChunk {
                extent: PreparedExtent {
                    logical_offset: chunk.offset,
                    content,
                },
                bytes: chunk.data,
            })
            .await
            .map_err(|_| unavailable("stage Managed files"))?;
    }
    if observed != size {
        return Err(invalid(
            "read frozen file",
            "frozen input changed while it was being staged",
        ));
    }
    Ok(logical.finalize().into())
}

fn take_segment_contents(
    contents: &mut BTreeMap<ContentRef, Vec<u8>>,
) -> Result<BTreeMap<ContentRef, Vec<u8>>, ManagedError> {
    let mut batch = BTreeMap::new();
    let mut batch_size = 0_u64;
    while let Some((&content, _)) = contents.first_key_value() {
        if !batch.is_empty() && batch_size.saturating_add(content.length) > TARGET_SEGMENT_SIZE {
            break;
        }
        batch_size = batch_size
            .checked_add(content.length)
            .ok_or_else(|| invalid("seal data segment", "segment content length overflows"))?;
        let (_, bytes) = contents
            .pop_first()
            .expect("content observed immediately before removal");
        batch.insert(content, bytes);
    }
    Ok(batch)
}

fn seal_segment(contents: BTreeMap<ContentRef, Vec<u8>>) -> Result<SealedSegment, ManagedError> {
    // Segment v1 is the stable ContentRef ordering of its raw contents. The
    // descriptor owns offsets; SegmentRef authenticates the complete object.
    let mut encoded = Vec::new();
    let mut offsets = BTreeMap::new();
    for (content, bytes) in contents {
        if content.length == 0 || content.length != bytes.len() as u64 {
            return Err(invalid(
                "seal data segment",
                "segment entry does not match its content reference",
            ));
        }
        let offset = encoded.len() as u64;
        encoded.extend_from_slice(&bytes);
        offsets.insert(content, offset);
    }
    if offsets.is_empty() {
        return Err(invalid(
            "seal data segment",
            "a segment must contain non-empty content",
        ));
    }
    let digest: [u8; 32] = Sha256::digest(&encoded).into();
    let reference = SegmentRef {
        digest,
        length: encoded.len() as u64,
    };
    let locations = offsets
        .into_iter()
        .map(|(content, offset)| {
            (
                content,
                StoredContent {
                    segment: reference,
                    offset,
                },
            )
        })
        .collect();
    Ok(SealedSegment {
        reference,
        bytes: encoded,
        locations,
    })
}

fn verify_complete_segment(reference: SegmentRef, bytes: &Buffer) -> Result<(), ManagedError> {
    if bytes.len() as u64 != reference.length
        || buffer_content_ref(bytes).digest != reference.digest
    {
        return Err(corrupt(
            "read data segment",
            "segment does not match its reference",
        ));
    }
    Ok(())
}

fn visit_reachable_file_versions(
    snapshot: &NamespaceSnapshot,
    action: &'static str,
    mut visit: impl FnMut(&FileVersionRecord) -> Result<(), ManagedError>,
) -> Result<(), ManagedError> {
    let mut visited = BTreeSet::new();
    let mut pending = vec![snapshot.root];
    while let Some(node_id) = pending.pop() {
        if !visited.insert(node_id) {
            continue;
        }
        let node = snapshot
            .nodes
            .get(&node_id)
            .ok_or_else(|| corrupt(action, "namespace references a missing node"))?;
        match node.kind {
            NodeKind::Directory => {
                let directory = snapshot
                    .directories
                    .get(&node_id)
                    .ok_or_else(|| corrupt(action, "namespace references a missing directory"))?;
                pending.extend(directory.entries.values().map(|entry| entry.node));
            }
            NodeKind::RegularFile => {
                let version_id = node
                    .file_version
                    .ok_or_else(|| corrupt(action, "live file has no file version"))?;
                let version = snapshot.file_versions.get(&version_id).ok_or_else(|| {
                    corrupt(action, "live node references a missing file version")
                })?;
                if !version.is_valid() {
                    return Err(corrupt(
                        action,
                        "live node references an invalid file version",
                    ));
                }
                visit(version)?;
            }
        }
    }
    Ok(())
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        digest: Sha256::digest(bytes).into(),
        length: bytes.len() as u64,
    }
}

fn segment_key(reference: SegmentRef) -> String {
    let digest = hex(&reference.digest);
    format!("{SEGMENT_ROOT}/{}/{}.seg", &digest[..2], digest)
}

fn segment_ref_from_key(path: &str, length: u64) -> Option<SegmentRef> {
    let relative = path.strip_prefix(&format!("{SEGMENT_ROOT}/"))?;
    let (partition, encoded) = relative.split_once('/')?;
    let encoded = encoded.strip_suffix(".seg")?;
    if partition.len() != 2 || encoded.len() != 64 || partition != &encoded[..2] {
        return None;
    }
    Some(SegmentRef {
        digest: parse_hex(encoded)?,
        length,
    })
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn parse_hex(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut output = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Some(output)
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn referenced_segment_error(action: &'static str, error: opendal::Error) -> ManagedError {
    if error.kind() == ErrorKind::NotFound {
        corrupt(action, "file version references a missing data segment")
    } else {
        unavailable(action)
    }
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "storage operation failed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::services;

    fn memory() -> Operator {
        Operator::new(services::Memory::default()).unwrap().finish()
    }

    #[tokio::test]
    async fn stages_and_materializes_whole_and_chunked_files() {
        let source = memory();
        let staging = memory();
        let storage = memory();
        let target = memory();
        let small = b"portable segment".to_vec();
        let large = (0..2 * 1024 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        source.write("small", small.clone()).await.unwrap();
        source.write("large", large.clone()).await.unwrap();
        let data = ManagedData::new(storage).unwrap();

        let staged = data
            .stage_files(
                &source,
                &staging,
                vec!["small".to_owned(), "large".to_owned()],
                &AuthorityKnownContent::default(),
                NonZeroUsize::new(2).unwrap(),
            )
            .await
            .unwrap();
        data.materialize(
            &target,
            staged.into_iter().collect(),
            false,
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(target.read("small").await.unwrap().to_bytes(), small);
        assert_eq!(target.read("large").await.unwrap().to_bytes(), large);
    }

    #[tokio::test]
    async fn materializes_repeated_content_extents() {
        let storage = memory();
        let target = memory();
        let bytes = b"repeated";
        let content = content_ref(bytes);
        let segment = seal_segment(BTreeMap::from([(content, bytes.to_vec())])).unwrap();
        let reference = segment.reference;
        let key = segment_key(reference);
        storage.write(&key, segment.bytes).await.unwrap();
        let logical = [bytes.as_slice(), bytes.as_slice()].concat();
        let version = FileVersionRecord::from_extents(
            logical.len() as u64,
            Sha256::digest(&logical).into(),
            ExtentMap {
                extents: vec![
                    Extent {
                        logical_offset: 0,
                        content,
                        segment: reference,
                        segment_offset: 0,
                    },
                    Extent {
                        logical_offset: content.length,
                        content,
                        segment: reference,
                        segment_offset: 0,
                    },
                ],
            },
        )
        .unwrap();

        ManagedData::new(storage)
            .unwrap()
            .materialize(
                &target,
                vec![("output".to_owned(), version)],
                false,
                NonZeroUsize::new(2).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(target.read("output").await.unwrap().to_bytes(), logical);
    }

    #[test]
    fn complete_segment_rejects_corruption() {
        let content = content_ref(b"verified bytes");
        let mut segment =
            seal_segment(BTreeMap::from([(content, b"verified bytes".to_vec())])).unwrap();
        assert!(
            verify_complete_segment(segment.reference, &Buffer::from(segment.bytes.clone()))
                .is_ok()
        );
        segment.bytes[0] ^= 1;
        assert!(verify_complete_segment(segment.reference, &Buffer::from(segment.bytes)).is_err());
    }
}
