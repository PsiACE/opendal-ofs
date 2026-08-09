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
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use fastcdc::v2020::AsyncStreamCDC;
use foyer::HybridCacheBuilder;
use futures::{StreamExt, TryStreamExt, stream};
use opendal::layers::{FoyerKey, FoyerLayer, FoyerValue};
use opendal::{Buffer, ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, OnceCell, mpsc};

use super::{ManagedError, ManagedErrorKind};
use crate::filesystem::NodeKind;
use crate::managed::format::{ContentRef, Extent, ExtentMap, SegmentRef};
use crate::managed::metadata::namespace::{FileVersionRecord, NamespaceSnapshot};
use crate::managed::metadata::object::ensure_immutable;

const SEGMENT_ROOT: &str = ".ofs/managed/data/v1/segments/sha256";
const SEGMENT_MAGIC: &[u8; 8] = b"OFSSEG01";
const TRAILER_MAGIC: &[u8; 8] = b"OFSSEGTR";
const FORMAT_MAJOR: u16 = 1;
const HEADER_LENGTH: u64 = 10;
const TRAILER_LENGTH: u64 = 56;
const REQUEST_EQUIVALENT_BYTES: u64 = 4 * 1024;
const RANGE_FETCH_GAP: usize = 256 * 1024;
// Placement policy. These values are not part of the durable format.
const TARGET_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
const MATERIALIZE_WINDOW_BYTES: u64 = TARGET_SEGMENT_SIZE;
const MATERIALIZE_CACHE_BYTES: usize = 64 * 1024 * 1024;
const FASTCDC_MINIMUM_FILE_SIZE: u64 = 1024 * 1024;
const FASTCDC_MINIMUM_SIZE: u32 = 64 * 1024;
const FASTCDC_TARGET_SIZE: u32 = 256 * 1024;
const FASTCDC_MAXIMUM_SIZE: u32 = 1024 * 1024;
const DELETE_BATCH_SIZE: usize = 1000;

/// Data segments removed by one namespace-fenced garbage-collection sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentGcMaintenance {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
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
    pub(crate) fn from_snapshot(snapshot: &NamespaceSnapshot) -> Result<Self, ManagedError> {
        let mut known = BTreeMap::new();
        visit_reachable_file_versions(snapshot, "derive authority-known content", |version| {
            for extent in &version.extent_map.extents {
                known.entry(extent.content).or_insert(StoredContent {
                    segment: extent.segment,
                    offset: extent.segment_offset,
                });
            }
        })?;
        Ok(Self(known))
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
    logical_digest: Option<[u8; 32]>,
    extents: Vec<PreparedExtent>,
}

#[derive(Debug)]
struct PreparedExtent {
    logical_offset: u64,
    content: ContentRef,
}

enum PreparedEvent {
    Start {
        path: String,
        logical_size: u64,
    },
    Extent {
        path: String,
        extent: PreparedExtent,
        bytes: Vec<u8>,
    },
    Finish {
        path: String,
        logical_digest: [u8; 32],
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Footer {
    major: u16,
    entries: Vec<FooterEntry>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct FooterEntry {
    content: ContentRef,
    offset: u64,
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
        plan: RangeReadPlan,
    },
}

struct RangeReadPlan {
    segment: SegmentRef,
    demands: BTreeSet<DemandKey>,
    state: Mutex<RangeReadState>,
}

struct RangeReadState {
    bytes: Option<BTreeMap<DemandKey, Buffer>>,
    remaining: usize,
}

enum WindowRead<'a> {
    Complete {
        index: usize,
        extent: Extent,
        demands: &'a BTreeSet<DemandKey>,
        verified: &'a OnceCell<()>,
    },
    Ranged {
        plan: &'a RangeReadPlan,
        extents: Vec<(usize, Extent)>,
    },
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
        paths: Vec<String>,
        known: &AuthorityKnownContent,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersionRecord>, ManagedError> {
        let (sender, mut receiver) = mpsc::channel(concurrency.get().saturating_mul(2).max(1));
        let producer_source = source.clone();
        let producer_sender = sender.clone();
        let producers = stream::iter(paths)
            .map(move |path| {
                let source = producer_source.clone();
                let sender = producer_sender.clone();
                async move { stream_file(&source, path, &sender).await }
            })
            .buffer_unordered(concurrency.get())
            .try_collect::<Vec<_>>();
        drop(sender);

        let collect = async {
            let mut files = BTreeMap::<String, PreparedFile>::new();
            let mut new_content = BTreeMap::<ContentRef, Vec<u8>>::new();
            let mut pending_bytes = 0_u64;
            let mut created = BTreeMap::new();
            while let Some(event) = receiver.recv().await {
                match event {
                    PreparedEvent::Start { path, logical_size } => {
                        if files
                            .insert(
                                path,
                                PreparedFile {
                                    logical_size,
                                    logical_digest: None,
                                    extents: Vec::new(),
                                },
                            )
                            .is_some()
                        {
                            return Err(invalid("stage Managed files", "input path is repeated"));
                        }
                    }
                    PreparedEvent::Extent {
                        path,
                        extent,
                        bytes,
                    } => {
                        let file = files.get_mut(&path).ok_or_else(|| {
                            corrupt(
                                "stage Managed files",
                                "file extent arrived before its start",
                            )
                        })?;
                        if file.logical_digest.is_some() {
                            return Err(corrupt(
                                "stage Managed files",
                                "file extent arrived after its finish",
                            ));
                        }
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
                        file.extents.push(extent);
                    }
                    PreparedEvent::Finish {
                        path,
                        logical_digest,
                    } => {
                        let file = files.get_mut(&path).ok_or_else(|| {
                            corrupt("stage Managed files", "file finished before its start")
                        })?;
                        if file.logical_digest.replace(logical_digest).is_some() {
                            return Err(corrupt(
                                "stage Managed files",
                                "file finished more than once",
                            ));
                        }
                    }
                }

                while pending_bytes >= TARGET_SEGMENT_SIZE {
                    let contents = take_segment_contents(&mut new_content)?;
                    pending_bytes -= contents.keys().map(|content| content.length).sum::<u64>();
                    created.extend(self.create_segment(seal_segment(contents)?).await?);
                }
            }

            while !new_content.is_empty() {
                let contents = take_segment_contents(&mut new_content)?;
                created.extend(self.create_segment(seal_segment(contents)?).await?);
            }

            files
                .into_iter()
                .map(|(path, file)| {
                    let logical_digest = file
                        .logical_digest
                        .ok_or_else(|| corrupt("stage Managed files", "file did not finish"))?;
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
                        logical_digest,
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
        for (_, version) in &requests {
            validate_materialized_version(version)?;
        }
        let plans = MaterializationPlan::new(
            &requests,
            full_tree,
            self.operator.info().full_capability().stat,
        );
        let cached = if plans.has_complete_segments() {
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
            .try_collect()
            .await
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
            let mut end = start;
            let mut window_bytes = 0_u64;
            while end < extents.len()
                && (end == start
                    || window_bytes.saturating_add(extents[end].content.length)
                        <= MATERIALIZE_WINDOW_BYTES)
            {
                window_bytes = window_bytes.saturating_add(extents[end].content.length);
                end += 1;
            }
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
        let mut reads = Vec::new();
        let mut ranged = BTreeMap::<SegmentRef, (&RangeReadPlan, Vec<(usize, Extent)>)>::new();
        for (index, extent) in extents.iter().copied().enumerate() {
            match &context.plans.segments[&extent.segment] {
                SegmentReadPlan::Complete { demands, verified } => {
                    reads.push(WindowRead::Complete {
                        index,
                        extent,
                        demands,
                        verified,
                    });
                }
                SegmentReadPlan::Ranged { plan } => {
                    ranged
                        .entry(plan.segment)
                        .or_insert_with(|| (plan, Vec::new()))
                        .1
                        .push((index, extent));
                }
            }
        }
        reads.extend(
            ranged
                .into_values()
                .map(|(plan, extents)| WindowRead::Ranged { plan, extents }),
        );

        let fetched = stream::iter(reads)
            .map(|read| async move {
                match read {
                    WindowRead::Complete {
                        index,
                        extent,
                        demands,
                        verified,
                    } => self
                        .read_complete_extent(context, &extent, demands, verified)
                        .await
                        .map(|bytes| vec![(index, bytes)]),
                    WindowRead::Ranged { plan, extents } => {
                        plan.read_extents(&self.operator, &extents).await
                    }
                }
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

    async fn read_complete_extent(
        &self,
        context: &MaterializationContext,
        extent: &Extent,
        demands: &BTreeSet<DemandKey>,
        verified: &OnceCell<()>,
    ) -> Result<Buffer, ManagedError> {
        let bytes = context
            .cached
            .read(&segment_key(extent.segment))
            .await
            .map_err(|error| referenced_segment_error("read data segment", error))?;
        verified
            .get_or_try_init(|| async {
                verify_complete_demands(extent.segment, &bytes.clone().to_bytes(), demands)
            })
            .await?;
        slice_extent(&bytes, 0, extent)
    }

    pub(crate) async fn collect_unreachable_segments(
        &self,
        snapshot: &NamespaceSnapshot,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        self.collect_unreachable_segments_from([snapshot]).await
    }

    pub(crate) async fn collect_unreachable_segments_from<'a>(
        &self,
        snapshots: impl IntoIterator<Item = &'a NamespaceSnapshot>,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let capability = self.operator.info().full_capability();
        if !capability.list || !capability.delete {
            return Err(unavailable("collect unreachable data segments"));
        }
        let mut live = BTreeSet::new();
        for snapshot in snapshots {
            live.extend(reachable_segments(
                snapshot,
                "collect unreachable data segments",
            )?);
        }
        let mut result = SegmentGcMaintenance::default();
        let mut unreachable = Vec::new();
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
            if live.contains(&reference) {
                continue;
            }
            if live
                .iter()
                .any(|candidate| candidate.digest == reference.digest)
            {
                return Err(corrupt(
                    "collect unreachable data segments",
                    "live segment has an unexpected physical length",
                ));
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
            unreachable.push(entry.path().to_owned());
            result.deleted += 1;
            result.deleted_bytes = deleted_bytes;
            if unreachable.len() == DELETE_BATCH_SIZE {
                self.operator
                    .delete_iter(unreachable.iter().map(String::as_str))
                    .await
                    .map_err(|_| unavailable("delete unreachable data segments"))?;
                unreachable.clear();
            }
        }
        if !unreachable.is_empty() {
            self.operator
                .delete_iter(unreachable.iter().map(String::as_str))
                .await
                .map_err(|_| unavailable("delete unreachable data segments"))?;
        }
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
        for (_, version) in requests {
            for extent in &version.extent_map.extents {
                *segments
                    .entry(extent.segment)
                    .or_default()
                    .entry(demand_key(extent))
                    .or_default() += 1;
            }
        }
        Self {
            segments: segments
                .into_iter()
                .map(|(segment, demands)| {
                    let plan = if cache_complete_segments
                        && usize::try_from(segment.length)
                            .is_ok_and(|length| length <= MATERIALIZE_CACHE_BYTES)
                        && prefer_complete_segment(segment, &demands, full_tree)
                    {
                        SegmentReadPlan::Complete {
                            demands: demands.keys().copied().collect(),
                            verified: OnceCell::new(),
                        }
                    } else {
                        SegmentReadPlan::Ranged {
                            plan: plan_range_read(segment, demands),
                        }
                    };
                    (segment, plan)
                })
                .collect(),
        }
    }

    fn has_complete_segments(&self) -> bool {
        self.segments
            .values()
            .any(|plan| matches!(plan, SegmentReadPlan::Complete { .. }))
    }
}

impl RangeReadPlan {
    async fn read_extents(
        &self,
        operator: &Operator,
        extents: &[(usize, Extent)],
    ) -> Result<Vec<(usize, Buffer)>, ManagedError> {
        let mut state = self.state.lock().await;
        if state.bytes.is_none() {
            let reader = operator
                .reader_with(&segment_key(self.segment))
                .gap(RANGE_FETCH_GAP)
                .content_length_hint(self.segment.length)
                .await
                .map_err(|error| referenced_segment_error("read data segment", error))?;
            let fetched = reader
                .fetch(
                    self.demands
                        .iter()
                        .map(|(offset, length, _)| *offset..*offset + *length)
                        .collect(),
                )
                .await
                .map_err(|error| referenced_segment_error("read data segment", error))?;
            let mut bytes = BTreeMap::new();
            for (demand, buffer) in self.demands.iter().copied().zip(fetched) {
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

fn plan_range_read(segment: SegmentRef, demands: SegmentDemand) -> RangeReadPlan {
    let remaining = demands.values().sum();
    RangeReadPlan {
        segment,
        demands: demands.keys().copied().collect(),
        state: Mutex::new(RangeReadState {
            bytes: None,
            remaining,
        }),
    }
}

fn demand_key(extent: &Extent) -> DemandKey {
    (extent.segment_offset, extent.content.length, extent.content)
}

fn verify_complete_demands(
    segment: SegmentRef,
    bytes: &[u8],
    demands: &BTreeSet<DemandKey>,
) -> Result<(), ManagedError> {
    let entries = verify_complete_segment(segment, bytes)?;
    for (offset, length, content) in demands {
        let agrees = entries.get(content).is_some_and(|range| {
            range.start as u64 == *offset && (range.end - range.start) as u64 == *length
        });
        if !agrees {
            return Err(corrupt(
                "read data segment",
                "file extent disagrees with the segment footer",
            ));
        }
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
    path: String,
    sender: &mpsc::Sender<PreparedEvent>,
) -> Result<(), ManagedError> {
    let metadata = source
        .stat(&path)
        .await
        .map_err(|_| unavailable("read frozen file"))?;
    if !metadata.is_file() {
        return Err(invalid("read frozen file", "input is not a regular file"));
    }
    let size = metadata.content_length();
    send_prepared(
        sender,
        PreparedEvent::Start {
            path: path.clone(),
            logical_size: size,
        },
    )
    .await?;
    if size == 0 {
        return send_prepared(
            sender,
            PreparedEvent::Finish {
                path,
                logical_digest: Sha256::digest([]).into(),
            },
        )
        .await;
    }
    if size < FASTCDC_MINIMUM_FILE_SIZE {
        let bytes = source
            .read(&path)
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
        let content = content_ref(&bytes);
        send_prepared(
            sender,
            PreparedEvent::Extent {
                path: path.clone(),
                extent: PreparedExtent {
                    logical_offset: 0,
                    content,
                },
                bytes,
            },
        )
        .await?;
        return send_prepared(
            sender,
            PreparedEvent::Finish {
                path,
                logical_digest: content.digest,
            },
        )
        .await;
    }

    stream_fastcdc(
        source,
        path,
        size,
        FastCdcSizes {
            minimum: FASTCDC_MINIMUM_SIZE,
            target: FASTCDC_TARGET_SIZE,
            maximum: FASTCDC_MAXIMUM_SIZE,
        },
        sender,
    )
    .await
}

async fn stream_fastcdc(
    source: &Operator,
    path: String,
    size: u64,
    sizes: FastCdcSizes,
    sender: &mpsc::Sender<PreparedEvent>,
) -> Result<(), ManagedError> {
    let reader = source
        .reader(&path)
        .await
        .map_err(|_| unavailable("read frozen file"))?
        .into_futures_async_read(..)
        .await
        .map_err(|_| unavailable("read frozen file"))?;
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
        send_prepared(
            sender,
            PreparedEvent::Extent {
                path: path.clone(),
                extent: PreparedExtent {
                    logical_offset: chunk.offset,
                    content,
                },
                bytes: chunk.data,
            },
        )
        .await?;
    }
    if observed != size {
        return Err(invalid(
            "read frozen file",
            "frozen input changed while it was being staged",
        ));
    }
    send_prepared(
        sender,
        PreparedEvent::Finish {
            path,
            logical_digest: logical.finalize().into(),
        },
    )
    .await
}

async fn send_prepared(
    sender: &mpsc::Sender<PreparedEvent>,
    event: PreparedEvent,
) -> Result<(), ManagedError> {
    sender
        .send(event)
        .await
        .map_err(|_| unavailable("stage Managed files"))
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
    let mut encoded = Vec::new();
    encoded.extend_from_slice(SEGMENT_MAGIC);
    encoded.extend_from_slice(&FORMAT_MAJOR.to_be_bytes());
    let mut entries = Vec::with_capacity(contents.len());
    for (content, bytes) in contents {
        if content.length == 0 || content_ref(&bytes) != content {
            return Err(invalid(
                "seal data segment",
                "segment entry does not match its content reference",
            ));
        }
        let offset = encoded.len() as u64;
        encoded.extend_from_slice(&bytes);
        entries.push(FooterEntry { content, offset });
    }
    if entries.is_empty() {
        return Err(invalid(
            "seal data segment",
            "a segment must contain non-empty content",
        ));
    }
    let footer_offset = encoded.len() as u64;
    let footer = encode(&Footer {
        major: FORMAT_MAJOR,
        entries: entries.clone(),
    })?;
    encoded.extend_from_slice(&footer);
    encoded.extend_from_slice(TRAILER_MAGIC);
    encoded.extend_from_slice(&footer_offset.to_be_bytes());
    encoded.extend_from_slice(&(footer.len() as u64).to_be_bytes());
    let digest: [u8; 32] = Sha256::digest(&encoded).into();
    encoded.extend_from_slice(&digest);
    let reference = SegmentRef {
        digest,
        length: encoded.len() as u64,
    };
    let locations = entries
        .into_iter()
        .map(|entry| {
            (
                entry.content,
                StoredContent {
                    segment: reference,
                    offset: entry.offset,
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

fn verify_complete_segment(
    reference: SegmentRef,
    bytes: &[u8],
) -> Result<BTreeMap<ContentRef, Range<usize>>, ManagedError> {
    if bytes.len() as u64 != reference.length || reference.length < HEADER_LENGTH + TRAILER_LENGTH {
        return Err(corrupt(
            "read data segment",
            "segment length does not match its reference",
        ));
    }
    let trailer_offset = bytes.len() - TRAILER_LENGTH as usize;
    let trailer = &bytes[trailer_offset..];
    if &trailer[..8] != TRAILER_MAGIC {
        return Err(corrupt("read data segment", "segment trailer is invalid"));
    }
    let footer_offset = u64_at(trailer, 8);
    let footer_length = u64_at(trailer, 16);
    let digest: [u8; 32] = trailer[24..56]
        .try_into()
        .expect("segment trailer digest has fixed length");
    if digest != reference.digest
        || footer_offset < HEADER_LENGTH
        || footer_offset.checked_add(footer_length) != Some(trailer_offset as u64)
        || <[u8; 32]>::from(Sha256::digest(&bytes[..bytes.len() - 32])) != digest
        || &bytes[..8] != SEGMENT_MAGIC
        || u16_at(bytes, 8) != FORMAT_MAJOR
    {
        return Err(corrupt(
            "read data segment",
            "segment envelope does not match its reference",
        ));
    }
    let footer: Footer = decode(&bytes[footer_offset as usize..trailer_offset])?;
    if footer.major != FORMAT_MAJOR {
        return Err(corrupt("read data segment", "segment footer is invalid"));
    }
    let mut locations = BTreeMap::new();
    let mut previous_content = None;
    let mut previous_end = HEADER_LENGTH;
    for entry in footer.entries {
        let end = entry.offset.checked_add(entry.content.length);
        if previous_content.is_some_and(|content| content >= entry.content)
            || entry.offset != previous_end
            || end.is_none_or(|end| end > footer_offset)
        {
            return Err(corrupt(
                "read data segment",
                "segment footer entry is invalid",
            ));
        }
        let end = end.expect("checked above");
        let range = entry.offset as usize..end as usize;
        if content_ref(&bytes[range.clone()]) != entry.content {
            return Err(corrupt(
                "read data segment",
                "segment entry fails content validation",
            ));
        }
        previous_content = Some(entry.content);
        previous_end = end;
        locations.insert(entry.content, range);
    }
    if previous_end != footer_offset {
        return Err(corrupt(
            "read data segment",
            "segment data region is not fully described",
        ));
    }
    Ok(locations)
}

fn validate_extent(extent: &Extent) -> Result<(), ManagedError> {
    let end = extent
        .segment_offset
        .checked_add(extent.content.length)
        .filter(|end| *end <= extent.segment.length.saturating_sub(TRAILER_LENGTH));
    if extent.content.length == 0 || extent.segment_offset < HEADER_LENGTH || end.is_none() {
        return Err(corrupt(
            "read data segment",
            "file extent has an invalid segment range",
        ));
    }
    Ok(())
}

fn validate_materialized_version(version: &FileVersionRecord) -> Result<(), ManagedError> {
    if !version.is_valid() {
        return Err(corrupt(
            "materialize Managed files",
            "file version identity is invalid",
        ));
    }
    for extent in &version.extent_map.extents {
        validate_extent(extent)?;
    }
    Ok(())
}

fn reachable_segments(
    snapshot: &NamespaceSnapshot,
    action: &'static str,
) -> Result<BTreeSet<SegmentRef>, ManagedError> {
    let mut segments = BTreeSet::new();
    visit_reachable_file_versions(snapshot, action, |version| {
        segments.extend(
            version
                .extent_map
                .extents
                .iter()
                .map(|extent| extent.segment),
        );
    })?;
    Ok(segments)
}

fn visit_reachable_file_versions(
    snapshot: &NamespaceSnapshot,
    action: &'static str,
    mut visit: impl FnMut(&FileVersionRecord),
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
                visit(version);
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

fn encode(value: &impl Serialize) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| invalid("seal data segment", "segment footer cannot be encoded"))?;
    Ok(bytes)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ManagedError> {
    let mut cursor = Cursor::new(bytes);
    let value = ciborium::de::from_reader(&mut cursor)
        .map_err(|_| corrupt("read data segment", "segment footer is not valid CBOR"))?;
    if cursor.position() != bytes.len() as u64 {
        return Err(corrupt(
            "read data segment",
            "segment footer has trailing bytes",
        ));
    }
    Ok(value)
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("checked segment envelope"),
    )
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked segment envelope"),
    )
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
                        segment_offset: HEADER_LENGTH,
                    },
                    Extent {
                        logical_offset: content.length,
                        content,
                        segment: reference,
                        segment_offset: HEADER_LENGTH,
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
        assert!(verify_complete_segment(segment.reference, &segment.bytes).is_ok());
        segment.bytes[HEADER_LENGTH as usize] ^= 1;
        assert!(verify_complete_segment(segment.reference, &segment.bytes).is_err());
    }
}
