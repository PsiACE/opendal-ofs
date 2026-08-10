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
use std::sync::Arc;

use fastcdc::v2020::AsyncStreamCDC;
use foyer::HybridCacheBuilder;
use futures::{StreamExt, TryStreamExt, stream};
use opendal::layers::{FoyerKey, FoyerLayer, FoyerValue};
use opendal::{Buffer, ErrorKind, Operator, Writer};
use sha2::{Digest as _, Sha256};
use tokio::sync::{OnceCell, mpsc, oneshot};

use super::error::{corrupt, invalid, unavailable};
use crate::filesystem::VolumeError;
use crate::managed::format::{ContentRef, Extent, ExtentMap, LowerHex, SegmentRef, V1Record};
use crate::managed::metadata::namespace::DecodedFileVersion;
use crate::managed::metadata::object::ensure_immutable;

const SEGMENT_ROOT: &str = ".ofs/managed/data/v1/segments/sha256";
const STAGING_PLAN_KEY: &str = "plan.ofs";
const STAGING_PLAN_RECORD: V1Record = V1Record::new(*b"OFS1DSP1", 64 * 1024 * 1024);
// Placement policy. These values are not part of the durable format.
const TARGET_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
const MATERIALIZE_WINDOW_BYTES: u64 = TARGET_SEGMENT_SIZE;
const MATERIALIZE_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MATERIALIZE_BATCH_BYTES: u64 = MATERIALIZE_CACHE_BYTES as u64;
const FASTCDC_MINIMUM_FILE_SIZE: u64 = 1024 * 1024;
const FASTCDC_MINIMUM_SIZE: u32 = 64 * 1024;
const FASTCDC_TARGET_SIZE: u32 = 256 * 1024;
const FASTCDC_MAXIMUM_SIZE: u32 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StoredContent {
    segment: SegmentRef,
    offset: u64,
}

/// Physical locations already referenced by one fixed authority snapshot.
#[derive(Clone, Debug, Default)]
pub(crate) struct AuthorityKnownContent {
    contents: BTreeMap<ContentRef, StoredContent>,
}

impl AuthorityKnownContent {
    pub(crate) fn include(&mut self, version: &DecodedFileVersion) {
        for extent in &version.extent_map.extents {
            self.contents
                .entry(extent.content)
                .or_insert(StoredContent {
                    segment: extent.segment,
                    offset: extent.segment_offset,
                });
        }
    }

    fn get(&self, content: &ContentRef) -> Option<StoredContent> {
        self.contents.get(content).copied()
    }
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
    completion: oneshot::Receiver<Result<(u64, [u8; 32]), VolumeError>>,
}

#[derive(Debug)]
struct SealedSegment {
    reference: SegmentRef,
    bytes: Vec<u8>,
    locations: BTreeMap<ContentRef, StoredContent>,
}

type DemandKey = (u64, u64, ContentRef);
type SegmentDemand = BTreeSet<DemandKey>;
type FetchedContent = BTreeMap<ContentRef, Buffer>;

/// Data segments removed by one explicit reachability sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentGcMaintenance {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
}

/// Immutable segments retained by one or more fixed namespace roots.
#[derive(Default)]
pub(crate) struct RetainedDataRoots(BTreeMap<[u8; 32], u64>);

impl RetainedDataRoots {
    pub(crate) fn retain(
        &mut self,
        snapshot: &crate::filesystem::VolumeSnapshot,
    ) -> Result<(), VolumeError> {
        for version in snapshot.file_versions.values() {
            self.retain_file_version(version)?;
        }
        Ok(())
    }

    pub(crate) fn retain_file_version(
        &mut self,
        version: &crate::filesystem::FileVersion,
    ) -> Result<(), VolumeError> {
        let version = crate::managed::metadata::namespace::decode_file_version(version)?;
        for extent in version.extent_map.extents {
            if self
                .0
                .insert(extent.segment.digest, extent.segment.length)
                .is_some_and(|length| length != extent.segment.length)
            {
                return Err(corrupt(
                    "mark retained data segments",
                    "one segment digest has conflicting physical lengths",
                ));
            }
        }
        Ok(())
    }
}

/// The Managed v1 data plane.
#[derive(Clone)]
pub(crate) struct ManagedData {
    operator: Operator,
    cached: Arc<OnceCell<Operator>>,
}

impl ManagedData {
    pub(crate) fn new(operator: Operator) -> Result<Self, VolumeError> {
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
        segment_staging: &Operator,
        paths: Vec<String>,
        known: &AuthorityKnownContent,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, DecodedFileVersion>, VolumeError> {
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
            let producer_path = path.clone();
            let (sender, chunks) = mpsc::channel(2);
            let (complete, completion) = oneshot::channel();
            producer_tasks.push(async move {
                let result = stream_file(&source, &producer_path, &sender).await;
                drop(sender);
                let _ = complete.send(result);
                Ok::<(), VolumeError>(())
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
                        let segment = seal_segment(contents)?;
                        created.extend(stage_segment(segment_staging, segment).await?);
                    }
                }
                let (logical_size, logical_digest) =
                    prepared.completion.await.map_err(|_| {
                        unavailable("stage Managed files", "storage operation failed")
                    })??;
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
                let segment = seal_segment(contents)?;
                created.extend(stage_segment(segment_staging, segment).await?);
            }

            let segments = created
                .values()
                .map(|stored| stored.segment)
                .collect::<BTreeSet<_>>();
            write_staging_plan(segment_staging, &segments).await?;

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
                            .collect::<Result<Vec<_>, VolumeError>>()?,
                    };
                    let version = DecodedFileVersion::from_extents(
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

    /// Publish the immutable segments frozen by `stage_files`.
    pub(crate) async fn finalize_staged_files(
        &self,
        segment_staging: &Operator,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        let segments = read_staging_plan(segment_staging).await?;

        // Every job retains at most one placement-sized segment. The same
        // storage execution width bounds both preparation and immutable writes.
        stream::iter(segments)
            .map(|reference| async move {
                let key = segment_key(reference);
                let segment = segment_staging.read(&key).await.map_err(|error| {
                    if error.kind() == ErrorKind::NotFound {
                        corrupt("finalize Managed files", "staged data segment is missing")
                    } else {
                        unavailable("finalize Managed files", "storage operation failed")
                    }
                })?;
                verify_complete_segment(reference, &segment)?;
                ensure_immutable(&self.operator, &key, segment, "create data segment").await
            })
            .buffer_unordered(concurrency.get())
            .try_for_each(|()| async { Ok(()) })
            .await
    }

    pub(crate) async fn collect_unreachable_segments(
        &self,
        roots: &RetainedDataRoots,
    ) -> Result<SegmentGcMaintenance, VolumeError> {
        let capability = self.operator.info().full_capability();
        if !capability.list || !capability.delete {
            return Err(unavailable(
                "collect unreachable data segments",
                "data storage requires list and delete",
            ));
        }
        let mut result = SegmentGcMaintenance::default();
        let mut deleter = self.operator.deleter().await.map_err(|_| {
            unavailable(
                "collect unreachable data segments",
                "storage operation failed",
            )
        })?;
        let mut entries = self
            .operator
            .lister_with(&format!("{SEGMENT_ROOT}/"))
            .recursive(true)
            .await
            .map_err(|_| {
                unavailable(
                    "collect unreachable data segments",
                    "storage operation failed",
                )
            })?;
        while let Some(entry) = entries.try_next().await.map_err(|_| {
            unavailable(
                "collect unreachable data segments",
                "storage operation failed",
            )
        })? {
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
            deleter.delete(entry.path()).await.map_err(|_| {
                unavailable(
                    "collect unreachable data segments",
                    "storage operation failed",
                )
            })?;
            result.deleted += 1;
            result.deleted_bytes = result
                .deleted_bytes
                .checked_add(reference.length)
                .ok_or_else(|| {
                    corrupt(
                        "collect unreachable data segments",
                        "deleted byte count overflows",
                    )
                })?;
        }
        deleter.close().await.map_err(|_| {
            unavailable(
                "collect unreachable data segments",
                "storage operation failed",
            )
        })?;
        Ok(result)
    }

    pub(crate) async fn materialize(
        &self,
        target: &Operator,
        segment_staging: Option<&Operator>,
        requests: Vec<(String, DecodedFileVersion)>,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        let staged_segments = match segment_staging {
            Some(staging) => read_staging_plan(staging).await?,
            None => BTreeSet::new(),
        };
        let mut batch = Vec::new();
        let mut batch_bytes = 0_u64;
        for request in requests {
            let size = request.1.logical_size;
            if !batch.is_empty() && batch_bytes.saturating_add(size) > MATERIALIZE_BATCH_BYTES {
                self.materialize_batch(
                    target,
                    segment_staging,
                    &staged_segments,
                    std::mem::take(&mut batch),
                    concurrency,
                )
                .await?;
                batch_bytes = 0;
            }
            batch_bytes = batch_bytes.saturating_add(size);
            batch.push(request);
        }
        if !batch.is_empty() {
            self.materialize_batch(
                target,
                segment_staging,
                &staged_segments,
                batch,
                concurrency,
            )
            .await?;
        }
        Ok(())
    }

    async fn materialize_batch(
        &self,
        target: &Operator,
        segment_staging: Option<&Operator>,
        staged_segments: &BTreeSet<SegmentRef>,
        requests: Vec<(String, DecodedFileVersion)>,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        if let [(path, version)] = requests.as_slice()
            && version.logical_size > MATERIALIZE_BATCH_BYTES
        {
            return self
                .materialize_large_file(
                    target,
                    segment_staging,
                    staged_segments,
                    path,
                    version,
                    concurrency,
                )
                .await;
        }

        let demands = segment_demands(
            requests
                .iter()
                .flat_map(|(_, version)| version.extent_map.extents.iter()),
        );
        // One fetch already deduplicates a segment within this batch. Foyer is
        // reserved for complete segments reused by separate fetch windows.
        let fetched = self
            .fetch_segments(
                &demands,
                &BTreeSet::new(),
                segment_staging,
                staged_segments,
                concurrency,
            )
            .await?;

        stream::iter(requests)
            .map(|(path, version)| {
                let fetched = &fetched;
                async move { materialize_file(target, &path, &version, fetched).await }
            })
            .buffer_unordered(concurrency.get())
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    async fn cached_operator(&self) -> Result<Operator, VolumeError> {
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
                    .map_err(|_| {
                        unavailable(
                            "open materialization segment cache",
                            "storage operation failed",
                        )
                    })?;
                Ok(self
                    .operator
                    .clone()
                    .layer(FoyerLayer::new(cache).with_size_limit(..=MATERIALIZE_CACHE_BYTES)))
            })
            .await
            .cloned()
    }

    async fn materialize_large_file(
        &self,
        target: &Operator,
        segment_staging: Option<&Operator>,
        staged_segments: &BTreeSet<SegmentRef>,
        path: &str,
        version: &DecodedFileVersion,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        let cache_complete_segments = self.operator.info().full_capability().stat;
        let mut writer = target
            .writer(path)
            .await
            .map_err(|_| unavailable("write materialized file", "storage operation failed"))?;
        let mut logical = Sha256::new();
        let mut written = 0_u64;
        let extents = &version.extent_map.extents;
        let reusable = segments_in_multiple_windows(extents);
        let mut start = 0;
        while start < extents.len() {
            let end = extent_window_end(extents, start);
            let demands = segment_demands(extents[start..end].iter());
            let complete = complete_segments(&demands, cache_complete_segments, &reusable);
            let fetched = match self
                .fetch_segments(
                    &demands,
                    &complete,
                    segment_staging,
                    staged_segments,
                    concurrency,
                )
                .await
            {
                Ok(fetched) => fetched,
                Err(error) => {
                    let _ = writer.abort().await;
                    return Err(error);
                }
            };
            if let Err(error) = write_extents(
                &mut writer,
                &extents[start..end],
                &fetched,
                &mut logical,
                &mut written,
            )
            .await
            {
                let _ = writer.abort().await;
                return Err(error);
            }
            start = end;
        }
        finish_materialized_file(writer, version, logical, written).await
    }

    async fn fetch_segments(
        &self,
        demands: &BTreeMap<SegmentRef, SegmentDemand>,
        complete: &BTreeSet<SegmentRef>,
        segment_staging: Option<&Operator>,
        staged_segments: &BTreeSet<SegmentRef>,
        concurrency: NonZeroUsize,
    ) -> Result<FetchedContent, VolumeError> {
        let cached = if complete.is_empty() {
            None
        } else {
            Some(self.cached_operator().await?)
        };
        let cached = cached.as_ref();
        // Schedule at the segment boundary. Reader::fetch owns range merging;
        // the outer bound gives concurrency one meaning across the data plane.
        stream::iter(demands.iter())
            .map(|(segment, demands)| async move {
                let segment = *segment;
                if staged_segments.contains(&segment) {
                    let staging =
                        segment_staging.expect("staged segment references have a local operator");
                    let bytes = staging.read(&segment_key(segment)).await.map_err(|error| {
                        if error.kind() == ErrorKind::NotFound {
                            corrupt("read data segment", "staged data segment is missing")
                        } else {
                            unavailable("read data segment", "storage operation failed")
                        }
                    })?;
                    return complete_demand_contents(segment, &bytes, demands);
                }
                if complete.contains(&segment) {
                    let cached = cached.expect("complete segment reads have a Foyer operator");
                    let bytes = cached
                        .read(&segment_key(segment))
                        .await
                        .map_err(|error| referenced_segment_error("read data segment", error))?;
                    return complete_demand_contents(segment, &bytes, demands);
                }

                let reader = self
                    .operator
                    .reader_with(&segment_key(segment))
                    .concurrent(concurrency.get())
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
                demands
                    .iter()
                    .copied()
                    .zip(fetched)
                    .map(|(demand, bytes)| {
                        verify_range_demand(&bytes, demand)?;
                        Ok((demand.2, bytes))
                    })
                    .collect::<Result<Vec<_>, VolumeError>>()
            })
            .buffer_unordered(concurrency.get())
            .try_fold(BTreeMap::new(), |mut fetched, segment| async move {
                fetched.extend(segment);
                Ok(fetched)
            })
            .await
    }
}

fn segment_demands<'a>(
    extents: impl IntoIterator<Item = &'a Extent>,
) -> BTreeMap<SegmentRef, SegmentDemand> {
    let mut demands = BTreeMap::new();
    for extent in extents {
        demands
            .entry(extent.segment)
            .or_insert_with(BTreeSet::new)
            .insert(demand_key(extent));
    }
    demands
}

fn complete_segments(
    demands: &BTreeMap<SegmentRef, SegmentDemand>,
    cache_complete_segments: bool,
    reusable: &BTreeSet<SegmentRef>,
) -> BTreeSet<SegmentRef> {
    // A full Foyer read is useful only when another fetch window will reuse it.
    // Sparse and one-shot reads stay with native Reader::fetch.
    demands
        .iter()
        .filter(|(segment, segment_demands)| {
            cache_complete_segments
                && reusable.contains(segment)
                && usize::try_from(segment.length)
                    .is_ok_and(|length| length <= MATERIALIZE_CACHE_BYTES)
                && demands_cover_segment(**segment, segment_demands)
        })
        .map(|(segment, _)| *segment)
        .collect()
}

fn segments_in_multiple_windows(extents: &[Extent]) -> BTreeSet<SegmentRef> {
    let mut seen = BTreeSet::new();
    let mut repeated = BTreeSet::new();
    let mut start = 0;
    while start < extents.len() {
        let end = extent_window_end(extents, start);
        for segment in extents[start..end]
            .iter()
            .map(|extent| extent.segment)
            .collect::<BTreeSet<_>>()
        {
            if !seen.insert(segment) {
                repeated.insert(segment);
            }
        }
        start = end;
    }
    repeated
}

fn demands_cover_segment(segment: SegmentRef, demands: &SegmentDemand) -> bool {
    let mut covered = 0_u64;
    for (offset, length, _) in demands {
        if *offset > covered {
            return false;
        }
        let Some(end) = offset.checked_add(*length) else {
            return false;
        };
        covered = covered.max(end);
    }
    covered == segment.length
}

async fn materialize_file(
    target: &Operator,
    path: &str,
    version: &DecodedFileVersion,
    fetched: &FetchedContent,
) -> Result<(), VolumeError> {
    let mut chunks = Vec::new();
    for extent in &version.extent_map.extents {
        let bytes = fetched
            .get(&extent.content)
            .cloned()
            .ok_or_else(|| corrupt("materialize Managed files", "extent was not fetched"))?;
        chunks.extend(bytes);
    }
    let bytes = Buffer::from(chunks);
    verify_materialized_file(version, buffer_content_ref(&bytes))?;
    target
        .write(path, bytes)
        .await
        .map(|_| ())
        .map_err(|_| unavailable("write materialized file", "storage operation failed"))
}

async fn write_extents(
    writer: &mut Writer,
    extents: &[Extent],
    fetched: &FetchedContent,
    logical: &mut Sha256,
    written: &mut u64,
) -> Result<(), VolumeError> {
    for extent in extents {
        let bytes = fetched
            .get(&extent.content)
            .cloned()
            .ok_or_else(|| corrupt("materialize Managed files", "extent was not fetched"))?;
        *written = written
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| corrupt("materialize Managed files", "logical file length overflows"))?;
        for chunk in bytes.clone() {
            logical.update(&chunk);
        }
        writer
            .write(bytes)
            .await
            .map_err(|_| unavailable("write materialized file", "storage operation failed"))?;
    }
    Ok(())
}

async fn finish_materialized_file(
    mut writer: Writer,
    version: &DecodedFileVersion,
    logical: Sha256,
    written: u64,
) -> Result<(), VolumeError> {
    let content = ContentRef {
        digest: logical.finalize().into(),
        length: written,
    };
    if let Err(error) = verify_materialized_file(version, content) {
        let _ = writer.abort().await;
        return Err(error);
    }
    writer
        .close()
        .await
        .map(|_| ())
        .map_err(|_| unavailable("write materialized file", "storage operation failed"))
}

fn verify_materialized_file(
    version: &DecodedFileVersion,
    content: ContentRef,
) -> Result<(), VolumeError> {
    if content.length != version.logical_size || content.digest != version.logical_digest {
        return Err(corrupt(
            "materialize Managed files",
            "logical digest does not match the file version",
        ));
    }
    Ok(())
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

fn complete_demand_contents(
    segment: SegmentRef,
    bytes: &Buffer,
    demands: &BTreeSet<DemandKey>,
) -> Result<Vec<(ContentRef, Buffer)>, VolumeError> {
    verify_complete_segment(segment, bytes)?;
    demands
        .iter()
        .map(|demand| {
            let bytes = slice_demand(bytes, *demand)?;
            verify_range_demand(&bytes, *demand)?;
            Ok((demand.2, bytes))
        })
        .collect()
}

fn verify_range_demand(bytes: &Buffer, demand: DemandKey) -> Result<(), VolumeError> {
    let (_, _, content) = demand;
    if buffer_content_ref(bytes) != content {
        return Err(corrupt(
            "read data segment",
            "extent bytes do not match their content reference",
        ));
    }
    Ok(())
}

fn slice_demand(bytes: &Buffer, demand: DemandKey) -> Result<Buffer, VolumeError> {
    let (offset, demand_length, _) = demand;
    let start = usize::try_from(offset)
        .map_err(|_| corrupt("read data segment", "extent offset exceeds this process"))?;
    let length = usize::try_from(demand_length)
        .map_err(|_| corrupt("read data segment", "extent length exceeds this process"))?;
    let end = start
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| corrupt("read data segment", "extent exceeds data segment"))?;
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

async fn stream_file(
    source: &Operator,
    path: &str,
    sender: &mpsc::Sender<PreparedChunk>,
) -> Result<(u64, [u8; 32]), VolumeError> {
    let metadata = source
        .stat(path)
        .await
        .map_err(|_| unavailable("read frozen file", "storage operation failed"))?;
    if !metadata.is_file() {
        return Err(invalid("read frozen file", "input is not a regular file"));
    }
    let size = metadata.content_length();
    if size == 0 {
        return Ok((size, Sha256::digest([]).into()));
    }
    if size < FASTCDC_MINIMUM_FILE_SIZE {
        let bytes = source
            .read(path)
            .await
            .map_err(|_| unavailable("read frozen file", "storage operation failed"))?
            .to_bytes()
            .to_vec();
        if bytes.len() as u64 != size {
            return Err(invalid(
                "read frozen file",
                "frozen input changed while it was being staged",
            ));
        }
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
            .map_err(|_| unavailable("stage Managed files", "storage operation failed"))?;
        return Ok((size, content.digest));
    }

    let digest = stream_fastcdc(source, path, size, sender).await?;
    Ok((size, digest))
}

async fn stream_fastcdc(
    source: &Operator,
    path: &str,
    size: u64,
    sender: &mpsc::Sender<PreparedChunk>,
) -> Result<[u8; 32], VolumeError> {
    let reader = source
        .reader(path)
        .await
        .map_err(|_| unavailable("read frozen file", "storage operation failed"))?
        .into_futures_async_read(..)
        .await
        .map_err(|_| unavailable("read frozen file", "storage operation failed"))?;
    let mut chunker = AsyncStreamCDC::new(
        reader,
        FASTCDC_MINIMUM_SIZE,
        FASTCDC_TARGET_SIZE,
        FASTCDC_MAXIMUM_SIZE,
    );
    let chunks = chunker.as_stream();
    futures::pin_mut!(chunks);
    let mut logical = Sha256::new();
    let mut observed = 0_u64;
    while let Some(chunk) = chunks.next().await {
        let chunk =
            chunk.map_err(|_| unavailable("chunk frozen file", "storage operation failed"))?;
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
            .map_err(|_| unavailable("stage Managed files", "storage operation failed"))?;
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
) -> Result<BTreeMap<ContentRef, Vec<u8>>, VolumeError> {
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

fn seal_segment(contents: BTreeMap<ContentRef, Vec<u8>>) -> Result<SealedSegment, VolumeError> {
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

async fn stage_segment(
    staging: &Operator,
    segment: SealedSegment,
) -> Result<BTreeMap<ContentRef, StoredContent>, VolumeError> {
    let SealedSegment {
        reference,
        bytes,
        locations,
    } = segment;
    staging
        .write(&segment_key(reference), bytes)
        .await
        .map_err(|_| unavailable("stage data segment", "storage operation failed"))?;
    Ok(locations)
}

async fn write_staging_plan(
    staging: &Operator,
    segments: &BTreeSet<SegmentRef>,
) -> Result<(), VolumeError> {
    let bytes = STAGING_PLAN_RECORD
        .encode(segments)
        .map_err(|_| invalid("stage Managed files", "segment plan cannot be encoded"))?;
    staging
        .write(STAGING_PLAN_KEY, bytes)
        .await
        .map(|_| ())
        .map_err(|_| unavailable("stage Managed files", "storage operation failed"))
}

async fn read_staging_plan(staging: &Operator) -> Result<BTreeSet<SegmentRef>, VolumeError> {
    let bytes = staging.read(STAGING_PLAN_KEY).await.map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            corrupt("finalize Managed files", "staged segment plan is missing")
        } else {
            unavailable("finalize Managed files", "storage operation failed")
        }
    })?;
    STAGING_PLAN_RECORD
        .decode(&bytes.to_bytes())
        .map_err(|_| corrupt("finalize Managed files", "staged segment plan is invalid"))
}

fn verify_complete_segment(reference: SegmentRef, bytes: &Buffer) -> Result<(), VolumeError> {
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

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        digest: Sha256::digest(bytes).into(),
        length: bytes.len() as u64,
    }
}

fn segment_key(reference: SegmentRef) -> String {
    let digest = LowerHex::encode(&reference.digest);
    format!("{SEGMENT_ROOT}/{}/{}.seg", &digest[..2], digest)
}

fn segment_ref_from_key(path: &str, length: u64) -> Option<SegmentRef> {
    let relative = path.strip_prefix(SEGMENT_ROOT)?.strip_prefix('/')?;
    let (shard, name) = relative.split_once('/')?;
    let digest = name.strip_suffix(".seg")?;
    if shard.len() != 2 || digest.len() != 64 || !digest.starts_with(shard) {
        return None;
    }
    let digest: [u8; 32] = LowerHex::decode(digest)?.try_into().ok()?;
    Some(SegmentRef { digest, length })
}

fn referenced_segment_error(action: &'static str, error: opendal::Error) -> VolumeError {
    if error.kind() == ErrorKind::NotFound {
        corrupt(action, "file version references a missing data segment")
    } else {
        unavailable(action, "storage operation failed")
    }
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
        let segment_staging = memory();
        let storage = memory();
        let target = memory();
        let small = b"portable segment".to_vec();
        let large = (0..2 * 1024 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        source.write("small", small.clone()).await.unwrap();
        source.write("large", large.clone()).await.unwrap();
        let data = ManagedData::new(storage.clone()).unwrap();

        let staged = data
            .stage_files(
                &source,
                &segment_staging,
                vec!["small".to_owned(), "large".to_owned()],
                &AuthorityKnownContent::default(),
                NonZeroUsize::new(2).unwrap(),
            )
            .await
            .unwrap();
        data.finalize_staged_files(&segment_staging, NonZeroUsize::new(2).unwrap())
            .await
            .unwrap();
        data.materialize(
            &target,
            Some(&segment_staging),
            staged.into_iter().collect(),
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
        let version = DecodedFileVersion::from_extents(
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
                None,
                vec![("output".to_owned(), version)],
                NonZeroUsize::new(2).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(target.read("output").await.unwrap().to_bytes(), logical);
    }

    #[test]
    fn complete_segment_rejects_corruption() {
        let first = content_ref(b"first");
        let other = content_ref(b"other");
        let mut segment = seal_segment(BTreeMap::from([
            (first, b"first".to_vec()),
            (other, b"other".to_vec()),
        ]))
        .unwrap();
        assert!(
            verify_complete_segment(segment.reference, &Buffer::from(segment.bytes.clone()))
                .is_ok()
        );
        let wrong_mapping =
            BTreeSet::from([(segment.locations[&first].offset, first.length, other)]);
        assert!(
            complete_demand_contents(
                segment.reference,
                &Buffer::from(segment.bytes.clone()),
                &wrong_mapping,
            )
            .is_err()
        );
        segment.bytes[0] ^= 1;
        assert!(verify_complete_segment(segment.reference, &Buffer::from(segment.bytes)).is_err());
    }
}
