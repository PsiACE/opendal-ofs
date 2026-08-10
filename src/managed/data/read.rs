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

//! Native range reads, bounded materialization, and complete-segment caching.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use blake3::Hasher;
use foyer::HybridCacheBuilder;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use opendal::layers::{FoyerKey, FoyerLayer, FoyerValue};
use opendal::{Buffer, ErrorKind, Operator, Writer};

use super::{
    ManagedData, buffer_content_ref, read_staging_plan, referenced_segment_error, segment_key,
    verify_complete_segment,
};
use crate::filesystem::VolumeError;
use crate::managed::error::{corrupt, unavailable};
use crate::managed::format::{ContentRef, Extent, SegmentRef};
use crate::managed::metadata::namespace::DecodedFileVersion;

const MATERIALIZE_WINDOW_BYTES: u64 = super::TARGET_SEGMENT_SIZE;
const MATERIALIZE_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MATERIALIZE_BATCH_BYTES: u64 = MATERIALIZE_CACHE_BYTES as u64;

type DemandKey = (u64, u64, ContentRef);
type SegmentDemand = BTreeSet<DemandKey>;
type FetchedContent = BTreeMap<ContentRef, Buffer>;

impl ManagedData {
    pub(crate) async fn materialize(
        &self,
        target: &Operator,
        segment_staging: Option<&Operator>,
        requests: Vec<(String, DecodedFileVersion)>,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError> {
        if requests.is_empty() {
            return Ok(());
        }
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
        let mut logical = Hasher::new();
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
    logical: &mut Hasher,
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
    logical: Hasher,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed::data::content_ref;

    #[test]
    fn complete_segment_rejects_corruption() {
        let first = content_ref(b"first");
        let other = content_ref(b"other");
        let mut bytes = b"firstother".to_vec();
        let segment = SegmentRef {
            digest: blake3::hash(&bytes).into(),
            length: bytes.len() as u64,
        };
        assert!(verify_complete_segment(segment, &Buffer::from(bytes.clone())).is_ok());
        let wrong_mapping = BTreeSet::from([(0, first.length, other)]);
        assert!(
            complete_demand_contents(segment, &Buffer::from(bytes.clone()), &wrong_mapping,)
                .is_err()
        );
        bytes[0] ^= 1;
        assert!(verify_complete_segment(segment, &Buffer::from(bytes)).is_err());
    }
}
