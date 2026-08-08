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

use fastcdc::v2020::AsyncStreamCDC;
use futures::{StreamExt, stream};
use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{ManagedError, ManagedErrorKind};
use crate::filesystem::NodeKind;
use crate::managed::format::{ContentRef, Extent, ExtentMap, SegmentRef};
use crate::managed::metadata::namespace::{FileVersionRecord, NamespaceSnapshot};

const SEGMENT_ROOT: &str = ".ofs/managed/data/v1/segments/sha256";
const SEGMENT_MAGIC: &[u8; 8] = b"OFSSEG01";
const TRAILER_MAGIC: &[u8; 8] = b"OFSSEGTR";
const FORMAT_MAJOR: u16 = 1;
const HEADER_LENGTH: u64 = 10;
const TRAILER_LENGTH: u64 = 56;
const REQUEST_EQUIVALENT_BYTES: u64 = 4 * 1024;
const RANGE_COALESCE_GAP: usize = REQUEST_EQUIVALENT_BYTES as usize;

// Placement policy. These values are not part of the durable format.
const TARGET_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
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
    path: String,
    logical_size: u64,
    logical_digest: [u8; 32],
    extents: Vec<PreparedExtent>,
}

#[derive(Debug)]
struct PreparedExtent {
    logical_offset: u64,
    content: ContentRef,
    bytes: Vec<u8>,
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

#[derive(Debug)]
struct MaterializedFile {
    path: String,
    version: FileVersionRecord,
    bytes: Vec<u8>,
}

type SegmentDemand = BTreeMap<(u64, u64, ContentRef), Vec<(usize, u64)>>;

/// The Managed v1 data plane.
#[derive(Clone)]
pub(crate) struct ManagedData {
    operator: Operator,
}

impl ManagedData {
    pub(crate) fn new(operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.read
            || !capability.write
            || !capability.write_can_empty
            || !capability.write_with_if_not_exists
        {
            return Err(invalid(
                "open Managed data",
                "data storage requires read, empty write, and create-only write",
            ));
        }
        Ok(Self { operator })
    }

    /// Freeze a set of files into as few immutable segments as placement policy permits.
    pub(crate) async fn stage_files(
        &self,
        source: &Operator,
        paths: Vec<String>,
        known: &AuthorityKnownContent,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersionRecord>, ManagedError> {
        let prepared = stream::iter(paths)
            .map(|path| {
                let source = source.clone();
                async move { prepare_file(&source, path).await }
            })
            .buffer_unordered(concurrency.get())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let mut new_content = BTreeMap::<ContentRef, Vec<u8>>::new();
        for file in &prepared {
            for extent in &file.extents {
                if known.get(&extent.content).is_none() {
                    match new_content.entry(extent.content) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(extent.bytes.clone());
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() != &extent.bytes =>
                        {
                            return Err(corrupt(
                                "stage Managed files",
                                "equal content references contain different bytes",
                            ));
                        }
                        std::collections::btree_map::Entry::Occupied(_) => {}
                    }
                }
            }
        }

        let mut created = BTreeMap::new();
        for segment in seal_segments(new_content)? {
            self.create_segment(&segment).await?;
            created.extend(segment.locations);
        }

        prepared
            .into_iter()
            .map(|file| {
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
                Ok((file.path, version))
            })
            .collect()
    }

    async fn create_segment(&self, segment: &SealedSegment) -> Result<(), ManagedError> {
        let key = segment_key(segment.reference);
        match self
            .operator
            .write_with(&key, segment.bytes.clone())
            .if_not_exists(true)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if already_exists(&error) => {
                let existing = self
                    .operator
                    .read(&key)
                    .await
                    .map_err(|_| unavailable("verify existing data segment"))?
                    .to_bytes()
                    .to_vec();
                verify_complete_segment(segment.reference, &existing).map(|_| ())
            }
            Err(_) => Err(unavailable("create data segment")),
        }
    }

    pub(crate) async fn materialize(
        &self,
        target: &Operator,
        requests: Vec<(String, FileVersionRecord)>,
        full_tree: bool,
        concurrency: NonZeroUsize,
    ) -> Result<(), ManagedError> {
        let mut files = Vec::with_capacity(requests.len());
        let mut segments = BTreeMap::<SegmentRef, SegmentDemand>::new();
        for (file_index, (path, version)) in requests.into_iter().enumerate() {
            if !version.is_valid() {
                return Err(corrupt(
                    "materialize Managed files",
                    "file version identity is invalid",
                ));
            }
            let length = usize::try_from(version.logical_size).map_err(|_| {
                invalid(
                    "materialize Managed files",
                    "file is too large for this process",
                )
            })?;
            for extent in &version.extent_map.extents {
                validate_extent(extent)?;
                segments
                    .entry(extent.segment)
                    .or_default()
                    .entry((extent.segment_offset, extent.content.length, extent.content))
                    .or_default()
                    .push((file_index, extent.logical_offset));
            }
            files.push(MaterializedFile {
                path,
                version,
                bytes: vec![0; length],
            });
        }

        let fetched = stream::iter(segments)
            .map(|(segment, demands)| {
                let data = self.clone();
                async move {
                    data.read_segment_ranges(segment, demands, full_tree, concurrency.get())
                        .await
                }
            })
            .buffer_unordered(concurrency.get())
            .collect::<Vec<_>>()
            .await;
        for result in fetched {
            for (file_index, logical_offset, bytes) in result? {
                let file = files.get_mut(file_index).ok_or_else(|| {
                    corrupt(
                        "materialize Managed files",
                        "extent references an unknown file",
                    )
                })?;
                let start = usize::try_from(logical_offset).map_err(|_| {
                    corrupt(
                        "materialize Managed files",
                        "extent offset exceeds this process",
                    )
                })?;
                let end = start.checked_add(bytes.len()).ok_or_else(|| {
                    corrupt("materialize Managed files", "extent range overflows")
                })?;
                let output = file.bytes.get_mut(start..end).ok_or_else(|| {
                    corrupt(
                        "materialize Managed files",
                        "extent exceeds logical file size",
                    )
                })?;
                output.copy_from_slice(&bytes);
            }
        }

        let writes = stream::iter(files)
            .map(|file| {
                let target = target.clone();
                async move {
                    if <[u8; 32]>::from(Sha256::digest(&file.bytes)) != file.version.logical_digest
                    {
                        return Err(corrupt(
                            "materialize Managed files",
                            "logical digest does not match the file version",
                        ));
                    }
                    target
                        .write(&file.path, file.bytes)
                        .await
                        .map_err(|_| unavailable("write materialized file"))?;
                    Ok(())
                }
            })
            .buffer_unordered(concurrency.get())
            .collect::<Vec<_>>()
            .await;
        writes.into_iter().collect()
    }

    async fn read_segment_ranges(
        &self,
        segment: SegmentRef,
        demands: SegmentDemand,
        full_tree: bool,
        range_concurrency: usize,
    ) -> Result<Vec<(usize, u64, Vec<u8>)>, ManagedError> {
        let key = segment_key(segment);
        let contents = if prefer_complete_segment(segment, &demands, full_tree) {
            let bytes = self
                .operator
                .read(&key)
                .await
                .map_err(|error| referenced_segment_error("read data segment", error))?
                .to_bytes()
                .to_vec();
            let entries = verify_complete_segment(segment, &bytes)?;
            demands
                .keys()
                .map(|(offset, length, content)| {
                    let range = entries.get(content).filter(|range| {
                        range.start as u64 == *offset && (range.end - range.start) as u64 == *length
                    });
                    let range = range.ok_or_else(|| {
                        corrupt(
                            "read data segment",
                            "file extent disagrees with the segment footer",
                        )
                    })?;
                    Ok(bytes[range.clone()].to_vec())
                })
                .collect::<Result<Vec<_>, ManagedError>>()?
        } else {
            let ranges = demands
                .keys()
                .map(|(offset, length, _)| *offset..*offset + *length)
                .collect::<Vec<_>>();
            let reader = self
                .operator
                .reader_with(&key)
                .content_length_hint(segment.length)
                .gap(RANGE_COALESCE_GAP)
                .concurrent(range_concurrency)
                .await
                .map_err(|error| referenced_segment_error("read data segment", error))?;
            reader
                .fetch(ranges)
                .await
                .map_err(|error| referenced_segment_error("read data segment", error))?
                .into_iter()
                .map(|buffer| buffer.to_bytes().to_vec())
                .collect()
        };

        let mut output = Vec::new();
        for (((_, length, content), targets), bytes) in demands.into_iter().zip(contents) {
            if bytes.len() as u64 != length
                || <[u8; 32]>::from(Sha256::digest(&bytes)) != content.digest
            {
                return Err(corrupt(
                    "read data segment",
                    "extent bytes do not match their content reference",
                ));
            }
            output.extend(
                targets
                    .into_iter()
                    .map(|(file, logical)| (file, logical, bytes.clone())),
            );
        }
        Ok(output)
    }

    pub(crate) async fn collect_unreachable_segments(
        &self,
        snapshot: &NamespaceSnapshot,
    ) -> Result<SegmentGcMaintenance, ManagedError> {
        let capability = self.operator.info().full_capability();
        if !capability.list || !capability.delete {
            return Err(unavailable("collect unreachable data segments"));
        }
        let live = reachable_segments(snapshot, "collect unreachable data segments")?;
        let mut result = SegmentGcMaintenance::default();
        let entries = self
            .operator
            .list_with(&format!("{SEGMENT_ROOT}/"))
            .recursive(true)
            .await
            .map_err(|_| unavailable("list data segments"))?;
        let mut deleted = Vec::new();
        for entry in entries {
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
            deleted.push(entry.path().to_owned());
            result.deleted += 1;
            result.deleted_bytes = result
                .deleted_bytes
                .checked_add(reference.length)
                .ok_or_else(|| {
                    corrupt(
                        "collect unreachable data segments",
                        "deleted byte count exceeds format v1",
                    )
                })?;
        }
        self.operator
            .delete_iter(deleted.iter().map(String::as_str))
            .await
            .map_err(|_| unavailable("delete unreachable data segments"))?;
        Ok(result)
    }
}

fn prefer_complete_segment(segment: SegmentRef, demands: &SegmentDemand, full_tree: bool) -> bool {
    let mut requests = 0_u64;
    let mut transferred = 0_u64;
    let mut span: Option<Range<u64>> = None;
    for (offset, length, _) in demands.keys() {
        let range = *offset..*offset + *length;
        match span.as_mut() {
            Some(current)
                if range.start.saturating_sub(current.end) <= RANGE_COALESCE_GAP as u64 =>
            {
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
    if requests <= 1 {
        return false;
    }

    // Read policy may trade a small transfer for fewer HTTP requests without
    // changing the segment or extent formats.
    let saved_requests = requests - 1;
    let byte_budget = REQUEST_EQUIVALENT_BYTES * if full_tree { 4 } else { 1 };
    segment.length.saturating_sub(transferred) <= saved_requests.saturating_mul(byte_budget)
}

async fn prepare_file(source: &Operator, path: String) -> Result<PreparedFile, ManagedError> {
    let metadata = source
        .stat(&path)
        .await
        .map_err(|_| unavailable("read frozen file"))?;
    if !metadata.is_file() {
        return Err(invalid("read frozen file", "input is not a regular file"));
    }
    let size = metadata.content_length();
    if size == 0 {
        return Ok(PreparedFile {
            path,
            logical_size: 0,
            logical_digest: Sha256::digest([]).into(),
            extents: Vec::new(),
        });
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
        return Ok(PreparedFile {
            path,
            logical_size: size,
            logical_digest: content.digest,
            extents: vec![PreparedExtent {
                logical_offset: 0,
                content,
                bytes,
            }],
        });
    }

    prepare_fastcdc(
        source,
        path,
        size,
        FastCdcSizes {
            minimum: FASTCDC_MINIMUM_SIZE,
            target: FASTCDC_TARGET_SIZE,
            maximum: FASTCDC_MAXIMUM_SIZE,
        },
    )
    .await
}

async fn prepare_fastcdc(
    source: &Operator,
    path: String,
    size: u64,
    sizes: FastCdcSizes,
) -> Result<PreparedFile, ManagedError> {
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
    let mut extents = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| unavailable("chunk frozen file"))?;
        logical.update(&chunk.data);
        extents.push(PreparedExtent {
            logical_offset: chunk.offset,
            content: content_ref(&chunk.data),
            bytes: chunk.data,
        });
    }
    let prepared = PreparedFile {
        path,
        logical_size: size,
        logical_digest: logical.finalize().into(),
        extents,
    };
    let observed = prepared.extents.iter().try_fold(0_u64, |end, extent| {
        (extent.logical_offset == end)
            .then(|| end.checked_add(extent.content.length))
            .flatten()
    });
    if observed != Some(size) {
        return Err(invalid(
            "read frozen file",
            "frozen input changed while it was being staged",
        ));
    }
    Ok(prepared)
}

fn seal_segments(
    contents: BTreeMap<ContentRef, Vec<u8>>,
) -> Result<Vec<SealedSegment>, ManagedError> {
    let mut segments = Vec::new();
    let mut batch = BTreeMap::new();
    let mut batch_size = 0_u64;
    for (content, bytes) in contents {
        if !batch.is_empty() && batch_size.saturating_add(content.length) > TARGET_SEGMENT_SIZE {
            segments.push(seal_segment(std::mem::take(&mut batch))?);
            batch_size = 0;
        }
        batch_size = batch_size
            .checked_add(content.length)
            .ok_or_else(|| invalid("seal data segment", "segment content length overflows"))?;
        batch.insert(content, bytes);
    }
    if !batch.is_empty() {
        segments.push(seal_segment(batch)?);
    }
    Ok(segments)
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

fn already_exists(error: &opendal::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
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
