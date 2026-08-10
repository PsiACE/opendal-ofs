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

//! Bounded file preparation, deterministic segment placement, and finalization.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use fastcdc::v2020::AsyncStreamCDC;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use opendal::{ErrorKind, Operator};
use sha2::{Digest as _, Sha256};
use tokio::sync::{mpsc, oneshot};

use super::{
    ManagedData, STAGING_PLAN_KEY, STAGING_PLAN_RECORD, TARGET_SEGMENT_SIZE, content_ref,
    read_staging_plan, segment_key, verify_complete_segment,
};
use crate::filesystem::VolumeError;
use crate::managed::error::{corrupt, invalid, unavailable};
use crate::managed::format::{ContentRef, Extent, ExtentMap, SegmentRef};
use crate::managed::metadata::namespace::DecodedFileVersion;
use crate::managed::metadata::object::ensure_immutable;

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
    contents: Vec<ContentRef>,
}

struct PreparedChunk {
    content: ContentRef,
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

impl ManagedData {
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
                let mut contents = Vec::new();
                while let Some(PreparedChunk { content, bytes }) = prepared.chunks.recv().await {
                    if known.get(&content).is_none() && !created.contains_key(&content) {
                        match new_content.entry(content) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                entry.insert(bytes);
                                pending_bytes =
                                    pending_bytes.checked_add(content.length).ok_or_else(|| {
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
                    contents.push(content);

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
                        contents,
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
                            .contents
                            .into_iter()
                            .map(|content| {
                                let stored = known
                                    .get(&content)
                                    .or_else(|| created.get(&content).copied())
                                    .ok_or_else(|| {
                                        corrupt(
                                            "stage Managed files",
                                            "prepared content has no segment location",
                                        )
                                    })?;
                                Ok(Extent {
                                    content,
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
            .send(PreparedChunk { content, bytes })
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
                content,
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
