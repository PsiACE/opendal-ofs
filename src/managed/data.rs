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

//! File manifests backed by immutable loose objects.

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use fastcdc::v2020::{
    AVERAGE_MAX, AVERAGE_MIN, AsyncStreamCDC, MAXIMUM_MAX, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN,
};
use futures::StreamExt;
use opendal::{ErrorKind, Operator, Writer};
use sha2::{Digest as _, Sha256};

use super::{ManagedError, ManagedErrorKind};
use crate::filesystem::{ChangeCursor, NodeKind, OperationId};
use crate::managed::namespace::{
    ChunkSpan, ChunkingAlgorithm, ChunkingSpec, ContentRef, DataExtent, FileExtent,
    FileVersionLayout, FileVersionRecord, NamespaceSnapshot,
};
use crate::managed::pack::{PackId, PackIndex, PackLocation, PackStore};

const READ_WINDOW: u64 = 4 * 1024 * 1024;
const LOOSE_ROOT: &str = "data/v1/loose/sha256";
const SMALL_CONTENT_LIMIT: u64 = 256 * 1024;
const PACK_LOGICAL_LIMIT: u64 = 8 * 1024 * 1024;
static PACK_RETIREMENT_EPOCH: AtomicU64 = AtomicU64::new(1);

/// Physical locations published by one explicit small-content maintenance run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackMaintenance {
    pub packs: Vec<PackId>,
    pub packed_content: Vec<ContentRef>,
    pub logical_bytes: u64,
    /// Loose objects that have a verified, published pack location.
    pub reclaimable_loose: Vec<ContentRef>,
}

/// One process-local grace boundary after replacement locations are published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackRetirement {
    epoch: u64,
    fixed_at: ChangeCursor,
    retired_packs: Vec<PackId>,
    replacement_packs: Vec<PackId>,
    protected_content: BTreeSet<ContentRef>,
}

impl PackRetirement {
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn fixed_at(&self) -> ChangeCursor {
        self.fixed_at
    }

    pub fn retired_packs(&self) -> &[PackId] {
        &self.retired_packs
    }

    pub fn replacement_packs(&self) -> &[PackId] {
        &self.replacement_packs
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FileLayoutPolicy {
    #[default]
    Whole,
    Fixed {
        chunk_size: u32,
    },
    FastCdcV2020 {
        minimum_size: u32,
        target_size: u32,
        maximum_size: u32,
    },
}

/// Ordered ranges discovered from one frozen sparse file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SparseExtent {
    Hole { logical_length: u64 },
    Data { logical_length: u64 },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Digest([u8; 32]);

impl Digest {
    const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// The concrete Managed v1 data plane.
#[derive(Clone)]
pub(crate) struct ManagedData {
    operator: Operator,
    policy: FileLayoutPolicy,
}

impl ManagedData {
    pub(crate) fn new(operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.stat
            || !capability.read
            || !capability.write
            || !capability.write_can_empty
            || !capability.write_with_if_not_exists
        {
            return Err(invalid(
                "open Managed data",
                "data storage requires stat, read, empty write, and create-only write",
            ));
        }
        Ok(Self {
            operator,
            policy: FileLayoutPolicy::Whole,
        })
    }

    pub(crate) fn set_policy(&mut self, policy: FileLayoutPolicy) -> Result<(), ManagedError> {
        validate_policy(policy)?;
        self.policy = policy;
        Ok(())
    }

    pub(crate) async fn seal_file(
        &self,
        local: &Operator,
        frozen_path: &str,
    ) -> Result<FileVersionRecord, ManagedError> {
        match self.policy {
            FileLayoutPolicy::Whole => self.seal_whole_file(local, frozen_path).await,
            FileLayoutPolicy::Fixed { chunk_size } => {
                self.seal_fixed(local, frozen_path, chunk_size).await
            }
            FileLayoutPolicy::FastCdcV2020 {
                minimum_size,
                target_size,
                maximum_size,
            } => {
                self.seal_fastcdc(local, frozen_path, minimum_size, target_size, maximum_size)
                    .await
            }
        }
    }

    /// Seal one file that Sync has already frozen against local mutation.
    pub(crate) async fn seal_whole_file(
        &self,
        local: &Operator,
        frozen_path: &str,
    ) -> Result<FileVersionRecord, ManagedError> {
        let metadata = local
            .stat(frozen_path)
            .await
            .map_err(|_| unavailable("read frozen file"))?;
        if !metadata.is_file() {
            return Err(invalid("read frozen file", "input is not a regular file"));
        }
        let size = metadata.content_length();
        let digest = digest_and_copy(local, frozen_path, size, None)
            .await
            .map_err(|_| unavailable("read frozen file"))?;
        let version = whole_file_version(size, digest);
        if size == 0 {
            return Ok(version);
        }
        let FileVersionLayout::Whole { content } = &version.layout else {
            unreachable!("whole-file sealing created another layout")
        };
        let key = loose_key(content);

        match self.operator.writer_with(&key).if_not_exists(true).await {
            Err(error) if already_exists(&error) => {}
            Err(_) => return Err(unavailable("create loose data")),
            Ok(mut writer) => {
                let observed =
                    match digest_and_copy(local, frozen_path, size, Some(&mut writer)).await {
                        Ok(observed) => observed,
                        Err(error) if already_exists(&error) => digest,
                        Err(_) => {
                            let _ = writer.abort().await;
                            return Err(unavailable("write loose data"));
                        }
                    };
                if observed != digest {
                    let _ = writer.abort().await;
                    return Err(invalid(
                        "write loose data",
                        "frozen input changed while it was being sealed",
                    ));
                }
                if let Err(error) = writer.close().await {
                    if !already_exists(&error) {
                        return Err(unavailable("commit loose data"));
                    }
                }
            }
        }

        self.verify(&version).await?;
        Ok(version)
    }

    async fn seal_fixed(
        &self,
        local: &Operator,
        path: &str,
        chunk_size: u32,
    ) -> Result<FileVersionRecord, ManagedError> {
        let size = frozen_size(local, path).await?;
        if size == 0 {
            return Ok(FileVersionRecord::whole(0, Sha256::digest([]).into()));
        }
        let reader = local
            .reader(path)
            .await
            .map_err(|_| unavailable("read frozen file"))?;
        let mut logical = Sha256::new();
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < size {
            let end = size.min(offset + u64::from(chunk_size));
            let buffer = reader
                .read(offset..end)
                .await
                .map_err(|_| unavailable("read frozen file"))?;
            let bytes = buffer.to_bytes();
            if bytes.len() as u64 != end - offset {
                return Err(corrupt("read frozen file", "source returned a short range"));
            }
            logical.update(&bytes);
            let content = self.persist_bytes(&bytes).await?;
            chunks.push(ChunkSpan {
                logical_offset: offset,
                logical_length: content.logical_length,
                content,
            });
            offset = end;
        }
        let chunk_size = u64::from(chunk_size);
        build_version(
            size,
            logical.finalize().into(),
            FileVersionLayout::Chunked {
                chunking: ChunkingSpec {
                    algorithm: ChunkingAlgorithm::Fixed,
                    minimum_size: chunk_size,
                    target_size: chunk_size,
                    maximum_size: chunk_size,
                },
                chunks,
            },
        )
    }

    async fn seal_fastcdc(
        &self,
        local: &Operator,
        path: &str,
        minimum_size: u32,
        target_size: u32,
        maximum_size: u32,
    ) -> Result<FileVersionRecord, ManagedError> {
        let size = frozen_size(local, path).await?;
        if size == 0 {
            return Ok(FileVersionRecord::whole(0, Sha256::digest([]).into()));
        }
        let reader = local
            .reader(path)
            .await
            .map_err(|_| unavailable("read frozen file"))?
            .into_futures_async_read(..)
            .await
            .map_err(|_| unavailable("read frozen file"))?;
        let mut chunker = AsyncStreamCDC::new(reader, minimum_size, target_size, maximum_size);
        let chunks = chunker.as_stream();
        futures::pin_mut!(chunks);
        let mut logical = Sha256::new();
        let mut spans = Vec::new();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| unavailable("chunk frozen file"))?;
            logical.update(&chunk.data);
            let content = self.persist_bytes(&chunk.data).await?;
            spans.push(ChunkSpan {
                logical_offset: chunk.offset,
                logical_length: content.logical_length,
                content,
            });
        }
        build_version(
            size,
            logical.finalize().into(),
            FileVersionLayout::Chunked {
                chunking: ChunkingSpec {
                    algorithm: ChunkingAlgorithm::FastCdcV2020 { revision: 1 },
                    minimum_size: u64::from(minimum_size),
                    target_size: u64::from(target_size),
                    maximum_size: u64::from(maximum_size),
                },
                chunks: spans,
            },
        )
    }

    pub(crate) async fn seal_extents(
        &self,
        local: &Operator,
        path: &str,
        staged: &[SparseExtent],
    ) -> Result<FileVersionRecord, ManagedError> {
        let size = frozen_size(local, path).await?;
        if size == 0 && staged.is_empty() {
            return Ok(FileVersionRecord::whole(0, Sha256::digest([]).into()));
        }
        let mut logical = Sha256::new();
        let mut logical_offset = 0_u64;
        let mut extents = Vec::with_capacity(staged.len());
        for extent in staged {
            let length = match extent {
                SparseExtent::Hole { logical_length } | SparseExtent::Data { logical_length } => {
                    *logical_length
                }
            };
            if length == 0 {
                return Err(invalid(
                    "seal sparse file",
                    "extent length must be positive",
                ));
            }
            let end = logical_offset
                .checked_add(length)
                .filter(|end| *end <= size)
                .ok_or_else(|| invalid("seal sparse file", "extent is outside the frozen file"))?;
            match extent {
                SparseExtent::Hole { .. } => {
                    let reader = local
                        .reader(path)
                        .await
                        .map_err(|_| unavailable("read frozen file"))?;
                    let mut offset = logical_offset;
                    while offset < end {
                        let range_end = (offset + READ_WINDOW).min(end);
                        let bytes = reader
                            .read(offset..range_end)
                            .await
                            .map_err(|_| unavailable("read frozen file"))?
                            .to_bytes();
                        if bytes.len() as u64 != range_end - offset {
                            return Err(corrupt(
                                "read frozen file",
                                "source returned a short range",
                            ));
                        }
                        if bytes.iter().any(|byte| *byte != 0) {
                            return Err(invalid("seal sparse file", "declared hole contains data"));
                        }
                        logical.update(&bytes);
                        offset = range_end;
                    }
                    extents.push(FileExtent::Hole {
                        logical_offset,
                        logical_length: length,
                    });
                }
                SparseExtent::Data { .. } => {
                    let digest =
                        digest_range(local, path, logical_offset..end, &mut logical).await?;
                    let content = self
                        .persist_range(local, path, logical_offset..end, digest)
                        .await?;
                    extents.push(FileExtent::Data {
                        extent: DataExtent {
                            logical_offset,
                            logical_length: length,
                            data_offset: 0,
                            content,
                        },
                    });
                }
            }
            logical_offset = end;
        }
        if logical_offset != size {
            return Err(invalid(
                "seal sparse file",
                "extents do not cover the frozen file",
            ));
        }
        build_version(
            size,
            logical.finalize().into(),
            FileVersionLayout::Extents { extents },
        )
    }

    pub(crate) async fn pack_reachable(
        &self,
        snapshot: &NamespaceSnapshot,
        operation: OperationId,
    ) -> Result<PackMaintenance, ManagedError> {
        let contents = reachable_content(snapshot, "pack reachable data")?;

        let mut index = PackIndex::open_or_empty(self.operator.clone()).await?;
        let store = PackStore::new(self.operator.clone())?;
        let mut batch = Vec::new();
        let mut batch_bytes = 0_u64;
        let mut sealed = Vec::new();
        let mut packed_content = Vec::new();
        let mut logical_bytes = 0_u64;
        for content in contents {
            if content.logical_length == 0
                || content.logical_length > SMALL_CONTENT_LIMIT
                || !index.locations(content).is_empty()
            {
                continue;
            }
            if batch_bytes + content.logical_length > PACK_LOGICAL_LIMIT {
                let pack = store.seal(operation, std::mem::take(&mut batch)).await?;
                index.add(&pack);
                sealed.push(pack);
                batch_bytes = 0;
            }
            batch.push(self.read_loose_content(content).await?);
            batch_bytes += content.logical_length;
            logical_bytes += content.logical_length;
            packed_content.push(content);
        }
        if !batch.is_empty() {
            let pack = store.seal(operation, batch).await?;
            index.add(&pack);
            sealed.push(pack);
        }
        if sealed.is_empty() {
            return Ok(PackMaintenance {
                packs: Vec::new(),
                packed_content: Vec::new(),
                logical_bytes: 0,
                reclaimable_loose: Vec::new(),
            });
        }

        index.persist().await?;
        Ok(PackMaintenance {
            packs: sealed.iter().map(|pack| pack.id).collect(),
            reclaimable_loose: packed_content.clone(),
            packed_content,
            logical_bytes,
        })
    }

    pub(crate) async fn repack_reachable(
        &self,
        snapshot: &NamespaceSnapshot,
        operation: OperationId,
    ) -> Result<Option<PackRetirement>, ManagedError> {
        let live = reachable_content(snapshot, "repack content")?;
        let Some(mut index) = PackIndex::open(self.operator.clone()).await? else {
            return Ok(None);
        };
        let store = PackStore::new(self.operator.clone())?;
        let mut retired = BTreeSet::new();
        let mut protected = BTreeSet::new();
        for id in index.pack_ids() {
            let pack = store.inspect(id).await?;
            index.validate_pack(&pack)?;
            let pack_live = pack
                .locations
                .keys()
                .filter(|content| live.contains(content))
                .copied()
                .collect::<BTreeSet<_>>();
            if pack_live.len() < pack.locations.len() {
                retired.insert(id);
                protected.extend(pack_live);
            }
        }
        if retired.is_empty() {
            return Ok(None);
        }
        index.require_update()?;

        let mut batch = Vec::new();
        let mut batch_bytes = 0_u64;
        let mut replacements = Vec::new();
        for content in &protected {
            if !batch.is_empty() && batch_bytes + content.logical_length > PACK_LOGICAL_LIMIT {
                let replacement = store.seal(operation, std::mem::take(&mut batch)).await?;
                store.inspect(replacement.id).await?;
                index.add(&replacement);
                replacements.push(replacement.id);
                batch_bytes = 0;
            }
            let mut bytes = None;
            let mut failure = None;
            for location in index.locations(*content) {
                match store.read(*content, *location).await {
                    Ok(value) => {
                        bytes = Some(value);
                        break;
                    }
                    Err(error) => failure = Some(error),
                }
            }
            batch.push(bytes.ok_or_else(|| {
                failure.unwrap_or_else(|| {
                    corrupt("repack content", "live pack content cannot be resolved")
                })
            })?);
            batch_bytes += content.logical_length;
        }
        if !batch.is_empty() {
            let replacement = store.seal(operation, batch).await?;
            store.inspect(replacement.id).await?;
            index.add(&replacement);
            replacements.push(replacement.id);
        }
        if !replacements.is_empty() {
            index.persist().await?;
        }

        Ok(Some(PackRetirement {
            epoch: PACK_RETIREMENT_EPOCH.fetch_add(1, Ordering::Relaxed),
            fixed_at: snapshot.cursor,
            retired_packs: retired.into_iter().collect(),
            replacement_packs: replacements,
            protected_content: protected,
        }))
    }

    pub(crate) async fn finalize_repack(
        &self,
        current: &NamespaceSnapshot,
        retirement: PackRetirement,
    ) -> Result<Vec<PackId>, ManagedError> {
        if current.cursor.sequence() < retirement.fixed_at.sequence() {
            return Err(invalid(
                "finalize repack",
                "current namespace predates the repack recovery root",
            ));
        }
        let current_live = reachable_content(current, "finalize repack")?;
        let mut index = PackIndex::open(self.operator.clone())
            .await?
            .ok_or_else(|| corrupt("finalize repack", "pack index is missing"))?;
        index.require_update()?;
        let store = PackStore::new(self.operator.clone())?;
        if !self.operator.info().full_capability().delete {
            return Err(unavailable("finalize repack"));
        }

        let mut verified = BTreeSet::new();
        for id in &retirement.replacement_packs {
            verified.extend(store.inspect(*id).await?.locations.into_keys());
        }
        if !retirement.protected_content.is_subset(&verified) {
            return Err(corrupt(
                "finalize repack",
                "replacement packs do not cover all retiring live content",
            ));
        }

        let retired = retirement
            .retired_packs
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let at_risk = current_live
            .into_iter()
            .filter(|content| {
                index
                    .locations(*content)
                    .iter()
                    .any(|location| retired.contains(&location.pack))
            })
            .collect::<Vec<_>>();
        index.remove_packs(&retired);
        for content in at_risk {
            let mut verified_pack = false;
            for location in index.locations(content) {
                if store.read(content, *location).await.is_ok() {
                    verified_pack = true;
                    break;
                }
            }
            if !verified_pack && self.read_loose_content(content).await.is_err() {
                return Err(corrupt(
                    "finalize repack",
                    "retiring a pack would remove the last verified live location",
                ));
            }
        }
        index.persist().await?;
        for id in &retirement.retired_packs {
            store.delete(*id).await?;
        }
        Ok(retirement.retired_packs)
    }

    pub(crate) async fn reclaim_packed_loose(
        &self,
        snapshot: &NamespaceSnapshot,
    ) -> Result<usize, ManagedError> {
        if !self.operator.info().full_capability().delete {
            return Ok(0);
        }
        let contents = reachable_content(snapshot, "reclaim packed loose data")?;
        let Ok(Some(index)) = PackIndex::open(self.operator.clone()).await else {
            return Ok(0);
        };
        let Ok(store) = PackStore::new(self.operator.clone()) else {
            return Ok(0);
        };
        let mut reclaimed = 0;
        for content in contents {
            if content.logical_length == 0 || index.locations(content).is_empty() {
                continue;
            }
            let mut verified = false;
            for location in index.locations(content) {
                if store.read(content, *location).await.is_ok() {
                    verified = true;
                    break;
                }
            }
            if !verified {
                continue;
            }
            let key = loose_key(&content);
            match self.operator.stat(&key).await {
                Ok(metadata)
                    if metadata.is_file()
                        && metadata.content_length() == content.logical_length => {}
                Ok(_) | Err(_) => continue,
            }
            if self.operator.delete(&key).await.is_ok() {
                reclaimed += 1;
            }
        }
        Ok(reclaimed)
    }

    async fn read_loose_content(&self, content: ContentRef) -> Result<Vec<u8>, ManagedError> {
        let key = loose_key(&content);
        let bytes = self
            .operator
            .read(&key)
            .await
            .map_err(|error| referenced_data_error("pack loose data", error))?
            .to_bytes()
            .to_vec();
        if bytes.len() as u64 != content.logical_length
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != content.digest
        {
            return Err(corrupt(
                "pack loose data",
                "loose content does not match its reference",
            ));
        }
        Ok(bytes)
    }

    async fn persist_bytes(&self, bytes: &[u8]) -> Result<ContentRef, ManagedError> {
        let content = ContentRef {
            digest: Sha256::digest(bytes).into(),
            logical_length: u64::try_from(bytes.len())
                .map_err(|_| invalid("write loose data", "content length exceeds format v1"))?,
        };
        if content.logical_length == 0 {
            return Ok(content);
        }
        let key = loose_key(&content);
        match self
            .operator
            .write_with(&key, bytes.to_vec())
            .if_not_exists(true)
            .await
        {
            Ok(_) => {}
            Err(error) if already_exists(&error) => {}
            Err(_) => return Err(unavailable("write loose data")),
        }
        let mut digest = Sha256::new();
        let mut pack_index = None;
        self.copy_content(
            &content,
            0..content.logical_length,
            None,
            &mut digest,
            &mut pack_index,
        )
        .await?;
        Ok(content)
    }

    async fn persist_range(
        &self,
        source: &Operator,
        path: &str,
        range: Range<u64>,
        digest: Digest,
    ) -> Result<ContentRef, ManagedError> {
        let content = ContentRef {
            digest: *digest.as_bytes(),
            logical_length: range.end - range.start,
        };
        let key = loose_key(&content);
        match self.operator.writer_with(&key).if_not_exists(true).await {
            Err(error) if already_exists(&error) => {}
            Err(_) => return Err(unavailable("create loose data")),
            Ok(mut writer) => {
                let observed = match copy_range(source, path, range, Some(&mut writer), None).await
                {
                    Ok(observed) => observed,
                    Err(error) => {
                        let _ = writer.abort().await;
                        return Err(error);
                    }
                };
                if observed != digest {
                    let _ = writer.abort().await;
                    return Err(invalid(
                        "write loose data",
                        "frozen input changed while it was being sealed",
                    ));
                }
                if let Err(error) = writer.close().await
                    && !already_exists(&error)
                {
                    return Err(unavailable("commit loose data"));
                }
            }
        }
        let mut observed = Sha256::new();
        let mut pack_index = None;
        self.copy_content(
            &content,
            0..content.logical_length,
            None,
            &mut observed,
            &mut pack_index,
        )
        .await?;
        Ok(content)
    }

    /// Stream verified content into a caller-owned materialization path.
    pub(crate) async fn read_to(
        &self,
        version: &FileVersionRecord,
        target: &Operator,
        target_path: &str,
    ) -> Result<(), ManagedError> {
        if !version.is_valid() {
            return Err(corrupt(
                "read loose data",
                "file manifest identity is invalid",
            ));
        }
        if version.logical_size == 0 {
            target
                .write(target_path, Vec::<u8>::new())
                .await
                .map_err(|_| unavailable("create materialized file"))?;
            return Ok(());
        }
        let mut writer = target
            .writer(target_path)
            .await
            .map_err(|_| unavailable("create materialized file"))?;
        if let Err(error) = self.copy_version(version, Some(&mut writer)).await {
            let _ = writer.abort().await;
            return Err(error);
        }
        writer
            .close()
            .await
            .map_err(|_| unavailable("commit materialized file"))?;
        Ok(())
    }

    async fn verify(&self, version: &FileVersionRecord) -> Result<(), ManagedError> {
        if !version.is_valid() {
            return Err(corrupt(
                "verify loose data",
                "file manifest identity is invalid",
            ));
        }
        self.copy_version(version, None).await
    }

    async fn copy_version(
        &self,
        version: &FileVersionRecord,
        mut target: Option<&mut Writer>,
    ) -> Result<(), ManagedError> {
        let mut logical = Sha256::new();
        let mut pack_index = None;
        match &version.layout {
            FileVersionLayout::Whole { content } => {
                self.copy_content(
                    content,
                    0..content.logical_length,
                    target.as_deref_mut(),
                    &mut logical,
                    &mut pack_index,
                )
                .await?;
            }
            FileVersionLayout::Chunked { chunks, .. } => {
                for chunk in chunks {
                    self.copy_content(
                        &chunk.content,
                        0..chunk.content.logical_length,
                        target.as_deref_mut(),
                        &mut logical,
                        &mut pack_index,
                    )
                    .await?;
                }
            }
            FileVersionLayout::Extents { extents } => {
                for extent in extents {
                    match extent {
                        FileExtent::Hole { logical_length, .. } => {
                            write_zeroes(target.as_deref_mut(), &mut logical, *logical_length)
                                .await?;
                        }
                        FileExtent::Data { extent } => {
                            let end = extent.data_offset + extent.logical_length;
                            self.copy_content(
                                &extent.content,
                                extent.data_offset..end,
                                target.as_deref_mut(),
                                &mut logical,
                                &mut pack_index,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
        let observed: [u8; 32] = logical.finalize().into();
        if observed != version.logical_digest {
            return Err(corrupt(
                "read loose data",
                "logical content digest does not match the file version",
            ));
        }
        Ok(())
    }

    async fn copy_content(
        &self,
        content: &ContentRef,
        selected: Range<u64>,
        mut target: Option<&mut Writer>,
        logical: &mut Sha256,
        pack_index: &mut Option<PackIndex>,
    ) -> Result<(), ManagedError> {
        if selected.start > selected.end || selected.end > content.logical_length {
            return Err(corrupt("read loose data", "content range is invalid"));
        }
        let key = loose_key(content);
        let reader = match self.operator.reader(&key).await {
            Ok(reader) => reader,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return self
                    .copy_packed_content(content, selected, target, logical, pack_index)
                    .await;
            }
            Err(error) => return Err(referenced_data_error("read loose data", error)),
        };
        let mut stream = match reader.into_stream(..).await {
            Ok(stream) => stream,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return self
                    .copy_packed_content(content, selected, target, logical, pack_index)
                    .await;
            }
            Err(error) => return Err(referenced_data_error("read loose data", error)),
        };
        let mut content_digest = Sha256::new();
        let mut offset = 0_u64;
        while let Some(buffer) = stream.next().await {
            let buffer = match buffer {
                Ok(buffer) => buffer,
                Err(error) if error.kind() == ErrorKind::NotFound && offset == 0 => {
                    return self
                        .copy_packed_content(content, selected, target, logical, pack_index)
                        .await;
                }
                Err(error) => return Err(referenced_data_error("read loose data", error)),
            };
            for bytes in buffer {
                let end = offset
                    .checked_add(bytes.len() as u64)
                    .filter(|end| *end <= content.logical_length)
                    .ok_or_else(|| {
                        corrupt("read loose data", "content is longer than its reference")
                    })?;
                content_digest.update(&bytes);
                let start = offset.max(selected.start);
                let selected_end = end.min(selected.end);
                if start < selected_end {
                    let value =
                        bytes.slice((start - offset) as usize..(selected_end - offset) as usize);
                    logical.update(&value);
                    if let Some(writer) = target.as_deref_mut() {
                        writer
                            .write(value)
                            .await
                            .map_err(|_| unavailable("write materialized file"))?;
                    }
                }
                offset = end;
            }
        }
        if offset != content.logical_length {
            return Err(corrupt("read loose data", "content returned a short range"));
        }
        let observed: [u8; 32] = content_digest.finalize().into();
        if observed != content.digest {
            return Err(corrupt(
                "read loose data",
                "content digest does not match its reference",
            ));
        }
        Ok(())
    }

    async fn copy_packed_content(
        &self,
        content: &ContentRef,
        selected: Range<u64>,
        target: Option<&mut Writer>,
        logical: &mut Sha256,
        pack_index: &mut Option<PackIndex>,
    ) -> Result<(), ManagedError> {
        if pack_index.is_none() {
            *pack_index = PackIndex::open(self.operator.clone()).await?;
        }
        let locations: Vec<PackLocation> = pack_index
            .as_ref()
            .map(|index| index.locations(*content).to_vec())
            .unwrap_or_default();
        if locations.is_empty() {
            return Err(corrupt(
                "read Managed data",
                "file version references missing content",
            ));
        }
        let store = PackStore::new(self.operator.clone())?;
        let mut failure = None;
        let mut packed = None;
        for location in locations {
            match store.read(*content, location).await {
                Ok(bytes) => {
                    packed = Some(bytes);
                    break;
                }
                Err(error) => failure = Some(error),
            }
        }
        let bytes = packed.ok_or_else(|| {
            failure.unwrap_or_else(|| {
                corrupt("read Managed data", "pack locations cannot be resolved")
            })
        })?;
        let value = &bytes[selected.start as usize..selected.end as usize];
        logical.update(value);
        if let Some(writer) = target {
            writer
                .write(value.to_vec())
                .await
                .map_err(|_| unavailable("write materialized file"))?;
        }
        Ok(())
    }
}

fn reachable_content(
    snapshot: &NamespaceSnapshot,
    action: &'static str,
) -> Result<BTreeSet<ContentRef>, ManagedError> {
    let mut contents = BTreeSet::new();
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
                collect_content_refs(&version.layout, &mut contents);
            }
        }
    }
    Ok(contents)
}

fn collect_content_refs(layout: &FileVersionLayout, output: &mut BTreeSet<ContentRef>) {
    match layout {
        FileVersionLayout::Whole { content } => {
            output.insert(*content);
        }
        FileVersionLayout::Chunked { chunks, .. } => {
            output.extend(chunks.iter().map(|chunk| chunk.content));
        }
        FileVersionLayout::Extents { extents } => {
            output.extend(extents.iter().filter_map(|extent| match extent {
                FileExtent::Data { extent } => Some(extent.content),
                FileExtent::Hole { .. } => None,
            }));
        }
    }
}

fn validate_policy(policy: FileLayoutPolicy) -> Result<(), ManagedError> {
    match policy {
        FileLayoutPolicy::Whole => Ok(()),
        FileLayoutPolicy::Fixed { chunk_size } if chunk_size > 0 => Ok(()),
        FileLayoutPolicy::Fixed { .. } => Err(invalid(
            "configure Managed data",
            "fixed chunk size must be positive",
        )),
        FileLayoutPolicy::FastCdcV2020 {
            minimum_size,
            target_size,
            maximum_size,
        } if (MINIMUM_MIN..=MINIMUM_MAX).contains(&minimum_size)
            && (AVERAGE_MIN..=AVERAGE_MAX).contains(&target_size)
            && (MAXIMUM_MIN..=MAXIMUM_MAX).contains(&maximum_size)
            && minimum_size <= target_size
            && target_size <= maximum_size
            && target_size.is_power_of_two() =>
        {
            Ok(())
        }
        FileLayoutPolicy::FastCdcV2020 { .. } => Err(invalid(
            "configure Managed data",
            "FastCDC v2020 sizes are invalid",
        )),
    }
}

async fn frozen_size(source: &Operator, path: &str) -> Result<u64, ManagedError> {
    let metadata = source
        .stat(path)
        .await
        .map_err(|_| unavailable("read frozen file"))?;
    if !metadata.is_file() {
        return Err(invalid("read frozen file", "input is not a regular file"));
    }
    Ok(metadata.content_length())
}

fn build_version(
    size: u64,
    digest: [u8; 32],
    layout: FileVersionLayout,
) -> Result<FileVersionRecord, ManagedError> {
    FileVersionRecord::from_layout(size, digest, layout)
        .ok_or_else(|| invalid("seal Managed data", "generated file manifest is invalid"))
}

async fn digest_range(
    source: &Operator,
    path: &str,
    range: Range<u64>,
    logical: &mut Sha256,
) -> Result<Digest, ManagedError> {
    copy_range(source, path, range, None, Some(logical)).await
}

async fn copy_range(
    source: &Operator,
    path: &str,
    range: Range<u64>,
    mut target: Option<&mut Writer>,
    mut logical: Option<&mut Sha256>,
) -> Result<Digest, ManagedError> {
    let reader = source
        .reader(path)
        .await
        .map_err(|_| unavailable("read frozen file"))?;
    let mut digest = Sha256::new();
    let mut offset = range.start;
    while offset < range.end {
        let end = (offset + READ_WINDOW).min(range.end);
        let bytes = reader
            .read(offset..end)
            .await
            .map_err(|_| unavailable("read frozen file"))?
            .to_bytes();
        if bytes.len() as u64 != end - offset {
            return Err(corrupt("read frozen file", "source returned a short range"));
        }
        digest.update(&bytes);
        if let Some(logical) = logical.as_deref_mut() {
            logical.update(&bytes);
        }
        if let Some(writer) = target.as_deref_mut() {
            writer
                .write(bytes)
                .await
                .map_err(|_| unavailable("write loose data"))?;
        }
        offset = end;
    }
    Ok(Digest::from_bytes(digest.finalize().into()))
}

async fn write_zeroes(
    mut target: Option<&mut Writer>,
    logical: &mut Sha256,
    mut length: u64,
) -> Result<(), ManagedError> {
    const ZEROES: [u8; 8192] = [0; 8192];
    while length > 0 {
        let count = length.min(ZEROES.len() as u64) as usize;
        logical.update(&ZEROES[..count]);
        if let Some(writer) = target.as_deref_mut() {
            writer
                .write(ZEROES[..count].to_vec())
                .await
                .map_err(|_| unavailable("write materialized file"))?;
        }
        length -= count as u64;
    }
    Ok(())
}

async fn digest_and_copy(
    source: &Operator,
    path: &str,
    size: u64,
    mut target: Option<&mut Writer>,
) -> opendal::Result<Digest> {
    let reader = source.reader(path).await?;
    let mut hash = Sha256::new();
    let mut offset = 0;
    while offset < size {
        let end = (offset + READ_WINDOW).min(size);
        let buffer = reader.read(offset..end).await?;
        if buffer.len() as u64 != end - offset {
            return Err(opendal::Error::new(
                ErrorKind::Unexpected,
                "source returned a short range",
            ));
        }
        for bytes in buffer.clone() {
            hash.update(&bytes);
        }
        if let Some(writer) = target.as_deref_mut() {
            writer.write(buffer).await?;
        }
        offset = end;
    }
    Ok(Digest::from_bytes(hash.finalize().into()))
}

fn whole_file_version(size: u64, digest: Digest) -> FileVersionRecord {
    FileVersionRecord::whole(size, *digest.as_bytes())
}

fn loose_key(content: &ContentRef) -> String {
    let digest = Digest::from_bytes(content.digest).hex();
    format!("{LOOSE_ROOT}/{}/{digest}", &digest[..2])
}

fn already_exists(error: &opendal::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
    )
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn referenced_data_error(action: &'static str, error: opendal::Error) -> ManagedError {
    if error.kind() == ErrorKind::NotFound {
        corrupt(action, "file version references missing content")
    } else {
        unavailable(action)
    }
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
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use opendal::services;

    use super::*;
    use crate::filesystem::{
        ChangeCursor, DirectoryEntry, Generation, NodeAttributes, NodeId, NodeKind, VolumeId,
    };
    use crate::managed::namespace::{DirectoryRecord, NodeRecord};

    fn memory() -> Operator {
        Operator::new(services::Memory::default()).unwrap().finish()
    }

    fn pack_test_storage() -> Operator {
        let url = url::Url::parse(
            &std::env::var("OFS_PACK_TEST_STORAGE")
                .expect("set OFS_PACK_TEST_STORAGE to an isolated S3-compatible root"),
        )
        .unwrap();
        let mut arguments = url.query_pairs().into_owned().collect::<Vec<_>>();
        arguments.push(("bucket".to_owned(), url.host_str().unwrap().to_owned()));
        arguments.push(("root".to_owned(), url.path().trim_matches('/').to_owned()));
        Operator::via_iter(url.scheme(), arguments).unwrap()
    }

    fn snapshot_with_file(version: FileVersionRecord) -> NamespaceSnapshot {
        let root = NodeId::from_bytes([21; 16]);
        let file = NodeId::from_bytes([22; 16]);
        let generation = Generation::from_bytes(vec![1]);
        NamespaceSnapshot {
            volume_id: VolumeId::from_bytes([23; 16]),
            cursor: ChangeCursor::at(
                NonZeroU64::new(1).unwrap(),
                OperationId::from_bytes([24; 16]),
            ),
            root,
            nodes: BTreeMap::from([
                (
                    root,
                    NodeRecord {
                        id: root,
                        generation: generation.clone(),
                        kind: NodeKind::Directory,
                        attributes: NodeAttributes::default(),
                        file_version: None,
                    },
                ),
                (
                    file,
                    NodeRecord {
                        id: file,
                        generation: generation.clone(),
                        kind: NodeKind::RegularFile,
                        attributes: NodeAttributes::default(),
                        file_version: Some(version.id),
                    },
                ),
            ]),
            directories: BTreeMap::from([(
                root,
                DirectoryRecord {
                    node: root,
                    generation,
                    entries: BTreeMap::from([(
                        "live".to_owned(),
                        DirectoryEntry {
                            node: file,
                            kind: NodeKind::RegularFile,
                        },
                    )]),
                },
            )]),
            file_versions: BTreeMap::from([(version.id, version)]),
        }
    }

    #[tokio::test]
    async fn fixed_cdc_and_sparse_layouts_round_trip() {
        let source = memory();
        let stored = memory();
        let target = memory();
        let bytes: Vec<u8> = (0..8192).map(|index| (index * 31) as u8).collect();
        source.write("input", bytes.clone()).await.unwrap();

        let mut data = ManagedData::new(stored).unwrap();
        data.set_policy(FileLayoutPolicy::Fixed { chunk_size: 1024 })
            .unwrap();
        let fixed = data.seal_file(&source, "input").await.unwrap();
        assert!(matches!(fixed.layout, FileVersionLayout::Chunked { .. }));
        data.read_to(&fixed, &target, "fixed").await.unwrap();
        assert_eq!(target.read("fixed").await.unwrap().to_bytes(), bytes);

        data.set_policy(FileLayoutPolicy::FastCdcV2020 {
            minimum_size: 64,
            target_size: 256,
            maximum_size: 1024,
        })
        .unwrap();
        let cdc = data.seal_file(&source, "input").await.unwrap();
        data.read_to(&cdc, &target, "cdc").await.unwrap();
        assert_eq!(target.read("cdc").await.unwrap().to_bytes(), bytes);

        let sparse = b"\0\0DATA\0\0\0";
        source.write("sparse", sparse.to_vec()).await.unwrap();
        let extents = data
            .seal_extents(
                &source,
                "sparse",
                &[
                    SparseExtent::Hole { logical_length: 2 },
                    SparseExtent::Data { logical_length: 4 },
                    SparseExtent::Hole { logical_length: 3 },
                ],
            )
            .await
            .unwrap();
        data.read_to(&extents, &target, "sparse").await.unwrap();
        assert_eq!(
            target.read("sparse").await.unwrap().to_bytes(),
            sparse.as_slice()
        );

        let invalid = data
            .seal_extents(
                &source,
                "input",
                &[SparseExtent::Hole {
                    logical_length: bytes.len() as u64,
                }],
            )
            .await
            .unwrap_err();
        assert_eq!(invalid.kind(), ManagedErrorKind::Invalid);
    }

    #[tokio::test]
    async fn reachable_small_content_falls_back_to_a_published_pack() {
        let source = memory();
        let stored = memory();
        let target = memory();
        source.write("small", b"small file".to_vec()).await.unwrap();
        source
            .write("large", vec![7; SMALL_CONTENT_LIMIT as usize + 1])
            .await
            .unwrap();
        source.write("orphan", b"orphan".to_vec()).await.unwrap();
        let data = ManagedData::new(stored.clone()).unwrap();
        let small = data.seal_whole_file(&source, "small").await.unwrap();
        let large = data.seal_whole_file(&source, "large").await.unwrap();
        let orphan = data.seal_whole_file(&source, "orphan").await.unwrap();

        let root = NodeId::from_bytes([1; 16]);
        let small_node = NodeId::from_bytes([2; 16]);
        let large_node = NodeId::from_bytes([3; 16]);
        let generation = Generation::from_bytes(vec![1]);
        let snapshot = NamespaceSnapshot {
            volume_id: VolumeId::from_bytes([4; 16]),
            cursor: ChangeCursor::Genesis,
            root,
            nodes: BTreeMap::from([
                (
                    root,
                    NodeRecord {
                        id: root,
                        generation: generation.clone(),
                        kind: NodeKind::Directory,
                        attributes: NodeAttributes::default(),
                        file_version: None,
                    },
                ),
                (
                    small_node,
                    NodeRecord {
                        id: small_node,
                        generation: generation.clone(),
                        kind: NodeKind::RegularFile,
                        attributes: NodeAttributes::default(),
                        file_version: Some(small.id),
                    },
                ),
                (
                    large_node,
                    NodeRecord {
                        id: large_node,
                        generation,
                        kind: NodeKind::RegularFile,
                        attributes: NodeAttributes::default(),
                        file_version: Some(large.id),
                    },
                ),
            ]),
            directories: BTreeMap::from([(
                root,
                DirectoryRecord {
                    node: root,
                    generation: Generation::from_bytes(vec![1]),
                    entries: BTreeMap::from([
                        (
                            "large".to_owned(),
                            DirectoryEntry {
                                node: large_node,
                                kind: NodeKind::RegularFile,
                            },
                        ),
                        (
                            "small".to_owned(),
                            DirectoryEntry {
                                node: small_node,
                                kind: NodeKind::RegularFile,
                            },
                        ),
                    ]),
                },
            )]),
            file_versions: BTreeMap::from([
                (small.id, small.clone()),
                (large.id, large.clone()),
                (orphan.id, orphan.clone()),
            ]),
        };

        assert_eq!(data.reclaim_packed_loose(&snapshot).await.unwrap(), 0);

        let packed = data
            .pack_reachable(&snapshot, OperationId::from_bytes([5; 16]))
            .await
            .unwrap();
        let small_content = match &small.layout {
            FileVersionLayout::Whole { content } => *content,
            _ => unreachable!(),
        };
        let orphan_content = match &orphan.layout {
            FileVersionLayout::Whole { content } => *content,
            _ => unreachable!(),
        };
        assert_eq!(packed.packed_content, vec![small_content]);
        assert_eq!(packed.reclaimable_loose, vec![small_content]);
        assert_eq!(packed.logical_bytes, small_content.logical_length);
        let index = PackIndex::open(stored.clone()).await.unwrap().unwrap();
        assert!(index.locations(orphan_content).is_empty());

        let pack_hex = packed.packs[0]
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let pack_key = format!("data/v1/packs/{pack_hex}.pack");
        let original = stored.read(&pack_key).await.unwrap().to_bytes();
        let mut corrupt_pack = original.to_vec();
        corrupt_pack[26] ^= 0xff;
        stored.write(&pack_key, corrupt_pack).await.unwrap();
        assert_eq!(data.reclaim_packed_loose(&snapshot).await.unwrap(), 0);
        assert!(stored.stat(&loose_key(&small_content)).await.is_ok());

        stored.write(&pack_key, original).await.unwrap();
        assert_eq!(data.reclaim_packed_loose(&snapshot).await.unwrap(), 1);
        assert!(stored.stat(&loose_key(&small_content)).await.is_err());
        assert!(stored.stat(&loose_key(&orphan_content)).await.is_ok());
        data.read_to(&small, &target, "restored").await.unwrap();
        assert_eq!(
            target.read("restored").await.unwrap().to_bytes(),
            b"small file".as_slice()
        );
        data.read_to(&large, &target, "large").await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires OFS_PACK_TEST_STORAGE pointing at an isolated MinIO root"]
    async fn repack_keeps_live_content_until_old_pack_retirement() {
        let stored = pack_test_storage();
        let target = memory();
        let data = ManagedData::new(stored.clone()).unwrap();
        let live_bytes = b"live after repack".to_vec();
        let dead_bytes = b"dead before repack".to_vec();
        let live_content = ContentRef {
            digest: Sha256::digest(&live_bytes).into(),
            logical_length: live_bytes.len() as u64,
        };
        let dead_content = ContentRef {
            digest: Sha256::digest(&dead_bytes).into(),
            logical_length: dead_bytes.len() as u64,
        };
        let version = whole_file_version(
            live_content.logical_length,
            Digest::from_bytes(live_content.digest),
        );
        let snapshot = snapshot_with_file(version.clone());
        let unchanged = snapshot.clone();
        let dead_version = whole_file_version(
            dead_content.logical_length,
            Digest::from_bytes(dead_content.digest),
        );

        let store = PackStore::new(stored.clone()).unwrap();
        let old = store
            .seal(OperationId::from_bytes([25; 16]), [live_bytes, dead_bytes])
            .await
            .unwrap();
        let mut index = PackIndex::open_or_empty(stored.clone()).await.unwrap();
        index.add(&old);
        index.persist().await.unwrap();

        let retirement = data
            .repack_reachable(&snapshot, OperationId::from_bytes([26; 16]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot, unchanged);
        assert_eq!(retirement.retired_packs(), &[old.id]);
        assert_eq!(retirement.replacement_packs().len(), 1);
        let replacement = store
            .inspect(retirement.replacement_packs()[0])
            .await
            .unwrap();
        assert!(replacement.locations.contains_key(&live_content));
        assert!(!replacement.locations.contains_key(&dead_content));

        let dual = PackIndex::open(stored.clone()).await.unwrap().unwrap();
        assert_eq!(dual.locations(live_content).len(), 2);
        assert_eq!(
            dual.locations(dead_content),
            &[old.locations[&dead_content]]
        );
        data.read_to(&version, &target, "before-finalize")
            .await
            .unwrap();

        let mut current = snapshot.clone();
        let dead_node = NodeId::from_bytes([27; 16]);
        current.cursor = ChangeCursor::at(
            NonZeroU64::new(2).unwrap(),
            OperationId::from_bytes([28; 16]),
        );
        current.nodes.insert(
            dead_node,
            NodeRecord {
                id: dead_node,
                generation: Generation::from_bytes(vec![1]),
                kind: NodeKind::RegularFile,
                attributes: NodeAttributes::default(),
                file_version: Some(dead_version.id),
            },
        );
        current
            .directories
            .get_mut(&current.root)
            .unwrap()
            .entries
            .insert(
                "reintroduced".to_owned(),
                DirectoryEntry {
                    node: dead_node,
                    kind: NodeKind::RegularFile,
                },
            );
        current
            .file_versions
            .insert(dead_version.id, dead_version.clone());
        let error = data
            .finalize_repack(&current, retirement.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
        assert!(store.inspect(old.id).await.is_ok());

        stored
            .write(&loose_key(&dead_content), b"dead before repack".to_vec())
            .await
            .unwrap();
        assert_eq!(
            data.finalize_repack(&current, retirement).await.unwrap(),
            vec![old.id]
        );
        assert!(store.inspect(old.id).await.is_err());
        let finalized = PackIndex::open(stored).await.unwrap().unwrap();
        assert_eq!(finalized.locations(live_content).len(), 1);
        assert!(finalized.locations(dead_content).is_empty());
        data.read_to(&version, &target, "after-finalize")
            .await
            .unwrap();
        data.read_to(&dead_version, &target, "reintroduced")
            .await
            .unwrap();
        assert_eq!(
            target.read("after-finalize").await.unwrap().to_bytes(),
            b"live after repack".as_slice()
        );
    }

    #[tokio::test]
    async fn referenced_chunk_corruption_fails_closed() {
        let source = memory();
        let stored = memory();
        let target = memory();
        source.write("input", b"abcdefgh".to_vec()).await.unwrap();
        let mut data = ManagedData::new(stored.clone()).unwrap();
        data.set_policy(FileLayoutPolicy::Fixed { chunk_size: 4 })
            .unwrap();
        let version = data.seal_file(&source, "input").await.unwrap();
        let FileVersionLayout::Chunked { chunks, .. } = &version.layout else {
            panic!("fixed policy returned another layout")
        };
        stored
            .write(&loose_key(&chunks[0].content), b"bad".to_vec())
            .await
            .unwrap();

        let error = data.read_to(&version, &target, "output").await.unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
    }
}
