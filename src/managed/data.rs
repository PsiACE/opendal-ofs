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

//! File manifests backed by immutable loose objects and whole-file packs.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use fastcdc::v2020::{
    AVERAGE_MAX, AVERAGE_MIN, AsyncStreamCDC, MAXIMUM_MAX, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN,
};
use futures::StreamExt;
use opendal::{ErrorKind, Operator, Writer};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{ManagedError, ManagedErrorKind, ManagedExtension, ManagedFormat};
use crate::filesystem::{NodeKind, OperationId};
use crate::managed::namespace::{
    ChunkSpan, ContentRef, FileVersionLayout, FileVersionRecord, NamespaceSnapshot,
};
use crate::managed::pack::{PackId, PackIndex, PackReadSession, PackStore};

const READ_WINDOW: u64 = 4 * 1024 * 1024;
const LOOSE_ROOT: &str = ".ofs/managed/data/v1/loose/sha256";
const SMALL_CONTENT_LIMIT: u64 = 256 * 1024;
const PACK_LOGICAL_LIMIT: u64 = 8 * 1024 * 1024;

/// Physical locations published by one explicit small whole-file maintenance run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackMaintenance {
    pub(crate) packs: Vec<PackId>,
    pub(crate) packed_content: Vec<ContentRef>,
    pub logical_bytes: u64,
}

impl PackMaintenance {
    pub fn pack_count(&self) -> usize {
        self.packs.len()
    }

    pub fn content_count(&self) -> usize {
        self.packed_content.len()
    }
}

/// Loose data removed by one namespace-fenced garbage-collection sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LooseGcMaintenance {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
}

/// Content identities already referenced by one fixed authority snapshot.
#[derive(Clone, Debug, Default)]
pub(crate) struct AuthorityKnownContent(BTreeSet<ContentRef>);

impl AuthorityKnownContent {
    pub(crate) fn from_snapshot(snapshot: &NamespaceSnapshot) -> Result<Self, ManagedError> {
        reachable_content(snapshot, "derive authority-known content").map(Self)
    }

    fn contains(&self, content: &ContentRef) -> bool {
        self.0.contains(content)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileLayoutPolicy {
    #[default]
    Whole,
    #[serde(rename = "fastcdc_v2020")]
    FastCdcV2020 {
        minimum_file_size: u64,
        minimum_size: u32,
        target_size: u32,
        maximum_size: u32,
    },
}

impl FileLayoutPolicy {
    pub fn validate(self) -> Result<(), ManagedError> {
        validate_policy(self)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Whole => "whole",
            Self::FastCdcV2020 { .. } => "fastcdc_v2020",
        }
    }
}

#[derive(Clone, Copy)]
struct FastCdcSizes {
    minimum: u32,
    target: u32,
    maximum: u32,
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
    fastcdc_enabled: bool,
}

impl ManagedData {
    pub(crate) fn new(operator: Operator, format: &ManagedFormat) -> Result<Self, ManagedError> {
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
            fastcdc_enabled: format.extension_enabled(ManagedExtension::FastCdc),
        })
    }

    pub(crate) fn set_policy(&mut self, policy: FileLayoutPolicy) -> Result<(), ManagedError> {
        policy.validate()?;
        if matches!(policy, FileLayoutPolicy::FastCdcV2020 { .. }) && !self.fastcdc_enabled {
            return Err(unsupported("data-fastcdc/1 is not enabled for this volume"));
        }
        self.policy = policy;
        Ok(())
    }

    pub(crate) fn read_session(&self) -> Result<PackReadSession, ManagedError> {
        PackReadSession::new(self.operator.clone())
    }

    pub(crate) async fn seal_file_with_known_content(
        &self,
        local: &Operator,
        frozen_path: &str,
        known: &AuthorityKnownContent,
    ) -> Result<FileVersionRecord, ManagedError> {
        match self.policy {
            FileLayoutPolicy::Whole => {
                self.seal_whole_file_with_known_content(local, frozen_path, known)
                    .await
            }
            FileLayoutPolicy::FastCdcV2020 {
                minimum_file_size,
                minimum_size,
                target_size,
                maximum_size,
            } => {
                let size = frozen_size(local, frozen_path).await?;
                if size < minimum_file_size {
                    self.seal_whole_file_with_known_content(local, frozen_path, known)
                        .await
                } else {
                    self.seal_fastcdc(
                        local,
                        frozen_path,
                        size,
                        FastCdcSizes {
                            minimum: minimum_size,
                            target: target_size,
                            maximum: maximum_size,
                        },
                        known,
                    )
                    .await
                }
            }
        }
    }

    pub(crate) async fn seal_whole_file_with_known_content(
        &self,
        local: &Operator,
        frozen_path: &str,
        known: &AuthorityKnownContent,
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
        if known.contains(content) {
            return Ok(version);
        }
        let key = loose_key(content);

        let created = match self.operator.writer_with(&key).if_not_exists(true).await {
            Err(error) if already_exists(&error) => false,
            Err(_) => return Err(unavailable("create loose data")),
            Ok(mut writer) => {
                let observed =
                    match digest_and_copy(local, frozen_path, size, Some(&mut writer)).await {
                        Ok(observed) => observed,
                        Err(error) if already_exists(&error) => {
                            let _ = writer.abort().await;
                            self.verify(&version).await?;
                            return Ok(version);
                        }
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
                match writer.close().await {
                    Ok(_) => true,
                    Err(error) if already_exists(&error) => false,
                    Err(_) => return Err(unavailable("commit loose data")),
                }
            }
        };

        if !created {
            self.verify(&version).await?;
        }
        Ok(version)
    }

    async fn seal_fastcdc(
        &self,
        local: &Operator,
        path: &str,
        size: u64,
        sizes: FastCdcSizes,
        known: &AuthorityKnownContent,
    ) -> Result<FileVersionRecord, ManagedError> {
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
        let mut chunker = AsyncStreamCDC::new(reader, sizes.minimum, sizes.target, sizes.maximum);
        let chunks = chunker.as_stream();
        futures::pin_mut!(chunks);
        let mut logical = Sha256::new();
        let mut spans = Vec::new();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| unavailable("chunk frozen file"))?;
            logical.update(&chunk.data);
            let content = self.persist_bytes(&chunk.data, known).await?;
            spans.push(ChunkSpan {
                logical_offset: chunk.offset,
                logical_length: content.logical_length,
                content,
            });
        }
        build_version(
            size,
            logical.finalize().into(),
            FileVersionLayout::FastCdc {
                revision: 1,
                minimum_size: u64::from(sizes.minimum),
                target_size: u64::from(sizes.target),
                maximum_size: u64::from(sizes.maximum),
                chunks: spans,
            },
        )
    }

    pub(crate) async fn pack_reachable(
        &self,
        snapshot: &NamespaceSnapshot,
        operation: OperationId,
    ) -> Result<PackMaintenance, ManagedError> {
        self.validate_snapshot_extensions(snapshot)?;
        let contents = reachable_whole_content(snapshot, "pack reachable data")?;

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
            });
        }

        index.persist().await?;
        Ok(PackMaintenance {
            packs: sealed.iter().map(|pack| pack.id).collect(),
            packed_content,
            logical_bytes,
        })
    }

    pub(crate) async fn rebuild_pack_index(&self) -> Result<usize, ManagedError> {
        PackStore::new(self.operator.clone())?.rebuild_index().await
    }

    pub(crate) async fn collect_unreachable_loose(
        &self,
        snapshot: &NamespaceSnapshot,
    ) -> Result<LooseGcMaintenance, ManagedError> {
        let capability = self.operator.info().full_capability();
        if !capability.list || !capability.delete {
            return Err(unavailable("collect unreachable loose data"));
        }
        let live = reachable_content(snapshot, "collect unreachable loose data")?;
        let mut live_lengths = BTreeMap::<[u8; 32], BTreeSet<u64>>::new();
        for content in &live {
            live_lengths
                .entry(content.digest)
                .or_default()
                .insert(content.logical_length);
        }
        let mut result = LooseGcMaintenance::default();
        let loose = list_loose_content(&self.operator, "collect unreachable loose data").await?;
        let mut deleted = Vec::new();
        for listed in loose {
            let content = listed.content;
            result.scanned += 1;
            if live.contains(&content) {
                continue;
            }
            if live_lengths
                .get(&content.digest)
                .is_some_and(|lengths| !lengths.contains(&content.logical_length))
            {
                return Err(corrupt(
                    "collect unreachable loose data",
                    "live loose content has an unexpected length",
                ));
            }
            deleted.push(listed.path);
            result.deleted += 1;
            result.deleted_bytes = result
                .deleted_bytes
                .checked_add(content.logical_length)
                .ok_or_else(|| {
                    corrupt(
                        "collect unreachable loose data",
                        "deleted byte count exceeds format v1",
                    )
                })?;
        }
        self.operator
            .delete_iter(deleted.iter().map(String::as_str))
            .await
            .map_err(|_| unavailable("collect unreachable loose data"))?;
        Ok(result)
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

    async fn persist_bytes(
        &self,
        bytes: &[u8],
        known: &AuthorityKnownContent,
    ) -> Result<ContentRef, ManagedError> {
        let content = ContentRef {
            digest: Sha256::digest(bytes).into(),
            logical_length: u64::try_from(bytes.len())
                .map_err(|_| invalid("write loose data", "content length exceeds format v1"))?,
        };
        if content.logical_length == 0 || known.contains(&content) {
            return Ok(content);
        }
        let key = loose_key(&content);
        match self
            .operator
            .write_with(&key, bytes.to_vec())
            .if_not_exists(true)
            .await
        {
            Ok(_) => return Ok(content),
            Err(error) if already_exists(&error) => {}
            Err(_) => return Err(unavailable("write loose data")),
        }
        let mut digest = Sha256::new();
        self.copy_content(&content, 0..content.logical_length, None, &mut digest, None)
            .await?;
        Ok(content)
    }

    /// Stream verified content into a caller-owned materialization path.
    #[cfg(test)]
    pub(crate) async fn read_to(
        &self,
        version: &FileVersionRecord,
        target: &Operator,
        target_path: &str,
    ) -> Result<(), ManagedError> {
        let packs = self.read_session()?;
        self.read_to_with(version, target, target_path, &packs)
            .await
    }

    pub(crate) async fn read_to_with(
        &self,
        version: &FileVersionRecord,
        target: &Operator,
        target_path: &str,
        packs: &PackReadSession,
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
        if let Err(error) = self
            .copy_version(version, Some(&mut writer), Some(packs))
            .await
        {
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
        self.copy_version(version, None, None).await
    }

    async fn copy_version(
        &self,
        version: &FileVersionRecord,
        mut target: Option<&mut Writer>,
        packs: Option<&PackReadSession>,
    ) -> Result<(), ManagedError> {
        self.validate_manifest_extension(version)?;
        let mut logical = Sha256::new();
        match &version.layout {
            FileVersionLayout::Whole { content } => {
                self.copy_content(
                    content,
                    0..content.logical_length,
                    target.as_deref_mut(),
                    &mut logical,
                    packs,
                )
                .await?;
            }
            FileVersionLayout::FastCdc { chunks, .. } => {
                for chunk in chunks {
                    self.copy_content(
                        &chunk.content,
                        0..chunk.content.logical_length,
                        target.as_deref_mut(),
                        &mut logical,
                        packs,
                    )
                    .await?;
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

    fn validate_manifest_extension(&self, version: &FileVersionRecord) -> Result<(), ManagedError> {
        if matches!(version.layout, FileVersionLayout::FastCdc { .. }) && !self.fastcdc_enabled {
            Err(unsupported(
                "file manifest requires the disabled data-fastcdc/1 extension",
            ))
        } else {
            Ok(())
        }
    }

    fn validate_snapshot_extensions(
        &self,
        snapshot: &NamespaceSnapshot,
    ) -> Result<(), ManagedError> {
        for version in snapshot.file_versions.values() {
            self.validate_manifest_extension(version)?;
        }
        Ok(())
    }

    async fn copy_content(
        &self,
        content: &ContentRef,
        selected: Range<u64>,
        mut target: Option<&mut Writer>,
        logical: &mut Sha256,
        packs: Option<&PackReadSession>,
    ) -> Result<(), ManagedError> {
        if selected.start > selected.end || selected.end > content.logical_length {
            return Err(corrupt("read loose data", "content range is invalid"));
        }
        let mut pack_failure = None;
        if let Some(packs) = packs {
            match packs.read(*content).await {
                Ok(Some(bytes)) => {
                    return write_packed_range(&bytes, selected, target, logical).await;
                }
                Ok(None) => {}
                Err(error) => pack_failure = Some(error),
            }
        }

        let key = loose_key(content);
        let reader = match self.operator.reader(&key).await {
            Ok(reader) => reader,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(pack_failure.unwrap_or_else(|| {
                    corrupt(
                        "read Managed data",
                        "file version references missing content",
                    )
                }));
            }
            Err(error) => return Err(referenced_data_error("read loose data", error)),
        };
        let mut stream = match reader.into_stream(..).await {
            Ok(stream) => stream,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(pack_failure.unwrap_or_else(|| {
                    corrupt(
                        "read Managed data",
                        "file version references missing content",
                    )
                }));
            }
            Err(error) => return Err(referenced_data_error("read loose data", error)),
        };
        let mut content_digest = Sha256::new();
        let mut offset = 0_u64;
        while let Some(buffer) = stream.next().await {
            let buffer = match buffer {
                Ok(buffer) => buffer,
                Err(error) if error.kind() == ErrorKind::NotFound && offset == 0 => {
                    return Err(pack_failure.unwrap_or_else(|| {
                        corrupt(
                            "read Managed data",
                            "file version references missing content",
                        )
                    }));
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
}

async fn write_packed_range(
    bytes: &[u8],
    selected: Range<u64>,
    target: Option<&mut Writer>,
    logical: &mut Sha256,
) -> Result<(), ManagedError> {
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

fn reachable_content(
    snapshot: &NamespaceSnapshot,
    action: &'static str,
) -> Result<BTreeSet<ContentRef>, ManagedError> {
    let mut contents = BTreeSet::new();
    visit_reachable_file_versions(snapshot, action, |version| {
        collect_content_refs(&version.layout, &mut contents);
    })?;
    Ok(contents)
}

fn reachable_whole_content(
    snapshot: &NamespaceSnapshot,
    action: &'static str,
) -> Result<BTreeSet<ContentRef>, ManagedError> {
    let mut contents = BTreeSet::new();
    visit_reachable_file_versions(snapshot, action, |version| {
        if let FileVersionLayout::Whole { content } = version.layout {
            contents.insert(content);
        }
    })?;
    Ok(contents)
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

fn collect_content_refs(layout: &FileVersionLayout, output: &mut BTreeSet<ContentRef>) {
    match layout {
        FileVersionLayout::Whole { content } => {
            output.insert(*content);
        }
        FileVersionLayout::FastCdc { chunks, .. } => {
            output.extend(chunks.iter().map(|chunk| chunk.content));
        }
    }
}

fn validate_policy(policy: FileLayoutPolicy) -> Result<(), ManagedError> {
    match policy {
        FileLayoutPolicy::Whole => Ok(()),
        FileLayoutPolicy::FastCdcV2020 {
            minimum_file_size: _,
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

struct ListedLooseContent {
    path: String,
    content: ContentRef,
}

async fn list_loose_content(
    operator: &Operator,
    action: &'static str,
) -> Result<Vec<ListedLooseContent>, ManagedError> {
    let entries = operator
        .list_with(&format!("{LOOSE_ROOT}/"))
        .recursive(true)
        .await
        .map_err(|_| unavailable(action))?;
    Ok(entries
        .into_iter()
        .filter_map(|entry| {
            if !entry.metadata().is_file() {
                return None;
            }
            let digest = parse_loose_key(entry.path())?;
            Some(ListedLooseContent {
                path: entry.path().to_owned(),
                content: ContentRef {
                    digest,
                    logical_length: entry.metadata().content_length(),
                },
            })
        })
        .collect())
}

fn parse_loose_key(path: &str) -> Option<[u8; 32]> {
    let relative = path.strip_prefix(&format!("{LOOSE_ROOT}/"))?;
    let (partition, encoded) = relative.split_once('/')?;
    if partition.len() != 2
        || encoded.len() != 64
        || encoded.contains('/')
        || partition != &encoded[..2]
    {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (output, pair) in digest.iter_mut().zip(encoded.as_bytes().chunks_exact(2)) {
        let high = decode_lower_hex(pair[0])?;
        let low = decode_lower_hex(pair[1])?;
        *output = high << 4 | low;
    }
    Some(digest)
}

const fn decode_lower_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
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

fn unsupported(message: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::UnsupportedFormat,
        "use Managed data",
        message,
    )
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

    fn managed_data(operator: Operator) -> ManagedData {
        let format = ManagedFormat::v1(
            VolumeId::from_bytes([42; 16]),
            crate::managed::MetadataPlacement::ColocatedObject,
            [ManagedExtension::FastCdc],
        )
        .unwrap();
        ManagedData::new(operator, &format).unwrap()
    }

    async fn seal_file(data: &ManagedData, source: &Operator, path: &str) -> FileVersionRecord {
        data.seal_file_with_known_content(source, path, &AuthorityKnownContent::default())
            .await
            .unwrap()
    }

    async fn seal_whole_file(
        data: &ManagedData,
        source: &Operator,
        path: &str,
    ) -> FileVersionRecord {
        data.seal_whole_file_with_known_content(source, path, &AuthorityKnownContent::default())
            .await
            .unwrap()
    }

    fn snapshot_with_file(version: FileVersionRecord) -> NamespaceSnapshot {
        snapshot_with_files([("live", version)])
    }

    fn snapshot_with_files<const N: usize>(
        files: [(&str, FileVersionRecord); N],
    ) -> NamespaceSnapshot {
        let root = NodeId::from_bytes([21; 16]);
        let generation = Generation::from_bytes(vec![1]);
        let mut nodes = BTreeMap::from([(
            root,
            NodeRecord {
                id: root,
                generation: generation.clone(),
                kind: NodeKind::Directory,
                attributes: NodeAttributes::default(),
                file_version: None,
            },
        )]);
        let mut entries = BTreeMap::new();
        let mut file_versions = BTreeMap::new();
        for (index, (path, version)) in files.into_iter().enumerate() {
            let file = NodeId::from_bytes([22 + index as u8; 16]);
            nodes.insert(
                file,
                NodeRecord {
                    id: file,
                    generation: generation.clone(),
                    kind: NodeKind::RegularFile,
                    attributes: NodeAttributes::default(),
                    file_version: Some(version.id),
                },
            );
            entries.insert(
                path.to_owned(),
                DirectoryEntry {
                    node: file,
                    kind: NodeKind::RegularFile,
                },
            );
            file_versions.insert(version.id, version);
        }
        NamespaceSnapshot {
            volume_id: VolumeId::from_bytes([23; 16]),
            cursor: ChangeCursor::at(
                NonZeroU64::new(1).unwrap(),
                OperationId::from_bytes([24; 16]),
            ),
            root,
            nodes,
            directories: BTreeMap::from([(
                root,
                DirectoryRecord {
                    node: root,
                    generation,
                    entries,
                },
            )]),
            file_versions,
        }
    }

    #[tokio::test]
    async fn whole_and_fastcdc_layouts_round_trip() {
        let source = memory();
        let stored = memory();
        let target = memory();
        let bytes: Vec<u8> = (0..8192).map(|index| (index * 31) as u8).collect();
        source.write("input", bytes.clone()).await.unwrap();

        let mut data = managed_data(stored);

        data.set_policy(FileLayoutPolicy::FastCdcV2020 {
            minimum_file_size: bytes.len() as u64 + 1,
            minimum_size: 64,
            target_size: 256,
            maximum_size: 1024,
        })
        .unwrap();
        let below_threshold = seal_file(&data, &source, "input").await;
        assert!(matches!(
            below_threshold.layout,
            FileVersionLayout::Whole { .. }
        ));

        data.set_policy(FileLayoutPolicy::FastCdcV2020 {
            minimum_file_size: 0,
            minimum_size: 64,
            target_size: 256,
            maximum_size: 1024,
        })
        .unwrap();
        let cdc = seal_file(&data, &source, "input").await;
        assert!(matches!(cdc.layout, FileVersionLayout::FastCdc { .. }));
        data.read_to(&cdc, &target, "cdc").await.unwrap();
        assert_eq!(target.read("cdc").await.unwrap().to_bytes(), bytes);
    }

    #[tokio::test]
    async fn loose_gc_retains_unknown_keys_and_fails_before_deleting_on_live_mismatch() {
        let source = memory();
        let stored = memory();
        source.write("live", b"live".to_vec()).await.unwrap();
        source.write("orphan", b"orphan".to_vec()).await.unwrap();
        let data = managed_data(stored.clone());
        let live = seal_whole_file(&data, &source, "live").await;
        let orphan = seal_whole_file(&data, &source, "orphan").await;
        let snapshot = snapshot_with_file(live.clone());
        let FileVersionLayout::Whole {
            content: live_content,
        } = live.layout
        else {
            unreachable!()
        };
        let FileVersionLayout::Whole {
            content: orphan_content,
        } = orphan.layout
        else {
            unreachable!()
        };
        let unknown = format!("{LOOSE_ROOT}/unknown");
        stored.write(&unknown, b"keep".to_vec()).await.unwrap();

        let collected = data.collect_unreachable_loose(&snapshot).await.unwrap();
        assert_eq!(collected.scanned, 2);
        assert_eq!(collected.deleted, 1);
        assert_eq!(collected.deleted_bytes, orphan_content.logical_length);
        assert!(stored.stat(&loose_key(&live_content)).await.is_ok());
        assert!(stored.stat(&loose_key(&orphan_content)).await.is_err());
        assert!(stored.stat(&unknown).await.is_ok());

        stored
            .write(&loose_key(&live_content), b"wrong length".to_vec())
            .await
            .unwrap();
        let later = ContentRef {
            digest: Sha256::digest(b"later orphan").into(),
            logical_length: b"later orphan".len() as u64,
        };
        stored
            .write(&loose_key(&later), b"later orphan".to_vec())
            .await
            .unwrap();
        let error = data.collect_unreachable_loose(&snapshot).await.unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
        assert!(stored.stat(&loose_key(&later)).await.is_ok());
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
        let data = managed_data(stored.clone());
        let small = seal_whole_file(&data, &source, "small").await;
        let large = seal_whole_file(&data, &source, "large").await;
        let orphan = seal_whole_file(&data, &source, "orphan").await;

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
        assert_eq!(packed.logical_bytes, small_content.logical_length);
        let index = PackIndex::open(stored.clone()).await.unwrap().unwrap();
        assert!(index.locations(orphan_content).is_empty());

        let pack_key = stored
            .list(".ofs/managed/indexes/data-pack/v1/packs/sha256/")
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.path().to_owned())
            .find(|path| path.ends_with(".pack"))
            .unwrap();
        let original = stored.read(&pack_key).await.unwrap().to_bytes();
        let mut corrupt_pack = original.to_vec();
        corrupt_pack[26] ^= 0xff;
        stored.write(&pack_key, corrupt_pack).await.unwrap();
        assert!(stored.stat(&loose_key(&small_content)).await.is_ok());
        data.read_to(&small, &target, "loose-fallback")
            .await
            .unwrap();
        assert_eq!(
            target.read("loose-fallback").await.unwrap().to_bytes(),
            b"small file".as_slice()
        );

        stored.write(&pack_key, original).await.unwrap();
        assert!(stored.stat(&loose_key(&small_content)).await.is_ok());
        assert!(stored.stat(&loose_key(&orphan_content)).await.is_ok());
        data.read_to(&small, &target, "restored").await.unwrap();
        assert_eq!(
            target.read("restored").await.unwrap().to_bytes(),
            b"small file".as_slice()
        );
        data.read_to(&large, &target, "large").await.unwrap();
    }

    #[tokio::test]
    async fn small_file_pack_excludes_fastcdc_chunks() {
        let source = memory();
        let stored = memory();
        let target = memory();
        source.write("whole", b"whole file".to_vec()).await.unwrap();
        source.write("chunked", b"abcdefgh".to_vec()).await.unwrap();

        let mut data = managed_data(stored.clone());
        let whole = seal_whole_file(&data, &source, "whole").await;
        data.set_policy(FileLayoutPolicy::FastCdcV2020 {
            minimum_file_size: 0,
            minimum_size: 64,
            target_size: 256,
            maximum_size: 1024,
        })
        .unwrap();
        let chunked = seal_file(&data, &source, "chunked").await;
        let snapshot =
            snapshot_with_files([("whole", whole.clone()), ("chunked", chunked.clone())]);

        let packed = data
            .pack_reachable(&snapshot, OperationId::from_bytes([31; 16]))
            .await
            .unwrap();
        let FileVersionLayout::Whole { content: whole_ref } = whole.layout else {
            unreachable!()
        };
        assert_eq!(packed.packed_content, vec![whole_ref]);

        let index = PackIndex::open(stored.clone()).await.unwrap().unwrap();
        let FileVersionLayout::FastCdc { chunks, .. } = &chunked.layout else {
            unreachable!()
        };
        assert!(
            chunks
                .iter()
                .all(|chunk| index.locations(chunk.content).is_empty())
        );
        data.read_to(&chunked, &target, "chunked").await.unwrap();
        assert_eq!(
            target.read("chunked").await.unwrap().to_bytes(),
            b"abcdefgh".as_slice()
        );
    }

    #[tokio::test]
    async fn referenced_chunk_corruption_fails_closed() {
        let source = memory();
        let stored = memory();
        let target = memory();
        source.write("input", vec![7; 8192]).await.unwrap();
        let mut data = managed_data(stored.clone());
        data.set_policy(FileLayoutPolicy::FastCdcV2020 {
            minimum_file_size: 0,
            minimum_size: 64,
            target_size: 256,
            maximum_size: 1024,
        })
        .unwrap();
        let version = seal_file(&data, &source, "input").await;
        let FileVersionLayout::FastCdc { chunks, .. } = &version.layout else {
            panic!("FastCDC policy returned another layout")
        };
        stored
            .write(&loose_key(&chunks[0].content), b"bad".to_vec())
            .await
            .unwrap();

        let error = data.read_to(&version, &target, "output").await.unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
    }
}
