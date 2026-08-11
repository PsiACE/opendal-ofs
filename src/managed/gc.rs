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

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use futures::TryStreamExt as _;

use crate::filesystem::{OperationId, VolumeError, VolumeErrorKind};

use super::ManagedVolume;
use super::head::GcFence;
use super::object;

const OBJECT_PREFIX: &str = "managed/1/objects/";
const INITIAL_PARTITIONS: usize = 256;
const MAX_UNIQUE_MARKS_PER_PARTITION: usize = 64 * 1024;
const MARK_RECORD_BYTES: usize = 1 + 32 + 8;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOutcome {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
    pub retained_from: u64,
}

impl ManagedVolume {
    pub async fn collect_unreachable(
        &self,
        resume: bool,
        retain_from: Option<u64>,
        orphan_grace: Duration,
    ) -> Result<GcOutcome, VolumeError> {
        if resume && retain_from.is_some() {
            return Err(VolumeError::new(
                VolumeErrorKind::Invalid,
                "resume Managed data collection: retained change cannot be replaced",
            ));
        }
        let capability = self.operator().info().full_capability();
        if !capability.list || !capability.delete || (!orphan_grace.is_zero() && !capability.stat) {
            return Err(VolumeError::new(
                VolumeErrorKind::Invalid,
                "collect Managed data: storage lacks a required list, stat, or delete capability",
            ));
        }
        if !orphan_grace.is_zero()
            && object::last_modified(self.operator(), "managed/1/head")
                .await?
                .is_none()
        {
            return Err(VolumeError::new(
                VolumeErrorKind::Invalid,
                "collect Managed data: storage does not expose object modification time",
            ));
        }
        let fence = self.begin_gc(resume, retain_from).await?;
        let delete_before = if orphan_grace.is_zero() {
            None
        } else {
            let Some(started_at) = object::last_modified(self.operator(), "managed/1/head").await?
            else {
                self.cancel_gc(fence).await?;
                return Err(VolumeError::new(
                    VolumeErrorKind::Invalid,
                    "collect Managed data: collection fence has no storage modification time",
                ));
            };
            Some(
                started_at
                    .checked_sub(orphan_grace)
                    .unwrap_or(SystemTime::UNIX_EPOCH),
            )
        };
        let mut live = LiveObjects::create(fence.owner)?;
        self.visit_reachable_objects(
            fence.namespace_commit,
            fence.retention_horizon,
            |key, length| live.insert(&key, length),
        )
        .await?;
        live.seal()?;
        let mut outcome = self.sweep(&mut live, delete_before).await?;
        self.finish_gc(fence).await?;
        outcome.retained_from = fence.retention_horizon.cursor().sequence();
        Ok(outcome)
    }

    async fn begin_gc(
        &self,
        resume: bool,
        retain_from: Option<u64>,
    ) -> Result<GcFence, VolumeError> {
        let (mut head, revision) = self.read_head().await?;
        let owner = OperationId::generate();
        let fence = match (resume, head.maintenance) {
            (false, None) => {
                let retention_horizon = match retain_from {
                    Some(sequence) => self.retention_horizon_at(&head, sequence).await?,
                    None => head.retention_horizon,
                };
                let maintenance_generation = head
                    .maintenance_generation
                    .checked_add(1)
                    .ok_or_else(|| corrupt("maintenance generation overflows"))?;
                head.maintenance_generation = maintenance_generation;
                GcFence {
                    owner,
                    namespace_commit: head.namespace_commit,
                    retention_horizon,
                    maintenance_generation,
                }
            }
            (false, Some(_)) => {
                return Err(conflict(
                    "begin Managed data collection: another collection is active",
                ));
            }
            (true, Some(active))
                if active.namespace_commit == head.namespace_commit
                    && active.maintenance_generation == head.maintenance_generation =>
            {
                GcFence { owner, ..active }
            }
            (true, Some(_)) => {
                return Err(corrupt(
                    "resume Managed data collection: fence cursor is invalid",
                ));
            }
            (true, None) => {
                return Err(conflict(
                    "resume Managed data collection: no interrupted collection is active",
                ));
            }
        };
        head.maintenance = Some(fence);
        if !self.replace_head(&revision, &head).await? {
            return Err(conflict(
                "begin Managed data collection: namespace authority changed",
            ));
        }
        Ok(fence)
    }

    async fn sweep(
        &self,
        live: &mut LiveObjects,
        delete_before: Option<SystemTime>,
    ) -> Result<GcOutcome, VolumeError> {
        let mut outcome = GcOutcome::default();
        let mut deleter = self
            .operator()
            .deleter()
            .await
            .map_err(|_| unavailable("open Managed data deleter"))?;

        let mut pending = live.initial_partitions();
        while let Some(partition) = pending.pop() {
            let Some(marks) = live.load_partition(&partition)? else {
                pending.extend(live.split_partition(partition)?);
                continue;
            };
            for kind in ObjectKind::ALL {
                let object_prefix = kind.object_prefix(&partition.digest_prefix);
                let mut lister = self
                    .operator()
                    .lister_with(&object_prefix)
                    .recursive(true)
                    .await
                    .map_err(|_| unavailable("list Managed data objects"))?;
                while let Some(entry) = lister
                    .try_next()
                    .await
                    .map_err(|_| unavailable("list Managed data objects"))?
                {
                    if !entry.metadata().is_file() {
                        continue;
                    }
                    let Some(identity) = ObjectIdentity::parse(entry.path()) else {
                        continue;
                    };
                    outcome.scanned += 1;
                    let length = entry.metadata().content_length();
                    match marks.get(&identity) {
                        Some(expected) if *expected == length => continue,
                        Some(_) => {
                            return Err(corrupt("live Managed object length is invalid"));
                        }
                        None => {}
                    }
                    if let Some(delete_before) = delete_before {
                        let mut metadata = entry.metadata().clone();
                        if metadata.last_modified().is_none() {
                            metadata = self
                                .operator()
                                .stat(entry.path())
                                .await
                                .map_err(|_| unavailable("inspect Managed data object"))?;
                        }
                        let Some(last_modified) = metadata.last_modified().map(SystemTime::from)
                        else {
                            continue;
                        };
                        if last_modified > delete_before {
                            continue;
                        }
                    }
                    deleter
                        .delete(entry.path())
                        .await
                        .map_err(|_| unavailable("delete Managed data object"))?;
                    outcome.deleted += 1;
                    outcome.deleted_bytes = outcome
                        .deleted_bytes
                        .checked_add(length)
                        .ok_or_else(|| corrupt("deleted Managed data byte count overflows"))?;
                }
            }
        }
        deleter
            .close()
            .await
            .map_err(|_| unavailable("finish Managed data deletion"))?;
        Ok(outcome)
    }

    async fn finish_gc(&self, fence: GcFence) -> Result<(), VolumeError> {
        let (mut head, revision) = self.read_head().await?;
        if head.maintenance != Some(fence) || head.namespace_commit != fence.namespace_commit {
            return Err(conflict(
                "finish Managed data collection: collection fence changed",
            ));
        }
        head.retention_horizon = fence.retention_horizon;
        head.maintenance = None;
        if self.replace_head(&revision, &head).await? {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed data collection: namespace authority changed",
            ))
        }
    }

    async fn cancel_gc(&self, fence: GcFence) -> Result<(), VolumeError> {
        let (mut head, revision) = self.read_head().await?;
        if head.maintenance != Some(fence) {
            return Err(conflict(
                "cancel Managed data collection: collection fence changed",
            ));
        }
        head.maintenance = None;
        if self.replace_head(&revision, &head).await? {
            Ok(())
        } else {
            Err(conflict(
                "cancel Managed data collection: namespace authority changed",
            ))
        }
    }
}

struct LiveObjects {
    directory: PathBuf,
    writers: Vec<Option<BufWriter<File>>>,
}

impl LiveObjects {
    fn create(operation: OperationId) -> Result<Self, VolumeError> {
        let directory = std::env::temp_dir().join(format!("ofs-managed-gc-{operation}"));
        fs::create_dir(&directory)
            .map_err(|_| unavailable("create Managed collection mark store"))?;
        Ok(Self {
            directory,
            writers: std::iter::repeat_with(|| None)
                .take(INITIAL_PARTITIONS)
                .collect(),
        })
    }

    fn insert(&mut self, key: &str, length: u64) -> Result<(), VolumeError> {
        let identity = ObjectIdentity::parse(key)
            .ok_or_else(|| corrupt("reachable Managed object key is invalid"))?;
        let partition = usize::from(identity.digest[0]);
        if self.writers[partition].is_none() {
            let path = self.partition_path(&format!("{partition:02x}"));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|_| unavailable("create Managed collection mark partition"))?;
            self.writers[partition] = Some(BufWriter::new(file));
        }
        MarkRecord { identity, length }.write(
            self.writers[partition]
                .as_mut()
                .expect("the mark partition writer is open"),
        )
    }

    fn seal(&mut self) -> Result<(), VolumeError> {
        for writer in self.writers.iter_mut().flatten() {
            writer
                .flush()
                .map_err(|_| unavailable("flush Managed collection marks"))?;
        }
        self.writers.clear();
        Ok(())
    }

    fn initial_partitions(&self) -> Vec<MarkPartition> {
        (0..INITIAL_PARTITIONS)
            .map(|partition| {
                let digest_prefix = format!("{partition:02x}");
                let path = self.partition_path(&digest_prefix);
                MarkPartition {
                    path: path.exists().then_some(path),
                    digest_prefix,
                }
            })
            .collect()
    }

    fn load_partition(
        &self,
        partition: &MarkPartition,
    ) -> Result<Option<BTreeMap<ObjectIdentity, u64>>, VolumeError> {
        let Some(path) = &partition.path else {
            return Ok(Some(BTreeMap::new()));
        };
        let mut reader = MarkReader::open(path)?;
        let mut marks = BTreeMap::new();
        while let Some(record) = reader.next()? {
            if marks
                .insert(record.identity, record.length)
                .is_some_and(|length| length != record.length)
            {
                return Err(corrupt("one Managed object has conflicting lengths"));
            }
            if marks.len() > MAX_UNIQUE_MARKS_PER_PARTITION && partition.digest_prefix.len() < 64 {
                return Ok(None);
            }
        }
        Ok(Some(marks))
    }

    fn split_partition(&self, partition: MarkPartition) -> Result<Vec<MarkPartition>, VolumeError> {
        let path = partition
            .path
            .as_ref()
            .expect("only a non-empty mark partition is split");
        let mut reader = MarkReader::open(path)?;
        let mut writers: Vec<Option<BufWriter<File>>> =
            std::iter::repeat_with(|| None).take(16).collect();
        while let Some(record) = reader.next()? {
            let child = record.identity.nibble(partition.digest_prefix.len());
            if writers[child].is_none() {
                let child_prefix = format!("{}{child:x}", partition.digest_prefix);
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(self.partition_path(&child_prefix))
                    .map_err(|_| unavailable("create Managed collection mark partition"))?;
                writers[child] = Some(BufWriter::new(file));
            }
            record.write(
                writers[child]
                    .as_mut()
                    .expect("the child mark partition writer is open"),
            )?;
        }
        for writer in writers.iter_mut().flatten() {
            writer
                .flush()
                .map_err(|_| unavailable("flush Managed collection marks"))?;
        }
        drop(writers);
        drop(reader);
        fs::remove_file(path)
            .map_err(|_| unavailable("remove split Managed collection mark partition"))?;

        Ok((0..16)
            .map(|child| {
                let digest_prefix = format!("{}{child:x}", partition.digest_prefix);
                let path = self.partition_path(&digest_prefix);
                MarkPartition {
                    path: path.exists().then_some(path),
                    digest_prefix,
                }
            })
            .collect())
    }

    fn partition_path(&self, digest_prefix: &str) -> PathBuf {
        self.directory.join(format!("marks-{digest_prefix}"))
    }
}

impl Drop for LiveObjects {
    fn drop(&mut self) {
        self.writers.clear();
        let _ = fs::remove_dir_all(&self.directory);
    }
}

struct MarkPartition {
    path: Option<PathBuf>,
    digest_prefix: String,
}

struct MarkReader {
    reader: BufReader<File>,
    remaining: u64,
}

impl MarkReader {
    fn open(path: &Path) -> Result<Self, VolumeError> {
        let file =
            File::open(path).map_err(|_| unavailable("open Managed collection mark partition"))?;
        let length = file
            .metadata()
            .map_err(|_| unavailable("inspect Managed collection mark partition"))?
            .len();
        let record_bytes = MARK_RECORD_BYTES as u64;
        if length % record_bytes != 0 {
            return Err(corrupt("Managed collection mark partition is invalid"));
        }
        Ok(Self {
            reader: BufReader::new(file),
            remaining: length / record_bytes,
        })
    }

    fn next(&mut self) -> Result<Option<MarkRecord>, VolumeError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0; MARK_RECORD_BYTES];
        self.reader
            .read_exact(&mut bytes)
            .map_err(|_| unavailable("read Managed collection mark"))?;
        self.remaining -= 1;
        MarkRecord::decode(bytes).map(Some)
    }
}

#[derive(Clone, Copy)]
struct MarkRecord {
    identity: ObjectIdentity,
    length: u64,
}

impl MarkRecord {
    fn decode(bytes: [u8; MARK_RECORD_BYTES]) -> Result<Self, VolumeError> {
        let kind = ObjectKind::from_byte(bytes[0])
            .ok_or_else(|| corrupt("Managed collection mark kind is invalid"))?;
        let mut digest = [0; 32];
        digest.copy_from_slice(&bytes[1..33]);
        let mut length = [0; 8];
        length.copy_from_slice(&bytes[33..]);
        Ok(Self {
            identity: ObjectIdentity { kind, digest },
            length: u64::from_be_bytes(length),
        })
    }

    fn write(self, writer: &mut BufWriter<File>) -> Result<(), VolumeError> {
        let mut bytes = [0; MARK_RECORD_BYTES];
        bytes[0] = self.identity.kind as u8;
        bytes[1..33].copy_from_slice(&self.identity.digest);
        bytes[33..].copy_from_slice(&self.length.to_be_bytes());
        writer
            .write_all(&bytes)
            .map_err(|_| unavailable("write Managed collection mark"))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObjectIdentity {
    kind: ObjectKind,
    digest: [u8; 32],
}

impl ObjectIdentity {
    fn parse(path: &str) -> Option<Self> {
        let suffix = path.strip_prefix(OBJECT_PREFIX)?;
        let (kind, suffix) = suffix.split_once('/')?;
        let kind = ObjectKind::parse(kind)?;
        let (prefix, digest) = suffix.split_once('/')?;
        if prefix.len() != 2 || digest.len() != 64 || prefix != &digest[..2] {
            return None;
        }
        let mut decoded = [0; 32];
        for (byte, pair) in decoded.iter_mut().zip(digest.as_bytes().chunks_exact(2)) {
            *byte = hex_nibble(pair[0])?.checked_shl(4)? | hex_nibble(pair[1])?;
        }
        Some(Self {
            kind,
            digest: decoded,
        })
    }

    fn nibble(self, position: usize) -> usize {
        let byte = self.digest[position / 2];
        usize::from(if position.is_multiple_of(2) {
            byte >> 4
        } else {
            byte & 0x0f
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
enum ObjectKind {
    Commit,
    Meta,
    Raw,
}

impl ObjectKind {
    const ALL: [Self; 3] = [Self::Commit, Self::Meta, Self::Raw];

    const fn parse(value: &str) -> Option<Self> {
        match value.as_bytes() {
            b"commit" => Some(Self::Commit),
            b"meta" => Some(Self::Meta),
            b"raw" => Some(Self::Raw),
            _ => None,
        }
    }

    const fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Commit),
            1 => Some(Self::Meta),
            2 => Some(Self::Raw),
            _ => None,
        }
    }

    const fn segment(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Meta => "meta",
            Self::Raw => "raw",
        }
    }

    fn object_prefix(self, digest_prefix: &str) -> String {
        format!(
            "{OBJECT_PREFIX}{}/{}/{digest_prefix}",
            self.segment(),
            &digest_prefix[..2]
        )
    }
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn conflict(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Conflict, message)
}

fn corrupt(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Corrupt, message)
}

fn unavailable(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Unavailable, message)
}
