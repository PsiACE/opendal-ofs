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

use futures::TryStreamExt as _;

use crate::filesystem::OperationId;
use crate::{Error, ErrorKind};

use super::ManagedVolume;
use super::head::GcFence;

const OBJECT_PREFIX: &str = "managed/1/objects/";
const INITIAL_PARTITIONS: usize = 256;
const MAX_UNIQUE_MARKS_PER_PARTITION: usize = 64 * 1024;
const MARK_RECORD_BYTES: usize = 1 + 32 + 8;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOutcome {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
}

impl ManagedVolume {
    pub async fn collect_unreachable(&self, resume: bool) -> Result<GcOutcome, Error> {
        let capability = self.operator().info().full_capability();
        if !capability.list || !capability.delete {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "collect Managed data",
                "storage lacks a required list or delete capability",
            ));
        }
        let fence = self.begin_gc(resume).await?;
        test_interrupt("after-gc-fence")?;
        let mut live = LiveObjects::create(fence.owner)?;
        self.visit_reachable_objects(fence.namespace_commit, |key, length| {
            live.insert(&key, length)
        })
        .await?;
        live.seal_marks()?;
        self.inventory_candidates(&mut live).await?;
        live.seal_candidates()?;
        let outcome = self.sweep(&mut live).await?;
        self.finish_gc(fence).await?;
        Ok(outcome)
    }

    async fn begin_gc(&self, resume: bool) -> Result<GcFence, Error> {
        let (mut head, revision) = self.read_head().await?;
        let owner = OperationId::generate();
        let fence = match (resume, head.maintenance) {
            (false, None) => {
                let maintenance_generation =
                    head.maintenance_generation.checked_add(1).ok_or_else(|| {
                        Error::corrupt("collect Managed data", "maintenance generation overflows")
                    })?;
                head.maintenance_generation = maintenance_generation;
                GcFence {
                    owner,
                    namespace_commit: head.namespace_commit,
                    maintenance_generation,
                }
            }
            (false, Some(_)) => {
                return Err(Error::conflict(
                    "collect Managed data",
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
                return Err(Error::corrupt(
                    "collect Managed data",
                    "resume Managed data collection: saved collection state is invalid",
                ));
            }
            (true, None) => {
                return Err(Error::conflict(
                    "collect Managed data",
                    "resume Managed data collection: no interrupted collection is active",
                ));
            }
        };
        head.maintenance = Some(fence);
        if !self.replace_head(&revision, &head).await? {
            return Err(Error::conflict(
                "collect Managed data",
                "begin Managed data collection: namespace authority changed",
            ));
        }
        Ok(fence)
    }

    async fn inventory_candidates(&self, live: &mut LiveObjects) -> Result<(), Error> {
        let mut lister = self
            .operator()
            .lister_with(OBJECT_PREFIX)
            .recursive(true)
            .await
            .map_err(|error| Error::from_storage("list Managed data objects", error))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|error| Error::from_storage("list Managed data objects", error))?
        {
            if !entry.metadata().is_file() {
                continue;
            }
            let Some(identity) = ObjectIdentity::parse(entry.path()) else {
                continue;
            };
            live.insert_candidate(identity, entry.metadata().content_length())?;
        }
        Ok(())
    }

    async fn sweep(&self, live: &mut LiveObjects) -> Result<GcOutcome, Error> {
        let mut outcome = GcOutcome::default();
        let mut deleter = self
            .operator()
            .deleter()
            .await
            .map_err(|error| Error::from_storage("open Managed data deleter", error))?;

        let mut pending = live.initial_partitions();
        while let Some(partition) = pending.pop() {
            let Some(marks) = live.load_partition(&partition)? else {
                pending.extend(live.split_partition(partition)?);
                continue;
            };
            let Some(path) = &partition.candidates else {
                continue;
            };
            let mut candidates = MarkReader::open(path)?;
            while let Some(candidate) = candidates.next()? {
                outcome.scanned += 1;
                match marks.get(&candidate.identity) {
                    Some(expected) if *expected == candidate.length => continue,
                    Some(_) => {
                        return Err(Error::corrupt(
                            "collect Managed data",
                            "live Managed object length is invalid",
                        ));
                    }
                    None => {}
                }
                deleter
                    .delete(candidate.identity.object_key())
                    .await
                    .map_err(|error| Error::from_storage("delete Managed data object", error))?;
                outcome.deleted += 1;
                outcome.deleted_bytes = outcome
                    .deleted_bytes
                    .checked_add(candidate.length)
                    .ok_or_else(|| {
                        Error::corrupt(
                            "collect Managed data",
                            "deleted Managed data byte count overflows",
                        )
                    })?;
            }
        }
        deleter
            .close()
            .await
            .map_err(|error| Error::from_storage("finish Managed data deletion", error))?;
        Ok(outcome)
    }

    async fn finish_gc(&self, fence: GcFence) -> Result<(), Error> {
        let (mut head, revision) = self.read_head().await?;
        if head.maintenance != Some(fence) || head.namespace_commit != fence.namespace_commit {
            return Err(Error::conflict(
                "collect Managed data",
                "finish Managed data collection: collection ownership changed",
            ));
        }
        head.reclamation_watermark = fence.namespace_commit.cursor();
        head.maintenance = None;
        if self.replace_head(&revision, &head).await? {
            Ok(())
        } else {
            Err(Error::conflict(
                "collect Managed data",
                "finish Managed data collection: namespace authority changed",
            ))
        }
    }
}

struct LiveObjects {
    directory: PathBuf,
    mark_writers: Vec<Option<BufWriter<File>>>,
    candidate_writers: Vec<Option<BufWriter<File>>>,
}

impl LiveObjects {
    fn create(operation: OperationId) -> Result<Self, Error> {
        let directory = std::env::temp_dir().join(format!("ofs-managed-gc-{operation}"));
        fs::create_dir(&directory).map_err(|error| {
            Error::from_io(
                "create Managed collection mark store",
                Some(&directory),
                error,
            )
        })?;
        Ok(Self {
            directory,
            mark_writers: std::iter::repeat_with(|| None)
                .take(INITIAL_PARTITIONS)
                .collect(),
            candidate_writers: std::iter::repeat_with(|| None)
                .take(INITIAL_PARTITIONS)
                .collect(),
        })
    }

    fn insert(&mut self, key: &str, length: u64) -> Result<(), Error> {
        let identity = ObjectIdentity::parse(key).ok_or_else(|| {
            Error::corrupt(
                "collect Managed data",
                "reachable Managed object key is invalid",
            )
        })?;
        write_partition_record(
            &self.directory,
            "marks",
            &mut self.mark_writers,
            MarkRecord { identity, length },
        )
    }

    fn insert_candidate(&mut self, identity: ObjectIdentity, length: u64) -> Result<(), Error> {
        write_partition_record(
            &self.directory,
            "candidates",
            &mut self.candidate_writers,
            MarkRecord { identity, length },
        )
    }

    fn seal_marks(&mut self) -> Result<(), Error> {
        seal_partition_writers(&mut self.mark_writers)
    }

    fn seal_candidates(&mut self) -> Result<(), Error> {
        seal_partition_writers(&mut self.candidate_writers)
    }

    fn initial_partitions(&self) -> Vec<MarkPartition> {
        (0..INITIAL_PARTITIONS)
            .map(|partition| {
                let digest_prefix = format!("{partition:02x}");
                let marks = self.partition_path("marks", &digest_prefix);
                let candidates = self.partition_path("candidates", &digest_prefix);
                MarkPartition {
                    marks: marks.exists().then_some(marks),
                    candidates: candidates.exists().then_some(candidates),
                    digest_prefix,
                }
            })
            .collect()
    }

    fn load_partition(
        &self,
        partition: &MarkPartition,
    ) -> Result<Option<BTreeMap<ObjectIdentity, u64>>, Error> {
        let Some(path) = &partition.marks else {
            return Ok(Some(BTreeMap::new()));
        };
        let mut reader = MarkReader::open(path)?;
        let mut marks = BTreeMap::new();
        while let Some(record) = reader.next()? {
            if marks
                .insert(record.identity, record.length)
                .is_some_and(|length| length != record.length)
            {
                return Err(Error::corrupt(
                    "collect Managed data",
                    "one Managed object has conflicting lengths",
                ));
            }
            if marks.len() > MAX_UNIQUE_MARKS_PER_PARTITION && partition.digest_prefix.len() < 64 {
                return Ok(None);
            }
        }
        Ok(Some(marks))
    }

    fn split_partition(&self, partition: MarkPartition) -> Result<Vec<MarkPartition>, Error> {
        let mark_path = partition
            .marks
            .as_ref()
            .expect("only a non-empty mark partition is split");
        let marks = self.split_records(mark_path, &partition.digest_prefix, "marks")?;
        let candidates = match &partition.candidates {
            Some(path) => self.split_records(path, &partition.digest_prefix, "candidates")?,
            None => std::iter::repeat_with(|| None).take(16).collect(),
        };

        Ok((0..16)
            .map(|child| MarkPartition {
                marks: marks[child].clone(),
                candidates: candidates[child].clone(),
                digest_prefix: format!("{}{child:x}", partition.digest_prefix),
            })
            .collect())
    }

    fn split_records(
        &self,
        path: &Path,
        digest_prefix: &str,
        stem: &str,
    ) -> Result<Vec<Option<PathBuf>>, Error> {
        let mut reader = MarkReader::open(path)?;
        let mut writers: Vec<Option<BufWriter<File>>> =
            std::iter::repeat_with(|| None).take(16).collect();
        while let Some(record) = reader.next()? {
            let child = record.identity.nibble(digest_prefix.len());
            if writers[child].is_none() {
                let child_prefix = format!("{digest_prefix}{child:x}");
                let path = self.partition_path(stem, &child_prefix);
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(|error| {
                        Error::from_io(
                            "create Managed collection mark partition",
                            Some(&path),
                            error,
                        )
                    })?;
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
                .map_err(|error| Error::io("flush Managed collection marks", error))?;
        }
        drop(writers);
        drop(reader);
        fs::remove_file(path).map_err(|error| {
            Error::from_io(
                "remove split Managed collection mark partition",
                Some(path),
                error,
            )
        })?;

        Ok((0..16)
            .map(|child| {
                let child_prefix = format!("{digest_prefix}{child:x}");
                let path = self.partition_path(stem, &child_prefix);
                path.exists().then_some(path)
            })
            .collect())
    }

    fn partition_path(&self, stem: &str, digest_prefix: &str) -> PathBuf {
        self.directory.join(format!("{stem}-{digest_prefix}"))
    }
}

impl Drop for LiveObjects {
    fn drop(&mut self) {
        self.mark_writers.clear();
        self.candidate_writers.clear();
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn write_partition_record(
    directory: &Path,
    stem: &str,
    writers: &mut [Option<BufWriter<File>>],
    record: MarkRecord,
) -> Result<(), Error> {
    let partition = usize::from(record.identity.digest[0]);
    if writers[partition].is_none() {
        let path = directory.join(format!("{stem}-{partition:02x}"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                Error::from_io(
                    "create Managed collection mark partition",
                    Some(&path),
                    error,
                )
            })?;
        writers[partition] = Some(BufWriter::new(file));
    }
    record.write(
        writers[partition]
            .as_mut()
            .expect("the collection partition writer is open"),
    )
}

fn seal_partition_writers(writers: &mut Vec<Option<BufWriter<File>>>) -> Result<(), Error> {
    for writer in writers.iter_mut().flatten() {
        writer
            .flush()
            .map_err(|error| Error::io("flush Managed collection marks", error))?;
    }
    writers.clear();
    Ok(())
}

struct MarkPartition {
    marks: Option<PathBuf>,
    candidates: Option<PathBuf>,
    digest_prefix: String,
}

struct MarkReader {
    reader: BufReader<File>,
    remaining: u64,
}

impl MarkReader {
    fn open(path: &Path) -> Result<Self, Error> {
        let file = File::open(path).map_err(|error| {
            Error::from_io("open Managed collection mark partition", Some(path), error)
        })?;
        let length = file
            .metadata()
            .map_err(|error| {
                Error::from_io(
                    "inspect Managed collection mark partition",
                    Some(path),
                    error,
                )
            })?
            .len();
        let record_bytes = MARK_RECORD_BYTES as u64;
        if length % record_bytes != 0 {
            return Err(Error::corrupt(
                "collect Managed data",
                "Managed collection mark partition is invalid",
            ));
        }
        Ok(Self {
            reader: BufReader::new(file),
            remaining: length / record_bytes,
        })
    }

    fn next(&mut self) -> Result<Option<MarkRecord>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0; MARK_RECORD_BYTES];
        self.reader
            .read_exact(&mut bytes)
            .map_err(|error| Error::io("read Managed collection mark", error))?;
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
    fn decode(bytes: [u8; MARK_RECORD_BYTES]) -> Result<Self, Error> {
        let kind = ObjectKind::from_byte(bytes[0]).ok_or_else(|| {
            Error::corrupt(
                "collect Managed data",
                "Managed collection mark kind is invalid",
            )
        })?;
        let mut digest = [0; 32];
        digest.copy_from_slice(&bytes[1..33]);
        let mut length = [0; 8];
        length.copy_from_slice(&bytes[33..]);
        Ok(Self {
            identity: ObjectIdentity { kind, digest },
            length: u64::from_be_bytes(length),
        })
    }

    fn write(self, writer: &mut BufWriter<File>) -> Result<(), Error> {
        let mut bytes = [0; MARK_RECORD_BYTES];
        bytes[0] = self.identity.kind as u8;
        bytes[1..33].copy_from_slice(&self.identity.digest);
        bytes[33..].copy_from_slice(&self.length.to_be_bytes());
        writer
            .write_all(&bytes)
            .map_err(|error| Error::io("write Managed collection mark", error))
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

    fn object_key(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let mut digest = String::with_capacity(64);
        for byte in self.digest {
            digest.push(char::from(HEX[usize::from(byte >> 4)]));
            digest.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        format!(
            "{OBJECT_PREFIX}{}/{}/{}",
            self.kind.segment(),
            &digest[..2],
            digest
        )
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
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(debug_assertions)]
fn test_interrupt(point: &str) -> Result<(), Error> {
    if std::env::var("OFS_INTERNAL_TEST_INTERRUPT").as_deref() == Ok(point) {
        return Err(Error::unavailable(
            "collect Managed data",
            "internal test interrupted Managed data collection",
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
const fn test_interrupt(_point: &str) -> Result<(), Error> {
    Ok(())
}
