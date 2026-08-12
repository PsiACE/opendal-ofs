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

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read as _, Write as _};
use std::path::{Path, PathBuf};

use futures::TryStreamExt as _;

use crate::filesystem::OperationId;
use crate::{Error, ErrorKind};

use super::ManagedVolume;
use super::object::{GcEpoch, ObjectClass, ObjectId, ObjectRef};

const OBJECT_PREFIX: &str = "managed/1/objects/";
const PARTITIONS: usize = 256;
const RECORD_BYTES: usize = 8 + 1 + 16 + 8;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOutcome {
    pub scanned: u64,
    pub deleted: u64,
    pub deleted_bytes: u64,
}

impl ManagedVolume {
    /// Rotate the upload epoch, then reclaim unreachable objects from older epochs.
    pub async fn collect_unreachable(&self) -> Result<GcOutcome, Error> {
        let (mut head, revision) = self.read_head().await?;
        let previous_commit = head.current_commit;
        let collection_epoch = head.gc_epoch.next()?;
        head.gc_epoch = collection_epoch;
        if !self.replace_head(&revision, &head).await? {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed objects",
                "namespace authority changed while rotating the GC epoch",
            ));
        }
        test_interrupt("after-gc-epoch-rotation")?;

        let (mut rotated, revision) = self.read_head().await?;
        if rotated.gc_epoch != collection_epoch || rotated.current_commit != previous_commit {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed objects",
                "namespace authority changed before metadata compaction",
            ));
        }
        let collection_commit = self
            .compact_for_collection(previous_commit, collection_epoch)
            .await?;
        rotated.current_commit = collection_commit;
        if !self.replace_head(&revision, &rotated).await? {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed objects",
                "namespace authority changed while publishing compacted metadata",
            ));
        }

        let mut inventory = PartitionedInventory::create(OperationId::generate())?;
        self.visit_reachable_objects(collection_commit, |reference| inventory.mark(reference))
            .await?;
        inventory.seal_marks()?;
        self.inventory_old_epochs(collection_epoch, &mut inventory)
            .await?;
        inventory.seal_candidates()?;
        let outcome = self.sweep_partitions(&inventory).await?;
        self.advance_reclamation_watermark(collection_commit.cursor())
            .await?;
        Ok(outcome)
    }

    async fn inventory_old_epochs(
        &self,
        current_epoch: GcEpoch,
        inventory: &mut PartitionedInventory,
    ) -> Result<(), Error> {
        for epoch in self.old_gc_epochs(current_epoch).await? {
            for class in ObjectClass::ALL {
                let class_prefix =
                    format!("{OBJECT_PREFIX}{}/{}/", epoch.value(), class.key_segment());
                for prefix in self.object_id_prefixes(&class_prefix).await? {
                    self.inventory_partition(epoch, class, prefix, inventory)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn old_gc_epochs(&self, current: GcEpoch) -> Result<Vec<GcEpoch>, Error> {
        let mut epochs = BTreeSet::new();
        let mut lister = self
            .operator()
            .lister(OBJECT_PREFIX)
            .await
            .map_err(|error| Error::from_storage("list Managed GC epochs", error))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|error| Error::from_storage("list Managed GC epochs", error))?
        {
            let Some(segment) = child_segment(OBJECT_PREFIX, entry.path()) else {
                continue;
            };
            let Ok(value) = segment.parse::<u64>() else {
                continue;
            };
            if value < current.value() {
                epochs.insert(GcEpoch::from_value(value));
            }
        }
        Ok(epochs.into_iter().collect())
    }

    async fn object_id_prefixes(&self, class_prefix: &str) -> Result<Vec<u8>, Error> {
        let mut prefixes = BTreeSet::new();
        let mut lister = self
            .operator()
            .lister(class_prefix)
            .await
            .map_err(|error| Error::from_storage("list Managed object prefixes", error))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|error| Error::from_storage("list Managed object prefixes", error))?
        {
            let Some(segment) = child_segment(class_prefix, entry.path()) else {
                continue;
            };
            if segment.len() == 2
                && let Ok(prefix) = u8::from_str_radix(segment, 16)
            {
                prefixes.insert(prefix);
            }
        }
        Ok(prefixes.into_iter().collect())
    }

    async fn inventory_partition(
        &self,
        epoch: GcEpoch,
        class: ObjectClass,
        prefix: u8,
        inventory: &mut PartitionedInventory,
    ) -> Result<(), Error> {
        let path = format!(
            "{OBJECT_PREFIX}{}/{}/{prefix:02x}/",
            epoch.value(),
            class.key_segment()
        );
        let mut lister = self
            .operator()
            .lister_with(&path)
            .recursive(true)
            .await
            .map_err(|error| Error::from_storage("list Managed object partition", error))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|error| Error::from_storage("list Managed object partition", error))?
        {
            if !entry.metadata().is_file() {
                continue;
            }
            let identity = ObjectIdentity::parse(entry.path()).ok_or_else(|| {
                Error::corrupt("collect Managed objects", "object key is invalid")
            })?;
            if identity.gc_epoch != epoch
                || identity.class != class
                || identity.id.as_bytes()[0] != prefix
            {
                return Err(Error::corrupt(
                    "collect Managed objects",
                    "listed object is outside its GC partition",
                ));
            }
            inventory.candidate(identity, entry.metadata().content_length())?;
        }
        Ok(())
    }

    async fn sweep_partitions(&self, inventory: &PartitionedInventory) -> Result<GcOutcome, Error> {
        let mut outcome = GcOutcome::default();
        let mut deleter = self
            .operator()
            .deleter()
            .await
            .map_err(|error| Error::from_storage("open Managed object deleter", error))?;
        for partition in 0..PARTITIONS {
            let marks = inventory.load_marks(partition)?;
            let path = inventory.candidate_path(partition);
            if !path.exists() {
                continue;
            }
            let mut candidates = RecordReader::open(&path)?;
            while let Some(candidate) = candidates.next()? {
                outcome.scanned = outcome.scanned.checked_add(1).ok_or_else(|| {
                    Error::corrupt("collect Managed objects", "scanned object count overflows")
                })?;
                match marks.get(&candidate.identity) {
                    Some(length) if *length == candidate.length => continue,
                    Some(_) => {
                        return Err(Error::corrupt(
                            "collect Managed objects",
                            "reachable object length changed",
                        ));
                    }
                    None => {}
                }
                deleter
                    .delete(candidate.identity.key())
                    .await
                    .map_err(|error| Error::from_storage("delete Managed object", error))?;
                outcome.deleted = outcome.deleted.checked_add(1).ok_or_else(|| {
                    Error::corrupt("collect Managed objects", "deleted object count overflows")
                })?;
                outcome.deleted_bytes = outcome
                    .deleted_bytes
                    .checked_add(candidate.length)
                    .ok_or_else(|| {
                        Error::corrupt("collect Managed objects", "deleted byte count overflows")
                    })?;
            }
        }
        deleter
            .close()
            .await
            .map_err(|error| Error::from_storage("finish Managed object deletion", error))?;
        Ok(outcome)
    }

    async fn advance_reclamation_watermark(
        &self,
        completed: crate::filesystem::ChangeCursor,
    ) -> Result<(), Error> {
        for _ in 0..8 {
            let (mut head, revision) = self.read_head().await?;
            if head.minimum_retained_cursor.sequence() >= completed.sequence() {
                return Ok(());
            }
            head.minimum_retained_cursor = completed;
            if self.replace_head(&revision, &head).await? {
                return Ok(());
            }
        }
        Err(Error::new(
            ErrorKind::Conflict,
            "collect Managed objects",
            "namespace kept changing while publishing the reclamation watermark",
        ))
    }
}

fn child_segment<'a>(parent: &str, path: &'a str) -> Option<&'a str> {
    let relative = path.strip_prefix(parent)?;
    let segment = relative.split('/').next()?;
    (!segment.is_empty()).then_some(segment)
}

struct PartitionedInventory {
    directory: PathBuf,
    marks: Vec<Option<BufWriter<File>>>,
    candidates: Vec<Option<BufWriter<File>>>,
}

impl PartitionedInventory {
    fn create(operation: OperationId) -> Result<Self, Error> {
        let directory = std::env::temp_dir().join(format!("ofs-managed-gc-{operation}"));
        fs::create_dir(&directory).map_err(|error| {
            Error::from_io("create Managed GC workspace", Some(&directory), error)
        })?;
        Ok(Self {
            directory,
            marks: std::iter::repeat_with(|| None).take(PARTITIONS).collect(),
            candidates: std::iter::repeat_with(|| None).take(PARTITIONS).collect(),
        })
    }

    fn mark(&mut self, reference: ObjectRef) -> Result<(), Error> {
        self.write(
            true,
            ObjectRecord {
                identity: ObjectIdentity::from_ref(reference),
                length: reference.encoded_length,
            },
        )
    }

    fn candidate(&mut self, identity: ObjectIdentity, length: u64) -> Result<(), Error> {
        self.write(false, ObjectRecord { identity, length })
    }

    fn write(&mut self, mark: bool, record: ObjectRecord) -> Result<(), Error> {
        let partition = usize::from(record.identity.id.as_bytes()[0]);
        let stem = if mark { "marks" } else { "candidates" };
        let directory = self.directory.clone();
        let writers = if mark {
            &mut self.marks
        } else {
            &mut self.candidates
        };
        if writers[partition].is_none() {
            let path = directory.join(format!("{stem}-{partition:02x}"));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|error| {
                    Error::from_io("create Managed GC partition", Some(&path), error)
                })?;
            writers[partition] = Some(BufWriter::new(file));
        }
        record.write(
            writers[partition]
                .as_mut()
                .expect("partition writer is open"),
        )
    }

    fn seal_marks(&mut self) -> Result<(), Error> {
        seal(&mut self.marks)
    }

    fn seal_candidates(&mut self) -> Result<(), Error> {
        seal(&mut self.candidates)
    }

    fn load_marks(&self, partition: usize) -> Result<BTreeMap<ObjectIdentity, u64>, Error> {
        let path = self.directory.join(format!("marks-{partition:02x}"));
        if !path.exists() {
            return Ok(BTreeMap::new());
        }
        let mut reader = RecordReader::open(&path)?;
        let mut marks = BTreeMap::new();
        while let Some(record) = reader.next()? {
            if marks
                .insert(record.identity, record.length)
                .is_some_and(|length| length != record.length)
            {
                return Err(Error::corrupt(
                    "collect Managed objects",
                    "one reachable object has conflicting lengths",
                ));
            }
        }
        Ok(marks)
    }

    fn candidate_path(&self, partition: usize) -> PathBuf {
        self.directory.join(format!("candidates-{partition:02x}"))
    }
}

impl Drop for PartitionedInventory {
    fn drop(&mut self) {
        self.marks.clear();
        self.candidates.clear();
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn seal(writers: &mut Vec<Option<BufWriter<File>>>) -> Result<(), Error> {
    for writer in writers.iter_mut().flatten() {
        writer
            .flush()
            .map_err(|error| Error::io("flush Managed GC partition", error))?;
    }
    writers.clear();
    Ok(())
}

#[derive(Clone, Copy)]
struct ObjectRecord {
    identity: ObjectIdentity,
    length: u64,
}

impl ObjectRecord {
    fn write(self, writer: &mut BufWriter<File>) -> Result<(), Error> {
        let mut bytes = [0; RECORD_BYTES];
        bytes[..8].copy_from_slice(&self.identity.gc_epoch.value().to_be_bytes());
        bytes[8] = self.identity.class.code();
        bytes[9..25].copy_from_slice(self.identity.id.as_bytes());
        bytes[25..].copy_from_slice(&self.length.to_be_bytes());
        writer
            .write_all(&bytes)
            .map_err(|error| Error::io("write Managed GC record", error))
    }

    fn decode(bytes: [u8; RECORD_BYTES]) -> Result<Self, Error> {
        let epoch = u64::from_be_bytes(bytes[..8].try_into().expect("fixed epoch"));
        let class = ObjectClass::from_code(bytes[8])
            .ok_or_else(|| Error::corrupt("read Managed GC record", "object class is invalid"))?;
        let id = ObjectId::from_bytes(bytes[9..25].try_into().expect("fixed object id"));
        let length = u64::from_be_bytes(bytes[25..].try_into().expect("fixed length"));
        Ok(Self {
            identity: ObjectIdentity {
                gc_epoch: GcEpoch::from_value(epoch),
                class,
                id,
            },
            length,
        })
    }
}

struct RecordReader {
    reader: BufReader<File>,
    remaining: u64,
}

impl RecordReader {
    fn open(path: &Path) -> Result<Self, Error> {
        let file = File::open(path)
            .map_err(|error| Error::from_io("open Managed GC partition", Some(path), error))?;
        let length = file
            .metadata()
            .map_err(|error| Error::from_io("inspect Managed GC partition", Some(path), error))?
            .len();
        if length % RECORD_BYTES as u64 != 0 {
            return Err(Error::corrupt(
                "read Managed GC partition",
                "partition length is invalid",
            ));
        }
        Ok(Self {
            reader: BufReader::new(file),
            remaining: length / RECORD_BYTES as u64,
        })
    }

    fn next(&mut self) -> Result<Option<ObjectRecord>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0; RECORD_BYTES];
        self.reader
            .read_exact(&mut bytes)
            .map_err(|error| Error::io("read Managed GC record", error))?;
        self.remaining -= 1;
        ObjectRecord::decode(bytes).map(Some)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObjectIdentity {
    gc_epoch: GcEpoch,
    class: ObjectClass,
    id: ObjectId,
}

impl ObjectIdentity {
    const fn from_ref(reference: ObjectRef) -> Self {
        Self {
            gc_epoch: reference.gc_epoch,
            class: reference.class,
            id: reference.id,
        }
    }

    fn parse(path: &str) -> Option<Self> {
        let suffix = path.strip_prefix(OBJECT_PREFIX)?;
        let mut parts = suffix.split('/');
        let gc_epoch = GcEpoch::from_value(parts.next()?.parse().ok()?);
        let class = ObjectClass::parse(parts.next()?)?;
        let prefix = parts.next()?;
        let encoded_id = parts.next()?;
        if parts.next().is_some()
            || prefix.len() != 2
            || encoded_id.len() != 32
            || prefix != &encoded_id[..2]
        {
            return None;
        }
        let mut id = [0; 16];
        for (byte, pair) in id.iter_mut().zip(encoded_id.as_bytes().chunks_exact(2)) {
            *byte = hex(pair[0])?.checked_shl(4)? | hex(pair[1])?;
        }
        Some(Self {
            gc_epoch,
            class,
            id: ObjectId::from_bytes(id),
        })
    }

    fn key(self) -> String {
        format!(
            "{OBJECT_PREFIX}{}/{}/{:02x}/{}",
            self.gc_epoch.value(),
            self.class.key_segment(),
            self.id.as_bytes()[0],
            self.id
        )
    }
}

const fn hex(byte: u8) -> Option<u8> {
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
            "collect Managed objects",
            "internal test interrupted Managed object collection",
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
const fn test_interrupt(_point: &str) -> Result<(), Error> {
    Ok(())
}
