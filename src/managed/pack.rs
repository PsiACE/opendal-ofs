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

//! Immutable content packs and their rebuildable physical index.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;

use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, OnceCell};

use super::{ManagedError, ManagedErrorKind};
use crate::filesystem::OperationId;
use crate::managed::namespace::ContentRef;
use crate::managed::section::{self, Record as SectionRecord, Reference as SectionReference};

const PACK_ROOT: &str = "data/v1/packs";
const INDEX_ROOT: &str = "data/v1/pack-index";
const INDEX_SECTION_ROOT: &str = "data/v1/pack-index/sections/sha256";
const HEAD_KEY: &str = "data/v1/pack-index/head.cbor";
const PACK_MAGIC: &[u8; 8] = b"OFSPACK1";
const TRAILER_MAGIC: &[u8; 8] = b"OFSPTRL1";
const FOOTER_MAGIC: &str = "ofs-pack-footer";
const CHECKPOINT_MAGIC: &str = "ofs-pack-index-checkpoint";
const REVISION_MAGIC: &str = "ofs-pack-index-revision";
const HEAD_MAGIC: &str = "ofs-pack-index-head";
const FORMAT_MAJOR: u16 = 1;
const INDEX_SECTION: u8 = 32;
const HEADER_LENGTH: u64 = 26;
const TRAILER_LENGTH: u64 = 56;

/// Identity of an immutable pack. It is the SHA-256 checksum in its trailer.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PackId([u8; 32]);

impl PackId {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Physical range containing one content object.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PackLocation {
    pub pack: PackId,
    pub offset: u64,
    pub stored_length: u64,
    pub logical_length: u64,
}

/// Result of sealing one immutable pack.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedPack {
    pub id: PackId,
    pub locations: BTreeMap<ContentRef, PackLocation>,
}

/// One completely downloaded pack whose envelope and entries have been verified.
#[derive(Debug)]
pub(crate) struct VerifiedPack {
    pack: SealedPack,
    bytes: Vec<u8>,
}

impl VerifiedPack {
    pub(crate) fn content(&self, content: ContentRef) -> Option<&[u8]> {
        let location = self.pack.locations.get(&content)?;
        self.bytes
            .get(location.offset as usize..(location.offset + location.stored_length) as usize)
    }
}

type CachedPack = Arc<OnceCell<Result<Arc<SealedPack>, ManagedError>>>;

/// Pack locations fixed for one materialization operation.
#[derive(Clone, Debug)]
pub(crate) struct PackReadSession {
    operator: Operator,
    store: PackStore,
    index: Arc<OnceCell<Result<Option<PackIndex>, ManagedError>>>,
    packs: Arc<Mutex<BTreeMap<PackId, CachedPack>>>,
}

impl PackReadSession {
    pub(crate) fn new(operator: Operator) -> Result<Self, ManagedError> {
        require_index_read_capabilities(&operator)?;
        Ok(Self {
            operator: operator.clone(),
            store: PackStore { operator },
            index: Arc::new(OnceCell::new()),
            packs: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Return locations from the pack index fixed for this operation.
    pub(crate) async fn locations(
        &self,
        content: ContentRef,
    ) -> Result<Vec<PackLocation>, ManagedError> {
        let operator = self.operator.clone();
        let index = self
            .index
            .get_or_init(|| async move { PackIndex::open(operator).await })
            .await;
        match index {
            Ok(Some(index)) => Ok(index.locations(content).to_vec()),
            Ok(None) => Ok(Vec::new()),
            Err(error) => Err(error.clone()),
        }
    }

    /// Download a complete pack without retaining it in this session.
    pub(crate) async fn read_full(&self, id: PackId) -> Result<VerifiedPack, ManagedError> {
        self.store.read_complete(id).await
    }

    /// Read one packed location. `None` means the fixed index has no location.
    pub(crate) async fn read(&self, content: ContentRef) -> Result<Option<Vec<u8>>, ManagedError> {
        let locations = self.locations(content).await?;
        if locations.is_empty() {
            return Ok(None);
        }

        let mut failure = None;
        for location in locations {
            let cell = {
                let mut packs = self.packs.lock().await;
                packs
                    .entry(location.pack)
                    .or_insert_with(|| Arc::new(OnceCell::new()))
                    .clone()
            };
            let store = self.store.clone();
            let pack = cell
                .get_or_init(|| async move {
                    store
                        .inspect_inner(location.pack, false)
                        .await
                        .map(|(pack, _)| pack)
                        .map(Arc::new)
                })
                .await;
            let pack = match pack {
                Ok(pack) => pack,
                Err(error) => {
                    failure = Some(error.clone());
                    continue;
                }
            };
            match self.store.read_verified(content, location, pack).await {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(error) => failure = Some(error),
            }
        }
        Err(failure
            .unwrap_or_else(|| corrupt("read Managed data", "pack locations cannot be resolved")))
    }
}

/// Concrete pack storage backed by one OpenDAL operator.
#[derive(Clone, Debug)]
pub struct PackStore {
    operator: Operator,
}

impl PackStore {
    pub fn new(operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.read
            || !capability.write
            || !capability.write_with_if_not_exists
            || !capability.stat
            || !capability.list
        {
            return Err(unavailable(
                "open pack store",
                "pack storage requires read, write, stat, list, and create-only write",
            ));
        }
        Ok(Self { operator })
    }

    /// Seal distinct, non-empty content objects into a format-v1 pack.
    pub async fn seal(
        &self,
        operation: OperationId,
        contents: impl IntoIterator<Item = Vec<u8>>,
    ) -> Result<SealedPack, ManagedError> {
        let mut unique = BTreeMap::new();
        for bytes in contents {
            if bytes.is_empty() {
                continue;
            }
            unique.entry(content_ref(&bytes)).or_insert(bytes);
        }
        if unique.is_empty() {
            return Err(invalid(
                "seal pack",
                "a pack must contain non-empty content",
            ));
        }

        let mut encoded = Vec::new();
        encoded.extend_from_slice(PACK_MAGIC);
        encoded.extend_from_slice(&FORMAT_MAJOR.to_be_bytes());
        encoded.extend_from_slice(operation.as_bytes());

        let mut footer_entries = Vec::with_capacity(unique.len());
        for (content, bytes) in unique {
            let offset = encoded.len() as u64;
            encoded.extend_from_slice(&bytes);
            footer_entries.push(FooterEntry {
                content,
                offset,
                stored_length: bytes.len() as u64,
                codec: Codec::Raw,
            });
        }
        let footer_offset = encoded.len() as u64;
        let footer = encode(
            &Footer {
                magic: FOOTER_MAGIC.to_owned(),
                major: FORMAT_MAJOR,
                entries: footer_entries,
            },
            "seal pack",
        )?;
        encoded.extend_from_slice(&footer);
        encoded.extend_from_slice(TRAILER_MAGIC);
        encoded.extend_from_slice(&footer_offset.to_be_bytes());
        encoded.extend_from_slice(&(footer.len() as u64).to_be_bytes());
        let checksum: [u8; 32] = Sha256::digest(&encoded).into();
        encoded.extend_from_slice(&checksum);

        let id = PackId(checksum);
        let key = pack_key(id);
        create_immutable(&self.operator, &key, &encoded, "seal pack").await?;
        self.inspect(id).await
    }

    /// Verify the complete pack and return its footer-derived locations.
    pub async fn inspect(&self, id: PackId) -> Result<SealedPack, ManagedError> {
        self.read_complete(id).await.map(|verified| verified.pack)
    }

    /// Download and verify a complete pack for reading several entries.
    pub(crate) async fn read_complete(&self, id: PackId) -> Result<VerifiedPack, ManagedError> {
        let (pack, bytes) = self.inspect_inner(id, true).await?;
        Ok(VerifiedPack {
            pack,
            bytes: bytes.expect("complete inspection retains the downloaded pack"),
        })
    }

    async fn inspect_inner(
        &self,
        id: PackId,
        verify_checksum: bool,
    ) -> Result<(SealedPack, Option<Vec<u8>>), ManagedError> {
        let key = pack_key(id);
        let complete = if verify_checksum {
            Some(
                self.operator
                    .read(&key)
                    .await
                    .map_err(|_| unavailable("inspect pack", "pack is unavailable"))?
                    .to_bytes()
                    .to_vec(),
            )
        } else {
            None
        };
        let length = match &complete {
            Some(bytes) => bytes.len() as u64,
            None => self
                .operator
                .stat(&key)
                .await
                .map_err(|_| unavailable("inspect pack", "pack metadata is unavailable"))?
                .content_length(),
        };
        if length < HEADER_LENGTH + TRAILER_LENGTH {
            return Err(corrupt("inspect pack", "pack is shorter than its envelope"));
        }

        let trailer = match &complete {
            Some(bytes) => bytes[(length - TRAILER_LENGTH) as usize..].to_vec(),
            None => self
                .operator
                .read_with(&key)
                .range(length - TRAILER_LENGTH..length)
                .await
                .map_err(|_| unavailable("inspect pack", "pack trailer is unavailable"))?
                .to_bytes()
                .to_vec(),
        };
        if trailer.len() != TRAILER_LENGTH as usize || &trailer[..8] != TRAILER_MAGIC {
            return Err(corrupt("inspect pack", "pack trailer is invalid"));
        }
        let footer_offset = u64_at(&trailer, 8);
        let footer_length = u64_at(&trailer, 16);
        let expected_checksum: [u8; 32] = trailer[24..56]
            .try_into()
            .expect("trailer checksum has fixed length");
        if expected_checksum != id.0 {
            return Err(corrupt(
                "inspect pack",
                "pack trailer does not match its identity",
            ));
        }
        let trailer_offset = length - TRAILER_LENGTH;
        if footer_offset < HEADER_LENGTH
            || footer_offset.checked_add(footer_length) != Some(trailer_offset)
        {
            return Err(corrupt("inspect pack", "pack footer range is invalid"));
        }

        if let Some(complete) = &complete {
            let body = &complete[..complete.len() - 32];
            let actual_checksum: [u8; 32] = Sha256::digest(body).into();
            if actual_checksum != expected_checksum {
                return Err(corrupt(
                    "inspect pack",
                    "pack checksum does not match its identity",
                ));
            }
            if &body[..8] != PACK_MAGIC || u16_at(body, 8) != FORMAT_MAJOR {
                return Err(corrupt("inspect pack", "pack header is invalid"));
            }
        }

        let footer_bytes = match &complete {
            Some(bytes) => bytes[footer_offset as usize..trailer_offset as usize].to_vec(),
            None => self
                .operator
                .read_with(&key)
                .range(footer_offset..trailer_offset)
                .await
                .map_err(|_| unavailable("inspect pack", "pack footer is unavailable"))?
                .to_bytes()
                .to_vec(),
        };
        let footer: Footer = decode(&footer_bytes, "inspect pack")?;
        if footer.magic != FOOTER_MAGIC || footer.major != FORMAT_MAJOR {
            return Err(corrupt("inspect pack", "pack footer version is invalid"));
        }

        let mut locations = BTreeMap::new();
        let mut previous = None;
        let mut previous_end = HEADER_LENGTH;
        for entry in footer.entries {
            if previous.is_some_and(|value| value >= entry.content)
                || entry.codec != Codec::Raw
                || entry.stored_length != entry.content.logical_length
                || entry.offset < previous_end
                || entry.offset.checked_add(entry.stored_length).is_none()
                || entry.offset + entry.stored_length > footer_offset
            {
                return Err(corrupt("inspect pack", "pack footer entry is invalid"));
            }
            if let Some(complete) = &complete {
                let start = entry.offset as usize;
                let end = (entry.offset + entry.stored_length) as usize;
                if content_ref(&complete[start..end]) != entry.content {
                    return Err(corrupt(
                        "inspect pack",
                        "pack entry does not match its content reference",
                    ));
                }
            }
            previous = Some(entry.content);
            previous_end = entry.offset + entry.stored_length;
            locations.insert(
                entry.content,
                PackLocation {
                    pack: id,
                    offset: entry.offset,
                    stored_length: entry.stored_length,
                    logical_length: entry.content.logical_length,
                },
            );
        }
        Ok((SealedPack { id, locations }, complete))
    }

    /// Read and validate one indexed content range.
    pub async fn read(
        &self,
        content: ContentRef,
        location: PackLocation,
    ) -> Result<Vec<u8>, ManagedError> {
        let (pack, _) = self.inspect_inner(location.pack, false).await?;
        self.read_verified(content, location, &pack).await
    }

    pub(crate) async fn read_verified(
        &self,
        content: ContentRef,
        location: PackLocation,
        pack: &SealedPack,
    ) -> Result<Vec<u8>, ManagedError> {
        validate_location(content, location, pack)?;
        let bytes = self
            .operator
            .read_with(&pack_key(pack.id))
            .range(location.offset..location.offset + location.stored_length)
            .await
            .map_err(|_| unavailable("read pack content", "content range is unavailable"))?
            .to_bytes()
            .to_vec();
        if bytes.len() as u64 != location.stored_length || content_ref(&bytes) != content {
            return Err(corrupt(
                "read pack content",
                "content range fails validation",
            ));
        }
        Ok(bytes)
    }

    /// Rebuild and publish the derived pack index from verified pack footers.
    pub async fn rebuild_index(&self) -> Result<PackIndex, ManagedError> {
        let mut locations: BTreeMap<ContentRef, Vec<PackLocation>> = BTreeMap::new();
        let entries = self
            .operator
            .list(&format!("{PACK_ROOT}/"))
            .await
            .map_err(|_| unavailable("rebuild pack index", "pack listing is unavailable"))?;
        for entry in entries {
            let path = entry.path();
            let Some(id) = pack_id_from_key(path) else {
                continue;
            };
            for (content, location) in self.inspect(id).await?.locations {
                locations.entry(content).or_default().push(location);
            }
        }
        normalize_locations(&mut locations);
        let (parent, head_etag) = read_head_state(&self.operator).await?;
        let mut index = PackIndex {
            operator: self.operator.clone(),
            locations,
            sections: BTreeSet::new(),
            revision: parent,
            head_etag,
        };
        index.persist().await?;
        Ok(index)
    }

    pub(crate) async fn delete(&self, id: PackId) -> Result<(), ManagedError> {
        if !self.operator.info().full_capability().delete {
            return Err(unavailable(
                "delete retired pack",
                "pack storage does not support delete",
            ));
        }
        self.operator
            .delete(&pack_key(id))
            .await
            .map_err(|_| unavailable("delete retired pack", "retired pack cannot be deleted"))
    }
}

fn validate_location(
    content: ContentRef,
    location: PackLocation,
    pack: &SealedPack,
) -> Result<(), ManagedError> {
    if location.logical_length != content.logical_length {
        return Err(corrupt(
            "read pack content",
            "index length disagrees with content",
        ));
    }
    if pack.locations.get(&content) != Some(&location) {
        return Err(corrupt(
            "read pack content",
            "index range disagrees with pack footer",
        ));
    }
    Ok(())
}

/// Rebuildable mapping from logical content identities to pack ranges.
#[derive(Clone, Debug)]
pub struct PackIndex {
    operator: Operator,
    locations: BTreeMap<ContentRef, Vec<PackLocation>>,
    sections: BTreeSet<[u8; 32]>,
    revision: Option<[u8; 32]>,
    head_etag: Option<String>,
}

impl PackIndex {
    pub fn locations(&self, content: ContentRef) -> &[PackLocation] {
        self.locations
            .get(&content)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn add(&mut self, pack: &SealedPack) {
        for (content, location) in &pack.locations {
            let locations = self.locations.entry(*content).or_default();
            if !locations.contains(location) {
                locations.push(*location);
                locations.sort_unstable();
            }
        }
    }

    pub(crate) fn pack_ids(&self) -> BTreeSet<PackId> {
        self.locations
            .values()
            .flatten()
            .map(|location| location.pack)
            .collect()
    }

    pub(crate) fn validate_pack(&self, pack: &SealedPack) -> Result<(), ManagedError> {
        let valid = self.locations.iter().all(|(content, locations)| {
            locations.iter().all(|location| {
                location.pack != pack.id || pack.locations.get(content) == Some(location)
            })
        });
        if valid {
            Ok(())
        } else {
            Err(corrupt(
                "repack content",
                "pack index disagrees with a verified pack footer",
            ))
        }
    }

    pub(crate) fn remove_packs(&mut self, retired: &BTreeSet<PackId>) {
        for locations in self.locations.values_mut() {
            locations.retain(|location| !retired.contains(&location.pack));
        }
        normalize_locations(&mut self.locations);
    }

    pub(crate) fn require_update(&self) -> Result<(), ManagedError> {
        if self.revision.is_some()
            && (!self.operator.info().full_capability().write_with_if_match
                || self.head_etag.is_none())
        {
            Err(unavailable(
                "persist pack index",
                "updating an existing pack index requires compare-and-swap and a revision token",
            ))
        } else {
            Ok(())
        }
    }

    /// Open the published index. A missing head means no index exists yet.
    pub async fn open(operator: Operator) -> Result<Option<Self>, ManagedError> {
        require_index_read_capabilities(&operator)?;
        let Some((head_bytes, etag)) = read_head(&operator).await? else {
            return Ok(None);
        };
        let head: IndexHead = decode(&head_bytes, "open pack index")?;
        validate_record(&head.magic, head.major, HEAD_MAGIC, "open pack index")?;
        let revision_bytes =
            read_required(&operator, &revision_key(head.revision), "open pack index").await?;
        if digest(&revision_bytes) != head.revision {
            return Err(corrupt("open pack index", "revision identity is invalid"));
        }
        let revision: IndexRevision = decode(&revision_bytes, "open pack index")?;
        validate_record(
            &revision.magic,
            revision.major,
            REVISION_MAGIC,
            "open pack index",
        )?;
        let checkpoint_bytes = read_required(
            &operator,
            &checkpoint_key(revision.checkpoint),
            "open pack index",
        )
        .await?;
        if digest(&checkpoint_bytes) != revision.checkpoint {
            return Err(corrupt("open pack index", "checkpoint identity is invalid"));
        }
        let checkpoint: IndexCheckpoint = decode(&checkpoint_bytes, "open pack index")?;
        validate_record(
            &checkpoint.magic,
            checkpoint.major,
            CHECKPOINT_MAGIC,
            "open pack index",
        )?;
        let sections = checkpoint
            .sections
            .iter()
            .map(|section| section.id)
            .collect();
        let locations = checkpoint.into_locations(&operator).await?;
        Ok(Some(Self {
            operator,
            locations,
            sections,
            revision: Some(head.revision),
            head_etag: etag,
        }))
    }

    pub(crate) async fn open_or_empty(operator: Operator) -> Result<Self, ManagedError> {
        match Self::open(operator.clone()).await? {
            Some(index) => Ok(index),
            None => Ok(Self {
                operator,
                locations: BTreeMap::new(),
                sections: BTreeSet::new(),
                revision: None,
                head_etag: None,
            }),
        }
    }

    /// Publish current entries through immutable records and a conditional head update.
    pub async fn persist(&mut self) -> Result<(), ManagedError> {
        require_index_create_capabilities(&self.operator)?;
        self.require_update()?;
        normalize_locations(&mut self.locations);
        let checkpoint =
            IndexCheckpoint::from_locations(&self.operator, &self.locations, &self.sections)
                .await?;
        let checkpoint_bytes = encode(&checkpoint, "persist pack index")?;
        let checkpoint_id = digest(&checkpoint_bytes);
        create_immutable(
            &self.operator,
            &checkpoint_key(checkpoint_id),
            &checkpoint_bytes,
            "persist pack index",
        )
        .await?;

        let revision = IndexRevision {
            magic: REVISION_MAGIC.to_owned(),
            major: FORMAT_MAJOR,
            parent: self.revision,
            checkpoint: checkpoint_id,
        };
        let revision_bytes = encode(&revision, "persist pack index")?;
        let revision_id = digest(&revision_bytes);
        create_immutable(
            &self.operator,
            &revision_key(revision_id),
            &revision_bytes,
            "persist pack index",
        )
        .await?;

        let head_bytes = encode(
            &IndexHead {
                magic: HEAD_MAGIC.to_owned(),
                major: FORMAT_MAJOR,
                revision: revision_id,
            },
            "persist pack index",
        )?;
        let result = match &self.head_etag {
            Some(etag) => {
                self.operator
                    .write_with(HEAD_KEY, head_bytes)
                    .if_match(etag)
                    .await
            }
            None => {
                self.operator
                    .write_with(HEAD_KEY, head_bytes)
                    .if_not_exists(true)
                    .await
            }
        };
        match result {
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                ) =>
            {
                return Err(ManagedError::new(
                    ManagedErrorKind::Conflict,
                    "persist pack index",
                    "pack index head changed concurrently",
                ));
            }
            Err(_) => {
                return Err(unavailable(
                    "persist pack index",
                    "pack index head cannot be written",
                ));
            }
        }
        let (_, etag) = read_head(&self.operator)
            .await?
            .ok_or_else(|| corrupt("persist pack index", "published head is missing"))?;
        self.revision = Some(revision_id);
        self.sections = checkpoint
            .sections
            .iter()
            .map(|section| section.id)
            .collect();
        self.head_etag = etag;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Footer {
    magic: String,
    major: u16,
    entries: Vec<FooterEntry>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FooterEntry {
    content: ContentRef,
    offset: u64,
    stored_length: u64,
    codec: Codec,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Codec {
    Raw,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IndexCheckpoint {
    magic: String,
    major: u16,
    sections: Vec<StoredIndexSectionReference>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IndexEntry {
    content: ContentRef,
    locations: Vec<PackLocation>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredIndexSectionReference {
    kind: u8,
    id: [u8; 32],
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    records: u32,
    encoded_bytes: u64,
}

impl From<SectionReference> for StoredIndexSectionReference {
    fn from(reference: SectionReference) -> Self {
        Self {
            kind: reference.kind,
            id: reference.id,
            first_key: reference.first_key,
            last_key: reference.last_key,
            records: reference.records,
            encoded_bytes: reference.encoded_bytes,
        }
    }
}

impl StoredIndexSectionReference {
    fn as_reference(&self) -> SectionReference {
        SectionReference {
            kind: self.kind,
            id: self.id,
            first_key: self.first_key.clone(),
            last_key: self.last_key.clone(),
            records: self.records,
            encoded_bytes: self.encoded_bytes,
        }
    }
}

impl IndexCheckpoint {
    async fn from_locations(
        operator: &Operator,
        locations: &BTreeMap<ContentRef, Vec<PackLocation>>,
        known_sections: &BTreeSet<[u8; 32]>,
    ) -> Result<Self, ManagedError> {
        let records = locations
            .iter()
            .map(|(content, locations)| {
                let entry = IndexEntry {
                    content: *content,
                    locations: locations.clone(),
                };
                Ok(SectionRecord {
                    key: content_key(*content),
                    value: encode(&entry, "persist pack index")?,
                })
            })
            .collect::<Result<Vec<_>, ManagedError>>()?;
        let encoded = section::encode([0; 16], INDEX_SECTION, records, "persist pack index")?;
        let mut sections = Vec::with_capacity(encoded.len());
        for section in encoded {
            let present = if known_sections.contains(&section.reference.id) {
                match operator
                    .stat(&index_section_key(section.reference.id))
                    .await
                {
                    Ok(metadata) if metadata.content_length() == section.bytes.len() as u64 => true,
                    Ok(_) => {
                        return Err(corrupt(
                            "persist pack index",
                            "existing index section has another length",
                        ));
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => false,
                    Err(_) => {
                        return Err(unavailable(
                            "persist pack index",
                            "index section cannot be inspected",
                        ));
                    }
                }
            } else {
                false
            };
            if !present {
                create_immutable(
                    operator,
                    &index_section_key(section.reference.id),
                    &section.bytes,
                    "persist pack index",
                )
                .await?;
            }
            sections.push(section.reference.into());
        }
        Ok(Self {
            magic: CHECKPOINT_MAGIC.to_owned(),
            major: FORMAT_MAJOR,
            sections,
        })
    }

    async fn into_locations(
        self,
        operator: &Operator,
    ) -> Result<BTreeMap<ContentRef, Vec<PackLocation>>, ManagedError> {
        let mut output = BTreeMap::new();
        let mut previous = None;
        let mut previous_section: Option<&StoredIndexSectionReference> = None;
        for stored in &self.sections {
            if stored.kind != INDEX_SECTION
                || stored.records == 0
                || stored.first_key > stored.last_key
                || previous_section.is_some_and(|previous| previous.last_key >= stored.first_key)
            {
                return Err(corrupt(
                    "open pack index",
                    "index section references are invalid",
                ));
            }
            let reference = stored.as_reference();
            let bytes = read_required(
                operator,
                &index_section_key(reference.id),
                "open pack index",
            )
            .await?;
            for record in section::decode(&reference, [0; 16], &bytes, "open pack index")? {
                let entry: IndexEntry = decode(&record.value, "open pack index")?;
                if record.key != content_key(entry.content)
                    || previous.is_some_and(|value| value >= entry.content)
                    || entry.locations.is_empty()
                    || !entry.locations.windows(2).all(|pair| pair[0] < pair[1])
                    || entry
                        .locations
                        .iter()
                        .any(|location| location.logical_length != entry.content.logical_length)
                {
                    return Err(corrupt("open pack index", "checkpoint entries are invalid"));
                }
                previous = Some(entry.content);
                output.insert(entry.content, entry.locations);
            }
            previous_section = Some(stored);
        }
        Ok(output)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IndexRevision {
    magic: String,
    major: u16,
    parent: Option<[u8; 32]>,
    checkpoint: [u8; 32],
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IndexHead {
    magic: String,
    major: u16,
    revision: [u8; 32],
}

async fn read_head(operator: &Operator) -> Result<Option<(Vec<u8>, Option<String>)>, ManagedError> {
    let reader = match operator.reader(HEAD_KEY).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable("read pack index", "index head is unavailable")),
    };
    let bytes = match reader.read(..).await {
        Ok(bytes) => bytes.to_bytes().to_vec(),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable("read pack index", "index head is unavailable")),
    };
    let etag = reader
        .metadata()
        .and_then(|metadata| metadata.etag())
        .map(str::to_owned);
    Ok(Some((bytes, etag)))
}

async fn read_head_state(
    operator: &Operator,
) -> Result<(Option<[u8; 32]>, Option<String>), ManagedError> {
    let Some((bytes, etag)) = read_head(operator).await? else {
        return Ok((None, None));
    };
    let revision = decode::<IndexHead>(&bytes, "read pack index")
        .ok()
        .filter(|head| head.magic == HEAD_MAGIC && head.major == FORMAT_MAJOR)
        .map(|head| head.revision);
    Ok((revision, etag))
}

async fn read_required(
    operator: &Operator,
    key: &str,
    action: &'static str,
) -> Result<Vec<u8>, ManagedError> {
    operator
        .read(key)
        .await
        .map(|bytes| bytes.to_bytes().to_vec())
        .map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                corrupt(action, "referenced index record is missing")
            } else {
                unavailable(action, "index record is unavailable")
            }
        })
}

async fn create_immutable(
    operator: &Operator,
    key: &str,
    bytes: &[u8],
    action: &'static str,
) -> Result<(), ManagedError> {
    match operator
        .write_with(key, bytes.to_vec())
        .if_not_exists(true)
        .await
    {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
            ) =>
        {
            let existing = operator
                .read(key)
                .await
                .map_err(|_| unavailable(action, "existing immutable record is unavailable"))?;
            if existing.to_bytes().as_ref() == bytes {
                Ok(())
            } else {
                Err(ManagedError::new(
                    ManagedErrorKind::Conflict,
                    action,
                    "immutable key already contains different bytes",
                ))
            }
        }
        Err(_) => Err(unavailable(action, "immutable record cannot be created")),
    }
}

fn require_index_read_capabilities(operator: &Operator) -> Result<(), ManagedError> {
    let capability = operator.info().full_capability();
    if capability.read && capability.stat {
        Ok(())
    } else {
        Err(unavailable(
            "open pack index",
            "reading a pack index requires read and stat",
        ))
    }
}

fn require_index_create_capabilities(operator: &Operator) -> Result<(), ManagedError> {
    let capability = operator.info().full_capability();
    if capability.read && capability.write && capability.write_with_if_not_exists && capability.stat
    {
        Ok(())
    } else {
        Err(unavailable(
            "persist pack index",
            "creating a pack index requires read, write, stat, and create-only write",
        ))
    }
}

fn normalize_locations(locations: &mut BTreeMap<ContentRef, Vec<PackLocation>>) {
    locations.retain(|_, values| {
        values.sort_unstable();
        values.dedup();
        !values.is_empty()
    });
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        digest: digest(bytes),
        logical_length: bytes.len() as u64,
    }
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn pack_key(id: PackId) -> String {
    format!("{PACK_ROOT}/{}.pack", hex(&id.0))
}

fn checkpoint_key(id: [u8; 32]) -> String {
    format!("{INDEX_ROOT}/checkpoints/{}.cbor", hex(&id))
}

fn index_section_key(id: [u8; 32]) -> String {
    let encoded = hex(&id);
    format!("{INDEX_SECTION_ROOT}/{}/{}.section", &encoded[..2], encoded)
}

fn content_key(content: ContentRef) -> Vec<u8> {
    let mut key = Vec::with_capacity(40);
    key.extend_from_slice(&content.digest);
    key.extend_from_slice(&content.logical_length.to_be_bytes());
    key
}

fn revision_key(id: [u8; 32]) -> String {
    format!("{INDEX_ROOT}/revisions/{}.cbor", hex(&id))
}

fn pack_id_from_key(key: &str) -> Option<PackId> {
    let value = key
        .strip_prefix(&format!("{PACK_ROOT}/"))?
        .strip_suffix(".pack")?;
    Some(PackId(parse_hex(value)?))
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

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("checked envelope"),
    )
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked envelope"),
    )
}

fn encode(value: &impl Serialize, action: &'static str) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| invalid(action, "record cannot be encoded"))?;
    Ok(bytes)
}

fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    action: &'static str,
) -> Result<T, ManagedError> {
    let mut cursor = Cursor::new(bytes);
    let value = ciborium::de::from_reader(&mut cursor)
        .map_err(|_| corrupt(action, "record is not valid deterministic CBOR"))?;
    if cursor.position() != bytes.len() as u64 {
        return Err(corrupt(action, "record has trailing bytes"));
    }
    Ok(value)
}

fn validate_record(
    magic: &str,
    major: u16,
    expected_magic: &str,
    action: &'static str,
) -> Result<(), ManagedError> {
    if magic == expected_magic && major == FORMAT_MAJOR {
        Ok(())
    } else {
        Err(corrupt(action, "record version is invalid"))
    }
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn unavailable(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Unavailable, action, message)
}

#[cfg(test)]
mod tests {
    use opendal::services;
    use tempfile::TempDir;

    use super::*;

    fn memory() -> Operator {
        Operator::new(services::Memory::default()).unwrap().finish()
    }

    #[tokio::test]
    async fn filesystem_pack_creates_index_but_refuses_unsafe_update() {
        let root = TempDir::new().unwrap();
        let operator = Operator::new(services::Fs::default().root(root.path().to_str().unwrap()))
            .unwrap()
            .finish();
        let store = PackStore::new(operator.clone()).unwrap();
        let operation = OperationId::from_bytes([7; 16]);
        let sealed = store
            .seal(
                operation,
                vec![b"alpha".to_vec(), b"beta".to_vec(), b"alpha".to_vec()],
            )
            .await
            .unwrap();
        assert_eq!(sealed.locations.len(), 2);
        let alpha = content_ref(b"alpha");
        let location = sealed.locations[&alpha];
        assert_eq!(store.read(alpha, location).await.unwrap(), b"alpha");

        let rebuilt = store.rebuild_index().await.unwrap();
        assert_eq!(rebuilt.locations(alpha), &[location]);
        let mut reopened = PackIndex::open(operator).await.unwrap().unwrap();
        let error = reopened.persist().await.unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Unavailable);
    }

    #[tokio::test]
    async fn memory_pack_reads_new_create_only_index() {
        let operator = memory();
        let store = PackStore::new(operator.clone()).unwrap();
        let sealed = store
            .seal(OperationId::from_bytes([3; 16]), vec![b"on disk".to_vec()])
            .await
            .unwrap();
        let content = content_ref(b"on disk");
        let complete = store.read_complete(sealed.id).await.unwrap();
        assert_eq!(complete.content(content), Some(b"on disk".as_slice()));
        assert_eq!(complete.content(content_ref(b"missing")), None);
        assert_eq!(
            store
                .read(content, sealed.locations[&content])
                .await
                .unwrap(),
            b"on disk"
        );
        store.rebuild_index().await.unwrap();
        let reopened = PackIndex::open(operator).await.unwrap().unwrap();
        assert_eq!(reopened.locations(content), &[sealed.locations[&content]]);
    }

    #[tokio::test]
    async fn corrupted_pack_is_rejected() {
        let operator = memory();
        let store = PackStore::new(operator.clone()).unwrap();
        let sealed = store
            .seal(OperationId::from_bytes([9; 16]), vec![b"payload".to_vec()])
            .await
            .unwrap();
        let key = pack_key(sealed.id);
        let mut bytes = operator.read(&key).await.unwrap().to_bytes().to_vec();
        bytes[HEADER_LENGTH as usize] ^= 0xff;
        operator.write(&key, bytes).await.unwrap();

        let error = store.inspect(sealed.id).await.unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
    }
}
