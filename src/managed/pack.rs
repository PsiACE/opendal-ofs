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

//! Immutable content packs and their derived physical index.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;

use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::OnceCell;

use super::{ManagedError, ManagedErrorKind};
use crate::filesystem::OperationId;
use crate::managed::namespace::ContentRef;
use crate::managed::section::{self, Record as SectionRecord, Reference as SectionReference};

const PACK_ROOT: &str = ".ofs/managed/indexes/data-pack/v1/packs/sha256";
const INDEX_SECTION_ROOT: &str = ".ofs/managed/indexes/data-pack/v1/sections/sha256";
const HEAD_KEY: &str = ".ofs/managed/indexes/data-pack/v1/head.ofs";
const PACK_MAGIC: &[u8; 8] = b"OFSPACK1";
const TRAILER_MAGIC: &[u8; 8] = b"OFSPTRL1";
const FOOTER_MAGIC: &str = "ofs-pack-footer";
const HEAD_MAGIC: &str = "ofs-pack-index-head";
const FORMAT_MAJOR: u16 = 1;
const INDEX_SECTION: u8 = 32;
const HEADER_LENGTH: u64 = 26;
const TRAILER_LENGTH: u64 = 56;

/// Identity of an immutable pack. It is the SHA-256 checksum in its trailer.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct PackId([u8; 32]);

/// Physical range containing one content object.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PackLocation {
    pub pack: PackId,
    pub offset: u64,
    pub stored_length: u64,
}

/// Result of sealing one immutable pack.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SealedPack {
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

/// Pack locations fixed for one materialization operation.
#[derive(Clone, Debug)]
pub(crate) struct PackReadSession {
    operator: Operator,
    store: PackStore,
    index: Arc<OnceCell<Result<Option<PackIndex>, ManagedError>>>,
}

impl PackReadSession {
    pub(crate) fn new(operator: Operator) -> Result<Self, ManagedError> {
        require_index_read_capabilities(&operator)?;
        Ok(Self {
            operator: operator.clone(),
            store: PackStore { operator },
            index: Arc::new(OnceCell::new()),
        })
    }

    /// Return locations from the pack index fixed for this operation.
    pub(crate) async fn locations(&self, content: ContentRef) -> Vec<PackLocation> {
        let operator = self.operator.clone();
        let index = self
            .index
            .get_or_init(|| async move { PackIndex::open(operator).await })
            .await;
        match index {
            Ok(Some(index)) => index.locations(content).to_vec(),
            Ok(None) | Err(_) => Vec::new(),
        }
    }

    /// Download a complete pack without retaining it in this session.
    pub(crate) async fn read_full(&self, id: PackId) -> Result<VerifiedPack, ManagedError> {
        self.store.read_complete(id).await
    }

    /// Read one packed location. `None` means the fixed index has no location.
    pub(crate) async fn read(&self, content: ContentRef) -> Result<Option<Vec<u8>>, ManagedError> {
        let locations = self.locations(content).await;
        if locations.is_empty() {
            return Ok(None);
        }

        let mut failure = None;
        for location in locations {
            match self
                .read_ranges(location.pack, &[(content, location)])
                .await
            {
                Ok(mut bytes) => return Ok(bytes.pop()),
                Err(error) => failure = Some(error),
            }
        }
        Err(failure
            .unwrap_or_else(|| corrupt("read Managed data", "pack locations cannot be resolved")))
    }

    /// Fetch several indexed entries from one pack with OpenDAL's native range
    /// merger. Content identities verify the derived index without separate
    /// stat, trailer, or footer requests on the read path.
    pub(crate) async fn read_ranges(
        &self,
        id: PackId,
        entries: &[(ContentRef, PackLocation)],
    ) -> Result<Vec<Vec<u8>>, ManagedError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut ranges = Vec::with_capacity(entries.len());
        for (content, location) in entries {
            validate_indexed_range(*content, *location, id)?;
            ranges.push(location.offset..location.offset + location.stored_length);
        }
        let first = ranges.iter().map(|range| range.start).min().unwrap_or(0);
        let last = ranges.iter().map(|range| range.end).max().unwrap_or(first);
        let gap = usize::try_from(last - first)
            .map_err(|_| invalid("read pack content", "pack range exceeds this platform"))?;
        let reader = self
            .operator
            .reader_with(&pack_key(id))
            .gap(gap)
            .await
            .map_err(|_| unavailable("read pack content", "pack is unavailable"))?;
        let buffers = reader
            .fetch(ranges)
            .await
            .map_err(|_| unavailable("read pack content", "content ranges are unavailable"))?;
        entries
            .iter()
            .zip(buffers)
            .map(|((content, _), buffer)| {
                let bytes = buffer.to_bytes().to_vec();
                if content_ref(&bytes) != *content {
                    return Err(corrupt(
                        "read pack content",
                        "content range fails validation",
                    ));
                }
                Ok(bytes)
            })
            .collect()
    }
}

/// Concrete pack storage backed by one OpenDAL operator.
#[derive(Clone, Debug)]
pub(crate) struct PackStore {
    operator: Operator,
}

impl PackStore {
    pub(crate) fn new(operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.read || !capability.write || !capability.write_with_if_not_exists {
            return Err(unavailable(
                "open pack store",
                "pack storage requires read, write, and create-only write",
            ));
        }
        Ok(Self { operator })
    }

    /// Seal distinct, non-empty content objects into a format-v1 pack.
    pub(crate) async fn seal(
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
                entries: footer_entries.clone(),
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
        let locations = footer_entries
            .into_iter()
            .map(|entry| {
                (
                    entry.content,
                    PackLocation {
                        pack: id,
                        offset: entry.offset,
                        stored_length: entry.stored_length,
                    },
                )
            })
            .collect();
        Ok(SealedPack { id, locations })
    }

    /// Download and verify a complete pack for reading several entries.
    pub(crate) async fn read_complete(&self, id: PackId) -> Result<VerifiedPack, ManagedError> {
        let key = pack_key(id);
        let bytes = self
            .operator
            .read(&key)
            .await
            .map_err(|_| unavailable("read pack", "pack is unavailable"))?
            .to_bytes()
            .to_vec();
        let length = bytes.len() as u64;
        if length < HEADER_LENGTH + TRAILER_LENGTH {
            return Err(corrupt("read pack", "pack is shorter than its envelope"));
        }

        let trailer = &bytes[(length - TRAILER_LENGTH) as usize..];
        if trailer.len() != TRAILER_LENGTH as usize || &trailer[..8] != TRAILER_MAGIC {
            return Err(corrupt("read pack", "pack trailer is invalid"));
        }
        let footer_offset = u64_at(trailer, 8);
        let footer_length = u64_at(trailer, 16);
        let expected_checksum: [u8; 32] = trailer[24..56]
            .try_into()
            .expect("trailer checksum has fixed length");
        if expected_checksum != id.0 {
            return Err(corrupt(
                "read pack",
                "pack trailer does not match its identity",
            ));
        }
        let trailer_offset = length - TRAILER_LENGTH;
        if footer_offset < HEADER_LENGTH
            || footer_offset.checked_add(footer_length) != Some(trailer_offset)
        {
            return Err(corrupt("read pack", "pack footer range is invalid"));
        }

        let body = &bytes[..bytes.len() - 32];
        let actual_checksum: [u8; 32] = Sha256::digest(body).into();
        if actual_checksum != expected_checksum {
            return Err(corrupt(
                "read pack",
                "pack checksum does not match its identity",
            ));
        }
        if &body[..8] != PACK_MAGIC || u16_at(body, 8) != FORMAT_MAJOR {
            return Err(corrupt("read pack", "pack header is invalid"));
        }

        let footer: Footer = decode(
            &bytes[footer_offset as usize..trailer_offset as usize],
            "read pack",
        )?;
        if footer.magic != FOOTER_MAGIC || footer.major != FORMAT_MAJOR {
            return Err(corrupt("read pack", "pack footer version is invalid"));
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
                return Err(corrupt("read pack", "pack footer entry is invalid"));
            }
            let start = entry.offset as usize;
            let end = (entry.offset + entry.stored_length) as usize;
            if content_ref(&bytes[start..end]) != entry.content {
                return Err(corrupt(
                    "read pack",
                    "pack entry does not match its content reference",
                ));
            }
            previous = Some(entry.content);
            previous_end = entry.offset + entry.stored_length;
            locations.insert(
                entry.content,
                PackLocation {
                    pack: id,
                    offset: entry.offset,
                    stored_length: entry.stored_length,
                },
            );
        }
        Ok(VerifiedPack {
            pack: SealedPack { id, locations },
            bytes,
        })
    }

    async fn read_footer(
        &self,
        id: PackId,
        length_hint: Option<u64>,
    ) -> Result<SealedPack, ManagedError> {
        let key = pack_key(id);
        let length = match length_hint {
            Some(length) => length,
            None => self
                .operator
                .stat(&key)
                .await
                .map_err(|_| unavailable("rebuild pack index", "pack metadata is unavailable"))?
                .content_length(),
        };
        if length < HEADER_LENGTH + TRAILER_LENGTH {
            return Err(corrupt(
                "rebuild pack index",
                "pack is shorter than its envelope",
            ));
        }

        let trailer_offset = length - TRAILER_LENGTH;
        let trailer = self
            .operator
            .read_with(&key)
            .range(trailer_offset..length)
            .content_length_hint(length)
            .await
            .map_err(|_| unavailable("rebuild pack index", "pack trailer is unavailable"))?
            .to_bytes();
        if trailer.len() != TRAILER_LENGTH as usize || &trailer[..8] != TRAILER_MAGIC {
            return Err(corrupt("rebuild pack index", "pack trailer is invalid"));
        }
        let footer_offset = u64_at(&trailer, 8);
        let footer_length = u64_at(&trailer, 16);
        let checksum: [u8; 32] = trailer[24..56]
            .try_into()
            .expect("trailer checksum has fixed length");
        if checksum != id.0
            || footer_offset < HEADER_LENGTH
            || footer_offset.checked_add(footer_length) != Some(trailer_offset)
        {
            return Err(corrupt(
                "rebuild pack index",
                "pack footer location is invalid",
            ));
        }

        let footer = self
            .operator
            .read_with(&key)
            .range(footer_offset..trailer_offset)
            .content_length_hint(length)
            .await
            .map_err(|_| unavailable("rebuild pack index", "pack footer is unavailable"))?;
        let footer: Footer = decode(&footer.to_bytes(), "rebuild pack index")?;
        if footer.magic != FOOTER_MAGIC || footer.major != FORMAT_MAJOR {
            return Err(corrupt(
                "rebuild pack index",
                "pack footer version is invalid",
            ));
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
                return Err(corrupt(
                    "rebuild pack index",
                    "pack footer entry is invalid",
                ));
            }
            previous = Some(entry.content);
            previous_end = entry.offset + entry.stored_length;
            locations.insert(
                entry.content,
                PackLocation {
                    pack: id,
                    offset: entry.offset,
                    stored_length: entry.stored_length,
                },
            );
        }
        Ok(SealedPack { id, locations })
    }

    pub(crate) async fn rebuild_index(&self) -> Result<usize, ManagedError> {
        let capability = self.operator.info().full_capability();
        if !capability.list || !capability.stat {
            return Err(unavailable(
                "rebuild pack index",
                "pack index rebuild requires list and stat",
            ));
        }
        let mut locations: BTreeMap<ContentRef, Vec<PackLocation>> = BTreeMap::new();
        let entries = self
            .operator
            .list(&format!("{PACK_ROOT}/"))
            .await
            .map_err(|_| unavailable("rebuild pack index", "pack listing is unavailable"))?;
        for entry in entries {
            let Some(id) = pack_id_from_key(entry.path()) else {
                continue;
            };
            let length = entry.metadata().content_length();
            let pack = self.read_footer(id, (length > 0).then_some(length)).await?;
            for (content, location) in pack.locations {
                locations.entry(content).or_default().push(location);
            }
        }
        normalize_locations(&mut locations);
        let (published, head_etag) = match read_head(&self.operator).await? {
            Some((_, etag)) => (true, etag),
            None => (false, None),
        };
        let mut index = PackIndex {
            operator: self.operator.clone(),
            locations,
            sections: Vec::new(),
            dirty: BTreeSet::new(),
            published,
            head_etag,
        };
        index.dirty.extend(index.locations.keys().copied());
        let content = index.locations.len();
        index.persist().await?;
        Ok(content)
    }
}

fn validate_indexed_range(
    content: ContentRef,
    location: PackLocation,
    pack: PackId,
) -> Result<(), ManagedError> {
    if location.pack != pack
        || location.stored_length != content.logical_length
        || location
            .offset
            .checked_add(location.stored_length)
            .is_none()
    {
        return Err(corrupt(
            "read pack content",
            "indexed content range is invalid",
        ));
    }
    Ok(())
}

/// Rebuildable mapping from logical content identities to pack ranges.
#[derive(Clone, Debug)]
pub(crate) struct PackIndex {
    operator: Operator,
    locations: BTreeMap<ContentRef, Vec<PackLocation>>,
    sections: Vec<StoredIndexSectionReference>,
    dirty: BTreeSet<ContentRef>,
    published: bool,
    head_etag: Option<String>,
}

impl PackIndex {
    pub(crate) fn locations(&self, content: ContentRef) -> &[PackLocation] {
        self.locations
            .get(&content)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn add(&mut self, pack: &SealedPack) {
        for (content, location) in &pack.locations {
            let locations = self.locations.entry(*content).or_default();
            if !locations.contains(location) {
                locations.push(*location);
                locations.sort_unstable();
                self.dirty.insert(*content);
            }
        }
    }

    /// Open the published index. A missing head means no index exists yet.
    pub(crate) async fn open(operator: Operator) -> Result<Option<Self>, ManagedError> {
        require_index_read_capabilities(&operator)?;
        let Some((head_bytes, etag)) = read_head(&operator).await? else {
            return Ok(None);
        };
        let head: IndexHead = decode(&head_bytes, "open pack index")?;
        validate_record(&head.magic, head.major, HEAD_MAGIC, "open pack index")?;
        let sections = head.sections.clone();
        let locations = head.into_locations(&operator).await?;
        Ok(Some(Self {
            operator,
            locations,
            sections,
            dirty: BTreeSet::new(),
            published: true,
            head_etag: etag,
        }))
    }

    pub(crate) async fn open_or_empty(operator: Operator) -> Result<Self, ManagedError> {
        match Self::open(operator.clone()).await? {
            Some(index) => Ok(index),
            None => Ok(Self {
                operator,
                locations: BTreeMap::new(),
                sections: Vec::new(),
                dirty: BTreeSet::new(),
                published: false,
                head_etag: None,
            }),
        }
    }

    /// Publish immutable sections through one conditional head update.
    pub(crate) async fn persist(&mut self) -> Result<(), ManagedError> {
        require_index_create_capabilities(&self.operator)?;
        if self.published
            && (!self.operator.info().full_capability().write_with_if_match
                || self.head_etag.is_none())
        {
            return Err(unavailable(
                "persist pack index",
                "updating an existing pack index requires compare-and-swap and a revision token",
            ));
        }
        normalize_locations(&mut self.locations);
        let head =
            IndexHead::from_locations(&self.operator, &self.locations, &self.sections, &self.dirty)
                .await?;
        let head_bytes = encode(&head, "persist pack index")?;
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
        let metadata = match result {
            Ok(metadata) => metadata,
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
        };
        let etag = match metadata.etag() {
            Some(etag) => Some(etag.to_owned()),
            None => self
                .operator
                .stat(HEAD_KEY)
                .await
                .map_err(|_| unavailable("persist pack index", "published head is unavailable"))?
                .etag()
                .map(str::to_owned),
        };
        self.published = true;
        self.sections = head.sections;
        self.dirty.clear();
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
struct IndexHead {
    magic: String,
    major: u16,
    sections: Vec<StoredIndexSectionReference>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredIndexSectionReference {
    kind: u8,
    id: [u8; 32],
    object: [u8; 32],
    offset: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    records: u32,
    encoded_bytes: u64,
}

impl StoredIndexSectionReference {
    fn from_located(object: [u8; 32], located: section::Located) -> Self {
        let reference = located.reference;
        Self {
            kind: reference.kind,
            id: reference.id,
            object,
            offset: located.offset,
            first_key: reference.first_key,
            last_key: reference.last_key,
            records: reference.records,
            encoded_bytes: reference.encoded_bytes,
        }
    }

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

    fn located(&self) -> section::Located {
        section::Located {
            reference: self.as_reference(),
            offset: self.offset,
        }
    }
}

impl IndexHead {
    async fn from_locations(
        operator: &Operator,
        locations: &BTreeMap<ContentRef, Vec<PackLocation>>,
        previous: &[StoredIndexSectionReference],
        dirty: &BTreeSet<ContentRef>,
    ) -> Result<Self, ManagedError> {
        validate_index_sections(previous, "persist pack index")?;
        let mut changes = BTreeMap::new();
        for content in dirty {
            changes.insert(
                content_key(*content),
                locations
                    .get(content)
                    .map(|locations| encode(locations, "persist pack index"))
                    .transpose()?,
            );
        }
        let mut sections = Vec::new();
        let mut encoded = Vec::new();
        let mut unassigned = changes.keys().cloned().collect::<BTreeSet<_>>();
        let mut affected = Vec::new();
        for stored in previous {
            let keys = unassigned
                .iter()
                .take_while(|key| key.as_slice() <= stored.last_key.as_slice())
                .cloned()
                .collect::<Vec<_>>();
            for key in &keys {
                unassigned.remove(key);
            }
            if !keys.is_empty() {
                affected.push(stored.clone());
            }
        }
        let mut affected_records = read_index_sections(operator, &affected, "persist pack index")
            .await?
            .into_iter()
            .map(|(stored, records)| ((stored.object, stored.offset), records))
            .collect::<BTreeMap<_, _>>();
        for stored in previous {
            let keys = changes
                .keys()
                .take_while(|key| key.as_slice() <= stored.last_key.as_slice())
                .cloned()
                .collect::<Vec<_>>();
            if keys.is_empty() {
                sections.push(stored.clone());
                continue;
            }
            let mut records = affected_records
                .remove(&(stored.object, stored.offset))
                .expect("affected pack index section was read")
                .into_iter()
                .map(|record| (record.key, record.value))
                .collect::<BTreeMap<_, _>>();
            let mut changed = false;
            for key in keys {
                match changes.remove(&key).expect("collected index change") {
                    Some(value) => {
                        changed |= records.insert(key, value.clone()).as_ref() != Some(&value);
                    }
                    None => changed |= records.remove(&key).is_some(),
                }
            }
            if !changed {
                sections.push(stored.clone());
                continue;
            }
            encoded.extend(section::encode(
                [0; 16],
                INDEX_SECTION,
                records
                    .into_iter()
                    .map(|(key, value)| SectionRecord { key, value })
                    .collect(),
                "persist pack index",
            )?);
        }
        if !changes.is_empty() {
            let records = changes
                .into_iter()
                .filter_map(|(key, value)| value.map(|value| SectionRecord { key, value }))
                .collect();
            encoded.extend(section::encode(
                [0; 16],
                INDEX_SECTION,
                records,
                "persist pack index",
            )?);
        }
        sections.extend(persist_index_sections(operator, encoded).await?);
        sections.sort_by(|left, right| left.first_key.cmp(&right.first_key));
        validate_index_sections(&sections, "persist pack index")?;
        Ok(Self {
            magic: HEAD_MAGIC.to_owned(),
            major: FORMAT_MAJOR,
            sections,
        })
    }

    async fn into_locations(
        self,
        operator: &Operator,
    ) -> Result<BTreeMap<ContentRef, Vec<PackLocation>>, ManagedError> {
        validate_index_sections(&self.sections, "open pack index")?;
        let mut output = BTreeMap::new();
        let mut previous = None;
        for (_, records) in read_index_sections(operator, &self.sections, "open pack index").await?
        {
            for record in records {
                let content = content_from_key(&record.key)
                    .ok_or_else(|| corrupt("open pack index", "index section key is invalid"))?;
                let locations: Vec<PackLocation> = decode(&record.value, "open pack index")?;
                if previous.is_some_and(|value| value >= content)
                    || locations.is_empty()
                    || !locations.windows(2).all(|pair| pair[0] < pair[1])
                {
                    return Err(corrupt("open pack index", "index entries are invalid"));
                }
                previous = Some(content);
                output.insert(content, locations);
            }
        }
        Ok(output)
    }
}

async fn read_index_sections(
    operator: &Operator,
    stored: &[StoredIndexSectionReference],
    action: &'static str,
) -> Result<Vec<(StoredIndexSectionReference, Vec<SectionRecord>)>, ManagedError> {
    let mut objects = BTreeMap::<[u8; 32], Vec<StoredIndexSectionReference>>::new();
    for section in stored {
        objects
            .entry(section.object)
            .or_default()
            .push(section.clone());
    }
    let mut decoded = Vec::with_capacity(stored.len());
    for (object, mut sections) in objects {
        sections.sort_by_key(|section| section.offset);
        let located = sections
            .iter()
            .map(StoredIndexSectionReference::located)
            .collect();
        let fetched = section::fetch(
            operator,
            &index_section_key(object),
            [0; 16],
            located,
            action,
        )
        .await?;
        for (stored, (_, records)) in sections.into_iter().zip(fetched) {
            decoded.push((stored, records));
        }
    }
    decoded.sort_by(|(left, _), (right, _)| left.first_key.cmp(&right.first_key));
    Ok(decoded)
}

fn validate_index_sections(
    sections: &[StoredIndexSectionReference],
    action: &'static str,
) -> Result<(), ManagedError> {
    if sections.iter().any(|section| {
        section.kind != INDEX_SECTION
            || section.records == 0
            || section.encoded_bytes == 0
            || section.offset.checked_add(section.encoded_bytes).is_none()
            || section.first_key > section.last_key
    }) || sections
        .windows(2)
        .any(|pair| pair[0].last_key >= pair[1].first_key)
    {
        return Err(corrupt(action, "index section references are invalid"));
    }
    let mut ranges = BTreeMap::<[u8; 32], Vec<(u64, u64)>>::new();
    for section in sections {
        ranges.entry(section.object).or_default().push((
            section.offset,
            section
                .offset
                .checked_add(section.encoded_bytes)
                .expect("section range was validated"),
        ));
    }
    for object_ranges in ranges.values_mut() {
        object_ranges.sort_unstable();
        if object_ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(corrupt(action, "index section ranges overlap"));
        }
    }
    Ok(())
}

async fn persist_index_sections(
    operator: &Operator,
    encoded: Vec<section::Encoded>,
) -> Result<Vec<StoredIndexSectionReference>, ManagedError> {
    let Some(object) = section::concatenate(encoded, "persist pack index")? else {
        return Ok(Vec::new());
    };
    create_immutable(
        operator,
        &index_section_key(object.id),
        &object.bytes,
        "persist pack index",
    )
    .await?;
    Ok(object
        .sections
        .into_iter()
        .map(|located| StoredIndexSectionReference::from_located(object.id, located))
        .collect())
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

fn index_section_key(id: [u8; 32]) -> String {
    let encoded = hex(&id);
    format!("{INDEX_SECTION_ROOT}/{encoded}.ofs")
}

fn content_key(content: ContentRef) -> Vec<u8> {
    let mut key = Vec::with_capacity(40);
    key.extend_from_slice(&content.digest);
    key.extend_from_slice(&content.logical_length.to_be_bytes());
    key
}

fn content_from_key(key: &[u8]) -> Option<ContentRef> {
    if key.len() != 40 {
        return None;
    }
    Some(ContentRef {
        digest: key[..32].try_into().expect("fixed digest prefix"),
        logical_length: u64::from_be_bytes(key[32..].try_into().expect("fixed length suffix")),
    })
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
    use super::*;
    use opendal::services;

    fn memory() -> Operator {
        Operator::new(services::Memory::default()).unwrap().finish()
    }

    #[tokio::test]
    async fn index_persist_rewrites_only_the_dirty_section() {
        let operator = memory();
        let pack = PackId([9; 32]);
        let mut locations = (0_u32..80)
            .map(|index| {
                let content = content_ref(&index.to_be_bytes());
                (
                    content,
                    vec![PackLocation {
                        pack,
                        offset: u64::from(index) * 4,
                        stored_length: 4,
                    }],
                )
            })
            .collect::<BTreeMap<_, _>>();
        let records = locations
            .iter()
            .map(|(content, locations)| SectionRecord {
                key: content_key(*content),
                value: encode(locations, "test").unwrap(),
            })
            .collect();
        let previous = persist_index_sections(
            &operator,
            section::encode_for_test([0; 16], INDEX_SECTION, records, 256, 512, 2048).unwrap(),
        )
        .await
        .unwrap();
        assert!(previous.len() > 2);
        assert!(
            previous
                .iter()
                .all(|section| section.object == previous[0].object)
        );
        let changed = *locations.keys().nth(40).unwrap();
        locations.get_mut(&changed).unwrap()[0].offset += 1;

        let checkpoint =
            IndexHead::from_locations(&operator, &locations, &previous, &BTreeSet::from([changed]))
                .await
                .unwrap();
        let previous_ids = previous
            .iter()
            .map(|section| section.id)
            .collect::<BTreeSet<_>>();
        let current_ids = checkpoint
            .sections
            .iter()
            .map(|section| section.id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            previous_ids.intersection(&current_ids).count(),
            previous.len() - 1
        );
        assert_eq!(
            checkpoint.into_locations(&operator).await.unwrap(),
            locations
        );
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

        let error = store.read_complete(sealed.id).await.unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
    }
}
