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

//! Persistent ordered indexes stored as immutable Managed metadata pages.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Cursor;

use opendal::{Buffer, Operator};
use serde::de::{DeserializeOwned, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::filesystem::VolumeError;

use super::error::{corrupt, invalid};
use super::object;

const LEAF_MAGIC: [u8; 8] = *b"OFSIDXL1";
const INTERNAL_MAGIC: [u8; 8] = *b"OFSIDXI1";
const CHECKSUM_BYTES: usize = 32;
const PAGE_TARGET_BYTES: usize = 128 * 1024;
const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_TREE_DEPTH: usize = 64;
const PAGE_FIXED_BYTES: usize = LEAF_MAGIC.len() + CHECKSUM_BYTES + 9;

/// An exact reference to one immutable ordered-index page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageRef {
    pub(crate) digest: [u8; 32],
    pub(crate) encoded_length: u64,
    pub(crate) first_key: Box<[u8]>,
    pub(crate) last_key: Box<[u8]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageKind {
    Leaf,
    Internal,
}

impl PageKind {
    const fn magic(self) -> [u8; 8] {
        match self {
            Self::Leaf => LEAF_MAGIC,
            Self::Internal => INTERNAL_MAGIC,
        }
    }
}

struct PageItem {
    record: EncodedRecord,
    first_key: Box<[u8]>,
    last_key: Box<[u8]>,
}

#[derive(Deserialize, Serialize)]
#[serde(transparent)]
struct PageBody(Vec<EncodedRecord>);

struct EncodedRecord(Vec<u8>);

impl Serialize for EncodedRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for EncodedRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(EncodedRecordVisitor)
    }
}

struct EncodedRecordVisitor;

impl<'de> Visitor<'de> for EncodedRecordVisitor {
    type Value = EncodedRecord;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a CBOR byte string containing one index record")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E> {
        Ok(EncodedRecord(value.to_vec()))
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E> {
        Ok(EncodedRecord(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut value = Vec::with_capacity(sequence.size_hint().unwrap_or_default());
        while let Some(byte) = sequence.next_element()? {
            value.push(byte);
        }
        Ok(EncodedRecord(value))
    }
}

/// Write a complete persistent ordered index and return its immutable root.
pub(crate) async fn write_index<K, V>(
    operator: &Operator,
    entries: &BTreeMap<K, V>,
) -> Result<PageRef, VolumeError>
where
    K: Ord + Serialize,
    V: Serialize,
{
    let mut leaf_items = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let key = encode_cbor(key)?;
        leaf_items.push(PageItem {
            record: EncodedRecord(encode_cbor(&(EncodedRecord(key.clone()), value))?),
            first_key: key.clone().into_boxed_slice(),
            last_key: key.into_boxed_slice(),
        });
    }

    let leaf_groups = if leaf_items.is_empty() {
        vec![Vec::new()]
    } else {
        group_items(leaf_items, 1)
    };
    let mut level = write_pages(operator, PageKind::Leaf, leaf_groups).await?;

    while level.len() > 1 {
        let mut items = Vec::with_capacity(level.len());
        for page in level {
            items.push(PageItem {
                record: EncodedRecord(encode_cbor(&page)?),
                first_key: page.first_key.clone(),
                last_key: page.last_key.clone(),
            });
        }
        level = write_pages(operator, PageKind::Internal, group_items(items, 2)).await?;
    }

    Ok(level
        .pop()
        .expect("an index always has one leaf or internal root"))
}

/// Read and verify a complete persistent ordered index.
pub(crate) async fn read_index<K, V>(
    operator: &Operator,
    root: &PageRef,
) -> Result<BTreeMap<K, V>, VolumeError>
where
    K: DeserializeOwned + Ord + Serialize,
    V: DeserializeOwned,
{
    let mut entries = BTreeMap::new();
    let mut visited = BTreeSet::new();
    let mut pending = vec![(root.clone(), 0_usize, true)];

    while let Some((reference, depth, is_root)) = pending.pop() {
        if depth > MAX_TREE_DEPTH {
            return Err(corrupt("read Managed index", "index tree is too deep"));
        }
        if !visited.insert(reference.digest) {
            return Err(corrupt(
                "read Managed index",
                "index page is referenced more than once",
            ));
        }

        let (kind, records) = read_page(operator, &reference).await?;
        match kind {
            PageKind::Leaf => {
                if records.is_empty() {
                    if !is_root || !reference.first_key.is_empty() || !reference.last_key.is_empty()
                    {
                        return Err(corrupt(
                            "read Managed index",
                            "empty index leaf has an invalid key range",
                        ));
                    }
                    continue;
                }

                let mut page_first = None;
                let mut page_last = None;
                for record in records {
                    let (EncodedRecord(key_bytes), value): (EncodedRecord, V) =
                        decode_cbor(&record.0)?;
                    let key: K = decode_cbor(&key_bytes)?;
                    if encode_cbor(&key)? != key_bytes {
                        return Err(corrupt(
                            "read Managed index",
                            "index key is not canonically encoded",
                        ));
                    }
                    if entries
                        .last_key_value()
                        .is_some_and(|(previous, _)| previous >= &key)
                    {
                        return Err(corrupt(
                            "read Managed index",
                            "index leaf records are not strictly ordered",
                        ));
                    }
                    page_first.get_or_insert_with(|| key_bytes.clone());
                    page_last = Some(key_bytes);
                    entries.insert(key, value);
                }
                if page_first.as_deref() != Some(reference.first_key.as_ref())
                    || page_last.as_deref() != Some(reference.last_key.as_ref())
                {
                    return Err(corrupt(
                        "read Managed index",
                        "index leaf range does not match its reference",
                    ));
                }
            }
            PageKind::Internal => {
                if records.len() < 2 {
                    return Err(corrupt(
                        "read Managed index",
                        "internal index page has fewer than two children",
                    ));
                }
                let mut children = Vec::with_capacity(records.len());
                for record in records {
                    children.push(decode_cbor::<PageRef>(&record.0)?);
                }
                validate_children::<K>(&reference, &children)?;
                pending.extend(
                    children
                        .into_iter()
                        .rev()
                        .map(|child| (child, depth + 1, false)),
                );
            }
        }
    }

    if entries.is_empty() {
        if !root.first_key.is_empty() || !root.last_key.is_empty() {
            return Err(corrupt(
                "read Managed index",
                "empty index root has an invalid key range",
            ));
        }
    } else {
        let first = encode_cbor(entries.first_key_value().expect("not empty").0)?;
        let last = encode_cbor(entries.last_key_value().expect("not empty").0)?;
        if first.as_slice() != root.first_key.as_ref() || last.as_slice() != root.last_key.as_ref()
        {
            return Err(corrupt(
                "read Managed index",
                "index contents do not match the root range",
            ));
        }
    }
    Ok(entries)
}

fn group_items(items: Vec<PageItem>, minimum_items: usize) -> Vec<Vec<PageItem>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut estimated_bytes = PAGE_FIXED_BYTES;

    for item in items {
        let item_bytes = item.record.0.len() + cbor_bytes_header(item.record.0.len());
        if current.len() >= minimum_items
            && estimated_bytes.saturating_add(item_bytes) > PAGE_TARGET_BYTES
        {
            groups.push(std::mem::take(&mut current));
            estimated_bytes = PAGE_FIXED_BYTES;
        }
        estimated_bytes = estimated_bytes.saturating_add(item_bytes);
        current.push(item);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    if minimum_items > 1
        && groups
            .last()
            .is_some_and(|group| group.len() < minimum_items)
    {
        let tail = groups.pop().expect("the undersized final group exists");
        groups
            .last_mut()
            .expect("an internal level with one item is already a root")
            .extend(tail);
    }
    groups
}

async fn write_pages(
    operator: &Operator,
    kind: PageKind,
    groups: Vec<Vec<PageItem>>,
) -> Result<Vec<PageRef>, VolumeError> {
    let mut pages = Vec::with_capacity(groups.len());
    for group in groups {
        let first_key = group
            .first()
            .map_or_else(Box::default, |item| item.first_key.clone());
        let last_key = group
            .last()
            .map_or_else(Box::default, |item| item.last_key.clone());
        let records = group.into_iter().map(|item| item.record).collect();
        pages.push(write_page(operator, kind, records, first_key, last_key).await?);
    }
    Ok(pages)
}

async fn write_page(
    operator: &Operator,
    kind: PageKind,
    records: Vec<EncodedRecord>,
    first_key: Box<[u8]>,
    last_key: Box<[u8]>,
) -> Result<PageRef, VolumeError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&kind.magic());
    ciborium::into_writer(&PageBody(records), &mut bytes)
        .map_err(|_| invalid("write Managed index", "index page cannot be encoded"))?;
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    if bytes.len() > MAX_PAGE_BYTES {
        return Err(invalid(
            "write Managed index",
            "one index page exceeds its record-size bound",
        ));
    }

    let digest: [u8; 32] = blake3::hash(&bytes).into();
    let encoded_length = u64::try_from(bytes.len())
        .map_err(|_| invalid("write Managed index", "index page length overflows"))?;
    object::create_immutable(operator, &page_key(digest), Buffer::from(bytes)).await?;
    Ok(PageRef {
        digest,
        encoded_length,
        first_key,
        last_key,
    })
}

async fn read_page(
    operator: &Operator,
    reference: &PageRef,
) -> Result<(PageKind, Vec<EncodedRecord>), VolumeError> {
    let length = usize::try_from(reference.encoded_length)
        .ok()
        .filter(|length| *length <= MAX_PAGE_BYTES)
        .ok_or_else(|| corrupt("read Managed index", "index page length is invalid"))?;
    let bytes = object::read(operator, &page_key(reference.digest), length)
        .await?
        .ok_or_else(|| corrupt("read Managed index", "referenced index page is missing"))?;
    if bytes.len() != length || blake3::hash(&bytes).as_bytes() != &reference.digest {
        return Err(corrupt(
            "read Managed index",
            "index page does not match its reference",
        ));
    }

    let (kind, body) = if let Some(body) = bytes.strip_prefix(&LEAF_MAGIC) {
        (PageKind::Leaf, body)
    } else if let Some(body) = bytes.strip_prefix(&INTERNAL_MAGIC) {
        (PageKind::Internal, body)
    } else {
        return Err(corrupt("read Managed index", "index page magic is invalid"));
    };
    let body = body
        .get(
            ..body
                .len()
                .checked_sub(CHECKSUM_BYTES)
                .ok_or_else(|| corrupt("read Managed index", "index page checksum is missing"))?,
        )
        .ok_or_else(|| corrupt("read Managed index", "index page checksum is missing"))?;
    if blake3::hash(&bytes[..bytes.len() - CHECKSUM_BYTES]).as_bytes()
        != &bytes[bytes.len() - CHECKSUM_BYTES..]
    {
        return Err(corrupt(
            "read Managed index",
            "index page checksum is invalid",
        ));
    }
    let PageBody(records) = decode_cbor(body)?;
    Ok((kind, records))
}

fn validate_children<K>(parent: &PageRef, children: &[PageRef]) -> Result<(), VolumeError>
where
    K: DeserializeOwned + Ord + Serialize,
{
    if children.first().map(|child| child.first_key.as_ref()) != Some(parent.first_key.as_ref())
        || children.last().map(|child| child.last_key.as_ref()) != Some(parent.last_key.as_ref())
    {
        return Err(corrupt(
            "read Managed index",
            "internal index range does not match its reference",
        ));
    }

    let mut previous_last = None;
    for child in children {
        if child.first_key.is_empty() || child.last_key.is_empty() {
            return Err(corrupt(
                "read Managed index",
                "internal index child has an empty key range",
            ));
        }
        let first: K = decode_cbor(&child.first_key)?;
        let last: K = decode_cbor(&child.last_key)?;
        if first > last
            || encode_cbor(&first)?.as_slice() != child.first_key.as_ref()
            || encode_cbor(&last)?.as_slice() != child.last_key.as_ref()
            || previous_last
                .as_ref()
                .is_some_and(|previous: &K| previous >= &first)
        {
            return Err(corrupt(
                "read Managed index",
                "internal index child ranges are invalid",
            ));
        }
        previous_last = Some(last);
    }
    Ok(())
}

fn encode_cbor<T>(value: &T) -> Result<Vec<u8>, VolumeError>
where
    T: Serialize + ?Sized,
{
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)
        .map_err(|_| invalid("write Managed index", "index record cannot be encoded"))?;
    Ok(bytes)
}

fn decode_cbor<T>(bytes: &[u8]) -> Result<T, VolumeError>
where
    T: DeserializeOwned,
{
    let mut input = Cursor::new(bytes);
    let value = ciborium::from_reader(&mut input)
        .map_err(|_| corrupt("read Managed index", "index record is invalid"))?;
    if input.position() != bytes.len() as u64 {
        return Err(corrupt(
            "read Managed index",
            "index record has trailing bytes",
        ));
    }
    Ok(value)
}

fn cbor_bytes_header(length: usize) -> usize {
    match length {
        0..=23 => 1,
        24..=0xff => 2,
        0x100..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}

fn page_key(digest: [u8; 32]) -> String {
    let digest = blake3::Hash::from_bytes(digest).to_hex();
    format!("managed/1/objects/meta/{}/{digest}", &digest[..2])
}
