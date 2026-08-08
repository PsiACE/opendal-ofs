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

//! Ordered immutable sections shared by Managed metadata indexes.

use sha2::{Digest as _, Sha256};

use super::{ManagedError, ManagedErrorKind};

const MAGIC: &[u8; 8] = b"OFSSECT1";
const TRAILER_MAGIC: &[u8; 8] = b"OFSSECTR";
const FORMAT_MAJOR: u16 = 1;
const HEADER_LENGTH: usize = 8 + 2 + 1 + 1 + 16 + 4;
const TRAILER_LENGTH: usize = 8 + 32;

pub(crate) const MIN_SECTION_BYTES: u32 = 1024 * 1024;
pub(crate) const TARGET_SECTION_BYTES: u32 = 4 * 1024 * 1024;
pub(crate) const MAX_SECTION_BYTES: u32 = 8 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Reference {
    pub(crate) kind: u8,
    pub(crate) id: [u8; 32],
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) records: u32,
    pub(crate) encoded_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct Encoded {
    pub(crate) reference: Reference,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct Located {
    pub(crate) reference: Reference,
    pub(crate) offset: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct DataObject {
    pub(crate) id: [u8; 32],
    pub(crate) bytes: Vec<u8>,
    pub(crate) sections: Vec<Located>,
}

/// Concatenate independently verifiable sections into one immutable object.
/// Readers can fetch one section by range or coalesce adjacent referenced
/// ranges into one object request.
pub(crate) fn concatenate(
    encoded: Vec<Encoded>,
    action: &'static str,
) -> Result<Option<DataObject>, ManagedError> {
    if encoded.is_empty() {
        return Ok(None);
    }
    let total_bytes = encoded.iter().try_fold(0_usize, |total, section| {
        total
            .checked_add(section.bytes.len())
            .ok_or_else(|| invalid(action, "section data object is too large"))
    })?;
    let mut bytes = Vec::with_capacity(total_bytes);
    let mut sections = Vec::with_capacity(encoded.len());
    for section in encoded {
        let offset = u64::try_from(bytes.len())
            .map_err(|_| invalid(action, "section data object is too large"))?;
        sections.push(Located {
            reference: section.reference,
            offset,
        });
        bytes.extend_from_slice(&section.bytes);
    }
    let id = Sha256::digest(&bytes).into();
    Ok(Some(DataObject {
        id,
        bytes,
        sections,
    }))
}

pub(crate) fn encode(
    scope: [u8; 16],
    kind: u8,
    records: Vec<Record>,
    action: &'static str,
) -> Result<Vec<Encoded>, ManagedError> {
    encode_with_sizes(
        scope,
        kind,
        records,
        MIN_SECTION_BYTES,
        TARGET_SECTION_BYTES,
        MAX_SECTION_BYTES,
        action,
    )
}

fn encode_with_sizes(
    scope: [u8; 16],
    kind: u8,
    records: Vec<Record>,
    minimum: u32,
    target: u32,
    maximum: u32,
    action: &'static str,
) -> Result<Vec<Encoded>, ManagedError> {
    let envelope_bytes =
        u32::try_from(HEADER_LENGTH + TRAILER_LENGTH).expect("section envelope length fits u32");
    if minimum <= envelope_bytes
        || minimum > target
        || target > maximum
        || target <= envelope_bytes
        || maximum <= envelope_bytes
    {
        return Err(invalid(action, "section size policy is invalid"));
    }
    if records.is_empty() {
        return Ok(Vec::new());
    }
    if records.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(invalid(action, "section records are not strictly ordered"));
    }

    let minimum_payload = minimum - envelope_bytes;
    let target_payload = target - envelope_bytes;
    let maximum_payload = maximum - envelope_bytes;
    let mut output = Vec::new();
    let mut previous: Option<PendingSection> = None;
    let mut current = PendingSection::default();
    for record in records {
        let frame = encode_record(&record)?;
        let next_bytes = current.payload_bytes.saturating_add(frame.len());
        if !current.records.is_empty()
            && (next_bytes > maximum_payload as usize
                || current.payload_bytes >= minimum_payload as usize
                    && next_bytes > target_payload as usize)
        {
            if let Some(ready) = previous.replace(current) {
                output.push(ready.encode(scope, kind)?);
            }
            current = PendingSection::default();
        }
        current.payload_bytes = current.payload_bytes.saturating_add(frame.len());
        current.records.push(record);
        current.frames.push(frame);
    }
    if let Some(mut previous) = previous {
        if current.payload_bytes < minimum_payload as usize
            && previous.payload_bytes.saturating_add(current.payload_bytes)
                <= maximum_payload as usize
        {
            previous.records.append(&mut current.records);
            previous.frames.append(&mut current.frames);
            output.push(previous.encode(scope, kind)?);
        } else {
            output.push(previous.encode(scope, kind)?);
            output.push(current.encode(scope, kind)?);
        }
    } else {
        output.push(current.encode(scope, kind)?);
    }
    Ok(output)
}

#[derive(Default)]
struct PendingSection {
    records: Vec<Record>,
    frames: Vec<Vec<u8>>,
    payload_bytes: usize,
}

impl PendingSection {
    fn encode(self, scope: [u8; 16], kind: u8) -> Result<Encoded, ManagedError> {
        encode_one(scope, kind, &self.records, &self.frames)
    }
}

#[cfg(test)]
pub(crate) fn encode_for_test(
    scope: [u8; 16],
    kind: u8,
    records: Vec<Record>,
    minimum: u32,
    target: u32,
    maximum: u32,
) -> Result<Vec<Encoded>, ManagedError> {
    encode_with_sizes(scope, kind, records, minimum, target, maximum, "test")
}

fn encode_record(record: &Record) -> Result<Vec<u8>, ManagedError> {
    let key_length = u32::try_from(record.key.len())
        .map_err(|_| invalid("encode Managed section", "section key is too long"))?;
    let value_length = u32::try_from(record.value.len())
        .map_err(|_| invalid("encode Managed section", "section value is too long"))?;
    let mut frame = Vec::with_capacity(8 + record.key.len() + record.value.len());
    frame.extend_from_slice(&key_length.to_be_bytes());
    frame.extend_from_slice(&value_length.to_be_bytes());
    frame.extend_from_slice(&record.key);
    frame.extend_from_slice(&record.value);
    Ok(frame)
}

fn encode_one(
    scope: [u8; 16],
    kind: u8,
    records: &[Record],
    frames: &[Vec<u8>],
) -> Result<Encoded, ManagedError> {
    let count = u32::try_from(records.len())
        .map_err(|_| invalid("encode Managed section", "section has too many records"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&FORMAT_MAJOR.to_be_bytes());
    bytes.push(kind);
    bytes.push(0);
    bytes.extend_from_slice(&scope);
    bytes.extend_from_slice(&count.to_be_bytes());
    for frame in frames {
        bytes.extend_from_slice(frame);
    }
    bytes.extend_from_slice(TRAILER_MAGIC);
    let id: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&id);
    Ok(Encoded {
        reference: Reference {
            kind,
            id,
            first_key: records.first().expect("non-empty section").key.clone(),
            last_key: records.last().expect("non-empty section").key.clone(),
            records: count,
            encoded_bytes: bytes.len() as u64,
        },
        bytes,
    })
}

pub(crate) fn decode(
    expected: &Reference,
    scope: [u8; 16],
    bytes: &[u8],
    action: &'static str,
) -> Result<Vec<Record>, ManagedError> {
    if bytes.len() < HEADER_LENGTH + TRAILER_LENGTH
        || &bytes[..8] != MAGIC
        || u16::from_be_bytes([bytes[8], bytes[9]]) != FORMAT_MAJOR
        || bytes[10] != expected.kind
        || bytes[11] != 0
        || bytes[12..28] != scope
        || bytes[bytes.len() - TRAILER_LENGTH..bytes.len() - 32] != *TRAILER_MAGIC
        || bytes.len() as u64 != expected.encoded_bytes
    {
        return Err(corrupt(action, "section envelope is invalid"));
    }
    let observed: [u8; 32] = Sha256::digest(&bytes[..bytes.len() - 32]).into();
    if observed != expected.id || bytes[bytes.len() - 32..] != expected.id {
        return Err(corrupt(action, "section identity is invalid"));
    }
    let count = u32::from_be_bytes(bytes[28..32].try_into().expect("fixed header"));
    if count != expected.records || count == 0 {
        return Err(corrupt(action, "section record count is invalid"));
    }

    let payload_end = bytes.len() - TRAILER_LENGTH;
    let mut offset = HEADER_LENGTH;
    let mut records = Vec::with_capacity(count as usize);
    while offset < payload_end {
        let lengths = bytes
            .get(offset..offset + 8)
            .ok_or_else(|| corrupt(action, "section record frame is truncated"))?;
        let key_length =
            u32::from_be_bytes(lengths[..4].try_into().expect("fixed length")) as usize;
        let value_length =
            u32::from_be_bytes(lengths[4..].try_into().expect("fixed length")) as usize;
        offset += 8;
        let end = offset
            .checked_add(key_length)
            .and_then(|end| end.checked_add(value_length))
            .filter(|end| *end <= payload_end)
            .ok_or_else(|| corrupt(action, "section record frame is invalid"))?;
        records.push(Record {
            key: bytes[offset..offset + key_length].to_vec(),
            value: bytes[offset + key_length..end].to_vec(),
        });
        offset = end;
    }
    if offset != payload_end
        || records.len() != count as usize
        || records.windows(2).any(|pair| pair[0].key >= pair[1].key)
        || records.first().map(|record| &record.key) != Some(&expected.first_key)
        || records.last().map(|record| &record.key) != Some(&expected.last_key)
    {
        return Err(corrupt(action, "section records are invalid"));
    }
    Ok(records)
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_size_policy_is_valid() {
        assert!(encode([0; 16], 1, Vec::new(), "test").is_ok());
    }

    #[test]
    fn sections_round_trip_and_preserve_ranges() {
        let records = (0_u32..80)
            .map(|index| Record {
                key: index.to_be_bytes().to_vec(),
                value: vec![index as u8; 32],
            })
            .collect();
        let encoded = encode_with_sizes([7; 16], 3, records, 256, 512, 2048, "test").unwrap();
        assert!(encoded.len() > 1);
        let mut previous = None;
        let mut count = 0;
        for section in encoded {
            if let Some(previous) = previous {
                assert!(previous < section.reference.first_key);
            }
            let decoded = decode(&section.reference, [7; 16], &section.bytes, "test").unwrap();
            previous = Some(section.reference.last_key.clone());
            count += decoded.len();
        }
        assert_eq!(count, 80);
    }

    #[test]
    fn corruption_is_detected_before_records_are_used() {
        let mut encoded = encode_with_sizes(
            [1; 16],
            9,
            vec![Record {
                key: b"a".to_vec(),
                value: b"value".to_vec(),
            }],
            256,
            512,
            2048,
            "test",
        )
        .unwrap()
        .remove(0);
        encoded.bytes[HEADER_LENGTH] ^= 1;
        assert!(decode(&encoded.reference, [1; 16], &encoded.bytes, "test").is_err());
    }

    #[test]
    fn deterministic_boundaries_follow_record_sizes() {
        let records = (0_u32..600)
            .map(|index| Record {
                key: index.to_be_bytes().to_vec(),
                value: vec![(index % 251) as u8; 64],
            })
            .collect::<Vec<_>>();
        let first = encode_with_sizes([2; 16], 5, records.clone(), 256, 512, 2048, "test").unwrap();
        let second = encode_with_sizes([2; 16], 5, records, 256, 512, 2048, "test").unwrap();
        assert_eq!(
            first
                .iter()
                .map(|section| &section.reference)
                .collect::<Vec<_>>(),
            second
                .iter()
                .map(|section| &section.reference)
                .collect::<Vec<_>>()
        );
        assert!(first.len() > 1);
    }

    #[test]
    fn record_alignment_does_not_merge_records_past_the_maximum() {
        let records = vec![
            Record {
                key: b"a".to_vec(),
                value: vec![1; 1400],
            },
            Record {
                key: b"b".to_vec(),
                value: vec![2; 1400],
            },
        ];
        let encoded = encode_with_sizes([3; 16], 6, records, 256, 512, 2048, "test").unwrap();
        assert_eq!(encoded.len(), 2);
        assert!(
            encoded
                .iter()
                .all(|section| section.reference.encoded_bytes <= 2048)
        );
    }
}
