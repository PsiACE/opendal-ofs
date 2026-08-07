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

//! Content-defined immutable sections shared by Managed metadata indexes.

use fastcdc::v2020::FastCDC;
use sha2::{Digest as _, Sha256};

use super::{ManagedError, ManagedErrorKind};

const MAGIC: &[u8; 8] = b"OFSSECT1";
const TRAILER_MAGIC: &[u8; 8] = b"OFSSECTR";
const FORMAT_MAJOR: u16 = 1;
const HEADER_LENGTH: usize = 8 + 2 + 1 + 1 + 16 + 4;
const TRAILER_LENGTH: usize = 8 + 32;

pub(crate) const MIN_SECTION_BYTES: u32 = 2 * 1024 * 1024;
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
    if records.is_empty() {
        return Ok(Vec::new());
    }
    if records.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(invalid(action, "section records are not strictly ordered"));
    }

    let frames = records
        .iter()
        .map(encode_record)
        .collect::<Result<Vec<_>, _>>()?;
    let mut stream = Vec::new();
    let mut record_ends = Vec::with_capacity(frames.len());
    for frame in &frames {
        stream.extend_from_slice(frame);
        record_ends.push(stream.len());
    }

    let boundaries = if stream.len() <= maximum as usize {
        vec![records.len()]
    } else {
        let chunks = FastCDC::new(&stream, minimum, target, maximum);
        let mut boundaries = Vec::new();
        for chunk in chunks {
            let desired = chunk.offset + chunk.length;
            let record = record_ends.partition_point(|end| *end < desired);
            boundaries.push((record + 1).min(records.len()));
        }
        boundaries.sort_unstable();
        boundaries.dedup();
        if boundaries.last().copied() != Some(records.len()) {
            boundaries.push(records.len());
        }
        boundaries
    };

    let mut output = Vec::with_capacity(boundaries.len());
    let mut start = 0;
    for end in boundaries {
        if end <= start {
            continue;
        }
        output.push(encode_one(
            scope,
            kind,
            &records[start..end],
            &frames[start..end],
        )?);
        start = end;
    }
    Ok(output)
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
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn sections_round_trip_and_preserve_ranges() {
        let records = (0_u32..80)
            .map(|index| Record {
                key: index.to_be_bytes().to_vec(),
                value: vec![index as u8; 32],
            })
            .collect();
        let encoded = encode_with_sizes([7; 16], 3, records, 256, 512, 1024, "test").unwrap();
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
            32,
            64,
            128,
            "test",
        )
        .unwrap()
        .remove(0);
        encoded.bytes[HEADER_LENGTH] ^= 1;
        assert!(decode(&encoded.reference, [1; 16], &encoded.bytes, "test").is_err());
    }

    #[test]
    fn content_defined_boundaries_reuse_unchanged_sections() {
        let records = (0_u32..600)
            .map(|index| Record {
                key: index.to_be_bytes().to_vec(),
                value: vec![(index % 251) as u8; 64],
            })
            .collect::<Vec<_>>();
        let before =
            encode_with_sizes([2; 16], 5, records.clone(), 256, 512, 1024, "test").unwrap();
        let mut changed = records;
        changed[300].value[0] ^= 1;
        let after = encode_with_sizes([2; 16], 5, changed, 256, 512, 1024, "test").unwrap();
        let before_ids = before
            .iter()
            .map(|section| section.reference.id)
            .collect::<BTreeSet<_>>();
        let reused = after
            .iter()
            .filter(|section| before_ids.contains(&section.reference.id))
            .count();
        assert!(reused > 0);
        assert!(reused < after.len());
    }
}
