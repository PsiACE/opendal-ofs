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

//! Immutable sorted tables with independently verified data blocks.

use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::managed::{ManagedError, ManagedErrorKind};

const BLOCK_MAGIC: &[u8; 8] = b"OFSBLK01";
const BLOCK_TRAILER_MAGIC: &[u8; 8] = b"OFSBLKTR";
const INDEX_MAGIC: &[u8; 8] = b"OFSIDX01";
const TABLE_MAGIC: &[u8; 8] = b"OFSTBL01";
const FORMAT_MAJOR: u16 = 1;
const BLOCK_HEADER_LENGTH: usize = 8 + 2 + 2 + 16 + 4;
const BLOCK_TRAILER_LENGTH: usize = 8 + 32;
#[cfg(test)]
const TABLE_TRAILER_LENGTH: usize = 8 + 2 + 2 + 8 + 8 + 32;

const MIN_BLOCK_BYTES: u32 = 64 * 1024;
const TARGET_BLOCK_BYTES: u32 = 256 * 1024;
const MAX_BLOCK_BYTES: u32 = 1024 * 1024;
const FETCH_COALESCING_GAP_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockHandle {
    pub(crate) offset: u64,
    pub(crate) encoded_bytes: u64,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) records: u32,
    pub(crate) checksum: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableRef {
    pub(crate) id: [u8; 32],
    pub(crate) encoded_bytes: u64,
    pub(crate) blocks: Vec<BlockHandle>,
}

#[derive(Clone, Debug)]
pub(crate) struct EncodedTable {
    pub(crate) reference: TableRef,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
struct BlockPolicy {
    minimum: u32,
    target: u32,
    maximum: u32,
}

const PRODUCTION_POLICY: BlockPolicy = BlockPolicy {
    minimum: MIN_BLOCK_BYTES,
    target: TARGET_BLOCK_BYTES,
    maximum: MAX_BLOCK_BYTES,
};

pub(crate) fn build(
    scope: [u8; 16],
    records: Vec<Record>,
    action: &'static str,
) -> Result<Option<EncodedTable>, ManagedError> {
    build_with_policy(scope, records, PRODUCTION_POLICY, action)
}

fn build_with_policy(
    scope: [u8; 16],
    records: Vec<Record>,
    policy: BlockPolicy,
    action: &'static str,
) -> Result<Option<EncodedTable>, ManagedError> {
    if records.is_empty() {
        return Ok(None);
    }
    if records.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(invalid(action, "SSTable records are not strictly ordered"));
    }
    let envelope =
        u32::try_from(BLOCK_HEADER_LENGTH + BLOCK_TRAILER_LENGTH).expect("block envelope fits u32");
    if policy.minimum <= envelope
        || policy.minimum > policy.target
        || policy.target > policy.maximum
    {
        return Err(invalid(action, "SSTable block policy is invalid"));
    }

    let minimum = (policy.minimum - envelope) as usize;
    let target = (policy.target - envelope) as usize;
    let maximum = (policy.maximum - envelope) as usize;
    let mut pending = Vec::<(Vec<Record>, Vec<Vec<u8>>, usize)>::new();
    let mut current_records = Vec::new();
    let mut current_frames = Vec::new();
    let mut current_bytes = 0_usize;
    for record in records {
        let frame = encode_record(&record)?;
        let next = current_bytes.saturating_add(frame.len());
        if !current_records.is_empty()
            && (next > maximum || current_bytes >= minimum && next > target)
        {
            pending.push((current_records, current_frames, current_bytes));
            current_records = Vec::new();
            current_frames = Vec::new();
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(frame.len());
        current_records.push(record);
        current_frames.push(frame);
    }
    if let Some((previous_records, previous_frames, previous_bytes)) = pending.last_mut()
        && current_bytes < minimum
        && previous_bytes.saturating_add(current_bytes) <= maximum
    {
        previous_records.append(&mut current_records);
        previous_frames.append(&mut current_frames);
        *previous_bytes = previous_bytes.saturating_add(current_bytes);
    } else {
        pending.push((current_records, current_frames, current_bytes));
    }

    let mut bytes = Vec::new();
    let mut blocks = Vec::with_capacity(pending.len());
    for (records, frames, _) in pending {
        let offset = u64::try_from(bytes.len())
            .map_err(|_| invalid(action, "SSTable object is too large"))?;
        let encoded = encode_block(scope, &records, &frames)?;
        blocks.push(BlockHandle {
            offset,
            encoded_bytes: encoded.len() as u64,
            first_key: records.first().expect("block is non-empty").key.clone(),
            last_key: records.last().expect("block is non-empty").key.clone(),
            records: records.len() as u32,
            checksum: encoded[encoded.len() - 32..]
                .try_into()
                .expect("block checksum has fixed length"),
        });
        bytes.extend_from_slice(&encoded);
    }
    let index_offset = bytes.len() as u64;
    let index = encode_index(scope, &blocks)?;
    let index_length = index.len() as u64;
    bytes.extend_from_slice(&index);
    bytes.extend_from_slice(TABLE_MAGIC);
    bytes.extend_from_slice(&FORMAT_MAJOR.to_be_bytes());
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&index_offset.to_be_bytes());
    bytes.extend_from_slice(&index_length.to_be_bytes());
    let id: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&id);
    Ok(Some(EncodedTable {
        reference: TableRef {
            id,
            encoded_bytes: bytes.len() as u64,
            blocks,
        },
        bytes,
    }))
}

pub(crate) async fn fetch(
    operator: &Operator,
    key: &str,
    scope: [u8; 16],
    blocks: Vec<BlockHandle>,
    action: &'static str,
) -> Result<Vec<(BlockHandle, Vec<Record>)>, ManagedError> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    let mut ranges = Vec::with_capacity(blocks.len());
    for block in &blocks {
        let end = block
            .offset
            .checked_add(block.encoded_bytes)
            .ok_or_else(|| corrupt(action, "SSTable block range is invalid"))?;
        ranges.push(block.offset..end);
    }
    let reader = operator
        .reader_with(key)
        .gap(FETCH_COALESCING_GAP_BYTES)
        .await
        .map_err(|error| storage_error(error, action, "referenced SSTable object is missing"))?;
    let buffers = reader
        .fetch(ranges)
        .await
        .map_err(|error| storage_error(error, action, "referenced SSTable object is missing"))?;
    blocks
        .into_iter()
        .zip(buffers)
        .map(|(block, bytes)| {
            let records = decode_block(&block, scope, &bytes.to_bytes(), action)?;
            Ok((block, records))
        })
        .collect()
}

fn encode_record(record: &Record) -> Result<Vec<u8>, ManagedError> {
    let key = u32::try_from(record.key.len())
        .map_err(|_| invalid("encode Managed SSTable", "SSTable key is too long"))?;
    let value = u32::try_from(record.value.len())
        .map_err(|_| invalid("encode Managed SSTable", "SSTable value is too long"))?;
    let mut frame = Vec::with_capacity(8 + record.key.len() + record.value.len());
    frame.extend_from_slice(&key.to_be_bytes());
    frame.extend_from_slice(&value.to_be_bytes());
    frame.extend_from_slice(&record.key);
    frame.extend_from_slice(&record.value);
    Ok(frame)
}

fn encode_block(
    scope: [u8; 16],
    records: &[Record],
    frames: &[Vec<u8>],
) -> Result<Vec<u8>, ManagedError> {
    let count = u32::try_from(records.len()).map_err(|_| {
        invalid(
            "encode Managed SSTable",
            "SSTable block has too many records",
        )
    })?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(BLOCK_MAGIC);
    bytes.extend_from_slice(&FORMAT_MAJOR.to_be_bytes());
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&scope);
    bytes.extend_from_slice(&count.to_be_bytes());
    for frame in frames {
        bytes.extend_from_slice(frame);
    }
    bytes.extend_from_slice(BLOCK_TRAILER_MAGIC);
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

fn encode_index(scope: [u8; 16], blocks: &[BlockHandle]) -> Result<Vec<u8>, ManagedError> {
    let count = u32::try_from(blocks.len())
        .map_err(|_| invalid("encode Managed SSTable", "SSTable has too many blocks"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(INDEX_MAGIC);
    bytes.extend_from_slice(&FORMAT_MAJOR.to_be_bytes());
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&scope);
    bytes.extend_from_slice(&count.to_be_bytes());
    for block in blocks {
        bytes.extend_from_slice(&block.offset.to_be_bytes());
        bytes.extend_from_slice(&block.encoded_bytes.to_be_bytes());
        bytes.extend_from_slice(&block.records.to_be_bytes());
        bytes.extend_from_slice(&(block.first_key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&(block.last_key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&block.first_key);
        bytes.extend_from_slice(&block.last_key);
        bytes.extend_from_slice(&block.checksum);
    }
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

fn decode_block(
    expected: &BlockHandle,
    scope: [u8; 16],
    bytes: &[u8],
    action: &'static str,
) -> Result<Vec<Record>, ManagedError> {
    if bytes.len() < BLOCK_HEADER_LENGTH + BLOCK_TRAILER_LENGTH
        || &bytes[..8] != BLOCK_MAGIC
        || u16::from_be_bytes([bytes[8], bytes[9]]) != FORMAT_MAJOR
        || bytes[10..12] != [0, 0]
        || bytes[12..28] != scope
        || bytes.len() as u64 != expected.encoded_bytes
        || bytes[bytes.len() - BLOCK_TRAILER_LENGTH..bytes.len() - 32] != *BLOCK_TRAILER_MAGIC
    {
        return Err(corrupt(action, "SSTable block envelope is invalid"));
    }
    let checksum: [u8; 32] = Sha256::digest(&bytes[..bytes.len() - 32]).into();
    if checksum != expected.checksum || bytes[bytes.len() - 32..] != expected.checksum {
        return Err(corrupt(action, "SSTable block checksum is invalid"));
    }
    let count = u32::from_be_bytes(bytes[28..32].try_into().expect("fixed block header"));
    let payload_end = bytes.len() - BLOCK_TRAILER_LENGTH;
    let mut offset = BLOCK_HEADER_LENGTH;
    let mut records = Vec::with_capacity(count as usize);
    while offset < payload_end {
        let lengths = bytes
            .get(offset..offset + 8)
            .ok_or_else(|| corrupt(action, "SSTable record frame is truncated"))?;
        let key = u32::from_be_bytes(lengths[..4].try_into().expect("fixed key length")) as usize;
        let value =
            u32::from_be_bytes(lengths[4..].try_into().expect("fixed value length")) as usize;
        offset += 8;
        let end = offset
            .checked_add(key)
            .and_then(|end| end.checked_add(value))
            .filter(|end| *end <= payload_end)
            .ok_or_else(|| corrupt(action, "SSTable record frame is invalid"))?;
        records.push(Record {
            key: bytes[offset..offset + key].to_vec(),
            value: bytes[offset + key..end].to_vec(),
        });
        offset = end;
    }
    if offset != payload_end
        || count != expected.records
        || records.len() != count as usize
        || records.windows(2).any(|pair| pair[0].key >= pair[1].key)
        || records.first().map(|record| &record.key) != Some(&expected.first_key)
        || records.last().map(|record| &record.key) != Some(&expected.last_key)
    {
        return Err(corrupt(action, "SSTable block records are invalid"));
    }
    Ok(records)
}

fn storage_error(
    error: opendal::Error,
    action: &'static str,
    missing: &'static str,
) -> ManagedError {
    if error.kind() == ErrorKind::NotFound {
        corrupt(action, missing)
    } else {
        ManagedError::new(
            ManagedErrorKind::Unavailable,
            action,
            "SSTable object is unavailable",
        )
    }
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

#[cfg(test)]
mod tests {
    use opendal::{Operator, services};

    use super::*;

    fn memory() -> Operator {
        Operator::new(services::Memory::default()).unwrap().finish()
    }

    fn small_policy() -> BlockPolicy {
        BlockPolicy {
            minimum: 256,
            target: 512,
            maximum: 2048,
        }
    }

    #[tokio::test]
    async fn table_round_trip_uses_verified_block_ranges() {
        let records = (0_u32..80)
            .map(|index| Record {
                key: index.to_be_bytes().to_vec(),
                value: vec![index as u8; 32],
            })
            .collect();
        let table = build_with_policy([7; 16], records, small_policy(), "test")
            .unwrap()
            .unwrap();
        assert!(table.reference.blocks.len() > 1);
        assert!(table.bytes.ends_with(&table.reference.id));
        assert!(table.bytes.len() >= TABLE_TRAILER_LENGTH);

        let operator = memory();
        operator.write("table", table.bytes).await.unwrap();
        let decoded = fetch(&operator, "table", [7; 16], table.reference.blocks, "test")
            .await
            .unwrap();
        assert_eq!(
            decoded
                .iter()
                .map(|(_, records)| records.len())
                .sum::<usize>(),
            80
        );
    }

    #[tokio::test]
    async fn corrupt_selected_block_fails_before_records_are_returned() {
        let operator = memory();
        let table = build_with_policy(
            [1; 16],
            vec![Record {
                key: b"a".to_vec(),
                value: b"value".to_vec(),
            }],
            small_policy(),
            "test",
        )
        .unwrap()
        .unwrap();
        let mut bytes = table.bytes;
        bytes[BLOCK_HEADER_LENGTH] ^= 1;
        operator.write("table", bytes).await.unwrap();
        assert!(
            fetch(&operator, "table", [1; 16], table.reference.blocks, "test")
                .await
                .is_err()
        );
    }
}
