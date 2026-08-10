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

//! Shared durable references and physical storage containers.

use std::io::Cursor;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub(crate) enum RecordEncodeError {
    Encode,
    TooLarge,
}

pub(crate) enum RecordDecodeError {
    Envelope,
    Checksum,
    Decode,
    TrailingBytes,
}

impl RecordEncodeError {
    pub(crate) const fn message(&self) -> &'static str {
        match self {
            Self::Encode => "record cannot be encoded",
            Self::TooLarge => "record exceeds its size limit",
        }
    }
}

impl RecordDecodeError {
    pub(crate) const fn message(&self) -> &'static str {
        match self {
            Self::Envelope => "record format is invalid",
            Self::Checksum => "record checksum is invalid",
            Self::Decode => "record cannot be decoded",
            Self::TrailingBytes => "record has trailing bytes",
        }
    }
}

/// The stable `magic || CBOR || SHA-256` envelope used by Managed v1 records.
pub(crate) struct V1Record {
    magic: [u8; 8],
    maximum_body_bytes: usize,
}

/// A typed `magic || decoded length || zstd(CBOR) || optional SHA-256`
/// envelope used by bounded Managed namespace records.
#[derive(Clone, Copy)]
enum LengthEncoding {
    U32,
    U64,
}

pub(crate) struct CompressedRecord {
    magic: [u8; 8],
    maximum_decoded_bytes: usize,
    maximum_encoded_bytes: usize,
    length: LengthEncoding,
    checksum: bool,
}

impl CompressedRecord {
    pub(crate) const fn with_u32_length(
        magic: [u8; 8],
        maximum_decoded_bytes: usize,
        maximum_encoded_bytes: usize,
        checksum: bool,
    ) -> Self {
        Self {
            magic,
            maximum_decoded_bytes,
            maximum_encoded_bytes,
            length: LengthEncoding::U32,
            checksum,
        }
    }

    pub(crate) const fn with_u64_length(
        magic: [u8; 8],
        maximum_decoded_bytes: usize,
        maximum_encoded_bytes: usize,
        checksum: bool,
    ) -> Self {
        Self {
            magic,
            maximum_decoded_bytes,
            maximum_encoded_bytes,
            length: LengthEncoding::U64,
            checksum,
        }
    }

    pub(crate) fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, RecordEncodeError> {
        let mut body = Vec::new();
        ciborium::into_writer(value, &mut body).map_err(|_| RecordEncodeError::Encode)?;
        if body.len() > self.maximum_decoded_bytes {
            return Err(RecordEncodeError::TooLarge);
        }
        let length = match self.length {
            LengthEncoding::U32 => u32::try_from(body.len())
                .map(u32::to_be_bytes)
                .map(Vec::from),
            LengthEncoding::U64 => u64::try_from(body.len())
                .map(u64::to_be_bytes)
                .map(Vec::from),
        }
        .map_err(|_| RecordEncodeError::TooLarge)?;
        let compressed = zstd::bulk::compress(&body, 3).map_err(|_| RecordEncodeError::Encode)?;
        let encoded_length = self
            .magic
            .len()
            .saturating_add(length.len())
            .saturating_add(compressed.len())
            .saturating_add(if self.checksum { 32 } else { 0 });
        if encoded_length > self.maximum_encoded_bytes {
            return Err(RecordEncodeError::TooLarge);
        }
        let mut bytes = Vec::with_capacity(encoded_length);
        bytes.extend_from_slice(&self.magic);
        bytes.extend_from_slice(&length);
        bytes.extend_from_slice(&compressed);
        if self.checksum {
            bytes.extend_from_slice(&Sha256::digest(&bytes));
        }
        Ok(bytes)
    }

    pub(crate) fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, RecordDecodeError> {
        if bytes.len() > self.maximum_encoded_bytes {
            return Err(RecordDecodeError::Envelope);
        }
        let payload = bytes
            .strip_prefix(&self.magic)
            .ok_or(RecordDecodeError::Envelope)?;
        let encoded = if self.checksum {
            let body = payload
                .get(
                    ..payload
                        .len()
                        .checked_sub(32)
                        .ok_or(RecordDecodeError::Envelope)?,
                )
                .ok_or(RecordDecodeError::Envelope)?;
            if Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != &bytes[bytes.len() - 32..] {
                return Err(RecordDecodeError::Checksum);
            }
            body
        } else {
            payload
        };
        let (decoded_length, compressed) = match self.length {
            LengthEncoding::U32 => {
                let (length, compressed) = encoded
                    .split_first_chunk::<4>()
                    .ok_or(RecordDecodeError::Envelope)?;
                (u32::from_be_bytes(*length) as usize, compressed)
            }
            LengthEncoding::U64 => {
                let (length, compressed) = encoded
                    .split_first_chunk::<8>()
                    .ok_or(RecordDecodeError::Envelope)?;
                let length = usize::try_from(u64::from_be_bytes(*length))
                    .map_err(|_| RecordDecodeError::Envelope)?;
                (length, compressed)
            }
        };
        if decoded_length > self.maximum_decoded_bytes {
            return Err(RecordDecodeError::Envelope);
        }
        let body = zstd::bulk::decompress(compressed, decoded_length)
            .map_err(|_| RecordDecodeError::Decode)?;
        if body.len() != decoded_length {
            return Err(RecordDecodeError::Envelope);
        }
        let mut input = Cursor::new(body);
        let value = ciborium::from_reader(&mut input).map_err(|_| RecordDecodeError::Decode)?;
        if input.position() != decoded_length as u64 {
            return Err(RecordDecodeError::TrailingBytes);
        }
        Ok(value)
    }
}

impl V1Record {
    pub(crate) const fn new(magic: [u8; 8], maximum_body_bytes: usize) -> Self {
        Self {
            magic,
            maximum_body_bytes,
        }
    }

    pub(crate) const fn maximum_encoded_bytes(&self) -> usize {
        self.magic
            .len()
            .saturating_add(self.maximum_body_bytes)
            .saturating_add(32)
    }

    pub(crate) fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, RecordEncodeError> {
        let mut body = Vec::new();
        ciborium::into_writer(value, &mut body).map_err(|_| RecordEncodeError::Encode)?;
        if body.len() > self.maximum_body_bytes {
            return Err(RecordEncodeError::TooLarge);
        }
        let mut bytes = Vec::with_capacity(self.magic.len() + body.len() + 32);
        bytes.extend_from_slice(&self.magic);
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(&Sha256::digest(&bytes));
        Ok(bytes)
    }

    pub(crate) fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, RecordDecodeError> {
        let body = bytes
            .strip_prefix(&self.magic)
            .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
            .ok_or(RecordDecodeError::Envelope)?;
        if body.len() > self.maximum_body_bytes
            || Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != &bytes[bytes.len() - 32..]
        {
            return Err(RecordDecodeError::Checksum);
        }
        let mut input = Cursor::new(body);
        let value = ciborium::from_reader(&mut input).map_err(|_| RecordDecodeError::Decode)?;
        if input.position() != body.len() as u64 {
            return Err(RecordDecodeError::TrailingBytes);
        }
        Ok(value)
    }
}

/// Canonical lowercase hexadecimal encoding used by Managed v1 durable identities.
pub(crate) struct LowerHex;

impl LowerHex {
    pub(crate) fn encode(bytes: &[u8]) -> String {
        hex::encode(bytes)
    }

    pub(crate) fn decode(value: &str) -> Option<Vec<u8>> {
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return None;
        }
        hex::decode(value).ok()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContentRef {
    pub(crate) digest: [u8; 32],
    pub(crate) length: u64,
}

/// Identity and physical length of one immutable data segment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SegmentRef {
    pub(crate) digest: [u8; 32],
    pub(crate) length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExtentMap {
    pub(crate) extents: Vec<Extent>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Extent {
    pub(crate) content: ContentRef,
    pub(crate) segment: SegmentRef,
    pub(crate) segment_offset: u64,
}
