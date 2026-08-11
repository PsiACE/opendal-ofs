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

//! Bounded immutable containers for independently readable metadata sections.

use opendal::{Buffer, ErrorKind, Operator};
use serde::{Deserialize, Serialize};

use crate::filesystem::{Checksum, Digest, VolumeError};

use super::error::{corrupt, invalid, unavailable};
use super::object;

const FOOTER_MAGIC: [u8; 8] = *b"OFSMETA1";
const FOOTER_BYTES: usize = 56;
const TARGET_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SectionRef {
    pub(crate) object: Digest,
    pub(crate) object_length: u64,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) checksum: Checksum,
    pub(crate) section_type: u8,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SectionRecord {
    offset: u64,
    length: u64,
    checksum: Checksum,
    section_type: u8,
}

struct Pending<T> {
    bytes: Vec<u8>,
    value: T,
    record: SectionRecord,
}

pub(crate) struct ContainerWriter<'a, T> {
    operator: &'a Operator,
    pending: Vec<Pending<T>>,
    bytes: usize,
}

impl<'a, T> ContainerWriter<'a, T> {
    pub(crate) const fn new(operator: &'a Operator) -> Self {
        Self {
            operator,
            pending: Vec::new(),
            bytes: 0,
        }
    }

    pub(crate) async fn push(
        &mut self,
        section_type: u8,
        bytes: Vec<u8>,
        value: T,
    ) -> Result<Vec<(T, SectionRef)>, VolumeError> {
        let mut completed = Vec::new();
        if !self.pending.is_empty() && self.bytes.saturating_add(bytes.len()) > TARGET_BYTES {
            completed = self.flush().await?;
        }
        let offset = u64::try_from(self.bytes)
            .map_err(|_| invalid("write Managed metadata", "container offset overflows"))?;
        let length = u64::try_from(bytes.len())
            .map_err(|_| invalid("write Managed metadata", "section length overflows"))?;
        self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(|| {
            invalid(
                "write Managed metadata",
                "metadata container length overflows",
            )
        })?;
        self.pending.push(Pending {
            record: SectionRecord {
                offset,
                length,
                checksum: Checksum::from_bytes(blake3::hash(&bytes).into()),
                section_type,
            },
            bytes,
            value,
        });
        Ok(completed)
    }

    pub(crate) async fn finish(mut self) -> Result<Vec<(T, SectionRef)>, VolumeError> {
        self.flush().await
    }

    async fn flush(&mut self) -> Result<Vec<(T, SectionRef)>, VolumeError> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let mut bytes = Vec::with_capacity(self.bytes.saturating_add(FOOTER_BYTES + 4096));
        let mut records = Vec::with_capacity(self.pending.len());
        for pending in &self.pending {
            bytes.extend_from_slice(&pending.bytes);
            records.push(&pending.record);
        }
        let directory_offset = bytes.len();
        ciborium::into_writer(&records, &mut bytes).map_err(|_| {
            invalid(
                "write Managed metadata",
                "container directory cannot be encoded",
            )
        })?;
        let directory_length = bytes.len() - directory_offset;
        let directory_checksum = blake3::hash(&bytes[directory_offset..]);
        bytes.extend_from_slice(&FOOTER_MAGIC);
        bytes.extend_from_slice(&(directory_offset as u64).to_be_bytes());
        bytes.extend_from_slice(&(directory_length as u64).to_be_bytes());
        bytes.extend_from_slice(directory_checksum.as_bytes());
        if bytes.len() > MAXIMUM_BYTES {
            return Err(invalid(
                "write Managed metadata",
                "one metadata container exceeds its size bound",
            ));
        }
        let object = Digest::from_bytes(blake3::hash(&bytes).into());
        let object_length = bytes.len() as u64;
        object::create_immutable(self.operator, &container_key(object), Buffer::from(bytes))
            .await?;
        self.bytes = 0;
        Ok(self
            .pending
            .drain(..)
            .map(|pending| {
                let reference = SectionRef {
                    object,
                    object_length,
                    offset: pending.record.offset,
                    length: pending.record.length,
                    checksum: pending.record.checksum,
                    section_type: pending.record.section_type,
                };
                (pending.value, reference)
            })
            .collect())
    }
}

pub(crate) async fn read_section(
    operator: &Operator,
    reference: SectionRef,
) -> Result<Vec<u8>, VolumeError> {
    let end = reference
        .offset
        .checked_add(reference.length)
        .filter(|end| *end <= reference.object_length.saturating_sub(FOOTER_BYTES as u64))
        .ok_or_else(|| corrupt("read Managed metadata", "section range is invalid"))?;
    let reader = match operator.reader(&container_key(reference.object)).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(corrupt(
                "read Managed metadata",
                "referenced container is missing",
            ));
        }
        Err(_) => {
            return Err(unavailable(
                "read Managed metadata",
                "object storage is unavailable",
            ));
        }
    };
    let buffer = reader
        .read(reference.offset..end)
        .await
        .map_err(|_| unavailable("read Managed metadata", "object storage is unavailable"))?;
    let bytes = buffer.to_vec();
    if bytes.len() as u64 != reference.length
        || blake3::hash(&bytes).as_bytes() != reference.checksum.as_bytes()
    {
        return Err(corrupt(
            "read Managed metadata",
            "section does not match its reference",
        ));
    }
    Ok(bytes)
}

fn container_key(digest: Digest) -> String {
    let digest = blake3::Hash::from_bytes(*digest.as_bytes()).to_hex();
    format!(
        "managed/1/objects/meta/{}/{}",
        &digest.as_str()[..2],
        digest
    )
}
