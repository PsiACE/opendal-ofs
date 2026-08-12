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

use std::collections::BTreeMap;

use opendal::{Buffer, ErrorKind as StorageErrorKind, Operator};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::{Checksum, Digest};

use super::object;

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

struct Pending<T> {
    bytes: Vec<u8>,
    value: T,
    offset: u64,
    length: u64,
    checksum: Checksum,
    section_type: u8,
}

pub(crate) struct ContainerWriter<'a, T> {
    operator: &'a Operator,
    known: Option<&'a BTreeMap<Digest, u64>>,
    pending: Vec<Pending<T>>,
    bytes: usize,
}

impl<'a, T> ContainerWriter<'a, T> {
    pub(crate) const fn new(operator: &'a Operator) -> Self {
        Self {
            operator,
            known: None,
            pending: Vec::new(),
            bytes: 0,
        }
    }

    pub(crate) const fn reusing(operator: &'a Operator, known: &'a BTreeMap<Digest, u64>) -> Self {
        Self {
            operator,
            known: Some(known),
            pending: Vec::new(),
            bytes: 0,
        }
    }

    pub(crate) async fn push(
        &mut self,
        section_type: u8,
        bytes: Vec<u8>,
        value: T,
    ) -> Result<Vec<(T, SectionRef)>, Error> {
        let mut completed = Vec::new();
        if !self.pending.is_empty() && self.bytes.saturating_add(bytes.len()) > TARGET_BYTES {
            completed = self.flush().await?;
        }
        let offset = u64::try_from(self.bytes)
            .map_err(|_| Error::invalid("write Managed metadata", "container offset overflows"))?;
        let length = u64::try_from(bytes.len())
            .map_err(|_| Error::invalid("write Managed metadata", "section length overflows"))?;
        self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(|| {
            Error::invalid(
                "write Managed metadata",
                "metadata container length overflows",
            )
        })?;
        self.pending.push(Pending {
            offset,
            length,
            checksum: Checksum::from_bytes(blake3::hash(&bytes).into()),
            section_type,
            bytes,
            value,
        });
        Ok(completed)
    }

    pub(crate) async fn finish(mut self) -> Result<Vec<(T, SectionRef)>, Error> {
        self.flush().await
    }

    async fn flush(&mut self) -> Result<Vec<(T, SectionRef)>, Error> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let mut bytes = Vec::with_capacity(self.bytes);
        for pending in &self.pending {
            bytes.extend_from_slice(&pending.bytes);
        }
        if bytes.len() > MAXIMUM_BYTES {
            return Err(Error::invalid(
                "write Managed metadata",
                "one metadata container exceeds its size bound",
            ));
        }
        let object = Digest::from_bytes(blake3::hash(&bytes).into());
        let object_length = bytes.len() as u64;
        match self.known.and_then(|known| known.get(&object)) {
            Some(length) if *length == object_length => {}
            Some(_) => {
                return Err(Error::corrupt(
                    "write Managed metadata",
                    "reused container has a conflicting length",
                ));
            }
            None => {
                object::create_immutable(self.operator, &object_key(object), Buffer::from(bytes))
                    .await?;
            }
        }
        self.bytes = 0;
        Ok(self
            .pending
            .drain(..)
            .map(|pending| {
                let reference = SectionRef {
                    object,
                    object_length,
                    offset: pending.offset,
                    length: pending.length,
                    checksum: pending.checksum,
                    section_type: pending.section_type,
                };
                (pending.value, reference)
            })
            .collect())
    }
}

pub(crate) async fn read_sections(
    operator: &Operator,
    references: &[SectionRef],
) -> Result<Vec<Vec<u8>>, Error> {
    let Some(first) = references.first() else {
        return Ok(Vec::new());
    };
    if first.object_length > MAXIMUM_BYTES as u64
        || references.iter().any(|reference| {
            reference.object != first.object || reference.object_length != first.object_length
        })
    {
        return Err(Error::corrupt(
            "read Managed metadata",
            "container reference is invalid",
        ));
    }
    let ranges = references
        .iter()
        .map(|reference| {
            let end = reference
                .offset
                .checked_add(reference.length)
                .filter(|end| *end <= reference.object_length)
                .ok_or_else(|| {
                    Error::corrupt("read Managed metadata", "section range is invalid")
                })?;
            Ok(reference.offset..end)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let requested_bytes = references.iter().try_fold(0_u64, |total, reference| {
        total.checked_add(reference.length)
    });
    if requested_bytes.is_none_or(|bytes| bytes > first.object_length) {
        return Err(Error::corrupt(
            "read Managed metadata",
            "section ranges exceed their container",
        ));
    }
    let reader = match operator
        .reader_with(&object_key(first.object))
        .content_length_hint(first.object_length)
        .await
    {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => {
            return Err(Error::corrupt(
                "read Managed metadata",
                "referenced container is missing",
            ));
        }
        Err(error) => return Err(Error::from_storage("read Managed metadata", error)),
    };
    let buffers = match reader.fetch(ranges).await {
        Ok(buffers) => buffers,
        Err(error) if error.kind() == StorageErrorKind::NotFound => {
            return Err(Error::corrupt(
                "read Managed metadata",
                "referenced container is missing",
            ));
        }
        Err(error) => return Err(Error::from_storage("read Managed metadata", error)),
    };
    if buffers.len() != references.len() {
        return Err(Error::unavailable(
            "read Managed metadata",
            "object storage returned incomplete section data",
        ));
    }
    let mut sections = Vec::with_capacity(references.len());
    for (reference, buffer) in references.iter().zip(buffers) {
        let bytes = buffer.to_vec();
        if bytes.len() as u64 != reference.length
            || blake3::hash(&bytes).as_bytes() != reference.checksum.as_bytes()
        {
            return Err(Error::corrupt(
                "read Managed metadata",
                "section does not match its reference",
            ));
        }
        sections.push(bytes);
    }
    Ok(sections)
}

pub(super) fn object_key(digest: Digest) -> String {
    let digest = blake3::Hash::from_bytes(*digest.as_bytes()).to_hex();
    format!(
        "managed/1/objects/meta/{}/{}",
        &digest.as_str()[..2],
        digest
    )
}
