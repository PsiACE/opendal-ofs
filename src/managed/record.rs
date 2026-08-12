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

use std::io::Cursor;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;

pub(crate) struct Record {
    magic: [u8; 8],
    maximum_body_bytes: usize,
}

impl Record {
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

    pub(crate) fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Error> {
        let mut body = Vec::new();
        ciborium::into_writer(value, &mut body)
            .map_err(|_| Error::invalid("encode Managed record", "record cannot be encoded"))?;
        if body.len() > self.maximum_body_bytes {
            return Err(Error::invalid(
                "encode Managed record",
                "record exceeds its size limit",
            ));
        }
        let mut bytes = Vec::with_capacity(self.magic.len() + body.len() + 32);
        bytes.extend_from_slice(&self.magic);
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
        Ok(bytes)
    }

    pub(crate) fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Error> {
        let body = bytes
            .strip_prefix(&self.magic)
            .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
            .ok_or_else(|| Error::corrupt("decode Managed record", "record format is invalid"))?;
        if body.len() > self.maximum_body_bytes
            || blake3::hash(&bytes[..bytes.len() - 32]).as_bytes() != &bytes[bytes.len() - 32..]
        {
            return Err(Error::corrupt(
                "decode Managed record",
                "record checksum is invalid",
            ));
        }
        let mut input = Cursor::new(body);
        let value = ciborium::from_reader(&mut input)
            .map_err(|_| Error::corrupt("decode Managed record", "record body is invalid"))?;
        if input.position() != body.len() as u64 {
            return Err(Error::corrupt(
                "decode Managed record",
                "record has trailing bytes",
            ));
        }
        Ok(value)
    }
}
