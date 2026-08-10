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

use serde::{Deserialize, Serialize};

use crate::filesystem::{VolumeError, VolumeId};

use super::error::{corrupt, invalid, unsupported};

pub(crate) const SUPERBLOCK_KEY: &str = ".ofs/managed/superblock.json";
pub(crate) const MAX_SUPERBLOCK_BYTES: usize = 64 * 1024;

const FORMAT: &str = "managed/1";

/// The sole Managed storage format understood by this build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedFormat {
    volume_id: VolumeId,
}

impl ManagedFormat {
    pub const fn v1(volume_id: VolumeId) -> Self {
        Self { volume_id }
    }

    pub const fn volume_id(self) -> VolumeId {
        self.volume_id
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, VolumeError> {
        serde_json::to_vec(&Superblock {
            format: FORMAT.to_owned(),
            volume_id: self.volume_id.to_string(),
        })
        .map_err(|_| invalid("initialize Managed volume", "superblock cannot be encoded"))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, VolumeError> {
        let superblock: Superblock = serde_json::from_slice(bytes)
            .map_err(|_| corrupt("open Managed volume", "superblock is not strict UTF-8 JSON"))?;
        if superblock.format != FORMAT {
            return Err(unsupported(
                "open Managed volume",
                "Managed format is unsupported",
            ));
        }
        Ok(Self {
            volume_id: parse_volume_id(&superblock.volume_id)?,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Superblock {
    format: String,
    volume_id: String,
}

fn parse_volume_id(value: &str) -> Result<VolumeId, VolumeError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(corrupt(
            "open Managed volume",
            "volume identity is not 16-byte lowercase hex",
        ));
    }
    let mut bytes = [0; 16];
    for (output, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *output = (hex_digit(pair[0]) << 4) | hex_digit(pair[1]);
    }
    Ok(VolumeId::from_bytes(bytes))
}

fn hex_digit(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("volume identity was validated"),
    }
}
