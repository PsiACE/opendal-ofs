// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! D1 implementation of the Branch authority's native record operations.

use serde_json::Value;

use super::object::{BranchBackend, BranchStore};
use crate::filesystem::VolumeId;
use crate::managed::metadata::d1::{D1Result, D1Session, D1Statement, statement};
use crate::managed::{D1Metadata, ManagedError, ManagedErrorKind};

const RECORDS: &str = "ofs_managed_branch_v1_records";
const DELETE_BATCH: usize = 100;

#[derive(Clone)]
pub struct D1BranchBackend {
    session: D1Session,
    volume: String,
}

pub type D1BranchStore = BranchStore<D1BranchBackend>;

impl BranchStore<D1BranchBackend> {
    pub fn new(volume_id: VolumeId, metadata: D1Metadata) -> Self {
        Self {
            volume_id,
            backend: D1BranchBackend {
                session: metadata.session(),
                volume: hex(volume_id.as_bytes()),
            },
        }
    }
}

impl BranchBackend for D1BranchBackend {
    type Revision = u64;

    async fn read(
        &self,
        key: &str,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, Self::Revision)>, ManagedError> {
        let results = self
            .session
            .query(
                vec![
                    schema(),
                    statement(
                        format!(
                            "SELECT value_hex, revision FROM {RECORDS} WHERE store_key = ? AND volume_id = ? AND record_key = ?"
                        ),
                        self.params(key),
                    ),
                ],
                action,
            )
            .await?;
        match rows(&results, 1, action)? {
            [] => Ok(None),
            [row] => Ok(Some((
                decode_hex(text(row, "value_hex", action)?, action)?,
                integer(row, "revision", action)?,
            ))),
            _ => Err(corrupt(action, "D1 returned duplicate branch records")),
        }
    }

    async fn read_bytes(
        &self,
        key: &str,
        action: &'static str,
    ) -> Result<Option<Vec<u8>>, ManagedError> {
        self.read(key, action)
            .await
            .map(|value| value.map(|(bytes, _)| bytes))
    }

    async fn create(
        &self,
        key: &str,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        let mut params = self.params(key);
        params.push(hex(&bytes).into());
        let results = self
            .session
            .query(
                vec![
                    schema(),
                    statement(
                        format!(
                            "INSERT OR IGNORE INTO {RECORDS} (store_key, volume_id, record_key, revision, value_hex) VALUES (?, ?, ?, 1, ?) RETURNING revision"
                        ),
                        params,
                    ),
                ],
                action,
            )
            .await?;
        changed(&results, action)
    }

    async fn replace(
        &self,
        key: &str,
        revision: &Self::Revision,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        let revision = i64::try_from(*revision)
            .map_err(|_| corrupt(action, "D1 branch revision is invalid"))?;
        let mut params = vec![hex(&bytes).into()];
        params.extend(self.params(key));
        params.push(revision.into());
        let results = self
            .session
            .query(
                vec![
                    schema(),
                    statement(
                        format!(
                            "UPDATE {RECORDS} SET revision = revision + 1, value_hex = ? WHERE store_key = ? AND volume_id = ? AND record_key = ? AND revision = ? RETURNING revision"
                        ),
                        params,
                    ),
                ],
                action,
            )
            .await?;
        changed(&results, action)
    }

    async fn list(&self, prefix: &str, action: &'static str) -> Result<Vec<String>, ManagedError> {
        let results = self
            .session
            .query(
                vec![
                    schema(),
                    statement(
                        format!(
                            "SELECT record_key FROM {RECORDS} WHERE store_key = ? AND volume_id = ? AND record_key LIKE ? ESCAPE '\\' ORDER BY record_key"
                        ),
                        vec![
                            self.session.store_key().to_owned().into(),
                            self.volume.clone().into(),
                            format!("{}%", escape_like(prefix)).into(),
                        ],
                    ),
                ],
                action,
            )
            .await?;
        rows(&results, 1, action)?
            .iter()
            .map(|row| text(row, "record_key", action).map(ToOwned::to_owned))
            .collect()
    }

    async fn delete(&self, keys: Vec<String>, action: &'static str) -> Result<(), ManagedError> {
        for keys in keys.chunks(DELETE_BATCH) {
            let placeholders = std::iter::repeat_n("?", keys.len())
                .collect::<Vec<_>>()
                .join(", ");
            let mut params = vec![
                self.session.store_key().to_owned().into(),
                self.volume.clone().into(),
            ];
            params.extend(keys.iter().cloned().map(Value::from));
            self.session
                .query(
                    vec![
                        schema(),
                        statement(
                            format!(
                                "DELETE FROM {RECORDS} WHERE store_key = ? AND volume_id = ? AND record_key IN ({placeholders})"
                            ),
                            params,
                        ),
                    ],
                    action,
                )
                .await?;
        }
        Ok(())
    }
}

impl D1BranchBackend {
    fn params(&self, key: &str) -> Vec<Value> {
        vec![
            self.session.store_key().to_owned().into(),
            self.volume.clone().into(),
            key.to_owned().into(),
        ]
    }
}

fn schema() -> D1Statement {
    statement(
        format!(
            "CREATE TABLE IF NOT EXISTS {RECORDS} (store_key TEXT NOT NULL, volume_id TEXT NOT NULL, record_key TEXT NOT NULL, revision INTEGER NOT NULL, value_hex TEXT NOT NULL, PRIMARY KEY (store_key, volume_id, record_key))"
        ),
        Vec::new(),
    )
}

fn changed(results: &[D1Result], action: &'static str) -> Result<bool, ManagedError> {
    match rows(results, 1, action)? {
        [] => Ok(false),
        [_] => Ok(true),
        _ => Err(corrupt(action, "D1 changed duplicate branch records")),
    }
}

fn rows<'a>(
    results: &'a [D1Result],
    index: usize,
    action: &'static str,
) -> Result<&'a [Value], ManagedError> {
    results
        .get(index)
        .map(|result| result.results.as_slice())
        .ok_or_else(|| corrupt(action, "D1 omitted a branch query result"))
}

fn text<'a>(row: &'a Value, field: &str, action: &'static str) -> Result<&'a str, ManagedError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid branch record"))
}

fn integer(row: &Value, field: &str, action: &'static str) -> Result<u64, ManagedError> {
    row.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid branch revision"))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(value: &str, action: &'static str) -> Result<Vec<u8>, ManagedError> {
    if value.len() % 2 != 0 {
        return Err(corrupt(action, "D1 returned an invalid branch value"));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = digit(pair[0])?;
            let low = digit(pair[1])?;
            Some((high << 4) | low)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| corrupt(action, "D1 returned an invalid branch value"))
}

fn digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}
