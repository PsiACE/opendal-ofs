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

//! D1 commit point for the shared Managed namespace implementation.

use opendal::Operator;
use serde_json::Value;

use super::object::{NamespaceHeadBackend, NamespaceObservation, NamespaceStore};
use crate::filesystem::VolumeId;
use crate::managed::metadata::d1::{D1Result, D1Session, D1Statement, statement};
use crate::managed::{ManagedError, ManagedErrorKind};

const HEADS: &str = "ofs_managed_v1_namespace_heads";

#[derive(Clone)]
pub(crate) struct D1HeadBackend {
    session: D1Session,
    volume: String,
}

pub(crate) type D1Namespace = NamespaceStore<D1HeadBackend>;
pub(crate) type D1NamespaceObservation = NamespaceObservation<u64>;

impl NamespaceStore<D1HeadBackend> {
    pub(crate) fn new(volume_id: VolumeId, operator: Operator, session: D1Session) -> Self {
        Self {
            volume_id,
            operator,
            backend: D1HeadBackend {
                session,
                volume: hex(volume_id.as_bytes()),
            },
        }
    }
}

impl NamespaceHeadBackend for D1HeadBackend {
    type Revision = u64;

    async fn read(
        &self,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, Self::Revision)>, ManagedError> {
        let results = self
            .session
            .query(
                vec![
                    schema(),
                    statement(
                        format!(
                            "SELECT value_hex, revision FROM {HEADS} WHERE store_key = ? AND volume_id = ?"
                        ),
                        self.params(),
                    ),
                ],
                action,
            )
            .await?;
        match rows(&results, action)? {
            [] => Ok(None),
            [row] => Ok(Some((
                decode_hex(text(row, "value_hex", action)?, action)?,
                integer(row, "revision", action)?,
            ))),
            _ => Err(corrupt(action, "D1 returned duplicate namespace HEADs")),
        }
    }

    async fn read_bytes(&self, action: &'static str) -> Result<Option<Vec<u8>>, ManagedError> {
        self.read(action)
            .await
            .map(|value| value.map(|(bytes, _)| bytes))
    }

    async fn create(&self, bytes: Vec<u8>, action: &'static str) -> Result<bool, ManagedError> {
        let mut params = self.params();
        params.push(hex(&bytes).into());
        let results = self
            .session
            .query(
                vec![
                    schema(),
                    statement(
                        format!(
                            "INSERT OR IGNORE INTO {HEADS} (store_key, volume_id, revision, value_hex) VALUES (?, ?, 1, ?) RETURNING revision"
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
        revision: &Self::Revision,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        let revision = i64::try_from(*revision)
            .map_err(|_| corrupt(action, "D1 namespace revision is invalid"))?;
        let mut params = vec![hex(&bytes).into()];
        params.extend(self.params());
        params.push(revision.into());
        let results = self
            .session
            .query(
                vec![
                    schema(),
                    statement(
                        format!(
                            "UPDATE {HEADS} SET revision = revision + 1, value_hex = ? WHERE store_key = ? AND volume_id = ? AND revision = ? RETURNING revision"
                        ),
                        params,
                    ),
                ],
                action,
            )
            .await?;
        changed(&results, action)
    }
}

impl D1HeadBackend {
    fn params(&self) -> Vec<Value> {
        vec![
            self.session.store_key().to_owned().into(),
            self.volume.clone().into(),
        ]
    }
}

fn schema() -> D1Statement {
    statement(
        format!(
            "CREATE TABLE IF NOT EXISTS {HEADS} (store_key TEXT NOT NULL, volume_id TEXT NOT NULL, revision INTEGER NOT NULL, value_hex TEXT NOT NULL, PRIMARY KEY (store_key, volume_id))"
        ),
        Vec::new(),
    )
}

fn changed(results: &[D1Result], action: &'static str) -> Result<bool, ManagedError> {
    match rows(results, action)? {
        [] => Ok(false),
        [_] => Ok(true),
        _ => Err(corrupt(action, "D1 changed duplicate namespace HEADs")),
    }
}

fn rows<'a>(results: &'a [D1Result], action: &'static str) -> Result<&'a [Value], ManagedError> {
    results
        .get(1)
        .map(|result| result.results.as_slice())
        .ok_or_else(|| corrupt(action, "D1 omitted a namespace query result"))
}

fn text<'a>(row: &'a Value, field: &str, action: &'static str) -> Result<&'a str, ManagedError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid namespace HEAD"))
}

fn integer(row: &Value, field: &str, action: &'static str) -> Result<u64, ManagedError> {
    row.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid namespace revision"))
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
        return Err(corrupt(action, "D1 returned an invalid namespace HEAD"));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Some((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| corrupt(action, "D1 returned an invalid namespace HEAD"))
}

fn digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}
