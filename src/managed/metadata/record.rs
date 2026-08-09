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

//! Native revision-CAS record operations shared by Managed authorities.

use opendal::Operator;
use serde_json::Value;

use crate::filesystem::VolumeId;
use crate::managed::metadata::d1::{D1Result, D1Session, D1Statement, statement};
use crate::managed::metadata::object;
use crate::managed::{D1Metadata, ManagedError, ManagedErrorKind};

const RECORDS: &str = "ofs_managed_v1_records";
#[cfg(feature = "managed-branch")]
const DELETE_BATCH: usize = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Revision {
    Object(String),
    D1(u64),
}

#[derive(Clone)]
pub(crate) enum RecordBackend {
    Object(ObjectBackend),
    D1(D1Backend),
}

#[derive(Clone)]
pub(crate) struct ObjectBackend {
    operator: Operator,
}

impl ObjectBackend {
    const fn new(operator: Operator) -> Self {
        Self { operator }
    }

    async fn read(
        &self,
        key: &str,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, String)>, ManagedError> {
        object::read_with_revision(&self.operator, key, action).await
    }

    async fn read_bytes(
        &self,
        key: &str,
        action: &'static str,
    ) -> Result<Option<Vec<u8>>, ManagedError> {
        object::read(&self.operator, key, action).await
    }

    async fn create(
        &self,
        key: &str,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        object::create(&self.operator, key, bytes, action).await
    }

    async fn replace(
        &self,
        key: &str,
        revision: &str,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        object::replace(&self.operator, key, revision, bytes, action).await
    }
    #[cfg(feature = "managed-branch")]
    async fn list(&self, prefix: &str, action: &'static str) -> Result<Vec<String>, ManagedError> {
        self.operator
            .list_with(prefix)
            .recursive(true)
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .filter(|entry| entry.metadata().is_file())
                    .map(|entry| entry.path().to_owned())
                    .collect()
            })
            .map_err(|_| unavailable(action))
    }

    #[cfg(feature = "managed-branch")]
    async fn delete(&self, keys: Vec<String>, action: &'static str) -> Result<(), ManagedError> {
        self.operator
            .delete_iter(keys.iter().map(String::as_str))
            .await
            .map_err(|_| unavailable(action))
    }
}

#[derive(Clone)]
pub(crate) struct D1Backend {
    session: D1Session,
    volume: String,
}

impl D1Backend {
    fn new(volume_id: VolumeId, metadata: D1Metadata) -> Self {
        Self {
            session: metadata.session(),
            volume: hex(volume_id.as_bytes()),
        }
    }
    async fn read(
        &self,
        key: &str,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, u64)>, ManagedError> {
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
            _ => Err(corrupt(action, "D1 returned duplicate Managed records")),
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
        revision: &u64,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        let revision = i64::try_from(*revision)
            .map_err(|_| corrupt(action, "D1 record revision is invalid"))?;
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
    #[cfg(feature = "managed-branch")]
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

    #[cfg(feature = "managed-branch")]
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
    fn params(&self, key: &str) -> Vec<Value> {
        vec![
            self.session.store_key().to_owned().into(),
            self.volume.clone().into(),
            key.to_owned().into(),
        ]
    }
}

impl RecordBackend {
    pub(crate) fn object(operator: Operator, action: &'static str) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.read
            || !capability.write
            || !capability.write_with_if_not_exists
            || !capability.write_with_if_match
        {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                action,
                "object metadata requires read, create-only write, and conditional replace",
            ));
        }
        Ok(Self::Object(ObjectBackend::new(operator)))
    }

    pub(crate) fn d1(volume_id: VolumeId, metadata: D1Metadata) -> Self {
        Self::D1(D1Backend::new(volume_id, metadata))
    }

    #[cfg(test)]
    pub(crate) const fn test_object(operator: Operator) -> Self {
        Self::Object(ObjectBackend::new(operator))
    }

    pub(crate) async fn read(
        &self,
        key: &str,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, Revision)>, ManagedError> {
        match self {
            Self::Object(backend) => backend
                .read(key, action)
                .await
                .map(|value| value.map(|(bytes, revision)| (bytes, Revision::Object(revision)))),
            Self::D1(backend) => backend
                .read(key, action)
                .await
                .map(|value| value.map(|(bytes, revision)| (bytes, Revision::D1(revision)))),
        }
    }

    pub(crate) async fn read_bytes(
        &self,
        key: &str,
        action: &'static str,
    ) -> Result<Option<Vec<u8>>, ManagedError> {
        match self {
            Self::Object(backend) => backend.read_bytes(key, action).await,
            Self::D1(backend) => backend.read_bytes(key, action).await,
        }
    }

    pub(crate) async fn create(
        &self,
        key: &str,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        match self {
            Self::Object(backend) => backend.create(key, bytes, action).await,
            Self::D1(backend) => backend.create(key, bytes, action).await,
        }
    }

    pub(crate) async fn replace(
        &self,
        key: &str,
        revision: &Revision,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, ManagedError> {
        match (self, revision) {
            (Self::Object(backend), Revision::Object(revision)) => {
                backend.replace(key, revision, bytes, action).await
            }
            (Self::D1(backend), Revision::D1(revision)) => {
                backend.replace(key, revision, bytes, action).await
            }
            _ => Err(corrupt(
                action,
                "record revision belongs to another backend",
            )),
        }
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) async fn list(
        &self,
        prefix: &str,
        action: &'static str,
    ) -> Result<Vec<String>, ManagedError> {
        match self {
            Self::Object(backend) => backend.list(prefix, action).await,
            Self::D1(backend) => backend.list(prefix, action).await,
        }
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) async fn delete(
        &self,
        keys: Vec<String>,
        action: &'static str,
    ) -> Result<(), ManagedError> {
        match self {
            Self::Object(backend) => backend.delete(keys, action).await,
            Self::D1(backend) => backend.delete(keys, action).await,
        }
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
        _ => Err(corrupt(action, "D1 changed duplicate Managed records")),
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
        .ok_or_else(|| corrupt(action, "D1 omitted a Managed query result"))
}

fn text<'a>(row: &'a Value, field: &str, action: &'static str) -> Result<&'a str, ManagedError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid Managed record"))
}

fn integer(row: &Value, field: &str, action: &'static str) -> Result<u64, ManagedError> {
    row.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid Managed revision"))
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
        return Err(corrupt(action, "D1 returned an invalid Managed value"));
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
        .ok_or_else(|| corrupt(action, "D1 returned an invalid Managed value"))
}

fn digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(feature = "managed-branch")]
fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

#[cfg(feature = "managed-branch")]
fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "Managed record storage is unavailable",
    )
}
