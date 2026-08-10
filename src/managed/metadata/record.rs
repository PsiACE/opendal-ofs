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

use crate::filesystem::VolumeError;
use crate::managed::error::{corrupt, invalid, unavailable};
use crate::managed::format::LowerHex;
use crate::managed::metadata::d1::{D1Result, D1Session, D1Statement};
use crate::managed::metadata::object;
use crate::managed::{D1Config, MetadataFormat};

const RECORDS: &str = "ofs_managed_v1_authority_records";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Revision {
    Object(String),
    D1(u64),
}

#[derive(Clone)]
pub(crate) enum RecordBackend {
    Object(Operator),
    D1(D1Backend),
}

#[derive(Clone)]
pub(crate) struct D1Backend {
    session: D1Session,
}

impl D1Backend {
    fn new(config: D1Config) -> Result<Self, VolumeError> {
        D1Session::new(config).map(|session| Self { session })
    }
    async fn read(
        &self,
        key: &str,
        maximum_bytes: usize,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, u64)>, VolumeError> {
        let results = self
            .session
            .query(
                vec![D1Statement {
                    sql: format!(
                        "SELECT value_hex, revision FROM {RECORDS} WHERE store_key = ? AND record_key = ?"
                    ),
                    params: self.params(key),
                }],
                action,
            )
            .await?;
        match results[0].results.as_slice() {
            [] => Ok(None),
            [row] => {
                let value = row
                    .value_hex
                    .as_deref()
                    .ok_or_else(|| corrupt(action, "D1 returned an invalid Managed record"))?;
                let revision = row
                    .revision
                    .ok_or_else(|| corrupt(action, "D1 returned an invalid Managed revision"))?;
                Ok(Some((decode_hex(value, maximum_bytes, action)?, revision)))
            }
            _ => Err(corrupt(action, "D1 returned duplicate Managed records")),
        }
    }

    async fn create(
        &self,
        key: &str,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, VolumeError> {
        let mut params = self.params(key);
        params.push(LowerHex::encode(&bytes).into());
        let results = self
            .session
            .query(
                vec![D1Statement {
                    sql: format!(
                        "INSERT OR IGNORE INTO {RECORDS} (store_key, record_key, revision, value_hex) VALUES (?, ?, 1, ?) RETURNING revision"
                    ),
                    params,
                }],
                action,
            )
            .await?;
        changed(&results[0], action)
    }

    async fn create_or_read(
        &self,
        key: &str,
        bytes: Vec<u8>,
        maximum_bytes: usize,
        action: &'static str,
    ) -> Result<Vec<u8>, VolumeError> {
        let mut params = self.params(key);
        params.push(LowerHex::encode(&bytes).into());
        let results = self
            .session
            .query(
                vec![
                    schema(),
                    D1Statement {
                        sql: format!(
                            "INSERT OR IGNORE INTO {RECORDS} (store_key, record_key, revision, value_hex) VALUES (?, ?, 1, ?) RETURNING value_hex"
                        ),
                        params,
                    },
                    D1Statement {
                        sql: format!(
                            "SELECT value_hex FROM {RECORDS} WHERE store_key = ? AND record_key = ?"
                        ),
                        params: self.params(key),
                    },
                ],
                action,
            )
            .await?;
        match results[1].results.as_slice() {
            [] => {}
            [row] if row.value_hex.is_some() => {}
            [_] => return Err(corrupt(action, "D1 returned an invalid Managed record")),
            _ => return Err(corrupt(action, "D1 changed duplicate Managed records")),
        }
        let [row] = results[2].results.as_slice() else {
            return Err(corrupt(action, "D1 returned an invalid Managed record"));
        };
        let value = row
            .value_hex
            .as_deref()
            .ok_or_else(|| corrupt(action, "D1 returned an invalid Managed record"))?;
        decode_hex(value, maximum_bytes, action)
    }

    async fn replace(
        &self,
        key: &str,
        revision: &u64,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, VolumeError> {
        let revision = i64::try_from(*revision)
            .map_err(|_| corrupt(action, "D1 record revision is invalid"))?;
        let mut params = vec![LowerHex::encode(&bytes).into()];
        params.extend(self.params(key));
        params.push(revision.into());
        let results = self
            .session
            .query(
                vec![D1Statement {
                    sql: format!(
                        "UPDATE {RECORDS} SET revision = revision + 1, value_hex = ? WHERE store_key = ? AND record_key = ? AND revision = ? RETURNING revision"
                    ),
                    params,
                }],
                action,
            )
            .await?;
        changed(&results[0], action)
    }
    fn params(&self, key: &str) -> Vec<Value> {
        vec![
            self.session.store_key().to_owned().into(),
            key.to_owned().into(),
        ]
    }
}

impl RecordBackend {
    pub(crate) fn object(operator: Operator, action: &'static str) -> Result<Self, VolumeError> {
        let capability = operator.info().full_capability();
        let supported = capability.read
            && capability.write
            && capability.write_with_if_not_exists
            && capability.write_with_if_match;
        if !supported {
            return Err(invalid(
                action,
                "object metadata lacks required record capabilities",
            ));
        }
        Ok(Self::Object(operator))
    }

    pub(crate) fn d1(config: D1Config) -> Result<Self, VolumeError> {
        D1Backend::new(config).map(Self::D1)
    }

    pub(crate) const fn metadata_format(&self) -> MetadataFormat {
        match self {
            Self::Object(_) => MetadataFormat::ObjectV1,
            Self::D1(_) => MetadataFormat::TransactionalV1,
        }
    }

    pub(crate) async fn read(
        &self,
        key: &str,
        maximum_bytes: usize,
        action: &'static str,
    ) -> Result<Option<(Vec<u8>, Revision)>, VolumeError> {
        match self {
            Self::Object(operator) => {
                object::read_with_revision(operator, key, maximum_bytes, action)
                    .await
                    .map(|value| value.map(|(bytes, revision)| (bytes, Revision::Object(revision))))
            }
            Self::D1(backend) => backend
                .read(key, maximum_bytes, action)
                .await
                .map(|value| value.map(|(bytes, revision)| (bytes, Revision::D1(revision)))),
        }
    }

    pub(crate) async fn create(
        &self,
        key: &str,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, VolumeError> {
        match self {
            Self::Object(operator) => object::create(operator, key, bytes, action).await,
            Self::D1(backend) => backend.create(key, bytes, action).await,
        }
    }

    pub(crate) async fn create_or_read(
        &self,
        key: &str,
        bytes: Vec<u8>,
        maximum_bytes: usize,
        action: &'static str,
    ) -> Result<Vec<u8>, VolumeError> {
        match self {
            Self::Object(operator) => {
                if object::create(operator, key, bytes.clone(), action).await? {
                    return Ok(bytes);
                }
                object::read_with_revision(operator, key, maximum_bytes, action)
                    .await?
                    .map(|(bytes, _)| bytes)
                    .ok_or_else(|| unavailable(action, "Managed record storage is unavailable"))
            }
            Self::D1(backend) => {
                backend
                    .create_or_read(key, bytes, maximum_bytes, action)
                    .await
            }
        }
    }

    pub(crate) async fn replace(
        &self,
        key: &str,
        revision: &Revision,
        bytes: Vec<u8>,
        action: &'static str,
    ) -> Result<bool, VolumeError> {
        match (self, revision) {
            (Self::Object(operator), Revision::Object(revision)) => {
                object::replace(operator, key, revision, bytes, action).await
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
}

fn schema() -> D1Statement {
    D1Statement {
        sql: format!(
            "CREATE TABLE IF NOT EXISTS {RECORDS} (store_key TEXT NOT NULL, record_key TEXT NOT NULL, revision INTEGER NOT NULL, value_hex TEXT NOT NULL, PRIMARY KEY (store_key, record_key))"
        ),
        params: Vec::new(),
    }
}

fn changed(result: &D1Result, action: &'static str) -> Result<bool, VolumeError> {
    match result.results.as_slice() {
        [] => Ok(false),
        [row] if row.revision.is_some() => Ok(true),
        [_] => Err(corrupt(action, "D1 returned an invalid Managed revision")),
        _ => Err(corrupt(action, "D1 changed duplicate Managed records")),
    }
}

fn decode_hex(
    value: &str,
    maximum_bytes: usize,
    action: &'static str,
) -> Result<Vec<u8>, VolumeError> {
    if value.len() > maximum_bytes.saturating_mul(2) {
        return Err(corrupt(action, "D1 Managed value exceeds its size limit"));
    }
    LowerHex::decode(value).ok_or_else(|| corrupt(action, "D1 returned an invalid Managed value"))
}
