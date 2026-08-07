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
use serde_json::Value;

use super::require_same_format;
use crate::managed::{ManagedError, ManagedErrorKind, ManagedFormat};

const FORMAT_TABLE: &str = "ofs_managed_v1_formats";

/// Connection scope for the D1 Query API.
#[derive(Clone)]
pub struct D1Config {
    account_id: String,
    database_id: String,
    store_key: String,
    token: String,
    api_base: String,
}

impl D1Config {
    pub fn new(
        account_id: impl Into<String>,
        database_id: impl Into<String>,
        store_key: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, ManagedError> {
        let config = Self {
            account_id: account_id.into(),
            database_id: database_id.into(),
            store_key: store_key.into(),
            token: token.into(),
            api_base: "https://api.cloudflare.com/client/v4".to_owned(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Result<Self, ManagedError> {
        self.api_base = api_base.into();
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), ManagedError> {
        let complete = [
            &self.account_id,
            &self.database_id,
            &self.store_key,
            &self.token,
            &self.api_base,
        ]
        .into_iter()
        .all(|value| !value.is_empty());
        let safe_scope = [&self.account_id, &self.database_id]
            .into_iter()
            .all(|value| {
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            });
        if !complete || !safe_scope {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "configure D1 metadata",
                "D1 scope is invalid",
            ));
        }
        Ok(())
    }
}

/// Managed metadata stored through the D1 Query API.
#[derive(Clone)]
pub struct D1Metadata {
    session: D1Session,
}

#[derive(Clone)]
pub(crate) struct D1Session {
    client: reqwest::Client,
    config: D1Config,
}

impl D1Metadata {
    pub fn new(config: D1Config) -> Self {
        Self {
            session: D1Session {
                client: reqwest::Client::new(),
                config,
            },
        }
    }

    pub(crate) fn session(&self) -> D1Session {
        self.session.clone()
    }

    pub async fn create_format(
        &self,
        desired: &ManagedFormat,
    ) -> Result<ManagedFormat, ManagedError> {
        desired.validate_for_write()?;
        let record = String::from_utf8(desired.encode()?).map_err(|_| {
            ManagedError::new(
                ManagedErrorKind::Invalid,
                "create Managed format",
                "format is not UTF-8 JSON",
            )
        })?;
        self.session
            .query(
                vec![
            statement(
                format!(
                    "CREATE TABLE IF NOT EXISTS {FORMAT_TABLE} (store_key TEXT PRIMARY KEY, record_json TEXT NOT NULL)"
                ),
                Vec::new(),
            ),
            statement(
                format!(
                    "INSERT OR IGNORE INTO {FORMAT_TABLE} (store_key, record_json) VALUES (?, ?)"
                ),
                vec![self.session.store_key().to_owned().into(), record.into()],
            ),
                ],
                "create Managed format",
            )
            .await?;
        require_same_format(desired, self.read_format().await?)
    }

    pub async fn read_format(&self) -> Result<ManagedFormat, ManagedError> {
        let results = self
            .session
            .query(
                vec![statement(
                    format!("SELECT record_json FROM {FORMAT_TABLE} WHERE store_key = ?"),
                    vec![self.session.store_key().to_owned().into()],
                )],
                "read Managed format",
            )
            .await?;
        let rows = &results
            .first()
            .ok_or_else(|| corrupt("read Managed format", "D1 omitted the query result"))?
            .results;
        let [row] = rows.as_slice() else {
            return Err(if rows.is_empty() {
                unavailable("read Managed format")
            } else {
                corrupt("read Managed format", "D1 returned duplicate formats")
            });
        };
        let record = row
            .get("record_json")
            .and_then(Value::as_str)
            .ok_or_else(|| corrupt("read Managed format", "D1 format row is invalid"))?;
        ManagedFormat::decode(record.as_bytes())
    }
}

impl D1Session {
    pub(crate) fn store_key(&self) -> &str {
        &self.config.store_key
    }

    pub(crate) async fn query(
        &self,
        statements: Vec<D1Statement>,
        action: &'static str,
    ) -> Result<Vec<D1Result>, ManagedError> {
        let endpoint = format!(
            "{}/accounts/{}/d1/database/{}/query",
            self.config.api_base.trim_end_matches('/'),
            self.config.account_id,
            self.config.database_id
        );
        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.config.token)
            .json(&D1Request { batch: statements })
            .send()
            .await
            .map_err(|_| unavailable(action))?;
        if !response.status().is_success() {
            return Err(unavailable(action));
        }
        let reply: D1Reply = response
            .json()
            .await
            .map_err(|_| corrupt(action, "D1 returned an invalid response"))?;
        if !reply.success
            || reply
                .result
                .iter()
                .any(|result| !result.success || result.meta.served_by_primary != Some(true))
        {
            return Err(unavailable(action));
        }
        Ok(reply.result)
    }
}

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "D1 metadata is unavailable",
    )
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

#[derive(Serialize)]
struct D1Request {
    batch: Vec<D1Statement>,
}

#[derive(Serialize)]
pub(crate) struct D1Statement {
    sql: String,
    params: Vec<Value>,
}

pub(crate) fn statement(sql: String, params: Vec<Value>) -> D1Statement {
    D1Statement { sql, params }
}

#[derive(Deserialize)]
struct D1Reply {
    success: bool,
    #[serde(default)]
    result: Vec<D1Result>,
}

#[derive(Deserialize)]
pub(crate) struct D1Result {
    success: bool,
    #[serde(default)]
    pub(crate) results: Vec<Value>,
    #[serde(default)]
    meta: D1Meta,
}

#[derive(Default, Deserialize)]
struct D1Meta {
    served_by_primary: Option<bool>,
}
