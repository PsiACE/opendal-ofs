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

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::filesystem::VolumeError;
use crate::managed::error::{corrupt, invalid, unavailable};

const MAX_D1_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

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
    ) -> Result<Self, VolumeError> {
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

    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Result<Self, VolumeError> {
        self.api_base = api_base.into();
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), VolumeError> {
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
            return Err(invalid("configure D1 metadata", "D1 scope is invalid"));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct D1Session {
    client: reqwest::Client,
    config: D1Config,
}

impl D1Session {
    pub(crate) fn new(config: D1Config) -> Result<Self, VolumeError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| unavailable("open D1 metadata", "D1 metadata is unavailable"))?;
        Ok(Self { client, config })
    }

    pub(crate) fn store_key(&self) -> &str {
        &self.config.store_key
    }

    pub(crate) async fn query(
        &self,
        statements: Vec<D1Statement>,
        action: &'static str,
    ) -> Result<Vec<D1Result>, VolumeError> {
        let expected_results = statements.len();
        let endpoint = format!(
            "{}/accounts/{}/d1/database/{}/query",
            self.config.api_base.trim_end_matches('/'),
            self.config.account_id,
            self.config.database_id
        );
        let mut response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.config.token)
            .json(&D1Request { batch: statements })
            .send()
            .await
            .map_err(|_| unavailable(action, "D1 metadata is unavailable"))?;
        if !response.status().is_success() {
            return Err(unavailable(action, "D1 metadata is unavailable"));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_D1_RESPONSE_BYTES as u64)
        {
            return Err(corrupt(action, "D1 response exceeds its size limit"));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| unavailable(action, "D1 metadata is unavailable"))?
        {
            if body.len().saturating_add(chunk.len()) > MAX_D1_RESPONSE_BYTES {
                return Err(corrupt(action, "D1 response exceeds its size limit"));
            }
            body.extend_from_slice(&chunk);
        }
        let reply: D1Reply = serde_json::from_slice(&body)
            .map_err(|_| corrupt(action, "D1 returned an invalid response"))?;
        if !reply.success
            || reply
                .result
                .iter()
                .any(|result| !result.success || result.meta.served_by_primary != Some(true))
        {
            return Err(unavailable(action, "D1 metadata is unavailable"));
        }
        if reply.result.len() != expected_results {
            return Err(corrupt(action, "D1 returned an invalid result count"));
        }
        Ok(reply.result)
    }
}

#[derive(Serialize)]
struct D1Request {
    batch: Vec<D1Statement>,
}

#[derive(Serialize)]
pub(crate) struct D1Statement {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
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
    pub(crate) results: Vec<D1Row>,
    #[serde(default)]
    meta: D1Meta,
}

#[derive(Deserialize)]
pub(crate) struct D1Row {
    pub(crate) value_hex: Option<String>,
    pub(crate) revision: Option<u64>,
}

#[derive(Default, Deserialize)]
struct D1Meta {
    served_by_primary: Option<bool>,
}
