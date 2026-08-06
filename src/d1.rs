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

//! Cloudflare D1 adapter for the provider-neutral Metadata Store contract.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use http::{Request, StatusCode, header};
use opendal::Buffer;
use opendal::raw::{HttpClient, format_authorization_by_bearer};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use url::Url;

use crate::catalog::StorageLocator;
use crate::model::{
    CommitRecord, Cursor, FormatRecord, HeadRecord, MetadataPlacement, OperationId, VolumeId,
};
use crate::store::{AuthorityToken, MetadataStore, Observation, PublicationOutcome};

const API_BASE: &str = "https://api.cloudflare.com/client/v4";
const SCHEMA_VERSION: u32 = 1;
const CREATE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS ofs_managed_v1_schema (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), schema_version INTEGER NOT NULL)";
const CREATE_FORMATS: &str = "CREATE TABLE IF NOT EXISTS ofs_managed_v1_formats (store_key TEXT PRIMARY KEY, record_json TEXT NOT NULL, digest TEXT NOT NULL)";
const CREATE_HEADS: &str = "CREATE TABLE IF NOT EXISTS ofs_managed_v1_heads (store_key TEXT PRIMARY KEY, volume_id TEXT NOT NULL, generation INTEGER NOT NULL, operation_id TEXT NOT NULL, revision INTEGER NOT NULL, head_json TEXT NOT NULL)";
const CREATE_COMMITS: &str = "CREATE TABLE IF NOT EXISTS ofs_managed_v1_commits (store_key TEXT NOT NULL, operation_id TEXT NOT NULL, volume_id TEXT NOT NULL, generation INTEGER NOT NULL, parent_generation INTEGER NOT NULL, parent_operation_id TEXT NOT NULL, record_json TEXT NOT NULL, digest TEXT NOT NULL, PRIMARY KEY (store_key, operation_id))";
const CREATE_COMMIT_GENERATION_INDEX: &str = "CREATE INDEX IF NOT EXISTS ofs_managed_v1_commits_by_generation ON ofs_managed_v1_commits (store_key, generation)";

pub(crate) struct D1Config {
    account: String,
    database: String,
    store_key: String,
    token: String,
}

impl D1Config {
    pub(crate) fn resolve(locator: &StorageLocator, current: Option<&Url>) -> Result<Self> {
        let environment = std::env::var("OFS_METADATA_URL")
            .ok()
            .map(|value| Url::parse(&value))
            .transpose()?;
        for overlay in [environment.as_ref(), current].into_iter().flatten() {
            if StorageLocator::parse(overlay)? != *locator {
                bail!("credential metadata URL does not match the catalog locator");
            }
        }
        let overlay = current
            .filter(|url| url.query_pairs().any(|(key, _)| key == "token"))
            .or(environment.as_ref())
            .context("D1 metadata requires --metadata with a token or OFS_METADATA_URL")?;
        if locator.scheme != "d1" || locator.port.is_some() || !locator.options.is_empty() {
            bail!("D1 URL must be d1://ACCOUNT/DATABASE/STORE?token=...");
        }
        let account = locator.host.clone().context("D1 account id is missing")?;
        let path = locator.path.trim_matches('/');
        let (database, store_key) = path
            .split_once('/')
            .context("D1 database id or store key is missing")?;
        let tokens = overlay
            .query_pairs()
            .filter(|(key, _)| key == "token")
            .map(|(_, value)| value.into_owned())
            .collect::<Vec<_>>();
        if database.is_empty() || store_key.is_empty() || tokens.len() != 1 || tokens[0].is_empty()
        {
            bail!("D1 URL must contain one account, database, store key, and token");
        }
        Ok(Self {
            account,
            database: database.to_owned(),
            store_key: store_key.to_owned(),
            token: tokens.into_iter().next().unwrap(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct D1MetadataStore {
    transport: Transport,
    store_key: String,
}

impl D1MetadataStore {
    pub(crate) fn new(config: D1Config) -> Result<Self> {
        let authorization = format_authorization_by_bearer(&config.token)
            .context("D1 token cannot be represented as an authorization header")?;
        let endpoint = format!(
            "{API_BASE}/accounts/{}/d1/database/{}/query",
            segment(&config.account),
            segment(&config.database)
        );
        Ok(Self {
            transport: Transport {
                client: HttpClient::new().context("construct D1 HTTP client")?,
                endpoint,
                authorization,
            },
            store_key: config.store_key,
        })
    }

    async fn ensure_schema(&self) -> Result<()> {
        for sql in [
            CREATE_SCHEMA,
            CREATE_FORMATS,
            CREATE_HEADS,
            CREATE_COMMITS,
            CREATE_COMMIT_GENERATION_INDEX,
        ] {
            self.transport
                .execute(sql, Vec::new(), Semantics::Idempotent)
                .await?;
        }
        self.transport
            .execute(
                "INSERT OR IGNORE INTO ofs_managed_v1_schema (singleton, schema_version) VALUES (1, ?)",
                vec![SCHEMA_VERSION.into()],
                Semantics::Idempotent,
            )
            .await?;
        let rows = self
            .transport
            .execute(
                "SELECT schema_version FROM ofs_managed_v1_schema WHERE singleton = 1",
                Vec::new(),
                Semantics::Read,
            )
            .await?;
        if one(&rows, "D1 schema")?
            .get("schema_version")
            .and_then(Value::as_u64)
            != Some(u64::from(SCHEMA_VERSION))
        {
            bail!("D1 contains an unsupported ofs metadata schema");
        }
        Ok(())
    }

    async fn read_format(&self) -> Result<FormatRecord> {
        let rows = self
            .transport
            .execute(
                "SELECT record_json, digest FROM ofs_managed_v1_formats WHERE store_key = ?",
                vec![self.store_key.clone().into()],
                Semantics::Read,
            )
            .await?;
        let value: FormatRecord = record(one(&rows, "D1 format")?)?;
        value.validate()?;
        Ok(value)
    }

    async fn read_head(&self) -> Result<(HeadRecord, u64)> {
        let rows = self
            .transport
            .execute(
                "SELECT revision, head_json FROM ofs_managed_v1_heads WHERE store_key = ?",
                vec![self.store_key.clone().into()],
                Semantics::Read,
            )
            .await?;
        let row = one(&rows, "D1 head")?;
        let head: HeadRecord = serde_json::from_str(required(row, "head_json")?)?;
        head.validate()?;
        Ok((head, required_u64(row, "revision")?))
    }

    async fn observe_inner(&self, volume: &VolumeId) -> Result<Observation> {
        let format = self.read_format().await?;
        let (head, revision) = self.read_head().await?;
        if &format.volume_id != volume || &head.volume_id != volume {
            bail!("D1 metadata scope belongs to another Managed Volume");
        }
        Ok(Observation {
            format,
            head,
            authority: AuthorityToken::D1Revision(revision),
        })
    }

    async fn write_format(&self, value: &FormatRecord) -> Result<()> {
        let json = serde_json::to_string(value)?;
        self.transport
            .execute(
                "INSERT OR IGNORE INTO ofs_managed_v1_formats (store_key, record_json, digest) VALUES (?, ?, ?)",
                vec![self.store_key.clone().into(), json.clone().into(), digest(&json).into()],
                Semantics::Idempotent,
            )
            .await?;
        Ok(())
    }

    async fn write_commit(&self, value: &CommitRecord) -> Result<String> {
        value.validate()?;
        let json = serde_json::to_string(value)?;
        let hash = digest(&json);
        let rows = self
            .transport
            .execute(
                "INSERT OR IGNORE INTO ofs_managed_v1_commits (store_key, operation_id, volume_id, generation, parent_generation, parent_operation_id, record_json, digest) VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING record_json, digest",
                vec![
                    self.store_key.clone().into(),
                    value.cursor.operation.as_str().into(),
                    value.volume_id.as_str().into(),
                    value.cursor.generation.into(),
                    value.parent.generation.into(),
                    value.parent.operation.as_str().into(),
                    json.into(),
                    hash.clone().into(),
                ],
                Semantics::Idempotent,
            )
            .await?;
        let stored = match rows.as_slice() {
            [row] => {
                let stored: CommitRecord = record(row)?;
                stored.validate()?;
                stored
            }
            [] => self.read_commit(&value.cursor.operation).await?,
            _ => bail!("D1 returned duplicate commit rows"),
        };
        if stored != *value {
            bail!("immutable D1 commit key was reused with different content");
        }
        Ok(hash)
    }

    async fn read_commit_optional(&self, operation: &OperationId) -> Result<Option<CommitRecord>> {
        let rows = self
            .transport
            .execute(
                "SELECT record_json, digest FROM ofs_managed_v1_commits WHERE store_key = ? AND operation_id = ?",
                vec![self.store_key.clone().into(), operation.as_str().into()],
                Semantics::Read,
            )
            .await?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => {
                let value: CommitRecord = record(row)?;
                value.validate()?;
                Ok(Some(value))
            }
            _ => bail!("D1 returned duplicate commit rows"),
        }
    }

    async fn read_commit(&self, operation: &OperationId) -> Result<CommitRecord> {
        self.read_commit_optional(operation)
            .await?
            .context("required D1 commit is missing")
    }

    async fn reachable(
        &self,
        volume: &VolumeId,
        operation: &OperationId,
    ) -> Result<Option<Cursor>> {
        if self.read_commit_optional(operation).await?.is_none() {
            return Ok(None);
        }
        let (head, _) = self.read_head().await?;
        if &head.volume_id != volume {
            bail!("D1 head belongs to another Managed Volume");
        }
        let mut cursor = head.cursor;
        loop {
            if &cursor.operation == operation {
                return Ok(Some(cursor));
            }
            if cursor.generation == 0 {
                return Ok(None);
            }
            let commit = self.read_commit(&cursor.operation).await?;
            if commit.cursor != cursor || &commit.volume_id != volume {
                bail!("D1 commit ancestry is not continuous");
            }
            cursor = commit.parent;
        }
    }
}

#[async_trait]
impl MetadataStore for D1MetadataStore {
    async fn initialize(&self, proposed: FormatRecord) -> Result<Observation> {
        proposed.validate()?;
        if proposed.placement() != MetadataPlacement::ExternalD1 {
            bail!("D1 adapter requires external_d1 metadata placement");
        }
        self.ensure_schema().await?;
        self.write_format(&proposed).await?;
        let format = self.read_format().await?;
        if !format.same_storage(&proposed) {
            bail!("D1 scope has a different metadata placement or Data Store binding");
        }
        let head = HeadRecord::initial(format.volume_id.clone());
        self.transport
            .execute(
                "INSERT OR IGNORE INTO ofs_managed_v1_heads (store_key, volume_id, generation, operation_id, revision, head_json) VALUES (?, ?, 0, 'initial', 0, ?)",
                vec![
                    self.store_key.clone().into(),
                    format.volume_id.as_str().into(),
                    serde_json::to_string(&head)?.into(),
                ],
                Semantics::Idempotent,
            )
            .await?;
        self.observe_inner(&format.volume_id).await
    }

    async fn observe(&self, volume: &VolumeId) -> Result<Observation> {
        self.observe_inner(volume).await
    }

    async fn changes(
        &self,
        volume: &VolumeId,
        after: &Cursor,
        through: &Cursor,
    ) -> Result<Vec<CommitRecord>> {
        if after.generation > through.generation {
            bail!("D1 change range is reversed");
        }
        if after == through {
            return Ok(Vec::new());
        }
        let rows = self
            .transport
            .execute(
                "SELECT record_json, digest FROM ofs_managed_v1_commits WHERE store_key = ? AND generation > ? AND generation <= ? ORDER BY generation ASC",
                vec![
                    self.store_key.clone().into(),
                    after.generation.into(),
                    through.generation.into(),
                ],
                Semantics::Read,
            )
            .await?;
        let mut candidates = BTreeMap::new();
        for row in &rows {
            let commit: CommitRecord = record(row)?;
            commit.validate()?;
            if &commit.volume_id != volume {
                bail!("D1 change belongs to another Managed Volume");
            }
            if candidates
                .insert(commit.cursor.operation.clone(), commit)
                .is_some()
            {
                bail!("D1 returned duplicate change rows");
            }
        }
        let mut cursor = through.clone();
        let mut changes = Vec::new();
        while cursor != *after {
            if cursor.generation <= after.generation {
                bail!("D1 change cursor is not in the target ancestry");
            }
            let commit = candidates
                .remove(&cursor.operation)
                .context("required D1 change commit is missing")?;
            if commit.cursor != cursor || &commit.volume_id != volume {
                bail!("D1 change ancestry is corrupt");
            }
            cursor = commit.parent.clone();
            changes.push(commit);
        }
        changes.reverse();
        Ok(changes)
    }

    async fn publish(
        &self,
        expected: &Observation,
        commit: CommitRecord,
    ) -> Result<PublicationOutcome> {
        commit.validate()?;
        if commit.volume_id != expected.format.volume_id || commit.parent != expected.head.cursor {
            bail!("D1 publication does not match its observed authority position");
        }
        let hash = self.write_commit(&commit).await?;
        let next = HeadRecord::advance(commit.volume_id.clone(), commit.cursor.clone());
        let expected_revision = match &expected.authority {
            AuthorityToken::D1Revision(value) => *value,
            AuthorityToken::ObjectEtag(_) => bail!("D1 metadata received an object ETag"),
        };
        let result = self
            .transport
            .execute(
                "UPDATE ofs_managed_v1_heads SET generation = ?, operation_id = ?, revision = revision + 1, head_json = ? WHERE store_key = ? AND volume_id = ? AND generation = ? AND operation_id = ? AND revision = ? AND EXISTS (SELECT 1 FROM ofs_managed_v1_commits c WHERE c.store_key = ofs_managed_v1_heads.store_key AND c.operation_id = ? AND c.generation = ? AND c.digest = ?) RETURNING generation",
                vec![
                    commit.cursor.generation.into(),
                    commit.cursor.operation.as_str().into(),
                    serde_json::to_string(&next)?.into(),
                    self.store_key.clone().into(),
                    commit.volume_id.as_str().into(),
                    commit.parent.generation.into(),
                    commit.parent.operation.as_str().into(),
                    expected_revision.into(),
                    commit.cursor.operation.as_str().into(),
                    commit.cursor.generation.into(),
                    hash.into(),
                ],
                Semantics::Publication,
            )
            .await;
        match result {
            Ok(rows) if rows.len() == 1 => Ok(PublicationOutcome::Committed(commit.cursor)),
            Ok(rows) if rows.is_empty() => match self
                .reachable(&commit.volume_id, &commit.cursor.operation)
                .await?
            {
                Some(cursor) => Ok(PublicationOutcome::AlreadyCommitted(cursor)),
                None => Ok(PublicationOutcome::Conflict(
                    self.observe_inner(&commit.volume_id).await?,
                )),
            },
            Ok(_) => bail!("D1 returned duplicate publication rows"),
            Err(error) if error.unknown => Ok(PublicationOutcome::Unknown),
            Err(error) => Err(error.into()),
        }
    }

    async fn resolve(&self, volume: &VolumeId, operation: &OperationId) -> Result<Option<Cursor>> {
        self.reachable(volume, operation).await
    }
}

#[derive(Clone)]
struct Transport {
    client: HttpClient,
    endpoint: String,
    authorization: String,
}

impl Transport {
    async fn execute(
        &self,
        sql: &'static str,
        params: Vec<Value>,
        semantics: Semantics,
    ) -> std::result::Result<Vec<Map<String, Value>>, QueryFailure> {
        let body = serde_json::to_vec(&QueryRequest { sql, params })
            .map(Buffer::from)
            .map_err(|error| QueryFailure::local("encode D1 query", error))?;
        if semantics == Semantics::Publication {
            return self.execute_once(body, semantics).await;
        }
        (|| self.execute_once(body.clone(), semantics))
            .retry(
                ExponentialBuilder::default()
                    .with_jitter()
                    .with_max_times(3),
            )
            .when(|error| error.retryable)
            .await
    }

    async fn execute_once(
        &self,
        body: Buffer,
        semantics: Semantics,
    ) -> std::result::Result<Vec<Map<String, Value>>, QueryFailure> {
        let request = Request::post(&self.endpoint)
            .header(header::AUTHORIZATION, self.authorization.clone())
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(|error| QueryFailure::local("build D1 query", error))?;
        let response = self.client.send(request).await.map_err(|error| {
            QueryFailure::new(
                QueryFailureKind::Transport,
                true,
                semantics == Semantics::Publication,
                format!("send D1 query: {error}"),
            )
        })?;
        let status = response.status();
        let response_body = response.into_body().to_vec();
        if !status.is_success() {
            let unknown = semantics == Semantics::Publication && status.is_server_error();
            let kind = if status == StatusCode::TOO_MANY_REQUESTS {
                QueryFailureKind::RateLimited
            } else if status == StatusCode::BAD_REQUEST {
                QueryFailureKind::InvalidRequest
            } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                QueryFailureKind::PermissionDenied
            } else if status == StatusCode::NOT_FOUND {
                QueryFailureKind::NotFound
            } else if status == StatusCode::CONFLICT {
                QueryFailureKind::Conflict
            } else if status.is_server_error() {
                QueryFailureKind::Service
            } else {
                QueryFailureKind::Rejected
            };
            return Err(QueryFailure::new(
                kind,
                status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error(),
                unknown,
                format!(
                    "D1 query returned HTTP {status}: {}",
                    response_detail(&response_body)
                ),
            ));
        }
        let response: QueryResponse = serde_json::from_slice(&response_body).map_err(|error| {
            QueryFailure::new(
                QueryFailureKind::InvalidResponse,
                false,
                semantics == Semantics::Publication,
                format!("decode D1 response: {error}"),
            )
        })?;
        if !response.success || response.result.len() != 1 || !response.errors.is_empty() {
            return Err(QueryFailure::new(
                QueryFailureKind::Statement,
                false,
                false,
                format!(
                    "D1 statement failed: {}",
                    response_detail(&serde_json::to_vec(&response.errors).unwrap_or_default())
                ),
            ));
        }
        let result = response.result.into_iter().next().unwrap();
        if !result.success {
            return Err(QueryFailure::new(
                QueryFailureKind::Statement,
                false,
                false,
                "D1 statement result was unsuccessful".to_owned(),
            ));
        }
        if result.meta.served_by_primary != Some(true) {
            return Err(QueryFailure::new(
                QueryFailureKind::NotAuthoritative,
                true,
                semantics == Semantics::Publication,
                "D1 did not prove an authoritative primary result".to_owned(),
            ));
        }
        Ok(result.results)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Semantics {
    Read,
    Idempotent,
    Publication,
}

#[derive(Debug)]
pub(crate) struct QueryFailure {
    pub(crate) kind: QueryFailureKind,
    retryable: bool,
    pub(crate) unknown: bool,
    message: String,
}

impl QueryFailure {
    fn new(kind: QueryFailureKind, retryable: bool, unknown: bool, message: String) -> Self {
        Self {
            kind,
            retryable,
            unknown,
            message,
        }
    }

    fn local(what: &str, error: impl fmt::Display) -> Self {
        Self::new(
            QueryFailureKind::Local,
            false,
            false,
            format!("{what}: {error}"),
        )
    }
}

impl fmt::Display for QueryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for QueryFailure {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryFailureKind {
    Local,
    Transport,
    RateLimited,
    InvalidRequest,
    NotFound,
    PermissionDenied,
    Conflict,
    Service,
    Rejected,
    InvalidResponse,
    Statement,
    NotAuthoritative,
}

#[derive(Serialize)]
struct QueryRequest {
    sql: &'static str,
    params: Vec<Value>,
}

#[derive(Deserialize)]
struct QueryResponse {
    success: bool,
    #[serde(default)]
    result: Vec<QueryResult>,
    #[serde(default)]
    errors: Vec<Value>,
}

#[derive(Deserialize)]
struct QueryResult {
    #[serde(default)]
    results: Vec<Map<String, Value>>,
    success: bool,
    #[serde(default)]
    meta: QueryMeta,
}

#[derive(Default, Deserialize)]
struct QueryMeta {
    served_by_primary: Option<bool>,
}

fn one<'a>(rows: &'a [Map<String, Value>], what: &str) -> Result<&'a Map<String, Value>> {
    match rows {
        [row] => Ok(row),
        [] => bail!("required {what} is missing"),
        _ => bail!("D1 returned duplicate {what} rows"),
    }
}

fn required<'a>(row: &'a Map<String, Value>, field: &str) -> Result<&'a str> {
    row.get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("D1 field {field:?} is missing or invalid"))
}

fn required_u64(row: &Map<String, Value>, field: &str) -> Result<u64> {
    row.get(field)
        .and_then(Value::as_u64)
        .with_context(|| format!("D1 field {field:?} is missing or invalid"))
}

fn record<T: for<'de> Deserialize<'de>>(row: &Map<String, Value>) -> Result<T> {
    let json = required(row, "record_json")?;
    if digest(json) != required(row, "digest")? {
        bail!("D1 Managed record digest is invalid");
    }
    serde_json::from_str(json).context("decode D1 Managed record")
}

fn digest(value: &str) -> String {
    let hash = Sha256::digest(value.as_bytes());
    let mut output = String::from("sha256:");
    for byte in hash {
        use fmt::Write as _;
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

fn segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn response_detail(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    if text.len() <= 1024 {
        return text.into_owned();
    }
    let mut end = 1024;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn d1_configuration_keeps_token_out_of_the_locator() {
        let url = Url::parse("d1://account/database/agent-home?token=private").unwrap();
        let locator = StorageLocator::parse(&url).unwrap();
        let config = D1Config::resolve(&locator, Some(&url)).unwrap();
        assert_eq!(config.account, "account");
        assert_eq!(config.database, "database");
        assert_eq!(config.store_key, "agent-home");
        assert!(!serde_json::to_string(&locator).unwrap().contains("private"));
    }
}
