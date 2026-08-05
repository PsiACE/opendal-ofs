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

//! Provider adapters for a Managed Volume.

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures::TryStreamExt;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer};
use opendal::{ErrorKind, Operator};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use crate::catalog::StorageLocator;
use crate::model::{
    CheckpointRecord, CommitRecord, ContentRef, Cursor, FormatRecord, HeadRecord, OperationId,
    VolumeId,
};

const FORMAT_KEY: &str = "metadata/format";
const HEAD_KEY: &str = "metadata/head";
const COMMIT_PREFIX: &str = "metadata/commits/";
const CHECKPOINT_PREFIX: &str = "metadata/checkpoints/";
const DATA_PREFIX: &str = "data/sha256/";
const INITIAL_KEY: &str = "initial";
const DATA_READ_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct Observation {
    pub(crate) format: FormatRecord,
    pub(crate) head: HeadRecord,
    pub(crate) token: String,
}

#[derive(Debug)]
pub(crate) enum PublicationOutcome {
    Committed(Cursor),
    AlreadyCommitted(Cursor),
    Conflict(Observation),
    Unknown,
}

#[async_trait]
pub(crate) trait MetadataStore: Send + Sync {
    async fn initialize(&self, proposed: FormatRecord) -> Result<Observation>;
    async fn observe(&self, volume: &VolumeId) -> Result<Observation>;
    async fn checkpoint(&self, volume: &VolumeId, at: &Cursor) -> Result<CheckpointRecord>;
    async fn changes(
        &self,
        volume: &VolumeId,
        after: &Cursor,
        through: &Cursor,
    ) -> Result<Vec<CommitRecord>>;
    async fn publish(
        &self,
        expected: &Observation,
        commit: CommitRecord,
    ) -> Result<PublicationOutcome>;
    async fn resolve(&self, volume: &VolumeId, operation: &OperationId) -> Result<Option<Cursor>>;
}

#[derive(Clone)]
pub(crate) struct ObjectMetadataStore {
    operator: Operator,
}

impl ObjectMetadataStore {
    pub(crate) fn new(operator: Operator) -> Result<Self> {
        let cap = operator.info().full_capability();
        if !cap.read
            || !cap.write
            || !cap.list
            || !cap.list_with_limit
            || !cap.write_with_if_match
            || !cap.write_with_if_not_exists
        {
            bail!(
                "Managed object metadata requires read, limited list, If-Match, and create-only write"
            );
        }
        Ok(Self { operator })
    }

    async fn read_format(&self) -> Result<Option<FormatRecord>> {
        match self.operator.read(FORMAT_KEY).await {
            Ok(bytes) => {
                let value: FormatRecord = decode(&bytes.to_vec(), "Managed format")?;
                value.validate()?;
                Ok(Some(value))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).context("read Managed format"),
        }
    }

    async fn read_head(&self) -> Result<(HeadRecord, String)> {
        let reader = self.operator.reader(HEAD_KEY).await?;
        let bytes = reader.read(..).await.context("read Managed head")?;
        let token = reader
            .metadata()
            .and_then(opendal::Metadata::etag)
            .context("Metadata Store read did not return the head ETag")?
            .to_owned();
        let head: HeadRecord = decode(&bytes.to_vec(), "Managed head")?;
        head.validate()?;
        Ok((head, token))
    }

    async fn read_commit(&self, cursor: &Cursor) -> Result<CommitRecord> {
        if cursor.generation == 0 {
            bail!("generation zero has no change commit");
        }
        let record: CommitRecord = self
            .read_json(&commit_key(&cursor.operation), "Managed commit")
            .await?;
        record.validate()?;
        if record.cursor != *cursor {
            bail!("Managed commit does not match its referenced cursor");
        }
        Ok(record)
    }

    async fn read_json<T: DeserializeOwned>(&self, key: &str, what: &str) -> Result<T> {
        let bytes = self
            .operator
            .read(key)
            .await
            .with_context(|| format!("read {what}"))?;
        decode(&bytes.to_vec(), what)
    }

    async fn write_immutable<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        match self
            .operator
            .write_with(key, bytes.clone())
            .if_not_exists(true)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if precondition(&error) => {
                let existing = self.operator.read(key).await?;
                if existing.to_vec() == bytes {
                    Ok(())
                } else {
                    bail!("immutable metadata key was reused with different content")
                }
            }
            Err(error) => Err(error).context("write immutable Managed metadata"),
        }
    }

    async fn observe_inner(&self, volume: &VolumeId) -> Result<Observation> {
        let format = self
            .read_format()
            .await?
            .context("Managed format is missing")?;
        let (head, token) = self.read_head().await?;
        if &format.volume_id != volume || &head.volume_id != volume {
            bail!("Managed metadata root belongs to another volume");
        }
        Ok(Observation {
            format,
            head,
            token,
        })
    }

    async fn reachable(&self, volume: &VolumeId, target: &OperationId) -> Result<Option<Cursor>> {
        let observed = self.observe_inner(volume).await?;
        let mut cursor = observed.head.cursor;
        loop {
            if &cursor.operation == target {
                return Ok(Some(cursor));
            }
            if cursor.generation == 0 {
                return Ok(None);
            }
            cursor = self.read_commit(&cursor).await?.parent;
        }
    }
}

#[async_trait]
impl MetadataStore for ObjectMetadataStore {
    async fn initialize(&self, proposed: FormatRecord) -> Result<Observation> {
        proposed.validate()?;
        let format = match self.read_format().await? {
            Some(format) => format,
            None => {
                if !self.operator.list_with("").limit(1).await?.is_empty() {
                    self.read_format()
                        .await?
                        .context("storage root is not empty and has no Managed format")?
                } else {
                    match self
                        .operator
                        .write_with(FORMAT_KEY, serde_json::to_vec(&proposed)?)
                        .if_not_exists(true)
                        .await
                    {
                        Ok(_) => proposed.clone(),
                        Err(error) if precondition(&error) => self
                            .read_format()
                            .await?
                            .context("concurrent initializer did not leave a Managed format")?,
                        Err(error)
                            if error.is_temporary() || error.kind() == ErrorKind::Unexpected =>
                        {
                            self.read_format()
                                .await?
                                .context("Managed format initialization result is unknown")?
                        }
                        Err(error) => return Err(error).context("initialize Managed format"),
                    }
                }
            }
        };
        if !format.same_storage(&proposed) {
            bail!("Managed metadata placement or Data Store binding differs");
        }
        match self.observe_inner(&format.volume_id).await {
            Ok(value) => Ok(value),
            Err(error)
                if error
                    .downcast_ref::<opendal::Error>()
                    .is_some_and(|source| source.kind() == ErrorKind::NotFound) =>
            {
                let checkpoint = CheckpointRecord::new(
                    format.volume_id.clone(),
                    Cursor::initial(),
                    Default::default(),
                )?;
                self.write_immutable(&checkpoint_key(&Cursor::initial()), &checkpoint)
                    .await?;
                let head = HeadRecord::initial(format.volume_id.clone());
                match self
                    .operator
                    .write_with(HEAD_KEY, serde_json::to_vec(&head)?)
                    .if_not_exists(true)
                    .await
                {
                    Ok(_) => {}
                    Err(error) if precondition(&error) => {}
                    Err(error) if error.is_temporary() || error.kind() == ErrorKind::Unexpected => {
                        return self.observe_inner(&format.volume_id).await;
                    }
                    Err(error) => return Err(error).context("initialize Managed head"),
                }
                self.observe_inner(&format.volume_id).await
            }
            Err(error) => Err(error),
        }
    }

    async fn observe(&self, volume: &VolumeId) -> Result<Observation> {
        self.observe_inner(volume).await
    }

    async fn checkpoint(&self, volume: &VolumeId, at: &Cursor) -> Result<CheckpointRecord> {
        let value: CheckpointRecord = self
            .read_json(&checkpoint_key(at), "Managed checkpoint")
            .await?;
        value.validate()?;
        if &value.volume_id != volume || value.cursor != *at {
            bail!("checkpoint does not match the requested volume cursor");
        }
        Ok(value)
    }

    async fn changes(
        &self,
        volume: &VolumeId,
        after: &Cursor,
        through: &Cursor,
    ) -> Result<Vec<CommitRecord>> {
        if after.generation > through.generation {
            bail!("change range is reversed");
        }
        let mut cursor = through.clone();
        let mut commits = Vec::new();
        while cursor != *after {
            if cursor.generation <= after.generation {
                bail!("change cursor is not in the target ancestry");
            }
            let commit = self.read_commit(&cursor).await?;
            if &commit.volume_id != volume {
                bail!("change commit belongs to another volume");
            }
            cursor = commit.parent.clone();
            commits.push(commit);
        }
        commits.reverse();
        Ok(commits)
    }

    async fn publish(
        &self,
        expected: &Observation,
        commit: CommitRecord,
    ) -> Result<PublicationOutcome> {
        commit.validate()?;
        if commit.volume_id != expected.format.volume_id || commit.parent != expected.head.cursor {
            bail!("publication does not match its observed authority position");
        }
        self.write_immutable(&commit_key(&commit.cursor.operation), &commit)
            .await?;
        let head = HeadRecord::advance(
            commit.volume_id.clone(),
            commit.cursor.clone(),
            expected.head.checkpoint.clone(),
        );
        match self
            .operator
            .write_with(HEAD_KEY, serde_json::to_vec(&head)?)
            .if_match(&expected.token)
            .await
        {
            Ok(_) => Ok(PublicationOutcome::Committed(commit.cursor)),
            Err(error) if precondition(&error) => match self
                .reachable(&commit.volume_id, &commit.cursor.operation)
                .await?
            {
                Some(cursor) => Ok(PublicationOutcome::AlreadyCommitted(cursor)),
                None => Ok(PublicationOutcome::Conflict(
                    self.observe_inner(&commit.volume_id).await?,
                )),
            },
            Err(error) if error.is_temporary() || error.kind() == ErrorKind::Unexpected => {
                Ok(PublicationOutcome::Unknown)
            }
            Err(error) => Err(error).context("publish Managed head"),
        }
    }

    async fn resolve(&self, volume: &VolumeId, operation: &OperationId) -> Result<Option<Cursor>> {
        self.reachable(volume, operation).await
    }
}

#[derive(Clone)]
pub(crate) struct DataStore {
    operator: Operator,
}

impl DataStore {
    pub(crate) fn new(operator: Operator) -> Result<Self> {
        let cap = operator.info().full_capability();
        if !cap.read || !cap.write || !cap.write_with_if_not_exists {
            bail!("Managed immutable data requires read and create-only write");
        }
        Ok(Self { operator })
    }

    pub(crate) async fn put_file(
        &self,
        source: &Path,
        expected_sha256: &str,
        expected_size: u64,
        concurrency: NonZeroUsize,
    ) -> Result<ContentRef> {
        let content = ContentRef {
            data_ref: format!("sha256:{expected_sha256}"),
            sha256: expected_sha256.to_owned(),
            size: expected_size,
        };
        content.validate()?;
        let key = data_key(&content)?;
        let mut writer = match self
            .operator
            .writer_with(&key)
            .if_not_exists(true)
            .concurrent(concurrency.get())
            .await
        {
            Ok(writer) => writer,
            Err(error) if precondition(&error) => {
                self.verify(&content, concurrency).await?;
                return Ok(content);
            }
            Err(error) => return Err(error).context("open immutable data writer"),
        };
        let mut file = tokio::fs::File::open(source).await?;
        let mut buffer = vec![0; 1024 * 1024];
        let mut hasher = Sha256::new();
        let mut size = 0_u64;
        loop {
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            size += read as u64;
            writer.write(buffer[..read].to_vec()).await?;
        }
        if size != expected_size || hex(hasher.finalize()) != expected_sha256 {
            writer.abort().await?;
            bail!("staged content changed before immutable upload completed");
        }
        let result = writer.close().await;
        drop(writer);
        match result {
            Ok(_) => self.verify(&content, concurrency).await?,
            Err(error) if precondition(&error) || error.is_temporary() => {
                self.verify(&content, concurrency).await?
            }
            Err(error) => return Err(error).context("finish immutable data write"),
        }
        Ok(content)
    }

    pub(crate) async fn fetch(
        &self,
        content: &ContentRef,
        target: &Path,
        concurrency: NonZeroUsize,
    ) -> Result<()> {
        let parent = target.parent().context("staged content has no parent")?;
        let temporary = tempfile::NamedTempFile::new_in(parent)?;
        let mut file = tokio::fs::File::from_std(temporary.reopen()?);
        self.read(content, Some(&mut file), concurrency).await?;
        file.sync_all().await?;
        drop(file);
        temporary
            .persist_noclobber(target)
            .map_err(|error| error.error)?;
        Ok(())
    }

    pub(crate) async fn verify(
        &self,
        content: &ContentRef,
        concurrency: NonZeroUsize,
    ) -> Result<()> {
        self.read(content, None, concurrency).await.map(|_| ())
    }

    async fn read(
        &self,
        content: &ContentRef,
        mut target: Option<&mut tokio::fs::File>,
        concurrency: NonZeroUsize,
    ) -> Result<u64> {
        content.validate()?;
        let key = data_key(content)?;
        let reader = self.operator.reader_with(&key);
        let reader = if content.size > DATA_READ_CHUNK_SIZE as u64 {
            reader
                .chunk(DATA_READ_CHUNK_SIZE)
                .concurrent(concurrency.get())
        } else {
            reader
        };
        let reader = reader.await?;
        let mut stream = reader.into_stream(..).await?;
        let mut hasher = Sha256::new();
        let mut size = 0_u64;
        while let Some(buffer) = stream.try_next().await? {
            for chunk in buffer {
                if let Some(file) = target.as_deref_mut() {
                    file.write_all(&chunk).await?;
                }
                hasher.update(&chunk);
                size += chunk.len() as u64;
            }
        }
        if size != content.size || hex(hasher.finalize()) != content.sha256 {
            bail!("immutable data does not match its content reference");
        }
        Ok(size)
    }
}

pub(crate) fn assemble_operator(
    locator: &StorageLocator,
    current: Option<&Url>,
    concurrency: Option<NonZeroUsize>,
) -> Result<Operator> {
    let environment = if current.is_none() {
        std::env::var("OFS_STORAGE_URL")
            .ok()
            .map(|value| Url::parse(&value))
            .transpose()?
    } else {
        None
    };
    let overlay = current.or(environment.as_ref());
    if overlay.is_some_and(|url| StorageLocator::parse(url).as_ref().ok() != Some(locator)) {
        bail!("credential storage URL does not match the volume catalog locator");
    }
    let mut config = locator
        .options
        .iter()
        .flat_map(|(key, values)| values.iter().map(move |value| (key.clone(), value.clone())))
        .collect::<Vec<_>>();
    if locator.scheme == "s3" {
        if let Some(bucket) = &locator.host {
            config.push(("bucket".to_owned(), bucket.clone()));
        }
    }
    if !locator.path.is_empty() && locator.path != "/" {
        config.push(("root".to_owned(), locator.path.clone()));
    }
    if let Some(url) = overlay {
        for (key, value) in url.query_pairs() {
            config.retain(|(existing, _)| existing != key.as_ref());
            config.push((key.into_owned(), value.into_owned()));
        }
    }
    let operator = Operator::via_iter(&locator.scheme, config)
        .map(|operator| operator.layer(RetryLayer::new().with_jitter().with_max_times(3)))
        .context("assemble OpenDAL operator")?;
    Ok(match concurrency {
        Some(value) => operator
            .layer(ConcurrentLimitLayer::new(value.get()).with_http_concurrent_limit(value.get())),
        None => operator,
    })
}

pub(crate) fn data_store_id(locator: &StorageLocator) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        hex(Sha256::digest(serde_json::to_vec(locator)?))
    ))
}

fn decode<T: DeserializeOwned>(bytes: &[u8], what: &str) -> Result<T> {
    serde_json::from_slice(bytes).with_context(|| format!("decode {what}"))
}

fn precondition(error: &opendal::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists
    )
}

fn commit_key(operation: &OperationId) -> String {
    format!("{COMMIT_PREFIX}{}", operation.as_str())
}

fn checkpoint_key(cursor: &Cursor) -> String {
    let suffix = if cursor.generation == 0 {
        INITIAL_KEY
    } else {
        cursor.operation.as_str()
    };
    format!("{CHECKPOINT_PREFIX}{suffix}")
}

fn data_key(content: &ContentRef) -> Result<String> {
    content.validate()?;
    Ok(format!("{DATA_PREFIX}{}", content.sha256))
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    let mut output = String::with_capacity(bytes.as_ref().len() * 2);
    for byte in bytes.as_ref() {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}
