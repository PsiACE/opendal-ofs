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

use std::fs::OpenOptions;
use std::path::PathBuf;

use futures::TryStreamExt as _;
use rusqlite::{Connection, OptionalExtension as _, params};

use crate::filesystem::{OperationId, VolumeError, VolumeErrorKind};

use super::ManagedVolume;
use super::head::GcFence;

const OBJECT_PREFIX: &str = "managed/1/objects/";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOutcome {
    pub scanned: usize,
    pub deleted: usize,
    pub deleted_bytes: u64,
}

impl ManagedVolume {
    pub async fn collect_unreachable(&self, resume: bool) -> Result<GcOutcome, VolumeError> {
        let capability = self.operator().info().full_capability();
        if !capability.list || !capability.delete {
            return Err(VolumeError::new(
                VolumeErrorKind::Invalid,
                "collect Managed data: storage lacks list or delete",
            ));
        }
        let fence = self.begin_gc(resume).await?;
        let live = LiveObjects::create(fence.owner)?;
        self.visit_reachable_objects(fence.namespace_commit, |key, length| {
            live.insert(&key, length)
        })
        .await?;
        let outcome = self.sweep(&live).await?;
        self.finish_gc(fence).await?;
        Ok(outcome)
    }

    async fn begin_gc(&self, resume: bool) -> Result<GcFence, VolumeError> {
        let (mut head, revision) = self.read_head().await?;
        let owner = OperationId::generate();
        let fence = match (resume, head.maintenance) {
            (false, None) => GcFence {
                owner,
                namespace_commit: head.namespace_commit,
            },
            (false, Some(_)) => {
                return Err(conflict(
                    "begin Managed data collection: another collection is active",
                ));
            }
            (true, Some(active)) if active.namespace_commit == head.namespace_commit => GcFence {
                owner,
                namespace_commit: active.namespace_commit,
            },
            (true, Some(_)) => {
                return Err(corrupt(
                    "resume Managed data collection: fence cursor is invalid",
                ));
            }
            (true, None) => {
                return Err(conflict(
                    "resume Managed data collection: no interrupted collection is active",
                ));
            }
        };
        head.maintenance = Some(fence);
        if !self.replace_head(&revision, &head).await? {
            return Err(conflict(
                "begin Managed data collection: namespace authority changed",
            ));
        }
        Ok(fence)
    }

    async fn sweep(&self, live: &LiveObjects) -> Result<GcOutcome, VolumeError> {
        let mut outcome = GcOutcome::default();
        let mut lister = self
            .operator()
            .lister_with(OBJECT_PREFIX)
            .recursive(true)
            .await
            .map_err(|_| unavailable("list Managed data objects"))?;
        let mut deleter = self
            .operator()
            .deleter()
            .await
            .map_err(|_| unavailable("open Managed data deleter"))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|_| unavailable("list Managed data objects"))?
        {
            if !entry.metadata().is_file() || !valid_object_key(entry.path()) {
                continue;
            }
            outcome.scanned += 1;
            let length = entry.metadata().content_length();
            if live.contains(entry.path(), length)? {
                continue;
            }
            deleter
                .delete(entry.path())
                .await
                .map_err(|_| unavailable("delete Managed data object"))?;
            outcome.deleted += 1;
            outcome.deleted_bytes = outcome
                .deleted_bytes
                .checked_add(length)
                .ok_or_else(|| corrupt("deleted Managed data byte count overflows"))?;
        }
        deleter
            .close()
            .await
            .map_err(|_| unavailable("finish Managed data deletion"))?;
        Ok(outcome)
    }

    async fn finish_gc(&self, fence: GcFence) -> Result<(), VolumeError> {
        let (mut head, revision) = self.read_head().await?;
        if head.maintenance != Some(fence) || head.namespace_commit != fence.namespace_commit {
            return Err(conflict(
                "finish Managed data collection: collection fence changed",
            ));
        }
        head.maintenance = None;
        if self.replace_head(&revision, &head).await? {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed data collection: namespace authority changed",
            ))
        }
    }
}

struct LiveObjects {
    connection: Option<Connection>,
    path: PathBuf,
}

impl LiveObjects {
    fn create(operation: OperationId) -> Result<Self, VolumeError> {
        let path = std::env::temp_dir().join(format!("ofs-managed-gc-{operation}.sqlite"));
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| unavailable("create Managed collection mark store"))?;
        let connection = Connection::open(&path)
            .map_err(|_| unavailable("open Managed collection mark store"))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; \
                 CREATE TABLE live (key TEXT PRIMARY KEY, length BLOB NOT NULL) \
                 STRICT, WITHOUT ROWID;",
            )
            .map_err(|_| unavailable("initialize Managed collection mark store"))?;
        Ok(Self {
            connection: Some(connection),
            path,
        })
    }

    fn insert(&self, key: &str, length: u64) -> Result<(), VolumeError> {
        let encoded_length = length.to_be_bytes();
        self.connection()
            .execute(
                "INSERT INTO live (key, length) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
                params![key, encoded_length.as_slice()],
            )
            .map_err(|_| unavailable("write Managed collection mark"))?;
        if self.length(key)? != Some(length) {
            return Err(corrupt("one Managed object has conflicting lengths"));
        }
        Ok(())
    }

    fn contains(&self, key: &str, length: u64) -> Result<bool, VolumeError> {
        match self.length(key)? {
            Some(expected) if expected == length => Ok(true),
            Some(_) => Err(corrupt("live Managed object length is invalid")),
            None => Ok(false),
        }
    }

    fn length(&self, key: &str) -> Result<Option<u64>, VolumeError> {
        let bytes = self
            .connection()
            .query_row("SELECT length FROM live WHERE key=?1", [key], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .optional()
            .map_err(|_| unavailable("read Managed collection mark"))?;
        bytes
            .map(|bytes| {
                bytes
                    .try_into()
                    .map(u64::from_be_bytes)
                    .map_err(|_| corrupt("Managed collection mark length is invalid"))
            })
            .transpose()
    }

    fn connection(&self) -> &Connection {
        self.connection
            .as_ref()
            .expect("the mark store connection is open")
    }
}

impl Drop for LiveObjects {
    fn drop(&mut self) {
        drop(self.connection.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

fn valid_object_key(path: &str) -> bool {
    let Some(suffix) = path.strip_prefix(OBJECT_PREFIX) else {
        return false;
    };
    let Some((kind, suffix)) = suffix.split_once('/') else {
        return false;
    };
    let Some((prefix, digest)) = suffix.split_once('/') else {
        return false;
    };
    matches!(kind, "commit" | "meta" | "raw")
        && prefix.len() == 2
        && digest.len() == 64
        && prefix == &digest[..2]
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn conflict(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Conflict, message)
}

fn corrupt(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Corrupt, message)
}

fn unavailable(message: &'static str) -> VolumeError {
    VolumeError::new(VolumeErrorKind::Unavailable, message)
}
