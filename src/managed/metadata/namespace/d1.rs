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

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::change::NamespaceChange;
use super::stored::{
    StoredDirectoryEntry, StoredDirectoryPrecondition, StoredFileVersion, StoredNode,
    StoredNodePrecondition,
};
use super::validation::{validate_publication, validate_snapshot};
use super::{
    DirectoryRecord, NamespaceGcSweep, NamespacePublication, NamespaceSnapshot, managed_generation,
    managed_generation_number,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, FileVersionId, NodeId, OperationId, VolumeId,
};
use crate::managed::metadata::d1::{D1Session, D1Statement, statement};
use crate::managed::{ManagedError, ManagedErrorKind};

#[cfg(test)]
use super::NodeRecord;
#[cfg(test)]
use crate::filesystem::{NodeAttributes, NodeKind};

const HEADS: &str = "ofs_managed_v1_heads";
const NODES: &str = "ofs_managed_v1_nodes";
const DIRECTORIES: &str = "ofs_managed_v1_directories";
const TRANSACTIONS: &str = "ofs_managed_v1_change_transactions";
const RESULTS: &str = "ofs_managed_v1_operation_results";
const CHECKPOINTS: &str = "ofs_managed_v1_checkpoints";
const CHECKPOINT_INTERVAL: u64 = 64;
const SCHEMA_RESULTS: usize = 6;

#[derive(Clone, Debug)]
pub(crate) struct D1NamespaceObservation {
    pub(crate) snapshot: NamespaceSnapshot,
    revision: u64,
    maintenance_epoch: u64,
    maintenance_owner: Option<[u8; 16]>,
    gc_sweep: Option<NamespaceGcSweep>,
}

impl D1NamespaceObservation {
    pub(crate) fn gc_sweep(&self) -> Option<NamespaceGcSweep> {
        self.gc_sweep
    }
}

#[derive(Clone)]
pub(crate) struct D1Namespace {
    volume_id: VolumeId,
    session: D1Session,
}

impl D1Namespace {
    pub(crate) fn new(volume_id: VolumeId, session: D1Session) -> Self {
        Self { volume_id, session }
    }

    pub(crate) async fn observe(&self) -> Result<Option<D1NamespaceObservation>, ManagedError> {
        let mut batch = schema_statements();
        batch.extend([
            statement(
            format!(
                "SELECT h.revision, h.target_sequence, h.target_operation, h.root_node, h.checkpoint_sequence, h.maintenance_epoch, h.maintenance_state, h.maintenance_owner, h.maintenance_fixed_sequence, h.maintenance_fixed_operation, c.snapshot_json FROM {HEADS} h JOIN {CHECKPOINTS} c ON c.store_key = h.store_key AND c.target_sequence = h.checkpoint_sequence WHERE h.store_key = ? AND h.volume_id = ?"
            ),
            vec![self.store_key().into(), self.volume().into()],
            ),
            statement(
                format!("SELECT operation_id, parent_sequence, parent_operation, target_sequence, payload_json FROM {TRANSACTIONS} WHERE store_key = ? AND target_sequence > COALESCE((SELECT checkpoint_sequence FROM {HEADS} WHERE store_key = ?), 0) AND status = 'committed' ORDER BY target_sequence"),
                vec![self.store_key().into(), self.store_key().into()],
            ),
        ]);
        let results = self.session.query(batch, "read Managed namespace").await?;
        let heads = rows(&results, SCHEMA_RESULTS, "read Managed namespace")?;
        let [head] = heads else {
            return if heads.is_empty() {
                Ok(None)
            } else {
                Err(corrupt(
                    "read Managed namespace",
                    "D1 returned duplicate heads",
                ))
            };
        };
        let revision = integer(head, "revision", "read Managed namespace")?;
        let cursor = stored_cursor(
            integer(head, "target_sequence", "read Managed namespace")?,
            Some(text(head, "target_operation", "read Managed namespace")?),
        )?;
        let maintenance_epoch = integer(head, "maintenance_epoch", "read Managed namespace")?;
        let maintenance_owner = nullable_text(head, "maintenance_owner", "read Managed namespace")?
            .map(decode_hex)
            .transpose()?;
        let gc_sweep = gc_sweep(head, maintenance_epoch, cursor)?;
        let root = node_id(text(head, "root_node", "read Managed namespace")?)?;
        let checkpoint_sequence = integer(head, "checkpoint_sequence", "read Managed namespace")?;
        let checkpoint: StoredSnapshot = decode(
            text(head, "snapshot_json", "read Managed namespace")?,
            "read Managed namespace",
        )?;
        let mut snapshot = checkpoint.into_snapshot()?;
        if snapshot.volume_id != self.volume_id || snapshot.cursor.sequence() != checkpoint_sequence
        {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint does not match the namespace head",
            ));
        }
        validate_snapshot(&snapshot)
            .map_err(|_| corrupt("read Managed namespace", "checkpoint is invalid"))?;
        snapshot = replay_tail(
            snapshot,
            cursor,
            root,
            rows(&results, SCHEMA_RESULTS + 1, "read Managed namespace")?,
        )?;
        Ok(Some(D1NamespaceObservation {
            snapshot,
            revision,
            maintenance_epoch,
            maintenance_owner,
            gc_sweep,
        }))
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<&D1NamespaceObservation>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        if publication.target.volume_id != self.volume_id {
            return Err(invalid(
                "publish Managed namespace",
                "publication belongs to another volume",
            ));
        }
        if observed.is_some_and(|value| value.gc_sweep().is_some()) {
            return Ok(CommitOutcome::Conflict {
                observed: observed.expect("checked above").snapshot.cursor,
            });
        }
        let base = observed.map(|value| &value.snapshot);
        if !validate_publication(publication, base)? {
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |state| state.cursor),
            });
        }
        let delta = PublicationDelta::new(publication, base);
        let payload = encode(&delta.change, "publish Managed namespace")?;
        let request_digest = hex(&Sha256::digest(payload.as_bytes()));
        let operation = hex(publication.operation.as_bytes());
        let target_sequence = sqlite_integer(publication.target.cursor.sequence())?;
        let parent_sequence = sqlite_integer(publication.parent.sequence())?;
        let parent_operation = publication
            .parent
            .operation()
            .map(|value| hex(value.as_bytes()));
        let checkpoint = is_checkpoint(publication.target.cursor.sequence())
            .then(|| {
                encode(
                    &StoredSnapshot::from(&publication.target),
                    "publish Managed namespace",
                )
            })
            .transpose()?;
        let mut batch = schema_statements();
        batch.extend([
            statement(
                format!(
                    "INSERT OR IGNORE INTO {TRANSACTIONS} (store_key, operation_id, request_digest, payload_json, parent_sequence, parent_operation, target_sequence, status, eligible) SELECT ?, ?, ?, ?, ?, ?, ?, 'pending', 0 WHERE NOT EXISTS (SELECT 1 FROM {RESULTS} WHERE store_key = ? AND operation_id = ?)"
                ),
                vec![
                    self.store_key().into(),
                    operation.clone().into(),
                    request_digest.clone().into(),
                    payload.clone().into(),
                    parent_sequence.into(),
                    option_text(parent_operation.clone()),
                    target_sequence.into(),
                    self.store_key().into(),
                    operation.clone().into(),
                ],
            ),
            statement(
                format!(
                    "SELECT request_digest FROM {RESULTS} WHERE store_key = ? AND operation_id = ? UNION ALL SELECT request_digest FROM {TRANSACTIONS} WHERE store_key = ? AND operation_id = ? AND NOT EXISTS (SELECT 1 FROM {RESULTS} WHERE store_key = ? AND operation_id = ?)"
                ),
                vec![
                    self.store_key().into(), operation.clone().into(),
                    self.store_key().into(), operation.clone().into(),
                    self.store_key().into(), operation.clone().into(),
                ],
            ),
        ]);
        batch.push(self.eligibility_statement(observed, publication, &payload)?);
        let guard = format!(
            "EXISTS (SELECT 1 FROM {TRANSACTIONS} WHERE store_key = ? AND operation_id = ? AND payload_json = ? AND status = 'pending' AND eligible = 1)"
        );
        let guarded = || {
            vec![
                self.store_key().into(),
                operation.clone().into(),
                payload.clone().into(),
            ]
        };
        batch.extend([
            delete_records(
                NODES,
                "node_id",
                self.store_key(),
                &delta.deleted_nodes,
                &guard,
                guarded(),
            )?,
            put_records(
                NODES,
                "node_id",
                self.store_key(),
                &delta.nodes,
                &guard,
                guarded(),
            )?,
            delete_records(
                DIRECTORIES,
                "node_id",
                self.store_key(),
                &delta.deleted_directories,
                &guard,
                guarded(),
            )?,
            put_records(
                DIRECTORIES,
                "node_id",
                self.store_key(),
                &delta.directories,
                &guard,
                guarded(),
            )?,
        ]);
        batch.push(match observed {
            Some(observed) => statement(
                format!(
                    "UPDATE {HEADS} SET revision = revision + 1, target_sequence = ?, target_operation = ?, root_node = ? WHERE store_key = ? AND volume_id = ? AND revision = ? AND target_sequence = ? AND target_operation IS ? AND maintenance_state = 'idle' AND {guard} RETURNING revision"
                ),
                guarded_params(vec![
                    target_sequence.into(),
                    operation.clone().into(),
                    hex(publication.target.root.as_bytes()).into(),
                    self.store_key().into(),
                    self.volume().into(),
                    sqlite_integer(observed.revision)?.into(),
                    parent_sequence.into(),
                    option_text(parent_operation.clone()),
                ], guarded()),
            ),
            None => statement(
                format!(
                    "INSERT OR IGNORE INTO {HEADS} (store_key, volume_id, revision, target_sequence, target_operation, root_node, checkpoint_sequence, maintenance_epoch, maintenance_state, maintenance_owner, maintenance_fixed_sequence, maintenance_fixed_operation) SELECT ?, ?, 1, ?, ?, ?, 0, 0, 'idle', NULL, NULL, NULL WHERE ? = 0 AND ? IS NULL AND {guard} RETURNING revision"
                ),
                guarded_params(vec![
                    self.store_key().into(),
                    self.volume().into(),
                    target_sequence.into(),
                    operation.clone().into(),
                    hex(publication.target.root.as_bytes()).into(),
                    parent_sequence.into(),
                    option_text(parent_operation.clone()),
                ], guarded()),
            ),
        });
        batch.extend([
            statement(
                format!(
                    "INSERT OR IGNORE INTO {RESULTS} (store_key, operation_id, request_digest, target_sequence) SELECT ?, ?, ?, ? FROM {HEADS} WHERE store_key = ? AND volume_id = ? AND target_sequence = ? AND target_operation = ?"
                ),
                vec![
                    self.store_key().into(),
                    operation.clone().into(),
                    request_digest.clone().into(),
                    target_sequence.into(),
                    self.store_key().into(),
                    self.volume().into(),
                    target_sequence.into(),
                    operation.clone().into(),
                ],
            ),
            statement(
                format!("UPDATE {TRANSACTIONS} SET status = 'committed' WHERE store_key = ? AND operation_id = ? AND status = 'pending' AND EXISTS (SELECT 1 FROM {RESULTS} WHERE store_key = ? AND operation_id = ? AND request_digest = ?)"),
                vec![
                    self.store_key().into(), operation.clone().into(),
                    self.store_key().into(), operation.clone().into(), request_digest.clone().into(),
                ],
            ),
            statement(
                format!(
                    "DELETE FROM {TRANSACTIONS} WHERE store_key = ? AND operation_id = ? AND status = 'pending'"
                ),
                vec![
                    self.store_key().into(),
                    operation.clone().into(),
                ],
            ),
            put_checkpoint(
                checkpoint.as_deref(),
                target_sequence,
                &operation,
                &request_digest,
                self.store_key(),
            )?,
            statement(
                format!("UPDATE {HEADS} SET checkpoint_sequence = ? WHERE store_key = ? AND target_sequence = ? AND target_operation = ? AND EXISTS (SELECT 1 FROM {CHECKPOINTS} WHERE store_key = ? AND target_sequence = ?)"),
                vec![
                    target_sequence.into(), self.store_key().into(), target_sequence.into(),
                    operation.clone().into(), self.store_key().into(), target_sequence.into(),
                ],
            ),
            statement(
                format!("DELETE FROM {CHECKPOINTS} WHERE store_key = ? AND target_sequence < (SELECT checkpoint_sequence FROM {HEADS} WHERE store_key = ?)"),
                vec![self.store_key().into(), self.store_key().into()],
            ),
            statement(
                format!("DELETE FROM {TRANSACTIONS} WHERE store_key = ? AND status = 'committed' AND target_sequence <= COALESCE((SELECT checkpoint_sequence FROM {HEADS} WHERE store_key = ?), 0)"),
                vec![self.store_key().into(), self.store_key().into()],
            ),
            statement(
                format!(
                    "SELECT target_sequence FROM {RESULTS} WHERE store_key = ? AND operation_id = ?"
                ),
                vec![self.store_key().into(), operation.into()],
            ),
        ]);

        let results = match self.session.query(batch, "publish Managed namespace").await {
            Ok(results) => results,
            Err(_) => {
                return match self.resolve(publication.operation).await {
                    Ok(CommitOutcome::Committed(cursor)) => Ok(CommitOutcome::Committed(cursor)),
                    Ok(CommitOutcome::Conflict { observed }) => {
                        Ok(CommitOutcome::Conflict { observed })
                    }
                    Ok(CommitOutcome::Absent | CommitOutcome::Unknown) | Err(_) => {
                        Ok(CommitOutcome::Unknown)
                    }
                };
            }
        };
        let transaction = rows(&results, SCHEMA_RESULTS + 1, "publish Managed namespace")?;
        let [transaction] = transaction else {
            return Err(corrupt(
                "publish Managed namespace",
                "D1 omitted the transaction",
            ));
        };
        validate_request_digest(transaction, &request_digest)?;
        let result = rows(&results, results.len() - 1, "publish Managed namespace")?;
        if let [result] = result {
            return operation_result(result, publication.operation, "publish Managed namespace");
        }
        if result.len() > 1 {
            return Err(corrupt(
                "publish Managed namespace",
                "D1 returned duplicate operation results",
            ));
        }
        self.outcome_after_race(publication.operation).await
    }

    pub(crate) async fn resolve(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "SELECT target_sequence FROM {RESULTS} WHERE store_key = ? AND operation_id = ?"
            ),
            vec![self.store_key().into(), hex(operation.as_bytes()).into()],
        ));
        let results = match self
            .session
            .query(batch, "resolve Managed publication")
            .await
        {
            Ok(results) => results,
            Err(error) if error.kind() == ManagedErrorKind::Unavailable => {
                return Ok(CommitOutcome::Unknown);
            }
            Err(error) => return Err(error),
        };
        resolve_operation_rows(
            rows(&results, SCHEMA_RESULTS, "resolve Managed publication")?,
            operation,
            "resolve Managed publication",
        )
    }

    pub(crate) async fn begin_gc(
        &self,
        observed: &D1NamespaceObservation,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        if observed.snapshot.volume_id != self.volume_id {
            return Err(invalid(
                "begin Managed namespace GC",
                "observation belongs to another volume",
            ));
        }
        if observed.gc_sweep().is_some() {
            return Err(conflict(
                "begin Managed namespace GC",
                "another namespace GC is active",
            ));
        }
        let owner = *OperationId::generate().as_bytes();
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {HEADS} SET revision = revision + 1, maintenance_epoch = maintenance_epoch + 1, maintenance_state = 'sweeping', maintenance_owner = ?, maintenance_fixed_sequence = target_sequence, maintenance_fixed_operation = target_operation WHERE store_key = ? AND volume_id = ? AND revision = ? AND target_sequence = ? AND target_operation = ? AND maintenance_epoch = ? AND maintenance_state = 'idle' RETURNING maintenance_epoch"
            ),
            vec![
                hex(&owner).into(),
                self.store_key().into(),
                self.volume().into(),
                sqlite_integer(observed.revision)?.into(),
                sqlite_integer(observed.snapshot.cursor.sequence())?.into(),
                hex(
                    observed
                        .snapshot
                        .cursor
                        .operation()
                        .expect("a namespace head is not genesis")
                        .as_bytes(),
                )
                .into(),
                sqlite_integer(observed.maintenance_epoch)?.into(),
            ],
        ));
        let results = self
            .session
            .query(batch, "begin Managed namespace GC")
            .await?;
        let changed = rows(&results, SCHEMA_RESULTS, "begin Managed namespace GC")?;
        if let [row] = changed {
            return Ok(NamespaceGcSweep::new(
                integer(row, "maintenance_epoch", "begin Managed namespace GC")?,
                owner,
                observed.snapshot.cursor,
            ));
        }
        if !changed.is_empty() {
            return Err(corrupt(
                "begin Managed namespace GC",
                "D1 returned duplicate namespace heads",
            ));
        }
        Err(conflict(
            "begin Managed namespace GC",
            "namespace authority changed",
        ))
    }

    pub(crate) async fn resume_gc(
        &self,
        observed: &D1NamespaceObservation,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        if observed.snapshot.volume_id != self.volume_id {
            return Err(invalid(
                "resume Managed namespace GC",
                "observation belongs to another volume",
            ));
        }
        let active = observed.gc_sweep().ok_or_else(|| {
            conflict(
                "resume Managed namespace GC",
                "no interrupted namespace GC is active",
            )
        })?;
        let owner = *OperationId::generate().as_bytes();
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {HEADS} SET revision = revision + 1, maintenance_owner = ? WHERE store_key = ? AND volume_id = ? AND revision = ? AND maintenance_epoch = ? AND maintenance_state = 'sweeping' AND maintenance_owner = ? AND maintenance_fixed_sequence = ? AND maintenance_fixed_operation = ? RETURNING revision"
            ),
            vec![
                hex(&owner).into(),
                self.store_key().into(),
                self.volume().into(),
                sqlite_integer(observed.revision)?.into(),
                sqlite_integer(active.epoch())?.into(),
                hex(&active.owner()).into(),
                sqlite_integer(active.fixed_cursor().sequence())?.into(),
                hex(
                    active
                        .fixed_cursor()
                        .operation()
                        .expect("a namespace GC cursor is not genesis")
                        .as_bytes(),
                )
                .into(),
            ],
        ));
        let results = self
            .session
            .query(batch, "resume Managed namespace GC")
            .await?;
        let changed = rows(&results, SCHEMA_RESULTS, "resume Managed namespace GC")?;
        match changed {
            [_] => Ok(NamespaceGcSweep::new(
                active.epoch(),
                owner,
                active.fixed_cursor(),
            )),
            [] => Err(conflict(
                "resume Managed namespace GC",
                "namespace authority changed",
            )),
            _ => Err(corrupt(
                "resume Managed namespace GC",
                "D1 returned duplicate namespace heads",
            )),
        }
    }

    pub(crate) async fn finish_gc(&self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        let mut batch = schema_statements();
        batch.push(statement(
            format!(
                "UPDATE {HEADS} SET revision = revision + 1, maintenance_state = 'idle', maintenance_fixed_sequence = NULL, maintenance_fixed_operation = NULL WHERE store_key = ? AND volume_id = ? AND maintenance_epoch = ? AND maintenance_owner = ? AND maintenance_state = 'sweeping' AND maintenance_fixed_sequence = ? AND maintenance_fixed_operation = ? RETURNING revision"
            ),
            vec![
                self.store_key().into(),
                self.volume().into(),
                sqlite_integer(sweep.epoch())?.into(),
                hex(&sweep.owner()).into(),
                sqlite_integer(sweep.fixed_cursor().sequence())?.into(),
                hex(
                    sweep
                        .fixed_cursor()
                        .operation()
                        .expect("a namespace GC cursor is not genesis")
                        .as_bytes(),
                )
                .into(),
            ],
        ));
        let results = self
            .session
            .query(batch, "finish Managed namespace GC")
            .await?;
        let changed = rows(&results, SCHEMA_RESULTS, "finish Managed namespace GC")?;
        if changed.len() == 1 {
            return Ok(());
        }
        if !changed.is_empty() {
            return Err(corrupt(
                "finish Managed namespace GC",
                "D1 returned duplicate namespace heads",
            ));
        }
        let current = self.observe().await?.ok_or_else(|| {
            conflict("finish Managed namespace GC", "namespace authority changed")
        })?;
        if current.maintenance_epoch == sweep.epoch()
            && current.gc_sweep().is_none()
            && current.maintenance_owner == Some(sweep.owner())
        {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed namespace GC",
                "GC sweep token does not match the authority",
            ))
        }
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        let outcome = self.resolve(operation).await?;
        if matches!(
            outcome,
            CommitOutcome::Committed(_) | CommitOutcome::Conflict { .. } | CommitOutcome::Unknown
        ) {
            return Ok(outcome);
        }
        let observed = self
            .observe()
            .await?
            .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor);
        Ok(CommitOutcome::Conflict { observed })
    }

    fn store_key(&self) -> String {
        self.session.store_key().to_owned()
    }

    fn volume(&self) -> String {
        hex(self.volume_id.as_bytes())
    }

    fn eligibility_statement(
        &self,
        observed: Option<&D1NamespaceObservation>,
        publication: &NamespacePublication,
        payload: &str,
    ) -> Result<D1Statement, ManagedError> {
        let operation = hex(publication.operation.as_bytes());
        let mut sql = format!(
            "UPDATE {TRANSACTIONS} SET eligible = 1 WHERE store_key = ? AND operation_id = ? AND payload_json = ? AND status = 'pending'"
        );
        let mut params = vec![
            self.store_key().into(),
            operation.into(),
            payload.to_owned().into(),
        ];
        match observed {
            Some(observed) => {
                sql.push_str(&format!(" AND EXISTS (SELECT 1 FROM {HEADS} WHERE store_key = ? AND volume_id = ? AND revision = ? AND target_sequence = ? AND target_operation IS ? AND maintenance_state = 'idle')"));
                params.extend([
                    self.store_key().into(),
                    self.volume().into(),
                    sqlite_integer(observed.revision)?.into(),
                    sqlite_integer(publication.parent.sequence())?.into(),
                    option_text(
                        publication
                            .parent
                            .operation()
                            .map(|value| hex(value.as_bytes())),
                    ),
                ]);
            }
            None => {
                sql.push_str(&format!(
                    " AND NOT EXISTS (SELECT 1 FROM {HEADS} WHERE store_key = ?)"
                ));
                params.push(self.store_key().into());
            }
        }
        append_preconditions(&mut sql, &mut params, publication, self.store_key())?;
        sql.push_str(" RETURNING eligible");
        Ok(statement(sql, params))
    }
}

fn schema_statements() -> Vec<D1Statement> {
    vec![
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {HEADS} (store_key TEXT PRIMARY KEY, volume_id TEXT NOT NULL, revision INTEGER NOT NULL, target_sequence INTEGER NOT NULL, target_operation TEXT NOT NULL, root_node TEXT NOT NULL, checkpoint_sequence INTEGER NOT NULL, maintenance_epoch INTEGER NOT NULL, maintenance_state TEXT NOT NULL CHECK (maintenance_state IN ('idle', 'sweeping')), maintenance_owner TEXT, maintenance_fixed_sequence INTEGER, maintenance_fixed_operation TEXT)"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {NODES} (store_key TEXT NOT NULL, node_id TEXT NOT NULL, generation INTEGER NOT NULL, PRIMARY KEY (store_key, node_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {DIRECTORIES} (store_key TEXT NOT NULL, node_id TEXT NOT NULL, generation INTEGER NOT NULL, PRIMARY KEY (store_key, node_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {TRANSACTIONS} (store_key TEXT NOT NULL, operation_id TEXT NOT NULL, request_digest TEXT NOT NULL, payload_json TEXT NOT NULL, parent_sequence INTEGER NOT NULL, parent_operation TEXT, target_sequence INTEGER NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending', 'committed')), eligible INTEGER NOT NULL, PRIMARY KEY (store_key, operation_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {RESULTS} (store_key TEXT NOT NULL, operation_id TEXT NOT NULL, request_digest TEXT NOT NULL, target_sequence INTEGER NOT NULL, PRIMARY KEY (store_key, operation_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {CHECKPOINTS} (store_key TEXT NOT NULL, target_sequence INTEGER NOT NULL, snapshot_json TEXT NOT NULL, PRIMARY KEY (store_key, target_sequence))"
            ),
            Vec::new(),
        ),
    ]
}

fn append_preconditions(
    sql: &mut String,
    params: &mut Vec<Value>,
    publication: &NamespacePublication,
    store_key: String,
) -> Result<(), ManagedError> {
    for condition in &publication.expected_nodes {
        match condition.expected_generation.as_ref() {
            Some(generation) => {
                sql.push_str(&format!(" AND EXISTS (SELECT 1 FROM {NODES} WHERE store_key = ? AND node_id = ? AND generation = ?)"));
                params.extend([
                    store_key.clone().into(),
                    hex(condition.node.as_bytes()).into(),
                    sqlite_integer(managed_generation_number(generation).ok_or_else(|| {
                        invalid("publish Managed namespace", "node precondition is invalid")
                    })?)?
                    .into(),
                ]);
            }
            None => {
                sql.push_str(&format!(
                    " AND NOT EXISTS (SELECT 1 FROM {NODES} WHERE store_key = ? AND node_id = ?)"
                ));
                params.extend([
                    store_key.clone().into(),
                    hex(condition.node.as_bytes()).into(),
                ]);
            }
        }
    }
    for condition in &publication.expected_directories {
        match condition.expected_generation.as_ref() {
            Some(generation) => {
                sql.push_str(&format!(" AND EXISTS (SELECT 1 FROM {DIRECTORIES} WHERE store_key = ? AND node_id = ? AND generation = ?)"));
                params.extend([
                    store_key.clone().into(),
                    hex(condition.directory.as_bytes()).into(),
                    sqlite_integer(managed_generation_number(generation).ok_or_else(|| {
                        invalid(
                            "publish Managed namespace",
                            "directory precondition is invalid",
                        )
                    })?)?
                    .into(),
                ]);
            }
            None => {
                sql.push_str(&format!(" AND NOT EXISTS (SELECT 1 FROM {DIRECTORIES} WHERE store_key = ? AND node_id = ?)"));
                params.extend([
                    store_key.clone().into(),
                    hex(condition.directory.as_bytes()).into(),
                ]);
            }
        }
    }
    Ok(())
}

fn guarded_params(mut params: Vec<Value>, guard: Vec<Value>) -> Vec<Value> {
    params.extend(guard);
    params
}

fn delete_records(
    table: &str,
    key: &str,
    store_key: String,
    deleted: &[String],
    guard: &str,
    guard_params: Vec<Value>,
) -> Result<D1Statement, ManagedError> {
    let params = guarded_params(
        vec![
            store_key.into(),
            encode(&deleted, "publish Managed namespace")?.into(),
        ],
        guard_params,
    );
    Ok(statement(
        format!(
            "DELETE FROM {table} WHERE store_key = ? AND {key} IN (SELECT value FROM json_each(?)) AND {guard}"
        ),
        params,
    ))
}

fn put_records(
    table: &str,
    key: &str,
    store_key: String,
    records: &[RecordRow],
    guard: &str,
    guard_params: Vec<Value>,
) -> Result<D1Statement, ManagedError> {
    let params = guarded_params(
        vec![
            store_key.into(),
            encode(&records, "publish Managed namespace")?.into(),
        ],
        guard_params,
    );
    Ok(statement(
        format!(
            "INSERT INTO {table} (store_key, {key}, generation) SELECT ?, json_extract(value, '$.key'), json_extract(value, '$.generation') FROM json_each(?) WHERE {guard} ON CONFLICT(store_key, {key}) DO UPDATE SET generation = excluded.generation"
        ),
        params,
    ))
}

fn put_checkpoint(
    checkpoint: Option<&str>,
    target_sequence: i64,
    operation: &str,
    request_digest: &str,
    store_key: String,
) -> Result<D1Statement, ManagedError> {
    let checkpoint = checkpoint.map_or(Value::Null, |value| value.to_owned().into());
    Ok(statement(
        format!(
            "INSERT OR IGNORE INTO {CHECKPOINTS} (store_key, target_sequence, snapshot_json) SELECT ?, ?, ? WHERE ? IS NOT NULL AND EXISTS (SELECT 1 FROM {RESULTS} WHERE store_key = ? AND operation_id = ? AND request_digest = ?)"
        ),
        vec![
            store_key.clone().into(),
            target_sequence.into(),
            checkpoint.clone(),
            checkpoint,
            store_key.into(),
            operation.to_owned().into(),
            request_digest.to_owned().into(),
        ],
    ))
}

const fn is_checkpoint(sequence: u64) -> bool {
    sequence == 1 || sequence % CHECKPOINT_INTERVAL == 0
}

fn validate_tail(
    mut cursor: ChangeCursor,
    head: ChangeCursor,
    rows: &[Value],
) -> Result<(), ManagedError> {
    if rows.len() >= CHECKPOINT_INTERVAL as usize {
        return Err(corrupt(
            "read Managed namespace",
            "change tail exceeds its recovery bound",
        ));
    }
    for row in rows {
        let parent = stored_cursor(
            integer(row, "parent_sequence", "read Managed namespace")?,
            row.get("parent_operation").and_then(Value::as_str),
        )?;
        let target = stored_cursor(
            integer(row, "target_sequence", "read Managed namespace")?,
            Some(text(row, "operation_id", "read Managed namespace")?),
        )?;
        if parent != cursor || parent.sequence().checked_add(1) != Some(target.sequence()) {
            return Err(corrupt(
                "read Managed namespace",
                "change tail ancestry is invalid",
            ));
        }
        cursor = target;
    }
    if cursor != head {
        return Err(corrupt(
            "read Managed namespace",
            "change tail does not reach the head",
        ));
    }
    Ok(())
}

fn replay_tail(
    mut snapshot: NamespaceSnapshot,
    head: ChangeCursor,
    root: NodeId,
    rows: &[Value],
) -> Result<NamespaceSnapshot, ManagedError> {
    validate_tail(snapshot.cursor, head, rows)?;
    for row in rows {
        let stored: StoredChange = decode(
            text(row, "payload_json", "read Managed namespace")?,
            "read Managed namespace",
        )?;
        let change = stored.into_change(snapshot.volume_id)?;
        let row_cursor = stored_cursor(
            integer(row, "target_sequence", "read Managed namespace")?,
            Some(text(row, "operation_id", "read Managed namespace")?),
        )?;
        if change.parent != snapshot.cursor || change.cursor != row_cursor {
            return Err(corrupt(
                "read Managed namespace",
                "change record disagrees with its transaction row",
            ));
        }
        snapshot = change.apply(Some(snapshot))?;
    }
    if snapshot.cursor != head || snapshot.root != root {
        return Err(corrupt(
            "read Managed namespace",
            "replayed namespace does not match its head",
        ));
    }
    Ok(snapshot)
}

fn stored_cursor(sequence: u64, operation: Option<&str>) -> Result<ChangeCursor, ManagedError> {
    match (sequence, operation) {
        (0, None) => Ok(ChangeCursor::Genesis),
        (sequence, Some(operation)) => Ok(ChangeCursor::at(
            NonZeroU64::new(sequence)
                .ok_or_else(|| corrupt("read Managed namespace", "cursor is invalid"))?,
            OperationId::from_bytes(decode_hex(operation)?),
        )),
        _ => Err(corrupt("read Managed namespace", "cursor is invalid")),
    }
}

fn gc_sweep(
    row: &Value,
    epoch: u64,
    head: ChangeCursor,
) -> Result<Option<NamespaceGcSweep>, ManagedError> {
    let fixed_sequence =
        nullable_integer(row, "maintenance_fixed_sequence", "read Managed namespace")?;
    let fixed_operation =
        nullable_text(row, "maintenance_fixed_operation", "read Managed namespace")?;
    let owner = nullable_text(row, "maintenance_owner", "read Managed namespace")?
        .map(decode_hex)
        .transpose()?;
    match (
        text(row, "maintenance_state", "read Managed namespace")?,
        owner,
        fixed_sequence,
        fixed_operation,
    ) {
        ("idle", _, None, None) => Ok(None),
        ("sweeping", Some(owner), Some(sequence), operation) if epoch > 0 => {
            let fixed = stored_cursor(sequence, operation)?;
            if fixed != head {
                return Err(corrupt(
                    "read Managed namespace",
                    "GC sweep is not fixed at the namespace head",
                ));
            }
            Ok(Some(NamespaceGcSweep::new(epoch, owner, fixed)))
        }
        _ => Err(corrupt(
            "read Managed namespace",
            "namespace maintenance state is invalid",
        )),
    }
}

fn node_id(value: &str) -> Result<NodeId, ManagedError> {
    Ok(NodeId::from_bytes(decode_hex(value)?))
}

fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N], ManagedError> {
    if value.len() != N * 2 {
        return Err(corrupt(
            "read Managed namespace",
            "record identity is invalid",
        ));
    }
    let mut output = [0; N];
    let nibble = |byte| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    };
    for (output, input) in output.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = nibble(input[0])
            .ok_or_else(|| corrupt("read Managed namespace", "record identity is invalid"))?;
        let low = nibble(input[1])
            .ok_or_else(|| corrupt("read Managed namespace", "record identity is invalid"))?;
        *output = high << 4 | low;
    }
    Ok(output)
}

fn rows<'a>(
    results: &'a [crate::managed::metadata::d1::D1Result],
    index: usize,
    action: &'static str,
) -> Result<&'a [Value], ManagedError> {
    results
        .get(index)
        .map(|result| result.results.as_slice())
        .ok_or_else(|| corrupt(action, "D1 omitted a query result"))
}

fn text<'a>(row: &'a Value, field: &str, action: &'static str) -> Result<&'a str, ManagedError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid namespace row"))
}

fn integer(row: &Value, field: &str, action: &'static str) -> Result<u64, ManagedError> {
    row.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt(action, "D1 returned an invalid namespace row"))
}

fn nullable_integer(
    row: &Value,
    field: &str,
    action: &'static str,
) -> Result<Option<u64>, ManagedError> {
    match row.get(field) {
        Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| corrupt(action, "D1 returned an invalid namespace row")),
        None => Err(corrupt(action, "D1 returned an invalid namespace row")),
    }
}

fn nullable_text<'a>(
    row: &'a Value,
    field: &str,
    action: &'static str,
) -> Result<Option<&'a str>, ManagedError> {
    match row.get(field) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        _ => Err(corrupt(action, "D1 returned an invalid namespace row")),
    }
}

fn operation_result(
    row: &Value,
    operation: OperationId,
    action: &'static str,
) -> Result<CommitOutcome, ManagedError> {
    let sequence = integer(row, "target_sequence", action)?;
    let sequence = NonZeroU64::new(sequence)
        .ok_or_else(|| corrupt(action, "committed result sequence is invalid"))?;
    Ok(CommitOutcome::Committed(ChangeCursor::at(
        sequence, operation,
    )))
}

fn resolve_operation_rows(
    rows: &[Value],
    operation: OperationId,
    action: &'static str,
) -> Result<CommitOutcome, ManagedError> {
    match rows {
        [] => Ok(CommitOutcome::Absent),
        [row] => operation_result(row, operation, action),
        _ => Err(corrupt(action, "D1 returned duplicate operation results")),
    }
}

fn validate_request_digest(row: &Value, request_digest: &str) -> Result<(), ManagedError> {
    if text(row, "request_digest", "publish Managed namespace")? == request_digest {
        return Ok(());
    }
    Err(ManagedError::new(
        ManagedErrorKind::Conflict,
        "publish Managed namespace",
        "operation identity was reused with another payload",
    ))
}

fn sqlite_integer(value: u64) -> Result<i64, ManagedError> {
    i64::try_from(value).map_err(|_| {
        invalid(
            "publish Managed namespace",
            "change sequence exceeds D1 integer range",
        )
    })
}

fn option_text(value: Option<String>) -> Value {
    value.map_or(Value::Null, Value::String)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn encode(value: &impl Serialize, action: &'static str) -> Result<String, ManagedError> {
    serde_json::to_string(value).map_err(|_| invalid(action, "namespace record cannot be encoded"))
}

fn decode<'a, T: Deserialize<'a>>(value: &'a str, action: &'static str) -> Result<T, ManagedError> {
    serde_json::from_str(value).map_err(|_| corrupt(action, "namespace record is invalid"))
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn conflict(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Conflict, action, message)
}

#[derive(Serialize)]
struct RecordRow {
    key: String,
    generation: u64,
}

struct PublicationDelta {
    change: StoredChange,
    nodes: Vec<RecordRow>,
    deleted_nodes: Vec<String>,
    directories: Vec<RecordRow>,
    deleted_directories: Vec<String>,
}

impl PublicationDelta {
    fn new(publication: &NamespacePublication, base: Option<&NamespaceSnapshot>) -> Self {
        let change = NamespaceChange::from_publication(publication, base);
        let nodes = change
            .put_nodes
            .iter()
            .map(|node| RecordRow {
                key: hex(node.id.as_bytes()),
                generation: managed_generation_number(&node.generation)
                    .expect("validated Managed node generation"),
            })
            .collect();
        let deleted_nodes = change
            .remove_nodes
            .iter()
            .map(|id| hex(id.as_bytes()))
            .collect();
        let directories = change
            .put_directories
            .iter()
            .map(|directory| RecordRow {
                key: hex(directory.node.as_bytes()),
                generation: managed_generation_number(&directory.generation)
                    .expect("validated Managed directory generation"),
            })
            .collect();
        let deleted_directories = change
            .remove_directories
            .iter()
            .map(|id| hex(id.as_bytes()))
            .collect();
        let mut effects =
            change
                .remove_nodes
                .iter()
                .map(|id| StoredEffect::DeleteNode(*id.as_bytes()))
                .chain(
                    change
                        .put_nodes
                        .iter()
                        .map(|node| StoredEffect::PutNode(StoredNode::from(node))),
                )
                .chain(
                    change
                        .remove_directories
                        .iter()
                        .map(|id| StoredEffect::DeleteDirectory(*id.as_bytes())),
                )
                .chain(
                    change.put_directories.iter().map(|directory| {
                        StoredEffect::PutDirectory(StoredDirectory::from(directory))
                    }),
                )
                .chain(
                    change
                        .remove_file_versions
                        .iter()
                        .map(|id| StoredEffect::DeleteFileVersion(*id.as_bytes())),
                )
                .chain(
                    change.put_file_versions.iter().map(|version| {
                        StoredEffect::PutFileVersion(StoredFileVersion::from(version))
                    }),
                )
                .collect::<Vec<_>>();
        effects.push(StoredEffect::SetRoot(*change.root.as_bytes()));
        let stored_change = StoredChange {
            operation: *change.operation.as_bytes(),
            parent: change.parent.into(),
            target: change.cursor.into(),
            root: *change.root.as_bytes(),
            expected_nodes: change
                .expected_nodes
                .iter()
                .map(StoredNodePrecondition::from)
                .collect(),
            expected_directories: change
                .expected_directories
                .iter()
                .map(StoredDirectoryPrecondition::from)
                .collect(),
            effects,
        };
        Self {
            change: stored_change,
            nodes,
            deleted_nodes,
            directories,
            deleted_directories,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredChange {
    operation: [u8; 16],
    parent: StoredCursor,
    target: StoredCursor,
    root: [u8; 16],
    expected_nodes: Vec<StoredNodePrecondition>,
    expected_directories: Vec<StoredDirectoryPrecondition>,
    effects: Vec<StoredEffect>,
}

impl StoredChange {
    fn into_change(self, volume_id: VolumeId) -> Result<NamespaceChange, ManagedError> {
        let mut put_nodes = Vec::new();
        let mut remove_nodes = Vec::new();
        let mut put_directories = Vec::new();
        let mut remove_directories = Vec::new();
        let mut put_file_versions = Vec::new();
        let mut remove_file_versions = Vec::new();
        let mut effect_root = None;
        for effect in self.effects {
            match effect {
                StoredEffect::PutNode(node) => put_nodes.push(node.into_record()),
                StoredEffect::DeleteNode(id) => remove_nodes.push(NodeId::from_bytes(id)),
                StoredEffect::PutDirectory(directory) => {
                    put_directories.push(directory.into_record());
                }
                StoredEffect::DeleteDirectory(id) => {
                    remove_directories.push(NodeId::from_bytes(id));
                }
                StoredEffect::PutFileVersion(version) => {
                    put_file_versions.push(version.into_record());
                }
                StoredEffect::DeleteFileVersion(id) => {
                    remove_file_versions.push(FileVersionId::from_bytes(id));
                }
                StoredEffect::SetRoot(root) if effect_root.replace(root).is_none() => {}
                StoredEffect::SetRoot(_) => {
                    return Err(corrupt(
                        "read Managed namespace",
                        "change contains duplicate root effects",
                    ));
                }
            }
        }
        if effect_root != Some(self.root) {
            return Err(corrupt(
                "read Managed namespace",
                "change root effect is invalid",
            ));
        }
        Ok(NamespaceChange {
            volume_id,
            operation: OperationId::from_bytes(self.operation),
            parent: self.parent.into_cursor()?,
            cursor: self.target.into_cursor()?,
            root: NodeId::from_bytes(self.root),
            expected_nodes: self
                .expected_nodes
                .into_iter()
                .map(StoredNodePrecondition::into_record)
                .collect(),
            expected_directories: self
                .expected_directories
                .into_iter()
                .map(StoredDirectoryPrecondition::into_record)
                .collect(),
            put_nodes,
            remove_nodes,
            put_directories,
            remove_directories,
            put_file_versions,
            remove_file_versions,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "kind",
    content = "record"
)]
enum StoredEffect {
    PutNode(StoredNode),
    DeleteNode([u8; 16]),
    PutDirectory(StoredDirectory),
    DeleteDirectory([u8; 16]),
    PutFileVersion(StoredFileVersion),
    DeleteFileVersion([u8; 32]),
    SetRoot([u8; 16]),
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCursor {
    sequence: u64,
    operation: Option<[u8; 16]>,
}

impl From<ChangeCursor> for StoredCursor {
    fn from(cursor: ChangeCursor) -> Self {
        Self {
            sequence: cursor.sequence(),
            operation: cursor.operation().map(|value| *value.as_bytes()),
        }
    }
}

impl StoredCursor {
    fn into_cursor(self) -> Result<ChangeCursor, ManagedError> {
        match (self.sequence, self.operation) {
            (0, None) => Ok(ChangeCursor::Genesis),
            (sequence, Some(operation)) => Ok(ChangeCursor::at(
                NonZeroU64::new(sequence)
                    .ok_or_else(|| corrupt("read Managed namespace", "cursor is invalid"))?,
                OperationId::from_bytes(operation),
            )),
            _ => Err(corrupt("read Managed namespace", "cursor is invalid")),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshot {
    volume_id: [u8; 16],
    cursor: StoredCursor,
    root: [u8; 16],
    nodes: Vec<StoredNode>,
    directories: Vec<StoredDirectory>,
    file_versions: Vec<StoredFileVersion>,
}

impl From<&NamespaceSnapshot> for StoredSnapshot {
    fn from(snapshot: &NamespaceSnapshot) -> Self {
        Self {
            volume_id: *snapshot.volume_id.as_bytes(),
            cursor: snapshot.cursor.into(),
            root: *snapshot.root.as_bytes(),
            nodes: snapshot.nodes.values().map(StoredNode::from).collect(),
            directories: snapshot
                .directories
                .values()
                .map(StoredDirectory::from)
                .collect(),
            file_versions: snapshot
                .file_versions
                .values()
                .map(StoredFileVersion::from)
                .collect(),
        }
    }
}

impl StoredSnapshot {
    fn into_snapshot(self) -> Result<NamespaceSnapshot, ManagedError> {
        let node_count = self.nodes.len();
        let directory_count = self.directories.len();
        let file_version_count = self.file_versions.len();
        let nodes = self
            .nodes
            .into_iter()
            .map(StoredNode::into_record)
            .map(|record| (record.id, record))
            .collect::<BTreeMap<_, _>>();
        let directories = self
            .directories
            .into_iter()
            .map(StoredDirectory::into_record)
            .map(|record| (record.node, record))
            .collect::<BTreeMap<_, _>>();
        let file_versions = self
            .file_versions
            .into_iter()
            .map(StoredFileVersion::into_record)
            .map(|record| (record.id, record))
            .collect::<BTreeMap<_, _>>();
        if nodes.len() != node_count
            || directories.len() != directory_count
            || file_versions.len() != file_version_count
        {
            return Err(corrupt(
                "read Managed namespace",
                "namespace contains duplicate records",
            ));
        }
        Ok(NamespaceSnapshot {
            volume_id: VolumeId::from_bytes(self.volume_id),
            cursor: self.cursor.into_cursor()?,
            root: NodeId::from_bytes(self.root),
            nodes,
            directories,
            file_versions,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectory {
    node: [u8; 16],
    generation: u64,
    entries: BTreeMap<String, StoredDirectoryEntry>,
}

impl From<&DirectoryRecord> for StoredDirectory {
    fn from(directory: &DirectoryRecord) -> Self {
        Self {
            node: *directory.node.as_bytes(),
            generation: managed_generation_number(&directory.generation)
                .expect("validated Managed directory generation"),
            entries: directory
                .entries
                .iter()
                .map(|(name, entry)| (name.clone(), (*entry).into()))
                .collect(),
        }
    }
}

impl StoredDirectory {
    fn into_record(self) -> DirectoryRecord {
        DirectoryRecord {
            node: NodeId::from_bytes(self.node),
            generation: managed_generation(self.generation),
            entries: self
                .entries
                .into_iter()
                .map(|(name, entry)| (name, entry.into()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(byte: u8) -> OperationId {
        OperationId::from_bytes([byte; 16])
    }

    fn cursor(sequence: u64, byte: u8) -> ChangeCursor {
        ChangeCursor::at(NonZeroU64::new(sequence).unwrap(), operation(byte))
    }

    fn root_snapshot(cursor: ChangeCursor) -> NamespaceSnapshot {
        let root = NodeId::from_bytes([2; 16]);
        NamespaceSnapshot {
            volume_id: VolumeId::from_bytes([1; 16]),
            cursor,
            root,
            nodes: BTreeMap::from([(
                root,
                NodeRecord {
                    id: root,
                    generation: managed_generation(1),
                    kind: NodeKind::Directory,
                    attributes: NodeAttributes { executable: false },
                    file_version: None,
                },
            )]),
            directories: BTreeMap::from([(
                root,
                DirectoryRecord {
                    node: root,
                    generation: managed_generation(1),
                    entries: BTreeMap::new(),
                },
            )]),
            file_versions: BTreeMap::new(),
        }
    }

    #[test]
    fn checkpoint_tail_is_consecutive_and_bounded() {
        let checkpoint = cursor(1, 1);
        let rows = vec![
            serde_json::json!({
                "operation_id": hex(operation(2).as_bytes()),
                "parent_sequence": 1,
                "parent_operation": hex(operation(1).as_bytes()),
                "target_sequence": 2,
            }),
            serde_json::json!({
                "operation_id": hex(operation(3).as_bytes()),
                "parent_sequence": 2,
                "parent_operation": hex(operation(2).as_bytes()),
                "target_sequence": 3,
            }),
        ];
        validate_tail(checkpoint, cursor(3, 3), &rows).unwrap();
        assert!(validate_tail(checkpoint, cursor(3, 3), &rows[..1]).is_err());

        let mut recovery_root = 1;
        for sequence in 2..=512 {
            if is_checkpoint(sequence) {
                recovery_root = sequence;
            }
            assert!(sequence - recovery_root < CHECKPOINT_INTERVAL);
        }
    }

    #[test]
    fn head_row_recovers_the_fixed_gc_sweep() {
        let fixed = cursor(7, 7);
        let sweeping = serde_json::json!({
            "maintenance_state": "sweeping",
            "maintenance_owner": hex(&[9; 16]),
            "maintenance_fixed_sequence": 7,
            "maintenance_fixed_operation": hex(operation(7).as_bytes()),
        });
        let sweep = gc_sweep(&sweeping, 3, fixed).unwrap().unwrap();
        assert_eq!(sweep.epoch(), 3);
        assert_eq!(sweep.fixed_cursor(), fixed);

        let idle = serde_json::json!({
            "maintenance_state": "idle",
            "maintenance_owner": hex(&[9; 16]),
            "maintenance_fixed_sequence": null,
            "maintenance_fixed_operation": null,
        });
        assert_eq!(gc_sweep(&idle, 3, fixed).unwrap(), None);

        let wrong_head = cursor(8, 8);
        assert!(gc_sweep(&sweeping, 3, wrong_head).is_err());
    }

    #[test]
    fn checkpoint_tail_replays_change_payloads() {
        let checkpoint = root_snapshot(cursor(1, 1));
        let target = root_snapshot(cursor(2, 2));
        let publication = NamespacePublication {
            operation: operation(2),
            parent: checkpoint.cursor,
            expected_nodes: Vec::new(),
            expected_directories: Vec::new(),
            target: target.clone(),
        };
        let payload = encode(
            &PublicationDelta::new(&publication, Some(&checkpoint)).change,
            "test D1 replay",
        )
        .unwrap();
        let rows = [serde_json::json!({
            "operation_id": hex(operation(2).as_bytes()),
            "parent_sequence": 1,
            "parent_operation": hex(operation(1).as_bytes()),
            "target_sequence": 2,
            "payload_json": payload,
        })];

        assert_eq!(
            replay_tail(checkpoint, target.cursor, target.root, &rows).unwrap(),
            target
        );
    }

    #[test]
    fn operation_results_resolve_only_committed_operations() {
        assert_eq!(
            resolve_operation_rows(&[], operation(2), "test operation result").unwrap(),
            CommitOutcome::Absent
        );
        let committed = serde_json::json!({
            "target_sequence": 2,
        });
        assert_eq!(
            operation_result(&committed, operation(2), "test operation result").unwrap(),
            CommitOutcome::Committed(cursor(2, 2))
        );
    }

    #[test]
    fn operation_identity_rejects_another_payload() {
        let result = serde_json::json!({"request_digest": "first"});
        assert_eq!(
            validate_request_digest(&result, "second")
                .unwrap_err()
                .kind(),
            ManagedErrorKind::Conflict
        );
    }
}
