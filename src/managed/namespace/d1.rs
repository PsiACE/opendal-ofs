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

use super::validation::{validate_publication, validate_snapshot};
use super::{
    DirectoryPrecondition, DirectoryRecord, FileVersionLayout, FileVersionRecord,
    NamespacePublication, NamespaceSnapshot, NodePrecondition, NodeRecord, managed_generation,
    managed_generation_number,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, DirectoryEntry, FileVersionId, NodeAttributes, NodeId, NodeKind,
    OperationId, VolumeId,
};
use crate::managed::metadata::d1::{D1Session, D1Statement, statement};
use crate::managed::{ManagedError, ManagedErrorKind};

const HEADS: &str = "ofs_managed_v1_heads";
const NODES: &str = "ofs_managed_v1_nodes";
const DIRECTORIES: &str = "ofs_managed_v1_directories";
const FILE_VERSIONS: &str = "ofs_managed_v1_file_versions";
const TRANSACTIONS: &str = "ofs_managed_v1_change_transactions";
const EFFECTS: &str = "ofs_managed_v1_change_effects";
const RESULTS: &str = "ofs_managed_v1_operation_results";
const CHECKPOINTS: &str = "ofs_managed_v1_checkpoints";
const CHECKPOINT_INTERVAL: u64 = 64;
const SCHEMA_RESULTS: usize = 8;

#[derive(Clone, Debug)]
pub(crate) struct D1NamespaceObservation {
    pub(crate) snapshot: NamespaceSnapshot,
    revision: u64,
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
                "SELECT h.revision, h.target_sequence, h.target_operation, h.root_node, h.checkpoint_sequence, c.snapshot_json FROM {HEADS} h JOIN {CHECKPOINTS} c ON c.store_key = h.store_key AND c.target_sequence = h.checkpoint_sequence WHERE h.store_key = ? AND h.volume_id = ?"
            ),
            vec![self.store_key().into(), self.volume().into()],
            ),
            statement(
                format!("SELECT node_id, generation, record_json FROM {NODES} WHERE store_key = ? ORDER BY node_id"),
                vec![self.store_key().into()],
            ),
            statement(
                format!("SELECT node_id, generation, record_json FROM {DIRECTORIES} WHERE store_key = ? ORDER BY node_id"),
                vec![self.store_key().into()],
            ),
            statement(
                format!("SELECT file_version_id, record_json FROM {FILE_VERSIONS} WHERE store_key = ? ORDER BY file_version_id"),
                vec![self.store_key().into()],
            ),
            statement(
                format!("SELECT operation_id, parent_sequence, parent_operation, target_sequence FROM {TRANSACTIONS} WHERE store_key = ? AND target_sequence > COALESCE((SELECT checkpoint_sequence FROM {HEADS} WHERE store_key = ?), 0) AND status = 'committed' ORDER BY target_sequence"),
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
        let root = node_id(text(head, "root_node", "read Managed namespace")?)?;
        let checkpoint_sequence = integer(head, "checkpoint_sequence", "read Managed namespace")?;
        let checkpoint: StoredSnapshot = decode(
            text(head, "snapshot_json", "read Managed namespace")?,
            "read Managed namespace",
        )?;
        let checkpoint = checkpoint.into_snapshot()?;
        if checkpoint.volume_id != self.volume_id
            || checkpoint.cursor.sequence() != checkpoint_sequence
        {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint does not match the namespace head",
            ));
        }
        validate_snapshot(&checkpoint)
            .map_err(|_| corrupt("read Managed namespace", "checkpoint is invalid"))?;
        validate_tail(
            checkpoint.cursor,
            cursor,
            rows(&results, SCHEMA_RESULTS + 4, "read Managed namespace")?,
        )?;
        let nodes = read_nodes(rows(
            &results,
            SCHEMA_RESULTS + 1,
            "read Managed namespace",
        )?)?;
        let directories = read_directories(rows(
            &results,
            SCHEMA_RESULTS + 2,
            "read Managed namespace",
        )?)?;
        let file_versions = read_file_versions(rows(
            &results,
            SCHEMA_RESULTS + 3,
            "read Managed namespace",
        )?)?;
        let snapshot = NamespaceSnapshot {
            volume_id: self.volume_id,
            cursor,
            root,
            nodes,
            directories,
            file_versions,
        };
        validate_snapshot(&snapshot)
            .map_err(|_| corrupt("read Managed namespace", "snapshot is invalid"))?;
        Ok(Some(D1NamespaceObservation { snapshot, revision }))
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
        let base = observed.map(|value| &value.snapshot);
        if !validate_publication(publication, base)? {
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |state| state.cursor),
            });
        }
        let delta = PublicationDelta::new(publication, base)?;
        let payload = encode(&delta.change, "publish Managed namespace")?;
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
                    "INSERT OR IGNORE INTO {TRANSACTIONS} (store_key, operation_id, payload_json, parent_sequence, parent_operation, target_sequence, status, eligible) SELECT ?, ?, ?, ?, ?, ?, 'pending', 0 WHERE NOT EXISTS (SELECT 1 FROM {RESULTS} WHERE store_key = ? AND operation_id = ?)"
                ),
                vec![
                    self.store_key().into(),
                    operation.clone().into(),
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
                    "SELECT payload_json FROM {RESULTS} WHERE store_key = ? AND operation_id = ? UNION ALL SELECT payload_json FROM {TRANSACTIONS} WHERE store_key = ? AND operation_id = ? AND NOT EXISTS (SELECT 1 FROM {RESULTS} WHERE store_key = ? AND operation_id = ?)"
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
            put_file_versions(self.store_key(), &delta.file_versions, &guard, guarded())?,
            put_effects(
                self.store_key(),
                &delta.effects,
                &operation,
                target_sequence,
                &guard,
                guarded(),
            )?,
        ]);
        batch.push(match observed {
            Some(observed) => statement(
                format!(
                    "UPDATE {HEADS} SET revision = revision + 1, target_sequence = ?, target_operation = ?, root_node = ? WHERE store_key = ? AND volume_id = ? AND revision = ? AND target_sequence = ? AND target_operation IS ? AND {guard} RETURNING revision"
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
                    "INSERT OR IGNORE INTO {HEADS} (store_key, volume_id, revision, target_sequence, target_operation, root_node, checkpoint_sequence) SELECT ?, ?, 1, ?, ?, ?, 0 WHERE ? = 0 AND ? IS NULL AND {guard} RETURNING revision"
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
                    "INSERT OR IGNORE INTO {RESULTS} (store_key, operation_id, payload_json, target_sequence) SELECT ?, ?, ?, ? FROM {HEADS} WHERE store_key = ? AND volume_id = ? AND target_sequence = ? AND target_operation = ?"
                ),
                vec![
                    self.store_key().into(),
                    operation.clone().into(),
                    payload.clone().into(),
                    target_sequence.into(),
                    self.store_key().into(),
                    self.volume().into(),
                    target_sequence.into(),
                    operation.clone().into(),
                ],
            ),
            statement(
                format!("UPDATE {TRANSACTIONS} SET status = CASE WHEN EXISTS (SELECT 1 FROM {RESULTS} WHERE store_key = ? AND operation_id = ?) THEN 'committed' ELSE 'rejected' END WHERE store_key = ? AND operation_id = ? AND status = 'pending'"),
                vec![
                    self.store_key().into(), operation.clone().into(),
                    self.store_key().into(), operation.clone().into(),
                ],
            ),
            put_checkpoint(
                checkpoint.as_deref(),
                publication,
                &operation,
                &payload,
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
                format!("DELETE FROM {EFFECTS} WHERE store_key = ? AND target_sequence <= (SELECT checkpoint_sequence FROM {HEADS} WHERE store_key = ?)"),
                vec![self.store_key().into(), self.store_key().into()],
            ),
            statement(
                format!("DELETE FROM {TRANSACTIONS} WHERE store_key = ? AND target_sequence < (SELECT checkpoint_sequence FROM {HEADS} WHERE store_key = ?) AND status = 'committed'"),
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
                    _ => Ok(CommitOutcome::Unknown),
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
        if text(transaction, "payload_json", "publish Managed namespace")? != payload {
            return Err(ManagedError::new(
                ManagedErrorKind::Conflict,
                "publish Managed namespace",
                "operation identity was reused with another payload",
            ));
        }
        if !rows(&results, results.len() - 1, "publish Managed namespace")?.is_empty() {
            return Ok(CommitOutcome::Committed(publication.target.cursor));
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
        let results = self
            .session
            .query(batch, "resolve Managed publication")
            .await?;
        let rows = rows(&results, SCHEMA_RESULTS, "resolve Managed publication")?;
        let [row] = rows else {
            return if rows.is_empty() {
                Ok(CommitOutcome::Absent)
            } else {
                Err(corrupt(
                    "resolve Managed publication",
                    "D1 returned duplicate results",
                ))
            };
        };
        let sequence = integer(row, "target_sequence", "resolve Managed publication")?;
        let sequence = NonZeroU64::new(sequence)
            .ok_or_else(|| corrupt("resolve Managed publication", "committed cursor is invalid"))?;
        Ok(CommitOutcome::Committed(ChangeCursor::at(
            sequence, operation,
        )))
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        if let CommitOutcome::Committed(cursor) = self.resolve(operation).await? {
            return Ok(CommitOutcome::Committed(cursor));
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
                sql.push_str(&format!(" AND EXISTS (SELECT 1 FROM {HEADS} WHERE store_key = ? AND volume_id = ? AND revision = ? AND target_sequence = ? AND target_operation IS ?)"));
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
                "CREATE TABLE IF NOT EXISTS {HEADS} (store_key TEXT PRIMARY KEY, volume_id TEXT NOT NULL, revision INTEGER NOT NULL, target_sequence INTEGER NOT NULL, target_operation TEXT NOT NULL, root_node TEXT NOT NULL, checkpoint_sequence INTEGER NOT NULL)"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {NODES} (store_key TEXT NOT NULL, node_id TEXT NOT NULL, generation INTEGER NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (store_key, node_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {DIRECTORIES} (store_key TEXT NOT NULL, node_id TEXT NOT NULL, generation INTEGER NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (store_key, node_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {FILE_VERSIONS} (store_key TEXT NOT NULL, file_version_id TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (store_key, file_version_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {TRANSACTIONS} (store_key TEXT NOT NULL, operation_id TEXT NOT NULL, payload_json TEXT NOT NULL, parent_sequence INTEGER NOT NULL, parent_operation TEXT, target_sequence INTEGER NOT NULL, status TEXT NOT NULL, eligible INTEGER NOT NULL, PRIMARY KEY (store_key, operation_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {EFFECTS} (store_key TEXT NOT NULL, operation_id TEXT NOT NULL, target_sequence INTEGER NOT NULL, effect_index INTEGER NOT NULL, effect_json TEXT NOT NULL, PRIMARY KEY (store_key, operation_id, effect_index))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {RESULTS} (store_key TEXT NOT NULL, operation_id TEXT NOT NULL, payload_json TEXT NOT NULL, target_sequence INTEGER NOT NULL, PRIMARY KEY (store_key, operation_id))"
            ),
            Vec::new(),
        ),
        statement(
            format!(
                "CREATE TABLE IF NOT EXISTS {CHECKPOINTS} (store_key TEXT NOT NULL, target_sequence INTEGER NOT NULL, operation_id TEXT NOT NULL, root_node TEXT NOT NULL, snapshot_json TEXT NOT NULL, PRIMARY KEY (store_key, target_sequence))"
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
            "INSERT INTO {table} (store_key, {key}, generation, record_json) SELECT ?, json_extract(value, '$.key'), json_extract(value, '$.generation'), json_extract(value, '$.record') FROM json_each(?) WHERE {guard} ON CONFLICT(store_key, {key}) DO UPDATE SET generation = excluded.generation, record_json = excluded.record_json"
        ),
        params,
    ))
}

fn put_file_versions(
    store_key: String,
    records: &[FileVersionRow],
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
            "INSERT OR IGNORE INTO {FILE_VERSIONS} (store_key, file_version_id, record_json) SELECT ?, json_extract(value, '$.key'), json_extract(value, '$.record') FROM json_each(?) WHERE {guard}"
        ),
        params,
    ))
}

fn put_effects(
    store_key: String,
    effects: &[String],
    operation: &str,
    target_sequence: i64,
    guard: &str,
    guard_params: Vec<Value>,
) -> Result<D1Statement, ManagedError> {
    let params = guarded_params(
        vec![
            store_key.into(),
            operation.to_owned().into(),
            target_sequence.into(),
            encode(&effects, "publish Managed namespace")?.into(),
        ],
        guard_params,
    );
    Ok(statement(
        format!(
            "INSERT OR IGNORE INTO {EFFECTS} (store_key, operation_id, target_sequence, effect_index, effect_json) SELECT ?, ?, ?, CAST(key AS INTEGER), value FROM json_each(?) WHERE {guard}"
        ),
        params,
    ))
}

fn put_checkpoint(
    checkpoint: Option<&str>,
    publication: &NamespacePublication,
    operation: &str,
    payload: &str,
    store_key: String,
) -> Result<D1Statement, ManagedError> {
    let checkpoint = checkpoint.map_or(Value::Null, |value| value.to_owned().into());
    Ok(statement(
        format!(
            "INSERT OR IGNORE INTO {CHECKPOINTS} (store_key, target_sequence, operation_id, root_node, snapshot_json) SELECT ?, ?, ?, ?, ? WHERE ? IS NOT NULL AND EXISTS (SELECT 1 FROM {RESULTS} WHERE store_key = ? AND operation_id = ? AND payload_json = ?)"
        ),
        vec![
            store_key.clone().into(),
            sqlite_integer(publication.target.cursor.sequence())?.into(),
            operation.to_owned().into(),
            hex(publication.target.root.as_bytes()).into(),
            checkpoint.clone(),
            checkpoint,
            store_key.into(),
            operation.to_owned().into(),
            payload.to_owned().into(),
        ],
    ))
}

const fn is_checkpoint(sequence: u64) -> bool {
    sequence == 1 || sequence % CHECKPOINT_INTERVAL == 0
}

fn read_nodes(rows: &[Value]) -> Result<BTreeMap<NodeId, NodeRecord>, ManagedError> {
    let mut records = BTreeMap::new();
    for row in rows {
        let key = node_id(text(row, "node_id", "read Managed namespace")?)?;
        let generation = integer(row, "generation", "read Managed namespace")?;
        let stored: StoredNode = decode(
            text(row, "record_json", "read Managed namespace")?,
            "read Managed namespace",
        )?;
        let record = stored.into_record();
        if record.id != key || managed_generation_number(&record.generation) != Some(generation) {
            return Err(corrupt(
                "read Managed namespace",
                "node row disagrees with its record",
            ));
        }
        if records.insert(key, record).is_some() {
            return Err(corrupt(
                "read Managed namespace",
                "D1 returned duplicate nodes",
            ));
        }
    }
    Ok(records)
}

fn read_directories(rows: &[Value]) -> Result<BTreeMap<NodeId, DirectoryRecord>, ManagedError> {
    let mut records = BTreeMap::new();
    for row in rows {
        let key = node_id(text(row, "node_id", "read Managed namespace")?)?;
        let generation = integer(row, "generation", "read Managed namespace")?;
        let stored: StoredDirectory = decode(
            text(row, "record_json", "read Managed namespace")?,
            "read Managed namespace",
        )?;
        let record = stored.into_record();
        if record.node != key || managed_generation_number(&record.generation) != Some(generation) {
            return Err(corrupt(
                "read Managed namespace",
                "directory row disagrees with its record",
            ));
        }
        if records.insert(key, record).is_some() {
            return Err(corrupt(
                "read Managed namespace",
                "D1 returned duplicate directories",
            ));
        }
    }
    Ok(records)
}

fn read_file_versions(
    rows: &[Value],
) -> Result<BTreeMap<FileVersionId, FileVersionRecord>, ManagedError> {
    let mut records = BTreeMap::new();
    for row in rows {
        let key = file_version_id(text(row, "file_version_id", "read Managed namespace")?)?;
        let stored: StoredFileVersion = decode(
            text(row, "record_json", "read Managed namespace")?,
            "read Managed namespace",
        )?;
        let record = stored.into_record();
        if record.id != key {
            return Err(corrupt(
                "read Managed namespace",
                "file-version row disagrees with its record",
            ));
        }
        if records.insert(key, record).is_some() {
            return Err(corrupt(
                "read Managed namespace",
                "D1 returned duplicate file versions",
            ));
        }
    }
    Ok(records)
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

fn node_id(value: &str) -> Result<NodeId, ManagedError> {
    Ok(NodeId::from_bytes(decode_hex(value)?))
}

fn file_version_id(value: &str) -> Result<FileVersionId, ManagedError> {
    Ok(FileVersionId::from_bytes(decode_hex(value)?))
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

#[derive(Serialize)]
struct RecordRow {
    key: String,
    generation: u64,
    record: String,
}

#[derive(Serialize)]
struct FileVersionRow {
    key: String,
    record: String,
}

struct PublicationDelta {
    change: StoredChange,
    nodes: Vec<RecordRow>,
    deleted_nodes: Vec<String>,
    directories: Vec<RecordRow>,
    deleted_directories: Vec<String>,
    file_versions: Vec<FileVersionRow>,
    effects: Vec<String>,
}

impl PublicationDelta {
    fn new(
        publication: &NamespacePublication,
        base: Option<&NamespaceSnapshot>,
    ) -> Result<Self, ManagedError> {
        let empty_nodes = BTreeMap::new();
        let empty_directories = BTreeMap::new();
        let empty_versions = BTreeMap::new();
        let base_nodes = base.map_or(&empty_nodes, |snapshot| &snapshot.nodes);
        let base_directories = base.map_or(&empty_directories, |snapshot| &snapshot.directories);
        let base_versions = base.map_or(&empty_versions, |snapshot| &snapshot.file_versions);
        let mut nodes = Vec::new();
        let mut deleted_nodes = Vec::new();
        let mut directories = Vec::new();
        let mut deleted_directories = Vec::new();
        let mut file_versions = Vec::new();
        let mut effects = Vec::new();

        for (id, version) in &publication.target.file_versions {
            if base_versions.get(id) != Some(version) {
                let stored = StoredFileVersion::from(version);
                file_versions.push(FileVersionRow {
                    key: hex(id.as_bytes()),
                    record: encode(&stored, "publish Managed namespace")?,
                });
                effects.push(StoredEffect::PutFileVersion(stored));
            }
        }
        for id in base_directories.keys() {
            if !publication.target.directories.contains_key(id) {
                deleted_directories.push(hex(id.as_bytes()));
                effects.push(StoredEffect::DeleteDirectory(*id.as_bytes()));
            }
        }
        for id in base_nodes.keys() {
            if !publication.target.nodes.contains_key(id) {
                deleted_nodes.push(hex(id.as_bytes()));
                effects.push(StoredEffect::DeleteNode(*id.as_bytes()));
            }
        }
        for (id, node) in &publication.target.nodes {
            if base_nodes.get(id) != Some(node) {
                let stored = StoredNode::from(node);
                nodes.push(RecordRow {
                    key: hex(id.as_bytes()),
                    generation: managed_generation_number(&node.generation)
                        .expect("validated Managed node generation"),
                    record: encode(&stored, "publish Managed namespace")?,
                });
                effects.push(StoredEffect::PutNode(stored));
            }
        }
        for (id, directory) in &publication.target.directories {
            if base_directories.get(id) != Some(directory) {
                let stored = StoredDirectory::from(directory);
                directories.push(RecordRow {
                    key: hex(id.as_bytes()),
                    generation: managed_generation_number(&directory.generation)
                        .expect("validated Managed directory generation"),
                    record: encode(&stored, "publish Managed namespace")?,
                });
                effects.push(StoredEffect::PutDirectory(stored));
            }
        }
        effects.push(StoredEffect::SetRoot(*publication.target.root.as_bytes()));
        let encoded_effects = effects
            .iter()
            .map(|effect| encode(effect, "publish Managed namespace"))
            .collect::<Result<Vec<_>, _>>()?;
        let mut change = StoredChange {
            operation: *publication.operation.as_bytes(),
            parent: publication.parent.into(),
            target: publication.target.cursor.into(),
            root: *publication.target.root.as_bytes(),
            expected_nodes: publication
                .expected_nodes
                .iter()
                .map(StoredNodePrecondition::from)
                .collect(),
            expected_directories: publication
                .expected_directories
                .iter()
                .map(StoredDirectoryPrecondition::from)
                .collect(),
            effects,
        };
        change.expected_nodes.sort_by_key(|value| value.node);
        change
            .expected_directories
            .sort_by_key(|value| value.directory);
        Ok(Self {
            change,
            nodes,
            deleted_nodes,
            directories,
            deleted_directories,
            file_versions,
            effects: encoded_effects,
        })
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
struct StoredNode {
    id: [u8; 16],
    generation: u64,
    kind: StoredNodeKind,
    attributes: StoredNodeAttributes,
    file_version: Option<[u8; 32]>,
}

impl From<&NodeRecord> for StoredNode {
    fn from(node: &NodeRecord) -> Self {
        Self {
            id: *node.id.as_bytes(),
            generation: managed_generation_number(&node.generation)
                .expect("validated Managed node generation"),
            kind: node.kind.into(),
            attributes: node.attributes.into(),
            file_version: node.file_version.map(|value| *value.as_bytes()),
        }
    }
}

impl StoredNode {
    fn into_record(self) -> NodeRecord {
        NodeRecord {
            id: NodeId::from_bytes(self.id),
            generation: managed_generation(self.generation),
            kind: self.kind.into(),
            attributes: self.attributes.into(),
            file_version: self.file_version.map(FileVersionId::from_bytes),
        }
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

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryEntry {
    node: [u8; 16],
    kind: StoredNodeKind,
}

impl From<DirectoryEntry> for StoredDirectoryEntry {
    fn from(entry: DirectoryEntry) -> Self {
        Self {
            node: *entry.node.as_bytes(),
            kind: entry.kind.into(),
        }
    }
}

impl From<StoredDirectoryEntry> for DirectoryEntry {
    fn from(entry: StoredDirectoryEntry) -> Self {
        Self {
            node: NodeId::from_bytes(entry.node),
            kind: entry.kind.into(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredNodeKind {
    Directory,
    RegularFile,
}

impl From<NodeKind> for StoredNodeKind {
    fn from(kind: NodeKind) -> Self {
        match kind {
            NodeKind::Directory => Self::Directory,
            NodeKind::RegularFile => Self::RegularFile,
        }
    }
}

impl From<StoredNodeKind> for NodeKind {
    fn from(kind: StoredNodeKind) -> Self {
        match kind {
            StoredNodeKind::Directory => Self::Directory,
            StoredNodeKind::RegularFile => Self::RegularFile,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNodeAttributes {
    executable: bool,
}

impl From<NodeAttributes> for StoredNodeAttributes {
    fn from(attributes: NodeAttributes) -> Self {
        Self {
            executable: attributes.executable,
        }
    }
}

impl From<StoredNodeAttributes> for NodeAttributes {
    fn from(attributes: StoredNodeAttributes) -> Self {
        Self {
            executable: attributes.executable,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredFileVersion {
    id: [u8; 32],
    logical_size: u64,
    logical_digest: [u8; 32],
    layout: FileVersionLayout,
}

impl From<&FileVersionRecord> for StoredFileVersion {
    fn from(version: &FileVersionRecord) -> Self {
        Self {
            id: *version.id.as_bytes(),
            logical_size: version.logical_size,
            logical_digest: version.logical_digest,
            layout: version.layout.clone(),
        }
    }
}

impl StoredFileVersion {
    fn into_record(self) -> FileVersionRecord {
        FileVersionRecord {
            id: FileVersionId::from_bytes(self.id),
            logical_size: self.logical_size,
            logical_digest: self.logical_digest,
            layout: self.layout,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNodePrecondition {
    node: [u8; 16],
    expected_generation: Option<u64>,
}

impl From<&NodePrecondition> for StoredNodePrecondition {
    fn from(condition: &NodePrecondition) -> Self {
        Self {
            node: *condition.node.as_bytes(),
            expected_generation: condition.expected_generation.as_ref().map(|value| {
                managed_generation_number(value)
                    .expect("validated Managed node precondition generation")
            }),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryPrecondition {
    directory: [u8; 16],
    expected_generation: Option<u64>,
}

impl From<&DirectoryPrecondition> for StoredDirectoryPrecondition {
    fn from(condition: &DirectoryPrecondition) -> Self {
        Self {
            directory: *condition.directory.as_bytes(),
            expected_generation: condition.expected_generation.as_ref().map(|value| {
                managed_generation_number(value)
                    .expect("validated Managed directory precondition generation")
            }),
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

    #[test]
    fn change_record_contains_effects_instead_of_a_namespace_snapshot() {
        let change = StoredChange {
            operation: [1; 16],
            parent: ChangeCursor::Genesis.into(),
            target: cursor(1, 1).into(),
            root: [2; 16],
            expected_nodes: Vec::new(),
            expected_directories: Vec::new(),
            effects: vec![StoredEffect::SetRoot([2; 16])],
        };

        let record = encode(&change, "test D1 change record").unwrap();
        let decoded: StoredChange = decode(&record, "test D1 change record").unwrap();

        assert_eq!(decoded.operation, [1; 16]);
        assert!(!record.contains("\"nodes\""));
        assert!(!record.contains("\"directories\""));
        assert!(!record.contains("\"file_versions\""));
    }

    #[test]
    fn normalized_row_and_strict_record_must_agree() {
        let stored = StoredNode {
            id: [2; 16],
            generation: 1,
            kind: StoredNodeKind::Directory,
            attributes: StoredNodeAttributes { executable: false },
            file_version: None,
        };
        let row = serde_json::json!({
            "node_id": hex(&[1; 16]),
            "generation": 1,
            "record_json": encode(&stored, "test D1 node record").unwrap(),
        });
        assert_eq!(
            read_nodes(&[row]).unwrap_err().kind(),
            ManagedErrorKind::Corrupt
        );

        let record = serde_json::json!({
            "id": vec![2; 16],
            "generation": 1,
            "kind": "directory",
            "attributes": { "executable": false },
            "file_version": null,
            "unexpected": true,
        });
        assert!(decode::<StoredNode>(&record.to_string(), "test D1 node record").is_err());
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
}
