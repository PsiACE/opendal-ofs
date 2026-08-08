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
use std::collections::BTreeSet;
use std::io::Cursor;
use std::num::NonZeroU64;

use opendal::{ErrorKind, Operator};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::change::NamespaceChange;
use super::validation::{validate_publication, validate_snapshot};
use super::{
    DirectoryPrecondition, DirectoryRecord, FileVersionLayout, FileVersionRecord, NamespaceGcSweep,
    NamespacePublication, NamespaceSnapshot, NodePrecondition, NodeRecord, managed_generation,
    managed_generation_number,
};
use crate::filesystem::{
    ChangeCursor, CommitOutcome, DirectoryEntry, FileVersionId, NodeAttributes, NodeId, NodeKind,
    OperationId, VolumeId,
};
use crate::managed::section::{self, Record as SectionRecord, Reference as SectionReference};
use crate::managed::{ManagedError, ManagedErrorKind};

const HEAD_KEY: &str = ".ofs/managed/metadata/v1/head.json";
const TRANSACTION_ROOT: &str = ".ofs/managed/metadata/v1/transactions";
const CHECKPOINT_ROOT: &str = ".ofs/managed/metadata/v1/checkpoints/sha256";
const SECTION_ROOT: &str = ".ofs/managed/metadata/v1/sections/sha256";
const RESULT_ROOT: &str = ".ofs/managed/metadata/v1/results";
const TRANSACTION_MAGIC: &[u8] = b"OFS1TXN\0";
const CHECKPOINT_MAGIC: &[u8] = b"OFS1CHK\0";
const RESULT_MAGIC: &[u8] = b"OFS1RES\0";
const HEAD_MAGIC: &str = "ofs-managed-head";
const FORMAT_MAJOR: u16 = 1;
const MAX_TAIL_TRANSACTIONS: u16 = 32;
const NODE_SECTION: u8 = 1;
const DIRECTORY_SECTION: u8 = 2;
const DIRECTORY_ENTRY_SECTION: u8 = 3;
const FILE_VERSION_SECTION: u8 = 4;

#[derive(Clone, Debug)]
pub struct NamespaceObservation {
    pub snapshot: NamespaceSnapshot,
    revision: String,
    authority: Box<ObservationAuthority>,
}

impl NamespaceObservation {
    pub fn gc_sweep(&self) -> Option<NamespaceGcSweep> {
        self.authority
            .head
            .gc_sweep()
            .expect("observed HEAD has valid maintenance state")
    }
}

#[derive(Clone, Debug)]
struct ObservationAuthority {
    head: StoredHead,
}

#[derive(Clone)]
pub struct ObjectNamespace {
    volume_id: VolumeId,
    operator: Operator,
}

impl ObjectNamespace {
    pub fn new(volume_id: VolumeId, operator: Operator) -> Result<Self, ManagedError> {
        let capability = operator.info().full_capability();
        if !capability.read
            || !capability.stat
            || !capability.write
            || !capability.write_with_if_not_exists
            || !capability.write_with_if_match
        {
            return Err(invalid(
                "open Managed namespace",
                "object metadata requires read, stat, create-only write, and conditional replace",
            ));
        }
        Ok(Self {
            volume_id,
            operator,
        })
    }

    pub async fn observe(&self) -> Result<Option<NamespaceObservation>, ManagedError> {
        let Some((bytes, revision)) = self.read_head().await? else {
            return Ok(None);
        };
        let head = decode_head(&bytes)?;
        self.recover_observation(head, revision).await.map(Some)
    }

    pub async fn observe_from(
        &self,
        base: &NamespaceSnapshot,
    ) -> Result<Option<NamespaceObservation>, ManagedError> {
        let Some((bytes, revision)) = self.read_head().await? else {
            return Ok(None);
        };
        let head = decode_head(&bytes)?;
        head.validate(self.volume_id)?;
        if base.volume_id == self.volume_id && base.cursor == head.cursor.into_cursor()? {
            validate_snapshot(base)?;
            return Ok(Some(NamespaceObservation {
                snapshot: base.clone(),
                revision,
                authority: Box::new(ObservationAuthority { head }),
            }));
        }
        self.recover_observation(head, revision).await.map(Some)
    }

    async fn recover_observation(
        &self,
        head: StoredHead,
        revision: String,
    ) -> Result<NamespaceObservation, ManagedError> {
        let recovered = self.recover(&head).await?;
        Ok(NamespaceObservation {
            snapshot: recovered,
            revision,
            authority: Box::new(ObservationAuthority { head }),
        })
    }

    pub async fn publish(
        &self,
        observed: Option<&NamespaceObservation>,
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

        let stored = StoredTransaction::from_publication(publication, base);
        let interpreted = apply_transaction(base.cloned(), &stored)?;
        if interpreted != publication.target {
            return Err(invalid(
                "publish Managed namespace",
                "transaction does not reproduce its target",
            ));
        }
        let bytes = encode_cbor(TRANSACTION_MAGIC, &stored, "publish Managed namespace")?;
        let transaction_sha256 = sha256(&bytes);
        self.ensure_transaction(publication.operation, &bytes)
            .await?;
        let checkpoint_due = observed.is_none()
            || observed.is_some_and(|value| {
                value.authority.head.tail_transactions + 1 >= MAX_TAIL_TRANSACTIONS
            });
        let (checkpoint, checkpoint_cursor, tail_transactions) = if checkpoint_due {
            let checkpoint = match observed {
                Some(observed) => {
                    let previous = self
                        .read_checkpoint(&observed.authority.head.checkpoint)
                        .await?;
                    if previous.cursor != observed.authority.head.checkpoint_cursor {
                        return Err(corrupt(
                            "checkpoint Managed namespace",
                            "checkpoint and HEAD disagree",
                        ));
                    }
                    let mut transactions = self.read_tail(&observed.authority.head).await?;
                    transactions.push(stored.clone());
                    self.checkpoint_incremental(&publication.target, &previous, &transactions)
                        .await?
                }
                None => self.checkpoint_full(&publication.target).await?,
            };
            let bytes = encode_cbor(
                CHECKPOINT_MAGIC,
                &checkpoint,
                "checkpoint Managed namespace",
            )?;
            let checkpoint_id = sha256(&bytes);
            self.ensure_immutable(&checkpoint_key(&checkpoint_id), &bytes)
                .await?;
            (checkpoint_id, publication.target.cursor.into(), 0)
        } else {
            let observed = observed.expect("checkpoint policy has an observation");
            (
                observed.authority.head.checkpoint,
                observed.authority.head.checkpoint_cursor,
                observed.authority.head.tail_transactions + 1,
            )
        };
        let head = StoredHead::new(
            self.volume_id,
            stored.cursor,
            stored.operation,
            transaction_sha256,
            checkpoint,
            checkpoint_cursor,
            tail_transactions,
        )?
        .with_maintenance_epoch(observed.map_or(0, |value| value.authority.head.maintenance_epoch));
        let head = encode_head(&head)?;
        let replaced = match observed {
            Some(observed) => self.replace_head(&observed.revision, head).await,
            None => self.create_head(head).await,
        };
        match replaced {
            Ok(true) => {
                let outcome = CommitOutcome::Committed(publication.target.cursor);
                self.ensure_result(publication.operation, transaction_sha256, &outcome)
                    .await?;
                Ok(outcome)
            }
            Ok(false) => self.outcome_after_race(publication.operation).await,
            Err(_) => match self.resolve(publication.operation).await {
                Ok(CommitOutcome::Committed(cursor)) => Ok(CommitOutcome::Committed(cursor)),
                _ => Ok(CommitOutcome::Unknown),
            },
        }
    }

    pub async fn begin_gc(
        &self,
        observed: &NamespaceObservation,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        if observed.snapshot.volume_id != self.volume_id {
            return Err(invalid(
                "begin Managed namespace GC",
                "observation belongs to another volume",
            ));
        }
        if let Some(sweep) = observed.gc_sweep() {
            return Ok(sweep);
        }
        let mut head = observed.authority.head.clone();
        let sweep = head.begin_gc()?;
        if self
            .replace_head(&observed.revision, encode_head(&head)?)
            .await?
        {
            return Ok(sweep);
        }
        let current = self
            .observe()
            .await?
            .ok_or_else(|| conflict("begin Managed namespace GC", "namespace authority changed"))?;
        current
            .gc_sweep()
            .filter(|value| value.fixed_cursor() == observed.snapshot.cursor)
            .ok_or_else(|| conflict("begin Managed namespace GC", "namespace authority changed"))
    }

    pub async fn finish_gc(&self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        let observed = self.observe().await?.ok_or_else(|| {
            conflict("finish Managed namespace GC", "namespace authority changed")
        })?;
        if observed.authority.head.maintenance_epoch == sweep.epoch()
            && observed.gc_sweep().is_none()
        {
            return Ok(());
        }
        if observed.gc_sweep() != Some(sweep) {
            return Err(conflict(
                "finish Managed namespace GC",
                "GC sweep token does not match the authority",
            ));
        }
        let mut head = observed.authority.head.clone();
        head.finish_gc(sweep)?;
        if self
            .replace_head(&observed.revision, encode_head(&head)?)
            .await?
        {
            return Ok(());
        }
        let current = self.observe().await?.ok_or_else(|| {
            conflict("finish Managed namespace GC", "namespace authority changed")
        })?;
        if current.authority.head.maintenance_epoch == sweep.epoch() && current.gc_sweep().is_none()
        {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed namespace GC",
                "namespace authority changed",
            ))
        }
    }

    pub async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        match self.resolve_known(operation).await {
            Err(error) if error.kind() == ManagedErrorKind::Unavailable => {
                Ok(CommitOutcome::Unknown)
            }
            outcome => outcome,
        }
    }

    async fn resolve_known(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        let result = self.read_result(operation).await?;
        let Some(target) = self.read_transaction(operation).await? else {
            if result.is_some() {
                return Err(corrupt(
                    "resolve Managed publication",
                    "operation result has no transaction",
                ));
            }
            return Ok(CommitOutcome::Absent);
        };
        let transaction_sha256 = sha256(&encode_cbor(
            TRANSACTION_MAGIC,
            &target,
            "resolve Managed publication",
        )?);
        if let Some(result) = result {
            if result.transaction_sha256 != transaction_sha256 {
                return Err(corrupt(
                    "resolve Managed publication",
                    "operation result identifies another transaction",
                ));
            }
            return match result.outcome {
                StoredResultKind::Committed { cursor } => {
                    Ok(CommitOutcome::Committed(cursor.into_cursor()?))
                }
                StoredResultKind::Conflict => Ok(CommitOutcome::Conflict {
                    observed: self
                        .observe()
                        .await?
                        .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor),
                }),
            };
        }
        let Some((bytes, _)) = self.read_head().await? else {
            return Ok(CommitOutcome::Absent);
        };
        let head = decode_head(&bytes)?;
        let snapshot = self.recover(&head).await?;
        if self.transaction_is_committed(&head, operation).await? {
            self.ensure_result(
                operation,
                transaction_sha256,
                &CommitOutcome::Committed(target.cursor.into_cursor()?),
            )
            .await?;
            return Ok(CommitOutcome::Committed(target.cursor.into_cursor()?));
        }
        let parent = target.parent.into_cursor()?;
        if snapshot.cursor == parent || snapshot.cursor.sequence() <= parent.sequence() {
            return Ok(CommitOutcome::Absent);
        }
        Ok(CommitOutcome::Conflict {
            observed: snapshot.cursor,
        })
    }

    async fn transaction_is_committed(
        &self,
        head: &StoredHead,
        operation: OperationId,
    ) -> Result<bool, ManagedError> {
        let target = self.required_transaction(operation).await?;
        let target_cursor = target.cursor.into_cursor()?;
        let target_parent = target.parent.into_cursor()?;
        let head_cursor = head.cursor.into_cursor()?;
        if head_cursor.sequence() < target_cursor.sequence() {
            return Ok(false);
        }

        let mut current = self
            .required_transaction(OperationId::from_bytes(head.latest_transaction))
            .await?;
        loop {
            let cursor = current.cursor.into_cursor()?;
            if current.operation == *operation.as_bytes() {
                return Ok(true);
            }
            if cursor.sequence() <= target_parent.sequence() {
                return Ok(false);
            }
            current = self
                .required_transaction(OperationId::from_bytes(
                    current.parent.operation.ok_or_else(|| {
                        corrupt(
                            "resolve Managed publication",
                            "transaction history is incomplete",
                        )
                    })?,
                ))
                .await?;
        }
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        let outcome = self.resolve(operation).await?;
        if matches!(
            outcome,
            CommitOutcome::Committed(_) | CommitOutcome::Unknown
        ) {
            return Ok(outcome);
        }
        let observed = self
            .observe()
            .await?
            .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor);
        let outcome = CommitOutcome::Conflict { observed };
        if let Ok(Some(transaction)) = self.read_transaction(operation).await {
            let bytes = encode_cbor(
                TRANSACTION_MAGIC,
                &transaction,
                "resolve Managed publication",
            )?;
            let _ = self
                .ensure_result(operation, sha256(&bytes), &outcome)
                .await;
        }
        Ok(outcome)
    }

    async fn ensure_transaction(
        &self,
        operation: OperationId,
        expected: &[u8],
    ) -> Result<(), ManagedError> {
        self.ensure_immutable(&transaction_key(operation), expected)
            .await
    }

    async fn ensure_immutable(&self, key: &str, expected: &[u8]) -> Result<(), ManagedError> {
        match self
            .operator
            .write_with(key, expected.to_vec())
            .if_not_exists(true)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error)
                if !matches!(
                    error.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                ) => {}
            Err(_) => {}
        }
        let existing = self
            .operator
            .read(key)
            .await
            .map_err(|_| unavailable("publish Managed namespace"))?;
        if existing.to_bytes().as_ref() == expected {
            Ok(())
        } else {
            Err(ManagedError::new(
                ManagedErrorKind::Conflict,
                "publish Managed namespace",
                "operation identity was reused with another payload",
            ))
        }
    }

    async fn read_transaction(
        &self,
        operation: OperationId,
    ) -> Result<Option<StoredTransaction>, ManagedError> {
        match self.operator.read(&transaction_key(operation)).await {
            Ok(bytes) => {
                let transaction: StoredTransaction = decode_cbor(
                    TRANSACTION_MAGIC,
                    &bytes.to_bytes(),
                    "read Managed transaction",
                )?;
                if transaction.operation != *operation.as_bytes() {
                    return Err(corrupt(
                        "read Managed transaction",
                        "transaction key and operation disagree",
                    ));
                }
                transaction.validate(self.volume_id)?;
                Ok(Some(transaction))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(unavailable("read Managed transaction")),
        }
    }

    async fn required_transaction(
        &self,
        operation: OperationId,
    ) -> Result<StoredTransaction, ManagedError> {
        self.read_transaction(operation)
            .await?
            .ok_or_else(|| corrupt("read Managed namespace", "transaction is missing"))
    }

    async fn recover(&self, head: &StoredHead) -> Result<NamespaceSnapshot, ManagedError> {
        head.validate(self.volume_id)?;
        self.recover_bounded(head).await
    }

    async fn recover_bounded(&self, head: &StoredHead) -> Result<NamespaceSnapshot, ManagedError> {
        let checkpoint = self.read_checkpoint(&head.checkpoint).await?;
        if checkpoint.major != FORMAT_MAJOR
            || checkpoint.volume_id != *self.volume_id.as_bytes()
            || checkpoint.cursor != head.checkpoint_cursor
        {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint and HEAD disagree",
            ));
        }
        let mut snapshot = self.read_snapshot(checkpoint).await?;
        validate_snapshot(&snapshot)
            .map_err(|_| corrupt("read Managed namespace", "checkpoint is invalid"))?;

        for transaction in self.read_tail(head).await? {
            if transaction.parent.into_cursor()? != snapshot.cursor {
                return Err(corrupt(
                    "read Managed namespace",
                    "transaction tail is not consecutive",
                ));
            }
            snapshot = apply_transaction(Some(snapshot), &transaction)?;
        }
        if snapshot.cursor != head.cursor.into_cursor()? {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint and transaction tail do not reach HEAD",
            ));
        }
        Ok(snapshot)
    }

    async fn read_tail(&self, head: &StoredHead) -> Result<Vec<StoredTransaction>, ManagedError> {
        if head.tail_transactions == 0 {
            return Ok(Vec::new());
        }
        let mut current = self
            .required_transaction(OperationId::from_bytes(head.latest_transaction))
            .await?;
        let latest_bytes = encode_cbor(TRANSACTION_MAGIC, &current, "read Managed namespace")?;
        if sha256(&latest_bytes) != head.latest_transaction_sha256 || current.cursor != head.cursor
        {
            return Err(corrupt(
                "read Managed namespace",
                "latest transaction and HEAD disagree",
            ));
        }
        let mut tail = Vec::with_capacity(head.tail_transactions.into());
        for index in 0..head.tail_transactions {
            if index != 0 {
                current = self
                    .required_transaction(OperationId::from_bytes(
                        current.parent.operation.ok_or_else(|| {
                            corrupt("read Managed namespace", "transaction tail is incomplete")
                        })?,
                    ))
                    .await?;
            }
            tail.push(current.clone());
        }
        tail.reverse();
        Ok(tail)
    }

    async fn checkpoint_full(
        &self,
        snapshot: &NamespaceSnapshot,
    ) -> Result<StoredCheckpoint, ManagedError> {
        let scope = *self.volume_id.as_bytes();
        let mut encoded = Vec::new();
        encoded.extend(section::encode(
            scope,
            NODE_SECTION,
            snapshot
                .nodes
                .values()
                .map(|node| {
                    Ok(SectionRecord {
                        key: node.id.as_bytes().to_vec(),
                        value: encode_section_value(
                            &StoredNodeSection::from(node),
                            "checkpoint Managed namespace",
                        )?,
                    })
                })
                .collect::<Result<_, ManagedError>>()?,
            "checkpoint Managed namespace",
        )?);
        encoded.extend(section::encode(
            scope,
            DIRECTORY_SECTION,
            snapshot
                .directories
                .values()
                .map(|directory| {
                    Ok(SectionRecord {
                        key: directory.node.as_bytes().to_vec(),
                        value: encode_section_value(
                            &StoredDirectorySection::from(directory),
                            "checkpoint Managed namespace",
                        )?,
                    })
                })
                .collect::<Result<_, ManagedError>>()?,
            "checkpoint Managed namespace",
        )?);
        encoded.extend(section::encode(
            scope,
            DIRECTORY_ENTRY_SECTION,
            snapshot
                .directories
                .values()
                .flat_map(|directory| {
                    directory.entries.iter().map(move |(name, entry)| {
                        Ok(SectionRecord {
                            key: directory_entry_key(directory.node, name),
                            value: encode_section_value(
                                &StoredDirectoryEntry::from(*entry),
                                "checkpoint Managed namespace",
                            )?,
                        })
                    })
                })
                .collect::<Result<_, ManagedError>>()?,
            "checkpoint Managed namespace",
        )?);
        encoded.extend(section::encode(
            scope,
            FILE_VERSION_SECTION,
            snapshot
                .file_versions
                .values()
                .map(|version| {
                    Ok(SectionRecord {
                        key: version.id.as_bytes().to_vec(),
                        value: encode_section_value(
                            &StoredFileVersionSection::from(version),
                            "checkpoint Managed namespace",
                        )?,
                    })
                })
                .collect::<Result<_, ManagedError>>()?,
            "checkpoint Managed namespace",
        )?);

        let mut sections = self.persist_sections(encoded).await?;
        sections.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.first_key.cmp(&right.first_key))
        });
        Ok(StoredCheckpoint {
            major: FORMAT_MAJOR,
            volume_id: scope,
            cursor: snapshot.cursor.into(),
            root: *snapshot.root.as_bytes(),
            sections,
        })
    }

    async fn checkpoint_incremental(
        &self,
        snapshot: &NamespaceSnapshot,
        previous: &StoredCheckpoint,
        transactions: &[StoredTransaction],
    ) -> Result<StoredCheckpoint, ManagedError> {
        validate_section_references(&previous.sections)?;
        let mut cursor = previous.cursor.into_cursor()?;
        for transaction in transactions {
            if transaction.parent.into_cursor()? != cursor {
                return Err(corrupt(
                    "checkpoint Managed namespace",
                    "checkpoint transactions are not consecutive",
                ));
            }
            cursor = transaction.cursor.into_cursor()?;
        }
        if cursor != snapshot.cursor {
            return Err(corrupt(
                "checkpoint Managed namespace",
                "checkpoint transactions do not reach the target",
            ));
        }
        let sections = self
            .rewrite_checkpoint_sections(&previous.sections, section_changes(transactions)?)
            .await?;
        Ok(StoredCheckpoint {
            major: FORMAT_MAJOR,
            volume_id: *self.volume_id.as_bytes(),
            cursor: snapshot.cursor.into(),
            root: *snapshot.root.as_bytes(),
            sections,
        })
    }

    async fn rewrite_checkpoint_sections(
        &self,
        previous: &[StoredSectionReference],
        mut changes: CheckpointChanges,
    ) -> Result<Vec<StoredSectionReference>, ManagedError> {
        validate_section_references(previous)?;
        let mut output = Vec::new();
        let mut encoded = Vec::new();
        for kind in [
            NODE_SECTION,
            DIRECTORY_SECTION,
            DIRECTORY_ENTRY_SECTION,
            FILE_VERSION_SECTION,
        ] {
            let pending = changes.records.entry(kind).or_default();
            let mut unassigned = pending.keys().cloned().collect::<BTreeSet<_>>();
            let mut affected = Vec::new();
            for stored in previous.iter().filter(|section| section.kind == kind) {
                let keys = unassigned
                    .iter()
                    .take_while(|key| key.as_slice() <= stored.last_key.as_slice())
                    .cloned()
                    .collect::<Vec<_>>();
                for key in &keys {
                    unassigned.remove(key);
                }
                let removes_directory = kind == DIRECTORY_ENTRY_SECTION
                    && changes
                        .removed_directories
                        .iter()
                        .any(|directory| section_may_contain_directory(stored, directory));
                if !keys.is_empty() || removes_directory {
                    affected.push(stored.clone());
                }
            }
            let mut affected_records = self
                .read_checkpoint_sections(&affected, "checkpoint Managed namespace")
                .await?
                .into_iter()
                .map(|(stored, records)| ((stored.object, stored.offset), records))
                .collect::<BTreeMap<_, _>>();
            for stored in previous.iter().filter(|section| section.kind == kind) {
                let keys = pending
                    .keys()
                    .take_while(|key| key.as_slice() <= stored.last_key.as_slice())
                    .cloned()
                    .collect::<Vec<_>>();
                let removes_directory = kind == DIRECTORY_ENTRY_SECTION
                    && changes
                        .removed_directories
                        .iter()
                        .any(|directory| section_may_contain_directory(stored, directory));
                if keys.is_empty() && !removes_directory {
                    output.push(stored.clone());
                    continue;
                }
                let mut records = affected_records
                    .remove(&(stored.object, stored.offset))
                    .expect("affected checkpoint section was read")
                    .into_iter()
                    .map(|record| (record.key, record.value))
                    .collect::<BTreeMap<_, _>>();
                let mut changed = false;
                if removes_directory {
                    let before = records.len();
                    records.retain(|key, _| {
                        !changes
                            .removed_directories
                            .iter()
                            .any(|directory| key.starts_with(directory))
                    });
                    changed = records.len() != before;
                }
                for key in keys {
                    match pending.remove(&key).expect("collected pending change") {
                        Some(value) => {
                            changed |= records.insert(key, value.clone()).as_ref() != Some(&value);
                        }
                        None => changed |= records.remove(&key).is_some(),
                    }
                }
                if !changed {
                    output.push(stored.clone());
                    continue;
                }
                let records = records
                    .into_iter()
                    .map(|(key, value)| SectionRecord { key, value })
                    .collect();
                encoded.extend(section::encode(
                    *self.volume_id.as_bytes(),
                    kind,
                    records,
                    "checkpoint Managed namespace",
                )?);
            }
            if !pending.is_empty() {
                let records = std::mem::take(pending)
                    .into_iter()
                    .filter_map(|(key, value)| value.map(|value| SectionRecord { key, value }))
                    .collect();
                encoded.extend(section::encode(
                    *self.volume_id.as_bytes(),
                    kind,
                    records,
                    "checkpoint Managed namespace",
                )?);
            }
        }
        output.extend(self.persist_sections(encoded).await?);
        output.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.first_key.cmp(&right.first_key))
        });
        validate_section_references(&output)?;
        Ok(output)
    }

    async fn persist_sections(
        &self,
        encoded: Vec<section::Encoded>,
    ) -> Result<Vec<StoredSectionReference>, ManagedError> {
        let Some(object) = section::concatenate(encoded, "checkpoint Managed namespace")? else {
            return Ok(Vec::new());
        };
        self.ensure_immutable(&section_key(&object.id), &object.bytes)
            .await?;
        Ok(object
            .sections
            .into_iter()
            .map(|located| StoredSectionReference::from_located(object.id, located))
            .collect())
    }

    async fn read_checkpoint_sections(
        &self,
        stored: &[StoredSectionReference],
        action: &'static str,
    ) -> Result<Vec<(StoredSectionReference, Vec<SectionRecord>)>, ManagedError> {
        let mut objects = BTreeMap::<[u8; 32], Vec<StoredSectionReference>>::new();
        for section in stored {
            objects
                .entry(section.object)
                .or_default()
                .push(section.clone());
        }
        let mut decoded = Vec::with_capacity(stored.len());
        for (object, mut sections) in objects {
            sections.sort_by_key(|section| section.offset);
            let first = sections
                .first()
                .expect("section object is non-empty")
                .offset;
            let end = sections.iter().try_fold(first, |end, section| {
                section
                    .offset
                    .checked_add(section.encoded_bytes)
                    .map(|section_end| end.max(section_end))
                    .ok_or_else(|| corrupt(action, "checkpoint section range is invalid"))
            })?;
            let bytes = self
                .operator
                .read_with(&section_key(&object))
                .range(first..end)
                .await
                .map_err(|error| {
                    if error.kind() == ErrorKind::NotFound {
                        corrupt(action, "checkpoint section data object is missing")
                    } else {
                        unavailable(action)
                    }
                })?
                .to_bytes();
            for section in sections {
                let start = usize::try_from(section.offset - first)
                    .map_err(|_| corrupt(action, "checkpoint section range is invalid"))?;
                let length = usize::try_from(section.encoded_bytes)
                    .map_err(|_| corrupt(action, "checkpoint section range is invalid"))?;
                let section_end = start
                    .checked_add(length)
                    .filter(|section_end| *section_end <= bytes.len())
                    .ok_or_else(|| corrupt(action, "checkpoint section range is invalid"))?;
                let records = section::decode(
                    &section.as_reference(),
                    *self.volume_id.as_bytes(),
                    &bytes[start..section_end],
                    action,
                )?;
                decoded.push((section, records));
            }
        }
        decoded.sort_by(|(left, _), (right, _)| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.first_key.cmp(&right.first_key))
        });
        Ok(decoded)
    }

    async fn read_snapshot(
        &self,
        checkpoint: StoredCheckpoint,
    ) -> Result<NamespaceSnapshot, ManagedError> {
        validate_section_references(&checkpoint.sections)?;
        let mut nodes = BTreeMap::new();
        let mut directories = BTreeMap::new();
        let mut entries = Vec::new();
        let mut file_versions = BTreeMap::new();
        for (stored, records) in self
            .read_checkpoint_sections(&checkpoint.sections, "read Managed namespace")
            .await?
        {
            let reference = stored.as_reference();
            for record in records {
                match reference.kind {
                    NODE_SECTION => {
                        let stored: StoredNodeSection =
                            decode_section_value(&record.value, "read Managed namespace")?;
                        let id = section_node_id(&record.key, "node section key is invalid")?;
                        if nodes.contains_key(&id) {
                            return Err(corrupt(
                                "read Managed namespace",
                                "node section key is invalid",
                            ));
                        }
                        let node = stored.into_record(id);
                        nodes.insert(node.id, node);
                    }
                    DIRECTORY_SECTION => {
                        let stored: StoredDirectorySection =
                            decode_section_value(&record.value, "read Managed namespace")?;
                        let node =
                            section_node_id(&record.key, "directory section key is invalid")?;
                        let directory = stored.into_record(node);
                        if directories.insert(directory.node, directory).is_some() {
                            return Err(corrupt(
                                "read Managed namespace",
                                "duplicate directory record",
                            ));
                        }
                    }
                    DIRECTORY_ENTRY_SECTION => {
                        let stored: StoredDirectoryEntry =
                            decode_section_value(&record.value, "read Managed namespace")?;
                        let (directory, name) = section_directory_entry_key(&record.key)?;
                        entries.push((directory, name, stored));
                    }
                    FILE_VERSION_SECTION => {
                        let stored: StoredFileVersionSection =
                            decode_section_value(&record.value, "read Managed namespace")?;
                        let id = section_file_version_id(&record.key)?;
                        let version = stored.into_record(id);
                        if file_versions.insert(version.id, version).is_some() {
                            return Err(corrupt(
                                "read Managed namespace",
                                "duplicate file version record",
                            ));
                        }
                    }
                    _ => unreachable!("section reference kinds were validated"),
                }
            }
        }
        for (directory_id, name, stored) in entries {
            let directory = directories.get_mut(&directory_id).ok_or_else(|| {
                corrupt(
                    "read Managed namespace",
                    "entry references a missing directory",
                )
            })?;
            if directory.entries.insert(name, stored.into()).is_some() {
                return Err(corrupt(
                    "read Managed namespace",
                    "duplicate directory entry",
                ));
            }
        }
        Ok(NamespaceSnapshot {
            volume_id: VolumeId::from_bytes(checkpoint.volume_id),
            cursor: checkpoint.cursor.into_cursor()?,
            root: NodeId::from_bytes(checkpoint.root),
            nodes,
            directories,
            file_versions,
        })
    }

    async fn read_checkpoint(&self, id: &[u8; 32]) -> Result<StoredCheckpoint, ManagedError> {
        let bytes = self
            .operator
            .read(&checkpoint_key(id))
            .await
            .map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    corrupt("read Managed namespace", "checkpoint is missing")
                } else {
                    unavailable("read Managed namespace")
                }
            })?
            .to_bytes();
        if sha256(&bytes) != *id {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint key and content disagree",
            ));
        }
        decode_cbor(CHECKPOINT_MAGIC, &bytes, "read Managed namespace")
    }

    async fn ensure_result(
        &self,
        operation: OperationId,
        transaction_sha256: [u8; 32],
        outcome: &CommitOutcome,
    ) -> Result<(), ManagedError> {
        let outcome = match outcome {
            CommitOutcome::Committed(cursor) => StoredResultKind::Committed {
                cursor: (*cursor).into(),
            },
            CommitOutcome::Conflict { .. } => StoredResultKind::Conflict,
            CommitOutcome::Absent | CommitOutcome::Unknown => return Ok(()),
        };
        let result = StoredResult {
            major: FORMAT_MAJOR,
            operation: *operation.as_bytes(),
            transaction_sha256,
            outcome,
        };
        let bytes = encode_cbor(RESULT_MAGIC, &result, "write Managed operation result")?;
        self.ensure_immutable(&result_key(operation), &bytes).await
    }

    async fn read_result(
        &self,
        operation: OperationId,
    ) -> Result<Option<StoredResult>, ManagedError> {
        match self.operator.read(&result_key(operation)).await {
            Ok(bytes) => {
                let result: StoredResult = decode_cbor(
                    RESULT_MAGIC,
                    &bytes.to_bytes(),
                    "read Managed operation result",
                )?;
                if result.major != FORMAT_MAJOR || result.operation != *operation.as_bytes() {
                    return Err(corrupt(
                        "read Managed operation result",
                        "operation result identity is invalid",
                    ));
                }
                Ok(Some(result))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(unavailable("read Managed operation result")),
        }
    }

    async fn read_head(&self) -> Result<Option<(Vec<u8>, String)>, ManagedError> {
        let reader = match self.operator.reader(HEAD_KEY).await {
            Ok(reader) => reader,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(unavailable("read Managed namespace")),
        };
        let bytes = match reader.read(..).await {
            Ok(bytes) => bytes.to_bytes().to_vec(),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(unavailable("read Managed namespace")),
        };
        let revision = reader
            .metadata()
            .and_then(|metadata| metadata.etag())
            .ok_or_else(|| unavailable("read Managed namespace"))?
            .to_owned();
        Ok(Some((bytes, revision)))
    }

    async fn create_head(&self, bytes: Vec<u8>) -> Result<bool, ManagedError> {
        conditional_result(
            self.operator
                .write_with(HEAD_KEY, bytes)
                .if_not_exists(true)
                .await,
        )
    }

    async fn replace_head(
        &self,
        expected_revision: &str,
        bytes: Vec<u8>,
    ) -> Result<bool, ManagedError> {
        conditional_result(
            self.operator
                .write_with(HEAD_KEY, bytes)
                .if_match(expected_revision)
                .await,
        )
    }
}

fn conditional_result(result: opendal::Result<opendal::Metadata>) -> Result<bool, ManagedError> {
    match result {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(_) => Err(unavailable("publish Managed namespace")),
    }
}

fn transaction_key(operation: OperationId) -> String {
    format!("{TRANSACTION_ROOT}/{}.ofs", hex(operation.as_bytes()))
}

fn result_key(operation: OperationId) -> String {
    format!("{RESULT_ROOT}/{}.ofs", hex(operation.as_bytes()))
}

fn checkpoint_key(id: &[u8; 32]) -> String {
    format!("{CHECKPOINT_ROOT}/{}.ofs", hex(id))
}

fn section_key(id: &[u8; 32]) -> String {
    let encoded = hex(id);
    format!("{SECTION_ROOT}/{encoded}.ofs")
}

fn directory_entry_key(directory: NodeId, name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + name.len());
    key.extend_from_slice(directory.as_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

fn section_node_id(key: &[u8], message: &'static str) -> Result<NodeId, ManagedError> {
    let bytes = key
        .try_into()
        .map_err(|_| corrupt("read Managed namespace", message))?;
    Ok(NodeId::from_bytes(bytes))
}

fn section_directory_entry_key(key: &[u8]) -> Result<(NodeId, String), ManagedError> {
    let (directory, name) = key.split_at_checked(16).ok_or_else(|| {
        corrupt(
            "read Managed namespace",
            "directory entry section key is invalid",
        )
    })?;
    let directory = NodeId::from_bytes(directory.try_into().expect("fixed key prefix"));
    let name = String::from_utf8(name.to_vec()).map_err(|_| {
        corrupt(
            "read Managed namespace",
            "directory entry section key is invalid",
        )
    })?;
    Ok((directory, name))
}

fn section_file_version_id(key: &[u8]) -> Result<FileVersionId, ManagedError> {
    let bytes = key.try_into().map_err(|_| {
        corrupt(
            "read Managed namespace",
            "file version section key is invalid",
        )
    })?;
    Ok(FileVersionId::from_bytes(bytes))
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

fn encode_head(value: &StoredHead) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|_| invalid("write Managed namespace", "HEAD cannot be encoded"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn decode_head(bytes: &[u8]) -> Result<StoredHead, ManagedError> {
    serde_json::from_slice(bytes).map_err(|_| corrupt("read Managed namespace", "HEAD is invalid"))
}

fn encode_cbor<T: Serialize>(
    magic: &[u8],
    value: &T,
    action: &'static str,
) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::from(magic);
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| invalid(action, "durable record cannot be encoded"))?;
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

fn encode_section_value<T: Serialize>(
    value: &T,
    action: &'static str,
) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|_| invalid(action, "section record cannot be encoded"))?;
    Ok(bytes)
}

fn decode_section_value<T: DeserializeOwned>(
    bytes: &[u8],
    action: &'static str,
) -> Result<T, ManagedError> {
    let mut input = Cursor::new(bytes);
    let value = ciborium::de::from_reader(&mut input)
        .map_err(|_| corrupt(action, "section record cannot be decoded"))?;
    if input.position() != bytes.len() as u64 {
        return Err(corrupt(action, "section record has trailing bytes"));
    }
    Ok(value)
}

fn decode_cbor<T: DeserializeOwned>(
    magic: &[u8],
    bytes: &[u8],
    action: &'static str,
) -> Result<T, ManagedError> {
    let body = bytes
        .strip_prefix(magic)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| corrupt(action, "durable record has the wrong format version"))?;
    let expected = bytes
        .get(bytes.len().saturating_sub(32)..)
        .ok_or_else(|| corrupt(action, "durable record checksum is missing"))?;
    if Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != expected {
        return Err(corrupt(action, "durable record checksum does not match"));
    }
    let mut input = Cursor::new(body);
    let value = ciborium::de::from_reader(&mut input)
        .map_err(|_| corrupt(action, "durable record is invalid"))?;
    if input.position() != body.len() as u64 {
        return Err(corrupt(action, "durable record has trailing bytes"));
    }
    Ok(value)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
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

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "object metadata is unavailable",
    )
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredHead {
    magic: String,
    major: u16,
    volume_id: [u8; 16],
    cursor: StoredCursor,
    latest_transaction: [u8; 16],
    latest_transaction_sha256: [u8; 32],
    checkpoint: [u8; 32],
    checkpoint_cursor: StoredCursor,
    tail_transactions: u16,
    maintenance_epoch: u64,
    maintenance_state: StoredMaintenanceState,
    maintenance_fixed_cursor: Option<StoredCursor>,
    checksum: [u8; 32],
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredMaintenanceState {
    Idle,
    Sweeping,
}

impl StoredHead {
    fn new(
        volume_id: VolumeId,
        cursor: StoredCursor,
        latest_transaction: [u8; 16],
        latest_transaction_sha256: [u8; 32],
        checkpoint: [u8; 32],
        checkpoint_cursor: StoredCursor,
        tail_transactions: u16,
    ) -> Result<Self, ManagedError> {
        let mut head = Self {
            magic: HEAD_MAGIC.into(),
            major: FORMAT_MAJOR,
            volume_id: *volume_id.as_bytes(),
            cursor,
            latest_transaction,
            latest_transaction_sha256,
            checkpoint,
            checkpoint_cursor,
            tail_transactions,
            maintenance_epoch: 0,
            maintenance_state: StoredMaintenanceState::Idle,
            maintenance_fixed_cursor: None,
            checksum: [0; 32],
        };
        head.validate_shape()?;
        head.checksum = head_checksum(&head);
        Ok(head)
    }

    fn with_maintenance_epoch(mut self, epoch: u64) -> Self {
        self.maintenance_epoch = epoch;
        self.checksum = head_checksum(&self);
        self
    }

    fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        self.validate_shape()?;
        if self.volume_id != *volume_id.as_bytes() || self.checksum != head_checksum(self) {
            return Err(corrupt(
                "read Managed namespace",
                "HEAD integrity is invalid",
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), ManagedError> {
        let cursor = self.cursor.into_cursor()?;
        let checkpoint = self.checkpoint_cursor.into_cursor()?;
        if self.magic != HEAD_MAGIC
            || self.major != FORMAT_MAJOR
            || cursor.operation() != Some(OperationId::from_bytes(self.latest_transaction))
            || self.tail_transactions > MAX_TAIL_TRANSACTIONS
            || checkpoint
                .sequence()
                .checked_add(u64::from(self.tail_transactions))
                != Some(cursor.sequence())
            || self.gc_sweep().is_err()
        {
            return Err(corrupt("read Managed namespace", "HEAD shape is invalid"));
        }
        Ok(())
    }

    fn gc_sweep(&self) -> Result<Option<NamespaceGcSweep>, ManagedError> {
        match (self.maintenance_state, self.maintenance_fixed_cursor) {
            (StoredMaintenanceState::Idle, None) => Ok(None),
            (StoredMaintenanceState::Sweeping, Some(fixed))
                if self.maintenance_epoch > 0 && fixed == self.cursor =>
            {
                Ok(Some(NamespaceGcSweep::new(
                    self.maintenance_epoch,
                    fixed.into_cursor()?,
                )))
            }
            _ => Err(corrupt(
                "read Managed namespace",
                "HEAD maintenance state is invalid",
            )),
        }
    }

    fn begin_gc(&mut self) -> Result<NamespaceGcSweep, ManagedError> {
        if let Some(sweep) = self.gc_sweep()? {
            return Ok(sweep);
        }
        self.maintenance_epoch = self.maintenance_epoch.checked_add(1).ok_or_else(|| {
            corrupt(
                "begin Managed namespace GC",
                "maintenance epoch is exhausted",
            )
        })?;
        self.maintenance_state = StoredMaintenanceState::Sweeping;
        self.maintenance_fixed_cursor = Some(self.cursor);
        self.checksum = head_checksum(self);
        Ok(NamespaceGcSweep::new(
            self.maintenance_epoch,
            self.cursor.into_cursor()?,
        ))
    }

    fn finish_gc(&mut self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        if self.gc_sweep()? != Some(sweep) {
            return Err(conflict(
                "finish Managed namespace GC",
                "GC sweep token does not match the authority",
            ));
        }
        self.maintenance_state = StoredMaintenanceState::Idle;
        self.maintenance_fixed_cursor = None;
        self.checksum = head_checksum(self);
        Ok(())
    }
}

fn head_checksum(head: &StoredHead) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"OFS1HEAD\0");
    digest.update(head.magic.as_bytes());
    digest.update(head.major.to_be_bytes());
    digest.update(head.volume_id);
    update_cursor_digest(&mut digest, head.cursor);
    digest.update(head.latest_transaction);
    digest.update(head.latest_transaction_sha256);
    digest.update(head.checkpoint);
    update_cursor_digest(&mut digest, head.checkpoint_cursor);
    digest.update(head.tail_transactions.to_be_bytes());
    digest.update(head.maintenance_epoch.to_be_bytes());
    digest.update([match head.maintenance_state {
        StoredMaintenanceState::Idle => 0,
        StoredMaintenanceState::Sweeping => 1,
    }]);
    match head.maintenance_fixed_cursor {
        Some(cursor) => {
            digest.update([1]);
            update_cursor_digest(&mut digest, cursor);
        }
        None => digest.update([0]),
    }
    digest.finalize().into()
}

fn update_cursor_digest(digest: &mut Sha256, cursor: StoredCursor) {
    digest.update(cursor.sequence.to_be_bytes());
    match cursor.operation {
        Some(operation) => {
            digest.update([1]);
            digest.update(operation);
        }
        None => digest.update([0]),
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredTransaction {
    major: u16,
    volume_id: [u8; 16],
    operation: [u8; 16],
    parent: StoredCursor,
    cursor: StoredCursor,
    root: [u8; 16],
    expected_nodes: Vec<StoredNodePrecondition>,
    expected_directories: Vec<StoredDirectoryPrecondition>,
    put_nodes: Vec<StoredNode>,
    remove_nodes: Vec<[u8; 16]>,
    put_directories: Vec<StoredDirectoryHeader>,
    remove_directories: Vec<[u8; 16]>,
    put_directory_entries: Vec<StoredNamedDirectoryEntry>,
    remove_directory_entries: Vec<StoredDirectoryEntryKey>,
    put_file_versions: Vec<StoredFileVersion>,
    remove_file_versions: Vec<[u8; 32]>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCheckpoint {
    major: u16,
    volume_id: [u8; 16],
    cursor: StoredCursor,
    root: [u8; 16],
    sections: Vec<StoredSectionReference>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredSectionReference {
    kind: u8,
    id: [u8; 32],
    object: [u8; 32],
    offset: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    records: u32,
    encoded_bytes: u64,
}

struct CheckpointChanges {
    records: BTreeMap<u8, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    removed_directories: BTreeSet<[u8; 16]>,
}

impl StoredSectionReference {
    fn from_located(object: [u8; 32], located: section::Located) -> Self {
        let reference = located.reference;
        Self {
            kind: reference.kind,
            id: reference.id,
            object,
            offset: located.offset,
            first_key: reference.first_key,
            last_key: reference.last_key,
            records: reference.records,
            encoded_bytes: reference.encoded_bytes,
        }
    }

    fn as_reference(&self) -> SectionReference {
        SectionReference {
            kind: self.kind,
            id: self.id,
            first_key: self.first_key.clone(),
            last_key: self.last_key.clone(),
            records: self.records,
            encoded_bytes: self.encoded_bytes,
        }
    }
}

fn validate_section_references(sections: &[StoredSectionReference]) -> Result<(), ManagedError> {
    let mut previous: Option<&StoredSectionReference> = None;
    let mut ranges = BTreeMap::<[u8; 32], Vec<(u64, u64)>>::new();
    for section in sections {
        let end = section.offset.checked_add(section.encoded_bytes);
        if !matches!(
            section.kind,
            NODE_SECTION | DIRECTORY_SECTION | DIRECTORY_ENTRY_SECTION | FILE_VERSION_SECTION
        ) || section.records == 0
            || section.encoded_bytes == 0
            || end.is_none()
            || section.first_key > section.last_key
            || previous.is_some_and(|previous| {
                previous.kind > section.kind
                    || previous.kind == section.kind && previous.last_key >= section.first_key
            })
        {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint section references are invalid",
            ));
        }
        ranges
            .entry(section.object)
            .or_default()
            .push((section.offset, end.expect("range end was validated")));
        previous = Some(section);
    }
    for object_ranges in ranges.values_mut() {
        object_ranges.sort_unstable();
        if object_ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint section ranges overlap",
            ));
        }
    }
    Ok(())
}

fn section_changes(transactions: &[StoredTransaction]) -> Result<CheckpointChanges, ManagedError> {
    let mut changes = CheckpointChanges {
        records: BTreeMap::new(),
        removed_directories: BTreeSet::new(),
    };
    for transaction in transactions {
        let nodes = changes.records.entry(NODE_SECTION).or_default();
        for node in &transaction.put_nodes {
            nodes.insert(
                node.id.to_vec(),
                Some(encode_section_value(
                    &StoredNodeSection {
                        generation: node.generation,
                        kind: node.kind,
                        attributes: node.attributes,
                        file_version: node.file_version,
                    },
                    "checkpoint Managed namespace",
                )?),
            );
        }
        for id in &transaction.remove_nodes {
            nodes.insert(id.to_vec(), None);
        }

        let directories = changes.records.entry(DIRECTORY_SECTION).or_default();
        for directory in &transaction.put_directories {
            directories.insert(
                directory.node.to_vec(),
                Some(encode_section_value(
                    &StoredDirectorySection {
                        generation: directory.generation,
                    },
                    "checkpoint Managed namespace",
                )?),
            );
        }
        for id in &transaction.remove_directories {
            directories.insert(id.to_vec(), None);
            changes.removed_directories.insert(*id);
        }

        let entries = changes.records.entry(DIRECTORY_ENTRY_SECTION).or_default();
        for id in &transaction.remove_directories {
            entries.retain(|key, _| !key.starts_with(id));
        }
        for entry in &transaction.put_directory_entries {
            entries.insert(
                directory_entry_key(NodeId::from_bytes(entry.directory), &entry.name),
                Some(encode_section_value(
                    &entry.entry,
                    "checkpoint Managed namespace",
                )?),
            );
        }
        for entry in &transaction.remove_directory_entries {
            entries.insert(
                directory_entry_key(NodeId::from_bytes(entry.directory), &entry.name),
                None,
            );
        }

        let versions = changes.records.entry(FILE_VERSION_SECTION).or_default();
        for version in &transaction.put_file_versions {
            versions.insert(
                version.id.to_vec(),
                Some(encode_section_value(
                    &StoredFileVersionSection {
                        logical_size: version.logical_size,
                        logical_digest: version.logical_digest,
                        layout: version.layout.clone(),
                    },
                    "checkpoint Managed namespace",
                )?),
            );
        }
        for id in &transaction.remove_file_versions {
            versions.insert(id.to_vec(), None);
        }
    }
    Ok(changes)
}

fn section_may_contain_directory(section: &StoredSectionReference, directory: &[u8; 16]) -> bool {
    match (section.first_key.get(..16), section.last_key.get(..16)) {
        (Some(first), Some(last)) => first <= directory.as_slice() && directory.as_slice() <= last,
        _ => true,
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredResult {
    major: u16,
    operation: [u8; 16],
    transaction_sha256: [u8; 32],
    outcome: StoredResultKind,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum StoredResultKind {
    Committed { cursor: StoredCursor },
    Conflict,
}

impl StoredTransaction {
    fn from_publication(
        publication: &NamespacePublication,
        base: Option<&NamespaceSnapshot>,
    ) -> Self {
        let change = NamespaceChange::from_publication(publication, base);
        Self {
            major: FORMAT_MAJOR,
            volume_id: *change.volume_id.as_bytes(),
            operation: *change.operation.as_bytes(),
            parent: change.parent.into(),
            cursor: change.cursor.into(),
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
            put_nodes: change.put_nodes.iter().map(StoredNode::from).collect(),
            remove_nodes: change
                .remove_nodes
                .iter()
                .map(|id| *id.as_bytes())
                .collect(),
            put_directories: change
                .put_directories
                .iter()
                .map(StoredDirectoryHeader::from)
                .collect(),
            remove_directories: change
                .remove_directories
                .iter()
                .map(|id| *id.as_bytes())
                .collect(),
            put_directory_entries: change
                .put_directories
                .iter()
                .flat_map(|directory| {
                    let base = base.and_then(|snapshot| snapshot.directories.get(&directory.node));
                    directory
                        .entries
                        .iter()
                        .filter(move |(name, entry)| {
                            base.and_then(|base| base.entries.get(*name)) != Some(*entry)
                        })
                        .map(move |(name, entry)| StoredNamedDirectoryEntry {
                            directory: *directory.node.as_bytes(),
                            name: name.clone(),
                            entry: (*entry).into(),
                        })
                })
                .collect(),
            remove_directory_entries: change
                .put_directories
                .iter()
                .flat_map(|directory| {
                    base.and_then(|snapshot| snapshot.directories.get(&directory.node))
                        .into_iter()
                        .flat_map(move |base| {
                            base.entries
                                .keys()
                                .filter(move |name| !directory.entries.contains_key(*name))
                                .map(move |name| StoredDirectoryEntryKey {
                                    directory: *directory.node.as_bytes(),
                                    name: name.clone(),
                                })
                        })
                })
                .collect(),
            put_file_versions: change
                .put_file_versions
                .iter()
                .map(StoredFileVersion::from)
                .collect(),
            remove_file_versions: change
                .remove_file_versions
                .iter()
                .map(|id| *id.as_bytes())
                .collect(),
        }
    }

    fn validate(&self, volume_id: VolumeId) -> Result<(), ManagedError> {
        let parent = self.parent.into_cursor()?;
        let cursor = self.cursor.into_cursor()?;
        if self.major != FORMAT_MAJOR
            || self.volume_id != *volume_id.as_bytes()
            || cursor.operation() != Some(OperationId::from_bytes(self.operation))
            || parent.sequence().checked_add(1) != Some(cursor.sequence())
        {
            return Err(corrupt(
                "read Managed transaction",
                "transaction ancestry is invalid",
            ));
        }
        Ok(())
    }

    fn to_change(&self, base: Option<&NamespaceSnapshot>) -> Result<NamespaceChange, ManagedError> {
        let volume_id = VolumeId::from_bytes(self.volume_id);
        self.validate(volume_id)?;
        let mut put_directories = BTreeMap::new();
        for header in &self.put_directories {
            let node = NodeId::from_bytes(header.node);
            if put_directories.contains_key(&node) {
                return Err(corrupt(
                    "read Managed transaction",
                    "transaction repeats a directory header",
                ));
            }
            let mut directory = base
                .and_then(|snapshot| snapshot.directories.get(&node))
                .cloned()
                .unwrap_or_else(|| header.into_record());
            directory.generation = managed_generation(header.generation);
            put_directories.insert(node, directory);
        }
        for removed in &self.remove_directory_entries {
            let directory = put_directories
                .get_mut(&NodeId::from_bytes(removed.directory))
                .ok_or_else(|| {
                    corrupt(
                        "read Managed transaction",
                        "directory entry removal has no directory header",
                    )
                })?;
            if directory.entries.remove(&removed.name).is_none() {
                return Err(corrupt(
                    "read Managed transaction",
                    "directory entry removal is stale",
                ));
            }
        }
        for stored in &self.put_directory_entries {
            let directory = put_directories
                .get_mut(&NodeId::from_bytes(stored.directory))
                .ok_or_else(|| {
                    corrupt(
                        "read Managed transaction",
                        "directory entry update has no directory header",
                    )
                })?;
            directory
                .entries
                .insert(stored.name.clone(), stored.entry.into());
        }
        Ok(NamespaceChange {
            volume_id,
            operation: OperationId::from_bytes(self.operation),
            parent: self.parent.into_cursor()?,
            cursor: self.cursor.into_cursor()?,
            root: NodeId::from_bytes(self.root),
            expected_nodes: self
                .expected_nodes
                .iter()
                .cloned()
                .map(StoredNodePrecondition::into_record)
                .collect(),
            expected_directories: self
                .expected_directories
                .iter()
                .cloned()
                .map(StoredDirectoryPrecondition::into_record)
                .collect(),
            put_nodes: self
                .put_nodes
                .iter()
                .cloned()
                .map(StoredNode::into_record)
                .collect::<Result<_, _>>()?,
            remove_nodes: self
                .remove_nodes
                .iter()
                .copied()
                .map(NodeId::from_bytes)
                .collect(),
            put_directories: put_directories.into_values().collect(),
            remove_directories: self
                .remove_directories
                .iter()
                .copied()
                .map(NodeId::from_bytes)
                .collect(),
            put_file_versions: self
                .put_file_versions
                .iter()
                .cloned()
                .map(StoredFileVersion::into_record)
                .collect::<Result<_, _>>()?,
            remove_file_versions: self
                .remove_file_versions
                .iter()
                .copied()
                .map(FileVersionId::from_bytes)
                .collect(),
        })
    }
}

fn apply_transaction(
    base: Option<NamespaceSnapshot>,
    transaction: &StoredTransaction,
) -> Result<NamespaceSnapshot, ManagedError> {
    let change = transaction.to_change(base.as_ref())?;
    change.apply(base)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCursor {
    sequence: u64,
    operation: Option<[u8; 16]>,
}

impl From<ChangeCursor> for StoredCursor {
    fn from(cursor: ChangeCursor) -> Self {
        Self {
            sequence: cursor.sequence(),
            operation: cursor.operation().map(|operation| *operation.as_bytes()),
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

#[derive(Clone, Deserialize, Serialize)]
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
            file_version: node.file_version.map(|version| *version.as_bytes()),
        }
    }
}

impl StoredNode {
    fn into_record(self) -> Result<NodeRecord, ManagedError> {
        Ok(NodeRecord {
            id: NodeId::from_bytes(self.id),
            generation: managed_generation(self.generation),
            kind: self.kind.into(),
            attributes: self.attributes.into(),
            file_version: self.file_version.map(FileVersionId::from_bytes),
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNodeSection {
    generation: u64,
    kind: StoredNodeKind,
    attributes: StoredNodeAttributes,
    file_version: Option<[u8; 32]>,
}

impl From<&NodeRecord> for StoredNodeSection {
    fn from(node: &NodeRecord) -> Self {
        Self {
            generation: managed_generation_number(&node.generation)
                .expect("validated Managed node generation"),
            kind: node.kind.into(),
            attributes: node.attributes.into(),
            file_version: node.file_version.map(|version| *version.as_bytes()),
        }
    }
}

impl StoredNodeSection {
    fn into_record(self, id: NodeId) -> NodeRecord {
        NodeRecord {
            id,
            generation: managed_generation(self.generation),
            kind: self.kind.into(),
            attributes: self.attributes.into(),
            file_version: self.file_version.map(FileVersionId::from_bytes),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryHeader {
    node: [u8; 16],
    generation: u64,
}

impl From<&DirectoryRecord> for StoredDirectoryHeader {
    fn from(directory: &DirectoryRecord) -> Self {
        Self {
            node: *directory.node.as_bytes(),
            generation: managed_generation_number(&directory.generation)
                .expect("validated Managed directory generation"),
        }
    }
}

impl StoredDirectoryHeader {
    fn into_record(self) -> DirectoryRecord {
        DirectoryRecord {
            node: NodeId::from_bytes(self.node),
            generation: managed_generation(self.generation),
            entries: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectorySection {
    generation: u64,
}

impl From<&DirectoryRecord> for StoredDirectorySection {
    fn from(directory: &DirectoryRecord) -> Self {
        Self {
            generation: managed_generation_number(&directory.generation)
                .expect("validated Managed directory generation"),
        }
    }
}

impl StoredDirectorySection {
    fn into_record(self, node: NodeId) -> DirectoryRecord {
        DirectoryRecord {
            node,
            generation: managed_generation(self.generation),
            entries: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNamedDirectoryEntry {
    directory: [u8; 16],
    name: String,
    entry: StoredDirectoryEntry,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDirectoryEntryKey {
    directory: [u8; 16],
    name: String,
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

#[derive(Clone, Deserialize, Serialize)]
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
    fn into_record(self) -> Result<FileVersionRecord, ManagedError> {
        Ok(FileVersionRecord {
            id: FileVersionId::from_bytes(self.id),
            logical_size: self.logical_size,
            logical_digest: self.logical_digest,
            layout: self.layout,
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredFileVersionSection {
    logical_size: u64,
    logical_digest: [u8; 32],
    layout: FileVersionLayout,
}

impl From<&FileVersionRecord> for StoredFileVersionSection {
    fn from(version: &FileVersionRecord) -> Self {
        Self {
            logical_size: version.logical_size,
            logical_digest: version.logical_digest,
            layout: version.layout.clone(),
        }
    }
}

impl StoredFileVersionSection {
    fn into_record(self, id: FileVersionId) -> FileVersionRecord {
        FileVersionRecord {
            id,
            logical_size: self.logical_size,
            logical_digest: self.logical_digest,
            layout: self.layout,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
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

impl StoredNodePrecondition {
    fn into_record(self) -> NodePrecondition {
        NodePrecondition {
            node: NodeId::from_bytes(self.node),
            expected_generation: self.expected_generation.map(managed_generation),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
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

impl StoredDirectoryPrecondition {
    fn into_record(self) -> DirectoryPrecondition {
        DirectoryPrecondition {
            directory: NodeId::from_bytes(self.directory),
            expected_generation: self.expected_generation.map(managed_generation),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::services::Memory;

    fn root_snapshot(cursor: ChangeCursor) -> NamespaceSnapshot {
        let root = NodeId::from_bytes([3; 16]);
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
                    attributes: NodeAttributes::default(),
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
    fn durable_cbor_is_deterministic_and_rejects_corruption() {
        let operation = OperationId::from_bytes([7; 16]);
        let result = StoredResult {
            major: FORMAT_MAJOR,
            operation: *operation.as_bytes(),
            transaction_sha256: [9; 32],
            outcome: StoredResultKind::Committed {
                cursor: ChangeCursor::at(NonZeroU64::new(1).unwrap(), operation).into(),
            },
        };
        let encoded = encode_cbor(RESULT_MAGIC, &result, "test").unwrap();
        assert_eq!(encoded, encode_cbor(RESULT_MAGIC, &result, "test").unwrap());
        assert_eq!(
            hex(&sha256(&encoded)),
            "142474aa1080f5ac4edc2708298130e4ca08a90d800f19c575d4283eede1362c"
        );
        let last = encoded.len() - 1;
        let mut corrupt = encoded;
        corrupt[last] ^= 1;
        assert!(decode_cbor::<StoredResult>(RESULT_MAGIC, &corrupt, "test").is_err());
    }

    #[test]
    fn head_recovers_the_same_gc_sweep_until_it_is_finished() {
        let volume = VolumeId::from_bytes([1; 16]);
        let operation = OperationId::from_bytes([2; 16]);
        let cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), operation);
        let mut head = StoredHead::new(
            volume,
            cursor.into(),
            *operation.as_bytes(),
            [3; 32],
            [4; 32],
            cursor.into(),
            0,
        )
        .unwrap();

        let sweep = head.begin_gc().unwrap();
        let mut recovered = decode_head(&encode_head(&head).unwrap()).unwrap();
        recovered.validate(volume).unwrap();
        assert_eq!(recovered.gc_sweep().unwrap(), Some(sweep));

        recovered.finish_gc(sweep).unwrap();
        let idle = decode_head(&encode_head(&recovered).unwrap()).unwrap();
        idle.validate(volume).unwrap();
        assert_eq!(idle.gc_sweep().unwrap(), None);
        assert_eq!(idle.maintenance_epoch, sweep.epoch());
    }

    #[test]
    fn one_interpreter_recovers_checkpoint_and_bounded_tail() {
        let first = OperationId::from_bytes([4; 16]);
        let first_cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), first);
        let first_snapshot = root_snapshot(first_cursor);
        let first_publication = NamespacePublication {
            operation: first,
            parent: ChangeCursor::Genesis,
            expected_nodes: vec![NodePrecondition {
                node: first_snapshot.root,
                expected_generation: None,
            }],
            expected_directories: vec![DirectoryPrecondition {
                directory: first_snapshot.root,
                expected_generation: None,
            }],
            target: first_snapshot.clone(),
        };
        let checkpoint = apply_transaction(
            None,
            &StoredTransaction::from_publication(&first_publication, None),
        )
        .unwrap();
        let second = OperationId::from_bytes([5; 16]);
        let mut target = checkpoint.clone();
        target.cursor = ChangeCursor::at(NonZeroU64::new(2).unwrap(), second);
        let publication = NamespacePublication {
            operation: second,
            parent: first_cursor,
            expected_nodes: Vec::new(),
            expected_directories: Vec::new(),
            target: target.clone(),
        };
        let recovered = apply_transaction(
            Some(checkpoint),
            &StoredTransaction::from_publication(&publication, Some(&first_snapshot)),
        )
        .unwrap();
        assert_eq!(recovered, target);
    }

    #[test]
    fn wide_directory_transaction_encodes_only_entry_changes() {
        let first = OperationId::from_bytes([11; 16]);
        let first_cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), first);
        let mut base = root_snapshot(first_cursor);
        let directory = base.directories.get_mut(&base.root).unwrap();
        directory.entries = (0..20_000)
            .map(|index| {
                (
                    format!("file-{index:05}"),
                    DirectoryEntry {
                        node: NodeId::from_bytes((index as u128).to_be_bytes()),
                        kind: NodeKind::RegularFile,
                    },
                )
            })
            .collect();
        let second = OperationId::from_bytes([12; 16]);
        let mut target = base.clone();
        target.cursor = ChangeCursor::at(NonZeroU64::new(2).unwrap(), second);
        let target_directory = target.directories.get_mut(&target.root).unwrap();
        target_directory.generation = managed_generation(2);
        target_directory.entries.insert(
            "one-new-file".to_owned(),
            DirectoryEntry {
                node: NodeId::from_bytes([13; 16]),
                kind: NodeKind::RegularFile,
            },
        );
        let publication = NamespacePublication {
            operation: second,
            parent: first_cursor,
            expected_nodes: Vec::new(),
            expected_directories: vec![DirectoryPrecondition {
                directory: target.root,
                expected_generation: Some(managed_generation(1)),
            }],
            target: target.clone(),
        };
        let stored = StoredTransaction::from_publication(&publication, Some(&base));
        let encoded = encode_cbor(TRANSACTION_MAGIC, &stored, "test").unwrap();
        assert!(encoded.len() < 2_000, "encoded {} bytes", encoded.len());
    }

    #[tokio::test]
    async fn checkpoint_sections_round_trip_without_a_whole_snapshot_record() {
        let operation = OperationId::from_bytes([8; 16]);
        let cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), operation);
        let mut snapshot = root_snapshot(cursor);
        snapshot
            .directories
            .get_mut(&snapshot.root)
            .unwrap()
            .entries
            .insert(
                "child".to_owned(),
                DirectoryEntry {
                    node: snapshot.root,
                    kind: NodeKind::Directory,
                },
            );
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let namespace = ObjectNamespace {
            volume_id: snapshot.volume_id,
            operator: operator.clone(),
        };
        let checkpoint = namespace.checkpoint_full(&snapshot).await.unwrap();
        assert!(
            checkpoint
                .sections
                .iter()
                .any(|section| section.kind == DIRECTORY_ENTRY_SECTION)
        );
        assert!(
            checkpoint
                .sections
                .iter()
                .all(|section| section.object == checkpoint.sections[0].object)
        );
        let mut ranges = checkpoint
            .sections
            .iter()
            .map(|section| (section.offset, section.encoded_bytes))
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        assert!(
            ranges
                .windows(2)
                .all(|pair| pair[0].0 + pair[0].1 == pair[1].0)
        );
        let missing = section_key(&checkpoint.sections[0].object);
        operator.delete(&missing).await.unwrap();
        let checkpoint = namespace.checkpoint_full(&snapshot).await.unwrap();
        assert!(operator.stat(&missing).await.is_ok());
        let recovered = namespace.read_snapshot(checkpoint).await.unwrap();
        assert_eq!(recovered, snapshot);
    }

    #[tokio::test]
    async fn incremental_checkpoint_rewrites_only_the_affected_section() {
        let scope = [21; 16];
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let namespace = ObjectNamespace {
            volume_id: VolumeId::from_bytes(scope),
            operator,
        };
        let records = (0_u32..80)
            .map(|index| SectionRecord {
                key: index.to_be_bytes().to_vec(),
                value: vec![index as u8; 32],
            })
            .collect::<Vec<_>>();
        let encoded =
            section::encode_for_test(scope, NODE_SECTION, records.clone(), 256, 512, 2048).unwrap();
        assert!(encoded.len() > 2);
        let previous = namespace.persist_sections(encoded).await.unwrap();
        let changed_key = 40_u32.to_be_bytes().to_vec();
        let changed_section = previous
            .iter()
            .find(|section| {
                section.first_key.as_slice() <= changed_key.as_slice()
                    && changed_key.as_slice() <= section.last_key.as_slice()
            })
            .unwrap()
            .id;
        let previous_ids = previous
            .iter()
            .map(|section| section.id)
            .collect::<std::collections::BTreeSet<_>>();
        let mut changes = CheckpointChanges {
            records: BTreeMap::new(),
            removed_directories: BTreeSet::new(),
        };
        changes
            .records
            .entry(NODE_SECTION)
            .or_default()
            .insert(changed_key.clone(), Some(b"changed".to_vec()));

        let rewritten = namespace
            .rewrite_checkpoint_sections(&previous, changes)
            .await
            .unwrap();
        let rewritten_ids = rewritten
            .iter()
            .map(|section| section.id)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            previous_ids.intersection(&rewritten_ids).count(),
            previous.len() - 1
        );
        assert!(!rewritten_ids.contains(&changed_section));

        let mut recovered = BTreeMap::new();
        for (_, records) in namespace
            .read_checkpoint_sections(&rewritten, "test")
            .await
            .unwrap()
        {
            for record in records {
                recovered.insert(record.key, record.value);
            }
        }
        let mut expected = records
            .into_iter()
            .map(|record| (record.key, record.value))
            .collect::<BTreeMap<_, _>>();
        expected.insert(changed_key, b"changed".to_vec());
        assert_eq!(recovered, expected);
    }

    #[tokio::test]
    async fn incremental_checkpoint_removes_a_directory_entry_range() {
        let scope = [22; 16];
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let namespace = ObjectNamespace {
            volume_id: VolumeId::from_bytes(scope),
            operator,
        };
        let removed = [2; 16];
        let records = [([1; 16], 30_u8), (removed, 60_u8), ([3; 16], 90_u8)]
            .into_iter()
            .flat_map(|(directory, end)| {
                (end - 30..end).map(move |index| SectionRecord {
                    key: directory_entry_key(
                        NodeId::from_bytes(directory),
                        &format!("entry-{index:03}"),
                    ),
                    value: vec![index; 32],
                })
            })
            .collect::<Vec<_>>();
        let encoded =
            section::encode_for_test(scope, DIRECTORY_ENTRY_SECTION, records, 256, 512, 2048)
                .unwrap();
        let previous = namespace.persist_sections(encoded).await.unwrap();
        let changes = CheckpointChanges {
            records: BTreeMap::new(),
            removed_directories: BTreeSet::from([removed]),
        };

        let rewritten = namespace
            .rewrite_checkpoint_sections(&previous, changes)
            .await
            .unwrap();
        let mut recovered = Vec::new();
        for (_, records) in namespace
            .read_checkpoint_sections(&rewritten, "test")
            .await
            .unwrap()
        {
            recovered.extend(records);
        }
        assert_eq!(recovered.len(), 60);
        assert!(
            recovered
                .iter()
                .all(|record| !record.key.starts_with(&removed))
        );
    }

    #[tokio::test]
    async fn incremental_checkpoint_drops_entries_for_a_directory_created_and_deleted_in_tail() {
        let first = OperationId::from_bytes([31; 16]);
        let first_cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), first);
        let base = root_snapshot(first_cursor);
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let namespace = ObjectNamespace {
            volume_id: base.volume_id,
            operator,
        };
        let previous = namespace.checkpoint_full(&base).await.unwrap();

        let second = OperationId::from_bytes([32; 16]);
        let second_cursor = ChangeCursor::at(NonZeroU64::new(2).unwrap(), second);
        let child = NodeId::from_bytes([9; 16]);
        let mut created = base.clone();
        created.cursor = second_cursor;
        created.nodes.insert(
            child,
            NodeRecord {
                id: child,
                generation: managed_generation(1),
                kind: NodeKind::Directory,
                attributes: NodeAttributes::default(),
                file_version: None,
            },
        );
        created.directories.insert(
            child,
            DirectoryRecord {
                node: child,
                generation: managed_generation(1),
                entries: BTreeMap::from([(
                    "entry".to_owned(),
                    DirectoryEntry {
                        node: base.root,
                        kind: NodeKind::Directory,
                    },
                )]),
            },
        );
        created
            .directories
            .get_mut(&created.root)
            .unwrap()
            .entries
            .insert(
                "child".to_owned(),
                DirectoryEntry {
                    node: child,
                    kind: NodeKind::Directory,
                },
            );
        let create = StoredTransaction::from_publication(
            &NamespacePublication {
                operation: second,
                parent: first_cursor,
                expected_nodes: Vec::new(),
                expected_directories: Vec::new(),
                target: created.clone(),
            },
            Some(&base),
        );

        let third = OperationId::from_bytes([33; 16]);
        let mut removed = base.clone();
        removed.cursor = ChangeCursor::at(NonZeroU64::new(3).unwrap(), third);
        let remove = StoredTransaction::from_publication(
            &NamespacePublication {
                operation: third,
                parent: second_cursor,
                expected_nodes: Vec::new(),
                expected_directories: Vec::new(),
                target: removed.clone(),
            },
            Some(&created),
        );

        let checkpoint = namespace
            .checkpoint_incremental(&removed, &previous, &[create, remove])
            .await
            .unwrap();
        assert_eq!(namespace.read_snapshot(checkpoint).await.unwrap(), removed);
    }

    #[tokio::test]
    async fn missing_checkpoint_section_is_reported_as_corruption() {
        let operation = OperationId::from_bytes([10; 16]);
        let cursor = ChangeCursor::at(NonZeroU64::new(1).unwrap(), operation);
        let snapshot = root_snapshot(cursor);
        let publication = NamespacePublication {
            operation,
            parent: ChangeCursor::Genesis,
            expected_nodes: vec![NodePrecondition {
                node: snapshot.root,
                expected_generation: None,
            }],
            expected_directories: vec![DirectoryPrecondition {
                directory: snapshot.root,
                expected_generation: None,
            }],
            target: snapshot.clone(),
        };
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let namespace = ObjectNamespace {
            volume_id: snapshot.volume_id,
            operator: operator.clone(),
        };
        assert_eq!(
            namespace.publish(None, &publication).await.unwrap(),
            CommitOutcome::Committed(cursor)
        );
        let head = operator.read(HEAD_KEY).await.unwrap().to_bytes();
        let head = decode_head(&head).unwrap();
        let checkpoint = namespace.read_checkpoint(&head.checkpoint).await.unwrap();
        operator
            .delete(&section_key(&checkpoint.sections[0].object))
            .await
            .unwrap();
        let error = namespace.recover(&head).await.unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Corrupt);
    }
}
