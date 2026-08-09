// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! One namespace authority state machine over a bound revision-CAS HEAD.

#[cfg(feature = "managed-branch")]
use std::collections::BTreeSet;
use std::io::Cursor;

use opendal::{ErrorKind, Operator};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{
    CheckpointRef, NamespaceChange, NamespaceGcSweep, NamespacePublication, NamespaceSnapshot,
    StoredChange, StoredCheckpoint, StoredCommittedResult, StoredHistory, StoredNamespaceState,
    StoredResults, recover_namespace, replay_tail_from, require_request_digest,
    results_for_rotation, validate_publication,
};
use crate::filesystem::{
    BranchBinding, BranchId, ChangeCursor, CommitOutcome, OperationId, VolumeId,
};
use crate::managed::metadata::object::ensure_immutable;
#[cfg(feature = "managed-branch")]
use crate::managed::metadata::object::read_content_addressed;
use crate::managed::metadata::record::{RecordBackend, Revision};
use crate::managed::{ManagedError, ManagedErrorKind};

const BASE_HEAD_KEY: &str = ".ofs/managed/metadata/v1/head.ofs";
const CHECKPOINT_ROOT: &str = ".ofs/managed/metadata/v1/checkpoints/sha256";
const HISTORY_ROOT: &str = ".ofs/managed/metadata/v1/extensions/branch/v1/history/sha256";
const HEAD_MAGIC: &[u8; 8] = b"OFS1HDZ1";
const CHECKPOINT_MAGIC: &[u8; 8] = b"OFS1CKZ1";
const HISTORY_MAGIC: &[u8; 8] = b"OFS1HST1";
const MAX_HEAD_BYTES: usize = 256 * 1024;
const MAX_CHECKPOINT_ENCODED_BYTES: usize = 256 * 1024 * 1024;
const MAX_CHECKPOINT_DECODED_BYTES: usize = 256 * 1024 * 1024;
const MAX_HISTORY_BYTES: usize = 512 * 1024;
const COMPRESSION_LEVEL: i32 = 3;

#[derive(Clone, Debug)]
pub(crate) struct NamespaceObservation {
    pub snapshot: NamespaceSnapshot,
    witness: NamespaceWitness,
}

#[derive(Clone, Debug)]
pub(crate) struct NamespaceWitness {
    pub(crate) revision: Revision,
    pub(crate) head: StoredHead,
    checkpoint_results: Option<StoredResults>,
}

impl NamespaceObservation {
    pub(crate) fn into_parts(self) -> (NamespaceSnapshot, NamespaceWitness) {
        (self.snapshot, self.witness)
    }
}

impl NamespaceWitness {
    pub(crate) fn gc_sweep(&self) -> Option<NamespaceGcSweep> {
        self.head
            .gc_sweep()
            .expect("observed HEAD has valid maintenance state")
    }
}

#[derive(Clone, Debug)]
enum NamespaceAuthority {
    Base,
    #[cfg(feature = "managed-branch")]
    Branch(BranchBinding),
}

impl NamespaceAuthority {
    fn branch_id(&self) -> Option<BranchId> {
        match self {
            Self::Base => None,
            #[cfg(feature = "managed-branch")]
            Self::Branch(binding) => Some(binding.id),
        }
    }

    fn binding(&self) -> Option<&BranchBinding> {
        match self {
            Self::Base => None,
            #[cfg(feature = "managed-branch")]
            Self::Branch(binding) => Some(binding),
        }
    }
}

#[derive(Clone)]
pub(crate) struct NamespaceStore {
    volume_id: VolumeId,
    data: Operator,
    backend: RecordBackend,
    authority: NamespaceAuthority,
    head_key: String,
}

impl NamespaceStore {
    pub(crate) fn new(volume_id: VolumeId, operator: Operator, backend: RecordBackend) -> Self {
        Self {
            volume_id,
            data: operator,
            backend,
            authority: NamespaceAuthority::Base,
            head_key: BASE_HEAD_KEY.to_owned(),
        }
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) fn branch(
        volume_id: VolumeId,
        data: Operator,
        backend: RecordBackend,
        binding: BranchBinding,
        head_key: String,
    ) -> Self {
        Self {
            volume_id,
            data,
            backend,
            authority: NamespaceAuthority::Branch(binding),
            head_key,
        }
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub(crate) fn binding(&self) -> Option<&BranchBinding> {
        self.authority.binding()
    }

    pub(crate) async fn observe(&self) -> Result<Option<NamespaceObservation>, ManagedError> {
        self.observe_from_optional(None).await
    }

    pub(crate) async fn observe_from(
        &self,
        base: &NamespaceSnapshot,
    ) -> Result<Option<NamespaceObservation>, ManagedError> {
        self.observe_from_optional(Some(base)).await
    }

    async fn observe_from_optional(
        &self,
        base: Option<&NamespaceSnapshot>,
    ) -> Result<Option<NamespaceObservation>, ManagedError> {
        let Some((head, revision)) = self.read_bound_head("read Managed namespace").await? else {
            return Ok(None);
        };
        let Some(state) = &head.state else {
            return Ok(None);
        };
        if let Some(base) = base
            && base.volume_id == self.volume_id
            && let Some(snapshot) = replay_tail_from(base, state)?
        {
            return Ok(Some(NamespaceObservation {
                snapshot,
                witness: NamespaceWitness {
                    revision,
                    head,
                    checkpoint_results: None,
                },
            }));
        }
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        let (snapshot, checkpoint_results) = recover_namespace(checkpoint, state, self.volume_id)?;
        Ok(Some(NamespaceObservation {
            snapshot,
            witness: NamespaceWitness {
                revision,
                head,
                checkpoint_results: Some(checkpoint_results),
            },
        }))
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<(&NamespaceWitness, &NamespaceSnapshot)>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        if publication.target.volume_id != self.volume_id {
            return Err(invalid(
                "publish Managed namespace",
                "publication belongs to another volume",
            ));
        }
        let (head, revision, base, checkpoint_results) = match observed {
            Some((witness, snapshot)) => (
                witness.head.clone(),
                Some(witness.revision.clone()),
                Some(snapshot),
                witness.checkpoint_results.clone(),
            ),
            None if self.authority.branch_id().is_some() => {
                let (head, revision) = self
                    .read_bound_head("publish Managed namespace")
                    .await?
                    .expect("a bound branch has a HEAD");
                if head.state.is_some() {
                    return self.outcome_after_race(publication.operation).await;
                }
                (head, Some(revision), None, None)
            }
            None => {
                if self.read_raw_head().await?.is_some() {
                    return self.outcome_after_race(publication.operation).await;
                }
                (StoredHead::unborn(self.volume_id, None), None, None, None)
            }
        };
        if head.maintenance_active {
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            });
        }

        let valid = validate_publication(publication, base)?;
        let change = StoredChange {
            origin_branch: self.authority.branch_id(),
            change: NamespaceChange::from_publication(publication, base),
        };
        let request_digest = change.request_digest()?;
        let change_bytes = change.encoded_len()?;
        if !valid {
            if matches!(
                self.resolve_known(publication.operation, Some(request_digest))
                    .await?,
                CommitOutcome::Committed(_)
            ) {
                return Ok(CommitOutcome::Committed(publication.target.cursor));
            }
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            });
        }

        let state = match (&head.state, checkpoint_results) {
            (None, None) => {
                let result = StoredCommittedResult::from_change(&change)?;
                let checkpoint = StoredCheckpoint {
                    snapshot: publication.target.clone(),
                    results: vec![result],
                };
                StoredNamespaceState {
                    checkpoint: self.write_checkpoint(&checkpoint).await?,
                    checkpoint_cursor: publication.target.cursor,
                    tail: Vec::new(),
                    previous_history: None,
                }
            }
            (Some(current), checkpoint_results) => {
                let tail_bytes = current.tail.iter().try_fold(0_usize, |total, change| {
                    change
                        .encoded_len()
                        .map(|length| total.saturating_add(length))
                })?;
                if current.tail.len() + 1 >= super::state::MAX_TAIL_TRANSACTIONS
                    || tail_bytes.saturating_add(change_bytes) > super::state::MAX_TAIL_BYTES
                {
                    let checkpoint_results = match checkpoint_results {
                        Some(results) => results,
                        None => {
                            self.read_checkpoint(current.checkpoint)
                                .await?
                                .recover(self.volume_id)?
                                .1
                        }
                    };
                    let previous_history = if self.authority.branch_id().is_some() {
                        let history = StoredHistory::new(self.volume_id, current)?;
                        Some(self.write_history(&history).await?)
                    } else {
                        None
                    };
                    let results = results_for_rotation(checkpoint_results, current, &change)?;
                    let checkpoint = StoredCheckpoint {
                        snapshot: publication.target.clone(),
                        results: results.into_values().collect(),
                    };
                    StoredNamespaceState {
                        checkpoint: self.write_checkpoint(&checkpoint).await?,
                        checkpoint_cursor: publication.target.cursor,
                        tail: Vec::new(),
                        previous_history,
                    }
                } else {
                    let mut next = current.clone();
                    next.tail.push(change);
                    next
                }
            }
            _ => {
                return Err(corrupt(
                    "publish Managed namespace",
                    "observation and HEAD disagree",
                ));
            }
        };
        let mut next = head;
        next.state = Some(state);
        let bytes = encode_head(&next)?;
        let replaced = match revision {
            Some(revision) => {
                self.backend
                    .replace(
                        &self.head_key,
                        &revision,
                        bytes,
                        "publish Managed namespace",
                    )
                    .await
            }
            None => {
                self.backend
                    .create(&self.head_key, bytes, "publish Managed namespace")
                    .await
            }
        };
        match replaced {
            Ok(true) => Ok(CommitOutcome::Committed(publication.target.cursor)),
            Ok(false) => self.outcome_after_race(publication.operation).await,
            Err(_) => match self.resolve(publication.operation).await {
                Ok(CommitOutcome::Committed(cursor)) => Ok(CommitOutcome::Committed(cursor)),
                _ => Ok(CommitOutcome::Unknown),
            },
        }
    }

    pub(crate) async fn resolve(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        match self.resolve_known(operation, None).await {
            Err(error) if error.kind() == ManagedErrorKind::Unavailable => {
                Ok(CommitOutcome::Unknown)
            }
            result => result,
        }
    }

    async fn resolve_known(
        &self,
        operation: OperationId,
        expected: Option<[u8; 32]>,
    ) -> Result<CommitOutcome, ManagedError> {
        let Some((head, _)) = self.read_bound_head("resolve Managed publication").await? else {
            return Ok(CommitOutcome::Absent);
        };
        let Some(state) = head.state else {
            return Ok(CommitOutcome::Absent);
        };
        if let Some(change) = state.tail.iter().find(|change| {
            change.origin_branch == self.authority.branch_id()
                && change.change.operation == operation
        }) {
            require_request_digest(expected, change.request_digest()?)?;
            return Ok(CommitOutcome::Committed(change.change.cursor));
        }
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        let Some(result) = checkpoint.resolve(self.authority.branch_id(), operation)? else {
            return Ok(CommitOutcome::Absent);
        };
        require_request_digest(expected, result.request_sha256)?;
        Ok(CommitOutcome::Committed(result.cursor))
    }

    async fn outcome_after_race(
        &self,
        operation: OperationId,
    ) -> Result<CommitOutcome, ManagedError> {
        match self.resolve(operation).await? {
            result @ (CommitOutcome::Committed(_) | CommitOutcome::Unknown) => Ok(result),
            _ => Ok(CommitOutcome::Conflict {
                observed: self
                    .observe()
                    .await?
                    .map_or(ChangeCursor::Genesis, |value| value.snapshot.cursor),
            }),
        }
    }

    pub(crate) async fn begin_gc(
        &self,
        observed: &NamespaceWitness,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        if self.authority.branch_id().is_some() {
            return Err(invalid(
                "begin Managed namespace GC",
                "branch GC belongs to its volume control plane",
            ));
        }
        let mut head = observed.head.clone();
        let sweep = head.begin_gc(*OperationId::generate().as_bytes())?;
        if self
            .backend
            .replace(
                &self.head_key,
                &observed.revision,
                encode_head(&head)?,
                "begin Managed namespace GC",
            )
            .await?
        {
            Ok(sweep)
        } else {
            Err(conflict(
                "begin Managed namespace GC",
                "namespace authority changed",
            ))
        }
    }

    pub(crate) async fn resume_gc(
        &self,
        observed: &NamespaceWitness,
    ) -> Result<NamespaceGcSweep, ManagedError> {
        let mut head = observed.head.clone();
        let sweep = head.resume_gc(*OperationId::generate().as_bytes())?;
        if self
            .backend
            .replace(
                &self.head_key,
                &observed.revision,
                encode_head(&head)?,
                "resume Managed namespace GC",
            )
            .await?
        {
            Ok(sweep)
        } else {
            Err(conflict(
                "resume Managed namespace GC",
                "namespace authority changed",
            ))
        }
    }

    pub(crate) async fn finish_gc(&self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        let (mut head, revision) = self
            .read_bound_head("finish Managed namespace GC")
            .await?
            .ok_or_else(|| {
                conflict("finish Managed namespace GC", "namespace authority changed")
            })?;
        if head.maintenance_epoch == sweep.epoch() && head.gc_sweep()?.is_none() {
            return Ok(());
        }
        if head.gc_sweep()? != Some(sweep) {
            return Err(conflict(
                "finish Managed namespace GC",
                "GC sweep token does not match the authority",
            ));
        }
        if self.authority.branch_id().is_some() {
            return Err(invalid(
                "sweep Managed checkpoints",
                "branch checkpoint GC belongs to its volume control plane",
            ));
        }
        let retained = head
            .state
            .as_ref()
            .map(|state| checkpoint_key(state.checkpoint.digest));
        sweep_checkpoint_objects(&self.data, retained.as_deref()).await?;
        head.finish_gc(sweep)?;
        if self
            .backend
            .replace(
                &self.head_key,
                &revision,
                encode_head(&head)?,
                "finish Managed namespace GC",
            )
            .await?
        {
            return Ok(());
        }
        let (current, _) = self
            .read_bound_head("finish Managed namespace GC")
            .await?
            .ok_or_else(|| {
                conflict("finish Managed namespace GC", "namespace authority changed")
            })?;
        if current.maintenance_epoch == sweep.epoch() && current.gc_sweep()?.is_none() {
            Ok(())
        } else {
            Err(conflict(
                "finish Managed namespace GC",
                "namespace authority changed",
            ))
        }
    }

    pub(crate) async fn read_checkpoint(
        &self,
        reference: CheckpointRef,
    ) -> Result<StoredCheckpoint, ManagedError> {
        let encoded_length = usize::try_from(reference.length)
            .ok()
            .filter(|length| *length <= MAX_CHECKPOINT_ENCODED_BYTES)
            .ok_or_else(|| {
                corrupt(
                    "read Managed namespace",
                    "checkpoint exceeds its encoded size limit",
                )
            })?;
        let key = checkpoint_key(reference.digest);
        let bytes = match self
            .data
            .read_with(&key)
            .range(0..reference.length)
            .content_length_hint(reference.length)
            .await
        {
            Ok(bytes) => bytes.to_bytes(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(corrupt("read Managed namespace", "checkpoint is missing"));
            }
            Err(_) => return Err(unavailable("read Managed namespace")),
        };
        if bytes.len() != encoded_length
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != reference.digest
        {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint identity is invalid",
            ));
        }
        let checkpoint = decode_checkpoint(&bytes)?;
        if checkpoint.snapshot.volume_id != self.volume_id {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint volume is invalid",
            ));
        }
        Ok(checkpoint)
    }

    async fn write_checkpoint(
        &self,
        checkpoint: &StoredCheckpoint,
    ) -> Result<CheckpointRef, ManagedError> {
        let bytes = encode_checkpoint(checkpoint)?;
        let reference = CheckpointRef {
            digest: Sha256::digest(&bytes).into(),
            length: bytes.len() as u64,
        };
        ensure_immutable(
            &self.data,
            &checkpoint_key(reference.digest),
            &bytes,
            "checkpoint Managed namespace",
            ManagedErrorKind::Corrupt,
            "immutable checkpoint changed",
        )
        .await?;
        Ok(reference)
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) async fn read_history(&self, id: [u8; 32]) -> Result<StoredHistory, ManagedError> {
        let bytes = read_content_addressed(
            &self.data,
            &history_key(id),
            &id,
            "read Managed history",
            "namespace history is missing",
            "namespace history identity is invalid",
        )
        .await?;
        let history: StoredHistory = decode_record(HISTORY_MAGIC, &bytes, MAX_HISTORY_BYTES)?;
        history.validate(self.volume_id)?;
        Ok(history)
    }

    async fn write_history(&self, history: &StoredHistory) -> Result<[u8; 32], ManagedError> {
        let bytes = encode_record(HISTORY_MAGIC, history, MAX_HISTORY_BYTES)?;
        let id: [u8; 32] = Sha256::digest(&bytes).into();
        ensure_immutable(
            &self.data,
            &history_key(id),
            &bytes,
            "archive Managed history",
            ManagedErrorKind::Corrupt,
            "immutable namespace history changed",
        )
        .await?;
        Ok(id)
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) async fn find_history_state(
        &self,
        mut history_id: Option<[u8; 32]>,
        sequence: u64,
    ) -> Result<Option<StoredNamespaceState>, ManagedError> {
        let mut visited = BTreeSet::new();
        while let Some(id) = history_id {
            if !visited.insert(id) {
                return Err(corrupt(
                    "read Managed history",
                    "namespace history contains a cycle",
                ));
            }
            let history = self.read_history(id).await?;
            if let Some(state) = history.state_at(sequence) {
                return Ok(Some(state));
            }
            history_id = history.state.previous_history;
        }
        Ok(None)
    }

    #[cfg(feature = "managed-branch")]
    pub(crate) async fn visit_retained(
        &self,
        state: &StoredNamespaceState,
        mut visit: impl FnMut(&NamespaceSnapshot) -> Result<(), ManagedError>,
    ) -> Result<(), ManagedError> {
        let checkpoint = self.read_checkpoint(state.checkpoint).await?;
        let (mut snapshot, _) = checkpoint.recover(self.volume_id)?;
        if snapshot.cursor != state.checkpoint_cursor {
            return Err(corrupt(
                "read Managed history",
                "checkpoint and retained state disagree",
            ));
        }
        visit(&snapshot)?;
        for change in &state.tail {
            snapshot = change.change.apply(Some(snapshot))?;
            visit(&snapshot)?;
        }
        if !state.tail.is_empty() {
            super::validate_snapshot(&snapshot)
                .map_err(|_| corrupt("read Managed history", "recovered namespace is invalid"))?;
        }
        Ok(())
    }

    pub(crate) async fn read_raw_head(
        &self,
    ) -> Result<Option<(StoredHead, Revision)>, ManagedError> {
        let Some((bytes, revision)) = self
            .backend
            .read(&self.head_key, "read Managed namespace")
            .await?
        else {
            return Ok(None);
        };
        let head = decode_head(&bytes)?;
        head.validate(self.volume_id, self.authority.branch_id())?;
        Ok(Some((head, revision)))
    }

    async fn read_bound_head(
        &self,
        action: &'static str,
    ) -> Result<Option<(StoredHead, Revision)>, ManagedError> {
        let value = self.read_raw_head().await?;
        if self.authority.branch_id().is_some() {
            let (head, _) = value
                .as_ref()
                .ok_or_else(|| conflict(action, "branch incarnation no longer exists"))?;
            if head.sealed {
                return Err(conflict(action, "branch incarnation no longer exists"));
            }
            if head.maintenance_active {
                return Err(conflict(action, "branch maintenance is active"));
            }
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredHead {
    pub(crate) volume_id: VolumeId,
    pub(crate) branch_id: Option<BranchId>,
    pub(crate) sealed: bool,
    pub(crate) state: Option<StoredNamespaceState>,
    pub(crate) maintenance_epoch: u64,
    pub(crate) maintenance_active: bool,
    pub(crate) maintenance_owner: Option<[u8; 16]>,
    pub(crate) maintenance_fixed_cursor: Option<ChangeCursor>,
}

impl StoredHead {
    pub(crate) const fn unborn(volume_id: VolumeId, branch_id: Option<BranchId>) -> Self {
        Self {
            volume_id,
            branch_id,
            sealed: false,
            state: None,
            maintenance_epoch: 0,
            maintenance_active: false,
            maintenance_owner: None,
            maintenance_fixed_cursor: None,
        }
    }

    pub(crate) fn cursor(&self) -> ChangeCursor {
        self.state
            .as_ref()
            .map_or(ChangeCursor::Genesis, StoredNamespaceState::cursor)
    }

    pub(crate) fn validate(
        &self,
        volume_id: VolumeId,
        branch_id: Option<BranchId>,
    ) -> Result<(), ManagedError> {
        if self.volume_id != volume_id || self.branch_id != branch_id {
            return Err(corrupt(
                "read Managed namespace",
                "HEAD identity is invalid",
            ));
        }
        if let Some(state) = &self.state {
            state.validate(volume_id)?;
        }
        self.gc_sweep()?;
        Ok(())
    }

    pub(crate) fn gc_sweep(&self) -> Result<Option<NamespaceGcSweep>, ManagedError> {
        match (
            self.maintenance_active,
            self.maintenance_owner,
            self.maintenance_fixed_cursor,
        ) {
            (false, _, None) => Ok(None),
            (true, Some(owner), Some(fixed))
                if self.maintenance_epoch > 0 && fixed == self.cursor() =>
            {
                Ok(Some(NamespaceGcSweep::new(
                    self.maintenance_epoch,
                    owner,
                    fixed,
                )))
            }
            _ => Err(corrupt(
                "read Managed namespace",
                "HEAD maintenance state is invalid",
            )),
        }
    }

    fn begin_gc(&mut self, owner: [u8; 16]) -> Result<NamespaceGcSweep, ManagedError> {
        if self.gc_sweep()?.is_some() {
            return Err(conflict(
                "begin Managed namespace GC",
                "another namespace GC is active",
            ));
        }
        self.maintenance_epoch = self.maintenance_epoch.checked_add(1).ok_or_else(|| {
            corrupt(
                "begin Managed namespace GC",
                "maintenance epoch is exhausted",
            )
        })?;
        self.maintenance_active = true;
        self.maintenance_owner = Some(owner);
        self.maintenance_fixed_cursor = Some(self.cursor());
        Ok(NamespaceGcSweep::new(
            self.maintenance_epoch,
            owner,
            self.cursor(),
        ))
    }

    fn resume_gc(&mut self, owner: [u8; 16]) -> Result<NamespaceGcSweep, ManagedError> {
        let active = self.gc_sweep()?.ok_or_else(|| {
            conflict(
                "resume Managed namespace GC",
                "no interrupted namespace GC is active",
            )
        })?;
        self.maintenance_owner = Some(owner);
        Ok(NamespaceGcSweep::new(
            active.epoch(),
            owner,
            active.fixed_cursor(),
        ))
    }

    fn finish_gc(&mut self, sweep: NamespaceGcSweep) -> Result<(), ManagedError> {
        if self.gc_sweep()? != Some(sweep) {
            return Err(conflict(
                "finish Managed namespace GC",
                "GC sweep token does not match the authority",
            ));
        }
        self.maintenance_active = false;
        self.maintenance_fixed_cursor = None;
        Ok(())
    }
}

fn encode_checkpoint(checkpoint: &StoredCheckpoint) -> Result<Vec<u8>, ManagedError> {
    let mut body = Vec::new();
    ciborium::into_writer(checkpoint, &mut body).map_err(|_| {
        invalid(
            "checkpoint Managed namespace",
            "checkpoint cannot be encoded",
        )
    })?;
    if body.len() > MAX_CHECKPOINT_DECODED_BYTES {
        return Err(invalid(
            "checkpoint Managed namespace",
            "checkpoint exceeds its decoded size limit",
        ));
    }
    let decoded_length = u64::try_from(body.len()).map_err(|_| {
        invalid(
            "checkpoint Managed namespace",
            "checkpoint exceeds its decoded size limit",
        )
    })?;
    let compressed = zstd::bulk::compress(&body, COMPRESSION_LEVEL).map_err(|_| {
        invalid(
            "checkpoint Managed namespace",
            "checkpoint cannot be compressed",
        )
    })?;
    let encoded_length = CHECKPOINT_MAGIC
        .len()
        .saturating_add(8)
        .saturating_add(compressed.len());
    if encoded_length > MAX_CHECKPOINT_ENCODED_BYTES {
        return Err(invalid(
            "checkpoint Managed namespace",
            "checkpoint exceeds its encoded size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(encoded_length);
    bytes.extend_from_slice(CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&decoded_length.to_be_bytes());
    bytes.extend_from_slice(&compressed);
    Ok(bytes)
}

fn decode_checkpoint(bytes: &[u8]) -> Result<StoredCheckpoint, ManagedError> {
    if bytes.len() > MAX_CHECKPOINT_ENCODED_BYTES {
        return Err(corrupt(
            "read Managed namespace",
            "checkpoint exceeds its encoded size limit",
        ));
    }
    let encoded = bytes
        .strip_prefix(CHECKPOINT_MAGIC)
        .ok_or_else(|| corrupt("read Managed namespace", "checkpoint format is invalid"))?;
    let (length, compressed) = encoded
        .split_first_chunk::<8>()
        .ok_or_else(|| corrupt("read Managed namespace", "checkpoint length is missing"))?;
    let decoded_length = usize::try_from(u64::from_be_bytes(*length))
        .ok()
        .filter(|length| *length <= MAX_CHECKPOINT_DECODED_BYTES)
        .ok_or_else(|| {
            corrupt(
                "read Managed namespace",
                "checkpoint decoded size exceeds its limit",
            )
        })?;
    let body = zstd::bulk::decompress(compressed, decoded_length).map_err(|_| {
        corrupt(
            "read Managed namespace",
            "checkpoint compression is invalid",
        )
    })?;
    if body.len() != decoded_length {
        return Err(corrupt(
            "read Managed namespace",
            "checkpoint decoded length does not match",
        ));
    }
    decode_value(&body)
}

pub(crate) fn encode_head(value: &StoredHead) -> Result<Vec<u8>, ManagedError> {
    let mut body = Vec::new();
    ciborium::into_writer(value, &mut body)
        .map_err(|_| invalid("write Managed namespace", "HEAD cannot be encoded"))?;
    if body.len() > MAX_HEAD_BYTES {
        return Err(invalid(
            "write Managed namespace",
            "HEAD exceeds its decoded size limit",
        ));
    }
    let decoded_length = u32::try_from(body.len()).map_err(|_| {
        invalid(
            "write Managed namespace",
            "HEAD exceeds its decoded size limit",
        )
    })?;
    let compressed = zstd::bulk::compress(&body, COMPRESSION_LEVEL)
        .map_err(|_| invalid("write Managed namespace", "HEAD cannot be compressed"))?;
    let mut bytes = Vec::with_capacity(12 + compressed.len() + 32);
    bytes.extend_from_slice(HEAD_MAGIC);
    bytes.extend_from_slice(&decoded_length.to_be_bytes());
    bytes.extend_from_slice(&compressed);
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

pub(crate) fn decode_head(bytes: &[u8]) -> Result<StoredHead, ManagedError> {
    let encoded = bytes
        .strip_prefix(HEAD_MAGIC)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| corrupt("read Managed namespace", "HEAD format is invalid"))?;
    if Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != &bytes[bytes.len() - 32..] {
        return Err(corrupt(
            "read Managed namespace",
            "HEAD checksum does not match",
        ));
    }
    let (length, compressed) = encoded
        .split_first_chunk::<4>()
        .ok_or_else(|| corrupt("read Managed namespace", "HEAD length is missing"))?;
    let decoded_length = u32::from_be_bytes(*length) as usize;
    if decoded_length > MAX_HEAD_BYTES {
        return Err(corrupt(
            "read Managed namespace",
            "HEAD decoded size exceeds its limit",
        ));
    }
    let body = zstd::bulk::decompress(compressed, decoded_length)
        .map_err(|_| corrupt("read Managed namespace", "HEAD compression is invalid"))?;
    if body.len() != decoded_length {
        return Err(corrupt(
            "read Managed namespace",
            "HEAD decoded length does not match",
        ));
    }
    decode_value(&body)
}

fn encode_record<T: Serialize>(
    magic: &[u8; 8],
    value: &T,
    maximum: usize,
) -> Result<Vec<u8>, ManagedError> {
    let mut bytes = Vec::from(magic);
    ciborium::into_writer(value, &mut bytes)
        .map_err(|_| invalid("write Managed history", "record cannot be encoded"))?;
    if bytes.len() - magic.len() > maximum {
        return Err(invalid(
            "write Managed history",
            "record exceeds its size limit",
        ));
    }
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

#[cfg(feature = "managed-branch")]
fn decode_record<T: DeserializeOwned>(
    magic: &[u8; 8],
    bytes: &[u8],
    maximum: usize,
) -> Result<T, ManagedError> {
    let body = bytes
        .strip_prefix(magic)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| corrupt("read Managed history", "record format is invalid"))?;
    if body.len() > maximum
        || Sha256::digest(&bytes[..bytes.len() - 32]).as_slice() != &bytes[bytes.len() - 32..]
    {
        return Err(corrupt(
            "read Managed history",
            "record checksum is invalid",
        ));
    }
    decode_value(body)
}

fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ManagedError> {
    let mut input = Cursor::new(bytes);
    let value = ciborium::from_reader(&mut input)
        .map_err(|_| corrupt("read Managed namespace", "record cannot be decoded"))?;
    if input.position() != bytes.len() as u64 {
        return Err(corrupt(
            "read Managed namespace",
            "record has trailing bytes",
        ));
    }
    Ok(value)
}

async fn sweep_checkpoint_objects(
    data: &Operator,
    retained: Option<&str>,
) -> Result<(), ManagedError> {
    let prefix = format!("{CHECKPOINT_ROOT}/");
    let unreachable = data
        .list_with(&prefix)
        .recursive(true)
        .await
        .map_err(|_| unavailable("sweep Managed checkpoints"))?
        .into_iter()
        .filter(|entry| entry.metadata().is_file() && retained != Some(entry.path()))
        .map(|entry| entry.path().to_owned())
        .collect::<Vec<_>>();
    if !unreachable.is_empty() {
        data.delete_iter(unreachable.iter().map(String::as_str))
            .await
            .map_err(|_| unavailable("sweep Managed checkpoints"))?;
    }
    Ok(())
}

pub(crate) fn checkpoint_key(id: [u8; 32]) -> String {
    format!("{CHECKPOINT_ROOT}/{}.ofs", hex(&id))
}

pub(crate) fn history_key(id: [u8; 32]) -> String {
    format!("{HISTORY_ROOT}/{}.ofs", hex(&id))
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

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn conflict(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Conflict, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "storage operation failed",
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use opendal::services::Memory;

    use super::*;
    use crate::filesystem::{DirectoryRecord, NodeAttributes, NodeId, NodeKind, NodeRecord};
    use crate::managed::metadata::namespace::managed_generation;

    fn checkpoint_snapshot(
        volume_id: VolumeId,
        cursor: ChangeCursor,
        root: NodeId,
    ) -> NamespaceSnapshot {
        NamespaceSnapshot {
            volume_id,
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

    #[tokio::test]
    async fn checkpoint_identity_and_base_gc_are_durable() {
        let volume_id = VolumeId::from_bytes([1; 16]);
        let operation = OperationId::from_bytes([2; 16]);
        let cursor = ChangeCursor::at(NonZeroU64::MIN, operation);
        let current = checkpoint_snapshot(volume_id, cursor, NodeId::from_bytes([3; 16]));
        let old = checkpoint_snapshot(
            volume_id,
            ChangeCursor::Genesis,
            NodeId::from_bytes([4; 16]),
        );
        let current_bytes = encode_checkpoint(&StoredCheckpoint {
            snapshot: current.clone(),
            results: Vec::new(),
        })
        .unwrap();
        let current_id: [u8; 32] = Sha256::digest(&current_bytes).into();
        assert_eq!(
            decode_checkpoint(&current_bytes)
                .unwrap()
                .recover(volume_id)
                .unwrap()
                .0,
            current
        );
        let mut corrupt = current_bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_checkpoint(&corrupt).unwrap_err().kind(),
            ManagedErrorKind::Corrupt
        );

        let old_bytes = encode_checkpoint(&StoredCheckpoint {
            snapshot: old,
            results: Vec::new(),
        })
        .unwrap();
        let old_id: [u8; 32] = Sha256::digest(&old_bytes).into();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        operator
            .write(&checkpoint_key(current_id), current_bytes)
            .await
            .unwrap();
        operator
            .write(&checkpoint_key(old_id), old_bytes)
            .await
            .unwrap();
        sweep_checkpoint_objects(&operator, Some(&checkpoint_key(current_id)))
            .await
            .unwrap();
        assert!(operator.exists(&checkpoint_key(current_id)).await.unwrap());
        assert!(!operator.exists(&checkpoint_key(old_id)).await.unwrap());
    }
}
