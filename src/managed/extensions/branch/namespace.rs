// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Branch binding over a backend-native authority.

use std::collections::BTreeMap;
use std::fmt::Debug;

use super::records::{
    BranchLifecycle, MAX_TAIL_BYTES, MAX_TAIL_TRANSACTIONS, StoredBranchHead, StoredChange,
    StoredCheckpoint, StoredCommittedResult, StoredHistory, StoredNamespaceState,
    recover_namespace, require_request_digest, results_for_rotation,
};
use crate::filesystem::{
    BranchBinding, BranchId, ChangeCursor, CommitOutcome, OperationId, VolumeId,
};
use crate::managed::metadata::namespace::{NamespacePublication, NamespaceSnapshot};
use crate::managed::{ManagedError, ManagedErrorKind};

#[allow(async_fn_in_trait)]
pub(crate) trait BranchNamespaceStore: Clone + Send + Sync {
    type Revision: Clone + Debug + Send + Sync;

    fn volume_id(&self) -> VolumeId;

    async fn current_head(
        &self,
        binding: &BranchBinding,
        action: &'static str,
    ) -> Result<(StoredBranchHead, Self::Revision), ManagedError>;

    async fn replace_head(
        &self,
        branch: BranchId,
        revision: &Self::Revision,
        head: &StoredBranchHead,
    ) -> Result<bool, ManagedError>;

    async fn read_checkpoint(&self, id: [u8; 32]) -> Result<StoredCheckpoint, ManagedError>;

    async fn write_checkpoint(
        &self,
        checkpoint: &StoredCheckpoint,
    ) -> Result<[u8; 32], ManagedError>;

    async fn write_history(&self, history: &StoredHistory) -> Result<[u8; 32], ManagedError>;
}

#[derive(Clone)]
pub struct BoundNamespace<S> {
    pub(crate) store: S,
    pub(crate) binding: BranchBinding,
}

#[derive(Clone, Debug)]
pub struct BranchObservation<R> {
    pub(crate) snapshot: NamespaceSnapshot,
    revision: R,
    head: StoredBranchHead,
    checkpoint: StoredCheckpoint,
}

#[allow(private_bounds)]
impl<S: BranchNamespaceStore> BoundNamespace<S> {
    pub fn binding(&self) -> &BranchBinding {
        &self.binding
    }

    pub fn volume_id(&self) -> VolumeId {
        self.store.volume_id()
    }

    pub(crate) async fn observe(
        &self,
    ) -> Result<Option<BranchObservation<S::Revision>>, ManagedError> {
        let (head, revision) = self
            .store
            .current_head(&self.binding, "read Managed branch")
            .await?;
        let Some(state) = &head.state else {
            return Ok(None);
        };
        let checkpoint = self.store.read_checkpoint(state.checkpoint).await?;
        let snapshot = recover_namespace(checkpoint.clone(), state, self.store.volume_id())?;
        Ok(Some(BranchObservation {
            snapshot,
            revision,
            head,
            checkpoint,
        }))
    }

    pub(crate) async fn observe_from(
        &self,
        _base: &NamespaceSnapshot,
    ) -> Result<Option<BranchObservation<S::Revision>>, ManagedError> {
        self.observe().await
    }

    pub(crate) async fn publish(
        &self,
        observed: Option<&BranchObservation<S::Revision>>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        let branch = self.binding.id;
        if publication.target.volume_id != self.store.volume_id() {
            return Err(invalid("publication belongs to another volume"));
        }
        let (head, revision, base, checkpoint) = match observed {
            Some(observed) => {
                observed.head.validate(self.store.volume_id(), branch)?;
                if observed.head.lifecycle != BranchLifecycle::Active
                    || observed.head.maintenance_active
                {
                    return Err(conflict("branch is sealed or under maintenance"));
                }
                (
                    observed.head.clone(),
                    observed.revision.clone(),
                    Some(&observed.snapshot),
                    Some(observed.checkpoint.clone()),
                )
            }
            None => {
                let (head, revision) = self
                    .store
                    .current_head(&self.binding, "publish Managed branch")
                    .await?;
                if head.state.is_some() {
                    return self.outcome_after_race(publication.operation).await;
                }
                (head, revision, None, None)
            }
        };
        let (change, valid) = StoredChange::prepare(branch, publication, base)?;
        let request_digest = change.request_digest()?;
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
        if let CommitOutcome::Committed(cursor) = self
            .resolve_known(publication.operation, Some(request_digest))
            .await?
        {
            return Ok(CommitOutcome::Committed(cursor));
        }

        let state = match (&head.state, checkpoint) {
            (None, None) => {
                let result = StoredCommittedResult::from_change(&change)?;
                let results = BTreeMap::from([((branch, publication.operation), result)]);
                let checkpoint = StoredCheckpoint::new(&publication.target, results)?;
                StoredNamespaceState {
                    checkpoint: self.store.write_checkpoint(&checkpoint).await?,
                    checkpoint_cursor: publication.target.cursor,
                    tail: Vec::new(),
                    previous_history: None,
                }
            }
            (Some(current), Some(checkpoint)) => {
                let appended_bytes = current
                    .tail
                    .iter()
                    .try_fold(0_usize, |total, change| {
                        change
                            .encoded_len()
                            .map(|length| total.saturating_add(length))
                    })?
                    .saturating_add(change.encoded_len()?);
                if current.tail.len() + 1 >= MAX_TAIL_TRANSACTIONS
                    || appended_bytes > MAX_TAIL_BYTES
                {
                    let history = StoredHistory::new(self.store.volume_id(), branch, current)?;
                    let history = self.store.write_history(&history).await?;
                    let results =
                        results_for_rotation(checkpoint, current, &change, self.store.volume_id())?;
                    let checkpoint = StoredCheckpoint::new(&publication.target, results)?;
                    StoredNamespaceState {
                        checkpoint: self.store.write_checkpoint(&checkpoint).await?,
                        checkpoint_cursor: publication.target.cursor,
                        tail: Vec::new(),
                        previous_history: Some(history),
                    }
                } else {
                    let mut next = current.clone();
                    next.tail.push(change);
                    next
                }
            }
            _ => return Err(corrupt("branch observation and checkpoint disagree")),
        };
        let next = StoredBranchHead {
            state: Some(state),
            ..head
        };
        match self.store.replace_head(branch, &revision, &next).await {
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
        let (head, _) = self
            .store
            .current_head(&self.binding, "resolve Managed branch publication")
            .await?;
        let Some(state) = head.state else {
            return Ok(CommitOutcome::Absent);
        };
        if let Some(change) = state.tail.iter().find(|change| {
            change.origin_branch == self.binding.id && change.operation() == operation
        }) {
            require_request_digest(expected, change.request_digest()?)?;
            return Ok(CommitOutcome::Committed(change.cursor()));
        }
        let checkpoint = self.store.read_checkpoint(state.checkpoint).await?;
        let Some(result) = checkpoint.resolve(self.binding.id, operation)? else {
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
}

fn invalid(message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, "publish Managed branch", message)
}

fn conflict(message: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Conflict,
        "publish Managed branch",
        message,
    )
}

fn corrupt(message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, "publish Managed branch", message)
}
