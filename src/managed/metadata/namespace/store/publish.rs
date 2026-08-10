// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Publication validation, retained-history rollover, and the final HEAD CAS.

use futures::future::try_join_all;

use super::{NamespaceStore, NamespaceWitness, StoredHead, committed_result, encode_head};
use crate::filesystem::{ChangeCursor, CommitOutcome, VolumeError, VolumeSnapshot};
use crate::managed::error::invalid;
use crate::managed::metadata::namespace::{
    NamespaceChange, StoredChangeSegment, StoredCommittedResult, StoredNamespaceState,
    require_request_digest,
};

impl NamespaceStore {
    pub(crate) async fn publish(
        &self,
        observed: Option<(&NamespaceWitness, &VolumeSnapshot)>,
        change: NamespaceChange,
    ) -> Result<CommitOutcome, VolumeError> {
        change.validate(self.volume_id).map_err(|_| {
            invalid(
                "publish Managed namespace",
                "publication belongs to another volume or has invalid ancestry",
            )
        })?;
        let operation = change.operation();
        let cursor = change.cursor();
        let request_digest = change.request_sha256()?;
        let change_bytes = change.encoded_len()?;
        let (mut head, revision, base) = match observed {
            Some((witness, snapshot)) => {
                if witness.head.volume_id != self.volume_id
                    || witness.head.branch_id != self.branch_id()
                {
                    return Err(invalid(
                        "publish Managed namespace",
                        "observation belongs to another authority",
                    ));
                }
                (
                    witness.head.clone(),
                    Some(witness.revision.clone()),
                    Some(snapshot),
                )
            }
            None if self.branch_id().is_some() => {
                let (head, revision) = self
                    .read_bound_head("publish Managed namespace")
                    .await?
                    .expect("a bound branch has a HEAD");
                if head.state.is_some() {
                    return self.outcome_after_race(operation, request_digest).await;
                }
                (head, Some(revision), None)
            }
            None => match self.read_raw_head().await? {
                Some((head, revision)) if head.state.is_none() => (head, Some(revision), None),
                Some(_) => return self.outcome_after_race(operation, request_digest).await,
                None => (StoredHead::unborn(self.volume_id, None), None, None),
            },
        };
        if head.maintenance.is_some() {
            return Ok(CommitOutcome::Conflict {
                observed: head.cursor(),
            });
        }
        if let Some(result) = head
            .state
            .as_ref()
            .and_then(|state| state.resolve(self.branch_id(), operation))
        {
            require_request_digest(Some(request_digest), result.request_sha256)?;
            return Ok(CommitOutcome::Committed(result.cursor));
        }
        let mut validated = change.validate_against(base).map_err(|_| {
            invalid(
                "publish Managed namespace",
                "publication mutation is invalid",
            )
        })?;
        if validated.is_none() {
            if matches!(
                self.resolve_known(operation, Some(request_digest)).await?,
                CommitOutcome::Committed(_)
            ) {
                return Ok(CommitOutcome::Committed(cursor));
            }
            return Ok(CommitOutcome::Conflict {
                observed: base.map_or(ChangeCursor::Genesis, |snapshot| snapshot.cursor),
            });
        }
        if head
            .state
            .as_ref()
            .is_some_and(|state| state.maybe_contains(operation))
            && let outcome = self.resolve_known(operation, Some(request_digest)).await?
            && let CommitOutcome::Committed(cursor) = outcome
        {
            return Ok(CommitOutcome::Committed(cursor));
        }

        let displaced_outcome = head
            .state
            .as_ref()
            .filter(|state| !state.outcome_is_retained())
            .and_then(|state| state.outcome.clone());
        let result = StoredCommittedResult::from_change(&change, request_digest);
        let state = match head.state.take() {
            None => {
                let target = change.apply_validated(
                    base.cloned(),
                    validated.take().expect("publication was validated above"),
                );
                let mut state = StoredNamespaceState {
                    checkpoint: self.write_checkpoint(&target).await?,
                    checkpoint_cursor: cursor,
                    tail: Vec::new(),
                    segments: Vec::new(),
                    operation_prefixes: StoredNamespaceState::empty_operation_index(),
                    outcome: None,
                };
                state.record_outcome(result);
                state
            }
            Some(mut current) => {
                let tail_bytes = current.tail.iter().try_fold(0_usize, |total, change| {
                    change
                        .encoded_len()
                        .map(|length| total.saturating_add(length))
                })?;
                if current.tail.len() + 1 >= super::super::state::MAX_TAIL_TRANSACTIONS
                    || tail_bytes.saturating_add(change_bytes) > super::super::state::MAX_TAIL_BYTES
                {
                    let mut segments = std::mem::take(&mut current.segments);
                    current.tail.push(change.clone());
                    let segment = StoredChangeSegment {
                        checkpoint: current.checkpoint,
                        changes: std::mem::take(&mut current.tail),
                    };
                    segments.push(self.write_change_segment(&segment).await?);
                    let excess = segments
                        .len()
                        .saturating_sub(super::super::state::MAX_CHANGE_SEGMENTS);
                    for reference in &segments[..excess] {
                        let segment = self.read_change_segment(*reference).await?;
                        let results = segment
                            .changes
                            .iter()
                            .map(committed_result)
                            .collect::<Result<Vec<_>, _>>()?;
                        try_join_all(
                            results
                                .iter()
                                .filter(|result| result.origin_branch == self.branch_id())
                                .map(|result| self.write_operation(result)),
                        )
                        .await?;
                    }
                    segments.drain(..excess);
                    let target = change.apply_validated(
                        base.cloned(),
                        validated.take().expect("publication was validated above"),
                    );
                    current.checkpoint = self.write_checkpoint(&target).await?;
                    current.checkpoint_cursor = cursor;
                    current.segments = segments;
                    current.record_outcome(result);
                    current
                } else {
                    current.tail.push(change);
                    current.record_outcome(result);
                    current
                }
            }
        };
        if let Some(result) = &displaced_outcome {
            self.write_operation(result).await?;
        }
        head.state = Some(state);
        let bytes = encode_head(&head)?;
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
            Ok(true) => Ok(CommitOutcome::Committed(cursor)),
            Ok(false) => self.outcome_after_race(operation, request_digest).await,
            Err(_) => match self.resolve_known(operation, Some(request_digest)).await {
                Ok(CommitOutcome::Committed(cursor)) => Ok(CommitOutcome::Committed(cursor)),
                _ => Ok(CommitOutcome::Unknown),
            },
        }
    }
}
