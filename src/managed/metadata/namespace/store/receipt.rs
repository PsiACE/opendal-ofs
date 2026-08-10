// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Durable operation receipts and unknown-publication resolution.

use super::{NamespaceStore, OPERATION_RECORD, OPERATION_ROOT, committed_result};
use crate::filesystem::{
    BranchId, ChangeCursor, CommitOutcome, OperationId, VolumeError, VolumeErrorKind,
};
use crate::managed::error::{corrupt, invalid};
use crate::managed::format::LowerHex;
use crate::managed::metadata::namespace::{
    NamespaceChange, StoredCommittedResult, StoredNamespaceState, require_request_digest,
};
use crate::managed::metadata::object::{ensure_immutable, read};

impl NamespaceStore {
    pub(super) async fn resolve_known(
        &self,
        operation: OperationId,
        expected: Option<[u8; 32]>,
    ) -> Result<CommitOutcome, VolumeError> {
        let Some((head, _)) = self.read_bound_head("resolve Managed publication").await? else {
            return Ok(CommitOutcome::Absent);
        };
        let Some(state) = &head.state else {
            return Ok(CommitOutcome::Absent);
        };
        self.resolve_from_state(state, operation, expected).await
    }

    async fn resolve_from_state(
        &self,
        state: &StoredNamespaceState,
        operation: OperationId,
        expected: Option<[u8; 32]>,
    ) -> Result<CommitOutcome, VolumeError> {
        if let Some(result) = state.resolve(self.branch_id(), operation) {
            require_request_digest(expected, result.request_sha256)?;
            return Ok(CommitOutcome::Committed(result.cursor));
        }
        if !state.maybe_contains(operation) {
            return Ok(CommitOutcome::Absent);
        }
        if let Some(result) = find_committed_result(&state.tail, self.branch_id(), operation)? {
            require_request_digest(expected, result.request_sha256)?;
            return Ok(CommitOutcome::Committed(result.cursor));
        }
        let Some(result) = self.read_operation(operation).await? else {
            for reference in state.segments.iter().rev() {
                let segment = self.read_change_segment(*reference).await?;
                if let Some(result) =
                    find_committed_result(&segment.changes, self.branch_id(), operation)?
                {
                    require_request_digest(expected, result.request_sha256)?;
                    return Ok(CommitOutcome::Committed(result.cursor));
                }
            }
            return Ok(CommitOutcome::Absent);
        };
        require_request_digest(expected, result.request_sha256)?;
        Ok(CommitOutcome::Committed(result.cursor))
    }

    async fn read_operation(
        &self,
        operation: OperationId,
    ) -> Result<Option<StoredCommittedResult>, VolumeError> {
        let Some(bytes) = read(
            &self.data,
            &self.operation_key(operation),
            OPERATION_RECORD.maximum_encoded_bytes(),
            "resolve Managed publication",
        )
        .await?
        else {
            return Ok(None);
        };
        let result: StoredCommittedResult = OPERATION_RECORD
            .decode(&bytes)
            .map_err(|error| corrupt("resolve Managed publication", error))?;
        if result.origin_branch != self.branch_id() || result.operation() != Some(operation) {
            return Err(corrupt(
                "resolve Managed publication",
                "operation receipt identity is invalid",
            ));
        }
        Ok(Some(result))
    }

    pub(super) async fn write_operation(
        &self,
        result: &StoredCommittedResult,
    ) -> Result<(), VolumeError> {
        let operation = result
            .operation()
            .expect("a committed result has a publication operation");
        let bytes = OPERATION_RECORD
            .encode(result)
            .map_err(|error| invalid("record Managed publication", error))?;
        ensure_immutable(
            &self.data,
            &self.operation_key(operation),
            bytes.into(),
            "record Managed publication",
        )
        .await
    }

    fn operation_key(&self, operation: OperationId) -> String {
        let scope = self.branch_id().map_or_else(
            || "base".to_owned(),
            |branch| LowerHex::encode(branch.as_bytes()),
        );
        format!(
            "{OPERATION_ROOT}/{scope}/{}.ofs",
            LowerHex::encode(operation.as_bytes())
        )
    }

    pub(super) async fn outcome_after_race(
        &self,
        operation: OperationId,
        request_digest: [u8; 32],
    ) -> Result<CommitOutcome, VolumeError> {
        let head = match self.read_bound_head("resolve Managed publication").await {
            Err(error) if error.kind() == VolumeErrorKind::Unavailable => {
                return Ok(CommitOutcome::Unknown);
            }
            result => result?,
        };
        let Some((head, _)) = head else {
            return Ok(CommitOutcome::Conflict {
                observed: ChangeCursor::Genesis,
            });
        };
        let observed = head.cursor();
        let resolved = match &head.state {
            Some(state) => {
                self.resolve_from_state(state, operation, Some(request_digest))
                    .await
            }
            None => Ok(CommitOutcome::Absent),
        };
        match resolved {
            Err(error) if error.kind() == VolumeErrorKind::Unavailable => {
                Ok(CommitOutcome::Unknown)
            }
            Err(error) => Err(error),
            Ok(result @ (CommitOutcome::Committed(_) | CommitOutcome::Unknown)) => Ok(result),
            Ok(_) => Ok(CommitOutcome::Conflict { observed }),
        }
    }
}

fn find_committed_result(
    changes: &[NamespaceChange],
    branch: Option<BranchId>,
    operation: OperationId,
) -> Result<Option<StoredCommittedResult>, VolumeError> {
    changes
        .iter()
        .rev()
        .find(|change| change.origin_branch == branch && change.operation() == operation)
        .map(committed_result)
        .transpose()
}
