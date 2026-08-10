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

use opendal::{Buffer, Operator};
use serde::{Deserialize, Serialize};

use crate::filesystem::{ChangeCursor, OperationId, VolumeError, VolumeErrorKind};

use super::object;
use super::record::Record;

const HISTORY_RECORD: Record = Record::new(*b"OFSHIST1", 1024);
const RESULT_RECORD: Record = Record::new(*b"OFSRSLT1", 1024);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoryRecord {
    previous: ChangeCursor,
    target: ChangeCursor,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PublicationOutcome {
    Committed,
    Conflict,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ResultRecord {
    operation: OperationId,
    outcome: PublicationOutcome,
}

pub(super) async fn prepare(
    operator: &Operator,
    previous: ChangeCursor,
    target: ChangeCursor,
) -> Result<(), VolumeError> {
    let operation = target
        .operation()
        .ok_or_else(|| invalid("history target is Genesis"))?;
    let bytes = HISTORY_RECORD.encode(&HistoryRecord { previous, target })?;
    object::create_immutable(operator, &history_key(operation), Buffer::from(bytes)).await
}

pub(super) async fn finish(
    operator: &Operator,
    operation: OperationId,
    committed: bool,
) -> Result<(), VolumeError> {
    let outcome = if committed {
        PublicationOutcome::Committed
    } else {
        PublicationOutcome::Conflict
    };
    let bytes = RESULT_RECORD.encode(&ResultRecord { operation, outcome })?;
    object::create_immutable(operator, &result_key(operation), Buffer::from(bytes)).await
}

pub(super) async fn committed(
    operator: &Operator,
    operation: OperationId,
    current: ChangeCursor,
) -> Result<bool, VolumeError> {
    if let Some(bytes) = object::read(
        operator,
        &result_key(operation),
        RESULT_RECORD.maximum_encoded_bytes(),
    )
    .await?
    {
        let result: ResultRecord = RESULT_RECORD.decode(&bytes)?;
        if result.operation != operation {
            return Err(corrupt("operation result identity does not match its key"));
        }
        return Ok(result.outcome == PublicationOutcome::Committed);
    }

    let mut cursor = current;
    while let Some(cursor_operation) = cursor.operation() {
        if cursor_operation == operation {
            return Ok(true);
        }
        let bytes = object::read(
            operator,
            &history_key(cursor_operation),
            HISTORY_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .ok_or_else(|| corrupt("committed history record is missing"))?;
        let history: HistoryRecord = HISTORY_RECORD.decode(&bytes)?;
        if history.target != cursor
            || history.previous.sequence().checked_add(1) != Some(cursor.sequence())
        {
            return Err(corrupt("committed history ancestry is invalid"));
        }
        cursor = history.previous;
    }
    Ok(false)
}

fn history_key(operation: OperationId) -> String {
    format!(".ofs/managed/history/{operation}")
}

fn result_key(operation: OperationId) -> String {
    format!(".ofs/managed/results/{operation}")
}

fn invalid(message: &'static str) -> VolumeError {
    VolumeError::new(
        VolumeErrorKind::Invalid,
        format!("publish Managed namespace: {message}"),
    )
}

fn corrupt(message: &'static str) -> VolumeError {
    VolumeError::new(
        VolumeErrorKind::Corrupt,
        format!("read Managed history: {message}"),
    )
}
