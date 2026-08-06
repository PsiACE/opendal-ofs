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

//! Stable user-facing error categories at provider boundaries.

use serde::Serialize;

use crate::d1::QueryFailureKind;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ErrorCategory {
    InvalidInput,
    NotFound,
    PermissionDenied,
    Conflict,
    RateLimited,
    Unavailable,
    NotAuthoritative,
    Corrupt,
    Unknown,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ErrorSummary {
    pub(crate) kind: ErrorCategory,
    pub(crate) message: String,
}

impl ErrorSummary {
    pub(crate) fn from_error(error: &anyhow::Error) -> Self {
        let kind = error
            .chain()
            .find_map(|source| {
                source
                    .downcast_ref::<opendal::Error>()
                    .map(open_dal_category)
                    .or_else(|| {
                        source
                            .downcast_ref::<crate::d1::QueryFailure>()
                            .map(|error| d1_category(error.kind))
                    })
                    .or_else(|| source.downcast_ref::<std::io::Error>().map(io_category))
            })
            .unwrap_or(ErrorCategory::Unknown);
        Self {
            kind,
            message: bounded_message(format!("{error:#}")),
        }
    }
}

fn open_dal_category(error: &opendal::Error) -> ErrorCategory {
    use opendal::ErrorKind;

    match error.kind() {
        ErrorKind::ConfigInvalid
        | ErrorKind::Unsupported
        | ErrorKind::IsADirectory
        | ErrorKind::NotADirectory
        | ErrorKind::IsSameFile
        | ErrorKind::RangeNotSatisfied => ErrorCategory::InvalidInput,
        ErrorKind::NotFound => ErrorCategory::NotFound,
        ErrorKind::PermissionDenied => ErrorCategory::PermissionDenied,
        ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch => ErrorCategory::Conflict,
        ErrorKind::RateLimited => ErrorCategory::RateLimited,
        ErrorKind::Unexpected if error.is_temporary() => ErrorCategory::Unavailable,
        ErrorKind::Unexpected => ErrorCategory::Unknown,
        _ => ErrorCategory::Unknown,
    }
}

fn d1_category(kind: QueryFailureKind) -> ErrorCategory {
    match kind {
        QueryFailureKind::Transport | QueryFailureKind::Service => ErrorCategory::Unavailable,
        QueryFailureKind::RateLimited => ErrorCategory::RateLimited,
        QueryFailureKind::InvalidRequest => ErrorCategory::InvalidInput,
        QueryFailureKind::NotFound => ErrorCategory::NotFound,
        QueryFailureKind::PermissionDenied => ErrorCategory::PermissionDenied,
        QueryFailureKind::Conflict => ErrorCategory::Conflict,
        QueryFailureKind::InvalidResponse => ErrorCategory::Corrupt,
        QueryFailureKind::NotAuthoritative => ErrorCategory::NotAuthoritative,
        QueryFailureKind::Local | QueryFailureKind::Rejected | QueryFailureKind::Statement => {
            ErrorCategory::Unknown
        }
    }
}

fn io_category(error: &std::io::Error) -> ErrorCategory {
    use std::io::ErrorKind;

    match error.kind() {
        ErrorKind::NotFound => ErrorCategory::NotFound,
        ErrorKind::PermissionDenied => ErrorCategory::PermissionDenied,
        ErrorKind::InvalidInput | ErrorKind::InvalidData => ErrorCategory::InvalidInput,
        ErrorKind::AlreadyExists => ErrorCategory::Conflict,
        ErrorKind::TimedOut
        | ErrorKind::WouldBlock
        | ErrorKind::Interrupted
        | ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe => ErrorCategory::Unavailable,
        ErrorKind::UnexpectedEof => ErrorCategory::Corrupt,
        _ => ErrorCategory::Unknown,
    }
}

fn bounded_message(message: String) -> String {
    if message.len() <= 1024 {
        return message;
    }
    let mut end = 1024;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &message[..end])
}
