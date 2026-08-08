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

use std::fmt;

/// Stable failure classes exposed by Managed volume actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedErrorKind {
    UnsupportedFormat,
    Invalid,
    Conflict,
    Corrupt,
    Unavailable,
}

/// Failure to create or read Managed volume state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedError {
    kind: ManagedErrorKind,
    action: &'static str,
    message: String,
}

impl ManagedError {
    pub const fn kind(&self) -> ManagedErrorKind {
        self.kind
    }

    pub(crate) fn new(
        kind: ManagedErrorKind,
        action: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            action,
            message: message.into(),
        }
    }
}

impl fmt::Display for ManagedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.action, self.message)
    }
}

impl std::error::Error for ManagedError {}
