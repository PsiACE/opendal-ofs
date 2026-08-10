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

use crate::filesystem::{VolumeError, VolumeErrorKind};

/// Stable failure classes exposed by Managed volume actions.
pub type ManagedErrorKind = VolumeErrorKind;

/// Failure to create or read Managed volume state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedError(VolumeError);

impl ManagedError {
    pub const fn kind(&self) -> ManagedErrorKind {
        self.0.kind()
    }

    pub(crate) fn new(
        kind: ManagedErrorKind,
        action: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self(VolumeError::new(
            kind,
            format!("{action}: {}", message.into()),
        ))
    }
}

impl fmt::Display for ManagedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for ManagedError {}

impl From<ManagedError> for VolumeError {
    fn from(error: ManagedError) -> Self {
        error.0
    }
}
