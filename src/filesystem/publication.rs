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

use super::ChangeCursor;

/// The authoritative result of a generation-checked publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    /// The mutation is visible at this change-stream position.
    Committed(ChangeCursor),
    /// Recovery proved that the operation did not commit.
    Absent,
    /// An observed precondition no longer matches authoritative state.
    Conflict { observed: ChangeCursor },
    /// The caller must retain its intent and resolve the original operation.
    Unknown,
}

/// Runtime-independent progress of one durable publication intent.
///
/// Sync implementations reconstruct this progress from the durable intent,
/// the authoritative outcome, and the installed replica. The common base and
/// intent are committed together only after the published tree is installed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationProgress {
    Prepared { base: ChangeCursor },
    Published { committed: ChangeCursor },
    Installed { common: ChangeCursor },
    CommonBaseAdvanced { common: ChangeCursor },
    IntentCleared { common: ChangeCursor },
    Retry { observed: ChangeCursor },
    Unknown { base: ChangeCursor },
}

impl PublicationProgress {
    pub const fn prepared(base: ChangeCursor) -> Self {
        Self::Prepared { base }
    }

    pub fn record_outcome(self, outcome: CommitOutcome) -> Option<Self> {
        let Self::Prepared { base } = self else {
            return None;
        };
        Some(match outcome {
            CommitOutcome::Committed(committed) => Self::Published { committed },
            CommitOutcome::Absent => Self::Retry { observed: base },
            CommitOutcome::Conflict { observed } => Self::Retry { observed },
            CommitOutcome::Unknown => Self::Unknown { base },
        })
    }

    pub fn record_install(self, common: ChangeCursor) -> Option<Self> {
        let Self::Published { committed } = self else {
            return None;
        };
        (common.sequence() >= committed.sequence()).then_some(Self::Installed { common })
    }

    pub fn record_common_base(self, common: ChangeCursor) -> Option<Self> {
        match self {
            Self::Installed { common: installed } if installed == common => {
                Some(Self::CommonBaseAdvanced { common })
            }
            _ => None,
        }
    }

    pub fn record_intent_clear(self) -> Option<Self> {
        match self {
            Self::CommonBaseAdvanced { common } => Some(Self::IntentCleared { common }),
            _ => None,
        }
    }
}
