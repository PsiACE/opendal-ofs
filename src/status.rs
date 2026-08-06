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

use std::fmt;

use anyhow::Result;
use serde::Serialize;

use crate::error::ErrorSummary;
use crate::model::VolumeId;
use crate::replica::{ConflictKind, ReplicaPaths, ReplicaState};
use crate::store::Observation;

#[derive(Debug, Serialize)]
pub(crate) struct SyncStatus {
    format_version: u32,
    volume: StatusVolume,
    access: &'static str,
    local: LocalState,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_error: Option<ErrorSummary>,
    base: Option<BaseState>,
    remote: RemoteState,
    publication: WorkState,
    materialize: WorkState,
    conflicts: usize,
    conflict_records: Vec<StatusConflict>,
    metadata: &'static str,
    capabilities: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct StatusVolume {
    name: String,
    id: VolumeId,
    model: &'static str,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum LocalState {
    Clean,
    Changed,
    Unknown,
}

#[derive(Debug, Serialize)]
struct BaseState {
    generation: u64,
}

#[derive(Debug, Serialize)]
struct RemoteState {
    state: RemotePosition,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorSummary>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum RemotePosition {
    AtBase,
    Ahead,
    Observed,
    Unknown,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkState {
    Idle,
    Pending,
    Conflict,
}

#[derive(Debug, Serialize)]
struct StatusConflict {
    path: String,
    kind: ConflictKind,
    remote_generation: u64,
    resolution: &'static str,
}

impl SyncStatus {
    pub(crate) fn inspect(
        volume_name: &str,
        metadata: &'static str,
        paths: &ReplicaPaths,
        state: &ReplicaState,
        remote: Result<&Observation, &ErrorSummary>,
    ) -> Result<Self> {
        let (local, local_error) = match crate::replica::scan(
            paths,
            state.common.as_ref().map(|base| &base.manifest),
            false,
        ) {
            Ok(manifest)
                if state
                    .common
                    .as_ref()
                    .is_some_and(|base| base.manifest == manifest) =>
            {
                (LocalState::Clean, None)
            }
            Ok(_) => (LocalState::Changed, None),
            Err(error) => (LocalState::Unknown, Some(ErrorSummary::from_error(&error))),
        };
        let base = state.common.as_ref().map(|value| BaseState {
            generation: value.cursor.generation,
        });
        let remote = match remote {
            Ok(observation) => {
                let generation = observation.head.cursor.generation;
                let position = match &state.common {
                    Some(base) if base.cursor == observation.head.cursor => RemotePosition::AtBase,
                    Some(base) if base.cursor.generation < generation => RemotePosition::Ahead,
                    _ => RemotePosition::Observed,
                };
                RemoteState {
                    state: position,
                    generation: Some(generation),
                    error: None,
                }
            }
            Err(error) => RemoteState {
                state: RemotePosition::Unknown,
                generation: None,
                error: Some(error.clone()),
            },
        };
        let conflict_records = state
            .conflicts
            .iter()
            .map(|value| StatusConflict {
                path: value.path.clone(),
                kind: value.kind.clone(),
                remote_generation: value.remote_cursor.generation,
                resolution: "unresolved",
            })
            .collect::<Vec<_>>();
        Ok(Self {
            format_version: 1,
            volume: StatusVolume {
                name: volume_name.to_owned(),
                id: state.volume_id.clone(),
                model: "managed",
            },
            access: "sync",
            local,
            local_error,
            base,
            remote,
            publication: if state.publication.is_some() {
                WorkState::Pending
            } else if !state.conflicts.is_empty() {
                WorkState::Conflict
            } else {
                WorkState::Idle
            },
            materialize: if state.materialization.is_some() {
                WorkState::Pending
            } else {
                WorkState::Idle
            },
            conflicts: conflict_records.len(),
            conflict_records,
            metadata,
            capabilities: crate::sync::CAPABILITIES.to_vec(),
        })
    }
}

impl fmt::Display for SyncStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "volume:       {} (managed)", self.volume.name)?;
        writeln!(formatter, "id:           {}", self.volume.id)?;
        writeln!(formatter, "access:       sync")?;
        writeln!(formatter, "local:        {}", word(&self.local))?;
        match &self.base {
            Some(base) => writeln!(formatter, "base:         generation {}", base.generation)?,
            None => writeln!(formatter, "base:         none")?,
        }
        match self.remote.generation {
            Some(generation) => writeln!(
                formatter,
                "remote:       {} (generation {generation})",
                remote_word(self.remote.state)
            )?,
            None => writeln!(formatter, "remote:       unknown")?,
        }
        writeln!(formatter, "publication:  {}", work_word(self.publication))?;
        writeln!(formatter, "materialize:  {}", work_word(self.materialize))?;
        writeln!(formatter, "conflicts:    {}", self.conflicts)?;
        writeln!(formatter, "metadata:     {}", self.metadata)?;
        write!(formatter, "capabilities: {}", self.capabilities.join(", "))
    }
}

fn word(value: &LocalState) -> &'static str {
    match value {
        LocalState::Clean => "clean",
        LocalState::Changed => "changed",
        LocalState::Unknown => "unknown",
    }
}

fn remote_word(value: RemotePosition) -> &'static str {
    match value {
        RemotePosition::AtBase => "at-base",
        RemotePosition::Ahead => "ahead",
        RemotePosition::Observed => "observed",
        RemotePosition::Unknown => "unknown",
    }
}

fn work_word(value: WorkState) -> &'static str {
    match value {
        WorkState::Idle => "idle",
        WorkState::Pending => "pending",
        WorkState::Conflict => "conflict",
    }
}
