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

mod engine;
mod install;
mod local_fs;
mod local_scan;
mod pack;
mod publication;
mod reconcile;
mod recovery;
mod rename;
mod scan;
mod state;
mod state_file;
mod transfer;

pub use engine::{SyncEngine, SyncOutcome};
pub(crate) use state::ConflictRecord;
pub use state::ReplicaState;

/// Load and validate the lightweight state of one local replica.
pub fn load_replica_state(path: &std::path::Path) -> Result<Option<ReplicaState>, crate::Error> {
    state_file::load(path)
}
