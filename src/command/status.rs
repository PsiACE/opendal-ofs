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

use std::fs;

use anyhow::{Context, Result, bail};

use crate::cli::StatusArgs;

use super::state::ReplicaState;

pub(super) fn run(args: StatusArgs) -> Result<()> {
    let root = fs::canonicalize(&args.replica)
        .with_context(|| format!("cannot open replica directory: {}", args.replica.display()))?;
    let state = ReplicaState::load(&args.state)?;
    if state.root != root {
        bail!("replica state belongs to a different local directory");
    }
    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "access_model": "sync",
                "conflicts": 0,
                "common_sequence": state.cursor.sequence(),
                "pending": false,
                "volume_id": state.volume_id.to_string(),
                "volume_model": "managed",
            })
        );
    } else {
        println!(
            "managed sync volume {} at change {}, 0 pending, 0 conflict(s)",
            state.volume_id,
            state.cursor.sequence()
        );
    }
    Ok(())
}
