// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Local replica foundations owned by the Sync access model.

mod engine;
mod local;
mod publication;
mod reconcile;
mod snapshot;
mod staging;
mod state;

pub use engine::{SyncEngine, SyncResult};
pub use state::{ConflictRecord, PendingIntent, ReplicaState};

pub(crate) use local::{LocalKind, LocalTree};
pub(crate) use publication::build_publication;
pub(crate) use reconcile::{ReconcileAction, reconcile};
pub(crate) use snapshot::snapshot_paths;
pub(crate) use staging::StagedTree;
pub(crate) use state::BaseEntry;
