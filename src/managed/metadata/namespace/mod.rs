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

//! Managed namespace records and authoritative snapshot publication.

mod change;
mod checkpoint;
mod d1;
mod records;
mod store;
mod validation;

#[cfg(feature = "managed-branch")]
pub(crate) use change::NamespaceChange;
pub(crate) use checkpoint::{CheckpointPart, CheckpointRoot, PendingCheckpoint};
pub(crate) use d1::{D1Namespace, D1NamespaceObservation};
pub use records::NamespaceGcSweep;
pub(crate) use records::{
    DirectoryPrecondition, DirectoryRecord, FileVersionRecord, NamespacePublication,
    NamespaceSnapshot, NodePrecondition, NodeRecord, managed_generation, next_managed_generation,
};
pub(crate) use store::{NamespaceObservation, ObjectNamespace};
#[cfg(feature = "managed-branch")]
pub(crate) use validation::{validate_publication, validate_snapshot};
