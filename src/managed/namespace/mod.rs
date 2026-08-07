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
mod d1;
mod object;
mod records;
mod validation;

pub(crate) use d1::{D1Namespace, D1NamespaceObservation};
pub use object::{NamespaceObservation, ObjectNamespace};
pub use records::{
    ChunkSpan, ChunkingAlgorithm, ChunkingSpec, ContentRef, DataExtent, DirectoryPrecondition,
    DirectoryRecord, FileExtent, FileVersionLayout, FileVersionRecord, NamespaceGcSweep,
    NamespacePublication, NamespaceSnapshot, NodePrecondition, NodeRecord,
};
pub(crate) use records::{managed_generation, managed_generation_number, next_managed_generation};
pub(crate) use validation::validate_snapshot;
