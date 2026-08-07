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

//! Managed volume authority and its durable format.

mod data;
mod data_format;
mod error;
mod format;
mod metadata;
pub mod namespace;
pub mod pack;
mod section;
mod volume;

pub(crate) use data::{AuthorityKnownContent, ManagedData};
pub use data::{
    FileLayoutPolicy, LooseGcMaintenance, PackMaintenance, PackRetirement, SparseExtent,
};
pub use data_format::{DigestAlgorithm, ManagedDataFormat};
pub use error::{ManagedError, ManagedErrorKind};
pub use format::{ManagedFormat, MetadataPlacement, NamingPolicy};
pub use metadata::{D1Config, D1Metadata, Metadata, ObjectMetadata};
pub use namespace::NamespaceGcSweep;
pub(crate) use volume::ManagedMaterializer;
pub use volume::{ManagedObservation, ManagedVolume};
