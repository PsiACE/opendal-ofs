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

//! D1 commit point for the shared Managed namespace implementation.

use opendal::Operator;

use super::store::{NamespaceObservation, NamespaceStore};
use crate::filesystem::VolumeId;
use crate::managed::D1Metadata;
use crate::managed::metadata::record::D1RecordBackend;

pub(crate) type D1Namespace = NamespaceStore<D1RecordBackend>;
pub(crate) type D1NamespaceObservation = NamespaceObservation<u64>;

impl NamespaceStore<D1RecordBackend> {
    pub(crate) fn new(volume_id: VolumeId, operator: Operator, metadata: D1Metadata) -> Self {
        Self {
            volume_id,
            operator,
            backend: D1RecordBackend::new(volume_id, metadata),
        }
    }
}
