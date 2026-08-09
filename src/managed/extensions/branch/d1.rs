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

use super::store::BranchStore;
use crate::filesystem::VolumeId;
use crate::managed::D1Metadata;
use crate::managed::metadata::record::D1RecordBackend;

pub type D1BranchStore = BranchStore<D1RecordBackend>;

impl BranchStore<D1RecordBackend> {
    pub fn new(volume_id: VolumeId, metadata: D1Metadata) -> Self {
        Self {
            volume_id,
            backend: D1RecordBackend::new(volume_id, metadata),
        }
    }
}
