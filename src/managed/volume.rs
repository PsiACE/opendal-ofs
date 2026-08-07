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

//! Concrete composition of the Managed namespace and data plane.

use opendal::Operator;

use super::namespace::{
    FileVersionRecord, NamespaceObservation, NamespacePublication, ObjectNamespace,
};
use super::{ManagedData, ManagedError};
use crate::filesystem::{CommitOutcome, OperationId, VolumeId};

#[derive(Clone)]
pub struct ManagedVolume {
    namespace: ObjectNamespace,
    data: ManagedData,
}

impl ManagedVolume {
    pub fn object(volume_id: VolumeId, data_operator: Operator) -> Result<Self, ManagedError> {
        Ok(Self {
            namespace: ObjectNamespace::new(volume_id, data_operator.clone())?,
            data: ManagedData::new(data_operator)?,
        })
    }

    pub async fn observe(&self) -> Result<Option<NamespaceObservation>, ManagedError> {
        self.namespace.observe().await
    }

    pub async fn seal_whole_file(
        &self,
        frozen: &Operator,
        path: &str,
    ) -> Result<FileVersionRecord, ManagedError> {
        self.data.seal_whole_file(frozen, path).await
    }

    pub async fn publish(
        &self,
        observed: Option<&NamespaceObservation>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        self.namespace.publish(observed, publication).await
    }

    pub async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        self.namespace.resolve(operation).await
    }

    pub async fn materialize(
        &self,
        version: &FileVersionRecord,
        target: &Operator,
        path: &str,
    ) -> Result<(), ManagedError> {
        self.data.read_to(version, target, path).await
    }
}
