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
    D1Namespace, D1NamespaceObservation, FileVersionRecord, NamespaceObservation,
    NamespacePublication, NamespaceSnapshot, ObjectNamespace,
};
use super::{
    D1Metadata, FileLayoutPolicy, ManagedData, ManagedError, ManagedErrorKind, PackMaintenance,
    SparseExtent,
};
use crate::filesystem::{CommitOutcome, OperationId, VolumeId};

#[derive(Clone)]
pub struct ManagedVolume {
    namespace: NamespaceAuthority,
    data: ManagedData,
}

#[derive(Clone)]
enum NamespaceAuthority {
    Object(ObjectNamespace),
    D1(D1Namespace),
}

#[derive(Clone, Debug)]
pub struct ManagedObservation {
    authority: AuthorityObservation,
}

#[derive(Clone, Debug)]
enum AuthorityObservation {
    Object(NamespaceObservation),
    D1(D1NamespaceObservation),
}

impl ManagedObservation {
    pub fn snapshot(&self) -> &NamespaceSnapshot {
        match &self.authority {
            AuthorityObservation::Object(observed) => &observed.snapshot,
            AuthorityObservation::D1(observed) => &observed.snapshot,
        }
    }
}

impl ManagedVolume {
    pub fn object(volume_id: VolumeId, data_operator: Operator) -> Result<Self, ManagedError> {
        Ok(Self {
            namespace: NamespaceAuthority::Object(ObjectNamespace::new(
                volume_id,
                data_operator.clone(),
            )?),
            data: ManagedData::new(data_operator)?,
        })
    }

    pub fn d1(
        volume_id: VolumeId,
        data_operator: Operator,
        metadata: D1Metadata,
    ) -> Result<Self, ManagedError> {
        Ok(Self {
            namespace: NamespaceAuthority::D1(D1Namespace::new(volume_id, metadata.session())),
            data: ManagedData::new(data_operator)?,
        })
    }

    pub fn with_file_layout(mut self, policy: FileLayoutPolicy) -> Result<Self, ManagedError> {
        self.data.set_policy(policy)?;
        Ok(self)
    }

    pub async fn observe(&self) -> Result<Option<ManagedObservation>, ManagedError> {
        match &self.namespace {
            NamespaceAuthority::Object(namespace) => {
                Ok(namespace
                    .observe()
                    .await?
                    .map(|observed| ManagedObservation {
                        authority: AuthorityObservation::Object(observed),
                    }))
            }
            NamespaceAuthority::D1(namespace) => {
                Ok(namespace
                    .observe()
                    .await?
                    .map(|observed| ManagedObservation {
                        authority: AuthorityObservation::D1(observed),
                    }))
            }
        }
    }

    pub async fn seal_whole_file(
        &self,
        frozen: &Operator,
        path: &str,
    ) -> Result<FileVersionRecord, ManagedError> {
        self.data.seal_whole_file(frozen, path).await
    }

    pub async fn seal_file(
        &self,
        frozen: &Operator,
        path: &str,
    ) -> Result<FileVersionRecord, ManagedError> {
        self.data.seal_file(frozen, path).await
    }

    pub async fn seal_extents(
        &self,
        frozen: &Operator,
        path: &str,
        extents: &[SparseExtent],
    ) -> Result<FileVersionRecord, ManagedError> {
        self.data.seal_extents(frozen, path, extents).await
    }

    pub async fn publish(
        &self,
        observed: Option<&ManagedObservation>,
        publication: &NamespacePublication,
    ) -> Result<CommitOutcome, ManagedError> {
        match (&self.namespace, observed.map(|value| &value.authority)) {
            (NamespaceAuthority::Object(namespace), Some(AuthorityObservation::Object(base))) => {
                namespace.publish(Some(base), publication).await
            }
            (NamespaceAuthority::D1(namespace), Some(AuthorityObservation::D1(base))) => {
                namespace.publish(Some(base), publication).await
            }
            (NamespaceAuthority::Object(namespace), None) => {
                namespace.publish(None, publication).await
            }
            (NamespaceAuthority::D1(namespace), None) => namespace.publish(None, publication).await,
            _ => Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "publish Managed namespace",
                "observation belongs to another metadata authority",
            )),
        }
    }

    pub async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, ManagedError> {
        match &self.namespace {
            NamespaceAuthority::Object(namespace) => namespace.resolve(operation).await,
            NamespaceAuthority::D1(namespace) => namespace.resolve(operation).await,
        }
    }

    /// Pack small content reachable from one fixed namespace observation.
    pub async fn pack_reachable_content(
        &self,
        observed: &ManagedObservation,
        operation: OperationId,
    ) -> Result<PackMaintenance, ManagedError> {
        self.data
            .pack_reachable(observed.snapshot(), operation)
            .await
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
