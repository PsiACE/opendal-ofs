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

use std::collections::BTreeMap;

use opendal::Operator;
use serde::{Deserialize, Serialize};

use crate::filesystem::{
    ChangeCursor, DirectoryRecord, Generation, NodeAttributes, NodeId, NodeKind, NodeRecord,
    VolumeError, VolumeErrorKind, VolumeId, VolumeSnapshot,
};

use super::format::ManagedFormat;
use super::history;
use super::object;
use super::record::Record;

const HEAD_KEY: &str = ".ofs/managed/head";
const HEAD_RECORD: Record = Record::new(*b"OFSHEAD1", 64 * 1024 * 1024);

#[derive(Clone)]
pub struct ManagedVolume {
    format: ManagedFormat,
    operator: Operator,
}

pub struct ManagedObservation {
    pub snapshot: VolumeSnapshot,
    revision: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Head {
    snapshot: VolumeSnapshot,
}

impl ManagedVolume {
    pub(super) fn new(format: ManagedFormat, operator: Operator) -> Self {
        Self { format, operator }
    }

    pub(super) async fn open(
        format: ManagedFormat,
        operator: Operator,
    ) -> Result<Self, VolumeError> {
        let volume = Self::new(format, operator);
        volume.observe().await?;
        Ok(volume)
    }

    pub const fn id(&self) -> VolumeId {
        self.format.volume_id()
    }

    pub(super) async fn initialize(&self) -> Result<(), VolumeError> {
        let snapshot = empty_snapshot(self.id());
        let bytes = HEAD_RECORD.encode(&Head { snapshot })?;
        if object::create(&self.operator, HEAD_KEY, bytes).await? {
            return Ok(());
        }
        self.observe().await.map(drop)
    }

    pub async fn observe(&self) -> Result<ManagedObservation, VolumeError> {
        let (bytes, revision) = object::read_with_revision(
            &self.operator,
            HEAD_KEY,
            HEAD_RECORD.maximum_encoded_bytes(),
        )
        .await?
        .ok_or_else(|| {
            VolumeError::new(
                VolumeErrorKind::Corrupt,
                "open Managed volume: namespace head is missing",
            )
        })?;
        let head: Head = HEAD_RECORD.decode(&bytes)?;
        head.snapshot.validate()?;
        if head.snapshot.volume_id != self.id() {
            return Err(VolumeError::new(
                VolumeErrorKind::Corrupt,
                "open Managed volume: namespace head belongs to a different volume",
            ));
        }
        Ok(ManagedObservation {
            snapshot: head.snapshot,
            revision,
        })
    }

    pub async fn publish(
        &self,
        observed: &ManagedObservation,
        target: VolumeSnapshot,
    ) -> Result<(), VolumeError> {
        target.validate()?;
        if target.volume_id != self.id()
            || target.cursor.sequence() != observed.snapshot.cursor.sequence() + 1
            || target.cursor.operation().is_none()
        {
            return Err(VolumeError::new(
                VolumeErrorKind::Invalid,
                "publish Managed namespace: publication ancestry is invalid",
            ));
        }
        let operation = target
            .cursor
            .operation()
            .expect("validated publication has an operation identity");
        history::prepare(
            &self.operator,
            observed.snapshot.cursor,
            target.cursor,
        )
        .await?;
        let bytes = HEAD_RECORD.encode(&Head { snapshot: target })?;
        let committed = object::replace(&self.operator, HEAD_KEY, &observed.revision, bytes).await?;
        history::finish(&self.operator, operation, committed).await?;
        if committed {
            Ok(())
        } else {
            Err(VolumeError::new(
                VolumeErrorKind::Conflict,
                "publish Managed namespace: observed generation changed",
            ))
        }
    }

    pub async fn operation_committed(
        &self,
        operation: crate::filesystem::OperationId,
        observed: &ManagedObservation,
    ) -> Result<bool, VolumeError> {
        history::committed(&self.operator, operation, observed.snapshot.cursor).await
    }

    pub(crate) fn operator(&self) -> &Operator {
        &self.operator
    }
}

fn empty_snapshot(volume_id: VolumeId) -> VolumeSnapshot {
    let root = NodeId::generate();
    let generation = Generation::from_bytes(0_u64.to_be_bytes().to_vec());
    VolumeSnapshot {
        volume_id,
        cursor: ChangeCursor::Genesis,
        root,
        nodes: BTreeMap::from([(
            root,
            NodeRecord {
                id: root,
                generation: generation.clone(),
                kind: NodeKind::Directory,
                attributes: NodeAttributes::default(),
                file_version: None,
            },
        )]),
        directories: BTreeMap::from([(
            root,
            DirectoryRecord {
                node: root,
                generation,
                entries: BTreeMap::new(),
            },
        )]),
        file_versions: BTreeMap::new(),
    }
}
