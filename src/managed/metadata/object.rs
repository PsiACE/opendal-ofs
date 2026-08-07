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

use opendal::{ErrorKind, Operator};

use super::require_same_format;
use crate::managed::{ManagedError, ManagedErrorKind, ManagedFormat};

const FORMAT_KEY: &str = ".ofs/managed-sync/format.json";

/// Managed metadata stored beside data through OpenDAL.
#[derive(Clone)]
pub struct ObjectMetadata {
    operator: Operator,
}

impl ObjectMetadata {
    pub const fn new(operator: Operator) -> Self {
        Self { operator }
    }

    pub async fn create_format(
        &self,
        desired: &ManagedFormat,
    ) -> Result<ManagedFormat, ManagedError> {
        desired.validate_for_write()?;
        if !self
            .operator
            .info()
            .full_capability()
            .write_with_if_not_exists
        {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "create Managed format",
                "object metadata requires create-only write",
            ));
        }
        let encoded = desired.encode()?;
        match self
            .operator
            .write_with(FORMAT_KEY, encoded)
            .if_not_exists(true)
            .await
        {
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                ) => {}
            Err(_) => return Err(unavailable("create Managed format")),
        }
        require_same_format(desired, self.read_format().await?)
    }

    pub async fn read_format(&self) -> Result<ManagedFormat, ManagedError> {
        let bytes = self
            .operator
            .read(FORMAT_KEY)
            .await
            .map_err(|_| unavailable("read Managed format"))?;
        ManagedFormat::decode(&bytes.to_bytes())
    }
}

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "object metadata is unavailable",
    )
}

#[cfg(test)]
mod tests {
    use opendal::services::Memory;

    use super::*;
    use crate::filesystem::VolumeId;
    use crate::managed::MetadataPlacement;

    #[tokio::test]
    async fn pre_section_object_v1_is_rejected_at_the_stable_marker() {
        let operator = Operator::new(Memory::default()).unwrap().finish();
        operator
            .write(
                FORMAT_KEY,
                br#"{
                    "magic":"ofs-managed-volume",
                    "major":1,
                    "minor":0,
                    "volume_id":"01010101010101010101010101010101",
                    "metadata_placement":"colocated_object",
                    "data_root_binding":"memory://root",
                    "naming_policy":"portable_utf8",
                    "required_reader_features":["file-version-layouts-v1"],
                    "required_writer_features":["file-version-layouts-v1"]
                }"#
                .to_vec(),
            )
            .await
            .unwrap();
        let desired = ManagedFormat::v1(
            VolumeId::from_bytes([1; 16]),
            MetadataPlacement::ColocatedObject,
            "memory://root",
        )
        .unwrap();
        let error = ObjectMetadata::new(operator)
            .create_format(&desired)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ManagedErrorKind::Invalid);
    }
}
