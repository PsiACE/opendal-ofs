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
use crate::managed::metadata::superblock::SUPERBLOCK_KEY;
use crate::managed::{ManagedError, ManagedErrorKind, ManagedFormat};

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
            .write_with(SUPERBLOCK_KEY, encoded)
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
            .read(SUPERBLOCK_KEY)
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
