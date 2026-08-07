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

mod d1;
mod object;

pub use d1::{D1Config, D1Metadata};
pub use object::ObjectMetadata;

use super::{ManagedError, ManagedFormat};

/// Physical metadata placement for a Managed volume.
#[derive(Clone)]
pub enum Metadata {
    Object(ObjectMetadata),
    D1(D1Metadata),
}

impl Metadata {
    pub async fn create_format(
        &self,
        desired: &ManagedFormat,
    ) -> Result<ManagedFormat, ManagedError> {
        match self {
            Self::Object(metadata) => metadata.create_format(desired).await,
            Self::D1(metadata) => metadata.create_format(desired).await,
        }
    }

    pub async fn read_format(&self) -> Result<ManagedFormat, ManagedError> {
        match self {
            Self::Object(metadata) => metadata.read_format().await,
            Self::D1(metadata) => metadata.read_format().await,
        }
    }
}

fn require_same_format(
    desired: &ManagedFormat,
    observed: ManagedFormat,
) -> Result<ManagedFormat, ManagedError> {
    if &observed == desired {
        Ok(observed)
    } else {
        Err(ManagedError::new(
            super::ManagedErrorKind::Conflict,
            "create Managed format",
            "metadata is bound to another Managed volume",
        ))
    }
}
