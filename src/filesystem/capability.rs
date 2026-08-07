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

/// One effective filesystem capability reported to a user.
///
/// The scopes are product vocabulary, not provider capability names. Missing
/// guarantees carry the error returned when the caller starts an access mode
/// that requires them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Capability {
    Supported {
        name: String,
        atomicity_scope: String,
        durability_boundary: String,
        multi_client_visibility: String,
    },
    Unsupported {
        name: String,
        access_start_error: String,
    },
}

impl Capability {
    pub fn name(&self) -> &str {
        match self {
            Self::Supported { name, .. } | Self::Unsupported { name, .. } => name,
        }
    }
}

/// Effective capabilities for one selected volume and access combination.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Capabilities(Vec<Capability>);

impl Capabilities {
    pub fn new(capabilities: Vec<Capability>) -> Self {
        Self(capabilities)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Capability> {
        self.0.iter()
    }
}
