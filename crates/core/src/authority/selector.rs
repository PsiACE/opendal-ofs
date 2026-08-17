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

//! Authority root selection. Default is the sole base root.

use std::fmt;

use crate::Error;
use crate::format::ExtensionDescriptor;

use super::{AuthorityStore, DEFAULT_AUTHORITY, DefaultAuthorityStore};

/// Selects one authority identity without implementing the store state machine.
pub trait AuthoritySelector: Send + Sync + Clone + fmt::Debug + Unpin + 'static {
    type Store: AuthorityStore + Clone;

    fn descriptor(&self) -> Option<&ExtensionDescriptor>;

    fn default_name(&self) -> &'static str {
        DEFAULT_AUTHORITY
    }

    fn validate_name(&self, name: &str) -> Result<(), Error> {
        if name == self.default_name() {
            Ok(())
        } else {
            Err(Error::new(
                crate::ErrorKind::NotFound,
                "open Managed authority",
                "the selected authority does not exist",
            ))
        }
    }

    fn store(&self) -> Self::Store;
}

/// Default root: the single `main` authority.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultSelector;

impl AuthoritySelector for DefaultSelector {
    type Store = DefaultAuthorityStore;

    fn descriptor(&self) -> Option<&ExtensionDescriptor> {
        None
    }

    fn store(&self) -> Self::Store {
        DefaultAuthorityStore
    }
}
