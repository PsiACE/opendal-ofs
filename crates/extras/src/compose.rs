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

//! Unique format-driven composition for this binary.

use ofs_core::Error;
use ofs_core::data::CoreAccess;
use ofs_core::format::VolumeFormat;

/// Components admitted by the current product for one `VolumeFormat`.
#[derive(Clone, Debug)]
pub struct VolumeComponents {
    format: VolumeFormat,
    access: CoreAccess,
}

impl VolumeComponents {
    pub const fn format(&self) -> &VolumeFormat {
        &self.format
    }

    pub const fn access(&self) -> &CoreAccess {
        &self.access
    }
}

/// Whole/Identity access used by this binary for new volumes.
pub fn access() -> CoreAccess {
    CoreAccess::default()
}

/// Admit a volume format for the shipped product.
///
/// Layout v0 only supports Whole/Identity file data and the default authority.
/// Unknown persisted extensions fail before any mutation.
pub fn compose(format: &VolumeFormat) -> Result<VolumeComponents, Error> {
    if format.file_data_layout().partitioning().is_some()
        || !format.file_data_layout().decodings().is_empty()
        || format.authority().is_some()
    {
        return Err(Error::new(
            ofs_core::ErrorKind::Unsupported,
            "compose Managed volume",
            "volume format uses an extension this binary does not implement",
        ));
    }
    Ok(VolumeComponents {
        format: format.clone(),
        access: access(),
    })
}
