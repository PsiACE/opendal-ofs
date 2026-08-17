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

//! One-shot create-time choices projected into `VolumeFormat`.

use ofs_core::Error;
use ofs_core::format::{DEFAULT_DATA_SEGMENT_TARGET_BYTES, FileDataLayout};

/// User-facing create options for the shipped Managed product.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateOptions {
    data_segment_target_bytes: u64,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            data_segment_target_bytes: DEFAULT_DATA_SEGMENT_TARGET_BYTES,
        }
    }
}

impl CreateOptions {
    pub fn new(data_segment_target_bytes: u64) -> Result<Self, Error> {
        FileDataLayout::whole_identity(data_segment_target_bytes)?;
        Ok(Self {
            data_segment_target_bytes,
        })
    }

    pub fn file_data_layout(&self) -> Result<FileDataLayout, Error> {
        FileDataLayout::whole_identity(self.data_segment_target_bytes)
    }
}
