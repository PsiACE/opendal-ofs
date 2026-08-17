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

//! One file-data pipeline: partition, encode, place, restore.

mod codec;
mod extent;
mod partition;
pub(crate) mod read;
pub(crate) mod reuse;
mod segment;
pub(crate) mod write;

pub use codec::{ContentHasher, ExtentCodec, IdentityCodec};
pub use extent::ExtentRunWriter;
pub use partition::{FilePartitioner, WholePartitioner};
pub(crate) use read::{RangeBatch, RangeBatcher};
pub use read::{RangeReader, ReusableFile, ReusableFileSource};
pub use reuse::ContentReuseLookup;
pub use segment::DataSegmentWriter;

use crate::Error;
use crate::authority::AuthoritySelector;
use crate::authority::DefaultSelector;
use crate::format::FileRange;
use crate::format::{ExtentRef, FileExtentMap};

/// Compile-time family of partition, codec, and authority-selection ports.
pub trait VolumeAccess: Clone + Send + Sync + std::fmt::Debug + Unpin + 'static {
    type Partitioner: FilePartitioner;
    type Codec: ExtentCodec;
    type Selector: AuthoritySelector;

    fn partitioner(&self) -> &Self::Partitioner;
    fn codec(&self) -> &Self::Codec;
    fn selector(&self) -> &Self::Selector;

    fn decoding_count(&self) -> usize {
        self.codec().decoding_count()
    }

    fn stored_size_bound(&self, logical_bytes: u64) -> Option<u64> {
        self.codec().stored_size_bound(logical_bytes)
    }

    fn validate_extent(&self, reference: &ExtentRef) -> Result<(), Error> {
        self.codec().validate(reference)
    }
}

/// Whole-file identity access with the default authority selector.
#[derive(Clone, Debug, Default)]
pub struct CoreAccess {
    partitioner: WholePartitioner,
    codec: IdentityCodec,
    selector: DefaultSelector,
}

impl VolumeAccess for CoreAccess {
    type Partitioner = WholePartitioner;
    type Codec = IdentityCodec;
    type Selector = DefaultSelector;

    fn partitioner(&self) -> &Self::Partitioner {
        &self.partitioner
    }

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn selector(&self) -> &Self::Selector {
        &self.selector
    }
}

pub(crate) fn validate_file_map(
    data: &FileExtentMap,
    content: crate::filesystem::ContentRef,
    decoding_count: usize,
) -> Result<(), Error> {
    data.validate(content)?;
    if content.length() == 0 {
        return Ok(());
    }
    if data.patch_levels.is_empty()
        && let Some(mapping) = data.inline_file_extent()
        && (mapping.logical_range.offset != 0
            || mapping.extent_offset != 0
            || mapping.logical_range.length != content.length()
            || mapping.extent.content() != content)
    {
        return Err(Error::corrupt(
            "read Managed file",
            "single extent does not match the file content reference",
        ));
    }
    for run in data.runs() {
        if run.inline_extent.extent.decoding_outputs.len() != decoding_count {
            return Err(Error::corrupt(
                "read Managed file extent run",
                "extent decoding chain does not match the volume",
            ));
        }
    }
    Ok(())
}

pub(crate) fn file_range_end(range: FileRange) -> Result<u64, Error> {
    range.end()
}
