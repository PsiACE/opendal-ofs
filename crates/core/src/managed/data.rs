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

//! The one ordered-extent representation used by every file placement.

use crate::Error;
use crate::filesystem::ContentRef;

use super::extension::{ExtentRef, SegmentRangeRef};
use super::object::ObjectClass;
use super::stream::{StreamKind, StreamRef};

/// Durable reference to one logical file's ordered extent stream.
///
/// The first extent is inline so whole files and packed small files do not
/// require a metadata request per file. Files with more extents continue in a
/// self-delimiting record stream. Both fields are projections of the same
/// ordered sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileDataRef {
    pub(super) first: Option<ExtentRef>,
    pub(super) tail: Option<StreamRef>,
}

super::wire::tuple_wire!(FileDataRef {
    first: Option<ExtentRef>,
    tail: Option<StreamRef>,
});

impl FileDataRef {
    /// Reference the empty byte sequence without creating a data object.
    pub const fn empty() -> Self {
        Self {
            first: None,
            tail: None,
        }
    }

    /// Reference one inline extent.
    pub const fn single(extent: ExtentRef) -> Self {
        Self {
            first: Some(extent),
            tail: None,
        }
    }

    /// Reference an ordered extent sequence with its first item inline.
    pub fn with_tail(first: ExtentRef, tail: StreamRef) -> Result<Self, Error> {
        tail.require(StreamKind::FILE_EXTENTS, ObjectClass::FileExtentSegment)?;
        Ok(Self {
            first: Some(first),
            tail: Some(tail),
        })
    }

    pub(crate) fn validate(&self, content: ContentRef, decoding_count: usize) -> Result<(), Error> {
        match (&self.first, self.tail) {
            (None, None) if content.length() == 0 => Ok(()),
            (Some(first), tail) if content.length() != 0 => {
                if first.decoded.len() != decoding_count
                    || first.content().length() == 0
                    || first.content().length() > content.length()
                {
                    return Err(Error::corrupt(
                        "read Managed file",
                        "first extent length is invalid",
                    ));
                }
                if tail.is_none() && first.content() != content {
                    return Err(Error::corrupt(
                        "read Managed file",
                        "single extent does not match the file content reference",
                    ));
                }
                first
                    .range
                    .offset
                    .checked_add(first.range.stored.length())
                    .ok_or_else(|| {
                        Error::corrupt("read Managed file", "stored extent range overflows")
                    })?;
                if let Some(tail) = tail {
                    tail.require(StreamKind::FILE_EXTENTS, ObjectClass::FileExtentSegment)?;
                }
                Ok(())
            }
            _ => Err(Error::corrupt(
                "read Managed file",
                "empty and non-empty file references do not match",
            )),
        }
    }

    pub(crate) const fn tail(&self) -> Option<StreamRef> {
        self.tail
    }

    pub(crate) fn inline_extent(&self) -> Option<ExtentRef> {
        self.first.clone()
    }

    pub(crate) fn identity_range(&self) -> Option<SegmentRangeRef> {
        self.tail
            .is_none()
            .then_some(self.first.as_ref())
            .flatten()
            .filter(|extent| extent.decoded.is_empty())
            .map(|extent| extent.range)
    }
}
