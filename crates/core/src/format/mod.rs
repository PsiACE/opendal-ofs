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

//! Experimental Managed layout v0. The encoding is not a compatibility promise.

pub mod codec;
pub(crate) mod fixed;

mod commit;
mod file;
mod namespace;
mod object;
mod stream;
mod volume;

pub use codec::RecordCodec;
pub use commit::{COMMIT_RECORD, NamespaceCommit, NamespaceRevision, OperationReceipt};
pub use file::{ExtentMapping, ExtentRef, ExtentRunRef, FileExtentMap, FileRange, SegmentRangeRef};
pub use namespace::{NamespaceChangeSegment, NamespaceSnapshot, OperationReceiptSegment};
pub use object::{GcEpoch, ObjectClass, ObjectId, ObjectLocator, ObjectRef, checksum};
pub use stream::{
    STREAM_TAIL_BYTES, StreamKind, StreamRef, encode_stream_tail, validate_stream_tail,
};
pub use volume::{
    DEFAULT_DATA_SEGMENT_TARGET_BYTES, ExtensionDescriptor, ExtensionId, FORMAT_KEY, FORMAT_RECORD,
    FileDataLayout, VolumeFormat,
};
