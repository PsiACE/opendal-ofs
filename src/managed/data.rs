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

use crate::Error;
use crate::filesystem::{Digest, FileFingerprint};

use super::object::{GcEpoch, ObjectClass, ObjectId, ObjectLocator, ObjectRef};
use super::stream::{STREAM_TAIL_BYTES, StreamKind, StreamRef};

/// Minimal durable reference carried by a regular-file namespace entry.
///
/// Its enclosing namespace record supplies the logical length and payload
/// digest. The field context fixes the object class and stream kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FileDataRef {
    gc_epoch: GcEpoch,
    object_id: ObjectId,
    object_digest: Digest,
}

super::wire::tuple_wire!(FileDataRef {
    gc_epoch: GcEpoch,
    object_id: ObjectId,
    object_digest: Digest,
});

impl FileDataRef {
    pub(super) fn from_stream(
        reference: StreamRef,
        fingerprint: FileFingerprint,
    ) -> Result<Self, Error> {
        if reference
            .require(StreamKind::FILE_BYTES, ObjectClass::FileData)
            .is_err()
            || reference.payload_length != fingerprint.logical_length()
            || reference.payload_digest != fingerprint.digest()
        {
            return Err(Error::corrupt(
                "publish Managed file",
                "file data does not match its fingerprint",
            ));
        }
        Ok(Self {
            gc_epoch: reference.object.locator.gc_epoch,
            object_id: reference.object.locator.id,
            object_digest: reference.object.digest,
        })
    }

    pub(crate) fn stream_ref(self, fingerprint: FileFingerprint) -> Result<StreamRef, Error> {
        let payload_length = fingerprint.logical_length();
        let encoded_length = payload_length
            .checked_add(STREAM_TAIL_BYTES as u64)
            .ok_or_else(|| Error::corrupt("read Managed file", "file length overflows"))?;
        Ok(StreamRef {
            kind: StreamKind::FILE_BYTES,
            object: ObjectRef {
                locator: ObjectLocator {
                    gc_epoch: self.gc_epoch,
                    class: ObjectClass::FileData,
                    id: self.object_id,
                },
                encoded_length,
                digest: self.object_digest,
            },
            payload_length,
            payload_digest: fingerprint.digest(),
        })
    }
}
