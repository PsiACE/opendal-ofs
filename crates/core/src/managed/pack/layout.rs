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

use super::super::object::{GcEpoch, ObjectClass, ObjectId, ObjectLocator};

pub(super) const MAGIC: [u8; 8] = *b"OFSPAK01";
pub(crate) const ENTRY_BYTES: u64 = 8 + 8 + 32;
pub(crate) const TRAILER_BYTES: u64 = 8 + 2 + 8 + 8 + 32 + 32;

/// Exact physical range start for one file stored in a Pack.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EntryRef {
    gc_epoch: GcEpoch,
    object_id: ObjectId,
    offset: u64,
}

super::super::wire::tuple_wire!(EntryRef {
    gc_epoch: GcEpoch,
    object_id: ObjectId,
    offset: u64,
});

impl EntryRef {
    pub(crate) const fn new(locator: ObjectLocator, offset: u64) -> Self {
        Self {
            gc_epoch: locator.gc_epoch,
            object_id: locator.id,
            offset,
        }
    }

    pub(crate) const fn locator(self) -> ObjectLocator {
        ObjectLocator {
            gc_epoch: self.gc_epoch,
            class: ObjectClass::FilePack,
            id: self.object_id,
        }
    }

    pub(crate) const fn offset(self) -> u64 {
        self.offset
    }
}
