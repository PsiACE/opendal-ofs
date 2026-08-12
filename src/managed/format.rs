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
use crate::filesystem::{NodeId, VolumeId};

use super::record::Record;

pub(crate) const FORMAT_KEY: &str = "managed/1/format";
const MAX_FORMAT_BODY_BYTES: usize = 64 * 1024;

const FORMAT_RECORD: Record = Record::new(*b"OFSFMT01", MAX_FORMAT_BODY_BYTES);
pub(crate) const MAX_FORMAT_BYTES: usize = FORMAT_RECORD.maximum_encoded_bytes();

/// The sole Managed storage format understood by this build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManagedFormat {
    volume_id: VolumeId,
    root_node_id: NodeId,
}

impl ManagedFormat {
    pub(crate) const fn new(volume_id: VolumeId, root_node_id: NodeId) -> Self {
        Self {
            volume_id,
            root_node_id,
        }
    }

    pub(crate) const fn volume_id(self) -> VolumeId {
        self.volume_id
    }

    pub(crate) const fn root_node_id(self) -> NodeId {
        self.root_node_id
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, Error> {
        FORMAT_RECORD.encode(&VolumeFormat {
            volume_id: self.volume_id,
            root_node_id: self.root_node_id,
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let format: VolumeFormat = FORMAT_RECORD.decode(bytes)?;
        Ok(Self {
            volume_id: format.volume_id,
            root_node_id: format.root_node_id,
        })
    }
}

#[derive(Debug)]
struct VolumeFormat {
    volume_id: VolumeId,
    root_node_id: NodeId,
}
super::wire::tuple_wire!(VolumeFormat {
    volume_id: VolumeId,
    root_node_id: NodeId,
});
