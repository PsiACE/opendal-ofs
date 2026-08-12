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

use std::fmt;
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

macro_rules! fixed_identity {
    ($(#[$meta:meta])* $name:ident, $length:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name([u8; $length]);

        impl $name {
            pub const fn from_bytes(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }
        }
    };
}

fixed_identity!(
    /// Stable identity of one Managed volume.
    VolumeId,
    16
);
fixed_identity!(
    /// Stable identity of one filesystem node.
    NodeId,
    16
);
fixed_identity!(
    /// Content identity used by immutable Managed records and file data.
    Digest,
    32
);
fixed_identity!(
    /// Stable identity of one immutable logical file version.
    FileVersionId,
    16
);
fixed_identity!(
    /// Integrity value used to verify one independently readable record.
    Checksum,
    32
);
fixed_identity!(
    /// Idempotency identity of one publication attempt.
    OperationId,
    16
);

macro_rules! generated_identity {
    ($($name:ident),+ $(,)?) => {
        $(
            impl $name {
                pub fn generate() -> Self {
                    Self::from_bytes(*uuid::Uuid::new_v4().as_bytes())
                }
            }
        )+
    };
}

generated_identity!(VolumeId, NodeId, FileVersionId, OperationId);

/// Content fingerprint used by Sync to compare a materialized file with a
/// logical file version. It is not the file version identity or its layout.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileFingerprint {
    digest: Digest,
    logical_length: u64,
}

impl FileFingerprint {
    pub const fn new(digest: Digest, logical_length: u64) -> Self {
        Self {
            digest,
            logical_length,
        }
    }

    pub const fn digest(self) -> Digest {
        self.digest
    }

    pub const fn logical_length(self) -> u64 {
        self.logical_length
    }
}

macro_rules! display_identity {
    ($($name:ident),+ $(,)?) => {
        $(
            impl fmt::Display for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    for byte in self.as_bytes() {
                        write!(formatter, "{byte:02x}")?;
                    }
                    Ok(())
                }
            }
        )+
    };
}

display_identity!(VolumeId, FileVersionId, OperationId);

/// A position in a Managed volume's ordered change stream.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeCursor {
    Genesis,
    At {
        sequence: NonZeroU64,
        operation: OperationId,
    },
}

impl ChangeCursor {
    pub const fn at(sequence: NonZeroU64, operation: OperationId) -> Self {
        Self::At {
            sequence,
            operation,
        }
    }

    pub const fn sequence(self) -> u64 {
        match self {
            Self::Genesis => 0,
            Self::At { sequence, .. } => sequence.get(),
        }
    }

    pub const fn operation(self) -> Option<OperationId> {
        match self {
            Self::Genesis => None,
            Self::At { operation, .. } => Some(operation),
        }
    }
}
