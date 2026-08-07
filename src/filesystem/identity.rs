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

use std::num::NonZeroU64;

macro_rules! fixed_identity {
    ($(#[$meta:meta])* $name:ident, $length:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
    /// Stable identity of one configured volume.
    VolumeId,
    16
);
fixed_identity!(
    /// Identity of one filesystem node.
    ///
    /// Managed volumes preserve it across rename. Direct volumes may derive it
    /// from a path and report that it is not stable across namespace changes.
    NodeId,
    16
);
fixed_identity!(
    /// Identity of one immutable logical file version.
    FileVersionId,
    32
);
fixed_identity!(
    /// Idempotency identity of one publication attempt.
    OperationId,
    16
);

/// An opaque optimistic-concurrency token owned by a volume implementation.
///
/// Callers may retain and compare the token, but must not infer ordering from
/// its bytes. A Managed volume may encode a local counter while a Direct
/// volume may use a storage version or ETag.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Generation(Box<[u8]>);

impl Generation {
    pub fn from_bytes(bytes: impl Into<Box<[u8]>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A position in a volume's ordered change stream.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
