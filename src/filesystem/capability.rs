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

/// A stable RFC 016 guarantee exposed by a volume and access combination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityName {
    StableNodeIdentity,
    ObjectScopedGenerations,
    AtomicNamespacePublication,
    DurableCommonBase,
    ExplicitConflictRetention,
}

impl CapabilityName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StableNodeIdentity => "stable_node_identity",
            Self::ObjectScopedGenerations => "object_scoped_generations",
            Self::AtomicNamespacePublication => "atomic_namespace_publication",
            Self::DurableCommonBase => "durable_common_base",
            Self::ExplicitConflictRetention => "explicit_conflict_retention",
        }
    }
}

/// A filesystem operation that the selected combination does not implement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LimitationName {
    HardLinks,
    SymbolicLinks,
    RandomWrite,
}

impl LimitationName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HardLinks => "hard_links",
            Self::SymbolicLinks => "symbolic_links",
            Self::RandomWrite => "random_write",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityGuarantee {
    pub name: CapabilityName,
    pub guarantee: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityLimitation {
    pub name: LimitationName,
    pub reason: &'static str,
}

/// Effective RFC 016 semantics for one selected volume and access model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capabilities {
    guarantees: &'static [CapabilityGuarantee],
    limitations: &'static [CapabilityLimitation],
}

impl Capabilities {
    /// Semantics currently provided by Managed volumes through Sync access.
    pub const fn managed_sync_v1() -> Self {
        Self {
            guarantees: &[
                CapabilityGuarantee {
                    name: CapabilityName::StableNodeIdentity,
                    guarantee: "NodeId is preserved when a known node is renamed.",
                },
                CapabilityGuarantee {
                    name: CapabilityName::ObjectScopedGenerations,
                    guarantee: "nodes and directories carry independent opaque generations",
                },
                CapabilityGuarantee {
                    name: CapabilityName::AtomicNamespacePublication,
                    guarantee: "one complete namespace publication becomes authoritative atomically",
                },
                CapabilityGuarantee {
                    name: CapabilityName::DurableCommonBase,
                    guarantee: "each replica durably records its last common change cursor",
                },
                CapabilityGuarantee {
                    name: CapabilityName::ExplicitConflictRetention,
                    guarantee: "concurrent local and remote candidates remain retained until explicit resolution",
                },
            ],
            limitations: &[
                CapabilityLimitation {
                    name: LimitationName::HardLinks,
                    reason: "hard-link identity is not represented; use independent regular files",
                },
                CapabilityLimitation {
                    name: LimitationName::SymbolicLinks,
                    reason: "symbolic links are rejected at the Sync boundary",
                },
                CapabilityLimitation {
                    name: LimitationName::RandomWrite,
                    reason: "Sync publishes complete immutable file versions, not range updates",
                },
            ],
        }
    }

    pub fn guarantees(self) -> impl Iterator<Item = &'static CapabilityGuarantee> {
        self.guarantees.iter()
    }

    pub fn limitations(self) -> impl Iterator<Item = &'static CapabilityLimitation> {
        self.limitations.iter()
    }
}
