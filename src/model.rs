// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Provider-neutral Managed Volume records.

use std::collections::BTreeMap;
use std::fmt;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

const MANAGED_FORMAT: &str = "ofs-managed-volume";
const RECORD_VERSION: u32 = 1;

macro_rules! identifier {
    ($name:ident) => {
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub(crate) struct $name(String);

        impl $name {
            pub(crate) fn parse(value: impl Into<String>) -> Result<Self> {
                let value = Self(value.into());
                value.validate()?;
                Ok(value)
            }

            fn validate(&self) -> Result<()> {
                let value = self.as_str();
                if value.is_empty()
                    || value.len() > 128
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
                {
                    bail!("invalid {}", stringify!($name));
                }
                Ok(())
            }

            pub(crate) fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

identifier!(VolumeId);
identifier!(NodeId);
identifier!(OperationId);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cursor {
    pub(crate) generation: u64,
    pub(crate) operation: OperationId,
}

impl Cursor {
    pub(crate) fn initial() -> Self {
        Self {
            generation: 0,
            operation: OperationId("initial".to_owned()),
        }
    }

    fn validate(&self) -> Result<()> {
        self.operation.validate()?;
        if (self.generation == 0) != (self.operation.as_str() == "initial") {
            bail!("generation zero and the initial operation must identify the same cursor");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MetadataPlacement {
    ColocatedObject,
    ExternalD1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContentRef {
    /// Opaque Data Store reference. Sync Access never interprets it.
    pub(crate) data_ref: String,
    pub(crate) sha256: String,
    pub(crate) size: u64,
}

impl ContentRef {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.data_ref.is_empty()
            || self.sha256.len() != 64
            || !self.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("invalid immutable content reference");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NodeKind {
    Directory,
    File {
        content: ContentRef,
        executable: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Node {
    pub(crate) id: NodeId,
    pub(crate) kind: NodeKind,
}

impl Node {
    fn validate(&self) -> Result<()> {
        self.id.validate()?;
        if let NodeKind::File { content, .. } = &self.kind {
            content.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    /// Portable relative paths. The root directory is implicit.
    pub(crate) entries: BTreeMap<String, Node>,
}

impl Manifest {
    pub(crate) fn validate(&self) -> Result<()> {
        for (path, node) in &self.entries {
            validate_path(path)?;
            node.validate()?;
        }
        Ok(())
    }

    pub(crate) fn apply(&self, changes: &[NamespaceChange]) -> Result<Self> {
        let mut next = self.clone();
        for change in changes {
            match change {
                NamespaceChange::Put {
                    path,
                    node,
                    replaces,
                } => {
                    validate_path(path)?;
                    node.validate()?;
                    let current = next.entries.get(path).map(|item| &item.id);
                    if current != replaces.as_ref() {
                        bail!("change precondition for {path:?} does not match its parent");
                    }
                    next.entries.insert(path.clone(), node.clone());
                }
                NamespaceChange::Remove { path, removed } => {
                    validate_path(path)?;
                    if next.entries.get(path).map(|item| &item.id) != Some(removed) {
                        bail!("removal precondition for {path:?} does not match its parent");
                    }
                    next.entries.remove(path);
                }
            }
        }
        next.validate()?;
        Ok(next)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NamespaceChange {
    Put {
        path: String,
        node: Node,
        /// Required node identity for replacement; absent means create.
        replaces: Option<NodeId>,
    },
    Remove {
        path: String,
        removed: NodeId,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FormatRecord {
    format: String,
    format_version: u32,
    pub(crate) volume_id: VolumeId,
    pub(crate) placement: MetadataPlacement,
    pub(crate) data_store_id: String,
}

impl FormatRecord {
    pub(crate) fn new(
        volume_id: VolumeId,
        placement: MetadataPlacement,
        data_store_id: String,
    ) -> Result<Self> {
        if data_store_id.is_empty() {
            bail!("data store identity is empty");
        }
        Ok(Self {
            format: MANAGED_FORMAT.to_owned(),
            format_version: RECORD_VERSION,
            volume_id,
            placement,
            data_store_id,
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.format != MANAGED_FORMAT || self.format_version != RECORD_VERSION {
            bail!("unsupported Managed Volume format or version");
        }
        self.volume_id.validate()?;
        if self.data_store_id.is_empty() {
            bail!("data store identity is empty");
        }
        Ok(())
    }

    pub(crate) fn same_storage(&self, other: &Self) -> bool {
        self.placement == other.placement && self.data_store_id == other.data_store_id
    }

    pub(crate) fn placement(&self) -> MetadataPlacement {
        self.placement
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HeadRecord {
    pub(crate) format: String,
    pub(crate) format_version: u32,
    pub(crate) volume_id: VolumeId,
    pub(crate) cursor: Cursor,
    pub(crate) checkpoint: Cursor,
}

impl HeadRecord {
    pub(crate) fn initial(volume_id: VolumeId) -> Self {
        Self {
            format: MANAGED_FORMAT.to_owned(),
            format_version: RECORD_VERSION,
            volume_id,
            cursor: Cursor::initial(),
            checkpoint: Cursor::initial(),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_record(&self.format, self.format_version)?;
        self.volume_id.validate()?;
        self.cursor.validate()?;
        self.checkpoint.validate()?;
        if self.checkpoint.generation > self.cursor.generation {
            bail!("checkpoint is newer than the authority head");
        }
        Ok(())
    }

    pub(crate) fn advance(volume_id: VolumeId, cursor: Cursor, checkpoint: Cursor) -> Self {
        Self {
            format: MANAGED_FORMAT.to_owned(),
            format_version: RECORD_VERSION,
            volume_id,
            cursor,
            checkpoint,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitRecord {
    format: String,
    format_version: u32,
    pub(crate) volume_id: VolumeId,
    pub(crate) parent: Cursor,
    pub(crate) cursor: Cursor,
    pub(crate) changes: Vec<NamespaceChange>,
}

impl CommitRecord {
    pub(crate) fn new(
        volume_id: VolumeId,
        parent: Cursor,
        operation: OperationId,
        changes: Vec<NamespaceChange>,
    ) -> Result<Self> {
        if changes.is_empty() {
            bail!("a Managed publication cannot contain an empty change set");
        }
        let generation = parent
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("generation overflow"))?;
        Ok(Self {
            format: MANAGED_FORMAT.to_owned(),
            format_version: RECORD_VERSION,
            volume_id,
            parent,
            cursor: Cursor {
                generation,
                operation,
            },
            changes,
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_record(&self.format, self.format_version)?;
        self.volume_id.validate()?;
        self.parent.validate()?;
        self.cursor.validate()?;
        if self.changes.is_empty()
            || self.parent.generation.checked_add(1) != Some(self.cursor.generation)
        {
            bail!("invalid Managed change commit");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointRecord {
    format: String,
    format_version: u32,
    pub(crate) volume_id: VolumeId,
    pub(crate) cursor: Cursor,
    pub(crate) manifest: Manifest,
}

impl CheckpointRecord {
    pub(crate) fn new(volume_id: VolumeId, cursor: Cursor, manifest: Manifest) -> Result<Self> {
        manifest.validate()?;
        Ok(Self {
            format: MANAGED_FORMAT.to_owned(),
            format_version: RECORD_VERSION,
            volume_id,
            cursor,
            manifest,
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_record(&self.format, self.format_version)?;
        self.volume_id.validate()?;
        self.cursor.validate()?;
        self.manifest.validate()
    }
}

fn validate_record(format: &str, version: u32) -> Result<()> {
    if format != MANAGED_FORMAT || version != RECORD_VERSION {
        bail!("unsupported Managed metadata record");
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        bail!("invalid Managed namespace path {path:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_set_replays_only_on_its_parent() {
        let node = Node {
            id: NodeId::parse("node-1").unwrap(),
            kind: NodeKind::Directory,
        };
        let change = NamespaceChange::Put {
            path: "skills".to_owned(),
            node: node.clone(),
            replaces: None,
        };
        let next = Manifest::default()
            .apply(std::slice::from_ref(&change))
            .unwrap();
        assert_eq!(next.entries.get("skills"), Some(&node));
        assert!(next.apply(&[change]).is_err());
    }

    #[test]
    fn strict_record_rejects_unknown_fields() {
        let encoded = r#"{
            "format":"ofs-managed-volume","format_version":1,
            "volume_id":"volume-1","cursor":{"generation":0,"operation":"initial"},
            "checkpoint":{"generation":0,"operation":"initial"},"legacy":true
        }"#;
        assert!(serde_json::from_str::<HeadRecord>(encoded).is_err());
    }
}
