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

use serde::{Deserialize, Serialize};
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use crate::Error;

use super::{ChangeCursor, FileFingerprint, FileVersionId, NodeAttributes, NodeId, NodeKind};

/// One path-ordered row in a Managed namespace stream.
///
/// `content` is supplied by the volume implementation. A durable Managed
/// namespace uses its immutable content reference; a local scan uses `()`
/// until publication attaches that reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceRecord<C> {
    pub path: String,
    pub change_cursor: ChangeCursor,
    pub value: Option<NamespaceNode<C>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceNode<C> {
    pub node_id: NodeId,
    pub generation: u64,
    pub attributes: NodeAttributes,
    pub value: NamespaceValue<C>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum NamespaceValue<C> {
    Directory {
        generation: u64,
    },
    RegularFile {
        version: FileVersionId,
        fingerprint: FileFingerprint,
        content: C,
    },
}

impl<C> NamespaceNode<C> {
    pub const fn kind(&self) -> NodeKind {
        match self.value {
            NamespaceValue::Directory { .. } => NodeKind::Directory,
            NamespaceValue::RegularFile { .. } => NodeKind::RegularFile,
        }
    }

    pub const fn file(&self) -> Option<(FileVersionId, FileFingerprint, &C)> {
        match &self.value {
            NamespaceValue::RegularFile {
                version,
                fingerprint,
                content,
            } => Some((*version, *fingerprint, content)),
            NamespaceValue::Directory { .. } => None,
        }
    }

    pub fn map_content<D>(self, map: impl FnOnce(C) -> D) -> NamespaceNode<D> {
        NamespaceNode {
            node_id: self.node_id,
            generation: self.generation,
            attributes: self.attributes,
            value: match self.value {
                NamespaceValue::Directory { generation } => {
                    NamespaceValue::Directory { generation }
                }
                NamespaceValue::RegularFile {
                    version,
                    fingerprint,
                    content,
                } => NamespaceValue::RegularFile {
                    version,
                    fingerprint,
                    content: map(content),
                },
            },
        }
    }
}

impl<C> NamespaceRecord<C> {
    pub fn map_content<D>(self, map: impl FnOnce(C) -> D + Copy) -> NamespaceRecord<D> {
        NamespaceRecord {
            path: self.path,
            change_cursor: self.change_cursor,
            value: self.value.map(|value| value.map_content(map)),
        }
    }
}

/// Validate one canonical path without retaining the namespace.
pub fn validate_portable_path(path: &str) -> Result<(), Error> {
    if path.is_empty() {
        return Ok(());
    }
    if path.len() > MAX_PATH_BYTES
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains("//")
    {
        return Err(Error::invalid(
            "validate filesystem path",
            "path is not portable",
        ));
    }
    for name in path.split('/') {
        validate_portable_component(name)?;
    }
    Ok(())
}

fn validate_portable_component(name: &str) -> Result<(), Error> {
    if name.len() > MAX_COMPONENT_BYTES
        || name == "."
        || name == ".."
        || name.ends_with([' ', '.'])
        || name.chars().any(|character| {
            character.is_control()
                || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
        })
        || !name.nfc().eq(name.chars())
    {
        return Err(Error::invalid(
            "validate filesystem path",
            "path component is not portable",
        ));
    }
    let folded_name = name.case_fold().nfc().collect::<String>();
    let stem = folded_name.split('.').next().unwrap_or_default();
    if matches!(stem, "con" | "prn" | "aux" | "nul")
        || stem.len() == 4
            && (stem.starts_with("com") || stem.starts_with("lpt"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9')
        || matches!(stem, "com¹" | "com²" | "com³" | "lpt¹" | "lpt²" | "lpt³")
    {
        return Err(Error::invalid(
            "validate filesystem path",
            "path component is reserved",
        ));
    }
    Ok(())
}

const MAX_COMPONENT_BYTES: usize = 255;
const MAX_PATH_BYTES: usize = 4096;
