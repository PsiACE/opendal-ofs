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

//! Correlate unique file fingerprints without materializing the namespace.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::{
    FileFingerprint, FileVersionId, NamespaceNode, NamespaceRecord, NamespaceValue, NodeAttributes,
    NodeId,
};
use crate::managed::FileDataRef;
use crate::workset;

use super::local_scan::LocalRecord;
use super::scan::next_generation;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalCandidate {
    path: String,
    executable: bool,
    fingerprint: FileFingerprint,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BaseCandidate {
    path: String,
    fingerprint: FileFingerprint,
    node: NamespaceNode<FileDataRef>,
}

pub(super) struct RenameCandidates {
    local: workset::SpoolWriter<LocalCandidate>,
    base: workset::SpoolWriter<BaseCandidate>,
}

impl RenameCandidates {
    pub(super) fn new(workspace: &workset::Workspace) -> Result<Self, Error> {
        Ok(Self {
            local: workspace.writer("local-rename-candidates")?,
            base: workspace.writer("base-rename-candidates")?,
        })
    }

    pub(super) fn local(&mut self, local: LocalRecord) -> Result<(), Error> {
        self.local.write(&LocalCandidate {
            path: local.path,
            executable: local.executable,
            fingerprint: local
                .fingerprint
                .expect("a local regular file has a fingerprint"),
        })
    }

    pub(super) fn removed_record(
        &mut self,
        record: NamespaceRecord<FileDataRef>,
    ) -> Result<(), Error> {
        let node = record
            .value
            .ok_or_else(|| Error::corrupt("scan replica", "base namespace contains a deletion"))?;
        self.removed_node(record.path, node)
    }

    pub(super) fn removed_node(
        &mut self,
        path: String,
        node: NamespaceNode<FileDataRef>,
    ) -> Result<(), Error> {
        let Some((_, fingerprint, _)) = node.file() else {
            return Ok(());
        };
        self.base.write(&BaseCandidate {
            path,
            fingerprint,
            node,
        })
    }

    pub(super) fn resolve(
        self,
        workspace: &workset::Workspace,
    ) -> Result<workset::Spool<NamespaceRecord<Option<FileDataRef>>>, Error> {
        let local = workset::sort(
            workspace,
            &self.local.finish()?,
            |candidate: &LocalCandidate| (candidate.fingerprint, candidate.path.clone()),
        )?;
        let base = workset::sort(
            workspace,
            &self.base.finish()?,
            |candidate: &BaseCandidate| (candidate.fingerprint, candidate.path.clone()),
        )?;
        let mut output = workspace.writer("resolved-renames")?;
        resolve_file_renames(&local, &base, &mut output)?;
        workset::sort(
            workspace,
            &output.finish()?,
            |record: &NamespaceRecord<Option<FileDataRef>>| record.path.clone(),
        )
    }
}

fn resolve_file_renames(
    local: &workset::Spool<LocalCandidate>,
    base: &workset::Spool<BaseCandidate>,
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<FileDataRef>>>,
) -> Result<(), Error> {
    let mut local_reader = local.reader()?;
    let mut base_reader = base.reader()?;
    let mut local_head = local_reader.next()?;
    let mut base_head = base_reader.next()?;
    while local_head.is_some() || base_head.is_some() {
        let ordering = match (&local_head, &base_head) {
            (Some(local), Some(base)) => local.fingerprint.cmp(&base.fingerprint),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => break,
        };
        match ordering {
            Ordering::Less => {
                let fingerprint = local_head
                    .as_ref()
                    .expect("local rename candidate exists")
                    .fingerprint;
                if let Some(local) =
                    take_local_group(fingerprint, &mut local_reader, &mut local_head, output)?
                {
                    write_new_file(output, local)?;
                }
            }
            Ordering::Greater => {
                let fingerprint = base_head
                    .as_ref()
                    .expect("base rename candidate exists")
                    .fingerprint;
                take_base_group(fingerprint, &mut base_reader, &mut base_head)?;
            }
            Ordering::Equal => {
                let fingerprint = local_head
                    .as_ref()
                    .expect("local rename candidate exists")
                    .fingerprint;
                let local =
                    take_local_group(fingerprint, &mut local_reader, &mut local_head, output)?;
                let base = take_base_group(fingerprint, &mut base_reader, &mut base_head)?;
                match (local, base) {
                    (Some(local), Some(base)) => write_renamed_file(output, local, base)?,
                    (Some(local), None) => write_new_file(output, local)?,
                    (None, _) => {}
                }
            }
        }
    }
    Ok(())
}

fn take_local_group(
    fingerprint: FileFingerprint,
    reader: &mut workset::SpoolReader<LocalCandidate>,
    head: &mut Option<LocalCandidate>,
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<FileDataRef>>>,
) -> Result<Option<LocalCandidate>, Error> {
    let first = head.take().expect("local rename candidate exists");
    *head = reader.next()?;
    if head
        .as_ref()
        .is_none_or(|candidate| candidate.fingerprint != fingerprint)
    {
        return Ok(Some(first));
    }
    write_new_file(output, first)?;
    while head
        .as_ref()
        .is_some_and(|candidate| candidate.fingerprint == fingerprint)
    {
        let candidate = head.take().expect("local rename candidate exists");
        write_new_file(output, candidate)?;
        *head = reader.next()?;
    }
    Ok(None)
}

fn take_base_group(
    fingerprint: FileFingerprint,
    reader: &mut workset::SpoolReader<BaseCandidate>,
    head: &mut Option<BaseCandidate>,
) -> Result<Option<BaseCandidate>, Error> {
    let first = head.take().expect("base rename candidate exists");
    *head = reader.next()?;
    if head
        .as_ref()
        .is_none_or(|candidate| candidate.fingerprint != fingerprint)
    {
        return Ok(Some(first));
    }
    while head
        .as_ref()
        .is_some_and(|candidate| candidate.fingerprint == fingerprint)
    {
        *head = reader.next()?;
    }
    Ok(None)
}

fn write_new_file(
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<FileDataRef>>>,
    local: LocalCandidate,
) -> Result<(), Error> {
    output.write(&NamespaceRecord {
        path: local.path,
        value: Some(NamespaceNode {
            node_id: NodeId::generate(),
            generation: 1,
            attributes: NodeAttributes {
                executable: local.executable,
            },
            value: NamespaceValue::RegularFile {
                version: FileVersionId::generate(),
                fingerprint: local.fingerprint,
                content: None,
            },
        }),
    })
}

fn write_renamed_file(
    output: &mut workset::SpoolWriter<NamespaceRecord<Option<FileDataRef>>>,
    local: LocalCandidate,
    base: BaseCandidate,
) -> Result<(), Error> {
    let BaseCandidate { node, .. } = base;
    let NamespaceValue::RegularFile {
        version,
        fingerprint,
        content,
    } = node.value
    else {
        return Err(Error::corrupt(
            "scan replica",
            "rename candidate is not a regular file",
        ));
    };
    let attributes = NodeAttributes {
        executable: local.executable,
    };
    let generation = if attributes == node.attributes {
        node.generation
    } else {
        next_generation(node.generation)?
    };
    output.write(&NamespaceRecord {
        path: local.path,
        value: Some(NamespaceNode {
            node_id: node.node_id,
            generation,
            attributes,
            value: NamespaceValue::RegularFile {
                version,
                fingerprint,
                content: Some(content),
            },
        }),
    })
}
