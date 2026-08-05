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

//! Pure namespace reconciliation for Managed Sync.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::{Cursor, Manifest, NamespaceChange, Node, NodeKind};
use crate::replica::{Conflict, ConflictKind};

pub(crate) fn diff(parent: &Manifest, target: &Manifest) -> Vec<NamespaceChange> {
    let mut changes = Vec::new();
    for (path, node) in &parent.entries {
        if !target.entries.contains_key(path) {
            changes.push(NamespaceChange::Remove {
                path: path.clone(),
                removed: node.id.clone(),
            });
        }
    }
    for (path, node) in &target.entries {
        let previous = parent.entries.get(path);
        if previous != Some(node) {
            changes.push(NamespaceChange::Put {
                path: path.clone(),
                node: node.clone(),
                replaces: previous.map(|value| value.id.clone()),
            });
        }
    }
    changes
}

pub(crate) fn merge(
    base: &Manifest,
    local: &Manifest,
    remote: &Manifest,
    cursor: &Cursor,
    resolved: &BTreeSet<String>,
) -> (Manifest, Vec<Conflict>) {
    let base_identities = identities(base);
    let local_identities = identities(local);
    let remote_identities = identities(remote);
    let mut identity_choices = BTreeMap::<String, Option<Node>>::new();
    let mut conflicts = Vec::new();
    for (id, (base_path, base_node)) in &base_identities {
        let local_value = local_identities.get(id);
        let remote_value = remote_identities.get(id);
        let local_path = local_value.map(|(path, _)| path.as_str());
        let remote_path = remote_value.map(|(path, _)| path.as_str());
        let local_relocated = local_path != Some(base_path.as_str());
        let remote_relocated = remote_path != Some(base_path.as_str());
        let divergent_location = local_relocated && remote_relocated && local_path != remote_path;
        let rename_vs_edit = local_path.is_some()
            && local_relocated
            && remote_path == Some(base_path.as_str())
            && remote_value.is_some_and(|(_, node)| node != base_node)
            || remote_path.is_some()
                && remote_relocated
                && local_path == Some(base_path.as_str())
                && local_value.is_some_and(|(_, node)| node != base_node);
        if !divergent_location && !rename_vs_edit {
            continue;
        }
        for path in [Some(base_path.as_str()), local_path, remote_path]
            .into_iter()
            .flatten()
        {
            identity_choices.insert(path.to_owned(), local.entries.get(path).cloned());
        }
        if !resolved.contains(base_path) {
            conflicts.push(Conflict {
                path: base_path.clone(),
                kind: ConflictKind::DivergentRename,
                base: Some(base_node.clone()),
                local: local_value.map(|(_, node)| node.clone()),
                remote: remote_value.map(|(_, node)| node.clone()),
                remote_cursor: cursor.clone(),
            });
        }
    }
    let paths = base
        .entries
        .keys()
        .chain(local.entries.keys())
        .chain(remote.entries.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut entries = BTreeMap::new();
    for path in paths {
        let base_node = base.entries.get(&path);
        let local_node = local.entries.get(&path);
        let remote_node = remote.entries.get(&path);
        let selected = if let Some(node) = identity_choices.get(&path) {
            node.as_ref()
        } else if resolved.contains(&path) {
            local_node
        } else if local_node == base_node {
            remote_node
        } else if remote_node == base_node || local_node == remote_node {
            local_node
        } else if independently_created_directories(
            base_node,
            local_node,
            remote_node,
            &base_identities,
        ) {
            // Directory identities are internal bookkeeping. When both replicas
            // created the same previously absent path, retain the authority's
            // identity and merge their children path by path.
            remote_node
        } else {
            conflicts.push(Conflict {
                path: path.clone(),
                kind: conflict_kind(base_node, local_node, remote_node),
                base: base_node.cloned(),
                local: local_node.cloned(),
                remote: remote_node.cloned(),
                remote_cursor: cursor.clone(),
            });
            local_node
        };
        if let Some(node) = selected {
            entries.insert(path, node.clone());
        }
    }
    (Manifest { entries }, conflicts)
}

fn independently_created_directories(
    base: Option<&Node>,
    local: Option<&Node>,
    remote: Option<&Node>,
    base_identities: &BTreeMap<crate::model::NodeId, (String, Node)>,
) -> bool {
    let (Some(local), Some(remote)) = (local, remote) else {
        return false;
    };
    base.is_none()
        && matches!(local.kind, NodeKind::Directory)
        && matches!(remote.kind, NodeKind::Directory)
        && local.id != remote.id
        && !base_identities.contains_key(&local.id)
        && !base_identities.contains_key(&remote.id)
}

fn identities(manifest: &Manifest) -> BTreeMap<crate::model::NodeId, (String, Node)> {
    manifest
        .entries
        .iter()
        .map(|(path, node)| (node.id.clone(), (path.clone(), node.clone())))
        .collect()
}

fn conflict_kind(base: Option<&Node>, local: Option<&Node>, remote: Option<&Node>) -> ConflictKind {
    match (base, local, remote) {
        (Some(_), None, Some(_)) | (Some(_), Some(_), None) => ConflictKind::DeleteVsModify,
        (_, Some(a), Some(b))
            if std::mem::discriminant(&a.kind) != std::mem::discriminant(&b.kind) =>
        {
            ConflictKind::IncompatibleTypeReplacement
        }
        (_, Some(a), Some(b)) if a.id == b.id => ConflictKind::SameNodeModified,
        _ => ConflictKind::DivergentRename,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentRef, NodeId, OperationId};

    fn directory(id: &str) -> Node {
        Node {
            id: NodeId::parse(id).unwrap(),
            kind: NodeKind::Directory,
        }
    }

    fn file(id: &str, digest_byte: char) -> Node {
        let sha256 = digest_byte.to_string().repeat(64);
        Node {
            id: NodeId::parse(id).unwrap(),
            kind: NodeKind::File {
                content: ContentRef { sha256, size: 1 },
                executable: false,
            },
        }
    }

    fn manifest(entries: Vec<(&str, Node)>) -> Manifest {
        Manifest {
            entries: entries
                .into_iter()
                .map(|(path, node)| (path.to_owned(), node))
                .collect(),
        }
    }

    fn cursor() -> Cursor {
        Cursor {
            generation: 1,
            operation: OperationId::parse("test-operation").unwrap(),
        }
    }

    #[test]
    fn established_empty_replicas_coalesce_nested_directory_creation() {
        let base = Manifest::default();
        let local = manifest(vec![
            (".agents", directory("local-agents")),
            (".agents/skills", directory("local-skills")),
            (".agents/skills/a.md", file("local-file", 'a')),
        ]);
        let remote = manifest(vec![
            (".agents", directory("remote-agents")),
            (".agents/skills", directory("remote-skills")),
            (".agents/skills/b.md", file("remote-file", 'b')),
        ]);

        let (target, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert!(conflicts.is_empty());
        assert_eq!(target.entries[".agents"].id, remote.entries[".agents"].id);
        assert_eq!(
            target.entries[".agents/skills"].id,
            remote.entries[".agents/skills"].id
        );
        assert!(target.entries.contains_key(".agents/skills/a.md"));
        assert!(target.entries.contains_key(".agents/skills/b.md"));
        target.validate().unwrap();
        let changes = diff(&remote, &target);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            NamespaceChange::Put { path, replaces: None, .. }
                if path == ".agents/skills/a.md"
        ));
    }

    #[test]
    fn upgrade_replicas_coalesce_one_new_public_directory() {
        let agents = directory("shared-agents");
        let base = manifest(vec![(".agents", agents.clone())]);
        let local = manifest(vec![
            (".agents", agents.clone()),
            (".agents/memory", directory("local-memory")),
            (".agents/memory/a.md", file("local-memory-file", 'a')),
        ]);
        let remote = manifest(vec![
            (".agents", agents),
            (".agents/memory", directory("remote-memory")),
            (".agents/memory/b.md", file("remote-memory-file", 'b')),
        ]);

        let (target, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert!(conflicts.is_empty());
        assert_eq!(
            target.entries[".agents/memory"].id,
            remote.entries[".agents/memory"].id
        );
        assert!(target.entries.contains_key(".agents/memory/a.md"));
        assert!(target.entries.contains_key(".agents/memory/b.md"));
        target.validate().unwrap();
    }

    #[test]
    fn replicas_coalesce_a_public_directory_recreated_after_deletion() {
        let agents = directory("shared-agents");
        let skills = directory("shared-skills");
        let base = manifest(vec![
            (".agents", agents.clone()),
            (".agents/skills", skills.clone()),
        ]);
        let local = manifest(vec![
            (".agents", agents.clone()),
            (".agents/skills", skills.clone()),
            (".agents/skills/shared", directory("local-recreated-shared")),
            (
                ".agents/skills/shared/a.md",
                file("local-recreated-file", 'a'),
            ),
        ]);
        let remote = manifest(vec![
            (".agents", agents),
            (".agents/skills", skills),
            (
                ".agents/skills/shared",
                directory("remote-recreated-shared"),
            ),
            (
                ".agents/skills/shared/b.md",
                file("remote-recreated-file", 'b'),
            ),
        ]);

        let (target, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert!(conflicts.is_empty());
        assert_eq!(
            target.entries[".agents/skills/shared"].id,
            remote.entries[".agents/skills/shared"].id
        );
        assert!(target.entries.contains_key(".agents/skills/shared/a.md"));
        assert!(target.entries.contains_key(".agents/skills/shared/b.md"));
        target.validate().unwrap();
    }

    #[test]
    fn a_rename_and_an_unrelated_new_directory_do_not_coalesce() {
        let old = directory("existing-directory");
        let base = manifest(vec![("old", old.clone())]);
        let local = manifest(vec![("shared", old.clone())]);
        let remote = manifest(vec![("old", old), ("shared", directory("new-directory"))]);

        let (_, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, "shared");
        assert_eq!(conflicts[0].kind, ConflictKind::DivergentRename);
    }

    #[test]
    fn coalesced_directories_do_not_hide_same_file_conflicts() {
        let base = Manifest::default();
        let local = manifest(vec![
            (".agents", directory("local-agents")),
            (".agents/config.toml", file("local-config", 'a')),
        ]);
        let remote = manifest(vec![
            (".agents", directory("remote-agents")),
            (".agents/config.toml", file("remote-config", 'b')),
        ]);

        let (_, conflicts) = merge(&base, &local, &remote, &cursor(), &BTreeSet::new());

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, ".agents/config.toml");
        assert_eq!(conflicts[0].kind, ConflictKind::DivergentRename);
    }
}
