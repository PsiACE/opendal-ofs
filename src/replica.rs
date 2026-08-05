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

//! Native tree and durable Sync Access state.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::model::{
    ContentRef, Cursor, Manifest, NamespaceChange, Node, NodeId, NodeKind, OperationId, VolumeId,
};

const STATE_FORMAT: &str = "ofs-sync-replica";
const STATE_VERSION: u32 = 1;
const STATE_FILE: &str = "state.json";
const LOCK_FILE: &str = "replica.lock";

#[derive(Clone, Debug)]
pub(crate) struct ReplicaPaths {
    pub(crate) local: PathBuf,
    pub(crate) state: PathBuf,
}

impl ReplicaPaths {
    pub(crate) fn resolve(local: &Path, state: Option<&Path>) -> Result<Self> {
        let local = std::fs::canonicalize(local)
            .with_context(|| format!("resolve local replica {}", local.display()))?;
        if !local.is_dir() {
            bail!("local replica is not a directory");
        }
        let requested = match state {
            Some(path) => absolute(path)?,
            None => {
                let parent = local
                    .parent()
                    .context("local replica has no sibling directory")?;
                let name = local
                    .file_name()
                    .context("local replica has no directory name")?;
                let mut sibling = OsString::from(".");
                sibling.push(name);
                sibling.push(".ofs-state");
                parent.join(sibling)
            }
        };
        let state = resolve_missing(&requested)?;
        if state.starts_with(&local) || local.starts_with(&state) {
            bail!("replica state must be outside and separate from the synchronized tree");
        }
        if state.exists() && !state.is_dir() {
            bail!("replica state path is not a directory");
        }
        Ok(Self { local, state })
    }

    pub(crate) fn staged(&self, sha256: &str) -> PathBuf {
        self.state.join("staging").join(sha256)
    }
}

pub(crate) struct ReplicaLock(File);

impl ReplicaLock {
    pub(crate) fn acquire(paths: &ReplicaPaths) -> Result<Self> {
        std::fs::create_dir_all(&paths.state)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(paths.state.join(LOCK_FILE))?;
        file.try_lock_exclusive()
            .context("another sync owns this local replica")?;
        Ok(Self(file))
    }
}

impl Drop for ReplicaLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommonBase {
    pub(crate) cursor: Cursor,
    pub(crate) manifest: Manifest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingPublication {
    pub(crate) operation: OperationId,
    pub(crate) parent: Cursor,
    pub(crate) target: Manifest,
    pub(crate) changes: Vec<NamespaceChange>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingMaterialization {
    pub(crate) target: Cursor,
    pub(crate) manifest: Manifest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConflictKind {
    SamePathModified,
    DeleteVsModify,
    Rename,
    TypeReplacement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Conflict {
    pub(crate) path: String,
    pub(crate) kind: ConflictKind,
    pub(crate) base: Option<Node>,
    pub(crate) local: Option<Node>,
    pub(crate) remote: Option<Node>,
    pub(crate) remote_cursor: Cursor,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicaState {
    format: String,
    format_version: u32,
    pub(crate) volume_id: VolumeId,
    pub(crate) local_root: PathBuf,
    pub(crate) common: Option<CommonBase>,
    pub(crate) publication: Option<PendingPublication>,
    pub(crate) materialization: Option<PendingMaterialization>,
    pub(crate) conflicts: Vec<Conflict>,
}

impl ReplicaState {
    pub(crate) fn new(volume_id: VolumeId, paths: &ReplicaPaths) -> Self {
        Self {
            format: STATE_FORMAT.to_owned(),
            format_version: STATE_VERSION,
            volume_id,
            local_root: paths.local.clone(),
            common: None,
            publication: None,
            materialization: None,
            conflicts: Vec::new(),
        }
    }

    pub(crate) fn load_or_new(volume_id: &VolumeId, paths: &ReplicaPaths) -> Result<Self> {
        let path = paths.state.join(STATE_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::new(volume_id.clone(), paths));
            }
            Err(error) => return Err(error).context("read replica state"),
        };
        let state: Self = serde_json::from_slice(&bytes).context("decode replica state")?;
        state.validate(volume_id, paths)?;
        Ok(state)
    }

    pub(crate) fn save(&self, paths: &ReplicaPaths) -> Result<()> {
        self.validate(&self.volume_id, paths)?;
        std::fs::create_dir_all(&paths.state)?;
        let mut temporary = tempfile::NamedTempFile::new_in(&paths.state)?;
        serde_json::to_writer(temporary.as_file_mut(), self)?;
        temporary.write_all(b"\n")?;
        temporary.as_file_mut().sync_all()?;
        temporary
            .persist(paths.state.join(STATE_FILE))
            .map_err(|error| error.error)?;
        Ok(())
    }

    fn validate(&self, volume_id: &VolumeId, paths: &ReplicaPaths) -> Result<()> {
        if self.format != STATE_FORMAT || self.format_version != STATE_VERSION {
            bail!("unsupported replica state format or version");
        }
        if &self.volume_id != volume_id || self.local_root != paths.local {
            bail!("replica state binding does not match the volume or local directory");
        }
        if let Some(common) = &self.common {
            common.manifest.validate()?;
        }
        if self.publication.is_some() && self.materialization.is_some() {
            bail!("replica cannot publish and materialize simultaneously");
        }
        Ok(())
    }
}

pub(crate) fn scan(
    paths: &ReplicaPaths,
    base: Option<&Manifest>,
    stable: bool,
) -> Result<Manifest> {
    let mut entries = BTreeMap::new();
    let mut identities = HashSet::new();
    scan_directory(
        paths,
        &paths.local,
        "",
        base,
        stable,
        &mut entries,
        &mut identities,
    )?;
    let mut manifest = Manifest { entries };
    reuse_renamed_file_ids(base, &mut manifest);
    manifest.validate()?;
    Ok(manifest)
}

fn scan_directory(
    paths: &ReplicaPaths,
    directory: &Path,
    parent: &str,
    base: Option<&Manifest>,
    stable: bool,
    entries: &mut BTreeMap<String, Node>,
    identities: &mut HashSet<(u64, u64)>,
) -> Result<()> {
    let mut children = std::fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    children.sort_by_key(std::fs::DirEntry::file_name);
    let mut folded = BTreeSet::new();
    for child in children {
        let name = child
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("Managed names must be UTF-8"))?;
        validate_name(&name)?;
        if !folded.insert(name.to_lowercase()) {
            bail!("portable name collision in {parent:?}");
        }
        let relative = if parent.is_empty() {
            name
        } else {
            format!("{parent}/{name}")
        };
        if relative.len() > 4096 {
            bail!("portable path exceeds 4096 bytes");
        }
        let path = child.path();
        let metadata = std::fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            bail!("symbolic links are not supported: {relative:?}");
        }
        if metadata.is_dir() {
            let id = base
                .and_then(|value| value.entries.get(&relative))
                .filter(|node| matches!(node.kind, NodeKind::Directory))
                .map(|node| node.id.clone())
                .unwrap_or(new_node_id()?);
            entries.insert(
                relative.clone(),
                Node {
                    id,
                    kind: NodeKind::Directory,
                },
            );
            scan_directory(paths, &path, &relative, base, stable, entries, identities)?;
        } else if metadata.is_file() {
            if let Some(identity) = file_identity(&metadata) {
                if !identities.insert(identity) {
                    bail!("hard-linked files are not supported: {relative:?}");
                }
            }
            let (sha256, size) = fingerprint(&path, &metadata)?;
            let executable = executable(&metadata);
            let previous = base.and_then(|value| value.entries.get(&relative));
            let unchanged = previous.is_some_and(|node| {
                matches!(&node.kind, NodeKind::File { content, executable: mode }
                    if content.sha256 == sha256 && content.size == size && *mode == executable)
            });
            if stable && !unchanged {
                stage(paths, &path, &sha256, size, &metadata)?;
            }
            let id = previous
                .filter(|node| matches!(node.kind, NodeKind::File { .. }))
                .map(|node| node.id.clone())
                .unwrap_or(new_node_id()?);
            entries.insert(
                relative,
                Node {
                    id,
                    kind: NodeKind::File {
                        content: ContentRef {
                            data_ref: format!("sha256:{sha256}"),
                            sha256,
                            size,
                        },
                        executable,
                    },
                },
            );
        } else {
            bail!("only regular files and directories are supported: {relative:?}");
        }
    }
    Ok(())
}

fn fingerprint(path: &Path, before: &std::fs::Metadata) -> Result<(String, u64)> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut size = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }
    let after = file.metadata()?;
    if !same_file(before, &after) || after.len() != size {
        bail!(
            "local file changed while it was scanned: {}",
            path.display()
        );
    }
    Ok((hex(hasher.finalize()), size))
}

fn stage(
    paths: &ReplicaPaths,
    source: &Path,
    sha256: &str,
    size: u64,
    before: &std::fs::Metadata,
) -> Result<()> {
    let directory = paths.state.join("staging");
    std::fs::create_dir_all(&directory)?;
    let target = directory.join(sha256);
    if target.exists() {
        let (actual, actual_size) = fingerprint(&target, &std::fs::metadata(&target)?)?;
        if actual == sha256 && actual_size == size {
            return Ok(());
        }
        bail!("staged content identity is corrupt");
    }
    let mut input = File::open(source)?;
    let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
    let copied = std::io::copy(&mut input, temporary.as_file_mut())?;
    temporary.as_file_mut().sync_all()?;
    let after = input.metadata()?;
    if copied != size || !same_file(before, &after) {
        bail!("local file changed while stable input was prepared");
    }
    let (actual, actual_size) = fingerprint(temporary.path(), &temporary.as_file().metadata()?)?;
    if actual != sha256 || actual_size != size {
        bail!("stable input does not match its observed content");
    }
    match temporary.persist_noclobber(&target) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.error.into()),
    }
}

fn reuse_renamed_file_ids(base: Option<&Manifest>, target: &mut Manifest) {
    let Some(base) = base else { return };
    let removed = base
        .entries
        .iter()
        .filter(|(path, node)| {
            !target.entries.contains_key(*path) && matches!(node.kind, NodeKind::File { .. })
        })
        .collect::<Vec<_>>();
    for (path, node) in &mut target.entries {
        if base.entries.contains_key(path) {
            continue;
        }
        let matches = removed
            .iter()
            .filter(|(_, old)| same_file_shape(old, node))
            .collect::<Vec<_>>();
        if matches.len() == 1 {
            node.id = matches[0].1.id.clone();
        }
    }
}

fn same_file_shape(left: &Node, right: &Node) -> bool {
    matches!((&left.kind, &right.kind),
        (NodeKind::File { content: a, executable: x }, NodeKind::File { content: b, executable: y })
        if a.sha256 == b.sha256 && a.size == b.size && x == y)
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.ends_with([' ', '.'])
        || name
            .chars()
            .any(|value| value.is_control() || "<>:\"/\\|?*".contains(value))
    {
        bail!("name is outside the portable Managed Sync policy: {name:?}");
    }
    Ok(())
}

fn new_node_id() -> Result<NodeId> {
    NodeId::parse(Uuid::new_v4().to_string())
}

fn absolute(path: &Path) -> Result<PathBuf> {
    Ok(if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    })
}

fn resolve_missing(path: &Path) -> Result<PathBuf> {
    let mut tail = Vec::new();
    let mut cursor = path;
    loop {
        match std::fs::canonicalize(cursor) {
            Ok(mut resolved) => {
                for name in tail.iter().rev() {
                    resolved.push(name);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tail.push(
                    cursor
                        .file_name()
                        .context("state path has no existing parent")?
                        .to_owned(),
                );
                cursor = cursor
                    .parent()
                    .context("state path has no existing parent")?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(unix)]
fn file_identity(metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn file_identity(_metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
    None
}

#[cfg(unix)]
fn executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable(_metadata: &std::fs::Metadata) -> bool {
    false
}

fn same_file(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
        && file_identity(left) == file_identity(right)
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    use std::fmt::Write as _;
    let mut output = String::new();
    for byte in bytes.as_ref() {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}
