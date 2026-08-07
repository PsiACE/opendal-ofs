// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write as _};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::filesystem::{ChangeCursor, Generation, NodeId, OperationId, VolumeId};
use crate::sync::local::NativeIdentity;

const STATE_FORMAT: &str = "ofs-sync-replica";
const STATE_MAJOR: u16 = 1;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseEntry {
    pub node: NodeId,
    pub generation: Generation,
    pub directory_generation: Option<Generation>,
    pub digest: Option<[u8; 32]>,
    pub local_identity: Option<NativeIdentity>,
    pub local_size: Option<u64>,
    pub local_modified: Option<String>,
    pub local_executable: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingIntent {
    pub operation: OperationId,
    pub base: ChangeCursor,
    pub staging: PathBuf,
    pub renames: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConflictRecord {
    pub path: String,
    pub local_digest: Option<[u8; 32]>,
    pub remote_digest: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaState {
    pub volume: VolumeId,
    pub common: ChangeCursor,
    pub base: BTreeMap<String, BaseEntry>,
    pub pending: Option<PendingIntent>,
    pub conflicts: Vec<ConflictRecord>,
}

impl ReplicaState {
    pub fn empty(volume: VolumeId) -> Self {
        Self {
            volume,
            common: ChangeCursor::Genesis,
            base: BTreeMap::new(),
            pending: None,
            conflicts: Vec::new(),
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let bytes = match fs::read(path.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("read replica state"),
        };
        let wire: StateWire = serde_json::from_slice(&bytes).context("parse replica state JSON")?;
        wire.try_into().map(Some)
    }

    pub fn install(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent).context("create replica state directory")?;
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary =
            path.with_extension(format!("ofs-state.{}.{}.tmp", std::process::id(), sequence));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let result = (|| -> Result<()> {
            let mut file = options.open(&temporary)?;
            serde_json::to_writer(&mut file, &StateWire::from(self))?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, path)?;
            sync_directory(parent)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.context("install replica state")
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StateWire {
    format: String,
    major: u16,
    volume: [u8; 16],
    common: CursorWire,
    base: BTreeMap<String, BaseWire>,
    pending: Option<IntentWire>,
    conflicts: Vec<ConflictWire>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CursorWire {
    sequence: u64,
    operation: Option<[u8; 16]>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BaseWire {
    node: [u8; 16],
    generation: Vec<u8>,
    #[serde(default)]
    directory_generation: Option<Vec<u8>>,
    digest: Option<[u8; 32]>,
    #[serde(default)]
    local_identity: Option<NativeIdentityWire>,
    #[serde(default)]
    local_size: Option<u64>,
    #[serde(default)]
    local_modified: Option<String>,
    #[serde(default)]
    local_executable: Option<bool>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IntentWire {
    operation: [u8; 16],
    base: CursorWire,
    staging: PathBuf,
    #[serde(default)]
    renames: BTreeMap<String, String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityWire {
    device: u64,
    inode: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ConflictWire {
    path: String,
    local_digest: Option<[u8; 32]>,
    remote_digest: Option<[u8; 32]>,
}

impl From<&ReplicaState> for StateWire {
    fn from(state: &ReplicaState) -> Self {
        Self {
            format: STATE_FORMAT.into(),
            major: STATE_MAJOR,
            volume: *state.volume.as_bytes(),
            common: CursorWire::from(state.common),
            base: state
                .base
                .iter()
                .map(|(path, entry)| {
                    (
                        path.clone(),
                        BaseWire {
                            node: *entry.node.as_bytes(),
                            generation: entry.generation.as_bytes().into(),
                            directory_generation: entry
                                .directory_generation
                                .as_ref()
                                .map(|generation| generation.as_bytes().into()),
                            digest: entry.digest,
                            local_identity: entry.local_identity.map(|identity| {
                                NativeIdentityWire {
                                    device: identity.device,
                                    inode: identity.inode,
                                }
                            }),
                            local_size: entry.local_size,
                            local_modified: entry.local_modified.clone(),
                            local_executable: entry.local_executable,
                        },
                    )
                })
                .collect(),
            pending: state.pending.as_ref().map(|intent| IntentWire {
                operation: *intent.operation.as_bytes(),
                base: CursorWire::from(intent.base),
                staging: intent.staging.clone(),
                renames: intent.renames.clone(),
            }),
            conflicts: state
                .conflicts
                .iter()
                .map(|conflict| ConflictWire {
                    path: conflict.path.clone(),
                    local_digest: conflict.local_digest,
                    remote_digest: conflict.remote_digest,
                })
                .collect(),
        }
    }
}

impl TryFrom<StateWire> for ReplicaState {
    type Error = anyhow::Error;

    fn try_from(wire: StateWire) -> Result<Self> {
        if wire.format != STATE_FORMAT || wire.major != STATE_MAJOR {
            bail!("replica state format is unsupported");
        }
        let base = wire
            .base
            .into_iter()
            .map(|(path, entry)| {
                Ok((
                    path,
                    BaseEntry {
                        node: NodeId::from_bytes(entry.node),
                        generation: Generation::from_bytes(entry.generation),
                        directory_generation: entry
                            .directory_generation
                            .map(Generation::from_bytes),
                        digest: entry.digest,
                        local_identity: entry.local_identity.map(|identity| NativeIdentity {
                            device: identity.device,
                            inode: identity.inode,
                        }),
                        local_size: entry.local_size,
                        local_modified: entry.local_modified,
                        local_executable: entry.local_executable,
                    },
                ))
            })
            .collect::<Result<_>>()?;
        let pending = wire
            .pending
            .map(|intent| -> Result<PendingIntent> {
                Ok(PendingIntent {
                    operation: OperationId::from_bytes(intent.operation),
                    base: intent.base.try_into()?,
                    staging: intent.staging,
                    renames: intent.renames,
                })
            })
            .transpose()?;
        Ok(Self {
            volume: VolumeId::from_bytes(wire.volume),
            common: wire.common.try_into()?,
            base,
            pending,
            conflicts: wire
                .conflicts
                .into_iter()
                .map(|conflict| ConflictRecord {
                    path: conflict.path,
                    local_digest: conflict.local_digest,
                    remote_digest: conflict.remote_digest,
                })
                .collect(),
        })
    }
}

impl From<ChangeCursor> for CursorWire {
    fn from(cursor: ChangeCursor) -> Self {
        Self {
            sequence: cursor.sequence(),
            operation: cursor.operation().map(|value| *value.as_bytes()),
        }
    }
}

impl TryFrom<CursorWire> for ChangeCursor {
    type Error = anyhow::Error;

    fn try_from(wire: CursorWire) -> Result<Self> {
        match (wire.sequence, wire.operation) {
            (0, None) => Ok(Self::Genesis),
            (sequence, Some(operation)) => Ok(Self::at(
                NonZeroU64::new(sequence).context("replica cursor sequence is zero")?,
                OperationId::from_bytes(operation),
            )),
            _ => bail!("replica cursor sequence and operation disagree"),
        }
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().map_err(Into::into)
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> Result<()> {
    Ok(())
}
