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

//! Atomic persistence for the lightweight replica recovery record.

use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Write as _};
use std::path::{Path, PathBuf};

use crate::Error;
use crate::filesystem::OperationId;

use super::state::ReplicaState;

const MAGIC: &[u8; 8] = b"OFSSTAT1";
const MAXIMUM_STATE_BYTES: usize = 16 * 1024;

pub(super) fn load(path: &Path) -> Result<Option<ReplicaState>, Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_io("read replica state", Some(path), error)),
    };
    let state = decode(&bytes)?;
    state.validate()?;
    Ok(Some(state))
}

pub(super) fn persist(state: &ReplicaState, path: &Path, replace: bool) -> Result<(), Error> {
    state.validate()?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent).map_err(|error| {
            Error::from_io("create replica state directory", Some(parent), error)
        })?;
    }
    let temporary = temporary_path(path, OperationId::generate());
    let bytes = encode(state)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| Error::from_io("create replica state", Some(&temporary), error))?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| Error::from_io("persist replica state", Some(&temporary), error))?;
    if replace {
        fs::rename(&temporary, path)
            .map_err(|error| Error::from_io("install replica state", Some(path), error))?;
    } else {
        fs::hard_link(&temporary, path).map_err(|error| {
            let _ = fs::remove_file(&temporary);
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Error::invalid(
                    "synchronize replica",
                    format!(
                        "cannot attach with an existing replica state: {}",
                        path.display()
                    ),
                )
            } else {
                Error::from_io("install new replica state", Some(path), error)
            }
        })?;
        fs::remove_file(&temporary).map_err(|error| {
            Error::from_io(
                "remove replica state temporary file",
                Some(&temporary),
                error,
            )
        })?;
    }
    sync_parent(path)
}

fn encode(state: &ReplicaState) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    ciborium::into_writer(state, &mut body)
        .map_err(|_| Error::corrupt("persist replica state", "state cannot be encoded"))?;
    if body.len() > MAXIMUM_STATE_BYTES {
        return Err(Error::corrupt(
            "persist replica state",
            "state exceeds its size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(MAGIC.len() + body.len() + 32);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<ReplicaState, Error> {
    let body = bytes
        .strip_prefix(MAGIC)
        .and_then(|bytes| bytes.get(..bytes.len().checked_sub(32)?))
        .ok_or_else(|| Error::corrupt("read replica state", "state is invalid"))?;
    if body.len() > MAXIMUM_STATE_BYTES
        || blake3::hash(&bytes[..bytes.len() - 32]).as_bytes() != &bytes[bytes.len() - 32..]
    {
        return Err(Error::corrupt(
            "read replica state",
            "state checksum is invalid",
        ));
    }
    let mut input = Cursor::new(body);
    let state = ciborium::from_reader(&mut input)
        .map_err(|_| Error::corrupt("read replica state", "state is invalid"))?;
    if input.position() != body.len() as u64 {
        return Err(Error::corrupt(
            "read replica state",
            "state has trailing bytes",
        ));
    }
    Ok(state)
}

fn temporary_path(path: &Path, operation: OperationId) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state");
    path.with_file_name(format!(".{name}.{operation}.tmp"))
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), Error> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::from_io("persist replica state directory", Some(parent), error))
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), Error> {
    Ok(())
}
