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

use opendal::{ErrorKind, Operator};
use sha2::{Digest as _, Sha256};

use crate::filesystem::{VolumeError, VolumeErrorKind};
use crate::managed::error::{corrupt, error, unavailable};

pub(crate) async fn read(
    operator: &Operator,
    key: &str,
    action: &'static str,
) -> Result<Option<Vec<u8>>, VolumeError> {
    match operator.read(key).await {
        Ok(bytes) => Ok(Some(bytes.to_bytes().to_vec())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(_) => Err(unavailable(action, "object metadata is unavailable")),
    }
}

pub(crate) async fn read_with_revision(
    operator: &Operator,
    key: &str,
    action: &'static str,
) -> Result<Option<(Vec<u8>, String)>, VolumeError> {
    let reader = match operator.reader(key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable(action, "object metadata is unavailable")),
    };
    let bytes = match reader.read(..).await {
        Ok(bytes) => bytes.to_bytes().to_vec(),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable(action, "object metadata is unavailable")),
    };
    let revision = reader
        .metadata()
        .and_then(|metadata| metadata.etag())
        .ok_or_else(|| unavailable(action, "object metadata is unavailable"))?
        .to_owned();
    Ok(Some((bytes, revision)))
}

pub(crate) async fn read_content_addressed(
    operator: &Operator,
    key: &str,
    expected: &[u8; 32],
    action: &'static str,
    missing: &'static str,
    invalid: &'static str,
) -> Result<Vec<u8>, VolumeError> {
    let bytes = read(operator, key, action)
        .await?
        .ok_or_else(|| corrupt(action, missing))?;
    if Sha256::digest(&bytes).as_slice() != expected {
        return Err(corrupt(action, invalid));
    }
    Ok(bytes)
}

pub(crate) async fn create(
    operator: &Operator,
    key: &str,
    bytes: Vec<u8>,
    action: &'static str,
) -> Result<bool, VolumeError> {
    match operator.write_with(key, bytes).if_not_exists(true).await {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(_) => Err(unavailable(action, "object metadata is unavailable")),
    }
}

pub(crate) async fn replace(
    operator: &Operator,
    key: &str,
    expected_revision: &str,
    bytes: Vec<u8>,
    action: &'static str,
) -> Result<bool, VolumeError> {
    match operator
        .write_with(key, bytes)
        .if_match(expected_revision)
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::ConditionNotMatch => Ok(false),
        Err(_) => Err(unavailable(action, "object metadata is unavailable")),
    }
}

pub(crate) async fn ensure_immutable(
    operator: &Operator,
    key: &str,
    expected: &[u8],
    action: &'static str,
    mismatch_kind: VolumeErrorKind,
    mismatch_message: &'static str,
) -> Result<(), VolumeError> {
    if operator
        .write_with(key, expected.to_vec())
        .if_not_exists(true)
        .await
        .is_ok()
    {
        return Ok(());
    }
    let observed = read(operator, key, action)
        .await?
        .ok_or_else(|| unavailable(action, "object metadata is unavailable"))?;
    if observed == expected {
        Ok(())
    } else {
        Err(error(mismatch_kind, action, mismatch_message))
    }
}
