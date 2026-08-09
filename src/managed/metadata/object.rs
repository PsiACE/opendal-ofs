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

use super::require_same_format;
use crate::managed::metadata::superblock::SUPERBLOCK_KEY;
use crate::managed::{ManagedError, ManagedErrorKind, ManagedFormat};

/// Managed metadata stored beside data through OpenDAL.
#[derive(Clone)]
pub(crate) struct ObjectMetadata {
    operator: Operator,
}

impl ObjectMetadata {
    pub(crate) const fn new(operator: Operator) -> Self {
        Self { operator }
    }

    pub(crate) async fn create_format(
        &self,
        desired: &ManagedFormat,
    ) -> Result<ManagedFormat, ManagedError> {
        if !self
            .operator
            .info()
            .full_capability()
            .write_with_if_not_exists
        {
            return Err(ManagedError::new(
                ManagedErrorKind::Invalid,
                "create Managed format",
                "object metadata requires create-only write",
            ));
        }
        let encoded = desired.encode()?;
        if create(
            &self.operator,
            SUPERBLOCK_KEY,
            encoded,
            "create Managed format",
        )
        .await?
        {
            Ok(desired.clone())
        } else {
            let observed = self
                .read_format_optional()
                .await?
                .ok_or_else(|| unavailable("create Managed format"))?;
            require_same_format(desired, observed)
        }
    }

    pub(crate) async fn read_format_optional(&self) -> Result<Option<ManagedFormat>, ManagedError> {
        read(&self.operator, SUPERBLOCK_KEY, "read Managed format")
            .await?
            .map(|bytes| ManagedFormat::decode(&bytes))
            .transpose()
    }
}

pub(crate) async fn read(
    operator: &Operator,
    key: &str,
    action: &'static str,
) -> Result<Option<Vec<u8>>, ManagedError> {
    match operator.read(key).await {
        Ok(bytes) => Ok(Some(bytes.to_bytes().to_vec())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(_) => Err(unavailable(action)),
    }
}

pub(crate) async fn read_with_revision(
    operator: &Operator,
    key: &str,
    action: &'static str,
) -> Result<Option<(Vec<u8>, String)>, ManagedError> {
    let reader = match operator.reader(key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable(action)),
    };
    let bytes = match reader.read(..).await {
        Ok(bytes) => bytes.to_bytes().to_vec(),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable(action)),
    };
    let revision = reader
        .metadata()
        .and_then(|metadata| metadata.etag())
        .ok_or_else(|| unavailable(action))?
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
) -> Result<Vec<u8>, ManagedError> {
    let bytes = read(operator, key, action)
        .await?
        .ok_or_else(|| ManagedError::new(ManagedErrorKind::Corrupt, action, missing))?;
    if Sha256::digest(&bytes).as_slice() != expected {
        return Err(ManagedError::new(
            ManagedErrorKind::Corrupt,
            action,
            invalid,
        ));
    }
    Ok(bytes)
}

pub(crate) async fn create(
    operator: &Operator,
    key: &str,
    bytes: Vec<u8>,
    action: &'static str,
) -> Result<bool, ManagedError> {
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
        Err(_) => Err(unavailable(action)),
    }
}

pub(crate) async fn replace(
    operator: &Operator,
    key: &str,
    expected_revision: &str,
    bytes: Vec<u8>,
    action: &'static str,
) -> Result<bool, ManagedError> {
    match operator
        .write_with(key, bytes)
        .if_match(expected_revision)
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::ConditionNotMatch => Ok(false),
        Err(_) => Err(unavailable(action)),
    }
}

pub(crate) async fn ensure_immutable(
    operator: &Operator,
    key: &str,
    expected: &[u8],
    action: &'static str,
    mismatch_kind: ManagedErrorKind,
    mismatch_message: &'static str,
) -> Result<(), ManagedError> {
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
        .ok_or_else(|| unavailable(action))?;
    if observed == expected {
        Ok(())
    } else {
        Err(ManagedError::new(mismatch_kind, action, mismatch_message))
    }
}

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "object metadata is unavailable",
    )
}
