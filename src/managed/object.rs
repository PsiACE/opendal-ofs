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

use futures::StreamExt as _;
use opendal::{Buffer, ErrorKind as StorageErrorKind, Operator};

use crate::Error;

pub(crate) async fn read(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
) -> Result<Option<Vec<u8>>, Error> {
    let reader = match operator.reader(key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed metadata", error)),
    };
    let mut stream = reader
        .into_stream(..)
        .await
        .map_err(|error| Error::from_storage("read Managed metadata", error))?;
    let mut bytes = Vec::with_capacity(maximum_bytes.min(64 * 1024));
    while let Some(buffer) = stream.next().await {
        let buffer = match buffer {
            Ok(buffer) => buffer,
            Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::from_storage("read Managed metadata", error)),
        };
        for chunk in buffer {
            if bytes.len().saturating_add(chunk.len()) > maximum_bytes {
                return Err(Error::corrupt(
                    "read Managed metadata",
                    "object exceeds its size limit",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
    }
    Ok(Some(bytes))
}

pub(crate) async fn read_with_revision(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
) -> Result<Option<(Vec<u8>, String)>, Error> {
    let reader = match operator.reader(key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed metadata", error)),
    };
    let metadata = reader.clone();
    let mut stream = reader
        .into_stream(..)
        .await
        .map_err(|error| Error::from_storage("read Managed metadata", error))?;
    let mut bytes = Vec::with_capacity(maximum_bytes.min(64 * 1024));
    while let Some(buffer) = stream.next().await {
        let buffer = buffer.map_err(|error| Error::from_storage("read Managed metadata", error))?;
        for chunk in buffer {
            if bytes.len().saturating_add(chunk.len()) > maximum_bytes {
                return Err(Error::corrupt(
                    "read Managed metadata",
                    "object exceeds its size limit",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
    }
    let revision = metadata
        .metadata()
        .and_then(|metadata| metadata.etag())
        .ok_or_else(|| {
            Error::unsupported("read Managed metadata", "object revision is unavailable")
        })?
        .to_owned();
    Ok(Some((bytes, revision)))
}

pub(crate) async fn create(operator: &Operator, key: &str, bytes: Vec<u8>) -> Result<bool, Error> {
    match operator.write_with(key, bytes).if_not_exists(true).await {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(Error::from_storage("write Managed metadata", error)),
    }
}

pub(crate) async fn replace(
    operator: &Operator,
    key: &str,
    expected_revision: &str,
    bytes: Vec<u8>,
) -> Result<bool, Error> {
    match operator
        .write_with(key, bytes)
        .if_match(expected_revision)
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == StorageErrorKind::ConditionNotMatch => Ok(false),
        Err(error) => Err(Error::from_storage("publish Managed metadata", error)),
    }
}

pub(crate) async fn create_immutable(
    operator: &Operator,
    key: &str,
    bytes: Buffer,
) -> Result<(), Error> {
    match operator.write_with(key, bytes).if_not_exists(true).await {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(Error::from_storage("publish Managed data", error)),
    }
}
