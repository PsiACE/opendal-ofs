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

use futures::StreamExt;
use opendal::{Buffer, ErrorKind, Operator};

use crate::filesystem::VolumeError;
use crate::managed::error::{corrupt, unavailable};

pub(crate) async fn read(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
    action: &'static str,
) -> Result<Option<Vec<u8>>, VolumeError> {
    read_object(operator, key, maximum_bytes, action)
        .await
        .map(|value| value.map(|(bytes, _)| bytes))
}

pub(crate) async fn read_with_revision(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
    action: &'static str,
) -> Result<Option<(Vec<u8>, String)>, VolumeError> {
    read_object(operator, key, maximum_bytes, action)
        .await?
        .map(|(bytes, revision)| {
            revision
                .ok_or_else(|| unavailable(action, "object metadata is unavailable"))
                .map(|revision| (bytes, revision))
        })
        .transpose()
}

async fn read_object(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
    action: &'static str,
) -> Result<Option<(Vec<u8>, Option<String>)>, VolumeError> {
    let reader = match operator.reader(key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable(action, "object metadata is unavailable")),
    };
    let metadata = reader.clone();
    let mut stream = reader
        .into_stream(..)
        .await
        .map_err(|_| unavailable(action, "object metadata is unavailable"))?;
    let mut bytes = Vec::with_capacity(maximum_bytes.min(64 * 1024));
    while let Some(buffer) = stream.next().await {
        let buffer = match buffer {
            Ok(buffer) => buffer,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(unavailable(action, "object metadata is unavailable")),
        };
        for chunk in buffer {
            if bytes.len().saturating_add(chunk.len()) > maximum_bytes {
                return Err(corrupt(action, "object metadata exceeds its size limit"));
            }
            bytes.extend_from_slice(&chunk);
        }
    }
    let revision = metadata
        .metadata()
        .and_then(|metadata| metadata.etag())
        .map(str::to_owned);
    Ok(Some((bytes, revision)))
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
    expected: Buffer,
    action: &'static str,
) -> Result<(), VolumeError> {
    match operator
        .write_with(key, expected.clone())
        .if_not_exists(true)
        .await
    {
        Ok(_) => return Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
            ) => {}
        Err(_) => return Err(unavailable(action, "storage operation failed")),
    }
    let observed = read(operator, key, expected.len(), action)
        .await?
        .ok_or_else(|| unavailable(action, "object metadata is unavailable"))?;
    let matches = Iterator::eq(observed.iter().copied(), Iterator::flatten(expected));
    if matches {
        Ok(())
    } else {
        Err(corrupt(action, "immutable object changed"))
    }
}

#[cfg(test)]
mod tests {
    use opendal::services;

    use super::*;
    use crate::filesystem::VolumeErrorKind;

    #[tokio::test]
    async fn bounded_record_read_rejects_an_oversized_object() {
        let operator = Operator::new(services::Memory::default()).unwrap().finish();
        operator.write("record", vec![1, 2]).await.unwrap();

        assert_eq!(
            read(&operator, "record", 1, "read bounded record")
                .await
                .unwrap_err()
                .kind(),
            VolumeErrorKind::Corrupt
        );
    }
}
