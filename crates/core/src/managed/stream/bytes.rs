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

use opendal::Operator;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};

use crate::Error;
use crate::filesystem::Digest;

use super::super::object::{GcEpoch, ObjectClass};
use super::super::storage::ImmutableWriter;
use super::{
    STREAM_TAIL_BYTES, StreamKind, StreamRef, finish_stream, validate_stream_layout,
    validate_stream_tail,
};

pub(crate) async fn write_byte_stream<R>(
    operator: &Operator,
    gc_epoch: GcEpoch,
    class: ObjectClass,
    source: &mut R,
    expected_length: u64,
    expected_digest: Digest,
) -> Result<StreamRef, Error>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut writer = ImmutableWriter::open(operator, gc_epoch, class).await?;
    let (payload_length, payload_digest) = writer.write_source(source).await?;
    if payload_length != expected_length || payload_digest != expected_digest {
        writer.abort().await?;
        return Err(Error::conflict(
            "write Managed byte stream",
            "source changed while being published",
        ));
    }
    finish_stream(
        writer,
        StreamKind::FILE_BYTES,
        payload_length,
        payload_digest,
    )
    .await
}

pub(crate) async fn write_unchecked_byte_stream<R>(
    operator: &Operator,
    gc_epoch: GcEpoch,
    class: ObjectClass,
    source: &mut R,
) -> Result<StreamRef, Error>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut writer = ImmutableWriter::open(operator, gc_epoch, class).await?;
    let (payload_length, payload_digest) = writer.write_source(source).await?;
    finish_stream(
        writer,
        StreamKind::FILE_BYTES,
        payload_length,
        payload_digest,
    )
    .await
}

pub(crate) async fn copy_byte_stream<W>(
    operator: &Operator,
    reference: StreamRef,
    range: std::ops::Range<u64>,
    destination: &mut W,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    if range.start > range.end || range.end > reference.payload_length {
        return Err(Error::invalid(
            "read Managed byte range",
            "logical byte range is invalid",
        ));
    }
    if range.start == 0 && range.end == reference.payload_length {
        return copy_complete_byte_stream(operator, reference, destination).await;
    }
    if range.is_empty() {
        return Ok(());
    }
    let mut stream = operator
        .reader_with(&reference.object.key())
        .content_length_hint(reference.object.encoded_length)
        .await
        .map_err(|error| Error::from_storage("read Managed byte stream", error))?
        .into_stream(range.clone())
        .await
        .map_err(|error| Error::from_storage("read Managed byte stream", error))?;
    let mut length = 0_u64;
    use futures::StreamExt as _;
    while let Some(buffer) = stream.next().await {
        let buffer =
            buffer.map_err(|error| Error::from_storage("read Managed byte stream", error))?;
        for chunk in buffer {
            length = length
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| Error::corrupt("read Managed byte stream", "length overflows"))?;
            destination
                .write_all(&chunk)
                .await
                .map_err(|error| Error::io("write Managed stream destination", error))?;
        }
    }
    if length != range.end - range.start {
        return Err(Error::corrupt(
            "read Managed byte stream",
            "payload does not match its reference",
        ));
    }
    Ok(())
}

async fn copy_complete_byte_stream<W>(
    operator: &Operator,
    reference: StreamRef,
    destination: &mut W,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    validate_stream_layout(reference)?;
    let mut stream = operator
        .reader_with(&reference.object.key())
        .content_length_hint(reference.object.encoded_length)
        .await
        .map_err(|error| Error::from_storage("read Managed byte stream", error))?
        .into_stream(0..reference.object.encoded_length)
        .await
        .map_err(|error| Error::from_storage("read Managed byte stream", error))?;
    let mut object_hasher = blake3::Hasher::new();
    let mut payload_hasher = blake3::Hasher::new();
    let mut payload_remaining = reference.payload_length;
    let mut tail = Vec::with_capacity(STREAM_TAIL_BYTES);
    use futures::StreamExt as _;
    while let Some(buffer) = stream.next().await {
        let buffer =
            buffer.map_err(|error| Error::from_storage("read Managed byte stream", error))?;
        for chunk in buffer {
            object_hasher.update(&chunk);
            let payload_bytes = usize::try_from(payload_remaining)
                .unwrap_or(usize::MAX)
                .min(chunk.len());
            if payload_bytes != 0 {
                let payload = &chunk[..payload_bytes];
                payload_hasher.update(payload);
                destination
                    .write_all(payload)
                    .await
                    .map_err(|error| Error::io("write Managed stream destination", error))?;
                payload_remaining -= payload_bytes as u64;
            }
            tail.extend_from_slice(&chunk[payload_bytes..]);
            if tail.len() > STREAM_TAIL_BYTES {
                return Err(Error::corrupt(
                    "read Managed byte stream",
                    "stream contains trailing bytes",
                ));
            }
        }
    }
    if payload_remaining != 0
        || Digest::from_bytes(payload_hasher.finalize().into()) != reference.payload_digest
    {
        return Err(Error::corrupt(
            "read Managed byte stream",
            "payload does not match its reference",
        ));
    }
    validate_stream_tail(reference, &tail)?;
    if Digest::from_bytes(object_hasher.finalize().into()) != reference.object.digest {
        return Err(Error::corrupt(
            "read Managed byte stream",
            "object does not match its reference",
        ));
    }
    Ok(())
}
