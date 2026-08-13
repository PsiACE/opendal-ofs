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
use tokio::io::AsyncRead;

use crate::Error;
use crate::filesystem::Digest;

use super::super::object::{GcEpoch, ObjectClass};
use super::super::storage::ImmutableWriter;
use super::{StreamKind, StreamRef, finish_stream};

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
        StreamKind::DATA_SEGMENT,
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
        StreamKind::DATA_SEGMENT,
        payload_length,
        payload_digest,
    )
    .await
}
