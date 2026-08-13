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
use crate::filesystem::ContentRef;

use super::super::object::{GcEpoch, ObjectClass};
use super::super::storage::ImmutableWriter;
use super::super::stream::{StreamKind, StreamRef, finish_stream};

/// Sequential writer for one immutable data segment.
pub(crate) struct Writer {
    writer: ImmutableWriter,
    payload_length: u64,
}

impl Writer {
    pub(crate) async fn open(operator: &Operator, gc_epoch: GcEpoch) -> Result<Self, Error> {
        Ok(Self {
            writer: ImmutableWriter::open(operator, gc_epoch, ObjectClass::DataSegment).await?,
            payload_length: 0,
        })
    }

    pub(crate) async fn abort(&mut self) -> Result<(), Error> {
        self.writer.abort().await
    }

    pub(crate) async fn write_file(
        &mut self,
        source: &mut (impl AsyncRead + Unpin),
        content: ContentRef,
    ) -> Result<u64, Error> {
        let offset = self.payload_length;
        let (length, digest) = self.writer.write_source(source).await?;
        if length != content.length() || digest != content.digest() {
            self.writer.abort().await?;
            return Err(Error::conflict(
                "write Managed data segment",
                "source changed while being published",
            ));
        }
        self.payload_length = self
            .payload_length
            .checked_add(length)
            .ok_or_else(|| Error::invalid("write Managed data segment", "length overflows"))?;
        Ok(offset)
    }

    pub(crate) async fn close(self) -> Result<StreamRef, Error> {
        let digest = self.writer.digest();
        finish_stream(
            self.writer,
            StreamKind::DATA_SEGMENT,
            self.payload_length,
            digest,
        )
        .await
    }
}
