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
use crate::filesystem::{Checksum, FileFingerprint};

use super::super::object::{GcEpoch, ObjectClass, ObjectRef, checksum};
use super::super::storage::ImmutableWriter;
use super::super::stream::StreamKind;
use super::layout::{ENTRY_BYTES, MAGIC, TRAILER_BYTES};

/// Sequential writer for one immutable Pack object.
pub(crate) struct Writer {
    writer: ImmutableWriter,
    payload_length: u64,
    index_length: u64,
    index_hasher: blake3::Hasher,
}

impl Writer {
    pub(crate) async fn open(operator: &Operator, gc_epoch: GcEpoch) -> Result<Self, Error> {
        Ok(Self {
            writer: ImmutableWriter::open(operator, gc_epoch, ObjectClass::FilePack).await?,
            payload_length: 0,
            index_length: 0,
            index_hasher: blake3::Hasher::new(),
        })
    }

    pub(crate) async fn abort(&mut self) -> Result<(), Error> {
        self.writer.abort().await
    }

    pub(crate) async fn write_file(
        &mut self,
        source: &mut (impl AsyncRead + Unpin),
        fingerprint: FileFingerprint,
    ) -> Result<u64, Error> {
        if self.index_length != 0 {
            return Err(Error::invalid(
                "write Managed pack",
                "file data cannot follow the pack index",
            ));
        }
        let offset = self.payload_length;
        let (length, digest) = self.writer.write_source(source).await?;
        self.payload_length = self
            .payload_length
            .checked_add(length)
            .ok_or_else(|| Error::invalid("write Managed pack", "payload length overflows"))?;
        if length != fingerprint.logical_length() || digest != fingerprint.digest() {
            self.writer.abort().await?;
            return Err(Error::conflict(
                "write Managed pack",
                "source changed while being published",
            ));
        }
        Ok(offset)
    }

    pub(crate) async fn write_entry(
        &mut self,
        offset: u64,
        fingerprint: FileFingerprint,
    ) -> Result<(), Error> {
        let end = offset
            .checked_add(fingerprint.logical_length())
            .ok_or_else(|| Error::invalid("write Managed pack", "entry range overflows"))?;
        if end > self.payload_length {
            return Err(Error::invalid(
                "write Managed pack",
                "entry range exceeds the pack payload",
            ));
        }
        let mut entry = Vec::with_capacity(ENTRY_BYTES as usize);
        entry.extend_from_slice(&offset.to_le_bytes());
        entry.extend_from_slice(&fingerprint.logical_length().to_le_bytes());
        entry.extend_from_slice(fingerprint.digest().as_bytes());
        self.index_hasher.update(&entry);
        self.index_length = self
            .index_length
            .checked_add(ENTRY_BYTES)
            .ok_or_else(|| Error::invalid("write Managed pack", "index length overflows"))?;
        self.writer.write(entry).await
    }

    pub(crate) async fn close(mut self) -> Result<ObjectRef, Error> {
        if self.index_length == 0 || !self.index_length.is_multiple_of(ENTRY_BYTES) {
            self.writer.abort().await?;
            return Err(Error::invalid(
                "write Managed pack",
                "pack index is empty or incomplete",
            ));
        }
        let index_digest = Checksum::from_bytes(self.index_hasher.finalize().into());
        let mut trailer = Vec::with_capacity(TRAILER_BYTES as usize);
        trailer.extend_from_slice(&MAGIC);
        trailer.extend_from_slice(&StreamKind::FILE_PACK.value().to_le_bytes());
        trailer.extend_from_slice(&self.payload_length.to_le_bytes());
        trailer.extend_from_slice(&self.index_length.to_le_bytes());
        trailer.extend_from_slice(index_digest.as_bytes());
        trailer.extend_from_slice(checksum(&trailer).as_bytes());
        self.writer.write(trailer).await?;
        self.writer.close().await
    }
}
