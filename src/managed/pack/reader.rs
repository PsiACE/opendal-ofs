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
use opendal::{Buffer, BufferStream, Operator};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use crate::Error;
use crate::filesystem::{Digest, FileFingerprint};

use super::super::object::{ObjectClass, ObjectLocator};

/// One bounded range stream from a Pack object.
pub(crate) struct RangeReader {
    stream: BufferStream,
    pending: Buffer,
    remaining: u64,
}

impl RangeReader {
    pub(crate) async fn open(
        operator: &Operator,
        locator: ObjectLocator,
        range: std::ops::Range<u64>,
    ) -> Result<Self, Error> {
        if locator.class != ObjectClass::FilePack || range.start > range.end {
            return Err(Error::corrupt(
                "read Managed pack",
                "pack range reference is invalid",
            ));
        }
        let remaining = range.end - range.start;
        let stream = operator
            .reader(&locator.key())
            .await
            .map_err(|error| Error::from_storage("open Managed pack", error))?
            .into_stream(range)
            .await
            .map_err(|error| Error::from_storage("read Managed pack", error))?;
        Ok(Self {
            stream,
            pending: Buffer::new(),
            remaining,
        })
    }

    pub(crate) async fn copy_file(
        &mut self,
        fingerprint: FileFingerprint,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        let expected = fingerprint.logical_length();
        let mut hasher = blake3::Hasher::new();
        self.copy_exact(expected, destination, Some(&mut hasher))
            .await?;
        if Digest::from_bytes(hasher.finalize().into()) != fingerprint.digest() {
            return Err(Error::corrupt(
                "read Managed pack",
                "pack entry does not match its fingerprint",
            ));
        }
        Ok(())
    }

    pub(crate) async fn copy_bytes(
        &mut self,
        length: u64,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        self.copy_exact(length, destination, None).await
    }

    async fn copy_exact(
        &mut self,
        length: u64,
        destination: &mut (impl AsyncWrite + Unpin),
        mut hasher: Option<&mut blake3::Hasher>,
    ) -> Result<(), Error> {
        if length > self.remaining {
            return Err(Error::corrupt(
                "read Managed pack",
                "pack entry exceeds the requested range",
            ));
        }
        let mut remaining = length;
        while remaining != 0 {
            if self.pending.is_empty() {
                self.pending = self
                    .stream
                    .next()
                    .await
                    .ok_or_else(|| Error::corrupt("read Managed pack", "pack range is truncated"))?
                    .map_err(|error| Error::from_storage("read Managed pack", error))?;
            }
            let take = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(self.pending.len());
            let bytes = self.pending.slice(..take);
            self.pending = self.pending.slice(take..);
            for chunk in bytes {
                if let Some(hasher) = hasher.as_deref_mut() {
                    hasher.update(&chunk);
                }
                destination
                    .write_all(&chunk)
                    .await
                    .map_err(|error| Error::io("write Managed pack destination", error))?;
            }
            remaining -= take as u64;
            self.remaining -= take as u64;
        }
        Ok(())
    }
}
