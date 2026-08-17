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

//! Format record stream on OpenDAL.

use std::io::Cursor;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use futures::AsyncReadExt as _;
use opendal::Operator;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;
use crate::filesystem::Digest;
use crate::format::{GcEpoch, ObjectClass};
use crate::format::{
    STREAM_TAIL_BYTES, StreamKind, StreamRef, checksum, encode_stream_tail, validate_stream_tail,
};

use super::object::ImmutableWriter;

const FRAME_MAGIC: [u8; 4] = *b"OFSF";
const FRAME_HEADER_BYTES: usize = 4 + 8 + 4 + 32;
const MAXIMUM_FRAME_BYTES: usize = 64 * 1024;

pub struct RecordStreamReader<T> {
    reference: StreamRef,
    reader: opendal::FuturesAsyncReader,
    object_hasher: blake3::Hasher,
    offset: u64,
    frame: Vec<u8>,
    record_offset: usize,
    records_remaining: u32,
    completed: bool,
    record: PhantomData<T>,
}

impl<T: DeserializeOwned> RecordStreamReader<T> {
    pub async fn open(operator: &Operator, reference: StreamRef) -> Result<Self, Error> {
        if reference
            .payload_length
            .checked_add(STREAM_TAIL_BYTES as u64)
            != Some(reference.object.encoded_length)
        {
            return Err(Error::corrupt(
                "read Managed stream",
                "stream length does not match its reference",
            ));
        }
        Ok(Self {
            reference,
            reader: open_payload_reader(operator, reference).await?,
            object_hasher: blake3::Hasher::new(),
            offset: 0,
            frame: Vec::new(),
            record_offset: 0,
            records_remaining: 0,
            completed: false,
            record: PhantomData,
        })
    }

    pub async fn next(&mut self) -> Result<Option<T>, Error> {
        loop {
            if self.records_remaining != 0 {
                let record = decode_record(&self.frame, &mut self.record_offset)?;
                self.records_remaining -= 1;
                return Ok(Some(record));
            }
            if !self.frame.is_empty() && self.record_offset != self.frame.len() {
                return Err(Error::corrupt(
                    "read Managed stream",
                    "frame record count does not match its payload",
                ));
            }
            if self.completed {
                return Ok(None);
            }
            if self.offset == self.reference.payload_length {
                if Digest::from_bytes(self.object_hasher.finalize().into())
                    != self.reference.payload_digest
                {
                    return Err(Error::corrupt(
                        "read Managed stream",
                        "stream payload does not match its reference",
                    ));
                }
                let mut tail = [0_u8; STREAM_TAIL_BYTES];
                self.reader
                    .read_exact(&mut tail)
                    .await
                    .map_err(|error| Error::io("read Managed stream tail", error))?;
                self.object_hasher.update(&tail);
                validate_stream_tail(self.reference, &tail)?;
                if Digest::from_bytes(self.object_hasher.finalize().into())
                    != self.reference.object.digest
                {
                    return Err(Error::corrupt(
                        "read Managed stream",
                        "object does not match its reference",
                    ));
                }
                self.completed = true;
                return Ok(None);
            }
            let (end, record_count) = read_next_frame(
                &mut self.reader,
                self.reference,
                self.offset,
                &mut self.frame,
            )
            .await?;
            self.object_hasher.update(&self.frame);
            self.record_offset = FRAME_HEADER_BYTES;
            self.records_remaining = record_count;
            self.offset = end;
        }
    }
}

pub struct RecordStreamWriter {
    writer: ImmutableWriter,
    kind: StreamKind,
    payload_length: u64,
    frame: Vec<u8>,
    frame_records: u32,
    record: Vec<u8>,
}

/// Exact payload sizing for the canonical record-stream framing.
pub(crate) struct RecordStreamSizer {
    payload_length: u64,
    frame_length: usize,
}

impl RecordStreamSizer {
    pub(crate) const fn new() -> Self {
        Self {
            payload_length: 0,
            frame_length: 0,
        }
    }

    pub(crate) fn write(&mut self, record: &impl Serialize) -> Result<(), Error> {
        let mut encoded = Vec::new();
        ciborium::into_writer(record, &mut encoded)
            .map_err(|_| Error::invalid("size Managed stream", "record cannot be encoded"))?;
        self.write_encoded(encoded.len())
    }

    pub(crate) fn write_encoded(&mut self, record_bytes: usize) -> Result<(), Error> {
        let encoded_length = record_bytes.saturating_add(size_of::<u32>());
        if encoded_length > MAXIMUM_FRAME_BYTES {
            return Err(Error::invalid(
                "size Managed stream",
                "one metadata record exceeds the frame range unit",
            ));
        }
        if self.frame_length != 0
            && self.frame_length.saturating_add(encoded_length) > MAXIMUM_FRAME_BYTES
        {
            self.finish_frame()?;
        }
        self.frame_length = self
            .frame_length
            .checked_add(encoded_length)
            .ok_or_else(|| Error::invalid("size Managed stream", "frame length overflows"))?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<u64, Error> {
        self.finish_frame()?;
        Ok(self.payload_length)
    }

    fn finish_frame(&mut self) -> Result<(), Error> {
        if self.frame_length == 0 {
            return Ok(());
        }
        self.payload_length = self
            .payload_length
            .checked_add(FRAME_HEADER_BYTES as u64)
            .and_then(|length| length.checked_add(self.frame_length as u64))
            .ok_or_else(|| Error::invalid("size Managed stream", "payload length overflows"))?;
        self.frame_length = 0;
        Ok(())
    }
}

impl RecordStreamWriter {
    pub async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
        kind: StreamKind,
        multipart_part_bytes: NonZeroUsize,
    ) -> Result<Self, Error> {
        Ok(Self {
            writer: ImmutableWriter::open(operator, gc_epoch, class, multipart_part_bytes).await?,
            kind,
            payload_length: 0,
            frame: Vec::new(),
            frame_records: 0,
            record: Vec::new(),
        })
    }

    pub async fn write(&mut self, record: &impl Serialize) -> Result<(), Error> {
        self.record.clear();
        ciborium::into_writer(record, &mut self.record)
            .map_err(|_| Error::invalid("write Managed stream", "record cannot be encoded"))?;
        let encoded_length = u32::try_from(self.record.len())
            .map_err(|_| Error::invalid("write Managed stream", "one record is too large"))?;
        if self.record.len().saturating_add(size_of::<u32>()) > MAXIMUM_FRAME_BYTES {
            return Err(Error::invalid(
                "write Managed stream",
                "one metadata record exceeds the frame range unit",
            ));
        }
        if !self.frame.is_empty()
            && self
                .frame
                .len()
                .saturating_add(size_of::<u32>())
                .saturating_add(self.record.len())
                > MAXIMUM_FRAME_BYTES
        {
            self.flush_frame().await?;
        }
        self.frame.extend_from_slice(&encoded_length.to_le_bytes());
        self.frame.extend_from_slice(&self.record);
        self.frame_records = self.frame_records.checked_add(1).ok_or_else(|| {
            Error::invalid("write Managed stream", "frame record count overflows")
        })?;
        Ok(())
    }

    async fn flush_frame(&mut self) -> Result<(), Error> {
        if self.frame_records == 0 {
            return Ok(());
        }
        let frame = std::mem::take(&mut self.frame);
        write_frame(
            &mut self.writer,
            &mut self.payload_length,
            self.frame_records,
            frame,
        )
        .await?;
        self.frame_records = 0;
        Ok(())
    }

    pub async fn close(mut self) -> Result<StreamRef, Error> {
        self.flush_frame().await?;
        let payload_digest = self.writer.digest();
        finish_stream(self.writer, self.kind, self.payload_length, payload_digest).await
    }

    pub(crate) async fn abort(mut self) {
        let _ = self.writer.abort().await;
    }
}

pub async fn finish_stream(
    mut writer: ImmutableWriter,
    kind: StreamKind,
    payload_length: u64,
    digest: Digest,
) -> Result<StreamRef, Error> {
    let tail = encode_stream_tail(kind, payload_length, digest)?;
    writer.write(tail).await?;
    let object = writer.close().await?;
    Ok(StreamRef {
        kind,
        object,
        payload_length,
        payload_digest: digest,
    })
}

async fn write_frame(
    writer: &mut ImmutableWriter,
    payload_length: &mut u64,
    record_count: u32,
    frame: Vec<u8>,
) -> Result<(), Error> {
    let frame_length = u64::try_from(frame.len())
        .map_err(|_| Error::invalid("write Managed stream", "frame length overflows"))?;
    let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
    header.extend_from_slice(&FRAME_MAGIC);
    header.extend_from_slice(&frame_length.to_le_bytes());
    header.extend_from_slice(&record_count.to_le_bytes());
    header.extend_from_slice(checksum(&frame).as_bytes());
    writer.write(header).await?;
    writer.write(frame).await?;
    *payload_length = payload_length
        .checked_add(FRAME_HEADER_BYTES as u64)
        .and_then(|length| length.checked_add(frame_length))
        .ok_or_else(|| Error::invalid("write Managed stream", "payload length overflows"))?;
    Ok(())
}

async fn open_payload_reader(
    operator: &Operator,
    reference: StreamRef,
) -> Result<opendal::FuturesAsyncReader, Error> {
    operator
        .reader_with(&reference.object.key())
        .content_length_hint(reference.object.encoded_length)
        .await
        .map_err(|error| Error::from_storage("read Managed stream", error))?
        .into_futures_async_read(0..reference.object.encoded_length)
        .await
        .map_err(|error| Error::from_storage("read Managed stream", error))
}

async fn read_next_frame(
    reader: &mut opendal::FuturesAsyncReader,
    reference: StreamRef,
    offset: u64,
    frame: &mut Vec<u8>,
) -> Result<(u64, u32), Error> {
    let header_end = offset
        .checked_add(FRAME_HEADER_BYTES as u64)
        .filter(|end| *end <= reference.payload_length)
        .ok_or_else(|| Error::corrupt("read Managed stream", "frame header is truncated"))?;
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| Error::io("read Managed stream", error))?;
    if header[..4] != FRAME_MAGIC {
        return Err(Error::corrupt(
            "read Managed stream",
            "frame magic is invalid",
        ));
    }
    let payload_length = usize::try_from(u64::from_le_bytes(
        header[4..12].try_into().expect("fixed frame length"),
    ))
    .ok()
    .filter(|length| *length <= MAXIMUM_FRAME_BYTES)
    .ok_or_else(|| Error::corrupt("read Managed stream", "frame length is invalid"))?;
    let payload_end = header_end
        .checked_add(payload_length as u64)
        .filter(|end| *end <= reference.payload_length)
        .ok_or_else(|| Error::corrupt("read Managed stream", "frame payload is truncated"))?;
    frame.clear();
    frame.reserve(FRAME_HEADER_BYTES + payload_length);
    frame.extend_from_slice(&header);
    frame.resize(FRAME_HEADER_BYTES + payload_length, 0);
    reader
        .read_exact(&mut frame[FRAME_HEADER_BYTES..])
        .await
        .map_err(|error| Error::io("read Managed stream", error))?;
    let record_count = validate_frame(frame)?;
    Ok((payload_end, record_count))
}

fn validate_frame(bytes: &[u8]) -> Result<u32, Error> {
    if bytes.len() < FRAME_HEADER_BYTES || bytes[..4] != FRAME_MAGIC {
        return Err(Error::corrupt("read Managed stream", "frame is invalid"));
    }
    let payload_length = usize::try_from(u64::from_le_bytes(
        bytes[4..12].try_into().expect("fixed frame length"),
    ))
    .ok()
    .filter(|length| *length <= MAXIMUM_FRAME_BYTES)
    .ok_or_else(|| Error::corrupt("read Managed stream", "frame length is invalid"))?;
    if bytes.len() != FRAME_HEADER_BYTES + payload_length
        || checksum(&bytes[FRAME_HEADER_BYTES..]).as_bytes() != &bytes[16..48]
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "frame payload is invalid",
        ));
    }
    let record_count = u32::from_le_bytes(bytes[12..16].try_into().expect("fixed count"));
    if record_count as usize > payload_length / size_of::<u32>() {
        return Err(Error::corrupt(
            "read Managed stream",
            "frame record count is invalid",
        ));
    }
    Ok(record_count)
}

fn decode_record<T: DeserializeOwned>(frame: &[u8], offset: &mut usize) -> Result<T, Error> {
    let length_end = offset
        .checked_add(size_of::<u32>())
        .filter(|end| *end <= frame.len())
        .ok_or_else(|| Error::corrupt("read Managed stream", "record is truncated"))?;
    let length = u32::from_le_bytes(
        frame[*offset..length_end]
            .try_into()
            .expect("fixed record length"),
    ) as usize;
    let record_end = length_end
        .checked_add(length)
        .filter(|end| *end <= frame.len())
        .ok_or_else(|| Error::corrupt("read Managed stream", "record is truncated"))?;
    let mut input = Cursor::new(&frame[length_end..record_end]);
    let record = ciborium::from_reader(&mut input)
        .map_err(|_| Error::corrupt("read Managed stream", "record body is invalid"))?;
    if input.position() != length as u64 {
        return Err(Error::corrupt(
            "read Managed stream",
            "record has trailing bytes",
        ));
    }
    *offset = record_end;
    Ok(record)
}
