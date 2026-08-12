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

use std::io::Cursor;

use futures::AsyncReadExt as _;
use opendal::Operator;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;
use crate::filesystem::Digest;

use super::super::object::{GcEpoch, ObjectClass, checksum};
use super::super::storage::ImmutableWriter;
use super::{
    STREAM_TAIL_BYTES, StreamKind, StreamRef, finish_stream, validate_stream_layout,
    validate_stream_tail,
};

const FRAME_MAGIC: [u8; 4] = *b"OFSF";
const FRAME_HEADER_BYTES: usize = 4 + 8 + 4 + 32;
// A frame bounds one decode and checksum working set. It is not a persisted
// object-size limit: the stream contains as many frames as required.
const MAXIMUM_FRAME_BYTES: usize = 64 * 1024;

pub(crate) async fn write_records<T: Serialize>(
    operator: &Operator,
    gc_epoch: GcEpoch,
    class: ObjectClass,
    kind: StreamKind,
    records: impl IntoIterator<Item = T>,
) -> Result<StreamRef, Error> {
    let mut writer = RecordStreamWriter::open(operator, gc_epoch, class, kind).await?;
    for record in records {
        writer.write(&record).await?;
    }
    writer.close().await
}

pub(crate) struct RecordStreamReader<T> {
    reference: StreamRef,
    reader: opendal::FuturesAsyncReader,
    object_hasher: blake3::Hasher,
    offset: u64,
    records: std::vec::IntoIter<T>,
    completed: bool,
}

impl<T: DeserializeOwned> RecordStreamReader<T> {
    pub(crate) async fn open(operator: &Operator, reference: StreamRef) -> Result<Self, Error> {
        validate_stream_layout(reference)?;
        Ok(Self {
            reference,
            reader: open_payload_reader(operator, reference).await?,
            object_hasher: blake3::Hasher::new(),
            offset: 0,
            records: Vec::new().into_iter(),
            completed: false,
        })
    }

    pub(crate) async fn next(&mut self) -> Result<Option<T>, Error> {
        loop {
            if let Some(record) = self.records.next() {
                return Ok(Some(record));
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
            let (frame, end) =
                read_next_frame(&mut self.reader, self.reference, self.offset).await?;
            self.object_hasher.update(&frame);
            self.records = decode_frame(&frame)?.into_iter();
            self.offset = end;
        }
    }
}

pub(crate) struct RecordStreamWriter {
    writer: ImmutableWriter,
    kind: StreamKind,
    payload_length: u64,
    frame: Vec<u8>,
    frame_records: u32,
    record: Vec<u8>,
}

impl RecordStreamWriter {
    pub(crate) async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
        kind: StreamKind,
    ) -> Result<Self, Error> {
        Ok(Self {
            writer: ImmutableWriter::open(operator, gc_epoch, class).await?,
            kind,
            payload_length: 0,
            frame: Vec::new(),
            frame_records: 0,
            record: Vec::new(),
        })
    }

    pub(crate) async fn write(&mut self, record: &impl Serialize) -> Result<(), Error> {
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

    pub(crate) async fn close(mut self) -> Result<StreamRef, Error> {
        self.flush_frame().await?;
        let payload_digest = self.writer.digest();
        finish_stream(self.writer, self.kind, self.payload_length, payload_digest).await
    }
}

async fn write_frame(
    writer: &mut ImmutableWriter,
    payload_length: &mut u64,
    record_count: u32,
    frame: Vec<u8>,
) -> Result<(), Error> {
    let frame_length = u64::try_from(frame.len())
        .map_err(|_| Error::invalid("write Managed stream", "frame length overflows"))?;
    let mut encoded = Vec::with_capacity(FRAME_HEADER_BYTES + frame.len());
    encoded.extend_from_slice(&FRAME_MAGIC);
    encoded.extend_from_slice(&frame_length.to_le_bytes());
    encoded.extend_from_slice(&record_count.to_le_bytes());
    encoded.extend_from_slice(checksum(&frame).as_bytes());
    encoded.extend_from_slice(&frame);
    writer.write(encoded).await?;
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
) -> Result<(Vec<u8>, u64), Error> {
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
    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + payload_length);
    frame.extend_from_slice(&header);
    frame.resize(FRAME_HEADER_BYTES + payload_length, 0);
    reader
        .read_exact(&mut frame[FRAME_HEADER_BYTES..])
        .await
        .map_err(|error| Error::io("read Managed stream", error))?;
    Ok((frame, payload_end))
}

fn decode_frame<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
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
    let payload = &bytes[FRAME_HEADER_BYTES..];
    if record_count as usize > payload.len() / size_of::<u32>() {
        return Err(Error::corrupt(
            "read Managed stream",
            "frame record count is invalid",
        ));
    }
    let mut records = Vec::with_capacity(record_count as usize);
    let mut record_offset = 0_usize;
    for _ in 0..record_count {
        let length_end = record_offset
            .checked_add(size_of::<u32>())
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| Error::corrupt("read Managed stream", "record is truncated"))?;
        let length = u32::from_le_bytes(
            payload[record_offset..length_end]
                .try_into()
                .expect("fixed record length"),
        ) as usize;
        let record_end = length_end
            .checked_add(length)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| Error::corrupt("read Managed stream", "record is truncated"))?;
        let mut input = Cursor::new(&payload[length_end..record_end]);
        let record = ciborium::from_reader(&mut input)
            .map_err(|_| Error::corrupt("read Managed stream", "record body is invalid"))?;
        if input.position() != length as u64 {
            return Err(Error::corrupt(
                "read Managed stream",
                "record has trailing bytes",
            ));
        }
        records.push(record);
        record_offset = record_end;
    }
    if record_offset != payload.len() {
        return Err(Error::corrupt(
            "read Managed stream",
            "frame contains trailing bytes",
        ));
    }
    Ok(records)
}
