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

//! Self-delimiting immutable streams shared by metadata and file data.

use std::io::Cursor;

use futures::AsyncReadExt as _;
use opendal::Operator;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::Error;
use crate::filesystem::Digest;

use super::object::{GcEpoch, ImmutableWriter, ObjectClass, ObjectRef, checksum, read_range};

const FRAME_MAGIC: [u8; 4] = *b"OFSF";
const TRAILER_MAGIC: [u8; 8] = *b"OFSSTR01";
const FRAME_HEADER_BYTES: usize = 4 + 8 + 4 + 32;
const FOOTER_BYTES: usize = 8 + 32;
const TRAILER_BYTES: usize = 8 + 2 + 8 + 8 + 32 + 32;
const MAXIMUM_FRAME_BYTES: usize = 4 * 1024 * 1024;
const SOURCE_BUFFER_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct StreamKind(u16);

impl StreamKind {
    pub(crate) const NODE_MUTATIONS: Self = Self(1);
    pub(crate) const DIRECTORY_MUTATIONS: Self = Self(2);
    pub(crate) const FILE_VERSION_RECORDS: Self = Self(3);
    pub(crate) const CHANGE_RECORDS: Self = Self(4);
    pub(crate) const OPERATION_RESULTS: Self = Self(5);
    pub(crate) const FILE_BYTES: Self = Self(6);

    pub(crate) const fn value(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChecksummedRange {
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) checksum: crate::filesystem::Checksum,
}
super::wire::tuple_wire!(ChecksummedRange {
    offset: u64,
    length: u64,
    checksum: crate::filesystem::Checksum,
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StreamRef {
    pub(crate) kind: StreamKind,
    pub(crate) object: ObjectRef,
    pub(crate) payload_length: u64,
    pub(crate) payload_digest: Digest,
    pub(crate) footer_range: ChecksummedRange,
}
super::wire::tuple_wire!(StreamRef {
    kind: StreamKind,
    object: ObjectRef,
    payload_length: u64,
    payload_digest: Digest,
    footer_range: ChecksummedRange,
});

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

pub(crate) async fn rewrite_records<T: DeserializeOwned + Serialize>(
    operator: &Operator,
    references: &[StreamRef],
    gc_epoch: GcEpoch,
    class: ObjectClass,
    kind: StreamKind,
    mut keep: impl FnMut(&T) -> bool,
) -> Result<Option<StreamRef>, Error> {
    let mut writer = None;
    for reference in references {
        read_footer(operator, *reference).await?;
        let mut reader = open_payload_reader(operator, *reference).await?;
        let mut payload_hasher = blake3::Hasher::new();
        let mut offset = 0_u64;
        while offset < reference.payload_length {
            let (frame, end) = read_next_frame(&mut reader, *reference, offset).await?;
            payload_hasher.update(&frame);
            for record in decode_frame::<T>(&frame)? {
                if !keep(&record) {
                    continue;
                }
                if writer.is_none() {
                    writer = Some(RecordStreamWriter::open(operator, gc_epoch, class, kind).await?);
                }
                writer
                    .as_mut()
                    .expect("record writer is open")
                    .write(&record)
                    .await?;
            }
            offset = end;
        }
        if Digest::from_bytes(payload_hasher.finalize().into()) != reference.payload_digest {
            return Err(Error::corrupt(
                "read Managed stream",
                "stream payload does not match its reference",
            ));
        }
    }
    match writer {
        Some(writer) => writer.close().await.map(Some),
        None => Ok(None),
    }
}

struct RecordStreamWriter {
    writer: ImmutableWriter,
    kind: StreamKind,
    payload_hasher: blake3::Hasher,
    payload_length: u64,
    frame: Vec<u8>,
    frame_records: u32,
}

impl RecordStreamWriter {
    async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
        kind: StreamKind,
    ) -> Result<Self, Error> {
        Ok(Self {
            writer: ImmutableWriter::open(operator, gc_epoch, class).await?,
            kind,
            payload_hasher: blake3::Hasher::new(),
            payload_length: 0,
            frame: Vec::new(),
            frame_records: 0,
        })
    }

    async fn write(&mut self, record: &impl Serialize) -> Result<(), Error> {
        let mut encoded = Vec::new();
        ciborium::into_writer(record, &mut encoded)
            .map_err(|_| Error::invalid("write Managed stream", "record cannot be encoded"))?;
        let encoded_length = u32::try_from(encoded.len())
            .map_err(|_| Error::invalid("write Managed stream", "one record is too large"))?;
        if !self.frame.is_empty()
            && self
                .frame
                .len()
                .saturating_add(size_of::<u32>())
                .saturating_add(encoded.len())
                > MAXIMUM_FRAME_BYTES
        {
            self.flush_frame().await?;
        }
        self.frame.extend_from_slice(&encoded_length.to_le_bytes());
        self.frame.extend_from_slice(&encoded);
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
            &mut self.payload_hasher,
            &mut self.payload_length,
            self.frame_records,
            frame,
        )
        .await?;
        self.frame_records = 0;
        Ok(())
    }

    async fn close(mut self) -> Result<StreamRef, Error> {
        self.flush_frame().await?;
        finish_stream(
            self.writer,
            self.kind,
            self.payload_length,
            Digest::from_bytes(self.payload_hasher.finalize().into()),
        )
        .await
    }
}

pub(crate) async fn write_byte_stream(
    operator: &Operator,
    gc_epoch: GcEpoch,
    source: &mut (impl AsyncRead + Unpin),
    expected_length: u64,
    expected_digest: Digest,
) -> Result<StreamRef, Error> {
    let mut writer = ImmutableWriter::open(operator, gc_epoch, ObjectClass::FileData).await?;
    let mut payload_hasher = blake3::Hasher::new();
    let mut payload_length = 0_u64;
    loop {
        let mut bytes = vec![0; SOURCE_BUFFER_BYTES];
        let read = source
            .read(&mut bytes)
            .await
            .map_err(|error| Error::io("read Managed byte stream source", error))?;
        if read == 0 {
            break;
        }
        bytes.truncate(read);
        payload_hasher.update(&bytes);
        payload_length = payload_length
            .checked_add(read as u64)
            .ok_or_else(|| Error::invalid("write Managed byte stream", "length overflows"))?;
        writer.write(bytes).await?;
    }
    let payload_digest = Digest::from_bytes(payload_hasher.finalize().into());
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

pub(crate) async fn copy_byte_stream(
    operator: &Operator,
    reference: StreamRef,
    range: std::ops::Range<u64>,
    destination: &mut (impl AsyncWrite + Unpin),
) -> Result<(), Error> {
    if range.start > range.end || range.end > reference.payload_length {
        return Err(Error::invalid(
            "read Managed byte range",
            "logical byte range is invalid",
        ));
    }
    if range.is_empty() {
        return Ok(());
    }
    read_footer(operator, reference).await?;
    let mut stream = operator
        .reader_with(&reference.object.key())
        .content_length_hint(reference.object.encoded_length)
        .await
        .map_err(|error| Error::from_storage("read Managed byte stream", error))?
        .into_stream(range.clone())
        .await
        .map_err(|error| Error::from_storage("read Managed byte stream", error))?;
    let verify_payload = range.start == 0 && range.end == reference.payload_length;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    use futures::StreamExt as _;
    while let Some(buffer) = stream.next().await {
        let buffer =
            buffer.map_err(|error| Error::from_storage("read Managed byte stream", error))?;
        for chunk in buffer {
            if verify_payload {
                hasher.update(&chunk);
            }
            length = length
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| Error::corrupt("read Managed byte stream", "length overflows"))?;
            destination
                .write_all(&chunk)
                .await
                .map_err(|error| Error::io("write Managed stream destination", error))?;
        }
    }
    if length != range.end - range.start
        || verify_payload
            && Digest::from_bytes(hasher.finalize().into()) != reference.payload_digest
    {
        return Err(Error::corrupt(
            "read Managed byte stream",
            "payload does not match its reference",
        ));
    }
    Ok(())
}

async fn write_frame(
    writer: &mut ImmutableWriter,
    payload_hasher: &mut blake3::Hasher,
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
    payload_hasher.update(&header);
    payload_hasher.update(&frame);
    writer.write(header).await?;
    writer.write(frame).await?;
    *payload_length = payload_length
        .checked_add(FRAME_HEADER_BYTES as u64)
        .and_then(|length| length.checked_add(frame_length))
        .ok_or_else(|| Error::invalid("write Managed stream", "payload length overflows"))?;
    Ok(())
}

pub(crate) async fn visit_records<T: DeserializeOwned>(
    operator: &Operator,
    reference: StreamRef,
    mut visit: impl FnMut(T) -> Result<(), Error>,
) -> Result<(), Error> {
    read_footer(operator, reference).await?;
    let mut reader = open_payload_reader(operator, reference).await?;
    let mut payload_hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    while offset < reference.payload_length {
        let (frame, end) = read_next_frame(&mut reader, reference, offset).await?;
        payload_hasher.update(&frame);
        for record in decode_frame(&frame)? {
            visit(record)?;
        }
        offset = end;
    }
    if Digest::from_bytes(payload_hasher.finalize().into()) != reference.payload_digest {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream payload does not match its reference",
        ));
    }
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
        .into_futures_async_read(0..reference.payload_length)
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

async fn finish_stream(
    mut writer: ImmutableWriter,
    kind: StreamKind,
    payload_length: u64,
    digest: Digest,
) -> Result<StreamRef, Error> {
    let (tail, footer_range) = encode_stream_tail(kind, payload_length, digest)?;
    writer.write(tail).await?;
    let object = writer.close().await?;
    Ok(StreamRef {
        kind,
        object,
        payload_length,
        payload_digest: digest,
        footer_range,
    })
}

fn encode_stream_tail(
    kind: StreamKind,
    payload_length: u64,
    digest: Digest,
) -> Result<(Vec<u8>, ChecksummedRange), Error> {
    let mut tail = Vec::new();
    let footer_offset = payload_length;
    let mut footer = Vec::with_capacity(FOOTER_BYTES);
    footer.extend_from_slice(&payload_length.to_le_bytes());
    footer.extend_from_slice(digest.as_bytes());
    let footer_length = FOOTER_BYTES as u64;
    let footer_checksum = checksum(&footer);
    tail.extend_from_slice(&footer);
    let mut trailer = Vec::with_capacity(TRAILER_BYTES);
    trailer.extend_from_slice(&TRAILER_MAGIC);
    trailer.extend_from_slice(&kind.value().to_le_bytes());
    trailer.extend_from_slice(&footer_offset.to_le_bytes());
    trailer.extend_from_slice(&footer_length.to_le_bytes());
    trailer.extend_from_slice(footer_checksum.as_bytes());
    trailer.extend_from_slice(checksum(&trailer).as_bytes());
    tail.extend_from_slice(&trailer);
    Ok((
        tail,
        ChecksummedRange {
            offset: footer_offset,
            length: footer_length,
            checksum: footer_checksum,
        },
    ))
}

async fn read_footer(operator: &Operator, reference: StreamRef) -> Result<(), Error> {
    let trailer_start = reference
        .object
        .encoded_length
        .checked_sub(TRAILER_BYTES as u64)
        .ok_or_else(|| Error::corrupt("read Managed stream", "stream trailer is missing"))?;
    if reference.footer_range.length != FOOTER_BYTES as u64
        || reference
            .footer_range
            .offset
            .checked_add(reference.footer_range.length)
            != Some(trailer_start)
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream footer range is invalid",
        ));
    }
    let tail = read_range(
        operator,
        reference.object,
        reference.footer_range.offset..reference.object.encoded_length,
    )
    .await?;
    let (footer_bytes, trailer) = tail.split_at(FOOTER_BYTES);
    if trailer.len() != TRAILER_BYTES || trailer[..8] != TRAILER_MAGIC {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream trailer is invalid",
        ));
    }
    if checksum(&trailer[..TRAILER_BYTES - 32]).as_bytes() != &trailer[TRAILER_BYTES - 32..] {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream trailer checksum is invalid",
        ));
    }
    let kind = StreamKind(u16::from_le_bytes(
        trailer[8..10].try_into().expect("fixed stream kind"),
    ));
    let footer_offset = u64::from_le_bytes(trailer[10..18].try_into().expect("fixed offset"));
    let encoded_footer_length =
        u64::from_le_bytes(trailer[18..26].try_into().expect("fixed length"));
    if kind != reference.kind
        || footer_offset != reference.footer_range.offset
        || encoded_footer_length != reference.footer_range.length
        || &trailer[26..58] != reference.footer_range.checksum.as_bytes()
        || footer_offset.checked_add(encoded_footer_length) != Some(trailer_start)
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream trailer does not match its reference",
        ));
    }
    if checksum(footer_bytes) != reference.footer_range.checksum {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream footer checksum is invalid",
        ));
    }
    let payload_length =
        u64::from_le_bytes(footer_bytes[..8].try_into().expect("fixed payload length"));
    if payload_length != reference.payload_length
        || &footer_bytes[8..] != reference.payload_digest.as_bytes()
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream footer does not match its reference",
        ));
    }
    Ok(())
}
