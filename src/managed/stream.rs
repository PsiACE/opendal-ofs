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
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use crate::Error;

use super::object::{
    GcEpoch, ImmutableWriter, ObjectClass, ObjectRef, PayloadDigest, RangeChecksum, checksum,
    read_range,
};

const FRAME_MAGIC: [u8; 4] = *b"OFSF";
const TRAILER_MAGIC: [u8; 8] = *b"OFSSTR01";
const FRAME_HEADER_BYTES: usize = 4 + 2 + 8 + 4 + 32;
const TRAILER_BYTES: usize = 8 + 2 + 2 + 8 + 8 + 32 + 32;
const MAXIMUM_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAXIMUM_FOOTER_BYTES: usize = 16 * 1024 * 1024;
const WRITER_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const DATA_BLOCK_BYTES: u64 = 4 * 1024 * 1024;
const RANGE_FETCH_BLOCKS: usize = 16;
const BLOCK_CHECKSUM_INDEX: &str = "block-checksum-index";
const FRAME_RANGE_INDEX: &str = "frame-range-index";

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct StreamKind(u16);

impl StreamKind {
    pub(crate) const NODE_MUTATIONS: Self = Self(1);
    pub(crate) const DIRECTORY_MUTATIONS: Self = Self(2);
    pub(crate) const FILE_VERSION_RECORDS: Self = Self(3);
    pub(crate) const CHANGE_RECORDS: Self = Self(4);
    pub(crate) const OPERATION_RESULTS: Self = Self(5);
    pub(crate) const FILE_EXTENT_RECORDS: Self = Self(6);
    pub(crate) const FILE_BYTES: Self = Self(9);

    pub(crate) const fn value(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChecksummedRange {
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) checksum: RangeChecksum,
}
super::wire::tuple_wire!(ChecksummedRange {
    offset: u64,
    length: u64,
    checksum: RangeChecksum,
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StreamRef {
    pub(crate) kind: StreamKind,
    pub(crate) schema_version: u16,
    pub(crate) object: ObjectRef,
    pub(crate) payload_length: u64,
    pub(crate) payload_digest: PayloadDigest,
    pub(crate) footer_range: ChecksummedRange,
}
super::wire::tuple_wire!(StreamRef {
    kind: StreamKind,
    schema_version: u16,
    object: ObjectRef,
    payload_length: u64,
    payload_digest: PayloadDigest,
    footer_range: ChecksummedRange,
});

#[derive(Debug)]
struct StreamFooter {
    payload_length: u64,
    payload_digest: PayloadDigest,
    projections: Vec<EmbeddedProjectionRef>,
}
super::wire::tuple_wire!(StreamFooter {
    payload_length: u64,
    payload_digest: PayloadDigest,
    projections: Vec<EmbeddedProjectionRef>,
});

#[derive(Debug)]
struct EmbeddedProjectionRef {
    kind: String,
    schema_version: u16,
    source: PayloadDigest,
    object_range: ChecksummedRange,
}
super::wire::tuple_wire!(EmbeddedProjectionRef {
    kind: String,
    schema_version: u16,
    source: PayloadDigest,
    object_range: ChecksummedRange,
});

#[derive(Debug)]
struct DataBlockIndex {
    blocks: Vec<ChecksummedRange>,
}
super::wire::tuple_wire!(DataBlockIndex {
    blocks: Vec<ChecksummedRange>,
});

struct EmbeddedProjection {
    kind: &'static str,
    schema_version: u16,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct FrameRangeIndex {
    frames: Vec<ChecksummedRange>,
}
super::wire::tuple_wire!(FrameRangeIndex {
    frames: Vec<ChecksummedRange>,
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
        let footer = read_footer(operator, *reference).await?;
        let mut reader = open_payload_reader(operator, *reference).await?;
        let mut payload_hasher = blake3::Hasher::new();
        let mut offset = 0_u64;
        while offset < reference.payload_length {
            let (frame, end) = read_next_frame(&mut reader, *reference, offset).await?;
            payload_hasher.update(&frame);
            for record in decode_frame::<T>(&frame, reference.schema_version)? {
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
        if PayloadDigest::from_bytes(payload_hasher.finalize().into()) != footer.payload_digest {
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
    schema_version: u16,
    payload_hasher: blake3::Hasher,
    payload_length: u64,
    frame: Vec<u8>,
    frame_records: u32,
    frames: Vec<ChecksummedRange>,
}

impl RecordStreamWriter {
    async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
        kind: StreamKind,
    ) -> Result<Self, Error> {
        Ok(Self {
            writer: ImmutableWriter::open(operator, gc_epoch, class, WRITER_CHUNK_BYTES).await?,
            kind,
            schema_version: 1,
            payload_hasher: blake3::Hasher::new(),
            payload_length: 0,
            frame: Vec::new(),
            frame_records: 0,
            frames: Vec::new(),
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
        self.frames.push(
            write_frame(
                &mut self.writer,
                self.schema_version,
                &mut self.payload_hasher,
                &mut self.payload_length,
                self.frame_records,
                &self.frame,
            )
            .await?,
        );
        self.frame.clear();
        self.frame_records = 0;
        Ok(())
    }

    async fn close(mut self) -> Result<StreamRef, Error> {
        self.flush_frame().await?;
        let mut frame_index = Vec::new();
        ciborium::into_writer(
            &FrameRangeIndex {
                frames: self.frames,
            },
            &mut frame_index,
        )
        .map_err(|_| Error::invalid("write Managed stream", "frame index cannot be encoded"))?;
        finish_stream(
            self.writer,
            self.kind,
            self.schema_version,
            self.payload_length,
            PayloadDigest::from_bytes(self.payload_hasher.finalize().into()),
            vec![EmbeddedProjection {
                kind: FRAME_RANGE_INDEX,
                schema_version: 1,
                bytes: frame_index,
            }],
        )
        .await
    }
}

pub(crate) async fn write_bytes(
    operator: &Operator,
    gc_epoch: GcEpoch,
    class: ObjectClass,
    kind: StreamKind,
    bytes: Vec<u8>,
) -> Result<StreamRef, Error> {
    let schema_version = 1_u16;
    let payload_length = bytes.len() as u64;
    let payload_digest = PayloadDigest::from_bytes(blake3::hash(&bytes).into());
    let blocks = bytes
        .chunks(DATA_BLOCK_BYTES as usize)
        .enumerate()
        .map(|(index, block)| ChecksummedRange {
            offset: index as u64 * DATA_BLOCK_BYTES,
            length: block.len() as u64,
            checksum: checksum(block),
        })
        .collect();
    let mut block_index = Vec::new();
    ciborium::into_writer(&DataBlockIndex { blocks }, &mut block_index)
        .map_err(|_| Error::invalid("write Managed stream", "block index cannot be encoded"))?;
    let (tail, footer_range) = encode_stream_tail(
        kind,
        schema_version,
        payload_length,
        payload_digest,
        vec![EmbeddedProjection {
            kind: BLOCK_CHECKSUM_INDEX,
            schema_version: 1,
            bytes: block_index,
        }],
    )?;

    let mut encoded = bytes;
    encoded.reserve(tail.len());
    encoded.extend_from_slice(&tail);
    let object = super::object::write_immutable(operator, gc_epoch, class, encoded).await?;
    Ok(StreamRef {
        kind,
        schema_version,
        object,
        payload_length,
        payload_digest,
        footer_range,
    })
}

pub(crate) async fn copy_bytes(
    operator: &Operator,
    reference: StreamRef,
    destination: &mut (impl AsyncWrite + Unpin),
) -> Result<(), Error> {
    let footer = read_footer(operator, reference).await?;
    let key = reference.object.key();
    let mut stream = operator
        .reader(&key)
        .await
        .map_err(|error| Error::from_storage("read Managed byte stream", error))?
        .into_stream(0..reference.payload_length)
        .await
        .map_err(|error| Error::from_storage("read Managed byte stream", error))?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    use futures::StreamExt as _;
    while let Some(buffer) = stream.next().await {
        let buffer =
            buffer.map_err(|error| Error::from_storage("read Managed byte stream", error))?;
        for chunk in buffer {
            hasher.update(&chunk);
            length = length
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| Error::corrupt("read Managed byte stream", "length overflows"))?;
            destination
                .write_all(&chunk)
                .await
                .map_err(|error| Error::io("write Managed stream destination", error))?;
        }
    }
    if length != reference.payload_length
        || PayloadDigest::from_bytes(hasher.finalize().into()) != footer.payload_digest
    {
        return Err(Error::corrupt(
            "read Managed byte stream",
            "payload does not match its reference",
        ));
    }
    Ok(())
}

pub(crate) async fn copy_byte_range(
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
    let footer = read_footer(operator, reference).await?;
    let Some(projection) = footer.projections.iter().find(|projection| {
        projection.kind == BLOCK_CHECKSUM_INDEX
            && projection.schema_version == 1
            && projection.source == reference.payload_digest
    }) else {
        return copy_byte_range_by_scanning(operator, reference, range, destination).await;
    };
    let projection_end = projection
        .object_range
        .offset
        .checked_add(projection.object_range.length);
    if projection.object_range.offset < reference.payload_length
        || projection_end.is_none_or(|end| end > reference.footer_range.offset)
    {
        return copy_byte_range_by_scanning(operator, reference, range, destination).await;
    }
    let bytes = match read_range(
        operator,
        reference.object,
        projection.object_range.offset
            ..projection.object_range.offset + projection.object_range.length,
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(_) => {
            return copy_byte_range_by_scanning(operator, reference, range, destination).await;
        }
    };
    if checksum(&bytes) != projection.object_range.checksum {
        return copy_byte_range_by_scanning(operator, reference, range, destination).await;
    }
    let Ok(index) = ciborium::from_reader::<DataBlockIndex, _>(Cursor::new(bytes)) else {
        return copy_byte_range_by_scanning(operator, reference, range, destination).await;
    };
    if !valid_block_index(reference.payload_length, &index.blocks) {
        return copy_byte_range_by_scanning(operator, reference, range, destination).await;
    }
    let selected = index
        .blocks
        .into_iter()
        .filter(|block| {
            block.offset < range.end && block.offset.saturating_add(block.length) > range.start
        })
        .collect::<Vec<_>>();
    let reader = operator
        .reader_with(&reference.object.key())
        .content_length_hint(reference.object.encoded_length)
        .concurrent(4)
        .gap(0)
        .await
        .map_err(|error| Error::from_storage("read Managed byte range", error))?;
    for batch in selected.chunks(RANGE_FETCH_BLOCKS) {
        let ranges = batch
            .iter()
            .map(|block| block.offset..block.offset + block.length)
            .collect();
        let buffers = reader
            .fetch(ranges)
            .await
            .map_err(|error| Error::from_storage("read Managed byte range", error))?;
        for (block, buffer) in batch.iter().zip(buffers) {
            let bytes = buffer.to_vec();
            if bytes.len() as u64 != block.length || checksum(&bytes) != block.checksum {
                return Err(Error::corrupt(
                    "read Managed byte range",
                    "file data block is invalid",
                ));
            }
            let start = range.start.saturating_sub(block.offset) as usize;
            let end = (range.end.min(block.offset + block.length) - block.offset) as usize;
            destination
                .write_all(&bytes[start..end])
                .await
                .map_err(|error| Error::io("write Managed byte range", error))?;
        }
    }
    Ok(())
}

fn valid_block_index(payload_length: u64, blocks: &[ChecksummedRange]) -> bool {
    let mut covered = 0_u64;
    for block in blocks {
        let Some(end) = block.offset.checked_add(block.length) else {
            return false;
        };
        if block.offset != covered || block.length == 0 || end > payload_length {
            return false;
        }
        covered = end;
    }
    covered == payload_length
}

async fn copy_byte_range_by_scanning(
    operator: &Operator,
    reference: StreamRef,
    range: std::ops::Range<u64>,
    destination: &mut (impl AsyncWrite + Unpin),
) -> Result<(), Error> {
    let key = reference.object.key();
    let mut stream = operator
        .reader(&key)
        .await
        .map_err(|error| Error::from_storage("read Managed byte range", error))?
        .into_stream(0..reference.payload_length)
        .await
        .map_err(|error| Error::from_storage("read Managed byte range", error))?;
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    use futures::StreamExt as _;
    while let Some(buffer) = stream.next().await {
        let buffer =
            buffer.map_err(|error| Error::from_storage("read Managed byte range", error))?;
        for chunk in buffer {
            let end = offset
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| Error::corrupt("read Managed byte range", "length overflows"))?;
            hasher.update(&chunk);
            if offset < range.end && end > range.start {
                let start = range.start.saturating_sub(offset) as usize;
                let selected_end = (range.end.min(end) - offset) as usize;
                destination
                    .write_all(&chunk[start..selected_end])
                    .await
                    .map_err(|error| Error::io("write Managed byte range", error))?;
            }
            offset = end;
        }
    }
    if offset != reference.payload_length
        || PayloadDigest::from_bytes(hasher.finalize().into()) != reference.payload_digest
    {
        return Err(Error::corrupt(
            "read Managed byte range",
            "payload does not match its reference",
        ));
    }
    Ok(())
}

async fn write_frame(
    writer: &mut ImmutableWriter,
    schema_version: u16,
    payload_hasher: &mut blake3::Hasher,
    payload_length: &mut u64,
    record_count: u32,
    frame: &[u8],
) -> Result<ChecksummedRange, Error> {
    let offset = *payload_length;
    let frame_length = u64::try_from(frame.len())
        .map_err(|_| Error::invalid("write Managed stream", "frame length overflows"))?;
    let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
    header.extend_from_slice(&FRAME_MAGIC);
    header.extend_from_slice(&schema_version.to_le_bytes());
    header.extend_from_slice(&frame_length.to_le_bytes());
    header.extend_from_slice(&record_count.to_le_bytes());
    header.extend_from_slice(checksum(frame).as_bytes());
    let mut frame_checksum = blake3::Hasher::new();
    frame_checksum.update(&header);
    frame_checksum.update(frame);
    writer.write(&header).await?;
    writer.write(frame).await?;
    payload_hasher.update(&header);
    payload_hasher.update(frame);
    *payload_length = payload_length
        .checked_add(header.len() as u64)
        .and_then(|length| length.checked_add(frame_length))
        .ok_or_else(|| Error::invalid("write Managed stream", "payload length overflows"))?;
    Ok(ChecksummedRange {
        offset,
        length: header.len() as u64 + frame_length,
        checksum: RangeChecksum::from_bytes(frame_checksum.finalize().into()),
    })
}

pub(crate) async fn visit_records<T: DeserializeOwned>(
    operator: &Operator,
    reference: StreamRef,
    mut visit: impl FnMut(T) -> Result<(), Error>,
) -> Result<(), Error> {
    let footer = read_footer(operator, reference).await?;
    let mut reader = open_payload_reader(operator, reference).await?;
    let mut payload_hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    while offset < reference.payload_length {
        let (frame, end) = read_next_frame(&mut reader, reference, offset).await?;
        payload_hasher.update(&frame);
        for record in decode_frame(&frame, reference.schema_version)? {
            visit(record)?;
        }
        offset = end;
    }
    if PayloadDigest::from_bytes(payload_hasher.finalize().into()) != footer.payload_digest {
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
        header[6..14].try_into().expect("fixed frame length"),
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

fn decode_frame<T: DeserializeOwned>(bytes: &[u8], schema_version: u16) -> Result<Vec<T>, Error> {
    if bytes.len() < FRAME_HEADER_BYTES || bytes[..4] != FRAME_MAGIC {
        return Err(Error::corrupt("read Managed stream", "frame is invalid"));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("fixed version"));
    if version != schema_version {
        return Err(Error::unsupported(
            "read Managed stream",
            "frame schema version is unsupported",
        ));
    }
    let payload_length = usize::try_from(u64::from_le_bytes(
        bytes[6..14].try_into().expect("fixed frame length"),
    ))
    .ok()
    .filter(|length| *length <= MAXIMUM_FRAME_BYTES)
    .ok_or_else(|| Error::corrupt("read Managed stream", "frame length is invalid"))?;
    if bytes.len() != FRAME_HEADER_BYTES + payload_length
        || checksum(&bytes[FRAME_HEADER_BYTES..]).as_bytes() != &bytes[18..50]
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "frame payload is invalid",
        ));
    }
    let record_count = u32::from_le_bytes(bytes[14..18].try_into().expect("fixed count"));
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
    schema_version: u16,
    payload_length: u64,
    digest: PayloadDigest,
    projections: Vec<EmbeddedProjection>,
) -> Result<StreamRef, Error> {
    let (tail, footer_range) =
        encode_stream_tail(kind, schema_version, payload_length, digest, projections)?;
    writer.write(&tail).await?;
    let object = writer.close().await?;
    Ok(StreamRef {
        kind,
        schema_version,
        object,
        payload_length,
        payload_digest: digest,
        footer_range,
    })
}

fn encode_stream_tail(
    kind: StreamKind,
    schema_version: u16,
    payload_length: u64,
    digest: PayloadDigest,
    projections: Vec<EmbeddedProjection>,
) -> Result<(Vec<u8>, ChecksummedRange), Error> {
    let mut tail = Vec::new();
    let mut projection_refs = Vec::with_capacity(projections.len());
    let mut footer_offset = payload_length;
    for projection in projections {
        let length = projection.bytes.len() as u64;
        let object_range = ChecksummedRange {
            offset: footer_offset,
            length,
            checksum: checksum(&projection.bytes),
        };
        tail.extend_from_slice(&projection.bytes);
        footer_offset = footer_offset
            .checked_add(length)
            .ok_or_else(|| Error::invalid("write Managed stream", "stream length overflows"))?;
        projection_refs.push(EmbeddedProjectionRef {
            kind: projection.kind.to_owned(),
            schema_version: projection.schema_version,
            source: digest,
            object_range,
        });
    }
    let mut footer = Vec::new();
    ciborium::into_writer(
        &StreamFooter {
            payload_length,
            payload_digest: digest,
            projections: projection_refs,
        },
        &mut footer,
    )
    .map_err(|_| Error::invalid("write Managed stream", "footer cannot be encoded"))?;
    if footer.len() > MAXIMUM_FOOTER_BYTES {
        return Err(Error::invalid(
            "write Managed stream",
            "footer exceeds its size limit",
        ));
    }
    let footer_length = footer.len() as u64;
    let footer_checksum = checksum(&footer);
    tail.extend_from_slice(&footer);
    let mut trailer = Vec::with_capacity(TRAILER_BYTES);
    trailer.extend_from_slice(&TRAILER_MAGIC);
    trailer.extend_from_slice(&schema_version.to_le_bytes());
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

async fn read_footer(operator: &Operator, reference: StreamRef) -> Result<StreamFooter, Error> {
    let trailer_start = reference
        .object
        .encoded_length
        .checked_sub(TRAILER_BYTES as u64)
        .ok_or_else(|| Error::corrupt("read Managed stream", "stream trailer is missing"))?;
    if reference.footer_range.length > MAXIMUM_FOOTER_BYTES as u64
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
    let footer_length = usize::try_from(reference.footer_range.length)
        .map_err(|_| Error::corrupt("read Managed stream", "stream footer is too large"))?;
    let (footer_bytes, trailer) = tail.split_at(footer_length);
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
    let schema_version = u16::from_le_bytes(trailer[8..10].try_into().expect("fixed version"));
    let kind = StreamKind(u16::from_le_bytes(
        trailer[10..12].try_into().expect("fixed stream kind"),
    ));
    let footer_offset = u64::from_le_bytes(trailer[12..20].try_into().expect("fixed offset"));
    let encoded_footer_length =
        u64::from_le_bytes(trailer[20..28].try_into().expect("fixed length"));
    if schema_version != reference.schema_version
        || kind != reference.kind
        || footer_offset != reference.footer_range.offset
        || encoded_footer_length != reference.footer_range.length
        || &trailer[28..60] != reference.footer_range.checksum.as_bytes()
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
    let footer: StreamFooter = ciborium::from_reader(Cursor::new(footer_bytes))
        .map_err(|_| Error::corrupt("read Managed stream", "stream footer is invalid"))?;
    if footer.payload_length != reference.payload_length
        || footer.payload_digest != reference.payload_digest
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream footer does not match its reference",
        ));
    }
    Ok(footer)
}
