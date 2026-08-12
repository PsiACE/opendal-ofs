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

use opendal::Operator;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

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

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct StreamKind(u16);

impl StreamKind {
    pub(crate) const NODE_MUTATIONS: Self = Self(1);
    pub(crate) const DIRECTORY_MUTATIONS: Self = Self(2);
    pub(crate) const FILE_VERSION_RECORDS: Self = Self(3);
    pub(crate) const CHANGE_RECORDS: Self = Self(4);
    pub(crate) const OPERATION_RESULTS: Self = Self(5);
    pub(crate) const FILE_EXTENT_MUTATIONS: Self = Self(6);
    pub(crate) const FILE_BYTES: Self = Self(9);

    pub(crate) const fn value(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ChecksummedRange {
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) checksum: RangeChecksum,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StreamRef {
    pub(crate) kind: StreamKind,
    pub(crate) schema_version: u16,
    pub(crate) object: ObjectRef,
    pub(crate) payload_length: u64,
    pub(crate) payload_digest: PayloadDigest,
    pub(crate) footer: ChecksummedRange,
}

#[derive(serde::Deserialize, Serialize)]
struct StreamFooter {
    payload_length: u64,
    payload_digest: PayloadDigest,
    projections: Vec<EmbeddedProjectionRef>,
}

#[derive(serde::Deserialize, Serialize)]
struct EmbeddedProjectionRef {
    kind: String,
    schema_version: u16,
    source: PayloadDigest,
    object_range: ChecksummedRange,
}

#[derive(serde::Deserialize, Serialize)]
struct DataBlockIndex {
    blocks: Vec<ChecksummedRange>,
}

struct EmbeddedProjection {
    kind: &'static str,
    schema_version: u16,
    bytes: Vec<u8>,
}

pub(crate) async fn write_records<T: Serialize>(
    operator: &Operator,
    gc_epoch: GcEpoch,
    class: ObjectClass,
    kind: StreamKind,
    records: impl IntoIterator<Item = T>,
) -> Result<StreamRef, Error> {
    let schema_version = 1;
    let mut writer = ImmutableWriter::open(operator, gc_epoch, class, WRITER_CHUNK_BYTES).await?;
    let mut payload_hasher = blake3::Hasher::new();
    let mut payload_length = 0_u64;
    let mut frame = Vec::new();
    let mut frame_records = 0_u32;

    for record in records {
        let mut encoded = Vec::new();
        ciborium::into_writer(&record, &mut encoded)
            .map_err(|_| Error::invalid("write Managed stream", "record cannot be encoded"))?;
        let encoded_length = u32::try_from(encoded.len())
            .map_err(|_| Error::invalid("write Managed stream", "one record is too large"))?;
        if !frame.is_empty()
            && frame
                .len()
                .saturating_add(size_of::<u32>())
                .saturating_add(encoded.len())
                > MAXIMUM_FRAME_BYTES
        {
            write_frame(
                &mut writer,
                schema_version,
                &mut payload_hasher,
                &mut payload_length,
                frame_records,
                &frame,
            )
            .await?;
            frame.clear();
            frame_records = 0;
        }
        frame.extend_from_slice(&encoded_length.to_le_bytes());
        frame.extend_from_slice(&encoded);
        frame_records = frame_records.checked_add(1).ok_or_else(|| {
            Error::invalid("write Managed stream", "frame record count overflows")
        })?;
    }
    if frame_records != 0 {
        write_frame(
            &mut writer,
            schema_version,
            &mut payload_hasher,
            &mut payload_length,
            frame_records,
            &frame,
        )
        .await?;
    }

    finish_stream(
        writer,
        kind,
        schema_version,
        payload_length,
        PayloadDigest::from_bytes(payload_hasher.finalize().into()),
        Vec::new(),
    )
    .await
}

pub(crate) async fn write_bytes(
    operator: &Operator,
    gc_epoch: GcEpoch,
    class: ObjectClass,
    kind: StreamKind,
    source: &mut (impl AsyncRead + Unpin),
) -> Result<StreamRef, Error> {
    let schema_version = 1;
    let mut writer = ImmutableWriter::open(operator, gc_epoch, class, WRITER_CHUNK_BYTES).await?;
    let mut hasher = blake3::Hasher::new();
    let mut payload_length = 0_u64;
    let mut block_hasher = blake3::Hasher::new();
    let mut block_offset = 0_u64;
    let mut block_length = 0_u64;
    let mut blocks = Vec::new();
    let mut buffer = vec![0; 256 * 1024];
    loop {
        let read = source
            .read(&mut buffer)
            .await
            .map_err(|error| Error::io("read Managed stream source", error))?;
        if read == 0 {
            break;
        }
        let bytes = &buffer[..read];
        writer.write(bytes).await?;
        hasher.update(bytes);
        let mut observed = bytes;
        while !observed.is_empty() {
            let remaining = (DATA_BLOCK_BYTES - block_length) as usize;
            let length = remaining.min(observed.len());
            block_hasher.update(&observed[..length]);
            block_length += length as u64;
            observed = &observed[length..];
            if block_length == DATA_BLOCK_BYTES {
                blocks.push(ChecksummedRange {
                    offset: block_offset,
                    length: block_length,
                    checksum: RangeChecksum::from_bytes(block_hasher.finalize().into()),
                });
                block_offset += block_length;
                block_length = 0;
                block_hasher = blake3::Hasher::new();
            }
        }
        payload_length = payload_length
            .checked_add(read as u64)
            .ok_or_else(|| Error::invalid("write Managed stream", "payload length overflows"))?;
    }
    if block_length != 0 {
        blocks.push(ChecksummedRange {
            offset: block_offset,
            length: block_length,
            checksum: RangeChecksum::from_bytes(block_hasher.finalize().into()),
        });
    }
    let mut block_index = Vec::new();
    ciborium::into_writer(&DataBlockIndex { blocks }, &mut block_index)
        .map_err(|_| Error::invalid("write Managed stream", "block index cannot be encoded"))?;
    finish_stream(
        writer,
        kind,
        schema_version,
        payload_length,
        PayloadDigest::from_bytes(hasher.finalize().into()),
        vec![EmbeddedProjection {
            kind: BLOCK_CHECKSUM_INDEX,
            schema_version: 1,
            bytes: block_index,
        }],
    )
    .await
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
        || projection_end.is_none_or(|end| end > reference.footer.offset)
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
) -> Result<(), Error> {
    let frame_length = u64::try_from(frame.len())
        .map_err(|_| Error::invalid("write Managed stream", "frame length overflows"))?;
    let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
    header.extend_from_slice(&FRAME_MAGIC);
    header.extend_from_slice(&schema_version.to_le_bytes());
    header.extend_from_slice(&frame_length.to_le_bytes());
    header.extend_from_slice(&record_count.to_le_bytes());
    header.extend_from_slice(checksum(frame).as_bytes());
    writer.write(&header).await?;
    writer.write(frame).await?;
    payload_hasher.update(&header);
    payload_hasher.update(frame);
    *payload_length = payload_length
        .checked_add(header.len() as u64)
        .and_then(|length| length.checked_add(frame_length))
        .ok_or_else(|| Error::invalid("write Managed stream", "payload length overflows"))?;
    Ok(())
}

pub(crate) async fn read_records<T: DeserializeOwned>(
    operator: &Operator,
    reference: StreamRef,
) -> Result<Vec<T>, Error> {
    let mut records = Vec::new();
    visit_records(operator, reference, |record| {
        records.push(record);
        Ok(())
    })
    .await?;
    Ok(records)
}

pub(crate) async fn visit_records<T: DeserializeOwned>(
    operator: &Operator,
    reference: StreamRef,
    mut visit: impl FnMut(T) -> Result<(), Error>,
) -> Result<(), Error> {
    let footer = read_footer(operator, reference).await?;
    let mut payload_hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    while offset < reference.payload_length {
        let header_end = offset
            .checked_add(FRAME_HEADER_BYTES as u64)
            .filter(|end| *end <= reference.payload_length)
            .ok_or_else(|| Error::corrupt("read Managed stream", "frame header is truncated"))?;
        let header = read_range(operator, reference.object, offset..header_end).await?;
        if header[..4] != FRAME_MAGIC {
            return Err(Error::corrupt(
                "read Managed stream",
                "frame magic is invalid",
            ));
        }
        let version = u16::from_le_bytes(header[4..6].try_into().expect("fixed version"));
        if version != reference.schema_version {
            return Err(Error::unsupported(
                "read Managed stream",
                "frame schema version is unsupported",
            ));
        }
        let payload_length = usize::try_from(u64::from_le_bytes(
            header[6..14].try_into().expect("fixed frame length"),
        ))
        .ok()
        .filter(|length| *length <= MAXIMUM_FRAME_BYTES)
        .ok_or_else(|| Error::corrupt("read Managed stream", "frame length is invalid"))?;
        let record_count = u32::from_le_bytes(header[14..18].try_into().expect("fixed count"));
        let payload_end = header_end
            .checked_add(payload_length as u64)
            .filter(|end| *end <= reference.payload_length)
            .ok_or_else(|| Error::corrupt("read Managed stream", "frame payload is truncated"))?;
        let payload = read_range(operator, reference.object, header_end..payload_end).await?;
        if checksum(&payload).as_bytes() != &header[18..50] {
            return Err(Error::corrupt(
                "read Managed stream",
                "frame checksum is invalid",
            ));
        }
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
            visit(record)?;
            record_offset = record_end;
        }
        if record_offset != payload.len() {
            return Err(Error::corrupt(
                "read Managed stream",
                "frame contains trailing bytes",
            ));
        }
        payload_hasher.update(&header);
        payload_hasher.update(&payload);
        offset = payload_end;
    }
    if PayloadDigest::from_bytes(payload_hasher.finalize().into()) != footer.payload_digest {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream payload does not match its reference",
        ));
    }
    Ok(())
}

async fn finish_stream(
    mut writer: ImmutableWriter,
    kind: StreamKind,
    schema_version: u16,
    payload_length: u64,
    digest: PayloadDigest,
    projections: Vec<EmbeddedProjection>,
) -> Result<StreamRef, Error> {
    let mut projection_refs = Vec::with_capacity(projections.len());
    let mut footer_offset = payload_length;
    for projection in projections {
        let length = projection.bytes.len() as u64;
        let object_range = ChecksummedRange {
            offset: footer_offset,
            length,
            checksum: checksum(&projection.bytes),
        };
        writer.write(&projection.bytes).await?;
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
        writer.abort().await;
        return Err(Error::invalid(
            "write Managed stream",
            "footer exceeds its size limit",
        ));
    }
    let footer_length = footer.len() as u64;
    let footer_checksum = checksum(&footer);
    writer.write(&footer).await?;
    let mut trailer = Vec::with_capacity(TRAILER_BYTES);
    trailer.extend_from_slice(&TRAILER_MAGIC);
    trailer.extend_from_slice(&schema_version.to_le_bytes());
    trailer.extend_from_slice(&kind.value().to_le_bytes());
    trailer.extend_from_slice(&footer_offset.to_le_bytes());
    trailer.extend_from_slice(&footer_length.to_le_bytes());
    trailer.extend_from_slice(footer_checksum.as_bytes());
    trailer.extend_from_slice(checksum(&trailer).as_bytes());
    writer.write(&trailer).await?;
    let object = writer.close().await?;
    Ok(StreamRef {
        kind,
        schema_version,
        object,
        payload_length,
        payload_digest: digest,
        footer: ChecksummedRange {
            offset: footer_offset,
            length: footer_length,
            checksum: footer_checksum,
        },
    })
}

async fn read_footer(operator: &Operator, reference: StreamRef) -> Result<StreamFooter, Error> {
    let trailer_start = reference
        .object
        .encoded_length
        .checked_sub(TRAILER_BYTES as u64)
        .ok_or_else(|| Error::corrupt("read Managed stream", "stream trailer is missing"))?;
    let trailer = read_range(
        operator,
        reference.object,
        trailer_start..reference.object.encoded_length,
    )
    .await?;
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
    let footer_length = u64::from_le_bytes(trailer[20..28].try_into().expect("fixed length"));
    if schema_version != reference.schema_version
        || kind != reference.kind
        || footer_offset != reference.footer.offset
        || footer_length != reference.footer.length
        || &trailer[28..60] != reference.footer.checksum.as_bytes()
        || footer_offset.checked_add(footer_length) != Some(trailer_start)
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream trailer does not match its reference",
        ));
    }
    let footer_bytes = read_range(
        operator,
        reference.object,
        footer_offset..footer_offset + footer_length,
    )
    .await?;
    if checksum(&footer_bytes) != reference.footer.checksum {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream footer checksum is invalid",
        ));
    }
    let footer: StreamFooter = ciborium::from_reader(Cursor::new(&footer_bytes))
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
