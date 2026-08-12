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

//! OpenDAL adapter for Managed control and immutable objects.

use blake3::Hasher;
use opendal::{Buffer, ErrorKind as StorageErrorKind, Operator, Writer};

use crate::Error;
use crate::filesystem::Digest;

use super::object::{GcEpoch, ObjectClass, ObjectId, ObjectRef, object_key};

pub(crate) struct ControlObject {
    pub(crate) bytes: Vec<u8>,
    pub(crate) revision: String,
}

pub(crate) enum ControlCondition<'a> {
    Missing,
    Revision(&'a str),
}

pub(crate) async fn read_control(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
) -> Result<Option<ControlObject>, Error> {
    let reader = match operator.reader(key).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed control object", error)),
    };
    let metadata = match operator.stat(key).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed control object", error)),
    };
    if metadata.content_length() > maximum_bytes as u64 {
        return Err(Error::corrupt(
            "read Managed control object",
            "control object exceeds its size limit",
        ));
    }
    let bytes = match reader.read(..).await {
        Ok(bytes) => bytes.to_vec(),
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage("read Managed control object", error)),
    };
    if bytes.len() > maximum_bytes {
        return Err(Error::corrupt(
            "read Managed control object",
            "control object exceeds its size limit",
        ));
    }
    let revision = metadata
        .etag()
        .ok_or_else(|| {
            Error::unsupported(
                "read Managed control object",
                "object revision is unavailable",
            )
        })?
        .to_owned();
    Ok(Some(ControlObject { bytes, revision }))
}

pub(crate) async fn write_control(
    operator: &Operator,
    key: &str,
    bytes: Vec<u8>,
    condition: ControlCondition<'_>,
) -> Result<bool, Error> {
    let write = operator.write_with(key, bytes);
    let result = match condition {
        ControlCondition::Missing => write.if_not_exists(true).await,
        ControlCondition::Revision(revision) => write.if_match(revision).await,
    };
    match result {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(Error::from_storage("publish Managed control object", error)),
    }
}

pub(crate) struct ImmutableWriter {
    gc_epoch: GcEpoch,
    class: ObjectClass,
    id: ObjectId,
    writer: Writer,
    hasher: Hasher,
    encoded_length: u64,
}

impl ImmutableWriter {
    pub(crate) async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
    ) -> Result<Self, Error> {
        let id = ObjectId::generate();
        let key = object_key(gc_epoch, class, id);
        let writer = operator
            .writer_with(&key)
            .if_not_exists(true)
            .await
            .map_err(|error| Error::from_storage("open Managed object writer", error))?;
        Ok(Self {
            gc_epoch,
            class,
            id,
            writer,
            hasher: Hasher::new(),
            encoded_length: 0,
        })
    }

    pub(crate) async fn write(&mut self, bytes: Vec<u8>) -> Result<(), Error> {
        self.encoded_length = self
            .encoded_length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::invalid("write Managed object", "object length overflows"))?;
        self.hasher.update(&bytes);
        if let Err(error) = self.writer.write(Buffer::from(bytes)).await {
            let error = Error::from_storage("write Managed object", error);
            let _ = self.writer.abort().await;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn digest(&self) -> Digest {
        Digest::from_bytes(self.hasher.finalize().into())
    }

    pub(crate) async fn abort(&mut self) -> Result<(), Error> {
        self.writer
            .abort()
            .await
            .map_err(|error| Error::from_storage("abort Managed object", error))
    }

    pub(crate) async fn close(mut self) -> Result<ObjectRef, Error> {
        if let Err(error) = self.writer.close().await {
            let error = Error::from_storage("finish Managed object", error);
            let _ = self.writer.abort().await;
            return Err(error);
        }
        Ok(ObjectRef {
            gc_epoch: self.gc_epoch,
            class: self.class,
            id: self.id,
            encoded_length: self.encoded_length,
            digest: Digest::from_bytes(self.hasher.finalize().into()),
        })
    }
}

pub(crate) async fn read_immutable(
    operator: &Operator,
    reference: ObjectRef,
    maximum_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let length = usize::try_from(reference.encoded_length)
        .ok()
        .filter(|length| *length <= maximum_bytes)
        .ok_or_else(|| Error::corrupt("read Managed object", "object length is invalid"))?;
    let key = reference.key();
    let bytes = operator
        .read(&key)
        .await
        .map_err(|error| missing_object("read Managed object", error))?
        .to_vec();
    if bytes.len() != length || blake3::hash(&bytes).as_bytes() != reference.digest.as_bytes() {
        return Err(Error::corrupt(
            "read Managed object",
            "object does not match its reference",
        ));
    }
    Ok(bytes)
}

fn missing_object(operation: &'static str, error: opendal::Error) -> Error {
    if error.kind() == StorageErrorKind::NotFound {
        Error::corrupt(operation, "referenced object is missing")
    } else {
        Error::from_storage(operation, error)
    }
}
