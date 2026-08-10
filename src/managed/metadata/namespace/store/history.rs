// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Immutable checkpoints and retained namespace history.

use blake3::hash;
use opendal::ErrorKind;

use super::{
    CHANGE_SEGMENT_RECORD, CHANGE_SEGMENT_ROOT, CHECKPOINT_RECORD, CHECKPOINT_ROOT,
    MAX_CHECKPOINT_ENCODED_BYTES, NamespaceStore,
};
use crate::filesystem::{VolumeError, VolumeSnapshot};
use crate::managed::error::{corrupt, invalid, unavailable};
use crate::managed::format::LowerHex;
use crate::managed::metadata::namespace::{
    ChangeSegmentRef, CheckpointRef, StoredChangeSegment, StoredNamespaceState,
};
use crate::managed::metadata::object::ensure_immutable;

impl NamespaceStore {
    pub(crate) async fn read_checkpoint(
        &self,
        reference: CheckpointRef,
    ) -> Result<VolumeSnapshot, VolumeError> {
        if reference.length > MAX_CHECKPOINT_ENCODED_BYTES as u64 {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint exceeds its encoded size limit",
            ));
        }
        let key = checkpoint_key(reference.digest);
        let bytes = match self.data.read_with(&key).range(0..reference.length).await {
            Ok(bytes) => bytes.to_bytes(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(corrupt("read Managed namespace", "checkpoint is missing"));
            }
            Err(_) => {
                return Err(unavailable(
                    "read Managed namespace",
                    "storage operation failed",
                ));
            }
        };
        if <[u8; 32]>::from(hash(&bytes)) != reference.digest {
            return Err(corrupt(
                "read Managed namespace",
                "checkpoint identity is invalid",
            ));
        }
        decode_checkpoint(&bytes)
    }

    pub(super) async fn write_checkpoint(
        &self,
        checkpoint: &VolumeSnapshot,
    ) -> Result<CheckpointRef, VolumeError> {
        let bytes = encode_checkpoint(checkpoint)?;
        let reference = CheckpointRef::from_encoded(&bytes);
        ensure_immutable(
            &self.data,
            &checkpoint_key(reference.digest),
            bytes.into(),
            "checkpoint Managed namespace",
        )
        .await?;
        Ok(reference)
    }

    pub(super) async fn read_change_segment(
        &self,
        reference: ChangeSegmentRef,
    ) -> Result<StoredChangeSegment, VolumeError> {
        if reference.length > CHANGE_SEGMENT_RECORD.maximum_encoded_bytes() as u64 {
            return Err(corrupt(
                "read Managed change segment",
                "namespace change segment exceeds its size limit",
            ));
        }
        let bytes = self
            .data
            .read_with(&change_segment_key(reference.digest))
            .range(0..reference.length)
            .await
            .map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    corrupt(
                        "read Managed change segment",
                        "namespace change segment is missing",
                    )
                } else {
                    unavailable("read Managed change segment", "storage operation failed")
                }
            })?
            .to_bytes();
        if *hash(&bytes).as_bytes() != reference.digest {
            return Err(corrupt(
                "read Managed change segment",
                "namespace change segment identity is invalid",
            ));
        }
        let segment: StoredChangeSegment = CHANGE_SEGMENT_RECORD
            .decode(&bytes)
            .map_err(|error| corrupt("read Managed change segment", error))?;
        segment.validate(self.volume_id)?;
        if segment.reference(reference.digest, reference.length) != reference {
            return Err(corrupt(
                "read Managed change segment",
                "change segment disagrees with its index",
            ));
        }
        Ok(segment)
    }

    pub(super) async fn write_change_segment(
        &self,
        segment: &StoredChangeSegment,
    ) -> Result<ChangeSegmentRef, VolumeError> {
        let bytes = CHANGE_SEGMENT_RECORD
            .encode(segment)
            .map_err(|error| invalid("write Managed change segment", error))?;
        let digest: [u8; 32] = hash(&bytes).into();
        let length = bytes.len() as u64;
        ensure_immutable(
            &self.data,
            &change_segment_key(digest),
            bytes.into(),
            "archive Managed changes",
        )
        .await?;
        Ok(segment.reference(digest, length))
    }

    pub(crate) async fn state_at_sequence(
        &self,
        current: &StoredNamespaceState,
        sequence: u64,
    ) -> Result<Option<StoredNamespaceState>, VolumeError> {
        if let Some(state) = current.at_sequence(sequence) {
            return Ok(Some(state));
        }
        let Some((position, reference)) =
            current.segments.iter().enumerate().find(|(_, segment)| {
                segment.start.sequence() <= sequence && sequence <= segment.end.sequence()
            })
        else {
            return Ok(None);
        };
        let segment = self.read_change_segment(*reference).await?;
        let length = (sequence - reference.start.sequence()) as usize;
        let outcome = current
            .outcome
            .clone()
            .filter(|result| result.cursor.sequence() <= sequence);
        Ok(Some(StoredNamespaceState {
            checkpoint: segment.checkpoint,
            checkpoint_cursor: reference.start,
            tail: segment.changes[..length].to_vec(),
            segments: current.segments[..position].to_vec(),
            operation_prefixes: current.operation_prefixes.clone(),
            outcome,
        }))
    }

    pub(super) async fn replay_retained_from(
        &self,
        base: &VolumeSnapshot,
        state: &StoredNamespaceState,
    ) -> Result<Option<VolumeSnapshot>, VolumeError> {
        let Some(position) = state.segments.iter().position(|segment| {
            segment.start.sequence() <= base.cursor.sequence()
                && base.cursor.sequence() <= segment.end.sequence()
        }) else {
            return Ok(None);
        };
        let mut snapshot = base.clone();
        for (offset, reference) in state.segments[position..].iter().enumerate() {
            let segment = self.read_change_segment(*reference).await?;
            let start = if snapshot.cursor == reference.start {
                Some(0)
            } else if snapshot.cursor == reference.end {
                Some(segment.changes.len())
            } else {
                segment
                    .changes
                    .iter()
                    .position(|change| change.parent() == snapshot.cursor)
            };
            let Some(start) = start else {
                return if offset == 0 {
                    Ok(None)
                } else {
                    Err(corrupt(
                        "read Managed change segment",
                        "retained change segments are not consecutive",
                    ))
                };
            };
            snapshot = super::super::state::apply_changes(snapshot, &segment.changes[start..])?;
        }
        snapshot = super::super::state::apply_changes(snapshot, &state.tail)?;
        super::super::validate_snapshot_structure(&snapshot).map_err(|_| {
            corrupt(
                "read Managed change segment",
                "replayed namespace is invalid",
            )
        })?;
        Ok(Some(snapshot))
    }
}

pub(super) fn encode_checkpoint(checkpoint: &VolumeSnapshot) -> Result<Vec<u8>, VolumeError> {
    CHECKPOINT_RECORD
        .encode(checkpoint)
        .map_err(|error| invalid("checkpoint Managed namespace", error))
}

pub(super) fn decode_checkpoint(bytes: &[u8]) -> Result<VolumeSnapshot, VolumeError> {
    CHECKPOINT_RECORD
        .decode(bytes)
        .map_err(|error| corrupt("read Managed namespace", error))
}

fn checkpoint_key(id: [u8; 32]) -> String {
    format!("{CHECKPOINT_ROOT}/{}.ofs", LowerHex::encode(&id))
}

fn change_segment_key(id: [u8; 32]) -> String {
    format!("{CHANGE_SEGMENT_ROOT}/{}.ofs", LowerHex::encode(&id))
}
