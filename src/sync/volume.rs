// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Volume capabilities required by the Sync access model.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use opendal::Operator;

use crate::filesystem::{
    AuthorityIdentity, CommitOutcome, FileVersion, Generation, OperationId, VolumeError, VolumeId,
    VolumePublication, VolumeSnapshot,
};

#[derive(Clone, Debug)]
pub struct MaterializeRequest {
    pub path: String,
    pub version: FileVersion,
}

/// A Sync observation retains the private compare-and-swap token needed by a
/// later publication while exposing only a filesystem snapshot to Sync.
pub trait SyncObservation: Clone + Send + Sync {
    fn snapshot(&self) -> &VolumeSnapshot;
}

/// Authoritative volume operations required by the Sync access model.
///
/// Replica state, local acknowledgement, and conflict presentation remain
/// owned by Sync. Implementations own namespace authority and durable content.
#[allow(async_fn_in_trait)]
pub trait SyncVolume: Clone + Send + Sync {
    type Observation: SyncObservation;

    fn id(&self) -> VolumeId;

    /// Stable identity of the authority used by this bound volume.
    fn authority(&self) -> AuthorityIdentity {
        AuthorityIdentity::base(self.id())
    }

    fn initial_generation(&self) -> Generation;

    fn next_generation(&self, generation: &Generation) -> Result<Generation, VolumeError>;

    async fn observe_from(
        &self,
        base: Option<&VolumeSnapshot>,
    ) -> Result<Option<Self::Observation>, VolumeError>;

    /// Freeze changed files and prepare every new immutable data object locally.
    ///
    /// `segment_staging` is private to the volume implementation and survives
    /// with the pending intent so that a retry never has to read or reconstruct
    /// data from the live source tree.
    async fn stage_files(
        &self,
        source: &Operator,
        segment_staging: &Operator,
        paths: Vec<String>,
        authority: Option<&VolumeSnapshot>,
        concurrency: NonZeroUsize,
    ) -> Result<BTreeMap<String, FileVersion>, VolumeError>;

    /// Make every locally prepared immutable data object durable.
    ///
    /// Sync persists its pending intent before calling this method and does not
    /// publish namespace metadata until it succeeds. Implementations must make
    /// retries idempotent and must use only `segment_staging`, never the live or
    /// user-visible frozen source tree.
    async fn finalize_staged_files(
        &self,
        segment_staging: &Operator,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError>;

    async fn publish(
        &self,
        observed: Option<&Self::Observation>,
        publication: &VolumePublication,
    ) -> Result<CommitOutcome, VolumeError>;

    async fn resolve(&self, operation: OperationId) -> Result<CommitOutcome, VolumeError>;

    async fn materialize(
        &self,
        target: &Operator,
        segment_staging: Option<&Operator>,
        requests: Vec<MaterializeRequest>,
        concurrency: NonZeroUsize,
    ) -> Result<(), VolumeError>;
}
