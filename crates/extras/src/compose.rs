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

//! Unique format-driven composition for this binary.

use ofs_core::Error;
use ofs_core::ErrorKind;
use ofs_core::ManagedVolume;
use ofs_core::VolumeRuntime;
use ofs_core::authority::{AuthoritySelector, DefaultSelector};
use ofs_core::data::{ExtentCodec, FilePartitioner, IdentityCodec, VolumeAccess, WholePartitioner};
use ofs_core::format::{FileDataLayout, VolumeFormat};
use ofs_core::sync::SyncEngine;
use ofs_core::volume::GcOutcome;
use ofs_ext_branch::{BranchAuthorityStore, BranchSelector};
use ofs_ext_fastcdc::FastCdcPartitioner;
use ofs_ext_zstd::ZstdCodec;
use opendal::Operator;

use super::options::CreateOptions;

#[derive(Clone, Debug)]
pub struct Stack<P, C, S> {
    partitioner: P,
    codec: C,
    selector: S,
}

impl<P, C, S> VolumeAccess for Stack<P, C, S>
where
    P: FilePartitioner,
    C: ExtentCodec,
    S: AuthoritySelector,
{
    type Partitioner = P;
    type Codec = C;
    type Selector = S;

    fn partitioner(&self) -> &Self::Partitioner {
        &self.partitioner
    }

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn selector(&self) -> &Self::Selector {
        &self.selector
    }
}

/// One opened volume using this binary's supported stacks.
pub enum ConfiguredVolume {
    Whole(ManagedVolume<Stack<WholePartitioner, IdentityCodec, DefaultSelector>>),
    FastCdc(ManagedVolume<Stack<FastCdcPartitioner, IdentityCodec, DefaultSelector>>),
    FastCdcZstd(ManagedVolume<Stack<FastCdcPartitioner, ZstdCodec, DefaultSelector>>),
    BranchWhole(ManagedVolume<Stack<WholePartitioner, IdentityCodec, BranchSelector>>),
    BranchFastCdc(ManagedVolume<Stack<FastCdcPartitioner, IdentityCodec, BranchSelector>>),
    BranchFastCdcZstd(ManagedVolume<Stack<FastCdcPartitioner, ZstdCodec, BranchSelector>>),
}

impl ConfiguredVolume {
    pub fn format(&self) -> &VolumeFormat {
        match self {
            Self::Whole(volume) => volume.format(),
            Self::FastCdc(volume) => volume.format(),
            Self::FastCdcZstd(volume) => volume.format(),
            Self::BranchWhole(volume) => volume.format(),
            Self::BranchFastCdc(volume) => volume.format(),
            Self::BranchFastCdcZstd(volume) => volume.format(),
        }
    }

    pub fn id(&self) -> ofs_core::filesystem::VolumeId {
        self.format().volume_id()
    }

    pub fn operator(&self) -> &Operator {
        match self {
            Self::Whole(volume) => volume.operator(),
            Self::FastCdc(volume) => volume.operator(),
            Self::FastCdcZstd(volume) => volume.operator(),
            Self::BranchWhole(volume) => volume.operator(),
            Self::BranchFastCdc(volume) => volume.operator(),
            Self::BranchFastCdcZstd(volume) => volume.operator(),
        }
    }

    pub fn multipart_part_bytes(&self) -> std::num::NonZeroUsize {
        match self {
            Self::Whole(volume) => volume.multipart_part_bytes(),
            Self::FastCdc(volume) => volume.multipart_part_bytes(),
            Self::FastCdcZstd(volume) => volume.multipart_part_bytes(),
            Self::BranchWhole(volume) => volume.multipart_part_bytes(),
            Self::BranchFastCdc(volume) => volume.multipart_part_bytes(),
            Self::BranchFastCdcZstd(volume) => volume.multipart_part_bytes(),
        }
    }

    pub async fn collect(&self) -> Result<GcOutcome, Error> {
        match self {
            Self::Whole(volume) => volume.collect().await,
            Self::FastCdc(volume) => volume.collect().await,
            Self::FastCdcZstd(volume) => volume.collect().await,
            Self::BranchWhole(volume) => volume.collect().await,
            Self::BranchFastCdc(volume) => volume.collect().await,
            Self::BranchFastCdcZstd(volume) => volume.collect().await,
        }
    }

    pub async fn sync(
        &self,
        root: &std::path::Path,
        state: &std::path::Path,
        resolve: &[String],
        mutations: Option<&[ofs_core::sync::FileChangeSetEntry]>,
    ) -> Result<ofs_core::sync::SyncOutcome, Error> {
        match self {
            Self::Whole(volume) => sync_one(volume, root, state, resolve, mutations).await,
            Self::FastCdc(volume) => sync_one(volume, root, state, resolve, mutations).await,
            Self::FastCdcZstd(volume) => sync_one(volume, root, state, resolve, mutations).await,
            Self::BranchWhole(volume) => sync_one(volume, root, state, resolve, mutations).await,
            Self::BranchFastCdc(volume) => sync_one(volume, root, state, resolve, mutations).await,
            Self::BranchFastCdcZstd(volume) => {
                sync_one(volume, root, state, resolve, mutations).await
            }
        }
    }

    pub fn branch_store(&self) -> Option<BranchAuthorityStore> {
        match self {
            Self::BranchWhole(_) | Self::BranchFastCdc(_) | Self::BranchFastCdcZstd(_) => {
                Some(BranchSelector::default().store())
            }
            _ => None,
        }
    }
}

async fn sync_one<A: VolumeAccess>(
    volume: &ManagedVolume<A>,
    root: &std::path::Path,
    state: &std::path::Path,
    resolve: &[String],
    mutations: Option<&[ofs_core::sync::FileChangeSetEntry]>,
) -> Result<ofs_core::sync::SyncOutcome, Error> {
    let engine = SyncEngine::new(volume.clone());
    match mutations {
        Some(mutations) => {
            engine
                .sync_with_mutations(root, state, resolve, mutations)
                .await
        }
        None => engine.sync(root, state, resolve).await,
    }
}

/// Admit a volume format and open it.
pub async fn open(
    operator: &Operator,
    runtime: VolumeRuntime,
    authority_name: &str,
) -> Result<ConfiguredVolume, Error> {
    let format = read_format(operator).await?;
    compose(&format)?;
    open_stack(operator, runtime, authority_name, &format).await
}

async fn open_stack(
    operator: &Operator,
    runtime: VolumeRuntime,
    authority_name: &str,
    format: &VolumeFormat,
) -> Result<ConfiguredVolume, Error> {
    let layout = format.file_data_layout();
    let branch = format.authority().is_some();
    match (layout.partitioning(), layout.decodings(), branch) {
        (None, [], false) => Ok(ConfiguredVolume::Whole(
            ManagedVolume::open(
                operator,
                stack(WholePartitioner, IdentityCodec, DefaultSelector),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (Some(partitioning), [], false) => Ok(ConfiguredVolume::FastCdc(
            ManagedVolume::open(
                operator,
                stack(
                    FastCdcPartitioner::from_descriptor(partitioning)?,
                    IdentityCodec,
                    DefaultSelector,
                ),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (Some(partitioning), [decoding], false) => Ok(ConfiguredVolume::FastCdcZstd(
            ManagedVolume::open(
                operator,
                stack(
                    FastCdcPartitioner::from_descriptor(partitioning)?,
                    ZstdCodec::from_descriptor(decoding)?,
                    DefaultSelector,
                ),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (None, [], true) => Ok(ConfiguredVolume::BranchWhole(
            ManagedVolume::open(
                operator,
                stack(WholePartitioner, IdentityCodec, BranchSelector::default()),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (Some(partitioning), [], true) => Ok(ConfiguredVolume::BranchFastCdc(
            ManagedVolume::open(
                operator,
                stack(
                    FastCdcPartitioner::from_descriptor(partitioning)?,
                    IdentityCodec,
                    BranchSelector::default(),
                ),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (Some(partitioning), [decoding], true) => Ok(ConfiguredVolume::BranchFastCdcZstd(
            ManagedVolume::open(
                operator,
                stack(
                    FastCdcPartitioner::from_descriptor(partitioning)?,
                    ZstdCodec::from_descriptor(decoding)?,
                    BranchSelector::default(),
                ),
                runtime,
                authority_name,
            )
            .await?,
        )),
        _ => Err(unsupported_stack()),
    }
}

async fn read_format(operator: &Operator) -> Result<VolumeFormat, Error> {
    use ofs_core::format::{FORMAT_KEY, FORMAT_RECORD};
    use ofs_core::storage::ControlRecord;

    const FORMAT: ControlRecord<VolumeFormat> = ControlRecord::new(FORMAT_KEY, FORMAT_RECORD);
    FORMAT
        .read(operator)
        .await?
        .ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "open Managed volume",
                "volume format is missing",
            )
        })
        .map(|observed| observed.value)
}

/// Create a volume from product options.
pub async fn create(
    operator: &Operator,
    runtime: VolumeRuntime,
    authority_name: &str,
    options: CreateOptions,
) -> Result<ConfiguredVolume, Error> {
    let layout = options.file_data_layout()?;
    compose_layout(&layout)?;
    let format =
        ofs_core::CreateOptions::new(layout.clone()).with_authority_opt(options.authority());
    match (
        layout.partitioning(),
        layout.decodings(),
        options.authority().is_some(),
    ) {
        (None, [], false) => Ok(ConfiguredVolume::Whole(
            ManagedVolume::create(
                operator,
                format,
                stack(WholePartitioner, IdentityCodec, DefaultSelector),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (Some(partitioning), [], false) => Ok(ConfiguredVolume::FastCdc(
            ManagedVolume::create(
                operator,
                format,
                stack(
                    FastCdcPartitioner::from_descriptor(partitioning)?,
                    IdentityCodec,
                    DefaultSelector,
                ),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (Some(partitioning), [decoding], false) => Ok(ConfiguredVolume::FastCdcZstd(
            ManagedVolume::create(
                operator,
                format,
                stack(
                    FastCdcPartitioner::from_descriptor(partitioning)?,
                    ZstdCodec::from_descriptor(decoding)?,
                    DefaultSelector,
                ),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (None, [], true) => Ok(ConfiguredVolume::BranchWhole(
            ManagedVolume::create(
                operator,
                format,
                stack(WholePartitioner, IdentityCodec, BranchSelector::default()),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (Some(partitioning), [], true) => Ok(ConfiguredVolume::BranchFastCdc(
            ManagedVolume::create(
                operator,
                format,
                stack(
                    FastCdcPartitioner::from_descriptor(partitioning)?,
                    IdentityCodec,
                    BranchSelector::default(),
                ),
                runtime,
                authority_name,
            )
            .await?,
        )),
        (Some(partitioning), [decoding], true) => Ok(ConfiguredVolume::BranchFastCdcZstd(
            ManagedVolume::create(
                operator,
                format,
                stack(
                    FastCdcPartitioner::from_descriptor(partitioning)?,
                    ZstdCodec::from_descriptor(decoding)?,
                    BranchSelector::default(),
                ),
                runtime,
                authority_name,
            )
            .await?,
        )),
        _ => Err(unsupported_stack()),
    }
}

/// Admit a volume format for the shipped product.
pub fn compose(format: &VolumeFormat) -> Result<(), Error> {
    compose_layout(format.file_data_layout())
}

fn compose_layout(layout: &FileDataLayout) -> Result<(), Error> {
    match (layout.partitioning(), layout.decodings()) {
        (None, []) | (Some(_), []) | (Some(_), [_]) => Ok(()),
        (None, [_]) => Err(Error::new(
            ErrorKind::Unsupported,
            "compose Managed volume",
            "Zstandard requires a partitioner with a finite maximum extent",
        )),
        _ => Err(unsupported_stack()),
    }
}

fn stack<P, C, S>(partitioner: P, codec: C, selector: S) -> Stack<P, C, S> {
    Stack {
        partitioner,
        codec,
        selector,
    }
}

fn unsupported_stack() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "compose Managed volume",
        "volume format uses an extension combination this binary does not implement",
    )
}
