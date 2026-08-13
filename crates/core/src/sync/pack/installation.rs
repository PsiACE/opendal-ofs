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

use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use futures::stream::{FuturesUnordered, StreamExt as _};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::ContentRef;
use crate::managed::{ManagedVolume, ObjectLocator, SegmentRangeReader};
use crate::workset::{self, Spool, SpoolWriter, Workspace};

use super::super::local_fs::{self, DirectoryDurability, StoredPath};
use super::super::transfer::StagedFile;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Installation {
    locator: ObjectLocator,
    offset: u64,
    destination: StoredPath,
    fingerprint: ContentRef,
    executable: bool,
    authoritative: bool,
}

impl Installation {
    pub(crate) fn create(
        locator: ObjectLocator,
        offset: u64,
        destination: &Path,
        fingerprint: ContentRef,
        executable: bool,
        authoritative: bool,
    ) -> Result<Self, Error> {
        Ok(Self {
            locator,
            offset,
            destination: StoredPath::from_path(destination)?,
            fingerprint,
            executable,
            authoritative,
        })
    }
}

type InstallationFuture = Pin<Box<dyn Future<Output = Result<Spool<StoredPath>, Error>> + Send>>;

pub(crate) async fn install(
    volume: &ManagedVolume,
    workspace: &Workspace,
    pending: &Spool<Installation>,
    concurrency: usize,
    durability: &mut DirectoryDurability,
) -> Result<(), Error> {
    let pending = workset::sort(workspace, pending, |file| (file.locator, file.offset))?;
    let mut source = pending.reader()?;
    let mut current_locator = None;
    let mut current: Option<SpoolWriter<Installation>> = None;
    let mut installations = FuturesUnordered::<InstallationFuture>::new();
    while let Some(file) = source.next()? {
        if current_locator.is_some_and(|locator| locator != file.locator) {
            schedule(
                &mut installations,
                volume.clone(),
                workspace.clone(),
                current_locator.expect("Pack group has a locator"),
                current.take().expect("Pack group exists").finish()?,
            );
            if installations.len() >= concurrency {
                let installed = installations
                    .next()
                    .await
                    .expect("a Pack installation remains")?;
                record_installed(installed, durability)?;
            }
        }
        if current_locator != Some(file.locator) {
            current_locator = Some(file.locator);
            current = Some(workspace.writer("pack-installation-group")?);
        }
        current.as_mut().expect("Pack group exists").write(&file)?;
    }
    if let Some(current) = current {
        schedule(
            &mut installations,
            volume.clone(),
            workspace.clone(),
            current_locator.expect("Pack group has a locator"),
            current.finish()?,
        );
    }
    while let Some(installed) = installations.next().await {
        record_installed(installed?, durability)?;
    }
    Ok(())
}

fn schedule(
    installations: &mut FuturesUnordered<InstallationFuture>,
    volume: ManagedVolume,
    workspace: Workspace,
    locator: ObjectLocator,
    files: Spool<Installation>,
) {
    installations.push(Box::pin(async move {
        install_object(&volume, &workspace, locator, &files).await
    }));
}

fn record_installed(
    paths: Spool<StoredPath>,
    durability: &mut DirectoryDurability,
) -> Result<(), Error> {
    let mut paths = paths.reader()?;
    while let Some(path) = paths.next()? {
        durability.changed_parent(&path.to_path_buf())?;
    }
    Ok(())
}

async fn install_object(
    volume: &ManagedVolume,
    workspace: &Workspace,
    locator: ObjectLocator,
    files: &Spool<Installation>,
) -> Result<Spool<StoredPath>, Error> {
    let mut installed = workspace.writer("installed-pack-files")?;
    let mut source = files.reader()?;
    let mut run = None;
    let mut run_end = 0_u64;
    while let Some(file) = source.next()? {
        let destination = file.destination.to_path_buf();
        if file.authoritative
            && local_fs::file_matches(&destination, file.fingerprint, file.executable).await?
        {
            continue;
        }
        if run.is_some() && run_end != file.offset {
            if let Some(run) = run.take() {
                install_range(volume, locator, run, &mut installed).await?;
            }
            run = Some(workspace.writer("pack-range-run")?);
        }
        if run.is_none() {
            run = Some(workspace.writer("pack-range-run")?);
        }
        run_end = file
            .offset
            .checked_add(file.fingerprint.length())
            .ok_or_else(|| Error::corrupt("install Managed pack", "file range overflows"))?;
        run.as_mut().expect("Pack run exists").write(&file)?;
    }
    if let Some(run) = run {
        install_range(volume, locator, run, &mut installed).await?;
    }
    installed.finish()
}

async fn install_range(
    volume: &ManagedVolume,
    locator: ObjectLocator,
    files: SpoolWriter<Installation>,
    installed: &mut SpoolWriter<StoredPath>,
) -> Result<(), Error> {
    let files = files.finish()?;
    let mut source = files.reader()?;
    let first = source
        .next()?
        .ok_or_else(|| Error::corrupt("install Managed pack", "Pack range run is empty"))?;
    let mut end = first
        .offset
        .checked_add(first.fingerprint.length())
        .ok_or_else(|| Error::corrupt("install Managed pack", "file range overflows"))?;
    while let Some(file) = source.next()? {
        if file.offset != end {
            return Err(Error::corrupt(
                "install Managed pack",
                "Pack range run is not contiguous",
            ));
        }
        end = end
            .checked_add(file.fingerprint.length())
            .ok_or_else(|| Error::corrupt("install Managed pack", "file range overflows"))?;
    }
    let mut reader =
        SegmentRangeReader::open(volume.operator(), locator, first.offset..end).await?;
    let mut source = files.reader()?;
    while let Some(file) = source.next()? {
        install_file(&mut reader, &file).await?;
        installed.write(&file.destination)?;
    }
    Ok(())
}

async fn install_file(reader: &mut SegmentRangeReader, file: &Installation) -> Result<(), Error> {
    let destination = file.destination.to_path_buf();
    if local_fs::path_metadata(&destination)?.is_some_and(|metadata| metadata.is_dir()) {
        local_fs::remove_replaced_directory(&destination)?;
        crate::fault::check("during-install")?;
    }
    let mut staging = StagedFile::create(&destination).await?;
    let result = reader.copy_file(file.fingerprint, staging.writer()).await;
    if let Err(error) = result {
        staging.abort().await;
        return Err(error);
    }
    staging.commit(file.executable).await?;
    crate::fault::check("during-install")?;
    Ok(())
}
