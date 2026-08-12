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

//! Bounded local worksets for streaming namespace operations.

mod sort;
mod spool;

use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use crate::Error;
use crate::filesystem::OperationId;

pub(crate) use sort::{MergeRuns, sort};
pub(crate) use spool::{Spool, SpoolReader, SpoolWriter};

const MEBIBYTE: usize = 1024 * 1024;

#[derive(Clone, Copy)]
pub(crate) struct WorksetOptions {
    sort_run_target_bytes: usize,
    merge_fan_in: usize,
}

impl WorksetOptions {
    pub(crate) fn new(memory_mib: NonZeroUsize, concurrency: NonZeroUsize) -> Result<Self, Error> {
        let sort_run_target_bytes = memory_mib.get().checked_mul(MEBIBYTE).ok_or_else(|| {
            Error::invalid("configure local worksets", "--work-memory-mib overflows")
        })?;
        Ok(Self {
            sort_run_target_bytes,
            merge_fan_in: if concurrency.get() == 1 {
                2
            } else {
                concurrency.get()
            },
        })
    }
}

struct WorkspaceInner {
    path: PathBuf,
    options: WorksetOptions,
}

impl Drop for WorkspaceInner {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Clone)]
pub(crate) struct Workspace {
    inner: Arc<WorkspaceInner>,
}

impl Workspace {
    pub(crate) fn create(options: WorksetOptions) -> Result<Self, Error> {
        let path = std::env::temp_dir().join(format!("ofs-sync-{}", OperationId::generate()));
        fs::create_dir(&path)
            .map_err(|error| Error::from_io("create Sync workspace", Some(&path), error))?;
        Ok(Self {
            inner: Arc::new(WorkspaceInner { path, options }),
        })
    }

    pub(super) fn path(&self) -> &std::path::Path {
        &self.inner.path
    }

    pub(super) fn sort_run_target_bytes(&self) -> usize {
        self.inner.options.sort_run_target_bytes
    }

    pub(crate) fn merge_fan_in(&self) -> usize {
        self.inner.options.merge_fan_in
    }

    pub(crate) fn writer<T>(&self, stem: &str) -> Result<SpoolWriter<T>, Error> {
        SpoolWriter::create(self.clone(), stem)
    }
}
