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

//! Runtime tracing for complete Managed logical-file access.

use std::fmt;
use std::future::Future;
use std::ops::Range;

use ofs_core::Error;
use ofs_core::filesystem::FileFingerprint;
use ofs_core::managed::extension::{
    AccessContext, ExtendedFileAccess, ExtensionFileRef, FileAccess, FileAccessExtension,
};
use ofs_core::managed::{GcEpoch, ObjectLocator};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{Instrument as _, info_span};

/// Add spans without changing the persisted file-access description.
#[derive(Clone, Copy, Debug, Default)]
pub struct TracingExtension;

impl TracingExtension {
    /// Construct a tracing extension.
    pub const fn new() -> Self {
        Self
    }
}

impl<A: FileAccess> FileAccessExtension<A> for TracingExtension {
    type ExtendedAccess = TracingFileAccess<A>;

    fn extend(&self, inner: A) -> Self::ExtendedAccess {
        TracingFileAccess { inner }
    }
}

/// Logical-file access instrumented with tracing spans.
pub struct TracingFileAccess<A> {
    inner: A,
}

impl<A: fmt::Debug> fmt::Debug for TracingFileAccess<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TracingFileAccess")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<A: FileAccess> ExtendedFileAccess for TracingFileAccess<A> {
    type Inner = A;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn write<'a>(
        &'a self,
        context: &'a AccessContext,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        fingerprint: FileFingerprint,
        gc_epoch: GcEpoch,
    ) -> impl Future<Output = Result<ExtensionFileRef, Error>> + Send + 'a {
        self.inner
            .write(context, source, fingerprint, gc_epoch)
            .instrument(info_span!(
                "managed.file.write",
                logical_length = fingerprint.logical_length()
            ))
    }

    fn read<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        fingerprint: FileFingerprint,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        let range_start = range.start;
        let range_end = range.end;
        self.inner
            .read(context, reference, fingerprint, range, destination)
            .instrument(info_span!(
                "managed.file.read",
                logical_length = fingerprint.logical_length(),
                range_start,
                range_end
            ))
    }

    fn visit_reachable<'a>(
        &'a self,
        context: &'a AccessContext,
        reference: ExtensionFileRef,
        visit: &'a mut (dyn FnMut(ObjectLocator) -> Result<(), Error> + Send),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        self.inner
            .visit_reachable(context, reference, visit)
            .instrument(info_span!("managed.file.visit_reachable"))
    }
}
