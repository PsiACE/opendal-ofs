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

use std::ffi::OsString;

use fuse3::Errno;

pub(crate) struct OpenedFile {
    pub(crate) path: OsString,
    pub(crate) content_length: u64,
    pub(crate) version: Option<String>,
    pub(crate) etag: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FileKey(pub(crate) usize);

impl TryFrom<u64> for FileKey {
    type Error = Errno;

    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        let key = value.checked_sub(1).ok_or(Errno::from(libc::EBADF))?;
        usize::try_from(key)
            .map(FileKey)
            .map_err(|_| Errno::from(libc::EOVERFLOW))
    }
}

impl FileKey {
    pub(crate) fn to_fh(self) -> u64 {
        u64::try_from(self.0).expect("usize always fits in u64 on supported targets") + 1
    }
}
