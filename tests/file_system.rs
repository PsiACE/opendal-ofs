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

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fuse3::Errno;
use fuse3::path::prelude::*;
use futures::TryStreamExt;
use opendal::Error;
use opendal::Operator;
use opendal::layers::{LoggingInterceptor, LoggingLayer};
use opendal::raw::{AccessorInfo, Operation};
use opendal::services;

#[derive(Clone, Debug, Default)]
struct RequestAudit {
    lists: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
    stats: Arc<AtomicUsize>,
}

impl LoggingInterceptor for RequestAudit {
    fn log(
        &self,
        _info: &AccessorInfo,
        operation: Operation,
        _context: &[(&str, &str)],
        message: &str,
        _error: Option<&Error>,
    ) {
        if message != "started" {
            return;
        }
        match operation {
            Operation::List => &self.lists,
            Operation::Read => &self.reads,
            Operation::Stat => &self.stats,
            _ => return,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn test_read_clamps_native_ranges_at_eof() {
    let operator = Operator::new(services::Memory::default()).unwrap().finish();
    operator.write("test.txt", "hello").await.unwrap();
    let audit = RequestAudit::default();
    let operator = operator.layer(LoggingLayer::new(audit.clone()));
    let filesystem = fuse3_opendal::Filesystem::new(operator, 0, 0);

    let path = OsStr::new("test.txt");
    let opened = filesystem
        .open(Request::default(), path, libc::O_RDONLY as u32)
        .await
        .unwrap();
    let err = filesystem
        .getattr(Request::default(), None, Some(opened.fh + 1), 0)
        .await
        .unwrap_err();
    assert_eq!(err, Errno::from(libc::EBADF));
    let err = filesystem
        .release(
            Request::default(),
            Some(OsStr::new("other.txt")),
            opened.fh,
            libc::O_RDONLY as u32,
            0,
            false,
        )
        .await
        .unwrap_err();
    assert_eq!(err, Errno::from(libc::EBADF));
    let reply = filesystem
        .read(Request::default(), Some(path), opened.fh, 2, 100)
        .await
        .unwrap();
    assert_eq!(reply.data.as_ref(), b"llo");

    let eof = filesystem
        .read(Request::default(), Some(path), opened.fh, 5, 100)
        .await
        .unwrap();
    let empty = filesystem
        .read(Request::default(), Some(path), opened.fh, 0, 0)
        .await
        .unwrap();
    assert!(eof.data.is_empty());
    assert!(empty.data.is_empty());
    assert_eq!(audit.stats.load(Ordering::Relaxed), 1);
    assert_eq!(audit.reads.load(Ordering::Relaxed), 1);

    filesystem
        .release(
            Request::default(),
            Some(path),
            opened.fh,
            libc::O_RDONLY as u32,
            0,
            false,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_open_handle_honors_native_object_condition() {
    let Some(operator) =
        opendal::tests::init_test_service().expect("initialize configured OpenDAL test service")
    else {
        return;
    };
    let path = format!("ofs-fuse-condition-{}", uuid::Uuid::new_v4());
    operator.write(&path, "original").await.unwrap();
    let metadata = operator.stat(&path).await.unwrap();
    let capability = operator.info().full_capability();
    let guarded = capability.read_with_version && metadata.version().is_some()
        || capability.read_with_if_match && metadata.etag().is_some();
    if !guarded {
        operator.delete(&path).await.unwrap();
        return;
    }

    let filesystem = fuse3_opendal::Filesystem::new(operator.clone(), 0, 0);
    let opened = filesystem
        .open(Request::default(), OsStr::new(&path), libc::O_RDONLY as u32)
        .await
        .unwrap();
    operator.write(&path, "replaced").await.unwrap();
    let result = filesystem
        .read(Request::default(), Some(OsStr::new(&path)), opened.fh, 0, 8)
        .await;
    operator.delete(&path).await.unwrap();

    match result {
        Ok(reply) => assert_eq!(reply.data.as_ref(), b"original"),
        Err(err) => assert_eq!(err, Errno::from(libc::ESTALE)),
    }
}

#[tokio::test]
async fn test_read_only_access_and_attributes() {
    let operator = Operator::new(services::Memory::default()).unwrap().finish();
    operator.write("file", "hello").await.unwrap();
    operator.create_dir("dir/").await.unwrap();
    let audit = RequestAudit::default();
    let operator = operator.layer(LoggingLayer::new(audit.clone()));
    let filesystem = fuse3_opendal::Filesystem::new(operator, 7, 9);

    let err = filesystem
        .access(
            Request::default(),
            OsStr::new("file"),
            (libc::R_OK | libc::W_OK) as u32,
        )
        .await
        .unwrap_err();
    assert_eq!(err, Errno::from(libc::EROFS));
    assert_eq!(audit.stats.load(Ordering::Relaxed), 0);

    let file = filesystem
        .lookup(Request::default(), OsStr::new(""), OsStr::new("file"))
        .await
        .unwrap();
    assert_eq!(file.attr.kind, FileType::RegularFile);
    assert_eq!(file.attr.perm, 0o444);
    assert_eq!(file.attr.nlink, 1);
    assert_eq!((file.attr.uid, file.attr.gid), (7, 9));

    let dir = filesystem
        .lookup(Request::default(), OsStr::new(""), OsStr::new("dir/"))
        .await
        .unwrap();
    assert_eq!(dir.attr.kind, FileType::Directory);
    assert_eq!(dir.attr.perm, 0o555);
    assert_eq!(dir.attr.nlink, 2);
}

#[tokio::test]
async fn test_non_utf8_path_is_rejected_before_storage_access() {
    let audit = RequestAudit::default();
    let operator = Operator::new(services::Memory::default())
        .unwrap()
        .layer(LoggingLayer::new(audit.clone()))
        .finish();
    let filesystem = fuse3_opendal::Filesystem::new(operator, 0, 0);

    let err = filesystem
        .lookup(
            Request::default(),
            OsStr::new(""),
            OsStr::from_bytes(b"\xff"),
        )
        .await
        .unwrap_err();
    assert_eq!(err, Errno::from(libc::EILSEQ));
    assert_eq!(audit.stats.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn test_directory_offsets_resume_without_losing_entries() {
    let audit = RequestAudit::default();
    let operator = Operator::new(services::Memory::default())
        .unwrap()
        .layer(LoggingLayer::new(audit.clone()))
        .finish();
    let expected = (0..300)
        .map(|index| format!("entry-{index:04}"))
        .collect::<BTreeSet<_>>();
    for name in &expected {
        operator.write(name, "").await.unwrap();
    }

    let filesystem = fuse3_opendal::Filesystem::new(operator, 0, 0);
    let root = OsStr::new("");
    let opened = filesystem
        .opendir(Request::default(), root, libc::O_RDONLY as u32)
        .await
        .unwrap();

    let initial = filesystem
        .readdir(Request::default(), root, opened.fh, 0)
        .await
        .unwrap()
        .entries
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let retried = filesystem
        .readdir(Request::default(), root, opened.fh, 0)
        .await
        .unwrap()
        .entries
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(initial, retried);
    assert!(initial.len() < expected.len());

    let mut offset = 0;
    let mut actual = BTreeSet::new();
    for _ in 0..100 {
        let entries = filesystem
            .readdir(Request::default(), root, opened.fh, offset)
            .await
            .unwrap()
            .entries
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        if entries.is_empty() {
            break;
        }

        // Model a kernel reply buffer that accepts only a prefix. The next
        // callback acknowledges exactly the last offset it received.
        for entry in entries.iter().take(11) {
            if entry.name != OsStr::new(".") && entry.name != OsStr::new("..") {
                assert!(actual.insert(entry.name.to_string_lossy().into_owned()));
            }
            offset = entry.offset;
        }
    }
    assert_eq!(actual, expected);
    assert_eq!(audit.lists.load(Ordering::Relaxed), 1);

    filesystem
        .releasedir(Request::default(), root, opened.fh, libc::O_RDONLY as u32)
        .await
        .unwrap();
    assert!(
        filesystem
            .readdir(Request::default(), root, opened.fh, 0)
            .await
            .is_err()
    );
}
