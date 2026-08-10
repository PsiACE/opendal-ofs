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

use std::ffi::OsStr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use bytes::Bytes;
use fuse3::Errno;
use fuse3::Result;
use fuse3::path::prelude::*;
use futures_util::StreamExt;
use futures_util::stream;
use futures_util::stream::BoxStream;
use opendal::EntryMode;
use opendal::ErrorKind;
use opendal::Metadata;
use opendal::Operator;
use sharded_slab::Slab;

use super::directory::OpenedDirectory;
use super::file::FileKey;
use super::file::OpenedFile;

const TTL: Duration = Duration::from_secs(1); // 1 second

/// Read-only [`PathFilesystem`] backed by an OpenDAL [`Operator`].
pub struct Filesystem {
    op: Operator,
    gid: u32,
    uid: u32,

    opened_files: Slab<OpenedFile>,
    opened_directories: Slab<Arc<OpenedDirectory>>,
}

impl Filesystem {
    /// Create a new filesystem with given operator, uid and gid.
    pub fn new(op: Operator, uid: u32, gid: u32) -> Self {
        Self {
            op,
            uid,
            gid,
            opened_files: Slab::new(),
            opened_directories: Slab::new(),
        }
    }

    fn check_open_flags(flags: u32) -> Result<()> {
        let access_mode = flags & libc::O_ACCMODE as u32;
        let mutating = flags & (libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND) as u32 != 0;
        if access_mode != libc::O_RDONLY as u32 || mutating {
            return Err(Errno::from(libc::EROFS));
        }
        Ok(())
    }

    // Get opened file and check given path
    fn get_opened_file(
        &self,
        key: FileKey,
        path: Option<&OsStr>,
    ) -> Result<sharded_slab::Entry<'_, OpenedFile>> {
        let file = self
            .opened_files
            .get(key.0)
            .ok_or(Errno::from(libc::EBADF))?;

        if matches!(path, Some(path) if path != file.path) {
            log::trace!(
                "get_opened_file: path not match: path={:?}, file={:?}",
                path,
                file.path
            );
            Err(Errno::from(libc::EBADF))?;
        }

        Ok(file)
    }

    fn get_opened_directory(&self, key: FileKey, path: &OsStr) -> Result<Arc<OpenedDirectory>> {
        let directory = self
            .opened_directories
            .get(key.0)
            .ok_or(Errno::from(libc::EBADF))?
            .clone();
        if !directory.matches(path) {
            Err(Errno::from(libc::EBADF))?;
        }
        Ok(directory)
    }
}

impl PathFilesystem for Filesystem {
    // Init a fuse filesystem
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(16 * 1024).unwrap(),
        })
    }

    // Callback when fs is being destroyed
    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        log::debug!("lookup(parent={parent:?}, name={name:?})");

        let path = child_path(parent, name)?;
        let metadata = self.op.stat(&path).await.map_err(opendal_error2errno)?;

        let now = SystemTime::now();
        let attr = metadata2file_attr(&metadata, now, self.uid, self.gid);

        Ok(ReplyEntry { ttl: TTL, attr })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: Option<u64>,
        flags: u32,
    ) -> Result<ReplyAttr> {
        log::debug!("getattr(path={path:?}, fh={fh:?}, flags={flags:?})");

        let fh_path = match fh {
            Some(fh) => Some(
                self.get_opened_file(FileKey::try_from(fh)?, path)?
                    .path
                    .clone(),
            ),
            None => None,
        };

        let file_path = match (path.map(Into::into), fh_path) {
            (Some(a), Some(_)) => Some(a),
            (a, b) => a.or(b),
        };

        let file_path = file_path.unwrap_or_default();
        let metadata = self
            .op
            .stat(opendal_path(&file_path)?)
            .await
            .map_err(opendal_error2errno)?;

        let now = SystemTime::now();
        let attr = metadata2file_attr(&metadata, now, self.uid, self.gid);

        Ok(ReplyAttr { ttl: TTL, attr })
    }

    async fn opendir(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        log::debug!("opendir(path={path:?}, flags=0x{flags:x})");
        Self::check_open_flags(flags)?;
        let key = self
            .opened_directories
            .insert(Arc::new(
                OpenedDirectory::new(path).ok_or(Errno::from(libc::EILSEQ))?,
            ))
            .ok_or(Errno::from(libc::EBUSY))?;
        Ok(ReplyOpen {
            fh: FileKey(key).to_fh(),
            flags,
        })
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        log::debug!("open(path={path:?}, flags=0x{flags:x})");

        Self::check_open_flags(flags)?;
        let metadata = self
            .op
            .stat(opendal_path(path)?)
            .await
            .map_err(opendal_error2errno)?;
        let capability = self.op.info().full_capability();
        let version = capability
            .read_with_version
            .then(|| metadata.version().map(str::to_owned))
            .flatten();
        let etag = (version.is_none() && capability.read_with_if_match)
            .then(|| metadata.etag().map(str::to_owned))
            .flatten();

        let key = self
            .opened_files
            .insert(OpenedFile {
                path: path.into(),
                content_length: metadata.content_length(),
                version,
                etag,
            })
            .ok_or(Errno::from(libc::EBUSY))?;

        Ok(ReplyOpen {
            fh: FileKey(key).to_fh(),
            flags,
        })
    }

    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        log::debug!("read(path={path:?}, fh={fh}, offset={offset}, size={size})");

        let (file_path, content_length, version, etag) = {
            let file = self.get_opened_file(FileKey::try_from(fh)?, path)?;
            (
                opendal_path(&file.path)?.to_owned(),
                file.content_length,
                file.version.clone(),
                file.etag.clone(),
            )
        };
        if size == 0 || offset >= content_length {
            return Ok(ReplyData { data: Bytes::new() });
        }
        let end = offset.saturating_add(u64::from(size)).min(content_length);

        let mut read = self.op.read_with(&file_path).range(offset..end);
        if let Some(version) = &version {
            read = read.version(version);
        } else if let Some(etag) = &etag {
            read = read.if_match(etag);
        }
        let data = read.await.map_err(opendal_error2errno)?;

        Ok(ReplyData {
            data: data.to_bytes(),
        })
    }

    async fn release(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> Result<()> {
        log::debug!(
            "release(path={path:?}, fh={fh}, flags=0x{flags:x}, lock_owner={lock_owner}, flush={flush})"
        );

        let key = FileKey::try_from(fh)?;
        {
            self.get_opened_file(key, path)?;
        }
        self.opened_files.take(key.0);
        Ok(())
    }

    /// Validate repeated close-time flushes without changing the read-only handle.
    async fn flush(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        lock_owner: u64,
    ) -> Result<()> {
        log::debug!("flush(path={path:?}, fh={fh}, lock_owner={lock_owner})");

        self.get_opened_file(FileKey::try_from(fh)?, path)?;
        Ok(())
    }

    type DirEntryStream<'a> = BoxStream<'a, Result<DirectoryEntry>>;

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        path: &'a OsStr,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        log::debug!("readdir(path={path:?}, fh={fh}, offset={offset})");
        let offset = u64::try_from(offset).map_err(|_| Errno::from(libc::EINVAL))?;
        let directory = self.get_opened_directory(FileKey::try_from(fh)?, path)?;
        let children = directory
            .entries(&self.op, offset)
            .await
            .map_err(opendal_error2errno)?;
        let child = offset.saturating_sub(2);
        let mut entries = Vec::with_capacity(children.len() + 2);
        if offset == 0 {
            entries.push(Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: ".".into(),
                offset: 1,
            }));
        }
        if offset <= 1 {
            entries.push(Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: "..".into(),
                offset: 2,
            }));
        }
        entries.extend(children.into_iter().enumerate().map(|(index, entry)| {
            Ok(DirectoryEntry {
                kind: entry_mode2file_type(entry.metadata().mode()),
                name: entry.name().trim_matches('/').into(),
                offset: directory_entry_offset(child, index)?,
            })
        }));

        Ok(ReplyDirectory {
            entries: stream::iter(entries).boxed(),
        })
    }

    async fn releasedir(&self, _req: Request, path: &OsStr, fh: u64, _flags: u32) -> Result<()> {
        log::debug!("releasedir(path={path:?}, fh={fh})");
        let key = FileKey::try_from(fh)?;
        self.get_opened_directory(key, path)?;
        self.opened_directories.take(key.0);
        Ok(())
    }

    async fn access(&self, _req: Request, path: &OsStr, mask: u32) -> Result<()> {
        log::debug!("access(path={path:?}, mask=0x{mask:x})");

        if mask & libc::W_OK as u32 != 0 {
            return Err(Errno::from(libc::EROFS));
        }
        let metadata = self
            .op
            .stat(opendal_path(path)?)
            .await
            .map_err(opendal_error2errno)?;
        if mask & libc::X_OK as u32 != 0 && metadata.mode() != EntryMode::DIR {
            return Err(Errno::from(libc::EACCES));
        }
        Ok(())
    }

    type DirEntryPlusStream<'a> = BoxStream<'a, Result<DirectoryEntryPlus>>;

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        parent: &'a OsStr,
        fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        log::debug!("readdirplus(parent={parent:?}, fh={fh}, offset={offset})");
        if offset > i64::MAX as u64 {
            Err(Errno::from(libc::EOVERFLOW))?;
        }
        let now = SystemTime::now();
        let uid = self.uid;
        let gid = self.gid;
        let directory = self.get_opened_directory(FileKey::try_from(fh)?, parent)?;
        let children = directory
            .entries(&self.op, offset)
            .await
            .map_err(opendal_error2errno)?;
        let child = offset.saturating_sub(2);
        let mut entries = Vec::with_capacity(children.len() + 2);
        let relative_path_attr = dummy_file_attr(FileType::Directory, now, uid, gid);
        if offset == 0 {
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: ".".into(),
                offset: 1,
                attr: relative_path_attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }));
        }
        if offset <= 1 {
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: "..".into(),
                offset: 2,
                attr: relative_path_attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }));
        }
        entries.extend(children.into_iter().enumerate().map(|(index, entry)| {
            Ok(DirectoryEntryPlus {
                kind: entry_mode2file_type(entry.metadata().mode()),
                name: entry.name().trim_matches('/').into(),
                offset: directory_entry_offset(child, index)?,
                attr: metadata2file_attr(entry.metadata(), now, uid, gid),
                entry_ttl: TTL,
                attr_ttl: TTL,
            })
        }));

        Ok(ReplyDirectoryPlus {
            entries: stream::iter(entries).boxed(),
        })
    }

    async fn statfs(&self, _req: Request, path: &OsStr) -> Result<ReplyStatFs> {
        log::debug!("statfs(path={path:?})");
        Ok(ReplyStatFs {
            blocks: 1,
            bfree: 0,
            bavail: 0,
            files: 1,
            ffree: 0,
            bsize: 4096,
            namelen: u32::MAX,
            frsize: 0,
        })
    }
}

fn opendal_path(path: &OsStr) -> Result<&str> {
    path.to_str().ok_or_else(|| Errno::from(libc::EILSEQ))
}

fn child_path(parent: &OsStr, name: &OsStr) -> Result<String> {
    PathBuf::from(parent)
        .join(name)
        .into_os_string()
        .into_string()
        .map_err(|_| Errno::from(libc::EILSEQ))
}

fn directory_entry_offset(base: u64, index: usize) -> Result<i64> {
    let index = u64::try_from(index).map_err(|_| Errno::from(libc::EOVERFLOW))?;
    base.checked_add(index)
        .and_then(|offset| offset.checked_add(3))
        .and_then(|offset| i64::try_from(offset).ok())
        .ok_or_else(|| Errno::from(libc::EOVERFLOW))
}

const fn entry_mode2file_type(mode: EntryMode) -> FileType {
    match mode {
        EntryMode::DIR => FileType::Directory,
        _ => FileType::RegularFile,
    }
}

fn metadata2file_attr(metadata: &Metadata, atime: SystemTime, uid: u32, gid: u32) -> FileAttr {
    let last_modified = match metadata.last_modified() {
        None => atime,
        Some(ts) => ts.into(),
    };
    let kind = entry_mode2file_type(metadata.mode());
    FileAttr {
        size: metadata.content_length(),
        mtime: last_modified,
        ctime: last_modified,
        ..dummy_file_attr(kind, atime, uid, gid)
    }
}

const fn dummy_file_attr(kind: FileType, now: SystemTime, uid: u32, gid: u32) -> FileAttr {
    let (mode, nlink) = match kind {
        FileType::Directory => (0o555, 2),
        _ => (0o444, 1),
    };
    FileAttr {
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        kind,
        perm: mode,
        nlink,
        uid,
        gid,
        rdev: 0,
        blksize: 4096,
        #[cfg(target_os = "macos")]
        crtime: now,
        #[cfg(target_os = "macos")]
        flags: 0,
    }
}

fn opendal_error2errno(err: opendal::Error) -> fuse3::Errno {
    log::trace!("opendal_error2errno: {err:?}");
    match err.kind() {
        ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
        ErrorKind::IsADirectory => Errno::from(libc::EISDIR),
        ErrorKind::NotFound => Errno::from(libc::ENOENT),
        ErrorKind::PermissionDenied => Errno::from(libc::EACCES),
        ErrorKind::AlreadyExists => Errno::from(libc::EEXIST),
        ErrorKind::NotADirectory => Errno::from(libc::ENOTDIR),
        ErrorKind::RangeNotSatisfied => Errno::from(libc::EINVAL),
        ErrorKind::RateLimited => Errno::from(libc::EAGAIN),
        ErrorKind::IsSameFile => Errno::from(libc::EINVAL),
        ErrorKind::ConditionNotMatch => Errno::from(libc::ESTALE),
        ErrorKind::ConfigInvalid | ErrorKind::Unexpected => Errno::from(libc::EIO),
        _ => Errno::from(libc::EIO),
    }
}
