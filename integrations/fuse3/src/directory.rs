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

use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};

use futures_util::StreamExt;
use opendal::raw::normalize_path;
use opendal::{Entry, Lister, Operator};
use tokio::sync::Mutex;

// fuse3's path bridge collects a callback's entire stream before its reply
// encoder applies the kernel buffer limit. Keep callbacks bounded and retain
// the returned entries until the next offset acknowledges them.
const REPLAY_WINDOW_ENTRIES: usize = 128;

pub(crate) struct OpenedDirectory {
    path: OsString,
    cursor: Mutex<DirectoryCursor>,
}

impl OpenedDirectory {
    pub(crate) fn new(path: &OsStr) -> Option<Self> {
        Some(Self {
            path: path.into(),
            cursor: Mutex::new(DirectoryCursor::new(path.to_str()?)),
        })
    }

    pub(crate) fn matches(&self, path: &OsStr) -> bool {
        self.path == path
    }

    pub(crate) async fn entries(
        &self,
        operator: &Operator,
        offset: u64,
    ) -> opendal::Result<Vec<Entry>> {
        self.cursor.lock().await.entries(operator, offset).await
    }
}

struct DirectoryCursor {
    path: String,
    lister: Option<Lister>,
    window: VecDeque<Entry>,
    base: u64,
    exhausted: bool,
}

impl DirectoryCursor {
    fn new(path: &str) -> Self {
        let mut path = path.to_owned();
        if !path.ends_with('/') {
            path.push('/');
        }
        Self {
            path: normalize_path(&path),
            lister: None,
            window: VecDeque::with_capacity(REPLAY_WINDOW_ENTRIES),
            base: 0,
            exhausted: false,
        }
    }

    async fn entries(&mut self, operator: &Operator, offset: u64) -> opendal::Result<Vec<Entry>> {
        let requested = offset.saturating_sub(2);
        let result = self.entries_inner(operator, requested).await;
        if result.is_err() {
            self.reset();
        }
        result
    }

    async fn entries_inner(
        &mut self,
        operator: &Operator,
        requested: u64,
    ) -> opendal::Result<Vec<Entry>> {
        if self.lister.is_none() && !self.exhausted || requested < self.base {
            self.restart(operator).await?;
        }

        while self.base < requested {
            if self.window.pop_front().is_some() {
                self.base += 1;
                continue;
            }
            let Some(entry) = self.next().await? else {
                return Ok(Vec::new());
            };
            drop(entry);
            self.base += 1;
        }

        while self.window.len() < REPLAY_WINDOW_ENTRIES {
            let Some(entry) = self.next().await? else {
                break;
            };
            self.window.push_back(entry);
        }
        Ok(self.window.iter().cloned().collect())
    }

    async fn restart(&mut self, operator: &Operator) -> opendal::Result<()> {
        self.window.clear();
        self.base = 0;
        self.exhausted = false;
        self.lister = Some(operator.lister(&self.path).await?);
        Ok(())
    }

    async fn next(&mut self) -> opendal::Result<Option<Entry>> {
        loop {
            let Some(lister) = self.lister.as_mut() else {
                return Ok(None);
            };
            match lister.next().await.transpose()? {
                Some(entry) if normalize_path(entry.path()) == self.path => continue,
                Some(entry) => return Ok(Some(entry)),
                None => {
                    self.lister = None;
                    self.exhausted = true;
                    return Ok(None);
                }
            }
        }
    }

    fn reset(&mut self) {
        self.lister = None;
        self.window.clear();
        self.base = 0;
        self.exhausted = false;
    }
}
