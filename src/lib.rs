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

//! Shared filesystem semantics used by ofs access and volume implementations.

pub mod catalog;
pub mod filesystem;
pub mod managed;
pub mod sync;

mod durable {
    use std::fs::{self, OpenOptions};
    use std::io::Write as _;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    use anyhow::Result;
    use serde::Serialize;

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    pub(crate) enum JsonFormat {
        Compact,
        Pretty,
    }

    pub(crate) fn install_json(
        path: &Path,
        temporary_tag: &str,
        value: &impl Serialize,
        format: JsonFormat,
    ) -> Result<()> {
        let mut bytes = match format {
            JsonFormat::Compact => serde_json::to_vec(value),
            JsonFormat::Pretty => serde_json::to_vec_pretty(value),
        }?;
        bytes.push(b'\n');
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = path.with_extension(format!(
            "{temporary_tag}.{}.{sequence}.tmp",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        let result = (|| -> Result<()> {
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, path)?;
            #[cfg(unix)]
            fs::File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(temporary);
        }
        result
    }
}
