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

//! Shared primitives for Managed Sync evaluation commands.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::ExitStatus;
use std::process::Stdio;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use serde_json::Value;
use serde_json::json;

pub(crate) const STREAM_BUFFER_SIZE: usize = 256 * 1024;
pub(crate) const PRODUCT_ACCESS_KEY: &str = "ofs-evaluation";
pub(crate) const PRODUCT_SECRET_KEY: &str = "ofs-evaluation-password";
const PROCESS_SAMPLE_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GitProvenance {
    git_revision: String,
    workspace_dirty: bool,
}

impl GitProvenance {
    pub(crate) fn capture(workspace: &Path) -> Self {
        let revision = Command::new("git")
            .current_dir(workspace)
            .args(["rev-parse", "--verify", "HEAD^{commit}"])
            .output()
            .expect("resolve candidate Git revision");
        assert!(
            revision.status.success(),
            "cannot resolve candidate Git revision: {}",
            String::from_utf8_lossy(&revision.stderr).trim()
        );

        let status = Command::new("git")
            .current_dir(workspace)
            .args(["status", "--porcelain=v1", "--untracked-files=all"])
            .output()
            .expect("inspect candidate Git workspace");
        assert!(
            status.status.success(),
            "cannot inspect candidate Git workspace: {}",
            String::from_utf8_lossy(&status.stderr).trim()
        );

        Self {
            git_revision: String::from_utf8_lossy(&revision.stdout).trim().into(),
            workspace_dirty: !status.stdout.is_empty(),
        }
    }

    pub(crate) fn document(&self) -> Value {
        json!({
            "git_revision": self.git_revision,
            "workspace_dirty": self.workspace_dirty,
        })
    }
}

pub(crate) fn write_xof_file(
    path: &Path,
    identity: &[u8],
    revision: u8,
    size: u64,
    buffer: &mut [u8],
) {
    let mut file = fs::File::create(path).expect("create evaluation file");
    write_xof(&mut file, identity, revision, size, buffer);
}

pub(crate) fn write_xof(
    writer: &mut impl Write,
    identity: &[u8],
    revision: u8,
    size: u64,
    buffer: &mut [u8],
) {
    assert!(!buffer.is_empty(), "evaluation stream buffer is empty");
    let mut seed = blake3::Hasher::new();
    seed.update(b"ofs-managed-sync-evaluation-content\0");
    seed.update(identity);
    seed.update(&[revision]);
    let mut source = seed.finalize_xof();
    let mut remaining = size;
    while remaining != 0 {
        let length = remaining.min(buffer.len() as u64) as usize;
        source.fill(&mut buffer[..length]);
        writer
            .write_all(&buffer[..length])
            .expect("write evaluation file content");
        remaining -= length as u64;
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TreeSummary {
    pub(crate) label: String,
    pub(crate) fingerprint: String,
    pub(crate) files: u64,
    pub(crate) directories: u64,
    pub(crate) bytes: u64,
}

impl TreeSummary {
    pub(crate) fn document(&self) -> Value {
        json!({
            "label": self.label,
            "tree_fingerprint": self.fingerprint,
            "files": self.files,
            "directories": self.directories,
            "logical_bytes": self.bytes,
        })
    }
}

pub(crate) fn tree_summary(root: &Path, label: &str) -> TreeSummary {
    let mut summary = TreeSummary {
        label: label.to_owned(),
        fingerprint: String::new(),
        files: 0,
        directories: 0,
        bytes: 0,
    };
    let mut buffer = vec![0; STREAM_BUFFER_SIZE];
    let fingerprint = hash_directory(root, &mut summary, &mut buffer);
    summary.fingerprint = fingerprint.to_hex().to_string();
    summary
}

fn hash_directory(directory: &Path, summary: &mut TreeSummary, buffer: &mut [u8]) -> blake3::Hash {
    let mut entries: Vec<_> = fs::read_dir(directory)
        .expect("read evaluation tree directory")
        .map(|entry| entry.expect("read evaluation tree entry"))
        .collect();
    entries.sort_by_key(|entry| entry.file_name());

    let mut fingerprint = blake3::Hasher::new();
    for entry in entries {
        let name: OsString = entry.file_name();
        let name = name.to_string_lossy();
        fingerprint.update(&(name.len() as u64).to_be_bytes());
        fingerprint.update(name.as_bytes());
        let metadata = entry.metadata().expect("read evaluation tree metadata");
        if metadata.is_dir() {
            summary.directories += 1;
            fingerprint.update(b"d");
            fingerprint.update(hash_directory(&entry.path(), summary, buffer).as_bytes());
        } else {
            summary.files += 1;
            summary.bytes += metadata.len();
            fingerprint.update(b"f");
            fingerprint.update(&metadata.len().to_be_bytes());
            let mut file = fs::File::open(entry.path()).expect("open evaluation tree file");
            loop {
                let read = file.read(buffer).expect("read evaluation tree file");
                if read == 0 {
                    break;
                }
                fingerprint.update(&buffer[..read]);
            }
        }
    }
    fingerprint.finalize()
}

pub(crate) struct ProcessMeasurement {
    pub(crate) status: ExitStatus,
    pub(crate) elapsed: Duration,
    pub(crate) peak_rss_bytes: u64,
    pub(crate) stdout_log: PathBuf,
    pub(crate) stderr_log: PathBuf,
}

pub(crate) fn capture_process(mut command: Command, logs: &Path, name: &str) -> ProcessMeasurement {
    let stdout_log = logs.join(format!("{name}.stdout"));
    let stderr_log = logs.join(format!("{name}.stderr"));
    let stdout = fs::File::create(&stdout_log).expect("create evaluation stdout log");
    let stderr = fs::File::create(&stderr_log).expect("create evaluation stderr log");
    command.stdout(Stdio::from(stdout));
    command.stderr(Stdio::from(stderr));
    let start = Instant::now();
    let mut child = command.spawn().expect("start evaluation phase");
    let mut peak_rss_bytes = 0;
    let status = loop {
        peak_rss_bytes = peak_rss_bytes.max(process_peak_rss_bytes(child.id()));
        if let Some(status) = child.try_wait().expect("poll evaluation phase") {
            break status;
        }
        thread::sleep(PROCESS_SAMPLE_INTERVAL);
    };
    ProcessMeasurement {
        status,
        elapsed: start.elapsed(),
        peak_rss_bytes,
        stdout_log,
        stderr_log,
    }
}

fn process_peak_rss_bytes(process: u32) -> u64 {
    let Ok(status) = fs::read_to_string(format!("/proc/{process}/status")) else {
        return 0;
    };
    status
        .lines()
        .filter_map(|line| {
            let kibibytes = line
                .strip_prefix("VmHWM:")
                .or_else(|| line.strip_prefix("VmRSS:"))?
                .trim();
            kibibytes.split_whitespace().next()?.parse::<u64>().ok()
        })
        .max()
        .unwrap_or_default()
        * 1024
}

pub(crate) fn log_contains(path: &Path, needle: &[u8]) -> bool {
    assert!(!needle.is_empty(), "evaluation log needle is empty");
    let mut file = fs::File::open(path).expect("open evaluation log");
    let mut buffer = [0; 8 * 1024];
    let mut overlap = Vec::new();
    loop {
        let read = file.read(&mut buffer).expect("read evaluation log");
        if read == 0 {
            return false;
        }
        overlap.extend_from_slice(&buffer[..read]);
        if overlap.windows(needle.len()).any(|window| window == needle) {
            return true;
        }
        let retained = needle.len().saturating_sub(1).min(overlap.len());
        overlap.drain(..overlap.len() - retained);
    }
}

pub(crate) fn wait_for_audit_marker(path: &Path, marker: &str, timeout: Duration) {
    assert!(!marker.is_empty(), "evaluation audit marker is empty");
    let deadline = Instant::now() + timeout;
    let mut previous = Vec::new();
    let mut stable_observations = 0;

    while Instant::now() < deadline {
        let current = fs::read(path).expect("read evaluation audit state");
        let document: Value = serde_json::from_slice(&current).expect("audit state is JSON");
        let found = document
            .get("markers")
            .and_then(Value::as_array)
            .is_some_and(|markers| markers.iter().any(|value| value.as_str() == Some(marker)));
        if found && current == previous {
            stable_observations += 1;
            if stable_observations == 25 {
                return;
            }
        } else {
            stable_observations = 0;
        }
        previous = current;
        thread::sleep(Duration::from_millis(200));
    }
    panic!("evaluation audit state did not reach its marker");
}

pub(crate) fn short_log_excerpt(path: &Path) -> String {
    let mut bytes = Vec::with_capacity(4 * 1024);
    fs::File::open(path)
        .expect("open evaluation failure log")
        .take(4 * 1024)
        .read_to_end(&mut bytes)
        .expect("read evaluation failure log");
    String::from_utf8_lossy(&bytes).trim().to_owned()
}

pub(crate) fn write_json_atomic(path: &Path, document: &Value) {
    let temporary = path.with_extension("json.tmp");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(document).expect("encode evaluation report"),
    )
    .expect("write evaluation report");
    fs::rename(&temporary, path).expect("publish evaluation report");
}

pub(crate) fn absolute_from_workspace(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        PathBuf::from(env!("CARGO_WORKSPACE_DIR")).join(path)
    }
}

pub(crate) fn use_product_credentials(command: &mut Command) {
    command
        .env("AWS_ACCESS_KEY_ID", PRODUCT_ACCESS_KEY)
        .env("AWS_SECRET_ACCESS_KEY", PRODUCT_SECRET_KEY);
}

pub(crate) fn inventory(mut command: Command) -> Value {
    command.stdout(Stdio::piped()).stderr(Stdio::inherit());
    let mut child = command.spawn().expect("start object inventory");
    let stdout = child.stdout.take().expect("object inventory has stdout");
    let mut classes = BTreeMap::<String, ObjectTotal>::new();
    let mut prefixes = BTreeMap::<String, ObjectTotal>::new();
    let mut objects = 0_u64;
    let mut bytes = 0_u64;
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("read object inventory");
        let line = line.trim();
        if line.is_empty()
            || (line.len() == 64 && line.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            continue;
        }
        let record: Value = serde_json::from_str(line).expect("object inventory line is JSON");
        if record.get("type").and_then(Value::as_str) != Some("file") {
            continue;
        }
        let key = record
            .get("key")
            .and_then(Value::as_str)
            .expect("object inventory entry has a key");
        let size = record
            .get("size")
            .and_then(Value::as_u64)
            .expect("object inventory entry has a size");
        objects += 1;
        bytes = bytes.checked_add(size).expect("inventory bytes fit in u64");
        classes
            .entry(object_class(key).into())
            .or_default()
            .add(size);
        if let Some(prefix) = digest_prefix(key) {
            prefixes.entry(prefix.into()).or_default().add(size);
        }
    }
    let status = child.wait().expect("finish object inventory");
    assert!(
        status.success(),
        "object inventory command failed: {status}"
    );
    assert!(objects > 0, "object inventory is empty");
    json!({
        "objects": objects,
        "bytes": bytes,
        "classes": totals_document(classes),
        "digest_prefixes": totals_document(prefixes),
    })
}

pub(crate) fn audit_summary(path: &Path, volume_root: &str) -> Value {
    let document: Value = serde_json::from_slice(&fs::read(path).expect("read MinIO audit state"))
        .expect("MinIO audit state is JSON");
    let summary = document
        .get("volumes")
        .and_then(|volumes| volumes.get(volume_root))
        .unwrap_or_else(|| panic!("MinIO audit state has no volume {volume_root}"));
    assert!(
        summary
            .get("requests")
            .and_then(Value::as_u64)
            .is_some_and(|requests| requests > 0),
        "MinIO audit summary is empty"
    );
    summary.clone()
}

#[derive(Default)]
struct ObjectTotal {
    objects: u64,
    bytes: u64,
}

impl ObjectTotal {
    fn add(&mut self, bytes: u64) {
        self.objects += 1;
        self.bytes = self.bytes.saturating_add(bytes);
    }
}

fn totals_document(totals: BTreeMap<String, ObjectTotal>) -> Value {
    Value::Object(
        totals
            .into_iter()
            .map(|(key, total)| (key, json!({"objects": total.objects, "bytes": total.bytes})))
            .collect(),
    )
}

fn object_class(key: &str) -> &'static str {
    if key.contains("/objects/raw/") || key.contains(".ofs/managed/data/") {
        "raw"
    } else if key.contains("/objects/meta/")
        || key.contains("/objects/commit/")
        || key.contains(".ofs/managed/metadata/")
    {
        "metadata"
    } else {
        "control"
    }
}

fn digest_prefix(key: &str) -> Option<&str> {
    [
        "/objects/raw/",
        "/objects/meta/",
        "/objects/commit/",
        ".ofs/managed/data/v1/segments/blake3/",
        ".ofs/managed/metadata/v1/checkpoints/blake3/",
        ".ofs/managed/metadata/v1/changes/blake3/",
    ]
    .into_iter()
    .find_map(|marker| {
        let suffix = key.split_once(marker)?.1;
        let prefix = suffix.split('/').next()?;
        (prefix.len() == 2).then_some(prefix)
    })
}
