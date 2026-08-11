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

//! Fixed-scale Managed Sync acceptance profiles.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde_json::Value;
use serde_json::json;

use super::Fixture;
use super::evaluation::STREAM_BUFFER_SIZE;
use super::evaluation::TreeSummary;
use super::evaluation::absolute_from_workspace;
use super::evaluation::audit_summary;
use super::evaluation::capture_process;
use super::evaluation::log_contains;
use super::evaluation::short_log_excerpt;
use super::evaluation::tree_summary;
use super::evaluation::use_product_credentials;
use super::evaluation::write_json_atomic;
use super::evaluation::write_xof_file;

const TINY_FILE_COUNT: u64 = 1_000_000;
const TINY_FILE_SIZE: u64 = 4 * 1024;
const TINY_CHANGE_COUNT: u64 = 4_096;
const LARGE_FILE_COUNT: u64 = 3;
const LARGE_FILE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
const MAX_REPLICA_STATE_BYTES: u64 = 16 * 1024;
const TRANSFER_CONCURRENCY: usize = 16;

pub(crate) fn run(profile: &str, output: &Path, keep: bool) {
    let profile = Profile::parse(profile);
    super::build_ofs_release();
    let binary = super::ofs_release_binary();

    let output = absolute_from_workspace(output);
    fs::create_dir_all(&output).expect("create scale report directory");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_secs();
    let run_name = format!("{}-{nonce}-{}", profile.name(), std::process::id());
    let report_path = output.join(format!("{run_name}.json"));
    let audit_directory = output.join(format!(".{run_name}.audit"));
    let audit_state = audit_directory.join("audit.json");
    let work = WorkRoot::new(output.join(format!(".{run_name}.work")), keep);
    let fixture = Fixture::new(keep).start_audited(audit_state.clone());
    fixture.create_bucket();
    fixture.create_evaluation_user();

    let replica_a = work.path.join("replica-a");
    let replica_b = work.path.join("replica-b");
    let replica_c = work.path.join("replica-c");
    let state_a = work.path.join("state-a");
    let state_b = work.path.join("state-b");
    let state_c = work.path.join("state-c");
    for replica in [&replica_a, &replica_b, &replica_c] {
        fs::create_dir(replica).expect("create scale replica");
    }

    let volume_root = format!("scale/{run_name}");
    let storage = fixture.storage_url(&volume_root);
    let mut runner = Runner::new(
        report_path,
        output.join(format!("{run_name}.logs")),
        profile,
        &volume_root,
        binary.clone(),
        keep,
    );
    runner.write_report();

    let generation_started = Instant::now();
    generate_initial(profile, &replica_a);
    runner.record_generation(generation_started.elapsed());
    runner.stage(
        "volume-create",
        scale_command(super::ofs_volume_create_with(&binary, &storage)),
        None,
    );
    runner.stage(
        "initial-publish-a",
        scale_command(super::ofs_sync_with(
            &binary, &replica_a, &state_a, &storage,
        )),
        Some((&replica_a, &state_a)),
    );
    runner.stage(
        "cold-restore-b",
        scale_command(super::ofs_sync_with(
            &binary, &replica_b, &state_b, &storage,
        )),
        Some((&replica_b, &state_b)),
    );
    runner.observe_equal(
        "initial-cold-restore",
        &replica_a,
        "replica-a",
        &replica_b,
        "replica-b",
    );

    for (name, replica, state) in [
        ("no-op-a", &replica_a, &state_a),
        ("no-op-b", &replica_b, &state_b),
    ] {
        let published = runner.stage(
            name,
            scale_command(super::ofs_sync_with(&binary, replica, state, &storage)),
            Some((replica, state)),
        );
        runner.require(
            !published,
            &format!("{name} unexpectedly published a generation"),
        );
    }

    mutate(profile, Side::A, &replica_a);
    mutate(profile, Side::B, &replica_b);
    runner.stage(
        "publish-a-update",
        scale_command(super::ofs_sync_with(
            &binary, &replica_a, &state_a, &storage,
        )),
        Some((&replica_a, &state_a)),
    );
    runner.stage(
        "merge-b-update",
        scale_command(super::ofs_sync_with(
            &binary, &replica_b, &state_b, &storage,
        )),
        Some((&replica_b, &state_b)),
    );
    runner.stage(
        "install-merged-a",
        scale_command(super::ofs_sync_with(
            &binary, &replica_a, &state_a, &storage,
        )),
        Some((&replica_a, &state_a)),
    );
    runner.observe_equal(
        "bidirectional-convergence",
        &replica_a,
        "replica-a",
        &replica_b,
        "replica-b",
    );

    runner.stage(
        "collect-current-namespace",
        scale_command(super::ofs_gc_with_binary(&binary, &storage, false)),
        Some((&replica_a, &state_a)),
    );
    runner.stage(
        "stateless-cold-restore-c",
        scale_command(super::ofs_sync_with(
            &binary, &replica_c, &state_c, &storage,
        )),
        Some((&replica_c, &state_c)),
    );
    runner.observe_equal(
        "post-collection-cold-restore",
        &replica_a,
        "replica-a",
        &replica_c,
        "replica-c",
    );
    let barrier_storage = fixture.storage_url(&format!("audit-barrier/{run_name}"));
    fixture.finish_audit_with(
        &run_name,
        scale_command(super::ofs_volume_create_with(&binary, &barrier_storage)),
    );
    runner.record_backend(
        fixture.inventory(&volume_root),
        audit_summary(&audit_state, &volume_root),
        keep.then(|| audit_state.to_string_lossy().into_owned()),
    );
    if !keep {
        fs::remove_dir_all(&audit_directory).expect("remove MinIO audit state");
    }
    runner.complete();

    println!(
        "Managed Sync scale profile passed: {}\nreport: {}",
        profile.name(),
        runner.report_path.display()
    );
}

#[derive(Clone, Copy)]
enum Profile {
    TinyFiles,
    LargeFiles,
}

impl Profile {
    fn parse(value: &str) -> Self {
        match value {
            "tiny-files" => Self::TinyFiles,
            "large-files" => Self::LargeFiles,
            _ => panic!("unknown Managed Sync scale profile: {value}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::TinyFiles => "tiny-files",
            Self::LargeFiles => "large-files",
        }
    }

    const fn file_count(self) -> u64 {
        match self {
            Self::TinyFiles => TINY_FILE_COUNT,
            Self::LargeFiles => LARGE_FILE_COUNT,
        }
    }

    const fn file_size(self) -> u64 {
        match self {
            Self::TinyFiles => TINY_FILE_SIZE,
            Self::LargeFiles => LARGE_FILE_SIZE,
        }
    }

    const fn mutation_count(self, side: Side) -> u64 {
        match (self, side) {
            (Self::TinyFiles, _) => TINY_CHANGE_COUNT * 4,
            (Self::LargeFiles, Side::A) => 1,
            (Self::LargeFiles, Side::B) => 2,
        }
    }

    fn mutation_plan(self) -> Value {
        match self {
            Self::TinyFiles => json!({
                "kind": "sparse-disjoint",
                "per_replica": {
                    "creates": TINY_CHANGE_COUNT,
                    "deletes": TINY_CHANGE_COUNT,
                    "modifications": TINY_CHANGE_COUNT,
                    "renames": TINY_CHANGE_COUNT,
                },
            }),
            Self::LargeFiles => json!({
                "kind": "whole-file-replacement",
                "replica_a": [data_path("data", 0)],
                "replica_b": [data_path("data", 1), data_path("data", 2)],
                "all_initial_files_replaced": true,
            }),
        }
    }
}

#[derive(Clone, Copy)]
enum Side {
    A,
    B,
}

impl Side {
    const fn name(self) -> &'static str {
        match self {
            Self::A => "a",
            Self::B => "b",
        }
    }

    const fn seed(self) -> u8 {
        match self {
            Self::A => 2,
            Self::B => 3,
        }
    }
}

fn generate_initial(profile: Profile, root: &Path) {
    if matches!(profile, Profile::TinyFiles) {
        for directory in ["created/a", "created/b", "renamed/a", "renamed/b"] {
            fs::create_dir_all(root.join(directory))
                .expect("create shared scale mutation directory");
        }
    }
    let mut buffer = vec![0; STREAM_BUFFER_SIZE];
    let mut previous_parent = PathBuf::new();
    for index in 0..profile.file_count() {
        let relative = data_path("data", index);
        let path = root.join(&relative);
        let parent = path.parent().expect("generated file has a parent");
        if parent != previous_parent {
            fs::create_dir_all(parent).expect("create generated data directory");
            previous_parent = parent.to_owned();
        }
        write_content(&path, &relative, 1, profile.file_size(), &mut buffer);
    }
}

fn mutate(profile: Profile, side: Side, root: &Path) {
    match profile {
        Profile::TinyFiles => mutate_tiny(side, root),
        Profile::LargeFiles => mutate_large(side, root),
    }
}

fn mutate_tiny(side: Side, root: &Path) {
    let offset = match side {
        Side::A => 0,
        Side::B => 4,
    };
    let mut buffer = vec![0; STREAM_BUFFER_SIZE];
    for change in 0..TINY_CHANGE_COUNT {
        let base = change * 16 + offset;

        let modified = data_path("data", base);
        write_content(
            &root.join(&modified),
            &modified,
            side.seed(),
            TINY_FILE_SIZE,
            &mut buffer,
        );

        let renamed = data_path("data", base + 1);
        let destination = data_path(&format!("renamed/{}", side.name()), base + 1);
        fs::create_dir_all(
            root.join(&destination)
                .parent()
                .expect("renamed file has a parent"),
        )
        .expect("create rename destination");
        fs::rename(root.join(renamed), root.join(destination)).expect("rename scale file");

        fs::remove_file(root.join(data_path("data", base + 2))).expect("delete scale file");

        let created_index = TINY_FILE_COUNT
            + change
            + match side {
                Side::A => 0,
                Side::B => TINY_CHANGE_COUNT,
            };
        let created = data_path(&format!("created/{}", side.name()), created_index);
        let path = root.join(&created);
        fs::create_dir_all(path.parent().expect("created file has a parent"))
            .expect("create scale addition directory");
        write_content(&path, &created, side.seed(), TINY_FILE_SIZE, &mut buffer);
    }
}

fn mutate_large(side: Side, root: &Path) {
    let files: &[u64] = match side {
        Side::A => &[0],
        Side::B => &[1, 2],
    };
    let mut buffer = vec![0; STREAM_BUFFER_SIZE];
    for index in files {
        let relative = data_path("data", *index);
        write_content(
            &root.join(&relative),
            &relative,
            side.seed(),
            LARGE_FILE_SIZE,
            &mut buffer,
        );
    }
}

fn data_path(prefix: &str, index: u64) -> PathBuf {
    PathBuf::from(prefix)
        .join(format!("{:02x}", (index >> 16) & 0xff))
        .join(format!("{:02x}", (index >> 8) & 0xff))
        .join(format!("{index:016x}.bin"))
}

fn write_content(path: &Path, identity: &Path, revision: u8, size: u64, buffer: &mut [u8]) {
    write_xof_file(
        path,
        identity.to_string_lossy().as_bytes(),
        revision,
        size,
        buffer,
    );
}

struct Runner {
    binary: PathBuf,
    report: Value,
    report_path: PathBuf,
    log_directory: PathBuf,
    last_status: Option<Value>,
}

impl Runner {
    fn new(
        report_path: PathBuf,
        log_directory: PathBuf,
        profile: Profile,
        volume_root: &str,
        binary: PathBuf,
        keep: bool,
    ) -> Self {
        fs::create_dir(&log_directory).expect("create scale log directory");
        let report = json!({
            "schema": "ofs.managed-sync.scale/1",
            "status": "running",
            "profile": {
                "name": profile.name(),
                "file_count": profile.file_count(),
                "file_size_bytes": profile.file_size(),
                "logical_bytes": profile.file_count() * profile.file_size(),
                "transfer_concurrency": TRANSFER_CONCURRENCY,
                "mutated_paths": {
                    "replica_a": profile.mutation_count(Side::A),
                    "replica_b": profile.mutation_count(Side::B),
                },
                "mutation_plan": profile.mutation_plan(),
            },
            "volume_root": volume_root,
            "work_tree_retained": keep,
            "phases": [],
            "tree_observations": [],
        });
        Self {
            binary,
            report,
            report_path,
            log_directory,
            last_status: None,
        }
    }

    fn stage(&mut self, name: &str, mut command: Command, replica: Option<(&Path, &Path)>) -> bool {
        println!("scale phase: {name}");
        use_product_credentials(&mut command);
        let captured = capture_process(command, &self.log_directory, name);
        let replica_status =
            replica.and_then(|(root, state)| read_status(&self.binary, root, state));
        let status_available = replica.is_none() || replica_status.is_some();
        if let Some(status) = &replica_status {
            self.last_status = Some(status.clone());
        }
        let state_bytes = replica.and_then(|(_, state)| fs::metadata(state).ok().map(|m| m.len()));
        let state_available = replica.is_none() || state_bytes.is_some();
        let state_bounded = state_bytes.is_none_or(|bytes| bytes <= MAX_REPLICA_STATE_BYTES);
        let success =
            captured.status.success() && status_available && state_available && state_bounded;
        let failure_excerpt = if !captured.status.success() {
            Some(short_log_excerpt(&captured.stderr_log))
        } else if !status_available {
            Some("replica status probe failed".to_owned())
        } else if !state_available {
            Some("replica state is missing".to_owned())
        } else if !state_bounded {
            Some(format!(
                "replica state exceeds {MAX_REPLICA_STATE_BYTES} bytes"
            ))
        } else {
            None
        };
        let stdout_log = self.relative_log(&captured.stdout_log);
        let stderr_log = self.relative_log(&captured.stderr_log);
        let published = log_contains(&captured.stdout_log, b"(published)");
        self.report["phases"]
            .as_array_mut()
            .expect("phase report is an array")
            .push(json!({
                "name": name,
                "elapsed_milliseconds": captured.elapsed.as_millis(),
                "peak_rss_bytes": captured.peak_rss_bytes,
                "status": {
                    "success": success,
                    "exit_code": captured.status.code(),
                    "replica": replica_status,
                },
                "state_bytes": state_bytes,
                "logs": {
                    "stdout": stdout_log,
                    "stderr": stderr_log,
                },
                "failure_excerpt": failure_excerpt,
            }));
        if !success {
            self.report["status"] = json!("failed");
        }
        self.write_report();
        assert!(
            success,
            "scale phase {name} failed: {}",
            failure_excerpt.unwrap_or_else(|| "unknown failure".to_owned())
        );
        published
    }

    fn record_generation(&mut self, elapsed: Duration) {
        self.report["generation"] = json!({
            "elapsed_milliseconds": elapsed.as_millis(),
            "stream_buffer_bytes": STREAM_BUFFER_SIZE,
        });
        self.write_report();
    }

    fn observe(&mut self, name: &str, trees: &[(&Path, &str)]) -> Vec<TreeSummary> {
        let summaries: Vec<_> = trees
            .iter()
            .map(|(root, label)| tree_summary(root, label))
            .collect();
        self.report["tree_observations"]
            .as_array_mut()
            .expect("tree observation report is an array")
            .push(json!({
                "name": name,
                "trees": summaries.iter().map(TreeSummary::document).collect::<Vec<_>>(),
            }));
        self.write_report();
        summaries
    }

    fn observe_equal(
        &mut self,
        name: &str,
        left: &Path,
        left_label: &str,
        right: &Path,
        right_label: &str,
    ) {
        let summaries = self.observe(name, &[(left, left_label), (right, right_label)]);
        self.require(
            summaries[0].fingerprint == summaries[1].fingerprint
                && summaries[0].files == summaries[1].files
                && summaries[0].directories == summaries[1].directories
                && summaries[0].bytes == summaries[1].bytes,
            &format!("tree mismatch at {name}"),
        );
    }

    fn require(&mut self, condition: bool, message: &str) {
        if condition {
            return;
        }
        self.report["status"] = json!("failed");
        self.report["failure"] = json!(message);
        self.write_report();
        panic!("{message}");
    }

    fn complete(&mut self) {
        self.report["status"] = json!("passed");
        self.write_report();
    }

    fn record_backend(&mut self, inventory: Value, audit: Value, audit_state: Option<String>) {
        self.report["object_inventory"] = inventory;
        self.report["backend_requests"] = audit;
        self.report["audit_state_retained"] = audit_state.map_or(Value::Null, Value::String);
        self.write_report();
    }

    fn write_report(&self) {
        write_json_atomic(&self.report_path, &self.report);
    }

    fn relative_log(&self, path: &Path) -> String {
        path.strip_prefix(
            self.report_path
                .parent()
                .expect("scale report has a parent"),
        )
        .expect("scale log is below report directory")
        .to_string_lossy()
        .into_owned()
    }
}

fn scale_command(mut command: Command) -> Command {
    command
        .arg("--transfer-concurrency")
        .arg(TRANSFER_CONCURRENCY.to_string());
    command
}

fn read_status(binary: &Path, replica: &Path, state: &Path) -> Option<Value> {
    if !state.is_file() {
        return None;
    }
    let mut command = super::ofs_status_with(binary, replica, state);
    use_product_credentials(&mut command);
    let output = command.output().expect("read scale replica status");
    if !output.status.success() {
        return None;
    }
    serde_json::from_slice(&output.stdout).ok()
}

struct WorkRoot {
    path: PathBuf,
    keep: bool,
}

impl WorkRoot {
    fn new(path: PathBuf, keep: bool) -> Self {
        fs::create_dir(&path).expect("create scale work root");
        Self { path, keep }
    }
}

impl Drop for WorkRoot {
    fn drop(&mut self) {
        if self.keep {
            println!("scale work tree retained: {}", self.path.display());
        } else if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "failed to remove scale work tree {}: {error}",
                self.path.display()
            );
        }
    }
}
