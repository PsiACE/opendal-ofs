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

//! Cross-revision Managed Sync calibration.

use std::env;
use std::fs;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde_json::Value;
use serde_json::json;

use crate::managed_sync::Fixture;
use crate::managed_sync::evaluation::{
    STREAM_BUFFER_SIZE, TreeSummary, absolute_from_workspace, audit_summary, capture_process,
    log_contains, short_log_excerpt, tree_summary, use_product_credentials, write_json_atomic,
    write_xof, write_xof_file,
};

pub(crate) const DEFAULT_REFERENCE: &str = "managed-sync-layers";

const HANDOFFS: usize = 12;
const INITIAL_SMALL_FILES: usize = 128;
const FINAL_FILES: u64 = 142;
const FINAL_BYTES: u64 = 104_837_952;
const SEED_BYTES: u64 = 16 * 1024 * 1024;
const REPEATED_BYTES: u64 = 80 * 1024 * 1024;
const GENERATION_BYTES: u64 = 256 * 1024;
const EDIT_BYTES: u64 = 64 * 1024;

pub(crate) fn run(reference: &str, samples: usize, output: Option<&Path>, keep: bool) {
    assert!(samples > 0, "--samples must be greater than zero");
    let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
    let output = output
        .map(absolute_from_workspace)
        .unwrap_or_else(|| default_output(&workspace));
    fs::create_dir_all(&output).expect("create calibration output directory");
    let report_path = output.join("report.json");
    assert!(
        !report_path.exists(),
        "calibration report already exists: {}",
        report_path.display()
    );

    let reference_revision = revision(&workspace, reference);
    let candidate_revision = revision(&workspace, "HEAD");
    let mut build = Build::new(&workspace, &output, &reference_revision, keep);
    build.compile();

    let audit_directory = output.join(".audit");
    let audit_state = audit_directory.join("audit.json");
    let fixture = Fixture::new(keep).start_audited(audit_state.clone());
    fixture.create_bucket();
    fixture.create_evaluation_user();

    let mut runs = Vec::new();
    let mut expected_tree: Option<(String, u64, u64)> = None;
    for (index, role) in schedule(samples).into_iter().enumerate() {
        let label = format!("{:02}-{}", index + 1, role.name());
        let volume_root = format!("calibration/{label}");
        println!("calibration run: {label}");
        let mut run = execute_run(
            role,
            &label,
            build.binary(role),
            &fixture.storage_url(&volume_root),
            &volume_root,
            &build.work.join("runs").join(&label),
        );
        run.inventory = fixture.inventory(&volume_root);
        let observed = (run.tree.fingerprint.clone(), run.tree.files, run.tree.bytes);
        if let Some(expected) = &expected_tree {
            assert_eq!(&observed, expected, "{label} produced a different tree");
        } else {
            expected_tree = Some(observed);
        }
        runs.push(run);
    }

    let barrier = format!("calibration-{}", std::process::id());
    let barrier_storage = fixture.storage_url(&format!("audit-barrier/{barrier}"));
    fixture.finish_audit_with(
        &barrier,
        CliDialect::ManagedSyncV1.create(build.binary(Role::Candidate), &barrier_storage),
    );
    for run in &mut runs {
        run.audit = audit_summary(&audit_state, &run.volume_root);
    }
    if !keep {
        fs::remove_dir_all(&audit_directory).expect("remove MinIO audit state");
    }

    write_json_atomic(
        &report_path,
        &report(
            reference,
            &reference_revision,
            &candidate_revision,
            samples,
            keep.then(|| audit_state.to_string_lossy().into_owned()),
            &runs,
        ),
    );
    println!("Managed Sync calibration passed: {}", report_path.display());
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Role {
    Reference,
    Candidate,
}

impl Role {
    const fn name(self) -> &'static str {
        match self {
            Self::Reference => "reference",
            Self::Candidate => "candidate",
        }
    }

    const fn dialect(self) -> CliDialect {
        match self {
            Self::Reference => CliDialect::ManagedSyncLayers,
            Self::Candidate => CliDialect::ManagedSyncV1,
        }
    }
}

fn schedule(samples: usize) -> Vec<Role> {
    let pattern = [
        Role::Reference,
        Role::Candidate,
        Role::Candidate,
        Role::Reference,
        Role::Reference,
        Role::Candidate,
    ];
    let mut counts = [0, 0];
    let mut result = Vec::with_capacity(samples * 2);
    for role in pattern.into_iter().cycle() {
        let index = usize::from(role == Role::Candidate);
        if counts[index] < samples {
            result.push(role);
            counts[index] += 1;
        }
        if counts == [samples, samples] {
            return result;
        }
    }
    unreachable!()
}

struct Build {
    workspace: PathBuf,
    work: PathBuf,
    worktree: PathBuf,
    reference_revision: String,
    binaries: [PathBuf; 2],
    keep: bool,
    worktree_added: bool,
}

impl Build {
    fn new(workspace: &Path, output: &Path, reference_revision: &str, keep: bool) -> Self {
        let work = output.join(".work");
        let worktree = workspace
            .parent()
            .expect("workspace has a parent directory")
            .join(format!(
                ".opendal-ofs-calibration-reference-{}",
                std::process::id()
            ));
        assert!(!work.exists(), "calibration work directory already exists");
        assert!(
            !worktree.exists(),
            "calibration reference worktree already exists: {}",
            worktree.display()
        );
        fs::create_dir(&work).expect("create calibration work directory");
        Self {
            workspace: workspace.into(),
            worktree,
            binaries: [
                work.join("reference-target/release/ofs"),
                work.join("candidate-target/release/ofs"),
            ],
            work,
            reference_revision: reference_revision.into(),
            keep,
            worktree_added: false,
        }
    }

    fn compile(&mut self) {
        checked(
            Command::new("git")
                .current_dir(&self.workspace)
                .args(["worktree", "add", "--detach"])
                .arg(&self.worktree)
                .arg(&self.reference_revision),
            "create detached reference worktree",
        );
        self.worktree_added = true;
        build_release(&self.workspace, &self.work.join("candidate-target"));
        build_release(&self.worktree, &self.work.join("reference-target"));
    }

    fn binary(&self, role: Role) -> &Path {
        &self.binaries[usize::from(role == Role::Candidate)]
    }
}

impl Drop for Build {
    fn drop(&mut self) {
        if self.keep {
            println!("calibration work retained: {}", self.work.display());
            println!(
                "calibration reference worktree retained: {}",
                self.worktree.display()
            );
            return;
        }
        if self.worktree_added {
            let status = Command::new("git")
                .current_dir(&self.workspace)
                .args(["worktree", "remove", "--force"])
                .arg(&self.worktree)
                .status();
            if !status.is_ok_and(|status| status.success()) {
                eprintln!("failed to remove worktree {}", self.worktree.display());
            }
        }
        if let Err(error) = fs::remove_dir_all(&self.work) {
            eprintln!("failed to remove {}: {error}", self.work.display());
        }
    }
}

fn build_release(worktree: &Path, target: &Path) {
    checked(
        Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .current_dir(worktree)
            .args([
                "build",
                "--release",
                "--locked",
                "--bin",
                "ofs",
                "--target-dir",
            ])
            .arg(target),
        "build locked release binary",
    );
}

#[derive(Clone, Copy)]
enum CliDialect {
    ManagedSyncLayers,
    ManagedSyncV1,
}

impl CliDialect {
    fn create(self, binary: &Path, storage: &str) -> Command {
        assert!(matches!(self, Self::ManagedSyncV1));
        let mut command = Command::new(binary);
        command.args(["volume", "create", storage, "--model", "managed"]);
        command
    }

    fn initial(self, binary: &Path, replica: &Path, state: &Path, storage: &str) -> Vec<Command> {
        match self {
            Self::ManagedSyncLayers => {
                let mut command = self.sync(binary, replica, state, storage);
                command.args(["--init", "--model", "managed"]);
                vec![command]
            }
            Self::ManagedSyncV1 => vec![
                self.create(binary, storage),
                self.sync(binary, replica, state, storage),
            ],
        }
    }

    fn sync(self, binary: &Path, replica: &Path, state: &Path, storage: &str) -> Command {
        let mut command = Command::new(binary);
        command.arg("sync");
        match self {
            Self::ManagedSyncLayers => {
                command
                    .arg(replica)
                    .arg("--state")
                    .arg(state)
                    .args(["--storage", storage]);
            }
            Self::ManagedSyncV1 => {
                command.arg(storage).arg(replica).arg("--state").arg(state);
            }
        }
        command
    }

    fn status(self, binary: &Path, replica: &Path, state: &Path) -> Command {
        let mut command = Command::new(binary);
        command.arg("status");
        if matches!(self, Self::ManagedSyncV1) {
            command.arg(replica);
        }
        command.arg("--state").arg(state).arg("--json");
        command
    }
}

struct Sample {
    role: Role,
    label: String,
    volume_root: String,
    product_elapsed_ms: u64,
    peak_rss_bytes: u64,
    state_bytes: u64,
    tree: TreeSummary,
    phases: Vec<Value>,
    inventory: Value,
    audit: Value,
}

fn execute_run(
    role: Role,
    label: &str,
    binary: &Path,
    storage: &str,
    volume_root: &str,
    work: &Path,
) -> Sample {
    fs::create_dir_all(work).expect("create calibration run directory");
    let source = work.join("replica-source");
    let source_state = work.join("state-source");
    generate_initial(&source);
    let dialect = role.dialect();
    let mut flow = Flow {
        dialect,
        binary,
        storage,
        runner: Runner::new(work.join("logs")),
    };
    let initial = dialect.initial(binary, &source, &source_state, storage);
    for (index, command) in initial.into_iter().enumerate() {
        let publishes = index + 1
            == match dialect {
                CliDialect::ManagedSyncLayers => 1,
                CliDialect::ManagedSyncV1 => 2,
            };
        flow.runner
            .stage(&format!("initial-{}", index + 1), command, Some(publishes));
    }
    flow.settled(&source, &source_state, 1);

    let lagging = work.join("replica-lagging");
    let lagging_state = work.join("state-lagging");
    fs::create_dir(&lagging).expect("create lagging replica");
    flow.sync(&lagging, &lagging_state, "cold-restore-lagging", false);
    let mut expected = tree_summary(&source, "current");
    same_tree(
        &expected,
        &tree_summary(&lagging, "lagging"),
        "initial restore",
    );

    let mut final_replica = source;
    let mut final_state = source_state;
    for handoff in 1..=HANDOFFS {
        let replica = work.join(format!("replica-{handoff}"));
        let state = work.join(format!("state-{handoff}"));
        fs::create_dir(&replica).expect("create handoff replica");
        flow.sync(&replica, &state, &format!("cold-restore-{handoff}"), false);
        same_tree(
            &expected,
            &tree_summary(&replica, "handoff"),
            "handoff restore",
        );
        mutate(&replica, handoff);
        flow.sync(&replica, &state, &format!("publish-{handoff}"), true);
        expected = tree_summary(&replica, "current");
        final_replica = replica;
        final_state = state;
    }

    flow.sync(&lagging, &lagging_state, "lagging-catch-up", false);
    flow.settled(&lagging, &lagging_state, HANDOFFS as u64 + 1);
    same_tree(
        &expected,
        &tree_summary(&lagging, "lagging"),
        "lagging catch-up",
    );
    flow.sync(&final_replica, &final_state, "final-no-op", false);
    flow.settled(&final_replica, &final_state, HANDOFFS as u64 + 1);
    let tree = tree_summary(&final_replica, "final");
    assert_eq!(tree.files, FINAL_FILES, "final logical file count");
    assert_eq!(tree.bytes, FINAL_BYTES, "final logical bytes");

    Sample {
        role,
        label: label.into(),
        volume_root: volume_root.into(),
        product_elapsed_ms: flow.runner.elapsed_ms,
        peak_rss_bytes: flow.runner.peak_rss_bytes,
        state_bytes: fs::metadata(final_state)
            .expect("read final state metadata")
            .len(),
        tree,
        phases: flow.runner.phases,
        inventory: Value::Null,
        audit: Value::Null,
    }
}

struct Flow<'a> {
    dialect: CliDialect,
    binary: &'a Path,
    storage: &'a str,
    runner: Runner,
}

impl Flow<'_> {
    fn sync(&mut self, replica: &Path, state: &Path, name: &str, published: bool) {
        self.runner.stage(
            name,
            self.dialect.sync(self.binary, replica, state, self.storage),
            Some(published),
        );
    }

    fn settled(&self, replica: &Path, state: &Path, sequence: u64) {
        let mut command = self.dialect.status(self.binary, replica, state);
        use_product_credentials(&mut command);
        let output = command.output().expect("read calibration replica status");
        assert!(
            output.status.success(),
            "status failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        let status: Value = serde_json::from_slice(&output.stdout).expect("status is JSON");
        assert_eq!(number(&status, "common_sequence"), sequence);
        assert!(!boolean(&status, "pending"), "status retained pending work");
        assert_eq!(number(&status, "conflicts"), 0, "status retained conflicts");
    }
}

struct Runner {
    logs: PathBuf,
    phases: Vec<Value>,
    elapsed_ms: u64,
    peak_rss_bytes: u64,
}

impl Runner {
    fn new(logs: PathBuf) -> Self {
        fs::create_dir(&logs).expect("create calibration log directory");
        Self {
            logs,
            phases: Vec::new(),
            elapsed_ms: 0,
            peak_rss_bytes: 0,
        }
    }

    fn stage(&mut self, name: &str, mut command: Command, published: Option<bool>) {
        use_product_credentials(&mut command);
        let measured = capture_process(command, &self.logs, name);
        assert!(
            measured.status.success(),
            "calibration phase {name} failed: {}",
            short_log_excerpt(&measured.stderr_log)
        );
        let observed = log_contains(&measured.stdout_log, b"(published)");
        if let Some(expected) = published {
            assert_eq!(observed, expected, "unexpected publication in {name}");
        }
        let elapsed_ms = milliseconds(measured.elapsed);
        self.elapsed_ms += elapsed_ms;
        self.peak_rss_bytes = self.peak_rss_bytes.max(measured.peak_rss_bytes);
        self.phases.push(json!({
            "name": name,
            "elapsed_ms": elapsed_ms,
            "peak_rss_bytes": measured.peak_rss_bytes,
            "published": observed,
        }));
    }
}

fn generate_initial(root: &Path) {
    fs::create_dir_all(root.join("memory")).expect("create memory directory");
    let mut buffer = vec![0; STREAM_BUFFER_SIZE];
    for index in 0..INITIAL_SMALL_FILES {
        let relative = format!("skills/group-{}/file-{}.dat", index / 16, index % 16);
        let path = root.join(&relative);
        fs::create_dir_all(path.parent().expect("small file has a parent"))
            .expect("create small file directory");
        write_xof_file(
            &path,
            relative.as_bytes(),
            1,
            (1024 + index * 113) as u64,
            &mut buffer,
        );
    }
    write_xof_file(
        &root.join("memory/seed.bin"),
        b"seed",
        1,
        SEED_BYTES,
        &mut buffer,
    );
    fs::File::create(root.join("memory/repeated.bin"))
        .and_then(|file| file.set_len(REPEATED_BYTES))
        .expect("create sparse repeated file");
}

fn mutate(root: &Path, handoff: usize) {
    let mut buffer = vec![0; STREAM_BUFFER_SIZE];
    let mut seed = fs::OpenOptions::new()
        .write(true)
        .open(root.join("memory/seed.bin"))
        .expect("open seed file");
    seed.seek(SeekFrom::Start(
        (handoff as u64 - 1) * 1024 * 1024 + 128 * 1024,
    ))
    .expect("seek seed edit");
    write_xof(
        &mut seed,
        b"seed-edit",
        handoff as u8,
        EDIT_BYTES,
        &mut buffer,
    );

    let index = handoff * 17 % INITIAL_SMALL_FILES;
    let relative = format!("skills/group-{}/file-{}.dat", index / 16, index % 16);
    write_xof_file(
        &root.join(relative),
        b"small-replacement",
        handoff as u8,
        (4096 + handoff * 257) as u64,
        &mut buffer,
    );
    write_xof_file(
        &root.join(format!("memory/generation-{handoff}.bin")),
        b"generation",
        handoff as u8,
        GENERATION_BYTES,
        &mut buffer,
    );
}

fn same_tree(left: &TreeSummary, right: &TreeSummary, action: &str) {
    assert!(
        left.fingerprint == right.fingerprint
            && left.files == right.files
            && left.directories == right.directories
            && left.bytes == right.bytes,
        "tree mismatch after {action}"
    );
}

fn report(
    reference: &str,
    reference_revision: &str,
    candidate_revision: &str,
    samples: usize,
    audit_state: Option<String>,
    runs: &[Sample],
) -> Value {
    json!({
        "schema": "ofs.managed-sync.calibration/1",
        "status": "passed",
        "revisions": {
            "reference_ref": reference,
            "reference": reference_revision,
            "candidate": candidate_revision,
        },
        "workload": {
            "samples_per_revision": samples,
            "handoffs": HANDOFFS,
            "logical_files": FINAL_FILES,
            "logical_bytes": FINAL_BYTES,
        },
        "schedule": runs.iter().map(|run| run.role.name()).collect::<Vec<_>>(),
        "aggregate": {
            "reference": aggregate(runs, Role::Reference),
            "candidate": aggregate(runs, Role::Candidate),
        },
        "runs": runs.iter().map(sample_document).collect::<Vec<_>>(),
        "audit_state_retained": audit_state,
        "checks": {
            "fixed_workload": true,
            "settled_status_at_acceptance_boundaries": true,
            "no_op_did_not_publish": true,
            "logical_trees_equal": true,
        },
    })
}

fn sample_document(run: &Sample) -> Value {
    json!({
        "role": run.role.name(),
        "label": run.label,
        "volume_root": run.volume_root,
        "product_elapsed_ms": run.product_elapsed_ms,
        "peak_rss_bytes": run.peak_rss_bytes,
        "state_bytes": run.state_bytes,
        "tree": run.tree.document(),
        "phases": run.phases,
        "object_inventory": run.inventory,
        "backend_requests": run.audit,
    })
}

fn aggregate(runs: &[Sample], role: Role) -> Value {
    let selected: Vec<_> = runs.iter().filter(|run| run.role == role).collect();
    let field = |extract: fn(&Sample) -> u64| median(selected.iter().map(|run| extract(run)));
    json!({
        "samples": selected.len(),
        "product_elapsed_ms_median": field(|run| run.product_elapsed_ms),
        "peak_rss_bytes_median": field(|run| run.peak_rss_bytes),
        "state_bytes_median": field(|run| run.state_bytes),
        "remote_objects_median": median(selected.iter().map(|run| number(&run.inventory, "objects"))),
        "remote_bytes_median": median(selected.iter().map(|run| number(&run.inventory, "bytes"))),
        "requests_median": median(selected.iter().map(|run| number(&run.audit, "requests"))),
        "request_bytes_median": median(selected.iter().map(|run| number(&run.audit, "request_bytes"))),
        "response_bytes_median": median(selected.iter().map(|run| number(&run.audit, "response_bytes"))),
    })
}

fn median(values: impl IntoIterator<Item = u64>) -> u64 {
    let mut values: Vec<_> = values.into_iter().collect();
    assert!(!values.is_empty(), "median requires values");
    values.sort_unstable();
    if values.len() % 2 == 1 {
        values[values.len() / 2]
    } else {
        let lower = values[values.len() / 2 - 1];
        let upper = values[values.len() / 2];
        lower + (upper - lower) / 2
    }
}

fn number(value: &Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("{field} is an unsigned integer"))
}

fn boolean(value: &Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(Value::as_bool)
        .unwrap_or_else(|| panic!("{field} is a Boolean"))
}

fn revision(workspace: &Path, reference: &str) -> String {
    let output = Command::new("git")
        .current_dir(workspace)
        .args(["rev-parse", "--verify", "--end-of-options"])
        .arg(format!("{reference}^{{commit}}"))
        .output()
        .expect("resolve Git revision");
    assert!(
        output.status.success(),
        "cannot resolve local revision {reference}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8_lossy(&output.stdout).trim().into()
}

fn checked(command: &mut Command, action: &str) {
    println!("{command:?}");
    let status = command.status().expect("execute calibration command");
    assert!(status.success(), "{action} failed: {status}");
}

fn default_output(workspace: &Path) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_secs();
    workspace
        .join(".local/move/calibration")
        .join(format!("managed-sync-{nonce}-{}", std::process::id()))
}

fn milliseconds(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
