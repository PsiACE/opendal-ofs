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

//! Managed Sync behavior fixture.

use std::env;
use std::fs;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Output;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

const DEFAULT_MINIO_PORT: u16 = 19_000;
const FIXTURE_READY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn run_fixture(keep: bool, case: Option<&str>) {
    build_ofs();
    let fixture = Fixture::new(keep).start();
    fixture.create_bucket();
    match case.unwrap_or("admission") {
        "admission" => admission(&fixture),
        name => panic!("unknown Managed Sync behavior case: {name}"),
    }
    println!(
        "Managed Sync behavior passed: {}",
        case.unwrap_or("admission")
    );
}

struct Fixture {
    compose_file: PathBuf,
    keep: bool,
    minio_port: u16,
    project: String,
    started: bool,
}

fn admission(fixture: &Fixture) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a.json");
    let state_b = root.path.join("state-b.json");
    fs::create_dir_all(&replica_a).expect("create replica A");
    fs::create_dir_all(&replica_b).expect("create replica B");

    let storage_a = fixture.storage_url("admission/a");
    let storage_b = fixture.storage_url("admission/b");
    let initialized_a = run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage_a, true),
        "initialize replica A",
    );
    let volume_a = output_text(&initialized_a.stdout)
        .split_whitespace()
        .last()
        .expect("initialization reports its volume identity")
        .to_owned();
    assert_eq!(volume_a.len(), 32, "volume identity is lowercase hex");

    let status = run_ofs_success(ofs_status(&replica_a, &state_a), "read replica status");
    let status = output_text(&status.stdout);
    assert!(
        status.contains(&format!("\"volume_id\":\"{volume_a}\"")),
        "status reports the initialized remote identity: {status}"
    );
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage_a, false),
        "reopen replica A",
    );

    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage_b, true),
        "initialize replica B",
    );
    let fenced = run_ofs_failure(
        ofs_sync(&replica_a, &state_a, &storage_b, false),
        "open replica A against volume B",
    );
    assert!(
        output_text(&fenced.stderr).contains("different volume"),
        "volume mismatch is reported at the command boundary"
    );

    for state in [&state_a, &state_b] {
        let bytes = fs::read(state).expect("read behavior state");
        assert!(
            !bytes
                .windows(b"minioadmin".len())
                .any(|part| part == b"minioadmin"),
            "provider credentials are not persisted in replica state"
        );
        assert!(
            !bytes
                .windows(storage_a.len())
                .any(|part| part == storage_a.as_bytes())
                && !bytes
                    .windows(storage_b.len())
                    .any(|part| part == storage_b.as_bytes()),
            "storage configuration is not persisted in replica state"
        );
    }
}

fn build_ofs() {
    let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .current_dir(env!("CARGO_WORKSPACE_DIR"))
        .args(["build", "--bin", "ofs"]);
    run(&mut command);
}

fn ofs_sync(replica: &Path, state: &Path, storage: &str, init: bool) -> Command {
    let mut command = ofs_command();
    command
        .arg("sync")
        .arg(replica)
        .arg("--state")
        .arg(state)
        .arg("--storage")
        .arg(storage);
    if init {
        command.args(["--init", "--model", "managed"]);
    }
    command
}

fn ofs_status(replica: &Path, state: &Path) -> Command {
    let mut command = ofs_command();
    command
        .arg("status")
        .arg(replica)
        .arg("--state")
        .arg(state)
        .arg("--json");
    command
}

fn ofs_command() -> Command {
    let mut command =
        Command::new(PathBuf::from(env!("CARGO_WORKSPACE_DIR")).join("target/debug/ofs"));
    command
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true");
    command
}

fn run_ofs_success(mut command: Command, action: &str) -> Output {
    let output = command.output().expect("execute ofs behavior command");
    assert!(
        output.status.success(),
        "{action} failed: {}",
        output_text(&output.stderr)
    );
    output
}

fn run_ofs_failure(mut command: Command, action: &str) -> Output {
    let output = command.output().expect("execute ofs behavior command");
    assert!(!output.status.success(), "{action} unexpectedly succeeded");
    output
}

fn output_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

struct CaseRoot {
    path: PathBuf,
}

impl CaseRoot {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "opendal-ofs-managed-sync-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create Managed Sync behavior root");
        Self { path }
    }
}

impl Drop for CaseRoot {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "failed to remove behavior root {}: {error}",
                self.path.display()
            );
        }
    }
}

impl Fixture {
    fn new(keep: bool) -> Self {
        let minio_port = env::var("OFS_MANAGED_SYNC_MINIO_PORT")
            .map(|value| {
                value
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid OFS_MANAGED_SYNC_MINIO_PORT: {value}"))
            })
            .unwrap_or(DEFAULT_MINIO_PORT);
        let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
        Self {
            compose_file: workspace.join("fixtures/managed-sync/compose.yaml"),
            keep,
            minio_port,
            project: format!("opendal-ofs-managed-sync-{}", std::process::id()),
            started: false,
        }
    }

    fn start(mut self) -> Self {
        self.started = true;
        run(self.compose().args(["up", "--detach", "minio"]));
        self.wait_until_ready();
        self
    }

    fn wait_until_ready(&self) {
        let deadline = Instant::now() + FIXTURE_READY_TIMEOUT;
        while Instant::now() < deadline {
            if minio_is_ready(self.minio_port) {
                return;
            }
            thread::sleep(Duration::from_millis(200));
        }
        panic!(
            "MinIO did not become ready on 127.0.0.1:{} within {} seconds",
            self.minio_port,
            FIXTURE_READY_TIMEOUT.as_secs()
        );
    }

    fn create_bucket(&self) {
        run(self.compose().args([
            "run",
            "--rm",
            "--no-deps",
            "minio-client",
            "mb",
            "--ignore-existing",
            "local/managed-sync",
        ]));
        run(self.compose().args([
            "run",
            "--rm",
            "--no-deps",
            "minio-client",
            "stat",
            "local/managed-sync",
        ]));
    }

    fn storage_url(&self, root: &str) -> String {
        format!(
            "s3://managed-sync/{root}?endpoint=http%3A%2F%2F127.0.0.1%3A{}&region=us-east-1",
            self.minio_port
        )
    }

    fn compose(&self) -> Command {
        let mut command = docker_compose();
        command
            .env("OFS_MANAGED_SYNC_MINIO_PORT", self.minio_port.to_string())
            .args(["--project-name", &self.project, "--file"])
            .arg(&self.compose_file);
        command
    }

    fn stop(&self) -> bool {
        self.compose()
            .args(["down", "--volumes", "--remove-orphans"])
            .status()
            .is_ok_and(|status| status.success())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if !self.started {
            return;
        }
        if self.keep {
            println!(
                "Managed Sync fixture retained: project={} port={}",
                self.project, self.minio_port
            );
        } else if !self.stop() {
            eprintln!("failed to stop Managed Sync fixture {}", self.project);
        }
    }
}

fn docker_compose() -> Command {
    let docker = which::which("docker").unwrap_or_else(|error| panic!("docker not found: {error}"));
    let mut command = Command::new(docker);
    command.arg("compose");
    command
}

fn minio_is_ready(port: u16) -> bool {
    let address = format!("127.0.0.1:{port}");
    let Ok(mut stream) = TcpStream::connect_timeout(
        &address.parse().expect("loopback fixture address is valid"),
        Duration::from_secs(1),
    ) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    if stream
        .write_all(
            b"GET /minio/health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .is_err()
    {
        return false;
    }
    let mut response = [0; 32];
    stream
        .read(&mut response)
        .is_ok_and(|read| response[..read].starts_with(b"HTTP/1.1 200"))
}

fn run(command: &mut Command) {
    println!("{command:?}");
    let status = command.status().expect("failed to execute process");
    assert!(status.success(), "command failed: {status}");
}
