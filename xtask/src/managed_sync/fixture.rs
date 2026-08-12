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

//! Disposable MinIO and container fixtures.

use std::env;
use std::fs;
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::cli::{output_text, run_logged};

const DEFAULT_MINIO_PORT: u16 = 19_000;
const FIXTURE_READY_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct Fixture {
    compose_file: PathBuf,
    keep: bool,
    minio_port: u16,
    project: String,
    started: bool,
}

pub(super) struct CaseRoot {
    pub(super) path: PathBuf,
}

impl CaseRoot {
    pub(super) fn new() -> Self {
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

pub(super) fn tree_fingerprint(root: &Path) -> blake3::Hash {
    let mut paths = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).expect("read behavior tree") {
            let entry = entry.expect("read behavior entry");
            if entry.file_type().expect("read behavior file type").is_dir() {
                pending.push(entry.path());
            }
            paths.push(entry.path());
        }
    }
    paths.sort();
    let mut fingerprint = blake3::Hasher::new();
    for path in paths {
        let relative = path
            .strip_prefix(root)
            .expect("behavior path is below root");
        let relative = relative.to_str().expect("behavior path is Unicode");
        let metadata = fs::metadata(&path).expect("read behavior metadata");
        fingerprint.update(&(relative.len() as u64).to_be_bytes());
        fingerprint.update(relative.as_bytes());
        if metadata.is_dir() {
            fingerprint.update(b"d");
        } else {
            fingerprint.update(b"f");
            fingerprint.update(&[u8::from(is_executable(&metadata))]);
            let mut file = fs::File::open(path).expect("open behavior file");
            let mut buffer = [0; 1024 * 1024];
            loop {
                let read = file.read(&mut buffer).expect("read behavior file");
                if read == 0 {
                    break;
                }
                fingerprint.update(&buffer[..read]);
            }
        }
    }
    fingerprint.finalize()
}

#[cfg(unix)]
fn is_executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &fs::Metadata) -> bool {
    false
}

impl Fixture {
    pub(crate) fn new(keep: bool) -> Self {
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

    pub(super) fn start(mut self) -> Self {
        self.started = true;
        run_logged(self.compose().args(["up", "--detach", "minio"]));
        self.wait_until_ready();
        self
    }

    pub(super) fn start_bub(mut self) -> Self {
        let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
        run_logged(
            docker_command()
                .args(["build", "--network", "host", "--tag", &self.bub_image()])
                .args(["--file"])
                .arg(workspace.join("fixtures/managed-sync/bub.Dockerfile"))
                .arg(&workspace),
        );
        self.started = true;
        run_logged(self.compose().args([
            "up",
            "--detach",
            "--no-build",
            "minio",
            "bub-a",
            "bub-b",
        ]));
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

    pub(crate) fn create_bucket(&self) {
        run_logged(self.compose().args([
            "run",
            "--rm",
            "--no-deps",
            "minio-client",
            "mb",
            "--ignore-existing",
            "local/managed-sync",
        ]));
        run_logged(self.compose().args([
            "run",
            "--rm",
            "--no-deps",
            "minio-client",
            "stat",
            "local/managed-sync",
        ]));
    }

    pub(crate) fn storage_url(&self, root: &str) -> String {
        format!(
            "s3://managed-sync/{root}?endpoint=http%3A%2F%2F127.0.0.1%3A{}&region=us-east-1",
            self.minio_port
        )
    }

    pub(crate) fn storage_usage(&self, target: &str) -> (u64, u64) {
        let output = self
            .compose()
            .args([
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "minio-client",
                "du",
                "--json",
                target,
            ])
            .output()
            .expect("inspect Managed storage usage");
        assert!(
            output.status.success(),
            "inspect Managed storage usage failed: {}",
            output_text(&output.stderr)
        );
        let document: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("Managed storage usage is valid JSON");
        let field = |name| {
            document[name]
                .as_u64()
                .unwrap_or_else(|| panic!("Managed storage usage field {name} is an integer"))
        };
        (field("objects"), field("size"))
    }

    pub(super) fn container_storage_url(&self, root: &str) -> String {
        format!(
            "s3://managed-sync/{root}?endpoint=http%3A%2F%2F127.0.0.1%3A{}&region=us-east-1",
            self.minio_port
        )
    }

    pub(super) fn copy_from_container(&self, service: &str, source: &str, destination: &Path) {
        fs::create_dir(destination).expect("create container evidence directory");
        let project_label = format!("label=com.docker.compose.project={}", self.project);
        let service_label = format!("label=com.docker.compose.service={service}");
        let container = docker_command()
            .args([
                "ps",
                "--filter",
                &project_label,
                "--filter",
                &service_label,
                "--quiet",
            ])
            .output()
            .expect("resolve fixture container");
        assert!(
            container.status.success(),
            "resolve fixture container failed: {}",
            output_text(&container.stderr)
        );
        let container = output_text(&container.stdout);
        assert!(
            !container.is_empty() && !container.contains('\n'),
            "fixture service did not resolve to exactly one running container"
        );
        let remote = format!("{container}:{source}");
        run_logged(docker_command().args(["cp", &remote]).arg(destination));
    }

    pub(super) fn compose(&self) -> Command {
        let mut command = docker_compose();
        command
            .env("OFS_BUB_IMAGE", self.bub_image())
            .env("OFS_MANAGED_SYNC_MINIO_PORT", self.minio_port.to_string())
            .args(["--project-name", &self.project, "--file"])
            .arg(&self.compose_file);
        command
    }

    fn bub_image(&self) -> String {
        format!("{}-bub:local", self.project)
    }

    fn stop(&self) -> bool {
        self.compose()
            .args(["down", "--volumes", "--remove-orphans"])
            .output()
            .is_ok_and(|output| output.status.success())
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
    let mut command = docker_command();
    command.arg("compose");
    command
}

fn docker_command() -> Command {
    Command::new(
        which::which("docker")
            .or_else(|_| which::which("podman"))
            .unwrap_or_else(|error| panic!("docker or podman not found: {error}")),
    )
}

pub(super) fn container_http_proxy() -> Option<String> {
    ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"]
        .into_iter()
        .find_map(|name| env::var(name).ok().filter(|value| !value.is_empty()))
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
