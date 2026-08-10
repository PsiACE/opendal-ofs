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
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::Duration;
use std::time::Instant;

const DEFAULT_MINIO_PORT: u16 = 19_000;
const FIXTURE_READY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn run_fixture(keep: bool) {
    let fixture = Fixture::new(keep).start();
    fixture.create_bucket();
    println!(
        "Managed Sync fixture passed: MinIO is ready on 127.0.0.1:{}",
        fixture.minio_port
    );
}

struct Fixture {
    compose_file: PathBuf,
    keep: bool,
    minio_port: u16,
    project: String,
    started: bool,
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
