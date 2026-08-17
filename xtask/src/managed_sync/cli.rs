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

//! OFS command construction and process assertions.

use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

#[derive(Clone)]
pub(super) struct Ofs {
    release: bool,
    transfer_concurrency: Option<usize>,
    home: PathBuf,
}

impl Ofs {
    pub(super) fn debug(home: impl Into<PathBuf>) -> Self {
        Self {
            release: false,
            transfer_concurrency: None,
            home: home.into(),
        }
    }

    pub(super) fn release(home: impl Into<PathBuf>) -> Self {
        Self {
            release: true,
            transfer_concurrency: Some(16),
            home: home.into(),
        }
    }

    pub(super) fn build(&self) {
        let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
        command
            .current_dir(env!("CARGO_WORKSPACE_DIR"))
            .arg("build");
        if self.release {
            command.arg("--release");
        }
        command.args(["--bin", "ofs"]);
        run_logged(&mut command);
    }

    pub(super) fn volume_create(&self, storage: &str) -> Command {
        let mut command = self.command();
        command.args([
            "volume",
            "create",
            &volume_name(storage),
            "--model",
            "managed",
            "--storage",
            storage,
        ]);
        command
    }

    pub(super) fn volume_create_fastcdc_zstd(&self, storage: &str) -> Command {
        let mut command = self.volume_create(storage);
        command.args(["--fastcdc", "--zstd-level", "3"]);
        command
    }

    pub(super) fn volume_create_branch(&self, storage: &str) -> Command {
        let mut command = self.volume_create(storage);
        command.arg("--authority-branch");
        command
    }

    pub(super) fn volume_create_zstd_only(&self, storage: &str) -> Command {
        let mut command = self.volume_create(storage);
        command.args(["--zstd-level", "3"]);
        command
    }

    pub(super) fn branch_create(&self, storage: &str, name: &str, source: &str) -> Command {
        let mut command = self.command();
        command.args([
            "branch",
            &volume_name(storage),
            "create",
            name,
            "--from",
            source,
        ]);
        command
    }

    pub(super) fn branch_delete(&self, storage: &str, name: &str) -> Command {
        let mut command = self.command();
        command.args(["branch", &volume_name(storage), "delete", name]);
        command
    }

    pub(super) fn branch_list(&self, storage: &str) -> Command {
        let mut command = self.command();
        command.args(["branch", &volume_name(storage), "list"]);
        command
    }

    pub(super) fn sync_branch(
        &self,
        replica: &Path,
        state: &Path,
        storage: &str,
        branch: &str,
    ) -> Command {
        let mut command = self.sync(replica, state, storage);
        command.arg("--branch").arg(branch);
        command
    }

    pub(super) fn sync(&self, replica: &Path, state: &Path, storage: &str) -> Command {
        let mut command = self.command();
        command
            .arg("sync")
            .arg(volume_name(storage))
            .arg(replica)
            .arg("--state")
            .arg(state);
        command
    }

    pub(super) fn sync_changes(
        &self,
        replica: &Path,
        state: &Path,
        storage: &str,
        changes: &Path,
    ) -> Command {
        let mut command = self.sync(replica, state, storage);
        command.arg("--change-set").arg(changes);
        command
    }

    pub(super) fn status(&self, replica: &Path, state: &Path) -> Command {
        let mut command = self.command();
        command
            .arg("status")
            .arg(replica)
            .arg("--state")
            .arg(state)
            .arg("--json");
        command
    }

    pub(super) fn gc(&self, storage: &str) -> Command {
        let mut command = self.command();
        command.arg("gc").arg(volume_name(storage));
        command
    }

    pub(super) fn sync_resolve(
        &self,
        replica: &Path,
        state: &Path,
        storage: &str,
        paths: &[&str],
    ) -> Command {
        let mut command = self.sync(replica, state, storage);
        for path in paths {
            command.arg("--resolve").arg(path);
        }
        command
    }

    fn command(&self) -> Command {
        let profile = if self.release { "release" } else { "debug" };
        let mut command = Command::new(
            PathBuf::from(env!("CARGO_WORKSPACE_DIR"))
                .join("target")
                .join(profile)
                .join("ofs"),
        );
        command
            .env("OFS_HOME", &self.home)
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true");
        if let Some(transfer_concurrency) = self.transfer_concurrency {
            command.env("OFS_TRANSFER_CONCURRENCY", transfer_concurrency.to_string());
        }
        command
    }
}

pub(super) fn volume_name(storage: &str) -> String {
    let root = storage
        .strip_prefix("s3://managed-sync/")
        .and_then(|value| value.split('?').next())
        .unwrap_or(storage);
    root.replace('/', "-")
}

pub(super) fn require_success(mut command: Command, action: &str) -> Output {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("{action}: {error}"));
    assert!(
        output.status.success(),
        "{action} failed: {}",
        output_text(&output.stderr)
    );
    output
}

pub(super) fn require_failure(mut command: Command, action: &str) -> Output {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("{action}: {error}"));
    assert!(!output.status.success(), "{action} unexpectedly succeeded");
    output
}

pub(super) fn output_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

pub(super) fn run_logged(command: &mut Command) {
    println!(
        "{} {}",
        command.get_program().to_string_lossy(),
        command
            .get_args()
            .map(|argument| argument.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
    );
    let status = command.status().expect("failed to execute process");
    assert!(status.success(), "command failed: {status}");
}

#[derive(serde::Deserialize)]
pub(super) struct ManagedStatus {
    #[serde(skip)]
    pub(super) document: String,
    pub(super) volume_id: String,
    pub(super) common_sequence: u64,
    pub(super) remote_sequence: u64,
    pub(super) pending: bool,
    pub(super) conflicts: u64,
    pub(super) base_expired: bool,
    pub(super) capabilities: ManagedCapabilities,
}

#[derive(serde::Deserialize)]
pub(super) struct ManagedCapabilities {
    pub(super) extended_attributes: bool,
    pub(super) portable_names: bool,
}

impl ManagedStatus {
    pub(super) fn parse(document: String) -> Self {
        let mut status: Self = serde_json::from_str(&document).expect("status is valid JSON");
        status.document = document;
        status
    }
}
