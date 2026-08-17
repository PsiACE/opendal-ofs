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

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use super::cli::{output_text, run_logged};
use super::evaluation::{ChaosController, ChaosReport, EvaluationOptions, TOXIPROXY_NAME};

const DEFAULT_MINIO_PORT: u16 = 19_000;
const DEFAULT_PROXY_PORT: u16 = 19_001;
const DEFAULT_PROXY_ADMIN_PORT: u16 = 19_002;
const FIXTURE_READY_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct Fixture {
    compose_file: PathBuf,
    keep: bool,
    evaluation: EvaluationOptions,
    minio_port: u16,
    ofs_home: PathBuf,
    _ofs_home_dir: tempfile::TempDir,
    project: String,
    proxy_admin_port: u16,
    proxy_port: u16,
    started: bool,
    variant: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct LogicalIo {
    pub(super) read_bytes: u64,
    pub(super) write_bytes: u64,
}

#[derive(Clone, Debug, Default)]
struct ProviderSnapshot {
    requests: BTreeMap<String, u64>,
    read_bytes: u64,
    write_bytes: u64,
}

#[derive(Clone, Debug, Default)]
struct StorageInventory {
    classes: BTreeMap<String, StorageClass>,
}

#[derive(Clone, Copy, Debug, Default)]
struct StorageClass {
    objects: u64,
    bytes: u64,
}

pub(super) struct CaseRoot {
    _directory: tempfile::TempDir,
    pub(super) path: PathBuf,
}

impl CaseRoot {
    pub(super) fn new() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("opendal-ofs-managed-sync-")
            .tempdir()
            .expect("create Managed Sync behavior root");
        Self {
            path: directory.path().to_owned(),
            _directory: directory,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct TreeSummary {
    pub(super) digest: blake3::Hash,
    pub(super) files: u64,
    pub(super) directories: u64,
    pub(super) bytes: u64,
}

pub(super) fn tree_summary(root: &Path) -> TreeSummary {
    let mut fingerprint = blake3::Hasher::new();
    let mut files = 0_u64;
    let mut directories = 0_u64;
    let mut bytes = 0_u64;
    for entry in walkdir::WalkDir::new(root).min_depth(1).sort_by_file_name() {
        let entry = entry.expect("read behavior tree");
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .expect("behavior path is below root");
        let relative = relative.to_str().expect("behavior path is Unicode");
        let metadata = fs::metadata(path).expect("read behavior metadata");
        fingerprint.update(&(relative.len() as u64).to_be_bytes());
        fingerprint.update(relative.as_bytes());
        if metadata.is_dir() {
            directories += 1;
            fingerprint.update(b"d");
        } else {
            files += 1;
            bytes += metadata.len();
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
    TreeSummary {
        digest: fingerprint.finalize(),
        files,
        directories,
        bytes,
    }
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
    pub(crate) fn new(
        keep: bool,
        variant: impl Into<String>,
        evaluation: EvaluationOptions,
    ) -> Self {
        evaluation.validate();
        let minio_port = configured_port("OFS_MANAGED_SYNC_MINIO_PORT", DEFAULT_MINIO_PORT);
        let proxy_port = configured_port("OFS_MANAGED_SYNC_PROXY_PORT", DEFAULT_PROXY_PORT);
        let proxy_admin_port = configured_port(
            "OFS_MANAGED_SYNC_PROXY_ADMIN_PORT",
            DEFAULT_PROXY_ADMIN_PORT,
        );
        assert!(
            !evaluation.network_enabled()
                || (proxy_port != minio_port
                    && proxy_admin_port != minio_port
                    && proxy_admin_port != proxy_port),
            "Managed Sync fixture ports must differ"
        );
        let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
        let ofs_home_dir = tempfile::Builder::new()
            .prefix("opendal-ofs-home-")
            .tempdir()
            .expect("create OFS home");
        let ofs_home = ofs_home_dir.path().to_owned();
        Self {
            compose_file: workspace.join("fixtures/managed-sync/compose.yaml"),
            keep,
            evaluation,
            minio_port,
            ofs_home,
            _ofs_home_dir: ofs_home_dir,
            project: format!("opendal-ofs-managed-sync-{}", std::process::id()),
            proxy_admin_port,
            proxy_port,
            started: false,
            variant: variant.into(),
        }
    }

    pub(super) fn ofs_home(&self) -> &Path {
        &self.ofs_home
    }

    pub(super) fn start(mut self) -> Self {
        self.started = true;
        if self.evaluation.network_enabled() {
            run_logged(
                self.compose()
                    .args(["up", "--detach", "minio", "toxiproxy"]),
            );
        } else {
            run_logged(self.compose().args(["up", "--detach", "minio"]));
        }
        self.wait_until_ready(self.minio_port);
        if self.evaluation.network_enabled() {
            self.configure_network();
            self.wait_until_ready(self.proxy_port);
        }
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
        self.wait_until_ready(self.minio_port);
        self
    }

    fn wait_until_ready(&self, port: u16) {
        let deadline = Instant::now() + FIXTURE_READY_TIMEOUT;
        let response_timeout = Duration::from_millis(
            self.evaluation
                .network_rtt_ms
                .saturating_add(self.evaluation.network_jitter_ms)
                .saturating_add(1_000),
        );
        while Instant::now() < deadline {
            if minio_is_ready(port, response_timeout) {
                return;
            }
            thread::sleep(Duration::from_millis(200));
        }
        panic!(
            "MinIO did not become ready on 127.0.0.1:{} within {} seconds",
            port,
            FIXTURE_READY_TIMEOUT.as_secs()
        );
    }

    fn configure_network(&self) {
        let deadline = Instant::now() + FIXTURE_READY_TIMEOUT;
        loop {
            let output = self
                .toxiproxy_cli()
                .args([
                    "create",
                    "--listen",
                    "0.0.0.0:8666",
                    "--upstream",
                    "minio:9000",
                    TOXIPROXY_NAME,
                ])
                .output()
                .expect("configure Managed Sync fault proxy");
            if output.status.success() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "configure Managed Sync fault proxy failed: {}",
                output_text(&output.stderr)
            );
            thread::sleep(Duration::from_millis(200));
        }
        if self.evaluation.network_rtt_ms != 0 || self.evaluation.network_jitter_ms != 0 {
            let latency = format!("latency={}", self.evaluation.network_rtt_ms);
            let jitter = format!("jitter={}", self.evaluation.network_jitter_ms);
            self.add_toxic([
                "toxic",
                "add",
                "--toxicName",
                "network-rtt",
                "--type",
                "latency",
                "--attribute",
                &latency,
                "--attribute",
                &jitter,
                "--downstream",
                TOXIPROXY_NAME,
            ]);
        }
        let bandwidth = self.evaluation.connection_bandwidth_kb_per_second;
        if bandwidth != 0 {
            let rate = format!("rate={bandwidth}");
            self.add_toxic([
                "toxic",
                "add",
                "--toxicName",
                "upload-bandwidth",
                "--type",
                "bandwidth",
                "--attribute",
                &rate,
                "--upstream",
                TOXIPROXY_NAME,
            ]);
            self.add_toxic([
                "toxic",
                "add",
                "--toxicName",
                "download-bandwidth",
                "--type",
                "bandwidth",
                "--attribute",
                &rate,
                "--downstream",
                TOXIPROXY_NAME,
            ]);
        }
    }

    fn add_toxic<const N: usize>(&self, arguments: [&str; N]) {
        let output = self
            .toxiproxy_cli()
            .args(arguments)
            .output()
            .expect("add Managed Sync network condition");
        assert!(
            output.status.success(),
            "add Managed Sync network condition failed: {}",
            output_text(&output.stderr)
        );
    }

    fn toxiproxy_cli(&self) -> Command {
        let mut command = self.compose();
        command.args([
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "/toxiproxy-cli",
            "toxiproxy",
            "--host",
            "http://toxiproxy:8474",
        ]);
        command
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
    }

    pub(crate) fn storage_url(&self, root: &str) -> String {
        format!(
            "s3://managed-sync/{root}?endpoint=http%3A%2F%2F127.0.0.1%3A{}&region=us-east-1",
            self.endpoint_port()
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
        if !output.status.success() {
            let missing = serde_json::from_slice::<serde_json::Value>(&output.stdout)
                .ok()
                .and_then(|document| document["error"]["message"].as_str().map(str::to_owned))
                .is_some_and(|message| message.contains("is not a folder"));
            assert!(
                missing,
                "inspect Managed storage usage failed: {}",
                output_text(&output.stderr)
            );
            return (0, 0);
        }
        let document: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("Managed storage usage is valid JSON");
        let field = |name| {
            document[name]
                .as_u64()
                .unwrap_or_else(|| panic!("Managed storage usage field {name} is an integer"))
        };
        (field("objects"), field("size"))
    }

    pub(crate) fn observe<T>(
        &self,
        root: &str,
        stage: &str,
        logical: LogicalIo,
        action: impl FnOnce() -> T,
    ) -> T {
        let before = self.provider_snapshot();
        let started = Instant::now();
        let chaos = self
            .evaluation
            .start_chaos(self.proxy_admin_port, root, stage);
        let result = catch_unwind(AssertUnwindSafe(action));
        let chaos = chaos.map(ChaosController::stop);
        let elapsed = started.elapsed();
        let provider = self.provider_snapshot().difference(&before);
        let inventory = self.storage_inventory(root);
        report_observation(
            &self.variant,
            root,
            stage,
            elapsed,
            logical,
            &provider,
            &inventory,
            &self.evaluation,
            chaos.as_ref(),
        );
        match result {
            Ok(result) => result,
            Err(payload) => resume_unwind(payload),
        }
    }

    fn provider_snapshot(&self) -> ProviderSnapshot {
        let output = self
            .compose()
            .args([
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "minio-client",
                "admin",
                "prometheus",
                "metrics",
                "--json",
                "local",
                "api",
                "--api-version",
                "v3",
                "--bucket",
                "managed-sync",
            ])
            .output()
            .expect("read MinIO provider metrics");
        assert!(
            output.status.success(),
            "read MinIO provider metrics failed: {}",
            output_text(&output.stderr)
        );
        parse_provider_metrics(&String::from_utf8(output.stdout).expect("MinIO metrics are UTF-8"))
    }

    fn storage_inventory(&self, root: &str) -> StorageInventory {
        let target = format!("local/managed-sync/{root}/managed/0/objects");
        let output = self
            .compose()
            .args([
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "minio-client",
                "du",
                "--recursive",
                "--depth",
                "3",
                "--json",
                &target,
            ])
            .output()
            .expect("inspect Managed storage inventory");
        assert!(
            output.status.success(),
            "inspect Managed storage inventory failed: {}",
            output_text(&output.stderr)
        );
        let mut inventory = StorageInventory::default();
        for line in output.stdout.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let document: serde_json::Value =
                serde_json::from_slice(line).expect("Managed storage inventory is valid JSON");
            let prefix = document["prefix"]
                .as_str()
                .expect("Managed storage inventory prefix is a string");
            let Some(relative) =
                prefix.strip_prefix(&format!("managed-sync/{root}/managed/0/objects/"))
            else {
                continue;
            };
            let mut parts = relative.split('/');
            let Some(_epoch) = parts.next() else {
                continue;
            };
            let Some(class) = parts.next() else {
                continue;
            };
            if parts.next().is_some() {
                continue;
            }
            let entry = inventory.classes.entry(class.to_owned()).or_default();
            entry.objects = entry
                .objects
                .checked_add(
                    document["objects"]
                        .as_u64()
                        .expect("Managed storage object count is an integer"),
                )
                .expect("Managed storage object count fits u64");
            entry.bytes = entry
                .bytes
                .checked_add(
                    document["size"]
                        .as_u64()
                        .expect("Managed storage object size is an integer"),
                )
                .expect("Managed storage byte count fits u64");
        }
        inventory
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
            .env(
                "OFS_MANAGED_SYNC_PROXY_ADMIN_PORT",
                self.proxy_admin_port.to_string(),
            )
            .env("OFS_MANAGED_SYNC_PROXY_PORT", self.proxy_port.to_string())
            .args(["--project-name", &self.project, "--file"])
            .arg(&self.compose_file);
        command
    }

    fn bub_image(&self) -> String {
        format!("{}-bub:local", self.project)
    }

    const fn endpoint_port(&self) -> u16 {
        if self.evaluation.network_enabled() {
            self.proxy_port
        } else {
            self.minio_port
        }
    }

    fn stop(&self) -> bool {
        self.compose()
            .args(["down", "--volumes", "--remove-orphans"])
            .output()
            .is_ok_and(|output| output.status.success())
    }
}

fn configured_port(name: &str, default: u16) -> u16 {
    env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("invalid {name}: {value}"))
    })
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

fn minio_is_ready(port: u16, response_timeout: Duration) -> bool {
    let address = format!("127.0.0.1:{port}");
    let Ok(mut stream) = TcpStream::connect_timeout(
        &address.parse().expect("loopback fixture address is valid"),
        Duration::from_secs(1),
    ) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(response_timeout));
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

impl ProviderSnapshot {
    fn difference(&self, before: &Self) -> Self {
        let mut requests = BTreeMap::new();
        for (name, after) in &self.requests {
            let delta = after
                .checked_sub(before.requests.get(name).copied().unwrap_or_default())
                .unwrap_or_else(|| panic!("MinIO request counter {name} moved backwards"));
            if delta != 0 {
                requests.insert(name.clone(), delta);
            }
        }
        Self {
            requests,
            read_bytes: self
                .read_bytes
                .checked_sub(before.read_bytes)
                .expect("MinIO sent-byte counter is monotonic"),
            write_bytes: self
                .write_bytes
                .checked_sub(before.write_bytes)
                .expect("MinIO received-byte counter is monotonic"),
        }
    }
}

fn parse_provider_metrics(document: &str) -> ProviderSnapshot {
    const REQUESTS: &str = "minio_bucket_api_total";
    const READ_BYTES: &str = "minio_bucket_api_traffic_received_bytes";
    const WRITE_BYTES: &str = "minio_bucket_api_traffic_sent_bytes";

    let families: serde_json::Value =
        serde_json::from_str(document).expect("MinIO provider metrics are valid JSON");
    let mut snapshot = ProviderSnapshot::default();
    for family in families
        .as_array()
        .expect("MinIO provider metrics are an array")
    {
        let family_name = family["name"]
            .as_str()
            .expect("MinIO metric family has a name");
        if !matches!(family_name, REQUESTS | READ_BYTES | WRITE_BYTES) {
            continue;
        }
        for metric in family["metrics"]
            .as_array()
            .expect("MinIO metric family has samples")
        {
            if metric["labels"]["type"].as_str() != Some("s3") {
                continue;
            }
            let value = parse_counter(&metric["value"])
                .expect("MinIO counter is a finite unsigned integer");
            match family_name {
                REQUESTS => {
                    let request = metric["labels"]["name"]
                        .as_str()
                        .expect("MinIO request metric has a name");
                    *snapshot.requests.entry(request.to_owned()).or_default() += value;
                }
                // MinIO reports bucket traffic from the client's direction.
                READ_BYTES => snapshot.read_bytes += value,
                WRITE_BYTES => snapshot.write_bytes += value,
                _ => unreachable!("metric families were filtered"),
            }
        }
    }
    snapshot
}

fn parse_counter(value: &serde_json::Value) -> Option<u64> {
    if let Some(value) = value.as_u64() {
        return Some(value);
    }
    let value = match value {
        serde_json::Value::String(value) => value.parse::<f64>().ok()?,
        serde_json::Value::Number(value) => value.as_f64()?,
        _ => return None,
    };
    (value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= u64::MAX as f64)
        .then_some(value as u64)
}

fn report_observation(
    variant: &str,
    root: &str,
    stage: &str,
    elapsed: Duration,
    logical: LogicalIo,
    provider: &ProviderSnapshot,
    inventory: &StorageInventory,
    evaluation: &EvaluationOptions,
    chaos: Option<&ChaosReport>,
) {
    let mut data_objects = 0_u64;
    let mut data_bytes = 0_u64;
    let mut metadata_objects = 0_u64;
    let mut metadata_bytes = 0_u64;
    let classes = inventory
        .classes
        .iter()
        .map(|(name, class)| {
            if name == "04-data-segment" {
                data_objects += class.objects;
                data_bytes += class.bytes;
            } else {
                metadata_objects += class.objects;
                metadata_bytes += class.bytes;
            }
            (
                name.clone(),
                serde_json::json!({"objects": class.objects, "bytes": class.bytes}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let request_count = provider.requests.values().sum::<u64>();
    let amplification = |provider_bytes: u64, logical_bytes: u64| {
        (logical_bytes != 0).then(|| provider_bytes as f64 / logical_bytes as f64)
    };
    let billing = estimate_billing(evaluation, provider, inventory);
    println!(
        "managed-sync observation: {}",
        serde_json::json!({
            "variant": variant,
            "scenario": root,
            "stage": stage,
            "elapsed_seconds": elapsed.as_secs_f64(),
            "logical": {
                "read_bytes": logical.read_bytes,
                "write_bytes": logical.write_bytes,
            },
            "provider": {
                "requests": provider.requests,
                "request_count": request_count,
                "read_bytes": provider.read_bytes,
                "write_bytes": provider.write_bytes,
                "read_amplification": amplification(provider.read_bytes, logical.read_bytes),
                "write_amplification": amplification(provider.write_bytes, logical.write_bytes),
            },
            "storage": {
                "data_objects": data_objects,
                "data_bytes": data_bytes,
                "metadata_objects": metadata_objects,
                "metadata_bytes": metadata_bytes,
                "classes": classes,
            },
            "network": {
                "rtt_ms": evaluation.network_rtt_ms,
                "jitter_ms": evaluation.network_jitter_ms,
                "connection_bandwidth_kb_per_second": evaluation.connection_bandwidth_kb_per_second,
            },
            "chaos": chaos.map(|report| serde_json::json!({
                "seed": evaluation.chaos_seed,
                "schedule_seed": report.schedule_seed,
                "reset_rate_per_million": evaluation.chaos_reset_rate_per_million,
                "tick_ms": evaluation.chaos_tick_ms,
                "outage_ms": evaluation.chaos_outage_ms,
                "faults_injected": report.faults_injected,
            })),
            "billing": billing,
        })
    );
}

fn estimate_billing(
    evaluation: &EvaluationOptions,
    provider: &ProviderSnapshot,
    inventory: &StorageInventory,
) -> Option<serde_json::Value> {
    if !evaluation.billing_enabled() {
        return None;
    }
    let (read_requests, write_list_requests) =
        provider
            .requests
            .iter()
            .fold((0_u64, 0_u64), |(reads, writes), (name, count)| {
                if name.starts_with("Get") || name.starts_with("Head") {
                    (reads + count, writes)
                } else {
                    (reads, writes + count)
                }
            });
    let stored_bytes = inventory
        .classes
        .values()
        .map(|class| class.bytes)
        .sum::<u64>();
    let request_usd = read_requests as f64 / 1_000_000.0 * evaluation.read_request_usd_per_million
        + write_list_requests as f64 / 1_000_000.0 * evaluation.write_list_request_usd_per_million;
    let gib = 1024.0 * 1024.0 * 1024.0;
    let egress_usd = provider.read_bytes as f64 / gib * evaluation.egress_usd_per_gib;
    let ingress_usd = provider.write_bytes as f64 / gib * evaluation.ingress_usd_per_gib;
    let transfer_usd = egress_usd + ingress_usd;
    let storage_usd_per_month = stored_bytes as f64 / gib * evaluation.storage_usd_per_gib_month;
    Some(serde_json::json!({
        "read_requests": read_requests,
        "write_list_requests": write_list_requests,
        "request_usd": request_usd,
        "egress_usd": egress_usd,
        "ingress_usd": ingress_usd,
        "transfer_usd": transfer_usd,
        "operation_usd": request_usd + transfer_usd,
        "stored_bytes": stored_bytes,
        "storage_usd_per_month": storage_usd_per_month,
    }))
}
