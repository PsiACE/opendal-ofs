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

//! Explicit network and billing conditions for Managed Sync evaluation.

use clap::Args;
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

pub(super) const TOXIPROXY_NAME: &str = "managed-sync-minio";

pub(super) struct ChaosController {
    stop: mpsc::Sender<()>,
    worker: thread::JoinHandle<ChaosReport>,
}

#[derive(Clone, Debug)]
pub(super) struct ChaosReport {
    pub(super) schedule_seed: String,
    pub(super) faults_injected: u64,
}

#[derive(Args, Clone, Debug, Default)]
pub(crate) struct EvaluationOptions {
    #[arg(
        long,
        default_value_t = 0,
        help = "Mean object-store round-trip delay in milliseconds."
    )]
    pub(super) network_rtt_ms: u64,

    #[arg(
        long,
        default_value_t = 0,
        help = "Random variation around --network-rtt-ms in milliseconds."
    )]
    pub(super) network_jitter_ms: u64,

    #[arg(
        long,
        default_value_t = 0,
        help = "Per-connection, per-direction bandwidth limit in decimal KB/s; 0 is unlimited."
    )]
    pub(super) connection_bandwidth_kb_per_second: u64,

    #[arg(long, help = "Seed for deterministic connection-reset scheduling.")]
    pub(super) chaos_seed: Option<u64>,

    #[arg(
        long,
        default_value_t = 0,
        help = "Connection-reset probability per scheduling tick, in millionths."
    )]
    pub(super) chaos_reset_rate_per_million: u32,

    #[arg(
        long,
        default_value_t = 250,
        help = "Deterministic chaos scheduling tick in milliseconds."
    )]
    pub(super) chaos_tick_ms: u64,

    #[arg(
        long,
        default_value_t = 100,
        help = "Duration of each injected connection outage in milliseconds."
    )]
    pub(super) chaos_outage_ms: u64,

    #[arg(
        long,
        default_value_t = 0.0,
        help = "Price per million read requests in USD."
    )]
    pub(super) read_request_usd_per_million: f64,

    #[arg(
        long,
        default_value_t = 0.0,
        help = "Price per million write or list requests in USD."
    )]
    pub(super) write_list_request_usd_per_million: f64,

    #[arg(
        long,
        default_value_t = 0.0,
        help = "Object-store egress price per GiB in USD."
    )]
    pub(super) egress_usd_per_gib: f64,

    #[arg(
        long,
        default_value_t = 0.0,
        help = "Object-store ingress price per GiB in USD."
    )]
    pub(super) ingress_usd_per_gib: f64,

    #[arg(
        long,
        default_value_t = 0.0,
        help = "Storage price per GiB-month in USD."
    )]
    pub(super) storage_usd_per_gib_month: f64,
}

impl EvaluationOptions {
    pub(super) fn validate(&self) {
        assert!(
            self.network_jitter_ms <= self.network_rtt_ms,
            "network jitter must not exceed mean RTT"
        );
        assert!(
            self.connection_bandwidth_kb_per_second <= i64::MAX as u64,
            "connection bandwidth exceeds the fault proxy range"
        );
        assert!(
            self.chaos_reset_rate_per_million <= 1_000_000,
            "chaos reset rate must not exceed one million"
        );
        assert!(
            self.chaos_reset_rate_per_million == 0 || self.chaos_seed.is_some(),
            "--chaos-seed is required when connection resets are enabled"
        );
        assert!(
            self.chaos_reset_rate_per_million == 0 || self.chaos_tick_ms != 0,
            "chaos tick must be positive when connection resets are enabled"
        );
        for (name, value) in [
            ("read request price", self.read_request_usd_per_million),
            (
                "write/list request price",
                self.write_list_request_usd_per_million,
            ),
            ("egress price", self.egress_usd_per_gib),
            ("ingress price", self.ingress_usd_per_gib),
            ("storage price", self.storage_usd_per_gib_month),
        ] {
            assert!(
                value.is_finite() && value >= 0.0,
                "{name} must be finite and non-negative"
            );
        }
    }

    pub(super) const fn network_enabled(&self) -> bool {
        self.network_rtt_ms != 0
            || self.network_jitter_ms != 0
            || self.connection_bandwidth_kb_per_second != 0
            || self.chaos_reset_rate_per_million != 0
    }

    pub(super) const fn billing_enabled(&self) -> bool {
        self.read_request_usd_per_million != 0.0
            || self.write_list_request_usd_per_million != 0.0
            || self.egress_usd_per_gib != 0.0
            || self.ingress_usd_per_gib != 0.0
            || self.storage_usd_per_gib_month != 0.0
    }

    pub(super) fn start_chaos(
        &self,
        admin_port: u16,
        root: &str,
        stage: &str,
    ) -> Option<ChaosController> {
        let rate = self.chaos_reset_rate_per_million;
        if rate == 0 {
            return None;
        }
        let seed = self
            .chaos_seed
            .expect("validated chaos configuration has a seed");
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"ofs-managed-sync-chaos\0");
        hasher.update(&seed.to_le_bytes());
        hasher.update(&(root.len() as u64).to_le_bytes());
        hasher.update(root.as_bytes());
        hasher.update(&(stage.len() as u64).to_le_bytes());
        hasher.update(stage.as_bytes());
        let schedule_seed = hasher.finalize();
        let mut schedule = blake3::Hasher::new();
        schedule.update(b"ofs-managed-sync-chaos-events\0");
        schedule.update(schedule_seed.as_bytes());
        let mut schedule = schedule.finalize_xof();
        let tick = Duration::from_millis(self.chaos_tick_ms);
        let outage = Duration::from_millis(self.chaos_outage_ms);
        let reset_threshold = u64::from(rate) * (1_u64 << 32) / 1_000_000;
        let (stop, receiver) = mpsc::channel();
        let worker = thread::spawn(move || {
            let mut faults_injected = 0_u64;
            loop {
                match receiver.recv_timeout(tick) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
                let mut random = [0_u8; size_of::<u32>()];
                schedule.fill(&mut random);
                if u64::from(u32::from_le_bytes(random)) >= reset_threshold {
                    continue;
                }
                set_proxy_enabled(admin_port, false)
                    .unwrap_or_else(|error| panic!("inject Managed Sync outage: {error}"));
                faults_injected += 1;
                let stopped = match receiver.recv_timeout(outage) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => true,
                    Err(mpsc::RecvTimeoutError::Timeout) => false,
                };
                set_proxy_enabled(admin_port, true)
                    .unwrap_or_else(|error| panic!("end Managed Sync outage: {error}"));
                if stopped {
                    break;
                }
            }
            ChaosReport {
                schedule_seed: schedule_seed.to_hex().to_string(),
                faults_injected,
            }
        });
        Some(ChaosController { stop, worker })
    }
}

impl ChaosController {
    pub(super) fn stop(self) -> ChaosReport {
        let _ = self.stop.send(());
        self.worker
            .join()
            .expect("Managed Sync chaos controller did not panic")
    }
}

fn set_proxy_enabled(port: u16, enabled: bool) -> Result<(), String> {
    let address = format!("127.0.0.1:{port}");
    let mut stream = TcpStream::connect_timeout(
        &address.parse().expect("loopback proxy address is valid"),
        Duration::from_secs(1),
    )
    .map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .map_err(|error| error.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .map_err(|error| error.to_string())?;
    let body = format!("{{\"enabled\":{enabled}}}");
    let request = format!(
        "POST /proxies/{TOXIPROXY_NAME} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    let mut response = [0; 32];
    let read = stream
        .read(&mut response)
        .map_err(|error| error.to_string())?;
    if response[..read].starts_with(b"HTTP/1.1 200") {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&response[..read]).into_owned())
    }
}
