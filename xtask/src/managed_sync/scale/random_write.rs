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

//! Continuous random writes to one file with periodic Managed Sync runs.

use std::fs;
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use super::{ScaleRoot, require_same_tree, write_xof};
use crate::managed_sync::cli::{Ofs, output_text, require_success};
use crate::managed_sync::evaluation::EvaluationOptions;
use crate::managed_sync::fixture::{Fixture, LogicalIo};

const WRITE_BYTES: usize = 4 * 1024;
const STREAM_BUFFER_BYTES: usize = 1024 * 1024;
const SCENARIO: &str = "scale/random-write";

pub(super) fn run(
    keep: bool,
    duration_seconds: u64,
    sync_interval_seconds: u64,
    file_size: u64,
    evaluation: EvaluationOptions,
) {
    let workload = Workload::new(duration_seconds, sync_interval_seconds, file_size);
    let fixture = Fixture::new(keep, "random-write", evaluation);
    let ofs = Ofs::release(fixture.ofs_home());
    ofs.build();

    let fixture = fixture.start();
    fixture.create_bucket();
    let work = ScaleRoot::new(keep);
    let source = work.path.join("replica-a");
    let restored = work.path.join("replica-b");
    let source_state = work.path.join("state-a");
    let restored_state = work.path.join("state-b");
    fs::create_dir(&source).expect("create random-write source replica");
    fs::create_dir(&restored).expect("create random-write restore replica");
    let file = source.join("random-write.bin");
    let storage = fixture.storage_url(SCENARIO);

    let mut buffer = vec![0; STREAM_BUFFER_BYTES];
    write_xof(&file, 0, 0, workload.file_bytes, &mut buffer);
    fixture.observe(SCENARIO, "create volume", LogicalIo::default(), || {
        require_success(ofs.volume_create(&storage), "create random-write volume");
    });
    fixture.observe(
        SCENARIO,
        "initial publish",
        LogicalIo {
            read_bytes: 0,
            write_bytes: workload.file_bytes,
        },
        || {
            require_success(
                ofs.sync(&source, &source_state, &storage),
                "publish random-write fixture",
            );
        },
    );

    let writer = RandomWriter::start(file);
    let workload_started = Instant::now();
    let workload_deadline = workload_started
        .checked_add(workload.duration)
        .expect("random-write deadline fits Instant");
    let mut scheduled = workload_started
        .checked_add(workload.interval)
        .expect("random-write sync deadline fits Instant");
    let mut round = 0_u64;
    let mut previous = WriterSnapshot::default();
    let mut successful_syncs = 0_u64;
    let mut failed_syncs = 0_u64;
    let mut missed_intervals = 0_u64;
    let mut maximum_sync = Duration::ZERO;

    while scheduled <= workload_deadline {
        round += 1;
        wait_until(scheduled);
        let started = Instant::now();
        let schedule_lag = started.saturating_duration_since(scheduled);
        let before = writer.snapshot();
        let stage = format!("periodic sync {round}");
        let mut sync_elapsed = Duration::ZERO;
        let output = fixture.observe(SCENARIO, &stage, LogicalIo::default(), || {
            let sync_started = Instant::now();
            let output = run_sync(&ofs, &source, &source_state, &storage);
            sync_elapsed = sync_started.elapsed();
            output
        });
        let observed_elapsed = started.elapsed();
        let after = writer.snapshot();
        let success = output.status.success();
        successful_syncs += u64::from(success);
        failed_syncs += u64::from(!success);
        missed_intervals += u64::from(sync_elapsed > workload.interval);
        maximum_sync = maximum_sync.max(sync_elapsed);
        println!(
            "random-write sync: {}",
            serde_json::json!({
                "round": round,
                "success": success,
                "scheduled_seconds": scheduled.saturating_duration_since(workload_started).as_secs(),
                "schedule_lag_seconds": schedule_lag.as_secs_f64(),
                "sync_elapsed_seconds": sync_elapsed.as_secs_f64(),
                "observation_elapsed_seconds": observed_elapsed.as_secs_f64(),
                "within_interval": sync_elapsed <= workload.interval,
                "writes_before_sync": before.difference(previous),
                "writes_during_sync": after.difference(before),
                "error": (!success).then(|| output_text(&output.stderr)),
            })
        );
        previous = after;
        let Some(next) = scheduled.checked_add(workload.interval) else {
            break;
        };
        scheduled = next;
    }

    wait_until(workload_deadline);
    let writer = writer.stop();
    let tail = writer.difference(previous);
    let final_output = fixture.observe(SCENARIO, "final sync", LogicalIo::default(), || {
        run_sync(&ofs, &source, &source_state, &storage)
    });
    assert!(
        final_output.status.success(),
        "final random-write sync failed: {}",
        output_text(&final_output.stderr)
    );
    fixture.observe(
        SCENARIO,
        "cold restore",
        LogicalIo {
            read_bytes: workload.file_bytes,
            write_bytes: 0,
        },
        || {
            require_success(
                ofs.sync(&restored, &restored_state, &storage),
                "restore random-write result",
            );
        },
    );
    require_same_tree(&source, &restored, "random-write cold restore");

    println!(
        "Managed Sync random-write scale result: {}",
        serde_json::json!({
            "duration_seconds": duration_seconds,
            "sync_interval_seconds": sync_interval_seconds,
            "file_bytes": workload.file_bytes,
            "write_bytes": WRITE_BYTES,
            "writes": {
                "operations": writer.operations,
                "bytes": writer.bytes,
            },
            "tail_writes": tail,
            "successful_syncs": successful_syncs,
            "failed_syncs": failed_syncs,
            "missed_intervals": missed_intervals,
            "maximum_sync_seconds": maximum_sync.as_secs_f64(),
        })
    );
    assert_eq!(
        failed_syncs, 0,
        "periodic sync failed while the file was being randomly written"
    );
    assert_eq!(
        missed_intervals, 0,
        "periodic sync did not finish within its scheduling interval"
    );
}

fn run_sync(ofs: &Ofs, replica: &Path, state: &Path, storage: &str) -> Output {
    ofs.sync(replica, state, storage)
        .output()
        .expect("run periodic random-write sync")
}

fn wait_until(deadline: Instant) {
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        thread::sleep(remaining.min(Duration::from_secs(1)));
    }
}

struct Workload {
    file_bytes: u64,
    duration: Duration,
    interval: Duration,
}

impl Workload {
    fn new(duration_seconds: u64, sync_interval_seconds: u64, file_bytes: u64) -> Self {
        assert!(
            duration_seconds != 0,
            "random-write duration must be positive"
        );
        assert!(
            sync_interval_seconds != 0,
            "random-write sync interval must be positive"
        );
        assert!(
            file_bytes >= WRITE_BYTES as u64,
            "random-write file must contain at least one write block"
        );
        assert_eq!(
            file_bytes % WRITE_BYTES as u64,
            0,
            "random-write file size must align to the write block"
        );
        Self {
            file_bytes,
            duration: Duration::from_secs(duration_seconds),
            interval: Duration::from_secs(sync_interval_seconds),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct WriterSnapshot {
    operations: u64,
    bytes: u64,
}

impl WriterSnapshot {
    fn difference(self, before: Self) -> serde_json::Value {
        serde_json::json!({
            "operations": self.operations - before.operations,
            "bytes": self.bytes - before.bytes,
        })
    }
}

struct RandomWriter {
    stop: Arc<AtomicBool>,
    operations: Arc<AtomicU64>,
    handle: thread::JoinHandle<()>,
}

impl RandomWriter {
    fn start(path: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let operations = Arc::new(AtomicU64::new(0));
        let thread_stop = Arc::clone(&stop);
        let thread_operations = Arc::clone(&operations);
        let handle = thread::spawn(move || {
            write_randomly(&path, &thread_stop, &thread_operations);
        });
        Self {
            stop,
            operations,
            handle,
        }
    }

    fn snapshot(&self) -> WriterSnapshot {
        let operations = self.operations.load(Ordering::Relaxed);
        WriterSnapshot {
            operations,
            bytes: operations * WRITE_BYTES as u64,
        }
    }

    fn stop(self) -> WriterSnapshot {
        let Self {
            stop,
            operations,
            handle,
        } = self;
        stop.store(true, Ordering::Relaxed);
        handle.join().expect("random-write worker did not panic");
        let operations = operations.load(Ordering::Relaxed);
        WriterSnapshot {
            operations,
            bytes: operations * WRITE_BYTES as u64,
        }
    }
}

fn write_randomly(path: &Path, stop: &AtomicBool, operations: &AtomicU64) {
    let file_bytes = fs::metadata(path)
        .expect("read random-write file metadata")
        .len();
    let block_count = file_bytes / WRITE_BYTES as u64;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open random-write file");
    let mut source = blake3::Hasher::new();
    source.update(b"ofs-managed-sync-random-write\0");
    let mut source = source.finalize_xof();
    let mut offset_bytes = [0_u8; 8];
    let mut payload = [0_u8; WRITE_BYTES];
    while !stop.load(Ordering::Relaxed) {
        source.fill(&mut offset_bytes);
        source.fill(&mut payload);
        let block = u64::from_le_bytes(offset_bytes) % block_count;
        file.seek(SeekFrom::Start(block * WRITE_BYTES as u64))
            .expect("seek random-write file");
        file.write_all(&payload).expect("update random-write file");
        operations.fetch_add(1, Ordering::Relaxed);
    }
    file.sync_all().expect("persist random-write file");
}
