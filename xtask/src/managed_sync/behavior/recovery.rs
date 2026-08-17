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

//! Collection and process-interruption behavior.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use super::super::cli::{ManagedStatus, Ofs, output_text, require_failure, require_success};
use super::super::fixture::{CaseRoot, Fixture, tree_summary};

pub(super) fn gc(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let a = root.path.join("a");
    let b = root.path.join("b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&a).expect("create GC source");
    fs::create_dir_all(&b).expect("create GC destination");
    let storage = fixture.storage_url("gc");

    require_success(ofs.volume_create(&storage), "create GC volume");
    fs::write(a.join("changing.bin"), b"superseded\n").expect("write initial GC content");
    fs::write(a.join("live.bin"), b"live\n").expect("write live GC content");
    require_success(ofs.sync(&a, &state_a, &storage), "publish initial GC tree");
    fs::write(a.join("changing.bin"), b"current\n").expect("replace GC content");
    require_success(ofs.sync(&a, &state_a, &storage), "publish replacement");
    require_success(ofs.gc(&storage), "collect unreachable objects");
    require_success(ofs.sync(&b, &state_b, &storage), "restore after collection");
    assert_eq!(tree_summary(&a), tree_summary(&b));
    require_success(ofs.gc(&storage), "repeat completed collection");
}

pub(super) fn interruption(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let a = root.path.join("a");
    let b = root.path.join("b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&a).expect("create interrupted source");
    fs::create_dir_all(&b).expect("create interrupted destination");
    fs::write(a.join("file"), b"interrupted publication\n").expect("write interrupted content");
    let storage = fixture.storage_url("interruption");
    require_success(ofs.volume_create(&storage), "create interruption volume");

    interrupt(ofs.sync(&a, &state_a, &storage), &state_a);
    require_success(
        ofs.sync(&a, &state_a, &storage),
        "retry interrupted publish",
    );
    interrupt(ofs.sync(&b, &state_b, &storage), &state_b);
    require_success(
        ofs.sync(&b, &state_b, &storage),
        "retry interrupted restore",
    );
    assert_eq!(tree_summary(&a), tree_summary(&b));
}

fn interrupt(mut command: Command, progress: &Path) {
    let mut child = command.spawn().expect("start interruptible OFS process");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !progress.exists() {
        assert!(
            child.try_wait().expect("inspect OFS process").is_none(),
            "OFS completed before the interruption point"
        );
        assert!(Instant::now() < deadline, "OFS did not expose progress");
        thread::sleep(Duration::from_millis(1));
    }
    child.kill().expect("interrupt OFS process");
    assert!(!child.wait().expect("wait for interrupted OFS").success());
}

pub(super) fn offline_gc(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let a = root.path.join("a");
    let b = root.path.join("b");
    let expired = root.path.join("expired");
    let restored = root.path.join("restored");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    let state_expired = root.path.join("state-expired");
    let state_restored = root.path.join("state-restored");
    for replica in [&a, &b, &expired, &restored] {
        fs::create_dir_all(replica).expect("create GC replica");
    }
    let storage = fixture.storage_url("offline-gc");
    require_success(ofs.volume_create(&storage), "create offline GC volume");

    fs::write(a.join("cursor.txt"), b"first base\n").expect("write first base");
    require_success(ofs.sync(&a, &state_a, &storage), "publish first base");
    require_success(
        ofs.sync(&expired, &state_expired, &storage),
        "attach replica before collection",
    );
    fs::write(a.join("cursor.txt"), b"collection base\n").expect("write collection base");
    require_success(ofs.sync(&a, &state_a, &storage), "publish collection base");
    require_success(ofs.sync(&b, &state_b, &storage), "attach current replica");
    require_success(ofs.gc(&storage), "collect from current namespace");

    fs::write(a.join("remote.txt"), b"remote\n").expect("write remote change");
    require_success(ofs.sync(&a, &state_a, &storage), "publish remote change");
    fs::write(b.join("local.txt"), b"local\n").expect("write local change");
    require_success(ofs.sync(&b, &state_b, &storage), "merge current replica");
    require_success(ofs.sync(&a, &state_a, &storage), "converge current replica");
    assert_eq!(tree_summary(&a), tree_summary(&b));

    fs::write(expired.join("cursor.txt"), b"expired local change\n").expect("write expired change");
    require_failure(
        ofs.sync(&expired, &state_expired, &storage),
        "reject an unavailable base",
    );
    let status = ManagedStatus::parse(output_text(
        &require_success(
            ofs.status(&expired, &state_expired),
            "read unavailable-base status",
        )
        .stdout,
    ));
    assert!(status.base_expired && status.conflicts != 0 && !status.pending);

    require_success(
        ofs.sync(&restored, &state_restored, &storage),
        "restore while an expired replica remains unresolved",
    );
    assert_eq!(tree_summary(&a), tree_summary(&restored));
}
