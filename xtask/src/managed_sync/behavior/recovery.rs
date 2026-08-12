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

//! Collection and interrupted-operation recovery behavior.

use std::fs;

use super::super::cli::{ManagedStatus, Ofs, output_text, require_failure, require_success};
use super::super::fixture::{CaseRoot, Fixture, tree_fingerprint};
use super::deterministic_bytes;

pub(super) fn gc(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create GC replica A");
    fs::create_dir_all(&replica_b).expect("create GC replica B");
    let storage = fixture.storage_url("gc");

    require_success(ofs.volume_create(&storage), "create GC volume");
    fs::write(
        replica_a.join("changing.bin"),
        deterministic_bytes(512 * 1024, 11),
    )
    .expect("write initial GC content");
    fs::write(
        replica_a.join("live.bin"),
        deterministic_bytes(512 * 1024, 29),
    )
    .expect("write live GC content");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish initial GC tree",
    );
    fs::write(
        replica_a.join("changing.bin"),
        deterministic_bytes(512 * 1024, 73),
    )
    .expect("replace GC content");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish replacement before GC",
    );

    require_success(ofs.gc(&storage), "collect unreachable objects");
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "cold restore after collection",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "collection preserves every object needed for cold restore"
    );
    require_success(ofs.gc(&storage), "repeat completed collection");
}

pub(super) fn recovery_gc(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create recovery replica A");
    fs::create_dir_all(&replica_b).expect("create recovery replica B");
    let storage = fixture.storage_url("recovery-gc");

    require_success(ofs.volume_create(&storage), "create recovery volume");
    fs::write(
        replica_a.join("generation.bin"),
        deterministic_bytes(512 * 1024, 19),
    )
    .expect("write prepared publication");
    let mut prepared = ofs.sync(&replica_a, &state_a, &storage);
    prepared.env("OFS_INTERNAL_TEST_INTERRUPT", "before-publish");
    require_failure(prepared, "interrupt prepared publication");
    let collected = require_success(
        ofs.gc(&storage),
        "collect an interrupted prepared publication",
    );
    assert!(
        !output_text(&collected.stdout).contains("deleted 0 object"),
        "collection reclaims an interrupted prepared publication: {}",
        output_text(&collected.stdout)
    );
    require_failure(
        ofs.sync(&replica_a, &state_a, &storage),
        "invalidate a prepared publication across collection",
    );
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "prepare and publish again after collection",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "attach recovery peer",
    );

    let mut interrupted_collection = ofs.gc(&storage);
    interrupted_collection.env("OFS_INTERNAL_TEST_INTERRUPT", "after-gc-epoch-rotation");
    require_failure(
        interrupted_collection,
        "interrupt collection after rotating the object epoch",
    );
    require_success(ofs.gc(&storage), "repeat interrupted namespace collection");
    fs::write(
        replica_a.join("after-resume.txt"),
        b"collection recovery keeps publication available\n",
    )
    .expect("write after collection recovery");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish after collection recovery",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "install publication after collection recovery",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "collection recovery preserves the namespace and later publication"
    );

    fs::write(
        replica_a.join("generation.bin"),
        deterministic_bytes(512 * 1024, 47),
    )
    .expect("write interrupted committed publication");
    let mut interrupted = ofs.sync(&replica_a, &state_a, &storage);
    interrupted.env("OFS_INTERNAL_TEST_INTERRUPT", "after-publish");
    require_failure(interrupted, "interrupt committed publication");

    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "install interrupted publication in peer",
    );
    fs::write(
        replica_b.join("generation.bin"),
        deterministic_bytes(512 * 1024, 113),
    )
    .expect("replace interrupted publication");
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "advance beyond interrupted publication",
    );
    require_success(ofs.gc(&storage), "collect superseded objects");
    fs::write(
        replica_b.join("after-collection.txt"),
        b"operation receipt must survive namespace collection\n",
    )
    .expect("write post-collection change");
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "publish after namespace collection",
    );

    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "recover committed publication from current head",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "a committed operation recovers to the current namespace after collection"
    );
    let status = ManagedStatus::parse(output_text(
        &require_success(
            ofs.status(&replica_a, &state_a),
            "read recovered publication status",
        )
        .stdout,
    ));
    assert!(
        !status.pending,
        "recovery clears the committed pending publication"
    );
}

pub(super) fn install_recovery(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create install recovery replica A");
    fs::create_dir_all(&replica_b).expect("create install recovery replica B");
    let storage = fixture.storage_url("install-recovery");

    require_success(
        ofs.volume_create(&storage),
        "create install recovery volume",
    );
    fs::write(replica_a.join("removed.txt"), b"old generation\n")
        .expect("write install recovery base");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish install recovery base",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "attach install recovery peer",
    );

    fs::remove_file(replica_a.join("removed.txt")).expect("remove install recovery base");
    fs::write(replica_a.join("current.txt"), b"current generation\n")
        .expect("write install recovery target");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish install recovery target",
    );
    let mut interrupted = ofs.sync(&replica_b, &state_b, &storage);
    interrupted.env("OFS_INTERNAL_TEST_INTERRUPT", "during-install");
    require_failure(interrupted, "interrupt remote installation");

    let recovered = require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "resume interrupted remote installation",
    );
    assert!(
        !output_text(&recovered.stdout).contains("(published)"),
        "install recovery does not publish its partial local tree"
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "an interrupted installation is safely rerunnable"
    );
    let status = ManagedStatus::parse(output_text(
        &require_success(
            ofs.status(&replica_b, &state_b),
            "read recovered installation status",
        )
        .stdout,
    ));
    assert!(
        !status.pending && status.conflicts == 0,
        "install recovery leaves no pending work or conflicts"
    );
}

pub(super) fn offline_gc(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let replica_c = root.path.join("replica-c");
    let replica_expired = root.path.join("replica-expired");
    let replica_restored = root.path.join("replica-restored");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    let state_c = root.path.join("state-c");
    let state_expired = root.path.join("state-expired");
    let state_restored = root.path.join("state-restored");
    fs::create_dir_all(&replica_a).expect("create offline GC replica A");
    fs::create_dir_all(&replica_b).expect("create offline GC replica B");
    fs::create_dir_all(&replica_c).expect("create offline GC replica C");
    fs::create_dir_all(&replica_expired).expect("create old-base replica");
    fs::create_dir_all(&replica_restored).expect("create restored replica");
    let storage = fixture.storage_url("offline-gc");

    require_success(ofs.volume_create(&storage), "create offline GC volume");
    fs::write(
        replica_a.join("cursor.txt"),
        deterministic_bytes(512 * 1024, 17),
    )
    .expect("write first common base");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish first common base",
    );
    require_success(
        ofs.sync(&replica_expired, &state_expired, &storage),
        "attach replica before collection",
    );

    fs::write(
        replica_a.join("cursor.txt"),
        deterministic_bytes(512 * 1024, 31),
    )
    .expect("write collection base");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish collection base",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "attach replica at the collection base",
    );
    let collected = require_success(ofs.gc(&storage), "collect from the current namespace");
    assert!(
        !output_text(&collected.stdout).contains("deleted 0 object"),
        "collection reclaims superseded data"
    );
    require_success(
        ofs.sync(&replica_c, &state_c, &storage),
        "cold restore after collection",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_c),
        "the current namespace remains sufficient for a cold restore"
    );

    fs::write(replica_a.join("remote.txt"), b"remote change\n")
        .expect("write remote change after collection");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish change after collection",
    );
    fs::write(replica_b.join("local.txt"), b"local change\n")
        .expect("write local change from the collection base");
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "merge from the collection base",
    );
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "install the merged namespace",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "a replica at the collection base performs an exact three-way merge"
    );

    fs::write(
        replica_expired.join("cursor.txt"),
        b"expired local change\n",
    )
    .expect("write change on an unavailable common base");
    require_failure(
        ofs.sync(&replica_expired, &state_expired, &storage),
        "report an expired reconciliation base",
    );
    let expired_status = ManagedStatus::parse(output_text(
        &require_success(
            ofs.status(&replica_expired, &state_expired),
            "read unavailable-base status",
        )
        .stdout,
    ));
    assert!(
        expired_status.base_expired && expired_status.conflicts != 0 && !expired_status.pending,
        "a replica from before collection reports an unavailable base without publishing"
    );

    require_success(
        ofs.sync(&replica_restored, &state_restored, &storage),
        "cold restore while an expired replica remains unresolved",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_restored),
        "an expired replica cannot overwrite the published namespace"
    );
    for (replica, state) in [(&replica_a, &state_a), (&replica_b, &state_b)] {
        let status = ManagedStatus::parse(output_text(
            &require_success(ofs.status(replica, state), "read converged status").stdout,
        ));
        assert!(
            !status.pending
                && status.conflicts == 0
                && status.common_sequence == status.remote_sequence,
            "retained replicas converge without pending work or conflicts"
        );
    }
}
