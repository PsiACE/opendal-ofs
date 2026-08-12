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

//! Volume admission, ordinary lifecycle, and growing-file behavior.

use std::fs;
use std::io::Write as _;

use super::super::cli::{ManagedStatus, Ofs, output_text, require_failure, require_success};
use super::super::fixture::{CaseRoot, Fixture, tree_fingerprint};
use super::{deterministic_bytes, make_executable};

pub(super) fn growing(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create growing replica A");
    fs::create_dir_all(&replica_b).expect("create growing replica B");
    let storage = fixture.storage_url("growing");

    require_success(ofs.volume_create(&storage), "create growing-file volume");
    let initial = deterministic_bytes(2 * 1024 * 1024, 17);
    fs::write(replica_a.join("session.tape"), initial).expect("write growing session");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish growing session",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "restore growing session",
    );

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(replica_a.join("session.tape"))
        .expect("open growing session");
    file.write_all(&deterministic_bytes(128 * 1024, 91))
        .expect("append growing session");
    file.sync_all().expect("persist growing session");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish appended session",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "install appended session",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "an appended session converges without changing its bytes"
    );
    let no_op = require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "repeat appended session sync",
    );
    assert!(
        !output_text(&no_op.stdout).contains("(published)"),
        "an unchanged appended session is a no-op"
    );
}

pub(super) fn extensions(fixture: &Fixture, ofs: Ofs) {
    for (name, zstd) in [("fastcdc", false), ("fastcdc-zstd", true)] {
        file_extension(fixture, ofs, name, zstd);
    }
}

pub(super) fn file_extension(fixture: &Fixture, ofs: Ofs, name: &str, zstd: bool) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create extension replica A");
    fs::create_dir_all(&replica_b).expect("create extension replica B");
    let storage = fixture.storage_url(&format!("extensions/{name}"));

    require_success(
        ofs.volume_create_extensions(&storage, zstd),
        "create extension volume",
    );
    let mut contents = deterministic_bytes(3 * 1024 * 1024, 41);
    fs::write(replica_a.join("stream.bin"), &contents).expect("write extension source file");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish extension file",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "cold-install extension file",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "an extension file converges across replicas"
    );

    contents[1024 * 1024..1024 * 1024 + 4096].fill(217);
    fs::write(replica_b.join("stream.bin"), &contents).expect("update extension file in replica B");
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "publish extension update",
    );
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "install extension update",
    );
    require_success(ofs.gc(&storage), "collect extension volume");
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "an updated extension file remains readable after collection"
    );
}

pub(super) fn branch(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let main = root.path.join("main");
    let feature_a = root.path.join("feature-a");
    let feature_b = root.path.join("feature-b");
    let main_state = root.path.join("main-state");
    let feature_a_state = root.path.join("feature-a-state");
    let feature_b_state = root.path.join("feature-b-state");
    for replica in [&main, &feature_a, &feature_b] {
        fs::create_dir_all(replica).expect("create branch replica");
    }
    let storage = fixture.storage_url("branch");

    require_success(ofs.volume_create_branch(&storage), "create branch volume");
    fs::write(main.join("shared.txt"), b"main\n").expect("write main file");
    require_success(
        ofs.sync_branch(&main, &main_state, &storage, "main"),
        "publish main branch",
    );
    require_success(
        ofs.branch_create(&storage, "feature", "main"),
        "create feature branch",
    );
    require_success(
        ofs.sync_branch(&feature_a, &feature_a_state, &storage, "feature"),
        "cold-install feature branch",
    );
    fs::write(feature_a.join("shared.txt"), b"feature\n").expect("change feature file");
    fs::write(feature_a.join("feature.txt"), b"isolated\n").expect("add feature file");
    require_success(
        ofs.sync_branch(&feature_a, &feature_a_state, &storage, "feature"),
        "publish feature branch",
    );
    require_success(
        ofs.sync_branch(&feature_b, &feature_b_state, &storage, "feature"),
        "install feature branch on a second replica",
    );
    assert_eq!(
        tree_fingerprint(&feature_a),
        tree_fingerprint(&feature_b),
        "two replicas of one branch converge"
    );
    require_success(
        ofs.sync_branch(&main, &main_state, &storage, "main"),
        "observe isolated main branch",
    );
    assert_eq!(
        fs::read(main.join("shared.txt")).expect("read main file"),
        b"main\n",
        "feature publication does not change main"
    );
    assert!(
        !main.join("feature.txt").exists(),
        "feature-only paths remain isolated"
    );

    require_success(ofs.gc(&storage), "collect branch volume");
    require_success(
        ofs.sync_branch(&feature_b, &feature_b_state, &storage, "feature"),
        "read feature branch after collection",
    );
    let branches = output_text(&require_success(ofs.branch_list(&storage), "list branches").stdout);
    assert_eq!(branches, "feature\nmain", "branch list is name ordered");
    require_success(
        ofs.branch_delete(&storage, "feature"),
        "delete feature branch",
    );
    require_success(ofs.gc(&storage), "collect deleted branch");
    let branches =
        output_text(&require_success(ofs.branch_list(&storage), "list remaining branches").stdout);
    assert_eq!(
        branches, "main",
        "deleted branches are no longer selectable"
    );
}

pub(super) fn admission(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create replica A");
    fs::create_dir_all(&replica_b).expect("create replica B");

    let storage_a = fixture.storage_url("admission/a");
    let storage_b = fixture.storage_url("admission/b");
    let initialized_a = require_success(ofs.volume_create(&storage_a), "create volume A");
    let volume_a = output_text(&initialized_a.stdout)
        .split_whitespace()
        .last()
        .expect("initialization reports its volume identity")
        .to_owned();
    let mut attach_a = ofs.sync(&replica_a, &state_a, &storage_a);
    attach_a.args(["--require", "portable-names"]);
    require_success(attach_a, "attach replica A with its required capability");

    let mut unsupported = ofs.sync(&replica_a, &state_a, &storage_a);
    unsupported.args(["--require", "hard-link"]);
    let unsupported = require_failure(unsupported, "require unsupported hard links");
    assert!(
        output_text(&unsupported.stderr).contains("required filesystem capability is unavailable"),
        "an unavailable required capability is rejected before synchronization"
    );

    let status = ManagedStatus::parse(output_text(
        &require_success(ofs.status(&replica_a, &state_a), "read replica status").stdout,
    ));
    assert_eq!(
        status.volume_id, volume_a,
        "status reports the initialized remote identity"
    );
    assert!(
        !status.extended_attributes,
        "status reports that extended attributes are unavailable"
    );
    assert!(
        status.portable_names,
        "status advertises the portable-name contract"
    );

    let portable_path = "资料/café.txt";
    fs::create_dir_all(replica_a.join("资料")).expect("create portable Unicode directory");
    fs::write(replica_a.join(portable_path), b"portable name\n")
        .expect("write portable Unicode file");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage_a),
        "publish portable Unicode name",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage_a),
        "install portable Unicode name in replica B",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "a valid NFC name converges across replicas"
    );
    let portable_sequence = ManagedStatus::parse(output_text(
        &require_success(
            ofs.status(&replica_b, &state_b),
            "read portable-name sequence",
        )
        .stdout,
    ))
    .remote_sequence;

    for (names, expected_error, action) in [
        (
            &["Case.txt", "case.txt"][..],
            "case-folding collision",
            "reject a case-folding collision",
        ),
        (
            &["cafe\u{301}.txt"][..],
            "path component is not portable",
            "reject a non-NFC component",
        ),
        (
            &["CON.txt"][..],
            "path component is reserved",
            "reject a platform-reserved name",
        ),
    ] {
        for name in names {
            fs::write(replica_a.join(name), b"invalid portable name\n")
                .expect("write rejected portable name");
        }
        let rejected = require_failure(ofs.sync(&replica_a, &state_a, &storage_a), action);
        assert!(
            output_text(&rejected.stderr).contains(expected_error),
            "the rejected name reports the violated portable-name contract"
        );
        for name in names {
            fs::remove_file(replica_a.join(name)).expect("remove rejected portable name");
        }
        let unchanged = require_success(
            ofs.sync(&replica_b, &state_b, &storage_a),
            "observe remote after rejected portable name",
        );
        assert!(
            !output_text(&unchanged.stdout).contains("(published)"),
            "a rejected portable name does not publish remote changes"
        );
        let remote = ManagedStatus::parse(output_text(
            &require_success(
                ofs.status(&replica_b, &state_b),
                "read sequence after rejected portable name",
            )
            .stdout,
        ));
        assert_eq!(
            remote.remote_sequence, portable_sequence,
            "a rejected portable name does not advance the remote sequence"
        );
    }

    require_success(ofs.volume_create(&storage_b), "create volume B");
    let fenced = require_failure(
        ofs.sync(&replica_a, &state_a, &storage_b),
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

pub(super) fn smoke(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(replica_a.join("nested")).expect("create replica A");
    fs::create_dir_all(&replica_b).expect("create replica B");
    let storage = fixture.storage_url("smoke");

    require_success(ofs.volume_create(&storage), "create smoke volume");
    fs::write(replica_a.join("empty"), []).expect("write empty file");
    fs::write(replica_a.join("nested/one"), b"shared content\n").expect("write nested file");
    fs::write(replica_a.join("two"), b"shared content\n").expect("write repeated file");
    fs::write(replica_a.join("tool"), b"#!/bin/sh\nexit 0\n").expect("write executable file");
    make_executable(&replica_a.join("tool"));

    let published = require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish smoke tree",
    );
    assert!(
        output_text(&published.stdout).contains("(published)"),
        "a changed tree reports remote publication"
    );
    let object_root = "local/managed-sync/smoke/managed/1/objects/00000000000000000000";
    let (packs, _) = fixture.storage_usage(&format!("{object_root}/05-file-pack"));
    let (loose_files, _) = fixture.storage_usage(&format!("{object_root}/04-file-data"));
    assert_eq!(packs, 1, "small files are published as one immutable Pack");
    assert_eq!(
        loose_files, 0,
        "packed files do not retain duplicate loose objects"
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "cold restore smoke tree",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "cold restore reproduces files, directories, content, and executable state"
    );

    let no_op = require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "repeat unchanged sync",
    );
    assert!(
        !output_text(&no_op.stdout).contains("(published)"),
        "an unchanged sync does not publish a namespace generation"
    );

    fs::write(replica_a.join("nested/one"), b"changed content\n").expect("change nested file");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish changed smoke tree",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "install changed smoke tree",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "a later remote generation converges into the peer replica"
    );
}
