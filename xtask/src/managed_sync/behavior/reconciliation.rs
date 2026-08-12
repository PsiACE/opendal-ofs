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

//! Multi-replica reconciliation and rename behavior.

use std::fs;

use super::super::cli::{ManagedStatus, Ofs, output_text, require_failure, require_success};
use super::super::fixture::{CaseRoot, Fixture, tree_fingerprint};
use super::make_executable;

pub(super) fn rename(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(replica_a.join("tree-before/subtree/empty"))
        .expect("create rename source tree");
    fs::create_dir_all(&replica_b).expect("create rename replica B");
    fs::write(replica_a.join("file-before"), b"stable file\n").expect("write rename file");
    fs::write(
        replica_a.join("tree-before/subtree/leaf"),
        b"stable directory tree\n",
    )
    .expect("write rename tree leaf");
    let storage = fixture.storage_url("rename");

    require_success(ofs.volume_create(&storage), "create rename volume");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish rename base",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "attach rename replica B",
    );

    fs::rename(replica_a.join("file-before"), replica_a.join("file-after")).expect("rename file");
    fs::rename(replica_a.join("tree-before"), replica_a.join("tree-after"))
        .expect("move directory tree");
    make_executable(&replica_a.join("file-after"));
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish file and directory moves",
    );

    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "install remote moves",
    );
    assert!(
        !replica_b.join("file-before").exists() && !replica_b.join("tree-before").exists(),
        "old paths disappear after remote moves"
    );
    assert!(
        replica_b.join("tree-after/subtree/empty").is_dir(),
        "a moved empty directory is retained"
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "file identity, directory contents, and attributes survive moves"
    );

    fs::write(replica_b.join("file-after"), b"edited after move\n")
        .expect("edit moved file in peer");
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "publish edit through moved identity",
    );
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "install post-move peer edit",
    );
    assert_eq!(
        fs::read(replica_a.join("file-after")).expect("read post-move edit"),
        b"edited after move\n"
    );
}

pub(super) fn reconcile(fixture: &Fixture, ofs: Ofs) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create reconcile replica A");
    fs::create_dir_all(&replica_b).expect("create reconcile replica B");
    let storage = fixture.storage_url("reconcile");

    require_success(ofs.volume_create(&storage), "create reconcile volume");
    fs::write(replica_a.join("shared.txt"), b"common\n").expect("write common file");
    fs::write(replica_a.join("delete-edit.txt"), b"common\n").expect("write delete-edit base");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish reconcile base",
    );
    require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "attach reconcile replica B",
    );

    fs::write(replica_a.join("from-a.txt"), b"from A\n").expect("write A-only change");
    fs::write(replica_b.join("from-b.txt"), b"from B\n").expect("write B-only change");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish A-only change",
    );
    let merged = require_success(
        ofs.sync(&replica_b, &state_b, &storage),
        "merge B-only change",
    );
    assert!(
        output_text(&merged.stdout).contains("(published)"),
        "a disjoint two-replica merge publishes one combined generation"
    );
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "install disjoint merge in A",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "disjoint changes from both replicas converge"
    );

    fs::write(replica_a.join("shared.txt"), b"candidate A\n").expect("write A candidate");
    fs::write(replica_b.join("shared.txt"), b"candidate B\n").expect("write B candidate");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish A conflict candidate",
    );
    let conflict = require_failure(
        ofs.sync(&replica_b, &state_b, &storage),
        "retain concurrent file conflict",
    );
    let conflict_message = output_text(&conflict.stderr);
    assert!(
        conflict_message.contains("retained 1 conflict")
            && conflict_message.contains("--resolve <relative-path>"),
        "a concurrent file update explains explicit resolution"
    );
    let conflict_path = conflict_message
        .lines()
        .find_map(|line| line.strip_prefix("  "))
        .expect("a concurrent file update reports its normalized relative path");
    assert_eq!(
        conflict_path, "shared.txt",
        "the reported conflict identifies the user-visible path"
    );
    assert_eq!(
        fs::read(replica_a.join("shared.txt")).expect("read remote candidate"),
        b"candidate A\n"
    );
    assert_eq!(
        fs::read(replica_b.join("shared.txt")).expect("read local candidate"),
        b"candidate B\n"
    );
    let status = ManagedStatus::parse(output_text(
        &require_success(ofs.status(&replica_b, &state_b), "report retained conflict").stdout,
    ));
    assert_eq!(
        status.conflicts, 1,
        "status reports the unresolved conflict count"
    );
    require_success(
        ofs.sync_resolve(&replica_b, &state_b, &storage, &[conflict_path]),
        "resolve file conflict with local candidate",
    );
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "install resolved file in A",
    );
    assert_eq!(
        fs::read(replica_a.join("shared.txt")).expect("read resolved candidate"),
        b"candidate B\n",
        "explicit resolution publishes the selected local content"
    );

    fs::write(replica_a.join("delete-edit.txt"), b"edited in A\n").expect("edit delete-edit file");
    fs::remove_file(replica_b.join("delete-edit.txt")).expect("delete file in B");
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "publish edit before delete conflict",
    );
    require_failure(
        ofs.sync(&replica_b, &state_b, &storage),
        "retain delete-versus-edit conflict",
    );
    assert!(
        replica_a.join("delete-edit.txt").is_file() && !replica_b.join("delete-edit.txt").exists(),
        "delete-versus-edit retains both available user outcomes"
    );
    require_success(
        ofs.sync_resolve(&replica_b, &state_b, &storage, &["delete-edit.txt"]),
        "resolve delete-versus-edit with local deletion",
    );
    require_success(
        ofs.sync(&replica_a, &state_a, &storage),
        "install resolved deletion in A",
    );
    assert!(
        !replica_a.join("delete-edit.txt").exists(),
        "explicit local deletion resolution converges"
    );
}
