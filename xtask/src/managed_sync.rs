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
use std::fs;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Output;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

const DEFAULT_MINIO_PORT: u16 = 19_000;
const FIXTURE_READY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn run_fixture(keep: bool, case: Option<&str>) {
    build_ofs();
    let fixture = Fixture::new(keep).start();
    fixture.create_bucket();
    match case {
        Some("admission") => admission(&fixture),
        Some("gc") => gc(&fixture),
        Some("growing") => growing(&fixture),
        Some("install-recovery") => install_recovery(&fixture),
        Some("offline-gc") => offline_gc(&fixture),
        Some("reconcile") => reconcile(&fixture),
        Some("recovery-gc") => recovery_gc(&fixture),
        Some("rename") => rename(&fixture),
        Some("smoke") => smoke(&fixture),
        Some(name) => panic!("unknown Managed Sync behavior case: {name}"),
        None => {
            admission(&fixture);
            smoke(&fixture);
            reconcile(&fixture);
            rename(&fixture);
            offline_gc(&fixture);
            install_recovery(&fixture);
            recovery_gc(&fixture);
            growing(&fixture);
            gc(&fixture);
        }
    }
    println!("Managed Sync behavior passed: {}", case.unwrap_or("all"));
}

pub(crate) fn run_bub_e2e(keep: bool) {
    let api_key = env::var("BUB_API_KEY")
        .ok()
        .filter(|value| !value.is_empty())
        .expect("BUB_API_KEY is required for the Bub end-to-end scenario");
    let fixture = Fixture::new(keep).start_bub();
    fixture.create_bucket();
    let sessions = fixture.container_storage_url("bub/sessions");
    let skills = fixture.container_storage_url("bub/skills");
    let mut observed_output = Vec::new();

    for service in ["bub-a", "bub-b"] {
        container_success(
            &fixture,
            service,
            &[
                "mkdir",
                "-p",
                "/sync/sessions/tapes",
                "/workspace/.agents/skills",
                "/var/lib/ofs",
                "/var/lib/bub/.bub",
            ],
            "prepare isolated Bub client",
        );
        container_success(
            &fixture,
            service,
            &["ofs", "--version"],
            "verify installed OFS",
        );
        container_success(
            &fixture,
            service,
            &["bub", "--help"],
            "verify installed Bub",
        );
    }
    container_success(
        &fixture,
        "bub-a",
        &["touch", "/var/lib/ofs/a-only"],
        "create client A isolation marker",
    );
    container_success(
        &fixture,
        "bub-b",
        &["test", "!", "-e", "/var/lib/ofs/a-only"],
        "verify client filesystem isolation",
    );
    container_success(
        &fixture,
        "bub-b",
        &["touch", "/var/lib/ofs/b-only"],
        "create client B isolation marker",
    );
    container_success(
        &fixture,
        "bub-a",
        &["test", "!", "-e", "/var/lib/ofs/b-only"],
        "verify reverse client filesystem isolation",
    );

    for (replica, state, storage) in [
        (
            "/sync/sessions/tapes",
            "/var/lib/ofs/sessions.state",
            &sessions,
        ),
        (
            "/workspace/.agents/skills",
            "/var/lib/ofs/skills.state",
            &skills,
        ),
    ] {
        container_volume_create(&fixture, "bub-a", storage);
        container_sync(&fixture, "bub-b", replica, state, storage, &[]);
    }

    let fact_a = format!("HARBOR-A-{}", std::process::id());
    observed_output.push(container_bub(
        &fixture,
        "bub-a",
        "handoff-a",
        &format!("Remember this exact handoff fact for later: {fact_a}. Reply with the fact."),
    ));
    let marker_a = format!("BUB-SKILL-A-{}", std::process::id());
    observed_output.push(container_bub(
        &fixture,
        "bub-a",
        "create-skill-a",
        &skill_creation_prompt("sync-a", &marker_a),
    ));
    container_success(
        &fixture,
        "bub-a",
        &["test", "-f", "/workspace/.agents/skills/sync-a/SKILL.md"],
        "verify Bub created skill A",
    );
    container_success(
        &fixture,
        "bub-b",
        &["test", "!", "-e", "/workspace/.agents/skills/sync-a"],
        "verify skill A is isolated before synchronization",
    );
    converge(&fixture, "bub-a", "bub-b", &sessions, &skills);

    let continued_a = container_bub(
        &fixture,
        "bub-b",
        "handoff-a",
        "Return the exact handoff fact that I previously asked you to remember.",
    );
    assert!(
        continued_a.contains(&fact_a),
        "Bub session created in A did not continue in B"
    );
    observed_output.push(continued_a);
    let invoked_a = container_bub(
        &fixture,
        "bub-b",
        "invoke-skill-a",
        "$sync-a Follow this project skill now.",
    );
    assert!(
        invoked_a.contains(&marker_a),
        "Bub skill created in A was not discovered and invoked in B"
    );
    observed_output.push(invoked_a);

    let fact_b = format!("HARBOR-B-{}", std::process::id());
    observed_output.push(container_bub(
        &fixture,
        "bub-b",
        "handoff-b",
        &format!("Remember this exact handoff fact for later: {fact_b}. Reply with the fact."),
    ));
    let marker_b = format!("BUB-SKILL-B-{}", std::process::id());
    observed_output.push(container_bub(
        &fixture,
        "bub-b",
        "create-skill-b",
        &skill_creation_prompt("sync-b", &marker_b),
    ));
    container_success(
        &fixture,
        "bub-b",
        &["test", "-f", "/workspace/.agents/skills/sync-b/SKILL.md"],
        "verify Bub created skill B",
    );
    container_success(
        &fixture,
        "bub-a",
        &["test", "!", "-e", "/workspace/.agents/skills/sync-b"],
        "verify skill B is isolated before synchronization",
    );
    converge(&fixture, "bub-b", "bub-a", &sessions, &skills);

    let continued_b = container_bub(
        &fixture,
        "bub-a",
        "handoff-b",
        "Return the exact handoff fact that I previously asked you to remember.",
    );
    assert!(
        continued_b.contains(&fact_b),
        "Bub session created in B did not continue in A"
    );
    observed_output.push(continued_b);
    let invoked_b = container_bub(
        &fixture,
        "bub-a",
        "invoke-skill-b",
        "$sync-b Follow this project skill now.",
    );
    assert!(
        invoked_b.contains(&marker_b),
        "Bub skill created in B was not discovered and invoked in A"
    );
    observed_output.push(invoked_b);

    converge(&fixture, "bub-a", "bub-b", &sessions, &skills);
    let before_noop = bub_statuses(&fixture);
    for (replica, state, storage) in [
        (
            "/sync/sessions/tapes",
            "/var/lib/ofs/sessions.state",
            &sessions,
        ),
        (
            "/workspace/.agents/skills",
            "/var/lib/ofs/skills.state",
            &skills,
        ),
    ] {
        for service in ["bub-a", "bub-b"] {
            let output = container_sync(&fixture, service, replica, state, storage, &[]);
            assert!(
                !output.contains("(published)"),
                "final Bub convergence is not a no-op"
            );
            observed_output.push(output);
        }
    }

    let statuses = bub_statuses(&fixture);
    for (before, after) in before_noop.iter().zip(&statuses) {
        assert_eq!(
            before.common_sequence, after.common_sequence,
            "final no-op advanced a common cursor"
        );
        assert_eq!(
            before.remote_sequence, after.remote_sequence,
            "final no-op advanced a remote cursor"
        );
    }
    for status in &statuses {
        assert!(
            !status.pending && status.conflicts == 0,
            "Bub convergence status contains pending work or conflicts"
        );
        assert_eq!(
            status.common_sequence, status.remote_sequence,
            "Bub replica stopped behind the remote cursor"
        );
    }
    assert_eq!(
        statuses[0].common_sequence, statuses[1].common_sequence,
        "Bub session replicas ended at different cursors"
    );
    assert_eq!(
        statuses[2].common_sequence, statuses[3].common_sequence,
        "Bub skill replicas ended at different cursors"
    );
    observed_output.extend(statuses.into_iter().map(|status| status.document));

    let evidence = CaseRoot::new();
    for (service, suffix) in [("bub-a", "a"), ("bub-b", "b")] {
        fixture.copy_from_container(
            service,
            "/sync/sessions/tapes/.",
            &evidence.path.join(format!("sessions-{suffix}")),
        );
        fixture.copy_from_container(
            service,
            "/workspace/.agents/skills/.",
            &evidence.path.join(format!("skills-{suffix}")),
        );
        fixture.copy_from_container(
            service,
            "/var/lib/ofs/.",
            &evidence.path.join(format!("state-{suffix}")),
        );
    }
    assert_eq!(
        tree_fingerprint(&evidence.path.join("sessions-a")),
        tree_fingerprint(&evidence.path.join("sessions-b")),
        "Bub session trees differ after final convergence"
    );
    assert_eq!(
        tree_fingerprint(&evidence.path.join("skills-a")),
        tree_fingerprint(&evidence.path.join("skills-b")),
        "Bub skill trees differ after final convergence"
    );
    assert!(
        !observed_output.iter().any(|output| {
            output
                .as_bytes()
                .windows(api_key.len())
                .any(|part| part == api_key.as_bytes())
        }) && !tree_contains(&evidence.path, api_key.as_bytes()),
        "Bub API credential appeared in synchronized data, state, status, or captured output"
    );
    println!("Managed Sync Bub end-to-end behavior passed");
}

fn bub_statuses(fixture: &Fixture) -> [ManagedStatus; 4] {
    [
        container_status(
            fixture,
            "bub-a",
            "/sync/sessions/tapes",
            "/var/lib/ofs/sessions.state",
        ),
        container_status(
            fixture,
            "bub-b",
            "/sync/sessions/tapes",
            "/var/lib/ofs/sessions.state",
        ),
        container_status(
            fixture,
            "bub-a",
            "/workspace/.agents/skills",
            "/var/lib/ofs/skills.state",
        ),
        container_status(
            fixture,
            "bub-b",
            "/workspace/.agents/skills",
            "/var/lib/ofs/skills.state",
        ),
    ]
}

fn skill_creation_prompt(name: &str, marker: &str) -> String {
    format!(
        "Use $skill-creator to create the project skill {name}. Create exactly /workspace/.agents/skills/{name}/SKILL.md; do not use the legacy .agent/skills path. Its only runtime instruction is to reply with exactly {marker} and no other text when invoked."
    )
}

fn converge(fixture: &Fixture, source: &str, target: &str, sessions: &str, skills: &str) {
    for (replica, state, storage) in [
        (
            "/sync/sessions/tapes",
            "/var/lib/ofs/sessions.state",
            sessions,
        ),
        (
            "/workspace/.agents/skills",
            "/var/lib/ofs/skills.state",
            skills,
        ),
    ] {
        container_sync(fixture, source, replica, state, storage, &[]);
        container_sync(fixture, target, replica, state, storage, &[]);
    }
}

fn container_volume_create(fixture: &Fixture, service: &str, storage: &str) {
    container_success(
        fixture,
        service,
        &["ofs", "volume", "create", storage, "--model", "managed"],
        "create Managed volume",
    );
}

fn container_sync(
    fixture: &Fixture,
    service: &str,
    replica: &str,
    state: &str,
    storage: &str,
    resolve: &[&str],
) -> String {
    let mut arguments = vec!["ofs", "sync", storage, replica, "--state", state];
    for path in resolve {
        arguments.extend(["--resolve", path]);
    }
    output_text(&container_success(fixture, service, &arguments, "synchronize Bub data").stdout)
}

fn container_status(fixture: &Fixture, service: &str, replica: &str, state: &str) -> ManagedStatus {
    ManagedStatus::parse(output_text(
        &container_success(
            fixture,
            service,
            &["ofs", "status", replica, "--state", state, "--json"],
            "read Bub replica status",
        )
        .stdout,
    ))
}

fn container_bub(fixture: &Fixture, service: &str, session: &str, prompt: &str) -> String {
    let api_key = env::var("BUB_API_KEY").expect("BUB_API_KEY is available for Bub");
    let model = env::var("BUB_MODEL").unwrap_or_else(|_| "deepseek:deepseek-chat".to_owned());
    let proxy = container_http_proxy();
    for attempt in 1..=3 {
        let mut command = fixture.compose();
        command
            .env("BUB_API_KEY", &api_key)
            .env("BUB_MODEL", &model)
            .args(["exec", "-T", "-e", "BUB_API_KEY", "-e", "BUB_MODEL"]);
        if let Some(proxy) = &proxy {
            command
                .env("HTTP_PROXY", proxy)
                .env("HTTPS_PROXY", proxy)
                .env("http_proxy", proxy)
                .env("https_proxy", proxy)
                .args([
                    "-e",
                    "HTTP_PROXY",
                    "-e",
                    "HTTPS_PROXY",
                    "-e",
                    "http_proxy",
                    "-e",
                    "https_proxy",
                ]);
        }
        let output = command
            .arg(service)
            .args([
                "bub",
                "--workspace",
                "/workspace",
                "run",
                prompt,
                "--session-id",
                session,
            ])
            .output()
            .expect("execute Bub turn");
        let transcript = format!(
            "{}\n{}",
            output_text(&output.stdout),
            output_text(&output.stderr)
        );
        assert!(
            !transcript.contains(&api_key),
            "Bub API credential appeared in captured turn output"
        );
        if output.status.success() {
            return transcript;
        }
        if attempt == 3 {
            panic!(
                "run Bub turn failed after {attempt} attempts ({}); output withheld",
                output.status
            );
        }
    }
    unreachable!()
}

fn container_success(fixture: &Fixture, service: &str, arguments: &[&str], action: &str) -> Output {
    let mut command = fixture.compose();
    command.args(["exec", "-T", service]).args(arguments);
    let output = command.output().expect("execute container command");
    assert!(
        output.status.success(),
        "{action} failed: {}",
        output_text(&output.stderr)
    );
    output
}

struct ManagedStatus {
    document: String,
    volume_id: String,
    common_sequence: u64,
    remote_sequence: u64,
    pending: bool,
    conflicts: u64,
    base_expired: bool,
    extended_attributes: bool,
    portable_names: bool,
}

impl ManagedStatus {
    fn parse(document: String) -> Self {
        let value: serde_json::Value =
            serde_json::from_str(&document).expect("status is valid JSON");
        Self {
            volume_id: status_string(&value, "volume_id").to_owned(),
            common_sequence: status_u64(&value, "common_sequence"),
            remote_sequence: status_u64(&value, "remote_sequence"),
            pending: status_bool(&value, "pending"),
            conflicts: status_u64(&value, "conflicts"),
            base_expired: status_bool(&value, "base_expired"),
            extended_attributes: value
                .get("capabilities")
                .and_then(|capabilities| capabilities.get("extended_attributes"))
                .and_then(serde_json::Value::as_bool)
                .expect("extended_attributes capability is a Boolean"),
            portable_names: value
                .get("capabilities")
                .and_then(|capabilities| capabilities.get("portable_names"))
                .and_then(serde_json::Value::as_bool)
                .expect("portable_names capability is a Boolean"),
            document,
        }
    }
}

fn status_u64(status: &serde_json::Value, field: &str) -> u64 {
    status
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| panic!("status field {field} is an unsigned integer"))
}

fn status_bool(status: &serde_json::Value, field: &str) -> bool {
    status
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or_else(|| panic!("status field {field} is a Boolean"))
}

fn status_string<'a>(status: &'a serde_json::Value, field: &str) -> &'a str {
    status
        .get(field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("status field {field} is a string"))
}

fn tree_contains(root: &Path, needle: &[u8]) -> bool {
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        let metadata = fs::metadata(&path).expect("read evidence metadata");
        if metadata.is_dir() {
            pending.extend(
                fs::read_dir(path)
                    .expect("read evidence directory")
                    .map(|entry| entry.expect("read evidence entry").path()),
            );
        } else if fs::read(path)
            .expect("read evidence file")
            .windows(needle.len())
            .any(|part| part == needle)
        {
            return true;
        }
    }
    false
}

fn gc(fixture: &Fixture) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create GC replica A");
    fs::create_dir_all(&replica_b).expect("create GC replica B");
    let storage = fixture.storage_url("gc");

    run_ofs_success(ofs_volume_create(&storage), "create GC volume");
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
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish initial GC tree",
    );
    fs::write(
        replica_a.join("changing.bin"),
        deterministic_bytes(512 * 1024, 73),
    )
    .expect("replace GC content");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish replacement before GC",
    );

    run_ofs_success(ofs_gc(&storage, false), "collect unreachable objects");
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "cold restore after collection",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "collection preserves every object needed for cold restore"
    );
    run_ofs_success(ofs_gc(&storage, false), "repeat completed collection");
}

fn recovery_gc(fixture: &Fixture) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create recovery replica A");
    fs::create_dir_all(&replica_b).expect("create recovery replica B");
    let storage = fixture.storage_url("recovery-gc");

    run_ofs_success(ofs_volume_create(&storage), "create recovery volume");
    fs::write(
        replica_a.join("generation.bin"),
        deterministic_bytes(512 * 1024, 19),
    )
    .expect("write prepared publication");
    let mut prepared = ofs_sync(&replica_a, &state_a, &storage);
    prepared.env("OFS_INTERNAL_TEST_INTERRUPT", "before-publish");
    run_ofs_failure(prepared, "interrupt prepared publication");
    let collected = run_ofs_success(
        ofs_gc(&storage, false),
        "collect an interrupted prepared publication",
    );
    assert!(
        !output_text(&collected.stdout).contains("deleted 0 object"),
        "collection reclaims an interrupted prepared publication: {}",
        output_text(&collected.stdout)
    );
    run_ofs_failure(
        ofs_sync(&replica_a, &state_a, &storage),
        "invalidate a prepared publication across collection",
    );
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "prepare and publish again after collection",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "attach recovery peer",
    );

    let mut interrupted_collection = ofs_gc(&storage, false);
    interrupted_collection.env("OFS_INTERNAL_TEST_INTERRUPT", "after-gc-fence");
    run_ofs_failure(
        interrupted_collection,
        "interrupt collection after freezing the namespace",
    );
    run_ofs_success(
        ofs_gc(&storage, true),
        "resume interrupted namespace collection",
    );
    fs::write(
        replica_a.join("after-resume.txt"),
        b"collection recovery keeps publication available\n",
    )
    .expect("write after collection recovery");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish after collection recovery",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
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
    let mut interrupted = ofs_sync(&replica_a, &state_a, &storage);
    interrupted.env("OFS_INTERNAL_TEST_INTERRUPT", "after-publish");
    run_ofs_failure(interrupted, "interrupt committed publication");

    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "install interrupted publication in peer",
    );
    fs::write(
        replica_b.join("generation.bin"),
        deterministic_bytes(512 * 1024, 113),
    )
    .expect("replace interrupted publication");
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "advance beyond interrupted publication",
    );
    run_ofs_success(ofs_gc(&storage, false), "collect superseded objects");
    fs::write(
        replica_b.join("after-collection.txt"),
        b"operation receipt must survive namespace collection\n",
    )
    .expect("write post-collection change");
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "publish after namespace collection",
    );

    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "recover committed publication from current head",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "a committed operation recovers to the current namespace after collection"
    );
    let status = ManagedStatus::parse(output_text(
        &run_ofs_success(
            ofs_status(&replica_a, &state_a),
            "read recovered publication status",
        )
        .stdout,
    ));
    assert!(
        !status.pending,
        "recovery clears the committed pending publication"
    );
}

fn install_recovery(fixture: &Fixture) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create install recovery replica A");
    fs::create_dir_all(&replica_b).expect("create install recovery replica B");
    let storage = fixture.storage_url("install-recovery");

    run_ofs_success(
        ofs_volume_create(&storage),
        "create install recovery volume",
    );
    fs::write(replica_a.join("removed.txt"), b"old generation\n")
        .expect("write install recovery base");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish install recovery base",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "attach install recovery peer",
    );

    fs::remove_file(replica_a.join("removed.txt")).expect("remove install recovery base");
    fs::write(replica_a.join("current.txt"), b"current generation\n")
        .expect("write install recovery target");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish install recovery target",
    );
    let mut interrupted = ofs_sync(&replica_b, &state_b, &storage);
    interrupted.env("OFS_INTERNAL_TEST_INTERRUPT", "during-install");
    run_ofs_failure(interrupted, "interrupt remote installation");

    let recovered = run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
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
        &run_ofs_success(
            ofs_status(&replica_b, &state_b),
            "read recovered installation status",
        )
        .stdout,
    ));
    assert!(
        !status.pending && status.conflicts == 0,
        "install recovery leaves no pending work or conflicts"
    );
}

fn offline_gc(fixture: &Fixture) {
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

    run_ofs_success(ofs_volume_create(&storage), "create offline GC volume");
    fs::write(
        replica_a.join("cursor.txt"),
        deterministic_bytes(512 * 1024, 17),
    )
    .expect("write first common base");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish first common base",
    );
    run_ofs_success(
        ofs_sync(&replica_expired, &state_expired, &storage),
        "attach replica before collection",
    );

    fs::write(
        replica_a.join("cursor.txt"),
        deterministic_bytes(512 * 1024, 31),
    )
    .expect("write collection base");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish collection base",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "attach replica at the collection base",
    );
    let collected = run_ofs_success(
        ofs_gc(&storage, false),
        "collect from the current namespace",
    );
    assert!(
        !output_text(&collected.stdout).contains("deleted 0 object"),
        "collection reclaims superseded data"
    );
    run_ofs_success(
        ofs_sync(&replica_c, &state_c, &storage),
        "cold restore after collection",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_c),
        "the current namespace remains sufficient for a cold restore"
    );

    fs::write(replica_a.join("remote.txt"), b"remote change\n")
        .expect("write remote change after collection");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish change after collection",
    );
    fs::write(replica_b.join("local.txt"), b"local change\n")
        .expect("write local change from the collection base");
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "merge from the collection base",
    );
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
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
    run_ofs_failure(
        ofs_sync(&replica_expired, &state_expired, &storage),
        "report an expired reconciliation base",
    );
    let expired_status = ManagedStatus::parse(output_text(
        &run_ofs_success(
            ofs_status(&replica_expired, &state_expired),
            "read unavailable-base status",
        )
        .stdout,
    ));
    assert!(
        expired_status.base_expired && expired_status.conflicts != 0 && !expired_status.pending,
        "a replica from before collection reports an unavailable base without publishing"
    );

    run_ofs_success(
        ofs_sync(&replica_restored, &state_restored, &storage),
        "cold restore while an expired replica remains unresolved",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_restored),
        "an expired replica cannot overwrite the published namespace"
    );
    for (replica, state) in [(&replica_a, &state_a), (&replica_b, &state_b)] {
        let status = ManagedStatus::parse(output_text(
            &run_ofs_success(ofs_status(replica, state), "read converged status").stdout,
        ));
        assert!(
            !status.pending
                && status.conflicts == 0
                && status.common_sequence == status.remote_sequence,
            "retained replicas converge without pending work or conflicts"
        );
    }
}

fn rename(fixture: &Fixture) {
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

    run_ofs_success(ofs_volume_create(&storage), "create rename volume");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish rename base",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "attach rename replica B",
    );

    fs::rename(replica_a.join("file-before"), replica_a.join("file-after")).expect("rename file");
    fs::rename(replica_a.join("tree-before"), replica_a.join("tree-after"))
        .expect("move directory tree");
    make_executable(&replica_a.join("file-after"));
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish file and directory moves",
    );

    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
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
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "publish edit through moved identity",
    );
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "install post-move peer edit",
    );
    assert_eq!(
        fs::read(replica_a.join("file-after")).expect("read post-move edit"),
        b"edited after move\n"
    );
}

fn reconcile(fixture: &Fixture) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create reconcile replica A");
    fs::create_dir_all(&replica_b).expect("create reconcile replica B");
    let storage = fixture.storage_url("reconcile");

    run_ofs_success(ofs_volume_create(&storage), "create reconcile volume");
    fs::write(replica_a.join("shared.txt"), b"common\n").expect("write common file");
    fs::write(replica_a.join("delete-edit.txt"), b"common\n").expect("write delete-edit base");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish reconcile base",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "attach reconcile replica B",
    );

    fs::write(replica_a.join("from-a.txt"), b"from A\n").expect("write A-only change");
    fs::write(replica_b.join("from-b.txt"), b"from B\n").expect("write B-only change");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish A-only change",
    );
    let merged = run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "merge B-only change",
    );
    assert!(
        output_text(&merged.stdout).contains("(published)"),
        "a disjoint two-replica merge publishes one combined generation"
    );
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "install disjoint merge in A",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "disjoint changes from both replicas converge"
    );

    fs::write(replica_a.join("shared.txt"), b"candidate A\n").expect("write A candidate");
    fs::write(replica_b.join("shared.txt"), b"candidate B\n").expect("write B candidate");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish A conflict candidate",
    );
    let conflict = run_ofs_failure(
        ofs_sync(&replica_b, &state_b, &storage),
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
        &run_ofs_success(ofs_status(&replica_b, &state_b), "report retained conflict").stdout,
    ));
    assert_eq!(
        status.conflicts, 1,
        "status reports the unresolved conflict count"
    );
    run_ofs_success(
        ofs_sync_resolve(&replica_b, &state_b, &storage, &[conflict_path]),
        "resolve file conflict with local candidate",
    );
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "install resolved file in A",
    );
    assert_eq!(
        fs::read(replica_a.join("shared.txt")).expect("read resolved candidate"),
        b"candidate B\n",
        "explicit resolution publishes the selected local content"
    );

    fs::write(replica_a.join("delete-edit.txt"), b"edited in A\n").expect("edit delete-edit file");
    fs::remove_file(replica_b.join("delete-edit.txt")).expect("delete file in B");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish edit before delete conflict",
    );
    run_ofs_failure(
        ofs_sync(&replica_b, &state_b, &storage),
        "retain delete-versus-edit conflict",
    );
    assert!(
        replica_a.join("delete-edit.txt").is_file() && !replica_b.join("delete-edit.txt").exists(),
        "delete-versus-edit retains both available user outcomes"
    );
    run_ofs_success(
        ofs_sync_resolve(&replica_b, &state_b, &storage, &["delete-edit.txt"]),
        "resolve delete-versus-edit with local deletion",
    );
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "install resolved deletion in A",
    );
    assert!(
        !replica_a.join("delete-edit.txt").exists(),
        "explicit local deletion resolution converges"
    );
}

fn growing(fixture: &Fixture) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create growing replica A");
    fs::create_dir_all(&replica_b).expect("create growing replica B");
    let storage = fixture.storage_url("growing");

    run_ofs_success(ofs_volume_create(&storage), "create growing-file volume");
    let initial = deterministic_bytes(2 * 1024 * 1024, 17);
    fs::write(replica_a.join("session.tape"), initial).expect("write growing session");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish growing session",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "restore growing session",
    );

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(replica_a.join("session.tape"))
        .expect("open growing session");
    file.write_all(&deterministic_bytes(128 * 1024, 91))
        .expect("append growing session");
    file.sync_all().expect("persist growing session");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish appended session",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "install appended session",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "an appended session converges without changing its bytes"
    );
    let no_op = run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "repeat appended session sync",
    );
    assert!(
        !output_text(&no_op.stdout).contains("(published)"),
        "an unchanged appended session is a no-op"
    );
}

fn deterministic_bytes(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|offset| seed.wrapping_add((offset.wrapping_mul(31) % 251) as u8))
        .collect()
}

pub(crate) struct Fixture {
    compose_file: PathBuf,
    keep: bool,
    minio_port: u16,
    project: String,
    started: bool,
}

fn admission(fixture: &Fixture) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(&replica_a).expect("create replica A");
    fs::create_dir_all(&replica_b).expect("create replica B");

    let storage_a = fixture.storage_url("admission/a");
    let storage_b = fixture.storage_url("admission/b");
    let initialized_a = run_ofs_success(ofs_volume_create(&storage_a), "create volume A");
    let volume_a = output_text(&initialized_a.stdout)
        .split_whitespace()
        .last()
        .expect("initialization reports its volume identity")
        .to_owned();
    let mut attach_a = ofs_sync(&replica_a, &state_a, &storage_a);
    attach_a.args(["--require", "portable-names"]);
    run_ofs_success(attach_a, "attach replica A with its required capability");

    let mut unsupported = ofs_sync(&replica_a, &state_a, &storage_a);
    unsupported.args(["--require", "hard-link"]);
    let unsupported = run_ofs_failure(unsupported, "require unsupported hard links");
    assert!(
        output_text(&unsupported.stderr).contains("required filesystem capability is unavailable"),
        "an unavailable required capability is rejected before synchronization"
    );

    let status = ManagedStatus::parse(output_text(
        &run_ofs_success(ofs_status(&replica_a, &state_a), "read replica status").stdout,
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
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage_a),
        "publish portable Unicode name",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage_a),
        "install portable Unicode name in replica B",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "a valid NFC name converges across replicas"
    );
    let portable_sequence = ManagedStatus::parse(output_text(
        &run_ofs_success(
            ofs_status(&replica_b, &state_b),
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
        let rejected = run_ofs_failure(ofs_sync(&replica_a, &state_a, &storage_a), action);
        assert!(
            output_text(&rejected.stderr).contains(expected_error),
            "the rejected name reports the violated portable-name contract"
        );
        for name in names {
            fs::remove_file(replica_a.join(name)).expect("remove rejected portable name");
        }
        let unchanged = run_ofs_success(
            ofs_sync(&replica_b, &state_b, &storage_a),
            "observe remote after rejected portable name",
        );
        assert!(
            !output_text(&unchanged.stdout).contains("(published)"),
            "a rejected portable name does not publish remote changes"
        );
        let remote = ManagedStatus::parse(output_text(
            &run_ofs_success(
                ofs_status(&replica_b, &state_b),
                "read sequence after rejected portable name",
            )
            .stdout,
        ));
        assert_eq!(
            remote.remote_sequence, portable_sequence,
            "a rejected portable name does not advance the remote sequence"
        );
    }

    run_ofs_success(ofs_volume_create(&storage_b), "create volume B");
    let fenced = run_ofs_failure(
        ofs_sync(&replica_a, &state_a, &storage_b),
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

fn smoke(fixture: &Fixture) {
    let root = CaseRoot::new();
    let replica_a = root.path.join("replica-a");
    let replica_b = root.path.join("replica-b");
    let state_a = root.path.join("state-a");
    let state_b = root.path.join("state-b");
    fs::create_dir_all(replica_a.join("nested")).expect("create replica A");
    fs::create_dir_all(&replica_b).expect("create replica B");
    let storage = fixture.storage_url("smoke");

    run_ofs_success(ofs_volume_create(&storage), "create smoke volume");
    fs::write(replica_a.join("empty"), []).expect("write empty file");
    fs::write(replica_a.join("nested/one"), b"shared content\n").expect("write nested file");
    fs::write(replica_a.join("two"), b"shared content\n").expect("write repeated file");
    fs::write(replica_a.join("tool"), b"#!/bin/sh\nexit 0\n").expect("write executable file");
    make_executable(&replica_a.join("tool"));

    let published = run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish smoke tree",
    );
    assert!(
        output_text(&published.stdout).contains("(published)"),
        "a changed tree reports remote publication"
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "cold restore smoke tree",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "cold restore reproduces files, directories, content, and executable state"
    );

    let no_op = run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "repeat unchanged sync",
    );
    assert!(
        !output_text(&no_op.stdout).contains("(published)"),
        "an unchanged sync does not publish a namespace generation"
    );

    fs::write(replica_a.join("nested/one"), b"changed content\n").expect("change nested file");
    run_ofs_success(
        ofs_sync(&replica_a, &state_a, &storage),
        "publish changed smoke tree",
    );
    run_ofs_success(
        ofs_sync(&replica_b, &state_b, &storage),
        "install changed smoke tree",
    );
    assert_eq!(
        tree_fingerprint(&replica_a),
        tree_fingerprint(&replica_b),
        "a later remote generation converges into the peer replica"
    );
}

fn tree_fingerprint(root: &Path) -> blake3::Hash {
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
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = fs::metadata(path)
        .expect("read executable metadata")
        .permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    fs::set_permissions(path, permissions).expect("set executable mode");
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}

#[cfg(unix)]
fn is_executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &fs::Metadata) -> bool {
    false
}

fn build_ofs() {
    let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .current_dir(env!("CARGO_WORKSPACE_DIR"))
        .args(["build", "--bin", "ofs"]);
    run(&mut command);
}

fn ofs_volume_create(storage: &str) -> Command {
    let mut command = ofs_command();
    command
        .arg("volume")
        .arg("create")
        .arg(storage)
        .args(["--model", "managed"]);
    command
}

fn ofs_sync(replica: &Path, state: &Path, storage: &str) -> Command {
    let mut command = ofs_command();
    command
        .arg("sync")
        .arg(storage)
        .arg(replica)
        .arg("--state")
        .arg(state);
    command
}

fn ofs_status(replica: &Path, state: &Path) -> Command {
    let mut command = ofs_command();
    command
        .arg("status")
        .arg(replica)
        .arg("--state")
        .arg(state)
        .arg("--json");
    command
}

fn ofs_gc(storage: &str, resume: bool) -> Command {
    let mut command = ofs_command();
    command.arg("gc").arg(storage);
    if resume {
        command.arg("--resume");
    }
    command
}

fn ofs_sync_resolve(replica: &Path, state: &Path, storage: &str, paths: &[&str]) -> Command {
    let mut command = ofs_sync(replica, state, storage);
    for path in paths {
        command.arg("--resolve").arg(path);
    }
    command
}

fn ofs_debug_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_WORKSPACE_DIR")).join("target/debug/ofs")
}

fn ofs_command() -> Command {
    let mut command = Command::new(ofs_debug_binary());
    command
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true");
    command
}

fn run_ofs_success(mut command: Command, action: &str) -> Output {
    let output = command.output().expect("execute ofs behavior command");
    assert!(
        output.status.success(),
        "{action} failed: {}",
        output_text(&output.stderr)
    );
    output
}

fn run_ofs_failure(mut command: Command, action: &str) -> Output {
    let output = command.output().expect("execute ofs behavior command");
    assert!(!output.status.success(), "{action} unexpectedly succeeded");
    output
}

fn output_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

struct CaseRoot {
    path: PathBuf,
}

impl CaseRoot {
    fn new() -> Self {
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

    fn start(mut self) -> Self {
        self.started = true;
        run(self.compose().args(["up", "--detach", "minio"]));
        self.wait_until_ready();
        self
    }

    fn start_bub(mut self) -> Self {
        let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
        run(docker_command()
            .args(["build", "--network", "host", "--tag", &self.bub_image()])
            .args(["--file"])
            .arg(workspace.join("fixtures/managed-sync/bub.Dockerfile"))
            .arg(&workspace));
        self.started = true;
        run(self
            .compose()
            .args(["up", "--detach", "--no-build", "minio", "bub-a", "bub-b"]));
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

    pub(crate) fn storage_url(&self, root: &str) -> String {
        format!(
            "s3://managed-sync/{root}?endpoint=http%3A%2F%2F127.0.0.1%3A{}&region=us-east-1",
            self.minio_port
        )
    }

    fn container_storage_url(&self, root: &str) -> String {
        format!(
            "s3://managed-sync/{root}?endpoint=http%3A%2F%2F127.0.0.1%3A{}&region=us-east-1",
            self.minio_port
        )
    }

    fn copy_from_container(&self, service: &str, source: &str, destination: &Path) {
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
        run(docker_command().args(["cp", &remote]).arg(destination));
    }

    fn compose(&self) -> Command {
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

fn container_http_proxy() -> Option<String> {
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

fn run(command: &mut Command) {
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
