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

//! Bub end-to-end scenario across isolated containers.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Output;

use super::cli::{ManagedStatus, output_text};
use super::fixture::{CaseRoot, Fixture, container_http_proxy, tree_fingerprint};

pub(crate) fn run(keep: bool) {
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
