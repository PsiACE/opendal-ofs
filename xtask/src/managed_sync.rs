// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const COMPOSE_FILE: &str = "fixtures/managed-sync/compose.yaml";
const DEFAULT_PROJECT: &str = "opendal-ofs-managed-sync";
const DEFAULT_MINIO_PORT: &str = "19000";
const DEFAULT_D1_PORT: &str = "19001";

pub(crate) fn run(mut arguments: impl Iterator<Item = String>) -> Result<(), String> {
    let command = arguments
        .next()
        .ok_or_else(|| "missing Managed Sync command; expected doctor, up, or down".to_string())?;
    match command.as_str() {
        "up" => no_arguments(arguments, up),
        "down" => no_arguments(arguments, down),
        "test" => test(arguments),
        "perf" => run_performance(arguments),
        "-h" | "--help" => {
            println!(
                "Usage: cargo x managed-sync <up|down|test all|workflow object|d1|branch object|d1|staging|perf [OPTIONS]>"
            );
            Ok(())
        }
        _ => Err(format!("unknown Managed Sync command {command:?}")),
    }
}

fn run_performance(arguments: impl Iterator<Item = String>) -> Result<(), String> {
    let mut command = Command::new("python3");
    command
        .current_dir(workspace_root())
        .arg("scripts/managed_sync_perf.py")
        .args(arguments);
    run_command(&mut command, "run Managed Sync release A/B")
}

fn no_arguments(
    mut arguments: impl Iterator<Item = String>,
    action: fn() -> Result<(), String>,
) -> Result<(), String> {
    if let Some(argument) = arguments.next() {
        return Err(format!("unexpected argument {argument:?}"));
    }
    action()
}

fn test(mut arguments: impl Iterator<Item = String>) -> Result<(), String> {
    let kind = arguments.next().ok_or_else(|| {
        "expected `test all`, `test workflow|branch object|d1`, or `test staging`".to_string()
    })?;
    let tests = match kind.as_str() {
        "workflow" | "branch" => {
            let metadata = arguments
                .next()
                .ok_or_else(|| format!("{kind} requires object or d1 metadata"))?;
            let metadata = match metadata.as_str() {
                "object" => "object",
                "d1" => "d1",
                _ => return Err(format!("{kind} metadata must be object or d1")),
            };
            vec![(Some(kind), metadata)]
        }
        "staging" => vec![(None, "object")],
        "all" => vec![
            (Some("workflow".into()), "object"),
            (Some("workflow".into()), "d1"),
            (Some("branch".into()), "object"),
            (Some("branch".into()), "d1"),
            (None, "object"),
        ],
        _ => {
            return Err(
                "expected `test all`, `test workflow|branch object|d1`, or `test staging`".into(),
            );
        }
    };
    if let Some(argument) = arguments.next() {
        return Err(format!("unexpected argument {argument:?}"));
    }
    build_ofs()?;
    up()?;
    let result = tests
        .into_iter()
        .try_for_each(|(kind, metadata)| match kind {
            Some(kind) => run_acceptance(&kind, metadata),
            None => run_staging_regression(),
        });
    let cleanup = down();
    result?;
    cleanup
}

fn build_ofs() -> Result<(), String> {
    run_command(
        Command::new("cargo")
            .current_dir(workspace_root())
            .args(["build", "--locked"]),
        "build ofs for Managed acceptance",
    )
}

fn run_staging_regression() -> Result<(), String> {
    let run_root = env::temp_dir().join(format!(
        "ofs-managed-sync-staging-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| format!("system clock is invalid: {error}"))?
            .as_nanos()
    ));
    std::fs::create_dir(&run_root)
        .map_err(|error| format!("could not create staging regression directory: {error}"))?;
    let endpoint = format!("http%3A%2F%2F127.0.0.1%3A{}", minio_port());
    let case_id = run_root.file_name().unwrap().to_string_lossy();
    let storage = format!("s3://managed-sync/{case_id}?endpoint={endpoint}&region=us-east-1");
    let result = run_command(
        Command::new("bash")
            .current_dir(workspace_root())
            .arg("tests/performance/managed-sync/staging.sh")
            .env("OFS_BIN", workspace_root().join("target/debug/ofs"))
            .env("OFS_CASE_ROOT", &run_root)
            .env("OFS_STORAGE_URL", storage)
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .env("AWS_REGION", "us-east-1"),
        "run Managed Sync staging regression",
    );
    if result.is_ok() {
        std::fs::remove_dir_all(&run_root)
            .map_err(|error| format!("could not remove staging regression directory: {error}"))?;
    } else {
        eprintln!(
            "Managed Sync staging evidence retained at {}",
            run_root.display()
        );
    }
    result
}

fn run_acceptance(suite: &str, metadata: &str) -> Result<(), String> {
    let script = match suite {
        "workflow" => "tests/behavior/managed-sync/workflow.sh",
        "branch" => "tests/behavior/managed-branch/workflow.sh",
        _ => return Err(format!("unknown acceptance suite {suite:?}")),
    };
    let run_root = env::temp_dir().join(format!(
        "ofs-managed-{suite}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| format!("system clock is invalid: {error}"))?
            .as_nanos()
    ));
    std::fs::create_dir(&run_root)
        .map_err(|error| format!("could not create acceptance directory: {error}"))?;
    let case_root = run_root.join("case");
    let binary = workspace_root().join("target/debug/ofs");
    let endpoint = format!("http%3A%2F%2F127.0.0.1%3A{}", minio_port());
    let case_id = run_root.file_name().unwrap().to_string_lossy();
    let storage = format!("s3://managed-sync/{case_id}?endpoint={endpoint}&region=us-east-1");

    let mut workflow = Command::new("bash");
    workflow
        .current_dir(workspace_root())
        .arg(script)
        .env("OFS_BIN", binary)
        .env("OFS_CASE_ROOT", &case_root)
        .env("OFS_STORAGE_URL", storage)
        .env("OFS_METADATA_MODE", metadata)
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1");
    if metadata == "d1" {
        let api_base = format!("http%3A%2F%2F127.0.0.1%3A{}%2Fclient%2Fv4", d1_port());
        workflow
            .env(
                "OFS_METADATA_URL",
                format!("d1://local/managed-sync/{case_id}?api_base={api_base}"),
            )
            .env("OFS_D1_TOKEN", "local-d1-token");
    }
    let result = run_command(&mut workflow, &format!("run Managed {suite} acceptance"));
    if result.is_ok() {
        std::fs::remove_dir_all(&run_root)
            .map_err(|error| format!("could not remove acceptance directory: {error}"))?;
    } else {
        eprintln!(
            "Managed {suite} evidence retained at {}",
            run_root.display()
        );
    }
    result
}

fn up() -> Result<(), String> {
    run_command(
        compose()?.args(["up", "--detach", "minio", "d1"]),
        "start MinIO and local D1",
    )?;
    wait_for_http(&format!(
        "http://127.0.0.1:{}/minio/health/ready",
        minio_port()
    ))?;
    wait_for_http(&format!("http://127.0.0.1:{}/health", d1_port()))?;
    run_command(
        compose()?.args([
            "run",
            "--rm",
            "-T",
            "minio-client",
            "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null; mc mb --ignore-existing local/managed-sync >/dev/null; mc stat local/managed-sync >/dev/null",
        ]),
        "create the Managed Sync MinIO bucket",
    )?;
    println!(
        "Managed Sync fixtures are ready: MinIO http://127.0.0.1:{}, D1 http://127.0.0.1:{}/client/v4.",
        minio_port(),
        d1_port()
    );
    Ok(())
}

fn down() -> Result<(), String> {
    run_command(
        compose()?.args(["down", "--volumes", "--remove-orphans"]),
        "stop Managed Sync fixtures",
    )
}

fn wait_for_http(url: &str) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if Command::new("curl")
            .args(["--fail", "--silent", "--output", "/dev/null", url])
            .status()
            .is_ok_and(|status| status.success())
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(250));
    }
    Err(format!("fixture did not become ready: {url}"))
}

fn compose() -> Result<Command, String> {
    let runtime = compose_runtime()?;
    let mut command = Command::new(runtime.program);
    command.current_dir(workspace_root());
    command.env("OFS_MANAGED_SYNC_MINIO_PORT", minio_port());
    command.env("OFS_MANAGED_SYNC_D1_PORT", d1_port());
    command.args(runtime.prefix);
    command.args(["--project-name", &project_name(), "--file", COMPOSE_FILE]);
    Ok(command)
}

struct ComposeRuntime {
    program: &'static OsStr,
    prefix: &'static [&'static str],
}

fn compose_runtime() -> Result<ComposeRuntime, String> {
    if let Ok(value) = env::var("OFS_COMPOSE") {
        return match value.as_str() {
            "docker" => Ok(ComposeRuntime {
                program: OsStr::new("docker"),
                prefix: &["compose"],
            }),
            "podman" => Ok(ComposeRuntime {
                program: OsStr::new("podman"),
                prefix: &["compose"],
            }),
            "podman-compose" => Ok(ComposeRuntime {
                program: OsStr::new("podman-compose"),
                prefix: &[],
            }),
            _ => Err("OFS_COMPOSE must be docker, podman, or podman-compose".into()),
        };
    }
    for runtime in [
        ComposeRuntime {
            program: OsStr::new("docker"),
            prefix: &["compose"],
        },
        ComposeRuntime {
            program: OsStr::new("podman"),
            prefix: &["compose"],
        },
        ComposeRuntime {
            program: OsStr::new("podman-compose"),
            prefix: &[],
        },
    ] {
        if Command::new(runtime.program)
            .args(runtime.prefix)
            .arg("version")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
        {
            return Ok(runtime);
        }
    }
    Err("Docker Compose or podman-compose is required".into())
}

fn run_command(command: &mut Command, purpose: &str) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|error| format!("could not {purpose}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("could not {purpose}: process exited with {status}"))
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be inside the workspace")
        .to_owned()
}

fn project_name() -> String {
    env::var("OFS_MANAGED_SYNC_PROJECT").unwrap_or_else(|_| DEFAULT_PROJECT.into())
}

fn minio_port() -> String {
    env::var("OFS_MANAGED_SYNC_MINIO_PORT").unwrap_or_else(|_| DEFAULT_MINIO_PORT.into())
}

fn d1_port() -> String {
    env::var("OFS_MANAGED_SYNC_D1_PORT").unwrap_or_else(|_| DEFAULT_D1_PORT.into())
}
