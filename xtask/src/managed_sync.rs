// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::path::Path;
use std::process::Command;

pub(crate) fn run(arguments: impl Iterator<Item = String>) -> Result<(), String> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be inside the workspace");
    let status = Command::new("bash")
        .current_dir(workspace)
        .arg("tests/behavior/managed-sync/run.sh")
        .args(arguments)
        .status()
        .map_err(|error| format!("could not run Managed Sync harness: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Managed Sync harness exited with {status}"))
    }
}
