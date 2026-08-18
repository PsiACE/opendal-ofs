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

use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

fn ofs() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ofs"))
}

fn fs_storage_url(path: &Path) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    format!("fs:///?root={path}")
}

#[test]
fn create_then_inspect_reports_the_same_volume() {
    let home = TempDir::new().expect("ofs home");
    let storage = TempDir::new().expect("volume storage");
    let storage_url = fs_storage_url(storage.path());

    let created = ofs()
        .env("OFS_HOME", home.path())
        .args([
            "volume",
            "create",
            "workspace",
            "--model",
            "managed",
            "--storage",
            &storage_url,
        ])
        .output()
        .expect("create volume");
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let created = String::from_utf8_lossy(&created.stdout);
    assert!(created.contains("created managed volume workspace"));

    let inspected = ofs()
        .env("OFS_HOME", home.path())
        .args(["volume", "inspect", "workspace"])
        .output()
        .expect("inspect volume");
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let inspected = String::from_utf8_lossy(&inspected.stdout);
    assert!(inspected.contains("volume workspace"));
    assert!(inspected.contains("model managed"));
    assert!(inspected.contains("layout v0"));
    assert!(inspected.contains("data-segment-target-size 8388608B"));
}

#[test]
fn inspect_missing_volume_fails() {
    let home = TempDir::new().expect("ofs home");
    let inspected = ofs()
        .env("OFS_HOME", home.path())
        .args(["volume", "inspect", "missing"])
        .output()
        .expect("inspect missing volume");
    assert!(!inspected.status.success());
}
