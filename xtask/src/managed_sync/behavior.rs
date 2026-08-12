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

//! User-visible Managed Sync behavior registry.

mod lifecycle;
mod reconciliation;
mod recovery;

use std::fs;
use std::path::Path;

use super::cli::Ofs;
use super::fixture::Fixture;

pub(crate) fn run(keep: bool, case: Option<&str>, extension: Option<&str>) {
    let ofs = Ofs::debug();
    ofs.build();
    let fixture = Fixture::new(keep).start();
    fixture.create_bucket();
    if let Some(extension) = extension {
        match extension {
            "pack" => lifecycle::smoke(&fixture, ofs),
            "fastcdc" => lifecycle::file_extension(&fixture, ofs, "fastcdc", false, false),
            "zstd" => lifecycle::file_extension(&fixture, ofs, "zstd", true, false),
            "branch" => lifecycle::branch(&fixture, ofs),
            "tracing" => lifecycle::file_extension(&fixture, ofs, "tracing", false, true),
            "all" => {
                lifecycle::smoke(&fixture, ofs);
                lifecycle::file_extension(&fixture, ofs, "fastcdc", false, false);
                lifecycle::file_extension(&fixture, ofs, "zstd", true, false);
                lifecycle::branch(&fixture, ofs);
                lifecycle::file_extension(&fixture, ofs, "tracing", false, true);
            }
            _ => unreachable!("clap validates extension IDs"),
        }
        println!("Managed Sync extension smoke passed: {extension}");
        return;
    }
    match case {
        Some("admission") => lifecycle::admission(&fixture, ofs),
        Some("gc") => recovery::gc(&fixture, ofs),
        Some("growing") => lifecycle::growing(&fixture, ofs),
        Some("extensions") => lifecycle::extensions(&fixture, ofs),
        Some("branch") => lifecycle::branch(&fixture, ofs),
        Some("install-recovery") => recovery::install_recovery(&fixture, ofs),
        Some("offline-gc") => recovery::offline_gc(&fixture, ofs),
        Some("reconcile") => reconciliation::reconcile(&fixture, ofs),
        Some("recovery-gc") => recovery::recovery_gc(&fixture, ofs),
        Some("rename") => reconciliation::rename(&fixture, ofs),
        Some("smoke") => lifecycle::smoke(&fixture, ofs),
        Some(name) => panic!("unknown Managed Sync behavior case: {name}"),
        None => {
            lifecycle::admission(&fixture, ofs);
            lifecycle::smoke(&fixture, ofs);
            lifecycle::extensions(&fixture, ofs);
            lifecycle::branch(&fixture, ofs);
            reconciliation::reconcile(&fixture, ofs);
            reconciliation::rename(&fixture, ofs);
            recovery::offline_gc(&fixture, ofs);
            recovery::install_recovery(&fixture, ofs);
            recovery::recovery_gc(&fixture, ofs);
            lifecycle::growing(&fixture, ofs);
            recovery::gc(&fixture, ofs);
        }
    }
    println!("Managed Sync behavior passed: {}", case.unwrap_or("all"));
}

pub(super) fn deterministic_bytes(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|offset| seed.wrapping_add((offset.wrapping_mul(31) % 251) as u8))
        .collect()
}

#[cfg(unix)]
pub(super) fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = fs::metadata(path)
        .expect("read executable metadata")
        .permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    fs::set_permissions(path, permissions).expect("set executable mode");
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}
