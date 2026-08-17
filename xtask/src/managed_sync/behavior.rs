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
use super::evaluation::EvaluationOptions;
use super::fixture::Fixture;

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(crate) enum BehaviorCase {
    Admission,
    ContentReuse,
    Gc,
    Growing,
    Extensions,
    Branch,
    Chaos,
    Interruption,
    OfflineGc,
    Reconcile,
    Rename,
    Smoke,
}

pub(crate) fn run(keep: bool, case: Option<BehaviorCase>, evaluation: EvaluationOptions) {
    let fixture = Fixture::new(keep, "behavior", evaluation);
    let ofs = Ofs::debug(fixture.ofs_home());
    ofs.build();
    let fixture = fixture.start();
    fixture.create_bucket();
    match case {
        Some(BehaviorCase::Admission) => lifecycle::admission(&fixture, ofs),
        Some(BehaviorCase::ContentReuse) => lifecycle::content_reuse(&fixture, ofs),
        Some(BehaviorCase::Gc) => recovery::gc(&fixture, ofs),
        Some(BehaviorCase::Growing) => lifecycle::growing(&fixture, ofs),
        Some(BehaviorCase::Extensions) => lifecycle::extensions(&fixture, ofs),
        Some(BehaviorCase::Branch) => lifecycle::branch(&fixture, ofs),
        Some(BehaviorCase::Chaos) => lifecycle::chaos(&fixture, ofs),
        Some(BehaviorCase::Interruption) => recovery::interruption(&fixture, ofs),
        Some(BehaviorCase::OfflineGc) => recovery::offline_gc(&fixture, ofs),
        Some(BehaviorCase::Reconcile) => reconciliation::reconcile(&fixture, ofs),
        Some(BehaviorCase::Rename) => reconciliation::rename(&fixture, ofs),
        Some(BehaviorCase::Smoke) => lifecycle::smoke(&fixture, ofs),
        None => {
            lifecycle::admission(&fixture, ofs.clone());
            lifecycle::smoke(&fixture, ofs.clone());
            lifecycle::content_reuse(&fixture, ofs.clone());
            lifecycle::extensions(&fixture, ofs.clone());
            lifecycle::branch(&fixture, ofs.clone());
            lifecycle::chaos(&fixture, ofs.clone());
            reconciliation::reconcile(&fixture, ofs.clone());
            reconciliation::rename(&fixture, ofs.clone());
            recovery::offline_gc(&fixture, ofs.clone());
            recovery::interruption(&fixture, ofs.clone());
            lifecycle::growing(&fixture, ofs.clone());
            recovery::gc(&fixture, ofs);
        }
    }
    println!("Managed Sync behavior passed: {case:?}");
}

pub(super) fn deterministic_bytes(length: usize, seed: u8) -> Vec<u8> {
    let mut bytes = vec![0_u8; length];
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ofs-managed-sync-behavior");
    hasher.update(&[seed]);
    hasher.finalize_xof().fill(&mut bytes);
    bytes
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
