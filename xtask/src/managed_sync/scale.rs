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

//! Fixed extreme-scale Managed Sync acceptance.

use std::fs;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::cli::{Ofs, output_text, require_success};
use super::fixture::Fixture;

const TINY_FILE_COUNT: u64 = 1_000_000;
const TINY_FILE_BYTES: u64 = 4 * 1024;
const TINY_CHANGE_COUNT: u64 = 4_096;
const TINY_DIRECTORY_FANOUT: u64 = 1_000;
const LARGE_FILE_COUNT: u64 = 3;
const LARGE_FILE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const STREAM_BUFFER_BYTES: usize = 1024 * 1024;

pub(crate) fn run(profile: &str, keep: bool) {
    let profile = Profile::parse(profile);
    let ofs = Ofs::release();
    ofs.build();
    let fixture = Fixture::new(keep).start();
    fixture.create_bucket();
    let work = ScaleRoot::new(keep);
    let replicas = [
        work.path.join("replica-a"),
        work.path.join("replica-b"),
        work.path.join("replica-c"),
    ];
    let states = [
        work.path.join("state-a"),
        work.path.join("state-b"),
        work.path.join("state-c"),
    ];
    for replica in &replicas {
        fs::create_dir(replica).expect("create scale replica");
    }
    let storage = fixture.storage_url(&format!("scale/{}", profile.name()));

    stage("generate fixture", || generate(profile, &replicas[0]));
    stage("create volume", || {
        require_success(ofs.volume_create(&storage), "create scale volume");
    });
    stage("initial publish", || {
        require_success(
            ofs.sync(&replicas[0], &states[0], &storage),
            "publish scale fixture",
        );
    });
    stage("inspect initial inventory", || {
        report_inventory(&fixture, profile)
    });
    stage("cold restore", || {
        require_success(
            ofs.sync(&replicas[1], &states[1], &storage),
            "cold restore scale fixture",
        );
    });
    require_same_tree(&replicas[0], &replicas[1], "initial cold restore");
    require_noop(ofs, &replicas[0], &states[0], &storage);
    require_noop(ofs, &replicas[1], &states[1], &storage);

    stage("mutate replica A", || {
        mutate(profile, Side::A, &replicas[0])
    });
    stage("mutate replica B", || {
        mutate(profile, Side::B, &replicas[1])
    });
    stage("publish replica A", || {
        require_success(
            ofs.sync(&replicas[0], &states[0], &storage),
            "publish scale changes from A",
        );
    });
    stage("merge replica B", || {
        require_success(
            ofs.sync(&replicas[1], &states[1], &storage),
            "merge scale changes from B",
        );
    });
    stage("converge replica A", || {
        require_success(
            ofs.sync(&replicas[0], &states[0], &storage),
            "converge scale replica A",
        );
    });
    require_same_tree(&replicas[0], &replicas[1], "two-writer convergence");

    stage("collect unreachable data", || {
        require_success(ofs.gc(&storage), "collect scale volume");
    });
    stage("post-GC cold restore", || {
        require_success(
            ofs.sync(&replicas[2], &states[2], &storage),
            "restore scale volume after collection",
        );
    });
    require_same_tree(&replicas[0], &replicas[2], "post-GC cold restore");
    for (replica, state) in replicas.iter().zip(&states) {
        require_noop(ofs, replica, state, &storage);
    }

    let summary = tree_summary(&replicas[0]);
    let state_bytes = states
        .iter()
        .map(|path| {
            fs::metadata(path)
                .expect("read replica state metadata")
                .len()
        })
        .sum::<u64>();
    println!(
        "Managed Sync scale passed: {} files={} directories={} logical_bytes={} state_bytes={}",
        profile.name(),
        summary.files,
        summary.directories,
        summary.bytes,
        state_bytes,
    );
}

#[derive(Clone, Copy)]
enum Profile {
    TinyFiles,
    LargeFiles,
}

impl Profile {
    fn parse(value: &str) -> Self {
        match value {
            "tiny-files" => Self::TinyFiles,
            "large-files" => Self::LargeFiles,
            _ => panic!("unknown Managed Sync scale profile: {value}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::TinyFiles => "tiny-files",
            Self::LargeFiles => "large-files",
        }
    }
}

#[derive(Clone, Copy)]
enum Side {
    A,
    B,
}

fn generate(profile: Profile, root: &Path) {
    let mut buffer = vec![0; STREAM_BUFFER_BYTES];
    match profile {
        Profile::TinyFiles => {
            for index in 0..TINY_FILE_COUNT {
                let path = tiny_path(root, index);
                if index % TINY_DIRECTORY_FANOUT == 0 {
                    fs::create_dir_all(path.parent().expect("tiny file has a parent"))
                        .expect("create tiny-file directory");
                }
                write_xof(&path, index, 0, TINY_FILE_BYTES, &mut buffer);
            }
        }
        Profile::LargeFiles => {
            for index in 0..LARGE_FILE_COUNT {
                write_xof(
                    &root.join(format!("large-{index}.bin")),
                    index,
                    0,
                    LARGE_FILE_BYTES,
                    &mut buffer,
                );
            }
        }
    }
}

fn mutate(profile: Profile, side: Side, root: &Path) {
    match profile {
        Profile::TinyFiles => mutate_tiny(side, root),
        Profile::LargeFiles => mutate_large(side, root),
    }
}

fn mutate_tiny(side: Side, root: &Path) {
    let start = match side {
        Side::A => 0,
        Side::B => TINY_CHANGE_COUNT,
    };
    let quarter = TINY_CHANGE_COUNT / 4;
    let mut buffer = vec![0; STREAM_BUFFER_BYTES];
    for index in start..start + quarter {
        write_xof(
            &tiny_path(root, index),
            index,
            1,
            TINY_FILE_BYTES,
            &mut buffer,
        );
    }
    for index in start + quarter..start + quarter * 2 {
        let source = tiny_path(root, index);
        let destination = source.with_extension("renamed");
        fs::rename(source, destination).expect("rename tiny scale file");
    }
    for index in start + quarter * 2..start + quarter * 3 {
        fs::remove_file(tiny_path(root, index)).expect("remove tiny scale file");
    }
    for index in start + quarter * 3..start + TINY_CHANGE_COUNT {
        let identity = TINY_FILE_COUNT + index;
        let path = tiny_path(root, identity);
        fs::create_dir_all(path.parent().expect("tiny file has a parent"))
            .expect("create added tiny-file directory");
        write_xof(&path, identity, 1, TINY_FILE_BYTES, &mut buffer);
    }
}

fn mutate_large(side: Side, root: &Path) {
    let files: &[u64] = match side {
        Side::A => &[0],
        Side::B => &[1, 2],
    };
    for index in files {
        let path = root.join(format!("large-{index}.bin"));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open large file for partial update");
        for (offset, marker) in [
            (0, 0x31_u8),
            (LARGE_FILE_BYTES / 2, 0x57),
            (LARGE_FILE_BYTES - 4096, 0x93),
        ] {
            file.seek(SeekFrom::Start(offset))
                .expect("seek large scale file");
            file.write_all(&[marker; 4096])
                .expect("write partial large-file update");
        }
        file.sync_all().expect("persist large-file update");
    }
}

fn tiny_path(root: &Path, index: u64) -> PathBuf {
    root.join(format!(
        "d{:04}/f{index:07}.bin",
        index / TINY_DIRECTORY_FANOUT
    ))
}

fn write_xof(path: &Path, identity: u64, revision: u8, size: u64, buffer: &mut [u8]) {
    let mut seed = blake3::Hasher::new();
    seed.update(b"ofs-managed-sync-scale\0");
    seed.update(&identity.to_le_bytes());
    seed.update(&[revision]);
    let mut source = seed.finalize_xof();
    let mut file = fs::File::create(path).expect("create scale file");
    let mut remaining = size;
    while remaining != 0 {
        let length = remaining.min(buffer.len() as u64) as usize;
        source.fill(&mut buffer[..length]);
        file.write_all(&buffer[..length]).expect("write scale file");
        remaining -= length as u64;
    }
}

#[derive(Debug, Eq, PartialEq)]
struct TreeSummary {
    digest: blake3::Hash,
    files: u64,
    directories: u64,
    bytes: u64,
}

fn tree_summary(root: &Path) -> TreeSummary {
    let mut summary = TreeSummary {
        digest: blake3::hash(&[]),
        files: 0,
        directories: 0,
        bytes: 0,
    };
    let mut buffer = vec![0; STREAM_BUFFER_BYTES];
    summary.digest = hash_directory(root, &mut summary, &mut buffer);
    summary
}

fn hash_directory(directory: &Path, summary: &mut TreeSummary, buffer: &mut [u8]) -> blake3::Hash {
    let mut entries = fs::read_dir(directory)
        .expect("read scale directory")
        .map(|entry| entry.expect("read scale entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(fs::DirEntry::file_name);
    let mut digest = blake3::Hasher::new();
    for entry in entries {
        let name = entry.file_name();
        let name = name.to_str().expect("scale path is Unicode");
        digest.update(&(name.len() as u64).to_le_bytes());
        digest.update(name.as_bytes());
        let metadata = entry.metadata().expect("read scale entry metadata");
        if metadata.is_dir() {
            summary.directories += 1;
            digest.update(b"d");
            digest.update(hash_directory(&entry.path(), summary, buffer).as_bytes());
        } else {
            summary.files += 1;
            summary.bytes += metadata.len();
            digest.update(b"f");
            let mut file = fs::File::open(entry.path()).expect("open scale file");
            let mut file_digest = blake3::Hasher::new();
            loop {
                let read = file.read(buffer).expect("read scale file");
                if read == 0 {
                    break;
                }
                file_digest.update(&buffer[..read]);
            }
            digest.update(file_digest.finalize().as_bytes());
        }
    }
    digest.finalize()
}

fn require_same_tree(left: &Path, right: &Path, phase: &str) {
    let left = tree_summary(left);
    let right = tree_summary(right);
    assert_eq!(left, right, "Managed Sync trees differ after {phase}");
}

fn require_noop(ofs: Ofs, replica: &Path, state: &Path, storage: &str) {
    let output = require_success(ofs.sync(replica, state, storage), "verify scale no-op");
    assert!(
        !output_text(&output.stdout).contains("(published)"),
        "unchanged scale sync published a new generation"
    );
}

fn stage(name: &str, action: impl FnOnce()) {
    let started = Instant::now();
    action();
    println!("scale stage: {name} elapsed={:?}", started.elapsed());
}

fn report_inventory(fixture: &Fixture, profile: Profile) {
    const CLASSES: &[&str] = &[
        "namespace-commit",
        "namespace-segment",
        "operation-result-segment",
        "file-data",
    ];
    let mut metadata_objects = 0_u64;
    let mut metadata_bytes = 0_u64;
    for class in CLASSES {
        let target = format!(
            "local/managed-sync/scale/{}/managed/1/objects/0/{class}",
            profile.name()
        );
        let (objects, bytes) = fixture.storage_usage(&target);
        println!(
            "scale inventory: phase=initial class={class} objects={objects} encoded_bytes={bytes}"
        );
        if *class == "file-data" {
            println!(
                "scale inventory: phase=initial payload_objects={objects} payload_encoded_bytes={bytes}"
            );
        } else {
            metadata_objects += objects;
            metadata_bytes += bytes;
        }
    }
    println!(
        "scale inventory: phase=initial metadata_objects={metadata_objects} metadata_encoded_bytes={metadata_bytes}"
    );
}

struct ScaleRoot {
    path: PathBuf,
    keep: bool,
}

impl ScaleRoot {
    fn new(keep: bool) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos();
        let scale_root = Path::new(env!("CARGO_WORKSPACE_DIR")).join(".local/scale");
        fs::create_dir_all(&scale_root).expect("create Managed Sync scale parent");
        let path = scale_root.join(format!("{}-{nonce}", std::process::id()));
        fs::create_dir(&path).expect("create Managed Sync scale root");
        Self { path, keep }
    }
}

impl Drop for ScaleRoot {
    fn drop(&mut self) {
        if self.keep {
            println!("Managed Sync scale files retained: {}", self.path.display());
        } else if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "failed to remove scale root {}: {error}",
                self.path.display()
            );
        }
    }
}
