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

mod random_write;

use std::fs;
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::cli::{Ofs, output_text, require_success};
use super::evaluation::EvaluationOptions;
use super::fixture::{Fixture, LogicalIo, tree_summary};

const TINY_FILE_COUNT: u64 = 1_000_000;
const TINY_FILE_BYTES: u64 = 4 * 1024;
const TINY_CHANGE_COUNT: u64 = 4_096;
const TINY_DIRECTORY_FANOUT: u64 = 1_000;
const LARGE_FILE_COUNT: u64 = 3;
const LARGE_FILE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const LARGE_FILE_CHANGED_BYTES: u64 = 3 * 4096;
const STREAM_BUFFER_BYTES: usize = 1024 * 1024;
const DATASET_VERSION: &str = "v1";

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(crate) enum ScaleScenario {
    TinyFiles,
    LargeFiles,
    RandomWrite,
}

pub(crate) fn run(
    scenario: ScaleScenario,
    keep: bool,
    duration_seconds: u64,
    sync_interval_seconds: u64,
    file_size: u64,
    evaluation: EvaluationOptions,
) {
    if matches!(scenario, ScaleScenario::RandomWrite) {
        random_write::run(
            keep,
            duration_seconds,
            sync_interval_seconds,
            file_size,
            evaluation,
        );
        return;
    }
    let profile = match scenario {
        ScaleScenario::TinyFiles => Profile::TinyFiles,
        ScaleScenario::LargeFiles => Profile::LargeFiles,
        ScaleScenario::RandomWrite => unreachable!("random write was dispatched"),
    };
    let fixture = Fixture::new(keep, profile.name(), evaluation);
    let ofs = Ofs::release(fixture.ofs_home());
    ofs.build();
    let fixture = fixture.start();
    fixture.create_bucket();
    let work = ScaleRoot::new(keep);
    let dataset = stage("prepare fixture", || ScaleDataset::open(profile));
    let replicas = [
        dataset.path.clone(),
        work.path.join("replica-b"),
        work.path.join("replica-c"),
    ];
    let states = [
        work.path.join("state-a"),
        work.path.join("state-b"),
        work.path.join("state-c"),
    ];
    let changes = [
        work.path.join("changes-a.ndjson"),
        work.path.join("changes-b.ndjson"),
    ];
    let no_changes = work.path.join("changes-none.ndjson");
    fs::write(&no_changes, []).expect("create empty scale mutation input");
    for replica in &replicas[1..] {
        fs::create_dir(replica).expect("create scale replica");
    }
    let storage = fixture.storage_url(&format!("scale/{}", profile.name()));

    remote_stage(
        &fixture,
        profile,
        "create volume",
        LogicalIo::default(),
        || {
            require_success(ofs.volume_create(&storage), "create scale volume");
        },
    );
    remote_stage(
        &fixture,
        profile,
        "initial publish",
        LogicalIo {
            read_bytes: 0,
            write_bytes: profile.logical_bytes(),
        },
        || {
            require_success(
                ofs.sync(&replicas[0], &states[0], &storage),
                "publish scale fixture",
            );
        },
    );
    remote_stage(
        &fixture,
        profile,
        "cold restore",
        LogicalIo {
            read_bytes: profile.logical_bytes(),
            write_bytes: 0,
        },
        || {
            require_success(
                ofs.sync(&replicas[1], &states[1], &storage),
                "cold restore scale fixture",
            );
        },
    );
    require_same_tree(&replicas[0], &replicas[1], "initial cold restore");
    require_noop(&ofs, &replicas[0], &states[0], &storage, &no_changes);
    require_noop(&ofs, &replicas[1], &states[1], &storage, &no_changes);

    stage("mutate replica A", || {
        mutate(
            profile,
            Side::A,
            &replicas[0],
            &dataset.initial,
            &changes[0],
        )
    });
    stage("mutate replica B", || {
        mutate(
            profile,
            Side::B,
            &replicas[1],
            &dataset.initial,
            &changes[1],
        )
    });
    remote_stage(
        &fixture,
        profile,
        "publish replica A",
        LogicalIo {
            read_bytes: 0,
            write_bytes: profile.changed_bytes(Side::A),
        },
        || {
            require_success(
                ofs.sync_changes(&replicas[0], &states[0], &storage, &changes[0]),
                "publish scale changes from A",
            );
        },
    );
    remote_stage(
        &fixture,
        profile,
        "merge replica B",
        LogicalIo {
            read_bytes: profile.changed_bytes(Side::A),
            write_bytes: profile.changed_bytes(Side::B),
        },
        || {
            require_success(
                ofs.sync_changes(&replicas[1], &states[1], &storage, &changes[1]),
                "merge scale changes from B",
            );
        },
    );
    remote_stage(
        &fixture,
        profile,
        "converge replica A",
        LogicalIo {
            read_bytes: profile.changed_bytes(Side::B),
            write_bytes: 0,
        },
        || {
            require_success(
                ofs.sync_changes(&replicas[0], &states[0], &storage, &no_changes),
                "converge scale replica A",
            );
        },
    );
    require_same_tree(&replicas[0], &replicas[1], "two-writer convergence");

    remote_stage(
        &fixture,
        profile,
        "collect unreachable data",
        LogicalIo::default(),
        || {
            require_success(ofs.gc(&storage), "collect scale volume");
        },
    );
    remote_stage(
        &fixture,
        profile,
        "post-GC cold restore",
        LogicalIo {
            read_bytes: profile.logical_bytes(),
            write_bytes: 0,
        },
        || {
            require_success(
                ofs.sync(&replicas[2], &states[2], &storage),
                "restore scale volume after collection",
            );
        },
    );
    require_same_tree(&replicas[0], &replicas[2], "post-GC cold restore");
    for (replica, state) in replicas.iter().zip(&states) {
        require_noop(&ofs, replica, state, &storage, &no_changes);
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
    dataset.restore();
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
    const fn name(self) -> &'static str {
        match self {
            Self::TinyFiles => "tiny-files",
            Self::LargeFiles => "large-files",
        }
    }

    const fn logical_bytes(self) -> u64 {
        match self {
            Self::TinyFiles => TINY_FILE_COUNT * TINY_FILE_BYTES,
            Self::LargeFiles => LARGE_FILE_COUNT * LARGE_FILE_BYTES,
        }
    }

    const fn changed_bytes(self, side: Side) -> u64 {
        match (self, side) {
            (Self::TinyFiles, _) => TINY_CHANGE_COUNT / 2 * TINY_FILE_BYTES,
            (Self::LargeFiles, Side::A) => LARGE_FILE_CHANGED_BYTES,
            (Self::LargeFiles, Side::B) => (LARGE_FILE_COUNT - 1) * LARGE_FILE_CHANGED_BYTES,
        }
    }
}

#[derive(Clone, Copy)]
enum Side {
    A,
    B,
}

struct InitialContent(Vec<blake3::Hash>);

impl InitialContent {
    fn digest(&self, index: u64) -> blake3::Hash {
        self.0[index as usize]
    }
}

fn generate(profile: Profile, root: &Path) -> InitialContent {
    let mut buffer = vec![0; STREAM_BUFFER_BYTES];
    match profile {
        Profile::TinyFiles => {
            let mut digests = Vec::with_capacity((TINY_CHANGE_COUNT * 2) as usize);
            for index in 0..TINY_FILE_COUNT {
                let path = tiny_path(root, index);
                if index % TINY_DIRECTORY_FANOUT == 0 {
                    fs::create_dir_all(path.parent().expect("tiny file has a parent"))
                        .expect("create tiny-file directory");
                }
                let digest = write_xof(&path, index, 0, TINY_FILE_BYTES, &mut buffer);
                if index < TINY_CHANGE_COUNT * 2 {
                    digests.push(digest);
                }
            }
            InitialContent(digests)
        }
        Profile::LargeFiles => {
            let mut digests = Vec::with_capacity(LARGE_FILE_COUNT as usize);
            for index in 0..LARGE_FILE_COUNT {
                digests.push(write_xof(
                    &root.join(format!("large-{index}.bin")),
                    index,
                    0,
                    LARGE_FILE_BYTES,
                    &mut buffer,
                ));
            }
            InitialContent(digests)
        }
    }
}

fn mutate(profile: Profile, side: Side, root: &Path, initial: &InitialContent, changes: &Path) {
    let mut output = fs::File::create(changes).expect("create scale mutation input");
    match profile {
        Profile::TinyFiles => mutate_tiny(side, root, initial, &mut output),
        Profile::LargeFiles => mutate_large(side, root, initial, &mut output),
    }
}

fn mutate_tiny(side: Side, root: &Path, initial: &InitialContent, changes: &mut fs::File) {
    let start = match side {
        Side::A => 0,
        Side::B => TINY_CHANGE_COUNT,
    };
    let quarter = TINY_CHANGE_COUNT / 4;
    let mut buffer = vec![0; STREAM_BUFFER_BYTES];
    for index in start..start + quarter {
        let _ = write_xof(
            &tiny_path(root, index),
            index,
            1,
            TINY_FILE_BYTES,
            &mut buffer,
        );
        write_change(
            changes,
            &tiny_relative_path(index),
            initial.digest(index),
            TINY_FILE_BYTES,
            &[(0, TINY_FILE_BYTES)],
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
        let _ = write_xof(&path, identity, 1, TINY_FILE_BYTES, &mut buffer);
    }
}

fn mutate_large(side: Side, root: &Path, initial: &InitialContent, changes: &mut fs::File) {
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
        write_change(
            changes,
            &format!("large-{index}.bin"),
            initial.digest(*index),
            LARGE_FILE_BYTES,
            &[
                (0, 4096),
                (LARGE_FILE_BYTES / 2, 4096),
                (LARGE_FILE_BYTES - 4096, 4096),
            ],
        );
    }
}

fn write_change(
    output: &mut fs::File,
    path: &str,
    digest: blake3::Hash,
    length: u64,
    ranges: &[(u64, u64)],
) {
    let ranges = ranges
        .iter()
        .map(|(offset, length)| serde_json::json!({ "offset": offset, "length": length }))
        .collect::<Vec<_>>();
    writeln!(
        output,
        "{}",
        serde_json::json!({
            "path": path,
            "base": { "digest": digest.to_hex().to_string(), "length": length },
            "ranges": ranges,
        })
    )
    .expect("write scale mutation input");
}

fn tiny_path(root: &Path, index: u64) -> PathBuf {
    root.join(tiny_relative_path(index))
}

fn tiny_relative_path(index: u64) -> String {
    format!("d{:04}/f{index:07}.bin", index / TINY_DIRECTORY_FANOUT)
}

pub(super) fn write_xof(
    path: &Path,
    identity: u64,
    revision: u8,
    size: u64,
    buffer: &mut [u8],
) -> blake3::Hash {
    let mut source = content_source(identity, revision);
    let mut file = fs::File::create(path).expect("create scale file");
    let mut content = blake3::Hasher::new();
    let mut remaining = size;
    while remaining != 0 {
        let length = remaining.min(buffer.len() as u64) as usize;
        source.fill(&mut buffer[..length]);
        file.write_all(&buffer[..length]).expect("write scale file");
        content.update(&buffer[..length]);
        remaining -= length as u64;
    }
    content.finalize()
}

pub(super) fn require_same_tree(left: &Path, right: &Path, phase: &str) {
    let left = tree_summary(left);
    let right = tree_summary(right);
    assert_eq!(left, right, "Managed Sync trees differ after {phase}");
}

fn require_noop(ofs: &Ofs, replica: &Path, state: &Path, storage: &str, changes: &Path) {
    let output = require_success(
        ofs.sync_changes(replica, state, storage, changes),
        "verify scale no-op",
    );
    assert!(
        !output_text(&output.stdout).contains("(published)"),
        "unchanged scale sync published a new generation"
    );
}

fn stage<T>(name: &str, action: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let result = action();
    println!("scale stage: {name} elapsed={:?}", started.elapsed());
    result
}

fn remote_stage(
    fixture: &Fixture,
    profile: Profile,
    name: &str,
    logical: LogicalIo,
    action: impl FnOnce(),
) {
    fixture.observe(&format!("scale/{}", profile.name()), name, logical, action);
}

pub(super) struct ScaleRoot {
    pub(super) path: PathBuf,
    _directory: tempfile::TempDir,
}

impl ScaleRoot {
    pub(super) fn new(keep: bool) -> Self {
        let scale_root = Path::new(env!("CARGO_WORKSPACE_DIR")).join(".local/scale/runs");
        fs::create_dir_all(&scale_root).expect("create Managed Sync scale parent");
        let mut directory = tempfile::Builder::new()
            .prefix("managed-sync-")
            .tempdir_in(scale_root)
            .expect("create Managed Sync scale root");
        directory.disable_cleanup(keep);
        if keep {
            println!(
                "Managed Sync scale files retained: {}",
                directory.path().display()
            );
        }
        Self {
            path: directory.path().to_owned(),
            _directory: directory,
        }
    }
}

struct ScaleDataset {
    profile: Profile,
    path: PathBuf,
    initial: InitialContent,
}

impl ScaleDataset {
    fn open(profile: Profile) -> Self {
        let root = Path::new(env!("CARGO_WORKSPACE_DIR"))
            .join(".local/scale/datasets")
            .join(format!("{}-{DATASET_VERSION}", profile.name()));
        let path = root.join("data");
        let manifest = root.join("digests");
        let initial = match fs::read_to_string(&manifest) {
            Ok(document) => {
                println!("scale dataset: reuse {}", path.display());
                parse_dataset_manifest(profile, &document)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if root.exists() {
                    fs::remove_dir_all(&root).expect("remove incomplete scale dataset");
                }
                fs::create_dir_all(&path).expect("create scale dataset");
                println!("scale dataset: generate {}", path.display());
                let initial = generate(profile, &path);
                write_dataset_manifest(profile, &root, &initial);
                return Self {
                    profile,
                    path,
                    initial,
                };
            }
            Err(error) => panic!("read scale dataset manifest: {error}"),
        };
        let dataset = Self {
            profile,
            path,
            initial,
        };
        dataset.restore();
        dataset
    }

    fn restore(&self) {
        let mut buffer = vec![0; STREAM_BUFFER_BYTES];
        match self.profile {
            Profile::TinyFiles => {
                for index in 0..TINY_CHANGE_COUNT * 2 {
                    let path = tiny_path(&self.path, index);
                    let renamed = path.with_extension("renamed");
                    if let Err(error) = fs::remove_file(&renamed)
                        && error.kind() != std::io::ErrorKind::NotFound
                    {
                        panic!("remove renamed scale file {}: {error}", renamed.display());
                    }
                    let digest = write_xof(&path, index, 0, TINY_FILE_BYTES, &mut buffer);
                    assert_eq!(digest, self.initial.digest(index));

                    let added = tiny_path(&self.path, TINY_FILE_COUNT + index);
                    if let Err(error) = fs::remove_file(&added)
                        && error.kind() != std::io::ErrorKind::NotFound
                    {
                        panic!("remove added scale file {}: {error}", added.display());
                    }
                }
            }
            Profile::LargeFiles => {
                for index in 0..LARGE_FILE_COUNT {
                    let path = self.path.join(format!("large-{index}.bin"));
                    let mut file = fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .expect("open fixed large scale file");
                    assert_eq!(
                        file.metadata().expect("stat fixed large scale file").len(),
                        LARGE_FILE_BYTES,
                    );
                    for offset in [0, LARGE_FILE_BYTES / 2, LARGE_FILE_BYTES - 4096] {
                        let mut source = content_source(index, 0);
                        source.set_position(offset);
                        source.fill(&mut buffer[..4096]);
                        file.seek(SeekFrom::Start(offset))
                            .expect("seek fixed large scale file");
                        file.write_all(&buffer[..4096])
                            .expect("restore fixed large scale file");
                    }
                    file.sync_all().expect("persist fixed large scale file");
                }
            }
        }
    }
}

fn parse_dataset_manifest(profile: Profile, document: &str) -> InitialContent {
    let mut lines = document.lines();
    assert_eq!(
        lines.next(),
        Some(
            format!(
                "ofs-managed-sync-scale-{DATASET_VERSION} {}",
                profile.name()
            )
            .as_str()
        ),
        "scale dataset manifest does not match this scenario",
    );
    let digests = lines
        .map(|line| line.parse().expect("scale dataset digest is valid BLAKE3"))
        .collect::<Vec<_>>();
    let expected = match profile {
        Profile::TinyFiles => TINY_CHANGE_COUNT * 2,
        Profile::LargeFiles => LARGE_FILE_COUNT,
    };
    assert_eq!(
        digests.len() as u64,
        expected,
        "scale dataset is incomplete"
    );
    InitialContent(digests)
}

fn write_dataset_manifest(profile: Profile, root: &Path, initial: &InitialContent) {
    let temporary = root.join("digests.tmp");
    let manifest = root.join("digests");
    let mut output = fs::File::create(&temporary).expect("create scale dataset manifest");
    writeln!(
        output,
        "ofs-managed-sync-scale-{DATASET_VERSION} {}",
        profile.name(),
    )
    .expect("write scale dataset manifest");
    for digest in &initial.0 {
        writeln!(output, "{digest}").expect("write scale dataset digest");
    }
    output.sync_all().expect("persist scale dataset manifest");
    fs::rename(temporary, manifest).expect("publish scale dataset manifest");
}

fn content_source(identity: u64, revision: u8) -> blake3::OutputReader {
    let mut seed = blake3::Hasher::new();
    seed.update(b"ofs-managed-sync-scale\0");
    seed.update(&identity.to_le_bytes());
    seed.update(&[revision]);
    seed.finalize_xof()
}
