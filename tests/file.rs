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

#![cfg(any(target_os = "linux", target_os = "freebsd"))]

mod common;

use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use common::OfsTestContext;
use test_context::test_context;

static TEST_TEXT: &str = include_str!("../Cargo.toml");

#[test_context(OfsTestContext)]
#[test]
fn test_file(ctx: &mut OfsTestContext) {
    runtime()
        .block_on(ctx.backend.write("test_file.txt", TEST_TEXT))
        .unwrap();
    let path = ctx.mount_point.path().join("test_file.txt");
    let mut file = File::open(&path).unwrap();
    let mut buf = String::new();
    file.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, TEST_TEXT);
}

#[test_context(OfsTestContext)]
#[test]
fn test_file_seek(ctx: &mut OfsTestContext) {
    runtime()
        .block_on(ctx.backend.write("test_file_seek.txt", TEST_TEXT))
        .unwrap();
    let path = ctx.mount_point.path().join("test_file_seek.txt");
    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(TEST_TEXT.len() as u64 / 2))
        .unwrap();
    let mut buf = String::new();
    file.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, TEST_TEXT[TEST_TEXT.len() / 2..]);
}

#[test_context(OfsTestContext)]
#[test]
fn test_mount_is_read_only(ctx: &mut OfsTestContext) {
    assert!(File::create(ctx.mount_point.path().join("new-file")).is_err());
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
