// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

mod managed_sync;

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("managed-sync") => managed_sync::run(arguments),
        Some("-h" | "--help") => {
            println!("Usage: cargo x managed-sync <doctor|up|down>");
            Ok(())
        }
        Some(command) => Err(format!("unknown xtask command {command:?}")),
        None => Err("missing xtask command; expected managed-sync".into()),
    }
}
