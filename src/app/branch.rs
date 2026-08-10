// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use anyhow::{Context, Result};
use ofs::managed::extensions::branch::{BranchInfo, ForkPoint};

use crate::cli::{BranchArgs, BranchCommand};

use super::providers::{ManagedContext, open_managed_context};

pub(super) async fn branch_command(args: BranchArgs) -> Result<()> {
    let concurrency = args.runtime.transfer_concurrency;
    let ManagedContext {
        format,
        data,
        metadata,
    } = open_managed_context(
        &args.remote.storage,
        args.remote.metadata.as_ref(),
        None,
        concurrency,
    )
    .await?;
    let branches = metadata.branches(&format, data)?;
    match args.command {
        BranchCommand::List { json } => {
            let listed = branches.list(concurrency).await?;
            if json {
                let default = listed
                    .iter()
                    .find(|branch| branch.is_default)
                    .context("Managed branch registry has no default branch")?;
                let entries = listed.iter().map(branch_json).collect::<Vec<_>>();
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "default_branch": default.binding.name.as_str(),
                        "branches": entries,
                    }))?
                );
            } else {
                for branch in listed {
                    println!(
                        "{}{} {} change {}",
                        if branch.is_default { "* " } else { "  " },
                        branch.binding.name,
                        branch.binding.id,
                        branch.cursor.sequence(),
                    );
                }
            }
            Ok(())
        }
        BranchCommand::Show { branch, json } => {
            let branch = branches.get(&branch).await?;
            if json {
                println!("{}", serde_json::to_string(&branch_json(&branch))?);
            } else {
                println!(
                    "branch {:?} {} at change {}{}",
                    branch.binding.name.as_str(),
                    branch.binding.id,
                    branch.cursor.sequence(),
                    if branch.is_default { " (default)" } else { "" },
                );
            }
            Ok(())
        }
        BranchCommand::Create { branch, from, at } => {
            let point = at.map_or(ForkPoint::Head, ForkPoint::Sequence);
            let (created, source) = branches.fork(from, point, branch).await?;
            println!(
                "created branch {:?} {} from {:?} at change {}",
                created.binding.name.as_str(),
                created.binding.id,
                source.as_str(),
                created.cursor.sequence(),
            );
            Ok(())
        }
        BranchCommand::Delete { branch } => {
            branches.delete(&branch).await?;
            println!("deleted branch {:?}", branch.as_str());
            Ok(())
        }
    }
}

fn branch_json(branch: &BranchInfo) -> serde_json::Value {
    serde_json::json!({
        "name": branch.binding.name.as_str(),
        "id": branch.binding.id.to_string(),
        "sequence": branch.cursor.sequence(),
        "default": branch.is_default,
        "lifecycle": match branch.lifecycle {
            ofs::managed::extensions::branch::BranchLifecycle::Active => "active",
            ofs::managed::extensions::branch::BranchLifecycle::Sealed => "sealed",
        },
    })
}
