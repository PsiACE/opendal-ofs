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

use anyhow::{Result, bail};
use ofs::managed::ManagedMetadata;
use ofs_extras::{BRANCH_EXTENSION_ID, BranchManager};

use crate::cli::{BranchArgs, BranchCommand};

use super::provider::open_operator;

pub(super) async fn run(args: BranchArgs) -> Result<()> {
    let operator = open_operator(
        &args.storage,
        args.resources.transfer_concurrency,
        args.resources.trace,
    )?;
    let metadata = ManagedMetadata::new(
        operator.clone(),
        args.resources.transfer_concurrency,
        args.resources.work_memory_mib,
    )?;
    if metadata
        .authority_extension()
        .await?
        .is_none_or(|extension| extension.id != BRANCH_EXTENSION_ID)
    {
        bail!("volume does not use the branch extension");
    }
    let manager = BranchManager::new(operator);
    match args.command {
        BranchCommand::Create { name, from } => {
            manager.create(&name, &from).await?;
            println!("created branch {name} from {from}");
        }
        BranchCommand::Delete { name } => {
            manager.delete(&name).await?;
            println!("deleted branch {name}");
        }
        BranchCommand::List => {
            let mut branches = manager.list().await?;
            while let Some(branch) = branches.next().await? {
                println!("{}", branch.name);
            }
        }
    }
    Ok(())
}
