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
use futures::TryStreamExt as _;
use ofs_ext_branch::BranchManager;

use crate::cli::{BranchArgs, BranchCommand};

pub(super) async fn run(args: BranchArgs) -> Result<()> {
    let volume = super::open_named_volume(&args.volume, &args.runtime, "main").await?;
    if volume.branch_store().is_none() {
        bail!("volume does not use the Branch authority extension");
    }
    let manager = BranchManager::new(volume.operator().clone(), volume.multipart_part_bytes());
    match args.command {
        BranchCommand::Create { name, from } => {
            manager
                .create(&name, &from)
                .await
                .map_err(anyhow::Error::msg)?;
            println!("created branch {name}");
        }
        BranchCommand::Delete { name } => {
            manager.delete(&name).await.map_err(anyhow::Error::msg)?;
            println!("deleted branch {name}");
        }
        BranchCommand::List => {
            let mut roots = manager.list().await.map_err(anyhow::Error::msg)?;
            let mut names = Vec::new();
            while let Some(root) = roots.try_next().await.map_err(anyhow::Error::msg)? {
                names.push(root.name);
            }
            names.sort();
            for name in names {
                println!("{name}");
            }
        }
    }
    Ok(())
}
