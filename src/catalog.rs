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

//! Credential-free local names for volumes.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::durable::{JsonFormat, install_json};
use crate::filesystem::{VolumeId, VolumeModel};

const SCHEMA_MAJOR: u16 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeDefinition {
    pub volume_id: VolumeId,
    pub model: VolumeModel,
    pub storage: Url,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Url>,
}

impl VolumeDefinition {
    pub fn direct(volume_id: VolumeId, storage: Url) -> Result<Self> {
        let definition = Self {
            volume_id,
            model: VolumeModel::Direct,
            storage,
            metadata: None,
        };
        definition.validate()?;
        Ok(definition)
    }
    pub fn managed(volume_id: VolumeId, storage: Url, metadata: Option<Url>) -> Result<Self> {
        let definition = Self {
            volume_id,
            model: VolumeModel::Managed,
            storage,
            metadata,
        };
        definition.validate()?;
        Ok(definition)
    }

    fn validate(&self) -> Result<()> {
        require_credential_free("storage", &self.storage)?;
        if let Some(metadata) = &self.metadata {
            require_credential_free("metadata", metadata)?;
        }
        if self.model == VolumeModel::Direct && self.metadata.is_some() {
            bail!("Direct volume contains Managed-only catalog settings");
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Catalog {
    path: PathBuf,
    volumes: BTreeMap<String, VolumeDefinition>,
}

impl Catalog {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(Self {
                    path,
                    volumes: BTreeMap::new(),
                });
            }
            Err(error) => return Err(error).context("read volume catalog"),
        };
        let stored: StoredCatalog =
            serde_json::from_slice(&bytes).context("parse volume catalog JSON")?;
        if stored.schema_major != SCHEMA_MAJOR {
            bail!(
                "unsupported volume catalog schema major {}",
                stored.schema_major
            );
        }
        let mut volumes: BTreeMap<String, VolumeDefinition> = BTreeMap::new();
        for (alias, definition) in stored.volumes {
            if alias.is_empty() {
                bail!("volume alias is empty");
            }
            definition.validate()?;
            if let Some((existing, _)) = volumes
                .iter()
                .find(|(_, current)| current.volume_id == definition.volume_id)
            {
                bail!(
                    "volume aliases {existing:?} and {alias:?} refer to the same volume identity"
                );
            }
            volumes.insert(alias, definition);
        }
        Ok(Self { path, volumes })
    }

    pub fn get(&self, alias: &str) -> Option<&VolumeDefinition> {
        self.volumes.get(alias)
    }

    pub fn find_by_id(&self, volume_id: VolumeId) -> Option<(&str, &VolumeDefinition)> {
        self.volumes
            .iter()
            .find(|(_, definition)| definition.volume_id == volume_id)
            .map(|(alias, definition)| (alias.as_str(), definition))
    }

    /// Returns true when a new local binding was added and false when it already exists.
    pub fn register(&mut self, alias: &str, definition: VolumeDefinition) -> Result<bool> {
        if alias.is_empty() {
            bail!("volume alias is empty");
        }
        definition.validate()?;
        match self.volumes.get(alias) {
            Some(existing) if existing == &definition => Ok(false),
            Some(_) => bail!("volume alias {alias:?} conflicts with its existing configuration"),
            None => {
                if let Some((existing, _)) = self.find_by_id(definition.volume_id) {
                    bail!("this volume identity is already registered as local alias {existing:?}");
                }
                self.volumes.insert(alias.into(), definition);
                Ok(true)
            }
        }
    }

    pub fn save(&self) -> Result<()> {
        let stored = StoredCatalog {
            schema_major: SCHEMA_MAJOR,
            volumes: self.volumes.clone(),
        };
        install_json(&self.path, "catalog", &stored, JsonFormat::Pretty)
            .context("write volume catalog")
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCatalog {
    schema_major: u16,
    volumes: BTreeMap<String, VolumeDefinition>,
}

fn require_credential_free(kind: &str, url: &Url) -> Result<()> {
    let has_query_secret = url.query_pairs().any(|(key, _)| {
        let key = key.to_ascii_lowercase();
        key.contains("secret")
            || key.contains("password")
            || key.contains("credential")
            || key == "token"
            || key.ends_with("_token")
            || key == "access_key"
            || key == "access_key_id"
    });
    if !url.username().is_empty() || url.password().is_some() || has_query_secret {
        bail!("{kind} URL must not contain credentials");
    }
    Ok(())
}
