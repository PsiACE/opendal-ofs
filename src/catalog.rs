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
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::filesystem::{VolumeId, VolumeModel};
use crate::managed::FileLayoutPolicy;

const SCHEMA_MAJOR: u16 = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeDefinition {
    pub volume_id: VolumeId,
    pub storage: Url,
    pub settings: VolumeSettings,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VolumeSettings {
    Direct,
    Managed(ManagedVolumeSettings),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedVolumeSettings {
    pub metadata: Option<Url>,
    pub file_layout: FileLayoutPolicy,
}

impl VolumeDefinition {
    pub fn direct(volume_id: VolumeId, storage: Url) -> Result<Self> {
        require_credential_free("storage", &storage)?;
        Ok(Self {
            volume_id,
            storage,
            settings: VolumeSettings::Direct,
        })
    }
    pub fn managed(
        volume_id: VolumeId,
        storage: Url,
        metadata: Option<Url>,
        file_layout: FileLayoutPolicy,
    ) -> Result<Self> {
        require_credential_free("storage", &storage)?;
        if let Some(metadata) = &metadata {
            require_credential_free("metadata", metadata)?;
        }
        Ok(Self {
            volume_id,
            storage,
            settings: VolumeSettings::Managed(ManagedVolumeSettings {
                metadata,
                file_layout,
            }),
        })
    }

    pub fn managed_settings(&self) -> Option<&ManagedVolumeSettings> {
        match &self.settings {
            VolumeSettings::Direct => None,
            VolumeSettings::Managed(settings) => Some(settings),
        }
    }

    pub const fn model(&self) -> VolumeModel {
        match &self.settings {
            VolumeSettings::Direct => VolumeModel::Direct,
            VolumeSettings::Managed(_) => VolumeModel::Managed,
        }
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
        let mut volumes = BTreeMap::new();
        for (alias, stored) in stored.volumes {
            if alias.is_empty() {
                bail!("volume alias is empty");
            }
            volumes.insert(alias, stored.try_into()?);
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

    /// Returns true when a new binding was added and false for an idempotent reopen.
    pub fn create(&mut self, alias: &str, definition: VolumeDefinition) -> Result<bool> {
        if alias.is_empty() {
            bail!("volume alias is empty");
        }
        match self.volumes.get(alias) {
            Some(existing) if existing == &definition => Ok(false),
            Some(_) => bail!("volume alias {alias:?} conflicts with its existing configuration"),
            None => {
                self.volumes.insert(alias.into(), definition);
                Ok(true)
            }
        }
    }

    pub fn save(&self) -> Result<()> {
        let stored = StoredCatalog::from(self);
        let mut bytes = serde_json::to_vec_pretty(&stored)?;
        bytes.push(b'\n');

        let parent = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent).context("create volume catalog directory")?;
        let temporary = self
            .path
            .with_extension(format!("catalog.{}.tmp", std::process::id()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .context("create temporary volume catalog")?;
        let result = (|| -> Result<()> {
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &self.path)?;
            #[cfg(unix)]
            std::fs::File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.context("write volume catalog")
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCatalog {
    schema_major: u16,
    volumes: BTreeMap<String, StoredVolume>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredVolume {
    volume_id: [u8; 16],
    model: String,
    storage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_layout: Option<FileLayoutPolicy>,
}

impl From<&Catalog> for StoredCatalog {
    fn from(catalog: &Catalog) -> Self {
        let volumes = catalog
            .volumes
            .iter()
            .map(|(alias, volume)| (alias.clone(), StoredVolume::from(volume)))
            .collect();
        Self {
            schema_major: SCHEMA_MAJOR,
            volumes,
        }
    }
}

impl From<&VolumeDefinition> for StoredVolume {
    fn from(volume: &VolumeDefinition) -> Self {
        let (metadata, file_layout) = match &volume.settings {
            VolumeSettings::Direct => (None, None),
            VolumeSettings::Managed(settings) => (
                settings.metadata.as_ref().map(ToString::to_string),
                Some(settings.file_layout),
            ),
        };
        Self {
            volume_id: *volume.volume_id.as_bytes(),
            model: match volume.model() {
                VolumeModel::Direct => "direct",
                VolumeModel::Managed => "managed",
            }
            .into(),
            storage: volume.storage.to_string(),
            metadata,
            file_layout,
        }
    }
}

impl TryFrom<StoredVolume> for VolumeDefinition {
    type Error = anyhow::Error;

    fn try_from(volume: StoredVolume) -> Result<Self> {
        let volume_id = VolumeId::from_bytes(volume.volume_id);
        let model = match volume.model.as_str() {
            "direct" => VolumeModel::Direct,
            "managed" => VolumeModel::Managed,
            value => bail!("unknown volume model {value:?}"),
        };
        let storage = Url::parse(&volume.storage).context("invalid storage URL in catalog")?;
        let metadata = volume
            .metadata
            .map(|value| Url::parse(&value).context("invalid metadata URL in catalog"))
            .transpose()?;
        match model {
            VolumeModel::Direct => {
                if metadata.is_some() || volume.file_layout.is_some() {
                    bail!("Direct volume contains Managed-only catalog settings");
                }
                Self::direct(volume_id, storage)
            }
            VolumeModel::Managed => {
                let file_layout = volume
                    .file_layout
                    .context("Managed volume is missing its file layout policy")?;
                Self::managed(volume_id, storage, metadata, file_layout)
            }
        }
    }
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
