// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeMap;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::model::VolumeId;

const FORMAT: &str = "ofs-volume-catalog";
const VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageLocator {
    pub(crate) scheme: String,
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) path: String,
    pub(crate) options: BTreeMap<String, Vec<String>>,
}

impl StorageLocator {
    pub(crate) fn parse(url: &Url) -> Result<Self> {
        let mut options = BTreeMap::<String, Vec<String>>::new();
        for (key, value) in url.query_pairs() {
            if !credential_key(&key) {
                options
                    .entry(key.into_owned())
                    .or_default()
                    .push(value.into_owned());
            }
        }
        Ok(Self {
            scheme: url.scheme().to_owned(),
            host: url.host_str().map(str::to_owned),
            port: url.port(),
            path: url.path().to_owned(),
            options,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.scheme.is_empty() {
            bail!("storage locator scheme is empty");
        }
        if self.options.keys().any(|key| credential_key(key)) {
            bail!("storage locator contains credentials");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MetadataConfig {
    ColocatedObject,
    External { locator: StorageLocator },
}

impl MetadataConfig {
    pub(crate) fn external(locator: StorageLocator) -> Self {
        Self::External { locator }
    }

    pub(crate) fn external_locator(&self) -> Option<&StorageLocator> {
        match self {
            Self::External { locator } => Some(locator),
            Self::ColocatedObject => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "model", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum VolumeDefinition {
    Direct {
        id: VolumeId,
        storage: StorageLocator,
    },
    Managed {
        id: VolumeId,
        storage: StorageLocator,
        metadata: MetadataConfig,
    },
}

impl VolumeDefinition {
    pub(crate) fn direct(id: VolumeId, storage: StorageLocator) -> Self {
        Self::Direct { id, storage }
    }

    pub(crate) fn managed(id: VolumeId, storage: StorageLocator, metadata: MetadataConfig) -> Self {
        Self::Managed {
            id,
            storage,
            metadata,
        }
    }

    pub(crate) fn id(&self) -> &VolumeId {
        match self {
            Self::Direct { id, .. } | Self::Managed { id, .. } => id,
        }
    }

    pub(crate) fn storage(&self) -> &StorageLocator {
        match self {
            Self::Direct { storage, .. } | Self::Managed { storage, .. } => storage,
        }
    }

    pub(crate) fn metadata(&self) -> Option<&MetadataConfig> {
        match self {
            Self::Managed { metadata, .. } => Some(metadata),
            Self::Direct { .. } => None,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.id().as_str().is_empty() {
            bail!("volume id is empty");
        }
        self.storage().validate()?;
        if let Some(MetadataConfig::External { locator }) = self.metadata() {
            locator.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SyncDefaults {
    transfer_concurrency: usize,
}

impl Default for SyncDefaults {
    fn default() -> Self {
        Self {
            transfer_concurrency: 4,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Catalog {
    format: String,
    format_version: u32,
    sync: SyncDefaults,
    volumes: BTreeMap<String, VolumeDefinition>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            format: FORMAT.to_owned(),
            format_version: VERSION,
            sync: SyncDefaults::default(),
            volumes: BTreeMap::new(),
        }
    }
}

impl Catalog {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => {
                return Err(error).with_context(|| format!("read catalog {}", path.display()));
            }
        };
        let catalog: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse catalog {}", path.display()))?;
        catalog.validate()?;
        Ok(catalog)
    }

    pub(crate) fn get(&self, name: &str) -> Result<&VolumeDefinition> {
        self.volumes
            .get(name)
            .with_context(|| format!("volume {name:?} is not defined"))
    }

    pub(crate) fn get_by_id(&self, id: &VolumeId) -> Result<(&str, &VolumeDefinition)> {
        self.volumes
            .iter()
            .find(|(_, value)| value.id() == id)
            .map(|(name, value)| (name.as_str(), value))
            .with_context(|| format!("volume id {id:?} is not defined"))
    }

    pub(crate) fn insert(&mut self, name: String, value: VolumeDefinition) -> Result<()> {
        if name.is_empty() || self.volumes.contains_key(&name) {
            bail!("volume {name:?} already exists or has an empty name");
        }
        if self.volumes.values().any(|item| item.id() == value.id()) {
            bail!("volume id {:?} already exists", value.id());
        }
        value.validate()?;
        self.volumes.insert(name, value);
        Ok(())
    }

    pub(crate) fn transfer_concurrency(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.sync.transfer_concurrency).expect("validated non-zero concurrency")
    }

    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create catalog directory {}", parent.display()))?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        set_private(temporary.path())?;
        serde_json::to_writer_pretty(temporary.as_file_mut(), self)?;
        temporary.write_all(b"\n")?;
        temporary.as_file_mut().sync_all()?;
        temporary.persist(path).map_err(|error| error.error)?;
        set_private(path)
    }

    fn validate(&self) -> Result<()> {
        if self.format != FORMAT || self.format_version != VERSION {
            bail!("unsupported volume catalog format or version");
        }
        if self.sync.transfer_concurrency == 0 {
            bail!("sync transfer concurrency must be greater than zero");
        }
        for (name, value) in &self.volumes {
            if name.is_empty() {
                bail!("volume name is empty");
            }
            value
                .validate()
                .with_context(|| format!("validate volume {name:?}"))?;
        }
        Ok(())
    }
}

pub(crate) fn catalog_path(requested: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = requested {
        return Ok(path);
    }
    if let Some(path) = std::env::var_os("OFS_CONFIG").filter(|value| !value.is_empty()) {
        return Ok(path.into());
    }
    if let Some(path) = std::env::var_os("XDG_CONFIG_HOME").filter(|value| !value.is_empty()) {
        let path = PathBuf::from(path);
        if path.is_absolute() {
            return Ok(path.join("ofs/volumes.json"));
        }
        bail!("XDG_CONFIG_HOME must be absolute");
    }
    if let Some(path) = std::env::var_os("HOME").filter(|value| !value.is_empty()) {
        let path = PathBuf::from(path);
        if path.is_absolute() {
            return Ok(path.join(".config/ofs/volumes.json"));
        }
        bail!("HOME must be absolute");
    }
    bail!("cannot determine catalog path; pass --config or set OFS_CONFIG")
}

fn credential_key(key: &str) -> bool {
    let key = key
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    key.contains("secret")
        || key.contains("credential")
        || key.ends_with("token")
        || key.ends_with("password")
        || key.ends_with("accesskey")
        || key.ends_with("accesskeyid")
        || key.ends_with("accountkey")
        || key.ends_with("apikey")
        || key.ends_with("privatekey")
        || key == "authorization"
        || key == "signature"
        || key == "sig"
        || key == "sas"
        || key == "username"
}

#[cfg(unix)]
fn set_private(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private(_path: &Path) -> Result<()> {
    Ok(())
}
