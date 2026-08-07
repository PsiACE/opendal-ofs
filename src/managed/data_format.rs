// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeSet;

use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};

use super::{ManagedError, ManagedErrorKind};

const FORMAT_KEY: &str = "data/v1/format.json";
const MAGIC: &str = "ofs-managed-data";
const MAJOR: u16 = 1;
const SUPPORTED_FEATURES: &[&str] = &["whole-file-v1"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestAlgorithm {
    Sha256,
}

/// Durable interpretation rules owned by the Managed data plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedDataFormat {
    digest_algorithm: DigestAlgorithm,
    required_reader_features: BTreeSet<String>,
    required_writer_features: BTreeSet<String>,
}

impl ManagedDataFormat {
    pub fn v1() -> Self {
        Self {
            digest_algorithm: DigestAlgorithm::Sha256,
            required_reader_features: BTreeSet::from(["whole-file-v1".to_owned()]),
            required_writer_features: BTreeSet::from(["whole-file-v1".to_owned()]),
        }
    }

    pub const fn digest_algorithm(&self) -> DigestAlgorithm {
        self.digest_algorithm
    }

    pub fn required_reader_features(&self) -> &BTreeSet<String> {
        &self.required_reader_features
    }

    pub fn required_writer_features(&self) -> &BTreeSet<String> {
        &self.required_writer_features
    }

    pub fn validate_for_read(&self) -> Result<(), ManagedError> {
        validate_features(&self.required_reader_features)
    }

    pub fn validate_for_write(&self) -> Result<(), ManagedError> {
        self.validate_for_read()?;
        validate_features(&self.required_writer_features)
    }

    /// Idempotently install and validate `data/v1/format.json`.
    pub async fn activate(&self, operator: &Operator) -> Result<Self, ManagedError> {
        self.validate_for_write()?;
        let capability = operator.info().full_capability();
        if !capability.read || !capability.write_with_if_not_exists {
            return Err(invalid(
                "activate Managed data",
                "data storage requires read and create-only write",
            ));
        }
        let encoded = self.encode()?;
        match operator
            .write_with(FORMAT_KEY, encoded)
            .if_not_exists(true)
            .await
        {
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                ) => {}
            Err(_) => return Err(unavailable("activate Managed data")),
        }
        let observed = Self::read(operator).await?;
        observed.validate_for_write()?;
        if observed != *self {
            return Err(ManagedError::new(
                ManagedErrorKind::Conflict,
                "activate Managed data",
                "data root uses another Managed data format",
            ));
        }
        Ok(observed)
    }

    pub async fn read(operator: &Operator) -> Result<Self, ManagedError> {
        let bytes = operator
            .read(FORMAT_KEY)
            .await
            .map_err(|_| unavailable("read Managed data format"))?;
        Self::decode(&bytes.to_bytes())
    }

    fn encode(&self) -> Result<Vec<u8>, ManagedError> {
        self.validate_for_write()?;
        serde_json::to_vec(&DataFormatWire::from(self))
            .map_err(|_| invalid("activate Managed data", "data format cannot be encoded"))
    }

    fn decode(bytes: &[u8]) -> Result<Self, ManagedError> {
        let wire: DataFormatWire = serde_json::from_slice(bytes)
            .map_err(|_| corrupt("read Managed data format", "data format is not valid JSON"))?;
        if wire.magic != MAGIC || wire.major != MAJOR {
            return Err(invalid(
                "read Managed data format",
                "data format major is unsupported",
            ));
        }
        let format = Self {
            digest_algorithm: wire.digest_algorithm.into(),
            required_reader_features: wire.required_reader_features,
            required_writer_features: wire.required_writer_features,
        };
        format.validate_for_read()?;
        Ok(format)
    }
}

impl Default for ManagedDataFormat {
    fn default() -> Self {
        Self::v1()
    }
}

fn validate_features(features: &BTreeSet<String>) -> Result<(), ManagedError> {
    if features
        .iter()
        .any(|feature| !SUPPORTED_FEATURES.contains(&feature.as_str()))
    {
        return Err(invalid(
            "activate Managed data",
            "data format requires an unsupported feature",
        ));
    }
    Ok(())
}

fn invalid(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Invalid, action, message)
}

fn corrupt(action: &'static str, message: &'static str) -> ManagedError {
    ManagedError::new(ManagedErrorKind::Corrupt, action, message)
}

fn unavailable(action: &'static str) -> ManagedError {
    ManagedError::new(
        ManagedErrorKind::Unavailable,
        action,
        "Managed data storage is unavailable",
    )
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DataFormatWire {
    magic: String,
    major: u16,
    digest_algorithm: DigestAlgorithmWire,
    required_reader_features: BTreeSet<String>,
    required_writer_features: BTreeSet<String>,
}

impl From<&ManagedDataFormat> for DataFormatWire {
    fn from(format: &ManagedDataFormat) -> Self {
        Self {
            magic: MAGIC.to_owned(),
            major: MAJOR,
            digest_algorithm: format.digest_algorithm.into(),
            required_reader_features: format.required_reader_features.clone(),
            required_writer_features: format.required_writer_features.clone(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum DigestAlgorithmWire {
    Sha256,
}

impl From<DigestAlgorithm> for DigestAlgorithmWire {
    fn from(value: DigestAlgorithm) -> Self {
        match value {
            DigestAlgorithm::Sha256 => Self::Sha256,
        }
    }
}

impl From<DigestAlgorithmWire> for DigestAlgorithm {
    fn from(value: DigestAlgorithmWire) -> Self {
        match value {
            DigestAlgorithmWire::Sha256 => Self::Sha256,
        }
    }
}
