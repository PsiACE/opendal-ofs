// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Portable identities used to bind an access model to one branch authority.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{BranchId, VolumeId};

const MAX_BRANCH_NAME_BYTES: usize = 63;

/// Portable, case-sensitive name of a Managed branch.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BranchName(String);

impl BranchName {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidBranchName> {
        let value = value.into();
        let mut bytes = value.bytes();
        let first = bytes.next().ok_or(InvalidBranchName)?;
        if value.len() > MAX_BRANCH_NAME_BYTES
            || !first.is_ascii_alphanumeric()
            || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(InvalidBranchName);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BranchName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for BranchName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for BranchName {
    type Err = InvalidBranchName;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for BranchName {
    type Error = InvalidBranchName;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for BranchName {
    type Error = InvalidBranchName;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl Serialize for BranchName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for BranchName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

/// A branch name was outside the portable branch-name grammar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidBranchName;

impl fmt::Display for InvalidBranchName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "branch name must start with an ASCII letter or digit and contain only ASCII letters, digits, '.', '_', or '-' (63 bytes maximum)",
        )
    }
}

impl Error for InvalidBranchName {}

/// Stable binding to one branch incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchBinding {
    pub name: BranchName,
    pub id: BranchId,
}

/// Stable identity of the namespace authority used by one access instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityIdentity {
    pub volume: VolumeId,
    pub branch: Option<BranchBinding>,
}

impl AuthorityIdentity {
    pub const fn base(volume: VolumeId) -> Self {
        Self {
            volume,
            branch: None,
        }
    }

    pub const fn branch(volume: VolumeId, branch: BranchBinding) -> Self {
        Self {
            volume,
            branch: Some(branch),
        }
    }
}
