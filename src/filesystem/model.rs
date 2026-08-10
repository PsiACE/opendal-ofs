// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

//! Namespace authority models understood by this build.

use serde::{Deserialize, Serialize};

/// The namespace authority selected when a volume is created.
///
/// RFC 016 also defines Direct. This build accepts only Managed until the
/// Direct contract and its conformance coverage are implemented.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeModel {
    Managed,
}

impl VolumeModel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Managed => "managed",
        }
    }
}
