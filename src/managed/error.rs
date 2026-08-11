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

use crate::filesystem::{VolumeError, VolumeErrorKind};

pub(crate) fn invalid(action: &'static str, message: &'static str) -> VolumeError {
    error(VolumeErrorKind::Invalid, action, message)
}

pub(crate) fn corrupt(action: &'static str, message: &'static str) -> VolumeError {
    error(VolumeErrorKind::Corrupt, action, message)
}

pub(crate) fn unavailable(action: &'static str, message: &'static str) -> VolumeError {
    error(VolumeErrorKind::Unavailable, action, message)
}

fn error(kind: VolumeErrorKind, action: &'static str, message: &'static str) -> VolumeError {
    VolumeError::new(kind, format!("{action}: {message}"))
}
