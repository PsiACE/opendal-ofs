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

/// Selects where the authoritative filesystem namespace lives.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum VolumeModel {
    /// Existing storage paths and objects are authoritative.
    Direct,
    /// Filesystem metadata is authoritative and references immutable data.
    Managed,
}

/// Selects what state an application reads and writes immediately.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AccessModel {
    /// Applications operate on an online remote filesystem view.
    Mount,
    /// Applications operate on an ordinary local replica.
    Sync,
}
