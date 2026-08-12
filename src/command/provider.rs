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

use std::num::NonZeroUsize;

use anyhow::{Result, anyhow};
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer, TracingLayer};

pub(super) fn open_operator(
    storage: &str,
    concurrency: NonZeroUsize,
    tracing: bool,
) -> Result<Operator> {
    let concurrency = concurrency.get();
    Operator::from_uri(storage)
        .map(|operator| {
            let operator = operator
                .layer(
                    ConcurrentLimitLayer::new(concurrency).with_http_concurrent_limit(concurrency),
                )
                .layer(TimeoutLayer::new())
                .layer(RetryLayer::new().with_jitter());
            if tracing {
                operator.layer(TracingLayer::new())
            } else {
                operator
            }
        })
        .map_err(|error| {
            anyhow!(
                "cannot configure --storage ({}); check its scheme, endpoint, bucket, and root",
                error.kind()
            )
        })
}
