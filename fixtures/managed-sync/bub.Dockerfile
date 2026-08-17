# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM rust:1.91-bookworm AS ofs-builder
ENV RUSTUP_TOOLCHAIN=1.91.1
WORKDIR /source
COPY Cargo.toml Cargo.lock LICENSE NOTICE README.md ./
COPY crates ./crates
COPY src ./src
COPY xtask/Cargo.toml ./xtask/Cargo.toml
COPY xtask/src/main.rs ./xtask/src/main.rs
RUN cargo install --locked --path . --root /opt/ofs

FROM python:3.13-slim-bookworm
ARG BUB_VERSION=0.4.0
ARG UV_VERSION=0.6.14
RUN python -m pip install --no-cache-dir "bub==${BUB_VERSION}" "uv==${UV_VERSION}"
COPY --from=ofs-builder /opt/ofs/bin/ofs /usr/local/bin/ofs

ENV BUB_HOME=/sync/sessions \
    HOME=/var/lib/bub
WORKDIR /workspace
CMD ["sleep", "infinity"]
