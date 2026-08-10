<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Managed acceptance

The five Managed Sync acceptance scripts are independent user journeys. Each
starts with a fresh case root, invokes only the `ofs` CLI, and inspects ordinary
replica files plus `ofs status`. Object metadata and D1 metadata run the same
scripts; the fixture changes only the environment. `common.sh` contains only
their environment, fixture, and CLI-driving helpers.

The harness must provide a fresh `OFS_CASE_ROOT`, a built `OFS_BIN`, and a
credential-free `OFS_STORAGE_URL`. It also requires the standard BLAKE3
`b3sum` command for exact tree and state fingerprints. Credentials belong in
provider environment variables. Select the metadata authority with:

```text
OFS_METADATA_MODE=object

OFS_METADATA_MODE=d1
OFS_METADATA_URL=<credential-free D1 URL>
```

`OFS_SECRET_PROBES` may contain additional newline-separated secret values that
must not appear in JSON status output. The script deliberately leaves its case
directory intact on failure so the caller can inspect the user-visible state.

The scenarios divide the public contract by user journey:

- `admission` validates volume registration, client-local aliases, and
  built-in extension admission.
- `smoke` publishes and materializes representative files and validates the
  portable namespace contract.
- `reconcile` merges independent changes, applies replacements and moves, and
  retains conflicts until explicit resolution.
- `recovery` rebuilds a cold client, verifies no-op behavior, publishes from the
  recovered client, and checks structured status without credentials.
- `scale` catches a replica up after a long publication history.

They do not inspect object keys, metadata rows, state-file contents, private
call order, or implementation-specific errors.

`../managed-branch/workflow.sh` is the corresponding user-visible `branch/v1`
extension contract. It covers the default branch, current, historical, and
genesis forks, independent publication, deletion and name reuse, stale replica
fencing, historical branch materialization and a retained large-namespace
parent. It uses the same provider inputs and likewise inspects only CLI output
and ordinary replica files.

Run one scenario against either metadata authority with:

```text
tests/behavior/managed-sync/run.sh test reconcile object
tests/behavior/managed-sync/run.sh test reconcile d1
```

Run the complete scenario and provider matrix, the Managed Branch workflow, and
the staging recovery regression with:

```text
tests/behavior/managed-sync/run.sh test all
```

The staging regression interrupts a sync after public status reports a durable
pending operation, restarts it, and verifies convergence. It also restores an
earlier durable pending state after the publication has committed and history
has advanced. The regression deliberately treats staging paths and byte layout
as private implementation details.

The performance command uses MinIO's native audit webhook as its request and
transfer-byte authority. It attributes operations to cold restore,
publication, incremental catch-up, and no-op phases, inventories remote data
and metadata objects separately, records latency and replica-state size, and
checks logical tree equality. Those measurements are report evidence; acceptance
requires complete phase, audit, and inventory evidence, equal trees, and no
data upload during no-op sync. The local D1 query fixture records
native HTTP request count, request bytes, response bytes, and SQL statement
count without logging credentials or query parameters.

```text
tests/behavior/managed-sync/run.sh perf --baseline <commit-or-binary>
```
