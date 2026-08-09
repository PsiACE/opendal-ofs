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

`workflow.sh` is the public Managed Sync contract. It invokes only the `ofs`
CLI and inspects ordinary replica files plus `ofs status`. Object metadata and
D1 metadata run the same workflow; the fixture changes only the environment.

The harness must provide a fresh `OFS_CASE_ROOT`, a built `OFS_BIN`, and a
credential-free `OFS_STORAGE_URL`. Credentials belong in provider environment
variables. Select the metadata authority with:

```text
OFS_METADATA_MODE=object

OFS_METADATA_MODE=d1
OFS_METADATA_URL=<credential-free D1 URL>
```

`OFS_SECRET_PROBES` may contain additional newline-separated secret values that
must not appear in JSON status output. The script deliberately leaves its case
directory intact on failure so the caller can inspect the user-visible state.

The workflow accepts registration of one remote volume under different
client-local aliases, explicit publication from those clients, empty and cold
materialization, disjoint merge, retained conflict candidates, explicit
resolution, no-op sync, structured status, and absence of credentials. It does
not inspect object keys, metadata rows, state-file contents, private call order,
or implementation-specific errors.

The same workflow also checks the shared command surface: a Direct volume can
be created and reopened by name, `mount` and `sync` are separate access
commands, and unavailable Direct Sync or Managed Mount combinations fail before
changing local or remote state. It does not start a FUSE session.

`../managed-branch/workflow.sh` is the corresponding user-visible `branch/v1`
contract. It covers the default branch, current, historical, and genesis forks,
independent publication, deletion and name reuse, stale replica fencing,
multi-root garbage collection, and a retained large-namespace parent. It uses
the same provider inputs and likewise inspects only CLI output and ordinary
replica files.

Run the complete provider matrix and the staging/cache-loss regression with:

```text
cargo x managed-sync test all
```
