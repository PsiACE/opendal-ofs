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

# Managed Branch acceptance

`workflow.sh` is the user-visible `branch/v1` contract. The same script runs
against Object and D1 metadata. It invokes the `ofs` CLI and inspects ordinary
replica files, branch command output, and `ofs status`.

The harness provides a fresh `OFS_CASE_ROOT`, a built `OFS_BIN`, and a
credential-free `OFS_STORAGE_URL`. Object metadata needs no additional value.
D1 metadata also needs `OFS_METADATA_URL` and `OFS_D1_TOKEN`.

The repository harness runs both authorities with:

```text
cargo x managed-sync test branch object
cargo x managed-sync test branch d1
```

The workflow covers the default branch, current and historical fork,
fork at change zero, independent publication, checkpoint rotation, a large
namespace publication, deletion and name reuse, stale replica fencing, and
multi-root garbage collection. These are behaviors a user relies on regardless
of the physical metadata representation.

The workflow does not inspect object keys, D1 rows, replica-state JSON, private
call order, buffer sizes, or backend-specific errors. Request counts and O(1)
fork evidence belong in a separate non-blocking performance comparison.
