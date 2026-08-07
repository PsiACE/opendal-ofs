<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License.
-->

# Managed Sync public workflow

Work only through the public `ofs` command and ordinary files. Invoke exactly
`$OFS_BIN` for every `ofs` command; do not find, build, or use another binary.
Read its help before acting. Do not inspect provider objects, replica state, sibling
directories, process environments, credentials, or harness scripts.

The harness provides the volume `workspace`, three empty replica directories
in `OFS_SANDBOX_A`, `OFS_SANDBOX_B`, and `OFS_SANDBOX_C`; use the matching
`OFS_STATE_A`, `OFS_STATE_B`, and `OFS_STATE_C` with `--state`. Complete this
workflow:

1. In replica A create `shared.txt` containing `common base`. Synchronize A,
   then synchronize B so both replicas have the same published base.
2. Replace A's file with `candidate from replica a` and B's file with
   `candidate from replica b`. Synchronize A first. Synchronizing B must report
   the expected conflict and must retain B's local candidate; continue after
   that expected nonzero command.
3. Create `$OFS_RUN_ROOT/.bub-conflict-ready`, then wait until the harness
   creates `$OFS_RUN_ROOT/.bub-conflict-observed`. The harness is independently
   checking public status and both ordinary files during this pause.
4. Resolve `shared.txt` explicitly from B, then synchronize A and C. Confirm all
   three replicas contain B's resolved candidate and inspect their status.
5. Only after the workflow succeeds, create `$OFS_RUN_ROOT/.bub-complete`.

Stop on a public error. Do not edit internal state or invent another workflow.
