# Managed Sync release A/B

Run the comparison with an executable, branch, or commit:

```shell
tests/performance/managed-sync/run.sh \
  --baseline managed-sync-layers \
  --rounds 20 \
  .local/evidence/managed-sync-ab
```

A baseline is required through `--baseline`, `OFS_PERF_BASELINE`, or
`OFS_PERF_BASELINE_BIN`. The candidate defaults to a fresh release build of the
current `HEAD` commit. Pass `--candidate PATH_OR_REF` to compare two supplied
binaries or refs.

Both binaries use the same host, one long-running local MinIO, and separate
object roots. Samples run in the fixed order baseline, candidate, candidate,
baseline, baseline, candidate. Run the command once with `--rounds 20` and
again with `--rounds 50` when evaluating checkpoint behavior.

The workload exercises publication, cold restore, incremental catch-up, and a
no-op over deterministic large and small files. `--rounds` controls the number
of publish/restore generations.

The binaries connect directly to MinIO. MinIO sends one native audit event per
API operation to an out-of-band webhook; the harness removes authentication
headers before retaining the JSON Lines evidence. The report records request
counts, request and response bytes, metadata/data/total object counts and
bytes, request distributions, replica-state size, lifecycle median, and
publication, cold-restore, and incremental-catch-up p95. Audit rows
distinguish full and range `GetObject` operations and classify authoritative
segments, metadata, and control objects. Initialization is reported separately
from product request and transfer evidence. The workload and the harness inventory
use separate MinIO identities, so administrative requests cannot enter product
metrics through a User-Agent heuristic. Every run writes a content-hashed
logical manifest. Acceptance requires equal final trees, complete phase, native
audit, and object-inventory evidence, and no data upload during no-op sync.
Latency, request, transfer, storage, metadata, and replica-state measurements
remain visible in the report for engineering comparison; machine acceptance does
not assign arbitrary relative thresholds to them.

`results.json` is the canonical report. Raw recomputable evidence remains in
`audit.jsonl`, `commands.tsv`, `samples.tsv`, `inputs.tsv`, and each run's
command output, logical manifest, and object inventory. Replica trees,
catalogs, and states are removed. A final native S3 audit event acts as the
drain barrier before analysis.

Provider startup and fixture construction are outside measured phases.
