# Managed Sync release A/B

Run the comparison with an executable, branch, or commit:

```shell
python3 scripts/managed_sync_perf.py \
  --baseline managed-sync-layers \
  --profile agent-home \
  --rounds 20 \
  .local/evidence/managed-sync-ab
```

The candidate defaults to a fresh release build of the current working tree.
Pass `--candidate PATH_OR_REF` to compare two supplied binaries or refs.
`cargo x managed-sync perf [OUTPUT_DIRECTORY]` invokes the same entry point.

Both binaries use the same host, one long-running local MinIO, and separate
object roots. Samples run in the fixed order baseline, candidate, candidate,
baseline, baseline, candidate. Run the command once with `--rounds 20` and
again with `--rounds 50` when evaluating checkpoint behavior.

The `standard` profile is a small lifecycle smoke test. The `agent-home`
profile models the measured file counts, directory counts, and size buckets of
`.agents`, `.bub`, and `.codex`. It keeps the measured 12,480 files and 3,832
directories, but caps representative large files so one fixture is about 149
MB. Three long-running containers use separate catalogs, replica states,
directories, and local volume aliases for the same remote `VolumeId`. They take
turns catching up, editing all three domains, adding files, deleting files, and
publishing. A fourth container registers another alias and
performs a cold restore.

The report records request counts, request and response bytes,
metadata/data/total object counts and bytes, request distributions, lifecycle
median, and publication/catch-up p95. Request rows distinguish full and range
GETs and classify authoritative segments, metadata, and control objects. Every
run writes a content-hashed logical manifest. Different final trees fail the
comparison. The other gates reject more than 10 percent
lifecycle regression, more than 15 percent publication or catch-up p95
regression, more than 10 percent growth in total requests, catch-up segment
GETs, transferred bytes, or stored bytes, or any no-op data upload.

`results.json` is the canonical report. Raw recomputable evidence remains in
`requests.jsonl`, `commands.tsv`, and each run directory; redundant TSV and
summary projections are removed after a passing run.

Provider startup and fixture construction are outside measured phases. Agent
planning wall time is not a storage metric.
