# Managed Sync release A/B

Run the readable comparison entry point with an executable, branch, or commit:

```shell
scripts/managed-sync-compare.sh \
  --baseline b262c3ae9f0c8147a3295072fc05e36adb1f9702 \
  --output .local/evidence/managed-sync-ab
```

The candidate defaults to a fresh release build of the current working tree.
Pass `--candidate PATH_OR_REF` to compare two supplied binaries or refs.
`cargo x managed-sync perf [OUTPUT_DIRECTORY]` remains the short form using the
archived default baseline and current `HEAD`.

Pass `--pack` for a separate maintenance scenario that packs eligible blobs
before a new empty replica is reconstructed and the lagging replica catches up.
Use it only when both compared releases expose `ofs volume pack`; the default
scenario measures the authoritative data format without a disposable index.

Both binaries use the same host, one long-running local MinIO, and separate
object roots. Samples run in the fixed order baseline, candidate, candidate,
baseline, baseline, candidate.

Each sample performs the same synthetic twelve-generation lifecycle. The
report records request counts, request and response bytes, metadata/data/total
object counts and bytes, request distributions, lifecycle median, and
publication/catch-up p95. Every run writes a content-hashed logical manifest;
different final trees fail the comparison. The other gates reject more than 10
percent lifecycle regression, more than 15 percent publication or catch-up p95
regression, or any no-op data upload.

Start with `comparison.json` and `results.json`. Raw recomputable evidence is
in `requests.jsonl`, `requests.tsv`, `objects.tsv`, `samples.tsv`, and each run
directory.

Provider startup and fixture construction are outside measured phases. Bub is
not part of this benchmark; agent planning wall time is not a storage metric.
