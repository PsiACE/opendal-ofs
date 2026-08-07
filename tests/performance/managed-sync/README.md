# Managed Sync release A/B

Run `cargo x managed-sync perf [OUTPUT_DIRECTORY]`. The harness compares the
archived implementation at `b262c3ae9f0c8147a3295072fc05e36adb1f9702`
with the current `HEAD`. Both binaries use the release profile, the same host,
one long-running local MinIO, and separate object roots. Samples run in the
fixed order baseline, candidate, candidate, baseline, baseline, candidate.

Each sample performs the same synthetic twelve-generation lifecycle. The
report records logical bytes, uploaded request bytes, stored bytes and objects,
request distributions, lifecycle median, and publication/catch-up p95. It
fails when candidate lifecycle regresses by more than 10%, either operation
p95 regresses by more than 15%, or a no-op uploads a data object.

Provider startup and fixture construction are outside measured phases. Bub is
not part of this benchmark; agent planning wall time is not a storage metric.
