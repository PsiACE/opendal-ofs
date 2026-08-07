#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

: "${OFS_BIN:?}" "${OFS_RUN_ROOT:?}" "${OFS_STORAGE_URL:?}"
api_key=${BUB_API_KEY:-${OPENROUTER_API_KEY:-}}
: "${api_key:?set BUB_API_KEY or OPENROUTER_API_KEY}"

suite=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
config="$OFS_RUN_ROOT/config.json"
a="$OFS_RUN_ROOT/replica-a"
b="$OFS_RUN_ROOT/replica-b"
c="$OFS_RUN_ROOT/replica-c"
state_a="$OFS_RUN_ROOT/state-a.json"
state_b="$OFS_RUN_ROOT/state-b.json"
state_c="$OFS_RUN_ROOT/state-c.json"
mkdir -p "$a" "$b" "$c" "$OFS_RUN_ROOT/bub-home"

OFS_CONFIG="$config" "$OFS_BIN" volume create workspace --model managed \
  --storage "$OFS_STORAGE_URL"

task=$(<"$suite/task.md")
rm -f "$OFS_RUN_ROOT/.bub-conflict-ready" \
  "$OFS_RUN_ROOT/.bub-conflict-observed" "$OFS_RUN_ROOT/.bub-complete"
env \
  "PATH=$suite/bub-bin:$PATH" \
  "BUB_HOME=$OFS_RUN_ROOT/bub-home" \
  "BUB_API_KEY=$api_key" \
  "BUB_OPENROUTER_API_KEY=${BUB_OPENROUTER_API_KEY:-$api_key}" \
  "BUB_MODEL=${BUB_MODEL:-openrouter:qwen/qwen3-coder-next}" \
  "BUB_MAX_STEPS=${BUB_MAX_STEPS:-80}" \
  "OFS_BIN=$suite/bub-bin/ofs" "OFS_CONFIG=$config" "OFS_VOLUME=workspace" \
  "OFS_RUN_ROOT=$OFS_RUN_ROOT" "OFS_SANDBOX_A=$a" "OFS_SANDBOX_B=$b" \
  "OFS_SANDBOX_C=$c" "OFS_STATE_A=$state_a" "OFS_STATE_B=$state_b" \
  "OFS_STATE_C=$state_c" \
  timeout --signal=TERM --kill-after=15s "${BUB_TIMEOUT_SECONDS:-840}s" \
  "${BUB_BIN:-bub}" --workspace "$OFS_RUN_ROOT" run --session-id workspace "$task" \
  >"$OFS_RUN_ROOT/bub.log" 2>&1 &
bub_pid=$!
trap 'kill "$bub_pid" >/dev/null 2>&1 || true' EXIT

deadline=$((SECONDS + ${BUB_TIMEOUT_SECONDS:-840}))
while [[ ! -f $OFS_RUN_ROOT/.bub-conflict-ready ]]; do
  kill -0 "$bub_pid" 2>/dev/null || { wait "$bub_pid"; exit 1; }
  ((SECONDS < deadline)) || { printf 'Bub did not reach the conflict checkpoint\n' >&2; exit 124; }
  sleep 1
done
OFS_CONFIG="$config" "$OFS_BIN" status "$b" --state "$state_b" --json \
  >"$OFS_RUN_ROOT/status-conflict.json"
grep -Fxq 'candidate from replica a' "$a/shared.txt"
grep -Fxq 'candidate from replica b' "$b/shared.txt"
python3 - "$OFS_RUN_ROOT/status-conflict.json" <<'PY'
import json, pathlib, sys
status = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert status["pending"] is False and status["conflicts"] == 1
PY
touch "$OFS_RUN_ROOT/.bub-conflict-observed"
while [[ ! -f $OFS_RUN_ROOT/.bub-complete ]]; do
  kill -0 "$bub_pid" 2>/dev/null || { wait "$bub_pid"; exit 1; }
  ((SECONDS < deadline)) || { printf 'Bub did not complete the workflow\n' >&2; exit 124; }
  sleep 1
done
kill "$bub_pid" >/dev/null 2>&1 || true
wait "$bub_pid" 2>/dev/null || true
trap - EXIT

diff -ru "$a" "$b"
diff -ru "$b" "$c"
grep -Fxq 'candidate from replica b' "$c/shared.txt"

for replica in a b c; do
  directory=${!replica}
  state_name="state_$replica"
  OFS_CONFIG="$config" "$OFS_BIN" status "$directory" --state "${!state_name}" --json \
    >"$OFS_RUN_ROOT/status-$replica.json"
done
python3 - "$OFS_RUN_ROOT" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
statuses = [json.loads((root / f"status-{name}.json").read_text()) for name in "abc"]
assert all(item["volume_model"] == "managed" for item in statuses)
assert all(item["access_model"] == "sync" for item in statuses)
assert all(item["pending"] is False and item["conflicts"] == 0 for item in statuses)
generations = {item["common_sequence"] for item in statuses}
assert len(generations) == 1 and next(iter(generations)) > 0
print(f"Bub Managed Sync oracle passed at generation {next(iter(generations))}.")
PY
