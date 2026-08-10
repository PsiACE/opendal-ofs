#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

: "${OFS_BIN:?}" "${OFS_CASE_ROOT:?}" "${OFS_STORAGE_URL:?}" "${OFS_REQUEST_LOG:?}"

fail() {
  printf 'managed-sync staging regression: %s\n' "$*" >&2
  exit 1
}

pause_when_pending() {
  python3 - "$1" "$2" <<'PY'
import json
import os
import pathlib
import signal
import sys
import time

state = pathlib.Path(sys.argv[1])
pid = int(sys.argv[2])
deadline = time.monotonic() + 10
while time.monotonic() < deadline:
    try:
        if json.loads(state.read_text()).get("pending") is not None:
            os.kill(pid, signal.SIGSTOP)
            raise SystemExit(0)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        break
raise SystemExit(1)
PY
}

catalog="$OFS_CASE_ROOT/catalog.json"
peer_catalog="$OFS_CASE_ROOT/peer-catalog.json"
replica="$OFS_CASE_ROOT/replica"
peer="$OFS_CASE_ROOT/peer"
state="$OFS_CASE_ROOT/state/replica.json"
peer_state="$OFS_CASE_ROOT/state/peer.json"
cold="$OFS_CASE_ROOT/cold"
cold_state="$OFS_CASE_ROOT/state/cold.json"
stable_bytes=$((64 * 1024 * 1024))
mkdir -p "$replica" "$peer" "$(dirname "$state")" "$cold"

OFS_CONFIG="$catalog" "$OFS_BIN" volume create staging \
  --model managed --storage "$OFS_STORAGE_URL" >/dev/null
OFS_CONFIG="$peer_catalog" "$OFS_BIN" volume create staging \
  --model managed --storage "$OFS_STORAGE_URL" >/dev/null
head -c "$stable_bytes" /dev/zero >"$replica/stable.bin"
printf '%s\n' 'common conflict content' >"$replica/conflict.txt"
OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$replica" --state "$state" >/dev/null
OFS_CONFIG="$peer_catalog" "$OFS_BIN" sync staging "$peer" --state "$peer_state" >/dev/null

printf '%s\n' 'remote conflict candidate' >"$replica/conflict.txt"
OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$replica" --state "$state" >/dev/null
printf '%s\n' 'resolved local candidate' >"$peer/conflict.txt"
mkdir "$peer/changed"
for index in $(seq -w 1 128); do
  {
    printf 'unique deferred candidate %s\n' "$index"
    head -c 65536 /dev/zero
  } >"$peer/changed/$index.bin"
done
changed_bytes=$(find "$peer/changed" -type f -printf '%s\n' | \
  awk '{ total += $1 } END { print total + 0 }')
candidate_bytes=$((changed_bytes + $(stat -c %s "$peer/conflict.txt")))

conflict_started_ns=$(date +%s%N)
if OFS_CONFIG="$peer_catalog" "$OFS_BIN" sync staging "$peer" --state "$peer_state" \
  >/dev/null 2>&1; then
  fail 'same-path conflict succeeded without explicit resolution'
fi
conflict_ended_ns=$(date +%s%N)
conflict_status=$(OFS_CONFIG="$peer_catalog" "$OFS_BIN" status --state "$peer_state" --json)
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*1' <<<"$conflict_status" || \
  fail 'unresolved conflict was not retained'

resolve_started_ns=$(date +%s%N)
OFS_CONFIG="$peer_catalog" "$OFS_BIN" sync staging "$peer" --state "$peer_state" \
  --resolve conflict.txt >/dev/null &
sync_pid=$!
pause_when_pending "$peer_state" "$sync_pid" || \
  fail 'could not pause resolved sync before deferred finalization'
resolve_paused_ns=$(date +%s%N)

staging=$(python3 - "$peer_state" <<'PY'
import json
import pathlib
import sys

state = pathlib.Path(sys.argv[1]).resolve()
value = pathlib.Path(json.loads(state.read_text())["pending"]["staging"])
if not value.is_absolute():
    value = state.parent / value
value = value.resolve()
if value.parent != state.parent:
    raise SystemExit("pending staging is outside the state directory")
print(value)
PY
)
[[ -d $staging ]] || fail 'pending staging directory is missing'
staged_bytes=$(find "$staging" -type f -printf '%s\n' | awk '{ total += $1 } END { print total + 0 }')
((staged_bytes >= candidate_bytes)) || \
  fail "resolved update staged $staged_bytes bytes; expected at least $candidate_bytes"
((staged_bytes <= candidate_bytes + 4 * 1024 * 1024)) || \
  fail "resolved update staged $staged_bytes bytes; expected about one candidate copy"
((staged_bytes < stable_bytes / 2)) || \
  fail "changed update staged the unchanged 64 MiB file"

kill -KILL "$sync_pid" 2>/dev/null || true
wait "$sync_pid" 2>/dev/null || true
[[ -d $staging ]] || fail 'pending staging was lost with the interrupted process'
retry_started_ns=$(date +%s%N)
OFS_CONFIG="$peer_catalog" "$OFS_BIN" sync staging "$peer" --state "$peer_state" \
  --resolve conflict.txt >/dev/null || fail 'pending resolved sync did not recover'
retry_ended_ns=$(date +%s%N)
OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$replica" --state "$state" >/dev/null
grep -Fxq 'resolved local candidate' "$replica/conflict.txt" || \
  fail 'explicit conflict resolution did not converge on the retained local candidate'
diff -qr "$replica" "$peer" >/dev/null || fail 'replicas diverged after conflict resolution recovery'

python3 - "$OFS_REQUEST_LOG" \
  "$conflict_started_ns" "$conflict_ended_ns" \
  "$resolve_started_ns" "$resolve_paused_ns" \
  "$retry_started_ns" "$retry_ended_ns" <<'PY'
import json
import pathlib
import sys

log = pathlib.Path(sys.argv[1])
windows = {
    "conflict": (int(sys.argv[2]), int(sys.argv[3])),
    "before_finalize": (int(sys.argv[4]), int(sys.argv[5])),
    "retry": (int(sys.argv[6]), int(sys.argv[7])),
}
puts = {name: 0 for name in windows}
for line in log.read_text(encoding="utf-8").splitlines():
    request = json.loads(line)
    if request["method"] != "PUT" or "/.ofs/managed/data/v1/segments/sha256/" not in request["path"]:
        continue
    for name, (start, end) in windows.items():
        if start <= request["start_ns"] <= end:
            puts[name] += 1
if puts["conflict"]:
    raise SystemExit(f"unresolved conflict uploaded {puts['conflict']} data segment(s)")
if puts["before_finalize"]:
    raise SystemExit(f"pending intent did not precede {puts['before_finalize']} segment upload(s)")
if not puts["retry"]:
    raise SystemExit("pending recovery finalized no unique data segments")
print(
    "deferred finalize evidence: "
    f"conflict_puts={puts['conflict']} "
    f"pre_finalize_puts={puts['before_finalize']} retry_puts={puts['retry']}"
)
PY

mkdir "$replica/committed-cache-loss"
for index in $(seq -w 1 128); do
  {
    head -c 65536 /dev/zero
    printf 'committed publication %s\n' "$index"
  } >"$replica/committed-cache-loss/$index.bin"
done
OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$replica" --state "$state" >/dev/null &
sync_pid=$!
pause_when_pending "$state" "$sync_pid" || \
  fail 'could not preserve a pending state before commit'
pending_state="$OFS_CASE_ROOT/pending-before-commit.json"
cp "$state" "$pending_state"
kill -CONT "$sync_pid"
wait "$sync_pid" || fail 'publication did not commit after preserving its pending state'
cp "$pending_state" "$state"
OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$replica" --state "$state" >/dev/null || \
  fail 'committed publication could not recover without its pending cache'
OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$cold" --state "$cold_state" >/dev/null
diff -qr "$replica" "$cold" >/dev/null || fail 'cold replica differs after staging recovery'

printf 'managed-sync staging regression passed: changed=%s staged=%s stable=%s\n' \
  "$changed_bytes" "$staged_bytes" "$stable_bytes"
