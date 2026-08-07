#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

: "${OFS_BIN:?}" "${OFS_CASE_ROOT:?}" "${OFS_STORAGE_URL:?}"

fail() {
  printf 'managed-sync staging regression: %s\n' "$*" >&2
  exit 1
}

catalog="$OFS_CASE_ROOT/catalog.json"
replica="$OFS_CASE_ROOT/replica"
state="$OFS_CASE_ROOT/state/replica.json"
cold="$OFS_CASE_ROOT/cold"
cold_state="$OFS_CASE_ROOT/state/cold.json"
stable_bytes=$((64 * 1024 * 1024))
changed_bytes=$((128 * 64 * 1024))
mkdir -p "$replica" "$(dirname "$state")" "$cold"

OFS_CONFIG="$catalog" "$OFS_BIN" volume create staging \
  --model managed --storage "$OFS_STORAGE_URL" >/dev/null
head -c "$stable_bytes" /dev/zero >"$replica/stable.bin"
OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$replica" --state "$state" >/dev/null

mkdir "$replica/changed"
for index in $(seq -w 1 128); do
  head -c 65536 /dev/zero >"$replica/changed/$index.bin"
done

OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$replica" --state "$state" >/dev/null &
sync_pid=$!
stopped=false
for _ in $(seq 1 300); do
  if ! kill -0 "$sync_pid" 2>/dev/null; then
    break
  fi
  status=$(OFS_CONFIG="$catalog" "$OFS_BIN" status "$replica" --state "$state" --json 2>/dev/null || true)
  if grep -Eq '"pending"[[:space:]]*:[[:space:]]*true' <<<"$status"; then
    if kill -STOP "$sync_pid" 2>/dev/null; then
      stopped=true
    fi
    break
  fi
  sleep 0.01
done
[[ $stopped == true ]] || fail 'could not pause sync with a durable publication intent'

staging=$(python3 - "$state" <<'PY'
import json
import pathlib
import sys

state = pathlib.Path(sys.argv[1]).resolve()
value = pathlib.Path(json.loads(state.read_text())["pending"]["staging"]).resolve()
if value.parent != state.parent:
    raise SystemExit("pending staging is outside the state directory")
print(value)
PY
)
[[ -d $staging ]] || fail 'pending staging directory is missing'
staged_bytes=$(find "$staging" -type f -printf '%s\n' | awk '{ total += $1 } END { print total + 0 }')
((staged_bytes <= changed_bytes + 4 * 1024 * 1024)) || \
  fail "changed update staged $staged_bytes bytes; expected at most changed bytes plus 4 MiB"
((staged_bytes < stable_bytes / 2)) || \
  fail "changed update staged the unchanged 64 MiB file"

kill -KILL "$sync_pid" 2>/dev/null || true
wait "$sync_pid" 2>/dev/null || true
recovered=false
for _ in $(seq 1 5); do
  if OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$replica" --state "$state" >/dev/null; then
    recovered=true
    break
  fi
  sleep 0.05
done
[[ $recovered == true ]] || fail 'killed changed-only publication did not recover'
OFS_CONFIG="$catalog" "$OFS_BIN" sync staging "$cold" --state "$cold_state" >/dev/null
diff -qr "$replica" "$cold" >/dev/null || fail 'cold replica differs after staging recovery'

printf 'managed-sync staging regression passed: changed=%s staged=%s stable=%s\n' \
  "$changed_bytes" "$staged_bytes" "$stable_bytes"
