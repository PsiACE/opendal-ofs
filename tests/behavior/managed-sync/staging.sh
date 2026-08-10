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

pause_when_pending() {
  local state=$1 pid=$2 status deadline=$((SECONDS + 10))
  while ((SECONDS < deadline)) && kill -0 "$pid" 2>/dev/null; do
    status=$("$OFS_BIN" status --state "$state" --json 2>/dev/null || true)
    if grep -Eq '"pending"[[:space:]]*:[[:space:]]*true' <<<"$status"; then
      kill -STOP "$pid" 2>/dev/null || return 1
      return
    fi
    sleep 0.01
  done
  return 1
}

replica="$OFS_CASE_ROOT/replica"
peer="$OFS_CASE_ROOT/peer"
state="$OFS_CASE_ROOT/state/replica.json"
peer_state="$OFS_CASE_ROOT/state/peer.json"
cold="$OFS_CASE_ROOT/cold"
cold_state="$OFS_CASE_ROOT/state/cold.json"
mkdir -p "$replica" "$peer" "$(dirname "$state")" "$cold"

target_options=(--model managed --storage "$OFS_STORAGE_URL")
if [[ -n ${OFS_METADATA_URL:-} ]]; then
  target_options+=(--metadata "$OFS_METADATA_URL")
fi
unset OFS_STORAGE_URL OFS_METADATA_URL
printf '%s\n' 'common conflict content' >"$replica/conflict.txt"
"$OFS_BIN" sync "$replica" --state "$state" --init "${target_options[@]}" >/dev/null
"$OFS_BIN" sync "$peer" --state "$peer_state" "${target_options[@]}" >/dev/null

printf '%s\n' 'remote conflict candidate' >"$replica/conflict.txt"
"$OFS_BIN" sync "$replica" --state "$state" >/dev/null
printf '%s\n' 'resolved local candidate' >"$peer/conflict.txt"
mkdir "$peer/changed"
for index in $(seq -w 1 128); do
  {
    printf 'unique deferred candidate %s\n' "$index"
    head -c 65536 /dev/zero
  } >"$peer/changed/$index.bin"
done

if "$OFS_BIN" sync "$peer" --state "$peer_state" \
  >/dev/null 2>&1; then
  fail 'same-path conflict succeeded without explicit resolution'
fi
conflict_status=$("$OFS_BIN" status --state "$peer_state" --json)
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*1' <<<"$conflict_status" || \
  fail 'unresolved conflict was not retained'

"$OFS_BIN" sync "$peer" --state "$peer_state" \
  --resolve conflict.txt >/dev/null &
sync_pid=$!
pause_when_pending "$peer_state" "$sync_pid" || \
  fail 'could not pause resolved sync before deferred finalization'

kill -KILL "$sync_pid" 2>/dev/null || true
wait "$sync_pid" 2>/dev/null || true
printf '%s\n' 'edited after the interrupted sync' >"$peer/after-crash.txt"
"$OFS_BIN" sync "$peer" --state "$peer_state" \
  --resolve conflict.txt >/dev/null || fail 'pending resolved sync did not recover'
"$OFS_BIN" sync "$replica" --state "$state" >/dev/null
grep -Fxq 'resolved local candidate' "$replica/conflict.txt" || \
  fail 'explicit conflict resolution did not converge on the retained local candidate'
grep -Fxq 'edited after the interrupted sync' "$replica/after-crash.txt" || \
  fail 'a local edit made after interruption was not included by the retry'
diff -qr "$replica" "$peer" >/dev/null || fail 'replicas diverged after conflict resolution recovery'

mkdir "$replica/committed-cache-loss"
for index in $(seq -w 1 128); do
  {
    head -c 65536 /dev/zero
    printf 'committed publication %s\n' "$index"
  } >"$replica/committed-cache-loss/$index.bin"
done
"$OFS_BIN" sync "$replica" --state "$state" >/dev/null &
sync_pid=$!
pause_when_pending "$state" "$sync_pid" || \
  fail 'could not preserve a pending state before commit'
pending_state="$OFS_CASE_ROOT/pending-before-commit.json"
cp "$state" "$pending_state"
kill -CONT "$sync_pid"
wait "$sync_pid" || fail 'publication did not commit after preserving its pending state'
for round in $(seq 1 40); do
  if ((round % 2)); then
    chmod u+x "$replica/conflict.txt"
  else
    chmod u-x "$replica/conflict.txt"
  fi
  "$OFS_BIN" sync "$replica" --state "$state" >/dev/null
done
cp "$pending_state" "$state"
"$OFS_BIN" sync "$replica" --state "$state" >/dev/null || \
  fail 'committed publication could not recover from retained history without its pending cache'
"$OFS_BIN" sync "$cold" --state "$cold_state" "${target_options[@]}" >/dev/null
diff -qr "$replica" "$cold" >/dev/null || fail 'cold replica differs after staging recovery'

printf '%s\n' 'managed-sync staging recovery passed'
