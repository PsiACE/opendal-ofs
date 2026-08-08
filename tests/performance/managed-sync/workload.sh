#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

: "${OFS_BIN:?}" "${OFS_RUN_ROOT:?}" "${OFS_STORAGE_URL:?}" "${OFS_METRICS:?}"
: "${OFS_INPUTS:?}" "${OFS_COMMANDS:?}" "${OFS_RELEASE:?}" "${OFS_RUN_ID:?}"

rounds=${OFS_PERF_ROUNDS:-12}
volume=performance-volume
catalog="$OFS_RUN_ROOT/catalog.json"
evidence="$OFS_RUN_ROOT/evidence"
mkdir -p "$evidence"

record_command() {
  {
    printf '%s\t%s\t' "$OFS_RELEASE" "$OFS_RUN_ID"
    printf '%q ' "$@"
    printf '\n'
  } >>"$OFS_COMMANDS"
}

run_command() {
  local output=$1
  shift
  record_command "$@"
  OFS_CONFIG="$catalog" "$@" >"$output"
}

measure() {
  local metric=$1 sample=$2 output=$3
  shift 3
  local started_ns ended_ns elapsed_ms
  started_ns=$(date +%s%N)
  run_command "$output" "$@"
  ended_ns=$(date +%s%N)
  elapsed_ms=$(((ended_ns - started_ns) / 1000000))
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$OFS_RELEASE" "$OFS_RUN_ID" "$metric" "$sample" "$elapsed_ms" \
    "$started_ns" "$ended_ns" >>"$OFS_METRICS"
}

create=("$OFS_BIN" volume create "$volume")
if "$OFS_BIN" volume create --help 2>&1 | grep -q -- '--model'; then
  create+=(--model managed)
fi
create+=(--storage "$OFS_STORAGE_URL")
run_command "$evidence/create.txt" "${create[@]}"

source_tree="$OFS_RUN_ROOT/replica-source"
source_state="$OFS_RUN_ROOT/state-source.json"
lagging_tree="$OFS_RUN_ROOT/replica-lagging"
lagging_state="$OFS_RUN_ROOT/state-lagging.json"
mkdir -p "$source_tree/memory" "$source_tree/skills" "$lagging_tree"
{
  printf 'shared seed\n'
  head -c 1048576 /dev/zero
} >"$source_tree/memory/seed.bin"
printf 'managed sync performance\n' >"$source_tree/skills/storage.txt"

run_command "$evidence/initial-publication.txt" \
  "$OFS_BIN" sync "$volume" "$source_tree" --state "$source_state"
run_command "$evidence/initial-catchup.txt" \
  "$OFS_BIN" sync "$volume" "$lagging_tree" --state "$lagging_state"
diff -qr "$source_tree" "$lagging_tree" >/dev/null

lifecycle_started_ns=$(date +%s%N)
current_tree=$source_tree
current_state=$source_state
for round in $(seq 1 "$rounds"); do
  next_tree="$OFS_RUN_ROOT/replica-$round"
  next_state="$OFS_RUN_ROOT/state-$round.json"
  mkdir "$next_tree"
  measure catchup "$round" "$evidence/catchup-$round.txt" \
    "$OFS_BIN" sync "$volume" "$next_tree" --state "$next_state"
  diff -qr "$current_tree" "$next_tree" >/dev/null
  {
    printf 'generation %02d\n' "$round"
    head -c 262144 /dev/zero
  } >"$next_tree/memory/generation-$round.bin"
  measure publication "$round" "$evidence/publication-$round.txt" \
    "$OFS_BIN" sync "$volume" "$next_tree" --state "$next_state"
  rm -rf "$current_tree"
  rm -rf "$current_state"
  current_tree=$next_tree
  current_state=$next_state
done

measure catchup lagging "$evidence/catchup-lagging.txt" \
  "$OFS_BIN" sync "$volume" "$lagging_tree" --state "$lagging_state"
diff -qr "$current_tree" "$lagging_tree" >/dev/null
lifecycle_ended_ns=$(date +%s%N)
printf '%s\t%s\tlifecycle\t1\t%s\t%s\t%s\n' \
  "$OFS_RELEASE" "$OFS_RUN_ID" \
  "$(((lifecycle_ended_ns - lifecycle_started_ns) / 1000000))" \
  "$lifecycle_started_ns" "$lifecycle_ended_ns" >>"$OFS_METRICS"

measure noop 1 "$evidence/noop.txt" \
  "$OFS_BIN" sync "$volume" "$current_tree" --state "$current_state"

read -r logical_files logical_bytes < <(
  find "$current_tree" -type f -printf '%s\n' |
    awk '{ bytes += $1; files += 1 } END { print files + 0, bytes + 0 }'
)
{
  printf '%s\t%s\trounds\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$rounds"
  printf '%s\t%s\tlogical_files\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$logical_files"
  printf '%s\t%s\tlogical_bytes\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$logical_bytes"
} >>"$OFS_INPUTS"
