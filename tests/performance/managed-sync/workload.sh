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
: "${OFS_PERF_ROUNDS:?}" "${OFS_RESOURCES:?}"

rounds=$OFS_PERF_ROUNDS
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

measure() {
  local metric=$1 sample=$2 output=$3
  shift 3
  local started_ns ended_ns elapsed_ms resource
  started_ns=$(date +%s%N)
  record_command "$@"
  resource=$(mktemp "${TMPDIR:-/tmp}/ofs-managed-resource.XXXXXX")
  /usr/bin/time -f '%M' -o "$resource" env OFS_CONFIG="$catalog" "$@" >"$output"
  ended_ns=$(date +%s%N)
  elapsed_ms=$(((ended_ns - started_ns) / 1000000))
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$OFS_RELEASE" "$OFS_RUN_ID" "$metric" "$sample" "$elapsed_ms" \
    "$started_ns" "$ended_ns" >>"$OFS_METRICS"
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$OFS_RELEASE" "$OFS_RUN_ID" "$metric" "$sample" "$(tail -n 1 "$resource")" \
    >>"$OFS_RESOURCES"
  rm "$resource"
}

write_deterministic() {
  local path=$1 size=$2 seed=$3
  python3 - "$path" "$size" "$seed" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
size = int(sys.argv[2])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_bytes(hashlib.shake_256(sys.argv[3].encode()).digest(size))
PY
}

rewrite_window() {
  local path=$1 offset=$2 size=$3 seed=$4
  python3 - "$path" "$offset" "$size" "$seed" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
offset = int(sys.argv[2])
size = int(sys.argv[3])
with path.open("r+b") as output:
    output.seek(offset)
    output.write(hashlib.shake_256(sys.argv[4].encode()).digest(size))
PY
}

measure init create "$evidence/create.txt" \
  "$OFS_BIN" volume create "$volume" --model managed --storage "$OFS_STORAGE_URL"

source_tree="$OFS_RUN_ROOT/replica-source"
source_state="$OFS_RUN_ROOT/state-source.json"
lagging_tree="$OFS_RUN_ROOT/replica-lagging"
lagging_state="$OFS_RUN_ROOT/state-lagging.json"
mkdir -p "$source_tree/memory" "$source_tree/skills" "$lagging_tree"
write_deterministic "$source_tree/memory/seed.bin" $((16 * 1024 * 1024)) seed
for group in $(seq 0 7); do
  for item in $(seq 0 15); do
    write_deterministic \
      "$source_tree/skills/group-$group/file-$item.dat" \
      "$((1024 + (group * 16 + item) * 113))" \
      "small-$group-$item"
  done
done

measure init initial-publication "$evidence/initial-publication.txt" \
  "$OFS_BIN" sync "$volume" "$source_tree" --state "$source_state"
measure cold_restore initial "$evidence/initial-catchup.txt" \
  "$OFS_BIN" sync "$volume" "$lagging_tree" --state "$lagging_state"
diff -qr "$source_tree" "$lagging_tree" >/dev/null

lifecycle_started_ns=$(date +%s%N)
current_tree=$source_tree
current_state=$source_state
for round in $(seq 1 "$rounds"); do
  next_tree="$OFS_RUN_ROOT/replica-$round"
  next_state="$OFS_RUN_ROOT/state-$round.json"
  mkdir "$next_tree"
  measure cold_restore "$round" "$evidence/catchup-$round.txt" \
    "$OFS_BIN" sync "$volume" "$next_tree" --state "$next_state"
  diff -qr "$current_tree" "$next_tree" >/dev/null
  rewrite_window \
    "$next_tree/memory/seed.bin" \
    "$(((round * 1048573) % (15 * 1024 * 1024)))" \
    $((64 * 1024)) "seed-edit-$round"
  group=$((round % 8))
  item=$((round % 16))
  write_deterministic \
    "$next_tree/skills/group-$group/file-$item.dat" \
    "$((4096 + round * 257))" "small-edit-$round"
  write_deterministic \
    "$next_tree/memory/generation-$round.bin" \
    $((256 * 1024)) "generation-$round"
  measure publication "$round" "$evidence/publication-$round.txt" \
    "$OFS_BIN" sync "$volume" "$next_tree" --state "$next_state"
  rm -rf "$current_tree"
  rm -rf "$current_state"
  current_tree=$next_tree
  current_state=$next_state
done

measure incremental_catchup lagging "$evidence/catchup-lagging.txt" \
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
python3 - "$current_tree" "$OFS_RUN_ROOT/logical-tree.json" <<'PY'
import hashlib
import json
import pathlib
import stat
import sys

root = pathlib.Path(sys.argv[1])
entries = []
for path in sorted(root.rglob("*")):
    relative = path.relative_to(root).as_posix()
    if path.is_dir():
        entries.append({"path": relative, "type": "directory"})
        continue
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    entries.append(
        {
            "path": relative,
            "type": "file",
            "bytes": path.stat().st_size,
            "executable": bool(path.stat().st_mode & stat.S_IXUSR),
            "sha256": digest.hexdigest(),
        }
    )
pathlib.Path(sys.argv[2]).write_text(
    json.dumps(entries, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
{
  printf '%s\t%s\trounds\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$rounds"
  printf '%s\t%s\tlogical_files\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$logical_files"
  printf '%s\t%s\tlogical_bytes\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$logical_bytes"
  printf '%s\t%s\treplica_state_bytes\t%s\n' \
    "$OFS_RELEASE" "$OFS_RUN_ID" "$(stat -c %s "$current_state")"
} >>"$OFS_INPUTS"
