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
: "${OFS_CONTAINER_RUNTIME:?}"

rounds=${OFS_PERF_ROUNDS:-20}
suite=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
evidence="$OFS_RUN_ROOT/evidence"
agent_image=${OFS_AGENT_IMAGE:-quay.io/fedora/fedora:44}
container_prefix="ofs-agent-${OFS_RUN_ID//[^a-zA-Z0-9_.-]/-}-$$"
mkdir -p "$evidence"

declare -a containers=()
cleanup() {
  local container
  for container in "${containers[@]}"; do
    "$OFS_CONTAINER_RUNTIME" rm -f "$container" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

record_command() {
  {
    printf '%s\t%s\t' "$OFS_RELEASE" "$OFS_RUN_ID"
    printf '%q ' "$@"
    printf '\n'
  } >>"$OFS_COMMANDS"
}

run_agent() {
  local output=$1 container=$2
  shift 2
  record_command "$OFS_CONTAINER_RUNTIME" exec "$container" "$@"
  "$OFS_CONTAINER_RUNTIME" exec "$container" "$@" >"$output"
}

measure_agent() {
  local metric=$1 sample=$2 output=$3 container=$4
  shift 4
  local started_ns ended_ns elapsed_ms
  started_ns=$(date +%s%N)
  run_agent "$output" "$container" "$@"
  ended_ns=$(date +%s%N)
  elapsed_ms=$(((ended_ns - started_ns) / 1000000))
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$OFS_RELEASE" "$OFS_RUN_ID" "$metric" "$sample" "$elapsed_ms" \
    "$started_ns" "$ended_ns" >>"$OFS_METRICS"
}

start_agent() {
  local name=$1 root=$2 container
  container="$container_prefix-$name"
  mkdir -p "$root/tree"
  "$OFS_CONTAINER_RUNTIME" run -d --rm --name "$container" --network host \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION \
    -e OFS_CONFIG=/agent/catalog.json \
    -v "$root:/agent:Z" -v "$OFS_BIN:/usr/local/bin/ofs:ro,z" \
    "$agent_image" sleep infinity >/dev/null
  containers+=("$container")
  started_agent=$container
}

register_agent() {
  local name=$1 alias=$2
  run_agent "$evidence/register-$alias.txt" "$name" \
    ofs volume create "$alias" --model managed --storage "$OFS_STORAGE_URL"
}

sync_agent() {
  local metric=$1 sample=$2 output=$3 container=$4 alias=$5
  measure_agent "$metric" "$sample" "$output" "$container" \
    ofs sync "$alias" /agent/tree --state /agent/state.json
}

mutate_agent() {
  local container=$1 generation=$2 domain retained delete_candidate
  case $((generation % 3)) in
    0) domain=.agents ;;
    1) domain=.bub ;;
    2) domain=.codex ;;
  esac
  retained="/agent/tree/$domain/changes/retained-$generation.bin"
  delete_candidate="/agent/tree/$domain/changes/delete-$generation.bin"
  # The single-quoted program expands inside the agent container.
  # shellcheck disable=SC2016
  "$OFS_CONTAINER_RUNTIME" exec "$container" bash -eu -c '
    generation=$1 domain=$2 retained=$3 delete_candidate=$4
    mkdir -p "/agent/tree/$domain/changes"
    printf "generation %s\n" "$generation" >"/agent/tree/.agents/d0000/f00000.bin"
    printf "generation %s\n" "$generation" | dd \
      of="/agent/tree/.bub/d0000/f00000.bin" conv=notrunc status=none
    printf "generation %s\n" "$generation" | dd \
      of="/agent/tree/.codex/d0000/f00000.bin" conv=notrunc status=none
    dd if=/dev/zero of="$retained" bs=4096 count=1 status=none
    printf "%s\n" "$generation" | dd of="$retained" conv=notrunc status=none
    printf "delete after six generations: %s\n" "$generation" >"$delete_candidate"
    if ((generation > 6)); then
      prior=$((generation - 6))
      for candidate in /agent/tree/.*/changes/delete-$prior.bin; do
        [[ -e $candidate ]] || continue
        rm "$candidate"
      done
    fi
  ' shell "$generation" "$domain" "$retained" "$delete_candidate"
}

python3 "$suite/agent-home-fixture.py" build "$OFS_RUN_ROOT/alpha/tree"
python3 "$suite/agent-home-fixture.py" describe "$OFS_RUN_ROOT/fixture.json"

start_agent alpha "$OFS_RUN_ROOT/alpha"
alpha=$started_agent
start_agent beta "$OFS_RUN_ROOT/beta"
beta=$started_agent
start_agent gamma "$OFS_RUN_ROOT/gamma"
gamma=$started_agent
aliases=(alpha-memory beta-workspace gamma-state)
agents=("$alpha" "$beta" "$gamma")

for index in 0 1 2; do
  register_agent "${agents[$index]}" "${aliases[$index]}"
done
run_agent "$evidence/initial-publication.txt" "$alpha" \
  ofs sync "${aliases[0]}" /agent/tree --state /agent/state.json

lifecycle_started_ns=$(date +%s%N)
for generation in $(seq 1 "$rounds"); do
  index=$(((generation - 1) % 3))
  container=${agents[$index]}
  alias=${aliases[$index]}
  sync_agent catchup "$generation" "$evidence/catchup-$generation.txt" "$container" "$alias"
  mutate_agent "$container" "$generation"
  sync_agent publication "$generation" "$evidence/publication-$generation.txt" "$container" "$alias"
done

for index in 0 1 2; do
  sync_agent catchup "final-$index" "$evidence/final-$index.txt" \
    "${agents[$index]}" "${aliases[$index]}"
done
diff -qr "$OFS_RUN_ROOT/alpha/tree" "$OFS_RUN_ROOT/beta/tree" >/dev/null
diff -qr "$OFS_RUN_ROOT/alpha/tree" "$OFS_RUN_ROOT/gamma/tree" >/dev/null
lifecycle_ended_ns=$(date +%s%N)
printf '%s\t%s\tlifecycle\t1\t%s\t%s\t%s\n' \
  "$OFS_RELEASE" "$OFS_RUN_ID" \
  "$(((lifecycle_ended_ns - lifecycle_started_ns) / 1000000))" \
  "$lifecycle_started_ns" "$lifecycle_ended_ns" >>"$OFS_METRICS"

start_agent cold "$OFS_RUN_ROOT/cold"
cold=$started_agent
register_agent "$cold" cold-recovery
sync_agent catchup cold "$evidence/cold-catchup.txt" "$cold" cold-recovery
diff -qr "$OFS_RUN_ROOT/alpha/tree" "$OFS_RUN_ROOT/cold/tree" >/dev/null
sync_agent noop 1 "$evidence/noop.txt" "$cold" cold-recovery

read -r logical_files logical_bytes < <(
  find "$OFS_RUN_ROOT/cold/tree" -type f -printf '%s\n' |
    awk '{ bytes += $1; files += 1 } END { print files + 0, bytes + 0 }'
)
python3 - "$OFS_RUN_ROOT/cold/tree" "$OFS_RUN_ROOT/logical-tree.json" <<'PY'
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
    digest = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
    entries.append({
        "path": relative,
        "type": "file",
        "bytes": path.stat().st_size,
        "executable": bool(path.stat().st_mode & stat.S_IXUSR),
        "sha256": digest,
    })
pathlib.Path(sys.argv[2]).write_text(
    json.dumps(entries, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
{
  printf '%s\t%s\trounds\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$rounds"
  printf '%s\t%s\tlogical_files\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$logical_files"
  printf '%s\t%s\tlogical_bytes\t%s\n' "$OFS_RELEASE" "$OFS_RUN_ID" "$logical_bytes"
} >>"$OFS_INPUTS"
