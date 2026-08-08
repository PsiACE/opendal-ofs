#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

baseline_sha=${OFS_PERF_BASELINE:-b262c3ae9f0c8147a3295072fc05e36adb1f9702}
baseline_binary=${OFS_PERF_BASELINE_BIN:-}
workspace=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
suite="$workspace/tests/performance/managed-sync"
candidate_sha=${OFS_PERF_CANDIDATE:-$(git -C "$workspace" rev-parse HEAD)}
candidate_binary=${OFS_PERF_CANDIDATE_BIN:-}
output=${1:-$workspace/.local/performance/managed-sync-ab-$(date -u +%Y%m%dT%H%M%SZ)}
rounds=${OFS_PERF_ROUNDS:-12}
pack=${OFS_PERF_PACK:-0}
bucket=ofs-managed-performance
access_key=ofs-performance
secret_key=ofs-performance-password
minio_image=${MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z}
mc_image=${MC_IMAGE:-quay.io/minio/mc:RELEASE.2024-09-16T17-43-14Z}
schedule=(baseline candidate candidate baseline baseline candidate)

[[ $pack == 0 || $pack == 1 ]] || { printf '%s\n' 'OFS_PERF_PACK must be 0 or 1' >&2; exit 2; }

if [[ -n ${OFS_CONTAINER_RUNTIME:-} ]]; then
  runtime=$OFS_CONTAINER_RUNTIME
elif command -v podman >/dev/null; then
  runtime=podman
else
  runtime=docker
fi
for command in git cargo curl python3 "$runtime"; do
  command -v "$command" >/dev/null || { printf 'required command is missing: %s\n' "$command" >&2; exit 2; }
done
if [[ -n $baseline_binary ]]; then
  [[ -x $baseline_binary ]] || { printf 'baseline binary is not executable: %s\n' "$baseline_binary" >&2; exit 2; }
  baseline_binary=$(cd "$(dirname "$baseline_binary")" && pwd)/$(basename "$baseline_binary")
  baseline_identity="binary:$baseline_binary"
else
  git -C "$workspace" cat-file -e "$baseline_sha^{commit}"
  baseline_identity=$(git -C "$workspace" rev-parse "$baseline_sha^{commit}")
fi
if [[ -n $candidate_binary ]]; then
  [[ -x $candidate_binary ]] || { printf 'candidate binary is not executable: %s\n' "$candidate_binary" >&2; exit 2; }
  candidate_binary=$(cd "$(dirname "$candidate_binary")" && pwd)/$(basename "$candidate_binary")
  candidate_identity="binary:$candidate_binary"
else
  git -C "$workspace" cat-file -e "$candidate_sha^{commit}"
  candidate_identity=$(git -C "$workspace" rev-parse "$candidate_sha^{commit}")
fi
[[ ! -e $output ]] || { printf 'output path already exists: %s\n' "$output" >&2; exit 2; }
mkdir -p "$output/runs"
output=$(cd "$output" && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/ofs-managed-perf.XXXXXX")
container="ofs-managed-perf-${PPID}-$$"
proxy_pid=

cleanup() {
  local status=$?
  if [[ -n $proxy_pid ]]; then
    kill "$proxy_pid" >/dev/null 2>&1 || true
    wait "$proxy_pid" 2>/dev/null || true
  fi
  "$runtime" rm -f "$container" >/dev/null 2>&1 || true
  for tree in "$scratch/baseline" "$scratch/candidate"; do
    if [[ -d $tree ]]; then
      git -C "$workspace" worktree remove --force "$tree" >/dev/null 2>&1 || true
    fi
  done
  rm -rf "$scratch"
  if ((status != 0)); then
    printf 'performance evidence retained at %s\n' "$output" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

build_release() {
  local release=$1 source=$2 binary=$3
  if [[ -n $binary ]]; then
    cp "$binary" "$scratch/ofs-$release"
    return
  fi
  git -C "$workspace" worktree add --detach "$source" "${4}" >/dev/null
  CARGO_TARGET_DIR="$scratch/target-$release" \
    cargo build --manifest-path "$source/Cargo.toml" --release --locked --bin ofs
  cp "$scratch/target-$release/release/ofs" "$scratch/ofs-$release"
}
build_release baseline "$scratch/baseline" "$baseline_binary" "$baseline_sha"
build_release candidate "$scratch/candidate" "$candidate_binary" "$candidate_sha"

"$runtime" run -d --rm --name "$container" -p 127.0.0.1::9000 \
  -e "MINIO_ROOT_USER=$access_key" -e "MINIO_ROOT_PASSWORD=$secret_key" \
  "$minio_image" server /data >/dev/null
minio_port=$("$runtime" port "$container" 9000/tcp | tail -n 1 | sed 's/.*://')
for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$minio_port/minio/health/ready" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$minio_port/minio/health/ready" >/dev/null

mc_run() {
  "$runtime" run --rm --network host --entrypoint /bin/sh "$mc_image" -c '
    endpoint=$1 user=$2 password=$3
    shift 3
    mc alias set performance "$endpoint" "$user" "$password" >/dev/null
    exec mc "$@"
  ' shell "http://127.0.0.1:$minio_port" "$access_key" "$secret_key" "$@"
}
mc_run mb --ignore-existing "performance/$bucket" >/dev/null

proxy_ready="$scratch/proxy.port"
python3 "$suite/s3-proxy.py" \
  --upstream "127.0.0.1:$minio_port" --log "$output/requests.jsonl" --ready "$proxy_ready" &
proxy_pid=$!
for _ in $(seq 1 100); do
  [[ -s $proxy_ready ]] && break
  sleep 0.05
done
[[ -s $proxy_ready ]]
proxy_port=$(<"$proxy_ready")

: >"$output/samples.tsv"
: >"$output/inputs.tsv"
: >"$output/commands.tsv"
{
  printf 'baseline\t%s\n' "$baseline_identity"
  printf 'candidate\t%s\n' "$candidate_identity"
  printf 'rustc\t%s\n' "$(rustc --version)"
  printf 'kernel\t%s\n' "$(uname -srmo)"
  printf 'container_runtime\t%s\n' "$runtime"
  printf 'minio_image\t%s\n' "$minio_image"
  printf 'rounds\t%s\n' "$rounds"
  printf 'pack\t%s\n' "$pack"
} >"$output/context.tsv"

for index in "${!schedule[@]}"; do
  release=${schedule[$index]}
  run=$(printf '%02d-%s' "$((index + 1))" "$release")
  run_root="$output/runs/$run"
  object_root="ab/$run"
  mkdir "$run_root"
  storage_url="s3://$bucket/$object_root?endpoint=http%3A%2F%2F127.0.0.1%3A$proxy_port&region=us-east-1"
  AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$secret_key" AWS_REGION=us-east-1 \
    OFS_BIN="$scratch/ofs-$release" OFS_RUN_ROOT="$run_root" OFS_STORAGE_URL="$storage_url" \
    OFS_METRICS="$output/samples.tsv" OFS_INPUTS="$output/inputs.tsv" \
    OFS_COMMANDS="$output/commands.tsv" OFS_RELEASE="$release" OFS_RUN_ID="$run" \
    OFS_PERF_ROUNDS="$rounds" OFS_PERF_PACK="$pack" "$suite/workload.sh"

  mc_run du --json "performance/$bucket/$object_root" >"$run_root/object-inventory.json"
  mc_run ls --recursive --json "performance/$bucket/$object_root" \
    >"$run_root/objects.jsonl"
  read -r stored_bytes stored_objects < <(
    python3 - "$run_root/object-inventory.json" <<'PY'
import json, sys
records = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8")]
record = [item for item in records if item.get("status") == "success"][-1]
print(record["size"], record["objects"])
PY
  )
  [[ $stored_bytes =~ ^[0-9]+$ && $stored_objects =~ ^[0-9]+$ ]]
  printf '%s\t%s\tstored_bytes\t%s\n' "$release" "$run" "$stored_bytes" >>"$output/inputs.tsv"
  printf '%s\t%s\tstored_objects\t%s\n' "$release" "$run" "$stored_objects" >>"$output/inputs.tsv"
done

python3 "$suite/analyze.py" "$output"
