#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

baseline_sha=${OFS_PERF_BASELINE:-}
baseline_binary=${OFS_PERF_BASELINE_BIN:-}
workspace=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
suite="$workspace/tests/performance/managed-sync"
candidate_sha=${OFS_PERF_CANDIDATE:-$(git -C "$workspace" rev-parse HEAD)}
candidate_binary=${OFS_PERF_CANDIDATE_BIN:-}
output=
rounds=${OFS_PERF_ROUNDS:-12}
bucket=ofs-managed-performance
access_key=ofs-performance
secret_key=ofs-performance-password
admin_access_key=ofs-performance-admin
admin_secret_key=ofs-performance-admin-password
minio_image=${MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z}
mc_image=${MC_IMAGE:-quay.io/minio/mc:RELEASE.2024-09-16T17-43-14Z}
audit_image=${AUDIT_IMAGE:-python:3.13.7-alpine3.22}
schedule=(baseline candidate candidate baseline baseline candidate)

usage() {
  cat <<'EOF'
Usage: tests/performance/managed-sync/run.sh [OPTIONS] [OUTPUT]

Options:
  --baseline REF_OR_BINARY
  --candidate REF_OR_BINARY
  --rounds N
EOF
}

select_source() {
  local role=$1 value=$2 path
  if [[ -f $value ]]; then
    path=$(cd "$(dirname "$value")" && pwd)/$(basename "$value")
    [[ -x $path ]] || { printf '%s binary is not executable: %s\n' "$role" "$value" >&2; exit 2; }
    if [[ $role == baseline ]]; then
      baseline_binary=$path
      baseline_sha=
    else
      candidate_binary=$path
      candidate_sha=
    fi
  elif [[ $role == baseline ]]; then
    baseline_binary=
    baseline_sha=$value
  else
    candidate_binary=
    candidate_sha=$value
  fi
}

while (($#)); do
  case $1 in
    --baseline|--candidate|--rounds)
      (($# >= 2)) || { printf '%s requires a value\n' "$1" >&2; exit 2; }
      option=$1
      value=$2
      shift 2
      case $option in
        --baseline) select_source baseline "$value" ;;
        --candidate) select_source candidate "$value" ;;
        --rounds) rounds=$value ;;
      esac
      ;;
    -h|--help) usage; exit ;;
    -*) printf 'unknown option: %s\n' "$1" >&2; exit 2 ;;
    *)
      [[ -z $output ]] || { printf 'unexpected argument: %s\n' "$1" >&2; exit 2; }
      output=$1
      shift
      ;;
  esac
done
[[ $rounds =~ ^[1-9][0-9]*$ ]] || { printf '--rounds must be greater than zero\n' >&2; exit 2; }
[[ -n $baseline_sha || -n $baseline_binary ]] || {
  printf 'a baseline is required; pass --baseline REF_OR_BINARY\n' >&2
  exit 2
}
output=${output:-$workspace/.local/performance/managed-sync-ab-$(date -u +%Y%m%dT%H%M%SZ)}

workload="$suite/workload.sh"

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
audit_container="$container-audit"
network="$container-network"
network_created=false

cleanup() {
  local status=$?
  "$runtime" stop --time 1 "$container" "$audit_container" >/dev/null 2>&1 || true
  if [[ $network_created == true ]]; then
    "$runtime" network rm "$network" >/dev/null 2>&1 || true
  fi
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

"$runtime" network create "$network" >/dev/null
network_created=true
: >"$output/audit.jsonl"
"$runtime" run -d --rm --name "$audit_container" --network "$network" \
  --network-alias audit -v "$suite/minio-audit.py:/fixture/minio-audit.py:ro,Z" \
  -v "$output/audit.jsonl:/evidence/audit.jsonl:Z" \
  "$audit_image" python /fixture/minio-audit.py --host 0.0.0.0 --port 8080 \
  --log /evidence/audit.jsonl --ready /tmp/audit.ready >/dev/null
for _ in $(seq 1 100); do
  "$runtime" exec "$audit_container" test -s /tmp/audit.ready && break
  sleep 0.05
done
"$runtime" exec "$audit_container" test -s /tmp/audit.ready

"$runtime" run -d --rm --name "$container" --network "$network" \
  --network-alias minio -p 127.0.0.1::9000 \
  -e NO_PROXY=audit,localhost,127.0.0.1 -e no_proxy=audit,localhost,127.0.0.1 \
  -e "MINIO_ROOT_USER=$admin_access_key" -e "MINIO_ROOT_PASSWORD=$admin_secret_key" \
  -e MINIO_AUDIT_WEBHOOK_ENABLE_HARNESS=on \
  -e MINIO_AUDIT_WEBHOOK_ENDPOINT_HARNESS=http://audit:8080 \
  -e MINIO_AUDIT_WEBHOOK_BATCH_SIZE_HARNESS=1 \
  "$minio_image" server /data >/dev/null
minio_port=$("$runtime" port "$container" 9000/tcp | tail -n 1 | sed 's/.*://')
for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$minio_port/minio/health/ready" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$minio_port/minio/health/ready" >/dev/null

mc_run_as() {
  local user=$1 password=$2
  shift 2
  # shellcheck disable=SC2016
  "$runtime" run --rm --network "$network" \
    -e NO_PROXY=minio,localhost,127.0.0.1 -e no_proxy=minio,localhost,127.0.0.1 \
    --entrypoint /bin/sh "$mc_image" -c '
    set -e
    endpoint=$1 user=$2 password=$3
    shift 3
    mc alias set performance "$endpoint" "$user" "$password" >/dev/null
    exec mc "$@"
  ' shell http://minio:9000 "$user" "$password" "$@"
}
mc_run() { mc_run_as "$admin_access_key" "$admin_secret_key" "$@"; }
mc_run mb --ignore-existing "performance/$bucket" >/dev/null
mc_run admin user add performance "$access_key" "$secret_key" >/dev/null
mc_run admin policy attach performance readwrite --user "$access_key" >/dev/null

: >"$output/samples.tsv"
: >"$output/resources.tsv"
: >"$output/inputs.tsv"
: >"$output/commands.tsv"
{
  printf 'baseline\t%s\n' "$baseline_identity"
  printf 'candidate\t%s\n' "$candidate_identity"
  printf 'rustc\t%s\n' "$(rustc --version)"
  printf 'kernel\t%s\n' "$(uname -srmo)"
  printf 'container_runtime\t%s\n' "$runtime"
  printf 'minio_image\t%s\n' "$minio_image"
  printf 'audit_image\t%s\n' "$audit_image"
  printf 'rounds\t%s\n' "$rounds"
} >"$output/context.tsv"

for index in "${!schedule[@]}"; do
  release=${schedule[$index]}
  run=$(printf '%02d-%s' "$((index + 1))" "$release")
  run_root="$output/runs/$run"
  object_root="ab/$run"
  mkdir "$run_root"
  storage_url="s3://$bucket/$object_root?endpoint=http%3A%2F%2F127.0.0.1%3A$minio_port&region=us-east-1"
  AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$secret_key" AWS_REGION=us-east-1 \
    OFS_BIN="$scratch/ofs-$release" OFS_RUN_ROOT="$run_root" OFS_STORAGE_URL="$storage_url" \
    OFS_METRICS="$output/samples.tsv" OFS_INPUTS="$output/inputs.tsv" \
    OFS_RESOURCES="$output/resources.tsv" \
    OFS_COMMANDS="$output/commands.tsv" OFS_RELEASE="$release" OFS_RUN_ID="$run" \
    OFS_PERF_ROUNDS="$rounds" OFS_CONTAINER_RUNTIME="$runtime" "$workload"

  mc_run ls --recursive --json "performance/$bucket/$object_root" \
    >"$run_root/objects.jsonl"
  find "$run_root" -mindepth 1 -maxdepth 1 \
    ! -name evidence ! -name logical-tree.json ! -name objects.jsonl \
    -exec rm -rf -- {} +
done

barrier_key="audit-barrier/${container}.txt"
printf '%s\n' "$container" | \
  mc_run_as "$access_key" "$secret_key" pipe "performance/$bucket/$barrier_key" >/dev/null
python3 - "$output/audit.jsonl" "$access_key" "/$bucket/$barrier_key" <<'PY'
import json
import pathlib
import sys
import time

log = pathlib.Path(sys.argv[1])
access_key = sys.argv[2]
request_path = sys.argv[3]
deadline = time.monotonic() + 10
while time.monotonic() < deadline:
    for line in log.read_text(encoding="utf-8").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("accessKey") == access_key and event.get("requestPath") == request_path:
            raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit("MinIO native audit barrier was not delivered")
PY

python3 "$suite/analyze.py" --access-key "$access_key" "$output"
printf 'canonical evidence: %s\n' "$output/results.json"
