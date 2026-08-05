#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0.

set -euo pipefail

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
runtime=${CONTAINER_RUNTIME:-podman}
actor=${1:-scripted}
ofs_bin=${OFS_BIN:-$workspace/target/debug/ofs}
run_root=$(mktemp -d)
container="ofs-managed-sync-${PPID}-$$"

cleanup() {
  status=$?
  if ! "$runtime" rm -f "$container" >/dev/null 2>&1; then
    status=1
  fi
  if ((status == 0)); then
    rm -rf "$run_root"
  else
    printf 'Managed Sync evidence retained at %s\n' "$run_root" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

test -x "$ofs_bin"
"$runtime" run -d --rm --name "$container" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=ofs-acceptance \
  -e MINIO_ROOT_PASSWORD=ofs-acceptance-password \
  quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z server /data >/dev/null
port=$("$runtime" port "$container" 9000/tcp | sed -n 's/.*://p')
for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:${port}/minio/health/ready" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${port}/minio/health/ready" >/dev/null

"$runtime" run --rm --network host --entrypoint /bin/sh \
  quay.io/minio/mc:RELEASE.2024-09-16T17-43-14Z -c \
  "mc alias set acceptance http://127.0.0.1:${port} ofs-acceptance ofs-acceptance-password >/dev/null && mc mb acceptance/ofs-managed-sync >/dev/null"

export OFS_BIN="$ofs_bin"
export OFS_RUN_ROOT="$run_root"
export OFS_VOLUME=agent-home
export OFS_STORAGE_URL="s3://?bucket=ofs-managed-sync&root=${actor}&endpoint=http://127.0.0.1:${port}&region=us-east-1&access_key_id=ofs-acceptance&secret_access_key=ofs-acceptance-password"
export OFS_PUBLIC_STORAGE_URL="s3://?bucket=ofs-managed-sync&root=${actor}&endpoint=http://127.0.0.1:${port}&region=us-east-1"
export OFS_MINIO_ENDPOINT="http://127.0.0.1:${port}"
export OFS_CONTAINER_RUNTIME="$runtime"

if [[ $actor == lifecycle ]]; then
  "$workspace/tests/behavior/managed-sync/lifecycle.sh"
else
  "$workspace/tests/behavior/managed-sync/acceptance.sh" "$actor"
fi
