#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

fail() {
  printf 'managed-sync %s: %s\n' "$(basename "$0" .sh)" "$*" >&2
  exit 1
}

tree_digest() {
  local root=$1
  (cd "$root" && find . -type f -exec b3sum {} + | LC_ALL=C sort | b3sum)
}

json_field() {
  local document=$1 field=$2
  python3 -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$field" \
    <<<"$document"
}

OFS_BIN=${OFS_BIN:-}
OFS_CASE_ROOT=${OFS_CASE_ROOT:-}
OFS_STORAGE_URL=${OFS_STORAGE_URL:-}
OFS_METADATA_MODE=${OFS_METADATA_MODE:-object}
OFS_METADATA_URL=${OFS_METADATA_URL:-}

[[ -n "$OFS_BIN" ]] || fail 'OFS_BIN must name the built ofs executable'
[[ -x "$OFS_BIN" ]] || fail "OFS_BIN is not executable: $OFS_BIN"
[[ -n "$OFS_CASE_ROOT" ]] || fail 'OFS_CASE_ROOT must name a fresh test directory'
[[ ! -e "$OFS_CASE_ROOT" ]] || fail "OFS_CASE_ROOT already exists: $OFS_CASE_ROOT"
[[ -n "$OFS_STORAGE_URL" ]] || fail 'OFS_STORAGE_URL must be a credential-free data URL'

case "$OFS_METADATA_MODE" in
  object) [[ -z "$OFS_METADATA_URL" ]] || fail 'object metadata requires OFS_METADATA_URL to be unset' ;;
  d1) [[ -n "$OFS_METADATA_URL" ]] || fail 'd1 metadata requires OFS_METADATA_URL' ;;
  *) fail "OFS_METADATA_MODE must be object or d1, got: $OFS_METADATA_MODE" ;;
esac

replica_a="$OFS_CASE_ROOT/replica-a"
replica_b="$OFS_CASE_ROOT/replica-b"
cold_replica="$OFS_CASE_ROOT/cold-replica"
state_a="$OFS_CASE_ROOT/state/replica-a.json"
state_b="$OFS_CASE_ROOT/state/replica-b.json"
cold_state="$OFS_CASE_ROOT/state/cold-replica.json"
target_options=(--model managed --storage "$OFS_STORAGE_URL")
if [[ "$OFS_METADATA_MODE" == d1 ]]; then
  target_options+=(--metadata "$OFS_METADATA_URL")
fi
unset OFS_STORAGE_URL OFS_METADATA_URL

mkdir -p "$replica_a" "$replica_b" "$cold_replica" "$(dirname "$state_a")"

sync_a() {
  "$OFS_BIN" sync "$replica_a" --state "$state_a"
}

sync_b() {
  "$OFS_BIN" sync "$replica_b" --state "$state_b"
}

sync_cold() {
  "$OFS_BIN" sync "$cold_replica" --state "$cold_state"
}

init_a() {
  "$OFS_BIN" sync "$replica_a" --state "$state_a" --init "${target_options[@]}"
}

attach_b() {
  "$OFS_BIN" sync "$replica_b" --state "$state_b" "${target_options[@]}"
}

attach_cold() {
  "$OFS_BIN" sync "$cold_replica" --state "$cold_state" "${target_options[@]}"
}

establish_pair() {
  init_a >/dev/null
  printf '%s\n' 'private before sync' >"$replica_a/first.txt"
  attach_b >/dev/null
  [[ ! -e "$replica_b/first.txt" ]] || fail 'volume creation published a local file without sync'
  sync_a >/dev/null
  sync_b >/dev/null
  cmp "$replica_a/first.txt" "$replica_b/first.txt" || \
    fail 'empty replica did not materialize first.txt'
}
