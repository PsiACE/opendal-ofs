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

set -euo pipefail

fail() {
  printf 'managed-sync acceptance: %s\n' "$*" >&2
  exit 1
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
  object)
    [[ -z "$OFS_METADATA_URL" ]] || fail 'object metadata uses OFS_STORAGE_URL; unset OFS_METADATA_URL'
    ;;
  d1)
    [[ -n "$OFS_METADATA_URL" ]] || fail 'd1 metadata requires OFS_METADATA_URL'
    ;;
  *)
    fail "OFS_METADATA_MODE must be object or d1, got: $OFS_METADATA_MODE"
    ;;
esac

config="$OFS_CASE_ROOT/client/config.json"
cold_config="$OFS_CASE_ROOT/cold-client/config.json"
replica_a="$OFS_CASE_ROOT/replica-a"
replica_b="$OFS_CASE_ROOT/replica-b"
cold_replica="$OFS_CASE_ROOT/cold-replica"
state_a="$OFS_CASE_ROOT/state/replica-a.json"
state_b="$OFS_CASE_ROOT/state/replica-b.json"
cold_state="$OFS_CASE_ROOT/state/cold-replica.json"

mkdir -p "$(dirname "$config")" "$(dirname "$cold_config")" \
  "$replica_a" "$replica_b" "$cold_replica" "$(dirname "$state_a")"

volume_create=(volume create workspace --model managed --storage "$OFS_STORAGE_URL")
if [[ "$OFS_METADATA_MODE" == d1 ]]; then
  volume_create+=(--metadata "$OFS_METADATA_URL")
fi

printf '%s\n' 'acceptance: create and reopen one managed volume'
OFS_CONFIG="$config" "$OFS_BIN" "${volume_create[@]}"
OFS_CONFIG="$config" "$OFS_BIN" "${volume_create[@]}"

printf '%s\n' 'private before sync' >"$replica_a/first.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
[[ ! -e "$replica_b/first.txt" ]] || fail 'volume creation published a local file without sync'

printf '%s\n' 'acceptance: first publication and empty-replica materialization'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
cmp "$replica_a/first.txt" "$replica_b/first.txt" || fail 'empty replica did not materialize first.txt'

printf '%s\n' 'acceptance: publish nested, empty, executable, and large files'
mkdir -p "$replica_a/nested/level" "$replica_a/tools"
printf '%s\n' 'created in a nested directory' >"$replica_a/nested/level/entry.txt"
printf '%s\n' 'removed after publication' >"$replica_a/nested/level/removed.txt"
: >"$replica_a/empty.bin"
dd if=/dev/zero of="$replica_a/large.bin" bs=1048576 count=8 2>/dev/null
printf '%s\n' '#!/bin/sh' 'printf "managed sync executable\\n"' >"$replica_a/tools/run.sh"
chmod u+x "$replica_a/tools/run.sh"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
[[ -d "$replica_b/nested/level" ]] || fail 'nested directories were not materialized'
cmp "$replica_a/nested/level/entry.txt" "$replica_b/nested/level/entry.txt" || \
  fail 'nested file content did not round trip'
[[ ! -s "$replica_b/empty.bin" ]] || fail 'empty file did not round trip'
cmp "$replica_a/large.bin" "$replica_b/large.bin" || fail 'large file did not round trip'
if [[ "$(uname -s)" != MINGW* && "$(uname -s)" != MSYS* ]]; then
  [[ -x "$replica_b/tools/run.sh" ]] || fail 'executable bit did not round trip'
fi

printf '%s\n' 'acceptance: modify, rename, and delete remote entries'
printf '%s\n' 'modified before rename' >"$replica_a/nested/level/entry.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
grep -Fxq 'modified before rename' "$replica_b/nested/level/entry.txt" || \
  fail 'remote file modification was not materialized'
mv "$replica_a/nested/level/entry.txt" "$replica_a/nested/renamed.txt"
rm "$replica_a/nested/level/removed.txt"
rmdir "$replica_a/nested/level"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
[[ ! -e "$replica_b/nested/level" ]] || fail 'deleted remote directory remained locally'
[[ ! -e "$replica_b/nested/level/entry.txt" ]] || fail 'old remote rename path remained locally'
[[ ! -e "$replica_b/nested/level/removed.txt" ]] || fail 'deleted remote file remained locally'
grep -Fxq 'modified before rename' "$replica_b/nested/renamed.txt" || \
  fail 'remote file rename was not materialized'

printf '%s\n' 'published by replica a' >"$replica_a/a-only.txt"
printf '%s\n' 'published by replica b' >"$replica_b/b-only.txt"
printf '%s\n' 'acceptance: merge disjoint changes from two replicas'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
cmp "$replica_a/a-only.txt" "$replica_b/a-only.txt" || fail 'replica b lost replica a disjoint change'
cmp "$replica_a/b-only.txt" "$replica_b/b-only.txt" || fail 'replica a lost replica b disjoint change'

printf '%s\n' 'common base' >"$replica_a/shared.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
printf '%s\n' 'candidate from replica a' >"$replica_a/shared.txt"
printf '%s\n' 'candidate from replica b' >"$replica_b/shared.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"

printf '%s\n' 'acceptance: retain and report a same-path conflict'
if OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"; then
  fail 'same-path concurrent edits succeeded without an explicit resolution'
fi
grep -Fxq 'candidate from replica a' "$replica_a/shared.txt" || fail 'remote conflict candidate was lost'
grep -Fxq 'candidate from replica b' "$replica_b/shared.txt" || fail 'local conflict candidate was lost'
conflict_status=$(OFS_CONFIG="$config" "$OFS_BIN" status "$replica_b" --state "$state_b" --json)
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*1' <<<"$conflict_status" || \
  fail 'status did not report the unresolved conflict'

printf '%s\n' 'acceptance: resolve explicitly with the retained local candidate'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b" --resolve shared.txt
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
grep -Fxq 'candidate from replica b' "$replica_a/shared.txt" || fail 'resolved content was not published'

printf '%s\n' 'acceptance: rebuild a cold client from remote authority'
OFS_CONFIG="$cold_config" "$OFS_BIN" "${volume_create[@]}"
OFS_CONFIG="$cold_config" "$OFS_BIN" sync workspace "$cold_replica" --state "$cold_state"
diff -ru "$replica_a" "$cold_replica" || fail 'cold replica does not match the published tree'
if [[ "$(uname -s)" != MINGW* && "$(uname -s)" != MSYS* ]]; then
  [[ -x "$cold_replica/tools/run.sh" ]] || fail 'cold rebuild lost executable bit'
fi

before_noop=$(cd "$cold_replica" && find . -type f -exec sha256sum {} + | LC_ALL=C sort)
OFS_CONFIG="$cold_config" "$OFS_BIN" sync workspace "$cold_replica" --state "$cold_state"
after_noop=$(cd "$cold_replica" && find . -type f -exec sha256sum {} + | LC_ALL=C sort)
[[ "$before_noop" == "$after_noop" ]] || fail 'a no-op sync changed user-visible files'

printf '%s\n' 'acceptance: status exposes the selected models without secrets'
status_json=$(OFS_CONFIG="$cold_config" "$OFS_BIN" status "$cold_replica" --state "$cold_state" --json)
grep -Eq '"volume_model"[[:space:]]*:[[:space:]]*"managed"' <<<"$status_json" || \
  fail 'status did not report volume_model=managed'
grep -Eq '"access_model"[[:space:]]*:[[:space:]]*"sync"' <<<"$status_json" || \
  fail 'status did not report access_model=sync'
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*0' <<<"$status_json" || \
  fail 'status still reports conflicts after explicit resolution'

for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN OFS_D1_TOKEN; do
  secret=${!name:-}
  if [[ -n "$secret" ]] && grep -Fq -- "$secret" <<<"$status_json"; then
    fail "status leaked credential from $name"
  fi
done
while IFS= read -r secret; do
  if [[ -n "$secret" ]] && grep -Fq -- "$secret" <<<"$status_json"; then
    fail 'status leaked a value listed in OFS_SECRET_PROBES'
  fi
done <<<"${OFS_SECRET_PROBES:-}"

printf 'managed-sync acceptance passed (%s metadata)\n' "$OFS_METADATA_MODE"
