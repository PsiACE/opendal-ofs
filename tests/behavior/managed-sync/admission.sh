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
# shellcheck source=tests/behavior/managed-sync/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

alternate_locator() {
  local url=$1 alternate
  alternate="${url%%\?*}-other"
  [[ "$url" != *\?* ]] || alternate+="?${url#*\?}"
  printf '%s\n' "$alternate"
}

printf '%s\n' 'admission: require explicit initialization for an absent Managed format'
if sync_b >"$OFS_CASE_ROOT/no-init.err" 2>&1; then
  fail 'ordinary sync implicitly created an absent Managed format'
fi
[[ ! -e "$state_b" ]] || fail 'failed attachment created replica state'

printf '%s\n' 'admission: initialize once and reopen from independent replica states'
init_a >/dev/null
first_volume=$(json_field "$("$OFS_BIN" status --state "$state_a" --json)" volume_id)

sync_b >/dev/null
[[ "$(json_field "$("$OFS_BIN" status --state "$state_b" --json)" volume_id)" == \
  "$first_volume" ]] || fail 'fresh replica state discovered another Managed volume identity'

printf '%s\n' 'admission: reject another existing volume for the same replica state'
other_environment=(env OFS_STORAGE_URL="$(alternate_locator "$OFS_STORAGE_URL")")
if [[ "$OFS_METADATA_MODE" == d1 ]]; then
  other_environment+=(OFS_METADATA_URL="$(alternate_locator "$OFS_METADATA_URL")")
fi
other_state="$OFS_CASE_ROOT/state/other-volume.json"
other_replica="$OFS_CASE_ROOT/other-volume-replica"
invalid_state="$OFS_CASE_ROOT/state/invalid-init.json"
if "${other_environment[@]}" "$OFS_BIN" sync "$other_replica" --state "$invalid_state" \
  --init --model managed --branch experiment >/dev/null 2>&1; then
  fail 'initialization accepted a branch before the branch format existed'
fi
[[ ! -e "$invalid_state" ]] || fail 'invalid initialization created replica state'
"${other_environment[@]}" "$OFS_BIN" sync "$other_replica" --state "$other_state" \
  --init --model managed --enable branch >/dev/null
other_volume=$(json_field "$("$OFS_BIN" status --state "$other_state" --json)" volume_id)
[[ "$other_volume" != "$first_volume" ]] || fail 'independent initialization reused a VolumeId'

state_before=$(b3sum "$state_a")
tree_before=$(tree_digest "$replica_a")
if "${other_environment[@]}" "$OFS_BIN" sync "$replica_a" --state "$state_a" \
  >/dev/null 2>&1; then
  fail 'an existing replica accepted another Managed volume'
fi
[[ "$(b3sum "$state_a")" == "$state_before" ]] || \
  fail 'volume mismatch changed replica state'
[[ "$(tree_digest "$replica_a")" == "$tree_before" ]] || \
  fail 'volume mismatch changed replica files'

printf '%s\n' 'admission: initialization cannot add an extension to an existing format'
extension_state="$OFS_CASE_ROOT/state/extension-mismatch.json"
extension_replica="$OFS_CASE_ROOT/extension-mismatch-replica"
if "$OFS_BIN" sync "$extension_replica" --state "$extension_state" --init --enable branch \
  --model managed >/dev/null 2>&1; then
  fail 'a branch extension request changed an existing base Managed volume'
fi
[[ ! -e "$extension_state" ]] || fail 'extension mismatch created replica state'

printf 'managed-sync admission passed (%s metadata)\n' "$OFS_METADATA_MODE"
