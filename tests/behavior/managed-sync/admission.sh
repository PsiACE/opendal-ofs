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

printf '%s\n' 'admission: require explicit initialization for an absent Managed format'
if attach_b >"$OFS_CASE_ROOT/no-init.err" 2>&1; then
  fail 'ordinary sync implicitly created an absent Managed format'
fi
[[ ! -e "$state_b" ]] || fail 'failed attachment created replica state'

printf '%s\n' 'admission: initialize once and reopen from independent replica states'
init_a >/dev/null
first_status=$("$OFS_BIN" status --state "$state_a" --json)
first_volume=$(json_field "$first_status" volume_id)

"$OFS_BIN" sync "$replica_b" --state "$state_b" --init "${target_options[@]}" >/dev/null
attach_cold >/dev/null
for state in "$state_b" "$cold_state"; do
  [[ "$(json_field "$("$OFS_BIN" status --state "$state" --json)" volume_id)" == \
    "$first_volume" ]] || fail 'fresh replica state discovered another Managed volume identity'
done
sync_a >/dev/null
sync_b >/dev/null
sync_cold >/dev/null

printf '%s\n' 'admission: reject target changes and credential-bearing locators'
state_before=$(b3sum "$state_a")
wrong_storage=${target_options[3]/managed-sync/managed-sync-other}
if "$OFS_BIN" sync "$replica_a" --state "$state_a" \
  --model managed --storage "$wrong_storage" >/dev/null 2>&1; then
  fail 'an existing replica accepted another Managed target'
fi
[[ "$(b3sum "$state_a")" == "$state_before" ]] || \
  fail 'target mismatch changed replica state'

credential_state="$OFS_CASE_ROOT/state/credential-bearing.json"
credential_replica="$OFS_CASE_ROOT/credential-bearing-replica"
if "$OFS_BIN" sync "$credential_replica" --state "$credential_state" --init \
  --model managed --storage "${target_options[3]}&access_key_id=forbidden" \
  >/dev/null 2>&1; then
  fail 'a credential-bearing storage locator was accepted'
fi
[[ ! -e "$credential_state" ]] || fail 'credential-bearing locator created replica state'

printf '%s\n' 'admission: initialization cannot add an extension to an existing format'
extension_state="$OFS_CASE_ROOT/state/extension-mismatch.json"
extension_replica="$OFS_CASE_ROOT/extension-mismatch-replica"
if "$OFS_BIN" sync "$extension_replica" --state "$extension_state" --init --enable branch \
  "${target_options[@]}" >/dev/null 2>&1; then
  fail 'a branch extension request changed an existing base Managed volume'
fi
[[ ! -e "$extension_state" ]] || fail 'extension mismatch created replica state'

printf 'managed-sync admission passed (%s metadata)\n' "$OFS_METADATA_MODE"
