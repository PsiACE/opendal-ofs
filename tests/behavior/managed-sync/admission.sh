#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail
# shellcheck source=tests/behavior/managed-sync/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

printf '%s\n' 'admission: register one remote volume under client-local aliases'
OFS_CONFIG="$config" "$OFS_BIN" volume create workspace "${volume_options[@]}" >/dev/null
OFS_CONFIG="$config" "$OFS_BIN" volume create workspace "${volume_options[@]}" >/dev/null
OFS_CONFIG="$peer_config" "$OFS_BIN" volume create "$peer_alias" \
  "${volume_options[@]}" >/dev/null

if OFS_CONFIG="$extension_mismatch_config" "$OFS_BIN" volume create branching-workspace \
  "${volume_options[@]}" --enable branch >/dev/null 2>&1; then
  fail 'a branch extension request changed an existing base Managed volume'
fi
[[ ! -e "$extension_mismatch_config" ]] || \
  fail 'extension mismatch wrote a local catalog binding'
if OFS_CONFIG="$config" "$OFS_BIN" volume create duplicate-workspace \
  "${volume_options[@]}" >/dev/null 2>&1; then
  fail 'one catalog registered the same volume identity under two aliases'
fi
printf 'managed-sync admission passed (%s metadata)\n' "$OFS_METADATA_MODE"
