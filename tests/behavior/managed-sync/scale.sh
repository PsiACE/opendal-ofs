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

printf '%s\n' 'scale: catch up through a long publication history'
establish_pair
for generation in $(seq 1 60); do
  printf 'history change %s\n' "$generation" >"$replica_a/checkpoint.txt"
  sync_a >/dev/null
done
sync_b >/dev/null
grep -Fxq 'history change 60' "$replica_b/checkpoint.txt" || \
  fail 'replica did not recover the fixed target after a long change history'

printf 'managed-sync scale passed (%s metadata)\n' "$OFS_METADATA_MODE"
