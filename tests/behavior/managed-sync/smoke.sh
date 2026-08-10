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

printf '%s\n' 'smoke: first publication and empty-replica materialization'
init_a >/dev/null
sync_b >/dev/null
empty_collection=$("$OFS_BIN" volume gc)
grep -Fq 'scanned=0 deleted=0 bytes=0' <<<"$empty_collection" || \
  fail 'an unpublished volume was not an empty collection'
printf '%s\n' 'private before sync' >"$replica_a/first.txt"
sync_b >/dev/null
sync_a >/dev/null
sync_b >/dev/null
cmp "$replica_a/first.txt" "$replica_b/first.txt" || \
  fail 'first publication failed after empty collection'

printf '%s\n' 'smoke: reject hard links before publication'
printf '%s\n' 'must remain local' >"$replica_a/hard-link-source.txt"
if ln "$replica_a/hard-link-source.txt" "$replica_a/hard-link-alias.txt" 2>/dev/null; then
  before_hard_link=$("$OFS_BIN" status --state "$state_a" --json)
  if sync_a >"$OFS_CASE_ROOT/hard-link.err" 2>&1; then
    fail 'hard-linked files were published'
  fi
  grep -Fq 'hard link' "$OFS_CASE_ROOT/hard-link.err" || fail 'hard-link rejection was not explicit'
  after_hard_link=$("$OFS_BIN" status --state "$state_a" --json)
  [[ "$before_hard_link" == "$after_hard_link" ]] || fail 'hard-link rejection changed replica state'
  rm "$replica_a/hard-link-alias.txt"
fi
rm "$replica_a/hard-link-source.txt"

printf '%s\n' 'smoke: reject ambiguous portable names before publication'
portable_state=$(b3sum "$state_a")
mkdir "$replica_a/portable-names"
: >"$replica_a/portable-names/CON"
if sync_a >"$OFS_CASE_ROOT/portable-name.err" 2>&1; then
  fail 'a platform-reserved name was published'
fi
grep -Fq 'non-portable path' "$OFS_CASE_ROOT/portable-name.err" || \
  fail 'reserved-name rejection was not actionable'
rm "$replica_a/portable-names/CON"
: >"$replica_a/portable-names/"$'e\u0301'
if sync_a >/dev/null 2>&1; then
  fail 'a non-normalized name was published'
fi
rm "$replica_a/portable-names/"$'e\u0301'
: >"$replica_a/portable-names/Foo"
: >"$replica_a/portable-names/foo"
if sync_a >/dev/null 2>&1; then
  fail 'a case-folding collision was published'
fi
rm -rf -- "$replica_a/portable-names"
[[ "$(b3sum "$state_a")" == "$portable_state" ]] || \
  fail 'portable-name rejection changed replica state'

printf '%s\n' 'smoke: publish nested, empty, executable, large, and reused content'
mkdir -p "$replica_a/nested/level" "$replica_a/tools"
printf '%s\n' 'created in a nested directory' >"$replica_a/nested/level/entry.txt"
: >"$replica_a/empty.bin"
dd if=/dev/zero of="$replica_a/large.bin" bs=1048576 count=80 2>/dev/null
printf '%s\n' '#!/bin/sh' 'printf "managed sync executable\\n"' >"$replica_a/tools/run.sh"
chmod u+x "$replica_a/tools/run.sh"
cp "$replica_a/first.txt" "$replica_a/reused-content.txt"
sync_a >/dev/null
sync_b >/dev/null
diff -ru "$replica_a" "$replica_b" || fail 'published tree did not round trip'

if [[ "$(uname -s)" != MINGW* && "$(uname -s)" != MSYS* ]]; then
  [[ -x "$replica_b/tools/run.sh" ]] || fail 'executable bit did not round trip'
  printf '%s\n' 'smoke regression: apply executable changes without content changes'
  chmod u-x "$replica_a/tools/run.sh"
  sync_a >/dev/null
  sync_b >/dev/null
  [[ ! -x "$replica_b/tools/run.sh" ]] || fail 'remote executable removal was not applied'
  chmod u+x "$replica_a/tools/run.sh"
  sync_a >/dev/null
  sync_b >/dev/null
  [[ -x "$replica_b/tools/run.sh" ]] || fail 'remote executable restoration was not applied'
fi

printf 'managed-sync smoke passed (%s metadata)\n' "$OFS_METADATA_MODE"
