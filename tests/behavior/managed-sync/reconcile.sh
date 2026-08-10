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

printf '%s\n' 'reconcile: establish two replicas'
establish_pair
mkdir -p "$replica_a/nested/level"
printf '%s\n' 'created in a nested directory' >"$replica_a/nested/level/entry.txt"
printf '%s\n' 'removed after publication' >"$replica_a/nested/level/removed.txt"
sync_a >/dev/null
sync_b >/dev/null

printf '%s\n' 'reconcile: merge disjoint directory changes from two replicas'
mkdir -p "$replica_a/from-a/empty" "$replica_b/from-b/empty"
printf '%s\n' 'nested change from a' >"$replica_a/from-a/value.txt"
printf '%s\n' 'nested change from b' >"$replica_b/from-b/value.txt"
sync_a >/dev/null
sync_b >/dev/null
sync_a >/dev/null
[[ -d "$replica_a/from-a/empty" && -d "$replica_a/from-b/empty" ]] || \
  fail 'replica a did not merge disjoint empty directories'
[[ -d "$replica_b/from-a/empty" && -d "$replica_b/from-b/empty" ]] || \
  fail 'replica b did not merge disjoint empty directories'
cmp "$replica_a/from-a/value.txt" "$replica_b/from-a/value.txt" || \
  fail 'replica b lost replica a nested directory change'
cmp "$replica_a/from-b/value.txt" "$replica_b/from-b/value.txt" || \
  fail 'replica a lost replica b nested directory change'

printf '%s\n' 'reconcile regression: resume a partially applied remote directory change'
mkdir -p "$replica_a/partial-before/child/empty"
printf '%s\n' 'remote directory content' >"$replica_a/partial-before/child/value.txt"
sync_a >/dev/null
sync_b >/dev/null
mv "$replica_a/partial-before" "$replica_a/partial-after"
sync_a >/dev/null
mkdir "$replica_b/partial-after"
partial_resume=$(sync_b)
if grep -Fq '(published)' <<<"$partial_resume"; then
  fail 'partial remote directory installation was republished as a local change'
fi
[[ ! -e "$replica_b/partial-before" ]] || fail 'partial remote directory removal did not resume'
[[ -d "$replica_b/partial-after/child/empty" ]] || \
  fail 'partial remote directory creation did not resume'
cmp "$replica_a/partial-after/child/value.txt" "$replica_b/partial-after/child/value.txt" || \
  fail 'resumed remote directory installation lost file content'

printf '%s\n' 'reconcile: publish one-sided file and directory replacements'
printf '%s\n' 'replace this file with a directory' >"$replica_a/file-to-directory"
mkdir "$replica_a/directory-to-file"
sync_a >/dev/null
sync_b >/dev/null
rm "$replica_a/file-to-directory"
mkdir "$replica_a/file-to-directory"
printf '%s\n' 'now nested' >"$replica_a/file-to-directory/value.txt"
rmdir "$replica_a/directory-to-file"
printf '%s\n' 'now a file' >"$replica_a/directory-to-file"
sync_a >/dev/null
sync_b >/dev/null
[[ -d "$replica_b/file-to-directory" ]] || fail 'remote file-to-directory replacement was rejected'
grep -Fxq 'now nested' "$replica_b/file-to-directory/value.txt" || \
  fail 'remote replacement directory lost its content'
[[ -f "$replica_b/directory-to-file" ]] || fail 'remote directory-to-file replacement was rejected'
grep -Fxq 'now a file' "$replica_b/directory-to-file" || \
  fail 'remote replacement file lost its content'

printf '%s\n' 'reconcile regression: reject a remote deletion overlapping a local subtree change'
mkdir -p "$replica_a/overlap"
printf '%s\n' 'base' >"$replica_a/overlap/value.txt"
sync_a >/dev/null
sync_b >/dev/null
printf '%s\n' 'changed locally' >"$replica_a/overlap/value.txt"
rm -rf -- "$replica_b/overlap"
sync_b >/dev/null
overlap_tree=$(tree_digest "$replica_a")
overlap_state=$(sha256sum "$state_a")
if sync_a 2>"$OFS_CASE_ROOT/directory-overlap.err"; then
  fail 'overlapping remote directory deletion replaced a local subtree change'
fi
grep -Fq 'directory deletion overlaps local changes' "$OFS_CASE_ROOT/directory-overlap.err" || \
  fail 'directory overlap error was not actionable'
[[ "$(tree_digest "$replica_a")" == "$overlap_tree" ]] || \
  fail 'directory overlap rejection changed user files'
[[ "$(sha256sum "$state_a")" == "$overlap_state" ]] || \
  fail 'directory overlap rejection changed replica state'
rm -rf -- "$replica_a/overlap"
sync_a >/dev/null

printf '%s\n' 'reconcile: modify, rename, delete, and move remote entries'
printf '%s\n' 'modified before rename' >"$replica_a/nested/level/entry.txt"
sync_a >/dev/null
sync_b >/dev/null
grep -Fxq 'modified before rename' "$replica_b/nested/level/entry.txt" || \
  fail 'remote file modification was not materialized'
mv "$replica_a/nested/level/entry.txt" "$replica_a/nested/renamed.txt"
rm "$replica_a/nested/level/removed.txt"
rmdir "$replica_a/nested/level"
sync_a >/dev/null
sync_b >/dev/null
[[ ! -e "$replica_b/nested/level" ]] || fail 'deleted remote directory remained locally'
grep -Fxq 'modified before rename' "$replica_b/nested/renamed.txt" || \
  fail 'remote file rename was not materialized'
mkdir -p "$replica_a/tree-before/branch/empty"
printf '%s\n' 'directory identity survives a move' >"$replica_a/tree-before/branch/leaf.txt"
sync_a >/dev/null
sync_b >/dev/null
mv "$replica_a/tree-before" "$replica_a/tree-after"
sync_a >/dev/null
sync_b >/dev/null
[[ ! -e "$replica_b/tree-before" ]] || fail 'old directory move path remained remotely'
[[ -d "$replica_b/tree-after/branch/empty" ]] || fail 'moved empty directory was not materialized'
grep -Fxq 'directory identity survives a move' "$replica_b/tree-after/branch/leaf.txt" || \
  fail 'moved directory subtree content was not materialized'

printf '%s\n' 'reconcile: retain, report, and explicitly resolve same-path conflicts'
printf '%s\n' 'common base' >"$replica_a/shared.txt"
printf '%s\n' 'second common base' >"$replica_a/shared-two.txt"
sync_a >/dev/null
sync_b >/dev/null
printf '%s\n' 'candidate from replica a' >"$replica_a/shared.txt"
printf '%s\n' 'second candidate from replica a' >"$replica_a/shared-two.txt"
printf '%s\n' 'candidate from replica b' >"$replica_b/shared.txt"
printf '%s\n' 'second candidate from replica b' >"$replica_b/shared-two.txt"
sync_a >/dev/null
if sync_b; then
  fail 'same-path concurrent edits succeeded without an explicit resolution'
fi
grep -Fxq 'candidate from replica a' "$replica_a/shared.txt" || fail 'remote conflict candidate was lost'
grep -Fxq 'candidate from replica b' "$replica_b/shared.txt" || fail 'local conflict candidate was lost'
grep -Fxq 'second candidate from replica a' "$replica_a/shared-two.txt" || \
  fail 'second remote conflict candidate was lost'
grep -Fxq 'second candidate from replica b' "$replica_b/shared-two.txt" || \
  fail 'second local conflict candidate was lost'
conflict_status=$(OFS_CONFIG="$peer_config" "$OFS_BIN" status --state "$state_b" --json)
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*2' <<<"$conflict_status" || \
  fail 'status did not report both unresolved conflicts'
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b" \
  --resolve shared.txt --resolve shared-two.txt >/dev/null
sync_a >/dev/null
grep -Fxq 'candidate from replica b' "$replica_a/shared.txt" || fail 'resolved content was not published'
grep -Fxq 'second candidate from replica b' "$replica_a/shared-two.txt" || \
  fail 'second resolved content was not published'

printf 'managed-sync reconcile passed (%s metadata)\n' "$OFS_METADATA_MODE"
