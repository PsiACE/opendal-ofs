# Managed Sync reference

This reference describes the public Managed Sync commands, configuration,
status vocabulary, supported filesystem surface, D1 requirements, and current
behavior validation.

## Commands

### Create or reopen a volume

```console
ofs [--config CATALOG] volume create NAME \
  --model managed \
  --storage STORAGE_URL \
  [--metadata METADATA_LOCATOR]
```

The command creates a new remote Managed Volume or reopens an existing volume
at the same metadata scope. Reopening requires the same metadata placement and
Data Store binding. A mismatch fails before the local catalog is written.

Omitting `--metadata` selects colocated object metadata. Supplying a D1 locator
selects external D1 metadata.

### Synchronize one replica

```console
ofs [--config CATALOG] sync NAME DIRECTORY \
  [--state STATE_DIRECTORY] \
  [--resolve PATH]... \
  [--require CAPABILITY]... \
  [--transfer-concurrency N]
```

The command performs one recovery and reconciliation pass, prints the final
generation, and exits. `DIRECTORY` may be non-empty only for the first
publication to an empty volume or when it already has a matching durable
replica state.

When `--state` is omitted, state is stored in a sibling directory named
`.DIRECTORY_NAME.ofs-state`. The state directory must remain outside the
synchronized tree.

`--resolve` selects the current local shape for a retained conflict path. It
may be repeated. `--require` fails before scan or mutation when a requested
public capability is unavailable.

An empty directory is a fresh replica only when it has no established replica
state. Recreating an empty directory while reusing its previous state presents
the missing paths as local deletions from the common base.

`--transfer-concurrency` is a positive integer. The catalog default is four.
It bounds complete content transfer and local installation jobs; OpenDAL owns
provider request limits, retry behavior, and object-level read/write options.

### Inspect a replica

```console
ofs [--config CATALOG] status DIRECTORY [--state STATE_DIRECTORY] [--json]
```

Status reads the local replica and attempts a live authority observation. It
does not reconcile, publish, materialize, or change local or remote state.

## Configuration

| Setting | Purpose |
| --- | --- |
| `--config`, `OFS_CONFIG` | Credential-free volume catalog path |
| `OFS_STORAGE_URL` | Storage URL and credentials used after volume creation |
| `OFS_METADATA_URL` | D1 locator plus token used for create, sync, and status |
| `OFS_SYNC_TRANSFER_CONCURRENCY` | Default override for one sync invocation |
| `XDG_CONFIG_HOME`, `HOME` | Default catalog location when `--config` and `OFS_CONFIG` are absent |

Command-line values take precedence over catalog defaults. `--config` is a
global option and appears before the subcommand.

The catalog stores storage and metadata locators, volume IDs, and transfer
defaults. Query credentials whose names contain token, secret, password,
access key, private key, authorization, or signature are removed before the
catalog is persisted. The catalog file is written with owner-only permissions
on Unix.

## Storage URL

Managed content uses an OpenDAL URL. The standard S3 form is:

```text
s3://?bucket=BUCKET&root=ROOT&endpoint=ENDPOINT&region=REGION&access_key_id=ACCESS&secret_access_key=SECRET
```

The selected service must support read, write, and create-only immutable
writes. Colocated object metadata additionally requires list with limit,
conditional write, and create-only write.

### Colocated object layout

All keys are relative to the configured storage root.

| Key | Contents | Mutation rule |
| --- | --- | --- |
| `metadata/format` | Volume identity, metadata placement, and Data Store binding | Created once |
| `metadata/head` | Current cursor and checkpoint cursor | Replaced with an ETag-conditional write |
| `metadata/checkpoints/initial` | Full empty generation-0 manifest | Immutable |
| `metadata/commits/OPERATION_ID` | Parent cursor, new cursor, and namespace changes | One immutable object per published generation |
| `data/sha256/DIGEST` | Raw file bytes | Immutable and deduplicated by SHA-256 |

The first non-empty commit contains one `put` for every directory and file.
Later commits contain only `diff(remote, merged_target)`: creates, changes, and
removals for affected paths. An unchanged file creates neither a new commit
entry nor a new data object. `metadata/head` is overwritten in place, while
commits and data versions accumulate.

A file `put` stores one content reference containing the full SHA-256 digest
and byte size. The Data Store key is derived from that digest; the metadata
does not repeat a `data_ref` string.

```text
G0  format + head(G0) + checkpoints/initial(empty)
 |
 | first publication: full namespace commit + every unique live file digest
 v
G1  commits/O1(full) + data/sha256/... + head -> G1
 |
 | incremental publication: changed-path commit + only new file digests
 v
G2  commits/O2(delta) + additional data/sha256/... + head -> G2
 |
 v
Gn  every earlier immutable commit and digest is still present
```

With external D1 metadata, S3 contains only `data/sha256/DIGEST`. The format,
head, immutable commits, and checkpoints are stored under the selected
`STORE_KEY` in the D1 tables listed below.

### Growth and retention

Current logical growth is approximately:

```text
data bytes     = bytes in every unique content digest ever published
metadata bytes = fixed format/head + checkpoints + sum of commit records
```

Failed or losing publications can also leave immutable commit or data objects
that are not reachable from the head. No current command removes historical
or unreachable objects.

In a measured 1,000-file, 100-update tiny-file workload, G101 retained 1,800
data objects and 101 commits. The 69,726-byte live tree occupied 130,526 bytes
of content objects and 837,214 bytes of metadata: 1.87x, 12.01x, and 13.88x
combined. G1 contributed a 420,499-byte full namespace commit; each later
20-change commit averaged about 4,161 bytes. Thus routine publication was
incremental, but retained history and namespace representation dominated the
small live payload.

Removing the derivable content-location string reduced the same workload's G1
commit by exactly 85,000 bytes and G101 metadata by 153,000 bytes. Data object
count and bytes did not change. This removes 85 bytes per file `put`; it does
not address retained commit history, old content versions, or provider
per-object overhead.

A future compactor must publish a full checkpoint with a conditional head
update before pruning earlier commits. Data collection must trace every
retained checkpoint and subsequent commit, preserve a retention window for
offline replicas and idempotent recovery, and delay deletion of newly uploaded
unreferenced blobs so it cannot race an in-flight publication. These retention
and garbage-collection operations are not implemented.

## D1 metadata locator

The credential-free catalog form is:

```text
d1://ACCOUNT_ID/DATABASE_ID/STORE_KEY
```

The runtime credential form is:

```text
d1://ACCOUNT_ID/DATABASE_ID/STORE_KEY?token=API_TOKEN
```

Set the runtime form in `OFS_METADATA_URL` and pass the credential-free form to
`--metadata`. Both forms must resolve to the same account, database, and store
key. A mismatch fails before a D1 statement is executed.

The token must allow D1 Query API access. Initialization executes idempotent
schema creation and requires `CREATE TABLE`, `SELECT`, `INSERT`, and `UPDATE`
on the selected database. The five shared tables are:

- `ofs_managed_schema`
- `ofs_managed_formats`
- `ofs_managed_heads`
- `ofs_managed_commits`
- `ofs_managed_checkpoints`

`STORE_KEY` isolates one Managed Volume within those tables. Reusing a store
key with a different Data Store binding is rejected. D1 query results must
report `served_by_primary=true`; ofs does not treat an unproven replica result
as an authoritative observation.

D1 is the Metadata Store only. File bytes remain in the configured OpenDAL
Data Store.

## Status JSON

| Field | Values |
| --- | --- |
| `format_version` | `1` |
| `volume.name`, `volume.id`, `volume.model` | Bound named Managed Volume |
| `access` | `sync` |
| `local` | `clean` or `changed` |
| `base.generation` | Durable common generation, or absent before binding |
| `remote.state` | `at_base`, `ahead`, `behind`, `diverged`, or `unknown` |
| `remote.generation` | Present only after a successful live authority observation |
| `publication` | `idle`, `pending`, or `conflict` |
| `materialize` | `idle` or `pending` |
| `conflicts` | Number of retained conflicts |
| `conflict_records` | Path and stable conflict kind |
| `metadata` | `colocated-object` or `external-d1` |
| `capabilities` | Admitted public capability names |

Conflict kinds are `same_node_modified`, `delete_vs_modify`,
`incompatible_type_replacement`, and `divergent_rename`.

## Admitted capabilities

Managed Sync reports these public capabilities:

- `atomic-snapshot`
- `change-feed`
- `conditional-publication`
- `conflict-retention`
- `idempotent-publication`
- `immutable-data`
- `local-replica`
- `offline-write`
- `portable-names`
- `stable-node-id`

## Filesystem support

| Supported | Rejected |
| --- | --- |
| Regular files | Symbolic links |
| Directories and empty directories | Hard links |
| Portable NFC names | Case-colliding or reserved portable names |
| Executable bit on Unix | ACLs, xattrs, locks, sparse semantics |
| Complete directory publication | Include/exclude filters and partial hydration |

Unsupported trees are rejected before a remote generation is advanced.

## Recovery and limitations

| Situation | Result |
| --- | --- |
| Local edit without sync | Remains private to that replica |
| Empty directory with no established replica state | Materializes the latest published generation |
| Replica behind several generations | Replays every consecutive change record to a fixed target |
| Local tree and replica state lost | Cold recovery from Metadata Store and Data Store |
| Local tree lost but established state retained | Use a fresh state path for cold recovery; reusing the old state treats missing paths as local deletions |
| Local catalog lost | Recreate the same definition, then cold sync |
| Same-path concurrent edits | Publication stops with a retained conflict |
| Concurrent first creation of the same absent directory | Coalesces the two new directory identities and merges children by path; overlapping child changes can still conflict |
| Authority unavailable during status | `remote.state` is `unknown` with no remote generation |
| Publication result unknown | Next sync resolves the durable operation before retrying |

The D1 change-log reader performs one query per missed generation. Large
generation gaps therefore increase foreground catch-up latency. There is no
background daemon, periodic checkpoint, change-set merge, history browser,
remote volume delete command, remote retention policy, or garbage collector.

## Current behavior validation

The checked-in acceptance assets exercise public commands and observable
filesystem or status results. Current provider-backed validation covers the
complete Managed Volume lifecycle with both colocated object metadata and a
real D1 Metadata Store backed by MinIO content storage.

- The full sanitized workspace contains 6,597 files and 662,007,352 logical
  bytes. Initial publication, another replica, incremental sync, no-op sync,
  and cold recovery converge to the same complete tree.
- The long-history workload keeps 1,000 files converged through 1,000 explicit
  publications, including stale readers and cold recovery.
- Checked-in directory-churn regressions hand publication from A to B after a
  complete catch-up and cover concurrent nested directory creation during
  initialization, upgrade, and recreation. A separate provider-backed run
  rotated 12 updates across four independent container volumes.
- Provider-backed lifecycle, conflict, recovery, and credential-boundary
  acceptance pass for their documented public behavior.

Run the provider-backed behavior suites with:

```console
cargo build --locked --release --bin ofs

OFS_BIN="$PWD/target/release/ofs" \
  tests/behavior/managed-sync/minio.sh lifecycle
OFS_BIN="$PWD/target/release/ofs" \
  tests/behavior/managed-sync/minio.sh scripted
OFS_BIN="$PWD/target/release/ofs" \
  tests/behavior/managed-sync/minio.sh recovery

set -a
. ./.env
set +a
OFS_BIN="$PWD/target/release/ofs" \
  tests/behavior/managed-sync/d1-minio.sh lifecycle
OFS_BIN="$PWD/target/release/ofs" \
  tests/behavior/managed-sync/d1-minio.sh scripted
```
