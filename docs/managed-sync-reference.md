# Managed Sync reference

This reference describes the public Managed Sync commands, configuration,
status vocabulary, supported filesystem surface, storage layout, and D1
requirements.

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

For settings that have all four forms, resolution is command line, then
environment, then catalog, then the built-in default. In particular,
`--transfer-concurrency` overrides `OFS_SYNC_TRANSFER_CONCURRENCY`, which
overrides `sync.transfer_concurrency` in the catalog; a newly created catalog
uses four. `--config` overrides `OFS_CONFIG`, followed by the XDG/HOME default
location. `--config` is global and appears before the subcommand.

During `volume create`, credential query values in `--storage` and `--metadata`
override values in the corresponding environment URL. Later commands use the
credential-free locator from the catalog with credentials from the environment.
Every credential-bearing URL must resolve to the same credential-free locator;
an environment variable cannot redirect a named volume to another root.

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
entry nor a new data object. `metadata/head` is overwritten in place. Its ETag
is the CAS authority token, and a successful conditional write returns the
token for the new observation. Commits and data versions accumulate.

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

Logical storage use grows as:

```text
data bytes     = bytes in every unique content digest ever published
metadata bytes = fixed format/head + checkpoints + sum of commit records
```

Failed or losing publications can also leave immutable commit or data objects
that are not reachable from the head. Managed Sync has no command that removes
historical or unreachable objects.

Safe compaction requires a full checkpoint published with a conditional head
update before earlier commits can be pruned. Data collection must trace every
retained checkpoint and subsequent commit, preserve a retention window for
offline replicas and idempotent recovery, and delay deletion of newly uploaded
unreferenced blobs so it cannot race an in-flight publication. Managed Sync
does not provide these retention and garbage-collection operations.

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

- `ofs_managed_v1_schema`
- `ofs_managed_v1_formats`
- `ofs_managed_v1_heads`
- `ofs_managed_v1_commits`
- `ofs_managed_v1_checkpoints`

`STORE_KEY` isolates one Managed Volume within those tables. Reusing a store
key with a different Data Store binding is rejected. D1 query results must
report `served_by_primary=true`; ofs does not treat an unproven replica result
as an authoritative observation.

D1 uses the head row's integer revision as the same CAS authority boundary as
an object ETag. A successful conditional update increments and returns the
replacement revision; it does not duplicate the namespace generation as a
second authority mechanism.
Read and idempotent statements retry transient transport, rate-limit, service,
and non-authoritative responses with bounded jittered backoff. The publication
statement is never retried blindly because a lost response can leave its result
unknown.

D1 is the Metadata Store only. File bytes remain in the configured OpenDAL
Data Store.

## Status JSON

| Field | Values |
| --- | --- |
| `format_version` | `1` |
| `volume.name`, `volume.id`, `volume.model` | Bound named Managed Volume |
| `access` | `sync` |
| `local` | `clean` or `changed` |
| `local_error` | Stable error kind and diagnostic message when local inspection fails |
| `base.generation` | Durable common generation, or absent before binding |
| `remote.state` | `at_base`, `ahead`, `behind`, `diverged`, or `unknown` |
| `remote.generation` | Present only after a successful live authority observation |
| `remote.error` | Stable error kind and diagnostic message when live observation fails |
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

The D1 change-log reader fetches the fixed generation interval in one ordered
query, then verifies the parent-cursor ancestry in memory. Payload size still
grows with the number and size of missed changes, but network round trips do
not grow once per generation. There is no
background daemon, periodic checkpoint, change-set merge, history browser,
remote volume delete command, remote retention policy, or garbage collector.
