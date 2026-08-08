# Managed Sync

Managed Sync reconciles an ordinary local directory with a Managed volume. The
local directory stays usable while disconnected. Remote publication happens
only when you run `ofs sync`.

This guide explains the Managed Sync contract and its local acceptance
environment. The filesystem concepts come from
[RFC 016](../rfcs/0016_filesystem_architecture.md) and are not specific to
Sync.

## The two RFC 016 axes

RFC 016 separates namespace authority from the way a user accesses it:

| Axis | Choices | Meaning |
| --- | --- | --- |
| Volume Model | Direct, Managed | Who owns the remote namespace and its filesystem identities |
| Access Model | Mount, Sync | Whether the remote view or a materialized local tree is the working filesystem |

`NodeId`, `FileVersion`, `Generation`, `DirectoryEntry`, `ChangeCursor`,
capabilities, errors, and publication outcomes are shared filesystem semantics.
A Managed volume uses the same authoritative identities and publication rules
whether it is opened through Mount or Sync. Direct volumes use the same
vocabulary where it applies, with storage object versions and paths supplying
their weaker identity model.

The named-volume commands in this guide assemble a Managed volume with Sync
access. Sync adds durable local intent, a common base, three-way reconciliation,
retained conflicts, and local materialization. Those concerns do not belong to
the Managed volume format.

## Runtime storage concurrency

Sync, Mount, pack, reindex, and garbage collection accept
`--transfer-concurrency N`; `OFS_TRANSFER_CONCURRENCY` provides the same
setting. The default is four, and the value must be greater than zero. The
assembled OpenDAL operator uses it as its operation limit, and Sync also uses
it to bound publication and materialization work. Volume creation and `status`
do not perform parallel transfers and therefore do not accept the option. This
is runtime configuration, not part of a Volume Model, Access Model, or durable
format.

## Managed volumes in Sync

Sync treats Managed metadata and file data as one remote Volume contract. It
does not interpret metadata checkpoints, file manifests, packs, or secondary
indexes. The volume validates its storage format and required extensions before
Sync scans or publishes anything.

Format v1 uses FastCDC at or above 1 MiB and whole-file content below that
threshold. Packing is explicit maintenance and does not change the
filesystem-visible file version, node generation, or change cursor.

Data is written and verified before metadata can reference it. If publication
fails after that write, the result may be an unreachable loose object. The
namespace remains valid, and explicit garbage collection can reclaim the
object later.

During an update, the fixed parent snapshot is also proof that its reachable
`ContentRef` values are durable. Sync still reads and hashes changed local
input, but it does not probe or download matching content again. New content
keeps the same create-only write and read-back verification. This applies to
whole files and FastCDC chunks without changing Sync semantics.

## Create an Object metadata volume on MinIO

Keep provider credentials in the environment. The volume catalog stores only
the credential-free storage URL and the stable volume binding.

```shell
export OFS_CONFIG="$PWD/ofs-volumes.json"
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1

ofs volume create workspace \
  --model managed \
  --storage 's3://managed-sync/workspace?endpoint=http%3A%2F%2F127.0.0.1%3A19000&region=us-east-1'
```

With no `--metadata` argument, namespace metadata is stored beside the data in
the same OpenDAL-backed root. Repeating the command opens the same format v1
volume and leaves its identity unchanged.

Use a different storage root for each volume. The root is private Managed
storage, not an object namespace for other tools to edit.

## Large-file chunking

Format v1 uses FastCDC v2020 for files of at least 1 MiB, with 64 KiB minimum,
256 KiB target, and 1 MiB maximum chunks. These writer values are fixed. Each
file-version manifest stores the values needed to interpret its chunks, so a
reader does not depend on local configuration.

Smaller files remain whole. Run `ofs volume pack` to aggregate their physical
content into a derived read index; packing does not change any published
`FileVersion`.

## Create a D1 metadata volume with MinIO data

D1 holds the authoritative namespace while MinIO holds immutable file data.
Set the D1 token separately from the catalog URL.

```shell
export OFS_CONFIG="$PWD/ofs-volumes.json"
export OFS_D1_TOKEN=local-d1-token
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1

ofs volume create workspace \
  --model managed \
  --storage 's3://managed-sync/workspace?endpoint=http%3A%2F%2F127.0.0.1%3A19000&region=us-east-1' \
  --metadata 'd1://local/managed-sync/workspace?api_base=http%3A%2F%2F127.0.0.1%3A19001%2Fclient%2Fv4'
```

The D1 URL has the form `d1://ACCOUNT/DATABASE/STORE`. For a remote D1
deployment, omit the local `api_base` override unless the endpoint requires it.

## Synchronize a local directory

Keep the replica state outside the synchronized directory. It contains the
durable common base, publication intent, and conflict records.

```shell
mkdir -p worktree state

ofs sync workspace worktree --state state/worktree.json
ofs status --state state/worktree.json
ofs status --state state/worktree.json --json
```

A sync freezes a stable view of local input, records its publication intent,
writes and verifies data, then publishes one metadata transaction. The common
base advances only after the committed target has been installed locally. If a
process stops after its intent becomes durable, repeat the same `ofs sync`
command. Recovery uses the original operation identity instead of guessing
whether the transaction committed.

The durable staging area records the complete logical tree but stores file
content only for changed paths. Unchanged files reuse the `FileVersion` fixed
by the authority snapshot. Remote reconciliation installs only affected local
paths. This keeps update staging proportional to the change while preserving
the same crash-recovery contract.

A no-op sync does not republish unchanged content. Hard links and symbolic
links are rejected before publication because format v1 does not advertise
those capabilities. Regular files, directories, empty files, executable bits,
renames, and directory moves use the Managed namespace contract.

## Resolve a conflict

Concurrent changes on different paths merge normally. When both replicas
change the same file from the same common base, Sync preserves the local and
remote candidates and reports a conflict.

```shell
ofs status --state state/worktree.json --json
ofs sync workspace worktree --state state/worktree.json --resolve path/to/file
```

`--resolve` publishes the current local candidate for that path. Review the
file before running the command. Sync never chooses a last writer silently.

## Pack data

Packing is explicit maintenance over a fixed namespace snapshot. It selects
reachable, non-empty whole-file content no larger than 256 KiB. A pack contains
at most 8 MiB of logical content. The command writes an immutable pack, verifies
its footer, and publishes a derived pack index.

```shell
ofs volume pack workspace
```

Running the same command again is safe. Content that already has a verified
pack location is not packed again.

The placement index is secondary state. Rebuild it from immutable pack
trailers and footers without repacking data:

```shell
ofs volume reindex workspace
```

Reindexing is the only pack operation that lists pack objects. It uses object
lengths returned by the listing when available, then reads only the fixed
trailer and referenced footer ranges. Foreground reads never list packs.

A cold full-tree materialization downloads and verifies each selected pack
once, then writes all of its files from that verified body. Incremental Sync
keeps range reads because downloading a whole pack for one changed file would
amplify data transfer. The choice belongs to the materialization operation and
does not change the pack format or index.

Packs and their placement index are derived read caches under
`.ofs/managed/indexes/data-pack/v1/`. Loose content remains authoritative.
Deleting the whole pack index tree cannot make a committed file version
unreadable. Garbage collection reads one recursive inventory of loose objects.
Eligible keys are submitted through the OpenDAL deleter, which uses the
provider's batch limits. Malformed loose keys remain untouched. A live digest
stored with an unexpected object length fails closed before deletion. If
deletion stops after some provider batches, rerun the command; the next
inventory continues with the objects that remain.

Unreachable loose objects require a namespace maintenance fence:

```shell
ofs volume gc workspace
```

The metadata authority fixes a change cursor and blocks publication before the
collector deletes anything. A stopped collector leaves a resumable sweep;
another invocation continues the same epoch. Finishing the sweep and returning
the authority to idle uses the matching authority token. This prevents a
writer from publishing a reference to an object that GC has just classified as
unreachable.

## Run the local acceptance environment

The xtask command surface starts MinIO and a local D1 Query API through Docker
Compose or Podman Compose. It also creates the MinIO bucket used by the
workflows.

```shell
cargo x managed-sync doctor
cargo x managed-sync test workflow object
cargo x managed-sync test workflow d1
```

The two workflow commands exercise the same user-visible contract with Object
metadata and D1 metadata. They cover publication, cold materialization,
rename, delete, disjoint merge, explicit conflict resolution, killed-process
recovery, checkpoint catch-up, pack maintenance, garbage collection, and
credential-free status output.

Bub drives the public CLI and ordinary directories. A separate oracle checks
the resulting tree and state:

```shell
cargo x managed-sync bub .local/evidence/managed-sync-bub
```

The release A/B harness writes its evidence to the requested directory:

```shell
cargo x managed-sync perf .local/evidence/managed-sync-performance
```

The fixed acceptance thresholds are at most 10 percent sustained lifecycle
regression and at most 15 percent publication or catch-up p95 regression. The
harness also checks that a no-op sync does not upload file data.

Use `cargo x managed-sync up` and `cargo x managed-sync down` only when you need
to inspect the fixture manually. Set `OFS_COMPOSE` to `docker`, `podman`, or
`podman-compose` if automatic runtime detection does not select the intended
command.

## OpenDAL boundary

Managed data and colocated Object metadata use OpenDAL `Operator` directly for
read, create-only write, stat, recursive list, provider-batched deletion, and
head compare-and-swap. Section reads use OpenDAL `Reader::fetch`, which merges
nearby byte ranges and returns zero-copy slices for the requested sections.
Local Sync scanning and ordinary file I/O use the OpenDAL filesystem service.
Native filesystem calls remain where object operations cannot express an
atomic local state replacement, Unix link inspection, or permission handling.

A custom OpenDAL layer is appropriate only when it preserves the object
operation contract and can report its capabilities accurately. Retry,
timeouts, telemetry, immutable caching, or a self-describing object codec can
fit that boundary. Namespace transactions, `FileVersion` construction,
packing, and Sync reconciliation do not. Each assembled storage operator uses
OpenDAL's `ConcurrentLimitLayer` with the shared runtime concurrency value and
its jittered `RetryLayer` for temporary provider failures. OFS does not add a
project-specific storage layer.
