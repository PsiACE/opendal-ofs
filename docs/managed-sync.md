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

`--transfer-concurrency N` and `OFS_TRANSFER_CONCURRENCY` set the shared OFS
storage concurrency value. The default is four, and the value must be greater
than zero. Each assembled OpenDAL storage operator uses it as its operation
limit. Sync uses the same value to bound publication and materialization work.
Direct Mount, Managed Sync, and Managed maintenance therefore use one setting.
This is runtime configuration, not part of a Volume Model, Access Model, or
durable format.

## Managed format v1

Managed volume format v1 keeps namespace identity separate from data
placement. Metadata owns stable nodes, directory entries, object-scoped
generations, immutable `FileVersion` manifests, and the ordered change log.
Apache OpenDAL™ stores immutable content.

The foreground write path stores loose content. The CLI exposes `whole` and
`fastcdc` writer policies, with whole-file manifests as the default. FastCDC
applies at or above its configured file-size threshold, so smaller files remain
whole. Format v1 also defines fixed-chunk and sparse/extents manifest records.
Packing changes only the physical location of a `ContentRef`; it does not change
a file manifest, node generation, or change cursor.

Metadata can be colocated with data as immutable objects plus a compare-and-swap
head, or stored in D1 as normalized rows and transactions. Both adapters apply
the same logical namespace changes and recover from a checkpoint plus a bounded
change tail. Unknown major formats and unknown required features fail before a
mutation.

Data is written and verified before metadata can reference it. If publication
fails after that write, the result may be an unreachable loose object. The
namespace remains valid, and explicit garbage collection can reclaim the
object later.

During an update, the fixed parent snapshot is also proof that its reachable
`ContentRef` values are durable. Sync still reads and hashes changed local
input, but it does not probe or download matching content again. New content
keeps the same create-only write and read-back verification. This applies to
whole files, chunks, and sparse data extents without changing format v1.

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

## Configure large-file chunking

Select FastCDC when creating or reopening a volume:

```shell
ofs volume create workspace \
  --model managed \
  --storage "$OFS_STORAGE_URL" \
  --file-layout fastcdc
```

The defaults use FastCDC v2020 for files of at least 1 MiB, with 64 KiB minimum,
256 KiB target, and 1 MiB maximum chunks. Override them with
`--fastcdc-minimum-file-size`, `--fastcdc-minimum-chunk-size`,
`--fastcdc-target-chunk-size`, and `--fastcdc-maximum-chunk-size`. Each option
takes a byte count. The corresponding environment variables are
`OFS_FASTCDC_MINIMUM_FILE_SIZE`, `OFS_FASTCDC_MINIMUM_CHUNK_SIZE`,
`OFS_FASTCDC_TARGET_CHUNK_SIZE`, and `OFS_FASTCDC_MAXIMUM_CHUNK_SIZE`.
`OFS_FILE_LAYOUT` sets `whole` or `fastcdc`.

The catalog keeps the selected policy. Reopening an existing catalog without
layout options preserves its current policy. Supplying a layout updates future
writes without rewriting existing file versions. When rebuilding a lost catalog,
pass the intended policy again because the remote format record does not choose a
writer policy.

Small files remain whole when FastCDC is enabled. Run `ofs volume pack` when you
want to aggregate their physical content. Packing does not change the configured
file layout or any published `FileVersion`.

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
ofs status worktree --state state/worktree.json
ofs status worktree --state state/worktree.json --json
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
ofs status worktree --state state/worktree.json --json
ofs sync workspace worktree --state state/worktree.json --resolve path/to/file
```

`--resolve` publishes the current local candidate for that path. Review the
file before running the command. Sync never chooses a last writer silently.

## Pack and reclaim data

Packing is explicit maintenance over a fixed namespace snapshot. It selects
reachable, non-empty whole-file content no larger than 256 KiB. A pack contains
at most 8 MiB of logical content. The command writes an immutable pack, verifies
its footer, and publishes a derived pack index.

```shell
ofs volume pack workspace
```

Running the same command again is safe. Content that already has a verified
pack location is not packed again.

A cold full-tree materialization downloads and verifies each selected pack
once, then writes all of its files from that verified body. Incremental Sync
keeps range reads because downloading a whole pack for one changed file would
amplify data transfer. The choice belongs to the materialization operation and
does not change the pack format or index.

Use the optional maintenance controls when you intend to replace packs with
dead entries or remove redundant loose copies:

```shell
ofs volume pack workspace --repack-grace-seconds 30
ofs volume pack workspace --reclaim-loose-after-seconds 30
```

Repack publishes replacement locations before retiring an old pack. Loose
reclamation removes content only when a verified pack location still serves
the same `ContentRef`.

Loose reclamation and garbage collection each read one recursive inventory of
loose objects. Eligible keys are submitted through the OpenDAL deleter, which
uses the provider's batch limits. Malformed loose keys remain untouched. A live
digest stored with an unexpected object length fails closed before deletion. If
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
read, range read, create-only write, stat, recursive list, provider-batched
deletion, and head compare-and-swap. Local Sync scanning and ordinary file I/O
use the OpenDAL filesystem service. Native filesystem calls remain where object
operations cannot express an atomic local state replacement, Unix link
inspection, or permission handling.

A custom OpenDAL layer is appropriate only when it preserves the object
operation contract and can report its capabilities accurately. Retry,
timeouts, telemetry, immutable caching, or a self-describing object codec can
fit that boundary. Namespace transactions, `FileVersion` construction,
packing, and Sync reconciliation do not. Each assembled storage operator uses
OpenDAL's `ConcurrentLimitLayer` with the shared runtime concurrency value. OFS
does not add a project-specific storage layer.
