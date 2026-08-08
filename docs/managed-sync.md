# Managed Sync

Managed Sync reconciles an ordinary local directory with a Managed volume. It
preserves stable filesystem identity, detects concurrent changes with
generation preconditions, retains conflicts for explicit resolution, and
publishes each accepted namespace change atomically.

RFC 016 defines two independent axes: Direct or Managed is the volume model;
Mount or Sync is the access model. Sync is a frontend over the common Volume
interface. Object and transactional metadata are implementations of the same
Managed namespace contract. The data format is shared by Managed Mount and
Managed Sync.

## Commands

Create a Managed volume and bind it to a local alias:

```shell
ofs --config volumes.json volume create workspace \
  --model managed \
  --storage 's3://bucket/prefix?region=us-east-1'
```

Use `OFS_STORAGE_URL` instead of `--storage` when runtime configuration should
come from the environment. Credentials remain in the provider's standard
environment or credential chain and are never written to the catalog or
volume.

Reconcile a local replica:

```shell
ofs --config volumes.json sync workspace ./tree --state ./workspace.state
```

Inspect replica state:

```shell
ofs --config volumes.json status --state ./workspace.state --json
```

Resolve a retained conflict with the current local file:

```shell
ofs --config volumes.json sync workspace ./tree \
  --state ./workspace.state \
  --resolve path/to/file
```

Collect unreachable data segments against a fenced namespace snapshot:

```shell
ofs --config volumes.json volume gc workspace
```

All storage commands accept `--transfer-concurrency`; the equivalent
environment variable is `OFS_TRANSFER_CONCURRENCY`. Retry and concurrency are
provided by OpenDAL layers shared by the assembled operators.

## Durable boundaries

The Managed superblock is the one persistent source of volume identity and
format selection. It records the Managed specification, naming policy,
metadata format, file-version format, data format, and required extensions. It
does not record credentials, endpoints, local paths, chunk sizes, segment size
targets, retry settings, or checkpoint schedules.

Object metadata uses these objects:

```text
.ofs/managed/metadata/v1/superblock.json
.ofs/managed/metadata/v1/head.ofs
.ofs/managed/metadata/v1/manifests/sha256/<digest>.ofs
.ofs/managed/metadata/v1/sstables/sha256/<digest>.sst
```

`head.ofs` is the sole mutable commit point. It has a fixed format marker,
decoded-length field, zstd-compressed canonical CBOR body, and SHA-256 checksum.
A conditional PUT replaces it.
It names an immutable checkpoint and contains a bounded ordered tail of
committed namespace changes. A reader fetches HEAD once, reads the manifest and
the selected SSTable blocks, then applies the inline tail. Recent operation
results resolve from HEAD; older results are typed records in the checkpoint
SSTable.

When a client snapshot cursor occurs in the inline tail, the reader applies
only the suffix after that cursor. It does not reread the checkpoint manifest
or SSTable blocks. A cold client, or a client older than the retained tail,
recovers from the checkpoint.

Transactional metadata stores the same filesystem facts and operation results
in its native transaction and snapshot model. It does not copy a second
metadata authority into object storage.

File data uses immutable content-addressed segments:

```text
.ofs/managed/data/v1/segments/sha256/<first-two-hex>/<digest>.seg
```

Each segment contains a fixed header, raw content regions, a canonical footer,
and a fixed trailer with the footer range and segment checksum. A `FileVersion`
stores its logical size, whole-file digest, and extent map. Every extent stores
its logical offset, `ContentRef`, `SegmentRef`, and byte offset in that segment.
The extent map is authoritative filesystem metadata; no secondary index is
needed to read a committed file.

The segment footer makes an object independently inspectable. The extent map
makes the read path direct: it does not LIST, read a footer, or fetch another
location object before accessing bytes. Full reconstruction downloads each
selected segment once. Incremental reconstruction sends all selected ranges
for one segment through OpenDAL's reader, allowing its native range coalescing
to reduce HTTP requests.

## Staging and publication

Sync freezes changed local files before remote mutation. Files smaller than the
chunking threshold produce one logical content extent. Larger files use
FastCDC, which improves reuse after localized changes. Whole-file and FastCDC
are placement policy, not format variants: the stored extent map is sufficient
for every reader.

Staging deduplicates new `ContentRef` values against the fixed authority
snapshot and within the publication. New content is sorted and placed into a
small number of immutable segments. Segment size, chunking parameters, and
concurrency are runtime policy and may change without changing the durable
format.

The end-to-end order is:

1. Observe one metadata snapshot and version token.
2. Freeze changed local files.
3. Stage and verify immutable data segments.
4. Build a namespace publication with generation preconditions.
5. Commit through the metadata authority.
6. Persist the new Sync common base and install the reconciled local tree.

Data written before a failed metadata commit is unreachable and can be removed
by fenced garbage collection. Data is never published after its metadata
reference.

## Validation and failure behavior

Readers fail closed on unknown format identifiers, required extensions, fields,
record kinds, codecs, invalid ranges, overflow, checksums, or filesystem
invariants. Immutable object collisions are accepted only when the existing
object validates against the same identity. A missing referenced segment or
checkpoint is corruption, not an empty file or absent namespace.

An `OperationId` is an idempotency key. Repeating the same committed request
returns its committed cursor. Reusing it for another payload is a conflict. A
conditional-write race returns the observed authority state and does not create
a second commit path.

Garbage collection first acquires a metadata maintenance fence and fixes the
namespace cursor. It marks segment references reachable from that snapshot,
uses one recursive inventory, deletes unreferenced segment objects through
OpenDAL's bulk deletion interface, and releases the fence. A segment containing
any reachable extent remains live.

## Tests and performance evidence

Behavior tests exercise user-visible Sync publication, catch-up, conflict,
recovery, and garbage collection. Regression tests cover failures that would
corrupt or misinterpret durable data. Tests do not constrain buffer sizes,
private call sequences, or other implementation details.

The release comparison runs the same deterministic multi-generation workload
against two binaries and verifies byte-for-byte logical equality. It reports
full and range requests, request and response bytes, metadata/data/total object
counts and sizes, lifecycle latency, publication latency, and catch-up latency:

```shell
scripts/managed-sync-compare.sh \
  --baseline psiace/managed-sync-layers \
  --output .local/evidence/managed-sync-authoritative-vs-layers
```

The harness retains raw request logs, object inventories, timings, and logical
manifests so every aggregate can be recomputed.
