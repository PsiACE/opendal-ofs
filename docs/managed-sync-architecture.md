# Managed Sync architecture

Managed Sync is a foreground reconciliation command for ordinary local
directories. It uses the filesystem abstractions defined by RFC 016 and the
portable `managed/1` storage format.

## Product boundary

RFC 016 separates the volume model from the access model:

| Volume model | Mount | Sync |
| --- | --- | --- |
| Direct | Available, read-only | Not implemented |
| Managed | Not implemented | Available, read-write |

Direct and Managed are implementations of `Volume`. Mount and Sync are
frontends. A frontend depends on the filesystem contract rather than a
provider-specific namespace.

```text
Mount -----+
           +--> Filesystem core --> Volume --> Direct
Sync ------+                         |
                                     +------> Managed
                                              |-- Metadata
                                              |    |-- Object
                                              |    +-- Transactional (D1)
                                              +-- Data
```

The shared `Volume` interface is the reuse boundary for access models. The
Managed namespace contract and extent map are the reuse boundary between
metadata implementations and Managed frontends. An unimplemented product cell
has no alternate command or storage layout.

## Ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| Application | Catalog, operator construction, credentials, runtime settings | Namespace state or data placement |
| Volume | Identity, capability admission, Metadata and Data composition | A second persistent namespace |
| Metadata | Nodes, directory entries, generations, file versions, snapshots, publication results | File bytes or local replica state |
| Data | Immutable segments, physical verification, materialization, reachability | Namespace authority |
| Sync | Common base, pending operation, conflicts, staging, local installation | Remote format selection |
| OpenDAL | Provider I/O, retries, concurrency limits, range fetching, conditional writes, bulk deletion | Filesystem merge or publication semantics |

Credentials, endpoints, local paths, and Sync state never enter the Managed
storage format. The catalog stores a credential-free volume binding. Runtime
credentials come from provider environment variables or the provider's normal
credential chain.

## Remote identity and local aliases

The superblock `VolumeId` identifies the remote Managed volume. A catalog name
is only a local alias for a credential-free tuple of `VolumeId`, data locator,
and optional metadata locator. Alias text has no remote meaning, so two
containers can use different names for the same volume.

`volume create` is an open-first ensure-and-register operation. An unbound
client looks for an existing superblock before attempting to initialize one.
If no superblock exists, Metadata atomically creates one with a new `VolumeId`;
if it does exist, the command registers the observed identity under the
requested local alias. A configured alias only verifies its saved identity and
locators against the remote format. It never recreates a missing superblock
from local catalog state. One catalog keeps at most one alias for a `VolumeId`,
while independent catalogs choose their aliases independently.

Required extensions are also observed from the superblock. An explicit
extension request must match an existing remote format, while omitting the
request still opens extensions already required by that format. Extension
metadata is initialized idempotently before the local alias is saved.

Replica state stores `VolumeId`, not the alias or catalog path. Before Sync
reads or mutates either side, the application resolves the current alias and
checks that its `VolumeId` matches the replica state. Losing a catalog therefore
does not change remote identity; a disposable client can register a new alias
from the same locators and cold-materialize into a new replica.

## Namespace model

The filesystem core uses stable `NodeId` values. A node has a generation,
kind, attributes, and an optional file version. A directory has its own
generation and maps names to node identities. A `ChangeCursor` identifies an
authoritative namespace position.

A publication contains a complete target snapshot and preconditions for every
changed node and directory. Metadata validates the snapshot, cursor ancestry,
generation changes, preconditions, and file version references before commit.
This validation is shared by Object Metadata and D1.

An `OperationId` is the idempotency identity for one publication. Repeating a
committed operation returns its committed cursor. Reusing the same identity
for another payload is a conflict.

## Data path

Sync freezes changed files before remote mutation. Files below the chunking
threshold produce one content extent. Larger files use FastCDC. These are
placement policies, not different storage formats.

Staging performs the following work:

1. Read each frozen changed file.
2. Reuse content already referenced by the fixed authority snapshot.
3. Deduplicate new content across the publication.
4. Seal and upload segment batches as prepared files arrive.
5. Return file versions with complete logical-to-physical extent maps.

Materialization reads file extents in logical order and writes each file
incrementally through an OpenDAL writer. A shared transfer semaphore bounds
all in-flight range reads for the operation. OFS verifies every returned
content digest, logical length, and assembled whole-file digest before closing
the writer. A failed read or digest check aborts the staged output.

The read path does not list objects or read a segment footer to locate file
bytes. The file version already contains the segment identity, byte offset,
and length.

## Metadata authorities

### Object Metadata

Object Metadata has one mutable `head.ofs` and immutable manifests and
SSTables. HEAD contains the current cursor, a checkpoint reference, an ordered
tail of committed namespace changes, and garbage-collection maintenance state.

Publication writes immutable data first. It writes new checkpoint objects only
when checkpoint policy requires them, then replaces HEAD with an ETag
precondition. The conditional replacement is the namespace commit point.

Each checkpoint is a complete snapshot partitioned into stable path ranges.
The writer groups the records for a path, chooses deterministic boundaries,
reuses an unchanged range's content-addressed SSTable, and uploads only changed
ranges. Opaque node and file-version identities remain record keys, but they do
not control physical placement. This distinction keeps edits near the affected
paths even when another replica created the nodes. One manifest is sufficient,
partition ranges never overlap, and there are no delta tables or tombstone
layers. A failed conditional HEAD replacement may leave unreferenced immutable
checkpoint objects, but it cannot expose a partial checkpoint.

An established replica can reuse its verified common snapshot when its cursor
is still covered by the HEAD tail. A cold reader loads the manifest and the
typed-key blocks needed to reconstruct the snapshot. Reads from separate
SSTable objects run with bounded concurrency. Blocks from one object are
submitted together through OpenDAL's range fetch interface.

### Transactional Metadata

D1 stores the same filesystem facts in native tables. Publication uses one
database transaction with revision and generation predicates. Snapshot reads,
operation resolution, and garbage-collection fencing remain native database
operations.

D1 does not emulate Object HEAD, manifests, object keys, or SSTable requests.
Object Metadata does not emulate database transactions. The two authorities
share records and validation, not storage mechanics.

## Sync transaction

One invocation follows a fixed order:

```text
load replica state
       |
resolve pending operation, if any
       |
observe one authority snapshot
       |
freeze and scan the local tree into a reconstructable cache
       |
three-way reconcile(base, local, remote)
       |
       +--> conflicts: retain local candidates and stop
       |
save pending OperationId
       |
stage immutable segments
       |
publish metadata with generation preconditions
       |
materialize and verify the merged tree
       |
advance the durable common base
```

Data is written before metadata, so metadata never references an object that
has not been staged. A failed metadata commit may leave unreachable immutable
segments. It cannot create another namespace authority.

The pending cache is stored beside replica state by relative name. It is not
authority. If it is missing before commit, Sync scans and freezes the local
tree again. If the operation committed, Sync reconstructs from the
authoritative snapshot when doing so cannot overwrite a local change. Replica
state formats with the old absolute cache path are rebased beside the state
file when read.

Directory presence is merged per path. Additions and deletions in disjoint
subtrees converge. Deleting a directory while the other side changes its
subtree is rejected before either the replica or its durable state is changed.

## Garbage collection

Garbage collection acquires a maintenance fence from Metadata and fixes one
namespace cursor. Data walks the reachable namespace, records referenced
segments, and streams the segment listing. Each unreachable object is deleted
without collecting the full provider listing in memory. A segment remains live
if any reachable file version references it.

The fence contains an epoch and owner token. A normal start conflicts with an
active fence. `--resume` conditionally replaces the owner only after the
operator has confirmed that the previous collector stopped; an old owner
cannot finish or continue metadata maintenance. An unpublished namespace is a
successful empty collection.

Foreground publication and materialization do not list the data prefix.

## Related documents

- [Managed Sync workflow](managed-sync-workflow.md)
- [Managed storage format](managed-storage-format.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
