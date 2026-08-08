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
4. Sort new content and write a small number of immutable segments.
5. Return file versions with complete logical-to-physical extent maps.

Materialization groups extents by segment. A full reconstruction downloads
each selected segment once. An incremental reconstruction sends the selected
ranges for one segment to `OpenDAL::Reader::fetch`. OpenDAL may merge nearby
ranges before issuing provider requests. OFS verifies every returned content
digest and the assembled whole-file digest.

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

An established replica can reuse its verified common snapshot when its cursor
is still covered by the HEAD tail. A cold reader loads the manifest and only
the SSTable blocks selected by their key ranges.

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
freeze and scan the local tree
       |
three-way reconcile(base, local, remote)
       |
       +--> conflicts: retain local candidates and stop
       |
stage immutable segments
       |
save pending OperationId
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

## Garbage collection

Garbage collection acquires a maintenance fence from Metadata and fixes one
namespace cursor. Data walks the reachable namespace, records referenced
segments, lists the segment prefix once, and submits unreferenced objects to
OpenDAL's bulk deletion interface. A segment remains live if any reachable
file version references it.

Foreground publication and materialization do not list the data prefix.

## Related documents

- [Managed Sync workflow](managed-sync-workflow.md)
- [Managed storage format](managed-storage-format.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
