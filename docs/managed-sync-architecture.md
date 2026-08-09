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

## Design constraints

The implementation follows four constraints:

1. RFC 016 owns the vocabulary and the separation between Volume and Access
   models. Sync does not interpret provider metadata, and Managed does not own
   local reconciliation state.
2. `managed/1` owns durable compatibility. Chunk sizes, segment packing,
   request coalescing, caches, and concurrency are policies and never become
   serialized requirements.
3. Each filesystem fact has one in-memory model. Managed parameterizes the
   shared snapshot and publication types with its decoded file-version record;
   it does not maintain a parallel node, directory, or precondition graph.
4. Orchestration remains readable in execution order. Helpers isolate an
   actual capability or format boundary; they do not turn a linear command
   into a framework.

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
The filesystem core validates tree shape, record identity, backing records,
entry names, entry kinds, and reachability once. Managed validation adds only
managed generations, immutable file-version descriptors, ancestry, and
publication preconditions. Object Metadata and D1 call the same validation.

An `OperationId` is the idempotency identity for one publication. Repeating a
committed operation returns its committed cursor. Reusing the same identity
for another payload is a conflict.

## Data path

Sync reads each changed live file once. The same bounded stream writes the
reconstructable local cache and feeds the volume-owned file-version builder.
The builder returns an opaque descriptor containing the logical digest and
extent plan; Sync persists that descriptor in the staging manifest. A retry or
process restart therefore does not read and hash the cached file again.
Namespace publication never depends on a live path that may change or
disappear. Files below the chunking threshold produce one content extent.
Larger files use FastCDC. These are placement policies, not different storage
formats.

Staging performs the following work:

1. Concurrent readers tee source bytes to local staging and stream chunks into
   a bounded channel.
2. One segment builder reuses content referenced by the fixed authority
   snapshot and deduplicates new content across the publication.
3. The builder seals a segment as soon as its placement target is reached and
   uploads it with create-only semantics.
4. File completion records supply the logical length and whole-file digest.
5. The builder returns file versions with complete logical-to-physical extent
   maps, which the staging manifest stores without interpreting.

Backpressure bounds buffered file data to the active segment, the channel, and
at most one chunk held by each reader; only the resulting extent metadata grows
with the publication. Readers remain concurrent, while a single builder
preserves cross-file packing and deduplication.

Materialization builds one read plan for the requested tree, reads file extents
in logical order, and writes each file incrementally through an OpenDAL writer.
The plan chooses complete-segment or coalesced-range reads by transferred bytes
and request count. Complete segments use one lazily initialized, 64 MiB
OpenDAL Foyer cache shared by the `ManagedData` instance and its clones; the
layer merges concurrent cold reads and handles eviction. Backends without
`stat` use the coalesced-range path instead. Each sparse range is fetched once,
shared by every planned consumer across file windows, and released after its
last consumer. Sparse segment ranges are submitted together through OpenDAL
`Reader::fetch`; OpenDAL removes overlap, coalesces nearby ranges with a 256 KiB
gap, and returns zero-copy slices. File writes remain windowed at 16 MiB.
The operator's OpenDAL concurrency and retry layers govern remote transfers.
OFS verifies segment structure, every returned content digest, logical length,
and the assembled whole-file digest before closing the writer. A failed read
or digest check aborts the staged output.

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

D1 requests have an explicit operation timeout. Schema creation remains
idempotent and is submitted in the same transactional batch as the operation
that first needs each table, so a new store has no separate migration race.

## OpenDAL boundary

Storage locators are opened with OpenDAL's URI constructor. Provider-specific
URI parsing, including filesystem roots and S3 bucket/root mapping, therefore
has one upstream implementation. OFS adds the configured concurrency limit and
retry layer once at this boundary.

The filesystem service is a required build dependency because Sync always uses
it for its durable local staging area. Remote services such as S3 remain Cargo
features. All supported feature combinations therefore retain the local Sync
capability instead of compiling a partially usable binary.

Capability admission is scoped to the operation that needs it. Normal Object
namespace reads and publication require read and conditional writes; listing
and deletion are checked only when garbage collection starts. Data garbage
collection streams the provider listing and submits bounded bulk-delete
batches through OpenDAL.

Object authority reads that also need a revision use one OpenDAL `Reader`: the
object body and ETag come from the same GET response. They do not issue a
preflight stat or maintain a private conditional-read retry loop.

## Sync transaction

One invocation follows a fixed order:

```text
load replica state
       |
resolve pending operation, if any
       |
observe one authority snapshot
       |
freeze changed files, stage immutable data, and save opaque file versions
       |
three-way reconcile(base, local, remote)
       |
       +--> conflicts: retain local candidates and stop
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

The pending cache is stored beside replica state by relative name. It is not
authority. Its manifest contains the already verified opaque file versions;
an absent or malformed current-format manifest invalidates the pending attempt
instead of selecting a compatibility path. If the operation committed, Sync
reconstructs from the authoritative snapshot when doing so cannot overwrite a
local change.

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

## Extensions

A Managed extension adds authority semantics without adding another filesystem
model. A branch binding wraps a backend-native authority behind one
`BoundNamespace<S>` state machine before Sync calls the same `Volume` contract.
Observe, publication validation, receipt resolution, tail rotation, and
unknown-commit recovery are shared. Object implements conditional object
replacement; D1 implements its native transactional predicate. Branch
snapshots and publications use the shared node, directory, precondition, and
Managed file-version records.

The branch feature controls commands and extension code only. It does not
change `managed/1` base-volume readability or create an alternate data plane.
Segments remain shared immutable content, and collection treats every retained
base or branch snapshot as a root.

## Acceptance and regression coverage

Tests protect contracts visible outside an implementation boundary:

- behavior acceptance runs the CLI against real OpenDAL operators and checks
  create/open, push, pull, conflict, recovery, branch, and garbage-collection
  workflows by their files, exit status, and durable results;
- format acceptance opens and mutates persisted `managed/1` fixtures through
  both metadata authorities where applicable;
- regression tests are added for a concrete failure that could recur, and name
  the user-visible or durable invariant that failed;
- the build matrix checks default, minimal, and optional remote-service feature
  combinations.

Tests do not prescribe buffer allocation, private helper calls, request order
when the protocol permits reordering, placement-policy constants, or private
error variants. Those details may change without changing Sync behavior or the
Managed format. Removed routes, fields, and implementations receive no
absence-only tests unless their absence is itself a current security,
persistence, or public API contract.

## Completion criteria

A Managed Sync change is complete when a fresh client can register or create a
volume, converge local and remote changes, recover an interrupted publication,
materialize verified bytes, and collect only unreachable data through each
enabled metadata authority. The same stored volume remains readable by its
declared `managed/1` format, buffered file data remains bounded by configured
concurrency and placement windows rather than dataset size, and every supported
Cargo feature combination builds.

## Related documents

- [Managed Sync workflow](managed-sync-workflow.md)
- [Managed storage format](managed-storage-format.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
