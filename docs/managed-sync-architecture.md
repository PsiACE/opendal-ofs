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
3. Each filesystem fact has one in-memory model. Managed stores the shared
   `VolumeSnapshot` and `VolumeMutation` directly. Only an opaque
   `FileVersion` descriptor crosses into the decoded extent-map data plane.
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
| OpenDAL | Provider I/O, retries, concurrency limits, range fetching, and conditional writes | Filesystem merge or publication semantics |

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

At the `Volume` boundary, an observation retains one RFC 016 `VolumeSnapshot`
and a small metadata CAS witness. Metadata stores that same snapshot; it does
not decode or clone descriptors into a second namespace graph. Data staging
decodes reachable descriptors only when changed files need authority-known
content. A no-change pass does not scan every live file version.

## Data path

Sync reads each changed live file once. The same bounded stream calculates its
whole-file digest, feeds FastCDC, packs immutable segments, and writes those
segments to local staging. The builder returns an opaque descriptor containing
the logical digest and extent plan; Sync persists that descriptor in the
pending replica state. A retry uploads already sealed segments instead of
rereading or rehashing the source file.
Namespace publication never depends on a live path that may change or
disappear. Files below the chunking threshold produce one content extent.
Larger files use FastCDC. These are placement policies, not different storage
formats.

Staging performs the following work:

1. Concurrent readers stream source chunks through bounded channels.
2. One segment builder reuses content referenced by the fixed authority
   snapshot and deduplicates new content across the publication.
3. The builder seals each placement-sized segment to calculate its identity
   and extent locations; it does not upload data before reconciliation.
4. File completion records supply the logical length and whole-file digest.
5. The builder returns file versions with complete logical-to-physical extent
   maps, which the pending replica state stores without interpreting.

After reconciliation has no unresolved conflict, Sync durably records its
pending intent. The Volume reads each sealed segment from local staging,
verifies its complete identity, and uploads it with create-only semantics.
Only after that succeeds may Sync materialize a different target tree and
publish namespace metadata. A durable finalization marker makes retries
independent of paths changed by target materialization.

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
`Reader::fetch`; OpenDAL removes overlap, coalesces nearby ranges with a 64 KiB
gap, and returns zero-copy slices. File writes remain windowed at 16 MiB.
The operator's OpenDAL concurrency and retry layers govern remote transfers.
OFS verifies segment structure, every returned content digest, logical length,
and the assembled whole-file digest before closing the writer. A failed read
or digest check aborts the staged output.

The read path does not list objects or consult a second segment index to locate
file bytes. The file version already contains the segment identity, byte
offset, and length.

Opening and observing a volume has one provider-independent shape:

| Authority | Stable binding | Established-replica observation |
| --- | --- | --- |
| Base | Superblock | Read HEAD, then replay its retained tail from the verified common snapshot |
| Branch | Superblock and registry name/id | Read branch HEAD, then replay its retained tail from the verified common snapshot |

The registry resolves a branch incarnation; it does not pre-read mutable HEAD
state that observation will immediately read again. A checkpoint is loaded
only for a cold replica or after its common cursor has fallen behind the
retained tail. Change segments are loaded only when the common cursor has
fallen behind the current checkpoint. A normal publication performs one
deterministic receipt lookup to reject reuse of an older `OperationId`.
Pending recovery checks the latest HEAD result first and reads a receipt only
for an older operation.

Tail replay applies each stored delta to one next snapshot. Validation borrows
the old and next snapshots; the enclosing HEAD validates cursor ancestry once,
while delta application validates the reconstructed namespace, generations,
preconditions, and immutable file versions. It does not clone either graph
into a temporary publication, and the same replay path is used by base and
branch authorities.

## Metadata authorities

Object or D1 selection is bound once as `ManagedMetadata`. Format creation,
volume opening, and branch-store opening pass through that authority. A branch
store cannot be opened without the matching format and its `branch/v1`
requirement. New-volume creation goes directly through the provider's
idempotent create operation instead of probing for absence first. The CLI and
Sync engine do not dispatch on provider types.

### Object Metadata

Object Metadata has one mutable `head.ofs` plus immutable checkpoints, change
segments, and operation receipts. HEAD contains the current cursor, checkpoint
reference, ordered transaction tail, retained change-segment index, and latest
committed operation result.

Publication writes immutable data first. It writes new checkpoint objects only
when checkpoint policy requires them, then replaces HEAD with an ETag
precondition. The conditional replacement is the namespace commit point.

Each checkpoint is one complete filesystem snapshot encoded as strict CBOR,
compressed, bounded, and stored as one content-addressed OpenDAL object. There
is no checkpoint-only filesystem model, delta table, part index, or tombstone
layer. Change segments use a strict checksummed v1 record and carry an exact
encoded length. A failed conditional HEAD replacement may leave an
unreferenced immutable object, but it cannot expose a partial checkpoint.

HEAD, its tail, and retained change segments resolve recent operations. The
initial checkpoint result is persisted when first displaced, and every result
in a change segment is persisted before that segment leaves the retained
index. A compact prefix filter skips receipt lookups for definitely new
operations. This ordering keeps exact idempotency without an outcome window or
an ever-growing result map in HEAD.

An established replica reuses its verified common snapshot when its cursor is
still covered by the HEAD tail. A cold reader loads the checkpoint with one
bounded OpenDAL read and verifies its complete content identity before
decompression.

### Transactional Metadata

D1 supplies the mutable namespace commit point as one revision-CAS record in
`ofs_managed_v1_authority_records`. Base and branch authorities use the same
key/value adapter, checkpoint codec, and namespace records as Object Metadata.
All immutable checkpoints stay in the volume's OpenDAL operator. Choosing D1
does not create another filesystem model or checkpoint format.

D1 requests have an explicit operation timeout. Schema creation is idempotent
and is submitted with the operation that first needs the record table, so a new
store has no separate migration race.

## OpenDAL boundary

Storage locators are opened with OpenDAL's URI constructor. Provider-specific
URI parsing, including filesystem roots and S3 bucket/root mapping, therefore
has one upstream implementation. OFS adds the configured concurrency limit and
retry layer once at this boundary.

The filesystem service is a required build dependency because Sync always uses
it for its durable local staging area. Remote services such as S3 remain Cargo
features. All supported feature combinations therefore retain the local Sync
capability instead of compiling a partially usable binary.

Capability admission is scoped to the operation that needs it. Object
namespace reads and publication require read and conditional writes.

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
materialize and verify the merged staging tree
       |
save pending OperationId
       |
publish metadata with generation preconditions
       |
install staged file replacements and attributes locally
       |
advance the durable common base
```

Data is written before metadata, so metadata never references an object that
has not been staged. A failed metadata commit may leave unreachable immutable
segments. It cannot create another namespace authority.

If reconciliation produces only remote changes, Sync skips the pending and
publication steps, installs the verified staging result, and then advances the
common base. If it produces a publication, the pending intent is durable before
the CAS write and remains present until the committed target is safely installed.

The pending cache is stored beside replica state by relative name. It is not
authority. The replica state's pending intent contains the already verified
opaque file versions; an absent or malformed cache invalidates the pending
attempt instead of selecting a compatibility path. If the operation committed,
Sync returns to the normal observe and reconciliation path so a local change is
never overwritten.

Directory presence is merged per path. Additions and deletions in disjoint
subtrees converge. Deleting a directory while the other side changes its
subtree is rejected before either the replica or its durable state is changed.
File content and executable state are reconciled together. A remote-only
executable change updates local attributes without reading or retransferring
unchanged file content.

The publication builder receives the current authoritative observation as its
parent and the durable replica state as its old local baseline. The common
cursor is the cursor of that stored authority snapshot; it is not persisted as
a second field. Reconciliation does not fabricate an intermediate replica
state or install a new authority snapshot before the metadata commit succeeds.
A local scan reads native identity, executable state, and link count from one
native metadata observation per path.

Each Sync pass expands the durable base and current authority into one
path-sorted index apiece. Reconciliation, subtree conflict checks, directory
installation, rename validation, publication, and final state installation
reuse those indexes. Canonical `/`-separated descendants form a bounded
`BTreeMap` range, so subtree lookup does not rescan the namespace and reverse
iteration gives children-before-parent deletion without a second depth sort.

Native identity may initially detect every descendant of a moved directory,
but reconciliation compacts matching mappings into non-overlapping subtree
roots. Validation and publication expand each root through one bounded path
range, so pending state grows with independent moves rather than subtree size.

The portable naming policy is shared by local admission and authoritative
snapshot validation. Names must be NFC, fit the component and path bounds,
avoid Windows-reserved characters and device names, and remain unique after
full Unicode case folding within a directory. Sync currently requires Unix
native identity and executable attributes; another platform is rejected at
the Sync boundary instead of silently weakening those semantics.

## Extensions

A Managed extension adds authority semantics without adding another filesystem
model. A branch binding selects a backend-native authority in one
`NamespaceStore` state machine before Sync calls the same `Volume` contract.
Base and branch expose the same observation, CAS publication, receipt
resolution, bounded-tail, and unknown-commit behavior. Object and D1 implement
the same small revision-CAS record operations. Base and branch checkpoints use
one content-addressed object codec, and branch snapshots and publications use
the shared node, directory, precondition, and Managed file-version records.

The required extension controls commands and authority binding only. It does
not create an alternate data plane. Segments remain shared immutable content.

## Acceptance and regression coverage

Tests protect contracts visible outside an implementation boundary:

- behavior acceptance runs the CLI against real OpenDAL operators and checks
  create/open, push, pull, conflict, recovery, and branch workflows by their
  files, exit status, and durable results;
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
and materialize verified bytes through each enabled metadata authority. The
same stored volume remains readable by its
declared `managed/1` format, buffered file data remains bounded by configured
concurrency and placement windows rather than dataset size, and every supported
Cargo feature combination builds.

## Related documents

- [Managed Sync workflow](managed-sync-workflow.md)
- [Managed storage format](managed-storage-format.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
