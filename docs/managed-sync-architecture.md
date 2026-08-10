# Managed Sync architecture

Managed Sync is a foreground reconciliation frontend for ordinary local
directories. RFC 016 defines its filesystem vocabulary; `managed/1` defines
its durable representation.

## Product boundary

The volume model and access model are independent:

| Volume | Mount | Sync |
| --- | --- | --- |
| Direct | Read-only | Not implemented |
| Managed | Not implemented | Read-write |

Mount and Sync are frontends over the shared filesystem model. Direct and
Managed are volume implementations. An unavailable combination has no hidden
fallback or alternate storage format.

```text
Sync -> filesystem model -> Managed volume -> Metadata (Object or D1)
                                      `----> Data (OpenDAL)
```

The catalog binds a client-local alias to a remote `VolumeId` and
credential-free locators. Provider credentials and replica paths remain local.
Different clients may use different aliases for the same volume.

## Ownership

| Component | Responsibility |
| --- | --- |
| Application | Catalog lookup, credentials, operator construction, runtime limits |
| Filesystem | Nodes, directories, snapshots, mutations, preconditions |
| Managed Metadata | Namespace authority, cursors, publication and operation results |
| Managed Data | Immutable segments, file-version descriptors, verified reads and writes |
| Sync | Common base, local scan, reconciliation, conflicts, pending intent, installation |
| OpenDAL | Provider I/O, retries, concurrency, range fetching, conditional writes |

Each filesystem fact has one in-memory representation. Managed Metadata stores
the shared `VolumeSnapshot` and `VolumeMutation`; only the opaque file-version
descriptor crosses into the data plane. Object Metadata and D1 use the same
namespace rules and checkpoint codec.

## OpenDAL integration

One application composition root constructs every remote OpenDAL operator.
Its concurrency layer bounds both logical operations and provider HTTP
requests; its retry layer supplies the native temporary-error policy. Direct
Mount and Managed Sync receive the resulting operator instead of assembling
their own transport behavior.

Managed Data submits sparse reads through the OpenDAL reader and uses Foyer
only for reusable, complete immutable segments. Publication CAS, operation
recovery, content verification, staging bounds, and branch authority remain
Managed domain behavior: they depend on volume history and are not transparent
storage layers. Provider-native audit counters are the performance authority
for request, transfer, and stored metadata costs.

The integration shape follows the behavior it exposes:

| Shape | Contract | OFS use |
| --- | --- | --- |
| OpenDAL Layer | The same storage operation with a transparent cross-cutting policy | Retry, concurrency limits, and complete immutable-segment Foyer caching |
| OpenDAL Accessor or Service | A storage API backed by another data model | Direct storage today; a future Managed filesystem view for Mount or embedding |
| Built-in Managed extension | New durable volume behavior, identities, or commands | `branch/v1` namespace authorities and their lifecycle |
| Domain operation | A visible multi-step state transition | Sync, publication recovery, staging, verification, and reachability collection |

Timeout and observability layers may be added at the same composition root when
they become configured runtime policies. Immutable-index and route layers do
not replace namespace metadata: its authority is mutable, D1 requires revision
CAS, and maintenance is the only path that lists data. Branch is therefore an
extension over the one namespace implementation, not a Layer that rewrites
arbitrary storage calls. Managed Mount, if added, should expose an Accessor
instead of hiding namespace transactions inside a Layer.

Object data and Object Metadata both use the composed OpenDAL operator. D1 is
an explicit metadata-authority exception, not another data store: OpenDAL 0.57's
D1 service does not expose the revision conditional writes required by the
namespace contract, so its Query API adapter remains behind the same
`RecordBackend` CAS boundary. It can move to an OpenDAL Accessor only after that
Accessor preserves revision creation and replacement atomically.

## Namespace and publication

A snapshot contains stable node identities, node generations, directory
entries, attributes, and file-version descriptors. A publication contains its
complete target snapshot plus preconditions for changed nodes and directories.
Metadata validates the tree and preconditions before advancing one
`ChangeCursor`.

An `OperationId` identifies one publication. Repeating the same operation and
payload returns its committed cursor; reusing the identity for another payload
is a conflict. A saved pending operation can therefore be resolved after a
timeout without guessing whether the commit happened.

Object Metadata commits by conditionally replacing one HEAD object. D1 commits
one revision-CAS authority record. Immutable checkpoints, change segments,
operation receipts, file versions, and data segments use the same v1 formats
for both authorities. A branch selects another authority; it does not add a
second namespace or data implementation.

## Data path

Sync streams each changed file once to calculate its digest, split content,
and seal immutable segments in local staging. The resulting file-version
descriptor contains the complete logical extent map. Pending state stores that
descriptor, so retry does not depend on the source path still containing the
same bytes.

After reconciliation succeeds, the volume verifies and uploads staged
segments with create-only semantics before publishing metadata. Failed
publication may leave unreachable immutable segments, but metadata cannot
reference an unstaged segment.

Materialization derives reads directly from file-version extents. It chooses
complete-segment or coalesced-range reads, submits sparse ranges through the
OpenDAL reader, verifies segment and content identities, and writes files in
bounded windows. Foyer caches only complete verified segment reads. OpenDAL
layers provide the storage concurrency and retry policy; individual chains do
not invent separate limits.

Explicit `ofs volume gc` is the only destructive maintenance path. It fixes
the authority before marking data: one base HEAD, or the branch registry and
all registered HEADs. Branch marking deduplicates shared checkpoint and change
references and retains every file version needed by a current or retained
branch position. The sweep uses OpenDAL's streaming lister and native deleter;
normal Sync, materialization, and branch operations never list the segment
prefix. A failed sweep leaves its durable fence in place, so a new collector
cannot guess whether deletion completed; `--resume` deliberately takes
ownership and repeats the safe mark.

## Sync transaction

One invocation has one forward path:

```text
load replica state
  -> resolve pending operation
  -> observe one remote snapshot
  -> scan and stage changed local files
  -> reconcile common, local, and remote trees
  -> stop with retained conflicts, or save pending intent
  -> upload immutable data and publish metadata
  -> install and verify the merged local tree
  -> advance the durable common base
```

Remote-only changes skip publication. Disjoint changes merge. Same-path or
overlapping subtree changes remain conflicts until explicitly resolved. The
common base advances only after the merged tree is installed and verified.

Replica state is local authority for recovery, not remote namespace state. It
stores the remote volume and branch identities, verified common snapshot,
pending operation, staged descriptors, and conflicts. The saved operation is
resolved before a missing or malformed staging cache is discarded and rebuilt.

## Filesystem admission

Local scanning and authoritative snapshot validation share one portable-name
policy: NFC names, fixed component and path limits, no Windows-reserved names,
and uniqueness after Unicode case folding. Sync accepts regular files,
directories, empty directories, and Unix executable state. It rejects links,
unsupported file types, and platforms that cannot supply stable native rename
identity and executable attributes.

Canonical relative paths are kept in ordered maps. Reconciliation and
installation use bounded subtree ranges; directory moves are stored as
non-overlapping root moves and expanded only at validation or publication.

## Related documents

- [Managed Sync workflow](managed-sync-workflow.md)
- [Managed storage format](managed-storage-format.md)
- [Managed branches](managed-branches.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
