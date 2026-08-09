# Managed branches

Define `branch/v1` as an optional Managed-volume extension for durable named
filesystem branches. A branch is a small mutable authority record over shared
immutable namespace checkpoints, history, file versions, and data segments.
Forking does not copy the namespace or file data.

The implementation is isolated behind the Cargo feature `managed-branch` and
the Managed-format requirement `branch/v1`. It behaves like a layer at the
metadata binding boundary:

```text
Managed metadata + branch/v1
    -> bind BranchName
    -> branch-bound Namespace
    -> existing ManagedVolume
    -> Sync
```

A future Managed Mount can consume the same bound `ManagedVolume`.

Branching does not wrap OpenDAL storage operations. It changes which metadata
authority owns the namespace, so an `opendal::Layer` would be the wrong
boundary. The data plane, filesystem core, and publication contract remain
unchanged after a branch has been bound.

`branch/v1` provides a linear history per branch, fork from the current head
or any retained sequence, deletion with safe name reuse, and volume-wide
garbage collection. It does not provide merge, tags, branch reset, automatic
history expiry, a mounted filesystem, or a per-filesystem-call event log.

# Why branches belong in Managed metadata

Agents, tests, and data jobs need isolated filesystem states without copying a
complete tree. They also need durable resume on another host and a way to
recover an earlier state without moving or destroying the current state.

Managed volumes already provide the expensive pieces:

- stable node and directory identities;
- immutable file versions and data segments;
- immutable namespace checkpoints;
- ordered, generation-checked publications;
- bounded change tails; and
- operation identities for retry recovery.

Adding another commit graph, copying every metadata record, or reference
counting every shared segment would duplicate those mechanisms. The missing
pieces are named authorities, retained tails after checkpoint rotation, and a
garbage collector that treats all branches as roots.

The extension reuses the common namespace snapshot, validation, publication,
file-version, data-segment, and SSTable models. Its checkpoint root and
retained-history records are extension-owned because they preserve multiple
branch-local positions.

The extension boundary keeps these rules out of base Managed volumes. The base
and extension formats may share code, but they do not share mutable authority
records. A build that cannot preserve branch roots must reject `branch/v1`
before mutation.

# Use managed branches

## Create and use branches

Branches are enabled when a Managed volume is created:

```text
ofs volume create workspace \
  --model managed \
  --enable branch \
  --storage <storage-url>
```

Creation records `branch/v1` in the Managed superblock and creates the default
branch `main`. Direct volumes reject `--enable branch` before changing the
catalog or storage.

The remote Managed format is authoritative when another client registers the
same volume. A client may omit `--enable branch` and still open an existing
`branch/v1` volume. An explicit extension request against an existing base
volume fails before the local catalog or branch metadata is changed. Repeating
creation also completes an interrupted default-branch initialization.

The default branch needs no additional Sync option:

```text
ofs sync workspace ./workspace --state ./workspace.state
```

Another branch is selected explicitly:

```text
ofs sync workspace ./experiment \
  --branch experiment \
  --state ./experiment.state
```

Replica state stores both `VolumeId` and the stable branch identity. Reusing a
state file with another branch fails before the local tree is scanned or
changed. Deleting and recreating the same display name creates a new identity,
so an old replica cannot attach to the replacement.

## Fork current or retained state

Fork the current default branch:

```text
ofs branch create workspace experiment
```

Select another source, or a retained branch-local sequence:

```text
ofs branch create workspace retry --from experiment
ofs branch create workspace rewind --from main --at 42
```

The source remains unchanged. The new branch receives a new `BranchId` and
reuses the selected immutable checkpoint, history, file versions, and data
segments. Fork cost does not scale with the number or size of files, although
finding an old sequence can require walking retained history.

Sequences are meaningful only with their branch ancestry. Two branches may
both publish sequence 43 after forking from sequence 42.

`branch/v1` does not move an existing branch backward. Creating a new branch
from an old sequence keeps current replicas valid and makes the retained state
explicit.

## Inspect and delete branches

```text
ofs branch list workspace
ofs branch show workspace experiment
ofs branch delete workspace experiment
```

The default branch cannot be deleted. Deletion seals the current incarnation
before its name becomes reusable. Shared data is not removed synchronously.

`--json` output contains stable user-facing fields such as branch name,
identity, sequence, default status, and lifecycle. It does not expose object
keys, SQL revisions, provider errors, or credentials.

## Garbage collection

```text
ofs volume gc workspace
```

On a branching volume this is a volume-wide operation. It freezes branch
lifecycle and publication, recovers every current and retained state of every
registered branch, unions their data-segment references, and performs one
sweep. A single-branch sweep would be unsafe because checkpoints and data are
shared.

If collection fails, the maintenance fence remains active. After confirming
that the prior collector process has stopped, an operator resumes it
explicitly:

```text
ofs volume gc workspace --resume
```

A second ordinary GC never joins an active epoch. Publication and branch
lifecycle operations return a conflict until collection finishes.

## Capability coverage

The extension deliberately covers storage history, not every workflow offered
by the systems that informed it:

| Capability | `branch/v1` with Sync | lakeFS | JuiceFS clone | Overeasy |
| --- | --- | --- | --- | --- |
| Named mutable branches | Yes | Yes | Clone namespace | Yes |
| Fork without copying file data | Yes | Yes | Yes | Yes |
| Fork independent of namespace size | Yes | Yes | No | Yes |
| Fork retained history | Sequence | Commit | Not its clone contract | Timestamp |
| Durable cross-host resume | After publication | Yes | Yes | Yes |
| Safe shared-data GC | Multi-root tracing | Commit reachability | Reference counting and trash | Log/object retention |
| Merge and tags | No | Yes | No | No |
| Mounted filesystem | Not yet | Gateway-dependent | Yes | Yes |
| Per-filesystem-call durable log | No | No | Metadata transactions | Yes |

The remaining gap to Overeasy is mainly the access contract. `branch/v1` plus
Sync provides durable published states, current and historical fork, and
shared storage. Its checkpoint repository also avoids one-value snapshot
limits, but it is not a lakeFS-style range index. A Managed Mount can reuse the
same branch authority. Matching Overeasy's eager filesystem recovery still
requires Mount writeback, session, and journal rules.

# Design and storage semantics

## Feature and module boundary

The implementation lives under:

```text
managed::extensions::branch
```

The Cargo feature `managed-branch` owns the Object and D1 implementations,
branch lifecycle API, history codec, and branch-bound `ManagedVolume`
adapters. Default builds enable the feature. Builds can exclude it and still
compile the base Object and D1 Managed paths.

A small amount of negotiation plumbing remains unconditional:

- `BranchName`, `BranchId`, and `BranchBinding`;
- `AuthorityIdentity`; and
- the optional branch binding in Sync replica state.

These values are part of the common authority and compatibility boundary, not
the extension implementation. Keeping them unconditional lets feature-on and
feature-off binaries read the same base replica-state format. A feature-off
binary rejects a superblock requiring `branch/v1` before opening its namespace.

The superblock stores required extension identifiers in strict sorted order.
Duplicates, unknown identifiers, and malformed records fail closed. Base
Managed constructors reject non-empty extension sets, while branch constructors
require `branch/v1` and a matching `VolumeId` and metadata format.

## Authority model

The logical registry is:

```text
BranchRegistry {
    volume_id
    default_branch: BranchId
    branches: BranchName -> BranchId
    maintenance_epoch
    maintenance_state
    maintenance_owner
}
```

The registry is authoritative for branch existence and name reuse. Ordinary
publication does not update it, so unrelated branches do not contend on a
global record.

Each registration points to one mutable head:

```text
BranchHead {
    volume_id
    branch_id
    lifecycle: active | sealed
    state: unborn | NamespaceState
    maintenance_epoch
    maintenance_state
    maintenance_owner
}

NamespaceState {
    checkpoint
    checkpoint_cursor
    bounded_tail
    previous_history
}
```

An unborn head represents an empty branch. The first publication creates its
checkpoint. An active head accepts publication. A sealed head never becomes
active again.

`BranchName` is case-sensitive and 1 to 63 ASCII bytes. It starts with an
ASCII letter or digit; remaining bytes may also contain `.`, `_`, and `-`.
Names are data inside the registry and are never interpolated into object keys
or SQL identifiers.

`BranchId` is a random 128-bit identity for one incarnation. A durable position
is therefore:

```text
(VolumeId, BranchId, ChangeCursor)
```

Backend revisions are compare-and-swap tokens, not persistent logical
identities.

## History and operation identity

A head tail is consecutive from `checkpoint_cursor` to the current cursor. On
rotation, metadata stores the old checkpoint and tail in an immutable history
record, writes the new immutable checkpoint, and conditionally replaces the
head. A failed head replacement can leave unreachable immutable records but
cannot expose a partial namespace.

Historical fork locates the requested sequence in the current tail or walks
the history chain. The target head references the matching checkpoint, tail
prefix, and older history. Namespace and data records are not copied.

Committed operation results are scoped by both origin branch and operation:

```text
(BranchId, OperationId) -> committed ChangeCursor
```

Forked checkpoints may contain results created by an ancestor. The origin
scope prevents a pending operation from one branch resolving as committed in
another. Reusing an operation in its origin branch with a different request
digest is a conflict.

## Bind once, then use the existing Volume contract

Branch control and namespace publication are separate:

```text
BranchStore
    initialize(default)
    list()
    get(name)
    fork(source, point, target)
    delete(name)
    bind(name) -> Namespace
    garbage_collect(data)

Namespace
    observe_from(base)
    publish(observation, publication)
    resolve(operation)
```

`bind` fixes one `BranchId`. The resulting namespace is composed with the
existing `ManagedData` implementation and exposed as the existing `Volume`
contract. Branch parameters do not leak into file staging, materialization,
filesystem validation, or every Volume method.

This is the same structural idea as an OpenDAL layer, applied one level above
OpenDAL. The composition changes metadata authority; it does not intercept
storage requests.

## Publication

Publication remains data before metadata:

```text
observe bound branch
    -> stage immutable segments
    -> validate generations against the observation
    -> prepare checkpoint or history if needed
    -> conditionally replace that branch head
    -> acknowledge the branch position
```

The head revision and registry maintenance state are checked at commit. A
concurrent publication, deletion, or GC fence causes a conflict rather than a
last-writer-wins update.

Sync writes a pending intent before staging remote data. If GC begins after
observation, the old publication cannot pass the fenced head. A later Sync
attempt resolves the intent and stages the immutable segments again before
retrying publication. Mount must preserve the same observation-to-publication
contract; if it reuses staged descriptors across sessions, it will need an
explicit pin or lease.

## Object Metadata representation

Object Metadata uses an extension-owned prefix:

```text
.ofs/managed/metadata/v1/extensions/branch/v1/registry.ofs
.ofs/managed/metadata/v1/extensions/branch/v1/heads/<branch-id>.ofs
.ofs/managed/metadata/v1/extensions/branch/v1/checkpoints/sha256/<digest>.ofs
.ofs/managed/metadata/v1/extensions/branch/v1/checkpoint-parts/sha256/<digest>.ofs
.ofs/managed/metadata/v1/extensions/branch/v1/history/sha256/<digest>.ofs
```

The registry and heads are mutable conditional-write objects. A checkpoint is
a small content-addressed root over immutable content-addressed parts. Parts
contain natural namespace records such as nodes, directory entries, file
extents, and operation receipts. The root is published only after every part
has been created and verified. History is also immutable and
content-addressed. The base Managed `head.ofs` is not read or mirrored for a
branching volume.

Object and D1 share the record-set builder, recovery validation, and mutable
record state machine. Their adapters provide only native read, create,
revision-CAS replace, list, and delete operations. Checkpoint capacity is the
sum of its immutable parts rather than one encoded snapshot value.

Reads obtain an ETag with `stat` and then issue an `If-Match` read. The decoded
bytes and the revision used by the next conditional write therefore belong to
the same object generation. Opening the store requires OpenDAL capabilities
for read, conditional read, stat, create-only write, conditional replace,
list, and delete as needed by the selected operation.

Correctness never depends on listing heads. The registry supplies live branch
roots. Listing is used only during garbage collection of unreachable objects.

## D1 representation

D1 uses one extension-owned record table:

```text
ofs_managed_branch_v1_records
```

Rows are scoped by the existing D1 store key, `VolumeId`, and the same logical
record key used by Object Metadata. Lifecycle and publication use revision
predicates. Immutable parts are written and verified idempotently before a
checkpoint root is published. Missing, duplicated, reordered, or modified
parts are corruption.

The registry remains one small mutable authority record. D1's value limit and
the selected Object provider's conditional-write limit are its real
boundaries. Very large branch registries need a different authority
representation, not an arbitrary format cutoff.

## Fork and deletion linearization

Fork prepares a new head with a new `BranchId`, then conditionally adds its
name to the observed registry. The registry update is the existence
linearization point. A source publication can order before or after the fork;
an existing target name, source deletion, or active GC fence prevents the
registry update.

Deletion first seals the exact registered head, then conditionally removes
that exact name-to-id mapping. A crash between those operations leaves a sealed
branch that a repeated delete can finish. Publication either wins the head
update before the seal or conflicts with it.

Name reuse is safe because deletion compares the registered `BranchId`. A
retry for an old incarnation never removes a replacement with the same name.

## Garbage collection

GC is fenced at the branch registry with an epoch and an owner token. Every
current head is marked with the same token because registry and heads are
separate revision-CAS records.

The collector recovers every snapshot represented by each current head and
each retained history interval. It unions reachable `SegmentRef` values and
then sweeps the shared data prefix once. Missing or corrupt roots abort the
sweep; uncertain reachability retains data.

Lifecycle and publication stay blocked until deletion succeeds and the fence
is released. A failed mark or sweep leaves the epoch active for explicit
recovery. A normal GC rejects an active epoch. After the operator has confirmed
that the previous process stopped, `--resume` conditionally replaces the owner
token and continues the same fixed epoch. The old owner can no longer mark,
sweep metadata, or release the fence. `--resume` is not a concurrent takeover
protocol; running it while the old collector can still delete data violates
its command precondition.

Unpublished immutable segments may be removed, but a fenced Sync publication
cannot reference them and retry stages them again.

Extension metadata uses the same roots. Registered heads, referenced
checkpoint roots, checkpoint parts, and reachable history remain live. Sealed
or unregistered heads and unreferenced immutable records can be reclaimed
under the same fence. Listing and deletion are paged or streamed, so provider
request size is not a namespace-format limit.

## Sync and future Mount

Sync owns replica state, local conflict retention, and the local durability
boundary. The branch binding is validated before replica directory creation,
scan, materialization, or state update. State format 1 records an optional
branch binding and a relative pending-cache name. Development-time layouts are
not compatibility formats and are rejected.

The pending staging tree is a cache, not authority. If it is missing or
damaged before publication, Sync rebuilds it from the current replica. If the
operation already committed, Sync rebuilds from the authoritative snapshot
when that cannot overwrite a local change. This permits moving a state file
with its replica and recovering after local cache cleanup.

Mount will consume the same bound `Volume` and can reuse branch selection,
publication, history, data staging, and GC roots. Mount still needs its own
contract for handle lifetime, cache coherence, writeback, `flush`, `fsync`,
writer sessions, and any eager journal. Those concerns do not belong in the
branch metadata extension.

## Failure behavior

| Failure or race | Result |
| --- | --- |
| Immutable preparation succeeds, head CAS fails | Unreachable immutable record; retry from the new head |
| Head update result is unknown | Resolve by `(BranchId, OperationId)` |
| Fork or delete result is unknown | Re-read the exact name-to-identity mapping before reporting failure |
| Source publishes during fork | Fork orders before or after that publication |
| Target names race | One new incarnation is registered |
| Publication races with deletion | Publication or seal wins; never both |
| Delete crashes after Object head seal | Repeated delete completes registry removal |
| Deleted name is reused | Old replica fails branch identity validation |
| GC races with publication or fork | Registry epoch forces the mutation to retry |
| Two ordinary GC commands overlap | The second command fails without joining the epoch |
| `--resume` while the prior collector still runs | Operator error; stop the prior process before recovery |
| Retained record is missing or corrupt | Fail closed; do not synthesize state or sweep data |

# Acceptance and regression coverage

One CLI acceptance workflow runs unchanged against Object and D1 metadata:

```text
cargo x managed-sync test branch object
cargo x managed-sync test branch d1
```

It covers Direct rejection before mutation, remote extension negotiation,
default branch creation, current fork, independent divergence, historical
fork after a long history, deletion and name reuse, stale replica rejection
without local mutation, multi-root GC, cold materialization after GC, a large
namespace publication with a retained parent, and stable JSON status.

Regression tests cover mistakes that would silently violate durable behavior:

- every cursor remains recoverable across checkpoint rotation;
- committed operations are scoped to their origin branch;
- deleting an old incarnation cannot remove a recreated name; and
- divergent branches contribute independent GC roots.

Tests do not constrain buffer allocation, SQL statement layout, object-key
call order, private error variants, or other implementation details users
cannot observe.

# Drawbacks and limits

Retaining every historical position retains its metadata and referenced data.
`branch/v1` has no pruning policy, immutable tag, or trash interval.

Historical lookup is linear in the number of archived history segments. A
disposable index may improve lookup without becoming authority.

Checkpoint storage is split and provider requests are bounded, but recovery
still constructs one in-memory `NamespaceSnapshot`. This is suitable for Sync,
but it is below lakeFS's range-indexed metadata lookup for very large
namespaces. The record-set root can later point to indexed ranges without
changing registry, head, history, or branch binding semantics.

The registry is a single mutable record. Branch lifecycle is therefore atomic
and simple, but the practical branch count remains bounded by the selected
backend. A content-addressed branch index behind the mutable root is a later
format change if that limit becomes material.

GC briefly blocks publication and branch lifecycle. This favors a small,
auditable safety protocol over concurrent reclamation. A future implementation
may add retention pins or generational deletion that permits more concurrency
without weakening reachability.

There is no merge, tag, branch reset, mounted frontend, writer lease, or
filesystem-operation journal. These are separate capabilities and should not
be hidden behind the branch flag.

# Rationale and prior art

[lakeFS](https://github.com/treeverse/lakeFS) separates immutable commits and
metadata ranges from small mutable branch references. That separation,
optimistic branch updates, and multi-root retention inform `branch/v1`.
lakeFS merge, tags, staging tokens, and object-key namespace semantics are not
required here.

[JuiceFS](https://github.com/juicedata/juicefs) separates transactional
filesystem metadata from immutable object data. Its clone shares data slices
but copies namespace metadata, so clone work scales with the tree. Its
sessions, trash, and delayed cleanup inform future Mount and retention work.
OFS instead reuses immutable whole-namespace checkpoints.

[Overeasy](https://github.com/modal-labs/overeasy) combines a copy-on-write
FUSE session with an append-only event stream, checkpoint barriers, and fork
from current or earlier time. `branch/v1` reuses its useful storage semantics
without putting a FUSE session or event-log contract into Managed metadata.
Matching its interactive filesystem experience requires the later Managed
Mount and journal work described above.

A lakeFS-style commit object for every publication would prepare for merge and
tags, but would duplicate the existing checkpoint and change-tail model.
Copying every namespace record, as in a metadata clone, would lose constant
size fork. Exact storage reference counts would make Object Metadata recovery
and fork more complex and could fail dangerously by undercounting. Tracing
from immutable roots is slower but fails safely by retaining data.

Branches also cannot be modeled as separate volumes. That would duplicate
catalog entries and superblocks while making shared-data GC unsafe. They are
independent namespace authorities inside one Managed volume.
