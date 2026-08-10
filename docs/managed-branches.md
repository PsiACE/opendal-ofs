# Managed branches

`branch/v1` is a required Managed-format extension for durable named namespace
authorities. Branches share immutable file versions, checkpoints, change
segments, and data segments. Forking changes metadata roots; it does not copy
file bytes or create another filesystem model.

## Use branches

Enable branches when creating a Managed volume:

```shell
ofs volume create workspace \
  --model managed \
  --enable branch \
  --storage <storage-url>
```

The superblock records `branch/v1`, and initialization creates the default
branch `main`. A client opening an existing volume observes this requirement
from the superblock even if its `volume create` command omits `--enable`.

Sync uses the default branch unless another name is selected:

```shell
ofs sync workspace ./main --state ./main.state
ofs sync workspace ./experiment --branch experiment --state ./experiment.state
```

Replica state stores the branch name and immutable `BranchId`. A state file
cannot move between branches. Deleting and recreating a name produces another
`BranchId`, so an old replica cannot attach to the replacement.

Create a branch from the current default head, another branch, or a retained
sequence:

```shell
ofs branch create workspace experiment
ofs branch create workspace retry --from experiment
ofs branch create workspace rewind --from main --at 42
```

Inspect and delete branches with:

```shell
ofs branch list workspace
ofs branch show workspace experiment
ofs branch delete workspace experiment
```

The default branch cannot be deleted. `--json` reports branch name, identity,
sequence, default status, and lifecycle without exposing physical metadata
keys or backend revisions.

## Authority boundary

Branching binds one authority before constructing the existing
`ManagedVolume`:

```text
superblock requires branch/v1
        |
registry resolves BranchName -> BranchId
        |
NamespaceStore binds that branch HEAD
        |
ManagedVolume -> Sync
```

The extension lives in `managed::extensions::branch`; it is not a Cargo
feature and has no alternate implementation graph. Base and branch authorities
use the same namespace state machine, checkpoint codec, publication
validation, operation receipts, file-version descriptors, and data plane.
Object and D1 metadata differ only through the shared revision-CAS record
backend.

The registry is authoritative for branch existence and name reuse. Ordinary
publication changes only the selected branch HEAD, so unrelated branches do
not contend on the registry.

## Registry and heads

The registry contains:

```text
volume_id
default_branch: BranchId
branches: BranchName -> BranchId
```

`BranchName` is case-sensitive ASCII, 1 to 63 bytes. It starts with an ASCII
letter or digit; later bytes may also contain `.`, `_`, and `-`. Names are
stored as registry data and are never interpolated into object keys or SQL
identifiers.

Each `BranchId` selects one HEAD:

```text
volume_id
branch_id
sealed
state: unborn | NamespaceState
```

An unborn HEAD represents an empty branch. An active HEAD accepts a
generation-checked publication. A sealed HEAD can only complete deletion.

Object metadata uses:

```text
.ofs/managed/metadata/v1/extensions/branch/v1/registry.ofs
.ofs/managed/metadata/v1/extensions/branch/v1/heads/<branch-id>.ofs
```

D1 stores the same logical record keys in
`ofs_managed_v1_authority_records`. Checkpoints, change segments, operation
receipts, file versions, and data segments remain in the configured OpenDAL
storage for both metadata authorities.

## Retained sequences

Each namespace state has one checkpoint, a current change tail, and up to
eight immutable change segments. A change segment contains its starting
checkpoint reference and at most 32 consecutive changes. The HEAD indexes
each segment by its start cursor, end cursor, digest, and encoded length.

Forking at a retained sequence selects the checkpoint and change prefix that
reconstruct that position. The new branch receives a new HEAD and reuses the
immutable records already referenced by the source. It does not copy namespace
records or data segments. A sequence older than the retained segment window is
rejected; `branch/v1` does not promise unbounded history.

Sequences are branch-local positions. Two branches may both publish sequence
43 after forking from sequence 42.

## Operation identity

A committed operation is scoped by its origin authority:

```text
(base | BranchId, OperationId) -> ChangeCursor, request digest
```

HEAD stores the most recent result and a fixed operation-prefix filter. Recent
results remain in its tail or retained change segments. Publication persists
the initial checkpoint result when displaced and persists a segment's results
before evicting that segment. Every committed operation is therefore in HEAD,
retained history, or an immutable receipt; results do not expire.

A fork clears the source's latest result from the target HEAD. Ancestor
operations therefore never resolve as operations committed by the new branch.
Reusing an operation identity with another publication payload is a conflict.

## Fork and deletion linearization

Fork writes a new HEAD with a new `BranchId`, then conditionally adds the
name-to-id mapping to the observed registry. The registry update is the branch
creation linearization point. A concurrent source publication orders before or
after the fork.

Deletion first seals the exact registered HEAD, then conditionally removes its
exact name-to-id mapping. A retry can finish a crash between those steps.
Publication either replaces the active HEAD before sealing or conflicts with
the sealed incarnation.

Name reuse is safe because deletion compares `BranchId`, not only display
name. A retry for an old incarnation cannot remove its replacement.

## Failure behavior

| Failure or race | Result |
| --- | --- |
| Immutable preparation succeeds and HEAD CAS loses | Unreachable immutable object; reconcile from the new HEAD |
| HEAD update result is unknown | Resolve the saved operation against HEAD or its receipt |
| Source publishes during fork | Fork orders before or after the publication |
| Two creators choose one name | One `BranchId` is registered |
| Publication races with deletion | Publication or sealing wins |
| Delete stops after sealing | Repeating delete removes the registry entry |
| A deleted name is reused | Old replica fails branch-incarnation validation |
| A referenced record is missing or corrupt | Fail closed |

## Limits

`branch/v1` provides named mutable branches, constant-copy fork of a retained
position, safe deletion, and cross-host Sync recovery. It does not provide
merge, tags, reset, unbounded history, automatic garbage collection, Mount,
writer leases, or a per-filesystem-call journal.

The registry is one bounded mutable record. Namespace checkpoints are bounded
compressed objects recovered into one in-memory snapshot. Those choices keep
the authority and recovery rules small and predictable; a range-indexed
namespace or another retention policy would require a later format design.

## Verification

`cargo x managed-sync test all` runs the same branch behavior against Object
and D1 metadata. It covers initialization, current and retained fork,
independent publication, deletion, name reuse, stale-replica rejection, cold
materialization, and large namespace publication. Tests assert those visible
and durable contracts rather than SQL layout, object request order, or helper
structure.

## Related documents

- [Managed Sync architecture](managed-sync-architecture.md)
- [Managed storage format](managed-storage-format.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
