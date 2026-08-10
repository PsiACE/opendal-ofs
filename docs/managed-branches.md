# Managed branches

`branch/v1` adds durable named namespace authorities to a Managed volume.
Branches share immutable checkpoints, file versions, and data segments. A fork
creates metadata references, not copies of file bytes or another filesystem
model.

## Commands

Enable branches when the volume is created:

```shell
ofs volume create workspace \
  --model managed \
  --enable branch \
  --storage <storage-url>
```

This creates the default branch `main`. Select another branch when syncing:

```shell
ofs sync workspace ./main --state ./main.state
ofs sync workspace ./experiment --branch experiment --state ./experiment.state
```

Create from the current default head, another branch, or a retained sequence:

```shell
ofs branch create workspace experiment
ofs branch create workspace retry --from experiment
ofs branch create workspace rewind --from main --at 42
```

Inspect and delete branches with `branch list`, `branch show`, and `branch
delete`. The default branch cannot be deleted. Both inspection commands accept
`--json`.

Replica state records the branch name and immutable `BranchId`. It cannot move
between branches. Deleting and recreating a name creates another identity, so
an old replica cannot attach to the replacement.

## Authority records

The registry maps case-sensitive `BranchName` values to `BranchId` values and
identifies the default branch. Names are 1 to 63 ASCII bytes, start with a
letter or digit, and otherwise accept letters, digits, `.`, `_`, and `-`.
Names are record values; they are never interpolated into object keys or SQL
identifiers.

Each `BranchId` selects one unborn, active, or sealed namespace HEAD. Object
Metadata stores the registry and heads at:

```text
.ofs/managed/metadata/v1/extensions/branch/v1/registry.ofs
.ofs/managed/metadata/v1/extensions/branch/v1/heads/<branch-id>.ofs
```

D1 stores the same logical records in its authority table. Both authorities
reuse the base namespace state machine, checkpoint codec, operation receipts,
publication validation, and data plane.

## Retained sequences

A namespace HEAD references one checkpoint, its current transaction tail, and
a bounded set of immutable change segments. Forking at a retained sequence
selects the checkpoint and change prefix that reconstruct that position. The
new branch reuses those immutable records.

A sequence older than retained history is rejected. Sequences are
branch-local: two branches may publish the same sequence after they fork.

Committed operation identity is scoped by its originating authority. A fork
does not copy the source's latest operation result into the target authority,
so ancestor operations cannot resolve as target-branch commits.

## Creation and deletion

Fork writes a new HEAD with a new `BranchId`, then conditionally registers its
name. The registry update is the creation commit point. A concurrent source
publication therefore orders before or after the fork.

Deletion seals the exact registered HEAD, then conditionally removes its exact
name-to-id mapping. Retrying finishes a crash between those steps. Comparing
`BranchId` as well as the name prevents an old deletion or replica from
affecting a replacement incarnation.

| Race or failure | Result |
| --- | --- |
| Source publishes during fork | Fork orders before or after it |
| Two creators choose one name | One identity is registered |
| Publication races with deletion | Publication or sealing wins |
| Delete stops after sealing | Retrying removes the registry entry |
| Commit result is unknown | Resolve the saved operation from HEAD or its receipt |
| Referenced data is missing or corrupt | Fail closed |

## Limits

`branch/v1` provides named mutable branches, constant-copy fork of retained
positions, deletion, and Sync recovery. It does not provide merge, tags,
reset, unbounded history, automatic garbage collection, Mount, or writer
leases.

Run the Object and D1 acceptance scenarios with:

```shell
tests/behavior/managed-sync/run.sh test branch object
tests/behavior/managed-sync/run.sh test branch d1
```

## Related documents

- [Managed Sync workflow](managed-sync-workflow.md)
- [Managed storage format](managed-storage-format.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
