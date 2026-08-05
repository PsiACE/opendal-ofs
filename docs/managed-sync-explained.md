# Managed Sync explained

Managed Sync turns remote object storage into a deliberate publication
workflow for ordinary directories. It combines a named Managed Volume with a
foreground Sync Access command. Nothing is mounted, and no daemon watches the
directory.

## The problem it solves

Short-lived agents often need the same memory, skills, session history, and
configuration on different servers or sandboxes. Those files must be easy to
use locally, but an unfinished or incorrect local edit should not immediately
become visible to every other agent.

Managed Sync separates local work from published state:

1. An agent synchronizes the latest published generation into an ordinary
   directory.
2. The agent reads and writes that directory with normal file tools.
3. Changes remain private while the agent works.
4. An explicit `ofs sync` reconciles and, when safe, publishes the complete
   directory change.
5. Other agents receive the new generation on their next explicit sync.

The normal operating model allows many readers at different generations and
one active publisher at a time. Conditional publication still protects the
volume if two publishers overlap.

## Volume, authority, content, and replica

A Managed Volume has four distinct kinds of state:

- The **volume definition** gives a local name to one Data Store and one
  Metadata Store. It is kept in the local catalog without credentials.
- The **Metadata Store** owns the volume identity, current generation, change
  log, publication fencing, and authoritative head.
- The **Data Store** holds immutable file content addressed by SHA-256 digest.
- A **replica** is one ordinary directory plus durable private state stored
  outside that directory.

Only content referenced by the authoritative Metadata Store head is published.
Uploading immutable data alone cannot make it visible.

The Metadata Store can be colocated in the same OpenDAL root as content or can
be an external D1 database. Both placements implement the same volume and sync
contract. Placement changes configuration, not user behavior.

## What one sync does

One `ofs sync` invocation performs a complete foreground reconciliation and
then exits.

It first recovers any interrupted publication or materialization recorded in
the replica state. It then observes one fixed authority position, reconstructs
the remote target, scans a stable local tree, and compares both sides with the
replica's durable common base.

If local and remote changes are disjoint, both are retained. If a local change
must be published, changed file content is stabilized and uploaded before one
immutable namespace change record is written. A conditional head update makes
that record visible as the next generation. The common base advances only
after the final local tree is durable and verified.

A create, modify, delete, rename, executable-bit change, and empty-directory
change prepared together form one publication unit and advance one generation.

## Incremental change log

Established replicas remember a cursor containing both generation and
operation identity. To catch up, a replica consumes consecutive namespace
change records from its common cursor to the authority position observed at
the start of sync.

Each regular change record contains changed paths, not a full directory
snapshot. The initial checkpoint exists for first binding and cold recovery.
This keeps routine publications proportional to the change set while retaining
a complete recovery path when no replica state survives.

A long-offline replica must still read every missed change record. With the D1
adapter, this currently requires one authoritative query per missed generation,
so catch-up latency grows with the generation gap even though transferred file
content remains deduplicated.

## Multiple agents

A fresh agent synchronizes an empty directory and becomes an established
reader. It can leave at any time because the published state does not depend on
that replica remaining online.

When agents take turns writing, each new publisher first synchronizes the
latest generation, makes its local changes, and explicitly synchronizes again.
Older readers remain on their existing local generation until they choose to
catch up.

If two publishers start from the same authority position, only one conditional
head update can win. The other publisher re-observes the volume and reconciles
its local work with the winner. A stale writer cannot overwrite a newer
generation.

## Conflicts

Managed Sync automatically retains changes that are independent. It does not
guess when both sides modify the same logical node, when one side deletes what
the other modifies, or when the same node is renamed differently.

Such a conflict blocks the complete publication unit. The local shape remains
in the directory, the remote candidate is recorded outside it, and `ofs
status` reports the conflict. The user chooses retained local paths with
`--resolve`; resolution revalidates the remote generation before publishing.

## Failure and recovery

Publication is ordered data before metadata. A durable operation identity is
recorded before the authority can advance. If a process or network failure
makes the publication result uncertain, the next sync resolves that same
operation before preparing another one.

Materialization uses private staging and a durable intent. Downloaded content
is verified before installation, and the replica common base does not advance
while the visible directory is partial.

Three forms of local loss have different meanings:

- Losing only the directory allows reconstruction from its retained replica
  state or a cold sync.
- Losing the directory and replica state creates a new cold replica.
- Losing the local catalog requires recreating the same named definition from
  the same Data Store and Metadata Store locators before cold sync.

None of these actions delete the remote volume. Remote destruction and garbage
collection require an explicit retention and ownership contract and are not
part of Managed Sync.

## Filesystem boundary

Managed Sync accepts regular files, directories, empty directories, portable
names, and executable bits on Unix. It rejects unsupported types and ambiguous
portable names before remote publication.

The synchronized directory never contains ofs control files. Catalog,
credentials, replica binding, common base, publication intent, materialization
journal, staging data, and conflicts remain outside the user's tree.
