# Managed Sync workflow

Managed Sync gives each agent an ordinary local directory while one remote
Managed Volume holds the published state. There is no background daemon. Local
edits remain private until an agent explicitly runs `ofs sync`.

This explanation shows how replicas share published generations, how one sync
uses a common base and incremental changes, and where directory identity
currently creates a conservative conflict.

## User-visible workflow

Each agent has its own volume catalog, local directory, and durable replica
state. Their volume definitions can refer to the same remote Managed Volume.

```text
                         Managed Volume
                 authoritative head and change log
                    immutable content objects
                                |
                         published G0
                                |
                 +--------------+--------------+
                 |                             |
                 v                             v
        +------------------+          +------------------+
        | Agent A replica  |          | Agent B replica  |
        | ordinary tree    |          | ordinary tree    |
        | common base: G0  |          | common base: G0  |
        +------------------+          +------------------+
                 |                             |
          local edits stay              keeps reading G0
              private                          |
                 |                             |
          explicit ofs sync                    |
                 |                             |
                 +---- publish changes ----> G1
                                               |
                                        explicit ofs sync
                                               |
                                        replay G0 -> G1
                                        materialize files
                                               |
                                               v
                                      common base becomes G1
```

A local file write or `fsync` does not publish anything. A reader receives a
new generation only when it runs its next explicit sync.

## What one sync does

An established replica stores a durable common base: a cursor and the manifest
that was last verified to match both its local directory and the published
volume.

```text
 Replica common base                    Observed authority head
 cursor and manifest Gx                 fixed target cursor Gy
          |                                      |
          |                           replay changes Gx -> Gy
          |                                      |
          v                                      v
    Base manifest                         Remote manifest
          |                                      |
          +------------------+-------------------+
                             |
 Local directory             |
       |                     |
       v                     v
 Local manifest ------> three-way merge
                             |
                +------------+------------+
                |                         |
                v                         v
            conflicts                merged target
                |                         |
       retain candidates       +----------+----------+
       and stop this sync      |                     |
                               v                     v
                       target == remote      target != remote
                               |                     |
                        materialize only     diff(remote, target)
                                                     |
                                             save publication intent
                                                     |
                                             upload immutable data
                                                     |
                                             write immutable commit
                                                     |
                                             CAS authority head
                               |                     |
                               +----------+----------+
                                          |
                                  materialize target
                                          |
                                  verify complete tree
                                          |
                                          v
                                advance durable common base
```

Remote reconstruction and publication use different comparison points:

```text
Remote catch-up: common base -> replay consecutive changes -> fixed remote head
Publication:     diff(fixed remote head, merged target)
Durability:      materialize and verify -> advance the common base
```

Publishing `diff(remote, target)` is important. Publishing a stale
`diff(base, local)` could discard changes that another agent has already made
visible.

If the head compare-and-swap is stale, the current sync returns an error. A
later explicit sync observes the new head and performs reconciliation again.
The command does not silently start another publication from a moving target.

## Multiple agents

The normal workflow establishes shared directory identities before agents
modify their children:

```text
Published G1
.agents, NodeId=X
        |
        +-----------------------+
        |                       |
        v                       v
Agent A syncs G1         Agent B syncs G1
.agents, NodeId=X        .agents, NodeId=X
        |                       |
add .agents/a.md         add .agents/b.md
        |                       |
        +-----------+-----------+
                    |
              three-way merge
                    |
                    v
          both child files are retained
```

Agents may remain at older generations until they choose to sync. Publishers
can take turns after catching up. If two publications race from the same head,
conditional publication allows only one to advance it; the loser reconciles
again on a later explicit sync.

## Concurrent creation of the same directory

Directory identity matters when two established replicas independently create
the same path that is absent from their common base:

```text
Shared base G0: .agents does not exist
          |
          +----------------------+----------------------+
          |                                             |
          v                                             v
Agent A creates                              Agent B creates
.agents, NodeId=A                            .agents, NodeId=B
.agents/a.md                                 .agents/b.md
          |                                             |
          | publishes G1                                | syncs later
          v                                             v
Remote: .agents, NodeId=A                    Base:   absent
Remote: .agents/a.md                         Local:  directory ID B
                                             Remote: directory ID A
                                                        |
                                                        v
                                           coalesce new directory
                                           to remote NodeId=A
                                                        |
                                                        v
                                           retain a.md and b.md
                                           publish only B's delta
```

The merge coalesces the two directory identities only when the path is absent
from the common base, both sides contain a directory there, and neither
directory identity belonged to any node in the base. It keeps the authority's
directory identity, then merges children by their own paths. The rule applies
independently at each missing component, so concurrent
`mkdir -p .agents/skills/...` can coalesce both `.agents` and
`.agents/skills`.

The base-identity check is the important safety boundary. A directory renamed
from another base path is not treated as a new directory, so a rename into the
same path as an unrelated new directory still reports `divergent_rename`.
Different node types still report `incompatible_type_replacement`, and two
different files created at the same child path still conflict. Coalescing a
directory does not silently select between overlapping child edits.

## Verified behavior

The directory-churn workflow was confirmed with four isolated Fedora agent
containers and one MinIO container. Every agent container had its own named
volume, catalog, home tree, replica state, and persistent container volume.
All four used one authoritative MinIO bucket and root.

The initial tree contained 1,000 files and 968 directories under `.agents`,
`.bub`, and `.codex`. The next 12 update publications rotated across A, B, C,
and D. Before editing, each next publisher completely caught up; it then
modified four files, deleted four files and their directories, and added four
new directories and files. The run reached generation 13 without a
materialization error. All four replicas converged, and a fifth empty replica
cold-recovered the same 1,000-file digest.

The checked-in tests separately cover established empty replicas concurrently
creating nested standard directories, an upgrade introducing a new public
directory, deletion followed by concurrent recreation, and the conflicts that
must not be coalesced.

The cold-recovery step removed the lost replica's tree and established state
before syncing an empty directory. An empty directory that still uses its old
replica state is not a cold replica: its missing paths are local deletions and
can be published as such.

A separate MinIO check confirmed this distinction. After a generation 1 tree
was removed and recreated empty while retaining its state, `ofs status`
reported `local=changed` with the remote still `at_base`. Running sync then
published the complete deletion as generation 2.
