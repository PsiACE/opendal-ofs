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

Agents may remain at older generations until they choose to sync. The expected
operating model has many readers and one active publisher, while conditional
publication prevents a stale writer from overwriting a newer head.

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
                                              current identity conflict
```

The current merge treats the two directory nodes as different identities and
reports the directory path as `divergent_rename`. This is a conservative
current behavior, not a claim that the child-file edits are semantically
overlapping. It applies only when the directory is independently created from
a base where it did not exist; agents that materialized the directory from a
published generation share its NodeId and do not conflict merely because they
use the same directory name.

An explicit `--resolve PATH` selects the local directory identity after
revalidating the remote cursor. In the observed disjoint-child case, the next
merge retained both children. Coalescing two genuinely new directory
identities automatically would require a narrower rule that does not also
merge a renamed existing directory with an unrelated new directory.

Until that behavior is refined, publish common top-level agent directories
before multiple agents begin editing them, or resolve a concurrent first
creation explicitly.

## Verified behavior

The workflow was confirmed with four isolated Fedora agent containers and one
MinIO container. Every agent container had its own catalog, named local volume
definition, ordinary tree, and replica state. All four definitions recovered
the same remote VolumeId from one MinIO bucket and root.

The run observed these results:

- initial publication and multiple readers converged at generation 1;
- an unpublished local edit remained absent from two other replicas;
- alternating publishers and a lagging reader converged at generation 3;
- a new fourth agent cold-recovered the same tree;
- concurrent disjoint edits below established directories were both retained,
  and all replicas converged at generation 5;
- concurrent first creation of one absent directory produced one
  `divergent_rename` conflict at that directory;
- explicit resolution retained both child files and converged at generation 7;
- deleting one agent's catalog, tree, and replica state still allowed exact
  cold recovery to generation 7.

At the end, all four trees were identical. Every replica reported `clean`,
`at_base`, idle publication and materialization state, and zero conflicts.
The concurrent disjoint publications happened to serialize successfully in
this run; this observation proves convergence without lost content, but does
not by itself demonstrate a forced compare-and-swap loser.

The cold-recovery step removed the lost replica's tree and established state
before syncing an empty directory. An empty directory that still uses its old
replica state is not a cold replica: its missing paths are local deletions and
can be published as such.

A separate MinIO check confirmed this distinction. After a generation 1 tree
was removed and recreated empty while retaining its state, `ofs status`
reported `local=changed` with the remote still `at_base`. Running sync then
published the complete deletion as generation 2.
