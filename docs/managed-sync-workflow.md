# Managed Sync workflow

Managed Sync uses an ordinary local directory as a replica. It reads and writes
remote state only when `ofs sync` runs. No daemon watches the directory, and
normal file writes do not publish data.

## Create a volume with Object Metadata

Choose a catalog path and configure provider credentials outside the catalog:

```shell
export OFS_CONFIG="$PWD/volumes.json"
export AWS_ACCESS_KEY_ID='<access-key-id>'
export AWS_SECRET_ACCESS_KEY='<secret-access-key>'
export AWS_REGION='us-east-1'

ofs volume create workspace \
  --model managed \
  --storage 's3://bucket/prefix?region=us-east-1'
```

Without `--metadata`, namespace metadata is stored beside the data objects.
The storage URL saved in the catalog must not contain credentials.
`OFS_STORAGE_URL` may provide the storage URL instead of `--storage`.

The command registers `workspace` as a client-local alias, creating the remote
volume only when it is absent.

## Create a volume with D1 Metadata

D1 stores namespace metadata while OpenDAL storage continues to hold file
segments:

```shell
export OFS_CONFIG="$PWD/volumes.json"
export OFS_D1_TOKEN='<api-token>'

ofs volume create workspace \
  --model managed \
  --storage 's3://bucket/prefix?region=us-east-1' \
  --metadata 'd1://ACCOUNT_ID/DATABASE_ID/STORE_KEY'
```

`OFS_METADATA_URL` may provide the same credential-free D1 URL when
`--metadata` is omitted. The D1 token is accepted only through
`OFS_D1_TOKEN`.

## Establish a replica

The state file must be outside the synchronized directory:

```shell
mkdir -p ./workspace

ofs sync workspace ./workspace \
  --state ./workspace.state
```

The first sync has two possible outcomes:

- If the volume is empty and the local directory has files, the command
  publishes the local tree.
- If the volume already contains files and the local directory is empty, the
  command materializes the published tree.

Do not place the state file inside the replica.

## Attach from another container

Aliases belong to a local catalog and do not need to match between containers.
With the same credentials and storage locators, another container can choose a
different catalog and alias:

```shell
export OFS_CONFIG=/state/agent-b-volumes.json

ofs volume create restored-memory \
  --model managed \
  --storage 's3://bucket/prefix?region=us-east-1'

mkdir -p /workspace/memory
ofs sync restored-memory /workspace/memory \
  --state /state/agent-b-replica.state
```

The create command discovers the existing remote volume. The first sync into
the empty directory materializes its current state; aliases do not participate
in reconciliation.

## Work and publish

Edit the replica with normal filesystem tools, then run the same command:

```shell
ofs sync workspace ./workspace \
  --state ./workspace.state
```

The command resolves interrupted work, reconciles one fixed remote snapshot,
and installs or publishes the result. Disjoint changes merge. A publisher that
loses a concurrent publication race stops and reconciles on the next explicit
sync. See [Managed Sync architecture](managed-sync-architecture.md#sync-transaction)
for the transaction path.

Use `--transfer-concurrency` or `OFS_TRANSFER_CONCURRENCY` to bound storage
operations for one command. The default is four.

## Inspect local state

Status reads the state file and the matching catalog entry. It does not
reconcile or modify local or remote data:

```shell
ofs status --state ./workspace.state
ofs status --state ./workspace.state --json
```

The JSON object contains:

```json
{
  "volume_alias": "workspace",
  "volume_id": "00112233445566778899aabbccddeeff",
  "volume_model": "managed",
  "access_model": "sync",
  "capabilities": {
    "portable_names": true,
    "stable_rename_identity": true,
    "executable": true,
    "symbolic_links": false,
    "hard_links": false,
    "remote_durability": "explicit_sync",
    "namespace_publication": "generation_cas"
  },
  "common_sequence": 12,
  "pending": false,
  "conflicts": 0
}
```

## Use branches

Enable the built-in branch extension when creating the volume:

```shell
ofs volume create workspace \
  --model managed \
  --enable branch \
  --storage <storage-url>
```

This creates the default branch `main`. Select another branch with `--branch`,
or create one from a current or retained position:

```shell
ofs sync workspace ./experiment --branch experiment --state ./experiment.state
ofs branch workspace create experiment
ofs branch workspace create retry --from experiment
ofs branch workspace create rewind --from main --at 42
```

Inspect or remove branches with:

```shell
ofs branch workspace list
ofs branch workspace show experiment
ofs branch workspace delete experiment
```

The default branch cannot be deleted. Deleting and recreating a name creates a
new branch identity, so start it with a new replica state file.

Reachability collection is always explicit:

```shell
ofs volume gc workspace
ofs volume gc workspace --resume # only after an interrupted collection
```

See [Managed Sync architecture](managed-sync-architecture.md#built-in-branches)
for branch semantics and limits.

## Resolve conflicts

When local and remote changes overlap, Sync keeps the local candidate and
records a conflict. It does not choose one side automatically.

Inspect the state, edit the local candidate if needed, then select that path
for publication:

```shell
ofs status --state ./workspace.state

ofs sync workspace ./workspace \
  --state ./workspace.state \
  --resolve path/to/file
```

`--resolve` may be repeated. Sync revalidates the remote snapshot before it
publishes a resolution.

## Recover an interrupted sync

If the command reports an unknown publication result, repeat the same sync
with the same replica and state file:

```shell
ofs sync workspace ./workspace \
  --state ./workspace.state
```

Keep the replica and state file together. The repeated command resolves any
pending publication or installation before starting new work.

## Recover a lost replica

The directory and its state file form one replica. Recreating an empty
directory while reusing its old state presents every missing path as a local
deletion.

For a cold recovery, use an empty directory and a new state path:

```shell
mkdir -p ./recovered

ofs sync workspace ./recovered \
  --state ./recovered.state
```

If the catalog was also lost, first run `volume create --model managed` with
the original storage and metadata locators. The replacement alias may differ
from the lost one. The command reads the existing superblock and binds the new
local alias to that volume identity.

## Filesystem surface

Managed Sync accepts regular files, directories, empty directories, portable
UTF-8 names, and the Unix executable bit. It rejects links, unsupported file
types, and ambiguous or non-portable names before publication. The precise
admission rules are explained in
[Managed Sync architecture](managed-sync-architecture.md#filesystem-admission).

The replica contains only user files. Catalog data, credentials, replica
state, staging data, and conflict records stay outside the synchronized tree.

## Related documents

- [Managed Sync architecture](managed-sync-architecture.md)
- [Managed storage format](managed-storage-format.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
