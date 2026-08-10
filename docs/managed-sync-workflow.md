# Managed Sync workflow

Managed Sync reconciles an ordinary local directory only when `ofs sync` runs.
There is no daemon and no central volume registry. Each replica state file
stores the credential-free locator and remote identity needed to reconnect.

## Initialize a volume

Keep the state file outside the synchronized directory. Configure provider
credentials through environment variables, then explicitly initialize the
remote Managed format:

```shell
export AWS_ACCESS_KEY_ID='<access-key-id>'
export AWS_SECRET_ACCESS_KEY='<secret-access-key>'
export AWS_REGION='us-east-1'

mkdir -p ./workspace

ofs sync ./workspace \
  --state ./workspace.state \
  --init \
  --model managed \
  --storage 's3://bucket/prefix?region=us-east-1'
```

`--init` is the only path that creates an absent Managed format. Without it, a
new replica can only attach to an existing volume. The storage URL is saved in
the replica state and must not contain credentials.

Without `--metadata`, namespace metadata is stored through the OpenDAL data
operator. To use D1 as the metadata authority, also provide its credential-free
locator and keep its token in the environment:

```shell
export OFS_D1_TOKEN='<api-token>'

ofs sync ./workspace \
  --state ./workspace.state \
  --init \
  --model managed \
  --storage 's3://bucket/prefix?region=us-east-1' \
  --metadata 'd1://ACCOUNT_ID/DATABASE_ID/STORE_KEY'
```

`OFS_STORAGE_URL` and `OFS_METADATA_URL` may provide the corresponding URLs.

## Synchronize an existing replica

After the first successful invocation, the state file is sufficient to locate
the volume:

```shell
ofs sync ./workspace --state ./workspace.state
```

The command resolves interrupted work, observes one remote snapshot, scans the
local directory, and either installs or publishes the reconciled result.
Disjoint changes merge. Overlapping changes remain conflicts.

Use `--transfer-concurrency` or `OFS_TRANSFER_CONCURRENCY` to bound storage
operations for one invocation. The default is four.

## Attach another replica

A new state file has no locator, so provide the model and original storage
locations. Do not use `--init` when the volume must already exist:

```shell
mkdir -p /workspace/recovered

ofs sync /workspace/recovered \
  --state /state/recovered.state \
  --model managed \
  --storage 's3://bucket/prefix?region=us-east-1'
```

The command reads the Managed superblock, records its `VolumeId`, and
materializes the current namespace. Later invocations use only the new state
file.

## Inspect replica state

Status reads only the local state file. It does not contact storage or modify
the replica:

```shell
ofs status --state ./workspace.state
ofs status --state ./workspace.state --json
```

The JSON output reports the durable `volume_id`, `volume_model`, branch
identity when present, common change sequence, pending publication state,
conflicts, and effective capabilities. It never includes provider credentials.

## Resolve conflicts

When local and remote changes overlap, Sync retains the local candidate and
records a conflict. Edit the candidate if needed, then select it explicitly:

```shell
ofs sync ./workspace \
  --state ./workspace.state \
  --resolve path/to/file
```

`--resolve` may be repeated. Sync revalidates the remote snapshot before
publishing a resolution.

## Recover interrupted or lost local state

For an interrupted command, repeat the same state-only sync. It resolves a
pending publication or installation before starting new work:

```shell
ofs sync ./workspace --state ./workspace.state
```

The directory and state file form one replica. Reusing old state with an empty
directory presents missing paths as local deletions. For a cold recovery, use
both an empty directory and a new state path, then attach with the original
credential-free locators as shown above.

## Use branches

Branches are an optional Managed format extension. Enable it only during
explicit initialization:

```shell
ofs sync ./workspace \
  --state ./workspace.state \
  --init \
  --enable branch \
  --model managed \
  --storage <storage-url>
```

Use any attached replica state to manage that volume's branches:

```shell
ofs branch --state ./workspace.state create experiment
ofs branch --state ./workspace.state create rewind --from main --at 42
ofs branch --state ./workspace.state list
ofs branch --state ./workspace.state show experiment
ofs branch --state ./workspace.state delete experiment
```

Attach a new replica to a branch by adding `--branch` to its first sync. A
deleted and recreated branch name has a new identity, so it needs a new replica
state.

Reachability collection also uses any attached replica state:

```shell
ofs volume gc --state ./workspace.state
ofs volume gc --state ./workspace.state --resume
```

## Filesystem surface

Managed Sync accepts regular files, directories, empty directories, portable
UTF-8 names, and the Unix executable bit. It rejects links, unsupported file
types, and ambiguous or non-portable names before publication.

The replica contains only user files. Credentials, replica state, staging data,
and conflict records stay outside the synchronized tree.

## Related documents

- [Managed Sync architecture](managed-sync-architecture.md)
- [Managed storage format](managed-storage-format.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
