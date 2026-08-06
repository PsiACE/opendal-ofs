# Apache OpenDAL™ ofs

Apache OpenDAL™ ofs makes remote storage available through filesystem
workflows. It supports two user-facing paths:

- **Managed Sync** keeps agent memory, skills, history, and configuration in
  ordinary local directories. Changes remain private until an explicit sync.
- **Direct Mount** exposes an OpenDAL backend through FUSE on supported Unix
  systems.

Managed Sync is intended for short-lived servers and sandbox agents that need
to recover a shared published state without using a storage SDK or running a
background synchronization service.

## Build

```console
cargo build --locked --release --bin ofs
export PATH="$PWD/target/release:$PATH"
```

## Create and synchronize a Managed Volume

Choose a catalog path and an OpenDAL storage URL. The catalog contains the
volume definition but does not retain credentials.

```console
export OFS_CONFIG="$PWD/volumes.json"
export OFS_STORAGE_URL='s3://?bucket=agent-state&root=home&endpoint=http://127.0.0.1:9000&region=us-east-1&access_key_id=ACCESS&secret_access_key=SECRET'

ofs volume create agent-home \
  --model managed \
  --storage "$OFS_STORAGE_URL"

mkdir agent-home
printf 'remember this\n' >agent-home/memory.md
ofs sync agent-home agent-home
```

The first non-empty sync publishes the directory. Local edits after that point
remain private until the same replica runs `ofs sync` again.

A new agent starts from an empty directory:

```console
mkdir new-agent-home
ofs sync agent-home new-agent-home
```

The directory is materialized as ordinary files. The agent can use normal file
tools and does not need an ofs process after the command exits.

## Use D1 as the Metadata Store

D1 can hold the authoritative namespace while any supported OpenDAL service
holds immutable file content. Keep the token in `OFS_METADATA_URL`; pass only
the credential-free locator to `volume create`.

```console
export OFS_METADATA_URL='d1://ACCOUNT_ID/DATABASE_ID/agent-home?token=API_TOKEN'

ofs volume create agent-home \
  --model managed \
  --storage "$OFS_STORAGE_URL" \
  --metadata 'd1://ACCOUNT_ID/DATABASE_ID/agent-home'
```

All subsequent `sync` and `status` commands use the same public workflow. D1
placement does not introduce a separate sync mode.

## Inspect a replica

```console
ofs status agent-home
ofs status agent-home --json
```

Status is read-only. If the authority cannot be reached, remote state is
reported as unknown instead of returning a cached generation as current.

## Recover after local loss

Losing local files, replica state, or the local catalog does not by itself
delete the remote Managed Volume. Cold recovery requires an empty directory
with no established replica state. If the original directory was lost but its
sibling ofs state remains, do not reuse that state with a recreated empty
directory: the missing files are treated as local deletions and a sync can
publish an empty tree. Use a new directory or a fresh `--state` path instead.

If the catalog was also lost, recreate the same named definition from the same
storage and metadata locators. For the D1 example above:

```console
ofs volume create agent-home \
  --model managed \
  --storage "$OFS_STORAGE_URL" \
  --metadata 'd1://ACCOUNT_ID/DATABASE_ID/agent-home'

mkdir recovered-home
ofs sync agent-home recovered-home
```

The volume identity and latest published generation are recovered from the
Metadata Store. For colocated object metadata, omit `--metadata` when
recreating the definition.

## Managed Sync scope

Managed Sync supports complete directory synchronization for regular files,
directories, empty directories, portable names, and executable bits on Unix.
It supports multiple readers, publishers that take turns after catching up,
and conditional fencing when publications overlap.

It does not provide background synchronization, path filters, symlinks, hard
links, history browsing, timestamp restore, partial hydration, remote volume
destruction, or remote garbage collection.

See [Managed Sync workflow](docs/managed-sync-workflow.md) for diagrams of
publication, reconciliation, and multiple agents; [Managed Sync
explained](docs/managed-sync-explained.md) for the behavior and recovery model;
and [Managed Sync reference](docs/managed-sync-reference.md) for commands,
configuration, status, D1 requirements, and validation coverage.

## Direct Mount

Direct Mount requires FUSE and currently supports the `fs` and `s3` OpenDAL
services on Linux.

```console
ofs volume create local --model direct --storage 'fs://?root=<directory>'
ofs mount local <mount-point>

ofs volume create remote --model direct \
  --storage 's3://?bucket=<bucket>&root=<path>&endpoint=<endpoint>&region=<region>'
ofs mount remote <mount-point>
```

## License and trademarks

Licensed under the Apache License, Version 2.0.

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or
trademarks of the Apache Software Foundation.
