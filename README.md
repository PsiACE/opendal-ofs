# Apache OpenDAL™ ofs

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal-ofs/ci.yml?branch=main
[actions]: https://github.com/apache/opendal-ofs/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/ofs.svg
[crates.io]: https://crates.io/crates/ofs
[crate downloads]: https://img.shields.io/crates/d/ofs.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

`ofs` provides Mount and Sync access to named filesystems backed by OpenDAL.

## Status

`ofs` is a work in progress. Its two independent choices are the volume model
and the access model:

| | Mount | Sync |
| --- | --- | --- |
| Direct volume | Available | Not yet available |
| Managed volume | Not yet available | Available |

A Direct volume exposes an existing object namespace. A Managed volume stores
filesystem identity and namespace metadata. Mount provides an online filesystem,
while Sync reconciles an ordinary local directory only when explicitly invoked.

Managed Sync uses a local directory as the working filesystem and publishes
explicitly to a format v1 Managed volume. Namespace metadata can use colocated
objects or D1, while immutable data is accessed through OpenDAL. See the
[Managed Sync documentation](docs/managed-sync.md) for the workflow,
architecture, and storage format.

## How to use `ofs`

### Install `FUSE` on Linux

```shell
sudo pacman -S fuse3 --noconfirm # archlinux
sudo apt-get -y install fuse3    # debian/ubuntu
```

### Load `FUSE` kernel module on FreeBSD

```shell
kldload fuse
```

### Install `ofs`

`ofs` can be installed by `cargo`:

```shell
cargo install ofs
```

> `cargo` is the Rust package manager. Follow the Rust [installation guide](https://www.rust-lang.org/tools/install) to install it.

### Create and mount a Direct volume

Choose a catalog path, then register a credential-free OpenDAL URL under a local
volume name:

```shell
export OFS_CONFIG="$PWD/ofs-volumes.json"

ofs volume create archive \
  --model direct \
  --storage 'fs://?root=/srv/archive'

mkdir -p /mnt/archive
ofs mount archive /mnt/archive
```

The mount runs in the foreground. Stop it with Ctrl-C.

For S3, keep credentials in provider environment variables rather than the
catalog URL:

```shell
export AWS_ACCESS_KEY_ID='<access-key-id>'
export AWS_SECRET_ACCESS_KEY='<secret-access-key>'
export AWS_REGION='<region>'

ofs volume create archive \
  --model direct \
  --storage 's3://<bucket>/<path>?endpoint=<endpoint>&region=<region>'

ofs mount archive /mnt/archive
```

Direct mounts are read-only. Writable Direct access remains unavailable until
the selected backend and frontend can enforce generation-checked publication.

### Create and synchronize a Managed volume

Managed Sync keeps the working tree on the native local filesystem and
publishes only during an explicit `ofs sync`. See the
[Managed Sync workflow](docs/managed-sync-workflow.md) for Object and D1 setup,
conflict handling, recovery, and maintenance.

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
