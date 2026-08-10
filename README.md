# Apache OpenDAL™ ofs

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal-ofs/ci.yml?branch=main
[actions]: https://github.com/apache/opendal-ofs/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/ofs.svg
[crates.io]: https://crates.io/crates/ofs
[crate downloads]: https://img.shields.io/crates/d/ofs.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

`ofs` synchronizes ordinary local directories with Managed volumes backed by
OpenDAL.

## Status

`ofs` is a work in progress. The current implementation provides Managed Sync:
filesystem identity and namespace metadata are authoritative remotely, while
an ordinary local directory is reconciled only when `ofs sync` is invoked.

See the [Managed Sync documentation](docs/managed-sync.md) for its workflow,
architecture, persistent format, and optional durable branches.

## How to use `ofs`

### Install `ofs`

`ofs` can be installed by `cargo`:

```shell
cargo install ofs
```

> `cargo` is the Rust package manager. Follow the Rust [installation guide](https://www.rust-lang.org/tools/install) to install it.

### Create and synchronize a Managed volume

Volume creation requires the explicit `--model managed` selection. Managed is
the only model accepted by the current build.

See the [Managed Sync workflow](docs/managed-sync-workflow.md) for Object and
D1 setup, synchronization, branches, conflict handling, and recovery.

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
