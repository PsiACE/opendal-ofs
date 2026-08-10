# Managed Sync

Managed Sync reconciles an ordinary local directory with a Managed volume only
when `ofs sync` runs. This page is the entry point for its user guide,
architecture, and persistent format.

The documentation is split by purpose:

- [Managed Sync workflow](managed-sync-workflow.md) explains how to initialize a
  volume, synchronize a replica, resolve conflicts, and recover local state.
- [Managed Sync architecture](managed-sync-architecture.md) explains the
  component boundaries, metadata authorities, publication path, and OpenDAL
  integration.
- [Managed storage format](managed-storage-format.md) specifies the persistent
  namespace, data layout, and built-in extensions for `managed/1`.

The architectural definitions for volume and access models are in
[RFC 016](../rfcs/0016_filesystem_architecture.md).
