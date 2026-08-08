# Managed Sync

Managed Sync reconciles an ordinary local directory with a Managed volume
selected through a client-local alias. Different clients may use different
aliases for the same remote `VolumeId`. Local edits remain private until `ofs
sync` publishes them. Each sync
observes a fixed remote snapshot, merges local and remote changes, and either
installs the result or publishes one generation-checked namespace change.

The current command surface provides read-only Direct Mount and read-write
Managed Sync. Direct Sync and Managed Mount are not implemented.

The documentation is split by purpose:

- [Managed Sync workflow](managed-sync-workflow.md) explains how to create a
  volume, synchronize a replica, resolve conflicts, recover local state, and
  collect unreachable data.
- [Managed Sync architecture](managed-sync-architecture.md) explains the
  component boundaries, metadata authorities, publication path, and OpenDAL
  integration.
- [Managed storage format](managed-storage-format.md) specifies the persistent
  namespace and data layout for `managed/1`.

The architectural definitions for volume and access models are in
[RFC 016](../rfcs/0016_filesystem_architecture.md).
