# Managed storage format

This document specifies the persistent namespace and file data representation
for an OFS Managed volume. The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and
MAY are normative.

## Format boundaries

The base specification uses these boundaries:

| Boundary | Purpose | Reader behavior |
| --- | --- | --- |
| Specification | Defines identities, formats, registries, and invariants | Reject an unknown specification major |
| Format | Stores facts required to recover the namespace and logical bytes | Reject unknown fields, tags, and record kinds |
| Index | Stores a disposable projection of format facts | Correctness MUST NOT depend on it |
| Extension | Adds required semantics that are absent from the base format | Reject an unknown required extension before mutation |
| Policy | Selects legal write, read, placement, and maintenance choices | MUST NOT change how existing bytes are decoded |

The base format has no data-location index. File versions contain the
authoritative segment locations. Chunk sizes, target segment size, range
coalescing, checkpoint cadence, transfer concurrency, retry settings, and
garbage-collection schedules are policy and are not stored in the superblock.

## Ownership and lifetime

| Record | Owner | Lifetime |
| --- | --- | --- |
| Superblock | Metadata | Immutable for the volume lifetime |
| Namespace HEAD | Object Metadata | Mutable commit point |
| Manifest and SSTable | Object Metadata | Immutable while referenced |
| Transactional namespace rows | Transactional Metadata | Managed by native transactions and checkpoints |
| File version and extent map | Metadata | Immutable logical file version |
| Segment | Data | Immutable until unreachable |
| Replica state | Sync frontend | Local and persistent, outside the volume |

## Volume identity

`VolumeId` is the only durable identity of a Managed volume. It is created once
with the superblock and remains stable for the volume lifetime. Metadata
records, file versions, data references, and replica bindings use that identity
where the containing format requires a volume scope.

A name passed to `ofs volume create` or `ofs sync` is a client-local catalog
alias. An alias MUST NOT be written to the superblock, namespace metadata, data
segments, or replica state. Different clients MAY register the same `VolumeId`
under different aliases. Readers MUST determine the remote identity from the
superblock and MUST reject an expected `VolumeId` that does not match it.

Storage and metadata locators select the physical stores; they are not volume
identities and are not persisted in this format. Moving a format-preserving
copy to equivalent storage does not change its `VolumeId`.

## Superblock

Object Metadata stores the superblock at:

```text
.ofs/managed/metadata/v1/superblock.json
```

Transactional Metadata stores the same fields in its native format table. The
JSON representation is a strict UTF-8 object:

```json
{
  "specification": "managed/1",
  "volume_id": "00112233445566778899aabbccddeeff",
  "naming_policy": "portable-utf8/1",
  "metadata_format": "object/1",
  "file_version_format": "extent-map/1",
  "data_format": "segment/1",
  "required_extensions": []
}
```

`volume_id` is 16 bytes encoded as 32 lowercase hexadecimal characters.
`metadata_format` is `object/1` or `transactional/1`. Required extensions MUST
be strictly ordered without duplicates. Readers reject unknown fields,
identifiers, formats, and required extensions.

The superblock does not contain credentials, endpoints, local paths,
client-local aliases, index inventory, or policy settings.

## Identities and references

- `VolumeId`, `NodeId`, and `OperationId` are 16 opaque bytes.
- `ContentRef` contains a SHA-256 digest and the length of one raw content
  extent.
- `SegmentRef` contains a SHA-256 digest and the total encoded segment length.
- `ChangeCursor` contains an ordered sequence and the operation identity for a
  committed namespace position.
- `FileVersionId` binds the logical file identity and its physical extent map.

All integers in fixed binary envelopes are unsigned and big-endian. Readers
check every offset and length for overflow and against the referenced object
before allocation or I/O.

## File version

A file version contains:

- the logical file length;
- SHA-256 of the complete logical byte sequence;
- an ordered extent map.

Each extent contains the logical offset, a `ContentRef`, a `SegmentRef`, and
the byte offset of the content in that segment. Extents MUST be non-empty,
contiguous, ordered, and non-overlapping. They start at logical offset zero and
cover the declared file length. An empty file has no extents.

`FileVersionId` is SHA-256 over this byte sequence:

```text
"OFS-FILE-V1\0"
logical length: u64
whole-file digest: 32 bytes
extent count: u64
for each extent:
  logical offset: u64
  content digest: 32 bytes
  content length: u64
  segment digest: 32 bytes
  segment encoded length: u64
  segment offset: u64
```

A single extent represents a whole file. Several extents may result from
FastCDC or another placement policy. Readers do not need the chunking
algorithm or its parameters.

## Data segment

Segments are stored at:

```text
.ofs/managed/data/v1/segments/sha256/<first-two-hex>/<64-hex>.seg
```

The directory partition is derived from the segment digest. It is not a second
identity. A segment has this layout:

```text
+-------------------------------+
| "OFSSEG01"                    | 8 bytes
| format major                  | u16, value 1
+-------------------------------+
| raw content extents           |
+-------------------------------+
| named-field CBOR footer       |
+-------------------------------+
| "OFSSEGTR"                    | 8 bytes
| footer offset                 | u64
| footer length                 | u64
| segment SHA-256               | 32 bytes
+-------------------------------+
```

The footer is equivalent to:

```text
{
  major: 1,
  entries: [
    {
      content: { digest: bytes(32), length: uint },
      offset: uint
    }
  ]
}
```

Footer entries are strictly ordered by `ContentRef`. They describe the entire
raw content region without gaps or overlaps. `ContentRef.length` is the stored
range length. The segment digest covers every byte before the digest field and
determines the object key.

A complete read verifies the envelope, segment digest, footer, entry ordering,
ranges, and each content digest. A sparse read fetches the extent range from
the file version and verifies its `ContentRef`.

## Object Metadata layout

Object Metadata uses these keys:

```text
.ofs/managed/metadata/v1/superblock.json
.ofs/managed/metadata/v1/head.ofs
.ofs/managed/metadata/v1/manifests/sha256/<digest>.ofs
.ofs/managed/metadata/v1/sstables/sha256/<digest>.sst
```

`head.ofs` is the only mutable namespace object. A conditional replacement is
the commit point. The other objects are immutable and content-addressed.

### HEAD

HEAD has this envelope:

```text
"OFS1HDZ1"                    8 bytes
decoded CBOR length           u32
zstd frame                    variable
SHA-256 of preceding bytes    32 bytes
```

The decoded named-field CBOR record contains format major, volume identity,
current cursor, checkpoint digest, checkpoint cursor, an ordered transaction
tail, maintenance epoch, maintenance state, and an optional fixed maintenance
cursor.

Transactions in the tail contain the operation identity, parent and committed
cursors, resulting root, generation preconditions, and ordered node,
directory, directory-entry, and file-version effects. The chain MUST be
consecutive from the checkpoint cursor to the current cursor.

### Manifest

A manifest has this envelope:

```text
"OFS1MAN\0"
named-field CBOR manifest
SHA-256 of preceding bytes
```

The manifest contains format major, volume identity, checkpoint cursor, root
node, and ordered SSTable references. Each reference carries the encoded table
length, the first and last physical partition keys, and the table's complete
block index. The partition ranges MUST be strictly ordered and non-overlapping.
Together, the referenced tables form one complete snapshot in which every
namespace record appears exactly once. A reader does not merge overlays or
apply tombstones. SHA-256 of the complete encoded manifest determines its
object key.

Snapshot partition keys are derived from portable filesystem paths, not from
local volume aliases or opaque node identities. The root partition starts with
byte `1`. Each child appends its UTF-8 name followed by a zero byte. A directory
entry is assigned to the child's path. Node and directory records use the
node's path, and a file version uses the lowest path of any file that references
it. Committed operation results use byte `2` followed by the operation identity.
The naming policy excludes the zero byte, so this encoding is unambiguous.

A checkpoint writer splits these ordered groups at deterministic,
content-defined path boundaries with bounded target sizes. It never divides the
records for one path. When a range has the same records and encoding as the
preceding checkpoint, the writer reuses its existing SSTable reference. Changed
ranges become new immutable SSTables. Adding, removing, renaming, or editing a
path therefore changes its containing range and, at most, the ranges up to the
next stable boundary. The new manifest still describes the complete namespace,
so recovery never depends on an older manifest.

### SSTable

An SSTable contains ordered data blocks, a block index, and a table trailer.
Each record frame is:

```text
key length: u32
value length: u32
key bytes
value bytes
```

A data block contains `OFSBLK01`, format major, volume scope, record count,
record frames, `OFSBLKTR`, and a SHA-256 checksum. The index starts with
`OFSIDX01` and records each block's object range, key bounds, record count, and
checksum. The table trailer contains `OFSTBL01`, format major, index offset,
index length, and SHA-256 of the preceding table bytes.

The format major in each block, index, and table envelope is followed by a
reserved `u16` that MUST be zero. The table checksum determines the SSTable
object key.

Within each SSTable, records remain strictly ordered by typed record key. The
namespace uses separate prefixes for nodes, directories, directory entries,
file versions, and committed operation results. Typed record ranges may overlap
between SSTables because physical partition order is path based. Readers use
the block index to select typed-key ranges and fetch those byte ranges through
OpenDAL.

Table splitting thresholds and the boundary function are write policy. They
are not persisted in the superblock and do not affect decoding. Every table
reference carries all persisted partition and block metadata needed to validate
and read that table.

## Transactional Metadata layout

Transactional Metadata stores the superblock and namespace in these D1 tables:

```text
ofs_managed_v1_formats
ofs_managed_v1_heads
ofs_managed_v1_nodes
ofs_managed_v1_directories
ofs_managed_v1_change_transactions
ofs_managed_v1_operation_results
ofs_managed_v1_checkpoints
```

`STORE_KEY` scopes one Managed volume inside the shared tables. The head row is
the commit point. D1 stores node and directory projections, ordered committed
transactions, operation results, and periodic snapshots. Publication uses one
native transaction with revision, cursor, and generation predicates.

The Data segment layout is identical for Object and Transactional Metadata.
D1 stores no copy of the Object HEAD, manifest, or SSTable format.

## Publication and reachability

Publication is ordered as follows:

1. Observe one Metadata snapshot and revision token.
2. Write and verify immutable Data segments.
3. Validate the target namespace and generation preconditions.
4. Write any immutable checkpoint objects or rows.
5. Commit with one conditional HEAD replacement or one native transaction.

Segments written before a failed Metadata commit are unreachable. Garbage
collection fixes a namespace cursor, marks reachable `SegmentRef` values, and
deletes unreferenced segments. A segment remains reachable if any live file
version references one of its extents.

## Validation

A reader MUST reject:

- an unknown specification, format, required extension, field, tag, or record
  kind;
- a mismatched volume identity, object key, digest, length, or scope;
- unordered or duplicate records and invalid key ranges;
- overflow, out-of-bounds extents, gaps, or overlaps;
- an invalid cursor chain, generation transition, or filesystem invariant;
- a missing referenced segment, manifest, or SSTable.

An immutable write may accept an existing object only after verifying that it
has the expected identity and contents. Missing referenced objects are
corruption, not an empty namespace or empty file.

## Related documents

- [Managed Sync architecture](managed-sync-architecture.md)
- [Managed Sync workflow](managed-sync-workflow.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
