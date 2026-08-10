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
coalescing, checkpoint cadence, transfer concurrency, and retry settings are
policy and are not stored in the superblock.

## Ownership and lifetime

| Record | Owner | Lifetime |
| --- | --- | --- |
| Superblock | Metadata | Immutable for the volume lifetime |
| Namespace HEAD | Object Metadata | Mutable commit point |
| Checkpoint and change segment | OpenDAL data storage | Immutable |
| Operation receipt | OpenDAL data storage | Immutable |
| Authority record | Metadata | Mutable through native revision CAS |
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

Object Metadata stores the superblock at the stable discovery key:

```text
.ofs/managed/superblock.json
```

Transactional Metadata stores the same bytes under the same logical key in its
native authority-record table. The JSON representation is a strict UTF-8
object:

```json
{
  "format": "managed/1",
  "volume_id": "00112233445566778899aabbccddeeff",
  "extensions": []
}
```

`volume_id` is 16 bytes encoded as 32 lowercase hexadecimal characters.
`extensions` contains required built-in format extensions and MUST be strictly
ordered by identifier without duplicates. Readers reject another format,
unknown fields, and unknown required extensions before mutation. The stable key
is not versioned: a client must discover and reject an unsupported format
instead of creating another superblock under a version-specific key.

The built-in `branch/v1` extension replaces the single namespace authority with
durable named authorities. Its records are specified below.

The superblock does not contain credentials, endpoints, local paths,
client-local aliases, index inventory, or policy settings.

## Identities and references

- `VolumeId`, `NodeId`, `OperationId`, and `BranchId` are 16 opaque bytes.
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

Each extent contains a `ContentRef`, a `SegmentRef`, and the byte offset of the
content in that segment. Extents MUST be non-empty and ordered. Their logical
offsets are the prefix sums of the preceding `ContentRef` lengths, so their
concatenation MUST cover the declared file length. Every occurrence of one
segment digest MUST declare the same total segment length. An empty file has no
extents.

`FileVersionId` is SHA-256 over this byte sequence:

```text
"OFS-FILE-V1\0"
logical length: u64
whole-file digest: 32 bytes
extent count: u64
for each extent:
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
identity. A segment is the raw content bytes concatenated in `ContentRef`
order. The extent map owns every offset and length; there is no second index or
footer in the segment. SHA-256 and the length of the complete object form its
`SegmentRef` and determine its key.

A complete read verifies the segment length and digest before slicing the
requested extents, then verifies each `ContentRef`. A sparse read fetches the
extent ranges from the file version and verifies each returned `ContentRef`.

## Object Metadata layout

Object Metadata uses these keys:

```text
.ofs/managed/superblock.json
.ofs/managed/metadata/v1/head.ofs
.ofs/managed/metadata/v1/checkpoints/sha256/<digest>.ofs
.ofs/managed/metadata/v1/changes/sha256/<digest>.ofs
.ofs/managed/metadata/v1/operations/<base-or-branch-id>/<operation-id>.ofs
```

`head.ofs` is the only mutable namespace object. A conditional replacement is
the commit point. Checkpoints and change segments are immutable and
content-addressed; operation receipts have immutable deterministic keys.

### branch/v1 authorities

The extension registry maps case-sensitive `BranchName` values to `BranchId`
values and identifies the default branch. Names are 1 to 63 ASCII bytes, start
with a letter or digit, and otherwise contain only letters, digits, `.`, `_`,
or `-`. Names are record values and are never interpolated into object keys or
SQL identifiers.

Each `BranchId` selects one unborn, active, or sealed namespace HEAD. Object
Metadata stores the mutable registry and heads at:

```text
.ofs/managed/metadata/v1/extensions/branch/v1/registry.ofs
.ofs/managed/metadata/v1/extensions/branch/v1/heads/<branch-id>.ofs
```

D1 stores the same logical records in its authority table. Branch HEADs use the
base HEAD envelope, namespace state machine, checkpoint and change formats,
operation receipts, validation rules, and data layout. Operation identities
are scoped to their originating authority and are not copied by a fork.

### HEAD

HEAD has this envelope:

```text
"OFS1HDZ1"                    8 bytes
decoded CBOR length           u32
zstd frame                    variable
SHA-256 of preceding bytes    32 bytes
```

The decoded strict CBOR record contains the volume identity, optional branch
identity, sealed state, optional namespace state, a monotonic maintenance
epoch, and an optional collection fence. A fence identifies its owner and the
exact cursor fixed by that owner. Namespace state contains
the checkpoint digest and encoded length, checkpoint cursor, an ordered
transaction tail, up to eight change-segment references, and the most recent
committed operation result.

Transactions in the tail contain the operation identity, parent and committed
cursors, resulting root, and ordered node, directory, and file-version
changes. Each node or directory change binds one identity, its generation
precondition, and exactly one put or remove effect. Each file-version change
likewise binds one identity to one put or remove effect. The chain MUST be
consecutive from the checkpoint cursor to the current cursor. The tail has at
most 32 transactions. Writers checkpoint before appending a change would make
the encoded transaction bodies exceed 128 KiB; this byte threshold is
checkpoint policy rather than a second HEAD decoding limit.

### Checkpoint

A checkpoint has this envelope:

```text
"OFS1CKZ1"                    8 bytes
decoded CBOR length           u64
zstd frame                    variable
```

The decoded strict CBOR record contains one complete filesystem
`VolumeSnapshot`. Managed file-version data remains in each ordinary
`FileVersion` descriptor; the format does not add another node, directory,
precondition, or snapshot graph.
SHA-256 of the encoded envelope determines its object key. Mutable namespace
state references that digest together with the encoded length, allowing one
bounded range GET without a preceding metadata request. Recovery verifies the
returned byte count and digest before decoding; bytes outside the referenced
range are not part of the checkpoint record.

Encoded and decoded checkpoint sizes are each limited to 256 MiB. Recovery
uses one bounded OpenDAL range read, verifies the referenced content identity,
requires the exact v1 magic and decoded length, rejects trailing or unknown
CBOR fields, and validates the volume and snapshot. A checkpoint never depends
on another checkpoint.

### Change segment

A change segment uses the common Managed v1 envelope:

```text
"OFS1CHG1"                    8 bytes
strict CBOR body              variable
SHA-256 of magic and body     32 bytes
```

The body contains the starting checkpoint reference and at most 32 consecutive
namespace changes. The first change's parent is the start cursor; every change
contains the volume identity and must continue the preceding cursor. HEAD
records the digest, encoded length, start cursor, and end cursor. Recovery uses
one bounded range GET, verifies the referenced bytes, then checks the derived
start and end against HEAD. The encoded body is limited to 16 MiB.

### Operation receipt

`request_sha256` is SHA-256 of this semantic publication preimage. It does not
depend on the CBOR representation of a transaction:

```text
"OFS1REQ1"                              8 bytes
authority scope                         0x00, or 0x01 || BranchId
VolumeId                                16 bytes
OperationId                             16 bytes
parent cursor
committed cursor
root NodeId                             16 bytes
node changes                            counted sequence
directory changes                       counted sequence
file-version changes                    counted sequence
```

All counts and string byte lengths are unsigned 64-bit big-endian integers.
An optional value is `0x00` when absent and `0x01 || value` when present. A
cursor is `0x00` for genesis or `0x01 || sequence || OperationId` otherwise,
where the sequence is unsigned 64-bit big-endian. A Managed generation is its
non-zero unsigned 64-bit big-endian value. A string is its byte length followed
by its canonical NFC UTF-8 bytes. A boolean is `0x00` or `0x01`; node kind is
`0x00` for a directory and `0x01` for a regular file.

A node change is `NodeId || optional expected generation || effect`. A remove
effect is `0x00`; a put effect is `0x01 || generation || kind || executable ||
optional FileVersionId`. A directory change starts with `NodeId || optional
expected generation || effect`. Its put effect is `0x01 || generation ||
removed names || put entries`, where each put entry is `name || NodeId ||
kind`; its remove effect is `0x00`. A file-version change is `FileVersionId ||
effect tag`, with `0x00` for removal and `0x01` for put. Each change sequence is
strictly ordered by identity and contains no duplicates. Directory names are
strictly ordered by their UTF-8 bytes.

A file-version put contributes its 32-byte `FileVersionId` and effect tag, not
its CBOR descriptor. Before calculating the request digest, the descriptor
MUST decode without trailing bytes and MUST match the `FileVersionId` algorithm
above.
Consequently, equivalent descriptor encodings have one publication identity,
while a malformed descriptor cannot pass operation-idempotency resolution.

An operation receipt uses magic `OFS1OPR1` with the same strict
`magic || CBOR || SHA-256` envelope and a 4096-byte body limit. Its body stores
authority scope, committed cursor, and publication request digest. The
committed cursor contains the `OperationId`; the deterministic key is scoped
by `base` or the lowercase branch id.

HEAD stores the most recent committed result and a fixed operation-prefix
filter. Recent results remain reconstructable from the transaction tail and
retained change segments. The writer persists the initial checkpoint result
when it is first displaced and persists every result in a change segment
before removing that segment from HEAD. A committed result is therefore in
HEAD, retained history, or its deterministic receipt; it does not expire.

## Transactional Metadata layout

Transactional Metadata stores the superblock and all mutable authority records
in one D1 table:

```text
ofs_managed_v1_authority_records
```

The primary key is `(store_key, record_key)`. Each row contains those two text
fields, an integer CAS `revision`, and the encoded bytes in lowercase
`value_hex`. `store_key` is the configured physical authority scope;
`record_key` is exactly the logical key used by Object Metadata, including the
superblock, base HEAD, and branch registry and heads. `VolumeId` is verified
from the stored value instead of being duplicated in the table schema.
Immutable checkpoints, change segments, and operation receipts for both base
and branch authorities remain in the configured OpenDAL storage. The
filesystem and Data segment layouts are identical for Object and Transactional
Metadata.

## Publication and reachability

Publication is ordered as follows:

1. Observe one Metadata snapshot and revision token.
2. Write and verify immutable Data segments.
3. Validate the target namespace and generation preconditions.
4. Write any immutable checkpoint or change-segment objects.
5. Persist any operation results that are about to leave retained metadata.
6. Commit with one conditional HEAD replacement or one native transaction.

Segments written before a failed Metadata commit are unreachable. Explicit
`volume gc` maintenance first conditionally fences the base HEAD, or the branch
registry followed by every registered HEAD. Publication, fork, and deletion
cannot cross that fence. The collector marks current and retained namespace
positions, streams the data-segment listing, and submits unreachable keys to
the native OpenDAL deleter. An interrupted fence remains authoritative until
an explicit `volume gc --resume` changes its owner and repeats the mark before
continuing deletion. Managed v1 does not reclaim immutable metadata objects.

## Validation

A reader MUST reject:

- an unknown specification, format, required extension, field, tag, or record
  kind;
- a mismatched volume identity, object key, digest, length, or scope;
- unordered or duplicate records and invalid key ranges;
- overflow, out-of-bounds extents, gaps, or overlaps;
- an invalid cursor chain, generation transition, or filesystem invariant;
- a missing referenced data segment, checkpoint, change segment, or operation
  receipt required to resolve a committed operation.

An immutable write may accept an existing object only after verifying that it
has the expected identity and contents. Missing referenced objects are
corruption, not an empty namespace or empty file.

## Related documents

- [Managed Sync architecture](managed-sync-architecture.md)
- [Managed Sync workflow](managed-sync-workflow.md)
- [RFC 016](../rfcs/0016_filesystem_architecture.md)
