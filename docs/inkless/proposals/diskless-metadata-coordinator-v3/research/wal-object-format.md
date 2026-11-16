# WAL object format and footer feasibility

Research for ticket [07-wal-object-format](../issues/07-wal-object-format.md).

## Summary

A WAL object is the concatenated Kafka v2 record batches of one broker's file, grouped
by topic ID and then partition, with nothing before, between, or after them. A batch
header stores its length, producer ID, producer epoch, base sequence, offset delta,
timestamps, and attributes. It doesn't store a topic ID or a partition. Its base offset
counts from 0 within the produce request. Its partition leader epoch is 0. Only the
control plane stores the partition of each batch, the absolute offsets, the log-append
time, and each batch's commit outcome.

Readers don't change if a footer goes at the end of the same upload:

- Every reader slices batches out of fetched bytes by coordinate. It never parses bytes
  past the last batch.
- The hot path fetches 16 MiB aligned blocks, and objects are about 8 MiB. The hot path
  transfers and caches the trailing bytes and ignores them.
- The cold and consolidation paths fetch the bounding range of the requested batches.
  They stop at the end of the last batch.
- All three backends upload one in-memory byte array in one logical write that becomes
  visible only when complete. A longer array uses the same upload call.

Time limits what a footer can contain. At upload, the broker knows every batch's user
partition, byte range, producer fields, and timestamps. It also knows its broker ID,
broker epoch, sequence, and tracking partition in the v2 design. It doesn't know the absolute offsets,
the log-append time, or the commit order against other brokers' objects. It also doesn't
know whether the commit accepts each batch or rejects it as a duplicate or out of order,
or anything that happens after the commit.

The local checkouts only link to KIP-1150, KIP-1163, and KIP-1164, and Inkless and Kafka
don't write a footer or manifest in any object or segment format. The closest Kafka prior art is a classic segment. Its directory name gives the
partition, and its batches store absolute offsets, so recovery rebuilds the indexes by
scanning.

## Sources

The research uses two local checkouts and vendor API documentation. Citations use paths
relative to each root.

- Inkless: `/Users/viktor.somogyi-vass/dev/github/aiven/workspace-inkless/diskless-metadata`
  at commit `2a412af730`.
- Kafka: `/Users/viktor.somogyi-vass/dev/github/apache/kafka` at commit `27d8bbc35e`.

Unless a citation says Kafka, it refers to the Inkless checkout. Citations into the map's
earlier work use their paths under `.scratch/`. Object-storage semantics come from the
vendors' API references, cited by URL. The size section marks its estimates as inference.

## The write path

### Classes that build an object

- `AppendHandler` rejects any produce request with a transactional batch with
  `INVALID_REQUEST`, so WAL objects contain no transactional or control batches
  (`storage/inkless/src/main/java/io/aiven/inkless/produce/AppendHandler.java:99-104`, `:122-131`).
- `Writer.write` adds each request to the `ActiveFile` under a lock. It rotates the file
  before the add when the request would push the file past `produce.buffer.max.bytes`, and
  after the add when the file size meets the limit. A tick rotates it after
  `produce.commit.interval.ms`
  (`storage/inkless/src/main/java/io/aiven/inkless/produce/Writer.java:164-202`, `:205-221`).
  The defaults are 8 MiB and 250 ms, and the buffer limit is best effort
  (`storage/inkless/src/main/java/io/aiven/inkless/config/InklessConfig.java:56-63`).
- `ActiveFile.add` validates each partition's records with `UnifiedLog.validateAndAppendBatch`
  and records per-partition errors without failing the request
  (`storage/inkless/src/main/java/io/aiven/inkless/produce/ActiveFile.java:92-173`).
- `UnifiedLog.validateAndAppendBatch` requires a client base offset of 0, runs Kafka's
  `LogValidator` with an offset counter that starts at the request's first offset, and adds
  each validated batch to the `BatchBuffer` with its `TopicIdPartition` and request ID
  (`storage/inkless/src/main/java/io/aiven/inkless/produce/UnifiedLog.java:110-114`, `:234-323`).
  The leader epoch it passes is `LeaderAndIsr.INITIAL_LEADER_EPOCH`, which is 0
  (`UnifiedLog.java:67-68`, `:278`; `metadata/src/main/java/org/apache/kafka/metadata/LeaderAndIsr.java:28`).
- `BatchBuffer.close` sorts the batches by topic ID and then partition with a stable sort,
  allocates one array of the exact total size, and writes each batch into it back to back.
  It builds one `CommitBatchRequest` per batch from the batch header, with the batch's byte
  position in the array (`storage/inkless/src/main/java/io/aiven/inkless/produce/BatchBuffer.java:51-78`).
- `ActiveFile.close` wraps that array and the requests in a `ClosedFile`
  (`ActiveFile.java:192-202`; `storage/inkless/src/main/java/io/aiven/inkless/produce/ClosedFile.java:46-51`).
- `FileCommitter.commit` skips an empty file. For a non-empty one, it submits three jobs: a
  `FileUploadJob` over the array, a `FileCommitJob` that waits for the upload and then
  commits on one thread, and a `CacheStoreJob` that puts the array into the object
  cache after the upload (`storage/inkless/src/main/java/io/aiven/inkless/produce/FileCommitter.java:172-245`).
- `FileUploadJob` names the object with a random UUID under the configured prefix and
  retries the upload with the same key (`storage/inkless/src/main/java/io/aiven/inkless/produce/FileUploadJob.java:104-153`).
  v2 replaces the UUID with `<clusterId>.<brokerId>.<brokerEpoch>.<sequence>`, built at
  file close (`.scratch/diskless-metadata-coordinator-v2/design.md:238-242`).
- `FileCommitJob` calls `ControlPlane.commitFile` with the key,
  `ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT`, the broker ID, the array length, and the
  requests (`storage/inkless/src/main/java/io/aiven/inkless/produce/FileCommitJob.java:103-122`;
  `storage/inkless/src/main/java/io/aiven/inkless/control_plane/ControlPlane.java:37-42`).

### Byte layout

- The object contains the batches and nothing else. `BatchBuffer` allocates `totalSize`
  bytes and writes each batch at the running position. The object has no header, no
  padding between batches, and no trailer (`BatchBuffer.java:61-74`).
- Each partition occupies one contiguous run. The sort runs once over every batch in the
  file, so two requests for the same partition end up next to each other, in request order
  (`BatchBuffer.java:54-59`).
- `ObjectFormat` has one value, `WRITE_AHEAD_MULTI_SEGMENT` with ID 1. Its Javadoc allows
  invalid data between batches and states that batches contain no valid log offsets
  (`storage/inkless/src/main/java/io/aiven/inkless/common/ObjectFormat.java:20-48`). The
  Postgres `format_t` domain accepts 1 to 3
  (`storage/inkless/src/main/resources/db/migration/V1__Create_tables.sql:14-15`). Only the
  two writers reference the enum. Readers ignore the stored format
  (`FileCommitJob.java:107`;
  `storage/inkless/src/main/java/io/aiven/inkless/control_plane/InMemoryControlPlane.java:179`).

### Batch header contents

A v2 batch header is 61 bytes. It stores the base offset, length, partition leader epoch,
magic, CRC, attributes, last offset delta, base timestamp, max timestamp, producer ID,
producer epoch, base sequence, and record count (Kafka
`clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecordBatch.java:45-62`, `:104-131`).
The header has no topic or partition field. Kafka computes the last sequence from the base
sequence and the offset delta (Kafka `DefaultRecordBatch.java:210-215`).

In a WAL object, the header fields contain these values:

| Field | Value in the object | Source |
| --- | --- | --- |
| Base offset and last offset delta | Relative to the request's first batch for that partition, starting at 0 | `UnifiedLog.java:110-114`, `:264-289`; `storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java:243` |
| Partition leader epoch | 0 | `UnifiedLog.java:67-68`; `LogValidator.java:245-246` |
| Max timestamp | The client's max for `CreateTime`, or the broker's validation time for `LogAppendTime` | `LogValidator.java:248-253` |
| Producer ID, epoch, and base sequence | As the client sent them | `CommitBatchRequest.java:39-56` |
| CRC | Covers the attributes through the end, but not the base offset or the leader epoch | Kafka `DefaultRecordBatch.java:67-71` |

The control plane's `batches` row identifies the batch. It stores the topic ID,
partition, absolute base and last offsets, file ID, byte offset, byte size, timestamp type,
log-append timestamp, and max timestamp (`V1__Create_tables.sql:85-101`;
`storage/inkless/src/main/java/io/aiven/inkless/control_plane/BatchMetadata.java:26-36`).
The reader writes the absolute offsets and the stored log-append time over the header at
serve time (`storage/inkless/src/main/java/io/aiven/inkless/consume/FetchCompleter.java:430-493`).
For `LogAppendTime`, the object contains the broker's validation time, and the reader
serves the control plane's commit time
(`storage/inkless/src/main/resources/db/migration/V28__Soft_delete_logs.sql:472`).

A batch in a WAL object stores only what the client and the validator give it. It has no
topic ID or partition, and its base offset is relative. The coordinator assigns the
absolute offset.

## The read path

### Coordinates drive every read

- A diskless fetch gets `BatchInfo` coordinates from `findBatches`. Each has an object key
  and a `BatchMetadata` with a byte offset and size
  (`storage/inkless/src/main/java/io/aiven/inkless/control_plane/BatchInfo.java:20-24`;
  `BatchMetadata.java:68-70`).
- `FetchPlanner.planJobs` groups the coordinates by object and turns their byte ranges into
  fetch ranges (`storage/inkless/src/main/java/io/aiven/inkless/consume/FetchPlanner.java:164-232`).
- `FetchCompleter` copies only the intersection of each batch's coordinate range with the
  fetched extents into a per-batch slot, then parses that slot
  (`FetchCompleter.java:358-425`). It ignores bytes outside every coordinate range.
- Consolidation reads through the same `Reader` and `FetchHandler`, with a consolidation
  flag (`core/src/main/scala/kafka/server/ReplicaManager.scala:330-357`;
  `core/src/main/scala/io/aiven/inkless/consolidation/DisklessLeaderEndPoint.scala:53-60`).
- Only tests fetch a whole object. `ByteRange.maxRange()` and a `null` range appear in
  test code, and a search of `storage/inkless/src/main` and `core/src/main` finds no
  production caller (`storage/inkless/src/main/java/io/aiven/inkless/common/ByteRange.java:55-57`).

### Bytes each path transfers

- Hot path: `FixedBlockAlignment` maps every range to the aligned blocks that cover it
  (`storage/inkless/src/main/java/io/aiven/inkless/cache/FixedBlockAlignment.java:30-55`). The
  block is `consume.cache.block.bytes`, 16 MiB by default
  (`InklessConfig.java:81-83`; `storage/inkless/src/main/java/io/aiven/inkless/common/SharedState.java:180`).
  An object is about 8 MiB, and its first block covers it whole. For an object up to
  16 MiB, the hot path transfers and caches every trailing byte, and the completer ignores
  them.
- Producer-side cache: `CacheStoreJob` stores the whole uploaded array in the same aligned
  blocks (`storage/inkless/src/main/java/io/aiven/inkless/produce/CacheStoreJob.java:63-84`).
  A footer appended to the array goes into the cache too.
- Cold and consolidation paths: `BoundingRangeAlignment` fetches one range from the lowest
  requested offset to the highest requested end
  (`storage/inkless/src/main/java/io/aiven/inkless/cache/BoundingRangeAlignment.java:35-52`;
  `FetchPlanner.java:217-222`). The range ends at the last requested batch.
- S3, GCS, and Azure each answer a block that extends past the object's end. S3 returns the bytes
  that exist (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/s3/S3Storage.java:135-138`).
  GCS sizes the read to the blob (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/gcs/GcsStorage.java:245-265`).
  Azure returns the downloaded range and answers 416 only for a start past the end
  (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/azure/AzureBlobStorage.java:181-199`).

Readers read byte ranges from batch coordinates only and never interpret trailing bytes.
The hot path transfers them as part of a block it fetches anyway.

## Uploads by backend

`ObjectUploader.upload` takes a key, an input stream, and its exact length, and must fail
on a length mismatch (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/common/ObjectUploader.java:25-37`).
`FileUploadJob` passes a `ByteArrayInputStream` over the `ClosedFile` array, so every
backend uploads one in-memory array (`FileUploadJob.java:78-96`).

| Backend | Call | Visibility | Sources |
| --- | --- | --- | --- |
| S3 | One `PutObject` with `RequestBody.fromInputStream` and the length | "Amazon S3 never adds partial objects; if you receive a success response, Amazon S3 added the entire object to the bucket." | `S3Storage.java:96-119`; [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) |
| GCS | `storage.create` with the bytes up to 1 MiB, otherwise a resumable `createFrom` | "Only a completed resumable upload appears in your bucket." | `GcsStorage.java:73-79`, `:122-153`; [Resumable uploads](https://cloud.google.com/storage/docs/resumable-uploads) |
| Azure | `getBlobOutputStream` on a block blob client, with 5 MiB blocks. It stages blocks and commits a block list on close | Staged blocks stay uncommitted until `Put Block List` | `AzureBlobStorage.java:120-173`; `storage/inkless/src/main/java/io/aiven/inkless/storage_backend/azure/AzureBlobStorageConfig.java:54`; [Put Block List](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list) |
| In memory | Copies the stream into a map entry | Immediate | `storage/inkless/src/main/java/io/aiven/inkless/storage_backend/in_memory/InMemoryStorage.java:58-77` |

Appending a footer changes the array that `BatchBuffer.close` builds and leaves every
backend as it is. The upload call, its length check, and its atomicity stay the same. Two
existing checks read the array: `ClosedFile` requires an empty array exactly when it has
no batch, and `FileCommitter` skips the upload of an empty file
(`ClosedFile.java:113-116`; `FileCommitter.java:179-181`). The `fileSize` that
`commitFile` stores is the array length, so it includes any footer
(`ClosedFile.java:119-121`; `FileCommitJob.java:107`).

Reading a footer back without the size differs per backend:

- GCS documents suffix ranges such as `Range: bytes=-2000`
  ([XML API headers](https://cloud.google.com/storage/docs/xml-api/reference-headers)).
- S3 refers its `Range` parameter to RFC 9110, which defines suffix ranges, and allows one
  range per `GET` ([GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)).
- Azure lists only `bytes=startByte-` and `bytes=startByte-endByte`
  ([Range header for Blob service operations](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-blob-service-operations)).
  A reader needs the blob size first.
- Inkless's `ByteRange` has an offset and a size and no suffix form. `GcsStorage` already
  reads blob metadata before every fetch (`ByteRange.java:20-31`; `GcsStorage.java:245`, `:289-295`).
- `StorageBackend` combines upload, fetch, and delete. It has no list operation
  (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/common/StorageBackend.java:25-32`).

## Rebuild inputs at upload time

v2's file-committed record stores the object key, format, size, uploader broker ID, broker
epoch, sequence, tracking partition, participant set, this partition's committed batches
with their assigned offsets and producer fields, and the outcome of each other batch
(`.scratch/diskless-metadata-coordinator-v2/design.md:145`). The table splits those fields
by who knows them and when.

| Field | Known at upload | Source |
| --- | --- | --- |
| User partition of each batch (topic ID and partition) | Yes, from the `BatchHolder` | `BatchBuffer.java:37-49`, `:84-87` |
| Byte offset and size of each batch | Yes | `BatchBuffer.java:63-74`; `CommitBatchRequest.java:24-37` |
| Magic, timestamp type, max timestamp, and offset delta | Yes, and also in the batch header | `CommitBatchRequest.java:39-56` |
| Producer ID, producer epoch, base sequence, and last sequence | Yes, and also in the batch header | `CommitBatchRequest.java:52-54` |
| Uploader broker ID | Yes | `FileCommitJob.java:49`, `:107` |
| Broker epoch and sequence | Yes under v2, where `FileCommitter` assigns them at file close, before the upload. The code has neither | `.scratch/diskless-metadata-coordinator-v2/design.md:225`, `:240` |
| Object key and format | Yes under v2. In the code, the upload job picks the key | `FileUploadJob.java:108`; v2 `design.md:238-240` |
| Tracking partition | Yes under v2, from the lease the sequence came from. A fallback lease can come from `(t + 1) % N`, so the broker ID alone doesn't determine it | v2 `design.md:86`, `:252-254` |
| Participant set | Yes, from the batches' partitions through the protocol mapping | v2 `design.md:78-80` |
| Absolute base and last offsets | No. The leader assigns them from the high watermark at commit | `V28__Soft_delete_logs.sql:515`, `:600-608`; v2 `design.md:300` |
| Log-append timestamp | No. The control plane stamps its commit time | `V28__Soft_delete_logs.sql:472`, `:628` |
| Outcome of each batch | No. The commit decides `none`, `duplicate_batch`, `invalid_producer_epoch`, `sequence_out_of_order`, or `nonexistent_log` | `V28__Soft_delete_logs.sql:508-571`, `:642` |
| Order against other brokers' commits to the same partition | No. Only the coordinator log records it | v2 `design.md:192-195` |
| Later removals, fences, releases, and leases | No. They happen after the commit | v2 `design.md:143-161` |

The producer-state rows also come from the commit. Postgres keeps the last 5 entries per
producer, each with its assigned offset (`V28__Soft_delete_logs.sql:574-597`).

These facts bear on what a footer can rebuild:

- The batch header already stores every producer field, the timestamps, and the offset
  delta. A footer that repeats them adds bytes and no information. The bytes lack only the
  topic ID and partition of each run, and the uploader's identity.
- Without a footer, a parser can walk the batch boundaries from byte 0, because each header
  stores its length and `BatchBuffer` writes the batches back to back. It can't find where
  one partition's run ends and the next begins. A base offset of 0 marks the start of one
  request's batches for a partition (`UnifiedLog.java:110-114`). Partition boundaries leave
  no mark in the bytes.
- An object stays in storage while any of its batches is live. Postgres marks a file for
  deletion only when no batch refers to it: `delete_records_v1` and
  `prune_batches_below_highest_tiered_offset_v1` check for remaining batches first
  (`V28__Soft_delete_logs.sql:818`, `:907-916`, `:1430`, `:1485-1488`). A footer still
  lists batches that retention, `DeleteRecords`, or consolidation pruning removed from a
  partition whose object has other live batches.
- A footer can't contain a value that the leader assigns after the upload. Writing one
  after the commit needs a second write. S3 `PutObject` replaces the whole object, and GCS
  ignores writes over persisted resumable bytes
  ([PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html);
  [Resumable uploads](https://cloud.google.com/storage/docs/resumable-uploads)).
- v2 reuses one key for every upload retry, and every backend overwrites unconditionally
  (v2 `design.md:242`). A footer built once in `BatchBuffer.close` is the same in every
  attempt.

## Upstream prior art

- KIP text: the Inkless docs link to the wiki pages of KIP-1150, KIP-1163, and KIP-1164
  (`docs/inkless/README.md:76-78`; `docs/inkless/proposals/VERSIONS.md:13`). Neither
  checkout contains their text, and a search of the Kafka checkout for the three numbers
  returns nothing.
- Inkless: a case-insensitive search for `footer` and `manifest` in `storage/inkless`,
  `docs/inkless`, and `core/src/main/scala/kafka/server` returns only the `LoaderManifest`
  of KRaft metadata publishing and unrelated changelog lines. The council's search of v2
  for both words came back empty
  (`.scratch/diskless-metadata-coordinator-v3/reviews/council.md:19`).
- Inkless consolidation: the fetcher reads WAL batches by coordinate and appends them to a
  per-partition `UnifiedLog`. `RemoteLogManager` then tiers classic segments
  (`docs/inkless/DISKLESS_CONSOLIDATION.md:44-48`). It leaves WAL objects unchanged and
  depends on the control plane's coordinates to read them.
- Kafka classic segments: a `.log` file contains batches only. The partition comes from the
  directory name (Kafka `storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java:752-755`).
  `LogSegment.recover` rebuilds the offset index, time index, and leader-epoch cache by
  scanning batches, because each batch stores its absolute offsets and leader epoch (Kafka
  `storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java:482-525`).
- Kafka tiered storage: `copyLogSegmentData` receives a segment with its offset index, time
  index, transaction index, producer snapshot, and leader-epoch index as separate pieces
  (Kafka `storage/api/src/main/java/org/apache/kafka/server/log/remote/storage/LogSegmentData.java:27-40`).

## Size cost

These estimates are inference from the field widths. The code has histograms for file
size, batches per file, and partition fan-in, but the checkouts contain no measured values
(`storage/inkless/src/main/java/io/aiven/inkless/produce/FileCommitterMetrics.java:176-199`).

Field widths:

- Per partition run, 32 bytes: the topic ID is 16 bytes, and the partition, start byte
  offset, byte length, and batch count are 4 bytes each.
- Per batch, 72 bytes, when the entry repeats `CommitBatchRequest` without the request ID
  and topic name. The fields are the magic, topic ID, partition, byte offset, size, base
  and last offsets, max timestamp, timestamp type, producer ID, producer epoch, base
  sequence, and last sequence. The entry drops to 52 bytes when each run stores the topic ID
  and partition once.
- Fixed part, about 40 to 50 bytes: a version, the broker ID, broker epoch, sequence,
  tracking partition, run count, CRC, footer length, and magic.

Typical counts:

- A full object is 8 MiB (`InklessConfig.java:60-63`). The producer default `batch.size` is
  16 KiB (Kafka `clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java:440`).
  With full batches, an object has about 512 batches.
- The smallest batch is the 61-byte header plus one record
  (Kafka `DefaultRecordBatch.java:131`).

| Object | Per-run footer | Per-batch footer (72 bytes each) |
| --- | --- | --- |
| 8 MiB, 512 batches across 100 partitions | 3.2 KB, 0.04% | 36.9 KB, 0.44% |
| 8 MiB, 512 batches across 512 partitions | 16.4 KB, 0.20% | 36.9 KB, 0.44% |
| 8 MiB of 70-byte batches, about 120,000 | Bounded by the partition count | About 8.6 MB, more than the data |
| A 250 ms file with 10 batches across 10 partitions | 370 bytes | 770 bytes |

A per-run footer grows with the partitions in a file. A per-batch footer grows with the
batch count, and in the worst case it's larger than the data. Either footer goes in the
same upload and doesn't add a storage request. On the hot path, the footer adds its size to
the bytes transferred and cached for the object's last block.

## Implications for the design

These facts constrain ticket [09-object-footer](../issues/09-object-footer.md). The decisions
stay there.

- A footer at the end of the object leaves the readers, the backends, and the storage
  request count unchanged. The array that `BatchBuffer.close` builds is the only write-side
  change.
- The bytes already describe each batch's producer fields, timestamps, and offset delta.
  Only the topic ID and partition of each run, and the uploader's identity, need a footer.
- A footer can't contain absolute offsets, the log-append time, each batch's commit
  outcome, the cross-broker commit order, or later removals. A rebuild from footers alone
  can list an object's batches and their partitions. It needs another source for the
  coordinator's offsets and producer state.
- An object stays in storage after retention, `DeleteRecords`, or pruning removes some of
  its batches. Its footer then lists batches that are no longer live.
- `ObjectFormat` and the stored `format` column are a version hook that readers ignore.
- Azure needs the blob size before a reader can fetch a footer, and `StorageBackend` lacks a
  list operation. A recovery that reads footers needs both a size lookup and a way to find
  the objects.

The sources leave these points open:

- Real batches per object and partitions per object. The histograms exist, and the
  checkouts contain no measured values.
- The text of KIP-1150, KIP-1163, and KIP-1164, which only the wiki has.
