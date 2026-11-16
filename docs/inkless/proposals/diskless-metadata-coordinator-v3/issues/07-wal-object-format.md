# WAL object format and footer feasibility

Type: research
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

What does an Inkless WAL object contain today, and can a self-describing footer be added without changing how readers fetch?

The council's [first fix](../reviews/council.md#what-would-close-the-gaps) is a small footer that names an object's batches, so the coordinator state for an object can be rebuilt from storage after every replica of a coordinator partition is lost. Find, in the Inkless checkout and the local Kafka checkout at `/Users/viktor.somogyi-vass/dev/github/apache/kafka`:

- How the broker builds a WAL object: the classes that write it, the byte layout, and what each batch carries. Does a batch carry its topic ID and partition, or only what the coordinator assigns, such as the base offset?
- How readers fetch: do they read byte ranges from batch coordinates only, so trailing bytes are never read?
- How each storage backend uploads, and whether appending a footer at the end of the same upload is possible on S3, GCS, and Azure.
- What a coordinator partition would need from a footer to rebuild its state for an object: the user partition, the byte range, the producer fields, the uploader's broker ID, epoch, and sequence. Which of these the broker knows at upload time and which only the leader assigns, such as offsets.
- Any upstream prior art: footers or manifests in KIP-1150, KIP-1163, or KIP-1164 text, if the local checkouts hold it, or in Inkless consolidation.
- The size cost of a footer per object at typical batch counts.

## Resolution

Resolved by research on 2026-11-11. The findings, with citations, are in [wal-object-format.md](../research/wal-object-format.md).

A WAL object is the Kafka v2 record batches of one file, written back to back by `BatchBuffer.close` and grouped by topic ID and then partition. The object has no header, separator, or trailer. Each batch header stores its length, producer ID, producer epoch, base sequence, offset delta, and timestamps. It has no topic ID or partition. Its base offset counts from 0 within the produce request, and its partition leader epoch is 0. The control plane's batch rows are the only record of which partition a byte range belongs to and of its absolute offsets.

- Readers ignore trailing bytes. `FetchCompleter` parses only each batch's coordinate range. The cold and consolidation paths fetch the bounding range of the requested batches. The hot path fetches 16 MiB aligned blocks, so for objects up to 16 MiB it transfers and caches a footer without reading it.
- S3, GCS, and Azure each upload the whole in-memory array in one write that becomes visible only when complete. Appending a footer changes only the array that `BatchBuffer.close` builds. The backends and the request count stay the same, and the stored `fileSize` grows by the footer.
- At upload, the broker knows each batch's user partition, byte range, producer fields, and timestamps. With v2, it also knows its broker epoch, sequence, object key, and tracking partition. The header bytes already contain the producer fields and timestamps. A footer needs to add only each run's topic ID and partition and the uploader's identity.
- The leader sets these at commit: the absolute offsets, the log-append time, each batch's outcome (accepted, duplicate, invalid epoch, out of order, or nonexistent log), producer state, and the order against other brokers' commits. A footer written at upload can't contain them, and rewriting an object after the commit needs a second write.
- An object stays in storage until no batch refers to it. Its footer keeps listing batches that retention, `DeleteRecords`, or consolidation pruning already removed.
- The checkouts only link to KIP-1150, KIP-1163, and KIP-1164, and Inkless and Kafka formats lack any footer or manifest. Kafka's classic segments recover by scanning batches that store absolute offsets, and the directory name gives the partition.
- Readers ignore the stored `ObjectFormat`, which has one value, and Postgres accepts IDs 1 to 3. Azure can't read a suffix range, and `StorageBackend` has no list operation. A recovery needs a size lookup and a way to find objects.
- Size: a per-partition-run footer is about 32 bytes per run plus about 45 fixed, which comes to 3.2 KB for 100 partitions in an 8 MiB object. A per-batch footer is about 72 bytes per batch, which comes to 37 KB for 512 batches. With tiny batches, it can exceed the data. The checkouts contain no measured batch or partition counts per object.

The footer's fields, what a recovery rebuilds, and whether it needs a listing belong to [Object footer and recovery after losing a replica set](09-object-footer.md).
