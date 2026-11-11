# Single-coordinator objects in the writer

Type: research
Status: open
Assignee: unassigned
Blocked by: none

## Question

How does the broker guarantee that every WAL object it uploads maps to exactly one coordinator partition, and what does that guarantee cost on the produce path?

The variant rests on this invariant, and the map's [Notes](../map.md#notes) say routing can't provide it. Today `Writer` holds one `ActiveFile` (`storage/inkless/src/main/java/io/aiven/inkless/produce/Writer.java`). Every produce request goes into it, and `ActiveFile.add` returns one future per request. `Writer` rotates the file when the commit tick fires or the buffer reaches `produce.buffer.max.bytes`. `FileCommitter` uploads on a pool of `fileUploaderThreadPoolSize` threads and commits on a single thread.

Find, in the Inkless checkout:

- Where the split belongs: one `ActiveFile` per coordinator partition inside `Writer`, a split at `AppendHandler` before the writer, or another seam. Say which classes change and whether the Postgres and in-memory backends keep today's single file. v2's commit queue key, which maps every user partition to one queue on those backends, is a candidate split key.
- How a produce request whose partitions map to several coordinator partitions gets one response: the futures it waits on, how per-partition results merge, and what latency it sees compared with one file today.
- The buffer limit: whether `produce.buffer.max.bytes` applies per file or to the sum, the worst-case buffer memory per broker for each choice, and what happens to rotation when one coordinator partition's buffer fills while others stay small.
- The commit tick: one timer per file or one shared tick that rotates every non-empty file, and how each choice changes the number of objects per second under light, moderate, and heavy load.
- Upload and commit concurrency: whether `fileUploaderThreadPoolSize` and the commit executor need to grow with `N`, and how v2's per-queue ordering in `FileCommitter` maps onto per-coordinator files.
- Back pressure: `Writer.write` has a `TODO add back pressure`. Say whether more files in flight change that risk.

## Resolution
