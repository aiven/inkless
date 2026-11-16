# Fsync and checkpoint cost per commit

Type: research
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

How many fsyncs does one file cost in v2, on which threads, and where does SQLite add more?

Both reviews call fsync cost the main risk against the Postgres bar. Fable counts up to (participants + 1) × replication factor fsyncs per file, against one for Postgres, worst on small clusters, and notes that SQLite's WAL auto-checkpoint fsyncs on the shard thread. See [Fable's fsync suggestions](../reviews/fable.md#design-and-maintainability) and v2's [Flush policy](../../diskless-metadata-coordinator-v2/design.md#flush-policy) and [Local state](../../diskless-metadata-coordinator-v2/design.md#local-state). Find, in the local Kafka checkout at `/Users/viktor.somogyi-vass/dev/github/apache/kafka`, the Inkless checkout, and the SQLite docs:

- With `flush.messages=1`: where `UnifiedLog` fsyncs on the leader append and the follower fetch append, and on which thread. How `CoordinatorRuntime`'s linger and adaptive batching group records per append, so how many fsyncs one coordinator partition does per commit round under load.
- The fsync count per file and per commit round for a cluster of `B` brokers with `N` coordinator partitions and replication factor `R`, for `B=3, N=8, R=3` and a larger cluster. Count the parts, the tracking part, and any fence, release, or participant-state records the normal path appends.
- SQLite in WAL mode with `synchronous=NORMAL`: when it fsyncs, including at auto-checkpoint, and what `wal_autocheckpoint=0` plus a checkpoint from another connection or thread changes. Whether one transaction per high-watermark advance instead of per record is possible with the apply rules.
- What the Postgres backend flushes per commit, from [postgres-performance-bar.md](../../diskless-metadata-coordinator-v2/research/postgres-performance-bar.md), for comparison.

## Resolution

Resolved by research on 2026-11-11. The findings, with citations, are in [fsync-checkpoint-cost.md](../research/fsync-checkpoint-cost.md).

One v2 file costs one coordinator log flush per part on the leader and one on each follower, so up to `parts × R`, where `parts` counts the participants plus the tracking partition when its part is empty. That's at most `N × R`, 24 at `N=8, R=3`, against one WAL flush on the Postgres server, or two with a synchronous standby. Fable's "(participants + 1) × replication factor" is the same bound. Every flush is synchronous: the leader flushes on the `CoordinatorRuntime` thread inside the log lock, and each follower flushes on its replica fetcher thread before the fetch that advances the high watermark, so a commit waits for two flushes in series.

- One flush is one `LogSegment.flush`: an `fsync` of the segment, an `msync` and an `fsync` per index, and an `fsync` of the transaction index if it exists. `LogFlushRateAndTimeMs` measures it.
- `CoordinatorRuntime` shares an append only between writes that queue while the shard is busy. Under adaptive linger, the group coordinator's default, a write at an idle shard gets its own append, so at moderate load each part gets its own flush. A fixed linger bounds appends per shard to one per linger and adds that much latency. v2 sets neither the linger nor the runtime thread count.
- Per commit round, with one file per broker, each broker flushes between `N × R / B` and `N × R` times. At `B=3, N=8, R=3`, every broker hosts all 8 partitions and flushes 8 to 24 times per round, 32 to 96 times per second at the 250 ms commit interval. At `B=12, N=8, R=3`, it's 2 to 24 per round. The upper bound per broker doesn't depend on `B`.
- An empty tracking part fences every participant at commit and appends one participant-state-recorded record per answer, up to `(parts − 1) × R` more flushes per file on the normal path. Releases add two records per participant later, off the commit path.
- SQLite with `synchronous=NORMAL` in WAL mode doesn't sync on commit. It syncs the WAL before a checkpoint, the database after a complete checkpoint, and the WAL header when a writer restarts the WAL. The automatic checkpoint runs at 1000 pages on the thread whose commit crosses the limit, which in v2 is the runtime thread that applies and replies.
- With `wal_autocheckpoint=0` and a checkpoint on another connection, the checkpoint's syncs and page copy move off the apply thread, but the writer still syncs the WAL header once per WAL cycle. Only `PASSIVE` checkpoints never block writers, and two connections on one database need SQLite 3.51.3 or a backport for the WAL-reset bug fix.
- One SQLite transaction per high-watermark advance fits every v2 apply rule. The runtime already merges high-watermark updates into one event. Stopping at an unreadable or impossible record needs a savepoint per record, and a large group needs a size limit, because a long write transaction keeps the WAL from resetting.

The checkpoint placement and transaction grouping belong to [SQLite apply and checkpoint off the shard thread](13-sqlite-apply-checkpoint.md). The default `N` and the linger belong to [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md). Flush latency and where a real cluster sits between the bounds need the benchmark.
