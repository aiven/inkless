# Fsync and checkpoint cost per commit

Research for ticket [12-fsync-checkpoint-cost](../issues/12-fsync-checkpoint-cost.md).

## Summary

One file in v2 costs one coordinator log flush per part on the leader and one on each
follower, so up to `parts × R` flushes, where `parts` is the file's participants plus the
tracking partition when that partition holds none of the file's batches. `parts` is at most
`N`, so the bound is `N × R`: 24 at `N=8, R=3`, against one WAL flush on the Postgres
server. Fable's "(participants + 1) × replication factor" is the same bound for a file whose
tracking part is empty.

- Every flush is synchronous. The leader flushes on the `CoordinatorRuntime` thread that runs
  the shard, inside `UnifiedLog`'s lock. Each follower flushes on its replica fetcher thread,
  one partition after another, before it sends the fetch that advances the leader's high
  watermark.
- One "flush" is one `LogSegment.flush`, which makes up to five sync calls: an `fsync` of the
  segment, an `msync` and an `fsync` for each of the offset and time indexes, and an `fsync`
  of the transaction index when that file exists.
- `CoordinatorRuntime` shares one append between writes only when they queue while the
  shard is busy. Under the adaptive linger that the group coordinator uses by default, a
  write that arrives at an idle shard gets its own append. Inference: at moderate load, each
  part of each file gets its own append, and the count per round sits at its upper bound.
- Per commit round, with one file per broker, the cluster flushes between `N × R` and
  `B × N × R` times. Each broker flushes between `N × R / B` and `N × R` times. At
  `B=3, N=8, R=3`, every broker hosts all 8 coordinator partitions and flushes 8 to 24
  times per round, 32 to 96 times per second at the 250 ms commit interval.
- An empty tracking part adds a normal-path fan-out. The tracking partition fences every
  participant at once, and it appends one participant-state-recorded record per answer, up
  to `(parts − 1) × R` more flushes per file.
- SQLite with `synchronous=NORMAL` in WAL mode doesn't sync on commit. It syncs in three
  places: the WAL before a checkpoint copies frames back, the database file after a complete
  checkpoint, and the WAL header when a writer restarts the WAL. The automatic checkpoint
  runs on the thread whose commit pushes the WAL past 1000 pages.
- With `wal_autocheckpoint=0` and a checkpoint on another connection, the first two syncs and
  the page copy move off the apply thread. The writer still syncs the WAL header once per
  WAL restart. The background connection is the pattern that the WAL-reset bug needs, which
  SQLite fixes in 3.51.3.
- One SQLite transaction per high-watermark advance fits v2's apply rules. The runtime
  already merges high-watermark updates into one event. The rule that stops apply at an
  unreadable or impossible record needs a savepoint per record or a decode pass first, so the
  transaction still commits everything before that record.

## Sources

The research uses two local checkouts, the SQLite documentation, and the SQLite source.
Citations use paths relative to each checkout root.

- Inkless: `/Users/viktor.somogyi-vass/dev/github/aiven/workspace-inkless/diskless-metadata`
  at commit `2a412af730`.
- Kafka: `/Users/viktor.somogyi-vass/dev/github/apache/kafka` at commit `27d8bbc35e`.
- SQLite documentation at `https://www.sqlite.org/`, read on 2026-11-11.
- SQLite source from `https://www.sqlite.org/src/`, trunk at `VERSION` 3.55.0, read on
  2026-11-11. Line numbers in `src/wal.c`, `src/wal.h`, and `src/pager.c` refer to that
  trunk.

Kafka citations refer to the Kafka checkout. The log, fetcher, and coordinator runtime code
is upstream code, and the Inkless checkout has the same logic at other line numbers, for
example the flush check at `storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java:1286`
in Inkless and `:1300` in Kafka. Citations into v2 use paths under `.scratch/`.

v2 isn't code yet. Where this research counts v2's records, it cites the design, and it
marks counts that go beyond the cited text as inference.

## What one log flush costs

- `UnifiedLog.append` flushes after an append once the unflushed message count reaches
  `flush.messages` (`storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java:1300`).
  The count is the log end offset minus the recovery point
  (`storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java:252-254`).
  Under `flush.messages=1`, every append flushes. The broker default is `Long.MAX_VALUE`
  (`server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java:101`).
- The flush call sits inside the `synchronized (lock)` block of the append
  (`UnifiedLog.java:1150`, `:1300`). `maybeIncrementHighWatermark` takes the same lock
  (`UnifiedLog.java:563-569`), so a follower fetch that would raise the high watermark waits
  for the leader's flush to end.
- `LocalLog.flush` calls `LogSegment.flush` for each segment past the recovery point. When
  the range includes a new segment, it also fsyncs the partition directory
  (`LocalLog.java:262-274`). Inference: that's one extra directory fsync per segment roll.
  The roll itself flushes the old segment on the scheduler thread, not on the append thread
  (`UnifiedLog.java:2221-2241`).
- `LogSegment.flush` flushes four files in order: the records, the offset index, the time
  index, and the transaction index
  (`storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java:628-636`).
  - The records flush is one `FileChannel.force(true)`
    (`clients/src/main/java/org/apache/kafka/common/record/internal/FileRecords.java:208-210`).
  - Each index flush is a `MappedByteBuffer.force()` and then a `Utils.flushPathIfExists` of
    the index file, which opens the file and calls `force(true)`
    (`storage/src/main/java/org/apache/kafka/storage/internals/log/AbstractIndex.java:245-252`;
    `clients/src/main/java/org/apache/kafka/common/utils/Utils.java:970-976`).
  - The transaction index forces its channel only if the file is open, and it opens the
    file only when the file exists or when the log appends an aborted transaction
    (`storage/src/main/java/org/apache/kafka/storage/internals/log/TransactionIndex.java:67-73`,
    `:95-99`). Coordinator records aren't transactional
    (`coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java:929-935`).
- A segment adds an index entry only after `index.interval.bytes` of records, 4096 by
  default (`LogSegment.java:270-274`; `ServerLogConfigs.java:97`). Inference: most flushes
  of small coordinator batches find the index pages clean. Whether a sync call on a clean
  file reaches the device is filesystem behavior that the sources don't cover.
- `LogSegment.flush` records its time in `LogFlushRateAndTimeMs`
  (`storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java:76` in
  Inkless), which already measures this cost per broker.

The rest of this file counts `LogSegment.flush` calls and calls each one a flush.

## Leader append

- The runtime writes a batch with one `partitionWriter.append` call, synchronously, on the
  thread that runs the shard's event
  (`CoordinatorRuntime.java:694-719`). The upstream writer appends through
  `ReplicaManager.appendRecordsToLeader` in the same call and returns only after the local
  append (`core/src/main/scala/kafka/coordinator/group/CoordinatorPartitionWriter.scala:163-193`;
  `core/src/main/scala/kafka/server/ReplicaManager.scala:586-611`). v2's own writer appends
  with `acks=-1` in the same way
  (`.scratch/diskless-metadata-coordinator-v2/design.md:206`).
- So the leader's flush runs on the runtime thread. Inference: while it runs, that thread
  can't serve another shard, and the shard's own next writes queue.
- The upstream writer passes `directActionQueue`, so the delayed-fetch purgatory runs right
  after the local append returns (`CoordinatorPartitionWriter.scala:178-180`;
  `ReplicaManager.scala:853-874`). A follower fetch parked in the purgatory wakes only after
  the leader's flush.
- `UnifiedLog.read` takes no lock and reads up to the log end offset metadata for a follower
  (`UnifiedLog.java:1663-1673`). The append moves the log end offset before it flushes
  (`UnifiedLog.java:1276-1277`, `:1300`). A follower fetch that arrives during the flush can
  read the new records already.
- Followers park with `replica.fetch.wait.max.ms=500` and `replica.fetch.min.bytes=1` by
  default (`server/src/main/java/org/apache/kafka/server/config/ReplicationConfigs.java:75`,
  `:80`). Inference: a follower that keeps up waits in the purgatory when the batch arrives,
  so the leader's flush and the follower's flush run one after the other, not in parallel.

## Follower append

- A follower appends each partition's fetched records with one `appendAsFollower` call
  (`core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:113-133`;
  `core/src/main/scala/kafka/cluster/Partition.scala:1169-1189`;
  `UnifiedLog.java:1093-1103`). That call runs the same `append` and the same flush check.
  So a follower flushes once per partition per fetch response, however many leader batches
  the response holds.
- `processFetchRequest` handles every partition of a response under `partitionMapLock`, in
  sequence, and computes the next fetch offset from each append
  (`core/src/main/scala/kafka/server/AbstractFetcherThread.scala:318-396`). The next fetch
  request goes out after that.
- A fetcher thread serves the partitions it gets from one leader by a hash of topic and
  partition (`core/src/main/scala/kafka/server/AbstractFetcherManager.scala:111-115`), and
  `num.replica.fetchers` is 1 by default (`ReplicationConfigs.java:96`). Inference: a
  follower that fetches `k` coordinator partitions from one leader flushes them in series on
  one thread, and its classic partitions from that leader wait behind them.
- The leader's high watermark rises when it handles the follower's next fetch. A commit part
  waits for the leader's flush, the follower's response, the follower's flushes for
  every partition in that response, and the follower's next fetch. The file waits for its
  slowest part (`.scratch/diskless-metadata-coordinator-v2/design.md:275`).

## How `CoordinatorRuntime` groups records into appends

- Events for one partition never run concurrently
  (`CoordinatorRuntime.java:93-95`;
  `coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/EventAccumulator.java:35-37`).
- A write opens a batch if none is open (`CoordinatorRuntime.java:937-943`), adds its records,
  attaches its reply, and then checks whether to flush (`:1036-1043`). `maybeFlushCurrentBatch`
  flushes a transactional batch, a full batch, or a batch past a fixed linger
  (`:770-780`). Each flush is one `partitionWriter.append`, so one leader flush.
- Fixed linger: opening a batch schedules a timer that puts a flush event at the front of the
  queue once the linger elapses (`:826-851`). A shard then appends at most once per linger,
  plus once per full batch.
- Adaptive linger: opening a batch puts a flush event at the back of the queue
  (`:852-859`, `:668-686`). The batch takes every write that was already queued, and
  nothing else. The group coordinator's default is adaptive: `group.coordinator.append.linger.ms`
  is `-1`, documented as "an adaptive linger time that minimizes latency based on the
  workload" (`group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorConfig.java:93-99`).
- A batch also flushes when the next record doesn't fit (`CoordinatorRuntime.java:995-1005`),
  with the topic's maximum message size as the bound (`:804`).
- Inference: under adaptive linger, a shard appends at most once per flush time, and its batch
  grows with the writes that queue during the previous flush. That's group commit. When parts
  arrive further apart than one flush, each part gets its own append.
- High-watermark updates merge. The listener enqueues an event only when no update is
  pending, and the event applies the newest value (`CoordinatorRuntime.java:1810-1831`).
- Shards share the runtime threads. The group coordinator uses 4 by default
  (`GroupCoordinatorConfig.java:101-103`; `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java:266`).
  v2 sets neither the thread count nor the linger default. The linger default is an open item
  (`.scratch/diskless-metadata-coordinator-v2/design.md:1097`).

## Records the normal path appends per file

v2 names these records. `P` is the file's participant set, `t` its tracking partition, and
`parts = |P ∪ {t}|`.

| Record | Partition | When | Per file |
| --- | --- | --- | --- |
| File committed | Every participant, and `t` | Commit | `parts`, at most `N` |
| Sequences fenced | A participant whose fence arrives before its part | Commit, empty tracking part only | 0 to `parts − 1` |
| Participant state recorded | `t` | Commit, empty tracking part only, one per fence answer | Up to `parts − 1` |
| Sequences leased | `t` | Lease renewal | One per range, and a range covers at most 1,024 files |
| Participant state recorded | `t` | Release report, after retention, consolidation, or deletion | One per participant other than `t` |
| Release accepted | Each participant other than `t` | After `t` accepts the report | One per participant other than `t` |
| Files deleted | `t` | `FileCleaner` cycle | One per cycle, shared by every key in it |

- Every file sends one part to every participant and one to `t`, and the part to `t` is
  empty when `t` holds none of the file's batches
  (`.scratch/diskless-metadata-coordinator-v2/design.md:272`). Each part appends one
  file-committed record (`design.md:300`).
- The apply that records an empty part at `t` also releases it (`design.md:396`). Once `t`
  and every participant it has heard from have released the object, `t` fences every silent
  participant (`design.md:411`). With an empty part, both conditions hold at once. Every
  participant then gets a fence at commit time, as [Fable's first finding](../reviews/fable.md#an-empty-tracking-part-fences-in-flight-commits)
  says.
- A participant appends a sequences-fenced record only if the fence is above its floor
  (`design.md:403`). Inference: if the part committed first, the floor is already at the
  sequence and the fence appends nothing there. If the fence wins, the participant appends
  the record and the part fails with stale-sequence. Either way, `t` records each answer in a
  participant-state-recorded record (`design.md:406`).
- When `t` holds batches of the file, `t` doesn't release the object at commit, and no fence
  goes out on the commit path. v2 expects most files to hold batches for most coordinator partitions
  (`design.md:264`, `:680`). Inference: an empty tracking part is most common for small files
  and for brokers whose produce traffic maps to few coordinator partitions.
- Leases: a range is at most `K = 1024` sequences, and the broker renews once fewer than
  `K/2` remain (`design.md:249`, `:251`).
- Releases run later, off the commit path: one report and two records per participant other
  than `t`, and none for `t` itself (`design.md:394-396`). `FileCleaner` appends one
  files-deleted record per partition per cycle (`design.md:459`).
- Barriers, lease ends, epoch retirements, and missed-sequence fences run only on reads above
  the high watermark, idle brokers, epoch changes, or failures.

## Flush counts per file and per commit round

Each record append costs one flush on the leader and one on each follower that fetches it.
A follower merges the leader appends it finds in one fetch response into one flush.

- Per file, with no sharing: `parts × R` flushes, at most `N × R`.
- An empty tracking part adds up to `(parts − 1) × R` flushes at `t`, and up to `R` at each
  participant whose fence wins its race.
- Per commit round, with one file per broker: coordinator partition `p` receives up to `B`
  parts and makes between 1 and `B` appends. The cluster flushes between `N × R` and
  `B × N × R` times, if every file touches every partition.
- Per broker, with `h = N × R / B` replicas per broker: between `h` and `h × B = N × R`
  flushes per round. The upper bound per broker doesn't depend on `B`.
- The commit interval is `produce.commit.interval.ms`, 250 ms by default
  (`storage/inkless/src/main/java/io/aiven/inkless/config/InklessConfig.java:56-58`). v2
  assumes about one file per broker per interval, and up to about 12 files per second when
  the produce buffer fills (`.scratch/diskless-metadata-coordinator-v2/design.md:249`).

| Quantity | `B=3, N=8, R=3` | `B=12, N=8, R=3` | `B=3, N=1, R=3`, for reference |
| --- | --- | --- | --- |
| Coordinator replicas per broker, `h` | 8, every partition | 2 | 1 |
| Flushes per file, no sharing | Up to 24 | Up to 24 | Up to 3 |
| Flushes per round, cluster | 24 to 72 | 24 to 288 | 3 to 9 |
| Flushes per round, per broker | 8 to 24 | 2 to 24 | 1 to 3 |
| Flushes per second per broker, 4 rounds per second | 32 to 96 | 8 to 96 | 4 to 12 |
| Postgres commits per round, one WAL flush each on the server | 3 | 12 | 3 |

Inference on which bound applies: nothing aligns the brokers' commit timers, so a shard
receives parts spread across the interval. At `B=3`, a shard gets about 12 parts per second. Under
adaptive linger, each gets its own append unless it arrives during the previous flush, so the
count stays near the upper bound until parts arrive about one flush time apart. A fixed
linger of one commit interval gives the lower bound, at the cost of up to one interval of
added latency per part. The sources don't give the flush latency, so they don't settle where
a real cluster sits between the bounds.

## SQLite fsyncs in WAL mode with `synchronous=NORMAL`

v2 runs SQLite in WAL mode with `synchronous=NORMAL`, and each apply stores the applied
offset in the same transaction as the state change
(`.scratch/diskless-metadata-coordinator-v2/design.md:536`, `:540-541`).

### When SQLite syncs

- Commit: "In WAL mode when synchronous is NORMAL (1), the WAL file is synchronized before
  each checkpoint and the database file is synchronized after each completed checkpoint and
  the WAL file header is synchronized when a WAL file begins to be reused after a checkpoint,
  but no sync operations occur during most transactions"
  ([`synchronous` pragma](https://www.sqlite.org/pragma.html#pragma_synchronous)). `FULL`
  adds a WAL sync after each commit.
- The source matches. `NORMAL` sets the checkpoint sync flags and leaves the commit sync flags
  at zero (`src/pager.c:3700-3713`; `src/wal.h:22-26`). The commit sync runs only when the
  commit flags are nonzero (`src/wal.c:4296-4314`).
- Checkpoint: the checkpointer syncs the WAL before it copies frames back
  (`src/wal.c:2359-2360`). After it copies every frame, it truncates and syncs the database
  file (`src/wal.c:2410-2417`). It does that work only when frames past the last copied one
  are safe to copy (`src/wal.c:2305`, `:2336-2342`).
- WAL restart: before a writer writes frames, it checks whether it can restart the WAL from
  the beginning (`src/wal.c:3976-3999`, `:4180`). The first frame after a restart writes a new
  WAL header and syncs it, unless the device reports `SQLITE_IOCAP_SEQUENTIAL`
  (`src/wal.c:4184-4226`, `:1753`, `:1769-1770`). That sync runs on the writer's thread.
- The WAL page disagrees on that last point: "with PRAGMA synchronous set to NORMAL, the
  checkpoint is the only operation to issue an I/O barrier or sync operation"
  ([WAL docs, performance considerations](https://www.sqlite.org/wal.html#performance_considerations)).
  The pragma page and the source both show the writer's header sync.
- A checkpoint is slower than a write transaction, because it syncs and because the page
  copy back to the database file seeks
  ([WAL docs](https://www.sqlite.org/wal.html#performance_considerations)).

### The automatic checkpoint

- By default, SQLite checkpoints when a commit leaves the WAL at 1000 pages or more, about
  4 MB, and it reuses the WAL from the start instead of truncating it
  ([WAL docs, automatic checkpoint](https://www.sqlite.org/wal.html#automatic_checkpoint);
  [WAL docs, large WAL files](https://www.sqlite.org/wal.html#bigwal)).
- "By default, the checkpoint will be run automatically by the same thread that does the
  COMMIT that pushes the WAL over its size limit." Once the WAL passes the limit, every
  following commit runs a checkpoint until a checkpoint lets the WAL reset
  ([WAL docs](https://www.sqlite.org/wal.html#performance_considerations)).
- The automatic checkpoint is a wrapper around `sqlite3_wal_hook`, runs in `PASSIVE` mode,
  and is on by default for every connection
  ([`sqlite3_wal_autocheckpoint`](https://www.sqlite.org/c3ref/wal_autocheckpoint.html)).
  SQLite calls the hook after the commit, once it has released the write lock
  ([`sqlite3_wal_hook`](https://www.sqlite.org/c3ref/wal_hook.html)).
- Inference for v2: the leader applies a record when the high watermark passes it, and its
  overlay lives in the shard's timeline structures
  (`.scratch/diskless-metadata-coordinator-v2/design.md:210`, `:301`). The runtime's
  high-watermark event runs on the shard (`CoordinatorRuntime.java:1819-1831`). So the leader's
  apply, and with it the automatic checkpoint's two syncs, page copy, and the writer's header
  sync, run on the runtime thread that also runs the shard's log flush. v2 doesn't name the
  follower applier's thread (`design.md:213`).
- Since SQLite 3.11.0, a transaction writes each changed page to the WAL once
  ([WAL docs, large WAL files](https://www.sqlite.org/wal.html#bigwal)). Inference: one
  transaction per record writes a hot page, such as the applied-offset row's page, once per
  record. Fewer, larger transactions write fewer frames and so reach the 1000-page threshold
  less often.

### `wal_autocheckpoint=0` and a checkpoint on another connection

- `PRAGMA wal_autocheckpoint=0` turns the automatic checkpoint off
  ([`wal_autocheckpoint` pragma](https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint)).
  Any writable connection can then checkpoint with `sqlite3_wal_checkpoint` or
  `sqlite3_wal_checkpoint_v2`, and SQLite documents running "the periodic checkpoints in a
  separate thread, or separate process" for applications that don't want the slow commit
  ([WAL docs, application-initiated checkpoints](https://www.sqlite.org/wal.html#application_initiated_checkpoints)).
- What moves: the WAL sync, the page copy, and the database sync run on the checkpointing
  connection's thread. What stays: the writer still syncs the WAL header once per WAL restart
  (`src/wal.c:4184-4226`). Inference: that's one sync per WAL cycle on the apply thread, in
  place of three syncs and a page copy.
- Checkpoint modes ([`sqlite3_wal_checkpoint_v2`](https://www.sqlite.org/c3ref/wal_checkpoint_v2.html)):
  - `PASSIVE` copies as many frames as it can without waiting for readers or writers, and
    never calls the busy handler. It can leave the checkpoint unfinished.
  - `FULL`, `RESTART`, and `TRUNCATE` take the writer lock and block new writers while they
    wait. A background checkpoint in one of these modes can stall the apply.
  - All modes take an exclusive checkpoint lock. A concurrent second checkpoint gets
    `SQLITE_BUSY` at once.
- With the automatic checkpoint off, "there is nothing to prevent the WAL file from growing
  excessively" if the application doesn't checkpoint
  ([WAL docs, large WAL files](https://www.sqlite.org/wal.html#bigwal)). Read speed falls as
  the WAL grows, because readers search it
  ([WAL docs](https://www.sqlite.org/wal.html#performance_considerations)).
- A reader stops a checkpoint at the reader's end mark, and only a checkpoint with no reader
  using the WAL can reset it ([WAL docs, concurrency](https://www.sqlite.org/wal.html#concurrency);
  [WAL docs, large WAL files](https://www.sqlite.org/wal.html#bigwal)). A large write
  transaction also keeps the WAL from resetting until it ends.
- Snapshot backups interact with both. `sqlite3_backup_step` holds a shared lock for the
  duration of each step. If another connection changes the source between steps, the backup
  restarts at the next step
  ([`sqlite3_backup_step`](https://www.sqlite.org/c3ref/backup_finish.html#sqlite3backupstep)).
  Inference: a backup on its own connection either copies in one step, holding a read lock
  that keeps the WAL from resetting for that step, which v2 already notes (`design.md:540`), or
  restarts on every apply.
- The WAL-reset bug: from 3.7.0 through 3.51.2, two connections that write or checkpoint the
  same WAL database at the same instant can corrupt it. 3.51.3 fixes it, and 3.44.6 and 3.50.7
  carry backports ([WAL docs, the WAL-reset bug](https://www.sqlite.org/wal.html#walresetbug)).
  A checkpoint connection next to an apply connection is that pattern. The repository has no
  SQLite dependency yet, and v2 leaves the driver choice open
  (`.scratch/diskless-metadata-coordinator-v2/design.md:544`).

### One transaction per high-watermark advance

The runtime already delivers one high-watermark event for any number of updates that arrive
while one is pending (`CoordinatorRuntime.java:1810-1831`). Each v2 apply rule against a
transaction that covers every record up to the new high watermark:

- Log order and the high-watermark bound (`design.md:138`, `:538-539`): unchanged. The group is a
  contiguous range below the high watermark.
- Applied offset and epoch in the same transaction as the state (`design.md:536`): the
  transaction stores the last record's offset and epoch. The open check compares that pair with
  the log (`design.md:537`), and the snapshot tag reads the same row from the copy
  (`design.md:577`). Both stay exact at group boundaries.
- Stop at an unreadable record, or at an impossible file-committed or sequences-leased record,
  with SQLite at the last applied offset (`design.md:175-180`, `:363-366`): a group must commit
  every record before the bad one and none after. SQLite's `ROLLBACK TO` reverts to a savepoint
  without canceling the transaction ([SAVEPOINT](https://www.sqlite.org/lang_savepoint.html)),
  so a savepoint per record does this. A decode pass before the transaction catches the
  unreadable cases, but not the impossible ones, which depend on state.
- No-op records still advance the applied offset (`design.md:368-372`): unchanged.
- The emptiness check runs in the transaction that removes batches (`design.md:435`):
  unchanged, because it's still one transaction.
- Replies and overlay removal after apply (`design.md:195`, `:301`): every reply in the group
  waits for one commit. Under `NORMAL`, that commit doesn't sync unless it restarts the WAL or
  runs the automatic checkpoint.
- Power loss rolls back recent applies, and the replica replays from the log
  (`design.md:541`): unchanged, at group granularity.
- Group size: a high-watermark advance can cover many records, for example on a new leader
  that applies up to its election-time log end. Inference: an unbounded group is a large write
  transaction that keeps the WAL from resetting, so the group needs a record or byte limit.

## The Postgres baseline

From [postgres-performance-bar.md](../../diskless-metadata-coordinator-v2/research/postgres-performance-bar.md#commit):

- A broker commits files on one thread, one file at a time. Each file is one transaction with
  one call to `commit_file_v2`, which locks the `logs` row of every partition in the file.
- The WAL flush on commit depends on the server's `synchronous_commit` and on any synchronous
  standby. Neither checkout sets them. Under the server's defaults, a commit is one WAL flush on
  the primary. A synchronous standby adds a flush and a network hop.
- The flush runs on the database host, not on a broker, so it doesn't share a broker's disk,
  page cache, or threads.

| | Postgres | v2 coordinator |
| --- | --- | --- |
| Flushes per file | 1, or 2 with a synchronous standby | Up to `N × R`, plus up to `(parts − 1) × R` for an empty tracking part |
| Where | Database host | Every coordinator replica's broker |
| Thread | Database server | Leader: runtime thread, inside the log lock. Follower: replica fetcher thread, in series with the other partitions it fetches from that leader |
| Flushes in series before the reply | 1, or 2 with a synchronous standby | 2: the leader's, then the follower's, plus the follower's flushes for other partitions in the same response |
| Merging across files | Not covered by the sources | One append per shard per flush under adaptive linger, when writes queue |

## What the grilling tickets need to settle

[SQLite apply and checkpoint off the shard thread](../issues/13-sqlite-apply-checkpoint.md)
needs to settle:

- Where checkpoints run. The automatic checkpoint puts two syncs and a page copy on the thread
  whose commit crosses 1000 pages. Moving it to another connection leaves one header sync per
  WAL cycle on the apply thread. Of the checkpoint modes, only `PASSIVE` never blocks the
  apply, and a second connection needs a SQLite build that has the WAL-reset fix.
- How many records one transaction applies, and how the stop-at-a-bad-record rule works inside
  a group.
- How the checkpoint thread, readers, and snapshot backups share the WAL without letting it grow
  without bound.

[Fsync amplification and default N on small clusters](../issues/14-small-cluster-fsync.md)
needs to settle:

- The default `N`, given that the per-broker flush count per round ranges from `N × R / B` to
  `N × R`, and that adaptive linger keeps it near the top until parts arrive about one flush
  apart.
- The linger: adaptive, which minimizes latency and shares appends only under load, or fixed,
  which bounds appends per shard per interval and adds latency.
- The empty-tracking-part fan-out, which [When the tracking partition fences silent participants](../issues/01-silent-participant-fences.md)
  owns, because it adds up to `(parts − 1) × R` flushes per file on the normal path.

The sources don't settle these points:

- Flush latency, and whether a sync call on a clean index file costs a device flush. These
  depend on the device and filesystem.
- Where a real cluster sits between the lower and upper flush bounds. That needs the benchmark
  in [Benchmark pass criteria](../issues/16-benchmark-pass-criteria.md).
- The Postgres server's `synchronous_commit`, standby mode, and how it merges concurrent commit
  flushes. Neither checkout configures the server.
