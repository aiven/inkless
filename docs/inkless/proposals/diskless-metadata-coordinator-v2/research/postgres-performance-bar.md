# Postgres performance bar for the v2 decisions

Research for ticket [12-postgres-performance-bar](../issues/12-postgres-performance-bar.md).

## Summary

No decision in the map adds blocking or a high per-operation cost to the normal produce,
commit, or read path beyond what the Postgres backend pays. The additions on the normal
path are each constant or linear in work the commit already does:

- One extra commit part per file to the broker's tracking partition, sent in parallel
  with the other parts. That part is often empty.
- One timer reschedule per part at the tracking partition, and one lease-range check.
- One `files` index entry per commit per coordinator partition.
- One fsync per flushed batch on the coordinator leader, and one per fetched batch on
  each follower, under `flush.messages=1`.
- One topic-name comparison per classic produce, and one tagged-field check per record
  and nested struct on apply.

Everything else runs on a failover, a registration, an admin request, an upload failure,
a lease end, or a seek past the end of a partition.

The rewrite has to correct ticket 07 on these points:

- The barrier deadline fails the bar as worded. Ticket 07 passes the 500 ms deadline as
  "the `CoordinatorRuntime` write-operation timeout", but the runtime has one write timeout
  for every write. Set to 500 ms, it fails every commit part that waits longer. The
  smallest fix is a per-operation timeout, which `CoordinatorWriteEvent` already supports
  internally, or a timeout on the barrier's own future.
- The barrier wait holds a thread on the calling broker. `ControlPlane.findBatches` is
  synchronous, and the broker calls it on a fetch metadata pool of 8 threads. With the
  batch coordinate cache turned off, the delayed-fetch probe also calls it from
  `tryComplete`. The barrier runs only for a deposed leader or a seek past the end. Still,
  the probe path must never run it, and a deposed window can tie up the pool.
- A fetch above the high watermark can wait about a long poll plus 500 ms, not one long
  poll. The barrier runs after the delayed fetch's wait, not instead of it. The bound of
  one barrier per linger interval also assumes a fixed linger, and v1 leaves the linger
  default open.

The registration guard from ticket 10 runs off the path, but `lastKnownElr` has no
index. The guard has to iterate the partitions of `__diskless_metadata`, which costs
`O(N × RF)` per registration. It must not iterate every topic.

## Sources

The research uses two local checkouts. Citations use paths relative to each root.

- Inkless: `/Users/viktor.somogyi-vass/dev/github/aiven/workspace-inkless/diskless-metadata`
  at commit `2a412af730`.
- Kafka: `/Users/viktor.somogyi-vass/dev/github/apache/kafka` at commit `27d8bbc35e`.

Unless a citation says Kafka, it refers to the Inkless checkout. Citations into the map's
tickets use their paths under `.scratch/`. The coordinator runtime, timer, and log flush
code is upstream code. The Kafka checkout has the same logic at different line numbers, and
each section notes where the difference matters.

Many decisions describe code that doesn't exist yet. For those, this research cites the
existing code that the decision extends and marks cost estimates that go beyond the code as
inference.

## The Postgres baseline

`PostgresControlPlane` keeps three Hikari pools: jobs, write, and read. The write and read
pools fall back to the jobs pool when the config has no separate connection string
(`storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/PostgresControlPlane.java:111-138`).
Every connection uses `READ_COMMITTED` and `autoCommit=false` (`PostgresControlPlane.java:181`,
`:190-191`). Each job runs through `JobUtils.run`, which retries up to 3 attempts on
`40001` and `40P01` with a 50 ms to 1 s jittered backoff
(`storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/JobUtils.java:42-58`, `:78-103`).

Round trips in this section are inference from jOOQ and JDBC behavior, which neither
checkout contains: one statement plus one `COMMIT` per job.

### Commit

- The broker commits files on one thread, one file at a time
  (`storage/inkless/src/main/java/io/aiven/inkless/produce/FileCommitter.java:117`;
  `storage/inkless/src/main/java/io/aiven/inkless/produce/FileCommitJob.java:107`).
- `commitFile` runs one transaction on the write pool, with one call to `commit_file_v2`
  (`PostgresControlPlane.java:213-225`;
  `storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/CommitFileJob.java:105-148`).
- The latest `commit_file_v2` does the following
  (`storage/inkless/src/main/resources/db/migration/V28__Soft_delete_logs.sql:440-683`):
  - It inserts one `files` row (`:474-477`).
  - It copies the `logs` row of every partition in the file into a temporary table and
    locks them `FOR UPDATE`, ordered by topic and partition to avoid deadlocks
    (`:482-492`). The locks last until the transaction commits. A concurrent commit from
    another broker that writes to any of the same partitions waits for them.
  - Per idempotent batch, it runs two or three indexed `producer_state` lookups, one insert,
    and one delete that keeps the last 5 rows (`:518-597`).
  - It inserts one `batches` row per byte-contiguous run of accepted batches in a partition
    (`:613-651`; `storage/inkless/src/main/resources/db/migration/V14__Commit_file_coalescing.sql:46-83`).
  - It writes the `logs` rows back once (`:654-659`). It then fills
    `earliest_batch_timestamp` where that column is null (`:666-677`).
  - If no batch committed, it marks the file for deletion (`:679-681`).
- The Postgres WAL flush on commit depends on the server's `synchronous_commit`, and on any
  synchronous standby. The database server owns those settings, and the checkouts don't
  set them. A search of `storage/inkless` and `docker` doesn't find `synchronous_commit`.

### `findBatches`

- Every completed diskless fetch calls the control plane once. `DelayedFetch.onComplete`
  calls `fetchDisklessMessages` (`core/src/main/scala/kafka/server/DelayedFetch.scala:309-321`),
  and `Reader.fetch` runs `FindBatchesJob` on the fetch metadata executor
  (`storage/inkless/src/main/java/io/aiven/inkless/consume/Reader.java:468-478`), which calls
  `ControlPlane.findBatches`
  (`storage/inkless/src/main/java/io/aiven/inkless/consume/FindBatchesJob.java:74`).
- The job runs one transaction on the read pool, with one call to `find_batches_v2`
  (`PostgresControlPlane.java:228-239`;
  `storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/FindBatchesJob.java:84-127`).
- `find_batches_v2` is `STABLE` and takes no row locks (`V28__Soft_delete_logs.sql:687-692`).
  Per partition, it reads the `logs` row through a left join (`:701-719`). It then walks
  `batches` in `last_offset` order on the covering index and stops at the first budget
  crossing (`:737-803`). Its cost follows the result, not the partition depth.
- It answers `offset_out_of_range` when the offset is below the log start or above the high
  watermark (`:710-715`). An offset equal to the high watermark is in range and returns no
  batches.

### `listOffsets`

- The job runs one transaction on the read pool, with one call to `list_offsets_v1`
  (`PostgresControlPlane.java:241-248`;
  `storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/ListOffsetsJob.java:66-91`).
- `list_offsets_v1` takes no locks (`storage/inkless/src/main/resources/db/migration/V30__List_offsets_min_offset_lookup.sql:56`, `:69`).
  Per partition, it reads one `logs` row (`:70-75`):
  - `EARLIEST`, `EARLIEST_LOCAL`, and `LATEST` answer from that row (`:84-102`).
  - `MAX_TIMESTAMP` runs three scans of that partition's batches (`:105-135`).
  - A timestamp lookup reads the oldest batch, and when that batch doesn't match, runs an
    index-only `MIN(base_offset)` over the partition's entries and one point lookup
    (`:153-188`). The cost follows the partition's batch count, not the table (`:3-21`).

### `FileCleaner` cycle

- Every broker runs `FileCleaner` every `file.cleaner.interval.ms`, 2 minutes by default
  (`core/src/main/scala/kafka/server/ReplicaManager.scala:563-566`;
  `storage/inkless/src/main/java/io/aiven/inkless/config/InklessConfig.java:138-142`).
- One cycle runs the following steps
  (`storage/inkless/src/main/java/io/aiven/inkless/delete/FileCleaner.java:99-174`):
  - One `SELECT` of up to `file.cleaner.max.files.per.cycle + 1` rows in `deleting` state
    older than the 1-minute grace period, on the jobs pool, with no row lock
    (`storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/FindFilesToDeleteJob.java:63-83`;
    `InklessConfig.java:152-163`).
  - One object-storage delete call for the keys (`FileCleaner.java:158`).
  - One transaction that calls `delete_files_v1` for the confirmed keys, which deletes only
    rows still in `deleting` state
    (`storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/DeleteFilesJob.java:56-61`;
    `storage/inkless/src/main/resources/db/migration/V12__Remove_file_merge.sql:23-33`).
- The `SELECT` has no `FOR UPDATE` or `SKIP LOCKED`, so brokers whose cycles overlap can
  pick the same keys. Inference: a key can get more than one storage delete, and the
  second `delete_files_v1` is a no-op.
- `FileCleaner` is off the produce, commit, and read path. Marking a file for deletion runs
  inside the commit or retention transaction that empties it
  (`storage/inkless/src/main/resources/db/migration/V1__Create_tables.sql:463-475`).

## Per-decision verdicts

| Decision | Path it touches | Added per operation or event | Waits on the path | Verdict |
| --- | --- | --- | --- | --- |
| [Ownership model](../issues/01-ownership-model.md) | Commit | One parallel part to the tracking partition per file, a lease check there, a floor check at each participant, and one `files` index entry per partition. Renewal, fences, and release reports run in the background. | The commit waits for its slowest part, which now always includes the tracking partition. | Within |
| [Internal-topic protections](../issues/05-internal-topic-protections.md) | None | Research only | No | Within |
| [Deletion after a partial send](../issues/02-delete-after-partial-send.md) | Failed commit | One capability check | No | Within |
| [Time bounds](../issues/03-time-bounds.md) | Commit at the tracking partition; `FileCleaner` | One timer reschedule per part. `FileCleaner` reads local state and appends one record per partition per cycle. | No | Within |
| [Flush policy](../issues/04-flush-policy.md) | Commit | One fsync per flushed batch on the leader, and one per fetched batch on each follower | Yes, as durability | Within, with a caveat on the Postgres comparison |
| [Admin protection](../issues/06-admin-protection.md) | Classic produce; admin requests | One name comparison per produced partition | No | Within |
| [Read consistency](../issues/07-read-consistency.md) | `findBatches` above the high watermark | One barrier record per linger interval per coordinator partition | Yes, up to 500 ms, only above the high watermark | Fails as worded; within with a per-operation deadline |
| [Record compatibility](../issues/08-record-compatibility.md) | Every record write and apply | One image lookup per record built, and one tag check per struct applied | No | Within |
| [Last-known-leader election](../issues/09-last-known-leader-election.md) | None | Research only | No | Within |
| [Last-known-leader guard](../issues/10-last-known-leader-guard.md) | Controller registration and directory assignment; partition directory creation | A scan of the topic's partitions per registration, and one fsync per partition directory | No | Within, if the scan covers only the topic |

### Ownership model: intents or leases

- Every file sends a part to every participant in parallel, and one part always goes to the
  tracking partition. It's empty when the tracking partition holds none of the file's
  batches (`.scratch/diskless-metadata-coordinator/issues/03-single-partition-scope.md:97`).
  The produce waits for its slowest part (`docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md:340`).
  So the lease model doesn't add a serial step, but it adds the tracking partition to every
  file's set of parts.
- Only the tracking partition checks the lease range. Participants check only the broker
  row's floor (`.scratch/diskless-metadata-coordinator/issues/05-sequence-leases.md:128`).
  The `files` table gains an index on uploader broker, epoch, and sequence
  (`.scratch/diskless-metadata-coordinator/issues/04-broker-sequence-model.md:150`). Each is
  a constant or logarithmic step per part, next to the producer-state checks that
  `commit_file_v2` already runs per batch.
- The broker renews its lease in the background at \(K/2 = 512\) sequences left
  (`05-sequence-leases.md:45`, `:52`). That's one replicated append per 512 files.
- Fan-out runs only on failure. A missed sequence fences all `N` partitions, and a lease end
  sends one range fence to each partition (`03-single-partition-scope.md:101-102`). A missed
  sequence comes from an upload failure or a failed part to the tracking partition.
- Release reports and release states are per object and participant
  (`.scratch/diskless-metadata-coordinator/issues/06-object-deletion.md:88-92`). They run when
  retention, consolidation, or deletion empties a participant, not on commit. Postgres
  marks an emptied file inside the same transaction
  (`V1__Create_tables.sql:463-475`). Inference: the background record count grows with files
  times participants, which is capacity work that the map puts out of scope.
- Postgres also blocks here. Commits that write to the same partitions serialize on the
  `logs` row locks (`V28__Soft_delete_logs.sql:482-492`), and each broker commits on one
  thread (`FileCommitter.java:117`).

### Broker-side deletion after a partial send

The capability check runs in `FileCommitJob.tryDeleteFile`, before the
`ControlPlaneUnavailableException` branch
(`.scratch/diskless-metadata-coordinator-v2/issues/02-delete-after-partial-send.md:31`;
`FileCommitJob.java:124-137`). It runs only for a failed file. Its cost is storage held until
the lease end and the grace period (`02-delete-after-partial-send.md:33`).

### Time bounds in the chosen model

- The idle lease timer reschedules on every part and lease request that names the tracking
  partition (`.scratch/diskless-metadata-coordinator-v2/issues/03-time-bounds.md:38`). With
  one part per file at the tracking partition, that's one reschedule per file. See
  [Idle lease timer](#idle-lease-timer) for its cost.
- The grace period keeps Postgres's wall-clock comparison (`03-time-bounds.md:30`, `:52`).
- `FileCleaner` runs only on tracking-partition leaders
  (`03-single-partition-scope.md:111`). `getFilesToDelete` reads the local queues of the
  partitions this broker leads, and `deleteFiles` appends one files-deleted record per
  partition (`06-object-deletion.md:91`). That's one replicated, fsynced append per
  partition per cycle, off the path. Unlike the Postgres `SELECT`, no two brokers read the
  same queue.

### Flush policy under Kafka parity

On the commit path. The section [`flush.messages=1` and append batching](#flushmessages1-and-append-batching)
gives the cost.

### Admin protection for `__diskless_metadata`

- The produce guard goes next to the internal-topic check in `ReplicaManager.appendToLocalLog`
  (`.scratch/diskless-metadata-coordinator-v2/issues/06-admin-protection.md:36`). That check
  is a `Set.contains` on the topic name per partition
  (`core/src/main/scala/kafka/server/ReplicaManager.scala:2096`;
  `clients/src/main/java/org/apache/kafka/common/internals/Topic.java:37`, `:74`). The guard
  adds one string comparison. Diskless partitions skip both `ReplicaManager` checks
  (`.scratch/diskless-metadata-coordinator-v2/issues/05-internal-topic-protections.md:33`).
- The config, `DeleteTopics`, `CreateTopics`, `CreatePartitions`, and `ElectLeaders` guards
  run only on admin requests (`06-admin-protection.md:35-40`, `:51`).

### Read consistency after a leader change

The normal read path doesn't change. The barrier is the only cost, and the section
[The barrier above the high watermark](#the-barrier-above-the-high-watermark) covers it.
The decision fails the bar as worded because of its deadline mechanism, not because of
the barrier itself.

### Record compatibility across broker versions

- The leader reads the finalized level from its `MetadataImage` once per record it builds
  (`.scratch/diskless-metadata-coordinator-v2/issues/08-record-compatibility.md:56`). That's
  a constant lookup.
- Apply checks the tagged fields of each struct. See
  [Tag registry check on apply](#tag-registry-check-on-apply).
- The downgrade refusal runs only on a feature update (`08-record-compatibility.md:65`).
  A replica reads the SQLite schema version only when it opens a database or snapshot
  (`08-record-compatibility.md:100`).

### Last-known-leader guard

Off the path. The section [The registration-time scan](#the-registration-time-scan) gives
the cost of each rule.

## The barrier above the high watermark

### How often a consumer reaches it

A consumer that keeps up never reaches it under a current leader:

- `find_batches_v2` returns only batches whose base offset is below the high watermark
  (`V28__Soft_delete_logs.sql:769`). A consumer's next offset is therefore at or below the
  high watermark it saw.
- An offset equal to the high watermark is in range (`V28__Soft_delete_logs.sql:712-713`).
  v1 keeps the `findBatches` response unchanged
  (`docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md:277`).
- A current leader's high watermark only rises. So a consumer that polls at the end of the
  log fetches at the high watermark, gets no batches, and doesn't reach the barrier.

Three cases reach it:

- A deposed leader. A consumer whose position advanced under the new leader is above the
  deposed leader's high watermark. Each broker routes a part to the leader in its own
  metadata cache (`design.md:60`). Every fetch that a stale broker routes to the deposed
  leader reaches the barrier until that broker learns of the new leader. That includes
  every fetch the deposed leader serves itself.
- A genuine seek past the end. That happens once per seek.
- A client that keeps fetching past the end, for example with `auto.offset.reset=none` and an
  application that retries. Ticket 07 bounds this case at one barrier per linger interval
  per coordinator partition
  (`.scratch/diskless-metadata-coordinator-v2/issues/07-read-consistency.md:60`).

A deposed window opens at every coordinator leader change while the old leader still runs.
That includes the periodic preferred election, every 300 seconds by default when leaders
are imbalanced (`server/src/main/java/org/apache/kafka/server/config/ReplicationConfigs.java:119`, `:148`),
a controlled shutdown, and a reassignment. If the old leader stays connected, the window
ends when it applies the new leader's record and the shard unloads. Parked requests then
get `NOT_COORDINATOR` at once (`07-read-consistency.md:55`). If the old leader is cut off
from the controller, the window lasts until it reconnects, because the broker doesn't step
down on its own (`07-read-consistency.md:27`).

### What one barrier costs

On the coordinator partition and its replicas:

- The barrier is a record in the current batch. If a batch is open, it adds no append.
  Otherwise it opens one, and the flush runs one `partitionWriter.append` on the event
  thread
  (`coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java:693-698`).
  Under `flush.messages=1`, that's one leader fsync and one fsync per follower, as in
  [`flush.messages=1` and append batching](#flushmessages1-and-append-batching).
- The write parks as a deferred event until the high watermark passes it
  (`CoordinatorRuntime.java:1275-1281`, `:1750-1771`). Every replica applies the barrier by
  advancing its applied offset only (`07-read-consistency.md:100`).
- A deposed leader appends barriers that never commit. They stay in its local log until the
  log truncates on the next fetch from the new leader. Each costs a local fsync.

On the calling broker:

- `ControlPlane.findBatches` returns a `List`, so the caller blocks
  (`storage/inkless/src/main/java/io/aiven/inkless/control_plane/ControlPlane.java:53`).
  `Reader` calls it on the fetch metadata pool
  (`Reader.java:228`, `:468-478`), which has 8 threads by default
  (`InklessConfig.java:194-202`). One parked part holds one of those threads for the whole
  wait.
- The delegate merges parts in request order (`design.md:330`), so the barrier delays the
  whole fetch response, including partitions on other coordinator partitions.
- Inference: in a deposed window, 8 parked requests occupy the pool for 500 ms each. That
  caps the broker's diskless fetches at about 16 per second, for every coordinator
  partition. The Postgres backend blocks longer when the database is unreachable, up to its
  5-second socket timeout per attempt
  (`storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/PostgresConnectionConfig.java:53`).
- With the batch coordinate cache disabled, the delayed-fetch probe also calls
  `findBatches` directly (`ReplicaManager.scala:2386-2389`;
  `DelayedFetch.scala:186-199`). The probe runs in `tryComplete`, which
  `tryCompleteElseWatch` calls on the request thread (`ReplicaManager.scala:2898`). A
  barrier there would block that thread for up to 500 ms. The cache is on by default
  (`InklessConfig.java:106`).

### The 500 ms deadline against a fetch's long poll

- The client's default `fetch.max.wait.ms` is 500 ms (Kafka
  `clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java:235`). The
  broker raises a diskless fetch's wait to at least `diskless.fetch.max.wait.ms`, 100 ms by
  default (`server-common/src/main/java/org/apache/kafka/server/config/ServerConfigs.java:117-118`;
  `ReplicaManager.scala:2864`).
- Every diskless fetch parks in `DelayedFetch` (`ReplicaManager.scala:2876-2898`, `:2901-2903`).
  The default probe reads only the local cache (`ReplicaManager.scala:2390-2412`). A local hit
  above the high watermark completes the fetch at once (`DelayedFetch.scala:224-227`). A miss,
  which every cross-broker partition gets, waits out the long poll.
- The barrier runs in `onComplete`, after that wait (`DelayedFetch.scala:250`, `:309-321`).
  A fetch above the high watermark can therefore take about the long poll plus 500 ms,
  about 1 second at client defaults. Ticket 07 says it "waits about as long as a long poll"
  (`07-read-consistency.md:55`). That holds only for a local cache hit.
- Ticket 07 passes the 500 ms "as the `CoordinatorRuntime` write-operation timeout"
  (`07-read-consistency.md:55`). The runtime has one write timeout, set on its builder
  (`CoordinatorRuntime.java:162-165`, `:1817`). `scheduleWriteOperation` passes that value
  to every write event (`CoordinatorRuntime.java:2058-2072`; Kafka `CoordinatorRuntime.java:2144-2158`).
  A 500 ms runtime timeout would fail every commit part whose high watermark takes longer,
  with a `TimeoutException` from `OperationTimeout` (`CoordinatorRuntime.java:1061-1081`).
  The upstream coordinators use seconds here: the group coordinator passes
  `offsets.commit.timeout.ms`, 5000 by default
  (`group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java:281`;
  `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorConfig.java:110`).
- `CoordinatorWriteEvent` already takes its own timeout in its constructor
  (`CoordinatorRuntime.java:1191-1206`, `:1221-1241`, `:1277`). A public overload of
  `scheduleWriteOperation` that passes a `Duration` exposes it. That's an upstream file, and
  the change has no effect on existing callers. A caller-side timeout on the barrier's
  future is the alternative that leaves upstream unchanged.
- The bound of one barrier per linger interval depends on the linger setting. v1 leaves the
  append linger default open (`design.md:569`). With a fixed linger, a flush runs at most
  once per interval (`CoordinatorRuntime.java:791-806`). With the adaptive linger that the
  group and share coordinators default to, a flush event joins the back of the queue as
  soon as a batch opens (`CoordinatorRuntime.java:807-814`, `:657-668`;
  `GroupCoordinatorConfig.java:88-93`;
  `share-coordinator/src/main/java/org/apache/kafka/coordinator/share/ShareCoordinatorConfig.java:75`).
  Inference: under adaptive linger, a client that keeps fetching past the end can cost one
  replicated, fsynced append per flush cycle, which is bounded only by fsync latency.

### What Postgres answers for the same offset

- Postgres answers `OFFSET_OUT_OF_RANGE` in the same transaction, with the log start and
  the high watermark (`V28__Soft_delete_logs.sql:710-715`, `:721-731`; `FindBatchesJob.java:114-116`;
  `storage/inkless/src/main/java/io/aiven/inkless/control_plane/FindBatchResponse.java:36-37`).
  It doesn't wait.
- The baseline has no deposed leader, but it can read from a separate pool
  (`PostgresControlPlane.java:131-138`;
  `storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/PostgresControlPlaneConfig.java:26`, `:84-86`).
  `JobUtils` retries recovery conflicts and refers to retries "against the same standby"
  (`JobUtils.java:48-58`). Inference: if `read.*` points at a lagging hot standby, Postgres
  answers a false `OFFSET_OUT_OF_RANGE` for an offset above the standby's high watermark,
  with no barrier. The sources don't show whether deployments do this.

## The registration-time scan

### What `registerBroker` iterates

- `QuorumController.registerBroker` runs as a write event on the controller event thread
  (`metadata/src/main/java/org/apache/kafka/controller/QuorumController.java:2147-2162`).
- `ClusterControlManager.registerBroker` iterates the request's directory list
  (`metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java:382-400`).
  On a new incarnation, it calls the shutdown handler in the same event
  (`ClusterControlManager.java:438-442`). The handler routes to
  `ReplicationControlManager.handleBrokerShutdown` (`QuorumController.java:2339-2340`), which
  iterates every partition where the broker is in the ISR, and with ELR on and an unclean
  shutdown, every partition where it's in the ELR
  (`metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:2279-2291`).
  That existing cost grows with the broker's partitions across every topic.
- The controller indexes ISR and ELR membership by broker and topic
  (`metadata/src/main/java/org/apache/kafka/controller/BrokersToElrs.java:33-34`;
  `metadata/src/main/java/org/apache/kafka/controller/BrokersToIsrs.java:277`).
  `lastKnownElr` has no index. It lives only in each `PartitionRegistration`.

### What the guard needs to iterate

- The guard has to find every `__diskless_metadata` partition where the broker is in the ELR
  or in `lastKnownElr`, and compare the replica's assigned directory with the new
  registration (`.scratch/diskless-metadata-coordinator-v2/issues/10-last-known-leader-guard.md:34`).
- Without an index for `lastKnownElr`, the cheapest path is the topic's own partitions:
  `topicsByName` to the topic ID, then `topics` to its `parts` map
  (`ReplicationControlManager.java:340-348`, `:447`, `:469`, `:1420`). That costs
  `O(N × RF)`, where `N` is 8 by default and at most 4096
  (`design.md:36`, `:47-49`). It doesn't grow with the cluster's partition count.
- A cluster-wide pass would grow with the cluster, for example iterating `topics.values()`
  as `replayClearAllElrs` does (`ReplicationControlManager.java:733-738`), or the
  `partitionsWithNoLeader` set that `handleBrokerUnfenced` scans
  (`ReplicationControlManager.java:2195-2196`).
- The guard runs on the controller event thread, in the registration event. It runs once
  per broker process start, because each process generates a new incarnation ID
  (`server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java:101`) and the
  handler runs only on a new incarnation (`ClusterControlManager.java:438`).

### `handleAssignReplicasToDirs`

- `handleAssignReplicasToDirs` iterates only the partitions named in the request, and
  builds one `PartitionChangeBuilder` for each (`ReplicationControlManager.java:3243-3310`).
  It runs as a write event on the controller event thread (`QuorumController.java:2288-2292`).
- The guard adds a constant check per requested `__diskless_metadata` partition. It doesn't
  grow with any partition count beyond the request.
- A broker sends the request when it creates or moves a replica, so it's off the path.

### The other rules in ticket 10

- ELR across an unclean shutdown adds a name branch to the existing per-partition loop in
  `handleBrokerShutdown` (`10-last-known-leader-guard.md:35`; `ReplicationControlManager.java:2279-2291`).
- The cluster-wide ELR clear adds a name check per topic to a replay that already visits
  every topic (`10-last-known-leader-guard.md:40`; `ReplicationControlManager.java:733-738`).
  The controller writes that record only when `min.insync.replicas` changes
  (`metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java:226-246`).
- The fixed replication factor and minimum ISR checks run on admin requests only
  (`10-last-known-leader-guard.md:36-39`).
- The directory fsync is one `Utils.flushDir` per partition directory creation
  (`10-last-known-leader-guard.md:41`; `core/src/main/scala/kafka/log/LogManager.scala:1116-1122`;
  `clients/src/main/java/org/apache/kafka/common/utils/Utils.java:963`).

## `flush.messages=1` and append batching

### Leader

- A flushed batch is one `partitionWriter.append` call with one `MemoryRecords`
  (`CoordinatorRuntime.java:693-698`). The call is synchronous on the event thread. The
  upstream writer appends through `ReplicaManager.appendRecordsToLeader` in the same call
  (`core/src/main/scala/kafka/coordinator/group/CoordinatorPartitionWriter.scala:137-148`).
  The Inkless writer appends with `acks=-1` (`design.md:118`).
- `UnifiedLog.append` holds the log's lock and flushes when the unflushed message count
  reaches `flush.messages` (`storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java:1137`,
  `:1286`; Kafka `UnifiedLog.java:1300`). Under `flush.messages=1`, every flushed batch
  costs one fsync, on the event thread, under the lock that the high watermark update also
  takes.
- The runtime flushes the open batch in these cases:
  - Fixed linger: a timer task puts a flush at the front of the queue once the linger
    elapses (`CoordinatorRuntime.java:791-806`), and each append checks the linger too
    (`:735-745`).
  - Adaptive linger: a flush event joins the back of the queue when a batch opens, so the
    batch takes every write already queued (`:807-814`, `:657-668`).
  - Full batch: the next record doesn't fit, with the topic's maximum message size as the
    bound (`:741`, `:769`, `:917-929`, `:941-958`).
  - Transactional write: the batch flushes at once (`:737`, `:884-889`). Coordinator records
    aren't transactional.
- Events for one shard never run concurrently (`CoordinatorRuntime.java:1248-1251`). Inference:
  while a shard's fsync runs, its next writes queue, and the next flush takes them all. That
  is group commit. One shard flushes at most once per fsync latency, and its batch size
  grows with load.
- Inference: runtime threads are shared across shards. The group coordinator uses 4 by
  default (`GroupCoordinatorConfig.java:97`; `GroupCoordinatorService.java:256`). A shard's
  fsync holds its thread, so other shards on that thread wait.

### Followers

- A follower appends each fetched chunk through the same `append` and flushes before it
  computes its next fetch offset (`core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:155`;
  `core/src/main/scala/kafka/server/AbstractFetcherThread.scala:377-395`). That's at most one
  fsync per leader batch.
- A fetcher thread serves several partitions from one leader, chosen by a hash of topic and
  partition (`core/src/main/scala/kafka/server/AbstractFetcherManager.scala:111-113`), and
  `num.replica.fetchers` is 1 by default (`ReplicationConfigs.java:95-96`). Inference: a
  coordinator partition's fsync delays classic partitions on the same fetcher thread. That's
  co-tenancy, which v1's [Capacity and isolation](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#capacity-and-isolation)
  covers and the map puts out of scope.

### Comparison with a Postgres commit

- A coordinator commit reply waits for the high watermark (`CoordinatorRuntime.java:1750-1771`).
  Inference: that's one leader fsync, a follower fetch round trip, one follower fsync, and
  the next follower fetch. SQLite applies with `synchronous=NORMAL` in WAL mode, so the apply
  adds no fsync (`design.md:274-275`).
- A Postgres commit is one transaction with two client round trips (inference) and one WAL
  flush on the primary under the server's default settings. A synchronous standby would add
  a second flush and a network hop. Neither checkout shows the server configuration.
- Ticket 04's claim, "one `LogSegment.flush()` per linger batch on the leader", matches the
  code (`.scratch/diskless-metadata-coordinator-v2/issues/04-flush-policy.md:29`). Its
  "matches the Postgres backend" holds for the number of flushes per batch on the primary.
  The follower fsync is a second, sequential flush per commit, which Postgres matches only
  with a synchronous standby. Ticket 04 requires that flush for Kafka parity, so it isn't
  unnecessary blocking. Measuring the latency is the benchmark question that the map puts
  out of scope.

## Other per-operation costs

### Idle lease timer

- `CoordinatorTimerImpl.schedule` allocates a `TimerTask`, replaces the key's entry in a
  `ConcurrentHashMap`, cancels the previous task, and adds the new one to the timer
  (`coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorTimerImpl.java:70-142`).
- `SystemTimer.add` takes a read lock and inserts into the timing wheel
  (`server-common/src/main/java/org/apache/kafka/server/util/timer/SystemTimer.java:67-74`).
  `TimerTask.cancel` unlinks the entry (`server-common/src/main/java/org/apache/kafka/server/util/timer/TimerTask.java:28-31`).
  Each is a constant-time step.
- The group coordinator reschedules a member's session timeout on every heartbeat with the
  same call (`group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java:2304`,
  `:2438`, `:4685-4697`).
- With one part per file at the tracking partition, the cost is one reschedule per file per
  broker. That's within the bar.

### Apply-time tag registry check

- Apply checks `unknownTaggedFields()` of each record and nested struct
  (`08-record-compatibility.md:79`). The generated getter allocates an empty list when the
  field is null (`generator/src/main/java/org/apache/kafka/message/MessageDataGenerator.java:337-347`).
  The field stays null unless the reader met an unknown tag (`MessageDataGenerator.java:475`, `:524`).
- A file-committed record holds a struct per batch. Inference: the check is linear in the
  record's batches on every replica, the same order as the SQLite rows the apply already
  writes. The getter's allocation is the only overhead, and it's small. That's within the bar.

### Costs that stay off the path

These costs run only on rare events:

- Lease renewal: one append per 512 files, ahead of need (`05-sequence-leases.md:52`).
- Missed-sequence and range fences: `N` appends per upload failure, failed tracking part,
  or lease end (`03-single-partition-scope.md:101-102`).
- Release reports: one per object and participant, when retention, consolidation, or
  deletion empties that participant (`06-object-deletion.md:88-92`).
- The new-leader gate: one wait per election until the high watermark reaches the log end
  seen at election (`design.md:383`).

## Implications for the design

The rewrite or the cross-cutting pass needs to settle these points:

- The barrier deadline. As worded, ticket 07 sets the runtime's single write timeout to
  500 ms, which applies to every commit part. The smallest change is a per-operation
  timeout for the barrier write, through a `scheduleWriteOperation` overload that passes its
  own `Duration` to `CoordinatorWriteEvent`. A timeout on the barrier's future is the
  alternative. The runtime's write timeout for commits then needs its own value.
- The thread that waits for the barrier. The delayed-fetch probe must never run the barrier,
  because with the cache disabled it calls `findBatches` from `tryComplete`. On the
  `Reader` path, a parked barrier holds a fetch metadata thread for up to 500 ms. The map
  lets `ControlPlane` change (`.scratch/diskless-metadata-coordinator-v2/map.md:11`). An
  asynchronous `findBatches` would remove the hold, at the cost of an interface change for
  every backend.
- The fetch latency above the high watermark. Ticket 07's "about as long as a long poll"
  becomes "up to the long poll plus 500 ms" for a fetch that misses the local cache.
- The barrier rate. The bound of one barrier per linger interval assumes a fixed linger. If
  the runtime uses adaptive linger, a fixed window for sharing a barrier keeps the bound.
  The append linger default is still an open v1 item.
- The registration guard's scope. It must iterate only the partitions of
  `__diskless_metadata`, because `lastKnownElr` has no index and a cluster-wide pass would
  grow with the cluster.
- Ticket 04's comparison. The flush count per batch matches Postgres. The follower fsync adds
  a second sequential flush per commit, which Postgres matches only with a synchronous
  standby.

The sources don't settle these points:

- The Postgres server's `synchronous_commit`, its standby mode, and whether `read.*` points
  at a standby. Neither checkout configures them.
- Fsync latency, replication round-trip time, and the resulting commit latency. These need
  the benchmark that the map puts out of scope.
- Client round trips per Postgres job. They depend on jOOQ and pgjdbc, which neither
  checkout contains.
