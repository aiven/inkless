# Consolidated remote data is unreadable after a local-log loss

## Summary

For a consolidating diskless topic (`diskless.enable=true` + `remote.storage.enable=true`),
once data has been consolidated to the classic remote tier and the diskless WAL
has been pruned, a broker that loses its **local** copy of the partition can no
longer serve the consolidated records — even though they are still durable in
object storage.

After the local log is gone and the broker restarts, the partition's **earliest
offset** comes back as the diskless control-plane `log_start_offset` (the start
of the *diskless* interval) instead of `0` (the true start of the log). The
records below that offset were moved to the classic remote tier, are still
present in MinIO/S3 and still referenced by `__remote_log_metadata`, yet a
consumer reset to `earliest` only sees the un-pruned WAL tip. The consolidated
data is effectively unreadable.

This is a **durability gap**: data that the system reported as safely tiered to
remote becomes inaccessible after a recoverable, local-only failure.

> **Correction (2026-06-15, session `--015`) — read this first.**
>
> Earlier revisions of this doc (the "Fix" / "Status" / "Why Part 2" sections
> below) assumed the rehydration goes through the consolidation fetcher's
> **`truncateFullyAndStartAt`** out-of-range reset, and that "Part 1" (preserving
> a lower `logStartOffset` in that override) is what made `earliest` report `0`.
> The system-test logs disprove that:
>
> - On the wiped leader there is **no `truncateFullyAndStartAt` call** for the
>   partition at all. The only resets are `truncateTo(0)` / `truncateTo(300000)`,
>   both logged as *"has no effect"* no-ops.
> - `initialFetchOffset(log)` returns `log.highWatermark` = **0** for the fresh
>   empty log (`ReplicaManager.scala:2698-2703`), so the consolidation fetcher
>   arms at offset 0 and simply **appends** the WAL batches forward from the
>   diskless start (231615) into the fresh base-0 segment.
> - Resulting on-disk state: segment file `00000000000000000000.log` (base 0),
>   `leader-epoch-checkpoint = "0 231615"`, `logStartOffset = 0`. So `earliest`
>   reads `0` simply because the wiped log's default `logStartOffset` was never
>   advanced — **not** because of the Part 1 override.
>
> Consequence: the `ConsolidationFetcherThread.truncateFullyAndStartAt` override
> (Part 1 + Part 2) is **dead code in this rehydration path** — it is never
> invoked, which is exactly why two consecutive fixes (sessions `--014` and
> `--015`) changed nothing. The real fix must move the trigger into the
> **seeding / reconciliation path** (`ConsolidationReconciler`), where the
> fetcher's start offset for a wiped born-consolidated partition is chosen. See
> the dated section *"Correction: the reset path is `truncateTo`, not
> `truncateFullyAndStartAt`"* below for the full analysis. The sections between
> here and there are retained for history but are superseded on this point.
>
> One incidental correction: the **brokers** do run the real Aiven tiered-storage
> RSM (`io.aiven.kafka.tieredstorage.RemoteStorageManager`) +
> `TopicBasedRemoteLogMetadataManager`; only the isolated **controller** uses the
> NoOp managers. So `TierStateMachine.buildRemoteLogAuxState` is a valid mechanism
> here — it just needs to be triggered.

## Severity

- **Data is not lost** (it remains in object storage), but it is **inaccessible**
  after a local-log loss until/unless the read path is fixed.
- Triggered by any event that drops a partition's local log while the remote
  tier and control plane survive: disk loss, replacing a broker's data volume,
  moving a replica to a fresh broker, etc.

## Affected configuration

- Topic: `diskless.enable=true`, `remote.storage.enable=true` (a "consolidating"
  diskless topic).
- The diskless WAL has been pruned at least once (so the diskless
  `log_start_offset` has advanced above `0`).

## Root cause

There are **two distinct "log start" notions** for a consolidating topic, and the
read/recovery path conflates them.

1. **Diskless `log_start_offset` (Postgres control plane).**
   This tracks the start of the **diskless interval** only — i.e. the oldest
   offset that still lives in the diskless WAL. When the WAL pruner runs, it
   advances this value up to `highestOffsetInRemoteStorage + 1`, because those
   offsets have been handed off to the classic remote tier and no longer need to
   live in the WAL. **This is correct.**

   The pruner computes the prune point from the highest remotely-stored offset:

   ```scala
   // core/.../consolidation/ConsolidatedDisklessLogPruner.scala
   val highestRemoteOffset = log.highestOffsetInRemoteStorage
   ...
   safePruneOffset(partition, seal, highestRemoteOffset).map { safeHighestRemoteOffset =>
     new PruneDisklessLogsRequest(topicIdPartition, safeHighestRemoteOffset)
   }
   ```

   and the control plane advances `log_start_offset` to `highestRemote + 1`:

   ```java
   // storage/inkless/.../control_plane/InMemoryControlPlane.java
   final long newLogStart;
   if (coordinates.isEmpty()) {
       newLogStart = Math.min(highWatermark, Math.max(highestRemote + 1, logStartBefore));
   } else {
       final long firstRemainingBaseOffset = coordinates.firstEntry().getValue().batchInfo().metadata().baseOffset();
       newLogStart = Math.max(logStartBefore, firstRemainingBaseOffset);
   }
   logInfo.logStartOffset = newLogStart;
   ```

2. **True log start (the partition / read path).**
   This is the oldest offset readable from anywhere — local log, classic remote
   tier, or diskless WAL. For a topic where nothing was ever deleted, this is
   `0`. In steady state the broker's *local* `UnifiedLog` keeps `logStartOffset = 0`
   and the leader-epoch lineage, and reads of `[logStartOffset, localLogStartOffset)`
   are routed to remote — so reads from `0` work and the diskless prune floor is
   irrelevant to the read path.

The bug: when the local log is **destroyed** and the broker restarts, the
control plane (which was not wiped) still reports `log_start_offset = diskless
start` (e.g. `232414`), and the rehydration path adopts that value as the
partition's true log start.

The exact mechanism is in the consolidation fetcher's out-of-range reset. After
the wipe the broker creates a fresh, empty local log (`logStartOffset = 0`). The
[`ConsolidationFetcherThread`](../../core/src/main/scala/io/aiven/inkless/consolidation/ConsolidationFetcherThread.scala)
(a `ReplicaFetcherThread` whose "leader" is the diskless WAL) starts fetching at
offset `0`, which is below the diskless WAL's earliest offset (`232414`). The base
`AbstractFetcherThread.fetchOffsetAndTruncate` therefore resets the local log to
the leader's earliest:

```scala
// AbstractFetcherThread.fetchOffsetAndTruncate
val offsetAndEpoch = leader.fetchEarliestOffset(topicPartition, currentLeaderEpoch) // diskless earliest = 232414
...
truncateFullyAndStartAt(topicPartition, leaderStartOffset)
```

`ReplicaFetcherThread.truncateFullyAndStartAt` calls
`partition.truncateFullyAndStartAt(offset, isFuture = false)` with an **empty**
`logStartOffsetOpt`, and `UnifiedLog` then sets the whole-log start to that
offset:

```java
// UnifiedLog.truncateFullyAndStartAt
logStartOffset = logStartOffsetOpt.orElse(newOffset);   // empty -> newOffset (232414)
if (remoteLogEnabled()) localLogStartOffset = newOffset; // 232414
```

So `logStartOffset` is raised from `0` to `232414`, dropping the consolidated
remote prefix `0 .. 232413` below the log start, where it can no longer be read.

In short: **the diskless `log_start_offset` is being treated as the start of the
whole log, when it is only the start of the diskless interval.** The true log
start should be preserved/reconstructed from the remote tier (the remote log
start / retention point), not raised to the diskless prune floor.

## Observed evidence

From a system-test run that produced 300,000 records, waited for tiering + WAL
prune, then `rm -rf`'d the topic's partition dirs on all brokers and restarted:

- Control plane after prune: `min_log_start_offset = 232414`
  (`= highestOffsetInRemoteStorage + 1`); the tiered-storage object count grew,
  confirming `0 .. 232413` are in the classic remote tier.
- Broker on restart: `Created log for partition ...-0 ... with initial high
  watermark 0` — a fresh, empty local log; no RemoteLogManager re-attach for the
  partition.
- Consumer reset to `earliest`: first consumed batch begins at **`minOffset
  232414`**, not `0`. It read only the un-pruned WAL tip (`232414 .. 299999`,
  ~67,586 records) and could never reach the full 300,000.
- A comparison run that did **not** wipe the local log (see
  `ConsolidationPipelineTest`) read the same dataset back from `minOffset 0` to
  `offset 300000` — all records — confirming the consolidated data is readable
  while the local log survives, and only becomes unreadable after the local-log
  loss.

## Expected behaviour

After a local-log loss and restart, a consolidating topic's partition should
reconstruct its **true** log start from the remote tier so that:

- the partition's earliest offset reflects the oldest remotely-stored record
  (`0`, when nothing was deleted), and
- a consumer reset to `earliest` reads every durable record, served from remote
  for the offsets below the diskless start and from the WAL/local log above it.

The diskless `log_start_offset` should continue to mark only the diskless
interval and must not be used as the lower bound for reads.

## Correction: the reset path is `truncateTo`, not `truncateFullyAndStartAt` (session `--015`)

The "Root cause" mechanism above (an out-of-range
`fetchOffsetAndTruncate → truncateFullyAndStartAt(diskless-start)` that raises
`logStartOffset`) is **not** what the logs show for the wiped-and-restarted
born-consolidated partition. The real sequence on the leader broker is:

```
14:30:50,868  Truncating to 0 has no effect as the largest offset in the log is -1
14:31:07,476  Truncating to 300000 has no effect as the largest offset in the log is 299999
```

1. After the wipe the broker creates a fresh, empty local log
   (`logStartOffset = 0`, LEO/HW = 0, largest offset = -1).
2. `ConsolidationReconciler.reconcileSwitchedConsolidatingDisklessPartition`
   takes the born-diskless branch (`NO_CLASSIC_TO_DISKLESS_START_OFFSET`) and
   arms the fetcher with `initialFetchOffset(log)`, which is `log.highWatermark`
   = **0** when there is no epoch cache (`ReplicaManager.scala:2698-2703`).
3. The consolidation fetcher fetches the diskless WAL from offset 0. The WAL's
   earliest is the diskless start (231615, post-prune). Rather than going
   out-of-range and triggering `truncateFullyAndStartAt`, the records are
   **appended forward** into the fresh base-0 segment, with `truncateTo` reduced
   to a no-op at both ends.

The resulting on-disk state (collected partition dir):

```
00000000000000000000.log        # segment base offset 0
leader-epoch-checkpoint:
  0
  1
  0 231615                       # epoch 0 begins at 231615; nothing for [0,231615)
```

So:

- `logStartOffset = 0` (never advanced) → `ListOffsets(earliest)` returns `0`.
- The local segment's base is `0`, but it physically holds **no records below
  231615** — `[0, 231615)` is a hole presented as local.
- A fetch at offset 0 reads that base-0 local segment and returns its first real
  record (231615). For a consolidating topic, `ReplicaManager.fetchMessages`
  routes the read to the local `UnifiedLog` whenever `fetchOffset < logEndOffset`
  (`ReplicaManager.scala:2050-2057`), so the consolidated remote prefix is never
  consulted.

**Why both prior fixes were inert.** Part 1 and Part 2 live entirely inside the
`ConsolidationFetcherThread.truncateFullyAndStartAt` override, and that method is
never called in this path. The brokers ran the new jar (verified:
`rebuildAuxState` is present in the deployed jar and `tryRebuild` in the deployed
`ConsolidationFetcherThread.class`), yet **no** rebuild log markers and **no**
`truncateFullyAndStartAt` lines appear for the partition.

**Where the fix actually belongs (implemented: Approach B).** The trigger must be
**retryable**, because the rebuild (`buildRemoteLogAuxState`) requires the
`RemoteLogManager` to be ready (`rlm.isPartitionReady`), which races broker
restart. A one-shot trigger (in the reconciler or `addPartitions`) can fire too
early and fall back permanently. The robust, low-churn fix reuses the **stock,
retrying** tiered-storage recovery path:

`DisklessLeaderEndPoint.fetch` now returns `OFFSET_MOVED_TO_TIERED_STORAGE` when
the consolidation fetcher requests an offset that is below the diskless WAL start
(`data.logStartOffset`) but at/above the whole-log `logStartOffset` — i.e. an
offset in the consolidated remote prefix `[logStartOffset, disklessStart)`. The
wiped fetcher is armed at `highWatermark = 0`, so it requests offset 0 < 231615
and this fires immediately. The stock
`AbstractFetcherThread.handleOffsetsMovedToTieredStorage → TierStateMachine.start
→ buildRemoteLogAuxState` then:

1. truncates/seeds the local log at the diskless start
   (`localLogStartOffset = 231615`) so reads below it route to remote,
2. sets `logStartOffset` from the fetch response (the local `0`), and
3. rebuilds the leader-epoch cache + producer snapshot for `[0, 231615)` from the
   remote tier,

retrying on each fetch cycle (`RemoteStorageNotReadyException` is retriable) until
the RLM is ready. No new reconciler/`addPartitions` plumbing is needed; the
earlier `ConsolidationFetcherThread.truncateFullyAndStartAt` override and the
`TierStateMachine.rebuildAuxState` helper (the inert Approach-A code) were
reverted.

See the revised "Approach A/B" discussion in
[`CONSOLIDATION_REMOTE_AUX_STATE_REBUILD_DESIGN.md`](./CONSOLIDATION_REMOTE_AUX_STATE_REBUILD_DESIGN.md).

---

The original (now-superseded) "Fix" write-up follows, retained for history.

## Fix

`ConsolidationFetcherThread` now overrides `truncateFullyAndStartAt` so that
resetting a consolidating partition to the diskless WAL earliest **does not raise
the whole-log `logStartOffset`** above the consolidated remote prefix. It passes
the existing (lower) `logStartOffset` through to
`Partition.truncateFullyAndStartAt(..., logStartOffsetOpt)` instead of the empty
default:

```scala
// core/.../consolidation/ConsolidationFetcherThread.scala
override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
  val partition = replicaMgr.getPartitionOrException(topicPartition)
  val preservedLogStartOffset: Optional[JLong] =
    partition.log.map(_.logStartOffset).filter(_ < offset) match {
      case Some(logStart) => Optional.of(JLong.valueOf(logStart))
      case None => Optional.empty[JLong]()
    }
  partition.truncateFullyAndStartAt(offset, isFuture = false, preservedLogStartOffset)
}
```

After the reset this yields `localLogStartOffset = offset` (the diskless WAL
earliest, where the local log resumes) and `logStartOffset = 0` (the true,
remote-backed start), so reads of `[logStartOffset, localLogStartOffset)` route
to the classic remote tier. The override only ever keeps an *already-lower*
`logStartOffset`; if the existing value is at/above the diskless earliest (e.g.
it was legitimately advanced past the remote tier by retention / DeleteRecords)
it falls back to the default behaviour.

This fix is **necessary but not sufficient** — see below.

### Status (verified against the system test)

Run after the fix (session `2026-06-15--014`):

- **Part 1 — fixed and verified.** `ListOffsets(earliest)` now returns `0` after
  the wipe + restart (it returned the diskless start `232414` before), so the
  reproducer's `earliest == 0` gate passes.
- **Part 2 — still broken.** A consumer reset to `earliest` (offset 0) is still
  only served records starting at the diskless log-start (`231900` in that run);
  its first consumed batch is `minOffset 231900`, and there is **no
  RemoteLogManager read activity** for the partition. The broker silently serves
  the offset-0 fetch from the local start instead of fetching the consolidated
  prefix `[0, 231900)` from remote.

### Why Part 2 is still broken

`UnifiedLog.truncateFullyAndStartAt` calls `leaderEpochCache.clearAndFlush()`.
After the consolidation fetcher rebuilds the local log from the diskless WAL, the
leader-epoch cache only covers the rebuilt range `[diskless-start, HW)`. A fetch
below the diskless start cannot be mapped to a leader epoch, so the
`RemoteLogManager` lookup for the corresponding remote segment never happens and
the read falls through to the local start.

The classic tiered-storage path solves exactly this with
`buildRemoteLogAuxState` (driven by the `TierStateMachine` on an
`OFFSET_MOVED_TO_TIERED_STORAGE` response), which reconstructs the leader-epoch
cache and producer snapshot from the remote segment metadata. That rebuild is
**not wired into the consolidation fetcher** (`DisklessLeaderEndPoint` returns the
diskless earliest, never `OFFSET_MOVED_TO_TIERED_STORAGE`), so a consolidating
partition that has lost its local log never reconstructs the aux state needed to
serve the remote prefix.

### Remaining work (Part 2)

When the consolidation fetcher resets a consolidating partition whose remote tier
holds data below the diskless WAL start, it must reconstruct the remote auxiliary
state for `[trueLogStart, diskless-start)` from the remote segment metadata
(leader-epoch cache + producer snapshot), the same way `buildRemoteLogAuxState`
does for classic tiered followers — not merely lower `logStartOffset`. This is a
larger change in the fetcher / tier-state-machine interaction and needs its own
design + system-test validation.

The Part 1 fix has been compiled (`./gradlew :core:compileScala`) and verified
end-to-end to flip `earliest` to `0`; Part 2 remains open. A focused design for
Part 2 is in
[`CONSOLIDATION_REMOTE_AUX_STATE_REBUILD_DESIGN.md`](./CONSOLIDATION_REMOTE_AUX_STATE_REBUILD_DESIGN.md).

## Reproducer

`tests/kafkatest/tests/inkless/consolidation_read_from_remote_after_prune_test.py`
(`ReadFromRemoteAfterPruneTest.test_read_from_remote_after_prune`).

The test produces a multi-segment dataset, waits for it to be tiered and the WAL
to be pruned, stops all brokers, `rm -rf`s the topic's partition directories
from every broker data dir, restarts, and then asserts the **correct** behaviour:

1. the partition's earliest offset is still `0` after the wipe (fast,
   deterministic assertion — this is what currently fails), and
2. a consumer reset to `earliest` reads all produced records back from remote
   (reached only once the bug is fixed).

While the bug exists, the test fails fast on assertion (1) with a message that
explains the diskless-vs-true log-start confusion. Once the read path is fixed,
both assertions pass.

Run it with:

```shell
docker exec ducker01 bash -c "cd /opt/kafka-dev && ducktape \
  --cluster-file /opt/kafka-dev/tests/docker/build/cluster.json \
  ./tests/kafkatest/tests/inkless/consolidation_read_from_remote_after_prune_test.py"
```
