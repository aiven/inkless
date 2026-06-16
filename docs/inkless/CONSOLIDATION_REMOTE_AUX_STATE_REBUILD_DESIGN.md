# Design proposal: rebuilding remote aux state for consolidating partitions after local-log loss (Part 2)

## Context

This is the design for **Part 2** of the consolidation durability fix. See
[`CONSOLIDATION_READ_FROM_REMOTE_AFTER_PRUNE_BUG.md`](./CONSOLIDATION_READ_FROM_REMOTE_AFTER_PRUNE_BUG.md)
for the full bug background.

> **Correction (2026-06-15, session `--015`).** The "Part 1 done/verified" claim
> below is wrong. Part 1 (and Part 2) were implemented inside
> `ConsolidationFetcherThread.truncateFullyAndStartAt`, but that override is
> **never invoked** in the wipe-rehydration path: the fetcher arms at
> `highWatermark = 0` and appends the WAL forward, never issuing a full truncate.
> `earliest` reads `0` only because the wiped log's default `logStartOffset` is
> never advanced — not because of the override. Both parts are inert until the
> trigger moves to the seeding path (see "The missing trigger" correction and the
> revised Approach A). The remaining sections still describe the correct *rebuild
> mechanism*; only the *trigger location* changed.

**Part 1 (superseded).** `ConsolidationFetcherThread.truncateFullyAndStartAt`
preserves the lower `logStartOffset` on reset, intended so that after wiping the
local log and restarting, `ListOffsets(earliest)` reports `0`. (Inert — the
override is not called; see correction above.)

**Part 2 (this doc, open).** Even with `earliest = 0`, a consumer reset to
`earliest` is *not actually served* offset 0. The system tests (sessions
`2026-06-15--014` and `--015`) showed the first record returned by a fetch from
offset 0 is the diskless start (`231900` / `231615`). The consolidated prefix
`[0, diskless-start)` lives in the remote tier but cannot be fetched.

## Root cause

When the consolidation fetcher rebuilds a wiped partition's local log from the
diskless WAL, `UnifiedLog.truncateFullyAndStartAt` calls
`leaderEpochCache.clearAndFlush()`. The rebuilt epoch cache therefore only covers
`[diskless-start, HW)`.

A consumer fetch below the diskless start needs the broker to:

1. detect `fetchOffset < localLogStartOffset` and route to the remote tier, then
2. map `fetchOffset → leader epoch → remote segment` via the leader-epoch cache
   and `RemoteLogManager`.

Step 2 fails because the epoch cache has no entry for offsets below the diskless
start, so the remote-segment lookup never happens and the read silently falls
through to the local start.

## The mechanism to reuse: `TierStateMachine.buildRemoteLogAuxState`

Classic tiered storage already solves exactly this for followers in
[`core/src/main/java/kafka/server/TierStateMachine.java`](../../core/src/main/java/kafka/server/TierStateMachine.java).
`buildRemoteLogAuxState(...)`:

- finds the remote segment metadata covering `leaderLocalLogStartOffset - 1`,
- reads that segment's `LEADER_EPOCH` index from remote storage and calls
  `unifiedLog.leaderEpochCache().assign(epochs)` to **rebuild the epoch cache**
  for the remote prefix,
- restores the `PRODUCER_SNAPSHOT` from remote (`buildProducerSnapshotFile`),
- truncates the local log to `remoteSegment.endOffset() + 1` and sets
  `logStartOffset = leaderLogStartOffset`, `localLogStartOffset = nextOffset`.

This is precisely the state a wiped consolidating partition is missing. It is
already constructed for every `ReplicaFetcherThread` (and therefore for
`ConsolidationFetcherThread`) as
`fetchTierStateMachine = new TierStateMachine(leader, replicaMgr, false)`.

### Why the inputs line up for the consolidation case

`TierStateMachine.start(tp, fetchState, fetchPartitionData)` needs three values;
all are already available on the diskless path:

| Input | Source | Value for our case |
|---|---|---|
| `leaderLocalStartOffset` + its epoch | `leader.fetchEarliestLocalOffset` → `DisklessLeaderEndPoint.listDisklessOffset(EARLIEST_LOCAL_TIMESTAMP)` | the diskless WAL start (`231900`) — the exact boundary up to which we must rebuild |
| `leaderLogStartOffset` (true start) | `fetchPartitionData.logStartOffset()` | **`0`** — `DisklessLeaderEndPoint.fetch` already sets `logStartOffset` to the *local* log's `logStartOffset`, which Part 1 keeps at `0` (see `DisklessLeaderEndPoint.scala` ~line 120-134) |
| remote segment for `leaderLocalStartOffset - 1` | `RemoteLogManager.fetchRemoteLogSegmentMetadata` | the consolidated segment written by the RLM copy task; carries the `LEADER_EPOCH` + `PRODUCER_SNAPSHOT` indexes |

The key realization: **Part 1 already makes `fetchPartitionData.logStartOffset()`
return the true start (0)**, so the aux-state rebuild has the correct lower bound
without needing a new "true log start" source.

## The missing trigger

`buildRemoteLogAuxState` only runs from
`AbstractFetcherThread.handleOffsetsMovedToTieredStorage`, which fires when a
fetch response carries `Errors.OFFSET_MOVED_TO_TIERED_STORAGE`.
`DisklessLeaderEndPoint.fetch` never returns that error, and the consolidation
fetcher only ever fetches *from the diskless start upward* (it is building the
diskless interval), so it never naturally requests an offset that would trigger
the tiered-storage path. That is why the rebuild never happens today.

> **Correction (2026-06-15, session `--015`).** There is a second, more basic
> missing trigger that invalidates the *implemented* form of Approach A below.
> Approach A was implemented by overriding
> `ConsolidationFetcherThread.truncateFullyAndStartAt`, on the assumption that the
> wipe-rehydration goes through an out-of-range
> `fetchOffsetAndTruncate → truncateFullyAndStartAt(diskless-start)`. The
> system-test logs show it does **not**: the fetcher arms at
> `initialFetchOffset(log) = highWatermark = 0` (empty wiped log) and **appends**
> the WAL forward from the diskless start into a fresh base-0 segment. The only
> resets are no-op `truncateTo(...)` calls; `truncateFullyAndStartAt` is never
> invoked, so the override (Part 1 + Part 2) is dead code. Result: `logStartOffset`
> stays at the wiped log's default `0` and `leader-epoch-checkpoint` is `0 231615`
> — `earliest` reads `0` for an unrelated reason, and a fetch at 0 is served the
> base-0 local hole's first record (231615).
>
> **Therefore the trigger must move to the seeding path**
> (`ConsolidationReconciler.reconcileSwitchedConsolidatingDisklessPartition`,
> born-diskless branch), where the fetcher's start offset is chosen. See the
> revised Approach A below.

## Proposed approaches

### Approach A (NOT chosen — superseded by B): rebuild the aux state when seeding the consolidation fetcher

> **Revised (session `--015`).** The trigger would be the **reconciler / seeding
> path**, not the fetcher's `truncateFullyAndStartAt` (which is never called in the
> wipe-rehydration flow — see "The missing trigger" correction above).
>
> **Not chosen:** a one-shot seeding trigger is *not retryable*, so it loses to the
> RLM-readiness race after restart. See Approach B (chosen) below.

In `ConsolidationReconciler.reconcileSwitchedConsolidatingDisklessPartition`
(the born-diskless / `NO_CLASSIC_TO_DISKLESS_START_OFFSET` branch), before arming
the fetcher at `initialFetchOffset(log)`, detect a consolidated remote prefix
below the diskless WAL start and, if present:

1. seed the local log at the diskless start via
   `truncateFullyAndStartAt(disklessStart)` so the segment base /
   `localLogStartOffset` becomes the diskless start (not a base-0 hole), which is
   what makes `ReplicaManager.fetchMessages` route reads below it to the remote
   tier rather than into the empty local prefix,
2. preserve `logStartOffset = 0` (Part 1), and
3. rebuild the leader-epoch cache (+ producer snapshot) for
   `[0, disklessStart)` from the remote tier (Part 2 / `buildRemoteLogAuxState`).

The existing `truncateFullyAndStartAt` override body and `rebuildAuxState` helper
are reused verbatim; only the **call site** changes — the reconciler invokes them
explicitly during seeding instead of relying on an out-of-range reset that the
fetcher never issues. The diskless start is the control-plane `log_start_offset`
(already available to the reconciler via the metadata view / `partition`).

Detection (all local, no extra RPC): a remote prefix exists when
`unifiedLog.logStartOffset < disklessStart` **and** the partition has remote
segments covering `disklessStart - 1`
(`RemoteLogManager.fetchRemoteLogSegmentMetadata(tp, epoch, disklessStart - 1)`
is present).

Integration options, in order of preference:

1. **Reuse `fetchTierStateMachine` via a small refactor.** Extract the body of
   `TierStateMachine.buildRemoteLogAuxState` into a package-visible method that
   takes `(tp, currentLeaderEpoch, leaderLocalStartOffset, epoch,
   leaderLogStartOffset)` and call it from the fetcher's reset path. This avoids
   synthesizing a fake `PartitionData` and keeps all remote-tier logic in one
   place. `TierStateMachine.start` itself can stay as the
   `OFFSET_MOVED_TO_TIERED_STORAGE` entry point.

2. **Synthesize a minimal `PartitionData`** with `logStartOffset = 0` and call
   `fetchTierStateMachine.start(tp, fetchState, syntheticData)` directly from the
   fetcher. Lower code churn but relies on `start` not reading other
   `PartitionData` fields (it currently only reads `logStartOffset()`), which is
   fragile against future changes.

The reset is rare (only on out-of-range / full rebuild), so the extra remote
metadata lookups are not a hot-path concern.

### Approach B (CHOSEN & IMPLEMENTED, session `--015`): emit `OFFSET_MOVED_TO_TIERED_STORAGE` from the diskless leader endpoint

Make `DisklessLeaderEndPoint.fetch` return `OFFSET_MOVED_TO_TIERED_STORAGE` when
an offset below the diskless start (but at/above the whole-log `logStartOffset`,
i.e. present in remote) is requested, so the stock
`handleOffsetsMovedToTieredStorage → TierStateMachine.start → buildRemoteLogAuxState`
path runs unchanged.

**Originally rejected, now chosen.** The original rejection ("the consolidation
fetcher never requests such offsets") was wrong: after a local-log loss the
fetcher is armed at `initialFetchOffset(log) = highWatermark = 0`, which is below
the diskless WAL start, so it *does* request the prefix and B fires immediately
on the leader. B is preferred over A because:

- **It is retryable.** `buildRemoteLogAuxState` needs `rlm.isPartitionReady`,
  which races broker restart. The `OFFSET_MOVED_TO_TIERED_STORAGE` path retries on
  each fetch cycle (`RemoteStorageNotReadyException` is a
  `RetriableRemoteStorageException`), whereas a one-shot Approach-A trigger in the
  reconciler/`addPartitions` would fall back permanently if it ran before the RLM
  was ready.
- **Less new code.** It reuses the stock `start`/`buildRemoteLogAuxState`
  machinery (which already truncates the local log to the remote segment end,
  sets `logStartOffset` from the fetch response, and rebuilds the epoch cache +
  producer snapshot). Only the error signal in the leader endpoint is new.

Implementation: in `DisklessLeaderEndPoint.fetch`, for a `NONE`-error diskless
response, if the partition's local log has remote storage enabled and the
requested fetch offset is in `[localLog.logStartOffset, data.logStartOffset)`
(the latter being the diskless WAL start), set the response error code to
`OFFSET_MOVED_TO_TIERED_STORAGE` (keeping `logStartOffset` = the local whole-log
start). Steady-state and first-switch fetches request offsets `>=` the diskless
start, so the signal only fires for the wiped/behind recovery case.

The inert Approach-A code (the `ConsolidationFetcherThread.truncateFullyAndStartAt`
override + `tryRebuildRemoteAuxState`, and `TierStateMachine.rebuildAuxState`) was
reverted.

### Approach C: rebuild at leadership transition (`makeLeader`) for diskless+remote partitions

Reconstruct the epoch cache from remote when a broker becomes leader of a
consolidating partition that has a remote tier but an empty/short local log. This
is closer to where classic tiered storage performs leader-side recovery and would
cover paths that do not go through the fetcher reset.

**Status:** plausible and possibly the cleanest long-term home, but the diskless
`makeLeader` path is specialized and needs investigation before committing. Worth
prototyping if Approach A's fetcher-reset timing proves awkward (e.g. the rebuild
must happen before the partition starts serving consumer fetches).

## Verification results (checked against the rebased diskless-epoch branch)

### Item 1 — does `fetchEarliestLocalOffset` return the diskless epoch `k+1`? **No (epoch-blind).**

The diskless offset path is epoch-blind. `FetchOffsetHandler.queryControlPlane`
hardcodes the epoch:

```java
new FileRecords.TimestampAndOffset(response.timestamp(), response.offset(),
    Optional.of(LeaderAndIsr.INITIAL_LEADER_EPOCH))   // == 0, always
```

`ListOffsetsResponse` carries no epoch field, and `InitDisklessLogRequest` does
not persist a leader epoch either — so `DisklessLeaderEndPoint.fetchEarliestLocalOffset`
always reports **epoch 0**. The "diskless epoch" (`last_classic + 1`) is a *KRaft
leader-epoch bump* (#626), not something the control plane exposes.

Consequence:
- **Born-diskless:** the whole topic is epoch 0 anyway (see item 3), so the
  hardcoded 0 is accidentally correct and `buildRemoteLogAuxState` short-circuits
  (`epochForLeaderLocalLogStartOffset == 0`, no `fetchEpochEndOffsets` RPC).
- **Mixed classic→diskless:** the diskless interval is owned by `k+1`, but the
  path still reports 0. `buildRemoteLogAuxState` would then look up
  `fetchRemoteLogSegmentMetadata(tp, 0, boundary)` for a boundary that lives in a
  segment whose per-segment `segmentLeaderEpochs` is `{k+1: seal}` — **no epoch-0
  entry → lookup fails → rebuild throws.** So we cannot trust the diskless
  ListOffsets epoch; the boundary epoch must be discovered from the remote
  metadata instead.

### Item 2 — are the remote `LEADER_EPOCH` checkpoints cumulative? **Yes.**

`RemoteLogManager.copyLogSegmentData` uploads the leader-epoch index as
`epochEntriesAsByteBuffer(getLeaderEpochEntries(log, -1, nextSegmentBaseOffset))`
— i.e. **all** epoch entries from offset `-1` up to the segment end, not just the
segment's own epochs. So reading the boundary segment's `LEADER_EPOCH` index and
`leaderEpochCache().assign(epochs)` restores the entire `0 … k+1` lineage in one
shot.

Caveat: the per-segment metadata map `segmentLeaderEpochs` (used as the *lookup
key* in `fetchRemoteLogSegmentMetadata`) only holds the segment's own epochs —
which is exactly why item 1's wrong epoch breaks the *lookup* even though the
*index* itself is complete.

### Item 3 — born-diskless boundary epoch? **0 (empirically confirmed).**

The rebuilt `leader-epoch-checkpoint` for the test partition on all brokers reads:

```
0
1
0 231900      # epoch 0 starts at the diskless WAL start
```

So the entire born-diskless topic sits at epoch 0, even though the partition's
KRaft leader epoch was 2 after the restart. The diskless records are stamped
epoch 0 regardless of the current partition leader epoch. This is consistent with
`diskless_epoch = last_classic + 1` where `last_classic = -1`.

**Net effect on the design:** for the born-diskless reproducer we can reuse
`buildRemoteLogAuxState` almost as-is (epoch-0 short-circuit avoids the broken
`fetchEpochEndOffsets` RPC). General correctness for mixed topics requires
discovering the boundary segment's epoch from the remote metadata rather than
from the diskless ListOffsets path.

## Risks and open questions

1. **Leader-epoch lookup for epoch > 0 — confirmed real for mixed topics, benign
   for born-diskless.** The diskless ListOffsets path always reports epoch 0
   (item 1). For born-diskless that matches reality (item 3); for mixed
   classic→diskless the boundary epoch is `k+1` and the epoch-0 lookup fails. The
   fix is to **not** route the boundary-epoch resolution through
   `leader.fetchEpochEndOffsets` (the diskless control plane) at all — instead
   determine the boundary segment from the remote metadata directly (e.g. iterate
   the partition's remote leader-epoch set, which `RemoteLogManager` already
   assembles, to find the segment covering `boundary`). With the segment found,
   item 2 guarantees its cumulative `LEADER_EPOCH` index restores the full
   lineage. This removes the dependence on the epoch-blind control-plane RPC.

2. **Producer-snapshot restore — verified low risk.** The RLM copy path
   *requires* a producer snapshot to exist: `RemoteLogManager.copyLogSegment`
   does `producerStateSnapshotFile = log.producerStateManager().fetchSnapshot(
   nextSegmentBaseOffset).orElse(null)` and then dereferences it unconditionally
   as `producerStateSnapshotFile.toPath()` (no null guard). So a segment can only
   be tiered if its boundary snapshot exists. Because the test's prefix segments
   *did* tier (the WAL pruner advanced `highestOffsetInRemoteStorage`, which
   requires a completed copy), the snapshots are present in remote, and
   `buildProducerSnapshotFile` restores them via the same standard mechanism.
   The consolidation fetcher maintains producer state through
   `appendRecordsToFollowerOrFutureReplica` (idempotent producers were active in
   the test, so the snapshots are non-trivial and were still copied). Residual
   check: exercise a transactional producer to confirm aborted-txn / first-
   unstable-offset state restores cleanly.

3. **`fetchLatestOffset` lag computation — verified benign.**
   `DisklessLeaderEndPoint.fetchLatestOffset` returns the diskless LEO. In
   `TierStateMachine.start`, `initialLag = leaderEndOffset - offsetToFetch` where
   `offsetToFetch = nextOffset = diskless-start`, so the lag is exactly the size
   of the un-fetched diskless interval `[diskless-start, disklessLEO)` — which is
   precisely what the fetcher goes on to fetch. The remote prefix
   `[0, diskless-start)` is correctly excluded from lag (it is served from remote,
   not fetched). The hardcoded epoch (item 1) is not used in the lag math.

4. **Idempotency / repeated resets — needs a trigger guard.** The offset math is
   idempotent: `maybeIncrementLogStartOffset` / `maybeIncrementLocalLogStartOffset`
   are strictly monotonic (`UnifiedLog` lines 1313, 1349) and never regress
   upward, and `truncateFullyAndStartAt(nextOffset, Optional.of(0))` deterministically
   lands at `logStartOffset = 0`, `localLogStartOffset = diskless-start` every
   time. **However**, `truncateFullyAndStartAt` *fully wipes the local log* and
   *clears the leader-epoch cache* (`UnifiedLog` lines 2364–2365). So if the
   rebuild were triggered on an ordinary mid-operation reset it would discard
   already-fetched diskless data and re-do the remote round-trips. The rebuild
   must therefore be **guarded to fire only when it is actually needed** — i.e.
   the local log is empty/fresh (`logEndOffset == localLogStartOffset`, or no
   epoch-cache entry below `diskless-start`) **and** a remote prefix exists below
   the diskless start. Otherwise fall back to the plain (Part 1) truncate.

5. **Interaction with Part 1 — confirmed complementary and required.** The rebuild
   sets `logStartOffset = leaderLogStartOffset`, and that value flows from
   `DisklessLeaderEndPoint.fetch`, which sets the fetch response's
   `logStartOffset` to `partition.localLogOrException.logStartOffset` (the *local*
   log's start). Part 1 is exactly what keeps that local value at `0` after the
   post-wipe reset; without it the reset would set the local `logStartOffset` to
   the diskless start and the rebuild would then re-orphan the remote prefix.
   In Approach A the two unify into a single override: *if* a remote prefix exists
   → rebuild (sourcing the true start = the preserved-lower `logStartOffset`);
   *else* → plain truncate that preserves the lower `logStartOffset` (Part 1).
   They are complementary, not alternatives.

   (Optional hardening: source `leaderLogStartOffset` from the earliest remote
   segment's start offset via `RemoteLogManager` instead of the local log, which
   would make the rebuild self-sufficient even if Part 1 regressed — but Part 1 is
   simpler and already verified, so keep it as the primary source.)

## Test plan

1. **Existing reproducer (primary signal).**
   `consolidation_read_from_remote_after_prune_test.py` now gates on both
   (a) `earliest == 0` and (b) the first *actually-served* offset from a fetch at
   0 equals `0`. Part 2 is done when gate (b) passes and the end-to-end consume
   returns all records from remote.
2. **Unit/integration (PR builds — DONE).** Two JVM suites run on every PR build,
   so the fix is verifiable without the (infrequent) system test:
   - `DisklessLeaderEndPointTest` — pins the leader-side redirect: a fetch in the
     consolidated prefix `[logStartOffset, disklessStart)` (and only there) returns
     `OFFSET_MOVED_TO_TIERED_STORAGE`, with `logStartOffset` preserved; plus a
     `buildFetch → fetch` round trip through the real `FetchRequest` plumbing.
   - `kafka.server.ConsolidationTierStateRebuildTest` — drives the *real*
     `TierStateMachine.start`/`buildRemoteLogAuxState` over mocks and asserts the
     rebuilt epoch cache, the resumed fetch offset (WAL start), and the preserved
     `logStartOffset = 0` via `truncateFullyAndStartAt`.
3. **Mixed-history case (PR builds — DONE at the rebuild layer).**
   `ConsolidationTierStateRebuildTest.testRebuildResolvesClassicEpochForSwitchedTopic`
   exercises the `fetchEpochEndOffsets` path (risk #1): a switched topic whose
   earliest-local (WAL start) offset rides at the diskless epoch `k+1`, where the
   boundary offset must be resolved back to the classic epoch and the remote segment
   located under that classic epoch — contrasted with
   `testRebuildShortCircuitsEpochZeroForBornConsolidatedTopic` (epoch 0, no RPC).
   Still missing: an end-to-end **switched-topic system test** (classic prefix tiered,
   then switched to diskless, consolidated, pruned, local-wiped) to validate the RLM
   copy path actually wrote usable classic-epoch segments — the one thing the mocks
   cannot prove.

## Recommendation

Pursue **Approach A, option 1**: refactor the remote-aux-state body out of
`TierStateMachine` into a reusable method and call it from
`ConsolidationFetcherThread`'s reset when a consolidated remote prefix exists
below the diskless start. It reuses the proven tiered-storage rebuild, the inputs
already line up (Part 1 supplies the true `logStartOffset = 0`), and it is
localized to the consolidation fetcher.

Stage it in two steps, which the verification results make clean:

1. **Born-diskless first (the current reproducer).** Item 3 confirms the prefix
   is epoch 0, so `buildRemoteLogAuxState` short-circuits and never calls the
   epoch-blind `fetchEpochEndOffsets` RPC. This is enough to make
   `consolidation_read_from_remote_after_prune_test.py` go green and is low-risk.
2. **Mixed classic→diskless next.** Replace the boundary-epoch resolution so it
   comes from the remote metadata (the `RemoteLogManager` remote leader-epoch set)
   instead of `leader.fetchEpochEndOffsets`, which the diskless control plane
   cannot answer for classic epochs (item 1). Cover it with the mixed-history
   test before relying on it.

Keep Approach C in mind if the fetcher-reset timing relative to the first consumer
fetch proves problematic.
