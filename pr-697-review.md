# PR Review: #697 — fix(inkless:switch): clear under-replicated partitions after classic-to-diskless switch

**Verdict**: **blockers found** (2 blockers, both with verified patches)

**Latest round**: Round 2 — execution-verified. PR head `8d537372`, merge base `f751436ad0`, not stacked.

**If you wrote this PR**: [Invariants at stake](#invariants-at-stake) is why two of these are blockers rather than opinions, [Decisions](#decisions) is the whole ask in dependency order, and every fix exists as a commit on `jeqo/pr-697-review` to cherry-pick. Appendix A is my draft PR comments — you do not need it, the findings say the same thing in place.

**If you are checking this review**: [Appendix B](#appendix-b---revert-experiment) is the revert experiment that decides which of the PR's tests are load-bearing, and [Appendix C](#appendix-c---round-2-changes) is what changed between rounds, including what I withdrew.

---

## Contents

- [Getting at the patches](#getting-at-the-patches)
- [Invariants at stake](#invariants-at-stake)
- [Decisions](#decisions)
  - [D1 — Do errors propagate on the at-seal follower path, or is `Errors.NO…](#d1-do-errors-propagate-on-the-at-seal-follower-path-or-is-errorsnone-intended)
  - [D2 — How is the at-seal fetch loop bounded?](#d2-how-is-the-at-seal-fetch-loop-bounded)
  - [D3 — What actually proves this fix end-to-end?](#d3-what-actually-proves-this-fix-end-to-end)
  - [D4 — Is the controller's seal-blind ISR expansion on unfence intended?](#d4-is-the-controllers-seal-blind-isr-expansion-on-unfence-intended)
  - [D5 — Accept the replication hot-path cost?](#d5-accept-the-replication-hot-path-cost)
  - [D6 — Comment and naming cleanup](#d6-comment-and-naming-cleanup)
- [Blockers](#blockers)
  - [B2 — the at-seal fetch loop has no backoff (high)](#b2-coresrcmainscalakafkaserverreplicafetcherthreadscala180-185-the-at-seal-fetch-loop-has-no-backoff-high)
  - [B3 — leader-epoch and leadership errors collapse to `Errors.NONE`, and…](#b3-coresrcmainscalakafkaserverreplicamanagerscala2518-2523-leader-epoch-and-leadership-errors-collapse-to-errorsnone-and-the-test-that-covers-it-is-vacuous-high)
- [Suggestions](#suggestions)
  - [S1 — no controlled-shutdown guard, so `isOutOfIsr` starts a fetcher tha…](#s1-coresrcmainscalakafkaserverreplicamanagerscala4151-4152-no-controlled-shutdown-guard-so-isoutofisr-starts-a-fetcher-that-can-never-evict-medium)
  - [S2 — two `Properties` copies per partition per fetch on the replication…](#s2-coresrcmainscalakafkaserverreplicafetcherthreadscala177-179-two-properties-copies-per-partition-per-fetch-on-the-replication-hot-path-medium)
  - [S3 — the system test cannot observe the mechanism it is named for (medi…](#s3-was-b1-testskafkatesttestsinklessinkless_topic_switch_testpy1301-the-system-test-cannot-observe-the-mechanism-it-is-named-for-medium-downgraded-from-blocker)
  - [S4 — two comments describe behaviour this PR removed (low)](#s4-coresrcmainscalakafkaserverreplicamanagerscala4156-and-2559-2560-two-comments-describe-behaviour-this-pr-removed-low)
  - [S5 — the eviction comment omits the consolidating exemption (low)](#s5-coresrcmainscalakafkaserverreplicafetcherthreadscala171-174-the-eviction-comment-omits-the-consolidating-exemption-low)
  - [S6 — the ISR assertion does not check the submitted set (low)](#s6-coresrctestscalaunitkafkaserverreplicamanagerinklesstestscala7182-the-isr-assertion-does-not-check-the-submitted-set-low)
  - [S7 — Untested branches: the consolidating eviction leg, and `isReplicaI…](#s7-untested-branches-the-consolidating-eviction-leg-and-isreplicainisr-with-the-partition-absent-medium-low)
- [Questions / confirm-intent](#questions-confirm-intent)
  - [Q1 — the clamp to the seal is deliberate; now documented](#q1-coresrcmainscalakafkaserverreplicamanagerscala2527-the-clamp-to-the-seal-is-deliberate-now-documented)
  - [Q2 / D4 — the controller's ISR expansion on unfence ignores the seal](#q2-d4-metadatareplicationcontrolmanagerjava2005-the-controllers-isr-expansion-on-unfence-ignores-the-seal)
- [Nits](#nits)
  - [N1 — dead stub from the first implementation](#n1-coresrctestscalaunitkafkaserverreplicafetcherthreadtestscala815-dead-stub-from-the-first-implementation)
  - [N2 — Folded into B3](#n2-folded-into-b3)
  - [N3 — two near-identical wait helpers (low)](#n3-testskafkatesttestsinklessinkless_topic_switch_testpy415430-two-near-identical-wait-helpers-low)
  - [N4 — no freshness bound on the JMX sample (low)](#n4-testskafkatesttestsinklessinkless_topic_switch_testpy382-no-freshness-bound-on-the-jmx-sample-low)
- [What I verified and found correct](#what-i-verified-and-found-correct)
- [Copilot triage](#copilot-triage)
- [Appendix A - comment drafts](#appendix-a---comment-drafts)
- [Appendix B - revert experiment](#appendix-b---revert-experiment)
  - [R1 — leader-side at-seal `fetchRecords` block reverted ()](#r1-leader-side-at-seal-fetchrecords-block-reverted-replicamanagerscala2517-2556)
  - [R2 — `makeFollower` `isOutOfIsr` clause reverted ()](#r2-makefollower-isoutofisr-clause-reverted-replicamanagerscala4151-4152)
  - [R3 — ISR-gated eviction reverted ()](#r3-isr-gated-eviction-reverted-replicafetcherthreadscala177-185)
  - [Reading](#reading)
- [Appendix C - round 2 changes](#appendix-c---round-2-changes)
  - [Confirmed by execution](#confirmed-by-execution)
  - [Downgraded](#downgraded)
  - [New in Round 2](#new-in-round-2)
  - [Withdrawn](#withdrawn)
  - [Not verifiable here](#not-verifiable-here)

## Getting at the patches

Round 2's fixes are six draft commits on `jeqo/pr-697-review`, based on PR head `8d537372`. They are
proposals, not a request to merge the branch: cherry-pick what you agree with. The PR branch
`glillo/resolve-urp-switch` was not touched.

```bash
git fetch origin jeqo/pr-697-review
git log --oneline f751436ad0..origin/jeqo/pr-697-review

# cherry-pick one onto the PR branch
git cherry-pick 4d44549317
```

One patch reaches outside the PR's own files: B2 widens `delayPartitions` from `private` to
`protected` in `core/src/main/scala/kafka/server/AbstractFetcherThread.scala` (one word) so
`ReplicaFetcherThread` can back off the at-seal partition. That is an upstream file, so if you would
rather not touch it, the alternative is noted under B2's fix draft.

The six commits, oldest first:

- `4d44549317` — B2: back off the at-seal fetch loop instead of spinning (also delivers S2 and S5)
- `a507ae1a23` — S1: no doomed catch-up fetcher during controlled shutdown
- `764c1adb77` — B3: report leader-epoch and leadership errors (also delivers N2)
- `dc415a8949` — D4/Q2: characterisation test pinning the controller's seal-blind ISR expansion
- `ebac227ddc` — S3/S6/S7: cover the untested branches (also delivers N1)
- `aae665d2e9` — S4/N3/N4: stale comments and the URP helper tidy-up

All six together: `382 tests, 0 failures, 0 errors` across `ReplicaManagerInklessTest`,
`ReplicaFetcherThreadTest`, `InklessMetadataViewTest` and `ReplicationControlManagerInklessTest`.

Line numbers in this report are **PR-head** line numbers, so they match the staged diff. The patches
shift them.

---

## Invariants at stake

The blockers are blockers because each one breaks a property of the follower-FETCH response contract
that is true independently of this diff. Wording here is verbatim from the traces.

- **I1 — validate before success.** *A leader must not answer a FETCH whose `(replicaId, leaderEpoch)`
  tuple it has not validated with a success code.* Broken by **B3 causes A, B and C** (epoch above the
  leader's, epoch below it, and replica no longer in the replica set). Enforced on the classic path by
  `Partition.scala:1444-1457 (fetchRecords)`, which runs `checkCurrentLeaderEpoch`
  (`Partition.scala:503`) and `followerReplicaOrThrow` (`Partition.scala:1482`) *before* the read; not
  enforced on the at-seal diskless path.
- **I2 — the wait contract.** *A follower FETCH carrying `minBytes > 0` and `maxWait > 0` that cannot be
  satisfied must park in `delayedFetchPurgatory` until `minBytes` is available or `maxWait` elapses.*
  Broken by **B2** at `ReplicaManager.scala:2625-2628 (fetchMessages)` (PR head; `:2579` at the merge
  base), which returns before the purgatory and discards the `maxWait`/`minBytes` the follower sent.
- **I3 — a failed log directory must not look healthy.** *A broker whose log directory has failed must
  not answer reads for that partition with a success code.* Broken by **B3 cause D**, where
  `Either.foreach` at `ReplicaManager.scala:2518` drops the `Left(KAFKA_STORAGE_ERROR)` that
  `getPartitionOrError` (`ReplicaManager.scala:755-756`) produced.

Three invariants, four break sites — and that mapping is what sets the decision boundaries:

- I1's three break sites are one question ("should this path report epoch and membership errors at
  all?"), which is why **D1** is a single decision rather than three comments. I3 is the same question
  applied to a different error source, so it belongs to D1 too — it is the increment I measured and
  deliberately left out of the patch.
- I2 is independent of all of them and is **D2**.
- No invariant is broken by the suggestions, which is the line between them and the blockers: S1
  worsens I2's blast radius but breaks nothing on its own, and S2-S7 are cost, coverage and accuracy.

The general shape outlives this PR either way: the at-seal branch is a *second* implementation of the
follower-fetch response contract, and it inherits none of the obligations the classic path discharges
by construction.

---

## Decisions

A thin index only. Each decision names the question, the findings that feed it, and what it depends
on. The findings below remain the single source of truth for evidence and patches.

### D1 — Do errors propagate on the at-seal follower path, or is `Errors.NONE` intended?

Feeds: **B3**, **N2**, **S7**. Depends on: nothing — settle first.

N2's test rename and S7's missing epoch case both hang off the answer: the existing test *asserts*
`Errors.NONE`, so it encodes the current behaviour as intended. Rename before deciding and you
rewrite the assertions afterwards.

Turns on **B3's four traces** (one per cause) and **B3.1's side-by-side trace** — the decision is
really "which of these four exchanges should return an error", and the traces are what distinguish
them.

### D2 — How is the at-seal fetch loop bounded?

Feeds: **B2**, **S1**. Depends on: nothing.

Candidates: (a) route the at-seal response through the fetch purgatory so `replica.fetch.wait.max.ms`
applies; (b) follower-side backoff via `delayPartitions`; (c) the controlled-shutdown guard alone.
Recommendation: **(b) plus the guard**. One choice settles both findings. Rationale and the rejected
alternatives are in B2.

Turns on **B2's trace**: step 6 is where `maxWait` is discarded (which is what option (a) would
address) and step 9 is where the pre-PR exit was removed (which is what option (b) restores). The S1
branch of the same trace is why option (c) alone is not enough.

### D3 — What actually proves this fix end-to-end?

Feeds: **B1**, **S3**, **S6**. Depends on: **D4**.

The unit coverage does discriminate (see the revert experiment). The system test does not. What a
replacement test is allowed to assert depends on D4.

### D4 — Is the controller's seal-blind ISR expansion on unfence intended?

Feeds: **Q2**. Depends on: nothing, but **gates D3**.

Not a plain out-of-scope follow-up. `expandIsrForDisklessManagedPartitions` re-admits a returning
replica to a *switched* partition's ISR with no reference to the seal — now proven by a passing
characterisation test. If that stays, no restart-based test can observe the PR's mechanism, and D3
must pick a different scenario. If it is wrong, the fix belongs in the controller and D3's test
inverts.

### D5 — Accept the replication hot-path cost?

Feeds: **S2**. Depends on: nothing. **The one item mergeable on its own** — no behaviour change.

### D6 — Comment and naming cleanup

Feeds: **S4**, **S5**, **N1**, **N3**, **N4**, plus **N2** *after* D1.

---

## Blockers

### B2. `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:180-185` — the at-seal fetch loop has no backoff (high)

**Risk**: a fetcher thread at 100% CPU issuing fetch requests as fast as the network allows, plus
`AlterPartition` resubmissions from the leader at loop rate, for the whole duration of ISR
convergence — and indefinitely in one reachable state.

**Scenario**: for a fully-switched partition the leader answers from `immediateFetchResponses` and
then short-circuits at `ReplicaManager.scala:2625-2626`
(`classicFetchInfos.isEmpty && disklessFetchInfos.isEmpty` → `respond(Seq.empty)`), never entering
the purgatory, so `replica.fetch.wait.max.ms` and `replica.fetch.min.bytes` are ignored and the
response returns at once with zero bytes. On the follower `ShutdownableThread.run:135-136` has no
sleep and `maybeFetch:129-132` only awaits `fetchBackOffMs` when no request can be built. Pre-PR the
partition was evicted on that first response, so it happened once; with eviction gated on ISR it
repeats.

Worst on exactly the cluster shape the new system test uses: with every partition in the fetcher's
set fully-switched there is nothing to park the request on. Unbounded during this broker's own
controlled shutdown — see the S1 branch of the trace.

**Trace**

Participants: `F` = recovering follower broker, `L` = leader broker, `C` = controller.

Preconditions, all load-bearing:

- topic `diskless.enable=true`; `diskless.remote.storage.consolidation.enable=false` on `F`, so
  `isConsolidatingPartition` is false and the ISR leg is the only way to evict.
- partition seal **committed**: `classicToDisklessStartOffset = S`, `S >= 0`.
- `L` is the leader, local classic log frozen at `LEO == HW == S`. Leader epoch `N` on both sides.
- `F` hosts the partition, is in the replica set, local log `LEO == S`, and is **not** in the ISR of
  `F`'s own metadata image.
- `F`'s fetcher for `L` has **no other partitions** - every partition in the set is fully-switched
  diskless. This is the precondition that makes the spin observable; with any classic partition in
  the same request `classicFetchInfos` is non-empty and the request parks normally.
- `F` is **not** in controlled shutdown (that is the branch at the end).

Steps:

1. `C -> F`: metadata delta bumping `partitionEpoch` with the ISR excluding `F`. Because only
   `partitionEpoch` changed, the partition still lands in `localChanges.followers`
   (`TopicDelta.java:203-208 (localChanges)`).
2. `F`: `isOutOfIsr` is true, so the partition is queued for fetching
   (`ReplicaManager.scala:4151-4159 (applyLocalFollowersDelta)`); the fetcher is added at
   `initOffset = initialFetchOffset(log) = log.logEndOffset = S`
   (`ReplicaManager.scala:3241-3246 (initialFetchOffset)`, `:4265-4271`).
3. `F` fetcher: `buildFetch` produces a non-empty request carrying `maxWait = replica.fetch.wait.max.ms`
   (500 ms) and `minBytes = replica.fetch.min.bytes` (1)
   (`RemoteLeaderEndPoint.scala:62-63`, `:213 (buildFetch)`). Because the request is non-empty,
   `maybeFetch` does **not** reach `partitionMapCond.await(fetchBackOffMs)`
   (`AbstractFetcherThread.scala:129-132 (maybeFetch)`).
4. `F -> L`: `FETCH(tp, fetchOffset=S, currentLeaderEpoch=N, maxWait=500, minBytes=1)`.
5. `L`: diskless topic, `S >= 0`, `fetchOffset >= S` so `shouldReadFromUnifiedLog` stays false ->
   `disklessSwitchCompleted` -> the at-seal block records `F`'s fetch state at `S` and
   `maybeExpandIsr` submits an `AlterPartition` (`ReplicaManager.scala:2510-2554`,
   `Partition.scala:1010-1028 (maybeExpandIsr)`). The response is appended to
   `immediateFetchResponses` as `Errors.NONE, HW=S, records=EMPTY` (`:2562-2575`).
   - Sub-case: if `L`'s image does not yet carry `F`'s new broker epoch,
     `isReplicaIsrEligible` fails on the stored-vs-cached epoch
     (`Partition.scala:1068-1071 (isBrokerEpochIsrEligible)`) and **no** `AlterPartition` is
     submitted on this iteration, which lengthens the loop.
6. `L`: `classicFetchInfos.isEmpty && disklessFetchInfos.isEmpty` -> `respond(Seq.empty); return`
   (`ReplicaManager.scala:2625-2628 (fetchMessages)`). <- **INVARIANT BROKEN.** The invariant is
   *A follower FETCH carrying `minBytes > 0` and `maxWait > 0` that cannot be satisfied must park in
   `delayedFetchPurgatory` until `minBytes` is available or `maxWait` elapses.* The partition never
   enters the purgatory, so the `maxWait=500` and `minBytes=1` from step 4 are discarded and the
   response returns in microseconds with zero bytes. **This is why `replica.fetch.wait.max.ms` never
   applies.**
7. `L -> F`: `FETCH_RESPONSE(Errors.NONE, HW=S, records=[])`.
8. `F`: `Errors.NONE` and no diverging epoch -> `processPartitionData`
   (`AbstractFetcherThread.scala:374-380`); appends EMPTY, `log.maybeUpdateHighWatermark(S)`.
9. `F`: eviction gate - `classicToDisklessStartOffset >= 0` [ok], `log.logEndOffset >= S` [ok],
   `isConsolidatingPartition` false, and `isReplicaInIsr(tp, F)` reads `F`'s own `currentImage()`,
   which still excludes `F` because step 5's `AlterPartition` has not been committed and propagated
   yet -> **no eviction** (`ReplicaFetcherThread.scala:180-185`).
   - **Pre-PR this is exactly where the loop terminated**: the gate was `LEO >= S` alone, so the
     partition was buffered and removed from the fetcher in the same `doWork`
     (`ReplicaFetcherThread.scala:197-204 (evictFullySwitchedDisklessPartitions)`).
10. `F`: back in `processFetchRequest`, `validBytes == 0` and `currentFetchState.lag` is present, so
    the guard at `AbstractFetcherThread.scala:388` is false and
    `partitionStates.updateAndMoveToEnd` at `:394` is **skipped** - the fetch state is left
    byte-identical to step 3.
11. `F`: `doWork` returns and `ShutdownableThread.run` calls it again with **no sleep**
    (`ShutdownableThread.java:135-136`) -> **step 3 repeats identically.** Loop period is one leader
    round trip, roughly 0.1 to 1 ms on a LAN, i.e. ~1k-10k FETCH/s per fetcher thread. There is no
    backoff at any point in steps 3-11.
12. Exit: only once step 5's `AlterPartition` is committed by `C` **and** the resulting delta reaches
    `F`, so that step 9's `isReplicaInIsr` returns true. Duration = AlterPartition round trip +
    metadata propagation.

Branch - `F` in controlled shutdown (this is S1's leg, and it has **no exit**):

- 1b. `F` begins controlled shutdown; `C` removes `F` from the ISR, producing the same delta as step 1.
- 2b. `F`: the switched-diskless branch has no `isInControlledShutdown` guard, unlike the classic
  branch at `ReplicaManager.scala:4191-4195`, so the fetcher is started anyway.
- 5b. `L`: `maybeExpandIsr` -> `canAddReplicaToIsr` -> `isReplicaIsrEligible` ->
  `!metadataCache.isBrokerShuttingDown(F)` is **false** (`Partition.scala:1063-1064`) -> **no
  `AlterPartition` is ever submitted.** <- the only exit condition in step 12 is now unreachable, so
  steps 3-11 repeat at the step-11 rate until `F`'s JVM exits.

**Evidence**: `static reasoning` for the spin itself (a hot loop is not directly assertable in a unit
test). The *mechanisms* it rests on are verified: `buildFetch` skips delayed partitions
(`RemoteLeaderEndPoint.scala:179` + `PartitionFetchState.java:66,78`), and the fix's effect is
asserted by a new test.

```
./gradlew :core:test --tests "kafka.server.ReplicaFetcherThreadTest"
→ 20 tests, 0 failures; shouldDelayPartitionAtSealWhileMetadataIsrDoesNotContainReplica PASSED
```

Negative control — with `delayPartitions(...)` neutralised:

```
→ 20 tests, 1 failure
FAILED shouldDelayPartitionAtSealWhileMetadataIsrDoesNotContainReplica (ReplicaFetcherThreadTest.scala:826)
  AssertionFailedError: Partition must be delayed after the at-seal fetch, got
  FetchState(topicId=..., fetchOffset=100, currentLeaderEpoch=0, lastFetchedEpoch=Optional[0],
  state=Fetching, lag=Optional.empty, delay=0ms) ==> expected: <true> but was: <false>
```

**Patch**: `4d44549317` (also delivers S2 and S5).

```scala
// core/src/main/scala/kafka/server/AbstractFetcherThread.scala:810
-  private def delayPartitions(partitions: Iterable[TopicPartition], delay: Long): Unit = {
+  protected def delayPartitions(partitions: Iterable[TopicPartition], delay: Long): Unit = {

// core/src/main/scala/kafka/server/ReplicaFetcherThread.scala
+  // Partitions at the seal that cannot be evicted yet because the controller has not put this
+  // replica back in ISR. Visible for testing.
+  private[server] val partitionsAwaitingIsrRecovery = mutable.Buffer[TopicPartition]()

   override def doWork(): Unit = {
     super.doWork()
     completeDelayedFetchRequests()
     evictFullySwitchedDisklessPartitions()
+    backOffPartitionsAwaitingIsrRecovery()
   }

-    val isConsolidatingPartition =
+    def isConsolidatingPartition: Boolean =
       brokerConfig.disklessRemoteStorageConsolidationEnabled &&
         inklessMetadataView.isConsolidatingDisklessTopic(topicPartition.topic)
     if (shouldEvictFullySwitchedDisklessPartitions &&
         classicToDisklessStartOffset >= 0 &&
-        log.logEndOffset >= classicToDisklessStartOffset &&
-        (isConsolidatingPartition || inklessMetadataView.isReplicaInIsr(topicPartition, brokerConfig.brokerId))) {
-      partitionsToEvictAfterDisklessSwitch += topicPartition
+        log.logEndOffset >= classicToDisklessStartOffset) {
+      if (isConsolidatingPartition || inklessMetadataView.isReplicaInIsr(topicPartition, brokerConfig.brokerId)) {
+        partitionsToEvictAfterDisklessSwitch += topicPartition
+      } else {
+        // The leader answers an at-seal follower fetch from immediateFetchResponses without entering
+        // the fetch purgatory, so the response returns at once with no records and the request's
+        // maxWaitMs is not honoured. Back off explicitly or this re-fetches as fast as the network
+        // allows until the ISR expansion lands.
+        partitionsAwaitingIsrRecovery += topicPartition
+      }
     }

+  // Visible for testing.
+  private[server] def backOffPartitionsAwaitingIsrRecovery(): Unit = {
+    if (partitionsAwaitingIsrRecovery.nonEmpty) {
+      val toDelay = partitionsAwaitingIsrRecovery.toSet
+      partitionsAwaitingIsrRecovery.clear()
+      delayPartitions(toDelay, brokerConfig.replicaFetchBackoffMs.toLong)
+    }
+  }
```

Why the drain lives in `doWork` and not inline: `processFetchRequest` can overwrite the fetch state
immediately after `processPartitionData` returns
(`AbstractFetcherThread.scala:388-394`, taken when `currentFetchState.lag.isEmpty`) and would drop a
delay set inline.

**Alternatives rejected** (this is D2):

- *Route the at-seal response through the fetch purgatory so `maxWaitMs` applies.* `DelayedFetch`
  would have to carry a partition class it must never read plus a completion predicate that never
  fires; it changes the delayed-fetch path shared by every diskless and consolidating fetch, and
  holds a purgatory slot per recovering partition on the leader. Much larger blast radius for the
  same effect.
- *The controlled-shutdown guard alone (S1).* Bounds only the worst case; the ordinary
  `AlterPartition` plus metadata round trip still spins, and the pre-existing `hw < seal` leg can
  still spin during shutdown.

**Comment draft**: [A1](#a1-b2-coresrcmainscalakafkaserverreplicafetcherthreadscala180-185-the-at-seal-fetch-loop-has-no-backoff-high) in Appendix A.


---

### B3. `core/src/main/scala/kafka/server/ReplicaManager.scala:2518-2523` — leader-epoch and leadership errors collapse to `Errors.NONE`, and the test that covers it is vacuous (high)

**Risk**: the commit series claims epoch validation, but the two pre-checks in front of `fetchRecords`
suppress exactly the errors upstream uses to fence or throttle a mis-epoched follower — which, after
the ISR-gated eviction, are the follower's only backoff signal. And the one test covering it cannot
detect any of this.

**Scenarios**:

- **Leader epoch mismatch** (`:2519-2521`). Without the pre-check, `fetchRecords` →
  `localLogWithEpochOrThrow` → `checkCurrentLeaderEpoch` throws `FencedLeaderEpochException`
  (request epoch below the leader's) or `UnknownLeaderEpochException` (above) — both already in the
  catch list at `:2544-2551`. Upstream handling is what the follower needs: `FENCED_LEADER_EPOCH` →
  `onPartitionFenced` drops the partition until the next metadata state; `UNKNOWN_LEADER_EPOCH` →
  `delayPartitions(fetchBackOffMs)`. Both cases are routine, not exotic: an epoch one above the
  leader's is the ordinary leader-election race, where the controller bumps the epoch and the
  follower applies the metadata delta before the leader applies its own. Upstream has a dedicated
  error code for precisely that.
- **Replica not in the leader's replica set** (`:2523`). After a reassignment the leader drops the
  departed replica from `remoteReplicasMap` (`Partition.scala:983`), so `getReplica(replicaId)` is
  empty and the block no-ops. Without the guard, `followerReplicaOrThrow` returns
  `UNKNOWN_LEADER_EPOCH` — and upstream's comment there names precisely this case
  (`Partition.scala:1493-1497`: "possible in KRaft, for example, when new replicas are added as part
  of a reassignment ... which causes the follower to retry"). Note the binding is discarded
  (`{ _ => }`), so the `Option` is being used purely as an `if`.
  **Reachability corrected in Round 2**: I first framed this as the *non-leader* case (a follower
  broker answering, via `updateAssignmentAndIsr:985`'s `remoteReplicasMap.clear()`). Writing the
  trace killed that framing — see the trace's note under cause C.
- **B3.1, the vacuous test.** `testFollowerFetchAtSealSkipsFetchStateAndIsrExpansionWhenLeaderEpochStale`
  (`:7361`) passes with the *entire* leader-side block reverted to the merge base. It asserts
  `Errors.NONE` at `:7395`, the LEO unchanged, and no `AlterPartition` — all of which the pre-PR code
  did unconditionally. It cannot fail if the feature is deleted.
- **N2, the labelling, three ways.** The name says `...WhenLeaderEpochStale` (stale = older = `-1` =
  `FENCED_LEADER_EPOCH`); the comment at `:7379` says `ahead-of-leader` (= `+1` =
  `UNKNOWN_LEADER_EPOCH`); the code does `+1`. Name contradicts comment, and name contradicts code.
  The comment's second half — "mirrors the classic read path, which validates the request epoch
  before touching follower state" — is also wrong: the classic path validates *and returns the
  error*. That gap is this finding.

**Trace**

Four causes reach the same wrong response by different routes, so one trace each. Participants: `F` =
fetching follower, `L` = the broker answering, `C` = controller. Shared preconditions: topic
`diskless.enable=true`, seal committed at `S`, `F` sends `FETCH(fetchOffset=S)` so the at-seal block
at `ReplicaManager.scala:2517` is entered.

#### Cause A — request epoch above the leader's (`+1`, should be `UNKNOWN_LEADER_EPOCH`)

Extra precondition: `C` has bumped the leader epoch to `N+1`; `F` has applied that delta, `L` has not
(`L`'s `partition.getLeaderEpoch == N`). This is the ordinary leader-election race, not an exotic state.

1. `C -> F`: delta with leader epoch `N+1`. `F` restarts its fetcher at `currentLeaderEpoch = N+1`.
2. `C -> L`: same delta, not yet applied.
3. `F -> L`: `FETCH(fetchOffset=S, currentLeaderEpoch=N+1)`.
4. `L`: `requestEpochMatchesLeader` evaluates `N+1 == N` -> false -> **the entire block is skipped**
   (`ReplicaManager.scala:2519-2521`). No validation, no fetch-state record, no error.
   - Without the pre-check: `partition.fetchRecords` -> `localLogWithEpochOrThrow` ->
     `getLocalLog:520` -> `checkCurrentLeaderEpoch:511-512` -> `localLeaderEpoch(N) <
     remoteLeaderEpoch(N+1)` -> `UNKNOWN_LEADER_EPOCH`, thrown and caught at `:2544-2551`.
5. `L -> F`: `Errors.NONE, HW=S, records=[]`. <- **INVARIANT BROKEN.** Invariant: *A leader must not
   answer a FETCH whose `(replicaId, leaderEpoch)` tuple it has not validated with a success code.*
   `NONE` asserts "your position is accepted" when nothing was checked.
6. Follower behaviour, which is the actual risk:
   - with `Errors.NONE`: steps 8-11 of B2's trace - appends empty, cannot evict (out of ISR), spins
     with no backoff until `L` applies the delta.
   - with `UNKNOWN_LEADER_EPOCH`: `AbstractFetcherThread.scala:423-426` -> `partitionsWithError` ->
     `handlePartitionsWithErrors:143-147` -> `delayPartitions(tp, fetchBackOffMs)` -> one backed-off
     retry.

#### Cause B — request epoch below the leader's (`-1`, should be `FENCED_LEADER_EPOCH`)

Extra precondition: the mirror image - `L` has applied leader epoch `N+1`, `F`'s fetcher is still at `N`.

1. `C` bumps the leader epoch to `N+1`; `L` applies it via `makeLeader`, `F` has not.
2. `F -> L`: `FETCH(fetchOffset=S, currentLeaderEpoch=N)`.
3. `L`: `requestEpochMatchesLeader` evaluates `N == N+1` -> false -> block skipped.
   - Without the pre-check: `checkCurrentLeaderEpoch:509-510` -> `localLeaderEpoch(N+1) >
     remoteLeaderEpoch(N)` -> `FENCED_LEADER_EPOCH`.
4. `L -> F`: `Errors.NONE, HW=S`. <- **INVARIANT BROKEN**, same invariant as cause A, and additionally
   `F` is told it is caught up *against a leader epoch that no longer exists*.
5. Follower behaviour:
   - with `Errors.NONE`: `F` keeps fetching at the dead epoch. The exit is not automatic - `F`'s
     eviction gate needs `isReplicaInIsr`, and `L` skipped the block so it never expands the ISR, so
     `F` spins until its own delta for `N+1` arrives.
   - with `FENCED_LEADER_EPOCH`: `AbstractFetcherThread.scala:428-430` -> `onPartitionFenced:303-316`
     -> `requestEpoch(N) == currentFetchState.currentLeaderEpoch(N)` -> `markPartitionFailed:495` ->
     the partition leaves the fetcher and waits for the new LeaderAndIsr. No spin at all.

#### Cause C — `F` is not in the leader's replica set (should be `UNKNOWN_LEADER_EPOCH`)

Extra precondition: a reassignment removed `F` from the replica set. It bumps `partitionEpoch` while
leaving `leaderEpoch` unchanged, which is what lets the request epoch still match.

1. `C -> L`: reassignment delta. `partitionEpoch` changed, so the partition lands in
   `localChanges.leaders` (`TopicDelta.java:190-200`), and `makeLeader` -> `updateAssignmentAndIsr`
   drops `F` via `remoteReplicasMap.keySet.removeIf` (`Partition.scala:983`). `leaderEpoch` is still `N`.
2. `F` (delta not yet applied) `-> L`: `FETCH(fetchOffset=S, currentLeaderEpoch=N)`.
3. `L`: `requestEpochMatchesLeader` -> `N == N` -> true, so the pre-check passes. Then
   `partition.getReplica(F)` -> **empty** -> `.foreach` no-ops (`ReplicaManager.scala:2523`).
   - Without the guard: `partition.fetchRecords` -> `followerReplicaOrThrow` ->
     `UNKNOWN_LEADER_EPOCH` (`Partition.scala:1492-1498`), whose own comment names this exact case:
     "possible in KRaft, for example, when new replicas are added as part of a reassignment ... which
     causes the follower to retry".
4. `L -> F`: `Errors.NONE, HW=S`. <- **INVARIANT BROKEN**, the same invariant as cause A (I1) - here it
   is the `replicaId` half of the `(replicaId, leaderEpoch)` tuple that went unvalidated rather than
   the epoch half.
5. Follower behaviour: with `NONE`, `F` advances its HW to `S` on the strength of a reply that
   validated nothing and keeps fetching; with `UNKNOWN_LEADER_EPOCH`, `delayPartitions` backs it off
   until its own delta arrives and removes the partition.

**Reachability correction — my first framing of this cause was wrong.** I originally wrote it as "`L`
is not the leader", a follower broker answering, because `updateAssignmentAndIsr:985` clears
`remoteReplicasMap` outright when `isLeader` is false. Writing the trace killed that: `leader` and
`leaderEpoch` live in the same `PartitionRegistration`, so if `F` believes "leader = L at epoch N" and
`L` also holds epoch `N`, then `L` holds `leader = L` too and *is* the leader. Any real leadership move
bumps the epoch, so `requestEpochMatchesLeader` catches it first and the exchange is cause B, not
cause C. The reassignment path above is the reachable one.

Consequence for the patch: the fix is unaffected, since dropping the guard yields the right error
either way. Consequence for my test: `testFollowerFetchAtSealFromNonLeaderReturnsNotLeaderOrFollower`
pins the guard's behaviour through a *constructed* non-leader state rather than a
production-reachable one, and asserts `NOT_LEADER_OR_FOLLOWER` where the reachable cause yields
`UNKNOWN_LEADER_EPOCH`. It still fails before the patch and passes after, so it is a valid
characterisation of the guard removal - but a reassignment-shaped test would be the honest one.
Flagged rather than quietly rewritten.

#### Cause D — leader's log directory is offline (should be `KAFKA_STORAGE_ERROR`); NOT fixed by my patch

Extra precondition: an IO failure on `L` marked the partition offline, so
`allPartitions(tp) = HostedPartition.Offline`.

1. `L`: log-dir failure -> `markPartitionOffline(tp)`.
2. `F -> L`: `FETCH(fetchOffset=S, currentLeaderEpoch=N)`.
3. `L`: `getPartitionOrError(tp)` -> `Left(Errors.KAFKA_STORAGE_ERROR)` (`ReplicaManager.scala:755-756`).
4. `L`: `Either.foreach` runs only on `Right`, so the `Left` is **discarded**
   (`ReplicaManager.scala:2518`). <- **INVARIANT BROKEN.** Invariant: *A broker whose log directory has
   failed must not answer reads for that partition with a success code.* Contrast the consolidating
   branch 60 lines above (`:2483-2500`), which does handle its `Left(error)` and returns it.
5. `L -> F`: `Errors.NONE, HW=S`.
6. Follower behaviour: with `NONE`, `F` advances its HW against a leader that cannot serve the data;
   with `KAFKA_STORAGE_ERROR` it hits the catch-all at `AbstractFetcherThread.scala:461-463` ->
   `delayPartitions`, while the controller's log-dir failure handling moves leadership.

This cause is deliberately **not** addressed by `764c1adb77` - see the "not included" note below.

#### B3.1 — the vacuous test, as two runs of one exchange

Same preconditions and same message exchange as cause A: seal `S = 5`, leader epoch `0`, request epoch
`0 + 1 = 1`, follower out of ISR. The only difference between the runs is the code on `L`.

Run 1, baseline `f751436ad0` (no at-seal block exists at all):

1. `F -> L`: `FETCH(fetchOffset=5, currentLeaderEpoch=1)`.
2. `L`: `params.isFromFollower && disklessSwitchCompleted` -> true. The baseline body has no
   validation of any kind; it appends
   `(Errors.NONE, classicToDisklessStartOffset, 0L, MemoryRecords.EMPTY, Optional.empty(), ...)`.
3. `L -> F`: `Errors.NONE, HW=5, records=[]`.
4. `L` state afterwards: `partition.getReplica(F).stateSnapshot.logEndOffset == UNKNOWN_OFFSET` (never
   touched), `partition.inSyncReplicaIds` excludes `F`, `alterPartitionManager.submit` never called.

Run 2, PR head `8d537372`:

1. `F -> L`: identical FETCH.
2. `L`: the at-seal block is entered, then `requestEpochMatchesLeader` -> `1 == 0` -> false -> block
   skipped. The response is built from the *initial* values of the two locals declared at
   `ReplicaManager.scala:2511-2512`, which are `Errors.NONE` and `Optional.empty()` - byte-identical
   to the baseline literals in run 1 step 2.
3. `L -> F`: identical response.
4. `L` state afterwards: identical - LEO `UNKNOWN_OFFSET`, `F` out of ISR, no submit.

Assertion by assertion, at PR-head line numbers:

- `:7395 assertEquals(Errors.NONE, data.error)` - run 1 holds, run 2 holds
- `:7396 assertEquals(MemoryRecords.EMPTY, data.records)` - holds, holds
- `:7397 assertEquals(sealOffset, data.highWatermark)` - holds, holds
- `:7401 assertEquals(UNKNOWN_OFFSET, ...stateSnapshot.logEndOffset)` - holds, holds
- `:7403 assertFalse(partition.inSyncReplicaIds.contains(followerId))` - holds, holds
- `:7404 verify(alterPartitionManager, never()).submit(...)` - holds, holds

Every assertion holds identically in both runs, which is what "asserts the pre-PR behaviour" means
concretely. The structural reason is step 2 of run 2: the test asserts only the *absence* of effects,
and a skipped block is indistinguishable from no block because `fetchError` and `divergingEpoch` are
initialised to exactly the values the baseline hard-coded. Confirmed empirically by R1 - with the
block reverted this test PASSED while its four siblings failed.

**Evidence**:

R1 established the vacuity (see the revert experiment). For the fix, before and after:

```
# with the pre-checks still in place (patch stashed)
./gradlew :core:test --tests "kafka.server.ReplicaManagerInklessTest"
→ 171 tests, 3 failures
  testFollowerFetchAtSealWithFencedLeaderEpochSkipsFetchState   expected: <FENCED_LEADER_EPOCH>   but was: <NONE>
  testFollowerFetchAtSealWithNewerLeaderEpochSkipsFetchState    expected: <UNKNOWN_LEADER_EPOCH>  but was: <NONE>
  testFollowerFetchAtSealFromNonLeaderReturnsNotLeaderOrFollower expected: <NOT_LEADER_OR_FOLLOWER> but was: <NONE>

# with the patch
→ 171 tests, 0 failures
```

**Patch**: `764c1adb77` — drops both pre-checks; splits the mislabelled test into
`testFollowerFetchAtSealWithNewerLeaderEpochSkipsFetchState` (`+1`) and
`testFollowerFetchAtSealWithFencedLeaderEpochSkipsFetchState` (`-1`) behind a shared helper; adds
`testFollowerFetchAtSealFromNonLeaderReturnsNotLeaderOrFollower`; deletes the "mirrors the classic
read path" claim.

```scala
-              getPartitionOrError(tp.topicPartition).foreach { partition =>
-                val requestEpochMatchesLeader =
-                  fetchPartitionData.currentLeaderEpoch.toScala.forall(_.intValue() == partition.getLeaderEpoch)
-                if (requestEpochMatchesLeader) {
-                  try {
-                    partition.getReplica(params.replicaId).foreach { _ =>
-                      // Use the classic follower-read validation without returning any records.
-                      val fetchAtSeal = new PartitionData(...)
-                      val readInfo = partition.fetchRecords(...)
-                      divergingEpoch = readInfo.divergingEpoch
-                    }
-                  } catch { ... }
-                }
-              }
+              getPartitionOrError(tp.topicPartition).foreach { partition =>
+                try {
+                  // Read at the seal rather than at the request's fetch offset: the leader's classic
+                  // log is frozen there, so recording exactly the seal keeps
+                  // ReplicaState.isCaughtUp's leaderEndOffset == logEndOffset leg true and the
+                  // follower stays caught up without another fetch. maxBytes = 0 runs the classic
+                  // follower-read validation (leader epoch, replica membership, divergence) without
+                  // returning any records.
+                  val fetchAtSeal = new PartitionData(...)
+                  divergingEpoch = partition.fetchRecords(...).divergingEpoch
+                } catch { ... }
+              }
```

**Deliberately not included** — propagating `getPartitionOrError`'s `Left`. It is correct in
principle (an offline log dir is `KAFKA_STORAGE_ERROR`; a partition not hosted here is
`NOT_LEADER_OR_FOLLOWER`, and neither should be answered with "you are caught up"). But I measured
the cost: it additionally breaks two **pre-existing** tests that never create a local `Partition`.

```
# with `case Left(error) => fetchError = error` added
→ 171 tests, 3 failures  (the epoch one, plus:)
  testFollowerFetchAtClassicToDisklessStartOffsetReturnsEmptyAndIdle          expected: <NONE> but was: <UNKNOWN_TOPIC_OR_PARTITION>
  testFollowerFetchAtClassicToDisklessStartOffsetEmptyEvenWhenManagedReplicasDisabled  expected: <NONE> but was: <UNKNOWN_TOPIC_OR_PARTITION>
```

Those two stub only the metadata view, so the `Left` there is a mock shortcut rather than a
production state — but fixing them properly means giving both a full leader+follower setup. Left as a
separate call for the author.

**Comment draft**: [A2](#a2-b31-the-vacuous-test-as-two-runs-of-one-exchange) in Appendix A.


---

## Suggestions

### S1. `core/src/main/scala/kafka/server/ReplicaManager.scala:4151-4152` — no controlled-shutdown guard, so `isOutOfIsr` starts a fetcher that can never evict (medium)

**Risk**: during this broker's own controlled shutdown the new clause starts a catch-up fetcher for a
partition the leader will refuse to re-admit. This is the unbounded leg of B2.

**Scenario**: the controller removes this broker from ISR, bumping `partitionEpoch`, which lands the
partition in `localChanges.followers` (`TopicDelta.localChanges:203-208`); `isOutOfIsr` is true; the
`hw < seal` clause was false so pre-PR nothing happened. On the leader, `isReplicaIsrEligible` returns
false for the whole shutdown (`Partition.scala:1064`, `isBrokerShuttingDown`). The sibling branch at
`:4190-4194` has exactly this guard.

**Evidence**: `static reasoning only`, plus compile and full-suite verification of the patch. I did
not build a controlled-shutdown integration test.

**Patch**: `a507ae1a23`.

```scala
+              // A shutting-down broker is not ISR-eligible on the leader (isReplicaIsrEligible), so a
+              // catch-up fetch started here could never complete; the classic branch below stops
+              // fetching in the same situation.
-              val isOutOfIsr = !info.partition.isr.contains(config.brokerId)
+              val isOutOfIsr = !isInControlledShutdown && !info.partition.isr.contains(config.brokerId)
```

Not sufficient alone: the pre-existing `hw < seal` leg can also start a fetcher during controlled
shutdown that can no longer evict at the seal. B2's backoff is what keeps that slow rather than a spin.

**Comment draft**: [A3](#a3-s1-coresrcmainscalakafkaserverreplicamanagerscala4151-4152-no-controlled-shutdown-guard-so-isoutofisr-starts-a-fetcher-that-can-never-evict-medium) in Appendix A.


---

### S2. `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:177-179` — two `Properties` copies per partition per fetch on the replication hot path (medium)

**Risk**: `processPartitionData` runs once per partition per fetch response. The `val` is evaluated
before the `if`, so it runs unconditionally when consolidation is enabled — including for pure classic
partitions that can never take the branch. Same cost lands on `ConsolidationFetcherThread`, which
inherits the method and for which the predicate is always true.

**Evidence**: the copy is verified at source, not assumed. `isConsolidatingDisklessTopic`
(`InklessMetadataView.scala:78`) calls `isDisklessTopic` and `isRemoteStorageEnabled`
(`:70`, `:74`), each `metadataCache.topicConfig(name)` →
`KRaftMetadataCache.config:466` → `ConfigurationsImage.configProperties:54` →
`ConfigurationImage.toProperties:42-46`:

```java
public Properties toProperties() {
    Properties properties = new Properties();
    properties.putAll(data);
    return properties;
}
```

A fresh allocation plus a full copy of every topic config entry, twice per call. By contrast the
existing `getClassicToDisklessStartOffset` lookup is a volatile read plus two map gets.

**Patch**: delivered inside `4d44549317` — `val` → `def`, evaluated inside the
`classicToDisklessStartOffset >= 0` guard, so classic partitions never reach it. `-Xfatal-warnings`
now enforces that the `def` stays referenced.

**Comment draft**: [A4](#a4-s2-coresrcmainscalakafkaserverreplicafetcherthreadscala177-179-two-properties-copies-per-partition-per-fetch-on-the-replication-hot-path-medium) in Appendix A.


---

### S3 (was B1). `tests/kafkatest/tests/inkless/inkless_topic_switch_test.py:1301` — the system test cannot observe the mechanism it is named for (medium, downgraded from blocker)

**Risk**: the PR advertises "a URP-recovery system test", but its scenario is healed by the controller
before any at-seal follower fetch happens. The unit coverage does guard the fix (revert experiment),
so this is a claim/scope problem rather than an unguarded fix.

**Scenario**: the recovery step is an unclean stop then start — a fence → unfence transition. On
unfence the controller re-adds the broker to the ISR of every diskless partition, switched hybrids
included, with no reference to the seal. So the sequence at `:1316-1326` goes URP=1 on fence, URP=0 on
unfence, and `_wait_for_all_partitions_isr_full` also passes, without the leader ever recording an
at-seal fetch.

The mechanism *is* needed where there is no fence/unfence edge — a replica newly added to an
already-switched partition (RF increase via reassignment), or a re-shrink after the controller's
one-shot expansion. That is what a replacement test should drive.

**Evidence**: Round 1 argued this statically. Round 2 **proved the premise** with a passing
characterisation test at the controller level (`dc415a8949`, and see D4):

```
./gradlew :metadata:test --tests "org.apache.kafka.controller.ReplicationControlManagerInklessTest"
→ BUILD SUCCESSFUL; DisklessManagedReplicasTests > testUnfenceExpandsIsrOfSwitchedPartitionRegardlessOfSeal() PASSED
   (seal committed at 100, broker 2 fenced then unfenced, broker 2 back in ISR, seal untouched)
```

**The end-to-end claim itself remains unrun.** The ducktape suite needs a multi-node
vagrant/docker cluster that is not available here. I did not run
`test_switched_topic_urp_clears_after_replica_recovery` and am not claiming it fails or passes —
only that the controller path it depends on heals the scenario.

**Fix draft** (no commit — this one needs a decision first, and depends on D4):

```python
# current — healed by the controller on unfence
        follower = self._get_follower_nodes(partition=0)[0]
        self._stop_broker(follower, clean_shutdown=False)
        self._wait_for_under_replicated_partitions(1, at_least=True)
        self._start_broker(follower)
        self._wait_for_all_partitions_isr_full(num_partitions=1)
        self._wait_for_under_replicated_partitions(0)

# proposed — no fence/unfence edge, so only the at-seal fetch can clear it
#   self._create_classic_topic(num_partitions=1, replica_assignment="1:2")
#   ... switch, wait ...
#   self._reassign_partition(partition=0, replicas=[1, 2, 3])
#   self._wait_for_under_replicated_partitions(1, at_least=True)
#   self._wait_for_all_partitions_isr_full(num_partitions=1)
#   self._wait_for_under_replicated_partitions(0)
```

Marked **unverified**: there is no `_reassign_partition` helper in the suite today, and whether
reassignment on a switched diskless topic is supported on `main` is PR #724's territory. If it is not,
the honest move is to keep the restart test as a cheap guard and narrow the PR body's claim to
unit-level.

**Comment draft**: [A5](#a5-s3-was-b1-testskafkatesttestsinklessinkless_topic_switch_testpy1301-the-system-test-cannot-observe-the-mechanism-it-is-named-for-medium-downgraded-from-blocker) in Appendix A.


---

### S4. `core/src/main/scala/kafka/server/ReplicaManager.scala:4156` and `:2559-2560` — two comments describe behaviour this PR removed (low)

**Risk**: the next reader concludes the fetcher self-terminates at the seal, which is exactly the
assumption B2 breaks.

**Evidence**: confirmed against the merge base with
`git show f751436ad0:core/src/main/scala/kafka/server/ReplicaManager.scala` — `:4156` is unchanged
from pre-PR while the behaviour it describes changed at `:183`.

**Patch**: `aae665d2e9`.

```scala
-                // were just added as a replica and have an empty local log. The
-                // ReplicaFetcher self-evicts once the follower has read past the seal.
-                // Also schedule one fetch when this replica is already caught up but out
-                // of ISR, so the leader observes its fetch state and can expand ISR.
+                // were just added as a replica and have an empty local log -- or when we are
+                // already at the seal but out of ISR, so the leader observes our fetch state
+                // and can expand ISR. The ReplicaFetcher self-evicts once the local log has
+                // reached the seal AND the controller has put this replica back in ISR.

-            // partition as caught up and goes idle, rather than treating it as out of range.
+            // partition as caught up rather than out of range. The fetcher keeps polling (with a
+            // backoff) until the controller puts this replica back in ISR.
```

**Comment draft**: [A6](#a6-s4-coresrcmainscalakafkaserverreplicamanagerscala4156-and-2559-2560-two-comments-describe-behaviour-this-pr-removed-low) in Appendix A.


---

### S5. `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:171-174` — the eviction comment omits the consolidating exemption (low)

**Risk**: the comment describes only the ISR leg while the code has two, and the consolidating
exemption is the non-obvious one — why a consolidating follower must *not* wait for ISR.

**Evidence**: independently found in Round 1; Copilot flagged and suppressed the same thing.

**Patch**: delivered inside `4d44549317`.

```scala
-    // Stop fetching after the switch from classic to diskless is completed: once the controller
-    // has committed a classicToDisklessStartOffset for this partition, our local LEO has reached it,
-    // and this replica is in ISR, the follower is fully caught up to the leader's frozen classic log
-    // and must not keep fetching.
+    // Stop fetching once the switch from classic to diskless is complete: the controller has
+    // committed a classicToDisklessStartOffset, our local LEO has reached it, and the controller has
+    // put this replica back in ISR. A consolidating partition is exempt from the ISR condition
+    // because it hands off to the consolidation fetcher at the seal and must not wait on ISR to do
+    // it.
```

**Comment draft**: [A7](#a7-s5-coresrcmainscalakafkaserverreplicafetcherthreadscala171-174-the-eviction-comment-omits-the-consolidating-exemption-low) in Appendix A.


---

### S6. `core/src/test/scala/unit/kafka/server/ReplicaManagerInklessTest.scala:7182` — the ISR assertion does not check the submitted set (low)

**Risk**: `verify(...).submit(any(), any())` proves an `AlterPartition` went out, not that it expands
the ISR to include the recovering follower — which is the invariant the test name claims.

**Evidence**: patch verified green in the 382-test run.

**Patch**: `ebac227ddc`.

```scala
-      verify(alterPartitionManager).submit(any(), any())
+      val isrCaptor: ArgumentCaptor[LeaderAndIsr] = ArgumentCaptor.forClass(classOf[LeaderAndIsr])
+      verify(alterPartitionManager).submit(any(), isrCaptor.capture())
+      assertTrue(isrCaptor.getValue.isr.contains(followerId),
+        s"AlterPartition must expand the ISR to include the recovered follower, got ${isrCaptor.getValue.isr}")
```

**Comment draft**: [A8](#a8-s6-coresrctestscalaunitkafkaserverreplicamanagerinklesstestscala7182-the-isr-assertion-does-not-check-the-submitted-set-low) in Appendix A.


---

### S7. Untested branches: the consolidating eviction leg, and `isReplicaInIsr` with the partition absent (medium / low)

**Risk**: the consolidating leg is load-bearing — without it a consolidating follower would sit at the
seal waiting for ISR and never hand off through
`startConsolidationFetchersForCaughtUpClassicPartitions` — and it was dead in every existing test.
`isReplicaInIsr`'s partition-missing branch decides whether a fetcher keeps running when its
partition is absent from a freshly published image.

**Evidence**: the consolidating leg was dead because `TestUtils.createBrokerConfig` leaves
consolidation off, so `brokerConfig.disklessRemoteStorageConsolidationEnabled` short-circuits. Turning
it on needs a four-config chain, which is presumably why the case was skipped:

```
requirement failed: diskless.remote.storage.consolidation.enable requires remote.log.storage.system.enable=true
requirement failed: diskless.remote.storage.consolidation.enable requires diskless.allow.from.classic.enable=true
```

With the new case in place, `20 tests, 0 failures`. Negative control — with the consolidating leg
disabled (kept referenced, since removing it trips `-Xfatal-warnings`):

```
→ 20 tests, 1 failure
FAILED shouldEvictConsolidatingPartitionAtSealEvenWhenNotInMetadataIsr
  Wanted but not invoked: replicaFetcherManager.removeFetcherForPartitions(Set(topic1-0))
```

The `FENCED_LEADER_EPOCH` half of this finding shipped in B3's patch instead.

**Patch**: `ebac227ddc`.

```scala
+  @Test
+  def shouldEvictConsolidatingPartitionAtSealEvenWhenNotInMetadataIsr(): Unit = {
+    // The consolidating leg of the eviction gate: a consolidating follower hands off to the
+    // consolidation fetcher at the seal and must not wait for ISR to do it.
+    verifyDisklessSwitchEviction(
+      classicToDisklessStartOffset = 100L, logEndOffsetAfterAppend = 100L,
+      expectEviction = true, replicaInIsr = false, consolidating = true)
+  }

+  @Test
+  def testIsReplicaInIsrReturnsFalseWhenPartitionMissing(): Unit = {
+    val tp = new TopicPartition("switched", 1)
+    stubImageTopic(tp.topic(), util.Map.of(Integer.valueOf(0), partitionRegistration()))
+    assertFalse(metadataView.isReplicaInIsr(tp, 1))
+  }
```

**Comment draft**: [A9](#a9-s7-untested-branches-the-consolidating-eviction-leg-and-isreplicainisr-with-the-partition-absent-medium-low) in Appendix A.


---

## Questions / confirm-intent

### Q1. `core/src/main/scala/kafka/server/ReplicaManager.scala:2527` — the clamp to the seal is deliberate; now documented

**Resolved as correct.** I wanted it on the record so nobody later "simplifies" it to
`fetchPartitionData.fetchOffset`. B3's patch adds the reasoning as a comment on the block, so this
needs no separate action.

**Fix draft**: delivered inside `764c1adb77` (the comment quoted in B3's patch).

**Comment draft**: [A10](#a10-q1-coresrcmainscalakafkaserverreplicamanagerscala2527-the-clamp-to-the-seal-is-deliberate-now-documented) in Appendix A.


---

### Q2 / D4. `metadata/.../ReplicationControlManager.java:2005` — the controller's ISR expansion on unfence ignores the seal

**Risk**: this is not a tidy out-of-scope follow-up. It **gates D3**: while it stands, no
restart-based test can observe the PR's mechanism. And it is a correctness question in its own right —
the ISR can end up containing a replica that is missing classic records below the seal, and for a
switched partition that prefix lives only in the replicas' local logs.

**Evidence**: proven, not argued. `dc415a8949` adds a characterisation test that passes today:

```
DisklessManagedReplicasTests > testUnfenceExpandsIsrOfSwitchedPartitionRegardlessOfSeal() PASSED
```

It creates a classic topic, switches it, commits a seal of 100 via a `PartitionChangeRecord`
(`InitDisklessLogFields.encodeClassicToDisklessStartOffset`), asserts the seal is committed, fences
broker 2, then unfences it — and broker 2 is back in ISR with the seal untouched. Also confirmed:
`PartitionChangeBuilder.build():446-453` writes `record.setIsr(targetIsr)` with no eligibility
filtering, and `expandIsrForDisklessManagedPartitions` selects on `isDisklessTopic:3461`, which is
purely config-based.

**Fix draft** (deliberately a question, not a patch — the direction is the author's call):

```java
// metadata/.../ReplicationControlManager.java:2005, inside the per-partition loop
             if (!Replicas.contains(partition.replicas, brokerId)) continue;
             if (Replicas.contains(partition.isr, brokerId)) continue;
+            // A switched partition's classic prefix below the seal lives only in the replicas'
+            // local logs, so a returning replica must earn ISR via AlterPartition rather than
+            // being re-admitted on unfence.
+            if (partition.classicToDisklessStartOffset >= 0) continue;
```

If that is the direction, the characterisation test in `dc415a8949` should be **inverted** to assert
the partition is skipped, and D3's replacement test becomes straightforward — the restart scenario
would then be discriminating on its own.

**Comment draft**: [A11](#a11-q2-d4-metadatareplicationcontrolmanagerjava2005-the-controllers-isr-expansion-on-unfence-ignores-the-seal) in Appendix A.


---

## Nits

### N1. `core/src/test/scala/unit/kafka/server/ReplicaFetcherThreadTest.scala:815` — dead stub from the first implementation

**Risk**: `when(partition.inSyncReplicaIds).thenReturn(Set.empty)` is unread since commit `88d5b944`
moved the ISR read to `inklessMetadataView.isReplicaInIsr`. Harmless with lenient mocks, but it
suggests the gate still reads `Partition` state, and it pins a value that would make the old
implementation always return `false` if anyone reverted.

**Patch**: `ebac227ddc` (in the coverage commit rather than the cleanup commit, because it lives in
the same helper that S3 modifies — splitting them would create a conflict-prone pair).

```scala
     when(partition.localLogOrException).thenReturn(log)
-    when(partition.inSyncReplicaIds).thenReturn(Set.empty)
     when(partition.appendRecordsToFollowerOrFutureReplica(...)
```

**Comment draft**: [A12](#a12-n1-coresrctestscalaunitkafkaserverreplicafetcherthreadtestscala815-dead-stub-from-the-first-implementation) in Appendix A.


---

### N2. Folded into B3

Not independent of B3 — `ReplicaManagerInklessTest.scala:7395` asserts `Errors.NONE`, which pins B3's
behaviour as intended, so the rename must not churn twice. See B3 for the three-way mislabelling and
the split. Patch: `764c1adb77`.

**Fix draft**: see B3. **Reply draft**: see B3 (last two paragraphs).

---

### N3. `tests/kafkatest/tests/inkless/inkless_topic_switch_test.py:415,430` — two near-identical wait helpers (low)

**Risk**: none functional; 30 duplicated lines that will drift.

**Evidence**: `python -m py_compile` clean. Not exercised — the ducktape suite was not run.

**Patch**: `aae665d2e9`.

```python
-    def _wait_for_under_replicated_partitions(self, expected_count, timeout_sec=120):
-    def _wait_for_under_replicated_partitions_at_least(self, min_count, timeout_sec=120):
+    def _wait_for_under_replicated_partitions(self, expected_count, at_least=False, timeout_sec=120):
+        qualifier = "at least " if at_least else ""
+        ...
+            return count >= expected_count if at_least else count == expected_count
```

**Comment draft**: [A13](#a13-n3-testskafkatesttestsinklessinkless_topic_switch_testpy415430-two-near-identical-wait-helpers-low) in Appendix A.


---

### N4. `tests/kafkatest/tests/inkless/inkless_topic_switch_test.py:382` — no freshness bound on the JMX sample (low)

**Risk**: `max(time_to_stats.keys())` is the newest sample *ever read* from that node, not a recent
one. `read_jmx_output` returns silently when `self.started[idx-1]` is false
(`services/monitor/jmx.py:96-97`), so a live broker whose JmxTool died contributes an arbitrarily old
value and still counts toward `observed_nodes` — which the `observed_nodes == len(live_nodes)` guard
cannot detect. In this test the ordering (`at_least(1)` between the two `== 0` waits) makes a stale
pass unlikely, so this is hardening rather than a live bug.

**Evidence**: `static reasoning only` for the risk; `python -m py_compile` clean for the patch.

**Patch**: `aae665d2e9`.

```python
+    # JmxTool polls once a second; a sample older than this is a dead scrape, not a reading.
+    JMX_SAMPLE_MAX_AGE_SEC = 15
...
             if time_to_stats:
                 latest = max(time_to_stats.keys())
+                if time.time() - latest > self.JMX_SAMPLE_MAX_AGE_SEC:
+                    continue
                 latest_stats = time_to_stats[latest]
```

**Comment draft**: [A14](#a14-n4-testskafkatesttestsinklessinkless_topic_switch_testpy382-no-freshness-bound-on-the-jmx-sample-low) in Appendix A.


---

## What I verified and found correct

Settled; a later round should not re-litigate these.

Round 1, by static tracing:

- **The recorded follower LEO is deliberately clamped to the seal, and that is load-bearing.**
  `ReplicaManager.scala:2527` passes `classicToDisklessStartOffset`, not the request's `fetchOffset`,
  so `Replica.updateFetchStateOrThrow` (`Replica.java:83-84`) sees
  `followerFetchOffset == leaderEndOffset` on a frozen log and sets `lastCaughtUpTimeMs = now`. That
  keeps `ReplicaState.isCaughtUp`'s `leaderEndOffset == logEndOffset` leg true forever. Passing the
  raw `fetchOffset` would break it.
- **`fetchedData.fetchOffsetMetadata` is full metadata.** `LocalLog.read` short-circuits at
  `startOffset == maxOffsetMetadata.messageOffset` (`LocalLog.java:482`) returning
  `emptyFetchDataInfo(logEndOffsetMetadata)`, so `maybeIncrementLeaderHW` gets a comparable
  `LogOffsetMetadata`. No `messageOffsetOnly` hazard.
- **`maxBytes = 0, minOneMessage = false` cannot allocate or throw on the happy path.**
- **Divergence correctly suppresses the fetch-state record** (`Partition.scala:1459`), so a divergent
  follower cannot be admitted to ISR by this path.
- **A single fetch at the seal is genuinely insufficient, so the retry loop is motivated.** After a
  follower restart the leader's `isReplicaIsrEligible` (`Partition.scala:1048-1071`) fails on
  `isBrokerFenced` and on the stored-vs-cached broker epoch until the *leader's* image catches up.
- **The consolidating leg of the leader-side block is unreachable, so it cannot pin the leader HW.**
  A consolidating follower below the seal stays on the classic fetcher
  (`isReadyForConsolidation`, `ReplicaManager.scala:3868`), is served from the unified log while
  `fetchOffset < leaderLEO`, and is evicted in the same `doWork` iteration in which its LEO reaches
  the seal. The consolidation fetcher reads object storage (`DisklessLeaderEndPoint`), never the
  leader.
- **No ISR shrink risk from stale follower state.** `ReplicaManager.maybeShrinkIsr:3253` skips
  diskless topics entirely; shrinks are controller-driven only.
- **Classic partitions do not regress** — every change sits behind `isDisklessTopic` /
  `classicToDisklessStartOffset >= 0`.
- **`isReplicaInIsr` is not permanently stale and is cheap** — volatile `currentImage()` read plus
  `Array[Int].contains` over RF. Swapping it in for `partition.inSyncReplicaIds` (which allocated a
  `Set[Int]` per call) was right.
- **`OFFSET_MOVED_TO_TIERED_STORAGE` is not a missing catch case.** It is only produced by
  `handleOffsetOutOfRangeError` (`ReplicaManager.scala:3063`), which needs
  `seal < log.localLogStartOffset()`; on a frozen log `localLogStartOffset <= LEO == seal`.
- **No non-ASCII in any added line.**

Round 2, settled by execution:

- **The PR's unit coverage is genuinely discriminating for all three mechanisms** — each revert breaks
  its own test and nothing else. See the revert experiment. This retires my Round 1 worry that the
  PR was unguarded.
- **`ConfigRepository.topicConfig` really does copy.** `KRaftMetadataCache.config:466` →
  `ConfigurationsImage.configProperties:54` → `ConfigurationImage.toProperties:42-46`, which is
  `new Properties(); properties.putAll(data)`. `isConsolidatingDisklessTopic` calls it twice. S2's
  premise holds.
- **`delayPartitions` is the right mechanism.** `RemoteLeaderEndPoint.buildFetch:179` skips
  partitions where `!fetchState.isReadyForFetch`, and `isReadyForFetch` excludes delayed
  (`PartitionFetchState.java:66,78`). Once every partition in the fetcher is delayed, `buildFetch`
  returns empty and `maybeFetch:129-132` sleeps `fetchBackOffMs`.
- **`-Xfatal-warnings` + `-Xlint:unused` are on** (`build.gradle:825,844`). This bit twice during the
  experiment and is worth knowing: reverting the leader-side block also requires reverting the
  `FetchResponseData` import, and S2's `val`→`def` change is now compiler-enforced.
- **Enabling consolidation in a broker config needs a four-config chain** —
  `remote.log.storage.system.enable`, `diskless.storage.system.enable`,
  `diskless.managed.rf.enable`, `diskless.allow.from.classic.enable`. Discovered while writing S3's
  test; it also confirms managed replicas is necessarily on wherever switching is possible, which is
  what makes D4 unavoidable.

---

## Copilot triage

Three suppressed comments on the 2026-08-07 review:

- **`inkless_topic_switch_test.py:409` — `_live_cluster_jmx_sum` can return 0 on a partial scrape.**
  **Stale-resolved.** The quoted "before" (`observed = True` / `return int(total) if observed else None`)
  no longer exists; commit `772f9500` replaced it with `observed_nodes == len(live_nodes)` at `:413`,
  which is exactly the fix Copilot asked for. The residual hole is narrower — sample freshness, N4.
- **`ReplicaManagerInklessTest.scala:7169` — "STALE (ahead-of-leader)" is the wrong word.**
  **Valid-open**, and understated. Copilot caught the comment; the mislabelling is three-way (name vs
  comment vs code) and the same test turns out to be vacuous. See B3.
- **`ReplicaFetcherThread.scala:174` — the eviction comment omits the consolidating leg.**
  **Valid-open.** Independently found; S5, patched in `4d44549317`.

The 2026-07-14 overview also mis-describes the change ("treat `fetchOffset == classicToDisklessStartOffset`
as still eligible for unified-log reads") — the PR does the opposite. Noise, no action.

---

## Appendix A - comment drafts

Verbatim text to post as inline comments, one per finding. Reviewer-facing: if you are
the author reading the branch, the findings themselves say the same thing in place.

### A0 - top-level review comment

> Reviewed the whole diff and then went further than reading it: I built a scratch worktree at your
> head, reverted each of your three behavioural hunks in turn to see which of your tests actually
> catch them, and implemented the fixes I am asking for as six commits on `jeqo/pr-697-review` so you
> can take or reject working code rather than prose. The full write-up, including the revert results
> and the traces, is `pr-697-review.md` on that branch; the short version is that most of
> this PR holds up well. Clamping the recorded LEO to the seal is necessary, not incidental — it is
> what keeps `ReplicaState.isCaughtUp`'s `leaderEndOffset == logEndOffset` leg true on a frozen log.
> One fetch at the seal genuinely is not enough, because the leader's `isReplicaIsrEligible` fails on
> the follower's broker epoch until the leader's own image catches up, so gating eviction on ISR is
> the right shape. Divergence properly suppresses the fetch-state record. And the revert experiment
> shows your `isOutOfIsr` clause and your ISR-gated eviction each break their own test when removed,
> so they are properly guarded.
>
> I would like six decisions settled rather than sixteen comments answered, in this order because
> they depend on each other:
>
> 1. **Do errors propagate on the at-seal path, or is `Errors.NONE` intended?** The two pre-checks in
>    front of `fetchRecords` swallow `FENCED_LEADER_EPOCH`, `UNKNOWN_LEADER_EPOCH` and
>    `NOT_LEADER_OR_FOLLOWER`, which after the ISR gate are the follower's only backoff signal. The
>    proof this matters: reverting your entire at-seal block breaks four of your five tests, but
>    `...WhenLeaderEpochStale` still passes — it asserts the pre-PR behaviour, so it cannot fail if the
>    feature is deleted. Settle this first; the test rename and the missing epoch case both depend on
>    it. Patch: `764c1adb77`.
> 2. **How is the at-seal fetch loop bounded?** Today it is not: the leader answers outside the
>    purgatory so `replica.fetch.wait.max.ms` is ignored, and the follower has no sleep. I recommend a
>    follower-side `delayPartitions` backoff plus the controlled-shutdown guard, over routing the
>    response through the purgatory. Patches: `4d44549317` and `a507ae1a23`.
> 3. **What proves this fix end-to-end?** Not the new system test, I believe — see decision 4. The unit
>    coverage does hold, so this is about the claim, not the fix.
> 4. **Is the controller's seal-blind ISR expansion on unfence intended?** This one gates decision 3,
>    so please do not let me file it as a follow-up. `expandIsrForDisklessManagedPartitions` re-admits
>    a returning replica to a switched partition's ISR without looking at the seal — I pinned it with a
>    passing test in `dc415a8949`. If that is wrong, the fix belongs in the controller and your restart
>    test becomes discriminating for free.
> 5. **Accept the replication hot-path cost?** `isConsolidatingDisklessTopic` does two full
>    `Properties` copies per partition per fetch, for classic partitions too. No behaviour change, so
>    this is mergeable on its own. Folded into `4d44549317`.
> 6. **Comment and naming cleanup.** Two comments still describe self-eviction at the seal, one omits
>    the consolidating exemption, plus the small test and system-test nits. Patches: `aae665d2e9` and
>    `ebac227ddc`.
>
> Two things I could not verify and am not claiming: I did not run the ducktape suite (no multi-node
> cluster available here), so everything about the system test is inference from the controller-level
> test; and the controlled-shutdown scenario in decision 2 is traced, not reproduced. Everything else
> in the write-up has a command and an observed result next to it.


### A1 - B2. `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:180-185` — the at-seal fetch loop has no backoff (high)

> `ReplicaFetcherThread.scala:180-185`: gating eviction on ISR removes the only thing that used to
> stop this loop, and the loop has no backoff. For a fully-switched partition the leader answers out
> of `immediateFetchResponses` and then hits `ReplicaManager.scala:2625-2626`
> (`classicFetchInfos.isEmpty && disklessFetchInfos.isEmpty` → `respond(Seq.empty)`), so it never
> enters the purgatory and `replica.fetch.wait.max.ms` / `replica.fetch.min.bytes` are ignored — the
> response comes back immediately with zero bytes. On the follower `ShutdownableThread.run` has no
> sleep and `maybeFetch:129-132` only awaits `fetchBackOffMs` when no request can be built. Before
> this PR the partition was evicted on that first response, so it happened once; now it repeats until
> the ISR expansion lands.
>
> It is worst on exactly the cluster shape your system test uses — when every partition in the
> fetcher's set is fully-switched there is nothing to park the request on. And there is a state where
> it never ends: this broker's own controlled shutdown (see my `:4151` comment).
>
> I have a verified patch on `review/pr697-fix-drafts` as `4d44549317`: collect the "at seal, not in
> ISR yet" partitions and `delayPartitions(toDelay, replicaFetchBackoffMs)` from `doWork`. Needs
> `AbstractFetcherThread.delayPartitions` to go `private` → `protected`. The drain has to be in
> `doWork` rather than `processPartitionData`, because `processFetchRequest` overwrites the fetch
> state right after and drops an inline delay. It comes with a test that fails with `delay=0ms` if
> the backoff is removed.
>
> I did consider routing the response through the fetch purgatory instead so `maxWaitMs` applies, but
> `DelayedFetch` would need to carry a partition it must never read plus a never-firing completion
> predicate, and it changes the path every diskless and consolidating fetch shares — too much for the
> same effect. Happy to be argued out of that.

### A2 - B3.1 — the vacuous test, as two runs of one exchange

> `ReplicaManager.scala:2519-2521` and `:2523`: these two pre-checks swallow the errors that would
> otherwise come out of `fetchRecords`, and after this PR those errors are the follower's only
> backoff signal.
>
> Without the epoch pre-check, `localLogWithEpochOrThrow` → `checkCurrentLeaderEpoch` throws
> `FencedLeaderEpochException` or `UnknownLeaderEpochException`, both already in your catch list at
> `:2544-2551`, and both get useful follower handling: `FENCED_LEADER_EPOCH` → `onPartitionFenced`
> drops the partition until the next metadata state; `UNKNOWN_LEADER_EPOCH` →
> `delayPartitions(fetchBackOffMs)`. With the pre-check the follower gets `Errors.NONE` + `HW = seal`
> and, since eviction now waits for ISR, just keeps fetching immediately.
>
> The `getReplica` guard at `:2523` has the same shape. After a reassignment the leader drops the
> departed replica from `remoteReplicasMap` (`Partition.scala:983`) while `leaderEpoch` is unchanged, so
> the epoch pre-check passes, `getReplica` comes back empty, and we answer "you are caught up,
> HW = seal" to a broker that is no longer one of our replicas. Without the guard,
> `followerReplicaOrThrow` returns `UNKNOWN_LEADER_EPOCH` — and the comment right there
> (`Partition.scala:1493-1497`) describes this exact reassignment case and says the point of the error
> is to make the follower retry.
>
> One correction against myself: I first described this as a *non-leader* answering, via
> `updateAssignmentAndIsr:985`'s `remoteReplicasMap.clear()`. When I wrote the message exchange out I
> could not build a metadata path to that state — `leader` and `leaderEpoch` are in the same
> registration, so a real leadership move bumps the epoch and your `requestEpochMatchesLeader` check
> catches it first. The reassignment path is the reachable one. That also means the test I added for it
> (`testFollowerFetchAtSealFromNonLeaderReturnsNotLeaderOrFollower`) constructs its state rather than
> reaching it, and asserts `NOT_LEADER_OR_FOLLOWER` where the reachable cause gives
> `UNKNOWN_LEADER_EPOCH`. It still fails before the patch and passes after, so it does pin the guard —
> but a reassignment-shaped test would be the better one, and I would rather tell you that than leave
> it looking stronger than it is.
>
> The reason I am confident rather than guessing: I ran the experiment. Reverting the whole at-seal
> block to `f751436ad0` and re-running your tests, four of the five fail — but
> `testFollowerFetchAtSealSkipsFetchStateAndIsrExpansionWhenLeaderEpochStale` **passes**. It asserts
> `Errors.NONE` at `:7395`, the LEO unchanged and no `AlterPartition`, which is exactly what the
> pre-PR code did unconditionally, so it cannot fail if the feature is deleted.
>
> Two other things about that test. Its labelling disagrees with itself three ways: the name says
> `...WhenLeaderEpochStale` (stale = older = `FENCED_LEADER_EPOCH`), the comment at `:7379` says
> `ahead-of-leader` (= `UNKNOWN_LEADER_EPOCH`), and the code does `+1`. And `+1` is not an edge case
> worth apologising for — it is the ordinary leader-election race, where the controller bumps the
> epoch and the follower applies the delta before the leader does. Upstream has a dedicated error
> code for it. Also please drop "mirrors the classic read path, which validates the request epoch
> before touching follower state": the classic path validates *and returns the error*, which is the
> gap here.
>
> `764c1adb77` on `review/pr697-fix-drafts` drops both pre-checks and splits that test into a `+1`
> case asserting `UNKNOWN_LEADER_EPOCH` and a `-1` case asserting `FENCED_LEADER_EPOCH` (only one was
> covered at all), plus a non-leader case. All three fail with `NONE` before the patch and pass after.
> I left `getPartitionOrError`'s `Left` still dropped, because propagating it also changes two
> pre-existing tests that never create a local `Partition` — your call whether to take that too.

### A3 - S1. `core/src/main/scala/kafka/server/ReplicaManager.scala:4151-4152` — no controlled-shutdown guard, so `isOutOfIsr` starts a fetcher that can never evict (medium)

> `ReplicaManager.scala:4151`: `isOutOfIsr` has no controlled-shutdown guard, unlike the classic
> branch at `:4190-4194`. During our own controlled shutdown the controller drops us from ISR, that
> bumps `partitionEpoch` so the partition reappears in `localChanges.followers`
> (`TopicDelta.localChanges:203-208`), and we start a catch-up fetcher for a partition the leader will
> not re-admit — `isReplicaIsrEligible` fails on `isBrokerShuttingDown` (`Partition.scala:1064`) for
> the whole shutdown. Combined with the missing backoff that is a fetcher spinning at full CPU against
> the leader while we are trying to exit. `a507ae1a23` adds `!isInControlledShutdown &&`. Note it does
> not stand alone — the pre-existing `hw < seal` leg has the same problem, which is why the backoff
> matters more.

### A4 - S2. `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:177-179` — two `Properties` copies per partition per fetch on the replication hot path (medium)

> `ReplicaFetcherThread.scala:177-179`: this `val` is evaluated on every `processPartitionData` call
> for every partition, including pure classic ones that can never reach the branch. I checked the cost
> rather than assuming it: `isConsolidatingDisklessTopic` → `isDisklessTopic` +
> `isRemoteStorageEnabled` does two `metadataCache.topicConfig()` calls, and that bottoms out in
> `ConfigurationImage.toProperties:42-46`, which is `new Properties(); putAll(data)` — a full copy of
> every topic config entry, twice per partition per fetch. `ConsolidationFetcherThread` inherits this
> method and the predicate is always true there. The `4d44549317` patch makes it a `def` inside the
> `classicToDisklessStartOffset >= 0` guard. This one has no behaviour change, so it is mergeable on
> its own.

### A5 - S3 (was B1). `tests/kafkatest/tests/inkless/inkless_topic_switch_test.py:1301` — the system test cannot observe the mechanism it is named for (medium, downgraded from blocker)

> `inkless_topic_switch_test.py:1301`: I do not think this test can fail on `main`, and I checked the
> premise rather than just arguing it. `handleBrokerUnfenced:1998-2000` calls
> `expandIsrForDisklessManagedPartitions` (`:2005-2033`), which on unfence adds any out-of-ISR
> assigned replica of any `diskless.enable=true` topic straight back into the ISR — and it never looks
> at `classicToDisklessStartOffset`. I wrote a characterisation test for exactly the switched case
> (`dc415a8949` on `review/pr697-fix-drafts`,
> `testUnfenceExpandsIsrOfSwitchedPartitionRegardlessOfSeal`): seal committed at 100, fence broker 2,
> unfence broker 2, and broker 2 is back in ISR with the seal untouched. It passes. The cluster here
> satisfies the gate too — `diskless.managed.rf.enable=true`, which
> `diskless.allow.from.classic.enable` requires per `KafkaConfig.scala:543-544`.
>
> So your stop/start sequence at `:1316-1326` should reach URP=0 via the controller, with no at-seal
> follower fetch involved. To be explicit about what I did not do: I could not run the ducktape suite
> (no multi-node cluster available), so this is inference from the controller test, not an observed
> red/green.
>
> The good news is the unit coverage does guard the fix — I reverted each of your three behavioural
> hunks in turn and each one breaks its own test. So this is about the end-to-end claim, not about the
> fix being untested. If you want a discriminating system test it needs a short ISR with no
> fence/unfence edge, which means a replica newly added to an already-switched partition. If
> reassignment on a switched diskless topic is not available on `main` yet, I would just narrow the
> claim in the PR body instead.

### A6 - S4. `core/src/main/scala/kafka/server/ReplicaManager.scala:4156` and `:2559-2560` — two comments describe behaviour this PR removed (low)

> Two comments are now inaccurate. `ReplicaManager.scala:4156` still says the ReplicaFetcher
> "self-evicts once the follower has read past the seal" — I diffed it against `f751436ad0` and the
> line is unchanged, so it documents pre-PR behaviour; eviction now also needs ISR membership.
> `:2559-2560` still says the empty response makes the fetcher "go idle", which is the opposite of the
> change. Both reworded in `aae665d2e9`.

### A7 - S5. `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:171-174` — the eviction comment omits the consolidating exemption (low)

> `ReplicaFetcherThread.scala:171-174`: the comment says eviction requires being in ISR, but the
> condition also lets a consolidating partition evict while out of ISR — and that exemption is the
> part a reader cannot infer. Reworded in `4d44549317`. (Copilot flagged this and suppressed it; it
> was right.)

### A8 - S6. `core/src/test/scala/unit/kafka/server/ReplicaManagerInklessTest.scala:7182` — the ISR assertion does not check the submitted set (low)

> `ReplicaManagerInklessTest.scala:7182`: `submit(any(), any())` only proves an `AlterPartition` went
> out, not that it adds the follower — which is what the test name claims. `ebac227ddc` captures the
> `LeaderAndIsr` and asserts `isr.contains(followerId)`.

### A9 - S7. Untested branches: the consolidating eviction leg, and `isReplicaInIsr` with the partition absent (medium / low)

> Two branches this PR adds had no coverage.
>
> `ReplicaFetcherThreadTest.scala:795-830`: none of the eviction cases exercised the
> `isConsolidatingPartition` leg — `TestUtils.createBrokerConfig` leaves
> `diskless.remote.storage.consolidation.enable` off, so that half of your condition was dead in the
> test. It is the load-bearing half for consolidation: if it regressed, a consolidating follower would
> sit at the seal waiting for ISR and never hand off through
> `startConsolidationFetchersForCaughtUpClassicPartitions`. I added the case in `ebac227ddc` and
> confirmed it fails when the leg is disabled. Fair warning on why it was probably skipped: enabling
> consolidation needs four configs (`remote.log.storage.system.enable`,
> `diskless.storage.system.enable`, `diskless.managed.rf.enable`,
> `diskless.allow.from.classic.enable`).
>
> `InklessMetadataViewTest.scala:300-313` covers "topic missing" but not "partition missing", unlike
> the sibling `testGetDisklessLeaderEpochReturnsSentinelWhenPartitionMissing`. The fetcher consults
> this per fetch, so it is worth pinning.

### A10 - Q1. `core/src/main/scala/kafka/server/ReplicaManager.scala:2527` — the clamp to the seal is deliberate; now documented

> `ReplicaManager.scala:2527`: confirming my reading — you pass `classicToDisklessStartOffset` rather
> than `fetchPartitionData.fetchOffset` on purpose, because the leader's classic log is frozen at the
> seal and recording exactly the seal keeps `ReplicaState.isCaughtUp`'s
> `leaderEndOffset == logEndOffset` leg true, so the follower stays caught up without another fetch.
> That is subtle enough to deserve a comment; `764c1adb77` adds one. I also chased whether the clamp
> could pin the leader HW on a *consolidating* partition and satisfied myself it cannot — a
> consolidating follower is served from the unified log while `fetchOffset < leaderLEO` and is evicted
> in the same `doWork` iteration in which its LEO reaches the seal, and the consolidation fetcher
> reads object storage via `DisklessLeaderEndPoint`, not the leader. Does that match your
> understanding?

### A11 - Q2 / D4. `metadata/.../ReplicationControlManager.java:2005` — the controller's ISR expansion on unfence ignores the seal

> Broader question, and I think it gates the test discussion rather than being a separate follow-up.
> `expandIsrForDisklessManagedPartitions` (`ReplicationControlManager.java:2005-2033`) adds a
> returning broker back into the ISR of every `diskless.enable=true` partition with no reference to
> `classicToDisklessStartOffset` or the replica's local LEO. I wrote it up as a characterisation test
> (`dc415a8949`, `testUnfenceExpandsIsrOfSwitchedPartitionRegardlessOfSeal`) so the behaviour is
> pinned rather than argued: seal committed at 100, fence broker 2, unfence broker 2, broker 2 is back
> in ISR. It passes today.
>
> Two consequences. The ISR can contain a replica missing classic records below the seal — for a
> switched partition that prefix exists only in the replicas' local logs, so the assumption behind the
> shortcut ("diskless data is in object storage, so any live replica is current", from #643) does not
> hold there. And it is why your system test cannot observe the fetch-driven expansion.
>
> Your PR builds the path that actually proves `LEO >= seal` before expansion. Should the controller
> shortcut skip partitions with `classicToDisklessStartOffset >= 0` and let them earn ISR through
> `AlterPartition`? If yes, my characterisation test should be inverted, and the restart-based system
> test becomes discriminating for free. If no, then the system test needs a different scenario. Either
> way I would rather settle this than file it and move on.

### A12 - N1. `core/src/test/scala/unit/kafka/server/ReplicaFetcherThreadTest.scala:815` — dead stub from the first implementation

> `ReplicaFetcherThreadTest.scala:815`: leftover from the first version of the fix — `88d5b944` moved
> the ISR read from `partition.inSyncReplicaIds` to `inklessMetadataView.isReplicaInIsr`, so this stub
> is dead and misleads about where the gate reads from. Dropped in `ebac227ddc`.

### A13 - N3. `tests/kafkatest/tests/inkless/inkless_topic_switch_test.py:415,430` — two near-identical wait helpers (low)

> `inkless_topic_switch_test.py:415-443`: these two helpers are identical apart from `==` vs `>=`.
> Collapsed to one with an `at_least` keyword in `aae665d2e9`. Syntax-checked only — I could not run
> the ducktape suite here.

### A14 - N4. `tests/kafkatest/tests/inkless/inkless_topic_switch_test.py:382` — no freshness bound on the JMX sample (low)

> `inkless_topic_switch_test.py:397-413`: `max(time_to_stats.keys())` is the newest sample ever read
> from that node, with no age bound, and `read_jmx_output` returns silently when `started[idx-1]` is
> false (`services/monitor/jmx.py:96-97`). So a live broker whose JmxTool died contributes a stale
> value and still counts toward `observed_nodes`, which the `observed_nodes == len(live_nodes)` guard
> cannot catch. The `at_least(1)` wait between the two `== 0` waits makes a stale pass unlikely, so
> this is just hardening — `aae665d2e9` skips samples older than a few poll intervals.

## Appendix B - revert experiment

Method: at PR head, revert one behavioural hunk at a time to its `f751436ad0` form, keep all of the
PR's tests, and record which still pass. A test that passes with its mechanism reverted is
non-discriminating. Pure API additions (`InklessMetadataView.isReplicaInIsr`) were kept so the test
sources still compile.

```
./gradlew :core:test --tests "kafka.server.ReplicaManagerInklessTest" \
                     --tests "kafka.server.ReplicaFetcherThreadTest" \
                     --tests "kafka.server.metadata.InklessMetadataViewTest"
```

Baseline, unmodified PR head: **216 tests, 0 failures, 0 errors**.

### R1 — leader-side at-seal `fetchRecords` block reverted (`ReplicaManager.scala:2517-2556`)

`216 tests, 4 failures`. Note the revert also had to restore the `FetchResponseData` import: it is
used only by this block, and `-Xlint:unused` + `-Xfatal-warnings` fail the build otherwise.

- FAIL `testFollowerFetchAtSealRecordsFetchStateAndAllowsIsrExpansionWhenEpochMatches` —
  `expected: <5> but was: <-1>`
- FAIL `testFollowerFetchAtSealReturnsPartitionErrorWhenBrokerEpochStale` —
  `expected: <NOT_LEADER_OR_FOLLOWER> but was: <NONE>`
- FAIL `testFollowerFetchAtSealIsolatesOffsetOutOfRangeError` —
  `expected: <OFFSET_OUT_OF_RANGE> but was: <NONE>`
- FAIL `testFollowerFetchAtSealReturnsDivergingEpochAndSkipsIsrExpansion` —
  `expected: <true> but was: <false>`
- **PASS `testFollowerFetchAtSealSkipsFetchStateAndIsrExpansionWhenLeaderEpochStale`** ← the finding

### R2 — `makeFollower` `isOutOfIsr` clause reverted (`ReplicaManager.scala:4151-4152`)

`216 tests, 1 failure` — discriminating.

- FAIL `testApplyDeltaStartsCatchUpFetcherWhenDisklessFollowerAtSealButOutOfIsr` —
  `Wanted but not invoked: replicaFetcherManager.addFetcherForPartitions(Map(switched-topic-0 -> InitialFetchState(...,0,10)))`

### R3 — ISR-gated eviction reverted (`ReplicaFetcherThread.scala:177-185`)

`216 tests, 1 failure` — discriminating.

- FAIL `shouldNotEvictPartitionAtSealUntilMetadataIsrContainsReplica` —
  `NeverWantedButInvoked: replicaFetcherManager.removeFetcherForPartitions(<any>)`

### Reading

Two mechanisms are properly guarded. The leader-side block is guarded for fetch-state recording,
broker-epoch isolation, offset isolation and divergence — but **not** for the epoch validation the
commit series claims, because that one test asserts the absence of behaviour the pre-PR code
provided unconditionally.

---

## Appendix C - round 2 changes

### Confirmed by execution

- **B2** — patch `4d44549317`; its new test fails with `delay=0ms` when the backoff is neutralised.
- **B3** — patch `764c1adb77`; three new tests, all three observed failing (`NONE`) before and
  passing after.
- **S2** — the `Properties` copy proven at source; fix rode along in `4d44549317`.
- **S3** — patch `ebac227ddc`; its test fails when the consolidating leg is disabled.
- **S4** — both stale comments confirmed unchanged against `f751436ad0`.
- **Q2/D4** — premise proven by a passing characterisation test, `dc415a8949`.

### Downgraded

- **B1: blocker → suggestion, and re-scoped.** Round 1 said the PR's "only end-to-end evidence" was
  non-discriminating. That was too broad: the revert experiment shows the unit tests each break with
  their own mechanism reverted, so the PR *is* guarded at unit level. What remains true, and is now
  proven at the controller level rather than argued, is that the **system test specifically** cannot
  observe the mechanism. Reframed as a test-scope suggestion.
- **N2: nit → part of B3.** It is not independent — `ReplicaManagerInklessTest.scala:7395` asserts
  `Errors.NONE`, which pins B3's behaviour as intended. Renaming before B3 lands would churn twice.
  Folded into `764c1adb77` as a split rather than a rename, since only one of the two epoch cases was
  covered at all.

### New in Round 2

- **B3.1 (new, execution-only)** — `testFollowerFetchAtSealSkipsFetchStateAndIsrExpansionWhenLeaderEpochStale`
  is **vacuous**: it passes with the *entire* leader-side at-seal block reverted to the merge base.
  This is the single strongest result of the round and could not have been found by reading. Folded
  into B3.

### Withdrawn

- **Round 1's claim that fetching a switched partition from a non-leader "now correctly returns
  `NOT_LEADER_OR_FOLLOWER`"** — wrong, and I withdrew it inside Round 1 itself.
  `updateAssignmentAndIsr:985` clears `remoteReplicasMap` when `isLeader` is false, so on a non-leader
  `getReplica(followerId)` is always empty and the block no-ops. It is a B3 *symptom*, not correct
  behaviour. Now covered by a passing test in `764c1adb77`.
- **B3's non-leader framing — re-derived, not withdrawn.** Writing B3's cause-C trace showed the
  *non-leader* state I had described is not reachable through the metadata path: `leader` and
  `leaderEpoch` live in the same `PartitionRegistration`, so a genuine leadership move bumps the epoch
  and the `requestEpochMatchesLeader` pre-check intercepts it as cause B instead. The underlying cause
  survives with a different, verified precondition — a **reassignment** drops the replica from
  `remoteReplicasMap` (`Partition.scala:983`) while leaving `leaderEpoch` unchanged, and upstream's own
  comment at `Partition.scala:1493-1497` names exactly that case. The patch is unaffected; the *test*
  I wrote for it pins the guard through a constructed state and asserts the wrong error code for the
  reachable cause. Flagged in the trace rather than quietly rewritten.
- **Nothing else withdrawn.** One Round 1 item was deliberately *narrowed* rather than dropped: see
  B3's "not included" note on `getPartitionOrError`'s `Left`.

### Not verifiable here

- **The ducktape system test was not run** and cannot be — it needs a multi-node vagrant/docker
  cluster that is not available in this environment. Every statement about it is either static
  reasoning or inference from the controller-level characterisation test. The Python edits in
  `aae665d2e9` are syntax-checked (`python -m py_compile`) only.
- **S1** has no dedicated test; it is compile-verified and static-only.

---
