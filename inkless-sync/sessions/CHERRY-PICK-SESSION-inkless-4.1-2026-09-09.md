# Cherry-pick Session: inkless-4.1

## Session Info
- **Date**: 2026-09-09
- **Source**: origin/main (72e432f6fb, #789)
- **Target**: inkless-4.1
- **Base**: 2ae259b970 (inkless-4.1.1-0.47)
- **Status**: Complete

---

## Commits to Cherry-pick

| # | Hash | PR | Subject | Status |
|---|------|-----|---------|--------|
| 1 | f2042aa116 | #764 | docs(inkless:release): add 0.47 changelog entry | Applied |
| 2 | d7ded0f603 | #765 | fix(inkless:build): stop reusing inklessTag as both the override key and the resolved value [KC-384] | Applied |
| 3 | 1c39908f9d | #767 | fix(inkless:metadata): accept pipe as a client.id AZ separator | Applied |
| 4 | 187e6213ff | #762 | fix(inkless:fetch): preserve legacy fetch request partition identity in response [KC-353] | Applied |
| 5 | f93145ff32 | #766 | feat(inkless:control-plane): enable batch coalescing by default | Applied |
| 6 | b1fcb5f6f7 | #750 | fix(inkless:control_plane): make the diskless fetch budget fair across partitions [KC-407] | Applied |
| 7 | 4925b82ccc | #769 | feat(inkless:consume): default the lagging request-rate limit off, cap batches per partition | Applied |
| 8 | d29cce09d1 | #777 | chore(inkless:ci): drop nightly workflow, schedule system tests instead | Applied |
| 9 | 17511cb7d8 | #775 | fix(inkless:gcs): count batch object deletions in metrics [KC-483] | Applied |
| 10 | fa2d0dd2b9 | #774 | fix(inkless:consume): serve consumer reads from a lagging AZ replica [KC-461] | Applied |
| 11 | 7eec2c8373 | #772 | fix(inkless:consume): size fetch buffers to the payload [KC-481] | Applied |
| 12 | 58aed69133 | #778 | fix(inkless:storage): report an oversized fetch as a storage failure [KC-481] | Applied |
| 13 | ea1dc9b0ed | #702 | docs(inkless:consolidation): Diskless Consolidation documentation | Applied |
| 14 | 3a00358ce4 | #781 | fix(inkless:gcs): narrow fetch length before opening the reader | Applied |
| 15 | a6716f7037 | #782 | feat(inkless:delete): soft-delete diskless topics and purge in the background [KC-349] | Applied |
| 16 | 6c57f5aadf | #785 | test(inkless:consolidation): tolerate re-delivery in the consume check | Applied |
| 17 | f7e7412d14 | #783 | feat(inkless:storage): bound and shorten the GCS upload path | Applied |
| 18 | 94fe3a0a02 | #788 | fix(inkless:ci): update Inkless CI workflow and JUnit catalog parsing | Applied |
| 19 | 5fa6669803 | #784 | feat(inkless:consume): measure the fetch data phase and its queueing [KC-446] | Applied |
| 20 | 470d46973e | #794 | fix(inkless:release): exclude sync-tooling commits from the branch-consistency gate | Applied |
| 21 | 095cf08133 | #787 | refactor(inkless:consume): drop four never-recorded fetch metrics | Applied |
| 22 | 5693b9d4a2 | #792 | fix(inkless:delete): do not sleep inside the topic purger tick [KC-349] | Applied |
| 23 | 8d97bb4235 | #793 | fix(inkless:ci): copy integrationTest JUnit XML into build/junit-xml | Applied |
| 24 | 62e4c51047 | #791 | feat(inkless:storage): default S3 API call timeouts to 2s and 1s | Applied |
| 25 | 72e432f6fb | #789 | fix(inkless:storage): count retried GCS error responses | Applied |

Excluded by the gate (sync tooling): #768, #770, #776, #780. #773 sync(ci) excluded by the existing rule.

---

## Conflict Resolution Log

### #750 - fix(inkless:control_plane): make the diskless fetch budget fair across partitions [KC-407]

**File**: `core/src/main/scala/kafka/server/DelayedFetch.scala` (2 hunks)
- **Conflict**: main's `tryCompleteDiskless` and `onComplete` iterate a `util.LinkedHashMap` via `.asScala` / `.asScala.iterator`; 4.1 holds `fetchPartitionStatus` and `disklessFetchPartitionStatus` as `Seq[(TopicIdPartition, FetchPartitionStatus)]`.
- **Resolution**: kept 4.1's `Seq` code in both hunks; carried over the "probe is unbudgeted" comment; dropped the LinkedHashMap-iterator comment.
- **Reason**: #750 requires request order to reach `ReplicaManager.fetchDisklessMessages`. `Seq.map` preserves order, so 4.1 already satisfies the invariant; the iterator rewrite exists only to avoid `Map.map` rebuilding a `HashMap`, which does not apply to a `Seq`.

### #777 - chore(inkless:ci): drop nightly workflow, schedule system tests instead

**Files**: `.github/workflows/inkless-nightly.yml` (deleted by pick, modified on 4.1), `.github/workflows/inkless-system-tests.yml`
- **Resolution**: `git rm` the nightly workflow; took the pick's `inkless-system-tests.yml`. Both now match origin/main.
- **Reason**: same as 4.2: 4.1 never received #773 (gate-excluded `sync(ci)` commit) that #777 builds on. No branch-specific content.

### #774 - fix(inkless:consume): serve consumer reads from a lagging AZ replica [KC-461]

**File**: `core/src/main/scala/kafka/server/DelayedFetch.scala`
- **Conflict**: `tryComplete` loop. main: `classicFetchPartitionStatus.forEach { (tp, fs) =>` over a Java map; 4.1: `foreach { case (tp, fs) =>` over a `Seq`, one indent level deeper.
- **Resolution**: kept 4.1's loop shape and inserted the two #774 edits at 4.1 indentation: `fetchOnlyFromLeader` via `replicaReadAllowed.getOrElseUpdate(... allowsOlderConsumerReplicaRead ...)`, and the `isBelowSealAndAheadOfLocalLog` guard ahead of case F. `private val replicaReadAllowed` auto-merged; `mutable` resolves through the existing `scala.collection._` import.

**File**: `core/src/main/scala/kafka/server/ReplicaManager.scala`
- **Conflict**: import block, same as 4.2. Took the pick's metric-name import; kept 4.1's `{InklessMetadataView, KRaftMetadataCache}`.

**File**: `core/src/test/scala/unit/kafka/server/ReplicaManagerInklessTest.scala`
- **Conflict**: `org.apache.kafka.storage.internals.log.{...}` import line. Union of both sides (main adds `LogReadInfo`, `LogReadResult`; 4.1 side had no extra symbols).

### #788 - fix(inkless:ci): update Inkless CI workflow and JUnit catalog parsing

**File**: `.github/workflows/inkless.yml`
- **Conflict**: JOOQ generation step: JDK 25 / `setup-java@v5` (main) vs JDK 23 / `setup-java@v4` (4.1).
- **Resolution**: kept 4.1's JDK 23 step. Auto-merged parts taken as-is (`checkout@v5`, `./.github/actions/setup-python` which exists on 4.1, junit catalog export step). Adjusted the two "only run on java N" comments to 23 to match 4.1's `[17, 23]` matrix.
- **Reason**: 4.1 CI runs Java 17/23; the JDK 25 move belongs to trunk.

---

## Compilation Errors

| # | File:Line | Error | Cherry-pick | Fix | Status |
|---|-----------|-------|-------------|-----|--------|
| 1 | core/src/test/java/kafka/server/PreKip392FetchClient.java:26 | package org.apache.kafka.common.record.internal does not exist (same as 4.2) | #774 | import org.apache.kafka.common.record.Record (separate sync(compile) commit) | Fixed |
| 3 | core/src/test/scala/integration/kafka/server/DelayedFetchTest.scala:312,322 | type LinkedHashMap is not a member of package util (DelayedFetch takes Seq on 4.1) | #750 | build disklessFetchPartitionStatus as Seq, classic as Seq.empty (sync(test) commit) | Fixed |
| 4 | core/src/test/scala/unit/kafka/server/ReplicaManagerInklessTest.scala:68 and 7 ctor sites | LogReadResult not in storage.internals.log; ctor last arg Errors not accepted | #774 | drop LogReadResult from storage import (already imported from org.apache.kafka.server); drop Errors.NONE; OFFSET_OUT_OF_RANGE -> Optional.of[Throwable](OffsetOutOfRangeException) (same sync(test) commit) | Fixed |
| 2 | core/src/main/scala/kafka/server/ReplicaManager.scala:3224 | multiple constructors for LogReadResult: last arg Errors not accepted | #774 | Optional.empty[Throwable]() in place of Errors.NONE (separate sync(compile) commit) | Fixed |

---

## Verification

- [x] make fmt: no changes
- [x] make build (BUILD SUCCESSFUL)
- [x] make test (all three gradle invocations BUILD SUCCESSFUL, no failed tests; the adapted #750 fetch-order test and the #774 ReplicaManagerInklessTest cases ran and passed)
- [x] branch-consistency.sh --check against the local head: 0 actionable missing, 2 old
- [x] #794, #787, #792, #793, #791, #789 applied clean after the first push; compile of storage:inkless and core main+test, then the tests these commits touch (TopicPurgerMockedTest, MetricCollectorTest, S3ClientBuilderTest, S3StorageConfigTest, GcsErrorHandlingTest, S3ErrorMetricsTest, InklessDisklessTopicDeleteTest): all passed.
