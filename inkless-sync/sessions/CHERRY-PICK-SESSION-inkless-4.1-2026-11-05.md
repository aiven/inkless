# Cherry-pick session: `inkless-4.1`

## Session info
- **Date**: 2026-11-05
- **Source**: origin/main (6074c0650f, #854)
- **Target**: inkless-4.1
- **Base**: inkless-4.1.2-0.50
- **Status**: complete, pushed, released in `inkless-release-0.51`

---

## Commits to cherry-pick

| # | Hash | PR | Subject | Status |
|---|------|-----|---------|--------|
| 1 | 7a41a38ec9 | #823 | `fix(inkless:ai): update vale config` | Applied |
| 2 | 08e6a71734 | #831 | `test(inkless:switch): check EARLIEST and remote expiration before the first prune [KC-581]` | Applied |
| 3 | 4fc13dbaba | #832 | `fix(inkless:controller): skip control plane calls for classic-only topic requests [KC-621]` | Applied |
| 4 | b78206f6af | #830 | `docs(inkless:release): add 0.50 changelog and cherry-pick sessions` | Applied |
| 5 | 6c6483b729 | #834 | `docs(inkless): document and test the migration of untiered diskless topics` | Applied |
| 6 | 27b691d570 | #833 | `feat(inkless:controller): gate ISR, reassignment, and leader election on switch phase` | Applied |
| 7 | be0ef1b9fd | #837 | `test(inkless): wait for the published first attempt in reconciler tests [KC-631]` | Applied |
| 8 | 7bb233d5ac | #838 | `fix(inkless:control-plane): close the retired delegate before building its replacement [KC-630]` | Applied |
| 9 | d7b603ea2a | #836 | `fix(inkless:control-plane): read unavailableReason once in recordGatedCall [KC-624]` | Applied |
| 10 | 6ef58bd1e5 | #835 | `feat(inkless:switch): detect classic-prefix upload and request the tiered flag [KC-588]` | Applied (conflict) |
| 11 | 13fb9116cb | #842 | `docs(inkless:ai): ask for imports over fully qualified class names` | Applied |
| 12 | 63cc8f0814 | #839 | `fix(inkless:controller): return a retriable error when the control plane is unavailable [KC-649]` | Applied |
| 13 | 36d39041aa | #846 | `fix(inkless:controller): parse diskless.enable case-insensitively on topic creation` | Applied |
| 14 | 1af55a0a82 | #843 | `feat(inkless:consolidation): bound the WAL prune per request [KC-444]` | Applied |
| 15 | eea3c262f8 | #844 | `fix(inkless:metadata): route consolidating partitions with one replica to the replica [KC-651]` | Applied |
| 16 | c8bb204a74 | #845 | `fix(inkless:controller): validate remote storage on consolidated diskless creates` | Applied |
| 17 | 9bc310258d | #840 | `feat(inkless:switch): gate consolidation on the switch phase` | Applied (conflict) |
| 18 | f30bf465a1 | #847 | `fix(inkless:consolidation): defer prefix reads on a rebuilding replica [KC-673]` | Applied (conflict) |
| 19 | 84be446fc0 | #850 | `test(inkless:control-plane): cover the availability gate across outages, restarts, and migration locks [KC-619]` | Applied |
| 20 | 5851744471 | #849 | `fix(inkless:controller): block unclean leader election in switch phase 1 [KC-636]` | Applied (conflict) |
| 21 | e2921f6265 | #852 | `fix(inkless:controller): skip unchanged control plane connection string record` | Applied (conflict) |
| 22 | 9957f7f383 | #841 | `feat(inkless:switch): gate broker ISR shrink on the switch phase` | Applied |
| 23 | 6074c0650f | #854 | `test(inkless:switch): run the stale high watermark promotion test in phase 2 [KC-721]` | Applied |

---

## Conflict resolution log

### #835 - classic-prefix upload and the tiered flag

**File**: `core/src/test/scala/unit/kafka/server/metadata/DisklessSwitchFlowTest.scala`
- **Conflict**: the `kafka.server` import. The release branch also imports `HostedPartition`.
- **Resolution**: kept `HostedPartition` and added `SendingFlagToController`.

### #840 - gate consolidation on the switch phase

**Files**: `core/.../ReplicaAlterLogDirsThreadTest.scala`, `storage/.../UnifiedLogTest.java`
- **Conflict**: `MemoryRecords` is in `org.apache.kafka.common.record`. The Java `UnifiedLogTest` is a stub, and the conflict pulled in about 6,700 lines of upstream tests.
- **Resolution**: imported `MemoryRecords` and `SimpleRecord` from `org.apache.kafka.common.record`. Kept the Java stub and ported the four `forceDeleteLocalSegmentsBelow` tests to the Scala `kafka.log.UnifiedLogTest`.

### #847 - deferred prefix reads

**Files**: `core/.../ReplicaManager.scala`, `core/.../DelayedFetch.scala`, `core/.../ReplicaManagerInklessTest.scala`
- **Conflict**: imports. The release branch also imports `KRaftMetadataCache`.
- **Resolution**: took the incoming `ReplicaManager` import list and kept `KRaftMetadataCache`. In `DelayedFetch`, kept 4.1's Scala `map` over the status map and passed `deferPrefixReads = true`. In `ReplicaManagerInklessTest`, added `RemoteLogReadResult` and `RemoteStorageFetchInfo` to 4.1's import list, which has no `LogReadResult` there.

### #849 - unclean leader election in switch phase 1

**File**: `metadata/.../ReplicationControlManager.java`
- **Conflict**: `maybeTriggerUncleanLeaderElectionForLeaderlessPartitions` has no `partitionId` local on the release branch.
- **Resolution**: removed the pending-switch check and added the local classic prefix check inside the unclean branch, with `topicIdPartition.partitionId()`.

### #852 - skip the record for an unchanged connection string

**Files**: `ConfigurationControlManager.java`, `ConfigurationControlManagerTest.java`
- **Conflict**: the incoming side adds imports and a cordoned log dirs test that the release branch lacks.
- **Resolution**: added only the `InklessControlPlaneConfigs` imports and the new tests. Dropped the upstream `testCordonedLogDirsFeature` from the incoming side.

---

## Compilation errors

Same fixes as 4.2 (errors 2 to 4), plus:

| # | File | Error | Cherry-pick | Fix | Status |
|---|------|-------|-------------|-----|--------|
| 5 | `core/.../ReplicaManager.scala` | `RemoteStorageFetchInfo` has no `topicIdPartition` | #847 | `topicIdPartition(remoteFetchInfo.topicPartition)` | Fixed |
| 6 | `core/.../ReplicaManagerInklessTest.scala` | `RemoteStorageFetchInfo` takes a `TopicPartition`; `LogReadResult` takes no `Errors` | #847 | `tp.topicPartition()`; dropped `Errors.NONE` | Fixed |
| 7 | `storage/.../RemoteLogManagerTest.java` | `RetriableRemoteStorageException` doesn't exist | #831 | assert `RemoteStorageException`, which 4.1 throws | Fixed |
| 8 | `storage/.../RemoteLogManagerTest.java` | `RemoteStorageFetchInfo` takes a `TopicPartition` | #847 | pass `tp` | Fixed |
| 9 | `InklessTopicTypeSwitcherClusterTest.java`, `InklessUntieredDisklessMigrationTest.java` | `REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP` doesn't exist | #834, #840 | `REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP`, which sizes the follower pool on 4.1 | Fixed |

Error 5 is the only change to main code. All fixes are in one sync(compile) commit.

## Build notes

After #847, `kafka.server.ReplicaManager` on 4.1 is too big for SpotBugs, and `:core:spotbugsMain` fails with `SKIPPED_CLASS_TOO_BIG`. The class is about 400 lines longer on 4.1 than on 4.2 and 4.3. A trial upgrade to SpotBugs 4.9.8 with plugin 6.2.3 fails the same way. The sync(compile) commit adds an exclude filter entry for that pattern on `ReplicaManager` only. `:core:spotbugsMain` passes with it.

## CI notes

The first CI run after the push failed in `:storage:inkless:integrationTest`. Testcontainers couldn't find a Docker environment on the `ubuntu-26.04` runner image. 4.1 declared `testcontainers` twice, and the stale 1.20.3 entry overrode the 1.21.4 entry from #509. A follow-up commit on 4.1 removes the 1.20.3 entry. The cherry-picked commits didn't cause the failure.
