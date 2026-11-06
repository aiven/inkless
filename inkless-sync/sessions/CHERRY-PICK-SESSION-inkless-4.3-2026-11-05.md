# Cherry-pick session: `inkless-4.3`

## Session info
- **Date**: 2026-11-05
- **Source**: origin/main (6074c0650f, #854)
- **Target**: inkless-4.3
- **Base**: inkless-4.3.1-0.50
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
| 17 | 9bc310258d | #840 | `feat(inkless:switch): gate consolidation on the switch phase` | Applied |
| 18 | f30bf465a1 | #847 | `fix(inkless:consolidation): defer prefix reads on a rebuilding replica [KC-673]` | Applied |
| 19 | 84be446fc0 | #850 | `test(inkless:control-plane): cover the availability gate across outages, restarts, and migration locks [KC-619]` | Applied |
| 20 | 5851744471 | #849 | `fix(inkless:controller): block unclean leader election in switch phase 1 [KC-636]` | Applied |
| 21 | e2921f6265 | #852 | `fix(inkless:controller): skip unchanged control plane connection string record` | Applied |
| 22 | 9957f7f383 | #841 | `feat(inkless:switch): gate broker ISR shrink on the switch phase` | Applied |
| 23 | 6074c0650f | #854 | `test(inkless:switch): run the stale high watermark promotion test in phase 2 [KC-721]` | Applied |

No other commit conflicted.

---

## Conflict resolution log

### #835 - classic-prefix upload and the tiered flag

**File**: `core/src/test/scala/unit/kafka/server/metadata/DisklessSwitchFlowTest.scala`
- **Conflict**: 4.3 builds deltas with `MetadataDelta.Builder`. Main uses the `MetadataDelta` constructor.
- **Resolution**: took the incoming comments and settle logic, and kept `MetadataDelta.Builder().setImage(...).build()`.

---

## Compilation errors

| # | File | Error | Cherry-pick | Fix | Status |
|---|------|-------|-------------|-----|--------|
| 1 | `metadata/.../ConfigurationControlManagerTest.java` | `incrementalAlterConfig` and `legacyAlterConfigs` need a `forwarded` argument | #852 | pass `false` | Fixed |
| 2 | `metadata/.../ReplicationControlManagerInklessTest.java` | `createTopics` needs a `forwarded` argument | #845 | pass `false` | Fixed |
| 3 | `metadata/.../metrics/ControllerMetadataMetricsPublisherTest.java` | `MetadataDelta` has no public constructor | #835 | `MetadataDelta.Builder` | Fixed |
| 4 | `core/.../ControllerApisTest.scala` | `ControllerApis#createTopics` and `Controller#createTopics` need a `forwarded` argument | #832, #839, #846 | `forwarded = false` and `anyBoolean()` | Fixed |

All fixes are in one sync(compile) commit.

## Test notes

`RemoteLogManagerTest.testHasReadableRemoteLogCoverageWhenCopyFinishedCoversOffset` failed once with a `ClassCastException` from `isReady`. A background thread called the mock while the test stubbed `listRemoteLogSegments`. The class passes on a rerun. The test is the same on main, so the flake isn't specific to 4.3.
