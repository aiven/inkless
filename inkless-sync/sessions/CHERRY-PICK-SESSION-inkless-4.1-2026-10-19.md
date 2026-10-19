# Cherry-pick session: `inkless-4.1`

## Session Info
- **Date**: 2026-10-19
- **Source**: origin/main (6952e9b043, #828)
- **Target**: inkless-4.1
- **Base**: inkless-4.1.2-0.49
- **Status**: complete, pushed

---

## Commits to cherry-pick

| # | Hash | PR | Subject | Status |
|---|------|-----|---------|--------|
| 1 | 8e0fa66813 | #811 | `fix(inkless:test): replace removed minio images with pgsty fork` | Applied |
| 2 | 6e618cdd1d | #807 | `chore(inkless): introduce vale for linting AI-generated prose` | Applied |
| 3 | c1744585c0 | #810 | `fix(inkless:build): enable Zip64 for the JMH shadow jar` | Applied |
| 4 | bf5295f741 | #813 | `fix(inkless:test): raise S3 timeouts and avoid the isolated broker in switch systests` | Applied |
| 5 | b2601d2f6c | #814 | `ci(inkless): add workflow for linting AI-attributed prose` | Applied |
| 6 | 0e8d3d12b6 | #816 | `fix(inkless:consolidation): exclude consolidation from ConsumerLag [KC-546]` | Applied (conflict) |
| 7 | 879b7e42f6 | #815 | `chore(inkless:ai): instruct agents to use Assisted-by AI attribution` | Applied |
| 8 | 95c461edee | #818 | `docs(inkless:release): add 0.49 changelog entry` | Applied |
| 9 | 3f58b1904d | #819 | `docs(inkless): list only maintained branches in RELEASES` | Applied |
| 10 | e466e58e8e | #820 | `test(inkless): pin ListOffsets answers on born-diskless topics before consolidation [KC-537]` | Applied |
| 11 | 9c98296554 | #798 | `fix(inkless:consume): stop a slow ListOffsets from hanging the broker [KC-537]` | Applied |
| 12 | b888104580 | #799 | `perf(inkless:control-plane): index and rewrite list_offsets_v1 timestamp lookups` | Applied |
| 13 | efbb8a16da | #817 | `feat(inkless:ai): support Cursor in the Vale gate hook` | Applied |
| 14 | 287ab06e20 | #822 | `fix(inkless:config): require consolidation for the classic-to-diskless switch` | Applied |
| 15 | 7ccb838e34 | #824 | `feat(inkless:metadata): add classicPrefixInRemoteStorage flag [KC-580]` | Applied |
| 16 | cfdb7fa221 | #821 | `feat(inkless:metrics): add inkless-tag and kafka-version to app-info` | Applied (conflict) |
| 17 | 3649de76a6 | #759 | `feat(inkless:control_plane): make control plane connection config dynamic` | Applied (conflict) |
| 18 | ae2d2d49fb | #827 | `feat(inkless:controller): add the SetClassicPrefixInRemoteStorage RPC [KC-582]` | Applied (conflict) |
| 19 | 6952e9b043 | #828 | `fix(inkless:control-plane): drop the batches timestamp index before release` | Applied |

Build and test 4.1 with JDK 17. Gradle 8.14 does not run on Java 25.

---

## Conflict resolution log

### #816 - consolidation and `ConsumerLag`

**File**: `core/src/main/scala/kafka/server/AbstractFetcherThread.scala`
- **Conflict**: `FetcherLagMetrics` constructor and metrics group.
- **Resolution**: kept 4.1's `new KafkaMetricsGroup(this.getClass)`. Added the `registerConsumerLag: Boolean = true` parameter.

### #821 - app-info metrics

**Files**: `AppInfoParser.java` and its test.
- **Conflict**: 4.1 has no client-id tagging of app-info metrics. Main registers `Gauge` metrics with `Map.of("client-id", ...)` duplicates.
- **Resolution**: kept 4.1's `ImmutableValue` registration. Added only the `inkless-tag` and `kafka-version` metrics. The test checks only those two names.
- **Reason**: the client-id duplicates depend on an upstream change that 4.1 does not have. 4.1 releases without those duplicates.

### #759 - reconfigurable connection strings

**Files**: same as 4.2, plus `storage/inkless/.../postgres/PostgresControlPlane.java`.
- **Resolution**: same Scala port as 4.2. In `PostgresControlPlane`, took `pgMetrics = PostgresControlPlaneMetrics.instance()` and dropped the `KafkaMetricsGroup metrics` field. The commit moves the pool metrics to `PostgresConnectionPoolMetrics.instance()`.

### #827 - `SetClassicPrefixInRemoteStorage`

**Files**: `ApiKeys.java`, `KRaftMetadataCache.java` (modify/delete), `ReplicationControlManager.java`, `server/.../network/RequestConvertToJson.java`.
- **Resolution**: same `ApiKeys` and Scala `KRaftMetadataCache` port as 4.2. Took 4.1's side for the `catch (Throwable e)` hunk in `ReplicationControlManager` (upstream-only). Took 4.1's `RequestConvertToJson` (old `switch` style) and added the imports and the request and response `case` for the new API by hand.

---

## Compilation errors

Same fixes as 4.2 (errors 1 to 7), plus:

| # | File | Error | Cherry-pick | Fix | Status |
|---|------|-------|-------------|-----|--------|
| 8 | `metadata/src/test/.../QuorumControllerTest.java` | `MockRaftClientTestEnv` does not exist | #827 | `LocalLogManagerTestEnv` | Fixed |
| 9 | `storage/inkless/.../PostgresControlPlane.java` | stale `KafkaMetricsGroup` field | #759 | removed the field | Fixed |
| 10 | `core/src/test/java/kafka/server/InklessDisklessListOffsetsTest.java` | `OffsetSpec.earliestPendingUpload` does not exist | #820 | removed that assertion and three unused imports | Fixed |

## Build notes

Building 4.1 on JDK 17 rewrites 124 files under `storage/inkless/src/main/jooq` by dropping `"this-escape"` from `@SuppressWarnings`. Do not commit that change. Gradle 8.14 does not run on Java 25, so use JDK 17.
