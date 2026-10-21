# Cherry-pick session: `inkless-4.2`

## Session Info
- **Date**: 2026-10-19
- **Source**: origin/main (6952e9b043, #828)
- **Target**: inkless-4.2
- **Base**: inkless-4.2.2-0.49
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
| 6 | 0e8d3d12b6 | #816 | `fix(inkless:consolidation): exclude consolidation from ConsumerLag [KC-546]` | Applied |
| 7 | 879b7e42f6 | #815 | `chore(inkless:ai): instruct agents to use Assisted-by AI attribution` | Applied |
| 8 | 95c461edee | #818 | `docs(inkless:release): add 0.49 changelog entry` | Applied |
| 9 | 3f58b1904d | #819 | `docs(inkless): list only maintained branches in RELEASES` | Applied |
| 10 | e466e58e8e | #820 | `test(inkless): pin ListOffsets answers on born-diskless topics before consolidation [KC-537]` | Applied |
| 11 | 9c98296554 | #798 | `fix(inkless:consume): stop a slow ListOffsets from hanging the broker [KC-537]` | Applied |
| 12 | b888104580 | #799 | `perf(inkless:control-plane): index and rewrite list_offsets_v1 timestamp lookups` | Applied |
| 13 | efbb8a16da | #817 | `feat(inkless:ai): support Cursor in the Vale gate hook` | Applied |
| 14 | 287ab06e20 | #822 | `fix(inkless:config): require consolidation for the classic-to-diskless switch` | Applied |
| 15 | 7ccb838e34 | #824 | `feat(inkless:metadata): add classicPrefixInRemoteStorage flag [KC-580]` | Applied |
| 16 | cfdb7fa221 | #821 | `feat(inkless:metrics): add inkless-tag and kafka-version to app-info` | Applied |
| 17 | 3649de76a6 | #759 | `feat(inkless:control_plane): make control plane connection config dynamic` | Applied (conflict) |
| 18 | ae2d2d49fb | #827 | `feat(inkless:controller): add the SetClassicPrefixInRemoteStorage RPC [KC-582]` | Applied (conflict) |
| 19 | 6952e9b043 | #828 | `fix(inkless:control-plane): drop the batches timestamp index before release` | Applied |

---

## Conflict resolution log

### #759 - reconfigurable connection strings

**Files**: `core/src/main/scala/kafka/server/DynamicBrokerConfig.scala`, `core/src/test/scala/unit/kafka/server/DynamicBrokerConfigTest.scala`, `server/.../config/DynamicBrokerConfig.java` and its test (modify/delete).
- **Conflict**: main moved the reconfigurable config sets to a Java `DynamicBrokerConfig`. 4.2 keeps them in the Scala `DynamicBrokerConfig.AllDynamicConfigs`.
- **Resolution**: removed the two Java files. Added `InklessControlPlaneConfigs.RECONFIGURABLE_CONFIGS.asScala` to the Scala `AllDynamicConfigs`. Kept the new test methods. Dropped the group and share coordinator tests that 4.2 does not have. Replaced the `JDynamicBrokerConfig` assertions with a check against `DynamicBrokerConfig.AllDynamicConfigs`.
- **Reason**: the Scala `BrokerReconfigurable#reconfigurableConfigs` returns a Scala `Set`. `DynamicInklessControlPlaneConfig` returns `ReconfigurableConfigs.asScala`, and `DynamicConfigChangeTest` compares with a Scala `Set`.

### #827 - `SetClassicPrefixInRemoteStorage`

**Files**: `clients/.../protocol/ApiKeys.java`, `metadata/.../KRaftMetadataCache.java` (modify/delete), `metadata/src/test/.../QuorumControllerTest.java`.
- **Conflict**: `ApiKeys` enum tail. 4.2 has no Java `KRaftMetadataCache`.
- **Resolution**: added `SET_CLASSIC_PREFIX_IN_REMOTE_STORAGE` after `REPAIR_DISKLESS_LOG`. Ported the `classicPrefixInRemoteStorage` tagged field to `core/.../metadata/KRaftMetadataCache.scala`.
- **Reason**: the Scala cache builds the same response on 4.2.
- **Follow-up**: the `QuorumControllerTest` conflict also pulled in `testIsNodeIdRegisteredWithDynamicQuorum`, a main-only test that fails on 4.2. The sync(compile) commit removes it.

---

## Compilation errors

| # | File | Error | Cherry-pick | Fix | Status |
|---|------|-------|-------------|-----|--------|
| 1 | `metadata/.../ReplicationControlManager.java` | `PartitionChangeBuilder` has no ELR argument | #827 | `.setEligibleLeaderReplicasEnabled(...)` | Fixed |
| 2 | `metadata/.../PartitionChangeBuilderTest.java` | same | #827 | same | Fixed |
| 3 | `core/.../DynamicBrokerConfig.scala` | `reconfigurableConfigs` must return a Scala `Set` | #759 | `ReconfigurableConfigs.asScala` | Fixed |
| 4 | `core/.../DynamicBrokerConfigTest.scala` | `JSocketServer` and `never` not found | #759 | `SocketServer.ReconfigurableConfigs`, `Mockito.never()` | Fixed |
| 5 | `core/.../DynamicConfigPublisherTest.scala` | `MetadataDelta` has no public constructor | #759 | `MetadataDelta.Builder` | Fixed |
| 6 | `storage/inkless/.../ListOffsetsJobTest.java:49` | package `common.record.internal` does not exist | #799 | import `org.apache.kafka.common.record.RecordBatch` | Fixed |
| 7 | `core/.../DynamicConfigChangeTest.scala` | Java `Set` is not equal to the Scala `Set` | #759 | compare with `Set(...)` | Fixed |

All fixes are in one sync(compile) commit.
