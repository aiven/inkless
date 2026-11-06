# Release sync session: `inkless-4.3` to 4.3.2

## Session info
- **Date**: 2026-11-06
- **Release branch**: `inkless-4.3`
- **Working branch**: `inkless-4.3-sync-4.3.2`
- **Target tag**: `4.3.2`
- **Commits to merge**: 102
- **Status**: complete locally, not pushed

---

## Phase 1: discovery

```bash
./inkless-sync/release-sync.sh inkless-4.3 --list-tags
```

- Current Inkless version: `4.3.1-inkless`
- Current base version: `4.3.1`
- Target version: `4.3.2`

The branch already contains Inkless 0.51 (`inkless-4.3.1-0.51`).

---

## Phase 2: merge

```bash
./inkless-sync/release-sync.sh inkless-4.3 --to-tag 4.3.2 --branch inkless-4.3-sync-4.3.2 --yes
```

Without `--yes`, the script waits at its confirmation prompt and exits when it runs without a terminal.

| Category | Count | Files |
|----------|-------|-------|
| Version files | 4 | `gradle.properties`, `tests/kafkatest/__init__.py`, `tests/kafkatest/version.py`, `committer-tools/kafka-merge-pr.py` |
| POM files | 3 | `streams/quickstart/pom.xml`, `streams/quickstart/java/pom.xml`, `streams/quickstart/java/src/main/resources/archetype-resources/pom.xml` |
| Test files | 2 | `RemoteLogManagerTest.java`, `server` `DynamicBrokerConfigTest.java` |
| Interleaved broker logic | 2 | `KafkaApis.scala`, `RemoteLogManager.java` |
| Dependencies | 0 | `gradle/dependencies.gradle` merged without conflicts |

---

## Phase 3: conflict resolution

### Version files

Each file keeps the Inkless side, with the version raised from 4.3.1 to 4.3.2:

| File | Resolution |
|------|------------|
| `gradle.properties` | `version=4.3.2-inkless`, with the Inkless note comment |
| `tests/kafkatest/__init__.py` | `__version__ = '4.3.2+inkless'` |
| `tests/kafkatest/version.py` | `DEV_VERSION = KafkaVersion("4.3.2-inkless")` |
| `committer-tools/kafka-merge-pr.py` | `DEFAULT_FIX_VERSION = "4.3.2-inkless"` |
| The three `streams/quickstart` POM files | `4.3.2-inkless` |

The branch has no `docs/js/templateData.js`.

### Dependencies

Upstream bumps Jackson, Jetty, JLine, Log4j 2, LZ4, and snappy-java, and replaces `jaxAnnotationApi` with `jakartaAnnotationApi`. Each key in `gradle/dependencies.gradle` appears once after the merge, and no Inkless module uses the removed artifact.

### Test files

- `storage/src/test/java/org/apache/kafka/server/log/remote/storage/RemoteLogManagerTest.java`: the import block. Kept the Inkless `OffsetOutOfRangeException` import and added the upstream `TopicConfig` import before it.
- `server/src/test/java/org/apache/kafka/server/config/DynamicBrokerConfigTest.java`: both sides added a test at the same spot. Kept the Inkless `testInklessControlPlaneConnectionStringsAreDynamicAndClusterWide` and the upstream `testValidateConfigTypesResolvesConfigProviders`.

### Interleaved broker logic

- `core/src/main/scala/kafka/server/KafkaApis.scala`: share fetch byte accounting. Upstream reads the topic name once and skips the metric when the name is unknown. Inkless passes an `isDiskless` flag to `updateBytesOut`. Took the upstream null check and computed `isDiskless` from that topic name.
- `storage/src/main/java/org/apache/kafka/server/log/remote/storage/RemoteLogManager.java`: remote retention. Upstream calls `maybeSeedHighestOffsetInRemoteStorage(log)` before size-based retention. Inkless reads the log start offset through `reclaimFloorLogStartOffset(log)`. Kept the upstream seed call first, then the Inkless reclaim floor.

No conflict touched an Inkless-owned path.

---

## Phase 4: verification

```bash
./gradlew compileTestJava compileTestScala
./gradlew verifyVersionConsistency
```

Both pass. `verifyVersionConsistency` reports that all versions match.

A targeted test run on JDK 17 ran 2809 tests with no failures:

- `:server:test`: `DynamicBrokerConfigTest`
- `:storage:test`: `RemoteLogManagerTest` and `UnifiedLogTest`
- `:storage:inkless:test`: the whole module
- `:metadata:test`: `ReplicationControlManagerInklessTest` and `ConfigurationControlManagerTest`
- `:core:test`: `KafkaApisTest`, `ReplicaManagerInklessTest`, `ControllerApisTest`, `io.aiven.inkless.consolidation.*`, `DisklessSwitchFlowTest`, and `DynamicBrokerConfigTest`

The full suite runs in CI on the PR.

---

## Summary

Merge commit: `8cf07c462b Merge upstream 4.3.2 into inkless-4.3-sync-4.3.2`

There are no blockers.

## Notes

After the PR merges, tag the branch head as `inkless-4.3.2-0.51`. The tag is a Kafka patch update on the same Inkless iteration. Pushing it runs `inkless-publish.yml`, which builds the `4.3.2-0.51` Docker images and moves the `4.3-latest` alias. On a tag push, `finalize-release` doesn't run, so the `inkless-release-0.51` GitHub Release stays as it is.
