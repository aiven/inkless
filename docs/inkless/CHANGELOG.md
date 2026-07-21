# Inkless Changelog

Detailed, per-increment log of Inkless changes. Each section covers one Inkless
iteration (`inkless-release-<N-1>` -> `inkless-release-<N>`) and lists the
mainline commits plus operator-facing config and metric changes.

- For release artifacts (Docker images, binaries), see [RELEASES.md](RELEASES.md).
- For how versions work, see [VERSIONING-STRATEGY.md](VERSIONING-STRATEGY.md).
- GitHub release notes are a **curated summary** of these entries (features and
  fixes plus config/metric changes; chores, tests, and docs are dropped).

## How this file is produced

Entries are generated from git and the auto-generated docs, then curated:

- **Commits** come from `git log --first-parent --no-merges` between the two
  release tags, categorized by conventional-commit type. `--first-parent`
  excludes the individual upstream commits pulled in by `apache/kafka` merge
  commits.
- **Config changes** are diffed from `docs/inkless/configs.rst` (reliable).
- **Metric changes** are diffed from `docs/inkless/metrics.rst`. This file is
  produced by a hand-maintained registry list in `MetricsDocs.main()`, so a
  delta can reflect documentation catch-up rather than a newly shipped metric
  (see the 0.39 note). Metric deltas are always curated before publishing.
- **Upstream syncs** are detected from `apache/kafka` merge commits in the range
  and a `gradle.properties` version delta; when `main`'s Kafka development base
  moves, it is noted as a blockquote under the increment heading.

Regenerate a draft with the `inkless-changelog` skill (or run it directly):

```bash
.ai-agents/skills/inkless-changelog/gen-changelog.py --version <N>            # detailed entry
.ai-agents/skills/inkless-changelog/gen-changelog.py --version <N> --summary  # release-notes summary
```

---

## 0.44 (Kafka 4.1.2, 4.2.1)

### Features
- (inkless:switch) auto-enable remote.storage atomically on diskless switch (#678)
- (inkless:consolidation) [KC-298] add JMX metrics for the cross-tier log start offset (#682)
- (inkless) [KC-298] track and serve the cross-tier earliest offset for consolidated topics (#670)
- (inkless:switch) implement AlterDisklessSwitch and tooling [KC-97] (#665)
- (inkless) resolve client AZ from listener map in metadata transformer (#684)
- (inkless) add inkless.client.az.listener.map config (#683)
- (inkless:consolidation) route consolidation fetcher through coldpath to avoid cache pollution [KC-171] (#679)
- (inkless) coalesce contiguous batches into one row in commit_file_v2 (#671)
- (inkless) add partition fan-in commit metrics (#675)

### Fixes
- (inkless:consolidation) serve cross-tier earliest offset promptly after startup (#688)
- (inkless) find_batches rejects offset below log start offset (#666)
- (inkless:consolidation) don't fence a consolidating leader below the seal (#677)
- (inkless:controller) fix leader skew for managed diskless after rolling restart (#643)
- (inkless:consolidation) recover a switched consolidated leader after local-log loss (#673)

### Tests
- (inkless:consolidation) add dependency-outage system test for consolidation pipeline (#669)
- (inkless:consolidation) add read-from-remote system tests, harness support (#654)
- (inkless:consolidation) add born-consolidated pipeline + WAL-prune system test (#674)

### Docs
- (inkless) document the interceptors introduced for CREATE_TOPIC (#680)

### Config & metric changes
- config added: `inkless.client.az.listener.map`
- config added: `inkless.consume.cross.tier.log.start.cache.enabled`
- config added: `inkless.consume.cross.tier.log.start.cache.ttl.ms`
- config added: `inkless.control.plane.batch.coalescing.enabled`
- config added: `inkless.cross.tier.log.start.report.interval.ms`
- metric mbean added: `io.aiven.inkless.cache:type=CrossTierLogStartCache`
- metric mbean added: `io.aiven.inkless.delete:type=CrossTierLogStartReporter`
- metric attrs added: `CrossTierLogStartCache :: CacheHits, CacheMisses, CacheSize`
- metric attrs added: `CrossTierLogStartReporter :: PartitionsReported, PendingPartitions, ReportErrors`
- metric attrs added: `PostgresControlPlane :: AdvanceCrossTierLogStartQueryRate, AdvanceCrossTierLogStartQueryTime`
- metric attrs added: `FileCommitter :: BatchesPerPartitionPerCommit, PartitionsPerCommit`

## 0.43 (Kafka 4.1.2, 4.2.1)

### Fixes
- (inkless:consolidation) start consolidation when remote storage is enabled on a diskless topic (#672)
- (inkless:consolidation) complete the classic-to-consolidated switch by always sealing and registering (#651)
- (inkless) Add PostgreSQL socket timeouts for Inkless control plane (#668)
- (inkless:metrics) exclude consolidating partitions from URP metrics (#640)
- (inkless) truncate above the seal only if those messages are not consolidated (#667)

### Config & metric changes
- config added: `inkless.control.plane.connection.pool.timeout.ms`
- config added: `inkless.control.plane.read.connection.pool.timeout.ms`
- config added: `inkless.control.plane.read.socket.timeout.ms`
- config added: `inkless.control.plane.read.tcp.connect.timeout.ms`
- config added: `inkless.control.plane.socket.timeout.ms`
- config added: `inkless.control.plane.tcp.connect.timeout.ms`
- config added: `inkless.control.plane.write.connection.pool.timeout.ms`
- config added: `inkless.control.plane.write.socket.timeout.ms`
- config added: `inkless.control.plane.write.tcp.connect.timeout.ms`

## 0.42 (Kafka 4.1.2, 4.2.1)

### Features
- (inkless:tools) add operator tooling for topic type switch [KC-97] (#661)
- (inkless:consolidation) supplement local log with diskless data on fetch [KC-168] (#638)
- (inkless:consolidation) serve consolidated reads from remote via OFFSET_MOVED_TO_TIERED_STORAGE (#650)
- (inkless) ensure unclean leader election is disabled on classic-to-diskless switch [KC-129] (#647)
- (inkless) KC-156 diskless leader epoch for consolidation truncation (#631)
- (inkless) add controller guards for classic-to-diskless switch [POD-2464] (#634)
- (inkless:test) implement system tests for classic to diskless switch (#581)
- (inkless:consolidation) make fetch quota config dynamic (#637)
- (inkless) add dedicated rate limit for consolidation fetch [KC-145] (#636)
- (inkless:consolidation) add separate wiring for consolidation fetcher [KC-160] (#635)

### Fixes
- (inkless:switch) Fix stale HW on switched leader promotion (#660)
- (inkless:systest) fix sigstop and slow consumer giving false negatives (#659)
- (inkless) create tiered topic when diskless.enable=false and system is disabled (#641)
- (inkless) reject topic creation with config conflicting with diskless regex (#642)
- (inkless) Remove consolidating fetchers unconditionally (#629)

### Refactors
- (inkless) clarify fetch routing for consolidating partitions (#633)

### Tests
- (inkless:systest) run ducktape system tests against consolidated topics (#645)
- (inkless) Integration test for diskless + consolidate separately (#639)

### Docs
- (inkless) Enrich documentation of classicToDisklessStartOffset (#658)
- (inkless) fix rendering of release sync prompt (#648)
- (inkless:switch) Document classic to diskless switch (#632)

### Config & metric changes
- no config changes

## 0.41 (Kafka 4.1.2, 4.2.0)

### Features
- (inkless) KC-72 Reconcile stale records after diskless switch (#612)
- (inkless:consume) Add request hedging for storage reads (#582)

### Fixes
- (inkless) initialize diskless switch for legacy alter configs (#630)
- (inkless:consolidation) prevent ConsolidationFetcherThread crash on topic deletion (#627)
- (inkless:switch) bump leader epoch on classic-to-diskless switch (#626)
- (inkless:build) pass commitId to Gradle for worktree builds (#628)
- (inkless) retry describeTopics in InklessManagedReplicasClusterTest for metadata propagation (#625)
- (inkless) ensure ReplicaManager shutdown in Inkless tests (#623)

### Refactors
- (inkless) SharedState to own StorageBackend lifecycle (#562)
- (inkless) extract Inkless tests from ReplicaManagerTest into standalone file (#624)

### Tests
- (inkless:switch) add consolidated diskless integration tests with transition matrix (#617)

### Docs
- (inkless) add system test documentation (#588)

### Config & metric changes
- config added: `inkless.fetch.hedge.total.time.threshold.ms`
- config added: `inkless.fetch.hedge.ttfb.threshold.ms`
- metric attrs added: `InklessFetchMetrics :: HedgeRequestRate, HedgeWonRate, HedgeTtfbTriggeredRate, HedgeTotalTimeTriggeredRate` (request hedging, #582)

## 0.40 (Kafka 4.1.2, 4.2.0)

### Features
- (inkless) Introduce CREATE_TOPICS config interceptor framework and DisklessForceCreateTopicInterceptor (#614)
- (inkless:switch) auto-enable remote.storage.enable on diskless topic creation (#619)
- (inkless:switch) validate diskless requires remote storage when consolidation enabled (#616)
- (inkless:switch) expose DisklessWithoutRemoteStorageCount metric for legacy topics (#618)
- (inkless) Support OffsetsForLeaderEpoch for partitions switched to diskless (#613)
- (inkless) POD-2395 Prune consolidated diskless offsets (#587)
- (inkless:switch) support DELETE_RECORDS for hybrid partitions (#611)
- (inkless) POD-2456 Enable offset fetch in consolidating partitions (#594)
- (inkless) POD-2457 Allow transactional offset commits for Diskless sources (#596)

### Fixes
- (inkless) add KafkaConfigTest validation for diskless force topic regexes (#622)
- (inkless) allow searching offsets in UnifiedLog only if switched topics are in sync (#620)
- (inkless) correctly migrate all producer states after a diskless switch (#615)
- (inkless:fetch) properly propagate exception back on failing FetchOffset (#608)
- (inkless:switch) Advance HW past stale checkpoint for sealed leader after restart (#605)
- (inkless:switch) Init Diskless Log on Control Plane on leader changes (#603)
- (inkless:migration) reschedule fetcher on leader change during pending migration (#600)
- (inkless) POD-1965 Use local log start in DisklessLeaderEndPoint (#591)
- (inkless:migration) catch up replicas that are below the seal (#598)
- (inkless) Ensure additional system topics created as classic when log.diskless.enable=true [POD-1312] (#586)

### Refactors
- (inkless:consolidation) rename and improve ConsolidationMetrics (#621)
- (inkless:switch) drop sealing and registering from BrokerMetadataPublisher (#604)
- (inkless) Rename diskless migration to diskless switch (#601)

### Tests
- (inkless:switch) Add invariant tests for sealed partition recovery (#609)
- (inkless) add test for verifying consolidating partition reassignment (#599)

### Chores
- (inkless) remove FileMerger component and all related infrastructure (#607)

### Config & metric changes
- config added: `inkless.consolidation.cleanup.interval.ms`
- config removed: `inkless.control.plane.file.merge.lock.period.ms`
- config removed: `inkless.control.plane.file.merge.size.threshold.bytes`
- config removed: `inkless.control.plane.read.file.merge.lock.period.ms`
- config removed: `inkless.control.plane.read.file.merge.size.threshold.bytes`
- config removed: `inkless.control.plane.write.file.merge.lock.period.ms`
- config removed: `inkless.control.plane.write.file.merge.size.threshold.bytes`
- config removed: `inkless.file.merger.interval.ms`
- config removed: `inkless.file.merger.temp.dir`
- metric mbean removed: `io.aiven.inkless.merge:type=FileMerger` (FileMerger component removed, #607)
- metric attrs removed: `FileMerger :: FileMergeErrorRate, FileMergeFilesRate, FileMergeRate, FileMergeTotalTime, FileUploadTime` (#607)
- metric attrs removed: `PostgresControlPlane :: {Commit,Get,Release}FileMergeWorkItemQuery{Rate,Time}` (#607)

## 0.39 (Kafka 4.1.2)

### Features
- (inkless) fast fail producing during classic-to-diskless migration (#595)
- (inkless) Always allow fetching from replicas for migrated partitions (#593)
- (inkless) extended metrics for diskless migration states tracking (#589)
- (inkless) add metrics for consolidated topic partitions (#590)

### Config & metric changes
- New metrics for diskless migration state tracking (#589) and consolidated
  topic partitions (#590).
- Documentation catch-up: `docs/inkless/metrics.rst` expanded from 3 to 16 mbeans
  in this release (KC-1, #592). Most of the mbeans that appear "new" in the doc
  diff (fetch, produce, delete, cache, control-plane, thread-pool, AZ-awareness)
  were already shipping earlier; they became documented here. Do not read the
  raw metrics diff for this release as newly introduced metrics.

## 0.38 (Kafka 4.1.2)

### Features
- (inkless) Support ListOffsets for all cases of diskless partitions (#584)
- (inkless) POD-2394 Implement read path for consolidated logs (#583)
- (inkless) Support basic ListOffset for hybrid topics (#577)
- (inkless) POD-2398 Integration test for consolidation produce path (#569)
- (inkless) POD-2393 Add DisklessLeaderEndPoint to handle fetch requests (#568)
- (inkless) Set migrating partitions in KRaft metadata (#580)
- (inkless) Abort transactions on partition sealing (#573)
- (inkless) POD-2392 Implement consolidating partition tracking (#567)
- (inkless:consume) Add time-to-first-byte (TTFB) metric for storage reads (#575)
- (inkless) Init diskless log on Control Plane (#563)
- (inkless) Add TS Consolidation configs (#556)
- (inkless:config) enforce diskless feature flag dependency chain (#560)
- (inkless:docker) add diskless tiered storage unification demo (#559)
- (inkless) allow reading from UnifiedLog for diskless topics (#553)
- (inkless:metadata) Remove topic already diskless from InitDisklessLog Controller API (#547)
- (inkless) Orchestrate classic-to-diskless migration (#536)
- (inkless) Parse request and response of InitDisklessLog (#545)
- (inkless:config) Enforce diskless.allow.from.classic.enable (#546)

### Fixes
- (inkless) Stop replicating after migration to diskless is completed (#585)
- (cache) respect max-idle=-1 (disable idle eviction) (#470)
- (inkless) Create Partition object even when diskless is enabled (#574)
- (inkless:migration) Avoid deadlock in InitDisklessLogBatchQueue (#578)
- (inkless) Add InitDisklessLog JSON conversion to RequestConvertToJson (#570)
- (inkless:migration) Fix InitDisklessLogManager gaps during partition registration (#566)
- (inkless:migration) fix classic-to-diskless migration validation and add LogConfigTest to CI (#558)
- (inkless:migration) bypass diskless/remote-storage mutual exclusion for classic-to-diskless migration (#555)
- (inkless:consume) handle error path in lastOffsetForLeaderEpoch for diskless topics (#554)
- (inkless:metrics) Fix double-counting of diskless topic and partition metrics (#552)

### Refactors
- (inkless) Rename disklessStartOffset to classicToDisklessStartOffset (#572)
- (inkless) Split InitDisklessLogManager logic into separate components (#561)
- (inkless) Refactor InitDisklessLog flow and add integration tests (#549)

### Other
- Fix potential leak when closing resources (#565)
- update Kafka version variables to 4.2.0-inkless-SNAPSHOT (#548)

### Config & metric changes
- config added: `inkless.consolidation.*` (TS Consolidation configs, #556)
- `diskless.allow.from.classic.enable` feature flag enforced (#546, #560).

## 0.37 (Kafka 4.0.2, 4.1.2)

### Features
- (inkless) add bytes limit to cache (#544)
- (inkless) Expose InitDisklessLog controller API (#541)
- (controller:diskless) add partitions support for diskless topics (#540)
- (controller:diskless) enable immediate partition reassignment (#537)
- (storage:inkless) add control plane method for getting the producer states (#530)
- (inkless) Add metrics for sealed partitions (#534)
- (inkless:sync) add upstream sync tooling and documentation (#498)
- (diskless) add managed replicas routing to metadata transformer (#504)
- (inkless) Seal the local log when a topic is migrated from classic to diskless (#533)
- (inkless) implement InitDisklessLog Controller API (#531)
- (metadata:diskless) add controller metrics for diskless topics (#503)
- (metadata:diskless) implement managed replicas for diskless topics (#492)
- (storage:inkless) InitDisklessLog Diskless Controller API (#528)
- (produce) optimize AppendCompleter to complete futures first (#529)

### Refactors
- (inkless:consume) replace ByteRange.coalesce with BoundingRangeAlignment strategy (#532)
- (inkless) improve thread pool lifecycle management (#475)
- (metadata:diskless) preserve leader epoch in metadata transformer (#539)
- (inkless) cache LogConfig in InklessMetadataView (#474)

### Tests
- (metadata:diskless) add integration tests for managed replicas (#542)

### Docs
- (inkless) add managed replicas documentation (#535)
- (docker:inkless) add managed replicas demo with test procedure (#543)

### Config & metric changes
- config added: `inkless.consume.cache.max.bytes`

## 0.36 (Kafka 4.0.0, 4.0.2, 4.1.1, 4.1.2)

### Features
- (inkless:config) disallow setting diskless.enable if diskless storage system is disabled (#520)

### Fixes
- (inkless:test) Fix KafkaConfigTest for CLASSIC_REMOTE_STORAGE_FORCE_EXCLUDE_TOPIC_REGEXES_CONFIG (#521)
- (inkless) Fix error message when diskless.enable and remote.storage.enable are set (#516)

### Chores
- (build) Set Docker API version 1.44 in Gradle config (#519)

### Config & metric changes
- no config changes

## 0.35 (Kafka 4.0.0, 4.1.1)

> Upstream sync: main development base moved to Kafka 4.2.0-SNAPSHOT (from 4.1.0-SNAPSHOT), Scala 2.13.16 -> 2.13.17.

### Features
- (metadata) Introduce ClassicTopicRemoteStorageForcePolicy (#514)
- Disallow setting remote.storage.enable when diskless.enable is set to true (#511)

### Chores
- (ci) use Docker API version 1.44 (#509)
- (ci) replace usage of JDK 23 with 25 (#502)
- (inkless) Update demo and documentation for GHCR images (#497)
- (inkless:release) add gh workflows to release inkless artifacts (#489)

### Other
- storage: add metrics constructor to InMemoryStorage (#510)

### Config & metric changes
- no config changes

## 0.34 (Kafka 4.0.0, 4.1.1)

### Features
- Allow switching diskless.enable from false to true (#486)

### Fixes
- (storage:metrics) eagerly initialize meters with only topicType tag (#493)

### Refactors
- (metadata:diskless) fail on topic creation with replica assignment (#488)

### Docs
- (inkless) update architecture diagram (#487)
- (inkless) update readme with new sections and glossary (#483)
- (inkless) fix performance docs (#482)
- (inkless) fix relation with kips on client-az awareness (#485)
- (inkless) add az-alignment feature documentation (#481)

### Chores
- (storage:inkless) add jooq classes (#490)

### Config & metric changes
- no config changes

## 0.33 (Kafka 4.0.0, 4.1.1)

First release under the global Inkless iteration counter (previously
`inkless-4.0.0-rc32` / `inkless-4.1.1-rc1`; see
[VERSIONING-STRATEGY.md](VERSIONING-STRATEGY.md)).

### Refactors
- (inkless:metrics) only add topic-type tag on all topic stats (#472)

### Docs
- (inkless) add versioning strategy (#479)

### Config & metric changes
- no config changes
