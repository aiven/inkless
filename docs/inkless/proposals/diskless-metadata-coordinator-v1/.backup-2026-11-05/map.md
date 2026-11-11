# Diskless metadata coordinator: map

## Destination

A spec, [spec.md](spec.md), that a team can hand off to implement a third `ControlPlane` backend. The wayfinding design record is [design.md](design.md). The backend stores coordinator state in a multi-partition `__diskless_metadata` topic, with a SQLite copy of each partition's state on every replica of that partition. It is a minimal subset of [KIP-1164](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783984/KIP-1164+Diskless+Coordinator), and it passes the shared `AbstractControlPlaneTest` suite. The comparison of owner-based and scanner deletion, written for the upstream KIPs, is [deletion-comparison.md](deletion-comparison.md).

## Notes

- Domain: [GLOSSARY.md](../../docs/inkless/GLOSSARY.md). The service is the Batch Coordinator. The `ControlPlane` interface is its storage layer.
- Style for the spec: [AGENTS.md](../../AGENTS.md), engineering rules and style section.
- Kafka references: look them up in the local Kafka checkout, not on the web.
- Tracker: local markdown, per the wayfinder default. No tickets exist yet. Decisions below were resolved in conversation and are recorded here instead.

## Decisions so far

- Destination is a spec to hand off. See [Goals](design.md#goals).
- Scope is a third `ControlPlane` implementation. The callers stay unchanged. See [Non-goals](design.md#non-goals).
- `__diskless_metadata` has `diskless.metadata.topic.num.partitions` partitions, 8 by default and provisional. Each coordinator partition's leader is the single writer for its user partitions. See [Topology](design.md#topology).
- A user partition maps to a slot, `Utils.toPositive(Utils.murmur2(logKey)) % S`, and the slot maps to coordinator partition `slot % N`, with `N` read from the metadata image. `N` is fixed after creation, and the controller rejects `CreatePartitions` for the topic. A test with fixed expected values pins the mapping, and each leader rejects a mismatched user partition with `INVALID_REQUEST`. See [Coordinator partitions](design.md#coordinator-partitions).
- The spec adopts the slot layer now, because the mapping is part of the protocol and can't change after release. It keeps growing `N` by splitting coordinator partitions possible without remapping every user partition. See [Future work](design.md#future-work).
- `S` is the protocol constant 4096, not a config. It caps the coordinator partition count and the grain at which state could move later. See [Coordinator partitions](design.md#coordinator-partitions).
- The spec assigns slots as `slot % N`, and `diskless.metadata.topic.num.partitions` accepts only a power of two from 1 to `S`. Doubling `N` then keeps the existing partition IDs and moves half of each partition's slots. The mapping equals `murmur2 % N`, so brokers don't store a slot table, and the slot layer costs nothing at runtime. Contiguous slot ranges lost, because doubling `N` would change the owner of almost every slot. See [Coordinator partitions](design.md#coordinator-partitions).
- Cell-aware slot placement is dropped. A user partition's slot comes from a hash of its log key, so one cell's partitions map to random slots, and grouping slots can't shrink an object's participant set.
- The mapping input is a log key. The Kafka-partition encoding, the topic ID and partition index in 20 bytes, is the only encoding. A leading type byte lost, because a later encoding can include its own. See [Coordinator partitions](design.md#coordinator-partitions).
- Records name the log they change with a `Log` struct that holds `TopicId` and `PartitionIndex`. A later encoding adds a tagged field. Opaque key bytes lost, because tooling couldn't read them. See [Log records](design.md#log-records).
- A coordinate row must be able to describe an extent, with the producer fields optional. This effort writes only WAL batch rows, and the `ControlPlane` interface doesn't change. The record type that swaps batch rows for extents is future work, not a reserved type. See [Local state](design.md#local-state).
- Tiered metadata in the coordinator stays out of scope. [Future work](design.md#future-work) sketches it and lists the two constraints this spec keeps for it: a user partition's batches and its remote segments map to the same coordinator partition by the same log key, and pruning stays decided on the broker that hosts the local log.
- [deletion-comparison.md](deletion-comparison.md#merged-and-multi-partition-objects) shows that owner-based deletion covers merged and multi-partition objects, as a consequence of the chosen design rather than a new decision.
- The delegate splits each call by coordinator partition, sends the parts in parallel, and merges the results in request order. A non-leader returns `NOT_COORDINATOR`. Private RPCs use the 504+ range. See [Transport](design.md#transport).
- Each replica keeps a SQLite copy of its coordinator partition's state. See [Local state](design.md#local-state).
- Every replica keeps its own snapshots. A replica that needs records below the log start fetches a covering snapshot over a private RPC, from an in-sync follower when one has it and from the leader otherwise. Only the leader prunes the log. Each coordinator partition runs these rules on its own. See [Snapshots](design.md#snapshots).
- Every replica takes a snapshot on an interval (1 hour) or a byte threshold (1 GiB), whichever comes first, so a newly elected leader with a damaged database can rebuild from a peer. Both defaults are provisional until the state-size and failover benchmarks run. Each replica keeps exactly two snapshots, as a constant. See [Snapshots](design.md#snapshots).
- The fetch-snapshot request follows KRaft's `FetchSnapshot` schemas, with 8 MiB chunks sent with zero copy from the file. See [Snapshots](design.md#snapshots).
- The log start is Kafka's own log start offset, advanced by the leader with `maybeIncrementLogStartOffset`. `retention.ms` and `retention.bytes` are unlimited on the topic, so only the snapshot rule moves it. See [Topology](design.md#topology) and [Snapshots](design.md#snapshots).
- The leader advances the log start only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for. A single-replica topic advances from the leader's snapshot alone. Before the first advancement, a rebuild replays from offset 0. See [Snapshots](design.md#snapshots).
- A snapshot's offset and epoch come from the applied-offset row inside the finished copy. A snapshot becomes visible only through a fsync and rename, and the old snapshot is deleted after that. See [Snapshots](design.md#snapshots).
- A transfer names the snapshot ID and a byte position, and the receiver resumes into a temporary file. Rotation keeps a file while its transfer is active, until `diskless.metadata.snapshot.transfer.idle.timeout.ms` (60 seconds) passes with no chunk request. A broker serves at most `diskless.metadata.snapshot.max.concurrent.transfers` (1) transfers across all its coordinator partitions and answers more with a retriable busy error. Snapshot reports retry with Kafka's `ExponentialBackoff` client defaults. A replica serves its finished snapshots whatever the state of its live database. See [Snapshots](design.md#snapshots).
- A snapshot report counts only while its replica is in the leader's committed ISR, and the leader drops it when the replica leaves. Reports run on the broker's scheduler, never on the replica fetcher thread. See [Snapshots](design.md#snapshots).
- SQLite runs in WAL journal mode with `synchronous=NORMAL`. A backup does not block applies, and a power loss costs a replay from the log. See [Local state](design.md#local-state).
- Every change to a diskless partition's coordinator state is a coordinator record, including creation and deletion, and topic purge has its own deleted-log-purged record. KRaft is the source of intent. The record fixes the position among commits, and apply reads nothing outside its own coordinator partition's log and never reads a clock. Every time-based decision is a record the leader appends. See [Log records](design.md#log-records) and [Maintenance](design.md#maintenance).
- Reconciliation runs per coordinator partition, counts pending overlay entries, and a repeated partition-created or topic-deleted record applies as a no-op. Ordering holds per user partition only. See [Lifecycle](design.md#lifecycle).
- The leader keeps an in-memory overlay of appended records that are not yet applied, and it runs that loop on `CoordinatorRuntime`, with one shard per coordinator partition. A failed append drops the failed entry and every entry after it. See [Speculative state](design.md#speculative-state) and [Write path](design.md#write-path).
- A retried commit part is identified by its object key and deduplicated against the overlay and SQLite of its coordinator partition, so a retry during the high-watermark wait appends nothing. See [Commit and retries](design.md#commit-and-retries).
- Apply rules for file records are strict. An identical duplicate is a no-op. A conflicting duplicate or a commit for a fenced or tombstoned key stops the replica, because the leader's checks make them unreachable. See [Commit and retries](design.md#commit-and-retries).
- Each commit part succeeds or fails on its own, with no two-phase commit. The delegate retries each part inside a commit deadline and never fences. Unconfirmed parts get `KAFKA_STORAGE_ERROR`. `commitFile` returns per-batch results if any part committed and throws otherwise. `isSafeToDeleteFile` returns false without a request. See [Commit and retries](design.md#commit-and-retries).
- Every part carries its first-send time, and a leader rejects a part older than `diskless.metadata.commit.max.age.ms` (1 hour, provisional). See [Commit and retries](design.md#commit-and-retries).
- Every object has an owner among its participants: the preferred owner by a hash of the object key, or the next participant in sorted order when the broker falls back. The broker stores the intent at the owner before the upload, and fails the produce with `KAFKA_STORAGE_ERROR` when no participant stores it. See [File references](design.md#file-references).
- Participants report only releases. Once the intent is older than the maximum commit age, and the owner and every participant it has heard from are released, the owner fences the silent ones. A fence is final, and an answer naming another owner makes a stale owner drop its entry. See [File references](design.md#file-references).
- The owner deletes the object once every participant is released and the intent is older than the maximum commit age. `FileCleaner` runs on owner leaders only, and a missing object counts as deleted. See [File references](design.md#file-references).
- A release within the maximum commit age of its commit keeps a tombstone. Tombstones and fences expire after twice the maximum commit age through keys-expired records, so SQLite stays bounded. See [File references](design.md#file-references).
- Owner-based deletion is chosen over a scanner that deletes objects, because a scanner fails toward data loss and depends on clocks. A report-only scanner is future work. See [deletion-comparison.md](deletion-comparison.md).
- Replication factor 3, with `min.insync.replicas=2`. See [Topology](design.md#topology).
- The coordinator topic is created with `flush.messages=1`, because replication alone does not cover a correlated power loss, and for a coordinator partition that loss affects every user partition it serves. The commit-throughput benchmark measures the cost. The applied offset is stored with the leader epoch of that record, so a restart or rebuild can tell a flushed-but-rewritten log from one that still matches SQLite. See [Topology](design.md#topology) and [Local state](design.md#local-state).
- Only the leader answers reads. `findBatches` sends the full `fetchMaxBytes` with each part and trims the merged result in request order. See [Reads](design.md#reads).
- The load on each coordinator leader broker is an open risk. Benchmarks against the Postgres backend, at `N=1` and `N=8`, decide how serious it is, and include partition count, intent cost, and partial failure. See [Capacity and isolation](design.md#capacity-and-isolation).
- The controller never calls the coordinator. Create and delete commit to metadata only, and each coordinator leader reconciles its user partitions from the metadata image, so the metadata log is the retry record. The partition leader calls `initDisklessLog` after the seal is committed, and it calls `repairDisklessLog`. See [Lifecycle](design.md#lifecycle).
- New clusters only. Migration is out of scope. See [Non-goals](design.md#non-goals).
- Retention and topic purge run on each coordinator leader for its user partitions. File deletion runs on owner leaders. See [Maintenance](design.md#maintenance).
- A new leader serves once its high watermark reaches its election-time log end offset. A new owner leader restarts its fence timers from the intents' append times. See [Leader change](design.md#leader-change).
- Every replica applies records only up to the high watermark. The leader drops unreplicated work from its overlay, so a truncation never changes SQLite. See [Local state](design.md#local-state).
- Metrics cover unknown-outcome parts, open owner entries and the oldest one's age, fences by answer, tombstones and fences held, age-check rejections, and mapping mismatches. See [Metrics](design.md#metrics).
- Testing has three layers, and the shared suite runs at `N=1` and `N=8`. See [Testing](design.md#testing).
- Consolidation pruning is decided on the hosting broker and recorded by the leader. See [Consolidation and cross-tier offsets](design.md#consolidation-and-cross-tier-offsets).
- The coordinator feature is finalized at format time and selects the backend. A conflicting `inkless.control.plane.class` fails startup. There is no rollback to Postgres. See [Rollout](design.md#rollout).

## Not yet specified

Items the spec leaves open for the implementer. None of them changes the design above.

- Names and numbers of the private request types, the number of each ApiKey, and the error codes for a busy snapshot source and for a part rejected by the age check.
- Feature flag name and its metadata version.
- Deletion grace period default for `FileCleaner`.
- SQLite driver choice, including its license and packaging check.
- How a leader that cannot rebuild its state gives up leadership of its coordinator partition alone. The controller has no request for this today. Until it exists, the leader marks only that partition offline. See [Lifecycle](design.md#lifecycle).
- Commit deadline default, which must not exceed the produce request timeout.
- How often leaders append keys-expired records.
- The cap on how far the overlay may run ahead of the high watermark.
- The append linger default.
- Benchmark workloads, scales, and pass criteria for each coordinator leader broker's load. Unlike the items in the preceding list, the results can change the design, including the default partition count, the default maximum commit age, and a return to Kafka's default flush.

## Out of scope

- Changing the partition count of `__diskless_metadata` after creation. The slot layer keeps it possible. See [Future work](design.md#future-work).
- Moving an object's owner to another coordinator partition after the commit.
- Orphan-object scanning. A scanner that deletes objects is rejected, and a report-only scanner is future work. See [deletion-comparison.md](deletion-comparison.md).
- Produce gateways and `PreferredProduceBrokers`.
- Moving a user partition between coordinators. Splitting a coordinator partition is the planned form of it. See [Future work](design.md#future-work).
- Transaction state. It stays in the group and transaction coordinators.
- The KIP's public APIs, keys 95 to 101.
- Follower reads of any kind.
- Migrating existing Postgres-backed clusters.
- A Raft quorum for the metadata, whether on the cluster quorum or a separate one.
- Tiered-storage offload, which the KIP leaves to a separate KIP. The design sketches tiered metadata in the coordinator as future work. See [Future work](design.md#future-work).
- Merging WAL objects into extents, and partitionless key ranges. See [Future work](design.md#future-work).
