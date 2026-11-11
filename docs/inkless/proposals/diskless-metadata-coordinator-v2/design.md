# Diskless metadata coordinator

Status: draft for review.

Source: [KIP-1164: Diskless Coordinator](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783984/KIP-1164+Diskless+Coordinator), scoped down to a third `ControlPlane` implementation. This is version 2. It rewrites [version 1](../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md) with the decisions in [map.md](map.md). The handoff spec is [spec.md](spec.md).

## Changes from version 1

A design review found gaps in version 1. Each closed ticket in [issues/](issues/) settles one gap, and this document applies all of them:

- [Ownership model: intents or leases](issues/01-ownership-model.md): per-broker sequence leases at a tracking partition replace the per-object intent at an owner. The produce path loses its round trip before the upload, and deletion safety no longer depends on broker clocks.
- [Broker-side deletion after a partial send](issues/02-delete-after-partial-send.md): the backend turns off the broker's direct delete with a capability that `FileCommitJob` checks before its `ControlPlaneUnavailableException` branch.
- [Time bounds in the chosen model](issues/03-time-bounds.md): no clock decides whether a key is deleted, only when. The idle lease timer is a `CoordinatorTimer`, and retention skew is a Kafka-parity risk with an NTP procedure.
- [Flush policy under Kafka parity](issues/04-flush-policy.md): `flush.messages=1` is required, and the weaker-flush fallback leaves the design.
- [What Kafka's internal-topic status protects](issues/05-internal-topic-protections.md) and [Admin protection for `__diskless_metadata`](issues/06-admin-protection.md): the topic joins `Topic.INTERNAL_TOPICS`, and name-keyed guards with no bypass protect it from admin requests, client produce, and config changes.
- [Read consistency after a leader change](issues/07-read-consistency.md): a deposed leader serves committed but older state, and a barrier record confirms leadership before `findBatches` answers `OFFSET_OUT_OF_RANGE`.
- [Record compatibility across broker versions](issues/08-record-compatibility.md): a replica stops applying at any record it can't read, feature levels gate writes, the controller refuses every downgrade, and the SQLite schema has its own version.
- [Last-known-leader election on `__diskless_metadata`](issues/09-last-known-leader-election.md) and [Guard for last-known-leader election on `__diskless_metadata`](issues/10-last-known-leader-guard.md): storage identity, not shutdown cleanliness, decides which replica may lead, and the replication factor and minimum ISR are fixed at creation.
- [Check against the Postgres performance bar](issues/12-postgres-performance-bar.md) and [Consequences for the cross-cutting sections](issues/11-cross-cutting-consequences.md): the barrier gets its own deadline and never runs from the delayed-fetch probe, and the metrics, failure modes, tests, and user stories are rewritten for the lease model.

Where tickets amend each other, the later one wins: ticket 10 over ticket 06, tickets 11 and 12 over ticket 07, and ticket 08 over ticket 07's note on tagged fields.

## Goals

- Add a `ControlPlane` implementation, `DisklessMetadataControlPlane`, selected with `inkless.control.plane.class`.
- Change the shared commit path in `FileCommitter` so that a broker orders its commits per coordinator partition, not across all its files. The Postgres and in-memory backends keep today's behavior. [Commit path](#commit-path) describes the change.
- Store the coordinator state in the internal topic `__diskless_metadata`, split across a fixed number of partitions. Each diskless user partition maps to one coordinator partition by a hash.
- Keep every replica's state in a local SQLite database, rebuilt from the log and from snapshots.
- Delete every WAL object that the design can orphan, through the uploading broker's tracking partition. No clock decides whether a deletion is safe.
- Lose no data that a classic topic keeps through the same failure, and add no blocking or high per-operation cost on the produce, commit, or read path compared with the Postgres backend.
- Pass the shared `AbstractControlPlaneTest` suite, and the tests in [Testing](#testing).
- Add no public Kafka protocol. The new requests use private ApiKeys in the 504+ range.

## Non-goals

- Changing the partition count of `__diskless_metadata` after the controller creates it. The slot layer keeps it possible, as [Future work](#future-work) describes.
- Orphan-object scanning. A scanner that only reports suspected orphans stays possible as future work.
- An off-broker copy of coordinator state, a restore procedure, and versioned buckets or soft-delete as cover for losing every replica of a coordinator partition. [Accepted risks](#accepted-risks) records that loss.
- A replay checker and an inspect-and-repair tool for a deterministic apply bug that corrupts every replica alike. [Accepted risks](#accepted-risks) records it.
- The benchmark question and the capacity risk in [Capacity and isolation](#capacity-and-isolation).
- Produce gateways and `PreferredProduceBrokers`.
- Moving a user partition between coordinators.
- Transaction state.
- The KIP's public APIs, keys 95 to 101.
- Follower reads.
- Migrating existing Postgres-backed clusters. New clusters only.
- Raft for the metadata quorum.

`__consumer_offsets` and `__transaction_state` split their state by key, because each request touches one group ID or one transactional ID. A WAL object is different: it carries batches for many user partitions, often across topics, and offsets for each user partition must come from one writer. The coordinator splits its state by user partition, so each user partition still has exactly one writer, and a commit goes to every coordinator partition its batches map to. That split needs no two-phase commit. Kafka doesn't make a non-transactional produce atomic across partitions, and `commitFile` already returns one result per batch, so a commit can succeed on one coordinator partition and fail on another. The only state that spans coordinator partitions is the object itself: whether any batch still references it, and whether it was ever committed at all. [Object tracking and deletion](#object-tracking-and-deletion) gives each broker one tracking partition that tracks this state for every object the broker uploads.

## Topology

- The internal topic `__diskless_metadata` has `diskless.metadata.topic.num.partitions` partitions, 8 by default. The partition-count default is provisional.
- The replication factor is `diskless.metadata.topic.replication.factor`, 3 by default. The controller places replicas across racks where possible. The replication factor is fixed at creation: a reassignment that changes the replica count gets `INVALID_REPLICATION_FACTOR`, and one that keeps it passes.
- The topic-level `min.insync.replicas` comes from the controller config `diskless.metadata.topic.min.insync.replicas`. It defaults to `min(2, diskless.metadata.topic.replication.factor)`, and startup validation requires it to be between that default and the replication factor. The controller sets it at creation, and changing the config afterward has no effect. An alter of the topic's `min.insync.replicas` gets `INVALID_CONFIG`, as [Topic protection](#topic-protection) describes. A fixed value of 2 would fail every append at a replication factor of 1.
- The controller creates the topic on first enable, with `unclean.leader.election.enable=false`.
- The leader of each coordinator partition is the single writer for the diskless user partitions that map to it. [Coordinator partitions](#coordinator-partitions) defines the mapping. In the rest of this document, "the leader" means the leader of the coordinator partition that the user partition, call, or object under discussion maps to.
- Every replica fetches its coordinator partitions with normal replica fetching. Replicas don't vote.
- The coordinator appends to the topic with `acks=-1`. `Partition.appendRecordsToLeader` then rejects the append with `NOT_ENOUGH_REPLICAS` whenever the ISR is below `min.insync.replicas`, before the leader writes the record to its log. The high watermark doesn't advance under the minimum ISR either, so an acknowledged record is always on at least `min.insync.replicas` replicas. [Write path](#write-path) names the writer that does this.
- Unclean leader election stays off, so a replica outside the ISR never becomes leader and truncates acknowledged records. The controller's automatic last-known-leader election stays on, with the guards in [Election of a coordinator leader](#election-of-a-coordinator-leader).
- The topic is created with `retention.ms=-1` and `retention.bytes=-1`. Time and size retention never delete a segment, so the snapshot rule in [Snapshots](#snapshots) is the only thing that moves the log start.
- After `LogManager.createLogDirectory` creates a `__diskless_metadata` partition directory, the broker calls `Utils.flushDir` on the log directory. A failed fsync takes the log directory offline, as a failed create does. Renames already fsync the parent through `Utils.atomicMoveWithFallback`. The cost is one fsync per partition directory creation, none on the produce path.

### Flush policy

The topic requires `flush.messages=1`, unlike other Kafka logs. A replica fsyncs every batch it appends, so the record a committed reply points to survives a power loss on every replica at once. The coordinator offers no weaker flush mode, neither as a named unsafe mode nor as a topic config override. [Topic protection](#topic-protection) rejects any alter of `flush.messages`.

On a classic topic, a correlated power loss across the ISR truncates only the unflushed tail of that partition. Here, a broker's objects span its tracking partition, the participant partitions that hold their batches, and object storage, which doesn't roll back when brokers lose power. An unflushed tail breaks invariants across those components and causes corrupted reads, which classic topics don't have:

- Release inversion: a participant applies a batch-removing record and reports the release, and the tracking partition queues the object. If the tracking partition's record reaches disk and the participant's doesn't, the participant gets its batch rows back after the power loss, while `FileCleaner` on the tracking partition deletes the object. Fetches for those offsets fail with `KeyNotFoundException`. No clock skew is needed.
- A lost `sequences-fenced` tail at a participant lowers its floor again. A late part for the fenced sequence then commits into a key that the tracking partition already queued for deletion.
- A lost `sequences-leased` tail at a tracking partition leaves a broker using a range that the partition doesn't know. The participants commit their parts, the tracking partition rejects its own, and nothing ever tracks the object.
- A lost file-committed tail reuses offsets across every diskless partition mapped to that coordinator partition, 1/N of the cluster. Brokers that stayed up still cache the old coordinates for those offsets, so they serve different records for the same offset until the cache entries expire. A lost diskless-log-initialized tail leaves a switched partition uninitialized until a manual `repairDisklessLog`.

With `flush.messages=1`, the append linger in `CoordinatorRuntime` batches reservations into one append, so `UnifiedLog.append` runs one fsync per linger batch on the leader and one per fetched batch on each follower. The leader's fsync matches a Postgres commit's WAL flush on the primary. The follower's fsync is a second sequential flush on every commit, which Postgres matches only with a synchronous standby. The design counts it as the price of Kafka parity, not as unnecessary blocking. Its latency is a benchmark question. The KRaft metadata log fsyncs every batch for the same reason.

## Coordinator partitions

- A diskless user partition maps to a slot, and the slot maps to a coordinator partition. The slot is `Utils.toPositive(Utils.murmur2(logKey)) % S`, where `S` is 4096, a protocol constant. The coordinator partition is `slot % N`, where `N` is the partition count of `__diskless_metadata`.
- The log key names the log a state change belongs to. The log key of a Kafka partition is the 16 bytes of the topic ID, most significant bits first, followed by the partition index as 4 big-endian bytes. That is the only encoding this design defines.
- `diskless.metadata.topic.num.partitions` accepts only a power of two from 1 to `S`. With such an `N`, the two steps give the same result as `Utils.toPositive(Utils.murmur2(logKey)) % N`, so the slot layer adds no runtime cost and no stored state.
- The slot layer exists so that `N` can grow later without remapping every user partition. When `N` doubles, coordinator partition `p` keeps half of its slots, and the new partition `p + N` takes the other half. [Future work](#future-work) describes what that growth still needs.
- The log key includes the partition index, so the partitions of one topic spread across coordinator partitions, and a busy topic doesn't load a single leader.
- Every broker reads `N` from the metadata image, as `BrokerMetadataPublisher` passes the `__consumer_offsets` partition count from the metadata cache to the group coordinator. The config sets `N` only when the controller creates the topic. A call made before the topic exists gets a retriable error.
- `N` never changes after creation. A different `N` would map user partitions to other coordinator partitions, and moving a user partition between coordinators is out of scope. The controller rejects `CreatePartitions` for `__diskless_metadata`, with the other guards in [Topic protection](#topic-protection).
- A broker's tracking partition is `Utils.toPositive(Utils.murmur2(brokerId)) % N`. [Tracking partition](#tracking-partition) describes its role.
- The mapping functions, the log key encoding, and `S` are part of the protocol. Every broker version must compute the same coordinator partition for a user partition and the same tracking partition for a broker, so the design fixes them and a test pins them with fixed expected values.
- Each leader recomputes the mapping for every user partition in a request it receives. If a user partition doesn't map to that coordinator partition, the leader rejects the request with `INVALID_REQUEST` and counts the mismatch. A broker that maps differently would otherwise give a user partition a second writer.
- Each coordinator partition is an independent coordinator, with its own log, its own SQLite database on each replica, its own snapshots, its own overlay, and its own leader. Kafka's normal leader placement spreads the leaders across brokers.
- With `N=1`, every user partition and every broker maps to partition 0, and the design reduces to a single coordinator.

## Topic protection

Deleting `__diskless_metadata` loses the batch index for every diskless partition, every lease, and every deletion queue, and no rollback to Postgres exists. Kafka's internal-topic status guards only produce and `DeleteRecords`, and produce has a bypass. The topic therefore joins `Topic.INTERNAL_TOPICS`, and explicit controller and broker guards cover every request and config that `Topic.isInternal` doesn't guard. No guard has a bypass. The guards key on the name `__diskless_metadata` only, so the three upstream internal topics keep upstream behavior.

Joining `Topic.INTERNAL_TOPICS` gives the topic the produce and `DeleteRecords` rejections in `ReplicaManager`, the `isInternal` flag in metadata responses, exclusion from `Admin.listTopics()` and client-side regex subscriptions by default, no tiered storage, and system-topic status in `ReplicationControlManager.isSystemTopic`, which keeps it classic.

| Request | Where the check lives | Error |
| --- | --- | --- |
| `DeleteTopics` | `ReplicationControlManager.deleteTopic`, after `ControllerApis` resolves names and IDs and checks authorization | `INVALID_TOPIC_EXCEPTION` |
| Produce | `ReplicaManager.appendToLocalLog`, next to the internal-topic check: reject unless `origin` is `AppendOrigin.COORDINATOR` | `INVALID_TOPIC_EXCEPTION` |
| `CreateTopics` from a client, including broker auto-create | `ControllerApis`, in the same check as `__cluster_metadata` | `INVALID_REQUEST` |
| `CreatePartitions` | `ReplicationControlManager.createPartitions`, before the count checks | `INVALID_PARTITIONS` |
| `ElectLeaders` with type `UNCLEAN` | `ReplicationControlManager.electLeader`, next to the pending-switch check | `INVALID_REQUEST` |
| `DeleteRecords` from a client | Inherited from `Topic.isInternal` in `ReplicaManager.deleteRecordsOnLocalLog` | `INVALID_TOPIC_EXCEPTION` |
| `AlterPartitionReassignments` that changes the replica count | `rfChangeAllowedForTopic` in `ReplicationControlManager.alterPartitionReassignments` is false for the topic | `INVALID_REPLICATION_FACTOR` |

- `DeleteTopics` departs from upstream, which lets a principal with `DELETE` remove `__consumer_offsets`. The check fails only this topic, so other topics in the same request proceed. Authorization runs first, so an unauthorized caller learns nothing new.
- The produce check closes the `__admin_client` bypass in `KafkaApis.handleProduceRequest`, which any client can use. A foreign record would apply on every replica and take an offset the leader's overlay doesn't expect. `WriteTxnMarkers` also appends with `COORDINATOR` origin. It isn't covered, because it needs `CLUSTER_ACTION`, which is broker-level trust.
- The `CreateTopics` check stops a client or auto-create from creating the topic first with broker defaults and the wrong partition count. The controller's own creation goes to `ReplicationControlManager` through the `QuorumController`, not through `ControllerApis`, so the check doesn't block it.
- The admin `UNCLEAN` election path never reads `unclean.leader.election.enable`, so the fixed config alone doesn't close it. `PREFERRED` elections and reassignments that keep the replica count stay unchanged: both keep acknowledged records.

`ConfigurationControlManager.validateAlterConfig` checks the topic's config after the alter, next to the `diskless.enable` system-topic check. Reading the result covers `SET`, `DELETE`, `APPEND`, `SUBTRACT`, and the implicit deletes of legacy `AlterConfigs`. Unlike the `diskless.enable` check, it also runs for a newly created resource, so the controller's own create passes the same rules. A violation gets `INVALID_CONFIG`.

| Config | Rule | Why |
| --- | --- | --- |
| `flush.messages` | Must be `1` | A lost unflushed tail deletes objects that live batches reference, as [Flush policy](#flush-policy) shows. Deleting the override would fall back to the broker default of `Long.MAX_VALUE`. |
| `unclean.leader.election.enable` | Must be `false` | An unclean leader truncates acknowledged records. |
| `retention.ms` | Must be `-1` | Segment deletion would move the log start past the snapshots. |
| `retention.bytes` | Must be `-1` | Same as `retention.ms`. |
| `cleanup.policy` | Must be `delete` | Compaction removes records from a log that replicas replay in full. |
| `min.insync.replicas` | Fixed at its creation value | A lower value acknowledges records on fewer replicas, and a change would fire the topic-level `ClearElrRecord`. |

Setting a key to its required value passes. Other topic configs stay alterable. Because the values are topic-level overrides, broker and cluster default changes don't reach the topic.

The guards take two incident workarounds away from operators: lowering `min.insync.replicas`, and an unclean election. With two of three replicas down, a coordinator partition stops until one returns. It stays offline for good only once every ELR member and the last known leader have lost their storage, which [Accepted risks](#accepted-risks) covers as the loss of every replica.

## Transport

- Each broker holds a `DisklessMetadataControlPlane` delegate. For each call, the delegate maps every user partition in the request to its coordinator partition, splits the request by coordinator partition, and sends the parts in parallel. A part runs locally when the broker leads that coordinator partition. Otherwise the broker sends a private RPC to that partition's leader, which it finds from the metadata cache.
- The delegate merges the results of the parts back into the order of the original request, as each `ControlPlane` method requires.
- A broker that receives a request for a coordinator partition it doesn't lead returns `NOT_COORDINATOR`.
- The RPCs carry the arguments of the `ControlPlane` methods that have remote callers, plus the requests that [Object tracking and deletion](#object-tracking-and-deletion) adds. The table in [Private RPCs](#private-rpcs) lists them.
- The Produce response can't carry the offsets that a commit assigns, so the commit needs its own response. That is why the RPCs exist.

## Log records

The leader appends one outcome record per committed call, or per part of a call that spans coordinator partitions. Every replica applies records to SQLite in log order, and only up to the high watermark. Each record carries the state change and the assigned values, so every replica reaches the same state without recomputing anything.

The record types follow the KIP's record set, and the implementation may merge or split them:

| Record | Written by | Content |
| --- | --- | --- |
| Sequences leased | Tracking partition, on a lease request | Broker ID, broker epoch, the range `[a, b]`, and the uploader's object key prefix |
| File committed | Commit part, one per participant and one at the tracking partition | Object key, format, size, uploader broker ID, broker epoch, sequence, tracking partition, participant set, this partition's committed batches with their assigned offsets and producer fields, and the outcome of each other batch in the part |
| Sequences fenced | Participant, on a fence | Broker ID, broker epoch, and the range `[a, b]` that raises the fenced-through mark |
| Participant state recorded | Tracking partition, on a release report or a fence answer | Broker ID, broker epoch, a sequence range, the answering participant, and each sequence's state: referenced or released |
| Release accepted | Participant, after the tracking partition accepts its release report | Object key |
| Lease ended | Tracking partition, when a broker's idle timer fires | Broker ID, broker epoch |
| Broker epoch retired | Any leader, when its metadata image shows a newer epoch or no registration | Broker ID, and the newer epoch or an unregistered flag |
| Files deleted | `FileCleaner` on the tracking partition's leader | The keys storage confirmed |
| Barrier | Any leader, for `findBatches` above the high watermark | Nothing |
| Partition created | create-partitions | Topic ID, name, partition index |
| Topic deleted | delete-topics | Topic ID |
| Records deleted | `deleteRecords` | Topic partition, new log start offset |
| Diskless log initialized | `initDisklessLog` | Topic partition, log start, diskless start, producer states |
| Diskless log repaired | `repairDisklessLog` | Topic partition, diskless start |
| Retention applied | `enforceRetention` | Topic partition, batches deleted, new log start |
| Log pruned | `pruneDisklessLogs` | Topic partition, new diskless log start |
| Cross-tier start advanced | `advanceCrossTierLogStartOffset` | Topic partition, remote log start offset |
| Deleted log purged | topic purge | Topic partition, optional boundary offset, whether the partition is now empty |

Version 1's intent-registered, owner-accepted, file-fenced, owner-entry-dropped, file-delete-marked, file-deleted, and keys-expired records leave the design.

The records use the message-JSON conventions that Kafka uses for its other records, so their schemas can be versioned. They carry no tagged fields. Every schema change is a new version or a new type, gated by the coordinator feature's level, as [Record compatibility](#record-compatibility) describes.

Every record names the log it changes with a `Log` struct that holds `TopicId` and `PartitionIndex`, not with a bare topic-partition field. The struct is the record form of the log key. A second log key encoding would be a new record version.

Every change to a diskless partition's coordinator state is a record, including its creation and deletion. KRaft decides that a topic exists. The coordinator record fixes the position of the coordinator's own state change among the commits. Apply reads nothing outside its own coordinator partition's log and the replica's own cluster ID, and it never reads a clock. A tracking partition's view of other participants comes from records in its own log, and every time-based decision is a record the leader appends, such as lease ended. The same log therefore produces the same state on every replica, and a snapshot's tag fully describes its content. If a replica instead removed a topic's rows when its own `MetadataImage` dropped the topic, each replica would remove them at a different log position.

### Record compatibility

A replica never skips a record it can't read. Every coordinator record is a delta that no later record overwrites, so a skipped files-deleted or retention-applied record would leave rows that the other replicas dropped, a permanent and silent divergence. Kafka's `CoordinatorLoaderImpl` skips an unknown record type, which is safe there only because group and offset records are keyed snapshots of state.

A replica that meets an unknown record type, an unknown value version, or an unrecognized tagged field, at load or during live apply, stops applying that coordinator partition at that offset:

- Its SQLite stays at the last applied offset.
- It stops serving reads and commits for the partition, and a leader resigns.
- It keeps fetching bytes, because replication doesn't parse records, so it stays in the ISR.
- `ApplyStoppedPartitionCount` and an error log report it. The log names the partition, offset, record type, version, and tag.

Leader epochs don't address this case. Below the high watermark, every batch was written by the leader of its epoch. The unreadable record is valid, and the reader's binary is older than the writer's.

The coordinator feature's level gates writes, as [Rollout](#rollout) describes. The leader reads the finalized level from its `MetadataImage` when it builds each record, and writes the highest version the level allows. A leader whose image lags writes an older version for a while, which every replica reads. With the write gate and the controller's downgrade refusal, a replica stops applying only through a bug or a manual binary downgrade.

Generated readers collect an unknown tag into `unknownTaggedFields()` and read the rest, so a tag that an old reader skipped would change apply silently. Apply checks the `unknownTaggedFields()` of each record and each nested struct against a registry of recognized extension tags. The registry is empty upstream, so any tag stops the replica. Inkless registers its extension tags at 100 and up, encoded by hand as `RawTaggedField` values, following the convention in [CLASSIC_TO_DISKLESS_SWITCH.md](../../docs/inkless/CLASSIC_TO_DISKLESS_SWITCH.md). The schema JSON stays identical to upstream, so syncs don't conflict. A vanilla build can't read an Inkless coordinator log that carries extension tags. It stops instead of dropping the extension's state.

## Speculative state

The leader keeps an in-memory overlay of records it has appended and not yet applied. The overlay runs from the offset after the applied offset through the last written offset. Followers keep no overlay.

- An overlay entry holds the object key, the uploader's broker ID, broker epoch, and sequence, the assigned offsets, the outcome of each batch that didn't commit, the producer epoch and sequence numbers, and a pending lease or fence when the entry is one.
- The leader checks and reserves on one thread. A commit reads SQLite plus every earlier overlay entry, so two commits can't receive the same offsets, and a broker's newest pending entry decides its acceptance rules.
- The leader collects reservations for one append linger interval, then appends them as one log batch. One replication round trip covers the batch.
- When the high watermark passes an entry, the leader applies it to SQLite, removes it from the overlay, and replies.
- If an append fails, the leader resigns, or the log truncates an entry, the leader removes that entry and every entry after it. It fails their parked replies with a retriable error. A later entry took its offsets and sequence numbers from an earlier entry, so the leader keeps no suffix past a broken entry.
- The overlay has a cap. Past that cap, the leader rejects a new commit with a retriable error.
- A new leader starts with an empty overlay. The overlay fills from the first commit after the high-watermark gate opens.

## Write path

The leader runs this loop on `CoordinatorRuntime`, the runtime the group coordinator and the share coordinator already use. The runtime supplies the single writer thread, the append linger, the revert to the last committed snapshot after a failed append, the parked replies that complete when the high watermark moves, the `CoordinatorTimer` for the idle lease timers, and the `NOT_COORDINATOR` failure on resignation.

`CoordinatorRuntime` already runs one shard per partition of a coordinator topic, as the group coordinator does for `__consumer_offsets`. Each coordinator partition is one shard, loaded and unloaded with its leadership.

The runtime writes through a `PartitionWriter` that Inkless owns, not through the upstream `CoordinatorPartitionWriter`. The upstream writer appends with `acks=1`, and the group and share coordinators keep using it unchanged. With `acks=1`, a leader under its minimum ISR would still write the record to its log and park the reply until the operation times out. The Inkless writer appends with `acks=-1`, so the append fails at once, the runtime reverts the batch, and the caller gets a retriable error it can act on, such as renewing its lease at another partition.

The runtime's write timeout equals the commit deadline, so a commit part never waits on the server longer than the broker waits for it. The barrier in [Reads](#reads) keeps this write timeout and takes its own 500 ms deadline from a timer, so upstream `CoordinatorRuntime` doesn't change.

The shard stores the overlay in timeline structures backed by a `SnapshotRegistry`. Committed state stays in SQLite. Two pieces stay outside the runtime:

- A custom `CoordinatorLoader` replays from the applied offset stored in SQLite. The stock loader starts at the log start and would replay records SQLite already holds.
- A follower applier writes fetched records to SQLite up to the high watermark. The runtime runs on the leader only.

Rejected alternative: a write loop written for this coordinator. It would reimplement batching, revert, parked replies, timers, and resignation handling.

Rejected alternative: the full state in `CoordinatorRuntime` timeline maps. Every election would replay the whole log, and `findBatches` and `listOffsets` would scan indexes held on the heap.

## Commit and retries

Every upload carries a pair of the uploader's broker epoch and a sequence number, and the coordinator treats the uploading broker as an idempotent producer. A broker leases ranges of sequences from its tracking partition ahead of time, off the produce path, so a file needs no round trip before its upload. The pair names the object key, deduplicates retries, and orders a broker's commits at each coordinator partition. A higher pair makes every lower pair from that broker impossible to commit there.

### Sequence source

- At file close, `FileCommitter.commit()` gives each non-empty file the current broker epoch and the next sequence, under its existing lock. It takes the sequence from `ControlPlane.sequenceSource()`, which returns a `SequenceSource` with one call: `OptionalLong next(long brokerEpoch)`. An empty result fails the file at once with `KAFKA_STORAGE_ERROR`, a retriable error, and the file isn't uploaded.
- `FileCommitter` gets the broker epoch from a supplier wired from `BrokerLifecycleManager`.
- "Higher" compares the pair (broker epoch, sequence), epoch first. A broker can get a new epoch without a restart: `BrokerRegistrationTracker` makes `BrokerLifecycleManager` register again when the metadata version moves to one that supports JBOD. The pair order covers that case and any other new registration.
- The sequence is an `int64` that starts at 0 in each broker epoch. The broker increments it with `Math.incrementExact`, and a leader rejects a negative sequence with `INVALID_REQUEST`. Overflow is unreachable in practice, and the code fails instead of wrapping.
- Gaps are allowed. A failed upload, a `ControlPlaneUnavailableException`, and an empty file consume no commit, and no partition requires `last + 1`.
- Nothing about the sequence is persisted. A restart registers a new, higher epoch, so the restarted broker's pairs are higher than every pair from before the restart.
- The default source is a counter that restarts at 0 on a new epoch and never returns empty. The Postgres and in-memory backends use it and behave as today.
- The coordinator backend's source holds the broker's leased ranges, hands sequences out in increasing order, renews as [Leases](#leases) describes, and returns empty when nothing is leased. After a stale-sequence reply it moves above the floor in the reply and renews. After a stale-epoch reply it drops its ranges until the broker has a new epoch.

The sequence must be ordered, so it can't be a UUID. Order lets each partition keep one floor per broker in place of a set of keys, find missed sequences, and decide both without a clock.

### Object keys

- The object name is `<clusterId>.<brokerId>.<brokerEpoch>.<sequence>`, in decimal with no padding, under the existing `inkless.object.key.prefix` and masked-prefix handling. `ObjectKey.Path` rejects a name with `/`, so the name stays one segment.
- The cluster ID keeps two clusters that share a bucket and prefix from overwriting or deleting each other's keys. KRaft never reuses a broker epoch within a cluster. The cluster ID is URL-safe base64, which never contains `.`.
- `FileCommitter` builds the key when it assigns the pair at file close and passes it to `FileUploadJob`, which no longer calls `Uuid.randomUuid()`. Every backend uses the deterministic key. Postgres stores keys as opaque strings, so existing UUID keys still read and delete.
- `FileCommitter` and the backend build names with one shared function. The backend builds names and never parses them, so it doesn't depend on the key format or on the prefix handling in `ObjectKeyCreator`.
- Upload retries are harmless. `FileUploadJob` reuses one key for every attempt, and every backend overwrites unconditionally: S3 `PutObject`, GCS `create` and `createFrom` with no generation precondition, Azure `getBlobOutputStream` with no request conditions, and the in-memory map.
- Sequential names at the target commit rate across many broker IDs stay below the GCS hotspot threshold of about 1,000 writes per second.

### Leases

- The broker asks its tracking partition `t` for a range `[a, b]` at its current epoch. It picks `a` above every range it requested in that epoch at any partition, including requests that got no reply, so ranges at several partitions never overlap.
- The tracking partition rejects a range that is longer than `K`, or whose `a` is at or below its own highest range end for that epoch, with `INVALID_REQUEST`. A request for a range it already holds is a retry: it appends nothing and confirms the range. Otherwise it appends a sequences-leased record and replies once the record applies, so a confirmed lease survives a failover.
- `K` is the protocol constant 1024, not a config. At the default `produce.commit.interval.ms` of 250 ms, a range lasts about 4 minutes, and about 85 seconds when the produce buffer fills at about 12 files per second. A lease end costs up to `K` deletes, mostly of missing keys, and deletes are free on S3, GCS, and Azure. The bound also limits the leftovers that a broker bug can cause.
- The lease request and the sequences-leased record carry the uploader's `inkless.object.key.prefix`, and the tracking partition's lease table stores it per broker epoch. The prefix is static, so it changes only with a restart, which is a new epoch.
- The broker asks for its first lease when `FileCommitter` starts and when the broker epoch changes. It renews in the background once fewer than `K/2` leased sequences remain.
- If `t` doesn't answer, the broker asks `(t + 1) % N`, and onward. During a fallback, the sequence source holds a short list of ranges and hands them out in increasing order. When its tracking partition is unavailable, only the user partitions that map to that partition fail. Without the fallback, an unavailable partition would stop all diskless produce on about one in `N` brokers once their leases ran out.
- A range stored at a partition whose reply was lost is never used, and that partition treats it as leftover when the lease ends.
- Every commit part names the tracking partition that issued its sequence's lease, so one broker epoch can hold leases at several tracking partitions.

Rejected alternatives:

- A configurable or adaptive `K`. Nobody would tune the config, and adapting only saves free deletes.
- Making a file wait for a renewal when nothing is leased. It needs a queue of closed files without a sequence, and it only helps the first lease at startup.
- Deleting leftovers by listing a retired epoch's key prefix in storage. It brings back a storage scan, and it can't end an idle lease within a live epoch.

### Commit path

`FileCommitter` commits on a single thread today, which keeps the files of a broker in commit order. Only the order within a user partition matters, and on this backend that means the order within a coordinator partition. With one thread, a part that waits on an unavailable or slow coordinator partition, or on a new leader that rebuilds its database, holds back every later file of the broker. Most files hold batches for most coordinator partitions, so one coordinator partition would stall all diskless produce on every broker.

This design changes the caller as well as adding a backend. `FileCommitter` and `ControlPlane` change as follows:

- `ControlPlane` maps each user partition to a commit queue. The new backend maps it to its coordinator partition. The Postgres and in-memory backends map every user partition to one queue, which keeps today's order.
- `ControlPlane` gets a per-part commit call that commits the batches of one file that map to one queue, with the broker epoch, the sequence, and the object key. The Postgres and in-memory backends call `commitFile` for it, because their one queue contains the whole file.
- `ControlPlane` gets `sequenceSource()`, as [Sequence source](#sequence-source) describes, and a capability that turns off the broker's direct delete, as [Deletes by the broker](#deletes-by-the-broker) describes.
- `FileCommitter` keeps one ordered queue per commit queue, in file rotation order. The part of file `F2` on a queue runs once `F2` is uploaded and the part of the previous file on the same queue has finished: committed, failed, or past its deadline. It doesn't wait for parts on other queues. A broker's queue for a partition therefore delivers that broker's sequences in order, with gaps for files that hold no batch for the partition.
- Every file also sends a part to its tracking partition. That part carries the participant set, and it's empty when the tracking partition holds none of the file's batches.
- Every part of a file shares the file's commit deadline, measured from the moment the file closes. The deadline doesn't exceed the produce request timeout. Parts queued behind a stuck part each expire at their own file's deadline, so a stuck coordinator partition delays each queued part by at most one deadline, not one deadline per earlier file.
- The backend retries each part inside its per-part call until the deadline, so the queue starts the next part only after the earlier one has finished. `FileCommitJob` waits for the upload before it commits, so no lower sequence is still uploading when a higher one commits.
- A file completes once every part has an answer. `AppendCompleter` merges the per-batch results in request order, as it does today.

Rejected alternative: committing at the tracking partition first and sending the other parts only after it confirms. It adds a second serial round trip to every commit, and it fails every file under a lease as a whole while its tracking partition is unavailable.

### Broker rows

Every coordinator partition keeps one row per uploading broker, keyed by broker ID:

- The broker epoch `e_r` and the last sequence `s_r` that committed at this partition. The last sequence is empty after a newer epoch moves the row.
- The complete per-batch result of that commit, in request order: each committed batch's assigned offset and timestamp, each failed batch's error code, and each duplicate's original offset and timestamp.
- The fenced-through pair `(e_f, s_f)`, kept apart from the last-commit fields. A fence that overwrote the last sequence would make a retry of `s_r` stale while the broker still waits for it, and the broker would report committed batches as failed.
- A retired marker, set only when the broker is unregistered.

The floor is the higher of `(e_r, s_r)` and `(e_f, s_f)`. The stored result doesn't depend on the batch rows, so a retry that arrives after retention, `DeleteRecords`, topic purge, or consolidation pruning removed the batches still gets the original answer. A higher sequence replaces the result. Rows are never deleted. They're bounded by the number of broker IDs the cluster has ever used, and a deleted row would let a late commit look like a new broker.

Deduplication uses the pair in this row, not the object key. The `files` table keeps the object key for reads and deletion, and an index on uploader broker, epoch, and sequence so that a fence can find what a partition holds.

### Commit on the leader

The delegate sends each part of a file to the leader of its coordinator partition. Every part carries the object key, the uploader's broker ID, broker epoch, and sequence, the tracking partition, and the participant set. Each leader runs its part in this order:

1. Check the mapping of every user partition in the part, and reject a mismatch with `INVALID_REQUEST`.
2. Reject the part with a non-retriable stale-epoch error when the leader's metadata image holds a newer epoch for the uploader, and append a broker-epoch-retired record if none is appended yet. A part at the image's epoch, at a newer one, or for a broker with no registration in the image goes on to the next step. The image can lag behind the controller, and the retired marker refuses a removed broker durably. A fenced uploader isn't rejected, so a short heartbeat gap doesn't fail commits in flight.
3. Compare the part's pair `(e, s)` with the broker's latest state: its newest pending overlay entry if one exists, and its SQLite row otherwise. [Acceptance rules](#acceptance-rules) lists the cases.
4. For a new commit, check the producer epoch and sequence rules against SQLite plus the overlay. The Postgres `commit_file` functions enforce the same rules against committed state.
5. Assign the offsets in the overlay, then add one file-committed record to the current batch. The record stores the committed batches with their offsets, and the outcome of each other batch in the part. Its apply moves the row to `(e, s)` with that result.
6. When the high watermark passes the record, apply it to SQLite in the same transaction as the applied offset, remove the overlay entry, and reply with the per-batch results.

At the tracking partition, every part and lease request that names the partition reschedules the broker's idle timer, including one that the leader rejects or that a failed append reverts. [Ending a lease](#ending-a-lease) describes the timer.

Every reply takes `logStartOffset` from current state, whether it answers the first attempt or a retry. The stored result contains only what the leader decided.

A commit whose batches all fail their checks is an ordinary new commit with zero committed batches. It appends a file-committed record with every batch's outcome and advances the row. A retry gets exactly the first answer, and the partition learns in that apply that it holds no batch of the object, so the object can be released at once. Recording nothing was rejected: it would make the all-fail commit the only commit that doesn't advance the row, and its object would wait for a later sequence or the lease end.

### Acceptance rules

The row holds `(e_r, s_r)`, the fenced-through pair, and the retired marker. A commit's pair is `(e, s)`:

| Case | What the leader does |
| --- | --- |
| The row is retired, or `e < e_r` | Reject as a stale epoch |
| `(e, s)` equals `(e_r, s_r)`, still in the overlay | Retry: append nothing, wait for the pending entry to apply, and reply with its result |
| `(e, s)` equals `(e_r, s_r)`, applied | Retry: reply at once with the stored result, with `logStartOffset` from current state, also after the batches were released |
| `(e, s)` above the floor, or no row | New commit, after the lease check at the tracking partition |
| Anything else | Reject as a stale sequence, with a non-retriable error and the floor in the reply |

- Only the tracking partition checks the lease. The part sent to it must carry a sequence inside one of its ranges for that epoch, or it gets `INVALID_REQUEST`, because only a broker bug uses an unleased sequence. Participants rely on their floor, which fences raise.
- A retry sends the same batches in the same order, so a batch's position in the part identifies it. If a retry's batch count differs from the stored result's, the leader answers `INVALID_REQUEST`, because only a broker bug produces it.
- A new commit at a newer epoch replaces the row, which makes every sequence of the old epoch impossible at that partition.
- Retry after a failover: the new leader serves only after applying everything up to its election-time log end, so an applied `s` is in the row. A truncated `s` isn't, and its retry runs as a new commit.
- A late `s` still pending when `s + 1` arrives: the leader queues `s + 1` behind `s`, and both commit in sequence order.
- A failed append under `s`: the overlay rule drops `s` and every later entry with retriable errors. A later arrival of `s` after `s + 1` committed is stale.
- A part that is rejected before the producer checks records nothing and leaves the row unchanged. This covers a stale epoch, a stale sequence, the image check, the mapping check, the lease check, a full overlay, and a non-leader.

### Ending a broker epoch

Each leader appends a broker-epoch-retired record when its metadata image shows a newer epoch for a broker than the broker's row or lease table holds, or no registration. It checks on each image change and once it starts serving after an election. A repeated record is a no-op.

- The record carries the broker ID and either the image's newer epoch `E` or an unregistered flag.
- With `E > e_r`, apply moves the row to `E` with no last sequence and no stored result, and creates the row if there's none. Every replica then rejects commits below `E`, whatever its image shows. If the broker already committed at `E` because the image lagged, the record is a no-op.
- With the unregistered flag, apply sets the retired marker. KRaft gives each new registration a higher epoch, so a broker that returns with the same ID commits as a new epoch.
- At a tracking partition, the same apply ends the old epoch's leases at that log position, as [Ending a lease](#ending-a-lease) describes. A lease request at a newer epoch does the same before it stores the new range.
- A fenced broker keeps its epoch, so fencing triggers nothing.

The record is needed because the image check alone isn't durable. After a failover, a new leader whose image lags could accept a late commit from an old epoch whose unused sequences were already deleted.

### Deadline and unknown outcomes

The delegate retries each part on its own against the current leader of that coordinator partition, after `NOT_COORDINATOR` or a retriable error, with the same pair and object key. If the first attempt is still pending on the leader, the retry waits for its result and appends no second record. The delegate stops at the file's commit deadline. When the deadline passes:

- Batches on parts that confirmed get their per-batch results.
- Batches on parts that didn't confirm get `KAFKA_STORAGE_ERROR`, as a whole failed commit does today. Their outcome is unknown: the part's last request can still reach the leader and commit later. That window closes once the broker's next commit to the same coordinator partition applies, a fence raises the floor there, the broker's epoch retires, or its lease ends.
- If at least one part committed, the file returns these per-batch results. The committed batches are already visible to consumers, so the delegate must not fail them. A producer retries only the failed partitions, as with any partial produce failure.
- If no part committed, the file fails as a whole, and `FileCommitJob` runs its existing cleanup path, which never deletes the object on this backend.

A late part from an unknown outcome affects producers as follows:

- Idempotent producers have no window. Producer sequence checks catch a late copy through any broker.
- A non-idempotent producer that retries through the same broker can see its data twice in the original order, as with an `acks=all` timeout on a classic topic. Its retry lands in a later file, whose commit at that coordinator partition makes the late part stale.
- A non-idempotent producer that retries through another broker can see the late copy after the retried one. The window closes when the first broker's next commit to that partition applies, its epoch retires, or its lease ends. On a busy broker that's about one commit round trip after the deadline. On an idle broker it stays open until one of those events, bounded in practice by how long a sent request can wait in the leader's request queue and for the high watermark. The Postgres backend has the same window, because a JDBC commit that hit `socketTimeout` can still commit on the server.

Rejected alternatives for the idle-broker window:

- A sequence bump at the deadline: an empty commit at `s + 1` when nothing else is queued. It costs a request per stuck part and can fail like the part it guards.
- A leader-side bound on request-queue time. It needs queue timing from request handling and doesn't cover a request that is already appended.

### Apply rules

The leader's checks make the cases in this list unreachable, so a replica that meets one treats it as a bug. It stops applying that coordinator partition and reports corruption, in the same state as an unreadable record:

- A file-committed record whose pair isn't above the row's floor.
- A sequences-leased record whose range overlaps a range already held for that epoch.

These cases apply as no-ops, and the applied offset still advances:

- A sequences-fenced record whose range end isn't above the floor.
- A broker-epoch-retired record at or below the row's epoch.
- A files-deleted record for a key that isn't in the deletion queue.

Rejected alternative: keeping the single commit thread. One stuck coordinator partition would then stall every broker's diskless produce, which defeats splitting the state across coordinator partitions.

Rejected alternative: answering a retry with a fixed error for the batches of a committed part that didn't commit. The error changes what the client does. `INVALID_PRODUCER_EPOCH` is fatal for a producer, while `KAFKA_STORAGE_ERROR` makes it retry, and a duplicate reported as an error is retried until it falls out of the producer's sequence window.

Rejected alternative: parsing the pair out of the object key. It ties the backend to the key format and to the prefix handling in `ObjectKeyCreator`.

## Object tracking and deletion

Every object belongs to one tracking partition, which records the object's participants and decides when it can be deleted. The participants report to it. This section is the only path that deletes an object whose commit may have reached a leader. [deletion-comparison.md](../../docs/inkless/proposals/v1/diskless-metadata-coordinator/deletion-comparison.md) compares log-driven deletion with a scanner. It was written for version 1's owners and maximum commit age, and its deciding argument holds more strongly for this design: the log deletes only keys that the coordinator's own records name, and no clock decides whether a delete is safe.

### Tracking partition

- Each broker's tracking partition holds its sequence leases, tracks which partitions reference each of its objects, and keeps the deletion queue for them. It's fixed before any upload and doesn't depend on which partitions an object's batches map to.
- The lease records every key the broker may upload, so no per-object record or round trip precedes an upload.
- A broker uses a sequence only after its lease is confirmed, so each key has exactly one tracking partition: the one that issued its range. After a lease fallback, that can be a partition other than `Utils.toPositive(Utils.murmur2(brokerId)) % N`.
- The participants of an object are the coordinator partitions that its batches map to. The tracking partition records the participant set from the part it receives.

### Releases

- A participant releases an object when it holds no batch of it anymore. [Emptiness check](#emptiness-check) decides that in the apply that removes the last batch.
- The participant leader reports each release to the tracking partition named in its part, on a private request. The broker's scheduler sends and retries the report with Kafka's `ExponentialBackoff` client defaults. The report is idempotent, so a repeat after a lost reply is a no-op at the tracking partition.
- The tracking partition appends a participant-state-recorded record for the report. When the tracking partition accepts it, the participant appends a release-accepted record and drops its pending release. A new participant leader resends only the releases without one.
- When the tracking partition is itself a participant, it records the release locally and sends nothing. An all-fail part, or an empty part at the tracking partition, is released in the apply that records it.
- Participants report only releases. The tracking partition learns everything else through fences.

### Fences

A fence asks a participant which sequences in a range it holds, and makes every other sequence in the range impossible there. A sequence fence is a range of one.

- A fence names the broker, the epoch, and a range `[a, b]`. If `(e, b)` is above the participant's floor, its leader appends a sequences-fenced record that raises the fenced-through pair to `(e, b)`. Otherwise it appends nothing, because a sequence below the floor is already impossible there.
- The fence is ordered in the overlay after every earlier entry. The leader replies once it applies, with the sequences in `[a, b]` whose batches this partition still holds. Every other sequence in the range is released: it never committed or its batches are gone, and it can't commit again. A repeated fence after a lost reply gets an answer from current state.
- A part pending before the fence commits first, and the fence answers referenced for it. A part that arrives after the fence gets stale-sequence, and its producer retries. That stays correct.
- The tracking partition records each answer in a participant-state-recorded record. A referenced sequence is marked referenced and waits for its release report. A released one counts as released.
- The fence needs no clock and no wait.

The tracking partition sends fences in three cases:

- Silent participants: a silent participant is one the tracking partition hasn't heard from. Once the tracking partition itself and every participant it has heard from are released, the tracking partition sends a sequence fence to each silent participant. Each participant gets at most one fence per object.
- Missed sequences: a missed sequence is a sequence in one of the tracking partition's own ranges that it never recorded while it recorded a later one. The tracking partition doesn't know its participant set, so it fences the missed range at all `N` partitions, one range fence each, and builds the set from the referenced answers. Deleting a missed sequence without the fences would lose data: its parts can have committed on other partitions while the part to the tracking partition failed. One partition's row can't tell a missed sequence from a file with no batch there, so participants never mark one.
- Lease ends: after a lease ends, the tracking partition sends one range fence to each partition for the lease's unused sequences. The broker uses sequences in increasing order, so an idle partition's ranges sit below the range in use, and the fence never cuts a live range.

For a missed or leftover range, keys that no partition holds go to the deletion queue, and the rest are tracked like any committed object until their participants release them. A new tracking leader resends every fence that has no recorded answer.

### Ending a lease

These events end a lease at a tracking partition: the broker-epoch-retired record, a lease request at a newer epoch, and the lease-ended record. Apply raises the tracking partition's own fenced-through pair to the highest range end, treats the unused part of its ranges as leftovers, and the leader then sends the range fences in [Fences](#fences). A renewal never lowers the floor, so a sequence whose leftover key was deleted can't be leased or committed again.

The leader appends a lease-ended record when a broker has sent no part and no lease request that names the partition for `diskless.metadata.lease.idle.timeout.ms`, 15 minutes by default and provisional:

- The timer is a `CoordinatorTimer` operation keyed per broker, `lease-idle-<brokerId>`. Every part and lease request that names the partition reschedules the key, including one that the leader rejects or that a failed append reverts. That only delays a lease end.
- Its `generateRecords` appends the lease-ended record with no further idleness check. `CoordinatorTimerImpl` already drops a timer that fired and was overridden before its write event ran, and it ignores `NOT_COORDINATOR`. The group coordinator uses the same pattern for member session expiry.
- The timer runs on the leader's monotonic clock and persists nothing. A new leader starts every timer at its election.
- After a lease fallback, the previous tracking partition's lease ends on its idle timer as expected.
- Controlled shutdown adds nothing. A restart registers a new epoch, and a broker that stays registered and silent is covered by the idle timer.

Correctness comes from the fences and the floor, not from the timer. The timer collects a silent broker's leftover keys and closes the duplicate window for an idle broker. [Time bounds](#time-bounds) lists what skew, pauses, and clock jumps do to it.

Rejected alternative: idle heartbeats. They would keep a live idle broker's lease, its leftover keys, and its duplicate window open until its next commit.

### Emptiness check

- The schema has an index on `batches(file_id)`. When an apply removes batches, it runs `NOT EXISTS` over that index for every file it touched, and releases the empty ones in the same SQLite transaction. Postgres makes the same check in `delete_batch_v1`, `delete_records_v1`, `prune_batches_below_highest_tiered_offset_v1`, and `purge_deleted_logs_v1`.
- Five records release objects, each in its own apply: records deleted, retention applied, log pruned, deleted log purged, and a file-committed record whose every batch failed.
- Partition created, topic deleted, diskless log initialized, diskless log repaired, and cross-tier start advanced never remove a batch, so they never release anything. Topic deleted leaves the batches to topic purge, as `delete_topic` does in Postgres.
- A later record type that removes batches, such as the extent swap in [Future work](#future-work), runs the same check.

Rejected alternatives:

- A batch count per file. It fails toward data loss: a path that adds a batch without raising the count reaches 0 while batches remain. The query reads the batch rows themselves, so it can't drift.
- A list of emptied objects in the record. Replicas would trust the leader's computation instead of their own state.

### Deletion queue

- Only tracking partitions keep a deletion queue: a SQLite table of keys waiting for deletion, each with its full key path and its mark time. Participants never queue a key.
- A committed object enters the queue in the apply that completes its set of released participants, the tracking partition included. Its tracking entry goes in the same apply.
- Missed and leftover keys enter the queue from the fence answers that no partition holds, one row per sequence. A lease end adds up to about `K` rows. A missed range has no fixed bound, but it never holds more keys than the files the broker closed in that time.
- Apply builds the key path of a sequence that never committed as `<prefix>/<clusterId>.<brokerId>.<brokerEpoch>.<sequence>`. The prefix is the uploader's, from the lease table for that epoch, not the leader's. A cluster whose brokers differ in prefix would otherwise delete keys that were never written and leak the real ones. Apply takes the cluster ID from the replica itself, which is fixed at format time and the same on every replica.
- A queue row's mark time is the timestamp the leader wrote on the marking record, so every replica stores the same time.

Rejected alternative: storing a range as one queue row and expanding it in `getFilesToDelete`. Storage can confirm part of a set under throttling, so the backend would have to map each key back to its range.

### `FileCleaner`

- `ReplicaManager` keeps scheduling `FileCleaner` on every broker, and neither changes. `getFilesToDelete` returns the union of the queues of the tracking partitions this broker leads, from SQLite, and an empty list on every other broker, without a request.
- The leader compares each row's mark time with its own clock through `markedBefore`. Every row waits `file.cleaner.retention.period.ms`, 1 minute by default, as today. For a committed object, the grace period protects readers that still hold its coordinates, because `consume.batch.coordinate.cache.ttl.ms` is at most half the grace period. For a key that never committed, it delays the delete past most uploads still in flight, which shrinks the leak in [Remaining leak](#remaining-leak).
- `deleteFiles` splits the confirmed keys by the partition whose queue holds each one, a local lookup, and appends one files-deleted record per partition. Its apply removes their queue rows. A partition that moved away answers `NOT_COORDINATOR` for its share, its rows stay, and the new leader deletes them again. Deleting a missing key counts as success: S3 answers a missing key with success, and the GCS and Azure backends map their not-found answer to success.
- Object storage needs no transactions and no atomic writes for this rule. It needs a delete by key that removes whatever is stored there. Readers reach an object only through committed batches, so a partial object is never read. S3 writes each WAL object with one `PutObject`. GCS uses a single-request upload or a resumable session. Azure commits a block blob's blocks at once and drops uncommitted blocks on its own.

### Deletes by the broker

- The backend declares a capability that turns off the broker's direct delete. `FileCommitJob.tryDeleteFile` checks it before its `ControlPlaneUnavailableException` branch, because that branch deletes without calling `isSafeToDeleteFile`. The backend's `isSafeToDeleteFile` also returns false without a request.
- The capability closes a partial send: part 1 of a file is sent with an unknown outcome, part 2 then hits `AvailabilityGatedControlPlane`, no part has confirmed, and the file fails as a whole. A direct delete would remove an object that part 1 can still commit into. A delegate that the reconciler replaces between two parts of one file opens the same path.
- Every upload already has a leased sequence that the tracking partition resolves, so this backend never needs the direct delete. The cost is storage held longer for a key whose sequence never commits: the broker's next commit or the lease end queues it, then the grace period runs. The produce path doesn't change.
- The Postgres and in-memory backends don't declare the capability, so they keep today's behavior.

### Orphan cases

The model covers every orphan case without a new mechanism. The only leak is the accepted late upload, and no case deletes a referenced object:

- A broker stops before the upload: its restart's lease request at a newer epoch, or the broker-epoch-retired record, ends its lease, and the range fences queue the missing key. A file that closed without a leased sequence was never uploaded.
- A broker stops after the upload and before any part commits: the same range holds the key, and this time the key exists. If the broker stays alive, its next commit at the tracking partition fences the key as missed. If it then goes idle, the lease end fences it.
- A broker stops after some parts commit: each committed participant reports its release later. Once the tracking partition and every participant it has heard from are released, it fences the silent ones, and the ones that never committed count as released.
- A part at `s` has an unknown outcome, then `s + 1` commits at that partition: if `s` committed, it's referenced and its batches release it later. Otherwise it's below the floor and can't commit again.
- Leadership moves between the mark and the storage delete, or between the storage delete and the files-deleted record: the queue is in SQLite on every replica, so the new leader deletes the key, at most twice.

### Remaining leak

An object leaks when storage completes a write after the tracking partition deleted its key. No delete ever reaches it again: retention, `DeleteRecords`, topic purge, and consolidation pruning work from batch rows, and a leaked object has none. A bucket lifecycle rule can't help, because topics can have unlimited retention.

- An attempt that storage received before the broker gave up and completed after the delete leaks one object. The grace period covers attempts that complete within a minute of the mark.
- A zombie broker that reaches a leader gets a stale-epoch reply and drops its lease, so only files already in flight leak. A zombie that is cut off can upload at most the rest of its lease, `K` files at most.
- Today's Postgres path leaks more. `FileCommitJob.tryDeleteFile` deletes a failed commit's object only when `isSafeToDeleteFile` allows it, and nothing collects the rest.

Rejected alternatives for closing the leak: a broker-side lease expiry on the broker's clock, a second delete of every never-committed key after the expiry plus an upload time limit, conditional creates, and placeholder objects. The first two add a clock and scheduling across leader changes. A deleted key accepts a new write, so conditional creates don't help, and a placeholder moves the race to its own delete. The report-only scanner stays future work, and it would make the leak visible.

### Rejected alternatives

Rejected alternative: a per-object intent at an owner, as in version 1. The intent is a serial, replicated, fsynced append before every upload, and the Postgres backend has no such step. Its safety also depended on broker clocks through the maximum commit age.

Rejected alternative: a two-phase commit across participants. A non-transactional produce has no cross-partition atomicity to preserve, and the extra round trip would block every commit.

Rejected alternative: one upload per coordinator partition per commit interval. It removes every cross-partition reference, but it multiplies object-storage writes by up to `N` and shrinks each object.

Rejected alternative: routing produce so each broker writes for one coordinator partition. Clients could no longer produce to any broker, and every availability zone would need a broker for each coordinator partition.

Rejected alternative: a scanner as the deletion path. It deletes objects that no coordinator partition knows, so a stale snapshot, a restore, or a shared bucket prefix deletes live data. Its safety also depends on storage timestamps that agree with broker clocks.

## Time bounds

No clock decides whether deleting a key is safe. A key enters a deletion queue only at a log position where a record makes it impossible to reference from then on. Clocks decide only when a queued key is deleted.

| Decision | Whose clock | Role |
| --- | --- | --- |
| Idle lease timeout, `diskless.metadata.lease.idle.timeout.ms` | The tracking partition leader's monotonic clock. It persists nothing and restarts at each election. | Ends a silent broker's lease, which collects its leftover keys and closes the duplicate window for an idle broker. The fenced-through mark and the range fence keep data safe, not the timer. |
| `FileCleaner` grace period, `file.cleaner.retention.period.ms` | The marking leader's wall clock, written on the record, against the deleting leader's wall clock | Protects readers that hold cached coordinates, and shrinks the late-upload leak. |
| Commit deadline | The uploading broker's clock | Decides when the broker gives up. The sequence rules resolve the unknown outcome, and the broker never deletes the object itself. |
| Retention (`retention.ms`) and `LogAppendTime` | The coordinator leader's wall clock against batch timestamps | A leader whose clock runs far ahead deletes early. A classic topic has the same exposure, so it's an accepted risk. |

The snapshot interval, the snapshot transfer idle timeout, release-report backoff, the delegate's retry backoff, the barrier deadline, and the reconciler affect liveness only. Fences, floors, epoch retirement, and membership in the deletion queue use no clock.

Under skew, a pause, or a clock jump, the worst outcome for the idle timer is a lease that ends without cause or ends late:

- A part reaches the shard at the moment the timer fires. If the part runs first, it overrides the timer and the lease stays. If the timer runs first, the part to the tracking partition gets stale-sequence. A part of the same file that committed at a participant before the range fence stays readable, the fence answers that the participant holds the sequence, and the tracking partition tracks the object until the participant releases it.
- The leader pauses between the timer and the append. A short pause only orders the two events. If leadership moves, the new leader restarts the timer at its election, and the old leader's append fails at its old leader epoch.
- The leader's monotonic clock jumps forward. Leases end at the tracking partitions this broker leads, for about 1/N of brokers. Their files in flight fail once with stale-sequence, the brokers renew above the floor, and producers retry.
- An uploading broker pauses past the timeout with files in flight. Its commits get stale-sequence. An upload that lands after the queue deleted its key is the accepted leak.
- Leadership of a tracking partition changes more often than the timeout. Idle leases don't end, so leftover keys and the idle-broker duplicate window stay open until leadership holds for one timeout. This is a liveness cost only.

The grace period keeps its wall-clock mark time and `markedBefore` comparison. The Postgres backend already compares the marking broker's time with the deleting broker's clock, so cross-broker skew in the grace period is the status quo:

- A deleting leader whose clock runs ahead deletes early. A reader with cached coordinates gets `KeyNotFoundException`, its fetch fails with a storage error, and the consumer retries with fresh coordinates. The batches were already gone from coordinator state, or for consolidation pruning, the data lives in tiered storage. Skew beyond half the grace period is a reader-availability risk shared with the Postgres backend, not data loss.
- A key that never committed and is deleted early widens the accepted leak window.
- A deleting leader whose clock runs behind holds storage longer.
- A `FileCleaner` that pauses after `getFilesToDelete` while leadership moves deletes from a stale list. Every key on that list is in committed queue state and can't be referenced again, so a second delete is harmless.

No metric measures clock skew. Adding one would bring back a broker timestamp on every request for a gauge, and skew matters only for retention, `LogAppendTime`, and reader availability, which classic Kafka leaves to NTP. [Accepted risks](#accepted-risks) gives the NTP procedure.

Rejected alternative: a monotonic grace period that restarts at each election. It delays every delete by one grace period after a failover, and it changes the `getFilesToDelete(markedBefore, …)` contract that the Postgres backend shares.

## Local state

- Each replica of a coordinator partition stores that partition's state in its own SQLite database. A broker that hosts several coordinator partitions keeps one database for each. The database is a cache. The log is the source of truth.
- Each apply stores the applied offset and the leader epoch of the record, in the same transaction as the state change. On restart, the replica replays from that offset.
- On open, a replica checks its stored offset and epoch against the log: the log must still hold that offset, and the record at that offset must carry that epoch. A log flushed with `flush.messages=1` can still be truncated and rewritten by a later leader, so the offset alone doesn't prove the applied row still matches the log. This is the same check KRaft makes for a snapshot's `OffsetAndEpoch`.
- Invariant: the applied offset is always at or below the high watermark. A replica learns the high watermark from each fetch response. A record above the high watermark stays in the log buffer, unapplied, until the watermark passes it.
- SQLite holds only records at or below the high watermark. The leader drops unreplicated work from the overlay, so a replica never reverses an applied row.
- SQLite runs in WAL journal mode with `synchronous=NORMAL`. In WAL mode a reader doesn't block a writer, so a snapshot backup and a `findBatches` read don't stall applies. The WAL file grows during a long backup, because a checkpoint can't pass an open reader, and it shrinks after the backup ends.
- With `synchronous=NORMAL`, a power loss can roll back the most recent applies but never corrupts the database. Each rolled-back apply takes its applied offset with it, so the replica reopens at an earlier offset and replays from the log, which `flush.messages=1` keeps durable. SQLite applies only records already in the local log, so it can fall behind the log but never get ahead of it. `synchronous=FULL` would add a second fsync to the leader's reply path.
- The schema has tables for logs, files, batches, producer state, broker rows, and the applied offset on every partition, and for lease tables, tracking entries with each participant's state, and the deletion queue on tracking partitions. Participants also keep their pending releases until the release-accepted record applies. The layout is the implementer's choice, but it must support indexed lookup by partition and offset, which `findBatches` and `listOffsets` need, by object key, by `batches(file_id)` for the emptiness check, and by uploader broker, epoch, and sequence for fences.
- A coordinate row must be able to describe an extent: an offset range mapped to an object and a byte range, with the producer fields optional. A WAL batch row always carries its producer fields, because commits check idempotence against them. This design writes only WAL batch rows. The requirement keeps merged and consolidated objects possible without one row per batch. The `ControlPlane` interface and the `findBatches` response don't change.
- The SQLite driver is a JDBC driver with a bundled native library. Its license and packaging are to be checked against the repo's dependency rules before the choice is final.

### SQLite schema version

A snapshot is a copy of the SQLite file, and its layout is the implementer's choice, so it changes between binaries independently of the feature level. The schema carries its own version:

- Each database stores its schema version, for example in `PRAGMA user_version`. Each snapshot carries the version in its file name and in the fetch-snapshot response.
- A binary opens and migrates forward any older schema it supports.
- A binary that finds a newer schema than it knows treats the database or snapshot as missing, as it treats a corrupt one. It rebuilds from a snapshot in a schema it reads, or from offset 0 while the log start is 0.

An aborted upgrade therefore costs a rebuild on each replica that rolled back after migrating, but it never costs correctness. During a roll, an old-binary replica that needs a snapshot uses a peer that hasn't been upgraded yet. If none remains, the replica stays out of the ISR until it's upgraded.

Rejected alternative: gating the schema on the feature level. A rollback before the raise would be free, but every binary would have to read and write two SQLite schemas, which is far heavier than two record versions.

## Snapshots

Each coordinator partition runs the rules in this section on its own, with its own log start, snapshots, and reports. The snapshot requests name the coordinator partition.

A replica keeping up with the log never needs another replica's snapshot. It replays the log into SQLite and takes its own snapshots from that state. Snapshots exist so the leader can delete the log prefix, and a transfer exists for the replica that needs records the prefix deletion removed.

### Log start

- The log start is Kafka's own log start offset of the coordinator partition. Only the leader advances it, through `maybeIncrementLogStartOffset`, the call the `DeleteRecords` path uses. Followers adopt it from fetch responses, as `ReplicaFetcherThread` already does. Kafka refuses a log start above the high watermark, so the leader never deletes a record the ISR hasn't replicated.
- The leader advances the log start only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for. The new log start is the lower of two offsets: the offset of the leader's own older snapshot, and the highest offset that at least `min.insync.replicas - 1` other in-sync replicas have reported a snapshot at or beyond. At a minimum ISR of 2, that is the leader and one other replica. A third in-sync replica that hasn't snapshotted yet doesn't hold the log start back. It fetches a snapshot if it later needs one.
- The leader uses its older snapshot, not its newest, so the snapshot it keeps after the next rotation still covers the log start.
- When the topic has a single replica, there is no other replica to confirm, and the leader advances the log start from its own older snapshot alone. A development cluster with `diskless.metadata.topic.replication.factor=1` still bounds its log.
- The wait delays log-start advancement only. Produce and fetch don't wait on a follower's snapshot report.
- The log start stays at offset 0 until the first advancement. A replica that loses its database before then replays the log from offset 0 and needs no snapshot. A new cluster therefore has no unavailability window before its first snapshot.

### Taking a snapshot

- Every replica takes its own snapshot from its own applied state, when either `diskless.metadata.snapshot.interval.ms` elapses, 1 hour by default, or `diskless.metadata.snapshot.max.bytes.between.snapshots` of log has been applied since its last snapshot, 1 GiB by default. Whichever comes first triggers it. This follows KRaft's `metadata.log.max.snapshot.interval.ms` and `metadata.log.max.record.bytes.between.snapshots`. The trigger counts bytes rather than records, because a file-committed record can carry hundreds of batches, and replay time follows bytes.
- Both defaults are provisional. KRaft's 20 MiB doesn't carry over, because a coordinator snapshot is a full copy of the SQLite database and the log grows with every broker's commit. With 1 GiB, a rebuild replays at most about 1 GiB after its snapshot, and each replica's log stays near 2 GiB. Consolidation keeps pruning batches as data moves to tiered storage, so the database is expected to stay small, and 1 GiB may be large next to it.
- The SQLite backup API writes a point-in-time copy to a temporary file in the snapshot directory. The replica reads the snapshot's offset and epoch from the applied-offset row inside the finished copy, never from the live database. The applied offset commits in the same transaction as the state it describes, so the copy's row is the exact position of the copy's content. A tag read from the live database before the backup can be too low, which replays records the copy already holds. A tag read after the backup can be too high, which lets the leader delete records the copy doesn't contain.
- The replica fsyncs the temporary file, renames it to a final name built from the offset, the epoch, the schema version, and a checksum, and fsyncs the directory. Only then does it delete the snapshot that falls out of the retained pair. A crash at any step leaves the previous pair intact, and a partial file never carries a final name.
- On startup, the replica deletes any leftover temporary file. It checks a snapshot's checksum before it loads or serves that snapshot.
- Each replica keeps the newest snapshot and one older snapshot of its own. The count is a constant, not a config, because the log-start rule depends on that pair: with one snapshot, the leader's only covering snapshot would disappear at its next rotation, and a third would only use more disk. Snapshots are written to the broker's disk. They never go to object storage.

### Reporting snapshots

- A replica reports its newest snapshot offset to the leader on a private request after it finishes a snapshot, when it sees a new leader, and when its metadata shows it back in the ISR. The report isn't a field on the public fetch request. Steady-state followers don't already send a private request, so this report is its own request.
- The broker's scheduler sends and retries the report with Kafka's `ExponentialBackoff`, using the client retry defaults: 100 ms initial, 1 second maximum, multiplier 2, and 20% jitter. These values are constants, not configs. A leader change needs no retry, because the replica reports again to the new leader. The replica fetcher thread never sends the report. A report blocked on an unresponsive leader would otherwise stall fetching and push the follower out of the ISR. A missed report only delays log-start advancement.
- The leader records the newest snapshot offset reported by each replica. When it considers advancing the log start, a report counts only if its replica is in the leader's committed ISR and has no pending removal. This is the ISR view the leader already uses for the high watermark, and the leader doesn't ask the controller.
- When a replica leaves the ISR, the leader drops that replica's report. The replica may have lost its snapshot files while it was out, and its report counts again only after it reports again.

### Fetching a snapshot

- A replica fetches a peer's snapshot only when it needs records below the log start. That happens when it starts empty, for example on a replaced broker or after a reassignment. It also happens when its database is lost, corrupt, or in a newer schema and its own snapshots don't reach the log start, or when it was out of the ISR while the log start moved past its newest snapshot.
- The replica fetches the newest snapshot that covers the log start from an in-sync follower that holds one. It falls back to the leader only when no follower has one. The leader already carries every commit and every cache-miss read for its coordinator partition.
- Every fetch-snapshot request names the snapshot ID, its offset and epoch, and a byte position. Rotation doesn't delete a file that has an active transfer. A transfer stays active until it finishes or until no chunk request has arrived for `diskless.metadata.snapshot.transfer.idle.timeout.ms`, 60 seconds by default. Each chunk request restarts that timer, so a large snapshot is never cut off for its total duration. Active transfers live in memory, so a source restart ends them all.
- A broker serves at most `diskless.metadata.snapshot.max.concurrent.transfers` active transfers across all the coordinator partitions it hosts, 1 by default. A request beyond that limit gets a retriable busy error, and the receiver tries the next replica with a covering snapshot, in the same follower-first order. A replaced broker therefore rebuilds its coordinator partitions one after another, and the brokers that serve the snapshots keep their disk and network for their own partitions.
- The receiver writes chunks to a temporary file under that snapshot ID, and a resume continues from its last fsynced byte. If the source answers that the ID is gone, the receiver discards its partial file and asks for the newest covering snapshot again, with the same follower-first order. The ID in every request means a resume never splices bytes from two snapshots, even across a source restart or a leader change.
- The request and response follow KRaft's `FetchSnapshotRequest` and `FetchSnapshotResponse` schemas. The request carries the receiver's replica ID, a maximum byte count, the highest SQLite schema version the receiver reads, and either a snapshot ID with a byte position or, on the first request, a minimum end offset that asks for the source's newest snapshot covering the log start. The response carries an error code, the snapshot ID the source picked or confirmed, its schema version, the total file size, the position of the chunk, and the bytes in a `records`-typed field.
- A source that holds no covering snapshot at or below the receiver's schema version answers `SNAPSHOT_NOT_FOUND`, and the receiver tries the next replica in the follower-first order.
- A chunk is at most 8 MiB, a constant equal to KRaft's `MAX_FETCH_SIZE_BYTES`. The `records` type lets the source send the chunk from the file with zero copy, as KRaft does with `UnalignedRecords`, so a transfer never copies the snapshot through the source's heap.
- `SNAPSHOT_NOT_FOUND` answers a snapshot ID that is gone, or no covering snapshot. `POSITION_OUT_OF_RANGE` answers a position outside the file. Both codes exist in Kafka already.
- When the file is complete, the receiver checks its checksum, replaces its database, takes its applied offset from the snapshot, and replays the log from there.
- A replica serves its finished snapshot files whatever the state of its live database. It stops serving a file only when that file fails its checksum. Two replicas that both need a rebuild can then still serve each other.
- A replica treats its database as missing or corrupt when it fails the open check, when its applied offset is ahead of its log, or when its schema is newer than the binary knows. Such a replica rebuilds before it serves: from a peer's snapshot once the log start has moved, and from offset 0 before then. This includes a newly elected leader: the high-watermark gate in [Leader change](#leader-change) stays closed until the rebuild and the replay finish.

This is the KIP-630 mechanism in outline, applied to a normal topic. It is the one part of the design that has no equivalent in the KIP's text. Snapshot distribution is a private RPC because a normal topic has no `FetchSnapshot`.

## Reads

- Only the leader answers reads. `findBatches`, `listOffsets`, and every other call go to the leader of each user partition's coordinator partition.
- The delegate splits `findBatches` by coordinator partition. `fetchMaxBytes` is one budget across the whole request, so the delegate sends the full budget with each part, merges the results in request order, and trims them so the response stays within that budget. The results are batch coordinates, not data, so the extra rows a part returns cost little.
- Brokers absorb most fetch-side lookups in the existing batch coordinate cache. A cache miss goes to the leader.
- The KIP's stale `DisklessFindBatches` path is an optimization. It isn't part of this design.

### Deposed leaders

A KRaft broker doesn't step down when it loses the controller. `BrokerLifecycleManager` acts on the fenced flag only at the first unfence, so a deposed coordinator leader keeps answering from its own SQLite until it applies the metadata record for the new leader. This is the deposed window. Unclean election is off, so that state is a committed prefix of the new leader's log. It lacks the new leader's commits and still holds rows that the new leader has since removed. Kafka's coordinators have the same window: a deposed group coordinator answers `OffsetFetch` from its old state, and the consumer re-reads.

Every coordinator read that a deposed leader answers is lower than or equal to the current answer, or unknown. A lower answer makes a consumer re-read records, and an unknown answer makes it retry. Neither makes it skip a record. `findBatches` above the high watermark is the only exception, and [Barrier above the high watermark](#barrier-above-the-high-watermark) fixes it. A new read RPC that breaks this rule needs the barrier too.

| Read | Stale answer | Effect |
| --- | --- | --- |
| `findBatches` below the high watermark | Batches from the older state, possibly in objects that the tracking partition has since deleted | A missing object surfaces as the retriable `KAFKA_STORAGE_ERROR` in `FetchCompleter`. Object keys are never reused, so a reader never gets another object's bytes. Records that the new leader deleted can still be read, as from a classic deposed leader. |
| `findBatches` below the log start | The stale log start is at or below the true one | The out-of-range answer is always genuine. |
| `listOffsets` with `LATEST` or `EARLIEST` | Lower | Re-read. A position below the true log start gets a genuine out-of-range at a current leader. |
| `listOffsets` with a timestamp, including `by_duration` resets | Lower, or `UNKNOWN_OFFSET` when the older state holds no record at or after the timestamp | The candidate records in the older state include every current candidate below its high watermark, so the first match can only be lower. The consumer leaves an `UNKNOWN_OFFSET` partition unreset and tries again. |
| `listOffsets` with `MAX_TIMESTAMP` | An older maximum | Same as a classic deposed leader. |
| `getCrossTierLogStart` | Lower or empty | The remote log manager reclaims less, or defers. The value only advances, so reporting it back does nothing. |
| `getFilesToDelete` | Keys in committed queue state | Those keys can't be referenced again, so a second delete is harmless. |

Rejected alternative: a leader lease. The deposed leader would stop serving once its followers stopped fetching, and each new leader would wait one lease before serving. That bounds the window, but it adds a wait to every failover and makes safety depend on clock drift rates.

### Barrier above the high watermark

`findBatches` answers `OFFSET_OUT_OF_RANGE` for an offset above the high watermark. From a deposed leader, that answer can be false. A consumer that starts from an offset committed under the new leader then resets by `auto.offset.reset`: `earliest` rewinds it to the log start, and `latest` answered by a current leader skips records. KIP-320 protects classic topics from this: the consumer takes the leader epoch from its committed offset and refuses older metadata. For diskless, a coordinator leader change doesn't bump the user partition's epoch, and clients never see coordinator epochs.

The rule:

- When `findBatches` gets an offset above the high watermark, the leader parks the request and appends a barrier record after the request arrives. Requests that arrive while a barrier is open share it.
- Once the high watermark passes the barrier, the leader checks the offset again. If the offset is still above the high watermark, the answer is `OFFSET_OUT_OF_RANGE`. If the high watermark passed the offset in the meantime, the fetch returns batches.
- Each parked request gets its own 500 ms deadline, measured from its arrival at the leader. The coordinator service schedules it on the runtime's `Timer` and answers `OFFSET_NOT_AVAILABLE` when it fires. The consumer refreshes its metadata and retries. If the shard unloads, the parked request gets `NOT_COORDINATOR`. The barrier write itself keeps the runtime's write timeout.
- The 500 ms is a constant, not a config. It's well above a healthy barrier commit, which is one replication round trip plus the leader and follower fsyncs, and far below the 30-second request timeout.
- A barrier from a deposed leader never commits. The append uses `acks=-1`, and the new leader came from the deposed leader's ISR and stops fetching from it once elected. A committed barrier therefore shows that no other leader took over before the request arrived, so the leader's state holds every record acknowledged before it.
- The barrier must be a new record. `CoordinatorRuntime` completes a write with no records once the pending writes complete. On an idle deposed leader nothing is pending, so the write proves nothing. An earlier pending write doesn't prove it either, because a follower fetch sent before the new election can still commit it.
- Out-of-range below the log start and `listOffsets` keep their current answers.

A consumer that keeps up never reaches the barrier, because an offset equal to the high watermark is in range. Only a deposed leader, a genuine seek past the end, or a client that keeps fetching past the end reaches it. The barrier adds one barrier commit to the answer, after any long-poll wait the Postgres backend also pays. A current leader commits it in one replication round trip. A deposed leader answers `OFFSET_NOT_AVAILABLE` after 500 ms, so a fetch that misses the local cache can take up to its long poll plus 500 ms.

The barrier rate is at most one barrier per flush of the coordinator partition's batch. When commits are flowing, the barrier joins an open batch and adds no append. The worst case is an idle partition that appends once per fsync, which is the rate a busy partition's commits already drive. The out-of-range count of `BarrierRate` shows a client that keeps fetching past the end.

### Delayed-fetch probe

The delayed-fetch probe never runs the barrier. `ControlPlane` gets a default method for the probe, `findBatchesNoWait`, that calls `findBatches`, so Postgres and in-memory don't change. The coordinator backend sets a no-wait field in the private find-batches request. With the field set, the leader answers an offset above the high watermark at once with `OFFSET_NOT_AVAILABLE` and appends no barrier. `DelayedFetch.tryCompleteDiskless` completes the fetch on any error, and `onComplete` then runs the real call through `Reader`. Below the high watermark the probe gets real answers, so a fetch with the batch coordinate cache off still finishes early when data arrives.

Rejected alternative: a backend capability that turns the probe into a cache miss. A fetch with the cache off would always wait out its long poll, even for data that already arrived.

### Thread hold

`ControlPlane.findBatches` stays synchronous. A parked part holds a fetch metadata thread on the calling broker for at most 500 ms, and the delegate's merge in request order delays the whole fetch response. Only a deposed leader holds the full 500 ms. In a deposed window, the pool of 8 threads limits that broker to about 16 diskless fetches per second until it learns of the new leader. Postgres holds the same thread for up to its 5-second socket timeout per attempt when the database is unreachable. This is a cost of the deposed window, not a risk to data.

Alternative for the design review group: an asynchronous `findBatches` that returns a `CompletableFuture` removes the hold. It changes `ControlPlane` for every backend, where Postgres and in-memory would wrap their calls in an executor, and it changes `Reader`'s `FindBatchesJob` chain. If the review group doesn't prefer the synchronous call, this is the replacement.

Rejected alternatives for the barrier:

- Always answering the retriable `OFFSET_NOT_AVAILABLE` above the high watermark. A consumer that genuinely seeks past the end would wait for the log to grow instead of resetting, which changes classic API behavior.
- Passing the fetch's `FetchParams.maxWaitMs` through `findBatches` as the barrier deadline. It changes `ControlPlane` for every backend, and a consumer with `fetch.max.wait.ms=0` would never see a genuine out-of-range.
- The request timeout or the commit deadline as the barrier deadline. A deposed leader would hold every fetch that touches it for about 30 seconds.
- A public `scheduleWriteOperation` overload that takes a `Duration`. Upstream has none. `OperationTimeout` starts only after the event runs and appends, so queue wait comes on top of the 500 ms, and a request that joins a shared barrier late gets less than 500 ms.
- A shorter deadline. A healthy leader under fsync load would fail more genuine seeks with `OFFSET_NOT_AVAILABLE`, and the hold only shrinks.
- A fixed barrier window, which adds a second timer next to the linger, and fixing the runtime to a fixed linger, which gives up adaptive linger's group commit to bound a misbehaving client.

### Leader epochs on private RPCs

The private RPCs carry no leader epoch. They rely on `NOT_COORDINATOR`, as the group and share coordinators do. KIP-320's check exists so that a client can detect log truncation, and a broker that calls the coordinator keeps no position that a truncation invalidates. Rejecting a caller whose epoch is lower than the receiver's would only force a needless metadata refresh. Rejecting a caller whose epoch is higher only speeds up failure in a race that the stale-read rule already makes safe. Adding the epoch later is a new request version.

## Capacity and isolation

Status: open, and out of scope for version 2's review, as [map.md](map.md#out-of-scope) records. This section carries version 1's analysis with the lease model's wording.

Each coordinator partition's leader serves every commit part, lease request, release report, fence, cache-miss `findBatches` and `listOffsets` call, and maintenance job for its user partitions and tracked brokers, on top of the broker's own partitions. `N` leaders share the cluster's load, and Kafka's leader placement spreads them across brokers.

Splitting reduces the work per batch, not the number of commit calls. An object usually holds batches for most coordinator partitions, so each leader still receives about one commit part per broker per commit interval. A produce waits for its slowest part, so the commit-latency tail grows with `N`. The part to the tracking partition runs in parallel with the others, and it's empty only when the tracking partition holds none of the file's batches.

The load differs from the Postgres backend in two directions:

- Lower cost per call: SQLite runs in the broker process, so a call has no network round trip, connection pool, or remote query planning. The Postgres backend spends part of its commit latency on those.
- Co-tenancy: SQLite shares the leader broker's heap, page cache, disk, and garbage collector with that broker's own partitions. A large `findBatches` scan can evict the broker's log pages. A garbage collection pause on that broker stalls every commit that has a part on the coordinator partitions it leads, and with most objects spanning most coordinator partitions, that is most commits. The Postgres backend runs on its own host and has none of this contention.

A cluster-wide consumer restart empties the batch coordinate cache on every broker at once. Every miss then reaches the leaders together.

### Benchmarks

Each benchmark runs the same workload against the Postgres backend and against this backend, at `N=1` and at `N=8`, and compares the broker metrics both backends already report: `FileCommitTime`, `FileCommitWaitTime`, `FindBatchesTime`, and `FileCommitErrorRate`. The benchmarks extend the producer setup in `inkless-benchmarks/`.

- Commit throughput: increase the number of brokers and producers until commit latency rises. Record the commit rate at which each backend's P99 `FileCommitTime` doubles. This backend runs with `flush.messages=1`, so the result includes the fsync cost on the leader and on the followers.
- Partition count: run a fixed commit rate at `N` of 1, 4, 8, and 16. Record P99 `FileCommitTime`, the commit parts each leader receives, and per-leader CPU and fsync rate. The result confirms or replaces the default of 8.
- Tracking part: measure the latency that the part to the tracking partition adds, and the lease renewals and fences each tracking partition handles.
- Partial failure: take one coordinator partition offline under load. Measure the share of produces that fail and the latency of parts on the healthy coordinator partitions, including parts of files that also hold batches for the offline one, with lease fallback. Parts on healthy coordinator partitions must not wait for the offline partition's deadline.
- Read-heavy load: run lagging consumers that miss the batch coordinate cache, alongside steady produce. Compare `FindBatchesTime` and the leader's produce latency for its own partitions.
- Cache storm: restart every consumer at once and measure how long `FindBatchesTime` stays above its steady-state P99.
- Co-tenancy: load a leader broker's own partitions heavily during the commit and read benchmarks. Compare that broker's produce latency with that of a broker that leads no coordinator partition.
- State size: grow the file and batch row count across several orders of magnitude, and record `findBatches` latency, election time, SQLite file size, snapshot backup duration, and log growth between snapshots at each step. Run it with consolidated topics, which is the expected production workload.
- Failover: kill a leader under load and record the time until commits succeed again.
- Snapshot transfer: rebuild one replica, then two at once, while commits and reads run at steady load. Record the leader's `FileCommitTime` and `FindBatchesTime`, the source replica's disk and network use, and the time each rebuild takes.

The workloads, scales, and pass criteria need more design before these run. Results decide whether the coordinator needs a separate disk for SQLite, a cap on `findBatches` result size, a higher `diskless.metadata.snapshot.max.concurrent.transfers` default, a different default partition count, a different snapshot byte trigger, or none of these.

## Metrics

The coordinator registers two metrics groups, each following its neighbors:

- `DisklessMetadataCoordinatorRuntimeMetrics extends CoordinatorRuntimeMetricsImpl`, with the group `diskless-metadata-coordinator-metrics`. `BrokerServer` registers it as it registers the group and share coordinators' runtime metrics. It brings `event-queue-time-ms`, `event-purgatory-time-ms`, `batch-flush-time-ms`, and the other runtime metrics unchanged. `event-queue-time-ms` and `event-purgatory-time-ms` bound the duplicate window.
- One Yammer `KafkaMetricsGroup` named after the backend class, with PascalCase names, as `PostgresControlPlaneMetrics` and `FileCommitterMetrics` do. A metric scoped to a coordinator partition carries a `partition` tag. A per-broker metric carries none.

The backend's metrics, where "leader" is the coordinator partition's leader and "broker" is the uploading or calling broker:

| Name | Scope | Shows |
| --- | --- | --- |
| `UnknownOutcomePartRate` | Broker, `partition` tag | Parts that reached the commit deadline without a confirmation. |
| `StaleEpochRejectRate` | Leader | Requests that a broker sent before it restarted. |
| `StaleSequenceRejectRate` | Leader | A spike right after an idle lease end at an active broker means the lease ended without cause. |
| `PartitionMappingMismatchRate` | Leader | Requests rejected because a user partition doesn't map to the receiving coordinator partition. |
| `LeaseRenewalRate` | Broker | Lease renewals. |
| `LeaseFallbackRate` | Broker | Renewals at a fallback partition. |
| `FileNoLeaseErrorRate` | Broker | Files failed with no leased sequence. |
| `IdleLeaseEndRate` | Leader of the tracking partition | A burst across many brokers points to a clock jump. |
| `FenceRate`, tagged `type` (sequence, missed, or range) and `answer` (referenced or released) | Leader of the tracking partition | The fences issued and what they found. |
| `DeletionQueueSize`, `DeletionQueueOldestAgeMs` | Leader of the tracking partition | A growing queue with a young oldest row means a clock running behind. An old oldest row means stuck deletes. |
| `BarrierRate`, tagged `outcome` (out of range, in range, or timed out) | Leader | A timed-out rate means the broker answers as a deposed leader. No other metric shows that. |
| `ApplyStoppedPartitionCount` | Broker, gauge | Coordinator partitions whose apply has stopped on an unreadable record or snapshot. |

Version 1's open owner entries, tombstones and fences held, age-check rejections, and predecessor fences leave with the owner model.

The design also cites these existing metrics: `FileCommitTime`, `FileCommitErrorRate`, `FileUploadErrorRate`, `FindBatchesTime`, `PartitionStorageErrorRate`, `OfflinePartitionsCount`, `UncleanLeaderElectionsPerSec`, and `ElectionFromEligibleLeaderReplicasPerSec`.

## Leader change

- Each coordinator partition changes leader on its own. A leader change stalls only the commit parts, lease requests, reads, and tracked objects of that coordinator partition.
- A new tracking leader starts every idle timer at its election and resends every fence that has no recorded answer. A new participant leader resends the releases that have no release-accepted record.
- A new leader starts with an empty overlay. It rejects requests with a retriable error until its high watermark reaches the log end offset it saw at election. Once it serves, it checks its metadata image for broker epochs to retire.
- Records that weren't committed under the old leader are discarded by the log. The leader drops those entries from its overlay and fails their parked replies with a retriable error. Their requests were never acknowledged, so the retry rule in [Commit and retries](#commit-and-retries) covers them.
- The delegate retries the commit part against the new leader after a `NOT_COORDINATOR` or retriable error, with the same pair and object key, until the commit deadline passes.
- A deposed leader serves committed but older state until it learns of the new leader, as [Deposed leaders](#deposed-leaders) describes.

### Election of a coordinator leader

The controller's automatic last-known-leader election elects `lastKnownElr[0]` when the ISR and ELR are both empty, on the online path as well as the unclean one, and ignores `unclean.leader.election.enable`. With intact storage and `flush.messages=1`, that leader holds every acknowledged record, and the election is the only way a coordinator partition comes back after every replica restarts uncleanly. A broker that returns under the same ID with a wiped or replaced disk would be elected with an empty log, and the surviving replicas would truncate to match it. A classic topic loses data the same way, but here the damage differs in kind: every follower truncates to offset 0, the partition loses its whole batch index, lease tables, and deletion queues, and object storage doesn't roll back with it. The election also destroys copies that survived.

Storage identity, not shutdown cleanliness, decides whether a replica may lead. Every rule keys on the name `__diskless_metadata`, so other topics keep upstream behavior:

- Storage change guard: in `ClusterControlManager.registerBroker`, next to the shutdown handler, the controller removes the broker from the ELR and `lastKnownElr` of every `__diskless_metadata` partition whose assigned directory for that replica isn't in the new registration. An `UNASSIGNED` directory counts as changed. `handleAssignReplicasToDirs` applies the same rule when a replica moves off a directory the broker no longer registers. Registration precedes unfencing, so the guard runs before the unfence scan can elect the broker. Formatting always creates a new directory ID, so the guard catches a wiped, replaced, or reformatted disk, and a multi-directory broker that restarts cleanly without the replica's directory.
- The guard looks up `__diskless_metadata` in `topicsByName` and walks only that topic's partitions. That costs `O(N × RF)`, once per broker process start, on the controller event thread. It never scans every topic, because `lastKnownElr` has no index.
- ELR across an unclean shutdown: `handleBrokerShutdown` handles `__diskless_metadata` partitions as it handles a clean shutdown. The broker leaves the ISR and joins the ELR if the ISR drops below the minimum. With `flush.messages=1`, every ELR member holds every acknowledged record on disk, and the storage change guard removes the members whose storage didn't survive. The first returning replica wins a clean ELR election. The last-known-leader election stays on as a fallback.
- Cluster-wide ELR clear: a cluster-level or broker-level `min.insync.replicas` change never reaches the topic-level override, so the cluster-wide `ClearElrRecord` skips `__diskless_metadata`. The skip lives in both replays: `ReplicationControlManager.replayClearAllElrs` on the controller, and `TopicsDelta` and `TopicDelta.replay()` in the metadata image. Every controller and broker applies it alike, because the topic exists only once the coordinator feature is finalized. The topic-level `ClearElrRecord` never fires either, because [Topic protection](#topic-protection) rejects any change to the topic's `min.insync.replicas`.

Rejected alternatives:

- Turning the last-known-leader election off for the topic. After a correlated power loss, every coordinator partition would stay offline with intact disks.
- Turning ELR off. The returning broker then wins a clean election, with nothing marking it unsafe.
- An operator procedure alone. It's fragile in the middle of an outage.

## Lifecycle

- The controller never calls the coordinator. A diskless topic's creation or deletion commits to cluster metadata only. `ControllerApis.createTopicsDiskless` and the diskless delete path stop calling the control plane, which removes the ten-second metadata wait on the request thread.
- Each coordinator partition's leader reconciles its state against the metadata image. `BrokerMetadataPublisher.onMetadataUpdate` already delivers that image to every broker, and it is where the group and share coordinators learn about topic changes. On election and on each metadata update, the leader diffs the user partitions in its SQLite against the diskless user partitions in `MetadataImage` that map to its coordinator partition. A delta's `deletedTopicIds` isn't enough: a broker that loads a snapshot never sees a delete for a topic the process didn't hold. For each mapped diskless user partition in the image that SQLite doesn't hold, the leader appends a partition-created record. For each topic ID SQLite holds that the image no longer has, it appends a topic-deleted record. The partitions of one topic map to several coordinator partitions, so each of those coordinator partitions creates and deletes its own share of the topic, at slightly different times. KRaft already stops produce to a deleted topic at the broker. A leader that was down catches up when it returns, and the metadata log is the retry record, so the coordinator keeps no separate queue. The diff is by topic ID, so a topic recreated under the same name is a new ID and doesn't collide with the one being deleted.
- The diff counts pending overlay entries as well as SQLite rows, so two metadata updates that arrive before the first partition-created record applies don't append it twice. Apply is idempotent as a second guard. A partition-created record for a row that exists, or a topic-deleted record for a topic already marked deleted, is a no-op, and the applied offset still advances. The Postgres insert does the same with `onConflictDoNothing`.
- A produce to a diskless partition that the leader hasn't reconciled yet gets a retriable error. Reconciliation finishes once the high-watermark gate opens.
- The admin client stops retrying a delete once the controller reports success, which is when the topic leaves the metadata log. It doesn't retry coordinator cleanup. Today's `ControllerApis` deletes from the control plane before the KRaft delete for that reason. Reconciliation removes the need for that order: a topic ID still in SQLite and absent from the image is retried on the next metadata update and on the next election.
- The leader of a switched partition calls `initDisklessLog` after the controller has committed the classic-to-diskless seal. `ControllerApis.handleInitDisklessLogRequest` writes that seal into cluster metadata. The control plane call happens later, from `InitDisklessLogManager` on the partition leader, through `AwaitingMetadata.sendBatch` in `InitDisklessLogState`.
- That same partition leader calls `repairDisklessLog` from `ReplicaManager`. `KafkaApis.handleRepairDisklessLogRequest` invokes `ReplicaManager`, which calls the control plane only while this broker leads the partition.
- Each call appends its record and replies once the high watermark passes it. A user partition's create, delete, initialization, and repair go to the same coordinator partition as its commits, so they are ordered with every commit to that user partition in one log. Ordering across user partitions isn't guaranteed, and nothing depends on it.

## Maintenance

- Retention and topic purge run on each coordinator partition's leader, for its own user partitions. Each run appends its outcome records to the log.
- Topic purge works through deleted partitions in capped passes, as `purge_deleted_logs_v1` does in Postgres. Each pass appends one deleted-log-purged record per partition it touched, in one log batch. Applying a deleted-log-purged record removes that partition's batches whose last offset is below the boundary, or all of them when the record has no boundary. When the record says the partition is empty, the apply also drops its `logs` and `producer_state` rows.
- The emptiness check in each batch-removing apply releases the objects whose last local batch it removed, and the participant reports them to their tracking partitions.
- Deletion runs on tracking partition leaders, through `FileCleaner` and the deletion queue, after the grace period.
- Every tracking partition leader runs its idle lease timers, and every leader appends broker-epoch-retired records from its metadata image.

## Consolidation and cross-tier offsets

- The broker that hosts a partition's local log decides the safe prune offset, from `highestOffsetInRemoteStorage` and the classic-to-diskless seal.
- The broker sends `pruneDisklessLogs` or `advanceCrossTierLogStartOffset` to the leader. The leader appends the outcome record.
- The broker advances its own prune floor from the response.
- The same pattern covers `CrossTierLogStartReporter` and the delete-records path.

The leader doesn't need a local log for any partition, because it never reads one.

## Rollout

One upstream feature, for example `diskless.coordinator.version`, has two roles:

- It selects the backend. Level 0 means the coordinator is off.
- Its level caps the record set. Level 1 is the initial set, including the barrier record, and each later level names the types and versions it adds.

An Inkless-owned feature, for example `inkless.diskless.coordinator.version`, gates the Inkless extension tags from [Record compatibility](#record-compatibility):

- Level 0 means no extension tags, and each later level names the tags it adds. The leader writes a tag only when the finalized level includes it.
- Each level declares the upstream feature level it needs, through `FeatureVersion.dependencies()`.
- The feature doesn't need to be finalized at format time.

Rejected alternatives: gating records on `metadata.version`, where fork-specific levels would collide with upstream numbering at every sync. A second upstream feature only for the record format, which adds a pair of levels that operators must keep consistent. Inkless levels in a high range of the upstream feature, because a supported range is contiguous, so a broker that supports level 100 claims every level below it. Gating on the Inkless release version, which the controller has no way to check. Letting unrecognized tags in the extension range pass so that vanilla can read an Inkless log, which works only if every extension is safe to drop.

Startup and configuration:

- A new cluster finalizes the upstream coordinator feature at format time, with `kafka-storage format --feature`. The coordinator is active from the first startup.
- The controller creates `__diskless_metadata` with the partition count from `diskless.metadata.topic.num.partitions`, the replication factor from `diskless.metadata.topic.replication.factor`, the minimum ISR from `diskless.metadata.topic.min.insync.replicas`, and the fixed configs in [Topic protection](#topic-protection). Changing these configs afterward has no effect, and [Topic protection](#topic-protection) rejects every request that would change the topic.
- `inkless.control.plane.class` must name the new class. If it names the in-memory or Postgres class while the feature is finalized, the broker refuses to start.
- Diskless topics still need the existing Inkless storage configuration and `diskless.enable=true`.

Upgrade and downgrade:

- `FeatureControlManager.reasonNotSupported` rejects an upgrade unless every registered broker and controller supports the new level, and registration rejects a broker that lacks a finalized level.
- The controller refuses every downgrade of both coordinator features, `SAFE_DOWNGRADE` and `UNSAFE_DOWNGRADE` alike. A name-keyed branch in `FeatureControlManager.updateFeature`, next to the `kraft.version` branch, does this. Without it, a safe downgrade from level N to N-1 leaves level-N records in the log and snapshots, and brokers rolled back to an N-1 binary stop on them. A downgrade to 0 would also switch the backend back to Postgres.
- Upgrade: roll every broker to the new binary, then raise the level. Raising the level is the point of no return.
- Aborted upgrade: the binaries roll back before the level rises. No leader has written a newer record, so the log stays readable. A rolled-back replica can still find a newer SQLite schema, and it rebuilds as [SQLite schema version](#sqlite-schema-version) describes.
- Binary downgrade below the finalized level: registration rejects the broker, so it never meets records it can't read.
- A later level that changes behavior but adds no records could allow a downgrade, as `MetadataVersion.checkIfMetadataChanged` does. The design can add that per level when such a level ships.
- There is no rollback onto Postgres. A downgrade would orphan the files and batches in `__diskless_metadata`, and no migration exists. Clusters already on Postgres are never formatted with the feature.

`INKLESS_OWNERSHIP` gains `Topic.java`, `Feature.java`, `FeatureControlManager.java`, `ClusterControlManager.java`, `ReplicationControlManager.java`, `ConfigurationControlManager.java`, `TopicsDelta.java`, `TopicDelta.java`, and `LogManager.scala` as `INTERLEAVED` entries where they aren't already, when the changes land. The probe rule touches `DelayedFetch.scala` and `ReplicaManager.scala`, which are already `INTERLEAVED` entries.

## Private RPCs

Numbers are assigned in the 504+ range at implementation time. Each request type and its handler is listed here. Every request names the coordinator partition it targets, and "Leader" means that partition's leader. The requests carry no tagged fields upstream, and a leader epoch added later is a new request version.

| Request | Caller | Handled by |
| --- | --- | --- |
| Lease sequences | Broker's sequence source, at its tracking partition and then at fallback partitions | Leader of the tracking partition |
| Commit file | Broker, from `FileCommitJob`, one request per participant plus the tracking part | Leader |
| Find batches, with a no-wait field for the delayed-fetch probe | Broker, on cache miss | Leader |
| List offsets | Broker | Leader |
| Delete records | Broker, from the delete-records path | Leader |
| Init diskless log | Partition leader, from `InitDisklessLogManager` after the controller commits the seal | Leader |
| Repair diskless log | Partition leader, from `ReplicaManager` | Leader |
| Enforce retention | Local only, on the leader | Not an RPC |
| Get files to delete, delete files | Local only, on tracking partition leaders | Not an RPC |
| Prune diskless logs | Broker, from `ConsolidatedDisklessLogPruner` | Leader |
| Advance cross-tier log start | Broker, from `CrossTierLogStartReporter` | Leader |
| Report release | Participant leader's scheduler | Leader of the tracking partition |
| Fence sequences | Tracking partition leader, for a silent participant, a missed range, or a lease's unused range | Leader of each fenced partition |
| Fetch snapshot | Replica, when it needs records below the log start | An in-sync follower that holds a covering snapshot, or the leader when no follower does |
| Report snapshot offset | Replica's scheduler, after a snapshot, on a new leader, and on rejoining the ISR | Leader |

Each request carries the coordinator partition and the topic partition, broker, or file it acts on, and it returns the same error codes as the `ControlPlane` method it wraps. The stale-epoch and stale-sequence errors are non-retriable. A lease or commit with an unleased sequence, a negative sequence, or a mismatched retry gets `INVALID_REQUEST`.

## Failure modes

The failure-mode section holds two tables. Failure modes lists what the design fixes or tolerates, with the test that covers each row. Accepted risks lists the losses the design accepts, with why and what shows them. A row that closes a review gap names a test. Any other row names one or says why it has none. Test names refer to [Testing](#testing).

### Commits and leases

| Failure | Behavior | Test |
| --- | --- | --- |
| Leader crashes during a commit | The part wasn't acknowledged. The delegate retries against the new leader within the commit deadline. The broker row's `(epoch, sequence)` returns the stored result, or the retry commits as new. | Cluster: failover mid-commit |
| A retry arrives while the first attempt is pending on the leader | The retry waits for the pending record's result. Only one record and one set of offsets exist. | Cluster: retry while pending |
| A commit part misses the deadline, and its record commits later | Its batches get `KAFKA_STORAGE_ERROR`. The late record still makes them visible, unless the broker's next commit, a fence, or an epoch retirement raised the floor first. Idempotent producers deduplicate their retries. | Cluster: deadline then next part |
| A coordinator partition is unavailable, slow, or rebuilding its database | Parts on it wait until their file's deadline, then get `KAFKA_STORAGE_ERROR`. Parts on other coordinator partitions keep committing. If it's a broker's tracking partition, the broker renews its lease at the next partition. | Cluster: one coordinator partition offline |
| The ISR of a coordinator partition drops below `min.insync.replicas`, including two of three replicas down | Appends fail at once with `NOT_ENOUGH_REPLICAS`, and the runtime reverts the batch. The delegate retries parts until the deadline, and a lease renewal falls back to the next partition. Other coordinator partitions keep committing. | Cluster: ISR below minimum |
| Some parts of a commit succeed and others fail | The committed batches return their offsets. The others get `KAFKA_STORAGE_ERROR`, and the producer retries only those partitions. The tracking partition deletes the object only once every participant is released. | Cluster: one part fails |
| A retry arrives for a part where some batches failed or were duplicates | The broker row's stored result answers, also after a failover and after retention removed the batches. | Cluster: retried mixed results |
| The uploader broker restarts with parts in flight | Leaders reject the old epoch as stale. The broker-epoch-retired record ends its leases, the range fences run, and leftover keys are queued. | Cluster: restart with part in flight |
| A file closes with no leased sequence | It fails at once with `KAFKA_STORAGE_ERROR` and isn't uploaded. | Shard: acceptance rules |
| The tracking partition doesn't answer a renewal | The broker leases at `(t + 1) % N`, and onward. Only the user partitions of the unavailable partition fail. | Cluster: tracking partition down |
| A lease reply is lost | The retry names the same range and is a no-op. A range stored at a fallback partition is never used, and becomes leftover at that partition's lease end. | Fault injection: lost lease reply |
| The part to the tracking partition fails while other parts commit | Once a later sequence commits there, the tracking partition fences the missed sequence at all `N` and builds the participant set from the referenced answers. | Cluster: missed sequence |
| A fence races a part in flight | A part pending before the fence commits, and the fence answers referenced. A part that arrives after it gets stale-sequence, and the producer retries. | Shard: acceptance rules |
| A silent broker's lease goes idle | The lease-ended record raises the floor, and range fences go to every partition. Keys that no partition holds are queued. | Cluster: idle lease end |
| The idle timer fires as a part arrives | The part to the tracking partition gets stale-sequence. A participant's committed part stays readable and tracked. | Shard: idle timer against a part |
| A tracking leader's monotonic clock jumps forward | Leases end at the partitions it leads. Files in flight fail once with stale-sequence, and the brokers renew. | Shard: idle timer against a part |
| A tracking leader pauses between the timer and the append while leadership moves | The old leader's append fails at its old epoch, and the new leader restarts the timer. | Shard: leader pause |
| Tracking-partition leadership changes faster than the idle timeout | Idle leases don't end. Leftover keys and the idle-broker duplicate window stay open until leadership holds for one timeout. | None: liveness only |
| A broker bug sends an unleased sequence or a retry with a different batch count | `INVALID_REQUEST`. | Shard: acceptance rules |
| A part names a user partition that doesn't map to the receiving coordinator partition | `INVALID_REQUEST`, counted in `PartitionMappingMismatchRate`. | Cluster: mapping mismatch |

### Deletion

| Failure | Behavior | Test |
| --- | --- | --- |
| A partial send followed by `ControlPlaneUnavailableException`, or a delegate replaced between two parts of one file | The broker never deletes the object directly. The queue deletes it after the next commit or the lease end, plus the grace period. | Shared suite: partial send |
| The broker stops before the upload | The epoch end marks the unused range, and the queue deletes the missing key, which counts as success. | Cluster: broker stops before and after upload |
| The broker stops after the upload and before any part commits | The same range holds the key, and the queue deletes it after the grace period. | Cluster: broker stops before and after upload |
| The broker stops after some parts commit | Each committed participant reports its release later. The tracking partition fences silent participants once it and every participant it has heard from are released, and the ones that never committed count as released. | Cluster: broker stops after some parts |
| A commit where every batch fails | Its file-committed record releases the object at once. | Shared suite: marking rule |
| An object's batches expire on one coordinator partition while another still holds batches | The object stays until both are released. | Cluster: object expires on one partition |
| The tracking partition's leader is unreachable for a release report | The participant retries. Only that object's deletion waits. Commits and reads continue. | Cluster: tracking leader change |
| An uploader pauses past the idle timeout with an upload in flight | Its commit gets stale-sequence, the broker doesn't delete the object, and no batch row points to the key. A late upload after the delete is the accepted leak. | Backend: uploader pause |
| Leadership moves between the storage delete and the files-deleted record, or `FileCleaner` deletes from a stale list | The key is deleted at most twice, which is harmless. | Backend: stale `FileCleaner` list |
| Two clusters share a bucket and prefix | The cluster ID in every key keeps deletes within this cluster. | Seam: key name function |
| An upload stops partway | A partial object is never visible. | None: storage property |

### Leader change and reads

| Failure | Behavior | Test |
| --- | --- | --- |
| Leader loses the partition | The new leader serves once its high watermark reaches its election-time log end offset. | Fault injection: high-watermark gate |
| Append fails, or the leader resigns, before the high watermark | The leader drops the failed overlay entry and every entry after it, and fails their parked replies with a retriable error. SQLite is unchanged. | Cluster: produce in flight during leader change |
| Leader change truncates unreplicated records | The leader drops the truncated entries from its overlay and fails their parked replies with a retriable error. A replica discards fetched records above its applied offset that the new leader doesn't have. Because the applied offset never exceeds the high watermark, the truncation never reaches applied state. | Cluster: truncation |
| Coordinator leader is down when a topic is created or deleted | The metadata change commits without it. The new leader reconciles on election. Produces to the new topic get a retriable error until reconciliation finishes. | Cluster: topic created while leader down |
| A deposed coordinator leader answers reads before it learns of the new leader | It serves committed but older state. `listOffsets` answers are lower or unknown. A coordinate into an object deleted since gives a retriable `KAFKA_STORAGE_ERROR`. A `findBatches` above the high watermark gets `OFFSET_NOT_AVAILABLE` 500 ms after its arrival, once the barrier times out. This matches a classic deposed leader. | Cluster: deposed leader. Backend: stale-read rule |
| With the batch coordinate cache off, the delayed-fetch probe meets an offset above the high watermark | The leader answers at once without a barrier, and the fetch's `onComplete` runs the real call. | Seam: delayed-fetch probe |
| A client keeps fetching past the end | It costs at most one barrier per flush, none extra under load, and the out-of-range count of `BarrierRate` shows it. | Shard: barrier rate under adaptive linger |

### Replicas and snapshots

| Failure | Behavior | Test |
| --- | --- | --- |
| Replica falls behind the log start | It fetches the newest snapshot that covers the log start, from an in-sync follower or, when no follower has one, from the leader, then replays. | Cluster: catch up from snapshot |
| Replica's SQLite is corrupt or missing, or its stored offset and epoch no longer match the log | It rebuilds from a peer's snapshot that covers the log start, then replays the log. Before the log start has first moved, it replays from offset 0 instead. A replica that is the elected leader rejects requests until the rebuild finishes. | Cluster: new leader with missing database. Cluster: database lost before first log start move |
| A rolled-back replica finds a newer SQLite schema | It treats the database as missing and rebuilds. A snapshot fetch above the receiver's schema gets `SNAPSHOT_NOT_FOUND`, and the receiver tries the next replica. | Backend: SQLite schema. Cluster: upgrade and rollback |
| A replica meets a record or snapshot it can't read | It stops applying that coordinator partition and doesn't diverge. A leader resigns. With the write gate and the downgrade refusal, this happens only through a bug or a manual binary downgrade. The operator rolls the broker to a binary that supports the finalized levels, and the replica resumes from its applied offset. | Backend: apply stop |
| Power loss on one replica or across every replica | `flush.messages=1` keeps every acknowledged record on each ISR member, so no unflushed tail exists to lose. SQLite may roll back its most recent applies, and the replica replays them from the log. | Cluster: unclean restart of every replica |
| Crash while a snapshot is written | Only the temporary file can be incomplete. Startup deletes it, and the previous pair of snapshots stays intact. | Seam: snapshot unit tests |
| Snapshot source rotates or restarts during a transfer | Rotation keeps a file while its transfer is active. If the transfer idled out or the source restarted and the snapshot ID is gone, the receiver discards its partial file and fetches the newest covering snapshot. | Cluster: source rotates during transfer |
| Every replica with a covering snapshot is already serving its limit of transfers | Each answers with a retriable busy error. The receiver retries in follower-first order until a source frees up. | None, unchanged from version 1 |
| Two replicas need a rebuild at once | Each serves its finished snapshot files to the other, whatever the state of its live database. | None, unchanged from version 1 |
| A replica leaves the ISR after reporting a snapshot | The leader drops its report. The log start doesn't advance on that report until the replica rejoins and reports again. | Cluster: report then leave ISR |
| Fewer than `min.insync.replicas` in-sync replicas, the leader included, hold a snapshot at the leader's older-snapshot offset | The leader doesn't advance the log start. Produce and fetch continue. | Cluster: older snapshot ahead of followers |
| The leader is unreachable when a replica sends a snapshot report | The scheduler retries with backoff. Replica fetching continues, and only log-start advancement waits. | None, unchanged from version 1 |
| Leader dies after the log start has moved | Another in-sync replica has reported a snapshot that covers the log start. A new leader whose database is missing rebuilds from that snapshot. | Cluster: new leader with missing database |
| The confirming replica later loses that snapshot, and the new leader's database is also missing | The leader can't rebuild and must not serve. Diskless writes for that coordinator partition's user partitions stay unavailable until a covering snapshot exists. How the leader gives up leadership is an open item. | Cluster: no covering snapshot |
| The topic has one replica | The leader advances the log start from its own older snapshot. There is no other replica to confirm. | Cluster: single-replica topic |
| A replica is down | With three replicas and a minimum ISR of 2, writes continue. | Cluster: failover mid-commit |

### Controller and admin

| Failure | Behavior | Test |
| --- | --- | --- |
| The last known leader returns under the same ID with a wiped, replaced, or reformatted disk | The controller removes it from the ELR and `lastKnownElr` at registration. The partition waits for a replica whose storage survived. | Seam: `ReplicationControlManagerTest` |
| A multi-directory broker restarts cleanly without the directory that held the replica | Same as the preceding row. | Seam: `ReplicationControlManagerTest` |
| Every replica restarts uncleanly with intact storage | They stay in the ELR, and the first one to return wins a clean ELR election with no acknowledged record lost. | Cluster: unclean restart of every replica |
| A broker crashes shortly after it creates a coordinator partition directory | The parent-directory fsync keeps the directory, and recovery finds the log. | Seam: `LogManagerTest` |
| An operator changes the cluster default `min.insync.replicas` while a coordinator partition has no leader | The cluster-wide ELR clear skips `__diskless_metadata`, and the partition keeps its candidates. | Seam: `ReplicationControlManagerTest` and `TopicsDeltaTest` |
| A reassignment changes the replica count, or an alter changes `min.insync.replicas` | The controller rejects it with `INVALID_REPLICATION_FACTOR` or `INVALID_CONFIG`. | Seam: `ReplicationControlManagerTest` and `ConfigurationControlManagerTest` |
| Every ELR member and the last known leader of a coordinator partition have lost their storage | The partition stays offline, and no unclean election is possible. | Seam: `ReplicationControlManagerTest` |
| An admin request or a client produce would delete, create, extend, or weaken the topic | `DeleteTopics`, client produce, and client `DeleteRecords` get `INVALID_TOPIC_EXCEPTION`. Client `CreateTopics` and `UNCLEAN` `ElectLeaders` get `INVALID_REQUEST`. `CreatePartitions` gets `INVALID_PARTITIONS`. A fixed config alter gets `INVALID_CONFIG`. | Cluster: admin protection |

### Accepted risks

| Risk | Why it's accepted | Signal or procedure |
| --- | --- | --- |
| Every replica of a coordinator partition is lost | A classic topic loses data the same way. The blast radius is 1/N of diskless partitions, and their WAL objects become orphans. | Procedure: the deployment docs require a replication factor of 3 across racks or zones. Signal: `OfflinePartitionsCount`. |
| A deterministic apply bug corrupts every replica alike | Kafka's coordinators share this limitation. The replay checker is out of scope. | Signal: a sustained `PartitionStorageErrorRate` on a diskless partition, outside a deposed-leader window or a grace-period race, means coordinator state points into a deleted object, whatever the cause. It catches the harmful effect, not the bug. Prevention: the shared suite, the golden-records test, and the apply-stop rule. |
| Retention and `LogAppendTime` under a skewed leader clock | Classic topics have the same exposure. | Procedure: monitor the NTP offset on every broker, and alert well below half the grace period. |
| Grace-period skew beyond half the grace period | It costs reader availability, not data, and the Postgres backend shares it. | The NTP procedure in the preceding row. |
| Storage completes an upload after its key was deleted | It leaks storage, not data. The bounds are one object per attempt and at most `K` files for a cut-off zombie. The grace period covers uploads that land within a minute of the mark. | Signal: `FileUploadErrorRate`, an upper bound on objects that may have leaked. The report-only scanner stays future work. |
| A non-idempotent producer that retries through another broker sees a late copy after the retried one | A classic `acks=all` timeout and the Postgres backend have the same window. | Signal: `event-purgatory-time-ms` bounds the window. |
| Log recovery truncates fsynced records after a read error or bit rot, under an unchanged directory ID | A classic topic loses data the same way. | Procedure: alert on log recovery's invalid-message truncation warning for `__diskless_metadata`, and stop the broker for inspection instead of letting it rejoin. |
| A device acknowledges an fsync without persisting it | A classic topic loses data the same way. | Procedure: the deployment docs require storage that honors fsync, with no volatile write cache that lacks power-loss protection. |
| An operator restores a log directory, `meta.properties` included, from an older copy | A classic topic loses data the same way. | Procedure: never restore a broker's log directory from a copy. Reformat it and let the broker re-replicate. |
| A stale leader-epoch entry over-truncates an ELR member, and the leader fails before that member fetches again | A classic topic loses data the same way. | Signal: `UncleanLeaderElectionsPerSec` and `ElectionFromEligibleLeaderReplicasPerSec`. No signal catches the over-truncation itself. |

The barrier's thread hold isn't a risk to data, so [Thread hold](#thread-hold) states it as a cost.

## Testing

Good tests check behavior through a public seam. Four layers, in this order:

1. The shared `AbstractControlPlaneTest` suite runs against `DisklessMetadataControlPlane`, as it runs against the in-memory and Postgres backends, at `N=1` and at `N=8`. One broker leads every coordinator partition, so the second run covers request splitting, result merging, tracking parts, and releases. It adds:
   - **Partial send**: part 1 is sent with an unknown outcome and part 2 hits `Unavailable`, and a delegate is replaced between two parts of one file. The broker never deletes the uploaded object directly. The coordinator backend's run also checks that the tracking partition's queue deletes the key after the grace period. The Postgres and in-memory runs check that the direct delete still happens.
   - **Marking rule**: each of the five batch-removing records releases an object whose last batch it removes, and no other record does.
   - The lease-model forms of version 1's commit and retry cases.
2. Coordinator backend tests, for cases that one clock or one state can't express. Every test that involves time runs at `N ≥ 2` and gives the uploader and each coordinator leader its own `MockTime`:
   - **Skewed clocks**: offset every uploader and leader by up to a day, then run commits, idle lease ends, epoch retirement, a failover, and `FileCleaner` cycles with `retention.ms=-1`. Every batch row's object exists in `InMemoryStorage`, and no queued key matches a batch row.
   - **Uploader pause**: hold an upload until the lease ends, the range fence runs, and the grace period deletes the key, then release it and commit. The commit gets stale-sequence, the broker doesn't delete the object, and no batch row points to the key.
   - **Stale `FileCleaner` list**: call `getFilesToDelete` on the old leader, move leadership, let the new leader delete and record its deletes, then let the old leader delete its list. Every key the old leader deletes was in the committed queue and has no batch row.
   - **Stale-read rule**: build an older state and a newer one, with commits, retention, and `DeleteRecords` between them. For `EARLIEST`, `LATEST`, `MAX_TIMESTAMP`, and timestamp lookups, the older answer is lower than or equal to the newer one, or unknown. A future read that breaks the rule fails this test.
   - **Apply stop**: an unknown type, an unknown value version, and an unknown tag at the top level and in a nested struct each leave the applied offset unchanged, raise `ApplyStoppedPartitionCount`, stop serving, and make a leader resign. A tag in a test extension registry applies normally.
   - **Write gate**: for each record type, the leader writes the version that the finalized level allows. A leader with a lagging `MetadataImage` writes the lower version. An Inkless tag appears only at an Inkless feature level that includes it.
   - **SQLite schema**: forward migration from each supported older schema succeeds. A newer schema counts as a missing database and triggers a rebuild. A snapshot fetch above the receiver's schema gets `SNAPSHOT_NOT_FOUND`, and the receiver moves to the next replica.
   - **Golden records**: a checked-in file holds every record type at every level, and the current binary decodes all of it.
3. A multi-broker `KafkaClusterTestKit` test covers these cases:
   - **Failover mid-commit**: leader failover in the middle of a commit, followed by the retry path. The retry resolves through `(epoch, sequence)`, and the client sees one successful produce with no duplicate records.
   - **Truncation**: a leader change that truncates unreplicated records. The follower's state matches the new leader's state, and no applied record was undone.
   - **Retry while pending**: a retry that arrives while its first attempt is pending on the leader. The client sees one record and one set of offsets, for idempotent and non-idempotent producers.
   - **One part fails**: a commit whose parts land on several coordinator partitions, where one part fails. The committed partitions return offsets, the failed ones get `KAFKA_STORAGE_ERROR`, and the tracking partition deletes the object once every participant is released.
   - **Tracking partition down**: the broker leases at the fallback partition, and only that partition's user partitions fail.
   - **Broker stops before and after upload**: a broker that stops before the upload, and one that stops after the upload and before any part commits. The epoch end marks the unused range, and the queue deletes the missing key and the uploaded key after the grace period.
   - **Broker stops after some parts**: the tracking partition fences the silent participant, and it deletes the object only after the committed participants release it.
   - **Object expires on one partition**: an object whose batches expire on one coordinator partition while another still holds batches. The object stays until both are released.
   - **One coordinator partition offline**: produce runs to user partitions on every coordinator partition. Parts on the other coordinator partitions commit without waiting for the offline partition's deadline.
   - **Deadline then next part**: a part reaches its deadline, then the same broker's next commit to that coordinator partition raises the floor. A late request for the earlier part gets stale-sequence, and the later batches commit.
   - **ISR below minimum**: a coordinator partition whose ISR drops below `min.insync.replicas`. Commit parts fail at once rather than at the deadline, and lease renewals fall back to another partition.
   - **Retried mixed results**: a retried part where some batches failed and some were duplicates, sent to a new leader after a failover. The retry returns the same per-batch results as the first attempt.
   - **Restart with part in flight**: the leader rejects the part's old epoch as stale, and the broker-epoch-retired record ends its leases.
   - **Tracking leader change**: a tracking partition changes leader with open entries. The new leader restarts the idle timers, and participants resend releases that weren't accepted.
   - **Idle lease end**: a silent broker's idle lease ends, and its leftover keys are deleted.
   - **Missed sequence**: the part to the tracking partition fails while others commit. A later commit there fences the missed sequence at all `N`, and the object stays until its participants release it.
   - **Mapping mismatch**: a part that names a user partition the receiving coordinator partition doesn't own. The leader rejects it with `INVALID_REQUEST`.
   - **Catch up from snapshot**: a replica catches up from a snapshot after the log start has moved, and serves the same reads as the leader afterward.
   - **New leader with missing database**: it rebuilds from a follower's snapshot, and it serves no request before the rebuild finishes.
   - **No covering snapshot**: a coordinator partition where no replica holds a snapshot covering the log start. Its user partitions stay unavailable for diskless writes, and no partial state is served.
   - **Older snapshot ahead of followers**: the leader's older snapshot is ahead of every other in-sync replica's reported snapshot. The log start stays put. Once a follower reports a snapshot at that offset, the log start advances, and that follower's snapshot still covers it after the leader stops.
   - **Report then leave ISR**: a follower that reports a snapshot and then leaves the ISR. The log start doesn't advance on its report until it rejoins and reports again.
   - **Source rotates during transfer**: a snapshot source that rotates or restarts during a transfer. The receiver never loads a file spliced from two snapshots, and it finishes from the newest covering snapshot.
   - **Follower serves rebuild**: a rebuild while the leader is under commit load. The leader serves no transfer while an in-sync follower holds a covering snapshot.
   - **Database lost before first log start move**: the replica replays from offset 0 and needs no snapshot.
   - **Single-replica topic**: the leader advances the log start from its own older snapshot.
   - **Topic deleted and purged**: every replica drops the same batches and rows as the leader, and the tracking partitions delete the emptied objects.
   - **Topic created while leader down**: a topic created and a topic deleted while a coordinator leader is down. The leader reconciles on return, and a produce before reconciliation gets a retriable error.
   - **Produce in flight during leader change**: the produce fails cleanly or succeeds once.
   - **Consolidated prune floor**: a consolidated topic whose prune floor moves only after the leader confirms it.
   - **Cluster formatted with the feature**: a commit goes through the coordinator. A broker whose class names another backend refuses to start. A broker that doesn't support either finalized feature level can't register.
   - **Admin protection**: the controller creates the topic with the fixed configs and the configured `min.insync.replicas`, `Admin.listTopics()` hides it by default, and every rejection holds end to end through `Admin`: delete, create, `CreatePartitions`, altering `flush.messages`, an unclean election, and an `__admin_client` produce. A `Metadata` request with auto-create on doesn't create the topic.
   - **Deposed leader**: cut B1 off from the controller and its followers, commit through the new leader up to offset 1500, then consume `orders-0` from committed offset 1400 through B1 under `auto.offset.reset=earliest` and under `latest`. No reset moves the position, and the consumer reads from 1400 once B1 learns it was deposed.
   - **Seek past the end**: `seek(highWatermark + 1000)` resets by the consumer's policy.
   - **Upgrade and rollback**: brokers roll to the new binary, the level rises, and a commit goes through the coordinator. A second variant rolls back before the raise, and the rolled-back replicas rebuild and rejoin the ISR.
   - **Unclean restart of every replica**: kill every replica of a coordinator partition uncleanly, restart them with intact disks, and the partition recovers with every acknowledged record.
4. Fault injection covers the leader's high-watermark gate after an election, the `(epoch, sequence)` retry rule, a lost lease reply, a part whose request reaches the leader after its deadline, and a broker stop between lease, upload, and commit, so these behaviors are deterministic to test.

### Seam tests

- On the tracking-partition shard, with `MockPartitionWriter` and `MockCoordinatorTimer`:
  - **Barrier**: the barrier commits with the offset still above the high watermark, and the answer is `OFFSET_OUT_OF_RANGE`. The high watermark passes the offset while the barrier waits, and the fetch returns batches. The high watermark never advances, and the answer after 500 ms is `OFFSET_NOT_AVAILABLE`. The shard unloads, and the answer is `NOT_COORDINATOR`.
  - **Barrier deadline**: a request queued behind slow events still gets 500 ms from its arrival. Two requests that share a barrier each get their own deadline. A commit part that waits longer than 500 ms doesn't time out.
  - **Barrier rate under adaptive linger**: a hot loop above the high watermark appends at most one barrier per flush.
  - **Idle timer against a part**: a part reschedules `lease-idle-<brokerId>`, and when the timer runs first, the participant's committed part stays readable and its key isn't queued. Advancing the leader's `MockTime` past the timeout between two parts covers a forward clock jump. `MockCoordinatorTimer` runs an expired operation inside `poll()`, so it can't hold a fired timer. Kafka's `testTimerCancelledAfterExpiryButBeforeWriteOperation` and `testTimerOverridden` already cover the override rule.
  - **Leader pause**: queue the lease-ended write at the old leader, move leadership, and commit the part at the new leader. No lease-ended record exists. `testTimerIgnoredOnNotCoordinatorException` covers the runtime's side.
  - **Acceptance rules**: a file with no leased sequence, a fence racing a part, and a broker bug's `INVALID_REQUEST`.
- **Delayed-fetch probe**, in `ReplicaManagerTest` or `DelayedFetchTest`: with the cache off, a probe above the high watermark returns at once and appends no barrier.
- Next to each guard:
  - `TopicTest`: `Topic.isInternal("__diskless_metadata")` is true.
  - `ControllerApisTest`: `CreateTopics` gets `INVALID_REQUEST`. `DeleteTopics` by name and by ID gets `INVALID_TOPIC_EXCEPTION`, an unauthorized caller still gets `TOPIC_AUTHORIZATION_FAILED`, and other topics in the same request are deleted.
  - `ReplicationControlManagerTest`: `CreatePartitions` gets `INVALID_PARTITIONS`. `UNCLEAN` `ElectLeaders` gets `INVALID_REQUEST`. A `PREFERRED` election and a reassignment that keeps the replica count succeed, and one that changes it gets `INVALID_REPLICATION_FACTOR`. The last known leader re-registers with a new directory ID and unfencing doesn't elect it, and with its old directory ID, unfencing elects it. A multi-directory broker that restarts cleanly without the replica's directory leaves the ELR. An unclean shutdown keeps the broker in the ELR for `__diskless_metadata` and removes it for a classic topic. A replica whose assigned directory is `UNASSIGNED` leaves the ELR and `lastKnownElr` at registration. After a storage change, a periodic and an admin `PREFERRED` election on the leaderless partition elect nobody. A cluster-wide `ClearElrRecord` keeps the ELR of `__diskless_metadata` and clears a classic topic's.
  - `TopicsDeltaTest`: the same cluster-wide `ClearElrRecord` case on the image side.
  - `ConfigurationControlManagerTest`: each fixed config is rejected under `SET`, `DELETE`, `APPEND` and `SUBTRACT` on `cleanup.policy`, and a legacy `AlterConfigs` that omits it. Its required value passes. Changing `min.insync.replicas` gets `INVALID_CONFIG`, and its current value passes. An unrelated config such as `segment.bytes` alters.
  - `ReplicaManagerTest`: a client produce with client ID `__admin_client` gets `INVALID_TOPIC_EXCEPTION`, an append with `AppendOrigin.COORDINATOR` succeeds, and client `DeleteRecords` gets `INVALID_TOPIC_EXCEPTION`. If `ReplicaManagerTest` can't drive a `COORDINATOR` append without the coordinator runtime, that case moves to the admin protection cluster test.
  - `LogManagerTest`: creating a `__diskless_metadata` partition flushes the log directory, and a failed flush takes the log directory offline.
  - `FeatureControlManagerTest`: both features refuse `SAFE_DOWNGRADE` and `UNSAFE_DOWNGRADE`. An upgrade fails while a registered broker lacks support. The Inkless feature's dependency on the upstream level holds.
  - Controller config validation: `diskless.metadata.topic.min.insync.replicas` outside its range fails startup.
- **Key name function**: the cluster ID keeps two clusters' keys apart, and apply builds the same name as `FileCommitter`.
- **Mapping**: the user-partition mapping and the tracking partition get a unit test with fixed expected values, so a change to either fails the build. The same test checks that the slot mapping equals `Utils.toPositive(Utils.murmur2(logKey)) % N` for every allowed `N`, and that the config rejects an `N` that isn't a power of two.
- **Snapshot unit tests**: snapshot writing, rotation, loading, and chunk serving get unit tests on a temporary directory, with a fault hook at each step of rotation and transfer. They cover a tag read from inside the copy while applies run during the backup, a crash at each rotation step, a transfer whose source rotates or restarts before the receiver resumes, a checksum mismatch on load and on serve, and the schema version in the file name. No public seam reproduces these cases deterministically.
- **Apply rules**: apply rules for file records, broker rows, leases, fences, releases, and the deletion queue get unit tests at the replica state layer, with records appended directly to a test log. No public call can produce a duplicate or conflicting record, so this is the one internal seam, and it is a deliberate exception.

Version 1's tests for a lost intent reply, a part older than the maximum commit age, and a release that keeps a tombstone leave with the owner model. The lost intent reply becomes the lost lease reply in fault injection, and the `CreatePartitions` cluster test joins the admin protection cluster test.

## Terms

These terms are new in version 2, or change meaning from version 1:

- Barrier record: a record with no content that a leader appends to confirm its leadership before it answers `OFFSET_OUT_OF_RANGE` above the high watermark.
- Broker epoch retired: the record a leader appends when its metadata image shows a newer epoch or no registration for a broker. It moves the broker row and ends the old epoch's leases at a fixed log position.
- Broker row: the per-partition state of one uploading broker: its epoch, last committed sequence and that commit's result, its fenced-through pair, and a retired marker.
- Deletion queue: the table of keys waiting for `FileCleaner` at a tracking partition, each with the mark time from its marking record.
- Deposed window: the time a deposed coordinator leader keeps answering reads before it applies the metadata record for the new leader.
- Fenced-through mark: the pair `(e_f, s_f)` in a broker row that a fence raises. No sequence at or below it can commit at that partition.
- Files-deleted record: the record that confirms which queued keys storage deleted.
- Floor: the higher of a broker row's last commit and its fenced-through mark. A new commit needs a pair above it.
- Lease: a range of sequences that a tracking partition grants a broker for one epoch before the broker uses them.
- Leftover sequence: a leased sequence that the broker never committed by the time its lease ended.
- Missed sequence: a sequence in a tracking partition's own ranges that it never recorded while it recorded a later one.
- Range fence: a fence over the unused or missed sequences of a range, sent to every partition.
- Sequence source: the `ControlPlane` component that gives `FileCommitter` the next sequence for an epoch.
- Tracking partition: the coordinator partition that leases a broker's sequences, tracks which partitions reference each of its objects, and deletes them. It replaces version 1's owner.

"Owner," "intent," "tombstone," "predecessor fence," and "maximum commit age" leave the design.

## Future work

None of these items is part of this design. Each one depends on a decision this design makes, and the design must keep that decision intact. The longer-term direction behind them is in the [Diskless north star RFC](https://app.notion.com/p/40eb1ae1752781c399c3d711f92bb209).

### Grow the coordinator by splitting partitions

- With `slot % N` and a power-of-two `N`, doubling `N` moves exactly half of the slots of each coordinator partition `p` to the new partition `p + N`. The IDs of the existing coordinator partitions don't change.
- The growth needs a handoff that moves a slot's state from `p` to `p + N`: its logs, batches, and producer state. It also needs broker rows and tracking entries that stay correct for objects whose batches move, a tracking partition mapping that survives the change of `N`, and a stored slot table once a slot's partition is no longer `slot % N`. Until that exists, the controller keeps rejecting `CreatePartitions` for `__diskless_metadata`.

### Merge WAL objects into extents

- A merged object is a new object with its own sequence and participants. Each participant appends a record that replaces a log's batch rows with extent rows pointing at the merged object, then releases the old objects through the emptiness check and the normal release path.
- This needs a record type for the swap, and a way to turn extent rows into the `BatchInfo` objects that `findBatches` returns. The extent requirement in [Local state](#local-state) keeps the schema ready for it.

### Tiered metadata in the coordinator

- The remote log segment metadata of a diskless user partition would live in the coordinator partition that owns its batches. A composite `RemoteLogMetadataManager` would route diskless partitions to the coordinator and classic partitions to the topic-based implementation, because a broker loads one class from `remote.log.metadata.manager.class.name`.
- WAL pruning and segment registration then become records in one log for that user partition. A lookup goes to the coordinator leader on demand, so brokers don't consume every metadata partition, and the [Snapshots](#snapshots) mechanism bounds the metadata log. These are the RLMM problems that Jun Rao raised in the KIP-1163 discussion (JR1 and JR11).
- This design must keep two constraints for this to work:
  - A user partition's batches and its remote segments map to the same coordinator partition, by the same log key.
  - Pruning and cross-tier offsets stay decided on the broker that hosts the local log and recorded by the leader, as [Consolidation and cross-tier offsets](#consolidation-and-cross-tier-offsets) describes.

### Partitionless key ranges

- A log that covers a key range instead of a fixed partition would get a second log key encoding, carried in a new version of the records that hold the `Log` struct.
- The input that encoding hashes decides whether a range split moves a child range to another coordinator partition. If a parent range and its children map to one coordinator partition, the split is ordered with their commits in one log.

## Open items

The implementer decides these. None of them changes the design.

- Names and numbers of the private request types, the number of each ApiKey, and the error codes for a busy snapshot source, a stale epoch, and a stale sequence.
- The names of the two coordinator features, and the record set and extension tags of each level.
- SQLite driver choice, including its license and packaging check.
- How a leader that can't rebuild its state gives up leadership of that one coordinator partition. The controller has no request that moves one partition's leadership off a broker, and the design needs one. Until then the leader marks only that partition offline and stops serving it. The directory stays online, and the broker doesn't halt.
- The commit deadline default, which must not exceed the produce request timeout. It also sets the runtime's write timeout.
- The idle lease timeout default, 15 minutes and provisional.
- The byte encoding of the broker ID that the tracking-partition hash reads. The fixed-value mapping test pins it.
- The cap on how far the overlay may run ahead of the high watermark.
- The append linger default.

This item can change the design, and it's out of scope for version 2's review:

- The workloads, scales, and pass criteria for the benchmarks in [Capacity and isolation](#capacity-and-isolation). Their results can require a separate SQLite disk, a `findBatches` result cap, a different default partition count, or a different snapshot byte trigger.
