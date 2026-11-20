# Diskless metadata coordinator

This design replaces [version 2](../diskless-metadata-coordinator-v2/design.md).

Status: draft for review.

Source: [KIP-1164: Diskless Coordinator](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783984/KIP-1164+Diskless+Coordinator), scoped down to a third `ControlPlane` implementation. The handoff spec is [spec.md](spec.md).

## Goals

- Add a `ControlPlane` implementation, `DisklessMetadataControlPlane`, selected with `inkless.control.plane.class`.
- Change the shared commit path in `FileCommitter` so that a broker orders its commits per coordinator partition, not across all its files, and completes each produce request once the parts that hold its batches finish. The Postgres and in-memory backends keep today's behavior. [Commit path](#commit-path) describes the change.
- Store the coordinator state in the internal topic `__diskless_metadata`, split across a fixed number of partitions. Each diskless user partition maps to one coordinator partition by a hash.
- Keep every replica's state in a local SQLite database, rebuilt from the log and from snapshots.
- Delete every WAL object that the design can orphan, through the tracking partition of the broker that uploaded it. No clock decides whether a deletion is safe.
- Route each client's produce and fetch to a gateway of its user partition's coordinator partition in the client's rack, so that a file sends fewer commit parts. Routing is a hint: a produce that reaches any broker still commits. [Gateway routing](#gateway-routing) describes it.
- Meet the data-loss bar:
  1. Any failure that a classic topic survives, diskless topics survive too.
  2. A failure that a classic topic doesn't survive can be an accepted risk only if its damage stays within the user partitions of the failed coordinator partition, and it loses no more than a classic topic loses to the same failure. One exception: losing every replica of a coordinator partition is an accepted risk, because the controller enforces a replication factor of 3 and maximal rack spread. Under the exception, the design has no procedure that brings the coordinator partition back. It stays leaderless, and about 1/N of every diskless topic stays unavailable. [Future work](#future-work) records a footer recovery that would meet this clause without the exception, and an empty restart that brings the partition back with no state.
  3. A failure that lets one coordinator partition's loss delete or resurrect another partition's data can't be an accepted risk. It needs a fix or a bound. The accepted risks for a lost tail, an apply bug, and a delete from outside the coordinator are the exception, because a classic topic loses data to the same causes. [Future work](#future-work) records a soft-delete bound that would meet this clause without the exception.
- Meet the performance bar: no worse than the Postgres backend. [Benchmarks](#benchmarks) makes the bar testable with pass criteria. Object-storage requests match the Postgres backend's, because the coordinator uploads the same objects, and routing changes no PUT count.
- Pass the shared `AbstractControlPlaneTest` suite, and the tests in [Testing](#testing).
- Add no public Kafka protocol. The new requests use private ApiKeys in the 504+ range.

## Non-goals

- Changing the partition count of `__diskless_metadata` after the controller creates it. The slot layer keeps it possible, as [Future work](#future-work) describes.
- Moving an object's tracking partition after its commit.
- Orphan-object scanning. A scanner that only reports suspected orphans stays possible as future work.
- An off-broker copy of coordinator state and a restore procedure. [Accepted risks](#accepted-risks) records the loss of every replica of a coordinator partition, and [Future work](#future-work) records the footer recovery, the soft-delete bound, and the empty restart.
- Recovery from losing the KRaft metadata. Classic topics don't survive it either.
- A replay checker and an inspect-and-repair tool for a deterministic apply bug that corrupts every replica alike. [Accepted risks](#accepted-risks) records it.
- Running the benchmarks. This design defines their pass criteria only.
- KIP-1163's `PreferredProduceBrokers` and any public protocol for gateways.
- Moving a user partition between coordinators.
- Transaction state.
- The KIP's public APIs, keys 95 to 101.
- Follower reads.
- Migrating existing Postgres-backed clusters. New clusters only.
- Raft for the metadata quorum.

`__consumer_offsets` and `__transaction_state` split their state by key, because each request touches one group ID or one transactional ID. A WAL object is different: it carries batches for many user partitions, often across topics, and offsets for each user partition must come from one writer. The coordinator splits its state by user partition, so each user partition still has exactly one writer, and a commit goes to every coordinator partition its batches map to. That split needs no two-phase commit. Kafka doesn't make a non-transactional produce atomic across partitions, and `commitFile` already returns one result per batch, so a commit can succeed on one coordinator partition and fail on another. The only state that spans coordinator partitions is the object itself: whether any batch still references it, and whether it was ever committed at all. [Object tracking and deletion](#object-tracking-and-deletion) gives each object one tracking partition, chosen by the broker that uploads it, which tracks this state.

## Topology

- The internal topic `__diskless_metadata` has `diskless.metadata.topic.num.partitions` partitions, 8 by default. The default is a constant and stays provisional until the partition-count benchmark in [Benchmarks](#benchmarks) confirms or replaces it.
  - The default doesn't depend on the broker count. The controller creates the topic once the registered, unfenced brokers reach the replication factor, so on a new cluster it sees about 3 brokers at creation. A derived default would fix every new cluster at its small-cluster `N`, and `N` can't grow after creation.
  - An operator can set a lower power of two at creation. The deployment docs say to choose `N` for the cluster's expected peak size.
- The replication factor is `diskless.metadata.topic.replication.factor`, 3 by default. Controller startup validation rejects a value below 3.
  - An internal controller config, defined with `defineInternal` and left out of the docs, lowers the minimum to 1. Tests and local development use it. When the controller creates the topic with a replication factor below 3, it logs a warning, and the data-loss bar's exception for losing every replica doesn't apply to that cluster.
  - The replication factor is fixed at creation: a reassignment that changes the replica count gets `INVALID_REPLICATION_FACTOR`, and changing the config afterward has no effect.
- The controller places each coordinator partition's replicas with maximal spread: across `min(replication factor, number of distinct racks among registered brokers)` racks. With 3 or more racks, that's one replica per rack. With 2 racks, both are used. With no rack declared, the rule reduces to distinct brokers.
  - `StripedReplicaPlacer` already produces maximal spread. It puts replicas on racks round-robin from a random starting rack, and a broker without `broker.rack` counts as one more rack. Creation goes ahead with any number of racks, and when there are fewer racks than the replication factor, the controller logs a warning.
  - A reassignment of a `__diskless_metadata` partition gets `INVALID_REPLICA_ASSIGNMENT` when its target replica set spans fewer racks than the current set and fewer than maximal spread. A move that keeps or widens the spread passes, including a move within a rack to decommission a broker. [Topic protection](#topic-protection) lists the check.
  - A broker that re-registers with another rack, and a rack that registers after creation, can leave a partition below maximal spread with no request to reject. The controller gauge `CoordinatorPartitionsBelowRackSpread` counts the partitions below maximal spread, and a reassignment that restores the spread is the procedure.
- The topic-level `min.insync.replicas` comes from the controller config `diskless.metadata.topic.min.insync.replicas`. It defaults to `min(2, diskless.metadata.topic.replication.factor)`, and startup validation requires it to be between that default and the replication factor. At the minimum replication factor of 3, its lower bound is 2. The controller sets it at creation, and changing the config afterward has no effect. An alter of the topic's `min.insync.replicas` gets `INVALID_CONFIG`, as [Topic protection](#topic-protection) describes. A fixed value of 2 would fail every append at a replication factor of 1.
- The active controller creates the topic, with `unclean.leader.election.enable=false`, once it sees at least as many registered, unfenced brokers as the replication factor. It checks on each broker registration and unfencing, and once after it becomes active, until creation succeeds. A coordinator call before then gets a retriable error. If the first brokers to register share a rack, creation places every replica on that rack, and `CoordinatorPartitionsBelowRackSpread` rises once the other racks register. The deployment docs say to start the first brokers across racks.
- The smallest supported cluster has 3 brokers. Below 3, the controller never creates the topic, so the coordinator never starts. On a 3-broker cluster, an operator can't decommission a broker without adding a replacement first, because the replica count is fixed, and the deployment docs state this constraint.
- The leader of each coordinator partition is the single writer for the diskless user partitions that map to it. [Coordinator partitions](#coordinator-partitions) defines the mapping. In the rest of this document, "the leader" means the leader of the coordinator partition that the user partition, call, or object under discussion maps to.
- Every replica fetches its coordinator partitions with normal replica fetching. Replicas don't vote.
- The coordinator appends to the topic with `acks=-1`. `Partition.appendRecordsToLeader` then rejects the append with `NOT_ENOUGH_REPLICAS` whenever the ISR is below `min.insync.replicas`, before the leader writes the record to its log. The high watermark doesn't advance under the minimum ISR either, so an acknowledged record is always on at least `min.insync.replicas` replicas. [Write path](#write-path) names the writer that does this.
- Unclean leader election stays off, so a replica outside the ISR never becomes leader and truncates acknowledged records. The controller's automatic last-known-leader election stays on, with the guards in [Election of a coordinator leader](#election-of-a-coordinator-leader).
- The topic is created with `retention.ms=-1` and `retention.bytes=-1`. Time and size retention never delete a segment, so the snapshot rule in [Snapshots](#snapshots) is the only thing that moves the log start.
- After `LogManager.createLogDirectory` creates a `__diskless_metadata` partition directory, the broker calls `Utils.flushDir` on the log directory. A failed fsync takes the log directory offline, as a failed create does. Renames already fsync the parent through `Utils.atomicMoveWithFallback`. The cost is one fsync per partition directory creation, none on the produce path.

### Flush policy

The topic requires `flush.messages=1`, unlike other Kafka logs. A replica fsyncs every batch it appends, so the record a committed reply points to survives a power loss on every replica at once. The coordinator offers no weaker flush mode, neither as a named unsafe mode nor as a topic config override. [Topic protection](#topic-protection) rejects any alter of `flush.messages`.

On a classic topic, a correlated power loss across the ISR truncates only the unflushed tail of that partition. Here, a broker's objects span their tracking partition, the participant partitions that hold their batches, and object storage, which doesn't roll back when brokers lose power. An unflushed tail breaks invariants across those components and causes corrupted reads, which classic topics don't have:

- Release inversion: a participant applies a batch-removing record and reports the release, and the tracking partition queues the object. If the tracking partition's record reaches disk and the participant's doesn't, the participant gets its batch rows back after the power loss, while `FileCleaner` on the tracking partition deletes the object. Fetches for those offsets fail with `KeyNotFoundException`. No clock skew is needed.
- A lost `sequences-fenced` tail at a participant lowers its floor again. A late part for the fenced sequence then commits into a key that the tracking partition already queued for deletion.
- A lost `sequences-leased` tail at a tracking partition leaves a broker using a range that the partition doesn't know. The participants commit their parts, the tracking partition rejects its own, and nothing ever tracks the object.
- A lost file-committed tail reuses offsets across every diskless partition mapped to that coordinator partition, 1/N of the cluster. Brokers that stayed up still cache the old coordinates for those offsets, so they serve different records for the same offset until the cache entries expire. A lost diskless-log-initialized tail leaves a switched partition uninitialized until a manual `repairDisklessLog`.

One file costs one coordinator log flush per part on the leader and one on each follower, so up to `parts × R` flushes, where `R` is the replication factor and `parts` counts the coordinator partitions the file sends a part to, the tracking partition included. That's at most `N × R`, 24 at `N=8` and `R=3`. One flush is one `LogSegment.flush`: an `fsync` of the segment, an `msync` and an `fsync` per index, and an `fsync` of the transaction index if it exists. `LogFlushRateAndTimeMs` measures it.

Every flush is synchronous. The leader flushes on the `CoordinatorRuntime` thread inside the log lock, and each follower flushes on its replica fetcher thread before the fetch that advances the high watermark, so a commit waits for two flushes in series. The leader's flush matches a Postgres commit's WAL flush on the primary. The follower's flush is a second sequential flush on every commit, which Postgres matches only with a synchronous standby. The design counts it as the price of Kafka parity, not as unnecessary blocking, and its latency is a benchmark question. The KRaft metadata log fsyncs every batch for the same reason.

The append linger defaults to adaptive, as `group.coordinator.append.linger.ms=-1` does for the group coordinator. `CoordinatorRuntime` then shares an append only between writes that queue while the shard is busy, so a write at an idle shard gets its own append, and at moderate load each part gets its own flush. The static broker config `diskless.metadata.append.linger.ms`, default `-1`, sets a fixed linger instead. A fixed linger bounds appends per shard to one per linger and adds up to that much latency to every part. It saves little on a small cluster: brokers' commit timers run out of step, so at 3 brokers a shard receives about 12 parts per second, about 80 ms apart, and a full merge cuts flushes by at most a factor of the broker count.

Per commit round, with one file per broker and every file touching every coordinator partition, each broker flushes between `N × R / B` and `N × R` times, where `B` is the broker count. At `B=3`, `N=8`, and `R=3`, every broker hosts all 8 coordinator partitions and flushes 8 to 24 times per round, 32 to 96 times per second at the 250 ms commit interval. Gateway routing cuts the parts per file from 2 brokers per rack up, as [Gateway routing](#gateway-routing) counts. With gateways on, each coordinator leader receives parts only from its gateways.

## Coordinator partitions

- A diskless user partition maps to a slot, and the slot maps to a coordinator partition. The slot is `Utils.toPositive(Utils.murmur2(logKey)) % S`, where `S` is 4096, a protocol constant. The coordinator partition is `slot % N`, where `N` is the partition count of `__diskless_metadata`.
- The log key names the log a state change belongs to. The log key of a Kafka partition is the 16 bytes of the topic ID, most significant bits first, followed by the partition index as 4 big-endian bytes. That is the only encoding this design defines.
- `diskless.metadata.topic.num.partitions` accepts only a power of two from 1 to `S`. With such an `N`, the two steps give the same result as `Utils.toPositive(Utils.murmur2(logKey)) % N`, so the slot layer adds no runtime cost and no stored state.
- The slot layer exists so that `N` can grow later without remapping every user partition. When `N` doubles, coordinator partition `p` keeps half of its slots, and the new partition `p + N` takes the other half. [Future work](#future-work) describes what that growth still needs.
- The log key includes the partition index, so the partitions of one topic spread across coordinator partitions, and a busy topic doesn't load a single leader.
- Every broker reads `N` from the metadata image, as `BrokerMetadataPublisher` passes the `__consumer_offsets` partition count from the metadata cache to the group coordinator. The config sets `N` only when the controller creates the topic. A call made before the topic exists gets a retriable error.
- `N` never changes after creation. A different `N` would map user partitions to other coordinator partitions, and moving a user partition between coordinators is out of scope. The controller rejects `CreatePartitions` for `__diskless_metadata`, with the other guards in [Topic protection](#topic-protection).
- A broker chooses its tracking partition locally, from its gateway assignment, as [Tracking partition](#tracking-partition) describes. Every part names the tracking partition, so no other broker computes it.
- The mapping functions, the log key encoding, and `S` are part of the protocol. Every broker version must compute the same coordinator partition for a user partition, so the design fixes them and a test pins them with fixed expected values.
- Each leader recomputes the mapping for every user partition in a request it receives. If a user partition doesn't map to that coordinator partition, the leader rejects the request with `INVALID_REQUEST` and counts the mismatch. A broker that maps differently would otherwise give a user partition a second writer.
- Each coordinator partition is an independent coordinator, with its own log, its own SQLite database on each replica, its own snapshots, its own overlay, and its own leader. Kafka's normal leader placement spreads the leaders across brokers.
- With `N=1`, every user partition maps to partition 0, every broker tracks its objects there, and the design reduces to a single coordinator.

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
| `AlterPartitionReassignments` that narrows the rack spread below the current set and below maximal spread | `ReplicationControlManager.alterPartitionReassignments`, after `rfChangeAllowedForTopic` | `INVALID_REPLICA_ASSIGNMENT` |

- `DeleteTopics` departs from upstream, which lets a principal with `DELETE` remove `__consumer_offsets`. The check fails only this topic, so other topics in the same request proceed. Authorization runs first, so an unauthorized caller learns nothing new.
- The produce check closes the `__admin_client` bypass in `KafkaApis.handleProduceRequest`, which any client can use. A foreign record would apply on every replica and take an offset the leader's overlay doesn't expect. `WriteTxnMarkers` also appends with `COORDINATOR` origin. The check doesn't cover it, because it needs `CLUSTER_ACTION`, which is broker-level trust.
- The `CreateTopics` check stops a client or auto-create from creating the topic first with broker defaults and the wrong partition count. The controller's own creation goes to `ReplicationControlManager` through the `QuorumController`, not through `ControllerApis`, so the check doesn't block it.
- The admin `UNCLEAN` election path never reads `unclean.leader.election.enable`, so the fixed config alone doesn't close it. `PREFERRED` elections and reassignments that keep the replica count and the rack spread stay unchanged: both keep acknowledged records.
- `ReplicationControlManager` never checks racks on a reassignment upstream, and a broker's rack comes from each registration. KRaft doesn't reassign partitions on its own, and `handleAssignReplicasToDirs` moves a replica only between directories of one broker, so the reassignment API is the only hook the rack check needs.

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
- Every private RPC that the delegate retries waits between attempts with Kafka's `ExponentialBackoff`: 100 ms initial, multiplier 2, 1000 ms maximum, and 0.2 jitter. These are constants that match the client defaults. The backoff resets after a success. It covers commit parts, lease requests, lease releases, release reports, and fences. The delegate doesn't retry read parts, as [Reads](#reads) describes.
- The RPCs carry the arguments of the `ControlPlane` methods that have remote callers, plus the requests that [Object tracking and deletion](#object-tracking-and-deletion) adds. The table in [Private RPCs](#private-rpcs) lists them.
- The Produce response can't carry the offsets that a commit assigns, so the commit needs its own response. That is why the RPCs exist.

## Log records

The leader appends one outcome record per committed call, or per part of a call that spans coordinator partitions. Every replica applies records to SQLite in log order, and only up to the high watermark. Each record carries the state change and the assigned values, so every replica reaches the same state without recomputing anything.

The record types follow the KIP's record set, and the implementation may merge or split them:

| Record | Written by | Content |
| --- | --- | --- |
| Sequences leased | Tracking partition, on a lease request for a new range | Broker ID, broker epoch, the range `[a, b]`, the uploader's object key prefix, and the finish report the request carried |
| File committed | Commit part, one per participant and one at the tracking partition | Object key, format, size, uploader broker ID, broker epoch, sequence, tracking partition, participant set, this partition's committed batches with their assigned offsets and producer fields, the outcome of each other batch in the part, and, at the tracking partition, the finish report the tracking part carried |
| Finish reported | Tracking partition, on a keepalive that brings a newer finish report than it has recorded | Broker ID, broker epoch, `finishedThrough`, and the exception ranges |
| Sequences fenced | Participant, on a fence request | `(brokerId, brokerEpoch)` groups, each with the sequence ranges whose highest end raises that broker's fenced-through mark |
| Participant state recorded | Tracking partition, on a release report or a fence answer | The answering participant, and `(brokerId, brokerEpoch)` groups, each with sequence ranges and each range's state: referenced or released |
| Release accepted | Participant, after the tracking partition accepts its release report | The report's `(brokerId, brokerEpoch)` groups and sequence ranges |
| Lease ended | Tracking partition, when a broker's idle timer fires or a lease release arrives | Broker ID, broker epoch, and, for a lease release, the range end `b` and the final finish report |
| Broker epoch retired | Any leader, when its metadata image shows a newer epoch or no registration | Broker ID and `retiredThrough` |
| Files deleted | `FileCleaner` on the tracking partition's leader | The keys storage confirmed |
| Barrier | Any leader, for `findBatches` above the high watermark | Nothing |
| Partition created | create-partitions | Topic ID, name, partition index, and `metadataOffset`, the image's last contained offset when the leader appended the record |
| Topic deleted | delete-topics | Topic ID |
| Records deleted | `deleteRecords` | Topic partition, new log start offset |
| Diskless log initialized | `initDisklessLog` | Topic partition, log start, diskless start, producer states |
| Diskless log repaired | `repairDisklessLog` | Topic partition, diskless start |
| Retention applied | `enforceRetention` | Topic partition, batches deleted, new log start |
| Log pruned | `pruneDisklessLogs` | Topic partition, new diskless log start |
| Cross-tier start advanced | `advanceCrossTierLogStartOffset` | Topic partition, remote log start offset |
| Deleted log purged | topic purge | Topic partition, optional boundary offset, whether the partition is now empty |

The records use the message-JSON conventions that Kafka uses for its other records, so their schemas can be versioned. They carry no tagged fields. Every schema change is a new version or a new type, gated by the coordinator feature's level, as [Record compatibility](#record-compatibility) describes.

Every record names the log it changes with a `Log` struct that holds `TopicId` and `PartitionIndex`, not with a bare topic-partition field. The struct is the record form of the log key. A second log key encoding would be a new record version.

Every change to a diskless partition's coordinator state is a record, including its creation and deletion. KRaft decides that a topic exists. The coordinator record fixes the position of the coordinator's own state change among the commits. Apply reads nothing outside its own coordinator partition's log and the replica's own cluster ID, and it never reads a clock. A tracking partition's view of other participants comes from records in its own log, and every time-based decision is a record the leader appends, such as lease ended. The same log therefore produces the same state on every replica, and a snapshot's tag fully describes its content. If a replica instead removed a topic's rows when its own `MetadataImage` dropped the topic, each replica would remove them at a different log position.

### Record compatibility

A replica never skips a record it can't read. Every coordinator record is a delta that no later record overwrites, so a skipped files-deleted or retention-applied record would leave rows that the other replicas dropped, a permanent and silent divergence. Kafka's `CoordinatorLoaderImpl` skips an unknown record type, which is safe there only because group and offset records are keyed snapshots of state.

A replica that meets an unknown record type, an unknown value version, or an unrecognized tagged field, at load or during live apply, stops applying that coordinator partition at that offset:

- The applier rolls back to that record's savepoint and commits its group up to the record before it, so SQLite stays at the last applied offset. [Local state](#local-state) describes the groups.
- It stops serving reads and commits for the partition, and a leader resigns after it replies to the records before the stop.
- It keeps fetching bytes, because replication doesn't parse records, so it stays in the ISR.
- `ApplyStoppedPartitionCount` and an error log report it. The log names the partition, offset, record type, version, and tag.

Leader epochs don't address this case. Below the high watermark, every batch was written by the leader of its epoch. The unreadable record is valid, and the reader's binary is older than the writer's.

The coordinator feature's level gates writes, as [Rollout](#rollout) describes. The leader reads the finalized level from its `MetadataImage` when it builds each record, and writes the highest version the level allows. A leader whose image lags writes an older version for a while, which every replica reads. With the write gate and the controller's downgrade refusal, a replica stops applying only through a bug or a manual binary downgrade.

Generated readers collect an unknown tag into `unknownTaggedFields()` and read the rest, so a tag that an old reader skipped would change apply silently. Apply checks the `unknownTaggedFields()` of each record and each nested struct against a registry of recognized extension tags. The registry is empty upstream, so any tag stops the replica. Inkless registers its extension tags at 100 and up, encoded by hand as `RawTaggedField` values, following the convention in `docs/inkless/CLASSIC_TO_DISKLESS_SWITCH.md`. The schema JSON stays identical to upstream, so syncs don't conflict. A vanilla build can't read an Inkless coordinator log that carries extension tags. It stops instead of dropping the extension's state.

## Speculative state

The leader keeps an in-memory overlay of records it has appended and not yet applied. The overlay runs from the offset after the applied offset through the last written offset. Followers keep no overlay.

- An overlay entry holds the object key, the uploader's broker ID, broker epoch, and sequence, the assigned offsets, the outcome of each batch that didn't commit, the producer epoch and sequence numbers, and a pending lease or fence when the entry is one.
- The leader checks and reserves on one thread. A commit reads SQLite plus every earlier overlay entry, so two commits can't receive the same offsets, and a broker's newest pending entry decides its acceptance rules.
- The leader appends the reservations that the append linger collects as one log batch, as [Flush policy](#flush-policy) describes. One replication round trip covers the batch.
- When the high watermark passes an entry, the leader applies it to SQLite, removes it from the overlay, and replies.
- If an append fails, the leader resigns, or the log truncates an entry, the leader removes that entry and every entry after it. It fails their parked replies with a retriable error. A later entry took its offsets and sequence numbers from an earlier entry, so the leader keeps no suffix past a broken entry.
- The overlay holds at most `4 × B` entries, where `B` is the number of registered brokers in the leader's metadata image, read at each check. The cap grows with the cluster, and the factor leaves room for one commit part, one lease request, and the fence and release records of each broker. An entry count is enough, because a part's record holds batch coordinates, and `inkless.produce.buffer.max.bytes` bounds the file behind it.
- Past the cap, the leader rejects a new request with `COORDINATOR_NOT_AVAILABLE` and records nothing, and the delegate retries it with the backoff. The in-flight limits in [Load bounds](#load-bounds) keep the overlay below the cap, so it trips only on a bug or a stalled high watermark, and `OverlayCapRejectRate` counts each rejection.
- A new leader starts with an empty overlay. The overlay fills from the first commit after the high-watermark gate opens.

Rejected alternatives: a byte cap, as a config or a constant, and a fixed entry-count config. Either can be wrong for the cluster's size.

## Write path

The leader runs this loop on `CoordinatorRuntime`, the runtime the group coordinator and the share coordinator already use. The runtime supplies the single writer thread, the append linger, the revert to the last committed snapshot after a failed append, the parked replies that complete when the high watermark moves, the `CoordinatorTimer` for the idle lease timers, and the `NOT_COORDINATOR` failure on resignation.

`CoordinatorRuntime` already runs one shard per partition of a coordinator topic, as the group coordinator does for `__consumer_offsets`. Each coordinator partition is one shard, loaded and unloaded with its leadership.

The runtime writes through a `PartitionWriter` that Inkless owns, not through the upstream `CoordinatorPartitionWriter`. The upstream writer appends with `acks=1`, and the group and share coordinators keep using it unchanged. With `acks=1`, a leader under its minimum ISR would still write the record to its log and park the reply until the operation times out. The Inkless writer appends with `acks=-1`, so the append fails at once, the runtime reverts the batch, and the caller gets a retriable error it can act on, such as renewing its lease at another partition.

The runtime's write timeout equals the commit deadline, so a commit part never waits on the server longer than the broker waits for it. The barrier in [Reads](#reads) keeps this write timeout and takes its own 500 ms deadline from a timer, so upstream `CoordinatorRuntime` doesn't change.

The shard stores the overlay in timeline structures backed by a `SnapshotRegistry`. Committed state stays in SQLite. Three pieces stay outside the runtime:

- A custom `CoordinatorLoader` replays from the applied offset stored in SQLite. The stock loader starts at the log start and would replay records SQLite already holds.
- A follower applier writes fetched records to SQLite up to the high watermark. The runtime runs on the leader only. One follower-applier thread per broker serves every coordinator database that the broker follows. The replica fetcher only appends bytes and moves the high watermark, and the applier reads from the local log up to the high watermark and applies in groups, as [Local state](#local-state) describes. Without checkpoints on the apply thread, an apply syncs only at a WAL restart, so one thread is enough.
- The leader's read pool answers `findBatches` and `listOffsets` from committed state, as [Reads](#reads) describes. Only a barrier enters the shard.

A follower whose applier falls behind stays in the ISR, because replication doesn't wait for the apply. If it becomes leader, the high-watermark gate in [Leader change](#leader-change) holds commits until it catches up. `ApplyLagBytes` shows the lag.

Rejected alternative: a write loop written for this coordinator. It would reimplement batching, revert, parked replies, timers, and resignation handling.

Rejected alternative: the full state in `CoordinatorRuntime` timeline maps. Every election would replay the whole log, and `findBatches` and `listOffsets` would scan indexes held on the heap.

Rejected alternatives for the follower applier:

- Applying on the replica fetcher thread. SQLite writes then delay the next fetch for every partition from that leader, classic ones included, and a slow fetcher drops the follower from the ISR.
- Running follower applies on the `CoordinatorRuntime` threads. They compete with the leader shards' commits, and the runtime would have to load shards for followers.

## Commit and retries

Every upload carries a pair of the uploader's broker epoch and a sequence number, and the coordinator treats the uploading broker as an idempotent producer. A broker leases ranges of sequences from its tracking partition ahead of time, off the produce path, so a file needs no round trip before its upload. The pair names the object key, deduplicates retries, and orders a broker's commits at each coordinator partition. A higher pair makes every lower pair from that broker impossible to commit there.

### Sequence source

- At file close, `FileCommitter.commit()` gives each non-empty file the current broker epoch and the next sequence, under its existing lock. It takes the sequence from `ControlPlane.sequenceSource()`, which returns a `SequenceSource` with one call: `OptionalLong next(long brokerEpoch)`. An empty result fails the file at once with `KAFKA_STORAGE_ERROR`, a retriable error, and the file isn't uploaded.
- `FileCommitter` gets the broker epoch from a supplier wired from `BrokerLifecycleManager`.
- "Higher" compares the pair (broker epoch, sequence), epoch first. A broker can get a new epoch without a restart: `BrokerRegistrationTracker` makes `BrokerLifecycleManager` register again when the metadata version moves to one that supports JBOD. The pair order covers that case and any other new registration.
- The sequence is an `int64` that starts at 0 in each broker epoch. The broker increments it with `Math.incrementExact`, and a leader rejects a negative sequence with `INVALID_REQUEST`. Overflow is unreachable in practice, and the code fails instead of wrapping.
- Gaps are allowed. A failed upload, a `ControlPlaneUnavailableException`, and an empty file consume no commit, and no partition requires `last + 1`.
- Nothing about the sequence is persisted. A restart registers a new, higher epoch, so the restarted broker's pairs are higher than every pair from before the restart.
- The default source is a counter that restarts at 0 on a new epoch and never returns empty. The Postgres and in-memory backends use it and behave as today. It has no tracking partition, so none of the lease, finish-report, or tracking rules apply to those backends.
- The coordinator backend's source holds the broker's leased ranges, hands sequences out in increasing order, keeps and renews its leases as [Leases](#leases) describes, and returns empty when nothing is leased. It takes the order in which it asks tracking partitions from the tracking selection interface in [Tracking partition](#tracking-partition). After a stale-sequence reply it moves above the floor in the reply and renews. After a stale-epoch reply it drops its ranges until the broker has a new epoch.

The sequence must be ordered, so it can't be a UUID. Order lets each partition keep one floor per broker in place of a set of keys, find missed sequences, and decide both without a clock.

### Object keys

- The object name is `<clusterId>.<brokerId>.<brokerEpoch>.<sequence>`, in decimal with no padding, under the existing `inkless.object.key.prefix` and masked-prefix handling. `ObjectKey.Path` rejects a name with `/`, so the name stays one segment.
- The cluster ID keeps two clusters that share a bucket and prefix from overwriting or deleting each other's keys. KRaft never reuses a broker epoch within a cluster. The cluster ID is URL-safe base64, which never contains `.`.
- `FileCommitter` builds the key when it assigns the pair at file close and passes it to `FileUploadJob`, which no longer calls `Uuid.randomUuid()`. Every backend uses the deterministic key. Postgres stores keys as opaque strings, so existing UUID keys still read and delete.
- `FileCommitter` and the backend build names with one shared function. The backend builds names and never parses them, so it doesn't depend on the key format or on the prefix handling in `ObjectKeyCreator`.
- Upload retries are harmless. `FileUploadJob` reuses one key for every attempt, and every backend overwrites unconditionally: S3 `PutObject`, GCS `create` and `createFrom` with no generation precondition, Azure `getBlobOutputStream` with no request conditions, and the in-memory map.
- Sequential names at the target commit rate across many broker IDs stay below the GCS hotspot threshold of about 1,000 writes per second.

### Leases

A broker holds its sequences in leased ranges at tracking partitions. A live broker never relies on the tracking partition's idle timer: it keeps its lease with keepalives, switches at once to a range at a new partition, and ends every range it abandons with a lease release that carries its final finish report. The idle timer in [Ending a lease](#ending-a-lease) is a backstop for brokers that are cut off or paused.

Requests and ranges:

- The broker asks a tracking partition `t` for a range `[a, b]` at its current epoch. It picks `a` above every range it requested in that epoch at any partition, including requests that got no reply, so ranges at several partitions never overlap, and a new range at a partition always starts above the end of any range the broker released there.
- The tracking partition rejects a range that is longer than `K`, or whose `a` is at or below its own highest range end for that epoch, with `INVALID_REQUEST`. A request for a range it already holds is a retry or a keepalive: it confirms the range and appends nothing, unless it carries a newer finish report, which appends a finish-reported record. Otherwise it appends a sequences-leased record and replies once the record applies, so a confirmed lease survives a failover.
- The lease reply carries the leader's idle timeout, so a broker whose config value differs still keeps its lease alive in time.
- `K` is the protocol constant 1024, not a config. At the default `produce.commit.interval.ms` of 250 ms, a range lasts about 4 minutes, and about 85 seconds when the produce buffer fills at about 12 files per second. A lease end costs up to `K` deletes, mostly of missing keys, and deletes are free on S3, GCS, and Azure. The bound also limits the leftovers that a broker bug can cause.
- The lease request and the sequences-leased record carry the uploader's `inkless.object.key.prefix`, and the tracking partition's lease table stores it per broker epoch. The prefix is static, so it changes only with a restart, which is a new epoch.
- Every lease request, keepalive, and lease release carries the broker's current finish report for its ranges at that partition, as [Finish reports](#finish-reports) describes.
- Every commit part names the tracking partition that issued its sequence's range, so one broker epoch can hold leases at several tracking partitions.

Renewals:

- The broker asks for its first lease when `FileCommitter` starts and when the broker epoch changes.
- A renewal walks the ordered list that the tracking selection interface returns, from the top: the preferred tracking partition, then the other members of the broker's gateway set by score, then every other coordinator partition by the broker's score. [Tracking partition](#tracking-partition) defines the list. Only one renewal runs at a time.
- Four events start a renewal, and `LeaseRenewalRate` counts each by its reason:
  - `threshold`: fewer than `K/2` leased sequences remain. The broker renews in the background.
  - `keepalive`: a keepalive got no answer.
  - `failed-tracking-part`: a tracking part finished without a leader answer, that is, with `NOT_COORDINATOR` or retriable errors until the deadline, or with an unknown outcome. The broker renews at once. It doesn't renew on the first `NOT_COORDINATOR`, because routine leader elections would then cause fallbacks.
  - `assignment`: the tracking selection's first entry changed, and the broker closed a file with no batch for its current tracking partition, as [Tracking partition](#tracking-partition) describes.
- A renewal confirmed at the partition in use keeps the order of sequences: the broker uses the remainder of its current range first. Abandoning the remainder would turn up to `K/2` sequences into missed sequences on every renewal, which costs `N` range fences each time.
- When the broker confirms a range at a partition other than the one it uses, it switches to that range at once and abandons its ranges at the old partition, including any range it requested there without a confirmation. The broker never sends a sequence from an abandoned range, so a lease end or a release never cuts a range in use. When a later renewal reaches the old partition again, the broker switches back the same way. `LeaseFallbackRate` counts renewals confirmed below the list's first entry.
- When its tracking partition is unavailable, only the user partitions that map to that partition fail. Without the fallback, an unavailable partition would stop all diskless produce on every broker that tracks there once their leases ran out.

Keepalives:

- If the broker has had no answer from the tracking partition it uses for half the idle timeout from the lease reply, it sends a keepalive there: a lease request for a range it already holds. It measures the time on its monotonic clock, from the send time of the last request that the tracking partition's leader answered.
- The tracking partition treats the keepalive as a retry: it confirms the range and reschedules the broker's idle timer. A live broker's lease therefore never ends idle, and the first file after a quiet period commits with no wait.
- The keepalive goes only to the partition in use. If it gets no answer, the broker treats it as a failed renewal and walks the ordered list.
- If no tracking part carries an urgent finish report within one `produce.commit.interval.ms` after it becomes urgent, the broker sends the keepalive early to carry it, and `FinishReportFlushRate` counts it. [Finish reports](#finish-reports) defines an urgent report. A busy broker never flushes early, because its next tracking part carries the report.

Lease releases:

- Once the broker abandons its ranges at a partition, it sends a lease release there when two conditions hold: every file of the broker epoch with a sequence at or below `b` is finished, and no planned sequence is at or below `b`. Here `b` is the highest range end it gives up at that partition, and a planned sequence is one in a range the broker still uses that no file has taken yet. A requested range that never got a confirmation is released too, scoped to its requested `b`.
- The release names `b` and carries the final finish report for the abandoned ranges. The broker retries it with the backoff until the partition answers.
- The tracking partition appends a lease-ended record with `b` and the report. Its apply raises the tracking partition's own fenced-through pair for the broker to `b`, ends the ranges that end at or below `b`, and leaves any higher range live. The leader then sends the lease-end range fences from the sequence after the reported `finishedThrough` to `b`, as [Fences](#fences) describes.
- A repeated or late release is idempotent. Once the floor is at `b`, the tracking partition appends nothing and confirms. A new range at the same partition always starts above `b`, so a release delayed until the broker has returned to that partition never cuts the new range.
- The planned-sequence condition matters when a renewal gets no answer. The release's lease-end fence raises each participant's single floor for the broker to `b`, so it would make stale any sequence below `b` that the broker still plans to use in a live range at another partition.

Rejected alternatives:

- A configurable or adaptive `K`. Nobody would tune the config, and adapting only saves free deletes.
- Making a file wait for a renewal when nothing is leased. It needs a queue of closed files without a sequence, and it only helps the first lease at startup. The keepalive makes the wait unnecessary after a quiet period.
- Deleting leftovers by listing a retired epoch's key prefix in storage. It brings back a storage scan, and it can't end an idle lease within a live epoch.
- Ordering on the broker, by not renewing while a release is unconfirmed. It blocks the return to a partition the broker released, and a request that waits in the leader's queue breaks it anyway.
- Fencing an abandoned range only when its idle timer fires. Below about 0.6 files per second, a fallback range above the remainder in use gets no parts until the broker reaches it, so its timer fires first, and its range fence raises every floor past the live range.

### Commit path

`FileCommitter` commits on a single thread today, which keeps the files of a broker in commit order. Only the order within a user partition matters, and on this backend that means the order within a coordinator partition. With one thread, a part that waits on an unavailable or slow coordinator partition, or on a new leader that rebuilds its database, holds back every later file of the broker. Without gateway routing, most files hold batches for most coordinator partitions, so one coordinator partition would stall all diskless produce on every broker.

This design changes the caller as well as adding a backend. `FileCommitter` and `ControlPlane` change as follows:

- `ControlPlane` maps each user partition to a commit queue. The new backend maps it to its coordinator partition. The Postgres and in-memory backends map every user partition to one queue, which keeps today's order.
- `ControlPlane` gets a per-part commit call that commits the batches of one file that map to one queue, with the broker epoch, the sequence, and the object key. The Postgres and in-memory backends call `commitFile` for it, because their one queue contains the whole file.
- `ControlPlane` gets `sequenceSource()`, as [Sequence source](#sequence-source) describes, and a capability that turns off the broker's direct delete, as [Deletes by the broker](#deletes-by-the-broker) describes.
- `FileCommitter` keeps one ordered queue per commit queue, in file rotation order. The part of file `F2` on a queue runs once `F2` is uploaded and the part of the previous file on the same queue has finished. It doesn't wait for parts on other queues. A broker's queue for a partition therefore delivers that broker's sequences in order, with gaps for files that hold no batch for the partition.
- Every file also sends a part to its tracking partition. That tracking part carries the participant set and the broker's finish report, and it's empty when the tracking partition holds none of the file's batches. Under gateway routing, the tracking partition is one of the broker's gateway coordinator partitions, so a routed file's tracking part usually holds batches, as [Gateway routing](#gateway-routing) describes.
- Every part of a file shares the file's commit deadline, measured from the moment the file closes. The deadline doesn't exceed the produce request timeout. Parts queued behind a stuck part each expire at their own file's deadline, so a stuck coordinator partition delays each queued part by at most one deadline, not one deadline per earlier file.
- The backend retries each part inside its per-part call until the deadline, with the backoff in [Transport](#transport), so the queue starts the next part only after the earlier one has finished. `FileCommitJob` waits for the upload before it commits, so no lower sequence is still uploading when a higher one commits.
- Each part reply carries a `Recorded` flag, set when the reply comes from a recorded file-committed record. Per-batch error codes alone can't tell a commit whose batches all failed from a rejection, and the finish report needs the difference.

A part is finished when the broker stops sending it: it committed, the leader rejected it with a non-retriable error, or its file's commit deadline passed. A file is finished when every part is finished, the tracking part included, and the broker never sends another attempt for it. A file whose upload failed is finished at once.

Produce requests complete per request, not per file:

- A produce request goes entirely into one file. `ActiveFile.add` registers one future per request. `ClosedFile` records, for each request, the set of commit queues that hold its batches. Batches rejected at validation, in `invalidResponseByRequest`, belong to no queue, so a request with only rejected batches completes at file close.
- A request waits only for the parts that hold its batches. It waits for the tracking part only when the tracking partition holds one of its batches. The tracking part's answer matters for deletion, and a participant's commit stands on its own.
- When a part finishes, `AppendCompleter` records the per-batch results for that queue. Each request whose queues have all finished then completes, with results merged in request order as today. The broker sends no partial answer before that. Committed batches get their offsets, rejected batches their errors, and batches whose part didn't confirm by the deadline get `KAFKA_STORAGE_ERROR`.
- Each part fills the batch coordinate cache when it commits, not after the whole file finishes, so a consumer can read a request's batches as soon as the producer gets its answer.
- The Postgres and in-memory backends have one queue, so every request completes when the file does, as today.
- `ReplicaManager.appendRecords` doesn't change. It sends diskless entries to `AppendHandler.handle` and never puts them in `DelayedProduce` purgatory. For a mixed request, the classic entries wait in purgatory, and the response callback joins them with the diskless future.
- File completion still drives `finishedThrough`, the finish report, and `FileCommitJob`'s cleanup when no part committed.
- `FileCommitTime` stays a per-file metric. It measures the slowest part, not produce latency.

Kafka sends exactly one response per produce request. In `DelayedProduce`, a classic `acks=all` request waits until every one of its partitions is satisfied or the timeout passes, so a classic request also waits for its slowest partition, and no other request waits on it. Per-request completion gives diskless topics the same isolation. A request that holds no batch for a stuck coordinator partition gets no added delay from it, and a request that holds one completes within one commit deadline of its file's close.

Rejected alternatives:

- Committing at the tracking partition first and sending the other parts only after it confirms. It adds a second serial round trip to every commit, and it fails every file under a lease as a whole while its tracking partition is unavailable.
- Completing every request of a file once every part has an answer. A stuck coordinator partition would then delay every request that shares a file with a request that touches it, by up to one commit deadline.
- `REQUEST_TIMED_OUT` for a batch whose part didn't confirm, which is closer to classic semantics. It's a client-visible change, and it would either differ between backends or change every backend. `KAFKA_STORAGE_ERROR` is the error every backend returns for a failed commit.

### Broker rows

Every coordinator partition keeps one row per uploading broker, keyed by broker ID:

- The broker epoch `e_r` and the last sequence `s_r` that committed at this partition. The last sequence is empty after a newer epoch moves the row.
- The complete per-batch result of that commit, in request order: each committed batch's assigned offset and timestamp, each failed batch's error code, and each duplicate's original offset and timestamp.
- The fenced-through pair `(e_f, s_f)`, kept apart from the last-commit fields. A fence that overwrote the last sequence would make a retry of `s_r` stale while the broker still waits for it, and the broker would report committed batches as failed.
- The retired-through epoch, `retiredThrough`, from the newest broker-epoch-retired record. Every epoch at or below it is gone.

The floor is the highest of `(e_r, s_r)`, `(e_f, s_f)`, and `(retiredThrough + 1, none)`, where `none` sits below every sequence of its epoch. The stored result doesn't depend on the batch rows, so a retry that arrives after retention, `DeleteRecords`, topic purge, or consolidation pruning removed the batches still gets the original answer. A higher sequence replaces the result. Rows are never deleted. They're bounded by the number of broker IDs the cluster has ever used, and a deleted row would let a late commit look like a new broker.

Deduplication uses the pair in this row, not the object key. The `files` table keeps the object key for reads and deletion, and an index on uploader broker, epoch, and sequence so that a fence can find what a partition holds.

### Commit on the leader

The delegate sends each part of a file to the leader of its coordinator partition. Every part carries the object key, the uploader's broker ID, broker epoch, and sequence, the tracking partition, and the participant set. The tracking part also carries the finish report. Each leader runs its part in this order:

1. Check the mapping of every user partition in the part, and reject a mismatch with `INVALID_REQUEST`.
2. Reject the part with `COORDINATOR_LOAD_IN_PROGRESS` when it names an unreconciled user partition, as [Lifecycle](#lifecycle) defines it, and with `UNKNOWN_TOPIC_OR_PARTITION` when it names a partition whose topic is marked deleted. Reject it with `COORDINATOR_NOT_AVAILABLE` when the overlay is full.
3. Reject the part with a non-retriable stale-epoch error when the leader's metadata image holds a newer epoch for the uploader, and append a broker-epoch-retired record when [Ending a broker epoch](#ending-a-broker-epoch) allows one. A part at the image's epoch, at a newer one, or for a broker with no registration in the image goes on to the next step. The image can lag behind the controller, and the retired-through epoch refuses a removed epoch durably. The leader doesn't reject a fenced uploader, so a short heartbeat gap doesn't fail commits in flight.
4. Compare the part's pair `(e, s)` with the broker's latest state: its newest pending overlay entry if one exists, and its SQLite row otherwise. [Acceptance rules](#acceptance-rules) lists the cases.
5. For a new commit, check the producer epoch and sequence rules against SQLite plus the overlay. The Postgres `commit_file` functions enforce the same rules against committed state.
6. Assign the offsets in the overlay, then add one file-committed record to the current batch. The record stores the committed batches with their offsets, the outcome of each other batch in the part, and, at the tracking partition, the finish report. Its apply moves the row to `(e, s)` with that result.
7. When the high watermark passes the record, apply it to SQLite in the same transaction as the applied offset, remove the overlay entry, and reply with the per-batch results and the `Recorded` flag set.

At the tracking partition, every part and lease request that names the partition reschedules the broker's idle timer, including one that the leader rejects or that a failed append reverts. [Ending a lease](#ending-a-lease) describes the timer.

Every reply takes `logStartOffset` from current state, whether it answers the first attempt or a retry. The stored result contains only what the leader decided.

A commit whose batches all fail their checks is an ordinary new commit with zero committed batches. It appends a file-committed record with every batch's outcome and advances the row. A retry gets exactly the first answer, and the partition learns in that apply that it holds no batch of the object, so it releases the object at once. Recording nothing was rejected: it would make the all-fail commit the only commit that doesn't advance the row, and its object would wait for a later sequence or the lease end.

### Acceptance rules

The row holds `(e_r, s_r)`, the fenced-through pair, and the retired-through epoch. A commit's pair is `(e, s)`:

| Case | What the leader does |
| --- | --- |
| `e` is at or below the row's retired-through epoch, or `e < e_r` | Reject as a stale epoch |
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
- A part that is rejected before the producer checks records nothing and leaves the row unchanged. This covers a stale epoch, a stale sequence, the image check, the mapping check, the lease check, an unreconciled partition, a full overlay, and a non-leader. Its reply has the `Recorded` flag unset.

### Ending a broker epoch

A KRaft broker epoch is the metadata offset of its registration: `QuorumController.registerBroker` passes `offsetControl.nextWriteOffset()` as the new epoch. So for a broker with no prior registration, an image at offset `X` shows every registration of that broker at an epoch at or below `X`. Each leader uses this to retire old epochs without guessing.

- The broker-epoch-retired record holds the broker ID and one value, `retiredThrough`. If the leader's image shows a newer epoch `E` than the broker's row or lease table holds, the leader sets `retiredThrough` to `E - 1`. If the image shows no registration, it sets `retiredThrough` to the image's last contained offset. Any epoch at or below that offset that the image doesn't show belongs to a registration that is gone, including one this partition never saw.
- The leader appends the record only when `retiredThrough` is at or above the broker's newest epoch in its row, its lease table, and its overlay. An image that lags a new registration therefore appends nothing, and commits from the new epoch proceed.
- It checks on each image change and once it starts serving after an election. A record whose `retiredThrough` is at or below the row's retired-through epoch is a no-op.
- Apply raises the row's floor to `(retiredThrough + 1, none)`, and creates the row if there's none. A commit at `e <= retiredThrough` gets a stale-epoch rejection, whatever the replica's image shows. A broker that returns under the same ID has a higher epoch and commits normally.
- At a tracking partition, the same apply ends every lease at an epoch at or below `retiredThrough`, at that log position, as [Ending a lease](#ending-a-lease) describes. A lease request at a newer epoch does the same for the older epochs before it stores the new range.
- A fenced broker keeps its epoch, so fencing triggers nothing.

The record is needed because the image check alone isn't durable. After a failover, a new leader whose image lags could accept a late commit from an old epoch whose unused sequences were already deleted.

The rule for an image with no registration depends on how the controller writes a registration. `QuorumController` sets the epoch to the offset of the first record the registration writes. For a new incarnation of a broker, `ClusterControlManager.registerBroker` first writes a `PartitionChangeRecord` for each partition where the old incarnation is in the ISR or ELR, then the `RegisterBrokerRecord`. The result isn't atomic, so it can span several batches, and an image can end between them. On a restart, that gap is harmless: the image still shows the old registration, and the leader retires through the old epoch. For a broker with no prior registration, unregistration has already removed it from every ISR and ELR, so the `RegisterBrokerRecord` is the only record, at the epoch's offset. A controller test pins this behavior. If an upstream change ever writes a record ahead of it, the test fails at the sync. The fallback then is to retire on proof: retire through the epoch of an unregistration that the leader's image showed, and let the idle lease end cover a leader that loaded a snapshot.

Within these rules, safety doesn't need retirement at a participant. A lease end at the tracking partition fences the participants before any leftover object is deleted, so a late part from a retired epoch fails on the floor. Retirement at a participant keeps its acceptance rules tidy and frees a dead broker's row.

Rejected alternatives:

- A retired marker with no epoch, set when the image shows no registration. A broker that returns under the same ID would be rejected forever, and a leader whose image lags a new registration would retire a live broker.
- Storing the row's last epoch as the retired-through value when the image shows no registration. It still retires a new broker that the image hasn't seen, and it misses an epoch that registered and was unregistered without this partition seeing it.

### Deadline and unknown outcomes

The delegate retries each part on its own against the current leader of that coordinator partition, after `NOT_COORDINATOR` or a retriable error, with the same pair and object key and with the backoff in [Transport](#transport). Within a 30-second deadline, one part makes about 30 attempts per broker, and the jitter keeps brokers from retrying in step after an election. If the first attempt is still pending on the leader, the retry waits for its result and appends no second record. The delegate stops at the file's commit deadline, and the last wait ends at the deadline. When the deadline passes:

- Batches on parts that confirmed get their per-batch results.
- Batches on parts that didn't confirm get `KAFKA_STORAGE_ERROR`, as a whole failed commit does today. Their outcome is unknown: the part's last request can still reach the leader and commit later. That window closes once the broker's next commit to the same coordinator partition applies, a fence raises the floor there, the broker's epoch retires, or its lease ends.
- If at least one part committed, the file returns these per-batch results. The committed batches are already visible to consumers, so the delegate must not fail them. A producer retries only the failed partitions, as with any partial produce failure.
- If no part committed, the file fails as a whole, and `FileCommitJob` runs its existing cleanup path, which never deletes the object on this backend.

An unknown outcome is an exception in the broker's next finish report, which makes that report urgent. A busy broker's next tracking part carries it, and a quiet broker sends it on an early keepalive within one `produce.commit.interval.ms`. The tracking partition then sends the exception range fence, which raises the participant's floor past the stuck part. The window therefore closes about one commit interval plus one append after the deadline, on a quiet broker as on a busy one.

A late part from an unknown outcome affects producers as follows:

- Idempotent producers have no window. Producer sequence checks catch a late copy through any broker.
- A non-idempotent producer that retries through the same broker can see its data twice in the original order, as with an `acks=all` timeout on a classic topic. Its retry lands in a later file, whose commit at that coordinator partition makes the late part stale.
- A non-idempotent producer that retries through another broker can see the late copy after the retried one, until the window closes. The Postgres backend has the same window, because a JDBC commit that hit `socketTimeout` can still commit on the server.

Rejected alternatives for the quiet-broker window:

- A sequence bump at the deadline: an empty commit at `s + 1` when nothing else is queued. It costs a request per stuck part and can fail like the part it guards. The early keepalive costs one request per quiet period after a failure.
- A leader-side bound on request-queue time. It needs queue timing from request handling and doesn't cover a request that is already appended.
- An admission cap on the leader or a retry budget in the delegate. A slow leader causes no retries, only fast errors do, and the backoff covers those. [Load bounds](#load-bounds) lists what bounds the load.

### Apply rules

The leader's checks make the cases in this list unreachable, so a replica that meets one treats it as a bug. It stops applying that coordinator partition and reports corruption, in the same state as an unreadable record, and rolls back to that record's savepoint:

- A file-committed record whose pair isn't above the row's floor.
- A sequences-leased record whose range overlaps a range already held for that epoch.
- A record that changes a user partition with no partition-created row: file-committed, diskless-log-initialized, diskless-log-repaired, records-deleted, retention-applied, log-pruned, or cross-tier-start-advanced. A leader bug that skipped the unreconciled-partition rejection then stops the partition instead of creating a row that a later topic-deleted record could purge.

These cases apply as no-ops, and the applied offset still advances:

- A sequences-fenced group whose range end isn't above the floor.
- A broker-epoch-retired record whose `retiredThrough` is at or below the row's retired-through epoch.
- A files-deleted record for a key that isn't in the deletion queue.

Rejected alternative: keeping the single commit thread. One stuck coordinator partition would then stall every broker's diskless produce, which defeats splitting the state across coordinator partitions.

Rejected alternative: answering a retry with a fixed error for the batches of a committed part that didn't commit. The error changes what the client does. `INVALID_PRODUCER_EPOCH` is fatal for a producer, while `KAFKA_STORAGE_ERROR` makes it retry, and a duplicate reported as an error is retried until it falls out of the producer's sequence window.

Rejected alternative: parsing the pair out of the object key. It ties the backend to the key format and to the prefix handling in `ObjectKeyCreator`.

## Object tracking and deletion

Every object belongs to one tracking partition, which records the object's participants and decides when it can be deleted. The participants report to it. This section is the only path that deletes an object whose commit may have reached a leader. The log deletes only keys that the coordinator's own records name, and no clock decides whether a delete is safe.

### Tracking partition

- A broker's tracking partition holds its sequence leases, tracks which partitions reference each object uploaded under those leases, and keeps the deletion queue for them.
- The lease records every key the broker may upload, so no per-object record or round trip precedes an upload.
- A broker uses a sequence only after its lease is confirmed, so each key has exactly one tracking partition: the one that issued its range. A broker can hold ranges at several tracking partitions in one epoch, after a fallback or a switch, and each key still names exactly one.
- The participants of an object are the coordinator partitions that its batches map to. The tracking partition records the participant set from the tracking part it receives. When the tracking partition holds batches of the object, it is a participant too.
- Nothing outside the broker computes its tracking partition. Every part names it, and release reports go to the partition the part names, so the choice needs no agreement across brokers or versions.

A broker picks its tracking partition from its own gateway set: the coordinator partitions it is a gateway for in its own rack, as [Gateway routing](#gateway-routing) defines them. A routed file then sends no extra part for tracking. The choice sits behind an internal tracking selection interface:

- Inputs: `N`, the broker's gateway set with each member's score from the gateway assignment interface, and the current tracking partition, which is empty at the start of an epoch. The current tracking partition is the one whose range is in use, so a fallback to another member of the set sticks.
- Output: an ordered list of coordinator partitions. It holds the preferred tracking partition, then the other members of the gateway set by score, then every other coordinator partition by the broker's score. Every renewal walks this list from the top, as [Leases](#leases) describes.
- The coordinator backend's sequence source calls the interface at the start of each epoch and on each metadata image that changes the broker's gateway set.
- The sticky rule is the only production implementation, and tests inject a stub, as for the assignment interface. No config selects the strategy, because a config would make the interface a public extension point that nobody needs.

The sticky rule picks the preferred tracking partition:

- While the current tracking partition stays in the gateway set, the broker keeps it.
- When the current one leaves the set, and at the start of an epoch, the broker takes the member with the highest score, `toPositive(murmur2(bytes(cp) ++ bytes(brokerId)))`, the score the gateway assignment uses. At saturation the set has one member, so the rule chooses only below `N` brokers per rack.
- A broker with an empty gateway set keeps its current tracking partition. At the start of an epoch with an empty set, it takes the coordinator partition with the highest score over all `N`. This is the sticky rule with the whole partition range in place of the empty set, so the interface needs no new input.
- A broker that only gains coordinator partitions never switches, which is the common case for a stand-in during a fence. A rolling restart doesn't bounce tracking partitions back and forth.

When the list's first entry changes, the broker switches once it closes the first file with no batch for its current tracking partition. That close starts a renewal at the preferred partition with the reason `assignment`, and the switch and the lease release then follow [Leases](#leases). The broker already splits each file by commit queue at close, so the check is local. Two cases don't trigger it:

- A broker on a fallback doesn't trigger on empty parts while the list's first entry stays the same. It returns at its next renewal. Otherwise, at saturation, every file close would send a lease request to a partition that can't answer.
- A broker that closes no files doesn't switch, and it sends no parts either.

The trigger waits for the first empty tracking part because a switch saves nothing while clients with cached metadata still send the lost coordinator partition's produce: the broker's files hold its batches, so its tracking part isn't empty. The tracking partition can lag the assignment for as long as that stale traffic lasts, which is harmless, because nothing outside the broker reads it.

With the gateway switch off, the interface still reads the assignment, which every broker computes from the image either way. Files then hold batches for almost every coordinator partition, so the tracking part is rarely empty, and the trigger rarely fires. Turning the switch on or off moves no tracking partition.

### Finish reports

The tracking partition fences a participant only after the broker has finished the file, and the broker tells it which files are finished, and which participants didn't answer, in a finish report. The tracking partition never fences a participant that answered. It waits for that participant's release report instead.

- `finishedThrough`: every file of this broker epoch with a sequence at or below this value is finished, whichever tracking partition issued its range. It's an inclusive bound, as the fenced-through pair is. Before the broker finishes any file in the epoch, it's one below the epoch's first sequence. Files finish out of order, so it trails the slowest unfinished file, by at most one commit deadline. It covers the whole epoch because a participant keeps one floor per broker, not one per tracking partition.
- A participant answered when its last reply for the part had the `Recorded` flag set, that is, it came from a recorded file-committed record, pending or applied. This includes a commit whose batches all failed, and a retry answered from the stored result.
- Every other outcome is an exception: a rejection before the producer checks, such as a stale epoch, a stale sequence, a mapping mismatch, an unreconciled partition, or a full overlay; `NOT_COORDINATOR` until the deadline; an unknown outcome at the deadline; and a part that the broker never sent. A stale-sequence rejection stays an exception. Its fence appends nothing at the participant, because the sequence is below the floor. Taking the broker's word as "released" would let a bug in the broker delete data.
- A finish report holds `finishedThrough` and at most one exception range `[a, b]` per participant, covering its exceptions at or below `finishedThrough` within the tracking partition's own ranges. If a range covers sequences the participant answered, the fence answers "referenced" or "released" for them, which is harmless. The payload is at most one `int64` plus `N - 1` ranges.
- The broker adds the current report to every tracking part, lease request, keepalive, and lease release. Each of them stores the report in its record: the file-committed record, the sequences-leased record, the finish-reported record, or the lease-ended record. So the report costs no append of its own on a busy broker, and it's still in SQLite for a new leader after a failover. A retry sends the same report as its first attempt.
- The broker keeps a report until a recorded tracking part, a confirmed lease request, a recorded finish-reported record, or a confirmed lease release includes it, and merges it into newer reports until then. It sends a tracking partition only the reports for its own ranges.
- A keepalive appends a finish-reported record only when it brings a newer report than the tracking partition has recorded, and the tracking partition confirms once the record applies. Its apply triggers the same fences as a report in a file-committed record. A keepalive with no newer report appends nothing.
- An urgent finish report holds an exception range, or covers a file whose tracking part the tracking partition didn't record. The broker sends it within one `produce.commit.interval.ms`, on the next tracking part or on an early keepalive, as [Leases](#leases) describes.
- The broker's claim decides only whether the tracking partition waits or fences. A participant is released only by its own release report or a fence answer, so a wrong "answered" claim leaks an object and never deletes one.

### Releases

- A participant releases an object when it holds no batch of it anymore. [Emptiness check](#emptiness-check) decides that in the apply that removes the last batch.
- The participant leader reports its releases to the tracking partition named in each part, on a private request. It keeps at most one report in flight per tracking partition. Releases that pile up while a report is in flight go into the next one, so batching needs no timer. Under a trickle, each release goes out at once. Under a retention pass, the reports grow by themselves.
- A report carries `(brokerId, brokerEpoch)` groups, and each group holds sequence ranges. A report's ranges cover only sequences that the participant released. Unlike a fence, a report can't merge ranges into their hull: if a participant released sequences 1 and 3 and still holds 2, a range from 1 to 3 would release 2 while a batch references it. Retention removes old objects first, and one uploader's sequences rise over time, so a retention pass mostly releases consecutive sequences.
- An internal constant caps a report's encoded size at 512 KiB, below the `max.message.bytes` of `__diskless_metadata`. The participant splits a larger set across consecutive reports. Nothing about batching is configurable.
- The broker's scheduler sends and retries the report with the backoff in [Transport](#transport). The report is idempotent, so a repeat after a lost reply is a no-op at the tracking partition.
- The tracking partition applies a whole report in one SQLite transaction and one participant-state-recorded record. It answers each `(brokerId, brokerEpoch)` group separately. It applies the valid groups, and it returns a non-retriable error only for a group outside its leased ranges, which only a bug sends, and counts it in `ReleaseReportRejectedRate`. A whole-report rejection would leave the valid releases in the report without a release-accepted record, and their entries would leak.
- The participant appends one release-accepted record per accepted report, with the accepted groups and ranges, and drops those pending releases. A new participant leader resends only the releases without one. `PendingReleaseCount` and `PendingReleaseOldestAgeMs` show releases without a release-accepted record.
- When the tracking partition is itself a participant, it records its own release in the apply that removes the batches, and sends nothing. Its own entry for an empty or all-fail tracking part is released in the apply that records the part. That release triggers no fence.
- Participants report only releases. The tracking partition learns everything else from finish reports and fences.

Every participant in a recorded participant set has an entry at the tracking partition for `(brokerId, brokerEpoch, sequence)`. Each entry stores its creation time, the leader's timestamp on the record that created it. The entry moves forward through four states and never back:

| State | Meaning |
| --- | --- |
| Pending | The participant set is recorded, and no finish report, fence answer, or release report covers this participant yet. |
| Fencing | A range fence that covers the entry is out, with no recorded answer. |
| Held | The participant holds batches and owes a release report. |
| Released | Terminal. |

- Pending to held: a finish report says the participant answered, or a fence answers "referenced". "Answered" and "referenced" mean the same thing to the tracking partition.
- Pending to fencing: a finish report lists the participant in an exception range, or the lease ends.
- Fencing to held: the fence answers "referenced".
- Any state except released, to released: the participant's release report arrives, or a fence answers "released".
- Held never goes back to pending or fencing. A finish report that lists a held entry as an exception leaves it held.
- Once an entry is released, every later event leaves it unchanged. This includes finish reports, fence answers, lease ends, and repeated release reports. The record still applies and still counts in metrics.

The object enters the deletion queue in the apply that moves its last entry to released, the tracking partition's own entry included.

Released is terminal because of an invariant: after a participant releases a sequence, it can't come to reference that sequence again.

- Release by the emptiness check: the part committed, so the broker's producer state at the participant is at or above the sequence. A retry gets the stored result and doesn't commit again.
- An all-fail release: a retry gets its answer from the stored file-committed record.
- Release by a fence answer: the sequence is at or below the participant's fenced-through pair and can't commit there again.
- A "referenced" answer that races a release report: the participant applied the fence before the apply that removed its last batch. Its log already orders the two, and only the network reorders the messages.

An all-fail participant releases in the apply that records its part, and it reports at once, so its report often arrives before the tracking part. The tracking partition stores the report. If it dropped the report and accepted it, the participant would append release-accepted and never resend, and its entry would wait forever once the tracking part recorded. When no entry exists for the sequence, the tracking partition classifies the report against a bound: the higher of the range's highest recorded sequence and its own fenced-through pair for the broker. A missed-sequence fence and a lease end both raise that pair.

| Sequence | Action |
| --- | --- |
| Above the bound | Early report. The tracking partition stores the entry as released. When the tracking part records, its participant set merges with the stored entry. |
| A missed sequence with unanswered fences | The tracking partition stores the entry as released. It counts as that participant's answer, the same one a fence gets. |
| At or below the bound, with no entry | The object is already resolved. The tracking partition accepts the report as a no-op. This case is a duplicate after a participant leader change, before release-accepted applied, or a resend after a lease end resolved the sequence. |
| Outside every range that this partition leased to the broker and epoch | The tracking partition rejects that group with a non-retriable error and counts it, because only a bug sends one. |

Early entries are bounded: at most `N - 1` per sequence, and only for sequences in leased ranges. The tracking part, the missed-sequence fences, or the lease end resolves each one.

A release report or a "referenced" answer from a partition outside the recorded participant set only comes from a bug, because a participant releases only a part it received. The tracking partition records the entry, leaves the participant set unchanged, and counts it in `ReleaseOutsideParticipantSetRate`.

Rejected alternatives:

- A retriable rejection of an early report until the tracking part records. A tracking part that never records resolves through fences, its entry goes, and the participant retries forever unless the tracking partition can answer "already resolved". That needs the same classification as storing the report.
- One report per object, as a private request with two records each. A retention pass releases many objects, and each would cost a request and two records. With replication factor 3 across three AZs, one report per object costs about 570 cross-AZ bytes per released object and participant. A batched report costs about 75 in the worst case, with one 16-byte range per sequence, and close to 0 under uniform retention.

### Fences

A fence asks a participant which sequences in a range it holds, and makes every other sequence in the range impossible there.

- A fence names the broker, the epoch, and a range `[a, b]`. If `(e, b)` is above the participant's floor, its leader raises the fenced-through pair to `(e, b)`. Otherwise it changes nothing, because a sequence below the floor is already impossible there.
- The fence is ordered in the overlay after every earlier entry. The leader replies once it applies, with the sequences in `[a, b]` whose batches this partition still holds. Every other sequence in the range is released: it never committed or its batches are gone, and it can't commit again. A repeated fence after a lost reply gets an answer from current state.
- A part pending before the fence commits first, and the fence answers referenced for it. A part that arrives after the fence gets stale-sequence, and its producer retries. That stays correct.
- The tracking partition records each answer in a participant-state-recorded record, and the answer moves the entries as [Releases](#releases) describes.
- The fence needs no clock and no wait.

Fences go out in batches:

- Each tracking partition keeps at most one fence request in flight per participant. Fence needs that its records create while a request is in flight go into the next one, so batching needs no timer.
- A request carries `(brokerId, brokerEpoch)` groups, and each group holds sequence ranges. Within a group, two ranges merge into their hull, `[min a, max b]`. A fence's effect on the floor depends only on `b`, and every sequence in the hull is at or below a bound the broker has finished or inside an ended lease. The answers for the sequences in between are truthful and leave held or released entries unchanged.
- The participant appends one sequences-fenced record per request, which raises each group's fenced-through pair, and the tracking partition appends one participant-state-recorded record per reply. A fence's record joins the leader's append, so it adds no flush of its own.
- The same 512 KiB constant as release reports caps a request's encoded size. The tracking partition splits a larger set across consecutive requests.
- A new tracking leader rebuilds the pending set from recorded state and sends one batched request per participant. Finish reports are in records, so it derives the same fences.
- Fences aren't rate-limited and don't queue behind commits. The in-flight bound already caps a participant's fence work at `N - 1` requests, one from each other tracking partition, whatever the broker count or the outage length.

The tracking partition sends fences in three cases. Each waits until the broker has finished the files it covers:

- Exceptions: one range fence per exception range in a recorded finish report. Raising the participant's floor to `b` is safe, because the broker never sends a sequence at or below `finishedThrough` again. A finish report triggers at most `N - 1` range fences, however many files it covers.
- Missed sequences: a sequence in a range that this partition leased to the broker is missed once a recorded `finishedThrough` reaches it while the tracking partition has no record of it. The tracking partition doesn't know its participant set, so it fences the missed range at all `N` partitions and builds the set from the referenced answers. Consecutive missed sequences merge into one range fence per partition. Deleting a missed sequence without the fences would lose data: its parts can have committed on other partitions while the tracking part failed. One partition's row can't tell a missed sequence from a file with no batch there, so participants never mark one. The broker never supplies a missed sequence's participant set, because a bug in the broker that left a participant out would delete an object that the participant still references.
- Lease ends: one range fence per partition, from the sequence after the recorded `finishedThrough` to the highest range end the lease covers, or to `b` for a lease release. The range covers used sequences without a finish report, missed sequences, and unused sequences. For a sequence with a recorded participant set, each participant's answer moves its entry, and answers from other partitions don't change it. For a missed or unused sequence, the referenced answers build the participant set.

When one record both reaches a missed sequence and ends a lease, such as a lease release whose `finishedThrough` reaches a sequence the tracking partition never recorded, the tracking partition applies the missed-sequence rule first. Otherwise the lease-end range would start above the sequence, and nothing would fence its key.

The tracking partition doesn't send an exception or lease-end range fence to a participant whose entries in that range are all held or released. Those sequences can't commit there again, so the fence would cost a request and a record for nothing. The common case is an early release followed by a finish report that lists the same participant in a merged exception range.

For a missed or leftover range, keys that no partition holds go to the deletion queue, and the rest are tracked like any committed object until their participants release them.

`FenceRate` counts fence ranges by cause and answer, `PendingFenceRangeCount` shows the backlog, and `TrackingEntryOldestAgeMs` with `state=fencing` shows a fence that never answers.

Rejected alternatives:

- Fencing every participant that the tracking partition hasn't heard from, once the tracking partition and every participant it has heard from are released. With an empty tracking part, both conditions hold at once, so every participant is fenced while its part may still be in flight, and a failover becomes failed produces. On the retention path it fences once the tracking partition's own batches go, so with mixed retention it can fence on day 1 and then wait 29 days.
- Fencing a missed sequence as soon as the tracking partition records a later sequence. Parts of the missed sequence on other partitions can still be retrying at that moment.
- A `finishedThrough` over the tracking partition's own ranges only. After a fallback or a switch back, a missed-sequence or lease-end fence at one partition raises a participant's floor past a lower-sequence part under another partition's range that is still retrying, and that part fails.
- Coalescing fences per broker epoch only. A tracking partition still sends up to `B` requests to one participant.
- Accepting a fence storm because a fence costs no flush. Its work still grows with the outage length and lands on the recovering partition's overlay.
- A rate limit on fences. It adds a config and delays resolution, and the in-flight bound already caps the work.

### Ending a lease

These events end a lease at a tracking partition: the broker-epoch-retired record, a lease request at a newer epoch, and the lease-ended record. The lease-ended record comes from the idle timer, which ends every range of the epoch at that partition, or from a lease release, which ends the ranges that end at or below its `b`. Apply raises the tracking partition's own fenced-through pair to the highest range end it ends, treats the unused part of those ranges as leftovers, and the leader then sends the range fences in [Fences](#fences). A renewal never lowers the floor, so a sequence whose leftover key was deleted can't be leased or committed again.

The leader appends a lease-ended record when a broker has sent no part and no lease request that names the partition for `diskless.metadata.lease.idle.timeout.ms`, 15 minutes by default and provisional:

- The timer is a `CoordinatorTimer` operation keyed per broker, `lease-idle-<brokerId>`. Every part, lease request, and keepalive that names the partition reschedules the key, including one that the leader rejects or that a failed append reverts. That only delays a lease end.
- Its `generateRecords` appends the lease-ended record with no further idleness check. `CoordinatorTimerImpl` already drops a timer that fired and was overridden before its write event ran, and it ignores `NOT_COORDINATOR`. The group coordinator uses the same pattern for member session expiry.
- The timer runs on the leader's monotonic clock and persists nothing. A new leader starts every timer at its election.
- A live broker sends keepalives at half the timeout, so the timer fires only for a broker that is cut off from its tracking partition or paused past the timeout. `IdleLeaseEndRate` counts each idle end as an anomaly.
- Controlled shutdown adds nothing. A restart registers a new epoch, and a broker that stays registered and cut off is covered by the idle timer.

The lease end is safe because the broker never sends a sequence from an ended range again. After an epoch ends, only late requests are in flight. After an idle end, every deadline has passed, because the commit deadline is at or below the produce request timeout. After a lease release, the broker has finished every file at or below `b`.

Correctness comes from the fences and the floor, not from the timer. The timer collects a cut-off broker's leftover keys. [Time bounds](#time-bounds) lists what skew, pauses, and clock jumps do to it.

Keepalives are safe to send because nothing waits for the lease end. Unused sequences were never uploaded. Uploaded objects whose commits failed are exceptions or missed sequences, and their fences follow the finish report, not the lease end. A part with an unknown outcome is an exception, so its range fence closes the duplicate window.

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
- A committed object enters the queue in the apply that moves its last entry to released. Its tracking entries go in the same apply.
- Missed and leftover keys enter the queue from the fence answers that no partition holds, one row per sequence. A lease end adds up to about `K` rows. A missed range has no fixed bound, but it never holds more keys than the files the broker closed in that time.
- Apply builds the key path of a sequence that never committed as `<prefix>/<clusterId>.<brokerId>.<brokerEpoch>.<sequence>`. The prefix is the uploader's, from the lease table for that epoch, not the leader's. A cluster whose brokers differ in prefix would otherwise delete keys that were never written and leak the real ones. Apply takes the cluster ID from the replica itself, which is fixed at format time and the same on every replica.
- A queue row's mark time is the timestamp the leader wrote on the marking record, so every replica stores the same time.

Rejected alternative: storing a range as one queue row and expanding it in `getFilesToDelete`. Storage can confirm part of a set under throttling, so the backend would have to map each key back to its range.

### `FileCleaner`

- `ReplicaManager` keeps scheduling `FileCleaner` on every broker, and neither changes. `getFilesToDelete` returns the union of the queues of the tracking partitions this broker leads, from SQLite, and an empty list on every other broker, without a request.
- The leader compares each row's mark time with its own clock through `markedBefore`. Every row waits `file.cleaner.retention.period.ms`, 1 minute by default, as today. For a committed object, the grace period protects readers that still hold its coordinates, because `consume.batch.coordinate.cache.ttl.ms` is at most half the grace period. For a key that never committed, it delays the delete past most uploads still in flight, which shrinks the leak in [Remaining leak](#remaining-leak).
- `deleteFiles` splits the confirmed keys by the partition whose queue holds each one, a local lookup, and appends one files-deleted record per partition. Its apply removes their queue rows. A partition that moved away answers `NOT_COORDINATOR` for its share, its rows stay, and the new leader deletes them again. Deleting a missing key counts as success: S3 answers a missing key with success, and the GCS and Azure backends map their not-found answer to success. `S3Storage` deletes up to 1000 keys per `DeleteObjects` request, and `GcsStorage` 50 per batch.
- Object storage needs no transactions and no atomic writes for this rule. It needs a delete by key that removes whatever is stored there. Readers reach an object only through committed batches, so a partial object is never read. S3 writes each WAL object with one `PutObject`. GCS uses a single-request upload or a resumable session. Azure commits a block blob's blocks at once and drops uncommitted blocks on its own.

### Deletes by the broker

- The backend declares a capability that turns off the broker's direct delete. `FileCommitJob.tryDeleteFile` checks it before its `ControlPlaneUnavailableException` branch, because that branch deletes without calling `isSafeToDeleteFile`. The backend's `isSafeToDeleteFile` also returns false without a request.
- The capability closes a partial send: part 1 of a file is sent with an unknown outcome, part 2 then hits `AvailabilityGatedControlPlane`, no part has confirmed, and the file fails as a whole. A direct delete would remove an object that part 1 can still commit into. A delegate that the reconciler replaces between two parts of one file opens the same path.
- Every upload already has a leased sequence that the tracking partition resolves, so this backend never needs the direct delete. The cost is storage held longer for a key whose sequence never commits: a finish report, a missed-sequence fence, or the lease end queues it, then the grace period runs. The produce path doesn't change.
- The Postgres and in-memory backends don't declare the capability, so they keep today's behavior.

### Orphan cases

The model covers every orphan case without a new mechanism. The only leaks are the accepted late upload and the accepted held entry, and no case deletes a referenced object:

| Orphan case | Path to the deletion queue |
| --- | --- |
| A broker stops before the upload | Its restart's lease request at a newer epoch, or the broker-epoch-retired record, ends its lease. The lease-end range fence answers "released," and the key is queued. A file that closed without a leased sequence was never uploaded. |
| A broker stops after the upload, before any part commits | If the broker is still running, its next finish report reaches the sequence, which the tracking partition has no record of, so the sequence is missed and its fences go out. If it goes idle or restarts, the lease end covers it. |
| A broker stops after some parts commit | No finish report covers the file. The lease end covers it instead. Committed participants answer "referenced" and release later, and the others answer "released." |
| A part at `s` has an unknown outcome, then `s + 1` commits there | `s` is an exception in a finish report. The range fence answers "referenced" or "released." |
| Leadership moves between the mark and the storage delete, or between the storage delete and the files-deleted record | The queue is in SQLite on every replica, so the new leader deletes the key, at most twice. |

### Remaining leak

An object leaks when storage completes a write after the tracking partition deleted its key. No delete ever reaches it again: retention, `DeleteRecords`, topic purge, and consolidation pruning work from batch rows, and a leaked object has none. A bucket lifecycle rule can't help, because topics can have unlimited retention.

- An attempt that storage received before the broker gave up and completed after the delete leaks one object. The grace period covers attempts that complete within a minute of the mark.
- A zombie broker that reaches a leader gets a stale-epoch reply and drops its lease, so only files already in flight leak. A zombie that is cut off can upload at most the rest of its lease, `K` files at most.
- A held entry whose participant no longer holds batches and never reports leaks its object. It needs a broker that wrongly claims "answered" or a participant that skips its emptiness check. [Accepted risks](#accepted-risks) records it.
- Today's Postgres path leaks more. `FileCommitJob.tryDeleteFile` deletes a failed commit's object only when `isSafeToDeleteFile` allows it, and nothing collects the rest.

Rejected alternatives for closing the leak: a broker-side lease expiry on the broker's clock, a second delete of every never-committed key after the expiry plus an upload time limit, conditional creates, and placeholder objects. The first two add a clock and scheduling across leader changes. A deleted key accepts a new write, so conditional creates don't help, and a placeholder moves the race to its own delete. The report-only scanner stays future work, and it would make the leak visible.

### Rejected alternatives

Rejected alternative: a per-object intent registered at an owner partition before every upload, with a maximum commit age. The intent is a serial, replicated, fsynced append before every upload, and the Postgres backend has no such step. Its safety also depends on broker clocks through the maximum commit age.

Rejected alternative: a two-phase commit across participants. A non-transactional produce has no cross-partition atomicity to preserve, and the extra round trip would block every commit.

Rejected alternative: single-coordinator objects, one upload per coordinator partition per commit interval. It removes every cross-partition reference, and with it the tracking partition, fences, and release reports. But a broker uploads one object per coordinator partition it writes for in each commit tick, which multiplies object-storage writes by up to `N` when clients don't route, by up to `⌈N × n_racks / B⌉` below saturation, and by about `N` on every broker where local-log partitions keep replica routing. It also shrinks each object. The tracking partition under gateways already gets a routed file to 1 part with one object per tick.

Rejected alternative: requiring each broker to write for one coordinator partition. Clients could no longer produce to any broker, and every availability zone would need a broker for each coordinator partition. Gateway routing is a hint for that reason.

Rejected alternative: a scanner as the deletion path. It deletes objects that no coordinator partition knows, so a stale snapshot, a restore, or a shared bucket prefix deletes live data. Its safety also depends on storage timestamps that agree with broker clocks.

## Time bounds

No clock decides whether deleting a key is safe. A key enters a deletion queue only at a log position where a record makes it impossible to reference from then on. Clocks decide only when a queued key is deleted.

| Decision | Whose clock | Role |
| --- | --- | --- |
| Idle lease timeout, `diskless.metadata.lease.idle.timeout.ms` | The tracking partition leader's monotonic clock. It persists nothing and restarts at each election. | A backstop. Ends the lease of a broker that is cut off from its tracking partition or paused past the timeout, which collects its leftover keys. The fenced-through mark and the range fence keep data safe, not the timer. |
| Keepalive interval, half the idle timeout from the lease reply | The broker's monotonic clock, from the send time of the last answered request | Liveness only. Keeps a live broker's lease from ending idle. |
| Urgent-report flush, one `produce.commit.interval.ms` | The broker's clock | Liveness only. Closes the quiet-broker duplicate window about one commit interval after the deadline. |
| `FileCleaner` grace period, `file.cleaner.retention.period.ms` | The marking leader's wall clock, written on the record, against the deleting leader's wall clock | Protects readers that hold cached coordinates, and shrinks the late-upload leak. |
| Commit deadline | The uploading broker's clock | Decides when the broker gives up. The sequence rules resolve the unknown outcome, and the broker never deletes the object itself. |
| Retention (`retention.ms`) and `LogAppendTime` | The coordinator leader's wall clock against batch timestamps | A leader whose clock runs far ahead deletes early. A classic topic has the same exposure, so it's an accepted risk. |

The snapshot interval, the snapshot transfer idle timeout, the delegate's retry backoff, the barrier deadline, the read-part deadline, and the reconciler affect liveness only. Fences, floors, epoch retirement, and membership in the deletion queue use no clock.

Under skew, a pause, or a clock jump, the worst outcome for the idle timer is a lease that ends without cause or ends late. Keepalives keep every case off the normal path:

- A part reaches the shard at the moment the timer fires. If the part runs first, it overrides the timer and the lease stays. If the timer runs first, the part to the tracking partition gets stale-sequence. A part of the same file that committed at a participant before the range fence stays readable, the fence answers that the participant holds the sequence, and the tracking partition tracks the object until the participant releases it.
- The leader pauses between the timer and the append. A short pause only orders the two events. If leadership moves, the new leader restarts the timer at its election, and the old leader's append fails at its old leader epoch.
- The leader's monotonic clock jumps forward. Leases end at the tracking partitions this broker leads. Their brokers' files in flight fail once with stale-sequence, the brokers renew above the floor, and producers retry.
- A broker pauses longer than half the timeout. Its lease can end, the first file after the pause gets stale-sequence, and the broker renews above the floor.
- An uploading broker pauses past the timeout with files in flight. Its commits get stale-sequence. An upload that lands after the queue deleted its key is the accepted leak.
- Leadership of a tracking partition changes more often than the timeout. Idle leases of cut-off brokers don't end, so their leftover keys stay until leadership holds for one timeout. This is a liveness cost only, and live brokers don't depend on the timer.

The grace period keeps its wall-clock mark time and `markedBefore` comparison. The Postgres backend already compares the marking broker's time with the deleting broker's clock, so cross-broker skew in the grace period is the status quo:

- A deleting leader whose clock runs ahead deletes early. A reader with cached coordinates gets `KeyNotFoundException`, its fetch fails with a storage error, and the consumer retries with fresh coordinates. The batches were already gone from coordinator state, or for consolidation pruning, the data lives in tiered storage. Skew beyond half the grace period is a reader-availability risk shared with the Postgres backend, not data loss.
- A key that never committed and is deleted early widens the accepted leak window.
- A deleting leader whose clock runs behind holds storage longer.
- A `FileCleaner` that pauses after `getFilesToDelete` while leadership moves deletes from a stale list. Every key on that list is in committed queue state and can't be referenced again, so a second delete is harmless.

No metric measures clock skew. Adding one would bring back a broker timestamp on every request for a gauge, and skew matters only for retention, `LogAppendTime`, and reader availability, which classic Kafka leaves to NTP. [Accepted risks](#accepted-risks) gives the NTP procedure.

Rejected alternative: a monotonic grace period that restarts at each election. It delays every delete by one grace period after a failover, and it changes the `getFilesToDelete(markedBefore, …)` contract that the Postgres backend shares.

## Local state

- Each replica of a coordinator partition stores that partition's state in its own SQLite database. A broker that hosts several coordinator partitions keeps one database for each. The database is a cache. The log is the source of truth.
- Each applying transaction stores the offset and the leader epoch of the last record it applied, in the same transaction as the state change. On restart, the replica replays from that offset.
- On open, a replica checks its stored offset and epoch against the log: the log must still hold that offset, and the record at that offset must carry that epoch. A log flushed with `flush.messages=1` can still be truncated and rewritten by a later leader, so the offset alone doesn't prove the applied row still matches the log. This is the same check KRaft makes for a snapshot's `OffsetAndEpoch`.
- Invariant: the applied offset is always at or below the high watermark. A replica learns the high watermark from each fetch response. A record above the high watermark stays in the log, unapplied, until the watermark passes it.
- SQLite holds only records at or below the high watermark. The leader drops unreplicated work from the overlay, so a replica never reverses an applied row.
- SQLite runs in WAL journal mode with `synchronous=NORMAL`. In WAL mode a reader doesn't block a writer, so a snapshot backup and a `findBatches` read don't stall applies.
- With `synchronous=NORMAL`, a power loss can roll back the most recent applying transactions but never corrupts the database. Each rolled-back transaction takes its applied offset with it, so the replica reopens at an earlier offset and replays from the log, which `flush.messages=1` keeps durable. SQLite applies only records already in the local log, so it can fall behind the log but never get ahead of it. `synchronous=FULL` would add a second fsync to the leader's reply path.

### Applying in groups

The leader, the follower applier, and the loader each apply every record from the applied offset up to the new high watermark in one SQLite transaction, a group. The runtime already merges high-watermark updates into one event on the leader. The follower applier groups up to the high watermark of each fetch response, and the loader groups as it replays.

- Each record runs under its own `SAVEPOINT`. When a record stops the apply, the applier runs `ROLLBACK TO` on that record's savepoint and then commits the group with the offset and epoch of the record before it. A record stops the apply when it's unreadable, when it's one of the unreachable cases in [Apply rules](#apply-rules), or when it changes a user partition with no partition-created row. SQLite then holds every record before the stop and none from the stop onward.
- The leader replies to the records before the stop and then resigns. The records after the stop fail with `NOT_COORDINATOR`.
- The open check and the snapshot tag read the stored offset and epoch, so both stay exact at group boundaries.
- No-op records still advance the applied offset, and the emptiness check stays in the transaction that removes the batches.
- A power loss rolls back whole groups, and the replica replays them from the log.
- A group closes before the first record that would push it past 4 MiB of log bytes, and the next group starts with that record. A record larger than the cap gets a group of its own. The cap is a constant. It counts bytes because one file-committed record can carry hundreds of batches. 4 MiB is about the 1000-page checkpoint trigger, so one transaction can push the WAL to at most about twice the trigger before it ends and lets the WAL reset.

Rejected alternatives:

- One transaction per record. Each record then pays its own commit, and a long replay applies record by record.
- A decode pass before the transaction, with no savepoints. It catches unreadable records only, not the stop cases that depend on state.
- An uncapped group. A new leader, a returning follower, or a restarting loader can cover a large span of the log, and one long transaction keeps the WAL from resetting and holds every reply in the group.
- A record-count cap. Bytes per group then vary with the batches per record.
- A config for the cap. Nothing points at an operator need to tune it.

### Checkpoints

No apply thread runs a checkpoint. The shard thread and the follower applier keep only the WAL header sync at each WAL restart.

- The apply connection sets `wal_autocheckpoint=0`.
- One checkpoint thread per broker serves every coordinator database that the broker hosts, with one checkpoint connection per database. At most one checkpoint writes to the disk at a time, so checkpoints don't pile up against the log flushes.
- The thread checkpoints a database once its WAL reaches 1000 pages, SQLite's own threshold. The threshold is a constant and the only trigger. A database that stays under it never syncs.
- Checkpoints run in `PASSIVE` mode only. `PASSIVE` never takes the writer lock, so it never blocks an apply.
- A `PASSIVE` checkpoint stops at the oldest open reader, and the WAL resets only after a checkpoint completes with no reader in it. A snapshot backup copies the database in one step and is the only long reader, so the WAL grows by the apply rate times the backup's duration and drops back after the backup ends. Long read transactions in the read pool hold the checkpoint back the same way. Nothing escalates. `SqliteWalPages` shows the WAL size of each database.
- A checkpoint that fails is logged, and the thread retries it on its next pass.
- The bundled SQLite must be 3.51.3 or later, or 3.44.6 or 3.50.7. Those versions carry the fix for SQLite's WAL-reset bug, which a checkpoint connection beside an apply connection triggers. The broker checks the version at startup and refuses to start the coordinator backend below it.

Rejected alternatives:

- Keeping the automatic checkpoint with a higher threshold. The stall is rarer and longer, and it stays on the commit path.
- Checkpointing on the shard thread when its queue is idle. Under load the queue is never idle, and the checkpoint still blocks the shard.
- `synchronous=OFF`. A power loss can then corrupt the database in ways that the offset-and-epoch check on open doesn't catch.
- One checkpoint thread per database. Up to one checkpoint per hosted database syncs at once against the log flushes on the same disk.
- A time-based trigger, alone or next to the page threshold. It syncs databases with little traffic, and a WAL under 1000 pages already keeps reads fast.
- Escalating to `RESTART` or `TRUNCATE` above a hard limit. Both take the writer lock and wait for readers, which stalls the apply and the commit path.
- Canceling the backup above a hard limit. Under steady load the backup may never finish, so no snapshot is taken, the log start never advances, and the coordinator log grows without bound instead.

### Schema

- The schema has tables for logs, files, batches, producer state, broker rows, and the applied offset on every partition. Each user partition's row keeps its partition-created record's `metadataOffset`.
- Tracking partitions add lease tables, the recorded finish report for each broker epoch, tracking entries with each participant's state and creation time, and the deletion queue. Participants add their pending releases, kept until the release-accepted record applies.
- The layout is the implementer's choice, but it must support indexed lookup by partition and offset, which `findBatches` and `listOffsets` need, by object key, by `batches(file_id)` for the emptiness check, and by uploader broker, epoch, and sequence for fences and release reports.
- A coordinate row must be able to describe an extent: an offset range mapped to an object and a byte range, with the producer fields optional. A WAL batch row always carries its producer fields, because commits check idempotence against them. This design writes only WAL batch rows. The requirement keeps merged and consolidated objects possible without one row per batch. The `findBatches` response doesn't change.
- The SQLite driver is a JDBC driver with a bundled native library. Its license and packaging are to be checked against the repo's dependency rules before the choice is final, and its bundled SQLite must meet the version floor in [Checkpoints](#checkpoints).

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
- When the topic has a single replica, which only the internal override in [Topology](#topology) allows, there is no other replica to confirm, and the leader advances the log start from its own older snapshot alone. A development cluster with one replica still bounds its log.
- The wait delays log-start advancement only. Produce and fetch don't wait on a follower's snapshot report.
- The log start stays at offset 0 until the first advancement. A replica that loses its database before then replays the log from offset 0 and needs no snapshot. A new cluster therefore has no unavailability window before its first snapshot.

### Taking a snapshot

- Every replica takes its own snapshot from its own applied state, when either `diskless.metadata.snapshot.interval.ms` elapses, 1 hour by default, or `diskless.metadata.snapshot.max.bytes.between.snapshots` of log has been applied since its last snapshot, 1 GiB by default. Whichever comes first triggers it. This follows KRaft's `metadata.log.max.snapshot.interval.ms` and `metadata.log.max.record.bytes.between.snapshots`. The trigger counts bytes rather than records, because a file-committed record can carry hundreds of batches, and replay time follows bytes.
- Both defaults are provisional. KRaft's 20 MiB doesn't carry over, because a coordinator snapshot is a full copy of the SQLite database and the log grows with every broker's commit. With 1 GiB, a rebuild replays at most about 1 GiB after its snapshot, and each replica's log stays near 2 GiB. Consolidation keeps pruning batches as data moves to tiered storage, so the database is expected to stay small, and 1 GiB may be large next to it. The snapshot-transfer benchmark in [Benchmarks](#benchmarks) settles the byte trigger: if replaying the log after the snapshot takes more than half of a one-rebuild time, the default drops to 256 MiB.
- The SQLite backup API writes a point-in-time copy to a temporary file in the snapshot directory. The replica reads the snapshot's offset and epoch from the applied-offset row inside the finished copy, never from the live database. The applied offset commits in the same transaction as the state it describes, so the copy's row is the exact position of the copy's content. A tag read from the live database before the backup can be too low, which replays records the copy already holds. A tag read after the backup can be too high, which lets the leader delete records the copy doesn't contain.
- The replica fsyncs the temporary file, renames it to a final name built from the offset, the epoch, the schema version, and a checksum, and fsyncs the directory. Only then does it delete the snapshot that falls out of the retained pair. A crash at any step leaves the previous pair intact, and a partial file never carries a final name.
- On startup, the replica deletes any leftover temporary file. It checks a snapshot's checksum before it loads or serves that snapshot.
- Each replica keeps the newest snapshot and one older snapshot of its own. The count is a constant, not a config, because the log-start rule depends on that pair: with one snapshot, the leader's only covering snapshot would disappear at its next rotation, and a third would only use more disk. Snapshots are written to the broker's disk. They never go to object storage.

### Reporting snapshots

- A replica reports its newest snapshot offset to the leader on a private request after it finishes a snapshot, when it sees a new leader, and when its metadata shows it back in the ISR. The report isn't a field on the public fetch request. Steady-state followers don't already send a private request, so this report is its own request.
- The broker's scheduler sends and retries the report with the backoff in [Transport](#transport). A leader change needs no retry, because the replica reports again to the new leader. The replica fetcher thread never sends the report. A report blocked on an unresponsive leader would otherwise stall fetching and push the follower out of the ISR. A missed report only delays log-start advancement.
- The leader records the newest snapshot offset reported by each replica. When it considers advancing the log start, a report counts only if its replica is in the leader's committed ISR and has no pending removal. This is the ISR view the leader already uses for the high watermark, and the leader doesn't ask the controller.
- When a replica leaves the ISR, the leader drops that replica's report. The replica may have lost its snapshot files while it was out, and its report counts again only after it reports again.

### Fetching a snapshot

- A replica fetches a peer's snapshot only when it needs records below the log start. That happens when it starts empty, for example on a replaced broker or after a reassignment. It also happens when its database is lost, corrupt, or in a newer schema and its own snapshots don't reach the log start, or when it was out of the ISR while the log start moved past its newest snapshot.
- The replica fetches the newest snapshot that covers the log start from an in-sync follower that holds one. It falls back to the leader only when no follower has one. The leader already carries every commit and every cache-miss read for its coordinator partition.
- Every fetch-snapshot request names the snapshot ID, its offset and epoch, and a byte position. Rotation doesn't delete a file that has an active transfer. A transfer stays active until it finishes or until no chunk request has arrived for `diskless.metadata.snapshot.transfer.idle.timeout.ms`, 60 seconds by default. Each chunk request restarts that timer, so a large snapshot is never cut off for its total duration. Active transfers live in memory, so a source restart ends them all.
- A broker serves at most `diskless.metadata.snapshot.max.concurrent.transfers` active transfers across all the coordinator partitions it hosts, 1 by default and provisional. A request beyond that limit gets a retriable busy error, and the receiver tries the next replica with a covering snapshot, in the same follower-first order. A replaced broker therefore rebuilds its coordinator partitions one after another, and the brokers that serve the snapshots keep their disk and network for their own partitions. The snapshot-transfer benchmark in [Benchmarks](#benchmarks) settles the default: the replaced-broker case runs with limits of 1, 2, and 4, and the default becomes the highest limit at which the one-rebuild pass still holds on the source brokers.
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
- A read that names an unreconciled user partition gets `OFFSET_NOT_AVAILABLE`, as the barrier does when it times out. [Lifecycle](#lifecycle) defines unreconciled partitions.
- The KIP's stale `DisklessFindBatches` path is an optimization. It isn't part of this design.

### Reads on the leader

- The leader answers `findBatches` and `listOffsets` from a read pool for the whole broker, with `diskless.metadata.read.threads` threads, 4 by default and provisional until the read-heavy and cache-storm benchmarks in [Benchmarks](#benchmarks). Each read thread keeps one read-only SQLite connection per coordinator database that the broker leads, opened on first use and closed when the shard unloads.
- A read takes the applied offset from its own read transaction and answers from committed state. A reply to a commit goes out only after its apply commits, so a producer that got its answer can read its batches.
- Only a `findBatches` above the applied high watermark, the barrier case, enters the shard, because the barrier appends a record.
- The pool's queue has no limit of its own, because the read slots on each calling broker bound what arrives. A read for a coordinator partition the broker no longer leads gets `NOT_COORDINATOR`.
- With reads off the shard, the runtime's `event-queue-time-ms` measures write load alone.

Rejected alternatives:

- Reads first in, first out on the shard queue. In a cache storm, a commit part waits behind about `8B` scans, about a second at `B=30` and 5 ms per scan.
- Commits and fences at the head of the shard queue with `enqueueFirst`. Upstream reserves it for the runtime's own events, and a steady commit flow starves reads.
- A thread count derived from the core count, which upstream coordinators don't use, and a pool per coordinator partition, which grows with `N` and idles on lightly loaded partitions.

### Reads on the calling broker

- `ControlPlane.findBatches` is asynchronous and returns a `CompletableFuture`. The Postgres and in-memory backends wrap their calls in an executor, and `Reader`'s `FindBatchesJob` chain and `findBatchesNoWait` follow. A read part holds no shared thread, including a part parked at a barrier.
- The delegate doesn't retry read parts, because the consumer fetches again. A coordinator partition with no leader, or a `NOT_COORDINATOR` answer, fails the part at once.
- A read part has a deadline of 1000 ms, a constant above the 500 ms barrier deadline. A part that fails or passes its deadline gives its user partitions `KAFKA_STORAGE_ERROR`, the retriable error `FetchCompleter` already returns, and the rest of the fetch returns normally.
- The delegate allows at most 8 read parts in flight per coordinator partition on each broker, a constant that matches today's `fetch.metadata.thread.pool.size` default. `listOffsets` parts share the slots and follow the same rules on `FetchOffsetHandler`'s pool. A part over the limit waits in that partition's queue on the calling broker, and its deadline counts from when it entered the queue.
- `ReadPartFailRate` counts read parts that failed or passed their deadline, by reason.

Rejected alternatives:

- A synchronous `findBatches` with only the deadline and fast-fail rules. A stuck partition still holds shared threads up to the deadline, and unrelated fetches queue behind them. A parked barrier request would hold a fetch metadata thread for up to 500 ms.
- No read-slot limit, with only the deadline. A cache storm sends every miss at once.
- One read limit for the whole broker. Work stuck on one coordinator partition would then block reads for every other.

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

A consumer that keeps up never reaches the barrier, because an offset equal to the high watermark is in range. Only a deposed leader, a genuine seek past the end, or a client that keeps fetching past the end reaches it. The barrier adds one barrier commit to the answer, after any long-poll wait the Postgres backend also pays. A current leader commits it in one replication round trip. A deposed leader answers `OFFSET_NOT_AVAILABLE` after 500 ms, so a fetch that misses the local cache can take up to its long poll plus 500 ms. The parked part holds no thread on the calling broker, because `findBatches` is asynchronous.

The barrier rate is at most one barrier per flush of the coordinator partition's batch. When commits are flowing, the barrier joins an open batch and adds no append. The worst case is an idle partition that appends once per fsync, which is the rate a busy partition's commits already drive. The out-of-range count of `BarrierRate` shows a client that keeps fetching past the end.

Rejected alternatives for the barrier:

- Always answering the retriable `OFFSET_NOT_AVAILABLE` above the high watermark. A consumer that genuinely seeks past the end would wait for the log to grow instead of resetting, which changes classic API behavior.
- Passing the fetch's `FetchParams.maxWaitMs` through `findBatches` as the barrier deadline. It changes `ControlPlane` for every backend, and a consumer with `fetch.max.wait.ms=0` would never see a genuine out-of-range.
- The request timeout or the commit deadline as the barrier deadline. A deposed leader would hold every fetch that touches it for about 30 seconds.
- A public `scheduleWriteOperation` overload that takes a `Duration`. Upstream has none. `OperationTimeout` starts only after the event runs and appends, so queue wait comes on top of the 500 ms, and a request that joins a shared barrier late gets less than 500 ms.
- A shorter deadline. A healthy leader under fsync load would fail more genuine seeks with `OFFSET_NOT_AVAILABLE`.
- A fixed barrier window, which adds a second timer next to the linger, and fixing the runtime to a fixed linger, which gives up adaptive linger's group commit to bound a misbehaving client.

### Delayed-fetch probe

The delayed-fetch probe never runs the barrier. `ControlPlane` gets a default method for the probe, `findBatchesNoWait`, that calls `findBatches`, so the Postgres and in-memory backends don't change. The coordinator backend sets a no-wait field in the private find-batches request. With the field set, the leader answers an offset above the high watermark at once with `OFFSET_NOT_AVAILABLE` and appends no barrier. `DelayedFetch.tryCompleteDiskless` completes the fetch on any error, and `onComplete` then runs the real call through `Reader`. Below the high watermark the probe gets real answers, so a fetch with the batch coordinate cache off still finishes early when data arrives.

Rejected alternative: a backend capability that turns the probe into a cache miss. A fetch with the cache off would always wait out its long poll, even for data that already arrived.

### Leader epochs on private RPCs

The private RPCs carry no leader epoch. They rely on `NOT_COORDINATOR`, as the group and share coordinators do. KIP-320's check exists so that a client can detect log truncation, and a broker that calls the coordinator keeps no position that a truncation invalidates. Rejecting a caller whose epoch is lower than the receiver's would only force a needless metadata refresh. Rejecting a caller whose epoch is higher only speeds up failure in a race that the stale-read rule already makes safe. Adding the epoch later is a new request version.

## Gateway routing

Without routing, each client produces to the broker that the metadata transformer picks by a hash of the user partition, so a broker's file holds batches for almost every coordinator partition and sends up to `N` commit parts. Gateway routing sends each client's produce and fetch for a user partition to a gateway of that partition's coordinator partition in the client's rack. A file then sends fewer parts, and each coordinator leader receives parts from fewer brokers.

- Gateway: a broker that routing sends one rack's clients to for a coordinator partition's user partitions. It accepts their produce, uploads the WAL file, and sends a commit part to the coordinator leader. It holds no coordinator state.
- Coordinator leader: the leader of a `__diskless_metadata` partition. It assigns offsets, appends and replicates the commit record, and is the only broker that can commit a part. Being a gateway and being a coordinator leader are independent.

Routing is a hint. A produce that reaches any broker still commits, at the cost of extra commit parts, so routing never decides correctness. It changes no WAL file count and no PUT count: a broker uploads one file per commit tick either way, and routing only decides what each file holds. No public protocol changes. The transformer rewrites the `LeaderId` in metadata responses, as it already does, and every other field keeps the controller's values.

### Assignment

Every broker computes the same gateways from its metadata image alone, per rack, with no RPC.

- Candidates: the registered, unfenced brokers in the image, including brokers in controlled shutdown, with no listener filter, minus the brokers that set `diskless.gateway.candidate.enable=false`. The controller fences a broker when it grants the shutdown, before the broker closes its sockets, so reconnecting clients already get the successor. The inputs come from one image through a new `MetadataView` method that returns unfenced brokers with racks, next to a method that returns `N`.
- Racks: the rule groups candidates by rack. A missing or blank rack forms its own group, which the rule handles like any other rack, as `StripedReplicaPlacer` treats rack-less brokers. A cluster with no racks is one group. Uneven racks need nothing extra, because each rack uses its own count and cap. A rack with no candidates has no gateways, and no rack's computation reads another rack.
- Count: a coordinator partition gets `g = max(1, ⌊b_r / N⌋)` gateways in a rack, where `b_r` is the rack's candidate count. Below `N` candidates in a rack, most brokers are gateways for one or more coordinator partitions, and the cap can leave a few with none. From `N` up, leftover brokers are spares: they get no diskless produce or fetch through gateways. A rack has reached saturation when it has at least `N` candidates, so each gateway serves exactly one coordinator partition.
- Rule: rendezvous hashing with a cap. Each pair of a coordinator partition and a candidate in the rack gets the score `toPositive(murmur2(bytes(cp) ++ bytes(brokerId)))`, with both as 4-byte big-endian integers. The rule walks the pairs from the highest score down, and assigns a pair when the coordinator partition still needs one of its `g` gateways and the broker holds fewer than `⌈N × g / b_r⌉` coordinator partitions. Equal scores go to the lower broker ID.
- Pick among several gateways: when a coordinator partition has several gateways in a rack, the highest `hash(logKey, brokerId)` picks one, so produce and fetch for a user partition reach one broker, and a gateway that drops out moves only its own user partitions.
- Leadership isn't an input. An election moves no client traffic, and the coordinator's work and a rack's client traffic for one coordinator partition stay on separate brokers.
- A broker's gateway set is the coordinator partitions it's a gateway for in its own rack, or its own group for a rack-less broker. A spare's set is empty.

A score depends only on its coordinator partition and broker, so a join or leave changes only one column of scores. In simulations of random clusters at `N=8`, the rule always met the cap, and it moved 2.7 gateway slots on average when a fourth broker joined a rack, against an ideal of 2, and 1.1 when one of 10 left, against about 0.8. No bound tighter than `N` is provable for the capped walk, because one coordinator partition taking its next choice can push later ones along. A stability test asserts a measured bound instead: at most 5 extra moves per join or leave over a seeded sweep of rack sizes from 2 to 20. A move is one gateway slot changing broker, and an extra move is a slot that changes between two brokers that both stayed. The simulation that measured the bound used MD5, so the reference implementation measures it again with murmur2 before the spec fixes it.

The rule sits behind an internal interface in the style of `ReplicaPlacer`. Its input is the candidate snapshot from one image, with broker IDs and racks after exclusions, plus `N`. Its output is each rack's gateways per coordinator partition and the rack's brokers ranked by that coordinator partition's score. The capped rule is the only implementation, the transformer and the produce path reach it through `SharedState`, and tests inject a stub. A fixed-value test pins the score encoding, the tiebreak, the cap, and the fallback ranking and pick. A deliberate change to the rule needs no version gate. During a rolling upgrade, old and new brokers then compute different gateways, which splits traffic and adds commit parts until the upgrade ends, and never affects correctness.

A gateway crash needs no extra mechanism. Until the controller fences the broker, after `broker.session.timeout.ms`, its coordinator partitions stall in its rack, as a classic partition does when its leader crashes. Other racks keep committing. After the fence, the same rule on the remaining candidates picks the stand-ins. When the broker returns, its coordinator partitions move back, and the misroute rejection in [Routing policy](#routing-policy) moves the stand-ins' clients within one retry.

### Fallback order

For a client in rack `r` and a user partition of coordinator partition `p`, the transformer answers the first match of these steps:

1. A gateway of `p` in `r`.
2. Another broker in `r`, in the order of `p`'s scores from the rule, highest first.
3. A gateway of `p` in another rack.
4. Any broker.

Every step skips brokers without the client's listener. Steps 2 and 4 skip excluded brokers unless no other broker matches. Each choice among several brokers takes the highest `hash(logKey, brokerId)`. The order keeps traffic in the client's AZ whenever a broker there can take it, at the cost of an extra commit part. When it can't, it crosses AZs only to a gateway that already sends a part for `p`.

### Routing policy

Local-log partitions:

- Every diskless user partition routes to gateways once routing is on, except a partition whose fetches need a broker-local log. Such a local-log partition keeps the transformer's replica routing, which picks the `LeaderId` only from the partition's assigned replicas.
- A design in which merged data uses the WAL format in the same object storage has no local-log partitions, so gateways reach every diskless partition. A classic prefix on local disks, after a switch from a classic topic, needs replica routing while it lasts.
- In Inkless, consolidating partitions, tiered partitions, and switched partitions whose classic prefix is still on local disks are local-log partitions. A broker that isn't a replica refuses every fetch for a consolidating partition.
- The cost: replica-routed traffic spreads over all `N` coordinator partitions, so it adds parts even to a gateway's files. With consolidation on, every new diskless topic also gets remote storage, so every file sends about 8 parts at `N=8`, and gateways save nothing. With equal traffic per partition at 24 brokers, 1% of traffic on consolidating topics sends about 2.1 parts per file, 2% about 3.1, 5% about 5.1, and 10% about 6.8.
- A part whose batches all belong to local-log partitions counts as `off-gateway` in `CommitPartRate`. A part with any batch on the gateway path counts as `routed` or `misrouted`.

The per-read rule narrows the exception: it routes every diskless partition to gateways and sends only a read that a replica alone can serve to a replica. A fetch below the WAL start, the first offset still in the diskless WAL, gets KIP-392's `preferredReadReplica` with no records from fetch version 11, and the gateway forwards it to the replica for versions 4 to 10. A `ListOffsets` or `OffsetForLeaderEpoch` lookup that needs the local log or the tiered index is forwarded, as Inkless already forwards `DeleteRecords`. A catch-up read inside the WAL stays on the gateway's cold path. The redirect costs the consumer one extra round trip per partition per `metadata.max.age.ms` while it reads below the WAL start. Inkless keeps per-partition replica routing for consolidating and tiered partitions, and implementing the per-read rule there is [Future work](#future-work). On two production services with consolidation on, local-log reads are 0.035% and 0.00014% of `BytesOut`.

Clients with an unknown AZ: a client whose AZ resolves to `null` counts as a client of the rack-less group, and the fallback order runs unchanged. A client resolves to `null` when its client ID has no `diskless_az`, its listener has no AZ mapping, or its AZ string matches no rack. In a cluster with no racks, step 1 picks a gateway. In a mixed cluster, the rack-less brokers serve these clients first. When every broker has a rack, step 3 picks the gateway of the coordinator partition with the highest log-key rendezvous across racks, so the producer and consumer of a user partition reach one broker. Without the client's AZ no rule can lower cross-AZ bytes, and about 2 picks in 3 cross an AZ with 3 racks under any rule. This rule only makes sure the pick already sends a part for the coordinator partition.

No replica preference: the gateway path doesn't prefer replicas in the client's AZ, and replica assignment isn't an input to the fallback order. Managed replicas of a partition without remote storage hold no data, and produce, fetch, `ListOffsets`, and `OffsetForLeaderEpoch` run on any broker through the diskless path and the control plane. Reassigning a replica therefore no longer moves client traffic. The per-broker exclusion and the gateway switch are the operator's levers.

Misroutes: a batch is misrouted when the receiving broker isn't a gateway of its coordinator partition in its own rack, or its own group for a rack-less broker. A cross-AZ fallback to a gateway of the same coordinator partition isn't a misroute, because it adds no commit part.

- On the gateway path, a broker rejects a misrouted batch with `NOT_LEADER_OR_FOLLOWER`, before writing it, at most once per connection and partition per `metadata.max.age.ms`. It accepts and commits the batch otherwise. Other partitions in the same produce request commit as usual.
- The rejection never applies to replica-routed partitions, never to fetches, and never with the gateway switch off.
- `NOT_LEADER_OR_FOLLOWER` is an `InvalidMetadataException`, so the producer refreshes at once and retries. Idempotent sequences stay intact, because the broker rejects before it writes.
- The guard keeps every false positive to one retry, then a commit, so a produce that reaches any broker still commits. False positives come from steps 2 and 4 of the fallback order, image skew between brokers, and a rolling upgrade that changes the rule. The state is a map per connection from partition to the time of the last rejection, and `KafkaApis` passes the connection ID to the produce path.
- A crash or controlled shutdown drops connections, so the stale window follows only moves away from a live broker: a return that takes slots back from stand-ins, extra moves among live brokers, joins, and exclusions. Without the rejection, a client keeps producing to a broker that lost its gateway slot until `metadata.max.age.ms`, 5 minutes by default. In a 24-broker example, that's up to about 139,000 extra parts per rolling restart, about as many as the routed parts in the same 24 minutes. With the rejection, each producer of a moved coordinator partition pays one retry backoff plus a metadata round trip, and the extra parts drop to a few per move.
- `MisroutedBatchRejectRate` counts rejected batches. `MisroutedBytesRate` and the `misrouted` value of `CommitPartRate` count accepted batches only.

Fenced and cut-off brokers: diskless produce gets no fencing check, and the broker's own image decides.

- After its first unfence, the controller fences a running broker only on a missed session or a shutdown, and a starting broker opens its client sockets only after the controller unfences it.
- A broker that paused, or whose heartbeats the controller processed late, applies its own fence record. It isn't in its own candidate set, so it rejects its first misrouted batch per connection and partition, and its metadata answers name the stand-in until its next heartbeat unfences it.
- A broker cut off from the controller never sees its fence in its image, because a running broker ignores `isFenced` in heartbeat responses. It keeps counting itself the gateway and commits its clients' produce while it reaches the coordinator leaders. Its clients drift to the stand-in as they refresh from other brokers. The cost is up to one extra part per file for the coordinator partition, cache misses for consumers that split across the two brokers, and uneven load. The controller's `FencedBrokerCount` and the broker's `last-applied-record-lag-ms` show the cut-off.
- A broker that also can't reach the coordinator leaders fails its commits with `KAFKA_STORAGE_ERROR`, and clients refresh. A client that refreshes from that broker can fail its batch at `delivery.timeout.ms`, as with a classic leader that's cut off under `acks=all`. Nothing acknowledged is lost.

### When routing turns on

Gateway routing is on only when three image inputs hold:

- The coordinator feature is finalized at level 1 or higher in `FeaturesImage`.
- `__diskless_metadata` is in the image, which gives `N`.
- `diskless.gateway.routing.enable` is on.

Otherwise the transformer uses the hash of the user partition, and the misroute rejection is off. Every input comes from the image, so brokers agree, and the rejection's guard turns the skew at each change into at most one retry. The finalized feature names the backend and never changes at runtime. Before the controller creates `__diskless_metadata`, there's no `N`, and coordinator calls get a retriable error, so produce fails whatever the routing.

### Switch and exclusion

`diskless.gateway.routing.enable` turns gateway routing, and the rejection with it, on or off for the whole cluster:

- It's a boolean that defaults to `true`, with no broker-count threshold. It applies only on clusters that chose the coordinator backend.
- It can be set only at the cluster default, the broker resource with an empty name. It's registered in `ServerConfigs` and `DynamicBrokerConfig.ALL_DYNAMIC_CONFIGS`, and brokers read it only from the cluster-default resource in `ConfigurationsImage`, with the built-in default when no record exists. Every broker flips at the same image offset.
- The controller's `ControllerConfigurationValidator` rejects a per-broker value with `INVALID_CONFIG`. A broker refuses to start with a static value in `server.properties`, and the error names the `kafka-configs --entity-default` command. Kafka's default precedence lets a per-broker or static value win on that broker, which would split traffic between brokers that disagree.
- The deployment docs say to leave it on and not to flip it for rolling restarts or upgrades, including an upgrade that changes the rule. An operator turns it off for a hot gateway that hurts produce latency, shown by `BytesInPerSec` against the rack mean and `TotalTimeMs` P99 on that broker, or for a suspected routing defect, shown by a steady nonzero `misrouted` or `empty-tracking` rate or a rejection count that keeps rising outside membership changes. The operator turns it back on once the cause is gone.
- A flip off rejects nothing, and producers drift back to the hash as their metadata expires, so parts per file climb over up to `metadata.max.age.ms`. A flip on moves most user partitions, and each producer's in-flight batches to non-gateways get one rejection each.

`diskless.gateway.candidate.enable` excludes one broker from gateway candidacy:

- It's a boolean that defaults to `true`, and `false` excludes the broker whatever its state, for a degraded broker or one about to be decommissioned.
- It's a per-broker dynamic config only, set through `AlterConfigs` on the broker's config resource. The controller rejects a cluster-default value with `INVALID_CONFIG`, and a broker refuses a static value at startup. Every broker must compute the same assignment, so the exclusion must come from the image: a static value reaches only one broker, and a cluster default of `false` removes every candidate.
- The excluded broker's coordinator partitions move by the rule, as on a fence, and the fallback order skips it unless no other broker can serve.

### Tracking partition under gateways

A broker's tracking partition is one of its own gateway coordinator partitions, as [Tracking partition](#tracking-partition) defines the selection. A routed file then sends no extra part for tracking, the tracking partition is a participant of every routed object, so its releases are local and send no release report, and tracking load evens out.

These are averages over random clusters at `N=8`, `R=3`, and 3 racks, for routed files with no misroutes. A round is one file per gateway. The last column is for a tracking partition fixed by a hash of the broker ID, which ignores the assignment and matches the broker's gateway coordinator partition at saturation with a probability of `1 / N`.

| Brokers | Parts per file on a gateway | Coordinator log flushes per file, upper bound | Parts per coordinator leader per round | Fixed tracking partition: parts per file and per leader |
| --- | --- | --- | --- | --- |
| 3 | 8 | 24 | 3 | 8 and 3 |
| 12 | 2.0 | 6 | 3 | 2.77 and 4.2 |
| 24 | 1.0 | 3 | 3 | 1.88 and 5.6 |
| 45 | 1.0, and spares upload nothing | 3 | 3 | 1.88 and 5.6 |
| 48 | 1.0 | 3 | 6 | 1.88 and 11.3 |

With gateways off, every file under the base workload touches all 8 coordinator partitions, so it sends 8 parts at every size, and each leader receives one part per broker per round. With gateways on, the cluster sends `N × n_racks × g` parts per round, against `B × N` with gateways off. Only at 1 broker per rack do gateways change nothing.

Costs of the selection:

- A broker switches its tracking partition when it loses that coordinator partition in the assignment and closes a file with no batch for it. Each switch is one lease request at the new partition and one lease release at the old one, with `N` lease-end range fences merged into pending fence requests, about 18 records at `N=8`. A switch abandons between `K/2` and `K` unused sequences, and their keys, mostly never written, go to the deletion queue.
- With the misroute rejection, stale traffic ends within one retry, so a rolling restart of 24 brokers costs about 90 switches, about 1,600 records and 720 range fences, plus 2 to 4 empty parts per change. The 24 epoch ends of the same restart already cost 192 range fences.
- When a routed file's only part fails, its sequence becomes missed and costs `N` range fences instead of one exception fence, as any failed tracking part does.
- A broker with an empty gateway set gets produce only from stale traffic, from a promotion, from step 2 of the fallback order, and from local-log partitions. A spare's first promotion costs at most one switch per epoch, and a demotion costs none.

### Gateway load

The assignment counts only brokers, with no traffic weighting, because every broker must compute the same answer and each measures only the traffic that reaches it. A gateway carries whole coordinator partitions, so its peak exceeds per-user-partition routing. With 512 equal user partitions per rack at `N=8`, the busiest gateway carries about the same as today's busiest broker at 3, 4, 8, and 16 per rack, 1.2 times at 5, 1.4 at 6 and 12, 1.6 at 7, and 1.7 at 15. At 7 per rack, the cap of 2 gives one broker about a quarter of the rack's traffic. [Accepted limits](#accepted-limits) records the cost, its signal, and its levers.

### Rejected alternatives

Candidates and leadership:

- The alive-broker list filtered by the request's listener. Each listener would get its own assignment, and the produce path, which has no listener, couldn't tell a misroute from a routed produce.
- Excluding brokers in controlled shutdown. Clients with cached metadata keep sending to the broker while it moves its leaderships, so its coordinator partitions split between it and the successor.
- Making each coordinator partition's leader the gateway in its rack. It saves one intra-AZ round trip per round in the leader's rack only. Every election moves the gateway in two racks, the leader carries its rack's client traffic on top of the coordinator work, and gateway balance follows leader placement.

Count and rule:

- Every broker a gateway for at least one coordinator partition. Under light traffic, each broker that receives produce uploads a file per commit tick, 40 files per second against 32 at 10 brokers per rack and `N=8`, and the busiest broker still carries `1/N` of the rack.
- Round-robin over sorted broker IDs. One new ID shifts every later position, 6.6 slots on average where balance needs 2.
- Plain rendezvous hashing. Balance is left to chance: at 3 brokers and `N=8`, about 1 cluster in 4 puts 5 or more coordinator partitions on one broker.
- Consistent hashing on a ring, jump consistent hash, Maglev-style turns, and stable matching. The ring has the same chance balance, jump hashing renumbers brokers after a departure, the turn order shifts on a join, and stable matching with one symmetric score gives what the capped walk gives.
- An assignment that the controller writes into the metadata log. It could move only what balance needs and weigh load, but it costs a new metadata record type and controller code that rewrites the assignment at every fence and registration, for about one move saved per membership change.
- Gateways that follow the replica placement of `__diskless_metadata`. It needs a second rule while a replica's broker is fenced and from `2N` brokers per rack up, and a broker added in a scale-out gets no gateway until an operator reassigns the topic.
- An operator-defined assignment in a dynamic config. Every scale-out, failure, and rack change needs a manual step.
- An assignment that each broker remembers locally. A restarted broker computes a different answer from its peers.
- A `MetadataVersion` gate on rule changes, or a rule selected by config. The first adds version machinery for a cost that lasts one upgrade. The second splits traffic like a rule change and makes the interface a public extension point.
- A cluster-wide count, `B / N / n_racks`. Uneven racks would get a count that fits none of them.
- Excluding rack-less brokers whenever some brokers have racks. The rule then needs a special case, and it differs from how `StripedReplicaPlacer` treats rack-less brokers.

Fallback and load:

- A gateway of the same coordinator partition in another rack before another broker in the client's rack. It saves a commit part but moves every produced and fetched byte across AZs.
- The hash of the user partition over the brokers in the client's AZ, then over all brokers. It ignores coordinator partitions, so every fallback is a misroute.
- A modulo over a sorted list to choose among several brokers. A change in the list reshuffles every user partition.
- Co-location mirroring, where the gateways of coordinator partitions led by one broker share a broker in every other rack. The coordinator sends one commit part per coordinator partition and flushes each log separately, so mirroring saves no requests and no flushes, and it ties gateways to leadership.
- Weights from each broker's own traffic, or from a controller-written record. Brokers see different traffic and would compute different gateways, and a controller record has the cost of a controller-written assignment.

Routing policy:

- Naming remote storage, consolidation, and the switch from classic topics as the general reasons for replica routing. The rule that matters is whether a fetch needs a broker-local log.
- Placing replicas on the gateways of their coordinator partition. Every join, fence, return, and exclusion moves gateway slots without moving replicas, so placement drifts and partitions misroute until a reassignment.
- A per-partition rule that keeps a partition on replica routing whenever any of its reads needs a local log. Those reads are under 0.04% of bytes on today's consolidation services, and the rule gives up the gateway saving for every produce to the partition.
- Forwarding every fetch that needs a replica instead of a redirect. Every response takes a broker hop for as long as the consumer reads old data.
- An error that makes the client refresh its metadata for a read that needs a replica. The refresh returns the gateway again, so the client loops.
- The hash over all alive brokers for clients with an unknown AZ. It has the same cross-AZ share, but 67% to 93% of their user partitions misroute.
- The gateway in the coordinator leader's rack for clients with an unknown AZ. Every election moves these clients, and it saves none of their cross-AZ bytes.
- Accepting the stale window after a gateway move, or recommending a lower `metadata.max.age.ms`. The first costs up to about 139,000 extra parts per rolling restart at 24 brokers. The second multiplies metadata requests and helps only clients that follow the docs.
- The misroute rejection without a guard. Image skew or a rule change can loop a client between brokers that disagree until `delivery.timeout.ms` fails the batch.
- Rejecting misrouted fetches too. The fetch cost of the stale window is object-storage reads, not commit parts.
- Refusing diskless produce once a broker's heartbeats have failed for longer than the session timeout. A controller-quorum outage would then stop every broker's diskless produce, while classic topics keep serving.
- Using that trigger only to stop naming itself in metadata responses. Each broker would compute a different assignment, so the rejection fires on every broker and traffic splits.
- Keeping the replica preference ahead of the gateways. Rack-aware placement puts a replica in every rack when the replication factor is at least the rack count, so the gateway step is never reached.
- The replica preference as a tiebreaker among several gateways. It replaces the stable log-key pick with a rule that changes on every reassignment, for no data locality.

Switch-on, switch, and exclusion:

- Turning routing on from the broker's `inkless.control.plane.class`. It's a local config outside the image, which breaks the image-only rule.
- A static per-broker switch. A flip needs a rolling restart, and brokers disagree for its length.
- Off by default, with operators turning it on. Every coordinator cluster pays 4 to 15 times the parts and flushes until someone sets a config.
- On above a broker count, or only at rack sizes that don't concentrate load. The worst load size, 45 brokers, sits above any sensible threshold, and every scale event across the threshold costs every producer a retry.
- The names `diskless.metadata.gateways.enable` and `diskless.produce.gateways.enable`. Routing lives in the transformer and the produce path, not in coordinator state, and fetches move with produce.
- Turning the switch off for every rolling upgrade. It costs 8 parts per file for the whole upgrade and a retry round afterward, more than the split traffic it avoids.
- Reusing KIP-1066's `cordoned.log.dirs` for exclusion. It couples "place no new replicas here" with "route no clients here", and a cordon acts only on future placements.
- A cluster-wide list of excluded broker IDs, or a new admin RPC. IDs of departed brokers linger in the list, and an RPC is public protocol for what a dynamic config does.
- `diskless.gateway.exclude`, default `false`. It breaks the `.enable` pattern and inverts the boolean.

Tracking partition:

- A tracking partition fixed by a hash of the broker ID. At saturation, a file sends about 1.88 parts and up to `1.88 × R` flushes instead of 1 and `R`, and every routed object needs a release report across partitions.
- Choosing each file's tracking partition among its participants. The broker would need live leases at several partitions and take sequences from them in increasing order, which disjoint ranges don't allow.
- Dropping the empty tracking part and registering commits at the tracking partition later. The tracking partition learns participant sets late, and missed-sequence detection loses its basis.
- Always the highest-scoring member of the gateway set. It switches whenever a higher-scoring member joins and switches back on the return, about twice the sticky rule's switches below saturation.
- The member with the most local traffic, or the lowest-numbered member. The first needs hysteresis and a measurement window, and the second piles tracking load onto low-numbered partitions.
- A switch on the image change. It switches while stale traffic makes the switch useless.
- A switch at the next threshold renewal. Up to `K/2 = 512` files send an empty tracking part before the renewal.
- A switch after a grace timer such as `metadata.max.age.ms`. The wait is too long when clients refresh early and too short when they don't.
- Never switching within an epoch. After any change, the broker pays a second part per file until it restarts.
- A hash of the broker ID for an empty gateway set. The preference moves away on every demotion, so a fence followed by a return can cost a spare two switches.
- The coordinator partition the broker would most likely stand in for. It reruns the capped walk `N` times per image to save one switch per spare and epoch.
- Following traffic on every empty part. Scattered misroutes make the broker switch on almost every file.
- The order `t + 1` and onward after the preferred partition. Below saturation it adds an empty part per file during an outage, and at saturation it sends all the gateways of one coordinator partition to the next one.
- A return on every empty part. During an outage, it sends up to 4 lease requests per second per broker to a partition that can't answer.
- A separate `EmptyTrackingPartRate` meter, or a gauge for whether the tracking partition is in the gateway set. The first is a second place that counts parts, and the second shows a harmless lag.

## Capacity and isolation

Each coordinator partition's leader serves every commit part, lease request, keepalive, release report, fence, cache-miss `findBatches` and `listOffsets` call, and maintenance job for its user partitions and tracked brokers, on top of the broker's own partitions. `N` leaders share the cluster's load, and Kafka's leader placement spreads them across brokers.

Splitting reduces the work per batch, not the number of commit calls. With gateways off, an object usually holds batches for most coordinator partitions, so each leader receives about one commit part per broker per commit interval. With gateways on, each leader receives parts only from its gateways, `n_racks × g` per round, about `B / N` once racks reach saturation. Off-gateway and misrouted traffic can still send a leader up to about one part per broker per round. A produce request waits for the slowest part that holds its batches, so the commit-latency tail grows with the number of coordinator partitions a request touches.

The load differs from the Postgres backend in two directions:

- Lower cost per call: SQLite runs in the broker process, so a call has no network round trip, connection pool, or remote query planning. The Postgres backend spends part of its commit latency on those.
- Co-tenancy: SQLite shares the leader broker's heap, page cache, disk, and garbage collector with that broker's own partitions. A large `findBatches` scan can evict the broker's log pages. A garbage collection pause on that broker stalls every commit that has a part on the coordinator partitions it leads. The Postgres backend runs on its own host and has none of this contention.

A cluster-wide consumer restart empties the batch coordinate cache on every broker at once. Every miss then reaches the leaders together.

### Load bounds

No admission cap on the leader and no retry budget in the delegate bound the load. In-flight limits on each broker do, and each limit is a requirement with its own test:

- Commit parts: one in flight per commit queue, because a part runs only after the previous file's part on that queue finished. Producer retries land in later files, so they add batches, not parts.
- Lease requests: one renewal at a time per broker.
- Release reports: one in flight per participant and tracking partition.
- Fences: one batched request in flight per tracking partition and participant.
- Read parts: at most 8 in flight per coordinator partition on each calling broker.

A coordinator leader therefore receives at most about `B` commit parts, `B` lease requests, one fence batch from each other tracking partition, and `8B` read parts for each coordinator partition it leads. Every limit is per commit queue or per coordinator partition, so none of them lets work stuck on one coordinator partition block another.

- On the shard, commit parts, lease requests, release reports, and fences stay first in, first out. The limits keep that queue short, so the shard needs no priority between them. Reads run in the read pool, off the shard.
- Every private RPC that the delegate retries waits between attempts with the backoff in [Transport](#transport). A slow leader causes no retries. Only fast errors do, such as a full overlay, a closed high-watermark gate, or an `acks=-1` append under the minimum ISR, and the backoff covers those.
- Nothing sheds load except the overlay cap of `4 × B` entries in [Speculative state](#speculative-state), which only trips on a bug or a stalled high watermark.
- `PrivateRpcRetryRate` shows a leader that fails fast, `OverlayCapRejectRate` shows the cap, and `ReadPartFailRate` shows read parts that fail.

Rejected alternatives:

- An admission cap on the leader, such as a queue-depth or queue-time limit that sheds with a retriable error. It duplicates the in-flight limits, and shedding commit parts turns slowness into failed produce requests sooner.
- A retry budget in the delegate, such as a token bucket per coordinator partition. The backoff already covers fast errors.
- One in-flight limit for the whole broker. Work stuck on one coordinator partition would block requests that don't touch it.
- Configs for the backoff. Nobody would tune them, as with `K` and the barrier deadline.

### Benchmarks

The benchmarks make the performance bar testable. Every benchmark has a pass rule, and every possible result maps to a new default, a docs change, a release blocker, or a separate effort. Nothing is left to judge after a run. Running them is out of scope for this design.

Baseline and pass rule:

- The baseline is the Postgres backend as Inkless deploys it in production: a managed primary on its own host, with its production replication settings. The follower flush that Kafka parity requires counts fully against the coordinator. The report records the baseline's `synchronous_commit` and standby settings next to the result.
- Produce latency is Kafka's per-request produce metric, `RequestMetrics` `TotalTimeMs` for `request=Produce`. `FileCommitTime` measures the slowest part of a file, so it's recorded, not judged.
- A latency comparison passes when the coordinator's P50 and P99 are each at most 1.1 times Postgres's, at the same produce throughput.
- Each side runs 3 times, 30 minutes each, after a 10-minute warmup, and the comparison uses the median of the 3 runs.

Sizes and base workload:

- Sizes: 3, 12, and 24 brokers, spread evenly over 3 racks with one rack per AZ. With `N=8` and `R=3`, every broker hosts all 8 coordinator replicas at `B=3`, about 2 at `B=12`, and about 1 at `B=24`. 24 is also `N × n_racks`, where racks reach saturation.
- Produce: 16 MiB/s per broker. A file closes after 250 ms or 8 MiB, so each broker commits 4 files per second on the timer.
- Topic shape: 64 diskless partitions per broker, so with gateways off each file holds batches for all 8 coordinator partitions, the worst-case fan-out. At the gateway switch's default, a file sends about 8 parts at 3 brokers, 2 at 12, and 1 at 24.
- Producers: one per broker, in the broker's AZ, with `acks=all`, 1 KiB records, `linger.ms=100`, `batch.size=16384`, and the default sticky partitioner. Each producer writes every diskless partition in the cluster. A 250 ms file at 16 MiB/s holds about 256 batches and reaches every coordinator partition.
- Consumers: one group that reads every partition at the tail.
- The Postgres run uses the same brokers, producers, and consumers, with only the control plane backend switched. The coordinator runs use the gateway switch's default.

The benchmarks, each with its pass rule:

| Benchmark | Runs | Pass and decision rule |
| --- | --- | --- |
| Partition count, run first | The base workload at `B=3` with `N` of 1, 2, 4, and 8, and at `B=12` and `B=24` with `N` of 4, 8, and 16. Records produce latency, P99 `FileCommitTime`, parts per leader per second, appends per commit part under adaptive linger, flushes per broker per second, and CPU on leader brokers. | If `N=8` passes at all three sizes, the default stays 8 and isn't provisional anymore. If it fails only at `B=3`, the default stays 8 and the deployment docs recommend the largest passing `N` for clusters that won't grow past the size where 8 passes. If it fails at `B=12` or `B=24`, the default becomes the largest `N` that passes at both sizes. If `N=16` passes at `B=24` and does better than 8, the docs recommend 16 for clusters expected to reach that size. |
| Commit throughput | A rate sweep at each size, from 16 MiB/s per broker in 8 MiB/s steps, and a broker sweep at 16 MiB/s from 24 to 96 brokers in steps of 12. A backend saturates at the highest load at which its produce P99 stays within 2 times its own base P99 with a zero `FileCommitErrorRate`. | The coordinator's saturation rate is at least Postgres's divided by 1.1 at each size, and in the broker sweep it reaches at least the broker count where Postgres saturates. The broker count where `N=8` saturates goes into the deployment docs' guidance to choose `N` for the expected peak cluster size. |
| Partial failure | The base workload plus a narrow producer per broker at 1 MiB/s that writes only to user partitions of one healthy coordinator partition. A test-only fault hook on another coordinator partition `P`, not any broker's tracking partition, makes its leader accept no part for 5 minutes, then delays every append by 500 ms for 5 minutes. All three sizes. | Against the coordinator's own steady phase: narrow requests keep P99 within 1.1 times with zero errors; in the unavailable mode, a request that touches `P` completes within one commit deadline of its file's close, with `KAFKA_STORAGE_ERROR` for `P`'s batches only; parts on healthy coordinator partitions keep P99 within 1.1 times; and produce P99 returns within 1.1 times within 60 seconds after the fault ends. |
| Read-heavy load | The base workload plus one lagging consumer group per broker that misses the batch coordinate cache, reading at 1 times and then 3 times the produce rate. All three sizes. | `FindBatchesTime` P50 and P99 and produce P50 and P99 against Postgres under the same read load. |
| Cache storm | Under the read-heavy load at 1 times, every consumer restarts at once. All three sizes. | The time until `FindBatchesTime` P99 returns to steady, produce P99 during the storm, and the time until every consumer is back at its fetch rate, each within 1.1 times Postgres's. Read parts that fail the 1000 ms deadline cost retries, and this check prices them. If the read-heavy or storm check fails and the read pool's queue time dominates `FindBatchesTime`, both run again with 8 and then 16 read threads, and `diskless.metadata.read.threads` defaults to the smallest count that passes. |
| Co-tenancy | The base workload, the read-heavy load at 1 times, and a classic topic at 16 MiB/s per broker with `acks=all` and replication factor 3. Records GC pause P99 and `LogFlushRateAndTimeMs` to explain a failure. | At 12 and 24 brokers, brokers that lead a coordinator partition keep classic and diskless produce P50 and P99 within 1.1 times the brokers that lead none. At all three sizes, each broker's classic produce P50 and P99 stay within 1.1 times its values in the Postgres run. |
| Failover | A `kill -9` and a controlled shutdown of a broker that leads coordinator partitions, against a hard kill and a switchover of the Postgres primary. All three sizes. | The time to the first commit behind the affected coordinator partitions, and the time until produce P99 returns within 1.1 times steady, are each at most 1.1 times Postgres's. A verifying consumer finds zero acknowledged records lost, a check that also runs in the partial-failure, co-tenancy, and snapshot-transfer benchmarks. |
| State size | \(10^6\), \(10^7\), \(10^8\), and \(10^9\) batch rows preloaded at `B=12`, with and without consolidated topics, under the base workload. Records SQLite file size, snapshot backup duration, `SqliteWalPages` during the backup, and log growth between snapshots. | `FindBatchesTime` P50 and P99, election time, and produce P50 and P99 against Postgres at the same row count. The largest passing step becomes the stated limit on batch rows per coordinator partition, which the docs turn into the retention and produce rate that stay under it. |
| Snapshot transfer | At `B=12`, preloaded at the largest passing state-size step, with the base and read-heavy loads: one follower loses its storage; both followers of one coordinator partition lose theirs; and a whole broker loses its storage at `B=3` and `B=12`, with transfer limits of 1, 2, and 4. | One rebuild keeps produce and `FindBatchesTime` P99 on the leader and the source within 1.1 times steady, and produce P99 within 1.1 times Postgres's during a standby rebuild. Two rebuilds meet the partial-failure criteria, with the time below the minimum ISR at most 1.1 times one rebuild. `diskless.metadata.snapshot.max.concurrent.transfers` defaults to the highest limit at which the one-rebuild pass holds on the sources. If replay after the snapshot takes more than half of one rebuild, the snapshot byte trigger drops to 256 MiB. |
| Retention burst | At `B=12`, 24 hours of base-workload objects expire at once. | Produce P50 and P99 stay within 1.1 times steady until `PendingReleaseCount` returns to zero. The drain time is recorded next to Postgres's `FileCleaner` time for the same backlog. |
| Inter-AZ traffic | Inter-AZ bytes per produced MB from network counters by source and destination AZ, broken down by commit parts, follower copies, and the lease, finish-report, release, and fence RPCs. All three sizes and the retention burst. | At most 1.1 times Postgres's at each size. |
| Gateway pairs | The coordinator with the gateway switch on and off at 12, 21, 24, 45, and 48 brokers. | On-side `TotalTimeMs` P50 and P99, cluster-wide and on the busiest gateway, and inter-AZ bytes per produced MB are within 1.1 times the off side. Steady parts per file are within 1.1 times the run's predicted value, `N × n_racks × g` divided by the brokers with a non-empty gateway set. |
| Clients without `diskless_az` | A gateway pair at 24 brokers. | As for gateway pairs. |
| Local-log mix | On-only runs at 24 brokers with 1%, 5%, and 10% of traffic on consolidating topics. | `TotalTimeMs` P50 and P99 within 1.1 times the 24-broker off side. Parts per file are recorded against the simulated curve. |
| Rolling restart | Gateway pairs at 24 and 48 brokers, one restart per run at 60 seconds per broker. | Zero `record-error-rate`, client-side P50 and P99 within 1.1 times the off-side restart, and parts per file back within 1.1 times the prediction within 60 seconds of the last return. |
| Switch flip | An on-side run at 24 brokers, flipped off at 10 minutes and on at 20. | Zero record errors, parts per file at the off value within `metadata.max.age.ms` plus 60 seconds after the flip off and within 1.1 times the prediction within 60 seconds after the flip on, and at most one rejection per producer connection and user partition. |

Every run records control traffic per tracking partition: tracking parts, finish reports, lease renewals and keepalives, release-report ranges, and `FenceRate` by type. On every steady-phase run, `FenceRate` is zero for the missed and exception types, and a nonzero rate fails the run as a protocol defect. The inter-AZ estimate: a 4 MiB file holds about 256 batches, about 25 KiB of coordinator records, or 0.6% of the produced bytes, and the coordinator sends about 2.7 times the record bytes across AZs, far below a classic topic's 2 MB per produced MB.

The gateway runs judge produce `TotalTimeMs` P50 and P99 cluster-wide and on the busiest gateway, the broker with the highest `BytesInPerSec`, because the cluster percentile dilutes one hot broker. The rolling restart judges client-side latency with the producer's `record-retry-rate` and `record-error-rate`, because a rejected request's `TotalTimeMs` is short and the retry arrives as a new request. Parts per file are the sum of `CommitPartRate`'s four kinds over the file count from `FileCommitTime`. The runs also record P99 `FileCommitTime`, `LogFlushRateAndTimeMs` per broker per round, parts per leader per round, peak `BytesInPerSec` against the rack mean, each `CommitPartRate` kind, `MisroutedBytesRate`, `MisroutedBatchRejectRate`, `assignment` renewals, the leaders' `event-queue-time-ms` and `batch-flush-time-ms`, and CPU on gateways and leaders. At 12 and 24 brokers, the off sides of the gateway pairs are also compared with the Postgres runs in the 1.1 band.

What a result changes:

- Tunable, for `N`, read threads, the transfer limit, or the byte trigger: the matching rule sets the new default, and the affected benchmarks run again. No design change.
- Defect: a nonzero fence rate other than lease ends, a lost acknowledged record, a failed partial-failure criterion, a gateway parts-per-file breach, a record error in the rolling restart, or an inter-AZ failure in a gateway pair with `diskless_az` clients. It blocks release until fixed and doesn't reopen the bar.
- Co-tenancy: the design adds a required, separate log directory for the coordinator log and SQLite, on its own disk, and co-tenancy runs again. If it still fails, the deployment docs say to place coordinator leaders on brokers with a lighter classic load.
- State size or `findBatches`: the stated row limit drops to the last passing step. If \(10^6\) rows fails, `findBatches` gets a cap on result size, and the benchmark runs again.
- Base latency or commit throughput, after `N` is tuned: the coordinator backend ships as opt-in, and Postgres stays the default backend. The report states the gap and the share that comes from the follower flush. Closing the gap is its own effort.
- Inter-AZ against Postgres: doesn't block release. The deployment docs state the gap, and AZ-aware leader placement for `__diskless_metadata` becomes its own effort.
- Gateway latency fails only at 21 or 45 brokers, the load sizes: the switch stays on by default. The deployment docs name the load sizes, 5 to 7 and 9 to 15 per rack at `N=8`, the trade of up to 1.7 times today's peak on a few brokers against 7 to 15 times fewer parts, and the levers: racks of up to 4 brokers or a multiple of `N`, or the switch off.
- Gateway latency fails at 12, 24, or 48 brokers, where load is even: the switch defaults to off, routing ships as an operator opt-in, and the base comparisons at 12 and 24 brokers run again at the new default.
- An off side of a gateway pair fails against Postgres at 12 or 24 brokers: the default doesn't change and nothing blocks. The deployment docs say that turning gateways off at that size puts produce latency above the Postgres backend.

The Docker scenarios in [laptop-bench/](laptop-bench/README.md) gauge the main risks against the Postgres backend on one laptop before the coordinator exists. They measure the Postgres backend end to end and a proxy for the coordinator's commit path and SQLite, at reduced scale. They don't replace these benchmarks.

### Accepted limits

Performance limits aren't risks to data. Each limit here has a signal and a lever:

| Limit | Effect | Signal | Lever |
| --- | --- | --- | --- |
| A cluster outgrows its partition count | Each coordinator partition has one writer, and with `N` fixed, the load on each leader grows with the broker count, so the `N` writers cap commit throughput. With gateways on, each leader receives `n_racks × g` parts per round, about `B / N` at saturation, and off-gateway and misrouted traffic can raise that toward `B`. State per coordinator partition grows, which lengthens elections and snapshots. Correctness, the data-loss bar, and the 1/N blast radius don't change. | `event-queue-time-ms` and `batch-flush-time-ms` on brokers that lead coordinator partitions, and P99 `FileCommitTime` on every broker. | At creation, choose `N` for the cluster's expected peak size. After creation, reassign coordinator leaders to less loaded brokers or brokers with faster disks, within the rack-spread rule. Splitting partitions is [Future work](#future-work). |
| The busiest gateway carries up to 1.7 times today's busiest broker | A gateway carries whole coordinator partitions. At `N=8`, the busiest gateway carries about 1.2, 1.4, and 1.6 times today's busiest broker at 5, 6, and 7 brokers per rack, and 1.7 times at 15 per rack. | `BytesInPerSec` against the rack mean, and `TotalTimeMs` P99 on that broker. | Racks of up to 4 brokers or a multiple of `N`, or `diskless.gateway.routing.enable=false`. Extra gateways for a hot coordinator partition are [Future work](#future-work). |

## Metrics

The coordinator registers two metrics groups, each following its neighbors:

- `DisklessMetadataCoordinatorRuntimeMetrics extends CoordinatorRuntimeMetricsImpl`, with the group `diskless-metadata-coordinator-metrics`. `BrokerServer` registers it as it registers the group and share coordinators' runtime metrics. It brings `event-queue-time-ms`, `event-purgatory-time-ms`, `batch-flush-time-ms`, and the other runtime metrics unchanged. `event-queue-time-ms` and `event-purgatory-time-ms` bound the duplicate window. With reads off the shard, `event-queue-time-ms` measures write load alone.
- One Yammer `KafkaMetricsGroup` named after the backend class, with PascalCase names, as `PostgresControlPlaneMetrics` and `FileCommitterMetrics` do. A metric scoped to a coordinator partition carries a `partition` tag. A per-broker metric carries none. Meters end in `Rate`, rejection meters in `RejectRate`, and gauges in `Count`, `Size`, or `AgeMs`.

The backend's metrics, where "leader" is the coordinator partition's leader and "broker" is the uploading or calling broker:

| Name | Scope | Shows |
| --- | --- | --- |
| `UnknownOutcomePartRate` | Broker, `partition` tag | Parts that reached the commit deadline without a confirmation. |
| `StaleEpochRejectRate` | Leader | Requests that a broker sent before it restarted. |
| `StaleSequenceRejectRate` | Leader | Parts below the floor. Any nonzero rate from a registered broker is an anomaly, because a live broker never relies on the idle timer. |
| `PartitionMappingMismatchRate` | Leader | Requests rejected because a user partition doesn't map to the receiving coordinator partition. |
| `UnreconciledPartitionRejectRate` | Leader | Calls rejected with `COORDINATOR_LOAD_IN_PROGRESS` for an unreconciled user partition. A rate that stays high signals a leader whose image is stuck. |
| `LeaseRenewalRate`, tagged `reason` (`threshold`, `keepalive`, `failed-tracking-part`, or `assignment`) | Broker | Lease renewals by cause. `keepalive` means a keepalive got no answer. `assignment` counts tracking-partition switches. Plain keepalives get no metric. |
| `LeaseFallbackRate` | Broker | Renewals confirmed below the first entry of the tracking selection's ordered list. |
| `FileNoLeaseErrorRate` | Broker | Files failed with no leased sequence. |
| `FinishReportFlushRate` | Broker | Keepalives sent early to carry an urgent finish report. |
| `IdleLeaseEndRate` | Leader of the tracking partition | Leases ended by the idle timer. Any idle end is an anomaly, and a burst across many brokers points to a clock jump. |
| `FenceRate`, tagged `type` (`exception`, `missed`, or `lease-end`) and `answer` (`referenced` or `released`) | Leader of the tracking partition | Fence ranges issued and what they found. |
| `PendingFenceRangeCount` | Leader of the tracking partition | Fence ranges waiting to go out or for an answer. |
| `TrackingEntryCount`, tagged `state` (`pending`, `fencing`, or `held`) | Leader of the tracking partition | Tracking entries by state. |
| `TrackingEntryOldestAgeMs`, tagged `state` (`pending` or `fencing`) | Leader of the tracking partition | The age of the oldest entry in each state. A growing `fencing` age shows a fence that never answers. |
| `ReleaseReportRejectedRate` | Leader of the tracking partition | Release-report groups rejected because they fall outside the leased ranges. Only a bug sends one. |
| `ReleaseOutsideParticipantSetRate` | Leader of the tracking partition | Release reports or referenced answers from a partition outside the recorded participant set. Only a bug sends one. |
| `PendingReleaseCount`, `PendingReleaseOldestAgeMs` | Leader of a participant | Releases without a release-accepted record. |
| `DeletionQueueSize`, `DeletionQueueOldestAgeMs` | Leader of the tracking partition | A growing queue with a young oldest row means a clock running behind. An old oldest row means stuck deletes. |
| `BarrierRate`, tagged `outcome` (out of range, in range, or timed out) | Leader | A timed-out rate means the broker answers as a deposed leader. No other metric shows that. |
| `ApplyStoppedPartitionCount` | Broker, gauge | Coordinator partitions whose apply has stopped on an unreadable record or snapshot. |
| `SqliteWalPages` | Broker, `partition` tag, gauge | The WAL size of each coordinator database, in pages. |
| `ApplyLagBytes` | Broker, `partition` tag, gauge | The log bytes between the high watermark and the applied offset. |
| `OverlayCapRejectRate` | Leader, `partition` tag | Requests rejected at the overlay cap. Any nonzero rate means a bug or a stalled high watermark. |
| `PrivateRpcRetryRate`, tagged `rpc` (commit, lease, release, or fence) and `error` | Broker, `partition` tag | Retries after a retriable error. A sustained rate on one partition shows a leader that fails fast. |
| `ReadPartFailRate`, tagged `reason` (no leader, not coordinator, or deadline) | Broker, `partition` tag | Read parts that failed or passed their deadline, which consumers see as `KAFKA_STORAGE_ERROR`. |
| `CommitPartRate`, tagged `kind` (`routed`, `misrouted`, `empty-tracking`, or `off-gateway`) | Broker | Commit parts sent, by kind, recorded where the delegate splits a file by coordinator partition. Parts per file is its total over `FileCommitRate`. A steady `misrouted` rate points at the assignment or the transformer, and a steady `empty-tracking` rate at the tracking selection. |
| `MisroutedBytesRate` | Broker | Bytes of accepted misrouted batches, on the gateway path only. |

Two metrics are recorded outside the backend, so they join the group where their code runs:

| Name | Group | Scope | Shows |
| --- | --- | --- | --- |
| `CoordinatorPartitionsBelowRackSpread` | Kafka's `ControllerMetadataMetrics`, under `kafka.controller:type=KafkaController` next to `FencedBrokerCount` and `OfflinePartitionsCount`, computed from the image by `ControllerMetadataMetricsPublisher` | Controller, gauge | Coordinator partitions whose replicas span fewer racks than maximal spread, after a rack change. |
| `MisroutedBatchRejectRate` | `io.aiven.inkless.produce:type=WriterMetrics`, next to `RequestRate` | Broker | Misrouted batches rejected with `NOT_LEADER_OR_FOLLOWER`. The produce path rejects them before they reach the backend. |

The leader's read pool registers a `ThreadPoolMonitor` named `diskless-metadata-read`, so `queued-task-count-total` and `active-thread-count-total` under `io.aiven.inkless.thread-pool:type=diskless-metadata-read` show its queue size and active threads, as for the other Inkless pools.

`ClientAzAwarenessMetrics` keeps its names, and the gateway path records them this way:

- `fallback-total` counts answers from steps 2 to 4 of the [fallback order](#fallback-order), that is, answers that aren't the gateway in the client's rack. On replica routing it keeps its meaning, a broker that isn't a replica. `metrics.rst` describes it as "Requests not routed to the first choice", with both paths spelled out.
- The gateway path doesn't record `offline-replicas-routed-around`, because replicas aren't an input there.
- `client-az-hit-rate`, `client-az-miss-rate`, `client-az-unaware-rate`, and `cross-az-routing-total` keep their meanings. Steps 1 and 2 count as hits, and steps 3 and 4 as misses, and as cross-AZ when the broker's rack is confirmed.

The design also cites these existing metrics: `FileCommitTime`, `FileCommitRate`, `FileCommitErrorRate`, `FileUploadErrorRate`, `FindBatchesTime`, `PartitionStorageErrorRate`, `BytesInPerSec`, `RequestMetrics` `TotalTimeMs`, `LogFlushRateAndTimeMs`, `OfflinePartitionsCount`, `FencedBrokerCount`, `UncleanLeaderElectionsPerSec`, `ElectionFromEligibleLeaderReplicasPerSec`, the broker's `last-applied-record-lag-ms`, and the producer's `record-retry-rate` and `record-error-rate`.

Rejected alternatives:

- A third coordinator-side group for the gateway meters and the rejection count. The `empty-tracking` kind counts tracking parts, not routing, and the produce path would depend on the backend's metrics class for one counter.
- Read pool gauges in the backend group, such as `ReadPoolQueueSize`. They duplicate `ThreadPoolMonitor` and report this pool unlike every other Inkless pool.
- Kafka's `PerSec` suffix. It breaks the `Rate` pattern of every Inkless group.
- A separate `gateway-fallback-total` with `fallback-total` silent on the gateway path. The old metric would read as broken at switch-on.
- Counting any answer whose broker isn't a replica in `fallback-total`. On the gateway path that's most answers, so the metric no longer shows a fallback.
- A destination `partition` tag on `CommitPartRate`. It multiplies the meter's series by `N` on every broker, and the rule already gives every leader the same count.
- A per-coordinator-partition saturation gauge, or a clock-skew metric. The runtime and commit metrics already show saturation, and a skew metric would bring back a broker timestamp on every request.

## Leader change

- Each coordinator partition changes leader on its own. A leader change stalls only the commit parts, lease requests, reads, and tracked objects of that coordinator partition.
- A new tracking leader starts every idle timer at its election. It rebuilds the set of pending fences from recorded state, including the recorded finish reports, and sends one batched fence request per participant. A new participant leader resends the releases that have no release-accepted record.
- A new leader starts with an empty overlay. It rejects requests with a retriable error until its high watermark reaches the log end offset it saw at election and its applier has caught up to it. Once it serves, it reconciles its user partitions against its metadata image, as [Lifecycle](#lifecycle) describes, and checks the image for broker epochs to retire.
- The log discards records that the old leader didn't commit. The leader drops those entries from its overlay and fails their parked replies with a retriable error. Their requests were never acknowledged, so the retry rule in [Commit and retries](#commit-and-retries) covers them.
- The delegate retries the commit part against the new leader after a `NOT_COORDINATOR` or retriable error, with the same pair and object key and with the backoff, until the commit deadline passes.
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
- Each coordinator partition's leader reconciles its state against the metadata image. `BrokerMetadataPublisher.onMetadataUpdate` already delivers that image to every broker, and it is where the group and share coordinators learn about topic changes. On election and on each metadata update, the leader diffs the user partitions in its SQLite and overlay against the diskless user partitions in `MetadataImage` that map to its coordinator partition. A delta's `deletedTopicIds` isn't enough: a broker that loads a snapshot never sees a delete for a topic the process didn't hold.
- For each mapped diskless user partition in the image that the leader doesn't hold, it appends a partition-created record whose `metadataOffset` is the image's last contained offset. That offset is at or above the topic's `TopicRecord` offset.
- For each topic ID the leader holds that the image no longer has, it appends a topic-deleted record, but only when the image's last contained offset is at or past the highest `metadataOffset` among that topic's partitions in SQLite and the overlay. Below that, it skips the topic until the next metadata update. Topic IDs are never reused, so absence from an image past the creation offset proves the deletion. The case that matters most is a leader change: the old leader appends partition-created at image offset `M`, and the new leader's image is at `M' < M`.
- The partitions of one topic map to several coordinator partitions, so each of those coordinator partitions creates and deletes its own share of the topic, at slightly different times. KRaft already stops produce to a deleted topic at the broker. A leader that was down catches up when it returns, and the metadata log is the retry record, so the coordinator keeps no separate queue. The diff is by topic ID, so a topic recreated under the same name is a new ID and doesn't collide with the one being deleted.
- The diff counts pending overlay entries as well as SQLite rows, so two metadata updates that arrive before the first partition-created record applies don't append it twice. Apply is idempotent as a second guard. A partition-created record for a row that exists, or a topic-deleted record for a topic already marked deleted, is a no-op, and the applied offset still advances. The Postgres insert does the same with `onConflictDoNothing`.
- The admin client stops retrying a delete once the controller reports success, which is when the topic leaves the metadata log. It doesn't retry coordinator cleanup. Reconciliation retries it instead: a topic ID still in SQLite and absent from the image is retried on the next metadata update and on the next election.

Rejected alternatives for topic deletion:

- Waiting until the image reaches the newest `metadataOffset` in all of SQLite before reconciling. It delays creations for every other topic.
- Deleting only on an explicit delete delta. A broker that loads a metadata snapshot never sees the delete.

### Unreconciled partitions

A user partition is unreconciled at a leader when a call names it and neither SQLite nor the overlay holds its partition-created record. The leader can't see the caller's image, so the definition uses only the leader's state.

- Every call that names an unreconciled partition gets `COORDINATOR_LOAD_IN_PROGRESS` on the private RPC and appends nothing. This covers commit parts, `initDisklessLog`, `repairDisklessLog`, `deleteRecords`, `pruneDisklessLogs`, `advanceCrossTierLogStartOffset`, and reads. `UnreconciledPartitionRejectRate` counts each rejection.
- The delegate retries a commit part with the backoff until the commit deadline. After the deadline, the part gets `KAFKA_STORAGE_ERROR`, as any unconfirmed part does.
- The `initDisklessLog` queue retries it with its existing backoff. `InitDisklessLogState` already retries every error except `NONE` and `INVALID_REQUEST`.
- `repairDisklessLog` returns it to the admin client as retriable. `UNKNOWN_TOPIC_OR_PARTITION` keeps meaning "no log entry to repair."
- `findBatches` and `listOffsets` answer `OFFSET_NOT_AVAILABLE`, as the barrier does when it times out.
- A partition whose topic is marked deleted still gets `UNKNOWN_TOPIC_OR_PARTITION`, so a caller with a stale image stops retrying.

Rejected alternatives:

- A new error code. A fork-only number in upstream `Errors.java` collides with upstream's next code at every sync. No caller would act on it differently, and the metric gives operators the distinction.
- `UNKNOWN_TOPIC_OR_PARTITION`. It would blur "not created yet," "deleted," and repair's "not found."
- `NOT_COORDINATOR`. It sends the delegate to look up a new leader for no reason.

### Rows ahead of the image

These are all the paths that can write a coordinator row ahead of the leader's metadata image, each with its guard:

| Row | Written by | Guard |
| --- | --- | --- |
| User partition, with its batches and producer state | Partition-created, from the leader's own image | The `metadataOffset` gate on topic-deleted |
| Batches, producer state, and log starts | Commit parts, `initDisklessLog`, `repairDisklessLog`, `deleteRecords`, retention, prune, and cross-tier | Rejected without a partition-created record |
| Broker row at an epoch the image hasn't seen | A commit from a new registration | No retirement below the newest held epoch |
| Lease table entry at an epoch the image hasn't seen | A lease request | The lease table counts toward the newest held epoch |
| Broker row moved to `E` by an old leader | Broker-epoch-retired | A new leader with a lagging image computes `retiredThrough < E` and appends nothing |
| Switched partition before the seal reaches the leader's image | `initDisklessLog` | Partition-created follows only an image that shows the partition as diskless |

New partitions of an existing topic are safe, because the topic ID is still in the image. Kafka never deletes a single partition.

### Calls from partition leaders

- The leader of a switched partition calls `initDisklessLog` after the controller has committed the classic-to-diskless seal. `ControllerApis.handleInitDisklessLogRequest` writes that seal into cluster metadata. The control plane call happens later, from `InitDisklessLogManager` on the partition leader, through `AwaitingMetadata.sendBatch` in `InitDisklessLogState`.
- That same partition leader calls `repairDisklessLog` from `ReplicaManager`. `KafkaApis.handleRepairDisklessLogRequest` invokes `ReplicaManager`, which calls the control plane only while this broker leads the partition.
- Each call appends its record and replies once the high watermark passes it. A user partition's create, delete, initialization, and repair go to the same coordinator partition as its commits, so they are ordered with every commit to that user partition in one log. The log doesn't order commits across user partitions, and nothing depends on that order.

## Maintenance

- Retention and topic purge run on each coordinator partition's leader, for its own user partitions. Each run appends its outcome records to the log.
- Topic purge works through deleted partitions in capped passes, as `purge_deleted_logs_v1` does in Postgres. Each pass appends one deleted-log-purged record per partition it touched, in one log batch. Applying a deleted-log-purged record removes that partition's batches whose last offset is below the boundary, or all of them when the record has no boundary. When the record says the partition is empty, the apply also drops its `logs` and `producer_state` rows.
- The emptiness check in each batch-removing apply releases the objects whose last local batch it removed, and the participant reports them to their tracking partitions in batched release reports.
- Deletion runs on tracking partition leaders, through `FileCleaner` and the deletion queue, after the grace period.
- Every tracking partition leader runs its idle lease timers, and every leader appends broker-epoch-retired records from its metadata image when [Ending a broker epoch](#ending-a-broker-epoch) allows one.

## Consolidation and cross-tier offsets

- The broker that hosts a partition's local log decides the safe prune offset, from `highestOffsetInRemoteStorage` and the classic-to-diskless seal.
- The broker sends `pruneDisklessLogs` or `advanceCrossTierLogStartOffset` to the leader. The leader appends the outcome record.
- The broker advances its own prune floor from the response.
- The same pattern covers `CrossTierLogStartReporter` and the delete-records path.

The coordinator leader doesn't need a local log for any user partition, because it never reads one.

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
- The controller creates `__diskless_metadata` once enough registered, unfenced brokers exist for its replication factor, as [Topology](#topology) describes, with the partition count from `diskless.metadata.topic.num.partitions`, the replication factor from `diskless.metadata.topic.replication.factor`, the minimum ISR from `diskless.metadata.topic.min.insync.replicas`, and the fixed configs in [Topic protection](#topic-protection). Changing these configs afterward has no effect, and [Topic protection](#topic-protection) rejects every request that would change the topic.
- `inkless.control.plane.class` must name the new class. If it names the in-memory or Postgres class while the feature is finalized, the broker refuses to start.
- The broker refuses to start the coordinator backend when its bundled SQLite is below the version floor in [Checkpoints](#checkpoints).
- Diskless topics still need the existing Inkless storage configuration and `diskless.enable=true`.
- Gateway routing turns on once the feature is finalized, `__diskless_metadata` is in the image, and `diskless.gateway.routing.enable` is on, as [When routing turns on](#when-routing-turns-on) describes. The switch defaults to on, so a new cluster routes to gateways as soon as the controller creates the topic.

Upgrade and downgrade:

- `FeatureControlManager.reasonNotSupported` rejects an upgrade unless every registered broker and controller supports the new level, and registration rejects a broker that lacks a finalized level.
- The controller refuses every downgrade of both coordinator features, `SAFE_DOWNGRADE` and `UNSAFE_DOWNGRADE` alike. A name-keyed branch in `FeatureControlManager.updateFeature`, next to the `kraft.version` branch, does this. Without it, a safe downgrade from level N to N-1 leaves level-N records in the log and snapshots, and brokers rolled back to an N-1 binary stop on them. A downgrade to 0 would also switch the backend back to Postgres.
- Upgrade: roll every broker to the new binary, then raise the level. Raising the level is the point of no return.
- Aborted upgrade: the binaries roll back before the level rises. No leader has written a newer record, so the log stays readable. A rolled-back replica can still find a newer SQLite schema, and it rebuilds as [SQLite schema version](#sqlite-schema-version) describes.
- Binary downgrade below the finalized level: registration rejects the broker, so it never meets records it can't read.
- A later level that changes behavior but adds no records could allow a downgrade, as `MetadataVersion.checkIfMetadataChanged` does. The design can add that per level when such a level ships.
- There is no rollback onto Postgres. A downgrade would orphan the files and batches in `__diskless_metadata`, and no migration exists. Clusters already on Postgres are never formatted with the feature.
- A rolling upgrade that changes the gateway rule splits traffic between old-rule and new-rule brokers until it ends. The misroute rejection's guard limits the cost to one retry per broker, connection, and partition per `metadata.max.age.ms`. The deployment docs say not to turn the gateway switch off for it.

`INKLESS_OWNERSHIP` gains `Topic.java`, `Feature.java`, `FeatureControlManager.java`, `ClusterControlManager.java`, `ReplicationControlManager.java`, `ConfigurationControlManager.java`, `ControllerConfigurationValidator.java`, `ControllerMetadataMetrics.java`, `TopicsDelta.java`, `TopicDelta.java`, and `LogManager.scala` as `INTERLEAVED` entries where they aren't already, when the changes land. The probe rule touches `DelayedFetch.scala` and `ReplicaManager.scala`, and the misroute rejection touches `KafkaApis.scala`, which are already `INTERLEAVED` entries.

## Private RPCs

Numbers are assigned in the 504+ range at implementation time. Each request type and its handler is listed here. Every request names the coordinator partition it targets, and "Leader" means that partition's leader. The requests carry no tagged fields upstream, and a leader epoch added later is a new request version.

| Request | Caller | Handled by |
| --- | --- | --- |
| Lease sequences, which carries the uploader's key prefix and its finish report. A request for a range the partition already holds is a retry or a keepalive. | Broker's sequence source, down the tracking selection's ordered list, and as a keepalive at the partition in use | Leader of the tracking partition |
| Release lease, which names `b` and carries the final finish report for the abandoned ranges | Broker's sequence source, after it abandons ranges at a partition | Leader of that tracking partition |
| Commit file, one request per participant plus the tracking part. The tracking part carries the participant set and the finish report. | Broker, from `FileCommitJob` | Leader |
| Find batches, asynchronous, with a no-wait field for the delayed-fetch probe | Broker, on cache miss, within the read slots | Leader, from the read pool |
| List offsets | Broker, within the read slots | Leader, from the read pool |
| Delete records | Broker, from the delete-records path | Leader |
| Init diskless log | Partition leader, from `InitDisklessLogManager` after the controller commits the seal | Leader |
| Repair diskless log | Partition leader, from `ReplicaManager` | Leader |
| Enforce retention | Local only, on the leader | Not an RPC |
| Get files to delete, delete files | Local only, on tracking partition leaders | Not an RPC |
| Prune diskless logs | Broker, from `ConsolidatedDisklessLogPruner` | Leader |
| Advance cross-tier log start | Broker, from `CrossTierLogStartReporter` | Leader |
| Report releases, with `(brokerId, brokerEpoch)` groups of sequence ranges, one in flight per tracking partition | Participant leader's scheduler | Leader of the tracking partition, which answers each group |
| Fence sequences, with `(brokerId, brokerEpoch)` groups of merged ranges, one in flight per participant | Tracking partition leader, for an exception range, a missed range, or a lease end | Leader of each fenced partition |
| Fetch snapshot | Replica, when it needs records below the log start | An in-sync follower that holds a covering snapshot, or the leader when no follower does |
| Report snapshot offset | Replica's scheduler, after a snapshot, on a new leader, and on rejoining the ISR | Leader |

- Each request carries the coordinator partition and the topic partition, broker, or file it acts on, and it returns the same error codes as the `ControlPlane` method it wraps.
- A commit reply carries the `Recorded` flag: set when the answer comes from a recorded file-committed record, pending or applied, and unset for a rejection before the producer checks. The broker builds its finish reports from the flag.
- The stale-epoch and stale-sequence errors are non-retriable. A lease or commit with an unleased sequence, a negative sequence, or a mismatched retry gets `INVALID_REQUEST`. A release-report group outside the leased ranges gets a non-retriable error for that group only.
- A call that names an unreconciled user partition gets `COORDINATOR_LOAD_IN_PROGRESS`, and a commit or lease request that would push the overlay past its cap gets `COORDINATOR_NOT_AVAILABLE`. Both are retriable, and the delegate retries them with the backoff.
- The report and fence requests are capped at 512 KiB encoded, and the sender splits a larger set across consecutive requests.

## Failure modes

The failure-mode section holds two tables. Failure modes lists what the design fixes or tolerates, with the test that covers each row. Accepted risks lists the losses the design accepts, with why and what shows them. A row that closes a review gap names a test. Any other row names one or says why it has none. Test names refer to [Testing](#testing).

### Commits and leases

| Failure | Behavior | Test |
| --- | --- | --- |
| Leader crashes during a commit | The leader didn't acknowledge the part. The delegate retries against the new leader within the commit deadline. The broker row's `(epoch, sequence)` returns the stored result, or the retry commits as new. | Cluster: failover mid-commit |
| A retry arrives while the first attempt is pending on the leader | The retry waits for the pending record's result. Only one record and one set of offsets exist. | Cluster: retry while pending |
| A commit part misses the deadline, and its record commits later | Its batches get `KAFKA_STORAGE_ERROR`. The late record still makes them visible, unless the broker's next commit, a fence, or an epoch retirement raised the floor first. The part is an exception in the broker's next finish report, which is urgent, so its range fence closes the window about one commit interval after the deadline, even on a quiet broker. Idempotent producers deduplicate their retries. | Cluster: deadline then next part. Backend: urgent report on a quiet broker |
| One coordinator partition is stuck: unavailable, slow, or rebuilding its database | Requests with a batch for it complete at their file's commit deadline, with `KAFKA_STORAGE_ERROR` for those batches. Requests without a batch for it don't wait for it. A stuck leader broker extends this to every coordinator partition it leads. Producers retry the failed partitions, and the sequence rules resolve the unknown outcomes. | Cluster: one coordinator partition offline |
| A broker's tracking partition is stuck | A request with no batch at the tracking partition completes once its parts commit. The file stays unfinished until the tracking part finishes, so `finishedThrough` doesn't pass that file's sequence until then, and the failed tracking part starts a renewal down the ordered list. | Cluster: stuck tracking partition |
| The ISR of a coordinator partition drops below `min.insync.replicas`, including two of three replicas down | Appends fail at once with `NOT_ENOUGH_REPLICAS`, and the runtime reverts the batch. The delegate retries parts with the backoff until the deadline, and a lease renewal walks the ordered list. Other coordinator partitions keep committing. | Cluster: ISR below minimum |
| Some parts of a commit succeed and others fail | The committed batches return their offsets. The others get `KAFKA_STORAGE_ERROR`, and the producer retries only those partitions. The tracking partition deletes the object only once every participant is released. | Cluster: one part fails |
| A retry arrives for a part where some batches failed or were duplicates | The broker row's stored result answers, also after a failover and after retention removed the batches. | Cluster: retried mixed results |
| The uploader broker restarts with parts in flight | Leaders reject the old epoch as stale. The broker-epoch-retired record carries `retiredThrough`, ends the old epoch's leases, and raises each broker row's floor. The range fences run, and leftover keys are queued. | Cluster: restart with part in flight |
| A broker ID re-registers after an unregistration | Commits at the new epoch succeed, and epochs at or below `retiredThrough` get stale-epoch. | Backend: re-registration under the same ID |
| A leader's image lags a new broker's registration | The leader appends no broker-epoch-retired record, and the new broker's commits proceed. | Backend: new broker ahead of the image |
| A file closes with no leased sequence | It fails at once with `KAFKA_STORAGE_ERROR` and isn't uploaded. | Shard: acceptance rules |
| The tracking partition doesn't answer a renewal, or becomes unavailable in the middle of a range | The first tracking part that finishes without a leader answer starts a renewal, and the broker walks the ordered list within one commit deadline. Only the user partitions of the unavailable partition fail. | Cluster: tracking partition down |
| The preferred tracking partition is unavailable | The broker walks the ordered list. Below saturation, it stays inside its gateway set at no extra part. At saturation, it pays one empty part per file until its next renewal after the recovery. | Backend: fallback inside the gateway set |
| A lease reply is lost | The retry names the same range and is a no-op. If the broker switches instead, it releases the unconfirmed range, scoped to the requested `b`, once no planned sequence is at or below `b`. | Fault injection: lost lease reply |
| A lease renewal gets no answer while the broker plans sequences in the requested range | The broker switches, and its release of the requested range waits until no planned sequence is at or below `b`. No part fails on the lease-end fence. | Fault injection: lost lease reply |
| A broker is idle longer than the idle timeout | The keepalive keeps the lease, and the first file commits. | Backend: resume after an idle period |
| A low-rate broker falls back | It switches at once and releases the abandoned range. No floor passes the range in use. | Backend: low-rate fallback |
| A late release arrives after the broker returned to the same tracking partition | The release stops at `b`, and the new range stays live. | Shard: late release against a newer range |
| A failover loses a finish-reported record before it applies | The broker still holds the report and resends it, and the new leader sends the fences once. | Shard: failover before the finish report applies |
| The part to the tracking partition fails while other parts commit | The sequence is missed once a recorded `finishedThrough` reaches it. The tracking partition then fences the missed range at all `N` and builds the participant set from the referenced answers. | Cluster: missed sequence |
| A broker's tracking part fails, and it switches before a later record at that tracking partition | The tracking partition marks the sequence missed when the release's `finishedThrough` reaches it, and fences it before the lease end. The object is queued. | Backend: abandoned range with a failed tracking part |
| A participant's part is delayed by a failover while the file's tracking part is empty | The part commits at the new leader within the deadline, and the produce succeeds. The tracking partition sends no fence and keeps the object's entry until the participants release it. | Cluster: empty tracking part with a participant failover |
| A fence races a part in flight | A part pending before the fence commits, and the fence answers referenced. A part that arrives after it gets stale-sequence, and the producer retries. | Shard: acceptance rules |
| A coordinator partition recovers after an outage under load | Each tracking partition keeps at most one fence request in flight to it. `PendingFenceRangeCount` drains, and no commit hits a full overlay. | Cluster: participant outage under load |
| A broker cut off from its tracking partition | The idle timer appends a lease-ended record, which raises the floor, and range fences go to every partition. Keys that no partition holds are queued. | Cluster: idle lease end |
| The idle timer fires as a part arrives | The part to the tracking partition gets stale-sequence. A participant's committed part stays readable and tracked. | Shard: idle timer against a part |
| A tracking leader's monotonic clock jumps forward | Leases end at the partitions it leads. Files in flight fail once with stale-sequence, and the brokers renew. | Shard: idle timer against a part |
| A tracking leader pauses between the timer and the append while leadership moves | The old leader's append fails at its old epoch, and the new leader restarts the timer. | Shard: leader pause |
| Tracking-partition leadership changes faster than the idle timeout | Idle leases of cut-off brokers don't end, so their leftover keys stay until leadership holds for one timeout. Live brokers don't depend on the timer. | None: liveness only |
| The assignment flaps during a rolling restart | The broker switches only once a file comes out empty, so a fence followed by a return costs nothing while stale traffic lasts. | Cluster: fence and return without a switch |
| The tracking selection picks a coordinator partition outside the gateway set, from a bug or a misroute | Cost only: one empty part per file. Any coordinator partition can track, so correctness doesn't change. The `empty-tracking` kind of `CommitPartRate` shows it. | Cluster: one part per routed file |
| A coordinator leader slows down or fails fast under load | In-flight work at that leader stays within about `B` commit parts and `8B` read parts. Retries follow the backoff, and producer retries add batches to later files, not parts. Read parts fail at 1000 ms with `KAFKA_STORAGE_ERROR` for its user partitions only. Load falls as the leader recovers, with nothing to multiply it. | Backend: backoff schedule. Shard: overlay cap. Backend: reads off the shard. Cluster: read isolation on the caller. Backend: read slots. Backend: in-flight limits |
| A broker bug sends an unleased sequence or a retry with a different batch count | `INVALID_REQUEST`. | Shard: acceptance rules |
| A part names a user partition that doesn't map to the receiving coordinator partition | `INVALID_REQUEST`, counted in `PartitionMappingMismatchRate`. | Cluster: mapping mismatch |

### Deletion

| Failure | Behavior | Test |
| --- | --- | --- |
| A partial send followed by `ControlPlaneUnavailableException`, or a delegate replaced between two parts of one file | The broker never deletes the object directly. The queue deletes it after a finish report, a missed-sequence fence, or the lease end, plus the grace period. | Shared suite: partial send |
| The broker stops before the upload | The epoch end ends its lease, the lease-end range fence answers "released," and the queue deletes the missing key, which counts as success. | Cluster: broker stops before and after upload |
| The broker stops after the upload and before any part commits | If the broker is still running, its next finish report marks the sequence missed and its fences run. If it goes idle or restarts, the lease end covers it. The queue deletes the key after the grace period. | Cluster: broker stops before and after upload |
| The broker stops after some parts commit | No finish report covers the file, so the lease end covers it. Committed participants answer "referenced" and release later, and the others answer "released." | Cluster: lease end covers unreported sequences |
| A commit where every batch fails | Its file-committed record releases the object at the participant at once. | Shared suite: marking rule |
| A participant's release report arrives before the tracking part records | The tracking partition stores the entry as released, sends that participant no fence, and queues the object once the others release. | Backend: release report before the tracking part |
| A "referenced" fence answer arrives after the participant's release report | The entry stays released, and the object is queued. | Backend: referenced after release |
| A participant resends a release report after a lease end resolved its sequence | The tracking partition accepts the report as a no-op and doesn't store an entry. | Backend: resent report after a lease end |
| A batched release report holds a group outside the leased ranges | The tracking partition applies the valid groups and rejects only that group. `ReleaseReportRejectedRate` rises. | Shard: batched report with one bad group |
| A retention pass releases many objects at one participant | One batched report per tracking partition, and one record on each side per report. | Backend: batched releases under retention |
| An object's batches expire on one coordinator partition while another still holds batches | The object stays until both are released. | Cluster: object expires on one partition |
| The tracking partition's leader is unreachable for a release report | The participant keeps one report in flight and retries it with the backoff. Releases that pile up meanwhile go into the next report. Only those objects' deletion waits, and commits and reads continue. | Cluster: tracking leader change |
| An uploader pauses past the idle timeout with an upload in flight | Its commit gets stale-sequence, the broker doesn't delete the object, and no batch row points to the key. A late upload after the delete is the accepted leak. | Backend: uploader pause |
| Leadership moves between the storage delete and the files-deleted record, or `FileCleaner` deletes from a stale list | The key is deleted at most twice, which is harmless. | Backend: stale `FileCleaner` list |
| Two clusters share a bucket and prefix | The cluster ID in every key keeps deletes within this cluster. | Seam: key name function |
| An upload stops partway | A partial object is never visible. | None: storage property |

### Leader change and reads

| Failure | Behavior | Test |
| --- | --- | --- |
| Leader loses the partition | The new leader serves once its high watermark reaches its election-time log end offset and its applier has caught up. | Fault injection: high-watermark gate |
| Append fails, or the leader resigns, before the high watermark | The leader drops the failed overlay entry and every entry after it, and fails their parked replies with a retriable error. SQLite is unchanged. | Cluster: produce in flight during leader change |
| Leader change truncates unreplicated records | The leader drops the truncated entries from its overlay and fails their parked replies with a retriable error. A replica discards fetched records above its applied offset that the new leader doesn't have. Because the applied offset never exceeds the high watermark, the truncation never reaches applied state. | Cluster: truncation |
| Coordinator leader is down when a topic is created or deleted | The metadata change commits without it. The new leader reconciles on election. Calls for the new topic get `COORDINATOR_LOAD_IN_PROGRESS` until reconciliation finishes. | Cluster: topic created while leader down |
| A new leader's image lags a partition-created record from the old leader | The leader appends no topic-deleted record until its image reaches the record's `metadataOffset`. | Backend: unreconciled partition across a leader change |
| A call names an unreconciled user partition | `COORDINATOR_LOAD_IN_PROGRESS`, with nothing appended. The caller retries, and `UnreconciledPartitionRejectRate` rises. | Cluster: topic created while leader down |
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
| A record stops the apply in the middle of a group | SQLite commits every record before it, and the applied offset is the record before the stop. Replies before the stop complete, and the leader resigns. Recovery is as for a record the replica can't read. | Backend: apply stop |
| A leader bug writes a row for an unreconciled partition | Apply stops at that record, and the replica reports corruption. | Seam: apply rules |
| Power loss on one replica or across every replica | `flush.messages=1` keeps every acknowledged record on each ISR member, so no unflushed tail exists to lose. SQLite may roll back its most recent groups, and the replica replays them from the log. | Cluster: unclean restart of every replica |
| A snapshot backup or a slow disk keeps the checkpoint behind, or a checkpoint fails | Applies continue. The WAL grows until the reader ends or a checkpoint succeeds, and `SqliteWalPages` rises. The checkpoint thread logs the error and retries on its next pass. | Seam: SQLite checkpoint |
| The bundled SQLite is older than 3.51.3 and isn't 3.44.6 or 3.50.7 | The broker refuses to start the coordinator backend. The fix is a build with a fixed SQLite. | Seam: SQLite version check |
| The follower applier falls behind | The replica stays in the ISR, and `ApplyLagBytes` rises. If it becomes leader, the high-watermark gate holds commits until it catches up. | Cluster: failover mid-commit |
| Crash while a snapshot is written | Only the temporary file can be incomplete. Startup deletes it, and the previous pair of snapshots stays intact. | Seam: snapshot unit tests |
| Snapshot source rotates or restarts during a transfer | Rotation keeps a file while its transfer is active. If the transfer idled out or the source restarted and the snapshot ID is gone, the receiver discards its partial file and fetches the newest covering snapshot. | Cluster: source rotates during transfer |
| Every replica with a covering snapshot is already serving its limit of transfers | Each answers with a retriable busy error. The receiver retries in follower-first order until a source frees up. | None: liveness only |
| Two replicas need a rebuild at once | Each serves its finished snapshot files to the other, whatever the state of its live database. | Seam: snapshot unit tests |
| A replica leaves the ISR after reporting a snapshot | The leader drops its report. The log start doesn't advance on that report until the replica rejoins and reports again. | Cluster: report then leave ISR |
| Fewer than `min.insync.replicas` in-sync replicas, the leader included, hold a snapshot at the leader's older-snapshot offset | The leader doesn't advance the log start. Produce and fetch continue. | Cluster: older snapshot ahead of followers |
| The leader is unreachable when a replica sends a snapshot report | The scheduler retries with the backoff. Replica fetching continues, and only log-start advancement waits. | None: liveness only |
| Leader dies after the log start has moved | Another in-sync replica has reported a snapshot that covers the log start. A new leader whose database is missing rebuilds from that snapshot. | Cluster: new leader with missing database |
| The confirming replica later loses that snapshot, and the new leader's database is also missing | The leader can't rebuild and must not serve. Diskless writes for that coordinator partition's user partitions stay unavailable until a covering snapshot exists. How the leader gives up leadership is an open item. | Cluster: no covering snapshot |
| The topic has one replica, through the internal override | The leader advances the log start from its own older snapshot. There is no other replica to confirm. | Cluster: single-replica topic |
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
| A reassignment would lower a partition's rack spread | The controller rejects it with `INVALID_REPLICA_ASSIGNMENT`. The operator picks a target that keeps the spread. | Seam: `ReplicationControlManagerTest`. Cluster: rack spread |
| A broker re-registers with another rack, or a new rack registers | `CoordinatorPartitionsBelowRackSpread` rises, and nothing is rejected. A reassignment restores maximal spread. | Seam: controller metrics |
| `diskless.metadata.topic.replication.factor` is below 3 without the internal override | The controller fails startup. | Seam: controller config validation |
| Fewer registered, unfenced brokers than the replication factor | The controller holds off creating the topic, and coordinator calls get the retriable error. It creates the topic at the registration or unfencing that reaches the replication factor. | Seam: `ReplicationControlManagerTest` |
| Fewer racks than the replication factor | Creation places replicas across every rack and logs a warning. | Seam: `ReplicationControlManagerTest` |
| An upstream change makes the controller write a record ahead of a new broker registration | The registration-order test fails at the upstream sync, before a leader retires a live broker. Retiring an epoch only on proof is the fallback. | Seam: `ClusterControlManagerTest` |
| Every ELR member and the last known leader of a coordinator partition have lost their storage | The partition stays offline, and no unclean election is possible. | Seam: `ReplicationControlManagerTest` |
| An admin request or a client produce would delete, create, extend, or weaken the topic | `DeleteTopics`, client produce, and client `DeleteRecords` get `INVALID_TOPIC_EXCEPTION`. Client `CreateTopics` and `UNCLEAN` `ElectLeaders` get `INVALID_REQUEST`. `CreatePartitions` gets `INVALID_PARTITIONS`. A fixed config alter gets `INVALID_CONFIG`. | Cluster: admin protection |

### Gateway routing

| Failure | Behavior | Test |
| --- | --- | --- |
| An operator sets `diskless.gateway.routing.enable` on one broker | The controller rejects the change with `INVALID_CONFIG`, so no broker diverges from the cluster default. | Cluster: gateway switch scope |
| A broker's `server.properties` sets `diskless.gateway.routing.enable` | The broker refuses to start, and the error names the `kafka-configs --entity-default` command. | Cluster: gateway switch scope |
| A live broker loses a gateway slot, on a return, a join, an extra move, or an exclusion | Its next batch per connection and partition gets one `NOT_LEADER_OR_FOLLOWER`. The client refreshes and commits at the new gateway. | Cluster: misrouted produce |
| Brokers disagree on the assignment, from image skew or a rolling upgrade that changes the rule | A client pays at most one retry per broker, connection, and partition per `metadata.max.age.ms`, then commits. | Cluster: no loop under disagreement |
| A gateway crashes | Its coordinator partitions stall in its rack until the controller fences it, as a classic partition does. Other racks keep committing. After the fence, the rule picks stand-ins, and produce commits there. | Cluster: gateway crash |
| A fenced broker sees its own fence in its image | It isn't in its own candidate set, so it rejects once per connection and partition and names the stand-in in its metadata answers, until its next heartbeat unfences it. | Cluster: fenced gateway |
| A gateway is cut off from the controller but reachable by clients | It can't see its fence, keeps counting itself the gateway, and commits its clients' produce while it reaches the coordinator leaders. Its clients drift to the stand-in as they refresh elsewhere. The cost is up to one extra part per file for the coordinator partition, cache misses, and uneven load, and nothing is lost. The controller's `FencedBrokerCount` and the broker's `last-applied-record-lag-ms` show it. | Cluster: fenced gateway |
| A fenced broker can't reach the coordinator leaders | Commits fail with `KAFKA_STORAGE_ERROR`, and clients refresh. A client that refreshes from that broker can fail its batch at `delivery.timeout.ms`. Nothing acknowledged is lost, as with a classic leader that's cut off under `acks=all`. | Cluster: fenced gateway |

### Accepted risks

Every risk here passes the [data-loss bar](#goals): clause 2's exception covers the loss of every replica, and clause 3's exception covers the lost tail, the apply bug, and the outside delete. The other rows lose no data.

| Risk | Why it's accepted | Signal or procedure |
| --- | --- | --- |
| Every replica of a coordinator partition is lost | Clause 2's exception, with replication factor 3 and maximal spread enforced. The coordinator partition stays leaderless, and its user partitions, about 1/N of every diskless topic, can't be written or read. Clause 3 holds: fences to it go unanswered and releases from it never arrive, so the objects it touched leak rather than get deleted. A cluster that uses the internal override to lower the replication factor gives up the exception. | Signal: `OfflinePartitionsCount` for `__diskless_metadata`. Procedure: none. The deployment docs recommend dedicated controllers, because in combined mode the same loss can take a controller majority. |
| A deterministic apply bug corrupts every replica alike | Kafka's coordinators share this limitation. A wrong release or fence answer deletes an object that another coordinator partition still references, and the delete is permanent. | Signal: a sustained `PartitionStorageErrorRate` on a diskless partition, outside a deposed-leader window or a grace-period race, means coordinator state points into a deleted object, whatever the cause. It catches the harmful effect, not the bug. Prevention: the shared suite, the golden-records test, and the apply-stop rule. |
| Log recovery truncates fsynced records after a read error or bit rot, under an unchanged directory ID | A classic topic loses data to the same cause. The damage stays at the coordinator partition that lost its tail. A lost removal brings back batch rows whose object is gone, and a lost floor lets a late part commit into a deleted key. Reads of those offsets then fail, which a classic topic doesn't do. The protocol model finds no path to another partition within its bounds. The soft-delete bound is future work. | Procedure: alert on log recovery's invalid-message truncation warning for `__diskless_metadata`, and stop the broker for inspection instead of letting it rejoin. |
| A device acknowledges an fsync without persisting it | Same as the preceding row. | Procedure: the deployment docs require storage that honors fsync, with no volatile write cache that lacks power-loss protection. |
| An operator restores a log directory, `meta.properties` included, from an older copy | Same as the preceding row. | Procedure: never restore a broker's log directory from a copy. Reformat it and let the broker re-replicate. |
| A stale leader-epoch entry over-truncates an ELR member, and the leader fails before that member fetches again | Same as the preceding row. | Signal: `UncleanLeaderElectionsPerSec` and `ElectionFromEligibleLeaderReplicasPerSec`. No signal catches the over-truncation itself. |
| Something outside the coordinator deletes a WAL key, such as a bucket expiration rule or a manual delete | Inkless can't prevent it. A classic topic loses data the same way when someone deletes its segment files. | Procedure: the deployment docs forbid expiration rules and manual deletes on the WAL prefix. Signal: `PartitionStorageErrorRate`. |
| Retention and `LogAppendTime` under a skewed leader clock | Classic topics have the same exposure. The blast radius is the user partitions of the coordinator partitions the skewed broker leads, about the same share as the classic partitions it leads. | Procedure: monitor the NTP offset on every broker, and alert well below half the grace period. |
| Grace-period skew beyond half the grace period | It costs reader availability, not data, and the Postgres backend shares it. | The NTP procedure in the preceding row. |
| Storage completes an upload after its key was deleted | It leaks storage, not data. The bounds are one object per attempt and at most `K` files for a cut-off zombie. The grace period covers uploads that land within a minute of the mark. | Signal: `FileUploadErrorRate`, an upper bound on objects that may have leaked. The report-only scanner stays future work. |
| A held entry whose participant no longer holds batches and never reports | It leaks storage and never deletes data. It needs a broker that wrongly claims "answered" or a participant that skips its emptiness check, and a wrong claim leaks rather than deletes. | Signal: `ReleaseReportRejectedRate`, `ReleaseOutsideParticipantSetRate`, and storage growth. The report-only scanner stays future work. |
| A non-idempotent producer that retries through another broker sees a late copy after the retried one | A classic `acks=all` timeout and the Postgres backend have the same window. The misroute rejection happens before the write, so it adds no late copy. | Signal: `event-purgatory-time-ms` bounds the window. |

Performance limits aren't risks to data, so Capacity and isolation states them as [accepted limits](#accepted-limits).

## Testing

Good tests check behavior through a public seam. Four layers, in this order:

1. The shared `AbstractControlPlaneTest` suite runs against `DisklessMetadataControlPlane`, as it runs against the in-memory and Postgres backends, at `N=1` and at `N=8`. One broker leads every coordinator partition, so the second run covers request splitting, result merging, tracking parts, and releases. It adds:
   - **Partial send**: part 1 is sent with an unknown outcome and part 2 hits `Unavailable`, and a delegate is replaced between two parts of one file. The broker never deletes the uploaded object directly. The coordinator backend's run also checks that the tracking partition's queue deletes the key after the grace period. The Postgres and in-memory runs check that the direct delete still happens.
   - **Marking rule**: each of the five batch-removing records releases an object whose last batch it removes, and no other record does.
   - **No fence at `N=8`**: with no failed part, the suite at `N=8` sends no fence, through commits and through retention.
   - **Per-file completion on single-queue backends**: on Postgres and in-memory, every request completes when its file does.
   - The suite's commit and retry cases, in their form with leased sequences.
2. Coordinator backend tests, for cases that one clock or one state can't express. Every test that involves time runs at `N ≥ 2` and gives the uploader and each coordinator leader its own `MockTime`:
   - **Skewed clocks**: offset every uploader and leader by up to a day, then run commits, idle lease ends, epoch retirement, a failover, and `FileCleaner` cycles with `retention.ms=-1`. Every batch row's object exists in `InMemoryStorage`, and no queued key matches a batch row.
   - **Uploader pause**: hold an upload until the lease ends, the range fence runs, and the grace period deletes the key, then release it and commit. The commit gets stale-sequence, the broker doesn't delete the object, and no batch row points to the key.
   - **Stale `FileCleaner` list**: call `getFilesToDelete` on the old leader, move leadership, let the new leader delete and record its deletes, then let the old leader delete its list. Every key the old leader deletes was in the committed queue and has no batch row.
   - **Stale-read rule**: build an older state and a newer one, with commits, retention, and `DeleteRecords` between them. For `EARLIEST`, `LATEST`, `MAX_TIMESTAMP`, and timestamp lookups, the older answer is lower than or equal to the newer one, or unknown. A future read that breaks the rule fails this test.
   - **Apply stop**: an unknown type, an unknown value version, and an unknown tag at the top level and in a nested struct each leave the applied offset unchanged, raise `ApplyStoppedPartitionCount`, stop serving, and make a leader resign. A tag in a test extension registry applies normally. A stopping record in the middle of a group commits the records before it and none after, on the leader, the follower applier, and the loader.
   - **Write gate**: for each record type, the leader writes the version that the finalized level allows. A leader with a lagging `MetadataImage` writes the lower version. An Inkless tag appears only at an Inkless feature level that includes it.
   - **SQLite schema**: forward migration from each supported older schema succeeds. A newer schema counts as a missing database and triggers a rebuild. A snapshot fetch above the receiver's schema gets `SNAPSHOT_NOT_FOUND`, and the receiver moves to the next replica.
   - **Golden records**: a checked-in file holds every record type at every level, and the current binary decodes all of it.
   - **Exception range fence**: a participant is unavailable across several files. The finish report holds one exception range, and the tracking partition sends that participant one range fence, which answers "referenced" for any late part that committed.
   - **`Recorded` flag**: the tracking partition doesn't fence a participant whose part failed every batch. It fences a participant that rejected the part as stale, and that fence appends nothing at the participant.
   - **Failed tracking part with a report**: the tracking part that carries a finish report fails, and the tracking partition doesn't record it. The next tracking part carries the merged report, and the fences follow it.
   - **Urgent report on a quiet broker**: one file's part ends with an unknown outcome, and the broker then goes quiet. The early keepalive appends a finish-reported record within one commit interval, and the exception fence closes the duplicate window.
   - **Resume after an idle period**: the broker stays quiet past the idle timeout, then produces. The keepalive kept the lease, the first file commits, and `StaleSequenceRejectRate` and `IdleLeaseEndRate` stay at zero.
   - **Low-rate fallback**: the broker's tracking partition is unavailable at renewal, and the broker produces below 0.6 files per second. It switches to the fallback range at once, and no file fails after the fallback. Once the old partition recovers, the lease release ends the abandoned range there, and its range fences queue the unused keys.
   - **Re-registration under the same ID**: a broker commits at `E1`, is unregistered, and re-registers under the same ID at `E2`. The leader appends broker-epoch-retired with `retiredThrough` at its image offset. A late part at `E1` gets stale-epoch, a part at `E2` commits, and the tracking partition has ended the `E1` leases.
   - **New broker ahead of the image**: a broker registers at `E` and commits to a leader whose image is below `E`. The leader appends no broker-epoch-retired record, and the commit succeeds before and after the image catches up.
   - **Unseen middle epoch**: a broker registers at `E1`, re-registers at `E2`, and is unregistered. The partition saw only `E1`. A late part at `E2` gets stale-epoch.
   - **Unreconciled partition across a leader change**: the old leader, with its image at `M`, appends partition-created for a new topic. The new leader's image is at `M' < M` and lacks the topic. It appends no topic-deleted. After the topic is deleted and the image passes `M`, it appends topic-deleted.
   - **Release report before the tracking part**: an all-fail participant reports, then the tracking part records, the finish report arrives, and the other participants release. The tracking partition queues the object and sends the early participant no fence.
   - **Referenced after release**: a fence answer of "referenced" arrives after the participant's release report. The entry stays released, and the object is queued.
   - **Early report for a missed sequence**: a participant reports a sequence whose tracking part never records. A later recorded `finishedThrough` marks it missed, and the missed-sequence fences resolve it. The stored release counts as that participant's answer, and the tracking partition sends that participant no fence.
   - **Duplicate report after resolution**: a new participant leader resends a release whose object is already deleted. The tracking partition accepts it as a no-op and stores no entry.
   - **Resent report after a lease end**: an all-fail participant releases, the tracking part fails, and the lease end resolves the sequence and deletes the object. The participant resends the report, and the tracking partition doesn't store an entry.
   - **Batched releases under retention**: a retention pass releases thousands of objects at one participant. The participant sends one report per tracking partition with merged ranges, and each side appends one record per report. A lost reply resends the same report, and the result doesn't change.
   - **Stuck fence age**: a participant doesn't answer a range fence, and `TrackingEntryOldestAgeMs` for fencing grows until the fence answers.
   - **Broker-wide `finishedThrough`**: the broker switches back to `t` while a file under `t + 1` retries at `p`. A missed sequence at `t` has no part at `p`. Its fence waits until the `t + 1` file finishes, and that file's part commits.
   - **Lease release waits after a switch**: a lease release after a switch waits for files with lower sequences under another partition's ranges.
   - **Abandoned range with a failed tracking part**: the tracking part at `t` fails, the broker switches, and the lease release reaches `t` with `finishedThrough` at the sequence. `t` fences the sequence as missed, the participants answer, and the object is queued.
   - **Backoff schedule**: a leader answers `COORDINATOR_LOAD_IN_PROGRESS`. One broker's attempts for one part follow the backoff schedule, and the part ends with `KAFKA_STORAGE_ERROR` at the deadline.
   - **Reads off the shard**: a read pool full of slow scans leaves a commit part's latency on that leader unchanged.
   - **Read slots**: with a stuck partition's 8 slots full, a read to a healthy partition on the same broker goes out at once, and the stuck partition's queued read part fails at its deadline.
   - **In-flight limits**: one test per limit in [Load bounds](#load-bounds) shows that a broker never has a second commit part per queue, lease renewal, release report per tracking partition, or fence batch per participant in flight.
   - **Fallback inside the gateway set**: below saturation, an outage of the preferred partition falls back to another member of the gateway set, and the broker doesn't switch back after the recovery. At saturation, no file close sends a lease request to the unavailable partition, and the broker returns at its next threshold renewal.
   - **Spare promotion and demotion**: the first promotion costs one switch, and a demotion costs none.
3. A multi-broker `KafkaClusterTestKit` test covers these cases:
   - **Failover mid-commit**: leader failover in the middle of a commit, followed by the retry path. The retry resolves through `(epoch, sequence)`, and the client sees one successful produce with no duplicate records. A second variant pauses the follower's applier: the follower stays in the ISR, and after it becomes leader, commits wait for the gate.
   - **Truncation**: a leader change that truncates unreplicated records. The follower's state matches the new leader's state, and no applied record was undone.
   - **Retry while pending**: a retry that arrives while its first attempt is pending on the leader. The client sees one record and one set of offsets, for idempotent and non-idempotent producers.
   - **One part fails**: a commit whose parts land on several coordinator partitions, where one part fails. The committed partitions return offsets, the failed ones get `KAFKA_STORAGE_ERROR`, and the tracking partition deletes the object once every participant is released.
   - **Tracking partition down**: the broker leases at the next partition in its ordered list, and only the unavailable partition's user partitions fail. A second case makes the tracking partition unavailable early in a range: the first tracking part that finishes without a leader answer starts one renewal, and the files after the fallback don't wait on the old partition.
   - **Stuck tracking partition**: a request with no batch at the tracking partition completes once its parts commit. The file stays unfinished until the tracking part finishes, so `finishedThrough` doesn't pass that file's sequence until then.
   - **Broker stops before and after upload**: a broker that stops before the upload, and one that stops after the upload and before any part commits. The epoch end marks the unused range, and the queue deletes the missing key and the uploaded key after the grace period.
   - **Lease end covers unreported sequences**: the broker stops after some parts commit, before a finish report covers the file. Its restart ends the lease, the range fence covers the file, and the tracking partition queues the object only after the committed participants release it.
   - **Object expires on one partition**: an object whose batches expire on one coordinator partition while another still holds batches. The object stays until both are released.
   - **One coordinator partition offline**: one file holds request `A`, whose partitions map only to healthy coordinator partition `P1`, and request `B`, whose partitions map to `P1` and to stuck coordinator partition `P2`. `A` completes once `P1`'s part commits, before `P2`'s deadline. `B` completes at the deadline, with its own results for the `P1` batches and `KAFKA_STORAGE_ERROR` for the `P2` batches.
   - **Deadline then next part**: a part reaches its deadline, then the same broker's next commit to that coordinator partition raises the floor. A late request for the earlier part gets stale-sequence, and the later batches commit.
   - **ISR below minimum**: a coordinator partition whose ISR drops below `min.insync.replicas`. Commit parts fail at once rather than at the deadline, and lease renewals walk the ordered list to another partition.
   - **Retried mixed results**: a retried part where some batches failed and some were duplicates, sent to a new leader after a failover. The retry returns the same per-batch results as the first attempt.
   - **Restart with part in flight**: the leader rejects the part's old epoch as stale, and the broker-epoch-retired record ends its leases.
   - **Tracking leader change**: a tracking partition changes leader with open entries. The new leader restarts the idle timers, rebuilds the pending fence set, and sends one batched request per participant. Participants resend releases that the old leader didn't accept.
   - **Idle lease end**: the lease of a broker cut off from its tracking partition ends, and its leftover keys are deleted.
   - **Missed sequence**: the tracking part of `s` fails while another participant's part of `s` is still retrying. That part commits, and the missed-sequence fence goes to all `N` partitions only after a recorded `finishedThrough` reaches `s`. The object stays until its participants release it.
   - **Empty tracking part with a participant failover**: the tracking part is empty, and the participant's part gets `NOT_COORDINATOR` and then commits at the new leader before the deadline. The produce succeeds, the tracking partition sends no fence before or after the finish report covers the sequence, and it keeps the object's entry until the participants release it.
   - **Participant outage under load**: one coordinator partition is unavailable for 60 seconds at `N=8`. When it recovers, each tracking partition has at most one fence request in flight to it, `PendingFenceRangeCount` drains, and no commit is rejected for a full overlay.
   - **Read isolation on the caller**: fetches on a broker touch a stuck coordinator partition. Fetches for user partitions on healthy coordinator partitions keep their latency, and the stuck partition's user partitions get `KAFKA_STORAGE_ERROR` within 1000 ms.
   - **Mapping mismatch**: a part that names a user partition the receiving coordinator partition doesn't own. The leader rejects it with `INVALID_REQUEST`.
   - **Catch up from snapshot**: a replica catches up from a snapshot after the log start has moved, and serves the same reads as the leader afterward.
   - **New leader with missing database**: it rebuilds from a follower's snapshot, and it serves no request before the rebuild finishes.
   - **No covering snapshot**: a coordinator partition where no replica holds a snapshot covering the log start. Its user partitions stay unavailable for diskless writes, and no partial state is served.
   - **Older snapshot ahead of followers**: the leader's older snapshot is ahead of every other in-sync replica's reported snapshot. The log start stays put. Once a follower reports a snapshot at that offset, the log start advances, and that follower's snapshot still covers it after the leader stops.
   - **Report then leave ISR**: a follower that reports a snapshot and then leaves the ISR. The log start doesn't advance on its report until it rejoins and reports again.
   - **Source rotates during transfer**: a snapshot source that rotates or restarts during a transfer. The receiver never loads a file spliced from two snapshots, and it finishes from the newest covering snapshot.
   - **Follower serves rebuild**: a rebuild while the leader is under commit load. The leader serves no transfer while an in-sync follower holds a covering snapshot.
   - **Database lost before first log start move**: the replica replays from offset 0 and needs no snapshot.
   - **Single-replica topic**: on a cluster that uses the internal override, the leader advances the log start from its own older snapshot.
   - **Topic deleted and purged**: every replica drops the same batches and rows as the leader, and the tracking partitions delete the emptied objects.
   - **Topic created while leader down**: a topic created and a topic deleted while a coordinator leader is down. The leader reconciles on return. Before reconciliation, `initDisklessLog` and a commit part both get `COORDINATOR_LOAD_IN_PROGRESS`, append nothing, and raise `UnreconciledPartitionRejectRate`. Init succeeds on retry after reconciliation, and the produce succeeds within its deadline.
   - **Produce in flight during leader change**: the produce fails cleanly or succeeds once.
   - **Consolidated prune floor**: a consolidated topic whose prune floor moves only after the leader confirms it.
   - **Cluster formatted with the feature**: a commit goes through the coordinator. A broker whose class names another backend refuses to start. A broker that doesn't support either finalized feature level can't register.
   - **Admin protection**: the controller creates the topic with the fixed configs and the configured `min.insync.replicas`, `Admin.listTopics()` hides it by default, and every rejection holds end to end through `Admin`: delete, create, `CreatePartitions`, altering `flush.messages`, an unclean election, and an `__admin_client` produce. A `Metadata` request with auto-create on doesn't create the topic.
   - **Rack spread**: on a 3-rack cluster, the controller creates the topic with one replica per rack, and a reassignment that narrows the spread fails through `Admin`.
   - **Deposed leader**: cut B1 off from the controller and its followers, commit through the new leader up to offset 1500, then consume `orders-0` from committed offset 1400 through B1 under `auto.offset.reset=earliest` and under `latest`. No reset moves the position, and the consumer reads from 1400 once B1 learns it was deposed.
   - **Seek past the end**: `seek(highWatermark + 1000)` resets by the consumer's policy.
   - **Upgrade and rollback**: brokers roll to the new binary, the level rises, and a commit goes through the coordinator. A second variant rolls back before the raise, and the rolled-back replicas rebuild and rejoin the ISR.
   - **Unclean restart of every replica**: kill every replica of a coordinator partition uncleanly, restart them with intact disks, and the partition recovers with every acknowledged record.
   - **Gateway agreement**: every broker computes the same assignment from the same image, including racks, exclusions, and `N`.
   - **Gateway exclusion**: setting `diskless.gateway.candidate.enable` to false on a broker moves that broker's coordinator partitions on every broker, and clearing it moves them back.
   - **Gateway switch scope**: a per-broker value of `diskless.gateway.routing.enable` gets `INVALID_CONFIG`, a static value stops startup, and a change to the cluster default flips routing on every broker at its config record, with no restart.
   - **Gateway switch toggle**: turning routing off and on moves no tracking partition.
   - **One part per routed file**: at saturation, a gateway's file sends only its gateway coordinator partition's part, and the `empty-tracking` kind of `CommitPartRate` stays at 0.
   - **Switch on the first empty part**: after an image change, files with stale batches don't switch. The first file without them starts an `assignment` renewal, the broker switches and releases, and no file fails.
   - **Fence and return without a switch**: a gateway is fenced and returns while stale traffic lasts, and the broker doesn't switch its tracking partition.
   - **Misrouted produce**: a produce on the gateway path at a broker that isn't a gateway gets one `NOT_LEADER_OR_FOLLOWER` and writes nothing. The client refreshes, and the batch commits at the gateway. A second misrouted batch on the same connection and partition within `metadata.max.age.ms` commits at the broker that isn't a gateway, and the misrouted meters count its part and bytes. Other partitions in the rejected request commit. `MisroutedBatchRejectRate` counts one rejection. A cross-AZ fallback to a gateway of the same coordinator partition and a local-log partition are neither rejected nor counted. With the switch off, nothing is rejected.
   - **No loop under disagreement**: with two brokers given different assignments through the stub interface, a client that refreshes from each in turn commits after at most one retry per broker.
   - **Local-log partitions**: consolidating, tiered, and switched partitions that still hold a local prefix keep replica routing, are never rejected, and their parts count as `off-gateway`.
   - **Fenced gateway**: a broker that applies its own fence rejects once per connection and partition, names the stand-in, and moves back after its unfence. A broker cut off from the controller keeps committing produce while other brokers name the stand-in, and once it's also cut off from the coordinator leaders, its produce fails with `KAFKA_STORAGE_ERROR`.
   - **Gateway crash**: kill a gateway. Its coordinator partitions stall in its rack until the controller fences it, other racks keep committing, and after the fence, produce commits at the stand-ins with no acknowledged record lost.
4. Fault injection has two hooks:
   - A hook on the client side of the private RPC covers the leader's high-watermark gate after an election, the `(epoch, sequence)` retry rule, a part whose request reaches the leader after its deadline, and a broker stop between lease, upload, and commit, so these behaviors are deterministic to test. It also runs **Lost lease reply**: the retry names the same range and is a no-op. When the broker switches instead, while it still plans a sequence at or below the requested range's `b`, it sends the lease release only once no planned sequence is at or below `b`, scoped to `b`, and every part under that sequence commits.
   - A test-only hook on a coordinator partition has three modes. Unavailable: the leader accepts no part. Delay: each append waits a set time. Lost tail: drop the unflushed records on every replica at a chosen fsync point, then restart. The benchmarks use the first two modes.
   - The **lost-tail suite** runs the lost-tail mode at each fsync point. After each run, it checks that no other coordinator partition deletes a referenced object or accepts a part into a deleted key. A real log exercises log recovery and replica truncation, the paths the lost-tail risks in [Accepted risks](#accepted-risks) name.

### Seam tests

- On the tracking-partition shard, with `MockPartitionWriter` and `MockCoordinatorTimer`:
  - **Barrier**: the barrier commits with the offset still above the high watermark, and the answer is `OFFSET_OUT_OF_RANGE`. The high watermark passes the offset while the barrier waits, and the fetch returns batches. The high watermark never advances, and the answer after 500 ms is `OFFSET_NOT_AVAILABLE`. The shard unloads, and the answer is `NOT_COORDINATOR`.
  - **Barrier deadline**: a request queued behind slow events still gets 500 ms from its arrival. Two requests that share a barrier each get their own deadline. A commit part that waits longer than 500 ms doesn't time out.
  - **Barrier rate under adaptive linger**: a hot loop above the high watermark appends at most one barrier per flush.
  - **Idle timer against a part**: a part reschedules `lease-idle-<brokerId>`, and when the timer runs first, the participant's committed part stays readable and the participant doesn't queue its key. Advancing the leader's `MockTime` past the timeout between two parts covers a forward clock jump. `MockCoordinatorTimer` runs an expired operation inside `poll()`, so it can't hold a fired timer. Kafka's `testTimerCancelledAfterExpiryButBeforeWriteOperation` and `testTimerOverridden` already cover the override rule.
  - **Leader pause**: queue the lease-ended write at the old leader, move leadership, and commit the part at the new leader. No lease-ended record exists. `testTimerIgnoredOnNotCoordinatorException` covers the runtime's side.
  - **Acceptance rules**: a file with no leased sequence, a fence racing a part, and a broker bug's `INVALID_REQUEST`.
  - **Late release against a newer range**: the lease release to `t` is delayed until the broker has a new range at `t`. The release stops at `b`, the new range stays live, and its files commit.
  - **Failover before the finish report applies**: a failover between the finish-reported append and its apply. The broker resends the report, and the new leader sends the fences once.
  - **Hull merge**: two exception ranges for one broker epoch, with answered sequences between them, go out as one fence. The entries in between stay held.
  - **Fence split at the size cap**: a fence set above 512 KiB splits across consecutive requests, and the answers don't change.
  - **Overlay cap**: with the high watermark stalled, the leader rejects the entry past `4 × B` with `COORDINATOR_NOT_AVAILABLE`, appends nothing for it, and the delegate's next attempt follows the backoff.
  - **Batched report with one bad group**: the report holds valid groups and a group outside the leased ranges. The valid groups apply in one record, the bad group gets the non-retriable error, and `ReleaseReportRejectedRate` rises. A participant that released sequences 1 and 3 and still holds 2 sends two ranges.
- **Delayed-fetch probe**, in `ReplicaManagerTest` or `DelayedFetchTest`: with the cache off, a probe above the high watermark returns at once and appends no barrier.
- Next to each guard:
  - `TopicTest`: `Topic.isInternal("__diskless_metadata")` is true.
  - `ControllerApisTest`: `CreateTopics` gets `INVALID_REQUEST`. `DeleteTopics` by name and by ID gets `INVALID_TOPIC_EXCEPTION`, an unauthorized caller still gets `TOPIC_AUTHORIZATION_FAILED`, and other topics in the same request are deleted.
  - `ReplicationControlManagerTest`:
    - `CreatePartitions` gets `INVALID_PARTITIONS`. `UNCLEAN` `ElectLeaders` gets `INVALID_REQUEST`. A `PREFERRED` election and a reassignment that keeps the replica count succeed, and one that changes it gets `INVALID_REPLICATION_FACTOR`.
    - The last known leader re-registers with a new directory ID and unfencing doesn't elect it, and with its old directory ID, unfencing elects it. A multi-directory broker that restarts cleanly without the replica's directory leaves the ELR. An unclean shutdown keeps the broker in the ELR for `__diskless_metadata` and removes it for a classic topic. A replica whose assigned directory is `UNASSIGNED` leaves the ELR and `lastKnownElr` at registration. After a storage change, a periodic and an admin `PREFERRED` election on the leaderless partition elect nobody. A cluster-wide `ClearElrRecord` keeps the ELR of `__diskless_metadata` and clears a classic topic's.
    - The controller attempts creation at each registration, at each unfencing, and on activation, and it succeeds once the registered, unfenced brokers reach the replication factor.
    - Placement spans `min(replication factor, number of racks)` racks with 1, 2, 3, and 4 racks, and with no rack declared.
    - A reassignment that narrows the rack spread gets `INVALID_REPLICA_ASSIGNMENT`. A move within a rack passes, and a move that widens the spread passes.
  - `ClusterControlManagerTest`: a broker registers, joins an ISR, unregisters, and registers again. The `RegisterBrokerRecord` is the registration's first record, at the offset that equals the new epoch. The `retiredThrough` rule depends on this order, so an upstream change to it fails here first.
  - `TopicsDeltaTest`: the same cluster-wide `ClearElrRecord` case on the image side.
  - `ConfigurationControlManagerTest`: each fixed config is rejected under `SET`, `DELETE`, `APPEND` and `SUBTRACT` on `cleanup.policy`, and a legacy `AlterConfigs` that omits it. Its required value passes. Changing `min.insync.replicas` gets `INVALID_CONFIG`, and its current value passes. An unrelated config such as `segment.bytes` alters.
  - `ControllerConfigurationValidator`: a per-broker value of `diskless.gateway.routing.enable` gets `INVALID_CONFIG`, and a cluster-default value passes.
  - `ReplicaManagerTest`: a client produce with client ID `__admin_client` gets `INVALID_TOPIC_EXCEPTION`, an append with `AppendOrigin.COORDINATOR` succeeds, and client `DeleteRecords` gets `INVALID_TOPIC_EXCEPTION`. If `ReplicaManagerTest` can't drive a `COORDINATOR` append without the coordinator runtime, that case moves to the admin protection cluster test.
  - `LogManagerTest`: creating a `__diskless_metadata` partition flushes the log directory, and a failed flush takes the log directory offline.
  - `FeatureControlManagerTest`: both features refuse `SAFE_DOWNGRADE` and `UNSAFE_DOWNGRADE`. An upgrade fails while a registered broker lacks support. The Inkless feature's dependency on the upstream level holds.
  - Controller config validation: `diskless.metadata.topic.min.insync.replicas` outside its range fails startup. A `diskless.metadata.topic.replication.factor` below 3 fails startup without the internal override and passes with it.
  - Controller metrics: `CoordinatorPartitionsBelowRackSpread` counts the partitions below maximal spread after a broker re-registers with another rack and after a new rack registers.
- **Key name function**: the cluster ID keeps two clusters' keys apart, and apply builds the same name as `FileCommitter`.
- **Mapping**: the user-partition mapping gets a unit test with fixed expected values, so a change to it fails the build. The same test checks that the slot mapping equals `Utils.toPositive(Utils.murmur2(logKey)) % N` for every allowed `N`, and that the config rejects an `N` that isn't a power of two.
- **Snapshot unit tests**: snapshot writing, rotation, loading, and chunk serving get unit tests on a temporary directory, with a fault hook at each step of rotation and transfer. They cover a tag read from inside the copy while applies run during the backup, a crash at each rotation step, a transfer whose source rotates or restarts before the receiver resumes, a checksum mismatch on load and on serve, and the schema version in the file name. No public seam reproduces these cases deterministically.
- **Apply rules**: apply rules for file records, broker rows, leases, fences, releases, and the deletion queue get unit tests at the replica state layer, with records appended directly to a test log. A file-committed record for a user partition with no partition-created row stops apply and reports corruption. No public call can produce a duplicate or conflicting record, so this is the one internal seam, and it's a deliberate exception.
- Routing, unit tests over the assignment interface and `InklessTopicMetadataTransformer`:
  - **Fixed values**: the default implementation gives hard-coded expected gateways and rankings for fixed clusters, including one with no racks, a mixed cluster, uneven racks, and one at `g = 2`. It pins the score encoding, the tiebreak, the cap, and the fallback ranking and pick.
  - **Stability**: after every join and leave, each coordinator partition has exactly `g` gateways, no broker exceeds the cap, a departing broker holds nothing, and a new broker holds no more than the cap. Over a seeded sweep of rack sizes from 2 to 20 at `N=8`, no event makes more than 5 extra moves.
  - **Transformer routing**, with a stubbed assignment: user partitions follow the assignment, with one case per fallback step. The cases are the gateway in the client's rack, a gateway without the client's listener, a rack with no candidates, no gateway with the listener anywhere, an excluded broker that's skipped, and at `g = 2`, a pick by user partition that stays put when the other gateway leaves.
  - **Tracking selection**, with fixed values and a stubbed assignment: the sticky rule after a member is gained and after one is lost, an empty gateway set, epoch start, and the ordered list.
  - **Predicted parts per file**: the prediction is `N × n_racks × g` divided by the brokers with a non-empty gateway set, 1.5 for the 21-broker cluster with IDs 1 to 21 and rack `(id − 1) % 3`.
  - **Unknown AZ**: a client with a `null` AZ reaches the rack-less group's gateway in a cluster without racks, the rack-less brokers first in a mixed cluster, and the cross-rack gateway with the highest log-key rendezvous when every broker has a rack.
  - **No replica preference**: for a managed partition without remote storage, a client in a rack whose replica isn't the gateway gets the gateway as `LeaderId`, and the replicas, ISR, and offline replicas stay as the controller wrote them.
  - **Routing switch-on**: the hash routing applies while the feature is unfinalized, while `__diskless_metadata` is missing, or while the switch is off, and gateway routing applies once all three hold. A switch change takes effect at its config record, with no restart.
- Produce path, unit tests on `AppendCompleter`:
  - **Rejected-only request**: a request with only rejected batches completes at file close, with no part to wait for.
  - **Cache fills per part**: a fetch for request `A`'s batch finds the batch coordinate in the cache before request `B`'s file finishes.
- SQLite:
  - **SQLite checkpoint**: the apply connection has `wal_autocheckpoint=0`. With the checkpoint thread paused, applies past 1000 pages run no checkpoint on the apply thread. With a backup held open, applies keep their latency, and the WAL drops back after the backup ends. The cap splits a group at 4 MiB, and a record larger than the cap gets a group of its own.
  - **SQLite version check**: a version below 3.51.3 that isn't a backport refuses to start, and 3.44.6, 3.50.7, and 3.51.3 pass.

### Model check

A TLA+ model checks the fence, floor, and release protocol. It lives in [model/](model/), and the reference implementation moves it into the repo with its bounds and last results:

- `FencesFloors.tla`: one broker and a set of coordinator partitions. It covers leases, the switch and fallback, commits, late attempts, finish reports, fences, floors, releases, idle and epoch lease ends, retention removals, and the deletion queue. One flag turns on merged fences.
- `FFScenarios.tla`: the scenario module. It pins the broker's lease and file choices to one named race, and TLC checks every interleaving within it.
- `TrackingEntries.tla`: one broker, one tracking partition, and its participants, over one leased range. Fences, fence answers, release reports, and late attempts are separate messages that any interleaving can reorder, and a release report can repeat.
- `EpochRetirement.tla`: one broker ID, a tracking partition, and a participant, each with its own leader image that can lag the metadata log.
- `run-all.sh`: runs every check unattended and writes a summary per run.

The model checks four invariants:

- `NoDeleteReferenced`: no object is deleted or queued while a batch references it, which also rules out a commit into a queued or deleted key.
- `NoCutLive`: no fence raises a floor past a part the broker still sends, or past a sequence it still plans to use.
- `NoLeak`: once an epoch ended and the coordinator is quiet, every uploaded key that no partition holds is queued.
- `EndedKnown`: once the lease covering a finished file ends, its tracking partition knows the key.

The bounds are 3 partitions, ranges of 2 sequences, up to 4 files, 1 late attempt in flight, 1 pause, and 1 or 2 epochs. No invariant fails within them. Each rule the model guards has a flag, and turning it off reproduces the race that rule closes within seconds. A lost tail damages only the coordinator partition that lost it, through release inversion and resurrection. The model leaves out the overlay, a tracking leader change, and more than one broker.

A change to a protocol rule updates the spec and reruns `run-all.sh`. CI doesn't run the model, because the full runs take hours.

### Benchmark tooling

The benchmarks in [Benchmarks](#benchmarks) use the coordinator-partition hook's unavailable and delay modes. A verifying consumer reads every acknowledged record, so each benchmark that checks zero acknowledged loss fails on a missing one.

## Terms

- Abandoned range: a range at a tracking partition that the broker stopped using after it switched to another tracking partition. The broker never sends a sequence from it again.
- Barrier record: a record with no content that a leader appends to confirm its leadership before it answers `OFFSET_OUT_OF_RANGE` above the high watermark.
- Blast radius: the user partitions whose data one failure can lose, delete, or resurrect. The data-loss bar limits a coordinator partition's failure to the user partitions that map to it, about 1/N of every diskless topic.
- Broker epoch retired: the record a leader appends when its metadata image shows a newer epoch or no registration for a broker. It carries `retiredThrough`, the highest epoch that can't commit anymore, moves the broker row, and ends that epoch's leases at a fixed log position.
- Broker row: the per-partition state of one uploading broker: its epoch, last committed sequence and that commit's result, its fenced-through pair, and its retired-through epoch.
- Coordinator leader: the broker that leads a coordinator partition. It runs that partition's commits, fences, and reads. A gateway isn't necessarily a coordinator leader.
- Deletion queue: the table of keys waiting for `FileCleaner` at a tracking partition, each with the mark time from its marking record.
- Deposed window: the time a deposed coordinator leader keeps answering reads before it applies the metadata record for the new leader.
- Entry states: the states of a tracking entry for one participant of an object. Pending: the participant set is recorded, and nothing covers this participant yet. Fencing: a range fence that covers the entry is out, with no recorded answer. Held: the participant holds batches and owes a release report. Released: the participant holds no batch of the object. Released is terminal.
- Fenced-through mark: the pair `(e_f, s_f)` in a broker row that a fence raises. No sequence at or below it can commit at that partition.
- Files-deleted record: the record that confirms which queued keys storage deleted.
- Finish report: the broker's report to a tracking partition of `finishedThrough` and at most one exception range per participant. A tracking part or a keepalive carries it.
- Finished file: a file whose parts are all finished, the tracking part included, and for which the broker never sends another attempt. A file whose upload failed is finished at once.
- Finished part: a part the broker stopped sending, because it committed, the leader rejected it with a non-retriable error, or its file's commit deadline passed.
- `finishedThrough`: the inclusive bound at or below which every file of the broker epoch is finished, whichever tracking partition issued its range.
- Floor: the higher of a broker row's last commit and its fenced-through mark. A new commit needs a pair above it.
- Gateway: a broker that a coordinator partition's user partitions route to for produce and fetch in one rack. Every broker computes the gateways from the metadata image.
- Gateway set: the coordinator partitions a broker is a gateway for in its own rack, or in the rack-less group for a broker without a rack.
- Kafka parity: a failure has Kafka parity when diskless topics survive it whenever a classic topic with the same replication factor and `min.insync.replicas`, written with `acks=all`, survives it, and lose no more than that classic topic loses when it doesn't.
- Keepalive: a lease request for a range the broker already holds. It resets the idle timer and can carry a finish report.
- Lease: a range of sequences that a tracking partition grants a broker for one epoch before the broker uses them.
- Lease release: the request that ends a broker's abandoned ranges at a tracking partition up to `b`. It carries the final finish report for those ranges.
- Leftover sequence: a leased sequence that the broker never committed by the time its lease ended.
- Local-log partition: a diskless user partition whose fetches need a broker-local log. It keeps replica routing instead of gateway routing.
- Maximal spread: replicas of a coordinator partition placed across `min(replication factor, number of racks)` racks.
- Misroute: a produce on the gateway path that reaches a broker that isn't the partition's gateway in the client's rack.
- Missed sequence: a sequence in a tracking partition's own ranges that it has no record of once a recorded `finishedThrough` reaches it.
- Planned sequence: a sequence in a range the broker still uses that no file has taken yet.
- Range fence: a fence over a range of sequences. An exception range fence goes to one participant, and a missed-sequence or lease-end fence goes to every partition. Pending fences to one participant and broker epoch merge into their hull.
- Saturation: the state of a rack with at least `N` candidates, where each gateway serves exactly one coordinator partition.
- Sequence source: the `ControlPlane` component that gives `FileCommitter` the next sequence for an epoch.
- Spare: a candidate broker with an empty gateway set.
- Tracking part: the part of a file sent to its tracking partition. It carries the participant set and the finish report, and it's empty when the tracking partition holds none of the file's batches.
- Tracking partition: the coordinator partition that leases a broker's sequences, tracks which partitions reference each of its objects, and deletes them. The broker picks it from its gateway set with a sticky rule.
- Unreconciled partition: a user partition in the leader's metadata image with no partition-created row applied yet. Calls that name it get `COORDINATOR_LOAD_IN_PROGRESS`.
- Urgent finish report: a finish report that holds an exception. If no tracking part carries it within one `produce.commit.interval.ms`, the broker sends an early keepalive.

## Future work

None of these items is part of this design. Each one depends on a decision this design makes, and the design must keep that decision intact. The longer-term direction behind them is in the [Diskless north star RFC](https://app.notion.com/p/40eb1ae1752781c399c3d711f92bb209).

### Grow the coordinator by splitting partitions

- With `slot % N` and a power-of-two `N`, doubling `N` moves exactly half of the slots of each coordinator partition `p` to the new partition `p + N`. The IDs of the existing coordinator partitions don't change.
- The growth needs a handoff that moves a slot's state from `p` to `p + N`: its logs, batches, and producer state. It also needs broker rows and tracking entries that stay correct for objects whose batches move, and a stored slot table once a slot's partition is no longer `slot % N`. Until that exists, the controller keeps rejecting `CreatePartitions` for `__diskless_metadata`.
- Splitting is the only fix for a cluster that outgrows its partition count.

### Merge WAL objects into extents

- A merged object is a new object with its own sequence and participants. Each participant appends a record that replaces a log's batch rows with extent rows pointing at the merged object, then releases the old objects through the emptiness check and the normal release path.
- This needs a record type for the swap, and a way to turn extent rows into the `BatchInfo` objects that `findBatches` returns. The extent requirement in [Local state](#local-state) keeps the schema ready for it.
- Merged objects in the WAL format keep every partition off a local log, so no partition then needs replica routing.

### Tiered metadata in the coordinator

- The remote log segment metadata of a diskless user partition would live in the coordinator partition that owns its batches. A composite `RemoteLogMetadataManager` would route diskless partitions to the coordinator and classic partitions to the topic-based implementation, because a broker loads one class from `remote.log.metadata.manager.class.name`.
- WAL pruning and segment registration then become records in one log for that user partition. A lookup goes to the coordinator leader on demand, so brokers don't consume every metadata partition, and the [Snapshots](#snapshots) mechanism bounds the metadata log. These are the RLMM problems that Jun Rao raised in the KIP-1163 discussion (JR1 and JR11).
- This design must keep two constraints for this to work:
  - A user partition's batches and its remote segments map to the same coordinator partition, by the same log key.
  - Pruning and cross-tier offsets stay decided on the broker that hosts the local log and recorded by the leader, as [Consolidation and cross-tier offsets](#consolidation-and-cross-tier-offsets) describes.

### Partitionless key ranges

- A log that covers a key range instead of a fixed partition would get a second log key encoding, carried in a new version of the records that hold the `Log` struct.
- The input that encoding hashes decides whether a range split moves a child range to another coordinator partition. If a parent range and its children map to one coordinator partition, the split is ordered with their commits in one log.

### Report-only orphan scanner

A scanner would list the cluster's WAL keys and report keys that no coordinator partition holds and no queue lists, without deleting them. It's the only detector for a held entry whose participant never reports, and for an upload that completes after its key was deleted.

### Object footer and recovery after losing a replica set

- Every WAL object would end with a footer that names its partition runs and an offset hint for each run. `ObjectFormat` keeps unused values, so a later format value can add it. Objects written before then have no footer and can't be recovered.
- After every replica of a coordinator partition `P` loses its storage, the operator triggers a recovery. A new leader of `P` refuses all work, lists the cluster's keys in storage, reads every footer, and filters the runs by what the surviving tracking partitions know. It then serves `P`'s records again at new offsets above a floor, with producer state rebuilt from the batch headers.
- Recovery assumes the KRaft metadata survives, because a footer names its runs by topic ID. It would remove clause 2's exception for the loss of every replica.

### Deletion bounds

- The bucket would keep deleted WAL objects recoverable for at least 7 days: S3 versioning with a noncurrent-version expiration of 7 days or more, GCS soft delete, or Azure blob soft delete. The cost is about double the WAL bytes for a 7-day-retention topic.
- Before a tracking partition leader's first `FileCleaner` pass, and every hour after that, the broker uploads and deletes a probe key and checks that the delete was soft. On S3 and Azure, every real delete response is checked too. A failed check stops deletes on that broker and sets a gauge, and produce and fetch continue.
- A fetch that finds a referenced key missing raises a metric and logs the key, and the operator restores the key by name. The batch rows then read again.
- A deletion-rate cap per tracking partition holds deletes at a multiple of the partition's trailing sequence rate, with a floor, so a runaway shows and a restore stays small.
- Under this bound, the lost-tail, apply-bug, and outside-delete rows in [Accepted risks](#accepted-risks) pass clause 3 as written. A restored object leaks, because the tracking partition already forgot it.

### Empty restart of a coordinator partition

- After every ISR and ELR candidate of `P` loses its storage, the operator would run an unclean election for `P`, which the controller accepts only when the ISR, the ELR, and `lastKnownElr` are all empty. The controller stores the election's leader epoch as `P`'s restart epoch.
- Every replica whose log predates that epoch truncates fully, and the leader appends a restart record that drops all of `P`'s state. The other tracking partitions fence `P` again, each broker raises `P`'s floor for its epoch before `P` accepts its parts, and `P` adopts the objects it can find again.
- `P` would come back empty, so its user partitions become writable again instead of staying offline.

### Extra gateways for a hot coordinator partition

A config would give a named coordinator partition extra gateways from a rack's spares, splitting its user partitions across them. It's the only lever that relieves a hot coordinator partition, since moving one only moves the heat. It needs a per-coordinator-partition traffic metric, so operators know which one is hot, and it costs extra commit parts for that coordinator partition. If the gateway pairs in [Benchmarks](#benchmarks) fail only on the busiest gateway at 21 or 45 brokers, that result is its motivation.

### Traffic-following tracking for brokers with an empty gateway set

When a broker with an empty gateway set gets steady misrouted produce for one coordinator partition, its tracking part stays empty unless it tracks at that partition. Following traffic needs damping, because scattered misroutes would switch it on almost every file. The misroute rejection ends stale traffic, and local-log traffic spreads over all coordinator partitions, so no known source of steady misrouted traffic exists. The entry stays for the case the `CommitPartRate` meter shows otherwise.

### A cluster-only scope for dynamic broker configs

An upstream set in `DynamicBrokerConfig` that mirrors `PER_BROKER_CONFIGS` and rejects per-broker values would replace the dedicated checks for `diskless.gateway.routing.enable` in `ControllerConfigurationValidator` and at startup. One key doesn't justify an upstream config-scope change, so the check stays in Inkless code.

### The per-read rule in Inkless

- A broker that isn't a replica would serve fetches at or above the WAL start of a consolidating or tiered partition from the diskless path. It answers fetches below the WAL start with `preferredReadReplica`, forwards them for fetch versions 4 to 10, and forwards `ListOffsets` and `OffsetForLeaderEpoch` lookups that need the local log or the tiered index.
- It takes consolidation clusters from about 8 parts per file to 1 at saturation. Catch-up reads inside the WAL move from the local log to cold object reads, about 0.04% of bytes on consolidation services and more for a replay-heavy workload.
- If Inkless consolidation instead writes merge-format objects that any broker reads, no read qualifies and none of this is needed. Support for `preferredReadReplica` in clients other than the Java client needs checking first.

### Cold-path fetches at brokers with an empty gateway set

This item is unanalyzed. A gateway could answer a fetch older than `fetch.lagging.consumer.threshold.ms` with a `preferredReadReplica` that names a broker in the client's rack with no gateway slot. These reads skip the cache on every broker, so moving them costs no cache hits, and it takes object-storage reads off the busiest gateways. Still to analyze: how much catch-up traffic there is to move, which spare a gateway picks, the client's extra round trip per partition per `metadata.max.age.ms`, and support in clients other than the Java client.

## Open items

The implementer decides these. None of them changes the design.

- Names and numbers of the private request types, the number of each ApiKey, and the error codes for a busy snapshot source, a stale epoch, and a stale sequence.
- The names of the two coordinator features, and the record set and extension tags of each level.
- The name of the internal controller config that lowers the minimum replication factor.
- SQLite driver choice, including its license and packaging check. The driver must ship a bundled SQLite at 3.51.3 or later, or at 3.44.6 or 3.50.7.
- The checkpoint trigger: polling the WAL size or a `wal_hook` callback.
- How a leader that can't rebuild its state gives up leadership of that one coordinator partition. The controller has no request that moves one partition's leadership off a broker, and the design needs one. Until then the leader marks only that partition offline and stops serving it. The directory stays online, and the broker doesn't halt.
- The commit deadline default, which must not exceed the produce request timeout. It also sets the runtime's write timeout.
- The idle lease timeout default, 15 minutes and provisional.
- The provisional defaults and the benchmark rules in [Benchmarks](#benchmarks) that settle them: `diskless.metadata.topic.num.partitions` by the partition-count rule, `diskless.metadata.read.threads` by the read-heavy and cache-storm rules, and `diskless.metadata.snapshot.max.concurrent.transfers` and the snapshot byte trigger by the snapshot-transfer rules.
- The stability bound of the gateway assignment, 5 extra moves per event, came from a simulation that used MD5. The reference implementation measures it again with murmur2 before the stability test fixes it.
- Where `SharedState` gets the gateway assignment implementation that the transformer and the produce path reach.
