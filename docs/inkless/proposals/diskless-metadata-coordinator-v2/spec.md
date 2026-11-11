# Diskless metadata coordinator

Status: ready-for-agent

Design record: [design.md](design.md). Decision log: [map.md](map.md) and the closed tickets in [issues/](issues/). This is version 2. It replaces [version 1's spec](../../docs/inkless/proposals/v1/diskless-metadata-coordinator/spec.md).

## Problem Statement

Diskless topics depend on a control plane that keeps their batch, file, and log state. Today the only durable implementation is the Postgres control plane, which every broker reaches through one shared database. A cluster that wants diskless topics without running that database has no option. The in-memory implementation is for tests, and it loses all state on restart.

KIP-1164 describes a Diskless coordinator that stores this state in an internal Kafka topic, so the cluster runs without an external database. Its full design has multiple coordinator partitions, file ownership handoff, orphan scanning, and new public protocol keys. This effort keeps multiple coordinator partitions and replaces file ownership with per-broker sequence leases. The rest is more than this effort needs, and it doesn't fit the control plane interface the broker code already uses.

Operators need a diskless control plane that keeps its state in Kafka, loses no data that a classic topic keeps through the same failure, costs no more per operation than the Postgres backend, spreads its load across brokers, and fits the existing control plane interface. The only shared broker code it changes is the commit path, which must order commits per coordinator partition so that one unavailable coordinator partition doesn't stall every commit.

A design review of version 1 found that its per-object intent added a replicated append before every upload, that its deletion safety depended on broker clocks, that admin requests could destroy or weaken the coordinator topic, that a deposed leader could reset a consumer's committed offset, that a replica could skip a record it couldn't read, and that the controller could elect a replica with a wiped disk. This version closes each of those gaps.

## Solution

Add a third control plane implementation that stores its state in an internal topic, `__diskless_metadata`, with a fixed number of partitions. Each diskless user partition maps to one coordinator partition by a hash of its log key, which is its topic ID and partition index, through a fixed set of slots. The leader of a coordinator partition is the only broker that assigns offsets and writes state changes for the user partitions that map to it. Every replica of a coordinator partition keeps a local SQLite copy of that partition's state, rebuilt from the log and from snapshots.

A broker splits each call by coordinator partition and sends each part to that partition's leader, locally or over a private RPC. A commit therefore goes to every coordinator partition its object has batches for. Each broker keeps one ordered commit queue per coordinator partition, so a part waits only for the previous file's part on the same coordinator partition. Each part succeeds or fails on its own, as a produce already can per partition, so no two-phase commit is needed. The leader appends one outcome record per committed part and replies only after the record is replicated.

The coordinator treats each uploading broker as an idempotent producer. Every file gets the broker's epoch and a sequence number at file close, and the object key is built from the cluster ID, broker ID, epoch, and sequence. Each broker leases ranges of sequences ahead of time from its tracking partition, a coordinator partition picked by a hash of its broker ID, so no round trip precedes an upload. Every coordinator partition keeps one row per uploading broker, which deduplicates retries and makes every lower sequence impossible once a higher one commits.

Every file also sends a part to its tracking partition, which records the object's participants. Participants report to the tracking partition when they no longer hold any batch of the object. The tracking partition fences the participants it never heard from, fences sequences it never recorded at every partition, and fences a lease's unused sequences when the lease ends. It deletes the object once every participant is released. The tracking partition's deletion queue is the only path that deletes an object whose commit may have reached a leader. No clock decides whether a deletion is safe.

The coordinator topic is protected: it joins Kafka's internal topics, and the controller and broker reject every request that would delete, create, extend, or weaken it. It requires `flush.messages=1`. Storage identity, not shutdown cleanliness, decides which replica may lead a coordinator partition. A replica stops applying at any record it can't read, and feature levels and a downgrade refusal keep that state out of reach.

Every replica keeps its own snapshots on its disk. A replica that needs records the log no longer holds fetches a covering snapshot over a private RPC, from an in-sync follower when one has it. Only the leader prunes the log. It advances Kafka's log start offset only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for.

Operators format a new cluster with the coordinator feature already finalized. `inkless.control.plane.class` must name the new class, and a broker refuses to start if it names another backend. Clusters that already run on Postgres aren't formatted with the feature, and they stay on Postgres.

## User Stories

1. As an operator, I want to run diskless topics without an external database, so that my cluster has fewer components to run and monitor.
2. As an operator, I want the coordinator state stored in an internal Kafka topic, so that I can back it up and replicate it with the tools I already use.
3. As an operator, I want the coordinator topic to have replication factor 3 by default, and a minimum ISR from `diskless.metadata.topic.min.insync.replicas` that defaults to `min(2, replication factor)` and is fixed at creation, so that one broker failure doesn't stop diskless writes.
4. As an operator, I want to set the replication factor of the coordinator topic with `diskless.metadata.topic.replication.factor`, so that a development cluster can run with one replica.
5. As an operator, I want the controller to create the coordinator topic on first enable, so that I don't create it by hand.
6. As an operator, I want replicas of the coordinator topic placed across racks where possible, so that losing one rack doesn't take out every copy.
7. As an operator, I want `inkless.control.plane.class` to name the coordinator class whenever the coordinator feature is finalized, so that broker configuration and cluster metadata select the same backend.
8. As an operator, I want the coordinator feature finalized when the cluster is formatted, so that the coordinator is active from the first startup.
9. As an operator, I want the controller to reject a broker that doesn't support the finalized feature, so that a broker without the private requests can't join.
10. As an operator, I want the broker to refuse to start when the feature is finalized and the class names the in-memory or Postgres backend, so that the two selectors can't disagree.
11. As an operator, I want clusters already on Postgres to stay on Postgres, so that enabling the coordinator can't break a running cluster.
12. As an operator, I want the coordinator to take snapshots on a time interval, so that the log stays bounded on an idle cluster.
13. As an operator, I want the coordinator to take snapshots after a set number of log bytes, so that a burst of commits doesn't grow the log until the next time tick.
14. As an operator, I want to configure the snapshot interval and byte threshold, so that I can trade disk use against snapshot cost.
15. As an operator, I want the coordinator to keep the newest snapshot and one older one, so that a replica that is mid-fetch can still finish.
16. As an operator, I want the coordinator log pruned only up to an offset that at least `min.insync.replicas` in-sync replicas hold a snapshot for, so that a leader failure still leaves a snapshot to rebuild from.
17. As an operator, I want a replica that falls behind the log start to catch up from a snapshot, so that it doesn't stop and wait for manual repair.
18. As an operator, I want a replica with a corrupt or missing local database to rebuild from a snapshot and the log, so that I don't restore it by hand.
19. As an operator, I want a broker restart to replay from its last applied offset, so that it doesn't reprocess the whole log.
20. As an operator, I want snapshots kept on broker disk rather than object storage, so that failover works while object storage is unavailable.
21. As a Kafka client, I want a produce to return the offsets the coordinator assigned, so that my records have the same ordering guarantees as on Postgres.
22. As a Kafka client, I want produce idempotence to hold across a leader change, so that a retried produce doesn't create duplicate records.
23. As a Kafka client, I want a non-idempotent producer's records to be written once even when the broker retries a commit, so that a broker-side retry doesn't duplicate data.
24. As a Kafka client, I want a fetch that the current coordinator leader answers to see every record committed before it, and a fetch that a deposed leader answers never to skip a record, so that a consumer doesn't miss data after a failover.
25. As a Kafka client, I want `ListOffsets` from the current coordinator leader to include every acknowledged record, and a deposed leader's answer to be no later than the current one, so that a stale answer makes me re-read records but never skip them.
26. As a Kafka client, I want `DeleteRecords` to move the log start on every replica, so that a deleted record stays deleted after a failover.
27. As a Kafka client, I want a topic deleted in the coordinator to stop accepting produce, so that a deleted topic doesn't keep taking data.
28. As a Kafka client, I want a produce to a new diskless partition to succeed once the coordinator has reconciled it, and to get a retriable error before then, so that the controller never waits on the coordinator.
29. As a Kafka client, I want a commit that is in flight during a leader change to succeed or fail cleanly, so that I don't see a half-applied write.
30. As a broker, I want a produce to wait for the coordinator reply only as long as its request timeout, and a commit deadline no longer than that timeout, so that a stalled coordinator doesn't stall the whole broker.
31. As a broker, I want the coordinator delegate to send each commit part to the current leader of its coordinator partition and retry on a `NOT_COORDINATOR` error within a commit deadline, so that a leader change looks like a short delay, not a failure.
32. As a broker, I want to find each coordinator partition's leader through the metadata cache, so that I don't add a discovery step.
33. As a broker, I want a fetch lookup that misses the batch coordinate cache to reach the leader, so that the reply reflects committed state.
34. As a broker, I want the cross-tier log start reports to reach the leader, so that remote storage cleanup stays on schedule.
35. As a broker, I want consolidation pruning to be decided on the broker that holds the partition's local log, so that the coordinator never needs a log it doesn't have.
36. As a broker, I want the prune floor on my partitions to move only after the coordinator confirms the prune, so that I don't delete a log the coordinator still needs.
37. As a broker, I want the delete-records path to report cross-tier log start changes to the leader, so that the logical earliest offset stays consistent.
38. As a controller, I want diskless topic creation and deletion to commit to metadata only, so that topic lifecycle doesn't depend on the coordinator being available.
39. As a partition leader, I want diskless log initialization to reach the coordinator after the controller commits the seal, so that a classic topic can become diskless.
40. As a partition leader, I want diskless log repair to reach the coordinator, so that a switch that stopped midway can be finished.
41. As a leader, I want retention enforcement to run only on the leader of each user partition's coordinator partition, so that each retention pass is applied once.
42. As a leader, I want topic purge to run only on the leader of each user partition's coordinator partition, so that a deleted topic's batches are removed once.
43. As a tracking partition, I want file deletion marking to run only at the uploading broker's tracking partition, so that no two coordinator partitions mark the same object.
44. As a tracking partition, I want object deletion to wait for the grace period, so that a reader still using an old file can finish.
45. As a leader, I want to serve only after my high watermark reaches the log end offset I saw at election, so that my replies include every record committed under earlier leaders.
46. As a leader, I want to reply with a retriable error while I catch up after election, so that brokers retry instead of getting a stale answer.
47. As a Kafka client, I want a fetch above the high watermark to get `OFFSET_OUT_OF_RANGE` only after the coordinator leader confirms its leadership with a barrier record, and a retriable `OFFSET_NOT_AVAILABLE` otherwise, so that a deposed leader never resets my committed offset.
48. As a developer, I want the shared control plane test suite to run against the new backend, so that its behavior matches the other backends.
49. As a developer, I want a cluster test for leader failover in the middle of a commit, so that the retry path is covered end to end.
50. As a developer, I want a cluster test for a replica that catches up from a snapshot, so that snapshot distribution is covered end to end.
51. As a developer, I want a cluster test where a conflicting control plane class refuses to start, and a broker that lacks the feature can't register, so that the format-time gate is covered.
52. As a developer, I want to inject a dropped reply after the leader has appended a record, so that the retry path can be tested deterministically.
53. As a developer, I want to inject a commit that arrives during the leader's catch-up, so that the high-watermark gate can be tested deterministically.
54. As a developer, I want a unit test of the snapshot writer and loader, so that I can diagnose a snapshot bug without starting a cluster.
55. As a support engineer, I want a clear error when a broker doesn't support the coordinator RPCs, so that I can find the broker that blocks the rollout.
56. As a support engineer, I want the coordinator to report its state as metrics, so that I can see lag, snapshot age, log size, the deletion queue, lease ends, barrier outcomes, stopped applies, and unknown-outcome commits.
57. As a support engineer, I want each private RPC to report errors with the same codes as the control plane method it wraps, so that existing dashboards and alerts still apply.
58. As a broker, I want an object deleted only after every participant has released it or been fenced, so that a late commit never points at a missing object.
59. As a broker, I want an object whose commit may have reached a leader deleted only through its tracking partition's queue, never by me directly, so that a late part never commits into a deleted object.
60. As an operator, I want a newly elected leader with a corrupt or missing database to rebuild from a follower's snapshot before it serves, so that a failover doesn't depend on the old leader's disk.
61. As an operator, I want a coordinator partition with no replica holding a snapshot that covers the log start to stay unavailable rather than serve partial state, so that a rebuild never silently loses data.
62. As an operator of a single-replica cluster, I want the log pruned from the leader's own snapshot, so that the log stays bounded when there is no other replica to confirm.
63. As a Kafka producer, I want a retried produce to return the same offsets while the first attempt is still pending, so that the log holds one copy of the data.
64. As an operator, I want `flush.messages=1`, `unclean.leader.election.enable=false`, `retention.ms` and `retention.bytes` at `-1`, `cleanup.policy=delete`, and the minimum ISR fixed on the coordinator topic, so that only the snapshot rule deletes its records and no config change weakens it.
65. As an operator, I want a crash during a snapshot to leave the previous snapshots intact, so that a replica never ends up without a valid snapshot.
66. As an operator, I want a resumed snapshot transfer never to mix bytes from two snapshots, so that a rebuilt database matches one position in the log.
67. As an operator, I want snapshot transfers served by an in-sync follower when one can, so that rebuilding a replica doesn't add load to the coordinator leader.
68. As an operator, I want a snapshot report to stop counting once its replica leaves the in-sync replica set, so that the log start never moves on a snapshot that may be gone.
69. As a broker, I want snapshot reports sent off the replica fetcher thread, so that an unresponsive leader can't push a follower out of the in-sync replica set.
70. As an operator, I want a topic purge to remove the same rows on every replica, so that followers don't keep state the leader dropped.
71. As an operator, I want a replica to rebuild by replaying the log before any snapshot exists, so that a new cluster has no unavailability window before its first snapshot.
72. As an operator, I want a power loss on a replica to cost only a replay from the log, so that it doesn't force a rebuild from a snapshot.
73. As an operator, I want to set the partition count of the coordinator topic with `diskless.metadata.topic.num.partitions`, so that commit and read load spread across several leaders.
74. As an operator, I want the partition count fixed once the topic exists, and `CreatePartitions` rejected for it, so that no user partition ever changes coordinator.
75. As an operator, I want a coordinator partition that loses its in-sync replicas to stop only the user partitions that map to it, so that one failure doesn't stop every diskless write.
76. As a Kafka client, I want a commit that succeeds on some coordinator partitions and fails on others to return offsets for the committed partitions and `KAFKA_STORAGE_ERROR` for the rest, so that a retry doesn't duplicate committed data.
77. As an operator, I want every object key leased at its broker's tracking partition before the upload, so that a crash after the upload never leaves an object no one will delete, and no round trip precedes the upload.
78. As an operator, I want a broker to renew its lease at the next coordinator partition when its tracking partition is unavailable, so that one unavailable coordinator partition doesn't fail whole objects.
79. As a developer, I want the user-partition mapping and the tracking partition pinned by a test with fixed expected values, so that brokers of different versions always agree on them.
80. As a broker, I want a leader to reject a part for a user partition that doesn't map to it, so that a broker that maps differently can't give a user partition a second writer.
81. As an operator, I want a broker to serve at most a set number of snapshot transfers across all its coordinator partitions, so that rebuilding a replaced broker doesn't starve the brokers that serve the snapshots.
82. As an operator, I want the coordinator partition count restricted to a power of two, so that the coordinator can later grow by splitting partitions without remapping every user partition.
83. As a Kafka client, I want a stuck coordinator partition to delay only the partitions that map to it, so that one failure doesn't stall every diskless produce.
84. As a Kafka client, I want a retried commit part to return the same per-batch results as the first attempt, including errors and duplicates, so that a failover doesn't change the answer my producer sees.
85. As an operator, I want the coordinator to reject commit parts and lease requests from an earlier incarnation of a broker, so that a restart ends every late commit from before it.
86. As a broker, I want coordinator appends rejected at once while a coordinator partition has fewer than `min.insync.replicas` in-sync replicas, so that a commit fails fast and a lease renewal can fall back instead of waiting for a timeout.
87. As a Kafka producer, I want a commit part that missed its deadline unable to commit once my broker's next commit to that coordinator partition applies, so that its late arrival can't land after newer batches.
88. As an operator, I want `flush.messages=1` fixed on `__diskless_metadata`, with no weaker mode, so that a correlated power loss never brings back references to objects that were already deleted.
89. As an operator, I want `DeleteTopics`, client `CreateTopics`, `CreatePartitions`, unclean `ElectLeaders`, client produce, and client `DeleteRecords` rejected for `__diskless_metadata` with no bypass, so that no admin action destroys or bypasses coordinator state.
90. As an operator, I want a replica that meets a record or snapshot it can't read to stop applying that coordinator partition instead of skipping it, so that replicas never silently diverge.
91. As an operator, I want the controller to refuse every downgrade of the coordinator features, so that no broker meets records newer than it can read.
92. As an operator, I want a broker rolled back before the feature level rises to rebuild from a snapshot in a schema it reads, so that an aborted upgrade costs a rebuild, not correctness.
93. As an operator, I want a broker that returns with a wiped, replaced, or reformatted disk never elected leader of a coordinator partition, so that surviving replicas never truncate to an empty log.
94. As an operator, I want a coordinator partition to recover on its own after every replica restarts uncleanly with intact disks, so that a correlated power loss needs no manual election.
95. As an operator, I want a silent broker's lease to end after `diskless.metadata.lease.idle.timeout.ms`, so that its unused keys are deleted without a restart.

## Implementation Decisions

Topology and placement

- The coordinator state lives in one internal topic, `__diskless_metadata`, with `diskless.metadata.topic.num.partitions` partitions, 8 by default. The default is provisional.
- The replication factor is set by `diskless.metadata.topic.replication.factor`, 3 by default, and is fixed at creation. A reassignment that changes the replica count gets `INVALID_REPLICATION_FACTOR`.
- The topic-level `min.insync.replicas` comes from the controller config `diskless.metadata.topic.min.insync.replicas`, which defaults to `min(2, replication factor)`. Startup validation requires it to be between that default and the replication factor. It's fixed at creation.
- The controller creates the topic on first enable, with `unclean.leader.election.enable=false`, `flush.messages=1`, `retention.ms=-1`, `retention.bytes=-1`, and `cleanup.policy=delete`. It places replicas across racks where possible.
- The leader of each coordinator partition is the single writer for the user partitions that map to it. Replicas fetch the partition with normal replica fetching. Replicas don't vote and no Raft protocol is used.
- The coordinator appends with `acks=-1`, so the leader rejects an append with `NOT_ENOUGH_REPLICAS` while the ISR is below `min.insync.replicas`, before the record reaches its log.
- The topic requires `flush.messages=1`, with no weaker mode. An unflushed tail across the coordinator partitions breaks invariants that span a tracking partition, its participants, and object storage, which doesn't roll back: a release that reaches disk at the tracking partition but not at the participant deletes an object that live batches reference, a lost fence or lease record lets a part commit into a queued key or leaves an object untracked, and a lost commit reuses offsets across 1/N of the cluster. The leader's fsync per linger batch matches a Postgres commit on the primary. The follower fsync is a second sequential flush, the price of Kafka parity.
- After the broker creates a `__diskless_metadata` partition directory, it fsyncs the log directory. A failed fsync takes the log directory offline.

Coordinator partitions

- A user partition maps to slot `Utils.toPositive(Utils.murmur2(logKey)) % S`, where `S` is the protocol constant 4096, and the slot maps to coordinator partition `slot % N`.
- The log key of a Kafka partition is the 16 bytes of the topic ID, most significant bits first, followed by the partition index as 4 big-endian bytes. It is the only log key encoding this effort defines.
- `diskless.metadata.topic.num.partitions` accepts only a power of two from 1 to 4096. With such an `N`, the slot mapping gives the same result as `Utils.toPositive(Utils.murmur2(logKey)) % N`, and brokers don't store a slot table.
- Every broker reads `N` from the metadata image. A call made before the topic exists gets a retriable error. `N` never changes after creation.
- A broker's tracking partition is `Utils.toPositive(Utils.murmur2(brokerId)) % N`.
- The mapping functions, the log key encoding, and `S` are part of the protocol. A test with fixed expected values pins them.
- Each leader recomputes the mapping for every user partition in a request it receives, and rejects a mismatch with `INVALID_REQUEST`.
- Each coordinator partition has its own log, SQLite database on each replica, snapshots, overlay, and leader. With `N=1`, the design reduces to a single coordinator.

Topic protection

- `__diskless_metadata` joins `Topic.INTERNAL_TOPICS`. It inherits the produce and `DeleteRecords` rejections, the internal flag in metadata responses, exclusion from topic listings and regex subscriptions by default, no tiered storage, and system-topic status, which keeps it classic.
- Explicit guards keyed on the topic name, with no bypass, reject: `DeleteTopics` with `INVALID_TOPIC_EXCEPTION`, after authorization; client produce, including the `__admin_client` client ID, unless the append origin is the coordinator, with `INVALID_TOPIC_EXCEPTION`; client `CreateTopics` and broker auto-create with `INVALID_REQUEST`; `CreatePartitions` with `INVALID_PARTITIONS`; `UNCLEAN` `ElectLeaders` with `INVALID_REQUEST`; and a reassignment that changes the replica count with `INVALID_REPLICATION_FACTOR`. Client `DeleteRecords` gets `INVALID_TOPIC_EXCEPTION` through the internal-topic status.
- The controller's config validation checks the topic's config after each alter and at creation, and rejects with `INVALID_CONFIG` any result where `flush.messages` isn't `1`, `unclean.leader.election.enable` isn't `false`, `retention.ms` or `retention.bytes` isn't `-1`, `cleanup.policy` isn't `delete`, or `min.insync.replicas` differs from its creation value. Setting a key to its required value passes. Other topic configs stay alterable.
- The three upstream internal topics keep upstream behavior.

Transport and protocol

- Each broker holds a delegate that implements the existing control plane interface. For each call, the delegate splits the request by coordinator partition, sends the parts in parallel, and merges the results back into request order. A part runs locally on its leader. On any other broker, the part goes to that leader over a private RPC.
- The new request types use private API keys in the 504 range and above. No public Kafka protocol changes, and the KIP's proposed keys 95 to 101 aren't used.
- A broker that receives a request for a coordinator partition it doesn't lead returns `NOT_COORDINATOR`.
- Each private request returns the same error codes as the control plane method it wraps. The stale-epoch and stale-sequence errors are non-retriable. An unleased sequence, a negative sequence, or a retry with a different batch count gets `INVALID_REQUEST`.
- The private request set is: lease sequences, commit file, find batches with a no-wait field, list offsets, delete records, initialize diskless log, repair diskless log, prune diskless logs, advance cross-tier log start offset, report release, fence sequences, fetch snapshot, and report snapshot offset.
- The private requests carry no tagged fields and no leader epoch. A leader epoch added later is a new request version.
- A produce reply can't carry the offsets a commit assigns, so commit has its own reply.

Log records

- The leader appends one outcome record per committed call or part. Every replica applies the records in log order, and only up to the high watermark.
- Each record carries the state change and every assigned value, so replicas reach the same state without recomputing it.
- The record types are: sequences leased, file committed, sequences fenced, participant state recorded, release accepted, lease ended, broker epoch retired, files deleted, barrier, partition created, topic deleted, records deleted, diskless log initialized, diskless log repaired, retention applied, log pruned, cross-tier start advanced, and deleted log purged. Implementers may merge or split them.
- The file-committed record carries the object key, the uploader's broker ID, broker epoch, and sequence, the tracking partition, the participant set, the committed batches with their offsets, and the outcome of each other batch.
- Every change to a diskless partition's coordinator state is a record, including its creation and deletion. KRaft stays the source of intent.
- Apply reads nothing outside its own coordinator partition's log and the replica's cluster ID, and it never reads a clock, so the same log produces the same state on every replica. Every time-based decision is a record the leader appends.
- Records use Kafka's message-JSON conventions with no tagged fields. Every schema change is a new version or a new type, gated by the coordinator feature's level.
- Every record names the log it changes with a `Log` struct that holds `TopicId` and `PartitionIndex`. A later log key encoding is a new record version.

Record compatibility

- A replica that meets an unknown record type, an unknown value version, or an unrecognized tagged field stops applying that coordinator partition at that offset. Its SQLite stays at the last applied offset, it stops serving, a leader resigns, and it keeps fetching bytes so it stays in the ISR. A gauge and an error log report it. No record type is ignorable, because every record is a delta that no later record overwrites.
- Apply checks the unknown tagged fields of each record and each nested struct against a registry of recognized extension tags. The registry is empty upstream. Inkless registers its tags at 100 and up, encoded by hand as raw tagged fields, so the schema JSON stays identical to upstream.
- The leader reads the finalized feature level from its metadata image when it builds each record, and writes the highest version that level allows.

Speculative state

- The leader keeps an in-memory overlay of records it has appended and not yet applied. The overlay runs from the offset after the applied offset through the last written offset. Followers keep no overlay.
- An overlay entry holds the object key, the uploader's pair, the assigned offsets, the outcome of each batch that didn't commit, the producer epoch and sequence numbers, and a pending lease or fence when the entry is one.
- The leader checks and reserves on one thread. A commit reads SQLite plus every earlier overlay entry.
- The leader collects reservations for one append linger interval, then appends them as one log batch.
- When the high watermark passes an entry, the leader applies it to SQLite, removes it from the overlay, and replies.
- If an append fails, the leader resigns, or the log truncates an entry, the leader removes that entry and every entry after it and fails their parked replies with a retriable error.
- The overlay has a cap. Past that cap, the leader rejects a new commit with a retriable error.
- A new leader starts with an empty overlay.

Write path

- The leader runs the overlay loop on `CoordinatorRuntime`, with one shard per coordinator partition, as the group coordinator does for `__consumer_offsets`. The runtime supplies the single writer thread, the append linger, the revert after a failed append, the parked replies, the timer for idle leases, and the `NOT_COORDINATOR` failure on resignation.
- The shard stores the overlay in timeline structures backed by a `SnapshotRegistry`. Committed state stays in SQLite.
- The runtime writes through a `PartitionWriter` that Inkless owns and that appends with `acks=-1`. The upstream `CoordinatorPartitionWriter` appends with `acks=1`, and the group and share coordinators keep using it unchanged.
- The runtime's write timeout equals the commit deadline. Upstream `CoordinatorRuntime` doesn't change.
- A custom `CoordinatorLoader` replays from the applied offset stored in SQLite.
- A follower applier writes fetched records to SQLite up to the high watermark. The runtime runs on the leader only.

Sequences, keys, and leases

- At file close, `FileCommitter` gives each non-empty file the current broker epoch and the next sequence, from a sequence source that the control plane provides. The pair orders by epoch first. The sequence starts at 0 in each epoch, isn't persisted, and allows gaps. A file that closes with no leased sequence fails at once with `KAFKA_STORAGE_ERROR` and isn't uploaded.
- The default sequence source is a counter that restarts at 0 on a new epoch and never returns empty, so the Postgres and in-memory backends behave as today.
- The object name is `<clusterId>.<brokerId>.<brokerEpoch>.<sequence>` under the existing key prefix handling. `FileCommitter` and the backend build it with one shared function, and the backend never parses it. The upload job no longer generates a random UUID. Every upload attempt reuses the key, and every storage backend overwrites unconditionally.
- A broker leases a range `[a, b]` of at most `K = 1024` sequences, a protocol constant, at its tracking partition. It picks `a` above every range it requested in that epoch at any partition. The tracking partition stores the range and the uploader's key prefix in its lease table and replies after the record applies. A repeat of a held range is a no-op.
- The broker asks for its first lease when `FileCommitter` starts and when its epoch changes, and renews in the background once fewer than `K/2` leased sequences remain. If its tracking partition doesn't answer, it asks the next partition in order, and its sequence source holds the short list of ranges and hands them out in increasing order.
- Every commit part names the tracking partition that issued its sequence's lease.

Commit path

- `FileCommitter` commits on a single thread today, so one unavailable or slow coordinator partition would stall every later file on every broker. This effort changes that shared caller, not only the backend.
- The control plane maps each user partition to a commit queue. The new backend maps it to its coordinator partition. The Postgres and in-memory backends map every user partition to one queue, which keeps today's order.
- The control plane gets a per-part commit call for the batches of one file that map to one queue, with the broker epoch, the sequence, and the object key. The Postgres and in-memory backends call `commitFile` for it.
- `FileCommitter` keeps one ordered queue per commit queue, in file rotation order. A file's part on a queue runs once the file is uploaded and the previous file's part on that queue has finished. It doesn't wait for parts on other queues.
- Every file also sends a part to its tracking partition, carrying the participant set. The part is empty when the tracking partition holds none of the file's batches.
- Every part of a file shares the file's commit deadline, measured from the moment the file closes, and the deadline doesn't exceed the produce request timeout. The backend retries each part until the deadline, so the queue starts the next part only after the earlier one has finished. A file completes once every part has an answer, and the per-batch results are merged in request order.

Commit and retry

- Every coordinator partition keeps one row per uploading broker: its epoch, the last sequence that committed there with that commit's complete per-batch result, a fenced-through pair, and a retired marker. The floor is the higher of the last commit and the fenced-through pair. Rows are never deleted.
- A part runs on its leader in this order: check the mapping; reject a stale epoch when the leader's image holds a newer epoch for the uploader; compare the pair with the broker's newest pending overlay entry or its row; run the producer epoch and sequence checks for a new commit; assign offsets and add one file-committed record; and, when the high watermark passes the record, apply it to SQLite, remove the overlay entry, and reply.
- The acceptance rules: a retired row or a lower epoch is a stale epoch. The pair equal to the last commit is a retry, which waits for a pending entry or replies at once with the stored result, also after retention removed the batches. A pair above the floor, or no row, is a new commit. Anything else is a stale sequence, with the floor in the reply. `logStartOffset` in a reply always comes from current state.
- Only the tracking partition checks the lease: the part sent to it must carry a sequence inside one of its ranges for that epoch, or it gets `INVALID_REQUEST`. Participants rely on their floor.
- A part at the image's epoch, at a newer one, or for a broker with no registration in the image isn't rejected for its epoch, because the image can lag. A fenced uploader isn't rejected.
- A new commit at a newer epoch replaces the row. A commit whose every batch fails is an ordinary new commit with zero committed batches, which advances the row and releases the object at once.
- A part that is rejected before the producer checks records nothing and leaves the row unchanged.
- Each leader appends a broker-epoch-retired record when its image shows a newer epoch or no registration for a broker, on each image change and once it starts serving after an election. Apply moves the row to the newer epoch, or sets the retired marker, and at a tracking partition ends the old epoch's leases.
- The delegate retries each part on its own against the current leader of its coordinator partition after `NOT_COORDINATOR` or a retriable error, with the same pair and object key. It stops at the file's commit deadline.
- At the deadline, batches on confirmed parts get their per-batch results, and batches on unconfirmed parts get `KAFKA_STORAGE_ERROR`. Their outcome is unknown until the broker's next commit to that coordinator partition applies, a fence raises the floor there, its epoch retires, or its lease ends. If any part committed, the file returns per-batch results. If none did, the file fails as a whole.
- Apply rules for file records are strict. A file-committed record whose pair isn't above the floor, or a lease record that overlaps a held range, is an invariant violation, and the replica stops applying that coordinator partition. A fence at or below the floor, a repeated epoch retirement, and a files-deleted record for a key not in the queue apply as no-ops.

Object tracking and deletion

- Each broker's tracking partition holds its leases, records each object's participant set from the tracking part, and keeps the deletion queue for the broker's objects. Each key has exactly one tracking partition: the one that issued its range.
- A participant releases an object when an apply removes its last batch of it. Five records release objects: records deleted, retention applied, log pruned, deleted log purged, and a file-committed record whose every batch failed. Apply runs a `NOT EXISTS` check over an index on `batches(file_id)` for every file it touched, in the same transaction.
- The participant leader reports each release to the tracking partition named in its part, retried with Kafka's `ExponentialBackoff` client defaults. The tracking partition records it, and the participant then appends a release-accepted record. A new participant leader resends only the releases without one. An all-fail part or an empty tracking part is released in the apply that records it.
- A fence names a broker, an epoch, and a range of sequences. A participant raises its fenced-through pair to the range end if that's above its floor, and answers with the sequences in the range whose batches it still holds. The rest are released and can't commit there again. A part pending before the fence commits first and is answered referenced. A part that arrives after it gets stale-sequence.
- The tracking partition fences silent participants once it and every participant it has heard from are released, at most once per participant per object. It fences a missed sequence, one in its own ranges that it never recorded while it recorded a later one, at all `N` partitions and builds the participant set from the referenced answers. After a lease ends, it fences the lease's unused sequences at every partition.
- A lease ends on a broker-epoch-retired record, a lease request at a newer epoch, or a lease-ended record. The tracking partition appends the lease-ended record when a broker has sent no part and no lease request that names it for `diskless.metadata.lease.idle.timeout.ms`, 15 minutes by default. The timer is a `CoordinatorTimer` per broker on the leader's monotonic clock, rescheduled by every part and lease request that names the partition, including rejected ones. A new leader starts every timer at its election and resends every unanswered fence.
- Only tracking partitions keep a deletion queue. A committed object enters it in the apply that completes its set of released participants. Missed and leftover keys enter it from fence answers that no partition holds, with key paths that apply builds from the uploader's prefix in the lease table and the replica's own cluster ID.
- `FileCleaner` keeps running on every broker. `getFilesToDelete` returns the union of the queues of the tracking partitions this broker leads, and an empty list elsewhere. Each queue row carries the mark time from its marking record and waits `file.cleaner.retention.period.ms`, 1 minute by default. `deleteFiles` appends one files-deleted record per tracking partition. Deleting a missing key counts as success, and deleting a key twice is harmless.
- The backend declares a capability that turns off the broker's direct delete. `FileCommitJob` checks it before its `ControlPlaneUnavailableException` branch, and the backend's `isSafeToDeleteFile` returns false without a request. The Postgres and in-memory backends don't declare it.
- An object leaks only when storage completes an upload after its key was deleted. A zombie that is cut off can leak at most the rest of its lease. No scanner deletes objects. A report-only scanner is future work.
- No clock decides whether a key is deleted, only when. The idle lease timeout and the grace period are the only time-based decisions that touch deletion.

Local state

- Each replica of a coordinator partition keeps that partition's state in its own local SQLite database. The database is a cache. The log is the source of truth.
- Every apply stores the applied offset and the leader epoch of that record in the same transaction as the state change.
- On open, a replica checks the stored offset and epoch against the log. It keeps SQLite only if the log still holds that offset and the record there carries that epoch.
- Invariant: the applied offset is always at or below the high watermark. SQLite holds only records at or below the high watermark, so a replica never reverses an applied row.
- SQLite runs in WAL journal mode with `synchronous=NORMAL`. A power loss can roll back the most recent applies, together with their applied offset, and the replica replays them from the log.
- The schema holds logs, files, batches, producer state, broker rows, and the applied offset on every partition, and lease tables, tracking entries, and the deletion queue on tracking partitions. It must support indexed lookup by topic partition and offset, by object key, by `batches(file_id)`, and by uploader broker, epoch, and sequence.
- A coordinate row must be able to describe an extent: an offset range mapped to an object and a byte range, with the producer fields optional. This effort writes only WAL batch rows, and each one carries its producer fields.
- Each database stores its schema version. A binary migrates any older schema it supports forward, and treats a newer schema as a missing database, which it rebuilds.
- The SQLite driver is a JDBC driver with a bundled native library. Its license and packaging must pass the repo's dependency checks before the choice is final.

Snapshots

- Each coordinator partition runs the snapshot rules on its own, with its own log start, snapshots, and reports.
- The log start is Kafka's own log start offset of the coordinator partition. Only the leader advances it, with the same call the delete-records path uses, and followers adopt it from fetch responses.
- The leader advances the log start only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for. The new log start is the lower of the leader's own older snapshot and the highest offset that at least `min.insync.replicas - 1` other in-sync replicas have reported a snapshot at or beyond. A single-replica topic advances from the leader's own older snapshot alone.
- The log start stays at offset 0 until the first advancement. A replica that loses its database before then replays from offset 0 and needs no snapshot.
- Every replica takes its own snapshot when either `diskless.metadata.snapshot.interval.ms` elapses, 1 hour by default, or `diskless.metadata.snapshot.max.bytes.between.snapshots` of log has been applied since its last snapshot, 1 GiB by default. Both defaults are provisional.
- A snapshot uses the SQLite backup API. Its offset and epoch come from the applied-offset row inside the finished copy. The replica fsyncs the temporary file, renames it to a final name built from the offset, the epoch, the schema version, and a checksum, and fsyncs the directory, before it deletes the snapshot that falls out of the retained pair.
- Snapshots are written to the broker's local disk, never to object storage. Each replica keeps the newest snapshot and one older one, as a constant.
- A replica reports its newest snapshot offset to the leader on a private request after a snapshot, on a new leader, and when it rejoins the in-sync replica set, from the broker's scheduler, with Kafka's `ExponentialBackoff` client retry defaults as constants. A report counts only while its replica is in the leader's committed in-sync replica set.
- A replica fetches a peer's snapshot only when it needs records below the log start. It fetches from an in-sync follower that holds a covering snapshot, and from the leader only when no follower does.
- Every fetch-snapshot request names the snapshot ID, a byte position, and the highest schema version the receiver reads. A source with no covering snapshot at or below that version answers `SNAPSHOT_NOT_FOUND`, and the receiver tries the next replica. Rotation keeps a file while its transfer is active, until `diskless.metadata.snapshot.transfer.idle.timeout.ms`, 60 seconds by default, passes without a chunk request.
- The request and response follow KRaft's fetch-snapshot schemas, with chunks of at most 8 MiB sent from the file with zero copy. A broker serves at most `diskless.metadata.snapshot.max.concurrent.transfers` active transfers, 1 by default, and answers a request beyond the limit with a retriable busy error.
- A replica serves its finished snapshot files whatever the state of its live database. A replica whose database fails the open check, runs ahead of its log, or has a newer schema rebuilds before it serves, a newly elected leader included.
- If no replica holds a snapshot that covers the log start, a broker that needs one can't rebuild, and that coordinator partition stays unavailable for diskless writes rather than serving partial state.

Reads

- Only the leader answers reads. The delegate splits find-batches by coordinator partition, sends the full `fetchMaxBytes` budget with each part, merges the results in request order, and trims them to that budget.
- Brokers absorb most fetch-side lookups in the existing batch coordinate cache. A cache miss goes to the leader. Follower reads aren't part of this effort.
- A deposed leader serves committed but older state until it learns of the new leader, as a classic deposed leader does. No leader lease and no read barrier run on the normal read path. Every stale read is lower than or equal to the current answer, or unknown, except find-batches above the high watermark. A new read RPC that breaks this rule needs the barrier too.
- When find-batches gets an offset above the high watermark, the leader parks the request and appends a barrier record. Requests that arrive while a barrier is open share it. Once the barrier commits, the leader checks the offset again and answers `OFFSET_OUT_OF_RANGE` or returns batches.
- Each parked request gets its own 500 ms deadline from its arrival, on the runtime's timer, and gets `OFFSET_NOT_AVAILABLE` when it fires. The 500 ms is a constant. A barrier from a deposed leader never commits.
- The delayed-fetch probe never runs the barrier. The control plane gets a default probe method that calls find-batches, and the new backend sets a no-wait field that makes the leader answer above the high watermark at once with `OFFSET_NOT_AVAILABLE`.
- `findBatches` stays synchronous. A parked part holds a fetch metadata thread on the calling broker for at most 500 ms, which costs throughput only in a deposed window. An asynchronous find-batches is the alternative for the design review group.

Capacity and isolation

- Each coordinator partition's leader serves every commit part, lease request, release report, fence, cache-miss read, and maintenance job for its user partitions and tracked brokers. SQLite shares that broker's heap, page cache, disk, and garbage collector with its own partitions. The severity of this risk is open, and the benchmarks are out of scope for this version's review.
- Splitting reduces work per batch, not the number of commit calls, because an object usually holds batches for most coordinator partitions. A produce waits for its slowest part.
- Benchmarks compare this backend with the Postgres backend on `FileCommitTime`, `FileCommitWaitTime`, `FindBatchesTime`, and `FileCommitErrorRate`, at `N=1` and at `N=8`. [design.md](design.md#capacity-and-isolation) lists them.

Metrics

- The coordinator registers a runtime metrics group, `diskless-metadata-coordinator-metrics`, that extends the upstream coordinator runtime metrics, and one Yammer metrics group named after the backend class.
- The backend's metrics are unknown-outcome parts, stale-epoch and stale-sequence rejections, mapping mismatches, lease renewals and fallbacks, files failed with no lease, idle lease ends, fences by type and answer, the deletion queue's size and oldest age, barrier outcomes, and coordinator partitions whose apply has stopped. [design.md](design.md#metrics) describes each one.

Leader change

- Each coordinator partition changes leader on its own, and a change stalls only that coordinator partition's parts, lease requests, reads, and tracked objects.
- A new tracking leader starts every idle timer at its election and resends every unanswered fence, and a new participant leader resends the releases that have no release-accepted record.
- A new leader starts with an empty overlay. It rejects requests with a retriable error until its high watermark reaches the log end offset it saw at election.
- A leader change may truncate records that were never replicated. A replica discards fetched records above its applied offset that the new leader doesn't have. Because the applied offset never exceeds the high watermark, no applied state is affected.
- At registration, the controller removes a broker from the ELR and last-known ELR of every `__diskless_metadata` partition whose assigned directory for that replica isn't in the new registration, and applies the same rule when a replica moves off a directory the broker no longer registers. An unassigned directory counts as changed. The guard walks only that topic's partitions.
- An unclean shutdown keeps a broker in the ELR of `__diskless_metadata` partitions, as a clean shutdown does. The last-known-leader election stays on as a fallback.
- The cluster-wide ELR clear skips `__diskless_metadata`, on the controller and in the metadata image.

Lifecycle

- The controller never calls the coordinator. Diskless topic creation and deletion commit to cluster metadata only. Each coordinator partition's leader learns the metadata image from `BrokerMetadataPublisher.onMetadataUpdate`. On election and on each metadata update it diffs the user partitions in its SQLite against the diskless user partitions in the image that map to it. It appends a partition-created record for each one the image has and SQLite doesn't, and a topic-deleted record for each topic ID SQLite has that the image doesn't. A produce to an unreconciled partition gets a retriable error until reconciliation finishes.
- The diff counts pending overlay entries as well as SQLite rows, so a record isn't appended twice. A repeated partition-created or topic-deleted record applies as a no-op.
- Reconciliation is the retry for a delete: a topic ID still in SQLite and absent from the image is appended again on the next metadata update and on the next election.
- The leader of a switched partition calls `initDisklessLog` after the controller has committed the classic-to-diskless seal, from `InitDisklessLogManager`. That same partition leader calls `repairDisklessLog` from `ReplicaManager`.
- A user partition's creates, deletes, initializations, and repairs go to the same coordinator partition as its commits, so they are ordered with every commit to that user partition.

Maintenance

- Retention enforcement and topic purge run on each coordinator partition's leader, for its own user partitions. Each run appends its outcome records to the log.
- Topic purge runs in capped passes. Each pass appends one deleted-log-purged record per partition it touched. Applying the record removes the partition's batches below the boundary, or all of them without one, and drops the partition's log and producer state rows when the partition is empty. The objects whose last local batch the pass removed are released and reported.
- Deletion runs on tracking partition leaders, after the existing grace period.

Consolidation and cross-tier offsets

- The broker that holds a partition's local log decides the safe prune offset, from its remote-storage high offset and the classic-to-diskless seal.
- The broker sends the prune or cross-tier start request to the leader. The leader appends the outcome record.
- The broker advances its own prune floor only from the leader's response.
- The cross-tier start reporter and the delete-records path follow the same pattern.
- The leader never reads a partition log.

Rollout

- An upstream coordinator feature, for example `diskless.coordinator.version`, selects the backend and caps the record set. Level 0 means the coordinator is off, and level 1 is the initial record set, including the barrier record. A new cluster finalizes it at format time with `kafka-storage format --feature`.
- An Inkless-owned feature, for example `inkless.diskless.coordinator.version`, gates the Inkless extension tags. Each of its levels declares the upstream level it needs. It doesn't need to be finalized at format time.
- The controller rejects an upgrade unless every registered broker and controller supports the new level, rejects registration from a broker that lacks a finalized level, and refuses every downgrade of both features.
- To upgrade, roll every broker to the new binary, then raise the level. Rolling back before the raise costs a rebuild on each replica that migrated its SQLite schema, never correctness.
- The controller creates `__diskless_metadata` from `diskless.metadata.topic.num.partitions`, `diskless.metadata.topic.replication.factor`, and `diskless.metadata.topic.min.insync.replicas`. Changing those configs afterward has no effect.
- `inkless.control.plane.class` must name the new class. If it names the in-memory or Postgres class while the feature is finalized, the broker refuses to start.
- Diskless topics still need the existing Inkless storage configuration and `diskless.enable=true`.
- There is no rollback onto Postgres. Clusters already on Postgres are never formatted with the feature.

## Testing Decisions

Good tests check behavior through a public seam: the control plane interface, a broker's observable behavior, or a client's observable behavior. They don't check internal structure such as record layout or the order of internal calls.

Four layers are used. Each one is an existing seam unless noted.

1. The control plane interface. The shared behavioral suite runs unchanged against the new backend, in a single-broker setup, at `N=1` and at `N=8`. One broker leads every coordinator partition, so the second run covers request splitting, result merging, tracking parts, and releases. The suite covers commits and fetches, topic and partition creation, deletion and purge, delete records, retention, file deletion, producer idempotence, diskless log initialization and repair, cross-tier offsets, and consolidation pruning. It adds:
   - A partial send followed by `Unavailable`, and a delegate replaced between two parts of one file. The broker never deletes the uploaded object directly. On the new backend the tracking partition's queue deletes it after the grace period. On the Postgres and in-memory backends the direct delete still happens.
   - The marking rule for each of the five batch-removing records.
2. Coordinator backend tests, a new layer for cases that one clock or one state can't express. Each test that involves time runs at `N ≥ 2` with its own mock clock per uploader and per leader:
   - Skewed clocks of up to a day across commits, idle lease ends, epoch retirement, a failover, and deletion cycles. Every batch row's object still exists, and no queued key matches a batch row.
   - An uploader that pauses until its lease ends and its key is deleted, then commits. It gets stale-sequence, the broker deletes nothing, and no batch row points to the key.
   - A stale deletion list from a deposed leader. Every key it deletes was queued and has no batch row.
   - The stale-read rule: for every list-offsets mode, an older state's answer is lower than or equal to a newer one's, or unknown.
   - Apply stop on an unknown record type, value version, and tag at the top level and in a nested struct, and normal apply for a registered extension tag.
   - The write gate per record type and feature level, including a leader with a lagging metadata image.
   - SQLite schema migration forward, a newer schema treated as missing, and a snapshot fetch above the receiver's schema.
   - Golden records: a checked-in file holds every record type at every level, and the current binary decodes all of it.
3. The multi-broker cluster test harness. Tests start a real cluster with real replica fetching and drive it through produce, fetch, admin, and controller operations. The cluster tests cover:
   - Leader failover in the middle of a commit, followed by the retry path, which resolves through the broker's epoch and sequence. The client sees one successful produce, with no duplicate records.
   - A leader change that truncates unreplicated records. The follower's state matches the new leader's state, and no applied record was undone.
   - A retry that arrives while its first attempt is pending on the leader. The client sees one record and one set of offsets, for idempotent and non-idempotent producers.
   - A commit whose parts land on several coordinator partitions, where one part fails. The committed partitions return offsets, the failed ones get `KAFKA_STORAGE_ERROR`, and the tracking partition deletes the object only after every participant is released.
   - A tracking partition that is down. The broker leases at the next partition, and only the down coordinator partition's user partitions fail.
   - A broker that stops before the upload, and one that stops after the upload and before any part commits. The epoch end marks the unused range, and the queue deletes both keys after the grace period.
   - A broker that stops after some parts commit. The tracking partition fences the participant that never committed, and deletes the object only after the committed participants release it.
   - An object whose batches expire on one coordinator partition while another still holds batches. The object stays until both are released.
   - One coordinator partition offline while produce runs to user partitions on every coordinator partition. Parts on the other coordinator partitions commit without waiting for the offline partition's deadline.
   - A part that reaches its deadline, followed by the same broker's next commit to that coordinator partition. A late request for the earlier part gets stale-sequence, and the later batches commit.
   - A coordinator partition whose ISR drops below `min.insync.replicas`. Commit parts fail at once rather than at the deadline, and lease renewals fall back to another partition.
   - A retried part where some batches failed and some were duplicates, sent to a new leader after a failover. The retry returns the same per-batch results as the first attempt.
   - A broker that restarts with a part in flight. The leader rejects the old epoch, and the epoch-retired record ends its leases.
   - A tracking partition leader change with open entries. The new leader restarts the idle timers, and the participants resend their unaccepted releases.
   - A silent broker whose idle lease ends. Its leftover keys are deleted.
   - A missed sequence: the part to the tracking partition fails while others commit. A later commit there fences it at every partition, and the object stays until its participants release it.
   - A part that names a user partition the receiving coordinator partition doesn't own. The leader rejects it with `INVALID_REQUEST`.
   - A replica that catches up from a snapshot after the log start has moved. It serves the same reads as the leader afterward.
   - A newly elected leader whose database is missing. It rebuilds from a follower's snapshot, and it serves no request before the rebuild finishes.
   - A coordinator partition where no replica holds a snapshot covering the log start. Its user partitions stay unavailable for diskless writes, and no partial state is served.
   - A leader whose older snapshot is ahead of every other in-sync replica's reported snapshot. The log start stays put, and produce and fetch continue. Once another in-sync replica reports a snapshot at that offset, the log start advances. After the leader stops, that replica's snapshot still covers the log start.
   - A single-replica topic. The leader advances the log start from its own older snapshot.
   - A follower that reports a snapshot and then leaves the in-sync replica set. The log start doesn't advance on its report until it rejoins and reports again.
   - A snapshot source that rotates or restarts during a transfer. The receiver never loads a file mixed from two snapshots.
   - A rebuild while an in-sync follower holds a covering snapshot. The follower serves the transfer, not the leader.
   - A replica that loses its database before the log start has first moved. It replays from offset 0.
   - A topic deleted and purged. Every replica drops the same batches and rows as the leader, and the tracking partitions delete the emptied objects.
   - A cluster formatted with the feature, where a commit goes through the coordinator. A broker whose class names another backend refuses to start. A broker that doesn't support a finalized feature level can't register.
   - A produce that is in flight during a leader change and that fails cleanly or succeeds once.
   - A topic created and a topic deleted while a coordinator leader is down. The leader reconciles on return. A produce before reconciliation gets a retriable error.
   - A consolidated topic whose prune floor moves only after the leader confirms it.
   - Admin protection: the controller creates the topic with the fixed configs and the configured minimum ISR, topic listings hide it by default, and delete, create, `CreatePartitions`, a `flush.messages` alter, an unclean election, and an `__admin_client` produce are all rejected through the admin client. A metadata request with auto-create on doesn't create the topic.
   - A deposed leader: a broker cut off from the controller and its followers keeps answering, and a consumer that starts from an offset committed under the new leader never resets its position under `earliest` or `latest`. It reads from that offset once the broker learns it was deposed.
   - A genuine seek past the end resets by the consumer's policy.
   - A rolling upgrade followed by a level raise, and a rollback before the raise, after which the rolled-back replicas rebuild and rejoin the ISR.
   - Every replica of a coordinator partition killed uncleanly and restarted with intact disks. The partition recovers with every acknowledged record.
4. A new injection hook on the client side of the private RPC, which is the one new seam. It lets a test drop a reply after the leader has appended a record, drop a lease reply, delay a request during the leader's catch-up, deliver a part's request after its deadline, and stop a broker between the lease, the upload, and the commit. These cases are otherwise hard to reproduce on a real cluster. The hook changes nothing about the wire protocol.

Seam tests sit next to the code they guard:

- On the coordinator shard, with the runtime's mock partition writer and mock timer: the barrier's outcomes, its per-request 500 ms deadline from arrival, a shared barrier with separate deadlines, a commit part that waits longer than 500 ms without timing out, at most one barrier per flush under adaptive linger, the idle timer racing a part, a forward clock jump, a leader pause between the timer and the append, and the acceptance rules for a file with no leased sequence, a fence racing a part, and a broker bug's `INVALID_REQUEST`.
- In the delayed-fetch or replica manager tests: with the cache off, a probe above the high watermark returns at once and appends no barrier.
- Next to each controller and broker guard: internal-topic status, `CreateTopics` and `DeleteTopics` in the controller request handler, `CreatePartitions`, unclean elections, reassignments, the storage change guard, the unclean-shutdown ELR rule, and the cluster-wide ELR clear in the replication control manager and the topics delta, every fixed config in the configuration control manager, produce and `DeleteRecords` in the replica manager, the directory fsync in the log manager, the downgrade refusal and feature dependency in the feature control manager, and the controller config range for the minimum ISR.
- The shared key-name function: the cluster ID keeps two clusters' keys apart.
- The mapping function and the tracking partition get a unit test with fixed expected values, so a change to either fails the build. The same test checks that the slot mapping equals `Utils.toPositive(Utils.murmur2(logKey)) % N` for every allowed `N`, and that the config rejects an `N` that isn't a power of two.
- Snapshot writing, rotation, loading, and chunk serving get unit tests on a temporary directory, with a fault hook at each step of rotation and transfer. They cover a tag read while applies run during the backup, a crash at each rotation step, a transfer whose source rotates or restarts before the receiver resumes, a checksum mismatch on load and on serve, and the schema version in the file name. This is a deliberate exception to the public-seam rule: no public seam reproduces these cases deterministically. The cluster tests still cover snapshot behavior end to end.
- Apply rules for file records, broker rows, leases, fences, releases, and the deletion queue get unit tests at the replica state layer, with records appended directly to a test log. No public call can produce a duplicate or conflicting record, so this is a second deliberate exception to the public-seam rule.

Prior art:

- The shared behavioral suite, with its in-memory and Postgres subclasses. The new backend gets a subclass in the same way.
- The group coordinator's shard tests with the mock partition writer and mock timer, and the runtime's timer tests for override and `NOT_COORDINATOR` handling.
- The controller's replication, configuration, cluster, and feature control manager tests, for the guards and the ELR rules.
- The Postgres test helpers that run migrations and clean up containers, for any test that needs a database.
- The control plane outage cluster test, for the availability gate and runtime reconfiguration.
- The consolidated diskless topics cluster test, for prune floors and cross-tier offsets.
- The untiered-to-consolidated migration test, for classic-to-diskless switch behavior.
- The diskless topic delete test, for topic deletion and file cleanup.

Optional, not required for done: a ducktape system test of the full deployment, following the existing consolidation pipeline test.

## Out of Scope

- Changing the partition count of `__diskless_metadata` after creation.
- Orphan-object scanning. A report-only scanner is future work.
- An off-broker copy of coordinator state, a restore procedure, and versioned buckets or soft-delete as cover for losing every replica of a coordinator partition. Losing every replica is an accepted risk.
- A replay checker and an inspect-and-repair tool for a deterministic apply bug that corrupts every replica alike. That bug is an accepted risk.
- The benchmark question and the capacity risk.
- Produce gateways and preferred produce brokers.
- Moving a user partition between coordinators.
- Transaction state.
- The KIP's public APIs, keys 95 to 101.
- Follower reads.
- Migrating existing Postgres-backed clusters.
- A Raft quorum for metadata.
- Tiered-storage offload.

## Further Notes

- The KIP text doesn't define snapshot distribution. This spec defines it, through the private fetch-snapshot request modeled on KIP-630. That request, the lease and fence rules in Object tracking and deletion, and the barrier in Reads are the parts most worth a reviewer's attention.
- Open items for the implementer, none of which changes the design: the private request names and API key numbers, the error codes for a busy snapshot source, a stale epoch, and a stale sequence, the names of the two features and the record set of each level, the SQLite driver choice and license, the commit deadline default, which also sets the runtime's write timeout, the idle lease timeout default, the byte encoding of the broker ID in the tracking-partition hash, how a leader that can't rebuild its state gives up leadership of its coordinator partition, the cap on how far the overlay may run ahead of the high watermark, and the append linger default.
- A late commit part can still commit after the producer got `KAFKA_STORAGE_ERROR`, until the broker's next commit to that coordinator partition applies, a fence raises the floor, its epoch retires, or its lease ends. A non-idempotent producer that retries through another broker can see its data twice, with the late copy after the retried one. A classic `acks=all` timeout and the Postgres backend have the same window.
- No clock decides whether a deletion is safe. Retention and `LogAppendTime` under a skewed leader clock, and grace-period skew beyond half the grace period, are accepted risks covered by an NTP procedure. [design.md](design.md#accepted-risks) lists every accepted risk with its signal or procedure.
- An object leaks only when storage completes an upload after its key was deleted. That leak is accepted over a scanner that deletes objects no coordinator partition knows, which fails toward data loss.
- The full design rationale, the failure-mode tables, the rejected alternatives, and the terms are in [design.md](design.md).
- Growing the coordinator, merging WAL objects into extents, tiered metadata in the coordinator, and partitionless key ranges are future work. [design.md](design.md#future-work) describes each one and the decisions in this spec it depends on.
