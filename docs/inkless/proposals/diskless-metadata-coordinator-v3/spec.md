# Diskless metadata coordinator

This spec replaces [version 2's spec](../diskless-metadata-coordinator-v2/spec.md).

Status: ready-for-agent

Design record: [design.md](design.md).

## Problem Statement

Diskless topics depend on a control plane that keeps their batch, file, and log state. Today the only durable implementation is the Postgres control plane, which every broker reaches through one shared database. A cluster that wants diskless topics without running that database has no option. The in-memory implementation is for tests, and it loses all state on restart.

KIP-1164 describes a Diskless coordinator that stores this state in an internal Kafka topic, so the cluster runs without an external database. Its full design has multiple coordinator partitions, file ownership handoff, orphan scanning, and new public protocol keys. This effort keeps multiple coordinator partitions and replaces file ownership with per-broker sequence leases. The rest is more than this effort needs, and it doesn't fit the control plane interface the broker code already uses.

Operators need a diskless control plane that keeps its state in Kafka, loses no data that a classic topic keeps through the same failure, performs no worse than the Postgres backend, spreads its load across brokers, and fits the existing control plane interface. The only shared broker code it changes is the commit path, which must order commits per coordinator partition so that one unavailable coordinator partition doesn't stall every commit, and must complete each produce request once the parts that hold its batches finish.

A broker's WAL file holds batches for almost every user partition it receives, so without help each file sends a commit part to almost every coordinator partition, and every coordinator leader hears from every broker. The coordinator needs a way to cut that fan-out without a public protocol change and without making correctness depend on where a client sends its produce.

## Solution

Add a third control plane implementation that stores its state in an internal topic, `__diskless_metadata`, with a fixed number of partitions. Each diskless user partition maps to one coordinator partition by a hash of its log key, which is its topic ID and partition index, through a fixed set of slots. The leader of a coordinator partition is the only broker that assigns offsets and writes state changes for the user partitions that map to it. Every replica of a coordinator partition keeps a local SQLite copy of that partition's state, rebuilt from the log and from snapshots.

A broker splits each call by coordinator partition and sends each part to that partition's leader, locally or over a private RPC. A commit therefore goes to every coordinator partition its object has batches for. Each broker keeps one ordered commit queue per coordinator partition, so a part waits only for the previous file's part on the same coordinator partition. Each part succeeds or fails on its own, as a produce already can per partition, so no two-phase commit is needed. The leader appends one outcome record per committed part and replies only after the record is replicated. A produce request completes once the parts that hold its batches finish.

The coordinator treats each uploading broker as an idempotent producer. Every file gets the broker's epoch and a sequence number at file close, and the object key is built from the cluster ID, broker ID, epoch, and sequence. Each broker leases ranges of sequences ahead of time from its tracking partition, so no round trip precedes an upload. A live broker keeps its lease with keepalives, switches at once to a range at another partition when it falls back, and ends every range it abandons with a lease release. Every coordinator partition keeps one row per uploading broker, which deduplicates retries and makes every lower sequence impossible once a higher one commits.

Every file also sends a part to its tracking partition, which records the object's participants and carries the broker's finish report: how far the broker has finished its files, and which participants didn't answer. The tracking partition fences only the participants the report names, the sequences it never recorded once a recorded report passes them, and a lease's unused sequences when the lease ends. Participants report in batches when they no longer hold any batch of an object. The tracking partition deletes the object once every participant is released. Its deletion queue is the only path that deletes an object whose commit may have reached a leader, and no clock decides whether a deletion is safe.

Gateway routing sends each client's produce and fetch for a user partition to a gateway of that partition's coordinator partition in the client's rack. Every broker computes the gateways from its metadata image, and the metadata transformer rewrites the `LeaderId` it already rewrites. A broker picks its tracking partition from the coordinator partitions it's a gateway for, so a routed file usually sends one part. Routing is a hint: a produce that reaches any broker still commits.

The coordinator topic is protected: it joins Kafka's internal topics, and the controller and broker reject every request that would delete, create, extend, or weaken it. It requires `flush.messages=1`, a replication factor of at least 3, and replicas spread across as many racks as the replication factor allows. Storage identity, not shutdown cleanliness, decides which replica may lead a coordinator partition. A replica stops applying at any record it can't read, and feature levels and a downgrade refusal keep that state out of reach.

Every replica keeps its own snapshots on its disk. A replica that needs records the log no longer holds fetches a covering snapshot over a private RPC, from an in-sync follower when one has it. Only the leader prunes the log. It advances Kafka's log start offset only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for.

Operators format a new cluster with the coordinator feature already finalized. `inkless.control.plane.class` must name the new class, and a broker refuses to start if it names another backend. Operators don't format clusters that already run on Postgres with the feature, and those clusters stay on Postgres.

## User Stories

1. As an operator, I want to run diskless topics without an external database, so that my cluster has fewer components to run and monitor.
2. As an operator, I want the coordinator state stored in an internal Kafka topic, so that I can back it up and replicate it with the tools I already use.
3. As an operator, I want the coordinator topic to have a replication factor of at least 3, 3 by default, and a minimum ISR from `diskless.metadata.topic.min.insync.replicas` that defaults to `min(2, replication factor)` and is fixed at creation, so that one broker failure doesn't stop diskless writes.
4. As an operator, I want to set the replication factor of the coordinator topic with `diskless.metadata.topic.replication.factor`, and a development cluster to run with one replica only through an internal override, so that a production cluster can't drop below 3 by mistake.
5. As an operator, I want the controller to create the coordinator topic once enough unfenced brokers register for its replication factor, so that I don't create it by hand.
6. As an operator, I want each coordinator partition's replicas spread across as many racks as the replication factor allows, enforced by the controller, so that losing one rack doesn't take out every copy.
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
31. As a broker, I want the coordinator delegate to send each commit part to the current leader of its coordinator partition and retry a `NOT_COORDINATOR` or `COORDINATOR_LOAD_IN_PROGRESS` error with exponential backoff within a commit deadline, so that a leader change looks like a short delay, not a failure.
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
56. As a support engineer, I want the coordinator to report its state as metrics, so that I can see lag, snapshot age, log size, WAL pages and apply lag, the deletion queue, tracking entries, pending releases and fences, lease ends, barrier outcomes, stopped applies, commit parts by kind, and unknown-outcome commits.
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
78. As an operator, I want a broker to renew its lease at the next coordinator partition in its ordered fallback list when its tracking partition is unavailable, so that one unavailable coordinator partition doesn't fail whole objects.
79. As a developer, I want the user-partition mapping pinned by a test with fixed expected values, so that brokers of different versions always agree on it.
80. As a broker, I want a leader to reject a part for a user partition that doesn't map to it, so that a broker that maps differently can't give a user partition a second writer.
81. As an operator, I want a broker to serve at most a set number of snapshot transfers across all its coordinator partitions, so that rebuilding a replaced broker doesn't starve the brokers that serve the snapshots.
82. As an operator, I want the coordinator partition count restricted to a power of two, so that the coordinator can later grow by splitting partitions without remapping every user partition.
83. As a Kafka client, I want a stuck coordinator partition to delay only the produce requests that write to its user partitions, and those by at most one commit deadline, so that one failure doesn't stall every diskless produce.
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
95. As an operator, I want the lease of a broker cut off from its tracking partition to end after `diskless.metadata.lease.idle.timeout.ms`, so that its unused keys are deleted without a restart.
96. As a producer on a quiet broker, I want my first produce after an idle period to succeed, so that low traffic doesn't cause failed produces.
97. As an operator, I want clients routed to a gateway of their user partition's coordinator partition in their own rack, so that each file sends fewer commit parts and produce stays in the client's AZ.
98. As an operator, I want every broker to compute the same gateways from the metadata image, so that routing needs no new RPC and a crashed gateway's duty moves once the controller fences it.
99. As an operator, I want to exclude a broker from gateway duty with a dynamic broker config, so that I can drain a degraded broker or one about to be decommissioned.
100. As an operator, I want one cluster-wide dynamic switch that turns gateway routing off, so that I can return to per-partition routing without a restart.
101. As a Kafka client, I want a produce at a broker that isn't my partition's gateway to fail once with a retriable error and then succeed, so that stale metadata costs one retry, not a failed produce.
102. As an operator, I want a broker's tracking partition to be one of its gateway coordinator partitions, so that a routed file sends no extra commit part for tracking.
103. As a Kafka client, I want partitions whose fetches need a broker-local log to keep replica routing, so that consolidating, tiered, and switched partitions keep serving fetches.
104. As an operator, I want each coordinator leader's in-flight work bounded per commit queue and per coordinator partition, so that a slow leader degrades only its own user partitions.
105. As a Kafka client, I want reads from a slow coordinator leader to fail fast for its user partitions only, so that fetches on other partitions keep their latency.

## Implementation Decisions

Topology and placement

- The coordinator state lives in one internal topic, `__diskless_metadata`, with `diskless.metadata.topic.num.partitions` partitions, 8 by default. The default is provisional until the partition-count benchmark.
- The replication factor is set by `diskless.metadata.topic.replication.factor`, 3 by default, and is fixed at creation. Controller startup validation rejects a value below 3. An internal controller config, defined with `defineInternal` and left out of the docs, lowers the minimum to 1 for tests and local development, and the controller logs a warning when it creates the topic below 3. A reassignment that changes the replica count gets `INVALID_REPLICATION_FACTOR`.
- The topic-level `min.insync.replicas` comes from the controller config `diskless.metadata.topic.min.insync.replicas`, which defaults to `min(2, replication factor)`. Startup validation requires it to be between that default and the replication factor. It's fixed at creation.
- The active controller creates the topic, with `unclean.leader.election.enable=false`, `flush.messages=1`, `retention.ms=-1`, `retention.bytes=-1`, and `cleanup.policy=delete`, once it sees at least as many registered, unfenced brokers as the replication factor. It checks on each registration and unfencing, and once after it becomes active. A coordinator call before then gets a retriable error.
- The controller places each coordinator partition's replicas with maximal spread, across `min(replication factor, number of racks)` racks, which `StripedReplicaPlacer` already produces. A reassignment that narrows the spread below the current set and below maximal spread gets `INVALID_REPLICA_ASSIGNMENT`. The gauge `CoordinatorPartitionsBelowRackSpread` counts partitions that a rack change left below maximal spread.
- The leader of each coordinator partition is the single writer for the user partitions that map to it. Replicas fetch the partition with normal replica fetching. Replicas don't vote and no Raft protocol is used.
- The coordinator appends with `acks=-1`, so the leader rejects an append with `NOT_ENOUGH_REPLICAS` while the ISR is below `min.insync.replicas`, before the record reaches its log.
- The topic requires `flush.messages=1`, with no weaker mode. An unflushed tail across the coordinator partitions breaks invariants that span a tracking partition, its participants, and object storage, which doesn't roll back: a release that reaches disk at the tracking partition but not at the participant deletes an object that live batches reference, a lost fence or lease record lets a part commit into a queued key or leaves an object untracked, and a lost commit reuses offsets across 1/N of the cluster. The leader's fsync per append matches a Postgres commit on the primary. The follower fsync is a second sequential flush, the price of Kafka parity.
- The append linger is adaptive by default, as `group.coordinator.append.linger.ms=-1` is. The static broker config `diskless.metadata.append.linger.ms`, default `-1`, sets a fixed linger instead.
- After the broker creates a `__diskless_metadata` partition directory, it fsyncs the log directory. A failed fsync takes the log directory offline.

Coordinator partitions

- A user partition maps to slot `Utils.toPositive(Utils.murmur2(logKey)) % S`, where `S` is the protocol constant 4096, and the slot maps to coordinator partition `slot % N`.
- The log key of a Kafka partition is the 16 bytes of the topic ID, most significant bits first, followed by the partition index as 4 big-endian bytes. It is the only log key encoding this effort defines.
- `diskless.metadata.topic.num.partitions` accepts only a power of two from 1 to 4096. With such an `N`, the slot mapping gives the same result as `Utils.toPositive(Utils.murmur2(logKey)) % N`, and brokers don't store a slot table.
- Every broker reads `N` from the metadata image. A call made before the topic exists gets a retriable error. `N` never changes after creation.
- The mapping functions, the log key encoding, and `S` are part of the protocol. A test with fixed expected values pins them. The tracking partition is a local choice of each broker, so it isn't part of the protocol.
- Each leader recomputes the mapping for every user partition in a request it receives, and rejects a mismatch with `INVALID_REQUEST`.
- Each coordinator partition has its own log, SQLite database on each replica, snapshots, overlay, and leader. With `N=1`, the design reduces to a single coordinator.

Topic protection

- `__diskless_metadata` joins `Topic.INTERNAL_TOPICS`. It inherits the produce and `DeleteRecords` rejections, the internal flag in metadata responses, exclusion from topic listings and regex subscriptions by default, no tiered storage, and system-topic status, which keeps it classic.
- Explicit guards keyed on the topic name, with no bypass, reject: `DeleteTopics` with `INVALID_TOPIC_EXCEPTION`, after authorization; client produce, including the `__admin_client` client ID, unless the append origin is the coordinator, with `INVALID_TOPIC_EXCEPTION`; client `CreateTopics` and broker auto-create with `INVALID_REQUEST`; `CreatePartitions` with `INVALID_PARTITIONS`; `UNCLEAN` `ElectLeaders` with `INVALID_REQUEST`; a reassignment that changes the replica count with `INVALID_REPLICATION_FACTOR`; and a reassignment that narrows the rack spread with `INVALID_REPLICA_ASSIGNMENT`. Client `DeleteRecords` gets `INVALID_TOPIC_EXCEPTION` through the internal-topic status.
- The controller's config validation checks the topic's config after each alter and at creation, and rejects with `INVALID_CONFIG` any result where `flush.messages` isn't `1`, `unclean.leader.election.enable` isn't `false`, `retention.ms` or `retention.bytes` isn't `-1`, `cleanup.policy` isn't `delete`, or `min.insync.replicas` differs from its creation value. Setting a key to its required value passes. Other topic configs stay alterable.
- The three upstream internal topics keep upstream behavior.

Transport and protocol

- Each broker holds a delegate that implements the existing control plane interface. For each call, the delegate splits the request by coordinator partition, sends the parts in parallel, and merges the results back into request order. A part runs locally on its leader. On any other broker, the part goes to that leader over a private RPC.
- Every private RPC that the delegate retries waits between attempts with Kafka's `ExponentialBackoff`: 100 ms initial, multiplier 2, 1000 ms maximum, and 0.2 jitter, as constants. It covers commit parts, lease requests, lease releases, release reports, and fences. The delegate doesn't retry read parts.
- The new request types use private API keys in the 504 range and above. No public Kafka protocol changes, and the backend doesn't use the KIP's proposed keys 95 to 101.
- A broker that receives a request for a coordinator partition it doesn't lead returns `NOT_COORDINATOR`.
- Each private request returns the same error codes as the control plane method it wraps. The stale-epoch and stale-sequence errors are non-retriable. An unleased sequence, a negative sequence, or a retry with a different batch count gets `INVALID_REQUEST`. A call that names an unreconciled user partition gets `COORDINATOR_LOAD_IN_PROGRESS`, and a request past the overlay cap gets `COORDINATOR_NOT_AVAILABLE`, both retriable.
- The private request set is: lease sequences, release lease, commit file, find batches with a no-wait field, list offsets, delete records, initialize diskless log, repair diskless log, prune diskless logs, advance cross-tier log start offset, report releases, fence sequences, fetch snapshot, and report snapshot offset.
- A commit reply carries a `Recorded` flag, set when the answer comes from a recorded file-committed record and unset for a rejection before the producer checks.
- Release reports and fences carry `(brokerId, brokerEpoch)` groups of sequence ranges, capped at 512 KiB encoded. The sender splits a larger set across consecutive requests.
- The private requests carry no tagged fields and no leader epoch. A leader epoch added later is a new request version.
- A produce reply can't carry the offsets a commit assigns, so commit has its own reply.

Log records

- The leader appends one outcome record per committed call or part. Every replica applies the records in log order, and only up to the high watermark.
- Each record carries the state change and every assigned value, so replicas reach the same state without recomputing it.
- The record types are: sequences leased, file committed, finish reported, sequences fenced, participant state recorded, release accepted, lease ended, broker epoch retired, files deleted, barrier, partition created, topic deleted, records deleted, diskless log initialized, diskless log repaired, retention applied, log pruned, cross-tier start advanced, and deleted log purged. Implementers may merge or split them.
- The file-committed record carries the object key, the uploader's broker ID, broker epoch, and sequence, the tracking partition, the participant set, the committed batches with their offsets, the outcome of each other batch, and, at the tracking partition, the finish report.
- The partition-created record carries `metadataOffset`, the image's last contained offset when the leader appended it. The broker-epoch-retired record carries `retiredThrough`.
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
- When the high watermark passes an entry, the leader applies it to SQLite, removes it from the overlay, and replies.
- If an append fails, the leader resigns, or the log truncates an entry, the leader removes that entry and every entry after it and fails their parked replies with a retriable error.
- The overlay holds at most `4 × B` entries, where `B` is the number of registered brokers in the leader's image. Past the cap, the leader rejects a new request with `COORDINATOR_NOT_AVAILABLE` and records nothing.
- A new leader starts with an empty overlay.

Write path

- The leader runs the overlay loop on `CoordinatorRuntime`, with one shard per coordinator partition, as the group coordinator does for `__consumer_offsets`. The runtime supplies the single writer thread, the append linger, the revert after a failed append, the parked replies, the timer for idle leases, and the `NOT_COORDINATOR` failure on resignation.
- The shard stores the overlay in timeline structures backed by a `SnapshotRegistry`. Committed state stays in SQLite.
- The runtime writes through a `PartitionWriter` that Inkless owns and that appends with `acks=-1`. The upstream `CoordinatorPartitionWriter` appends with `acks=1`, and the group and share coordinators keep using it unchanged.
- The runtime's write timeout equals the commit deadline. Upstream `CoordinatorRuntime` doesn't change.
- A custom `CoordinatorLoader` replays from the applied offset stored in SQLite.
- One follower-applier thread per broker applies fetched records to SQLite up to the high watermark, for every coordinator database the broker follows. The replica fetcher only appends bytes. The runtime runs on the leader only.

Sequences, keys, and leases

- At file close, `FileCommitter` gives each non-empty file the current broker epoch and the next sequence, from a sequence source that the control plane provides. The pair orders by epoch first. The sequence starts at 0 in each epoch and allows gaps, and the broker doesn't persist it. A file that closes with no leased sequence fails at once with `KAFKA_STORAGE_ERROR`, and the broker doesn't upload it.
- The default sequence source is a counter that restarts at 0 on a new epoch and never returns empty, so the Postgres and in-memory backends behave as today.
- The object name is `<clusterId>.<brokerId>.<brokerEpoch>.<sequence>` under the existing key prefix handling. `FileCommitter` and the backend build it with one shared function, and the backend never parses it. The upload job no longer generates a random UUID. Every upload attempt reuses the key, and every storage backend overwrites unconditionally.
- A broker leases a range `[a, b]` of at most `K = 1024` sequences, a protocol constant, at a tracking partition. It picks `a` above every range it requested in that epoch at any partition. The tracking partition stores the range and the uploader's key prefix in its lease table and replies after the record applies. A request for a held range is a retry or a keepalive and appends nothing, unless it carries a newer finish report.
- The broker asks for its first lease when `FileCommitter` starts and when its epoch changes. A renewal walks an ordered list from the tracking selection: the preferred tracking partition, then the other members of the broker's gateway set by score, then every other coordinator partition by the broker's score. One renewal runs at a time, started by fewer than `K/2` leased sequences left, a keepalive with no answer, a tracking part that finished without a leader answer, or a change of the list's first entry followed by a file with no batch for the current tracking partition.
- A renewal confirmed at the partition in use keeps the order of sequences. A range confirmed at another partition takes effect at once, and the broker abandons its ranges at the old partition, including one it requested there without a confirmation.
- After half the idle timeout from the lease reply with no answer from the tracking partition it uses, the broker sends a keepalive there: a lease request for a range it already holds. A live broker's lease therefore never ends idle.
- Once the broker abandons ranges at a partition, it sends a lease release there naming `b`, the highest range end it gives up, with the final finish report, once every file of the epoch at or below `b` is finished and no planned sequence is at or below `b`. The tracking partition appends a lease-ended record that ends the ranges at or below `b`. A repeated or late release is a no-op.
- Every commit part names the tracking partition that issued its sequence's range.

Commit path

- `FileCommitter` commits on a single thread today, so one unavailable or slow coordinator partition would stall every later file on every broker. This effort changes that shared caller, not only the backend.
- The control plane maps each user partition to a commit queue. The new backend maps it to its coordinator partition. The Postgres and in-memory backends map every user partition to one queue, which keeps today's order.
- The control plane gets a per-part commit call for the batches of one file that map to one queue, with the broker epoch, the sequence, and the object key. The Postgres and in-memory backends call `commitFile` for it.
- `FileCommitter` keeps one ordered queue per commit queue, in file rotation order. A file's part on a queue runs once the file is uploaded and the previous file's part on that queue has finished. It doesn't wait for parts on other queues.
- Every file also sends a part to its tracking partition, carrying the participant set and the finish report. The part is empty when the tracking partition holds none of the file's batches.
- Every part of a file shares the file's commit deadline, measured from the moment the file closes, and the deadline doesn't exceed the produce request timeout. The backend retries each part with the backoff until the deadline, so the queue starts the next part only after the earlier one has finished.
- A part is finished when it committed, the leader rejected it with a non-retriable error, or the deadline passed. A file is finished when every part is finished, the tracking part included.
- A produce request completes once every part that holds its batches has finished, with per-batch results merged in request order. It waits for the tracking part only when the tracking partition holds one of its batches. A request with only rejected batches completes at file close. Each part fills the batch coordinate cache when it commits. The Postgres and in-memory backends have one queue, so every request completes when the file does.

Commit and retry

- Every coordinator partition keeps one row per uploading broker: its epoch, the last sequence that committed there with that commit's complete per-batch result, a fenced-through pair, and a retired-through epoch. The floor is the highest of the last commit, the fenced-through pair, and the start of the epoch after the retired-through epoch. Rows are never deleted.
- A part runs on its leader in this order: check the mapping; reject an unreconciled partition with `COORDINATOR_LOAD_IN_PROGRESS`, a deleted topic's partition with `UNKNOWN_TOPIC_OR_PARTITION`, and a full overlay with `COORDINATOR_NOT_AVAILABLE`; reject a stale epoch when the leader's image holds a newer epoch for the uploader; compare the pair with the broker's newest pending overlay entry or its row; run the producer epoch and sequence checks for a new commit; assign offsets and add one file-committed record; and, when the high watermark passes the record, apply it to SQLite, remove the overlay entry, and reply with the `Recorded` flag set.
- The acceptance rules: an epoch at or below the retired-through epoch, or a lower epoch, is a stale epoch. The pair equal to the last commit is a retry, which waits for a pending entry or replies at once with the stored result, also after retention removed the batches. A pair above the floor, or no row, is a new commit. Anything else is a stale sequence, with the floor in the reply. `logStartOffset` in a reply always comes from current state.
- Only the tracking partition checks the lease: the part sent to it must carry a sequence inside one of its ranges for that epoch, or it gets `INVALID_REQUEST`. Participants rely on their floor.
- The leader doesn't reject a part for its epoch when the part is at the image's epoch, at a newer one, or for a broker with no registration in the image, because the image can lag. It doesn't reject a fenced uploader either.
- A new commit at a newer epoch replaces the row. A commit whose every batch fails is an ordinary new commit with zero committed batches, which advances the row and releases the object at once.
- A part that is rejected before the producer checks records nothing and leaves the row unchanged.
- A broker epoch is the metadata offset of its registration. Each leader appends a broker-epoch-retired record on each image change and once it starts serving after an election. `retiredThrough` is `E - 1` when the image shows a newer epoch `E`, and the image's last contained offset when the image shows no registration. The leader appends it only when `retiredThrough` is at or above the broker's newest epoch in its row, lease table, and overlay, so an image that lags a new registration appends nothing. Apply raises the floor, and at a tracking partition ends the leases at or below `retiredThrough`. A controller test pins that a new registration of a broker with no prior registration writes its `RegisterBrokerRecord` as its first record.
- The delegate retries each part on its own against the current leader of its coordinator partition after `NOT_COORDINATOR` or a retriable error, with the same pair and object key. It stops at the file's commit deadline.
- At the deadline, batches on confirmed parts get their per-batch results, and batches on unconfirmed parts get `KAFKA_STORAGE_ERROR`. Their outcome is unknown until the broker's next commit to that coordinator partition applies, a fence raises the floor there, its epoch retires, or its lease ends. The unknown outcome is an exception in an urgent finish report, so its range fence closes the window about one commit interval after the deadline. If any part committed, the file returns per-batch results. If none did, the file fails as a whole.
- Apply rules for file records are strict. A file-committed record whose pair isn't above the floor, a lease record that overlaps a held range, or a record that changes a user partition with no partition-created row is an invariant violation, and the replica stops applying that coordinator partition. A fence at or below the floor, a repeated epoch retirement, and a files-deleted record for a key not in the queue apply as no-ops.

Object tracking and deletion

- Each key has exactly one tracking partition: the one that issued its range. It holds the broker's leases, records each object's participant set from the tracking part, and keeps the deletion queue.
- A broker picks its tracking partition from its gateway set through an internal tracking selection interface. The sticky rule keeps the current tracking partition while it stays in the set. When it leaves, and at the start of an epoch, the broker takes the member with the highest score `toPositive(murmur2(bytes(cp) ++ bytes(brokerId)))`. A broker with an empty gateway set keeps its current one, or at the start of an epoch takes the highest score over all `N`. Tests inject a stub, and no config selects the strategy.
- A finish report holds `finishedThrough`, the inclusive bound at or below which every file of the broker epoch is finished, and at most one exception range per participant. An exception is any part outcome without the `Recorded` flag. A report with an exception, or one covering a file whose tracking part the tracking partition didn't record, is urgent: if no tracking part carries it within one `produce.commit.interval.ms`, the broker sends an early keepalive. The broker keeps a report until a recorded record includes it.
- A participant releases an object when an apply removes its last batch of it. Five records release objects: records deleted, retention applied, log pruned, deleted log purged, and a file-committed record whose every batch failed. Apply runs a `NOT EXISTS` check over an index on `batches(file_id)` for every file it touched, in the same transaction.
- The participant leader keeps at most one release report in flight per tracking partition, and releases that pile up go into the next report. The tracking partition applies a whole report in one record and answers each group separately, rejecting only a group outside its leased ranges. The participant then appends a release-accepted record, and a new participant leader resends only the releases without one.
- Each participant's tracking entry moves through pending, fencing, held, and released, and released is terminal. A release report for a sequence with no entry is classified against the higher of the range's highest recorded sequence and the tracking partition's fenced-through pair: above it, the entry is stored as released; for a missed sequence with unanswered fences, it counts as that participant's answer; at or below it, it's a no-op.
- A fence names a broker, an epoch, and a range of sequences. A participant raises its fenced-through pair to the range end if that's above its floor, and answers with the sequences in the range whose batches it still holds. A part pending before the fence commits first and is answered referenced. A part that arrives after it gets stale-sequence.
- The tracking partition fences in three cases, each after the broker has finished the files it covers: an exception range in a recorded finish report, to that participant; a missed sequence, one in its own ranges that it has no record of once a recorded `finishedThrough` reaches it, at all `N` partitions; and a lease end, from the sequence after the recorded `finishedThrough` to the highest range end, at every partition. When one record does both, the missed-sequence rule applies first. It skips a fence whose entries are all held or released.
- Each tracking partition keeps at most one fence request in flight per participant. Ranges of one broker epoch merge into their hull, and the participant appends one sequences-fenced record per request.
- A lease ends on a broker-epoch-retired record, a lease request at a newer epoch, or a lease-ended record from a lease release or the idle timer. The tracking partition appends the idle lease-ended record when a broker has sent no part and no lease request that names it for `diskless.metadata.lease.idle.timeout.ms`, 15 minutes by default and provisional. The timer is a `CoordinatorTimer` per broker on the leader's monotonic clock, rescheduled by every part, lease request, and keepalive that names the partition, including rejected ones. A new leader starts every timer at its election and resends every unanswered fence.
- Only tracking partitions keep a deletion queue. A committed object enters it in the apply that moves its last entry to released. Missed and leftover keys enter it from fence answers that no partition holds, with key paths that apply builds from the uploader's prefix in the lease table and the replica's own cluster ID.
- `FileCleaner` keeps running on every broker. `getFilesToDelete` returns the union of the queues of the tracking partitions this broker leads, and an empty list elsewhere. Each queue row carries the mark time from its marking record and waits `file.cleaner.retention.period.ms`, 1 minute by default. `deleteFiles` appends one files-deleted record per tracking partition. Deleting a missing key counts as success, and deleting a key twice is harmless.
- The backend declares a capability that turns off the broker's direct delete. `FileCommitJob` checks it before its `ControlPlaneUnavailableException` branch, and the backend's `isSafeToDeleteFile` returns false without a request. The Postgres and in-memory backends don't declare it.
- An object leaks only when storage completes an upload after its key was deleted, or when a held entry's participant never reports. Neither deletes data. No scanner deletes objects. A report-only scanner is future work.
- No clock decides whether a key is deleted, only when. The idle lease timeout and the grace period are the only time-based decisions that touch deletion.

Local state

- Each replica of a coordinator partition keeps that partition's state in its own local SQLite database. The database is a cache. The log is the source of truth.
- The leader, the follower applier, and the loader apply every record up to the new high watermark in one SQLite transaction, with a savepoint per record. A group closes before the record that would push it past 4 MiB of log bytes. A record that stops the apply rolls back to its savepoint, and the group commits up to the record before it.
- Every group stores the applied offset and the leader epoch of its last record in the same transaction as the state change.
- On open, a replica checks the stored offset and epoch against the log. It keeps SQLite only if the log still holds that offset and the record there carries that epoch.
- Invariant: the applied offset is always at or below the high watermark. SQLite holds only records at or below the high watermark, so a replica never reverses an applied row.
- SQLite runs in WAL journal mode with `synchronous=NORMAL`. A power loss can roll back the most recent groups, together with their applied offset, and the replica replays them from the log.
- The apply connection sets `wal_autocheckpoint=0`. One checkpoint thread per broker runs `PASSIVE` checkpoints for every hosted database once its WAL reaches 1000 pages, one at a time. No apply thread runs a checkpoint. The bundled SQLite must be 3.51.3 or later, or 3.44.6 or 3.50.7, and the broker refuses to start the coordinator backend below that.
- The schema holds logs, files, batches, producer state, broker rows, and the applied offset on every partition, and lease tables, recorded finish reports, tracking entries, and the deletion queue on tracking partitions. Participants keep their pending releases. Each user partition's row keeps its `metadataOffset`. The schema must support indexed lookup by topic partition and offset, by object key, by `batches(file_id)`, and by uploader broker, epoch, and sequence.
- A coordinate row must be able to describe an extent: an offset range mapped to an object and a byte range, with the producer fields optional. This effort writes only WAL batch rows, and each one carries its producer fields.
- Each database stores its schema version. A binary migrates any older schema it supports forward, and treats a newer schema as a missing database, which it rebuilds.
- The SQLite driver is a JDBC driver with a bundled native library. Its license and packaging must pass the repo's dependency checks before the choice is final.

Snapshots

- Each coordinator partition runs the snapshot rules on its own, with its own log start, snapshots, and reports.
- The log start is Kafka's own log start offset of the coordinator partition. Only the leader advances it, with the same call the delete-records path uses, and followers adopt it from fetch responses.
- The leader advances the log start only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for. The new log start is the lower of the leader's own older snapshot and the highest offset that at least `min.insync.replicas - 1` other in-sync replicas have reported a snapshot at or beyond. A single-replica topic advances from the leader's own older snapshot alone.
- The log start stays at offset 0 until the first advancement. A replica that loses its database before then replays from offset 0 and needs no snapshot.
- Every replica takes its own snapshot when either `diskless.metadata.snapshot.interval.ms` elapses, 1 hour by default, or `diskless.metadata.snapshot.max.bytes.between.snapshots` of log has been applied since its last snapshot, 1 GiB by default. Both defaults are provisional, and the snapshot-transfer benchmark can drop the byte trigger to 256 MiB.
- A snapshot uses the SQLite backup API. Its offset and epoch come from the applied-offset row inside the finished copy. The replica fsyncs the temporary file, renames it to a final name built from the offset, the epoch, the schema version, and a checksum, and fsyncs the directory, before it deletes the snapshot that falls out of the retained pair.
- Snapshots are written to the broker's local disk, never to object storage. Each replica keeps the newest snapshot and one older one, as a constant.
- A replica reports its newest snapshot offset to the leader on a private request after a snapshot, on a new leader, and when it rejoins the in-sync replica set, from the broker's scheduler, with the backoff. A report counts only while its replica is in the leader's committed in-sync replica set.
- A replica fetches a peer's snapshot only when it needs records below the log start. It fetches from an in-sync follower that holds a covering snapshot, and from the leader only when no follower does.
- Every fetch-snapshot request names the snapshot ID, a byte position, and the highest schema version the receiver reads. A source with no covering snapshot at or below that version answers `SNAPSHOT_NOT_FOUND`, and the receiver tries the next replica. Rotation keeps a file while its transfer is active, until `diskless.metadata.snapshot.transfer.idle.timeout.ms`, 60 seconds by default, passes without a chunk request.
- The request and response follow KRaft's fetch-snapshot schemas, with chunks of at most 8 MiB sent from the file with zero copy. A broker serves at most `diskless.metadata.snapshot.max.concurrent.transfers` active transfers, 1 by default and provisional, and answers a request beyond the limit with a retriable busy error.
- A replica serves its finished snapshot files whatever the state of its live database. A replica whose database fails the open check, runs ahead of its log, or has a newer schema rebuilds before it serves, a newly elected leader included.
- If no replica holds a snapshot that covers the log start, a broker that needs one can't rebuild, and that coordinator partition stays unavailable for diskless writes rather than serving partial state.

Reads

- Only the leader answers reads. The delegate splits find-batches by coordinator partition, sends the full `fetchMaxBytes` budget with each part, merges the results in request order, and trims them to that budget.
- Brokers absorb most fetch-side lookups in the existing batch coordinate cache. A cache miss goes to the leader. Follower reads aren't part of this effort.
- The leader answers `findBatches` and `listOffsets` from a read pool for the whole broker, with `diskless.metadata.read.threads` threads, 4 by default and provisional, each with a read-only SQLite connection per led database. Only a find-batches above the high watermark enters the shard.
- `ControlPlane.findBatches` is asynchronous and returns a `CompletableFuture`. The Postgres and in-memory backends wrap their calls in an executor. A read part holds no shared thread, a part parked at a barrier included.
- The delegate doesn't retry read parts. Each part has a 1000 ms deadline, and a part that fails or passes it gives its user partitions `KAFKA_STORAGE_ERROR`. A calling broker allows at most 8 read parts in flight per coordinator partition, and `listOffsets` shares the slots.
- A read that names an unreconciled user partition gets `OFFSET_NOT_AVAILABLE`.
- A deposed leader serves committed but older state until it learns of the new leader, as a classic deposed leader does. No leader lease and no read barrier run on the normal read path. Every stale read is lower than or equal to the current answer, or unknown, except find-batches above the high watermark. A new read RPC that breaks this rule needs the barrier too.
- When find-batches gets an offset above the high watermark, the leader parks the request and appends a barrier record. Requests that arrive while a barrier is open share it. Once the barrier commits, the leader checks the offset again and answers `OFFSET_OUT_OF_RANGE` or returns batches.
- Each parked request gets its own 500 ms deadline from its arrival, on the runtime's timer, and gets `OFFSET_NOT_AVAILABLE` when it fires. The 500 ms is a constant. A barrier from a deposed leader never commits.
- The delayed-fetch probe never runs the barrier. The control plane gets a default probe method that calls find-batches, and the new backend sets a no-wait field that makes the leader answer above the high watermark at once with `OFFSET_NOT_AVAILABLE`.

Gateway routing

- Every broker computes the gateways from its metadata image alone, per rack, behind an internal interface in the style of `ReplicaPlacer` that the transformer and the produce path reach through `SharedState`. Candidates are the registered, unfenced brokers, minus those with `diskless.gateway.candidate.enable=false`. A missing rack forms its own group.
- A coordinator partition gets `g = max(1, ⌊b_r / N⌋)` gateways in a rack of `b_r` candidates. The rule walks pairs of a coordinator partition and a candidate by the score `toPositive(murmur2(bytes(cp) ++ bytes(brokerId)))`, highest first, and assigns a pair while the coordinator partition needs a gateway and the broker holds fewer than `⌈N × g / b_r⌉`. Equal scores go to the lower broker ID. Among several gateways, the highest `hash(logKey, brokerId)` picks one.
- The transformer answers a client in rack `r` for a user partition of `p` with the first match: a gateway of `p` in `r`; another broker in `r` by `p`'s scores; a gateway of `p` in another rack; any broker. Every step skips brokers without the client's listener. A client with an unknown AZ counts as a client of the rack-less group. The gateway path doesn't prefer replicas.
- A local-log partition, one whose fetches need a broker-local log, keeps the transformer's replica routing. In Inkless, consolidating partitions, tiered partitions, and switched partitions whose classic prefix is still on local disks are local-log partitions.
- On the gateway path, a broker that isn't the batch's gateway in its rack rejects the batch with `NOT_LEADER_OR_FOLLOWER`, before writing it, at most once per connection and partition per `metadata.max.age.ms`, and commits it otherwise. A cross-AZ fallback to a gateway of the same coordinator partition isn't a misroute.
- Routing is on only when the coordinator feature is finalized, `__diskless_metadata` is in the image, and `diskless.gateway.routing.enable` is on. Otherwise the transformer uses its hash of the user partition, and nothing is rejected.
- `diskless.gateway.routing.enable` is a boolean, `true` by default, set only at the cluster default. The controller rejects a per-broker value with `INVALID_CONFIG`, and a broker refuses a static value at startup. `diskless.gateway.candidate.enable` is a boolean, `true` by default, set only per broker through `AlterConfigs`. The controller rejects a cluster-default value with `INVALID_CONFIG`, and a broker refuses a static value.
- A deliberate change to the assignment rule needs no version gate. During a rolling upgrade, old and new brokers compute different gateways, which adds commit parts until the upgrade ends and never affects correctness.

Capacity and isolation

- Each coordinator partition's leader serves every commit part, lease request, keepalive, release report, fence, cache-miss read, and maintenance job for its user partitions and tracked brokers. SQLite shares that broker's heap, page cache, disk, and garbage collector with its own partitions.
- In-flight limits on each broker bound the load: one commit part per commit queue, one lease renewal per broker, one release report per participant and tracking partition, one fence batch per tracking partition and participant, and 8 read parts per coordinator partition. A coordinator leader therefore receives at most about `B` commit parts and `8B` read parts for each coordinator partition it leads. No admission cap on the leader and no retry budget in the delegate exist, and the overlay cap only trips on a bug or a stalled high watermark.
- Benchmarks compare this backend with the Postgres backend as Inkless deploys it in production, at 3, 12, and 24 brokers. Produce latency is `RequestMetrics` `TotalTimeMs` for `request=Produce`, and a comparison passes when P50 and P99 are each at most 1.1 times Postgres's. Every benchmark has a decision rule that maps each result to a new default, a docs change, a release blocker, or a separate effort. The gateway runs compare the switch on and off at 12, 21, 24, 45, and 48 brokers. The co-tenancy benchmark prices the capacity risk. [The design's Benchmarks section](design.md#benchmarks) lists every benchmark with its rule.

Metrics

- The coordinator registers a runtime metrics group, `diskless-metadata-coordinator-metrics`, that extends the upstream coordinator runtime metrics, and one Yammer metrics group named after the backend class.
- The backend's metrics are unknown-outcome parts, stale-epoch, stale-sequence, and unreconciled-partition rejections, mapping mismatches, lease renewals by reason and fallbacks, files failed with no lease, early finish-report flushes, idle lease ends, fences by type and answer, pending fence ranges, tracking entries by state and their oldest age, rejected release-report groups, releases from outside the participant set, pending releases, the deletion queue's size and oldest age, barrier outcomes, coordinator partitions whose apply has stopped, WAL pages, apply lag, overlay-cap rejections, private RPC retries, failed read parts, commit parts by kind, and misrouted bytes.
- `CoordinatorPartitionsBelowRackSpread` joins Kafka's `ControllerMetadataMetrics`, and `MisroutedBatchRejectRate` joins the Inkless `WriterMetrics`. The read pool registers a `ThreadPoolMonitor` named `diskless-metadata-read`. [The design's Metrics section](design.md#metrics) describes each metric.

Leader change

- Each coordinator partition changes leader on its own, and a change stalls only that coordinator partition's parts, lease requests, reads, and tracked objects.
- A new tracking leader starts every idle timer at its election, rebuilds the pending fence set from recorded state, and sends one batched fence request per participant. A new participant leader resends the releases that have no release-accepted record.
- A new leader starts with an empty overlay. It rejects requests with a retriable error until its high watermark reaches the log end offset it saw at election and its applier has caught up.
- A leader change may truncate records that were never replicated. A replica discards fetched records above its applied offset that the new leader doesn't have. Because the applied offset never exceeds the high watermark, no applied state is affected.
- At registration, the controller removes a broker from the ELR and last-known ELR of every `__diskless_metadata` partition whose assigned directory for that replica isn't in the new registration, and applies the same rule when a replica moves off a directory the broker no longer registers. An unassigned directory counts as changed. The guard walks only that topic's partitions.
- An unclean shutdown keeps a broker in the ELR of `__diskless_metadata` partitions, as a clean shutdown does. The last-known-leader election stays on as a fallback.
- The cluster-wide ELR clear skips `__diskless_metadata`, on the controller and in the metadata image.

Lifecycle

- The controller never calls the coordinator. Diskless topic creation and deletion commit to cluster metadata only. Each coordinator partition's leader learns the metadata image from `BrokerMetadataPublisher.onMetadataUpdate`. On election and on each metadata update it diffs the user partitions in its SQLite and overlay against the diskless user partitions in the image that map to it. It appends a partition-created record with `metadataOffset` for each one the image has and it doesn't. It appends a topic-deleted record for each topic ID it holds that the image doesn't, only once the image's last contained offset reaches the highest `metadataOffset` among that topic's partitions.
- A user partition with no partition-created record in SQLite or the overlay is unreconciled. Every call that names it gets `COORDINATOR_LOAD_IN_PROGRESS` and appends nothing, and reads get `OFFSET_NOT_AVAILABLE`.
- A repeated partition-created or topic-deleted record applies as a no-op.
- Reconciliation is the retry for a delete: a topic ID still in SQLite and absent from the image is appended again on the next metadata update and on the next election.
- The leader of a switched partition calls `initDisklessLog` after the controller has committed the classic-to-diskless seal, from `InitDisklessLogManager`. That same partition leader calls `repairDisklessLog` from `ReplicaManager`.
- A user partition's creates, deletes, initializations, and repairs go to the same coordinator partition as its commits, so they are ordered with every commit to that user partition.

Maintenance

- Retention enforcement and topic purge run on each coordinator partition's leader, for its own user partitions. Each run appends its outcome records to the log.
- Topic purge runs in capped passes. Each pass appends one deleted-log-purged record per partition it touched. Applying the record removes the partition's batches below the boundary, or all of them without one, and drops the partition's log and producer state rows when the partition is empty. The objects whose last local batch the pass removed are released and reported in batched release reports.
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
- `inkless.control.plane.class` must name the new class. If it names the in-memory or Postgres class while the feature is finalized, the broker refuses to start. The broker also refuses to start the coordinator backend below the SQLite version floor.
- Diskless topics still need the existing Inkless storage configuration and `diskless.enable=true`.
- Gateway routing turns on by default as soon as the controller creates the topic.
- There is no rollback onto Postgres. Clusters already on Postgres are never formatted with the feature.

## Testing Decisions

Good tests check behavior through a public seam: the control plane interface, a broker's observable behavior, or a client's observable behavior. They don't check internal structure such as record layout or the order of internal calls.

Four layers are used. Each one is an existing seam unless noted.

1. The control plane interface. The shared behavioral suite runs unchanged against the new backend, in a single-broker setup, at `N=1` and at `N=8`. One broker leads every coordinator partition, so the second run covers request splitting, result merging, tracking parts, and releases. The suite covers commits and fetches, topic and partition creation, deletion and purge, delete records, retention, file deletion, producer idempotence, diskless log initialization and repair, cross-tier offsets, and consolidation pruning. It adds:
   - A partial send followed by `Unavailable`, and a delegate replaced between two parts of one file. The broker never deletes the uploaded object directly. On the new backend the tracking partition's queue deletes it after the grace period. On the Postgres and in-memory backends the direct delete still happens.
   - The marking rule for each of the five batch-removing records.
   - No fence at `N=8` with no failed part, through commits and retention.
   - Per-file completion on the single-queue backends.
2. Coordinator backend tests, a new layer for cases that one clock or one state can't express. Each test that involves time runs at `N ≥ 2` with its own mock clock per uploader and per leader:
   - Skewed clocks of up to a day across commits, idle lease ends, epoch retirement, a failover, and deletion cycles. Every batch row's object still exists, and no queued key matches a batch row.
   - An uploader that pauses until its lease ends and its key is deleted, then commits. It gets stale-sequence, the broker deletes nothing, and no batch row points to the key.
   - A stale deletion list from a deposed leader. Every key it deletes was queued and has no batch row.
   - The stale-read rule: for every list-offsets mode, an older state's answer is lower than or equal to a newer one's, or unknown.
   - Apply stop on an unknown record type, value version, and tag at the top level and in a nested struct, a stopping record in the middle of a group, and normal apply for a registered extension tag.
   - The write gate per record type and feature level, including a leader with a lagging metadata image.
   - SQLite schema migration forward, a newer schema treated as missing, and a snapshot fetch above the receiver's schema.
   - Golden records: a checked-in file holds every record type at every level, and the current binary decodes all of it.
   - Finish reports: exceptions merged into one range fence, the `Recorded` flag, a failed tracking part whose report merges into the next, and an urgent report on a quiet broker.
   - Leases: a broker that resumes after an idle period, a low-rate broker after a fallback, a lease release that waits after a switch, and broker-wide `finishedThrough`.
   - Epochs: re-registration under the same ID, a new broker ahead of the image, an epoch the partition never saw, and an unreconciled partition across a leader change.
   - Releases: a report before the tracking part, a referenced answer after a release, an early report for a missed sequence, a duplicate after resolution, a resend after a lease end, an abandoned range with a failed tracking part, batched releases under retention, and a stuck fence's age.
   - Load bounds: the backoff schedule, reads off the shard, read slots, and one test per in-flight limit.
   - Tracking under gateways: a fallback inside the gateway set, and spare promotion and demotion.
3. The multi-broker cluster test harness. Tests start a real cluster with real replica fetching and drive it through produce, fetch, admin, and controller operations. The cluster tests cover:
   - Leader failover in the middle of a commit, followed by the retry path, which resolves through the broker's epoch and sequence. The client sees one successful produce, with no duplicate records. A variant pauses the follower's applier, and the new leader's commits wait for the gate.
   - A leader change that truncates unreplicated records. The follower's state matches the new leader's state, and no applied record was undone.
   - A retry that arrives while its first attempt is pending on the leader. The client sees one record and one set of offsets, for idempotent and non-idempotent producers.
   - A commit whose parts land on several coordinator partitions, where one part fails. The committed partitions return offsets, the failed ones get `KAFKA_STORAGE_ERROR`, and the tracking partition deletes the object only after every participant is released.
   - A tracking partition that is down, from the start of a range and early in one. The broker walks its ordered list, and only the down coordinator partition's user partitions fail.
   - A stuck tracking partition: a request with no batch there completes once its parts commit.
   - A broker that stops before the upload, and one that stops after the upload and before any part commits. The epoch end marks the unused range, and the queue deletes both keys after the grace period.
   - A broker that stops after some parts commit, before a finish report covers the file. The lease end covers the file, and the tracking partition deletes the object only after the committed participants release it.
   - An object whose batches expire on one coordinator partition while another still holds batches. The object stays until both are released.
   - One stuck coordinator partition, with two requests in one file. The request whose batches map only to healthy coordinator partitions completes before the stuck partition's deadline, and the other completes at the deadline with `KAFKA_STORAGE_ERROR` for the stuck partition's batches only.
   - A part that reaches its deadline, followed by the same broker's next commit to that coordinator partition. A late request for the earlier part gets stale-sequence, and the later batches commit.
   - A coordinator partition whose ISR drops below `min.insync.replicas`. Commit parts fail at once rather than at the deadline, and lease renewals walk the ordered list.
   - A retried part where some batches failed and some were duplicates, sent to a new leader after a failover. The retry returns the same per-batch results as the first attempt.
   - A broker that restarts with a part in flight. The leader rejects the old epoch, and the epoch-retired record ends its leases.
   - A tracking partition leader change with open entries. The new leader restarts the idle timers, rebuilds the pending fences, and the participants resend their unaccepted releases.
   - A broker cut off from its tracking partition whose idle lease ends. Its leftover keys are deleted.
   - A missed sequence: the part to the tracking partition fails while another participant's part is still retrying. The fence goes to every partition only once a recorded `finishedThrough` reaches the sequence, and the object stays until its participants release it.
   - An empty tracking part with a participant failover. The produce succeeds, and the tracking partition sends no fence.
   - A participant outage under load. Each tracking partition keeps one fence request in flight to it, the backlog drains, and no commit hits a full overlay.
   - Read isolation on the calling broker: fetches for healthy coordinator partitions keep their latency while one is stuck.
   - A part that names a user partition the receiving coordinator partition doesn't own. The leader rejects it with `INVALID_REQUEST`.
   - A replica that catches up from a snapshot after the log start has moved. It serves the same reads as the leader afterward.
   - A newly elected leader whose database is missing. It rebuilds from a follower's snapshot, and it serves no request before the rebuild finishes.
   - A coordinator partition where no replica holds a snapshot covering the log start. Its user partitions stay unavailable for diskless writes, and no partial state is served.
   - A leader whose older snapshot is ahead of every other in-sync replica's reported snapshot. The log start stays put, and produce and fetch continue. Once another in-sync replica reports a snapshot at that offset, the log start advances. After the leader stops, that replica's snapshot still covers the log start.
   - A single-replica topic through the internal override. The leader advances the log start from its own older snapshot.
   - A follower that reports a snapshot and then leaves the in-sync replica set. The log start doesn't advance on its report until it rejoins and reports again.
   - A snapshot source that rotates or restarts during a transfer. The receiver never loads a file mixed from two snapshots.
   - A rebuild while an in-sync follower holds a covering snapshot. The follower serves the transfer, not the leader.
   - A replica that loses its database before the log start has first moved. It replays from offset 0.
   - A topic deleted and purged. Every replica drops the same batches and rows as the leader, and the tracking partitions delete the emptied objects.
   - A cluster formatted with the feature, where a commit goes through the coordinator. A broker whose class names another backend refuses to start. A broker that doesn't support a finalized feature level can't register.
   - A produce that is in flight during a leader change and that fails cleanly or succeeds once.
   - A topic created and a topic deleted while a coordinator leader is down. The leader reconciles on return. Calls before reconciliation get `COORDINATOR_LOAD_IN_PROGRESS` and append nothing, and they succeed on retry after reconciliation.
   - A consolidated topic whose prune floor moves only after the leader confirms it.
   - Admin protection: the controller creates the topic with the fixed configs and the configured minimum ISR, topic listings hide it by default, and delete, create, `CreatePartitions`, a `flush.messages` alter, an unclean election, and an `__admin_client` produce are all rejected through the admin client. A metadata request with auto-create on doesn't create the topic.
   - Rack spread: on a 3-rack cluster, the topic gets one replica per rack, and a reassignment that narrows the spread fails through the admin client.
   - A deposed leader: a broker cut off from the controller and its followers keeps answering, and a consumer that starts from an offset committed under the new leader never resets its position under `earliest` or `latest`. It reads from that offset once the broker learns it was deposed.
   - A genuine seek past the end resets by the consumer's policy.
   - A rolling upgrade followed by a level raise, and a rollback before the raise, after which the rolled-back replicas rebuild and rejoin the ISR.
   - Every replica of a coordinator partition killed uncleanly and restarted with intact disks. The partition recovers with every acknowledged record.
   - Gateways: agreement across brokers, exclusion, the switch's cluster-default scope, a switch toggle that moves no tracking partition, one part per routed file at saturation, a switch on the first empty tracking part, a fence and return without a switch, a misrouted produce that fails once and then commits, no loop under disagreement, local-log partitions, a fenced or cut-off gateway, and a gateway crash.
4. Fault injection, with two new hooks. These cases are otherwise hard to reproduce on a real cluster, and neither hook changes the wire protocol:
   - A hook on the client side of the private RPC. It lets a test drop a reply after the leader has appended a record, drop a lease reply, delay a request during the leader's catch-up, deliver a part's request after its deadline, and stop a broker between the lease, the upload, and the commit. The lost-lease-reply test checks that the broker releases the unconfirmed range only once no planned sequence is at or below its `b`.
   - A test-only hook on a coordinator partition, with three modes: unavailable, delay, and lost tail, which drops the unflushed records on every replica at a chosen fsync point and restarts. A lost-tail suite runs at each fsync point and checks that no other coordinator partition deletes a referenced object or accepts a part into a deleted key.

Seam tests sit next to the code they guard:

- On the coordinator shard, with the runtime's mock partition writer and mock timer: the barrier's outcomes, its per-request 500 ms deadline from arrival, a shared barrier with separate deadlines, a commit part that waits longer than 500 ms without timing out, at most one barrier per flush under adaptive linger, the idle timer racing a part, a forward clock jump, a leader pause between the timer and the append, the acceptance rules for a file with no leased sequence, a fence racing a part, and a broker bug's `INVALID_REQUEST`, a late lease release against a newer range, a failover before a finish report applies, the hull merge, a fence split at the size cap, the overlay cap, and a batched release report with one bad group.
- In the delayed-fetch or replica manager tests: with the cache off, a probe above the high watermark returns at once and appends no barrier.
- Next to each controller and broker guard: internal-topic status, `CreateTopics` and `DeleteTopics` in the controller request handler, `CreatePartitions`, unclean elections, reassignments by replica count and by rack spread, creation at registration and unfencing, placement across racks, the storage change guard, the unclean-shutdown ELR rule, and the cluster-wide ELR clear in the replication control manager and the topics delta, the registration order in the cluster control manager, every fixed config in the configuration control manager, the gateway switch's scope in the controller config validator, produce and `DeleteRecords` in the replica manager, the directory fsync in the log manager, the downgrade refusal and feature dependency in the feature control manager, the controller config ranges for the minimum ISR and the replication factor, and the rack-spread gauge in the controller metrics.
- The shared key-name function: the cluster ID keeps two clusters' keys apart.
- The mapping function gets a unit test with fixed expected values, so a change to it fails the build. The same test checks that the slot mapping equals `Utils.toPositive(Utils.murmur2(logKey)) % N` for every allowed `N`, and that the config rejects an `N` that isn't a power of two.
- Snapshot writing, rotation, loading, and chunk serving get unit tests on a temporary directory, with a fault hook at each step of rotation and transfer. They cover a tag read while applies run during the backup, a crash at each rotation step, a transfer whose source rotates or restarts before the receiver resumes, a checksum mismatch on load and on serve, and the schema version in the file name. This is a deliberate exception to the public-seam rule: no public seam reproduces these cases deterministically. The cluster tests still cover snapshot behavior end to end.
- Apply rules for file records, broker rows, leases, fences, releases, the deletion queue, and a record for a user partition with no partition-created row get unit tests at the replica state layer, with records appended directly to a test log. No public call can produce a duplicate or conflicting record, so this is a second deliberate exception to the public-seam rule.
- Routing, unit tests over the assignment interface and the metadata transformer: fixed values for the default rule, stability with at most 5 extra moves per join or leave, transformer routing with one case per fallback step, the tracking selection with fixed values, predicted parts per file, unknown AZ, no replica preference, and the conditions that turn routing on.
- Produce path, unit tests on the append completer: a request with only rejected batches completes at file close, and the batch coordinate cache fills per part.
- SQLite: the checkpoint rules, with no checkpoint on the apply thread, a backup held open, and the 4 MiB group cap, and the version check at startup.

Model check: the TLA+ model of the fence, floor, and release protocol in [model/](model/) moves into the repo, with its three specs, the scenario module, and `run-all.sh`, plus their bounds and last results. A change to a protocol rule updates the spec and reruns `run-all.sh`. CI doesn't run the model, because the full runs take hours.

Benchmark tooling: the benchmarks use the coordinator-partition hook's unavailable and delay modes, and a verifying consumer checks that no acknowledged record is lost.

Prior art:

- The shared behavioral suite, with its in-memory and Postgres subclasses. The new backend gets a subclass in the same way.
- The group coordinator's shard tests with the mock partition writer and mock timer, and the runtime's timer tests for override and `NOT_COORDINATOR` handling.
- The controller's replication, configuration, cluster, and feature control manager tests, for the guards and the ELR rules.
- The Postgres test helpers that run migrations and clean up containers, for any test that needs a database.
- The control plane outage cluster test, for the availability gate and runtime reconfiguration.
- The consolidated diskless topics cluster test, for prune floors and cross-tier offsets.
- The untiered-to-consolidated migration test, for classic-to-diskless switch behavior.
- The diskless topic delete test, for topic deletion and file cleanup.
- The metadata transformer's existing tests, for routing.

Optional, not required for done: a ducktape system test of the full deployment, following the existing consolidation pipeline test.

## Out of Scope

- Changing the partition count of `__diskless_metadata` after creation.
- Orphan-object scanning. A report-only scanner is future work.
- An off-broker copy of coordinator state and a restore procedure. Losing every replica of a coordinator partition is an accepted risk. The object footer recovery, the soft-delete bound, and the empty restart of a coordinator partition are future work.
- A replay checker and an inspect-and-repair tool for a deterministic apply bug that corrupts every replica alike. That bug is an accepted risk.
- Running the benchmarks. The design defines their pass criteria.
- KIP-1163's `PreferredProduceBrokers` and any public protocol for gateways.
- Moving a user partition between coordinators.
- Transaction state.
- The KIP's public APIs, keys 95 to 101.
- Follower reads.
- Migrating existing Postgres-backed clusters.
- A Raft quorum for metadata.
- Tiered-storage offload.

## Further Notes

- The KIP text doesn't define snapshot distribution. This spec defines it, through the private fetch-snapshot request modeled on KIP-630. That request, the lease, finish-report, and fence rules in Object tracking and deletion, the gateway assignment, and the barrier in Reads are the parts most worth a reviewer's attention.
- Open items for the implementer, none of which changes the design: the private request names and API key numbers, the error codes for a busy snapshot source, a stale epoch, and a stale sequence, the names of the two features and the record set of each level, the name of the internal replication-factor override, the SQLite driver choice and license with a bundled SQLite at the version floor, the checkpoint trigger by polling or `wal_hook`, the commit deadline default, which also sets the runtime's write timeout, the idle lease timeout default, how a leader that can't rebuild its state gives up leadership of its coordinator partition, the provisional defaults and the benchmark rules that settle them, the stability bound measured again with murmur2, and where `SharedState` gets the gateway assignment implementation.
- A late commit part can still commit after the producer got `KAFKA_STORAGE_ERROR`, until the broker's next commit to that coordinator partition applies, a fence raises the floor, its epoch retires, or its lease ends. A non-idempotent producer that retries through another broker can see its data twice, with the late copy after the retried one. A classic `acks=all` timeout and the Postgres backend have the same window.
- No clock decides whether a deletion is safe. Retention and `LogAppendTime` under a skewed leader clock, and grace-period skew beyond half the grace period, are accepted risks covered by an NTP procedure. A lost tail, a deterministic apply bug, and a delete from outside the coordinator are accepted risks, because a classic topic loses data to the same causes. [The design's Accepted risks section](design.md#accepted-risks) lists every accepted risk with its signal or procedure.
- An object leaks only when storage completes an upload after its key was deleted, or when a held entry's participant never reports. Those leaks are accepted over a scanner that deletes objects no coordinator partition knows, which fails toward data loss.
- The full design rationale, the failure-mode tables, the rejected alternatives, and the terms are in [design.md](design.md).
- Growing the coordinator, merging WAL objects into extents, tiered metadata in the coordinator, partitionless key ranges, a report-only orphan scanner, the object footer recovery, the deletion bounds, the empty restart, extra gateways for a hot coordinator partition, traffic-following tracking, a cluster-only config scope upstream, the per-read rule in Inkless, and cold-path fetches at brokers with an empty gateway set are future work. [The design's Future work section](design.md#future-work) describes each one and the decisions in this spec it depends on.
