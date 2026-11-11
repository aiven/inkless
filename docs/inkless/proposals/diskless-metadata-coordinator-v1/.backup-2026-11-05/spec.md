# Diskless metadata coordinator

Status: ready-for-agent

Design record and decision log: [design.md](design.md). Map: [map.md](map.md). Comparison of owner-based and scanner deletion: [deletion-comparison.md](deletion-comparison.md).

## Problem Statement

Diskless topics depend on a control plane that keeps their batch, file, and log state. Today the only durable implementation is the Postgres control plane, which every broker reaches through one shared database. A cluster that wants diskless topics without running that database has no option. The in-memory implementation is for tests, and it loses all state on restart.

KIP-1164 describes a Diskless coordinator that stores this state in an internal Kafka topic, so the cluster runs without an external database. Its full design has multiple coordinator partitions, file ownership handoff, orphan scanning, and new public protocol keys. This effort keeps multiple coordinator partitions and a narrower form of file ownership. The rest is more than this effort needs, and it does not fit the control plane interface the broker code already uses.

Operators need a diskless control plane that keeps its state in Kafka, survives broker failures, spreads its load across brokers, and fits the existing control plane interface, so the rest of the broker code does not change.

## Solution

Add a third control plane implementation that stores its state in an internal topic, `__diskless_metadata`, with a fixed number of partitions. Each diskless user partition maps to one coordinator partition by a hash of its log key, which is its topic ID and partition index, through a fixed set of slots. The leader of a coordinator partition is the only broker that assigns offsets and writes state changes for the user partitions that map to it. Every replica of a coordinator partition keeps a local SQLite copy of that partition's state, rebuilt from the log and from snapshots.

A broker splits each call by coordinator partition and sends each part to that partition's leader, locally or over a private RPC. A commit therefore goes to every coordinator partition its object has batches for. Each part succeeds or fails on its own, as a produce already can per partition, so no two-phase commit is needed. The leader appends one outcome record per committed part and replies only after the record is replicated.

Every object has one owner, a coordinator partition among its participants. Before the upload starts, the broker stores an intent at the owner, so every object in storage is recorded first. The other participants report to the owner when they no longer hold any batch of the object. The owner fences the participants it never heard from, and it deletes the object once every participant is released. The owner is the only path that deletes an object.

Every replica keeps its own snapshots on its disk. A replica that needs records the log no longer holds fetches a covering snapshot over a private RPC, from an in-sync follower when one has it. Only the leader prunes the log. It advances Kafka's log start offset only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for. A topic with one replica advances from the leader's own older snapshot alone.

Operators format a new cluster with the coordinator feature already finalized. `inkless.control.plane.class` must name the new class, and a broker refuses to start if it names another backend. Clusters that already run on Postgres are not formatted with the feature, and they stay on Postgres.

## User Stories

1. As an operator, I want to run diskless topics without an external database, so that my cluster has fewer components to run and monitor.
2. As an operator, I want the coordinator state stored in an internal Kafka topic, so that I can back it up and replicate it with the tools I already use.
3. As an operator, I want the coordinator to have replication factor 3 with a minimum of 2 in-sync replicas by default, so that one broker failure does not stop diskless writes.
4. As an operator, I want to set the replication factor of the coordinator topic with `diskless.metadata.topic.replication.factor`, so that a development cluster can run with one replica.
5. As an operator, I want the controller to create the coordinator topic on first enable, so that I do not create it by hand.
6. As an operator, I want replicas of the coordinator topic placed across racks where possible, so that losing one rack does not take out every copy.
7. As an operator, I want `inkless.control.plane.class` to name the coordinator class whenever the coordinator feature is finalized, so that broker configuration and cluster metadata select the same backend.
8. As an operator, I want the coordinator feature finalized when the cluster is formatted, so that the coordinator is active from the first startup.
9. As an operator, I want the controller to reject a broker that does not support the finalized feature, so that a broker without the private requests cannot join.
10. As an operator, I want the broker to refuse to start when the feature is finalized and the class names the in-memory or Postgres backend, so that the two selectors cannot disagree.
11. As an operator, I want clusters already on Postgres to stay on Postgres, so that enabling the coordinator cannot break a running cluster.
12. As an operator, I want the coordinator to take snapshots on a time interval, so that the log stays bounded on an idle cluster.
13. As an operator, I want the coordinator to take snapshots after a set number of log bytes, so that a burst of commits does not grow the log until the next time tick.
14. As an operator, I want to configure the snapshot interval and byte threshold, so that I can trade disk use against snapshot cost.
15. As an operator, I want the coordinator to keep the newest snapshot and one older one, so that a replica that is mid-fetch can still finish.
16. As an operator, I want the coordinator log pruned only up to an offset that at least `min.insync.replicas` in-sync replicas hold a snapshot for, so that a leader failure still leaves a snapshot to rebuild from.
17. As an operator, I want a replica that falls behind the log start to catch up from a snapshot, so that it does not stop and wait for manual repair.
18. As an operator, I want a replica with a corrupt or missing local database to rebuild from a snapshot and the log, so that I do not restore it by hand.
19. As an operator, I want a broker restart to replay from its last applied offset, so that it does not reprocess the whole log.
20. As an operator, I want snapshots kept on broker disk rather than object storage, so that failover works while object storage is unavailable.
21. As a Kafka client, I want a produce to return the offsets the coordinator assigned, so that my records have the same ordering guarantees as on Postgres.
22. As a Kafka client, I want produce idempotence to hold across a leader change, so that a retried produce does not create duplicate records.
23. As a Kafka client, I want a non-idempotent producer's records to be written once even when the broker retries a commit, so that a broker-side retry does not duplicate data.
24. As a Kafka client, I want a fetch to see every record committed before it, so that a consumer does not miss data after a failover.
25. As a Kafka client, I want `ListOffsets` to return strict results, so that the earliest and latest offsets match what was acknowledged.
26. As a Kafka client, I want `DeleteRecords` to move the log start on every replica, so that a deleted record stays deleted after a failover.
27. As a Kafka client, I want a topic deleted in the coordinator to stop accepting produce, so that a deleted topic does not keep taking data.
28. As a Kafka client, I want a produce to a new diskless partition to succeed once the coordinator has reconciled it, and to get a retriable error before then, so that the controller never waits on the coordinator.
29. As a Kafka client, I want a commit that is in flight during a leader change to succeed or fail cleanly, so that I do not see a half-applied write.
30. As a broker, I want a produce to wait for the coordinator reply only as long as its request timeout, and a commit deadline no longer than that timeout, so that a stalled coordinator does not stall the whole broker.
31. As a broker, I want the coordinator delegate to send each commit part to the current leader of its coordinator partition and retry on a `NOT_COORDINATOR` error within a commit deadline, so that a leader change looks like a short delay, not a failure.
32. As a broker, I want to find each coordinator partition's leader through the metadata cache, so that I do not add a discovery step.
33. As a broker, I want a fetch lookup that misses the batch coordinate cache to reach the leader, so that the reply reflects committed state.
34. As a broker, I want the cross-tier log start reports to reach the leader, so that remote storage cleanup stays on schedule.
35. As a broker, I want consolidation pruning to be decided on the broker that holds the partition's local log, so that the coordinator never needs a log it does not have.
36. As a broker, I want the prune floor on my partitions to move only after the coordinator confirms the prune, so that I do not delete a log the coordinator still needs.
37. As a broker, I want the delete-records path to report cross-tier log start changes to the leader, so that the logical earliest offset stays consistent.
38. As a controller, I want diskless topic creation and deletion to commit to metadata only, so that topic lifecycle does not depend on the coordinator being available.
39. As a partition leader, I want diskless log initialization to reach the coordinator after the controller commits the seal, so that a classic topic can become diskless.
40. As a partition leader, I want diskless log repair to reach the coordinator, so that a switch that stopped midway can be finished.
41. As a leader, I want retention enforcement to run only on the leader of each user partition's coordinator partition, so that each retention pass is applied once.
42. As a leader, I want topic purge to run only on the leader of each user partition's coordinator partition, so that a deleted topic's batches are removed once.
43. As an owner, I want file deletion marking to run only on an object's owner, so that no two coordinator partitions mark the same object.
44. As an owner, I want object deletion to wait for the grace period, so that a reader still using an old file can finish.
45. As a leader, I want to serve only after my high watermark reaches the log end offset I saw at election, so that my reply to a strict read includes every committed record.
46. As a leader, I want to reply with a retriable error while I catch up after election, so that brokers retry instead of getting a stale answer.
47. As a developer, I want the shared control plane test suite to run against the new backend, so that its behavior matches the other backends.
48. As a developer, I want a cluster test for leader failover in the middle of a commit, so that the retry path is covered end to end.
49. As a developer, I want a cluster test for a replica that catches up from a snapshot, so that snapshot distribution is covered end to end.
50. As a developer, I want a cluster test where a conflicting control plane class refuses to start, and a broker that lacks the feature cannot register, so that the format-time gate is covered.
51. As a developer, I want to inject a dropped reply after the leader has appended a record, so that the retry path can be tested deterministically.
52. As a developer, I want to inject a commit that arrives during the leader's catch-up, so that the high-watermark gate can be tested deterministically.
53. As a developer, I want a unit test of the snapshot writer and loader, so that I can diagnose a snapshot bug without starting a cluster.
54. As a support engineer, I want a clear error when a broker does not support the coordinator RPCs, so that I can find the broker that blocks the rollout.
55. As a support engineer, I want the coordinator to report its state as metrics, so that I can see lag, snapshot age, log size, open owner entries, and unknown-outcome commits.
56. As a support engineer, I want each private RPC to report errors with the same codes as the control plane method it wraps, so that existing dashboards and alerts still apply.
57. As a broker, I want an object deleted only after every participant has released it or been fenced, so that a late commit never points at a missing object.
58. As a broker, I want an object whose commit outcome is unknown left to its owner, so that the broker never deletes an object a late commit may still reference.
59. As an operator, I want a newly elected leader with a corrupt or missing database to rebuild from a follower's snapshot before it serves, so that a failover does not depend on the old leader's disk.
60. As an operator, I want a coordinator partition with no replica holding a snapshot that covers the log start to stay unavailable rather than serve partial state, so that a rebuild never silently loses data.
61. As an operator of a single-replica cluster, I want the log pruned from the leader's own snapshot, so that the log stays bounded when there is no other replica to confirm.
62. As a Kafka producer, I want a retried produce to return the same offsets while the first attempt is still pending, so that the log holds one copy of the data.
63. As an operator, I want time and size retention disabled on the coordinator topic, so that only the snapshot rule deletes its records.
64. As an operator, I want a crash during a snapshot to leave the previous snapshots intact, so that a replica never ends up without a valid snapshot.
65. As an operator, I want a resumed snapshot transfer never to mix bytes from two snapshots, so that a rebuilt database matches one position in the log.
66. As an operator, I want snapshot transfers served by an in-sync follower when one can, so that rebuilding a replica does not add load to the coordinator leader.
67. As an operator, I want a snapshot report to stop counting once its replica leaves the in-sync replica set, so that the log start never moves on a snapshot that may be gone.
68. As a broker, I want snapshot reports sent off the replica fetcher thread, so that an unresponsive leader cannot push a follower out of the in-sync replica set.
69. As an operator, I want a topic purge to remove the same rows on every replica, so that followers do not keep state the leader dropped.
70. As an operator, I want a replica to rebuild by replaying the log before any snapshot exists, so that a new cluster has no unavailability window before its first snapshot.
71. As an operator, I want a power loss on a replica to cost only a replay from the log, so that it does not force a rebuild from a snapshot.
72. As an operator, I want to set the partition count of the coordinator topic with `diskless.metadata.topic.num.partitions`, so that commit and read load spread across several leaders.
73. As an operator, I want the partition count fixed once the topic exists, and `CreatePartitions` rejected for it, so that no user partition ever changes coordinator.
74. As an operator, I want a coordinator partition that loses its in-sync replicas to stop only the user partitions that map to it, so that one failure does not stop every diskless write.
75. As a Kafka client, I want a commit that succeeds on some coordinator partitions and fails on others to return offsets for the committed partitions and `KAFKA_STORAGE_ERROR` for the rest, so that a retry does not duplicate committed data.
76. As an operator, I want every uploaded object recorded at its owner before the upload starts, so that a broker crash after the upload never leaves an object no one will delete.
77. As an operator, I want the broker to fall back to another owner when the preferred owner is unavailable, so that one unavailable coordinator partition does not fail whole objects.
78. As an operator, I want the coordinator to reject a commit part older than the maximum commit age, so that a delayed request can never add batches to an object that may be gone.
79. As an operator, I want tombstones and fences to expire after twice the maximum commit age, so that SQLite does not grow with every object the cluster ever deleted.
80. As a developer, I want the user-partition mapping and the preferred-owner choice pinned by a test with fixed expected values, so that brokers of different versions always agree on them.
81. As a broker, I want a leader to reject a part for a user partition that does not map to it, so that a broker that maps differently cannot give a user partition a second writer.
82. As an operator, I want a broker to serve at most a set number of snapshot transfers across all its coordinator partitions, so that rebuilding a replaced broker does not starve the brokers that serve the snapshots.
83. As an operator, I want the coordinator partition count restricted to a power of two, so that the coordinator can later grow by splitting partitions without remapping every user partition.

## Implementation Decisions

Topology and placement

- The coordinator state lives in one internal topic, `__diskless_metadata`, with `diskless.metadata.topic.num.partitions` partitions, 8 by default. The default is provisional until the partition-count benchmark runs.
- The topic has replication factor 3 and a minimum of 2 in-sync replicas by default. The replication factor is set by `diskless.metadata.topic.replication.factor`.
- The controller creates the topic on first enable. It places replicas across racks where possible.
- The leader of each coordinator partition is the single writer for the user partitions that map to it.
- Replicas fetch the partition with normal replica fetching. Replicas do not vote and no Raft protocol is used.
- The topic is created with `flush.messages=1`. Each replica fsyncs every batch, so a committed reply does not point at a record that only the page cache holds. The leader's append linger keeps this to one fsync per batch. Replication alone does not cover a correlated power loss on every replica that holds a record, and for a coordinator partition that loss reuses offsets and can leave a late commit pointing at a deleted object. The commit-throughput benchmark measures the fsync cost.
- The topic is created with `retention.ms=-1` and `retention.bytes=-1`, so only the snapshot rule moves its log start.

Coordinator partitions

- A user partition maps to slot `Utils.toPositive(Utils.murmur2(logKey)) % S`, where `S` is the protocol constant 4096, and the slot maps to coordinator partition `slot % N`.
- The log key of a Kafka partition is the 16 bytes of the topic ID, most significant bits first, followed by the partition index as 4 big-endian bytes. It is the only log key encoding this effort defines. The partitions of one topic therefore spread across coordinator partitions.
- `diskless.metadata.topic.num.partitions` accepts only a power of two from 1 to 4096. With such an `N`, the slot mapping gives the same result as `Utils.toPositive(Utils.murmur2(logKey)) % N`, and brokers don't store a slot table.
- Every broker reads `N` from the metadata image. The config sets `N` only when the controller creates the topic. A call made before the topic exists gets a retriable error.
- `N` never changes after creation, and the controller rejects `CreatePartitions` for `__diskless_metadata`. Growing `N` by splitting coordinator partitions is future work, which the slot layer keeps possible.
- The mapping function, the log key encoding, and `S` are part of the protocol. A test with fixed expected values pins them.
- Each leader recomputes the mapping for every user partition in a request it receives, and rejects a mismatch with `INVALID_REQUEST`.
- Each coordinator partition has its own log, SQLite database on each replica, snapshots, overlay, and leader. With `N=1`, the design reduces to a single coordinator.

Transport and protocol

- Each broker holds a delegate that implements the existing control plane interface. For each call, the delegate splits the request by coordinator partition, sends the parts in parallel, and merges the results back into request order. A part runs locally on its leader. On any other broker, the part goes to that leader over a private RPC.
- The new request types use private API keys in the 504 range and above. No public Kafka protocol changes, and the KIP's proposed keys 95 to 101 are not used.
- A broker that receives a request for a coordinator partition it does not lead returns `NOT_COORDINATOR`.
- Each private request returns the same error codes as the control plane method it wraps.
- The private request set is: register intent, commit file, find batches, list offsets, delete records, initialize diskless log, repair diskless log, prune diskless logs, advance cross-tier log start offset, report release, fence file, fetch snapshot, and report snapshot offset.
- A produce reply cannot carry the offsets a commit assigns, so commit has its own reply.

Log records

- The leader appends one outcome record per committed call or part. Every replica applies the records in log order, and only up to the high watermark.
- Each record carries the state change and every assigned value, so replicas reach the same state without recomputing it.
- The record types are: intent registered, file committed, file fenced, participant state recorded, owner accepted, owner entry dropped, keys expired, partition created, topic deleted, records deleted, diskless log initialized, diskless log repaired, file delete marked, file deleted, retention applied, log pruned, cross-tier start advanced, and deleted log purged. Implementers may merge or split them.
- Every change to a diskless partition's coordinator state is a record, including its creation and deletion. KRaft stays the source of intent. The record fixes the position of the coordinator's state change among the commits.
- Apply reads nothing outside its own coordinator partition's log, and it never reads a clock, so the same log produces the same state on every replica. Every time-based decision is a record the leader appends.
- Records use Kafka's message-JSON conventions, so their schemas can be versioned.
- Every record names the log it changes with a `Log` struct that holds `TopicId` and `PartitionIndex`. A later log key encoding adds a tagged field to the struct, with no record version change.

Speculative state

- The leader keeps an in-memory overlay of records it has appended and not yet applied. The overlay runs from the offset after the applied offset through the last written offset. Followers keep no overlay.
- An overlay entry holds the object key, the assigned offsets, the producer epoch and sequence numbers, and a pending fence or intent when the entry is one.
- The leader checks and reserves on one thread. A commit reads SQLite plus every earlier overlay entry.
- The leader collects reservations for one append linger interval, then appends them as one log batch.
- When the high watermark passes an entry, the leader applies it to SQLite, removes it from the overlay, and replies.
- If an append fails, the leader resigns, or the log truncates an entry, the leader removes that entry and every entry after it and fails their parked replies with a retriable error.
- The overlay has a cap. Past that cap, the leader rejects a new commit with a retriable error.
- A new leader starts with an empty overlay.

Write path

- The leader runs the overlay loop on `CoordinatorRuntime`, with one shard per coordinator partition, as the group coordinator does for `__consumer_offsets`. The runtime supplies the single writer thread, the append linger, the revert after a failed append, the parked replies, and the `NOT_COORDINATOR` failure on resignation.
- The shard stores the overlay in timeline structures backed by a `SnapshotRegistry`. Committed state stays in SQLite.
- A custom `CoordinatorLoader` replays from the applied offset stored in SQLite.
- A follower applier writes fetched records to SQLite up to the high watermark. The runtime runs on the leader only.

Commit and retry

- The broker stores an intent at the owner before the upload starts, then uploads, then sends the commit parts.
- Every commit part carries the object key, the participant set, the owner, and the first-send time: the broker's wall-clock time when the delegate first sent that part, reused on every retry.
- A part runs on its leader in this order: reject it when its first-send time is older than `diskless.metadata.commit.max.age.ms`, one hour by default; look up the object key in the overlay and in SQLite and reject it when the key is fenced or tombstoned; check the producer epoch and sequence rules against both; reserve the key and add one file-committed record to the current batch; and, when the high watermark passes the record, apply it to SQLite, remove the overlay entry, and reply.
- The check enforces the same producer epoch and sequence rules as the Postgres commit functions.
- A commit identifies its upload by object key. The broker generates a fresh key for every upload. Deduplication runs per coordinator partition.
- If a part arrives with an object key that already has a pending or applied commit, the leader assigns no offsets and appends nothing. It returns the stored offsets, waiting for the pending record if needed.
- A part whose batches all fail their checks records nothing. The owner resolves that participant by fencing it.
- The file record insert at apply has a uniqueness check on the object key. A conflict follows the apply rules below.
- The `DisklessMetadataControlPlane` delegate retries each part on its own against the current leader of its coordinator partition after `NOT_COORDINATOR` or a retriable error, with the same object key and first-send time. If the first attempt is still pending on the leader, the retry waits for its result and appends no second record. The delegate stops at a commit deadline, which does not exceed the produce request timeout. The delegate never fences.
- At the deadline, batches on confirmed parts get their offsets, and batches on unconfirmed parts get `KAFKA_STORAGE_ERROR`. Their outcome is unknown until the part's first-send time passes the maximum commit age. If any part committed, `commitFile` returns per-batch results. If none did, it throws a control plane exception, and `FileCommitJob` runs its existing cleanup path unchanged.
- On the new backend, `isSafeToDeleteFile` returns false without a request. The broker never deletes an object itself, except on the existing `ControlPlaneUnavailableException` path, where no part was sent.
- Apply rules for file records are strict. The leader's checks make the cases below unreachable, so a replica that meets one treats it as a bug:
  - A file-committed record for a key that is already applied with identical offsets and content is a no-op for file state. The applied offset still advances.
  - A file-committed record for a key that is already applied with different content, or for a fenced or tombstoned key, is an invariant violation. The replica stops serving that coordinator partition and reports corruption.
  - A file-fenced record for a key that is already fenced is a no-op. A file-fenced record for a key with an applied commit leaves the commit in place, and its answer is referenced with the owner named in that commit.

File references

- The participants of an object are the coordinator partitions that its batches map to. The preferred owner is the participant at index `Utils.toPositive(Utils.murmur2(objectKeyBytes)) % participants.size()` in the sorted participant list. A test with fixed expected values pins this choice.
- The broker stores the intent, meaning the object key, the participant set, and the owner, at the preferred owner. If that participant answers `NOT_COORDINATOR` or a retriable error, or times out, the broker tries the next participants in sorted order. If none stores it, nothing is uploaded and the produce fails with `KAFKA_STORAGE_ERROR`.
- The owner is whichever participant stored the intent. Every part names it, and every record stores it. The owner never changes.
- The broker gives up on the upload and the commit once the intent is older than the maximum commit age.
- The owner's entry holds each participant's state: silent, referenced, or released. A state only moves forward. The owner tracks its own state locally, and fences the key on itself if its own part has not committed when the intent passes the maximum commit age.
- A participant releases an object when it holds no batch of it anymore, through retention, delete records, topic purge, or consolidation pruning. Its leader reports the release to the owner named in its part. The broker's scheduler sends and retries the report with Kafka's `ExponentialBackoff` client defaults. A repeated report is a no-op at the owner.
- When the owner accepts a report, the participant appends an owner-accepted record. A new participant leader resends only the releases without one. When the record applies, the participant deletes its row for the object if the part's first-send time is older than the maximum commit age at the record's append time, and keeps a tombstone otherwise. A tombstone rejects any later commit for the key.
- Participants report only releases. Once the intent is older than the maximum commit age, and the owner and every participant it has heard from are released, the owner fences the participants that are still silent. Each participant is fenced at most once per object.
- A fence is final. A participant that holds a commit for the key answers referenced, with the owner named in its part, and the owner waits for its release report. Any other participant records the fence, rejects later commits for the key, and counts as released.
- A fence answer that names a different owner means the fencing partition holds a stale intent from a lost reply. That partition drops its entry and never deletes the object.
- The owner marks the object for deletion once every participant is released and the intent is older than the maximum commit age. `FileCleaner` runs on owner leaders only, after the deletion grace period. Deleting a missing object counts as success on every backend.
- Tombstones and fences expire once they are older than twice the maximum commit age. The leader appends a keys-expired record for them, and apply removes them. This assumes broker clocks differ by less than one maximum commit age.
- Object storage needs no transactions and no atomic writes. It needs a delete by key that removes whatever is stored under it. Unfinished uploads must never be visible as objects, which holds for S3 `PutObject`, GCS single-request and resumable uploads, and Azure block blobs.
- An object leaks only when storage completes an upload after the owner deleted it, or when a bug loses a record. No scanner deletes objects. A report-only scanner is future work.

Local state

- Each replica of a coordinator partition keeps that partition's state in its own local SQLite database. The database is a cache. The log is the source of truth.
- Every apply stores the applied offset and the leader epoch of that record in the same transaction as the state change.
- On open, a replica checks the stored offset and epoch against the log. It keeps SQLite only if the log still holds that offset and the record there carries that epoch. A flushed log can still be truncated and rewritten under a later leader epoch, so the offset alone does not confirm the applied row is still current.
- Invariant: the applied offset is always at or below the high watermark. A replica learns the high watermark from each fetch response. A record above the high watermark stays unapplied in the log buffer until the watermark passes it.
- SQLite holds only records at or below the high watermark. The leader drops unreplicated work from the overlay, so a replica never reverses an applied row.
- SQLite runs in WAL journal mode, so a snapshot backup or a read does not block applies. The WAL file grows during a long backup and shrinks after it.
- SQLite runs with `synchronous=NORMAL`. A power loss can roll back the most recent applies, together with their applied offset. The replica then reopens at an earlier offset and replays from the log, which `flush.messages=1` keeps durable. SQLite can fall behind the log but never get ahead of it.
- The schema must support indexed lookup by topic partition and offset, because find-batches and list-offsets need it, and by object key, for owner entries, participant release states, tombstones, and fences.
- A coordinate row must be able to describe an extent: an offset range mapped to an object and a byte range, with the producer fields optional. This effort writes only WAL batch rows, and each one carries its producer fields. The control plane interface and the find-batches response don't change.
- The SQLite driver is a JDBC driver with a bundled native library. Its license and packaging must pass the repo's dependency checks before the choice is final.

Snapshots

- Each coordinator partition runs the snapshot rules on its own, with its own log start, snapshots, and reports.
- The log start is Kafka's own log start offset of the coordinator partition. Only the leader advances it, with the same call the delete-records path uses, and followers adopt it from fetch responses. Kafka refuses a log start above the high watermark.
- The leader advances the log start only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for. The new log start is the lower of two offsets: the offset of the leader's own older snapshot, and the highest offset that at least `min.insync.replicas - 1` other in-sync replicas have reported a snapshot at or beyond. At the default of 2, that is the leader and one other replica.
- When the topic has a single replica, there is no other replica to confirm, and the leader advances the log start from its own older snapshot alone.
- The wait delays log-start advancement only. Produce and fetch do not wait on a follower's snapshot report.
- The log start stays at offset 0 until the first advancement. A replica that loses its database before then replays from offset 0 and needs no snapshot.
- Every replica takes its own snapshot, from its own applied state, when either `diskless.metadata.snapshot.interval.ms` elapses, 1 hour by default, or `diskless.metadata.snapshot.max.bytes.between.snapshots` of log has been applied since its last snapshot, 1 GiB by default. The first condition to trigger wins. Both defaults are provisional, and the state-size and failover benchmarks confirm or lower them. With consolidated topics the database is expected to stay small, so the byte default may come down or scale with the database size.
- A snapshot uses the SQLite backup API, which writes a point-in-time copy to a temporary file. The snapshot's offset and epoch come from the applied-offset row inside the finished copy, never from the live database.
- The replica fsyncs the temporary file, renames it to a final name built from the offset, the epoch, and a checksum, and fsyncs the directory. Only then does it delete the snapshot that falls out of the retained pair. On startup it deletes any leftover temporary file. It checks a snapshot's checksum before loading or serving it.
- Snapshots are written to the broker's local disk. They never go to object storage. Each replica keeps the newest snapshot and one older snapshot of its own. The count is a constant, not a config, because the log-start rule depends on that pair.
- A replica reports its newest snapshot offset to the leader on a private request after it finishes a snapshot, when it sees a new leader, and when it rejoins the in-sync replica set. The report is not a field on the public fetch request. The broker's scheduler sends and retries it, never the replica fetcher thread. Retries use Kafka's `ExponentialBackoff` with the client retry defaults, 100 ms initial, 1 second maximum, multiplier 2, and 20% jitter, as constants rather than configs.
- The leader records the newest snapshot offset reported by each replica. A report counts only if its replica is in the leader's committed in-sync replica set, with no pending removal, when the leader considers advancing the log start. The leader drops a replica's report when that replica leaves the set.
- A replica fetches a peer's snapshot only when it needs records below the log start: when it starts empty, when its database is lost or corrupt and its own snapshots do not reach the log start, or when the log start moved past its newest snapshot while it was out of the in-sync replica set.
- It fetches the newest covering snapshot from an in-sync follower that holds one, and from the leader only when no follower does.
- Every fetch-snapshot request names the snapshot ID, its offset and epoch, and a byte position. Rotation does not delete a file that has an active transfer. A transfer stays active until it finishes or until no chunk request has arrived for `diskless.metadata.snapshot.transfer.idle.timeout.ms`, 60 seconds by default, and each chunk request restarts that timer. The receiver writes to a temporary file under that ID and resumes from its last fsynced byte. If the source answers that the ID is gone, the receiver discards its partial file and asks for the newest covering snapshot again.
- The request and response follow KRaft's fetch-snapshot schemas. The request carries the receiver's replica ID, a maximum byte count, and either a snapshot ID with a byte position or, on the first request, a minimum end offset. The response carries an error code, the snapshot ID, the total file size, the chunk position, and the bytes in a `records`-typed field, so the source sends from the file with zero copy. A chunk is at most 8 MiB, as in KRaft. `SNAPSHOT_NOT_FOUND` and `POSITION_OUT_OF_RANGE` cover a missing snapshot and a bad position.
- A broker serves at most `diskless.metadata.snapshot.max.concurrent.transfers` active transfers across all its coordinator partitions, 1 by default. A request beyond the limit gets a retriable busy error, and the receiver tries the next replica with a covering snapshot, in follower-first order. The snapshot-transfer benchmark confirms or raises the default.
- When the file is complete, the receiver checks its checksum, replaces its database, takes its applied offset from the snapshot, and replays the log from there.
- A replica serves its finished snapshot files whatever the state of its live database, and stops serving a file only when it fails its checksum.
- A replica treats its database as missing or corrupt when it fails the open check, or when its applied offset is ahead of its log. Such a replica rebuilds before it serves requests: from a peer's snapshot once the log start has moved, and from offset 0 before then. This applies to a newly elected leader too, which does not serve until the rebuild and the high-watermark gate both finish.
- If no replica holds a snapshot that covers the log start, a broker that needs one cannot rebuild, and that coordinator partition stays unavailable for diskless writes rather than serving partial state.
- The KIP does not define how snapshots reach other replicas. This effort defines it through the private fetch-snapshot request, modeled on KIP-630.

Reads

- Only the leader answers reads. Find-batches, list-offsets, and every other read go to the leader of each user partition's coordinator partition and reflect every committed record.
- The delegate splits find-batches by coordinator partition, sends the full `fetchMaxBytes` budget with each part, merges the results in request order, and trims them to that budget.
- Brokers absorb most fetch-side lookups in the existing batch coordinate cache. A cache miss goes to the leader.
- Follower reads are not part of this effort.

Capacity and isolation

- Each coordinator partition's leader serves every commit part, intent, release report, cache-miss read, and maintenance job for its user partitions and owned objects. SQLite shares that broker's heap, page cache, disk, and garbage collector with its own partitions. The severity of this risk is open.
- Splitting reduces work per batch, not the number of commit calls, because an object usually holds batches for most coordinator partitions. A produce waits for the intent and then for its slowest part.
- Benchmarks compare this backend with the Postgres backend on `FileCommitTime`, `FileCommitWaitTime`, `FindBatchesTime`, and `FileCommitErrorRate`, at `N=1` and at `N=8`. They cover commit throughput with `flush.messages=1`, partition count, intent cost, partial failure, read-heavy load, a cache storm after a consumer restart, co-tenancy with the leader's own partitions, state size, failover, and snapshot transfers during load. [design.md](design.md#capacity-and-isolation) lists them.
- The benchmark workloads, scales, and pass criteria are not yet defined. The results can require a separate SQLite disk, a `findBatches` result cap, a higher concurrent-transfer default, a different default partition count, a lower default maximum commit age, or a return to Kafka's default flush with the correlated power-loss exposure accepted.

Metrics

- Unknown-outcome parts, open owner entries with the age of the oldest, fences by answer, tombstones and fences held, parts rejected by the age check, and mapping mismatches. [design.md](design.md#metrics) describes each one.

Leader change

- Each coordinator partition changes leader on its own, and a change stalls only that coordinator partition's parts, intents, reads, and owned objects.
- A new owner leader restarts its fence timers from the intents' append times, and a new participant leader resends the releases that have no owner-accepted record.
- A new leader starts with an empty overlay. It rejects requests with a retriable error until its high watermark reaches the log end offset it saw at election.
- A leader change may truncate records that were never replicated. A replica discards fetched records above its applied offset that the new leader does not have. Because the applied offset never exceeds the high watermark, no applied state is affected.
- Records not committed under the old leader are discarded by the log. Their requests were never acknowledged, so the retry rule covers them.
- The delegate retries the part against the new leader after `NOT_COORDINATOR` or a retriable error, with the same object key and first-send time, until the commit deadline.

Lifecycle

- The controller never calls the coordinator. Diskless topic creation and deletion commit to cluster metadata only. Each coordinator partition's leader learns the metadata image from `BrokerMetadataPublisher.onMetadataUpdate`, the same path the group and share coordinators use. On election and on each metadata update it diffs the user partitions in its SQLite against the diskless user partitions in the image that map to it. It appends a partition-created record for each one the image has and SQLite does not, and a topic-deleted record for each topic ID SQLite has that the image does not. The diff is by topic ID, and a delta's deleted-topic set is not sufficient, because a snapshot load does not emit a delete for a topic the process never held. A produce to an unreconciled partition gets a retriable error until reconciliation finishes.
- The diff counts pending overlay entries as well as SQLite rows, so a record is not appended twice. A partition-created record for an existing row, or a topic-deleted record for a topic already marked deleted, applies as a no-op, and the applied offset still advances.
- The admin client stops retrying a delete once the topic leaves the metadata log. Reconciliation is the retry: a topic ID still in SQLite and absent from the image is appended again on the next metadata update and on the next election.
- The leader of a switched partition calls `initDisklessLog` after the controller has committed the classic-to-diskless seal. `ControllerApis.handleInitDisklessLogRequest` writes that seal into cluster metadata. The control plane call happens later, from `InitDisklessLogManager` on the partition leader, through `AwaitingMetadata.sendBatch`.
- That same partition leader calls `repairDisklessLog` from `ReplicaManager`. `KafkaApis` handles the repair request and invokes `ReplicaManager`, which calls the control plane only while this broker leads the partition.
- Each call appends its record and replies after the high watermark passes it. A user partition's creates, deletes, initializations, and repairs go to the same coordinator partition as its commits, so they are ordered with every commit to that user partition. Ordering across user partitions is not guaranteed.

Maintenance

- Retention enforcement and topic purge run on each coordinator partition's leader, for its own user partitions. Each run appends its outcome records to the log.
- Topic purge runs in capped passes. Each pass appends one deleted-log-purged record per partition it touched, with an optional boundary offset and a flag for whether the partition is now empty, in the same log batch. Applying the record removes the partition's batches below the boundary, or all of them without one, and drops the partition's log and producer state rows when the flag is set. Objects whose last local batch the pass removed become released and are reported to their owners.
- File deletion marking and object deletion run on owner leaders. Object deletion waits for the existing deletion grace period.
- Every leader expires its tombstones and fences through keys-expired records.

Consolidation and cross-tier offsets

- The broker that holds a partition's local log decides the safe prune offset, from its remote-storage high offset and the classic-to-diskless seal.
- The broker sends the prune or cross-tier start request to the leader. The leader appends the outcome record.
- The broker advances its own prune floor only from the leader's response.
- The cross-tier start reporter and the delete-records path follow the same pattern.
- The leader never reads a partition log.

Rollout

- A new cluster finalizes the coordinator feature at format time, with `kafka-storage format --feature`. The coordinator is active from the first startup. There is no earlier phase on another backend.
- The controller creates `__diskless_metadata` with `diskless.metadata.topic.num.partitions` partitions. Changing that config afterward has no effect, and the controller rejects `CreatePartitions` for the topic.
- The controller rejects registration from a broker that does not support the finalized feature.
- `inkless.control.plane.class` must name the new class. If it names the in-memory or Postgres class while the feature is finalized, the broker refuses to start.
- Diskless topics still need the existing Inkless storage configuration and `diskless.enable=true`.
- There is no rollback onto Postgres. Downgrading the feature would orphan every file and batch recorded in `__diskless_metadata`, and no migration exists. Clusters already on Postgres are never formatted with the feature.

Out of scope for implementation

- Changing the partition count of `__diskless_metadata` after creation.
- Moving an object's owner to another coordinator partition after the commit.
- Orphan-object scanning, including a scanner that deletes objects. A report-only scanner is future work.
- Produce gateways and preferred produce brokers.
- Moving a user partition between coordinators.
- Transaction state, which stays with the group and transaction coordinators.
- The KIP's public APIs, keys 95 to 101.
- Follower reads of any kind.
- Migrating existing Postgres-backed clusters.
- A Raft quorum for metadata, on either the cluster quorum or a separate one.
- Tiered-storage offload, which the KIP leaves to a separate KIP.

## Testing Decisions

Good tests check behavior through a public seam: the control plane interface, a broker's observable behavior, or a client's observable behavior. They do not check internal structure such as record layout or the order of internal calls.

Three seams are used. Each one is an existing seam unless noted.

1. The control plane interface. The shared behavioral suite runs unchanged against the new backend, in a single-broker setup, at `N=1` and at `N=8`. One broker leads every coordinator partition, so the second run covers request splitting, result merging, intents, and releases. The suite covers commits and fetches, topic and partition creation, deletion and purge, delete records, retention, file marking and deletion, producer idempotence, diskless log initialization and repair, cross-tier offsets, and consolidation pruning.
2. The multi-broker cluster test harness. Tests start a real cluster with real replica fetching and drive it through produce, fetch, admin, and controller operations. The cluster tests cover:
   - Leader failover in the middle of a commit, followed by the retry path. The client sees one successful produce, with no duplicate records.
   - A leader change that truncates unreplicated records. The follower's state matches the new leader's state, and no applied record was undone.
   - A retry that arrives while its first attempt is pending on the leader. The client sees one record and one set of offsets, for idempotent and non-idempotent producers.
   - A commit whose parts land on several coordinator partitions, where one part fails. The committed partitions return offsets, the failed ones get `KAFKA_STORAGE_ERROR`, and the object is deleted only after every participant is released.
   - A preferred owner that is down. The broker falls back to the next participant, and only the down coordinator partition's user partitions fail.
   - An intent stored at the preferred owner whose reply is lost. The stale owner drops its entry after a fence answer names the real owner, and it never deletes the object.
   - A broker that stops after the intent and before the upload, and one that stops after the upload and before any part commits. The owner fences every participant once the intent passes the maximum commit age, then deletes the object.
   - A broker that stops after some parts commit. The owner fences the participant that never committed, and it deletes the object only after the committed participants release it.
   - An object whose batches expire on one coordinator partition while another still holds batches. The object stays until both are released.
   - A part whose first-send time is older than the maximum commit age. The leader rejects it.
   - A release within the maximum commit age of its commit. The participant keeps a tombstone, a delayed retry of the commit is rejected, and the tombstone expires after twice the maximum commit age.
   - An owner leader change with open entries. The new leader restarts its fence timers, and the participants resend their unaccepted releases.
   - A `CreatePartitions` request for `__diskless_metadata`. The controller rejects it.
   - A part that names a user partition the receiving coordinator partition doesn't own. The leader rejects it with `INVALID_REQUEST`.
   - A replica that catches up from a snapshot after the log start has moved. It serves the same reads as the leader afterward.
   - A newly elected leader whose database is missing. It rebuilds from a follower's snapshot, and it serves no request before the rebuild finishes.
   - A coordinator partition where no replica holds a snapshot covering the log start. Its user partitions stay unavailable for diskless writes, and no partial state is served.
   - A leader whose older snapshot is ahead of every other in-sync replica's reported snapshot. The log start stays put, and produce and fetch continue. Once another in-sync replica reports a snapshot at that offset, the log start advances. After the leader stops, that replica's snapshot still covers the log start.
   - A single-replica topic. The leader advances the log start from its own older snapshot.
   - A follower that reports a snapshot and then leaves the in-sync replica set. The log start does not advance on its report until it rejoins and reports again.
   - A snapshot source that rotates or restarts during a transfer. The receiver never loads a file mixed from two snapshots.
   - A rebuild while an in-sync follower holds a covering snapshot. The follower serves the transfer, not the leader.
   - A replica that loses its database before the log start has first moved. It replays from offset 0.
   - A topic deleted and purged. Every replica drops the same batches and rows as the leader, and the owners delete the emptied objects.
   - A cluster formatted with the feature, where a commit goes through the coordinator. A broker whose class names another backend refuses to start. A broker that does not support the feature cannot register.
   - A produce that is in flight during a leader change and that fails cleanly or succeeds once.
   - A topic created and a topic deleted while a coordinator leader is down. The leader reconciles on return. A produce before reconciliation gets a retriable error.
   - A consolidated topic whose prune floor moves only after the leader confirms it.
3. A new injection hook on the client side of the private RPC, which is the one new seam. It lets a test drop a reply after the leader has appended a record, drop an intent reply, delay a request during the leader's catch-up, and stop a broker between the intent, the upload, and the commit. These cases are otherwise hard to reproduce on a real cluster. The hook changes nothing about the wire protocol.

The mapping function and the preferred-owner choice get a unit test with fixed expected values, so a change to either fails the build. The same test checks that the slot mapping equals `Utils.toPositive(Utils.murmur2(logKey)) % N` for every allowed `N`, and that the config rejects an `N` that isn't a power of two.

Snapshot writing, rotation, loading, and chunk serving get unit tests on a temporary directory, with a fault hook at each step of rotation and transfer. They cover a tag read while applies run during the backup, a crash at each rotation step, a transfer whose source rotates or restarts before the receiver resumes, and a checksum mismatch on load and on serve. This is the second deliberate exception to the public-seam rule, for the same reason as the apply rules: no public seam reproduces these cases deterministically. The cluster tests still cover snapshot behavior end to end.

Apply rules for file records, owner entries, releases, tombstones, and expiry get unit tests at the replica state layer, with records appended directly to a test log. No public call can produce a duplicate or conflicting record, or control the append times the tombstone rule compares, so this is a deliberate internal exception to the public-seam rule.

Prior art:

- The shared behavioral suite, with its in-memory and Postgres subclasses. The new backend gets a subclass in the same way.
- The Postgres test helpers that run migrations and clean up containers, for any test that needs a database.
- The control plane outage cluster test, for the availability gate and runtime reconfiguration.
- The consolidated diskless topics cluster test, for prune floors and cross-tier offsets.
- The untiered-to-consolidated migration test, for classic-to-diskless switch behavior.
- The diskless topic delete test, for topic deletion and file cleanup.

Optional, not required for done: a ducktape system test of the full deployment, following the existing consolidation pipeline test.

## Out of Scope

- Changing the partition count of `__diskless_metadata` after creation.
- Moving an object's owner to another coordinator partition after the commit.
- Orphan-object scanning. A report-only scanner is future work.
- Produce gateways and preferred produce brokers.
- Moving a user partition between coordinators.
- Transaction state.
- The KIP's public APIs, keys 95 to 101.
- Follower reads.
- Migrating existing Postgres-backed clusters.
- A Raft quorum for metadata.
- Tiered-storage offload.

## Further Notes

- The KIP text does not define snapshot distribution. This spec defines it, and the private fetch-snapshot request is the part most worth a reviewer's attention, together with the owner rules in File references.
- Open items for the implementer, none of which changes the design: the private request names and API key numbers, the error code for a busy snapshot source and for a part rejected by the age check, the feature flag name and its metadata version, the deletion grace period, the SQLite driver choice and license, the commit deadline default, how often leaders append keys-expired records, how a leader that cannot rebuild its state gives up leadership of its coordinator partition, the cap on how far the overlay may run ahead of the high watermark, and the append linger default.
- A late commit part can still commit for up to one maximum commit age after the producer got `KAFKA_STORAGE_ERROR`. That is the same unknown outcome as an `acks=all` timeout on a classic topic.
- The age check and the expiry of tombstones and fences assume broker clocks differ by less than one maximum commit age. The age-check rejection metric shows skew before it reaches that size.
- An object leaks only when storage completes an upload after the owner deleted it, or when a bug loses a record. That leak is accepted over a scanner that deletes objects no coordinator partition knows, which fails toward data loss. [deletion-comparison.md](deletion-comparison.md) has the comparison.
- Earlier wayfinding notes said the Inkless private keys are already gated by a version check. That was not verified, and this spec does not rely on it. The coordinator feature is finalized at format time, and a conflicting class fails startup. There is no rollback to Postgres.
- The full design rationale, the rejected alternatives, and the decision log are in [design.md](design.md).
- Growing the coordinator, merging WAL objects into extents, tiered metadata in the coordinator, and partitionless key ranges are future work. [design.md](design.md#future-work) describes each one and the decisions in this spec it depends on.
