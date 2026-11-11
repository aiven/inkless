# Consequences for the cross-cutting sections

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: 12

## Question

What do v1's cross-cutting sections become in v2: the metrics, the failure-mode table, the tests, and the user stories?

v1's [Metrics](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#metrics), [Failure modes](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#failure-modes), and [Testing](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#testing) sections describe the owner and intent model, with age checks, fences, and tombstones. The user stories and testing decisions in the v1 [spec](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/spec.md) describe the same model. Each closed ticket left candidates and rows for this pass, and the rewrite needs one decided set instead of a list of proposals per ticket.

Settle these points:

- The metric set. Choose final names and scopes from the candidates in [Time bounds in the chosen model](03-time-bounds.md#metrics), [Read consistency after a leader change](07-read-consistency.md#failure-mode-and-metric), [Record compatibility across broker versions](08-record-compatibility.md#failure-mode-and-metric), and [Guard for last-known-leader election on `__diskless_metadata`](10-last-known-leader-guard.md#accepted-risks). Decide which v1 metrics leave with the owner model, such as the age-check rejections, open owner entries, and fences, and whether the coordinator runtime registers `CoordinatorRuntimeMetricsImpl` or needs its own metrics group.
- One failure-mode table that replaces v1's. Merge every ticket's failure-mode rows and accepted risks, including [Flush policy under Kafka parity](04-flush-policy.md) and the accepted consequences in [Admin protection for `__diskless_metadata`](06-admin-protection.md#accepted-consequences). Add the retention and `LogAppendTime` accepted-risk row with its NTP procedure, and the reader-availability row for grace-period skew, from ticket 03. Add the accepted-risk rows from the map's [Out of scope](../map.md#out-of-scope): losing every replica of a coordinator partition with only local snapshots, with its blast radius of 1/N of diskless partitions and their orphaned WAL objects, and a deterministic apply bug that corrupts every replica alike. Give each accepted risk its metric or operator procedure, and drop or rewrite each v1 row that the lease model invalidates, such as broker clocks beyond the maximum commit age.
- The tests. Place the tests from tickets 03, 06, 07, 08, and 10 in v1's three layers, and say which v1 tests leave with the owner model.
- The user stories. Decide what changes in the v1 spec's user stories: the rewrites of stories 24, 25, and 45 and the new barrier story from ticket 07, stories that describe intents, owners, fences, tombstones, or the age check, and new stories that tickets 02, 04, 06, 08, and 10 imply.
- The section changes that ticket 08 lists for Rollout, Log records, and Snapshots, and the Topology, Leader change, and Rollout changes from tickets 04, 06, and 10. Confirm them as the rewrite's list, or adjust them where the merged table or stories change them.
- The changes that the research in [Check against the Postgres performance bar](12-postgres-performance-bar.md) surfaced:
  - Ticket 07 passes its 500 ms barrier deadline as the `CoordinatorRuntime` write-operation timeout, but the public `scheduleWriteOperation` applies one runtime-wide `writeTimeout` to every write, so every commit part would time out at 500 ms. Decide how the barrier gets its own deadline.
  - The barrier wait holds a fetch metadata thread, because `findBatches` is synchronous, and the barrier must never run from the delayed-fetch `tryComplete` probe.
  - A fetch above the high watermark can wait the long poll plus 500 ms, and the one-barrier-per-linger-interval bound assumes a fixed linger. Decide whether ticket 07's wording and story change.
  - Ticket 10's registration guard must iterate only the partitions of `__diskless_metadata`.

## Resolution

Resolved in a grilling session on 2026-11-10. The rewrite takes one decided set for the cross-cutting sections: five corrections to the barrier from [Read consistency after a leader change](07-read-consistency.md), a failure-mode section of two tables, a metric set in two metrics groups, four test layers with seam tests, and the story and section changes in this resolution. The facts behind the barrier corrections, with citations, are in [postgres-performance-bar.md](../research/postgres-performance-bar.md). No answer opens a new decision.

### Barrier corrections

1. Deadline. Each parked `findBatches` request gets its own 500 ms deadline, measured from its arrival at the leader. The coordinator service schedules it on the runtime's `Timer`, so `MockTimer` drives it in tests, and answers `OFFSET_NOT_AVAILABLE` when it fires. The barrier write keeps the runtime's write timeout. That timeout equals the commit deadline, so a commit part never waits on the server longer than the broker waits for it. Upstream `CoordinatorRuntime` doesn't change.
   - Rejected: a public `scheduleWriteOperation` overload that takes a `Duration`. Upstream has none. `OperationTimeout` starts only after the event runs and appends, so queue wait comes on top of the 500 ms, and a request that joins a shared barrier late gets less than 500 ms. Fixing both would change what the runtime's write timeout measures and need an empty write per request. Permitting the upstream edit doesn't change the choice.
2. Thread hold. `ControlPlane.findBatches` stays synchronous. A parked part holds a fetch metadata thread on the calling broker for at most 500 ms, and the delegate's merge in request order delays the whole fetch response. A current leader commits a barrier in one replication round trip, so only a deposed leader holds the full 500 ms. In a deposed window, the pool of 8 limits that broker to about 16 diskless fetches per second until it learns of the new leader. Postgres holds the same thread for up to its 5-second socket timeout per attempt when the database is unreachable. Reads states this as a deposed-window cost.
   - Alternative for the design review group: an asynchronous `findBatches` that returns a `CompletableFuture` removes the hold. It changes `ControlPlane` for every backend, where Postgres and in-memory would wrap their calls in an executor, and it changes `Reader`'s `FindBatchesJob` chain. If the review group doesn't prefer the synchronous call, this is the replacement.
   - Rejected: a shorter deadline. A healthy leader under fsync load would fail more genuine seeks with `OFFSET_NOT_AVAILABLE`, and the hold only shrinks.
3. Probe. The delayed-fetch probe never runs the barrier. `ControlPlane` gets a default method for the probe, for example `findBatchesNoWait`, that calls `findBatches`, so Postgres and in-memory don't change. The coordinator backend sets a no-wait field in the first version of the private find-batches request. It's an ordinary field, because [Record compatibility across broker versions](08-record-compatibility.md#tagged-fields-and-inkless-extensions) rules out tagged fields upstream. With the field set, the leader answers an offset above the high watermark at once with `OFFSET_NOT_AVAILABLE` and appends no barrier. `DelayedFetch.tryCompleteDiskless` completes the fetch on any error, and `onComplete` then runs the real call through `Reader`. Below the high watermark the probe gets real answers, so a fetch with the cache off still finishes early when data arrives.
   - Rejected: a backend capability that turns the probe into a cache miss. A fetch with the cache off would always wait out its long poll, even for data that already arrived.
4. Latency wording. Ticket 07's "a fetch on this path waits about as long as a long poll" becomes: "The barrier adds one barrier commit to the answer, after any long-poll wait the Postgres backend also pays. A current leader commits it in one replication round trip. A deposed leader answers `OFFSET_NOT_AVAILABLE` after 500 ms, so a fetch that misses the local cache can take up to its long poll plus 500 ms." The 500 ms stays a constant, because it's well above a healthy barrier commit, which is one round trip plus the leader and follower fsyncs, and far below the 30-second request timeout. It no longer ties to `fetch.max.wait.ms`. No story changes.
5. Barrier rate. Ticket 07's "one append per linger interval per coordinator partition" becomes: "At most one barrier per flush of the coordinator partition's batch. When commits are flowing, the barrier joins an open batch and adds no append." The worst case is an idle partition that appends once per fsync, which is the rate a busy partition's commits already drive. The out-of-range outcome of `BarrierRate` shows such a client. The append linger default stays an open item.
   - Rejected: a fixed barrier window, which adds a second timer next to the linger, and fixing the runtime to a fixed linger, which gives up adaptive linger's group commit to bound a misbehaving client.

Two more research findings change wording only:

- The storage change guard from [Guard for last-known-leader election on `__diskless_metadata`](10-last-known-leader-guard.md) looks up `__diskless_metadata` in `topicsByName` and walks only that topic's `parts` map. That costs `O(N × RF)`, once per broker process start, on the controller event thread. It never scans every topic, because `lastKnownElr` has no index. No test checks the cost.
- [Flush policy under Kafka parity](04-flush-policy.md)'s comparison with Postgres holds for the flush count on the primary. The follower fsync is a second sequential flush on every commit, which Postgres matches only with a synchronous standby. The design calls it the price of Kafka parity, not unnecessary blocking. The latency stays a benchmark question.

### Failure modes

The section holds two tables:

- "Failure modes" has the columns Failure, Behavior, and Test. Its rows sit under five subheadings: commits and leases, deletion, leader change and reads, replicas and snapshots, and controller and admin. Ticket 10's "Outcome" column becomes "Behavior".
- "Accepted risks" has the columns Risk, Why it's accepted, and Signal or procedure. "Why it's accepted" replaces "Why it's Kafka parity", because two rows aren't data loss.

Test rule: every row that closes a review gap names a test. Any other row names one or says why it has none.

#### v1's rows

v1's table has 35 rows:

- Kept as written (19): a retry while the first attempt is pending, the leader losing the partition, a coordinator leader down at topic create or delete, an append failing before the high watermark, a leader change truncating unreplicated records, the 11 snapshot and rebuild rows, a single-replica topic, one replica down, and power loss on a replica. The power-loss row takes ticket 04's reason, and [From v2's closed tickets](#from-v2s-closed-tickets) widens it.
- Rewritten for the lease model (11):
  - Leader crashes during a commit: the retry goes to the new leader, and the broker row's `(epoch, sequence)` returns the stored result or commits as new. It replaces the object-key rule.
  - A part misses the deadline and commits later: `KAFKA_STORAGE_ERROR`. The late record still makes its batches visible, unless the broker's next commit, a fence, or an epoch retirement raised the floor first. The earlier effort's [Duplicate window](../../diskless-metadata-coordinator/issues/04-broker-sequence-model.md#duplicate-window) replaces the predecessor fence.
  - A coordinator partition is unavailable or slow: as v1, plus "if it's a broker's tracking partition, the broker renews its lease at the next partition".
  - The ISR drops below `min.insync.replicas`: as v1, with lease fallback in place of intent fallback. v1's two-replicas-down row merges into it.
  - Some parts succeed and others fail: as v1, plus "the tracking partition deletes the object only once every participant is released".
  - A retry of a part with failed or duplicate batches: the broker row's stored result answers, also after a failover and after retention removed the batches.
  - The uploader restarts with parts in flight: stale-epoch rejection. The broker-epoch-retired record ends its leases, the range fences run, and leftover keys are queued.
  - The broker stops before the upload, in place of "after the intent": the epoch end marks the unused range, and the queue deletes a missing key.
  - The broker stops after the upload and before any part commits: the same range holds the key, and the queue deletes it after the grace period.
  - The broker stops after some parts commit: the tracking partition fences silent participants once it and every participant it has heard from are released.
  - The tracking partition's leader, in place of the owner, is unreachable for a release report.
- Dropped (4): the preferred owner can't store the intent, an intent reply is lost, broker clocks beyond the maximum commit age, and two replicas down. The lease rows replace the first two, the retention and grace-period risks replace the third, and the ISR row absorbs the fourth.
- Moved to Accepted risks (1): storage completes an upload after the broker gave up.

#### New rows for the lease model

These come from the earlier effort's tickets 03 to 06 and from [Broker-side deletion after a partial send](02-delete-after-partial-send.md) and [Time bounds in the chosen model](03-time-bounds.md).

| Group | Failure | Behavior |
| --- | --- | --- |
| Commits and leases | A file closes with no leased sequence | It fails at once with `KAFKA_STORAGE_ERROR` and isn't uploaded. |
| Commits and leases | The tracking partition doesn't answer a renewal | The broker leases at `(t + 1) % N`, and onward. Only the user partitions of the unavailable partition fail. |
| Commits and leases | A lease reply is lost | The retry names the same end and is a no-op. A range stored at a fallback partition is never used, and becomes leftover at that partition's lease end. |
| Commits and leases | The part to the tracking partition fails while other parts commit | Once a later sequence commits there, the tracking partition fences the missed sequence at all `N` and builds the participant set from the referenced answers. |
| Commits and leases | A fence races a part in flight | A part pending before the fence commits, and the fence answers referenced. A part that arrives after it gets stale-sequence, and the producer retries. |
| Commits and leases | A silent broker's lease goes idle | The lease-ended record raises the floor, and range fences go to every partition. Keys that no partition holds are queued. |
| Commits and leases | The idle timer fires as a part arrives | The part to the tracking partition gets stale-sequence. A participant's committed part stays readable and tracked. |
| Commits and leases | A tracking leader's monotonic clock jumps forward | Leases end at the partitions it leads. Files in flight fail once with stale-sequence, and the brokers renew. |
| Commits and leases | A tracking leader pauses between the timer and the append while leadership moves | The old leader's append fails at its old epoch, and the new leader restarts the timer. |
| Commits and leases | Tracking-partition leadership changes faster than the idle timeout | Idle leases don't end. This costs liveness only. |
| Commits and leases | A broker bug sends an unleased sequence or a retry with a different batch count | `INVALID_REQUEST`. |
| Deletion | A partial send followed by `ControlPlaneUnavailableException` | The broker never deletes the object directly. The queue deletes it after the next commit or the lease end, plus the grace period. |
| Deletion | A commit where every batch fails | Its file-committed record marks the object at once. |
| Deletion | Leadership moves between the storage delete and the files-deleted record, or `FileCleaner` deletes from a stale list | The key is deleted at most twice, which is harmless. |
| Deletion | Two clusters share a bucket and prefix | The cluster ID in every key keeps deletes within this cluster. |
| Deletion | An upload stops partway | A partial object is never visible. |

An uploader that pauses past the idle timeout and a cut-off zombie broker fall under the late-upload leak in Accepted risks.

#### From v2's closed tickets

- Leader change and reads:
  - Ticket 07's deposed-leader row, with "`OFFSET_NOT_AVAILABLE` after 500 ms, measured from arrival".
  - New: with the batch coordinate cache off, the delayed-fetch probe meets an offset above the high watermark. The leader answers at once without a barrier, and the fetch's `onComplete` runs it.
  - New: a client keeps fetching past the end. It costs one barrier per flush, none extra under load, and the out-of-range count of `BarrierRate` shows it.
- Replicas and snapshots:
  - Ticket 08's unreadable-record row.
  - New, from ticket 08's schema rules: a rolled-back replica finds a newer SQLite schema. It treats the database as missing and rebuilds. A snapshot fetch above the receiver's schema gets `SNAPSHOT_NOT_FOUND`, and the receiver tries the next replica.
  - Ticket 04 widens v1's power-loss row to "Power loss on one replica or across every replica". `flush.messages=1` keeps every acknowledged record on each ISR member, so no unflushed tail exists to lose.
- Controller and admin:
  - Ticket 10's six rows as written.
  - Ticket 06's offline row, as ticket 10 amended it: once every ELR member and the last known leader have lost their storage, the partition stays offline, and no unclean election is possible.
  - New: an admin request or a client produce would delete, create, extend, or weaken the topic. One row lists the requests and errors from ticket 06's tables.

#### Accepted risks

| Risk | Why it's accepted | Signal or procedure |
| --- | --- | --- |
| Every replica of a coordinator partition is lost | A classic topic loses data the same way. The blast radius is 1/N of diskless partitions, and their WAL objects become orphans. | Procedure: the deployment docs require a replication factor of 3 across racks or zones. Signal: `OfflinePartitionsCount`. |
| A deterministic apply bug corrupts every replica alike | Kafka's coordinators share this limitation. The replay checker is out of scope. | Signal: a sustained `PartitionStorageErrorRate` on a diskless partition, outside a deposed-leader window or a grace-period race, means coordinator state points into a deleted object, whatever the cause. It catches the harmful effect, not the bug. Prevention: the shared suite, the golden-records test, and ticket 08's apply-stop rule. |
| Retention and `LogAppendTime` under a skewed leader clock | Classic topics have the same exposure. | Procedure: monitor the NTP offset on every broker, and alert well below half the grace period. |
| Grace-period skew beyond half the grace period | It costs reader availability, not data, and the Postgres backend shares it. | The NTP procedure in the preceding row. |
| Storage completes an upload after its key was deleted | It leaks storage, not data. The bounds are one object per attempt and at most `K` files for a cut-off zombie. The grace period covers uploads that land within a minute of the mark. | Signal: `FileUploadErrorRate`, an upper bound on objects that may have leaked. The report-only scanner stays future work. |
| A non-idempotent producer that retries through another broker sees a late copy after the retried one | A classic `acks=all` timeout and the Postgres backend have the same window. | Signal: `event-purgatory-time-ms` bounds the window. |
| Log recovery truncates fsynced records after a read error or bit rot, under an unchanged directory ID | A classic topic loses data the same way. | Procedure: alert on log recovery's invalid-message truncation warning for `__diskless_metadata`, and stop the broker for inspection instead of letting it rejoin. |
| A device acknowledges an fsync without persisting it | A classic topic loses data the same way. | Procedure: the deployment docs require storage that honors fsync, with no volatile write cache that lacks power-loss protection. |
| An operator restores a log directory, `meta.properties` included, from an older copy | A classic topic loses data the same way. | Procedure: never restore a broker's log directory from a copy. Reformat it and let the broker re-replicate. |
| A stale leader-epoch entry over-truncates an ELR member, and the leader fails before that member fetches again | A classic topic loses data the same way. | Signal: `UncleanLeaderElectionsPerSec` and `ElectionFromEligibleLeaderReplicasPerSec`. No signal catches the over-truncation itself. |

The barrier's thread hold isn't a risk to data, so Reads states it as a cost, with the asynchronous `findBatches` as the alternative.

### Metrics

The coordinator registers two metrics groups, each following its neighbors:

- `DisklessMetadataCoordinatorRuntimeMetrics extends CoordinatorRuntimeMetricsImpl`, with the group `diskless-metadata-coordinator-metrics`. `BrokerServer` registers it as it registers the group and share coordinators' runtime metrics. It brings `event-queue-time-ms`, `event-purgatory-time-ms`, `batch-flush-time-ms`, and the other runtime metrics unchanged.
- One Yammer `KafkaMetricsGroup` named after the backend class, with PascalCase names, as `PostgresControlPlaneMetrics` and `FileCommitterMetrics` do. A metric scoped to a coordinator partition carries a `partition` tag. A per-broker metric carries none.

The backend's metrics, where "leader" is the coordinator partition's leader and "broker" is the uploading or calling broker:

| Name | Scope | Shows |
| --- | --- | --- |
| `UnknownOutcomePartRate` | Broker, `partition` tag | Parts that reached the commit deadline. Kept from v1. |
| `StaleEpochRejectRate` | Leader | Requests sent before a restart. Rewritten from v1's broker-epoch metric, without intents. |
| `StaleSequenceRejectRate` | Leader | A spike right after an idle lease end at an active broker means the lease ended without cause. |
| `PartitionMappingMismatchRate` | Leader | Kept from v1. |
| `LeaseRenewalRate` | Broker | Lease renewals. |
| `LeaseFallbackRate` | Broker | Renewals at a fallback partition. |
| `FileNoLeaseErrorRate` | Broker | Files failed with no leased sequence. |
| `IdleLeaseEndRate` | Leader of the tracking partition | A burst across many brokers points to a clock jump. |
| `FenceRate`, tagged `type` (sequence, missed, or range) and `answer` (referenced or released) | Leader of the tracking partition | Rewritten from v1's fences by answer. |
| `DeletionQueueSize`, `DeletionQueueOldestAgeMs` | Leader of the tracking partition | A growing queue with a young oldest row means a clock running behind. An old oldest row means stuck deletes. |
| `BarrierRate`, tagged `outcome` (out of range, in range, or timed out) | Leader | From ticket 07. A timed-out rate means the broker answers as a deposed leader. |
| `ApplyStoppedPartitionCount` | Broker, gauge | From ticket 08. |

v1's open owner entries, tombstones and fences held, age-check rejections, and predecessor fences leave with the owner model.

The design also cites these existing metrics: `FileCommitTime`, `FileCommitErrorRate`, `FileUploadErrorRate`, `FindBatchesTime`, `PartitionStorageErrorRate`, `OfflinePartitionsCount`, `UncleanLeaderElectionsPerSec`, and `ElectionFromEligibleLeaderReplicasPerSec`.

### Tests

#### Layers

1. The shared `AbstractControlPlaneTest`, at `N=1` and `N=8`: ticket 02's partial send and replaced delegate, the marking rule for each of the five batch-removing records, and the lease-model rewrites of v1's commit and retry cases.
2. Coordinator backend tests, a new layer for cases that one clock or one state can't express: ticket 03's skewed clocks, uploader pause, and stale `FileCleaner` list. Ticket 07's stale-read rule. Ticket 08's apply stop, write gate, SQLite schema, and golden records.
3. `KafkaClusterTestKit`: ticket 06's admin cluster test, ticket 07's deposed leader and seek past the end, ticket 08's upgrade and rollback, ticket 10's unclean restart of every replica, and v1's cases as [v1's tests](#v1s-tests) lists them.
4. Fault injection: v1's list, with the object-key retry rule as the `(epoch, sequence)` rule, a lost intent reply as a lost lease reply, and the broker stop between lease, upload, and commit.

#### Seam tests

- On the shard, with `MockPartitionWriter` and `MockCoordinatorTimer`:
  - Ticket 07's barrier cases.
  - Ticket 03's idle timer and leader pause.
  - A request queued behind slow events still gets 500 ms from its arrival. Two requests that share a barrier each get their own deadline. A commit part that waits longer than 500 ms doesn't time out.
  - Under adaptive linger, a hot loop above the high watermark appends at most one barrier per flush.
  - The acceptance rules for a file with no leased sequence, a fence racing a part, and a broker bug's `INVALID_REQUEST`.
- `ReplicaManagerTest` or `DelayedFetchTest`: with the cache off, a probe above the high watermark returns at once and appends no barrier.
- Next to each guard: the controller and broker tests of tickets 06 and 10, ticket 08's `FeatureControlManagerTest`, and `LogManagerTest`.
- The shared key-name function: the cluster ID keeps two clusters' keys apart.
- v1's mapping test pins the tracking partition, `toPositive(murmur2(brokerId)) % N`, in place of the preferred-owner choice.
- v1's snapshot unit tests stay, with the schema version in the file name.
- v1's apply-rule seam covers file records, broker rows, leases, fences, releases, and the deletion queue. Its reason drops "control the append times that the tombstone rule compares", and keeps "no public call can produce a duplicate or conflicting record".

#### v1's tests

design.md lists 30 cluster cases:

- Kept as written (16): truncation, a retry while pending, an object expiring on one partition while another holds batches, one coordinator partition offline, a retried part with mixed results after a failover, the mapping mismatch, the 8 snapshot cases, a topic created while the leader is down, and the cluster formatted with the feature, which adds ticket 08's two feature names.
- Rewritten (10):
  - Failover in the middle of a commit: the retry resolves through `(epoch, sequence)`.
  - A part fails on one of several coordinator partitions: the tracking partition deletes the object once every participant is released.
  - The preferred owner is down: the tracking partition is down, the broker leases at the fallback partition, and only that partition's user partitions fail.
  - The broker stops before the upload, and after the upload before any commit: the epoch end marks the unused range, and the queue deletes the missing key and the uploaded key after the grace period.
  - The broker stops after some parts commit: the tracking partition fences the silent participant.
  - A part reaches its deadline, then the next part: the next commit raises the floor, a late request gets stale-sequence, and the later batches commit.
  - The ISR drops below `min.insync.replicas`: lease fallback, not intent fallback.
  - A broker restarts with a part in flight: stale-epoch, plus the epoch-retired record that ends its leases.
  - An owner leader change with open entries: a tracking leader change restarts the idle timers, and participants resend releases that weren't accepted.
  - A topic deleted and purged: the tracking partitions delete the emptied objects.
- Dropped (4): a lost intent reply, which moves to fault injection as a lost lease reply. A part older than the maximum commit age. A release within the maximum commit age that keeps a tombstone. `CreatePartitions`, which joins ticket 06's admin cluster test.
- Added (2): a silent broker's idle lease ends, and its leftover keys are deleted. A missed sequence, where the part to the tracking partition fails while others commit, is fenced at all `N`, and the object stays until its participants release it.

The spec's Testing Decisions lists three more cluster cases: no replica holds a covering snapshot, a produce in flight during a leader change, and a consolidated prune floor. They stay.

#### Test cells

- The power-loss row names ticket 10's cluster test that kills every replica uncleanly and restarts them with intact disks.
- Leadership changing faster than the idle timeout says "None: liveness only". A partial upload says "None: storage property".
- v1's busy snapshot sources, two replicas rebuilding at once, and a leader unreachable for a snapshot report say "None, unchanged from v1". They aren't review gaps, and v1's snapshot fault hooks are where a test can go later.

### User stories

#### Changed

- Stories 24, 25, and 45, and the new barrier story next to story 46, as [Read consistency after a leader change](07-read-consistency.md#user-stories) decided.
- Story 3: the minimum ISR comes from `diskless.metadata.topic.min.insync.replicas`, defaults to `min(2, replication factor)`, and is fixed at creation.
- Story 43: "As a tracking partition, I want file deletion marking to run only at the uploading broker's tracking partition, so that no two coordinator partitions mark the same object."
- Story 44: "As an owner" becomes "As a tracking partition".
- Story 55: "open owner entries" becomes "the deletion queue, lease ends, barrier outcomes, and stopped applies".
- Story 58: "As a broker, I want an object whose commit may have reached a leader deleted only through its tracking partition's queue, never by me directly, so that a late part never commits into a deleted object." This covers ticket 02.
- Story 63 widens to ticket 06's fixed configs: `flush.messages=1`, `unclean.leader.election.enable=false`, `retention.ms` and `retention.bytes` at `-1`, `cleanup.policy=delete`, and the fixed `min.insync.replicas`.
- Story 76: "As an operator, I want every object key leased at its broker's tracking partition before the upload, so that a crash after the upload never leaves an object no one will delete, and no round trip precedes the upload."
- Story 77: owner fallback becomes lease fallback to the next partition.
- Story 80: the preferred-owner choice becomes the tracking partition.
- Story 86: "commit parts and intents" becomes "commit parts and lease requests".
- Story 87: "an intent fails fast and can fall back" becomes "a lease renewal can fall back".
- Story 88: "fenced before my broker's next commit" becomes "unable to commit once my broker's next commit to that coordinator partition applies".
- Stories 78, the age check, and 79, tombstone and fence expiry, leave.

Every other story stays as written. Story 23 still holds, because the broker row deduplicates by `(epoch, sequence)` in place of the object key.

#### New

1. "As an operator, I want `flush.messages=1` fixed on `__diskless_metadata`, with no weaker mode, so that a correlated power loss never brings back references to objects that were already deleted."
2. "As an operator, I want `DeleteTopics`, client `CreateTopics`, `CreatePartitions`, unclean `ElectLeaders`, client produce, and client `DeleteRecords` rejected for `__diskless_metadata` with no bypass, so that no admin action destroys or bypasses coordinator state." Story 73 stays for the partition count.
3. "As an operator, I want a replica that meets a record or snapshot it can't read to stop applying that coordinator partition instead of skipping it, so that replicas never silently diverge."
4. "As an operator, I want the controller to refuse every downgrade of the coordinator features, so that no broker meets records newer than it can read."
5. "As an operator, I want a broker rolled back before the feature level rises to rebuild from a snapshot in a schema it reads, so that an aborted upgrade costs a rebuild, not correctness."
6. "As an operator, I want a broker that returns with a wiped, replaced, or reformatted disk never elected leader of a coordinator partition, so that surviving replicas never truncate to an empty log."
7. "As an operator, I want a coordinator partition to recover on its own after every replica restarts uncleanly with intact disks, so that a correlated power loss needs no manual election."
8. "As an operator, I want a silent broker's lease to end after `diskless.metadata.lease.idle.timeout.ms`, so that its unused keys are deleted without a restart."

The thread hold and the probe rule add no story. Both are broker internals, and the barrier story covers what a client sees.

### Section changes

The tickets' own lists stand. This is the consolidated list for the rewrite:

- Topology: ticket 04's lease-model rationale for `flush.messages=1`. Ticket 10's fixed replication factor, `diskless.metadata.topic.min.insync.replicas`, and directory fsync. The Postgres comparison that counts the follower fsync as a second flush.
- Coordinator partitions and Rollout: ticket 06's guard set replaces the single `CreatePartitions` rejection. Rollout gains ticket 08's upgrade procedure, downgrade refusal, and Inkless feature, and ticket 10's controller config.
- Log records: a second log key encoding is a new record version, not a tagged field, as ticket 08 decided. The same holds for spec line 160 and the partitionless key ranges entry in Future work.
- Snapshots: ticket 08's schema version, forward-only migration, and schema bound on the fetch-snapshot request.
- Reads: ticket 07's barrier rule with [Barrier corrections](#barrier-corrections) applied, including the thread hold as a stated cost and the asynchronous `findBatches` as the alternative.
- Leader change: ticket 10's storage change guard and ELR rule, with the guard's scope.
- Private RPCs: the find-batches request has the no-wait field. A leader epoch added later is a new request version.
- Metrics, Failure modes, and Testing: replaced by this resolution.
- Capacity and isolation, and Open items: ticket 04 removes the flush fallback sentences. The commit deadline default also sets the runtime's write timeout. The append linger default stays open.
- Tombstones and fences, and the age check, leave, as ticket 03 decided.
- `INKLESS_OWNERSHIP`: the entries that tickets 06, 08, and 10 list. The probe rule touches `DelayedFetch.scala` and `ReplicaManager.scala`, which are already `INTERLEAVED` entries, and the deadline adds no upstream file.

### Terms

"Barrier record" and "deposed window" join the terms that earlier tickets left for the rewrite. The proposal glossary is generated from `design.md`, so the rewrite adds them there.
