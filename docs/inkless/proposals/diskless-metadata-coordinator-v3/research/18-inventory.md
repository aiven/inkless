# Inventory for the cross-cutting sections

Input for [Consequences for the cross-cutting sections](../issues/18-cross-cutting-consequences.md). It lists every failure row, accepted risk, metric, test, story, Terms entry, Future work entry, and config that v2 and the closed v3 tickets leave for the rewrite, each with its source. The starting structure is v2's [design.md](../../diskless-metadata-coordinator-v2/design.md) and v2's [Consequences for the cross-cutting sections](../../diskless-metadata-coordinator-v2/issues/11-cross-cutting-consequences.md).

Sources read: the resolution of every closed v3 ticket, with its "Amended by", "Update from", and "Correction from" sections, the reviews in [reviews/](../reviews/), and `docs/inkless/metrics.rst`. Tickets 07, 08, 12, and 20 are research and add facts, not rows. Tickets 09, 10's deferred design, and 19 are future work, so their rows, metrics, tests, and stories stay out of v3. Only 10's six accepted risks enter v3.

Status values: `kept` (v2 as written), `changed` (v2 item that a v3 ticket rewrites), `new`, `removed`, `merged` (folded into another item), and `derived` (no ticket states it, but a decided ticket implies it). Flags in the last column point at [Conflicts and duplicates](#conflicts-and-duplicates).

## Failure rows

v2 groups its rows under five subheadings, with the columns Failure, Behavior, and Test. Group codes: CL commits and leases, DE deletion, LR leader change and reads, RS replicas and snapshots, CA controller and admin, GW gateway routing (no v2 group).

### v2 rows

| ID | Group | Failure | Status | Driving ticket and change | Flag |
| --- | --- | --- | --- | --- | --- |
| F01 | CL | Leader crashes during a commit | kept | | |
| F02 | CL | A retry arrives while the first attempt is pending | kept | | |
| F03 | CL | A part misses the deadline, and its record commits later | changed | 02: the urgent finish report and its exception fence close the window about one commit interval after the deadline | |
| F04 | CL | A coordinator partition is unavailable, slow, or rebuilding | changed | 04: requests without a batch for it aren't delayed, and a stuck leader broker widens it. 22: a tracking partition's renewal walks the ordered list | |
| F05 | CL | ISR below `min.insync.replicas` | changed | 22: "a lease renewal falls back to the next partition" becomes the ordered list | |
| F06 | CL | Some parts succeed and others fail | kept | | |
| F07 | CL | A retry for a part with failed or duplicate batches | kept | | |
| F08 | CL | The uploader restarts with parts in flight | changed | 03: broker-epoch-retired carries `retiredThrough` | |
| F09 | CL | A file closes with no leased sequence | kept | | |
| F10 | CL | The tracking partition doesn't answer a renewal | changed | 02: a failed tracking part starts a renewal. 22: the ordered list replaces `(t + 1) % N` | X16 |
| F11 | CL | A lease reply is lost | changed | 02: the broker releases the unconfirmed range, scoped to the requested `b`. 17: the release waits until no planned sequence is at or below `b` | X09 |
| F12 | CL | The part to the tracking partition fails while other parts commit | changed | 01 and 17: the sequence is missed once a recorded `finishedThrough` reaches it, and the fence waits for that | |
| F13 | CL | A fence races a part in flight | kept | | |
| F14 | CL | A silent broker's lease goes idle | changed | 02: renamed "a broker cut off from its tracking partition", mechanics unchanged | |
| F15 | CL | The idle timer fires as a part arrives | kept | 02: now off the normal path | |
| F16 | CL | A tracking leader's monotonic clock jumps forward | kept | 02: now off the normal path | |
| F17 | CL | A tracking leader pauses between the timer and the append | kept | | |
| F18 | CL | Tracking-partition leadership changes faster than the idle timeout | changed | 02: affects only cut-off brokers, and the idle timer no longer closes the duplicate window, so the row's "duplicate window stays open" goes | X29 |
| F19 | CL | A broker bug sends an unleased sequence or a different batch count | kept | | |
| F20 | CL | A part names a user partition that doesn't map to the receiving coordinator partition | kept | | |
| F21 | DE | A partial send, or a delegate replaced between parts | kept | | |
| F22 | DE | The broker stops before the upload | kept | | |
| F23 | DE | The broker stops after the upload, before any part commits | changed | 01: a running broker's next tracking part marks the sequence missed, and 17: the recorded `finishedThrough` does too. An idle or restarted broker's lease end covers it | |
| F24 | DE | The broker stops after some parts commit | changed | 01: no finish report covers the file, so the lease end covers it. Test 01-6 replaces v2's test | |
| F25 | DE | A commit where every batch fails | kept | | |
| F26 | DE | An object's batches expire on one coordinator partition while another holds batches | kept | | |
| F27 | DE | The tracking partition's leader is unreachable for a release report | changed | 05: one batched report in flight per tracking partition. 15: retries back off | |
| F28 | DE | An uploader pauses past the idle timeout with an upload in flight | kept | | |
| F29 | DE | Leadership moves between the storage delete and the files-deleted record | kept | | |
| F30 | DE | Two clusters share a bucket and prefix | kept | | |
| F31 | DE | An upload stops partway | kept | | |
| F32 | LR | Leader loses the partition | kept | | |
| F33 | LR | Append fails, or the leader resigns, before the high watermark | kept | | |
| F34 | LR | Leader change truncates unreplicated records | kept | | |
| F35 | LR | Coordinator leader is down when a topic is created or deleted | changed | 03: the retriable error is `COORDINATOR_LOAD_IN_PROGRESS` | |
| F36 | LR | A deposed leader answers reads | kept | | |
| F37 | LR | Delayed-fetch probe above the high watermark with the cache off | kept | 15: `findBatchesNoWait` follows the asynchronous `findBatches` | |
| F38 | LR | A client keeps fetching past the end | kept | | |
| F39 to F50 | RS | The 12 snapshot and rebuild rows, except power loss | kept | | |
| F51 | RS | Power loss on one replica or across every replica | changed | 13: SQLite rolls back whole groups, not single applies | X31 |
| F52 to F57 | CA | The six controller and admin rows | kept | | |

### New rows from v3 tickets

| ID | Group | Failure | Behavior, in brief | Test | Source | Flag |
| --- | --- | --- | --- | --- | --- | --- |
| F60 | CL | A broker is idle longer than the idle timeout | The keepalive keeps the lease, and the first file commits. | T02-1, named | 02 Failure rows | |
| F61 | CL | A low-rate broker falls back | It switches at once and releases the abandoned range. No floor passes the range in use. | T02-2, named | 02 | |
| F62 | CL | The tracking partition becomes unavailable mid-range | The failed tracking part triggers a renewal within one commit deadline. | T02-3 | 02 | |
| F63 | CL | A late release arrives after the broker returned to `t` | The release stops at `b`, and the new range stays live. | T02-4 | 02 | |
| F64 | CL | A failover loses a finish-reported record before it applies | The broker still holds the report and resends it. | T02-6 | 02 | |
| F65 | CL | A broker ID re-registers after an unregistration | Commits at the new epoch succeed, and epochs at or below `retiredThrough` are stale. | T03-1, named | 03 | |
| F66 | CL | A leader's image lags a new registration | No broker-epoch-retired record. Commits proceed. | T03-2 | 03 | |
| F67 | LR | A new leader's image lags a partition-created record from the old leader | No topic-deleted until the image reaches `metadataOffset`. | T03-4, named | 03 | |
| F68 | LR | A call names an unreconciled partition | `COORDINATOR_LOAD_IN_PROGRESS`, and `UnreconciledPartitionRejectCount` rises. | T03-5 | 03 | |
| F69 | RS | A leader bug writes a row for an unreconciled partition | Apply stops at that record, and the replica reports corruption. | T03-6 | 03 | |
| F70 | CL | One coordinator partition is stuck | Requests with a batch for it complete at their file's commit deadline. Others aren't delayed. A stuck leader broker extends this to every coordinator partition it leads. | T04-1, named | 04; replaces F04's first half | X02 |
| F71 | CA | `diskless.metadata.topic.replication.factor` below 3 without the internal override | Controller fails startup. | T11-1 | 11 | X02 |
| F72 | CA | Fewer registered, unfenced brokers than the replication factor | The topic isn't created, and calls get the retriable error. | T11-2 | 11 | X02 |
| F73 | CA | Fewer racks than the replication factor | Placement uses every rack and logs a warning. | T11-3 | 11 | X02 |
| F74 | CA | A reassignment would lower a partition's spread | `INVALID_REPLICA_ASSIGNMENT`. | T11-4, T11-7 | 11 | X02 |
| F75 | CA | A broker re-registers with another rack, or a new rack registers | `CoordinatorPartitionsBelowRackSpread` rises. Nothing is rejected. | T11-6 | 11 | X02 |
| F76 | RS | A snapshot backup or a slow disk keeps the checkpoint behind, or a checkpoint fails | Applies continue, and `SqliteWalPages` rises. The thread retries. | T13-1 | 13 | X02 |
| F77 | RS | A record stops the apply in the middle of a group | SQLite commits every record before it. The leader resigns. | T13-3 | 13 | X02 |
| F78 | RS | The bundled SQLite is below the version floor | The broker refuses to start the coordinator backend. | T13-2 | 13 | X02 |
| F79 | RS | The follower applier falls behind | The replica stays in the ISR, and `ApplyLagBytes` rises. A new leader's high-watermark gate holds commits. | T13-4 | 13 | X02 |
| F80 | CL | A coordinator leader slows down or fails fast under load | In-flight work stays within about `B` commit parts and `8B` read parts. Retries back off. Read parts fail at 1000 ms for its user partitions only. | T15-1 to T15-6 | 15 | X02, X15 |
| F81 | CL | A lease renewal gets no answer while the broker plans sequences in the requested range | The release waits until no planned sequence is at or below `b`. | T17-1, named | 17 | X09 |
| F82 | CL | A tracking part fails, and the broker switches before a later record at `t` | `t` marks the sequence missed when the release's `finishedThrough` reaches it. | T17-2, named | 17 | |
| F83 | DE | A participant resends a release report after a lease end resolved its sequence | Accepted as a no-op, and no entry is stored. | T17-3, named | 17 | X10 |
| F84 | DE | A batched release report holds a group outside the leased ranges | The valid groups apply, and only that group is rejected. | T17-4, named | 17 | X08 |
| F85 | CA | An upstream change makes the controller write a record ahead of a new registration | The pinning test fails at the upstream sync. | T17-5, named | 17 | |
| F86 | CL | The preferred tracking partition is unavailable | The broker walks the ordered list. Below saturation it stays in its gateway set at no extra part. | T22-5 | 22 | |
| F87 | CL | The assignment flaps during a rolling restart | The broker switches only once a file comes out empty. | T22-4 | 22 | X11 |
| F88 | CL | The selection picks a coordinator partition outside the gateway set | Cost only: one empty part per file, shown by `empty-tracking`. | T22-2 | 22 | |
| F89 | GW | An operator sets the gateway switch on one broker | `INVALID_CONFIG`. | T23-1 | 23 | |
| F90 | GW | A broker's `server.properties` sets the gateway switch | The broker refuses to start. | T23-1 | 23 | |
| F91 | GW | A live broker loses a gateway slot | One `NOT_LEADER_OR_FOLLOWER` per connection and partition, then the client commits at the new gateway. | T24-1 | 24 | |
| F92 | GW | Brokers disagree on the assignment | At most one retry per broker, connection, and partition per `metadata.max.age.ms`. | T24-2 | 24 | |
| F93 | GW | A fenced broker sees its own fence | It isn't in its own candidate set, so it rejects once and names the stand-in until it unfences. | T24-6 | 24 | |
| F94 | GW | A fenced broker can't reach the coordinator leaders | Commits fail with `KAFKA_STORAGE_ERROR`. Nothing acknowledged is lost. | T24-6 | 24 | |

### Rows no ticket wrote, needed to close a finding

Four tickets resolved a review finding without a failure-mode row, and one ticket left a failure behavior without a row. The rewrite needs these rows to meet the map's closing rule.

| ID | Group | Failure | Behavior | Test | Source | Closes |
| --- | --- | --- | --- | --- | --- | --- |
| F95 | CL | A participant's part is delayed by a failover while the file's tracking part is empty | The part commits at the new leader within the deadline, and the produce succeeds. The tracking partition sends no fence and keeps the object's entry until the participants release it. | T01-1, named | derived from 01 | Fable's defect 1 |
| F96 | DE | A participant's release report arrives before the tracking part records | The tracking partition stores the entry as released, sends that participant no fence, and queues the object once the others release. | T05-1, named | derived from 05 | Fable's release-ordering finding |
| F97 | DE | A "referenced" fence answer arrives after the participant's release report | The entry stays released, and the object is queued. | T05-2 | derived from 05 | Fable's release-ordering finding |
| F98 | DE | A retention pass releases many objects at one participant | One batched report per tracking partition, and one record on each side per report. | T05-6 | derived from 05 | Fable's batching suggestion |
| F99 | CL | A coordinator partition recovers after an outage under load | Each tracking partition keeps at most one fence request in flight to it. `PendingFenceRangeCount` drains, and no commit hits a full overlay. | T06-1, named | derived from 06 | Council's fence-storm risk |
| F100 | GW | A gateway crashes | Its coordinator partitions stall in its rack until the controller fences it, as a classic partition does. After the fence, the rule picks stand-ins. | New cluster test: gateway crash | derived from 21 | none, a behavior row |

## Accepted risks

v2's columns are Risk, Why it's accepted, and Signal or procedure. The bar column checks each risk against the map's data-loss bar.

| ID | Risk | Source and status | Bar check | Signal or procedure | Flag |
| --- | --- | --- | --- | --- | --- |
| R01 | Every replica of a coordinator partition is lost | 11, replaces v2's row | Clause 2 by its exception, with replication factor 3 and maximal spread enforced. Clause 3 holds: objects leak, nothing is deleted. Voided on a cluster that uses the internal override. | Signal: `OfflinePartitionsCount` for `__diskless_metadata`. Procedure: none in v3, `P` stays leaderless. Deployment docs recommend dedicated controllers. | |
| R02 | A deterministic apply bug corrupts every replica alike | 10, replaces v2's row | Clause 3 by its exception. | Signal: sustained `PartitionStorageErrorRate`. Prevention: shared suite, golden records, apply stop. | |
| R03 | Log recovery truncates fsynced records after a read error or bit rot | 10, replaces v2's row | Clause 3 by its exception. | Procedure: alert on the truncation warning and stop the broker. | X01 |
| R04 | A device acknowledges an fsync without persisting it | 10, replaces v2's row | Clause 3 by its exception. | Procedure: storage that honors fsync. | X01 |
| R05 | An operator restores a log directory from an older copy | 10, replaces v2's row | Clause 3 by its exception. | Procedure: never restore, reformat and re-replicate. | X01 |
| R06 | A stale leader-epoch entry over-truncates an ELR member | 10, replaces v2's row | Clause 3 by its exception. | Signal: `UncleanLeaderElectionsPerSec`, `ElectionFromEligibleLeaderReplicasPerSec`. | X01 |
| R07 | Something outside the coordinator deletes a WAL key | 10, new | Clause 3 by its exception. | Procedure: docs forbid expiration rules and manual deletes on the WAL prefix. Signal: `PartitionStorageErrorRate`. | |
| R08 | Retention and `LogAppendTime` under a skewed leader clock | v2, kept | Clause 2: the damage covers the user partitions of the coordinator partitions the skewed broker leads, about the share of classic partitions it leads. Clause 3 holds: removed batches release normally. v2's reason "Classic topics have the same exposure" doesn't state the blast radius. | Procedure: NTP offset alert below half the grace period. | X32 |
| R09 | Grace-period skew beyond half the grace period | v2, kept | Not data loss: reader availability. | The NTP procedure. | |
| R10 | Storage completes an upload after its key was deleted | v2, kept | Not data loss: a storage leak. | Signal: `FileUploadErrorRate`. | |
| R11 | A non-idempotent producer that retries through another broker sees a late copy | v2, kept | Not data loss: a duplicate. 24's rejection happens before the write, so it adds no late copy. | Signal: `event-purgatory-time-ms`. | |
| R12 | A held entry whose participant no longer holds batches and never reports | 05, new | Not data loss: a storage leak. A wrong "answered" claim leaks and never deletes. | Signal: `ReleaseReportRejectedRate`, `ReleaseOutsideParticipantSetRate`, and storage growth. The report-only scanner stays future work. | |
| R13 | A gateway cut off from the controller but reachable by clients keeps committing | 24, new | Not data loss: extra parts, cache misses, and uneven load. | Signal: `FencedBrokerCount`, the broker's `last-applied-record-lag-ms`, and parts per round. | X25 |
| R14 | A cluster outgrows its partition count | 14, new, placed in Capacity and isolation | Performance bar only. Correctness and the 1/N blast radius don't change. | Signal: `event-queue-time-ms`, `batch-flush-time-ms`, P99 `FileCommitTime`. Procedure: choose `N` for peak size, reassign leaders. | X12, X25 |
| R15 | The busiest gateway carries up to 1.7 times today's busiest broker | 21, stated as an accepted cost, no table | Performance bar only. | Signal: `BytesInPerSec` against the rack mean. Lever: the switch, racks of up to 4 or a multiple of `N`. | X25 |

## Metrics

v2 registers two groups: the runtime group `diskless-metadata-coordinator-metrics` (`DisklessMetadataCoordinatorRuntimeMetrics extends CoordinatorRuntimeMetricsImpl`) and one Yammer `KafkaMetricsGroup` named after the backend class, PascalCase, with a `partition` tag on metrics scoped to a coordinator partition. A name check against `docs/inkless/metrics.rst` finds no clash for any new name in this table.

| ID | Name | Scope | Source and status | Group | Flag |
| --- | --- | --- | --- | --- | --- |
| M01 | `UnknownOutcomePartRate` | Broker, `partition` tag | v2, kept | Backend | |
| M02 | `StaleEpochRejectRate` | Leader | v2, kept | Backend | |
| M03 | `StaleSequenceRejectRate` | Leader | v2, changed by 02: any nonzero rate from a registered broker is an anomaly | Backend | |
| M04 | `PartitionMappingMismatchRate` | Leader | v2, kept | Backend | |
| M05 | `LeaseRenewalRate`, tagged `reason` | Broker | v2, changed by 02 (`threshold`, `keepalive`, `failed-tracking-part`) and 22 (`assignment`). `keepalive` means a keepalive got no answer. Plain keepalives get no metric. | Backend | X19 |
| M06 | `LeaseFallbackRate` | Broker | v2, kept by 02. Under 22's ordered list, a fallback is a renewal confirmed below the list's first entry. | Backend | X40 |
| M07 | `FileNoLeaseErrorRate` | Broker | v2, kept | Backend | |
| M08 | `IdleLeaseEndRate` | Leader of the tracking partition | v2, changed by 02: any idle end is an anomaly | Backend | |
| M09 | `FenceRate`, tagged `type` (exception, missed, lease-end) and `answer` | Leader of the tracking partition | v2, changed by 06: counts ranges, and `sequence` leaves | Backend | X18 |
| M10 | `DeletionQueueSize`, `DeletionQueueOldestAgeMs` | Leader of the tracking partition | v2, kept | Backend | |
| M11 | `BarrierRate`, tagged `outcome` | Leader | v2, kept | Backend | |
| M12 | `ApplyStoppedPartitionCount` | Broker, gauge | v2, kept | Backend | |
| M13 | `FinishReportFlushRate` | Broker | 02, new | Backend | |
| M14 | `UnreconciledPartitionRejectCount` | Leader | 03, new. It counts rejections, but its name follows the gauge convention. | Backend | X20 |
| M15 | `TrackingEntryCount`, tagged `state` (pending, fencing, held) | Leader of the tracking partition | 05, new | Backend | |
| M16 | `TrackingEntryOldestAgeMs`, tagged `state` (pending, fencing) | Leader of the tracking partition | 05, new | Backend | |
| M17 | `PendingReleaseCount` | Leader of a participant | 05, new | Backend | |
| M18 | `PendingReleaseOldestAgeMs` | Leader of a participant | 05, new | Backend | |
| M19 | `ReleaseReportRejectedRate` | Leader of the tracking partition | 05, new; 17 counts per rejected group | Backend | |
| M20 | `ReleaseOutsideParticipantSetRate` | Leader of the tracking partition | 05, new | Backend | |
| M21 | `PendingFenceRangeCount` | Leader of the tracking partition | 06, new | Backend | |
| M22 | `CoordinatorPartitionsBelowRackSpread` | Controller, gauge | 11, new | None of v2's two. Kafka's `ControllerMetadataMetrics` (`kafka.controller:type=KafkaController`) holds `FencedBrokerCount` and `OfflinePartitionsCount`. | X24 |
| M23 | `SqliteWalPages` | Broker, `partition` tag, gauge | 13, new | Backend | |
| M24 | `ApplyLagBytes` | Broker, `partition` tag, gauge | 13, new | Backend | |
| M25 | `OverlayCapRejectRate` | Leader, `partition` tag | 15, new | Backend | |
| M26 | `PrivateRpcRetryRate`, tagged `rpc` and `error` | Broker, `partition` tag | 15, new | Backend | |
| M27 | `ReadPartFailRate`, tagged `reason` | Broker, `partition` tag | 15, new | Backend | |
| M28 | Read pool queue-size and active-thread gauges | Leader broker | 15, new, unnamed | Inkless already has `ThreadPoolMonitor` (`io.aiven.inkless.thread-pool:type={pool-name}`) with `queued-task-count-total` and `active-thread-count-total`. | X21 |
| M29 | Commit-parts meter, tagged by part kind: `routed`, `misrouted`, `empty-tracking`, `off-gateway` | Broker | 21, 22, and 24, new, unnamed | Backend, at the split by coordinator partition | X23 |
| M30 | Misrouted bytes meter, accepted batches only | Broker | 21, redefined by 24, unnamed | Backend | X23 |
| M31 | Misrouted-batch rejection count | Broker | 24, new, unnamed | Produce path, before the write: not the backend | X23, X24 |
| M32 | `ClientAzAwarenessMetrics` (`client-az-hit-rate`, `fallback-total`, `cross-az-routing-total`, `offline-replicas-routed-around`) | Broker | Existing. `fallback-total` counts "requests routed to non-replica brokers", which the gateway path makes the normal case. No ticket decides it. | `io.aiven.inkless.metadata:type=ClientAzAwarenessMetrics` | X22 |
| M33 | Runtime group: `event-queue-time-ms`, `event-purgatory-time-ms`, `batch-flush-time-ms` | Leader | v2, kept. 15: with reads off the shard, `event-queue-time-ms` measures write load alone. | Runtime | |

Existing metrics the design cites: `FileCommitTime` (04: measures the slowest part, recorded and not judged), `FileCommitErrorRate`, `FileUploadErrorRate`, `FindBatchesTime`, `PartitionStorageErrorRate`, `OfflinePartitionsCount`, `UncleanLeaderElectionsPerSec`, `ElectionFromEligibleLeaderReplicasPerSec` (all v2). New citations: `FencedBrokerCount` and `last-applied-record-lag-ms` (24), `BytesInPerSec` (21), `RequestMetrics` `TotalTimeMs` for `request=Produce` (04, 16, 23), `LogFlushRateAndTimeMs` (12, 16, 23), and the client's `record-retry-rate` and `record-error-rate` (23).

Removed: `FenceRate` `type=sequence` (06). Not in v3: 09's recovery metrics and 10's missing-key metric.

## Tests

v2's layers: L1 the shared `AbstractControlPlaneTest` at `N=1` and `N=8`, L2 coordinator backend tests with a mock clock per uploader and leader, L3 `KafkaClusterTestKit` cluster tests, L4 fault injection through a hook on the client side of the private RPC. Seam tests: the shard with `MockPartitionWriter` and `MockCoordinatorTimer`, the delayed-fetch probe, the guards next to each controller and broker check, the key-name function, the mapping, snapshot unit tests, and apply rules. "Proposed" is this inventory's placement, for the session to confirm.

### Fable's five named tests

| Fable's test | Ticket test |
| --- | --- |
| An empty tracking part with a participant failover mid-commit | T01-1 |
| A low-rate broker after a lease fallback | T02-2 |
| A broker that resumes after an idle lease end | T02-1 |
| A broker that is unregistered, re-registers with the same ID, and commits | T03-1 |
| A release report that arrives before the tracking part | T05-1 |

### Tests by ticket

| ID | Test | Source | Proposed placement | Flag |
| --- | --- | --- | --- | --- |
| T01-1 | Named: empty tracking part with a participant failover, produce succeeds, no fence | 01 | L3 | |
| T01-2 | A missed sequence waits for `finishedThrough` | 01 | L3, rewrites v2's "Missed sequence" | |
| T01-3 | Exceptions coalesce into one range fence | 01 | L2 | |
| T01-4 | The `Recorded` flag: no fence for an all-fail part, a fence for a stale rejection | 01 | L2 | |
| T01-5 | A failed tracking part with a report, merged into the next | 01 | L2 | |
| T01-6 | A lease end covers unreported sequences | 01 | L3, replaces v2's "Broker stops after some parts" | |
| T01-7 | No fence at `N=8` through commits and retention | 01 | L1 | |
| T02-1 | Named: a broker resumes after an idle period | 02 | L2, because the idle timeout needs a mock clock | |
| T02-2 | Named: a low-rate broker after a lease fallback | 02 | L2 | |
| T02-3 | Tracking partition unavailable early in a range | 02 | L3, extends v2's "Tracking partition down" | |
| T02-4 | A late release against a newer range | 02 | Seam: shard | |
| T02-5 | An urgent report on a quiet broker | 02 | L2 | |
| T02-6 | Failover between the finish-reported append and its apply | 02 | Seam: shard | |
| T03-1 | Named: re-registration under the same ID | 03 | L2 | |
| T03-2 | A new broker ahead of the image | 03 | L2 | |
| T03-3 | A middle epoch the partition never saw | 03 | L2 | |
| T03-4 | Named: unreconciled partition across a leader change | 03 | L2 | |
| T03-5 | A call before reconciliation | 03 | L3, extends v2's "Topic created while leader down" | |
| T03-6 | Apply rule for a user partition with no partition-created row | 03 | Seam: apply rules | |
| T04-1 | Named: requests `A` and `B` around a stuck coordinator partition | 04 | L3, extends v2's "One coordinator partition offline" | |
| T04-2 | A stuck tracking partition | 04 | L3 | |
| T04-3 | A request with only rejected batches completes at file close | 04 | Seam: produce path (`AppendCompleter`), new seam | |
| T04-4 | The cache fills per part | 04 | Seam: produce path | |
| T04-5 | Single-queue backends complete per file | 04 | L1 | |
| T05-1 | Named: release report before the tracking part | 05 | L2 | |
| T05-2 | Late "referenced" after a release | 05 | L2 | |
| T05-3 | Early report for a missed sequence | 05 | L2 | |
| T05-4 | Duplicate after resolution | 05 | L2 | X10 |
| T05-5 | Report outside the leased ranges | 05 | merged into T17-4 | X08 |
| T05-6 | Batching under retention | 05 | L2 | |
| T05-7 | A stuck fence raises `TrackingEntryOldestAgeMs` | 05 | L2 | |
| T06-1 | Named: participant outage under load | 06 | L3 | |
| T06-2 | Hull merge | 06 | Seam: shard | |
| T06-3 | Broker-wide `finishedThrough` | 06 | L2 | |
| T06-4 | Lease release after a switch waits | 06 | L2 | |
| T06-5 | A new tracking leader rebuilds the pending set | 06 | L3, extends v2's "Tracking leader change" | |
| T06-6 | Fence set above the size cap splits | 06 | Seam: shard | |
| T11-1 | Replication factor below 3 fails startup without the override | 11 | Seam: guards (controller config validation) | |
| T11-2 | Creation at registration, unfencing, and activation | 11 | Seam: guards (`ReplicationControlManagerTest`) | |
| T11-3 | Placement spans `min(RF, racks)` racks | 11 | Seam: guards | |
| T11-4 | A narrowing reassignment gets `INVALID_REPLICA_ASSIGNMENT` | 11 | Seam: guards | |
| T11-5 | A replica-count change still gets `INVALID_REPLICATION_FACTOR` | 11 | Seam: guards, v2's existing case | |
| T11-6 | `CoordinatorPartitionsBelowRackSpread` after a rack change | 11 | Seam: guards (controller metrics) | |
| T11-7 | 3-rack cluster: one replica per rack, narrowing fails through `Admin` | 11 | L3 | |
| T13-1 | SQLite checkpoint: `wal_autocheckpoint=0`, no checkpoint on the apply thread, backup held open, 4 MiB group cap | 13 | Seam: SQLite, new | |
| T13-2 | SQLite version check | 13 | Seam: SQLite, new | |
| T13-3 | Apply stop in the middle of a group | 13 | L2, extends v2's "Apply stop" | |
| T13-4 | Failover mid-commit with the follower applier paused | 13 | L3, extends v2's "Failover mid-commit" | |
| T15-1 | Backoff schedule against `COORDINATOR_LOAD_IN_PROGRESS` | 15 | L2 | |
| T15-2 | Overlay cap at `4 × B` | 15 | Seam: shard | |
| T15-3 | Reads off the shard keep commit latency | 15 | L2 | |
| T15-4 | Read isolation on the calling broker | 15 | L3 | |
| T15-5 | Read slots per coordinator partition | 15 | L2 | |
| T15-6 | One test per in-flight limit | 15 | L2 | |
| T16-1 | Test-only fault hook on a coordinator partition, with unavailable and delay modes | 16 | L4, as a new server-side hook | |
| T16-2 | Verifying consumer for zero acknowledged loss | 16 | Benchmark tooling | |
| T17-1 | Named: planned sequence and lost lease reply | 17 | L4, with v2's "lost lease reply" | X09 |
| T17-2 | Named: abandoned range with a failed tracking part | 17 | L2 | |
| T17-3 | Named: resent report after a lease end | 17 | L2 | X10 |
| T17-4 | Named: batched report with one bad group | 17 | Seam: shard | X08 |
| T17-5 | Named: controller registration order | 17 | Seam: guards (`ClusterControlManagerTest`) | |
| T17-M | The three TLA+ specs and `run-all.sh` move into the repo | 17 | Testing decisions, not a layer | X28 |
| T17-F | Fault injection that drops the unflushed tail of a coordinator partition at each fsync point | 17 | 17 says "integration layer". The drop is in the log, not on the client side of the RPC, so it needs a server-side hook. | |
| T20-1 | Two user partitions of one coordinator partition get one broker per AZ | 20 | merged into T21-4 | X07 |
| T21-1 | Agreement: every broker computes the same assignment | 21 | L3 | |
| T21-2 | Fixed values for the default rule | 21 | Seam: routing, new | |
| T21-3 | Stability: invariants and at most 5 extra moves per event | 21 | Seam: routing | |
| T21-4 | Transformer routing, one case per fallback step | 21 | Seam: routing | |
| T21-5 | A produce to a non-gateway commits and the meters count it | 21 | merged into T24-1 | X06 |
| T21-6 | Exclusion moves a broker's coordinator partitions | 21 | L3 | |
| T22-1 | Selection with fixed values | 22 | Seam: routing | |
| T22-2 | One part per routed file at saturation | 22 | L3 | |
| T22-3 | Switch on the first empty part | 22 | L3 | |
| T22-4 | A fence followed by a return: no switch | 22 | L3, amended by 24 | X11 |
| T22-5 | Fallback inside the set, no loop at saturation | 22 | L2 | |
| T22-6 | Spare promotion and demotion | 22 | L2 | |
| T22-7 | Gateway switch toggle moves no tracking partition | 22 | L3 | |
| T23-1 | Cluster-default scope of the switch | 23 | L3, plus a `ControllerConfigurationValidator` seam case | |
| T23-2 | Predicted parts per file | 23 | Seam: routing | |
| T24-1 | Misrouted produce: one rejection, then commit | 24 | L3 | X06 |
| T24-2 | No loop under disagreement | 24 | L3 | |
| T24-3 | Unknown AZ | 24 | Seam: routing | |
| T24-4 | No replica preference | 24 | Seam: routing | |
| T24-5 | Local-log partitions keep replica routing, count as `off-gateway` | 24 | L3 | |
| T24-6 | Fenced broker, with and without coordinator reach | 24 | L3 | |
| T24-7 | Routing switch-on conditions | 24 | Seam: routing | |

### v2 tests that change

| v2 test | Change | Ticket |
| --- | --- | --- |
| L3 "Missed sequence" | Fenced once `finishedThrough` reaches it, not on a later commit | 01, 17 |
| L3 "Broker stops after some parts" | Replaced by T01-6 | 01 |
| L3 "Tracking partition down" | Extended by T02-3, and the fallback follows the ordered list | 02, 22 |
| L3 "Idle lease end" | Renamed for a cut-off broker | 02 |
| L3 "ISR below minimum" | Renewals walk the ordered list | 22 |
| L3 "Failover mid-commit" | Extended by T13-4 | 13 |
| L3 "One coordinator partition offline" | Extended by T04-1 | 04 |
| L3 "Tracking leader change" | Extended by T06-5 | 06 |
| L3 "Topic created while leader down" | Extended by T03-5 | 03 |
| L2 "Apply stop" | Extended by T13-3 | 13 |
| L4 "Lost lease reply" | Merged with T17-1 | 02, 17 |
| Seam "Mapping" | Pins the tracking partition hash `toPositive(murmur2(brokerId)) % N`, which 22 replaces with a local choice that needs no agreement | 22 | 

## User stories

v2's spec has stories 1 to 95. New stories continue the numbering.

| ID | Story | Source | Change | Flag |
| --- | --- | --- | --- | --- |
| S03 | Replication factor 3 by default | v2 | derived from 11: a minimum of 3, enforced | |
| S04 | A development cluster with one replica | v2 | changed by 11: through the internal override | |
| S05 | The controller creates the topic on first enable | v2 | derived from 11: once enough unfenced brokers register | |
| S06 | Replicas across racks where possible | v2 | changed by 11: maximal spread, enforced by the controller | |
| S31 | Retry on `NOT_COORDINATOR` within the commit deadline | v2 | derived from 15: with exponential backoff | |
| S56 | The coordinator reports its state as metrics | v2 | derived: the list widens to tracking entries, pending releases, fence backlog, WAL pages, apply lag, and gateway parts | |
| S78 | Renew at the next coordinator partition | v2 | derived from 22: the ordered fallback list | |
| S79 | Mapping and tracking partition pinned by a fixed-value test | v2 | derived from 22: the tracking partition is a local choice, so the cross-version reason no longer applies to it | X17 |
| S83 | A stuck coordinator partition delays only its partitions | v2 | changed by 04: per request, at most one commit deadline | |
| S95 | A silent broker's lease ends after the idle timeout | v2 | changed by 02: a broker cut off from its tracking partition | |
| S96 | A quiet producer's first produce succeeds | 02, new | | |
| S-adm | Stories for the load bounds and read isolation | 15 | none written; ticket 18's question asks for them | |
| S-gw | Stories for gateway routing | 20 to 24 | none written. v2's spec lists "Produce gateways and preferred produce brokers" as out of scope | |
| S-fut | 09's recovery story | 09 | not in v3: a Future work entry instead | |

## Terms entries

| ID | Term | Source | Status |
| --- | --- | --- | --- |
| TM01 | Kafka parity | map Notes | new, with the bar's meaning |
| TM02 | Blast radius | map Notes | new, with the bar's meaning |
| TM03 | Finished part | 01 | new |
| TM04 | Finished file | 01 | new |
| TM05 | `finishedThrough` | 01, widened by 06 to the whole broker epoch | new |
| TM06 | Finish report | 01 | new |
| TM07 | Keepalive | 02 | new |
| TM08 | Abandoned range | 02 | new |
| TM09 | Lease release | 02 | new |
| TM10 | Urgent finish report | 02 | new |
| TM11 | Entry states: pending, fencing, held, released | 05 | new |
| TM12 | Maximal spread | 11 | new |
| TM13 | Gateway | 21 | new |
| TM14 | Coordinator leader | 21 | new |
| TM15 | Gateway set | 22 | new |
| TM16 | Spare | 21, 22 | new |
| TM17 | Saturation | 22, 23 | new |
| TM18 | Misroute | 21, 24 | new |
| TM19 | Local-log partition | 24 | new |
| TM20 | Unreconciled partition | 03 | new |
| TM21 | Broker epoch retired | v2 | changed by 03: carries `retiredThrough` |
| TM22 | Broker row | v2 | changed by 03: a retired-through epoch replaces the retired marker |
| TM23 | Missed sequence | v2 | changed by 17: missed once a recorded `finishedThrough` reaches it |
| TM24 | Range fence | v2 | changed by 01 and 06: exception range fences go to one participant, and fences merge into hulls |
| TM25 | Tracking partition | v2 | changed by 22: one of the broker's gateway coordinator partitions, chosen by the sticky rule |
| TM26 | Tracking part | 22 | new; v2 uses it without defining it |
| TM27 | Barrier record, Deletion queue, Deposed window, Fenced-through mark, Files-deleted record, Floor, Lease, Leftover sequence, Sequence source | v2 | kept |

## Future work entries

| ID | Entry | Source | Note |
| --- | --- | --- | --- |
| FW01 | Grow the coordinator by splitting partitions | v2 | kept. 14 names splitting as the only fix for a cluster that outgrows `N`. |
| FW02 | Merge WAL objects into extents | v2 | kept. 24 notes that upstream's merge framework means no partition needs a local log. |
| FW03 | Tiered metadata in the coordinator | v2 | kept |
| FW04 | Partitionless key ranges | v2 | kept |
| FW05 | Report-only orphan scanner | v2 Non-goals, 05, 19 | kept as a Non-goal. 05 cites it as the only detector for R12. |
| FW06 | Object footer and recovery after losing a replica set | 09 | new |
| FW07 | Deletion bounds: soft-delete requirement and a deletion-rate cap | 10 | new |
| FW08 | Empty restart of a coordinator partition | 19 | new |
| FW09 | Extra gateways for a hot coordinator partition, with a per-coordinator-partition traffic metric | 21, plus 23's measured motivation if the busiest-gateway rule fails at 21 or 45 brokers | new, one entry |
| FW10 | Traffic-following tracking for brokers with an empty gateway set | 22, with 24's finding that no source of steady misrouted traffic remains | new |
| FW11 | A cluster-only scope for dynamic broker configs upstream | 23 | new |
| FW12 | The per-read rule in Inkless for consolidating and tiered partitions | 24 | new |
| FW13 | Cold-path fetches at brokers with an empty gateway set | 24 | new, unanalyzed |

Not future work: 17's "retire on proof" is a fallback if an upstream sync breaks T17-5. 16's AZ-aware leader placement and a gap-closing effort are outcomes only if a benchmark fails.

## Configs

| ID | Config | Scope | Default | Source and status | Flag |
| --- | --- | --- | --- | --- | --- |
| C01 | `diskless.metadata.topic.num.partitions` | Controller, fixed at creation, power of two | 8, provisional until the partition-count benchmark | v2, kept by 14 | |
| C02 | `diskless.metadata.topic.replication.factor` | Controller, fixed at creation | 3, minimum 3 | v2, changed by 11 | |
| C03 | Internal override of the minimum replication factor | Controller, `defineInternal`, undocumented | minimum 3; lowers it to 1 | 11, new, unnamed: the name is an open item | X39 |
| C04 | `diskless.metadata.topic.min.insync.replicas` | Controller, fixed at creation | `min(2, RF)` | v2, kept | |
| C05 | `diskless.metadata.lease.idle.timeout.ms` | Broker, the lease reply carries the leader's value | 15 minutes, provisional | v2, changed role by 02: a backstop | |
| C06 | `diskless.metadata.snapshot.interval.ms` | Broker | v2's value | v2, kept | |
| C07 | `diskless.metadata.snapshot.max.bytes.between.snapshots` | Broker | 1 GiB; 16's rule may drop it to 256 MiB | v2, kept | |
| C08 | `diskless.metadata.snapshot.max.concurrent.transfers` | Broker | v2's value; 16's rule sets it | v2, kept | |
| C09 | `diskless.metadata.snapshot.transfer.idle.timeout.ms` | Broker | v2's value | v2, kept | |
| C10 | `diskless.metadata.read.threads` | Broker | 4, provisional until the read benchmarks | 15, new | |
| C11 | `diskless.gateway.routing.enable` | Cluster default only; the controller rejects per-broker values, and a broker refuses a static value | `true` | 23, new | |
| C12 | Per-broker gateway exclusion | Per-broker dynamic config through `AlterConfigs` | not excluded | 21, new, unnamed. 21 says the spec names it, and 23 gives it the `diskless.gateway.` prefix. | X37 |
| C13 | Append linger | Broker | adaptive, as `group.coordinator.append.linger.ms=-1` | 14, new default. 14 says a fixed linger stays available as a config, but no ticket names it. | X38 |
| C14 | Commit deadline | Broker | open item: at most the produce request timeout, and it sets the runtime's write timeout | v2, kept as an open item | |
| C15 | `inkless.control.plane.class`, `diskless.coordinator.version`, `inkless.diskless.coordinator.version` | Broker, features | v2 | v2, kept. 24 reads the feature level for routing. | |
| C16 | `produce.commit.interval.ms` | Broker, existing | 250 ms | existing. 02 uses it for the urgent-report flush. | |

Constants, not configs, as the tickets decided: the backoff (100 ms initial, multiplier 2, 1000 ms maximum, 0.2 jitter) and the overlay cap `4 × B` (15), the read-part deadline of 1000 ms and 8 read slots per coordinator partition (15), the 512 KiB cap on report and fence requests (05, 06), the 1000-page checkpoint trigger and the 4 MiB apply-group cap (13), and the 500 ms barrier deadline (v2).

## Conflicts and duplicates

| ID | Conflict or duplicate | Sources | Status |
| --- | --- | --- | --- |
| X01 | 10's reason for the four lost-tail rows says "the consequence crosses coordinator partitions". 17's lost-tail runs find damage only at the partition that lost its tail, within the model's bounds of one broker and no tracking leader change. | 10, 17 | open |
| X02 | v2's failure tables have the columns Failure, Behavior, and Test. Tickets 04, 11, 13, and 15 write Failure, Effect, Recovery, and Test, and name test classes instead of v2's "Layer: test" form. | v2, 04, 11, 13, 15 | open |
| X03 | 24's 10%-mix figure of 4.9 parts per file is now 6.8. 24's Resolution still says 4.9 above its correction. | 23, 24 | settled by 23: the rewrite uses 23's curve, 2.1 at 1% up to 6.8 at 10% |
| X04 | 22's working notes say a rolling restart of 24 brokers adds about 50 switches, and that the first-empty-part trigger avoids nearly all of the 90 a switch on the image change costs. After 24's rejection, both triggers make about 90. | 22, 24 | settled by 22's update from 24: about 90 switches plus 2 to 4 empty parts per change |
| X05 | 21 said every broker is a gateway below `N` per rack and skipped load at 5 to 7 per rack. | 21, 22, 23 | settled: empty gateway sets of 0.28, 0.92, and 1.62 brokers per rack at 5, 6, and 7, and peak load of 1.2, 1.4, and 1.6 times |
| X06 | T21-5 says a produce at a non-gateway commits and the meters count it. T24-1 says the first batch per connection and partition is rejected and the meters count accepted batches only. | 21, 24 | open: merge |
| X07 | 20's named test is T21-4's routing case. | 20, 21 | settled by 21 |
| X08 | T05-5 rejects a whole report outside the leased ranges. T17-4 rejects only the bad group. | 05, 17 | open: merge |
| X09 | v2's fault-injection "lost lease reply", 02's row for it, and T17-1 test the same fault with different assertions. | v2, 02, 17 | open: merge |
| X10 | T05-4 and T17-3 both resend a report for a resolved sequence and expect no entry, through different classification branches: below the highest recorded sequence, and at or below the fenced-through pair. | 05, 17 | keep both, flag only |
| X11 | T22-4 needs stale traffic, which 24's rejection ends within one retry. | 22, 24 | settled by 22's update: drive traffic through a broker whose rejection already fired, or with the switch off |
| X12 | 14's accepted limit says each leader receives about `B` parts per round. With gateways on by default, it receives `n_racks × g`, about `B / N`. | 14, 23 | settled by 23: the rewrite states both |
| X13 | 16's base workload says every file holds batches for all 8 coordinator partitions, "the worst-case fan-out". 16's update from 23 runs it at the gateway default, so at 12 and 24 brokers a file holds 2 and 1. With gateways off, the coordinator is never compared with Postgres. | 16, 23 | open |
| X14 | 06's count of about `B × D / 0.25` exception fences assumes every file has a part at `t`, which holds with gateways off. | 06, 23 | note: the rewrite labels it the gateways-off worst case |
| X15 | 15's load bound of about `B` commit parts per leader holds as an upper bound with gateways on, from off-gateway and misrouted traffic. | 15, 21 | note |
| X16 | 02's renewal order "`t`, then `t + 1`, and onward" is replaced by 22's ordered list. 02 has no "Amended by" pointer for it. | 02, 22 | settled by 22 |
| X17 | v2's mapping seam test, story 79, and the open item on the broker ID's byte encoding pin the tracking partition hash. 22 replaces the hash with a local choice. No ticket records the consequence. | v2, 22 | open: derived consequence |
| X18 | `FenceRate` types change from `sequence`, `missed`, `range` to `exception`, `missed`, `lease-end`. | v2, 06 | settled by 06 |
| X19 | 02 adds a `reason` tag to "the renewal counter" and says plain keepalives get no metric, while one reason is `keepalive`. | 02, 22 | settled: `keepalive` means a keepalive got no answer |
| X20 | `UnreconciledPartitionRejectCount` counts rejections but is named like v2's gauges. v2 names meters `...Rate` and gauges `...Count`. | 03 | open |
| X21 | 15's read pool gauges duplicate `ThreadPoolMonitor`. | 15, metrics.rst | open |
| X22 | `ClientAzAwarenessMetrics.fallback-total` counts routing to non-replica brokers, which the gateway path makes the normal case. | 24, transformer | open |
| X23 | The commit-parts meter, the misrouted bytes meter, and the rejection count have no names. | 21, 22, 24 | open |
| X24 | `CoordinatorPartitionsBelowRackSpread` is a controller metric, and the rejection count is on the produce path. Neither fits v2's two coordinator groups. | 11, 24 | open |
| X25 | Cost-only risks sit in three places: 24 puts the cut-off gateway in Accepted risks, 14 puts the outgrown `N` in Capacity and isolation, and 21 accepts the busiest-gateway load in prose. | 14, 21, 24 | open |
| X26 | Findings closed without a failure-mode row: 01, 05, and 06 resolve review findings and write no rows. F95 to F99 fill them. | 01, 05, 06 | open: confirm |
| X27 | The council's benchmark finding is closed by pass criteria, which have no failure-mode row. | 16 | open: confirm |
| X28 | 17 says "three TLA+ specs" and lists four modules. `FFScenarios.tla` pins scenarios of `FencesFloors.tla`. | 17 | settled: three specs plus a scenario module |
| X29 | v2's row for leadership changing faster than the idle timeout says the duplicate window stays open. 02 closes it with the urgent finish report. | v2, 02 | settled by 02 |
| X31 | v2's power-loss row rolls back single applies. 13 rolls back whole groups. | v2, 13 | settled by 13 |
| X32 | v2's reason for the retention-skew risk doesn't state its blast radius. | v2 | open: reason rewrite |
| X37 | The per-broker exclusion has no name. | 21, 23 | open |
| X38 | The append linger config has no name. | 14 | open |
| X39 | The internal replication-factor override has no name. | 11 | settled by 11: an open item for the implementer |
| X40 | Under 22's ordered list, `LeaseFallbackRate` needs a definition. | 22 | settled: a renewal confirmed below the list's first entry |

## Review findings

A finding is closed by a fix with a failure-mode row and a named test, or by an accepted risk with a signal or an operator procedure.

### Council review

| Finding | Closed by | Row | Test or signal | Gap |
| --- | --- | --- | --- | --- |
| 1. Delete after a partial send | v2 fix | F21 | L1 partial send | none |
| 2. Wall-clock expiry and long pauses | v2 fix, grace period as accepted risk | F28, R08, R09 | L2 uploader pause, NTP procedure | none |
| 3. Losing every replica | 11 accepted risk under clause 2's exception | R01, F71 to F75 | `OfflinePartitionsCount`, T11-1 to T11-7 | none |
| 4. Correlated apply bug and repair tool | 10 accepted risk under clause 3's exception | R02 | `PartitionStorageErrorRate` | none |
| 5. A stalled partition stalls most produces, story 83 | 04 fix | F70 | T04-1 | none |
| 6. Benchmark criteria, load shedding | 16 criteria, 15 in-flight limits | F80 for load. None for criteria. | T15-1 to T15-6, the benchmark list | X27 |
| 7. Sweep, footers, partial-commit release | Scanner and footer as future work, leak as accepted risk | R10, R01 | `FileUploadErrorRate` | none |
| New risk: lost-tail amplification | 10 accepted risks, 17 model and fault-injection suite | R03 to R06 | Procedures, T17-M, T17-F | X01 |
| New risk: tracking partition as hub, fallback path, fence storm | 02, 04, 06 fixes | F61, F62, F70, F99 | T02-2, T02-3, T04-2, T06-1 | F99 is derived (X26) |
| New risk: lost release report has no metric | 05 metrics | R12 | M15 to M20 | none |
| New risk: co-tenancy and fsync cost unmeasured | 12, 13, 14, 16 | F76 to F79, R14 | T13-1, co-tenancy benchmark | X27 |
| Close 1: footer | 09 deferred, R01 instead | R01 | as finding 3 | none |
| Close 2: soft-delete and rate cap | 10 deferred, clause 3's exception | R02 to R07 | as their rows | none |
| Close 3: benchmark criteria and admission cap | 16, 15 | F80 | as finding 6 | X27 |
| Close 4: story 83 | 04 | F70 | T04-1, S83 | none |
| Close 5: model check and lost-tail fault injection | 17 | none needed | T17-M, T17-F | none |
| Close 6: enforced RF and rack spread | 11 | F71 to F75 | T11-1 to T11-7 | none |

### Fable's review

| Finding | Closed by | Row | Test or signal | Gap |
| --- | --- | --- | --- | --- |
| 1. Empty tracking part fences in-flight commits | 01 fix | F95, derived | T01-1, named | X26 |
| 2. Fallback lease's idle timer cuts the live range | 02 fix | F61 | T02-2, named | none |
| 3. First file after an idle period fails | 02 fix | F60 | T02-1, named | none |
| 4. Retired marker rejects a re-registered ID | 03 fix | F65 | T03-1, named | none |
| 5. Topic deletion inferred from absence | 03 fix | F67 | T03-4, named | none |
| 6. Release-report state machine | 05 fix | F96, F97, derived | T05-1, named, T05-2 | X26 |
| 7. SQLite checkpoint on the shard thread | 13 fix | F76 to F79 | T13-1 to T13-4 | none |
| 8. Fsync amplification on small clusters | 14 decisions, 16 benchmark at 3 brokers first | R14 | runtime and commit metrics, partition-count benchmark | none |
| 9. Release traffic isn't batched | 05 batching | F98, derived | T05-6 | X26 |
| Tests: five missing tests | Named tests of tickets 01, 02, 03, 05 | as above | T01-1, T02-1, T02-2, T03-1, T05-1 | none |

## Sections

Draft of what the rewrite keeps, changes, adds, or removes in v2's `design.md`, and which tickets drive each change.

| Section | Action | Driving tickets |
| --- | --- | --- |
| Changes from version 1 | changed: becomes "Changes from version 2", one bullet per v3 ticket | all |
| Goals | changed: the data-loss bar and the performance bar from the map's Notes | map, 16 |
| Non-goals | changed: "Produce gateways" leaves, "The benchmark question" leaves, and the footer, soft delete, and empty restart point at Future work | 09, 10, 16, 19, 20 to 24 |
| Topology | changed: minimum RF, override, maximal spread, creation trigger, default `N` reason, 3 brokers as the smallest cluster | 11, 14 |
| Flush policy | changed: flushes per file and per broker, adaptive linger | 12, 14 |
| Coordinator partitions | kept | |
| Topic protection | changed: the rack check on reassignments | 11 |
| Transport | kept | |
| Log records | changed: finish report in file-committed and sequences-leased, finish-reported record, `retiredThrough`, `metadataOffset`, batched groups in release and fence records | 01, 02, 03, 05, 06 |
| Record compatibility | changed: a stop rolls back to the record's savepoint | 13 |
| Speculative state | changed: the overlay cap | 15 |
| Write path | changed: the follower applier thread | 13 |
| Commit and retries: Sequence source | changed: the tracking selection interface | 22 |
| Leases | changed: keepalives, abandoned ranges, lease release, planned-sequence wait, ordered list | 02, 17, 22 |
| Commit path | changed: request completion, finish report on the tracking part, tracking part under gateways | 01, 04, 22 |
| Broker rows, Acceptance rules, Ending a broker epoch | changed: `retiredThrough` and the narrowed registration sentence | 03, 17 |
| Commit on the leader | changed: unreconciled partitions | 03 |
| Deadline and unknown outcomes | changed: the urgent report and the backoff | 02, 15 |
| Apply rules | changed: the unreachable case and the savepoint | 03, 13 |
| Object tracking and deletion: Tracking partition, Releases, Fences, Ending a lease, Orphan cases, Remaining leak, Rejected alternatives | changed | 01, 02, 05, 06, 17, 22 |
| Emptiness check, Deletion queue, `FileCleaner`, Deletes by the broker | kept | |
| Time bounds | changed: keepalive and urgent flush rows | 02 |
| Local state | changed: checkpoint thread, version floor, one transaction per advance | 13 |
| SQLite schema version, Snapshots | kept, with 16's byte-trigger rule | 16 |
| Reads | changed: read pool, asynchronous `findBatches`, read deadline and slots | 15 |
| Thread hold | removed | 15 |
| Gateway routing | added: assignment, routing policy, tracking part under gateways, switch and exclusion | 20 to 24 |
| Capacity and isolation | changed: per-request wording, load bounds, outgrown `N`, gateway load | 04, 14, 15, 21 |
| Benchmarks | replaced | 16, 23 |
| Metrics, Failure modes, Testing, Terms | replaced or changed by this ticket | 18 |
| Leader change, Maintenance, Consolidation and cross-tier offsets | kept | |
| Lifecycle | changed: the `metadataOffset` gate | 03 |
| Rollout | changed: routing turns on with the feature, the topic, and the switch | 23, 24 |
| Private RPCs | changed: finish report, `Recorded` flag, keepalive, lease release, batched groups, asynchronous `findBatches`, `COORDINATOR_LOAD_IN_PROGRESS` | 01, 02, 03, 05, 06, 15 |
| Future work | changed: FW06 to FW13 | 09, 10, 19, 21 to 24 |
| Open items | changed: remove the linger, the overlay cap, the benchmark criteria, and the tracking-hash encoding; add the override name, the SQLite driver floor and checkpoint trigger, the provisional defaults with their rules, and the murmur2 re-measure | 11, 13, 14, 15, 16, 21, 22 |

`spec.md`: Out of Scope loses "Produce gateways and preferred produce brokers", and its User Stories, Implementation Decisions, Testing Decisions, and Further Notes follow the decisions above.
