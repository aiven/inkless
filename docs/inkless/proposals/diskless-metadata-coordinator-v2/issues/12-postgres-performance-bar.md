# Check against the Postgres performance bar

Type: research
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

Does any decision in the map's Decisions so far add blocking or a high per-operation cost on the produce, commit, or read path, compared with the Postgres backend?

The map's performance bar is "no worse than the Postgres backend": no unnecessary blocking on the produce, commit, or read path, and no change that adds high per-operation complexity. Each ticket argued its own cost, but no one has checked the decisions together against what the Postgres backend does per operation.

Find, in the Inkless checkout and the local Kafka checkout at `/Users/viktor.somogyi-vass/dev/github/apache/kafka`:

- The Postgres baseline: what a commit, a `findBatches`, a `listOffsets`, and a `FileCleaner` cycle cost there, in round trips, transactions, and locks, with citations to the Postgres control plane and its SQL functions.
- For each decision in Decisions so far, which path it touches, what it adds per operation or per event, and whether that cost waits on the produce, commit, or read path. A cost that runs only on a failover, a registration, or another rare event counts as off the path, but say how often the event can happen.
- The barrier record that [Read consistency after a leader change](07-read-consistency.md#barrier-for-an-offset-above-the-high-watermark) adds to `findBatches` above the high watermark: how often a consumer that keeps up or a consumer that polls at the end of the log reaches it, what one barrier costs on the coordinator partition and its replicas, how the 500 ms deadline compares with a fetch's long poll, and what Postgres answers for the same offset.
- The registration-time scan in [Guard for last-known-leader election on `__diskless_metadata`](10-last-known-leader-guard.md#decisions): what `ClusterControlManager.registerBroker` and `handleAssignReplicasToDirs` iterate to find the broker's `__diskless_metadata` replicas, whether that cost grows with the cluster's partition count or only with the topic's, and whether it runs on the controller's event thread.
- `flush.messages=1` from [Flush policy under Kafka parity](04-flush-policy.md): the fsync per append on the coordinator partition's leader and followers, how `CoordinatorRuntime` batches records per append within a linger interval, and how that compares with a Postgres commit's WAL flush.
- Any other decision that turns out to add a cost on those paths, such as the idle lease timer rescheduling per part from [Time bounds in the chosen model](03-time-bounds.md#idle-lease-timer), or the apply-time tag registry check from [Record compatibility across broker versions](08-record-compatibility.md#tagged-fields-and-inkless-extensions).

## Resolution

Resolved by research on 2026-11-10. The findings, with citations, are in [postgres-performance-bar.md](../research/postgres-performance-bar.md).

No decision adds blocking or a high per-operation cost to the normal produce, commit, or read path beyond what the Postgres backend pays. A Postgres commit is one transaction that locks every `logs` row in the file, and each broker commits on one thread. `findBatches` and `listOffsets` are one lock-free read transaction each, and every broker runs a `FileCleaner` cycle every 2 minutes. Against that baseline, v2's normal-path additions are constant or linear in work the commit already does: one parallel part per file to the tracking partition, one timer reschedule there, one `files` index entry, one tag check per applied struct, and one fsync per flushed batch on the leader and on each follower.

- Read consistency fails the bar as worded. Ticket 07 passes the 500 ms barrier deadline as the `CoordinatorRuntime` write timeout, but the runtime has one write timeout for every write, so every commit part would time out at 500 ms. The smallest fix is a per-operation timeout, which `CoordinatorWriteEvent` already takes internally, or a timeout on the barrier's own future.
- A consumer that keeps up never reaches the barrier, because an offset equal to the high watermark is in range. Only a deposed leader, a seek past the end, or a client that keeps fetching past the end reaches it. Postgres answers `OFFSET_OUT_OF_RANGE` at once, and with a read pool on a lagging standby, it can answer a false one.
- The barrier wait holds a thread on the calling broker, because `ControlPlane.findBatches` is synchronous and runs on a fetch metadata pool of 8 threads. With the batch coordinate cache turned off, the delayed-fetch probe calls it from `tryComplete`, so the probe path must never run the barrier.
- A fetch above the high watermark can wait the long poll plus 500 ms, not one long poll, because the barrier runs after the delayed fetch's wait. The bound of one barrier per linger interval assumes a fixed linger, and v1 leaves the linger default open.
- The registration guard from ticket 10 runs on the controller event thread, once per broker process start. `lastKnownElr` has no index, so the guard must iterate only the partitions of `__diskless_metadata`, at `O(N × RF)`. A pass over every topic would grow with the cluster.
- `flush.messages=1` costs one fsync per flushed batch on the leader's coordinator thread and one per fetched batch on each follower. That matches a Postgres commit's flush count on the primary, but the follower fsync is a second sequential flush that Postgres matches only with a synchronous standby. The sources don't show the Postgres server's settings.

The deadline mechanism, the thread hold, the fetch latency wording, the barrier rate under adaptive linger, and the guard's scope need decisions in [Consequences for the cross-cutting sections](11-cross-cutting-consequences.md).
