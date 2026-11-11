# Flush policy under Kafka parity

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

Does `__diskless_metadata` keep `flush.messages=1`, and if a weaker flush mode exists, how is it exposed?

v1 creates the topic with `flush.messages=1`, and it names Kafka's default flush as a fallback if the benchmark finds the fsync cost too high. See [Topology](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#topology). The review asked for that fallback to be an explicit unsafe mode. Under the Kafka-parity bar, a correlated power loss that loses acknowledged records is a limitation that classic topics share. The case for `flush.messages=1` is that the coordinator's damage differs in kind: a lost tail can reuse offsets across 1/N of all diskless partitions, and it can bring back batches that point into objects that `FileCleaner` already deleted. Postgres fsyncs its WAL on every commit, so `flush.messages=1` with linger batching is near Postgres cost.

Settle these points:

- Whether a correlated power loss on the coordinator stays within what classic-topic users accept, or causes corrupted reads that classic topics don't have.
- The default.
- If both modes exist, whether the weaker one is a named unsafe mode or a plain topic config, and what the broker logs or reports when it's on.

## Resolution

Resolved in a grilling session on 2026-11-09, for the multi-partition lease model that [Ownership model: intents or leases](01-ownership-model.md) chose. `__diskless_metadata` requires `flush.messages=1`. v2 drops the fallback to Kafka's default flush and doesn't expose a weaker flush mode.

### Kafka baseline

- `UnifiedLog.append` flushes after an append on the leader or on a follower only when `localLog.unflushedMessages() >= config().flushInterval` (`storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java:1300`). With the default `flush.messages` of `Long.MAX_VALUE`, `UnifiedLog` flushes only on segment roll (`UnifiedLog.java:2238-2240`) and relies on operating-system writeback in between.
- Upstream coordinator topics (`__consumer_offsets`, `__transaction_state`, and `__share_group_state`) don't set `flush.messages` (`server/src/main/java/org/apache/kafka/server/DefaultAutoTopicCreationManager.java:239-262`).
- KRaft's metadata log flushes every batch on the leader (`raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java:682`) and on every voter follower before its next fetch (`KafkaRaftClient.java:1852-1857`). A voter never loses a record it acknowledged.
- With `flush.messages=1` on `__diskless_metadata`, the append linger in `CoordinatorRuntime` batches reservations into one `MemoryRecords` append, so `UnifiedLog.append` runs one `LogSegment.flush()` per linger batch on the leader and one per fetched batch on each follower. That matches the Postgres backend, which fsyncs its WAL on every commit.

### Unflushed tails and Kafka parity

On a classic topic, a correlated power loss across the ISR truncates only the unflushed tail at the end of that partition. On `__diskless_metadata`, the coordinator's state spans a broker's tracking partition, the participant partitions that hold the file's batches, and external object storage, which doesn't roll back when brokers lose power. Under the rules from [Broker-side deletion after a partial send](02-delete-after-partial-send.md) and [Time bounds in the chosen model](03-time-bounds.md), an unflushed tail breaks invariants across those components and causes corrupted reads (`KeyNotFoundException` on live batch rows):

1. Cross-partition release inversion deletes an object that a participant still references, with no clock skew needed:
   - Object $K$ at $(e, s)$ committed earlier on participant $P$ and tracking partition $T$, and its file-committed records are already on disk.
   - On participant $P$, a batch-removing record $R_P$ (log pruned from consolidation, records deleted, retention applied, or deleted log purged) removes $P$'s last batch for $K$. $P$ applies $R_P$ to SQLite (`synchronous=NORMAL`) and sends a release report to $T$.
   - $T$ appends a participant-state-recorded record $R_T$. That completes the released participant set, so $T$'s apply puts $K$ into $T$'s deletion queue.
   - Because operating-system writeback and segment rolls happen independently on $T$'s replicas and $P$'s replicas, $R_T$ can reach disk on $T$ while $R_P$ is still in $P$'s page cache.
   - If power fails across the cluster before $P$ writes back $R_P$, $P$'s log truncates before $R_P$. On reboot, $P$'s SQLite either rolled back before $R_P$ or fails the open check (`appliedOffset > logEndOffset`) and rebuilds from its snapshot plus the truncated log. Either way, $P$ has the batch rows for $K$ back in active state. If $R_P$ was a log-pruned record, $P$'s `disklessLogStartOffset` also moves backward, routing fetches for consolidated offsets back to the WAL object $K$ instead of tiered storage.
   - $T$ recovers with $K$ still in its deletion queue. After the 1-minute `file.cleaner.retention.period.ms` grace period, `FileCleaner` on $T$ deletes $K$ from object storage while $P$ still serves batch coordinates for $K$. Fetches for those offsets fail with `KeyNotFoundException` (`KAFKA_STORAGE_ERROR`). The same failure happens if `FileCleaner` on $T$ already deleted $K$ before the crash while $R_P$ was still unflushed on $P$, such as under delayed writeback on $P$ or the deleting-leader wall-clock skew from [Time bounds in the chosen model](03-time-bounds.md#grace-period).
2. A lost `sequences-fenced` tail at a participant lets a late part commit into a key queued for deletion:
   - Broker $B$ uploads $K$ at $(e, s)$ with batches on $P$ and an empty part on $T$ that includes participant set $\{P\}$. Under [Broker-side deletion after a partial send](02-delete-after-partial-send.md), $B$ never deletes $K$ directly.
   - $T$'s empty part commits first while $P$'s part is still in flight. Because $T$ doesn't store batches for $K$, $T$ sends a sequence fence for $(e, s)$ to silent participant $P$ (or a missed-sequence fence after $s+1$ commits at $T$, or a range fence $[a, b]$ after $T$'s `CoordinatorTimer` idle lease timeout fires).
   - $P$ appends a `sequences-fenced` record and raises its fenced-through mark $f$, then replies `released` to $T$. $T$ queues $K$ in its deletion queue.
   - If $P$'s ISR loses the unflushed `sequences-fenced` record in a power loss while $B$ stays in epoch $e$ and its commit part for $(e, s)$ retries within the commit deadline, $P$ recovers with its floor below $s$ and commits $(e, s)$. Once the grace period elapses on $T$, `FileCleaner` deletes $K$ from object storage while $P$ holds live committed batches in $K$.
3. A lost `sequences-leased` tail at a tracking partition leaves committed batches untracked:
   - $T$ appends `sequences-leased` for $(e, a, b)$ and confirms the lease to $B$, then $T$'s ISR loses that unflushed record while $B$ stays up in epoch $e$.
   - When $B$ commits a file with $s \in [a, b]$, participants accept and commit their parts (only $T$ checks the lease table; participants check only their floor), while $T$ rejects its part with `INVALID_REQUEST`. The batches on the participants are live and visible to consumers, while $T$ has no lease range covering $s$ and $B$'s next renewal picks $a' > b$, so $T$ never tracks $K$.
4. Offset reuse across $1/N$ of diskless partitions, cache divergence, and switch state:
   - A lost file-committed tail on $P$ reuses offsets across every diskless partition mapped to $P$ ($1/N$ of the cluster). Any broker that stayed up still holds pre-crash `BatchCoordinateCache` entries that map those offsets to the old WAL object while cache misses return the new WAL object, serving different records for the same offset until the cache entries expire.
   - A lost diskless-log-initialized tail on $P$ after KRaft committed the classic-to-diskless seal and `InitDisklessLogManager` finished (`core/src/main/scala/kafka/server/InitDisklessLogState.scala:275-295`) leaves the switched partition uninitialized in $P$ until a manual `repairDisklessLog`.

### Decision

- The controller creates `__diskless_metadata` with `flush.messages=1`.
- The coordinator doesn't offer a weaker flush mode, neither as a named unsafe mode nor as a topic config override. An accepted risk can't lose data beyond what Kafka parity allows, and the benchmark question that motivated v1's fallback is out of scope for v2.

### Consequences for other tickets and the rewrite

- [Admin protection for `__diskless_metadata`](06-admin-protection.md) rejects config alters that change `flush.messages` on `__diskless_metadata`, and covers the test that the controller creates the topic with `flush.messages=1` and rejects altering it.
- In `design.md` and `spec.md`, the `Topology` section updates the `flush.messages=1` rationale to the multi-partition lease model, replacing v1's "a lost intent leaves an object with no owner" with the cross-partition release inversion, `sequences-fenced`, and `sequences-leased` cases. The fallback sentences in `Capacity and isolation` and `Open items` leave the design.
