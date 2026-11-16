# Benchmark pass criteria

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 01, 13, 14, 15

## Question

Which workloads, scales, and thresholds against the Postgres backend decide whether v3 meets the performance bar?

v2's [Benchmarks](../../diskless-metadata-coordinator-v2/design.md#benchmarks) lists benchmark kinds but says the workloads, scales, and pass criteria "need more design". All five council members ask for pass criteria, including a produce-latency test under partial failure. See the council's [third fix](../reviews/council.md#what-would-close-the-gaps). Fable asks to benchmark the smallest supported cluster first. Running the benchmarks is out of scope.

Settle these points:

- The workloads and scales, starting with the cluster that [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md) names.
- For each benchmark, the metric and the threshold relative to Postgres that passes, including commit latency P99, `FindBatchesTime`, co-tenancy on a leader broker, failover time, and snapshot transfer.
- The partial-failure produce-latency test: one coordinator partition slow or unavailable, and the produce latency for files that don't touch it and files that do, consistent with [File completion and story 83](04-file-completion.md).
- What a failing result changes in the design, replacing v2's list of possible outcomes.
- Inter-AZ traffic: inter-AZ bytes per produced MB, coordinator against the Postgres backend, at the smallest supported cluster and at a mid-size cluster, and the threshold that passes. v2 never prices coordinator traffic between AZs. Part requests, fences, lease requests, release reports, and the `RF - 1` follower copies of every coordinator record all cross AZs. The Postgres backend crosses them too, from the broker to the primary and from the primary to its replicas. If the result shows a gap, a design change such as AZ-aware leader placement for `__diskless_metadata` becomes its own effort.

Starting figures from [Release reports: ordering, batching, and a leak signal](05-release-reports.md#batching): one unbatched release costs about 570 cross-AZ bytes per object and participant, mostly for the follower copies of its two records. Each record costs about 100 bytes per follower copy. The commit path sends a part to every participant of each file, so it likely outweighs release traffic several times over.

Constraints from [File completion and story 83](04-file-completion.md):

- Produce latency is judged with Kafka's per-request produce metrics, not `FileCommitTime`. `FileCommitTime` stays per file and measures the slowest part.
- The partial-failure benchmark measures requests, not files. It passes only if requests that hold no batch for the offline coordinator partition don't wait for its deadline.

Constraints from [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md):

- The first benchmark is the partition-count benchmark at `B=3`, the smallest supported cluster, with `N` of 1, 2, 4, and 8. Its result confirms or replaces the provisional default of 8 and decides whether the deployment docs recommend a lower `N` for small clusters.
- The benchmarks record appends per commit part under the adaptive linger default, so the result shows how much the runtime merges parts.
- The commit-throughput benchmark finds where a fixed `N` caps commit throughput as `B` grows, the accepted performance limit in that ticket.

## Resolution

Resolved on 2026-11-13 in a grilling session.

The baseline is the Postgres backend in its production configuration. A latency comparison passes when the coordinator's P50 and P99 are each within 1.1 times Postgres's at the same throughput. The benchmarks run at 3, 12, and 24 brokers on one base workload. Every benchmark has a pass rule, and every possible result maps to a new default, a docs change, a release blocker, or a separate effort. Nothing is left to judge after a run.

This resolution closes the council's [third fix](../reviews/council.md#what-would-close-the-gaps) for pass criteria and Fable's request to benchmark the smallest supported cluster first. The admission-cap half of the third fix belongs to [Admission cap or retry budget](15-admission-cap.md).

### Baseline and pass rule

- The baseline is Postgres as Inkless deploys it in production: a managed primary on its own host, with its production replication settings. The follower flush that Kafka parity requires counts fully against the coordinator. The report records the baseline's `synchronous_commit` and standby settings next to the result.
- The latency metric is Kafka's per-request produce metric, `RequestMetrics` `TotalTimeMs` for `request=Produce`, per [File completion and story 83](04-file-completion.md). `FileCommitTime` is recorded, not judged.
- Pass rule for every latency comparison: the coordinator's P50 and P99 are each at most 1.1 times Postgres's, at the same produce throughput.
- Run protocol: each side runs 3 times, 30 minutes each, after a 10-minute warmup. The comparison uses the median of the 3 runs.

Rejected alternatives:

- A synchronous-standby baseline, alone or as a reference run. It's fair on durability, but it isn't what users run, and the bar means what users run.
- A strict rule with no band, which fails on run-to-run noise.
- Judging P99 alone. It lets a constant cost on every commit, such as the second flush, pass.

### Sizes and base workload

- Sizes: 3, 12, and 24 brokers, spread evenly over 3 racks with one rack per AZ. With `N=8` and `R=3`, every broker hosts all 8 coordinator replicas at `B=3`, about 2 at `B=12`, and about 1 at `B=24`. 24 is also `N × n_racks`, where the gateway effort's fan-out stops tracking `B`.
- Produce: 16 MiB/s per broker. A file closes after 250 ms or 8 MiB, so each broker commits 4 files per second on the timer.
- Topic shape: 64 diskless partitions per broker, so each file holds batches for all 8 coordinator partitions, the worst-case fan-out.
- Producers: one per broker, in the broker's AZ, with `acks=all`, 1 KiB records, `linger.ms=100`, `batch.size=16384`, and the default sticky partitioner. Each producer writes every diskless partition in the cluster. The sticky partitioner moves to the next partition when a batch fills, so a 250 ms file at 16 MiB/s holds about 256 batches and reaches every coordinator partition. With `PERFORMANCE.md`'s `batch.size=1000000`, a file holds about 4 batches. `RoundRobinPartitioner` reaches about 150 records per second on a diskless topic, because each partition's batch becomes ready on its own linger timer.
- Consumers: one group that reads every partition at the tail.
- The Postgres run uses the same brokers, producers, and consumers, with only the control plane backend switched.

Rejected alternatives: 3 and 12 brokers only, which says nothing about the sizes the accepted limit "a cluster outgrows its partition count" is about. 3 brokers only. A lighter rate or fewer partitions, which understate the fan-out.

### Partition count

- Matrix: the base workload at `B=3` with `N` of 1, 2, 4, and 8, and at `B=12` and `B=24` with `N` of 4, 8, and 16.
- Recorded per run: produce P50 and P99, P99 `FileCommitTime`, parts per leader per second, appends per commit part, flushes per broker per second, and CPU on leader brokers.
- Decision rule:
  - If `N=8` passes at all three sizes, the default stays 8 and isn't provisional anymore.
  - If `N=8` fails only at `B=3`, the default stays 8. The deployment docs recommend the largest passing `N` for clusters that won't grow past the size where 8 passes.
  - If `N=8` fails at `B=12` or `B=24`, the default becomes the largest `N` that passes at both sizes.
  - If `N=16` passes at `B=24` and does better than `N=8`, the docs recommend 16 for clusters expected to reach that size. The default doesn't change.
  - If no `N` passes at some size, the bar fails at that size, with the outcome in [What a failure changes](#what-a-failure-changes).

### Commit throughput

- Saturation point: the highest load at which a backend's produce P99 stays within 2 times its own P99 under the base workload, with a zero `FileCommitErrorRate`.
- Rate sweep: at each of the three sizes, the rate per broker rises from 16 MiB/s in 8 MiB/s steps. Above 32 MiB/s, files close on size, so the file rate and the parts per leader climb.
- Broker sweep: at 16 MiB/s per broker, `B` grows from 24 in steps of 12, up to 96.
- Pass: in the rate sweep, the coordinator's saturation rate is at least Postgres's divided by 1.1 at each size. In the broker sweep, the coordinator reaches at least the broker count where Postgres saturates. If neither saturates by 96 brokers, it passes.
- The broker count where `N=8` saturates goes into the deployment docs' guidance to choose `N` for the expected peak cluster size.

Rejected alternative: v2's rule on P99 `FileCommitTime`, which measures the slowest part, not what clients see.

### Partial failure

- Load: the base workload, plus one narrow producer per broker at 1 MiB/s. A narrow producer writes only to user partitions that map to one healthy coordinator partition `P1`. Without it, almost every request touches every coordinator partition, and nothing measures the requests that avoid the failed one.
- Fault: a test-only hook on coordinator partition `P`, which is neither `P1` nor any broker's tracking partition. After a 10-minute steady phase, it runs two modes, 5 minutes each: `P`'s leader accepts no part, then every append on `P` waits an extra 500 ms.
- Measured: client-side produce latency per producer, part latency per coordinator partition, and errors per user partition. The broker's `TotalTimeMs` can't split requests that touch `P` from requests that don't.
- Pass, against the coordinator's own steady phase, because Postgres has one queue and no partial failure:
  1. Narrow requests keep P99 within 1.1 times the steady phase, with zero errors.
  2. In the unavailable mode, a request that touches `P` completes within one commit deadline of its file's close. `P`'s batches get `KAFKA_STORAGE_ERROR`, and its other batches succeed.
  3. Parts on healthy coordinator partitions keep P99 within 1.1 times the steady phase.
  4. After the fault ends, produce P99 returns within 1.1 times the steady phase within 60 seconds.
- Sizes: all three.

Rejected alternative: killing a broker, which stalls every coordinator partition it leads. The failover benchmark covers that case.

### Read-heavy load and cache storm

- Read-heavy load: the base workload, plus one lagging consumer group per broker. Each group starts far enough behind the tail that every fetch misses the batch coordinate cache, and reads at 1 times, then 3 times, the produce rate.
- Read-heavy pass, against Postgres under the same read load: `FindBatchesTime` P50 and P99, and produce P50 and P99.
- Cache storm: under read-heavy load at 1 times the produce rate, every consumer restarts at once.
- Cache-storm pass, against Postgres's own storm:
  - The time until `FindBatchesTime` P99 returns to its steady value is at most 1.1 times Postgres's.
  - Produce P99 during the storm is within 1.1 times Postgres's.
  - Every consumer is back at its pre-storm fetch rate within 1.1 times Postgres's time. Read parts that fail the 1000 ms read deadline cost retries, and this check prices them.
- Read pool rule: both benchmarks run with 4 read threads first. If either fails and the read pool's queue time is the largest part of `FindBatchesTime`, they run again with 8, then 16. The default for `diskless.metadata.read.threads` becomes the smallest count that passes. If none passes, the bar fails on reads.
- Sizes: all three.

### Co-tenancy

- Load: the base workload, the read-heavy load at 1 times the produce rate, and a classic topic at 16 MiB/s per broker with `acks=all` and replication factor 3. The classic topic stands in for the broker's own partitions.
- Measured per broker: produce P50 and P99 for the classic partitions it leads and for the diskless requests it receives, GC pause P99, and `LogFlushRateAndTimeMs`.
- Pass:
  1. At 12 and 24 brokers, brokers that lead a coordinator partition keep classic and diskless produce P50 and P99 within 1.1 times the brokers that lead none.
  2. At all three sizes, each broker's classic produce P50 and P99 stay within 1.1 times its values in the Postgres run. At `B=3`, every broker leads a coordinator partition, so this is the only check.
- GC pauses and flush latency are recorded to explain a failure. They don't decide it.

Rejected alternative: v2's diskless-only comparison, which misses SQLite evicting the broker's log pages and taking its disk.

### Failover

- Faults, under the base workload at all three sizes: a `kill -9` of a broker that leads coordinator partitions, and a controlled shutdown of the same broker. On the Postgres side, the counterparts are a hard kill of the primary and a switchover, both through the managed service's failover.
- Measured: the time to first commit, from the fault until the first successful produce to a user partition behind the affected coordinator partitions or behind Postgres, and the time to steady, until produce P99 returns within 1.1 times the steady phase.
- Pass:
  1. For each fault type, both times are at most 1.1 times Postgres's.
  2. Zero acknowledged records lost. A verifying consumer reads every acknowledged record. This check also runs in the partial-failure, co-tenancy, and snapshot-transfer benchmarks.

Rejected alternative: comparing with a classic partition's leader election. The bar is Postgres's failover.

### State size

- Ladder: \(10^6\), \(10^7\), \(10^8\), and \(10^9\) batch rows preloaded cluster-wide at `B=12`, with the base workload running at each step. Postgres gets the same rows. Each step runs with consolidated topics and without.
- Pass at each step, against Postgres at the same row count: `FindBatchesTime` P50 and P99, election time measured as the failover benchmark's time to first commit after a `kill -9`, and produce P50 and P99.
- Recorded: SQLite file size, snapshot backup duration, `SqliteWalPages` during the backup, and log growth between snapshots.
- The largest step that passes becomes the stated limit on batch rows per coordinator partition. The docs turn it into the retention and produce rate that stay under it, and they require consolidation above it.

Rejected alternative: a ladder in hours of retention, which ties the result to one workload shape.

### Snapshot transfer

- Setup: `B=12`, preloaded at the largest state-size step that passed, with the base workload and the read-heavy load at 1 times the produce rate.
- One rebuild: one follower of one coordinator partition loses its storage. Pass: on that partition's leader and on the snapshot source, produce P50 and P99 and `FindBatchesTime` P99 stay within 1.1 times the steady phase. Produce P99 during the rebuild is within 1.1 times Postgres's during a standby rebuild.
- Two rebuilds: both followers of one coordinator partition lose their storage, so the partition falls below `min.insync.replicas` until one rebuild finishes. Pass: criteria 1 to 3 of the partial-failure benchmark hold, and the time below the minimum ISR is at most 1.1 times the one-rebuild time.
- Replaced broker, at `B=3` and `B=12`: a whole broker loses its storage. Total rebuild time and the load on the source brokers are recorded.
- Transfer limit rule: the replaced-broker case runs with `diskless.metadata.snapshot.max.concurrent.transfers` of 1, 2, and 4. The default becomes the highest limit at which the one-rebuild pass still holds on the source brokers.
- Byte trigger rule: if replaying the log after the snapshot takes more than half of a one-rebuild time, `diskless.metadata.snapshot.max.bytes.between.snapshots` drops from 1 GiB to 256 MiB. Otherwise it stays at 1 GiB.

### Control traffic and retention burst

- v2's tracking-part benchmark leaves. [When the tracking partition fences silent participants](01-silent-participant-fences.md) removed normal-path fences, and [File completion and story 83](04-file-completion.md) made a request wait for the tracking part only when it holds one of the request's batches, so the base comparison already prices the tracking part.
- Every run records control traffic per tracking partition: tracking parts, finish reports, lease renewals and keepalives, release-report ranges, and `FenceRate` by type.
- Invariant on every steady-phase run: `FenceRate` is zero for the missed-sequence and exception types. A nonzero rate fails the run as a protocol defect.
- Retention burst: at `B=12`, 24 hours of base-workload objects are preloaded, then `retention.ms` drops on every diskless topic so the whole backlog expires at once. Pass: produce P50 and P99 stay within 1.1 times the steady phase until the pending-release gauge from [Release reports: ordering, batching, and a leak signal](05-release-reports.md) returns to zero. The drain time is recorded next to the time Postgres's `FileCleaner` takes for the same backlog.

### Inter-AZ traffic

- Metric: inter-AZ bytes per produced MB, from network counters by source and destination AZ, such as the cloud provider's flow logs. Clients are AZ-aligned and diskless data doesn't replicate between brokers, so in the base workload the inter-AZ traffic between brokers is coordinator traffic.
- Breakdown, from Kafka's per-API request byte metrics and replication byte metrics: commit parts, follower copies, and the lease, finish-report, release, and fence RPCs.
- Runs: the base workload at all three sizes, measured during the latency runs, and the retention-burst run.
- Pass: at each size, the coordinator's inter-AZ bytes per produced MB are at most 1.1 times Postgres's.
- Estimate: a 4 MiB file holds about 256 batches, about 25 KiB of coordinator records, or 0.6% of the produced bytes. The coordinator sends about 2.7 times the record bytes across AZs: two thirds of parts cross to a leader, and both follower copies cross. Postgres sends two thirds of commits across to the primary, and its WAL, likely 2 to 3 times the row bytes, crosses to the standby. Both sit far below a classic topic's 2 MB per produced MB.

Rejected alternative: recording the numbers without a threshold, which leaves the ticket's question open.

### What a failure changes

This list replaces v2's list of possible outcomes:

- Tunable (`N`, read threads, transfer limit, or byte trigger): the matching rule sets the new default, and the affected benchmarks run again. No design change.
- Defect (a nonzero fence rate other than lease ends, a lost acknowledged record, or a failed partial-failure criterion): a bug against the design. It blocks release until fixed and doesn't reopen the bar.
- Co-tenancy: the design adds a required, separate log directory for the coordinator log and SQLite, on its own disk, and co-tenancy runs again. If it still fails, the deployment docs say to place coordinator leaders on brokers with a lighter classic load.
- State size or `findBatches`: the stated row limit drops to the last passing step. If \(10^6\) rows fails, `findBatches` gets a cap on result size, and the benchmark runs again.
- Base latency or commit throughput, after `N` is tuned: the coordinator backend ships as opt-in, and Postgres stays the default backend. The report states the gap and the share that comes from the follower flush. Closing the gap is its own effort, such as the gateway effort or a flush-policy change, and doesn't change v3.
- Inter-AZ: doesn't block release. The deployment docs state the gap, and AZ-aware leader placement for `__diskless_metadata` becomes its own effort.

### Laptop scenarios

[laptop-bench/](../laptop-bench/README.md) holds Docker scenarios that gauge the main risks against the Postgres backend on one laptop before the coordinator exists. They measure the Postgres backend end to end and a proxy for the coordinator's commit path and SQLite, at reduced scale. They don't replace these benchmarks.

### Consequences for the rewrite

- `design.md` Capacity and isolation, Benchmarks: this resolution replaces v2's list and its closing paragraph. The tracking-part benchmark leaves, and the retention-burst and inter-AZ benchmarks join.
- `design.md` Testing: the test-only fault hook on a coordinator partition, with an unavailable mode and a delay mode, and the verifying consumer for zero acknowledged loss.
- `design.md` Open items: "The workloads, scales, and pass criteria for the benchmarks" leaves. The provisional defaults name the benchmark rule that settles each one: `N`, `diskless.metadata.read.threads`, `diskless.metadata.snapshot.max.concurrent.transfers`, and the snapshot byte trigger.
- spec.md: the benchmark line names the baseline, the pass rule, and the sizes. The capacity-risk line points at the co-tenancy benchmark.
- Deployment docs: the stated row limit from the state-size benchmark, the saturation broker count for `N=8`, and any recommendation for a lower or higher `N`.
