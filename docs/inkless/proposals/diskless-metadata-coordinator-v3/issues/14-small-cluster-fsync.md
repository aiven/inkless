# Fsync amplification and default N on small clusters

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 12

## Question

Is the default coordinator partition count fixed at 8, or does it depend on the cluster, given the fsync cost per file?

On a 3-broker cluster with `N=8`, every broker fsyncs all 8 coordinator partitions on each commit round, and one file costs up to (participants + 1) × replication factor fsyncs. See [Fable's amplification suggestion](../reviews/fable.md#fsync-amplification-on-small-clusters), v2's [Topology](../../diskless-metadata-coordinator-v2/design.md#topology), and [Capacity and isolation](../../diskless-metadata-coordinator-v2/design.md#capacity-and-isolation). The counts are in [Fsync and checkpoint cost per commit](12-fsync-checkpoint-cost.md).

Settle these points:

- The default for `diskless.metadata.topic.num.partitions`: a constant, or derived from the broker count at first enable, given that `N` is fixed after creation and must be a power of two.
- Whether the smallest supported cluster size is stated, and which cluster the benchmarks run first.
- What a cluster that grows past its creation-time `N` gives up, given that splitting partitions is future work.

## Resolution

Resolved on 2026-11-13 in a grilling session.

The default `N` stays a constant 8, provisional until the benchmarks. v3 states 3 brokers as the smallest supported cluster and benchmarks it first. The append linger defaults to adaptive. A cluster that outgrows its `N` keeps correctness and loses commit headroom, which v3 records as an accepted performance limit.

### What remains of the flush storm

Closed tickets already cut or bound three of v2's four sources of flushes:

- Commit-time fences for an empty tracking part: gone, per [When the tracking partition fences silent participants](01-silent-participant-fences.md).
- Release traffic: batched into sequence ranges with one report in flight per tracking partition, per [Release reports: ordering, batching, and a leak signal](05-release-reports.md).
- Fences after a correlated failure: one batched request in flight per participant, per [Fence fan-out under correlated failure](06-fence-fan-out.md).

The commit fan-out remains. One file costs up to `parts × R` flushes, at most `N × R`, and each broker flushes between `N × R / B` and `N × R` times per commit round. Each leader receives about `B` parts per round, and each broker, as leader, handles about `N` parts per round. So a larger `N` adds parts and flushes on every cluster. It buys more single writers in parallel, a smaller blast radius, and smaller state per coordinator partition. A small cluster stands out because every broker hosts every coordinator replica, so even the lower bound is high: 8 flushes per broker per round at `B=3, N=8, R=3`.

### Default partition count

- `diskless.metadata.topic.num.partitions` keeps a constant default of 8, still marked provisional. The partition-count benchmark at the smallest supported cluster confirms or replaces it.
- The default doesn't depend on the broker count. [Enforced replication factor and rack spread](11-replication-guard.md) creates the topic once the registered, unfenced brokers reach the replication factor, so on a new cluster the controller sees about 3 brokers at creation. A derived default would fix every new cluster at its small-cluster `N`, and `N` can't grow after creation.
- An operator can still set a lower power of two at creation. Going lower is cheap before creation, and a default that's too low on a cluster that grows can't be undone until splitting exists.

Rejected alternatives:

- A default derived from the broker count at creation, with creation delayed until a target broker count. It brings back the quiet period that [Enforced replication factor and rack spread](11-replication-guard.md) rejected, and it still guesses.
- No default, so the operator must set `N`. It moves the same guess to every operator without data to make it.
- A default of 4. It halves parts and flushes on every cluster, but it caps an unconfigured cluster at 4 single writers for good.

### Smallest supported cluster and benchmark order

- v3 states 3 brokers as the smallest supported cluster. The limit follows from the minimum replication factor of 3 and the creation trigger in [Enforced replication factor and rack spread](11-replication-guard.md): below 3 brokers, the coordinator never starts.
- The first benchmark is v2's partition-count benchmark at `B=3`, with `N` of 1, 2, 4, and 8. It records appends per commit part, so the result shows how much the linger merges. Its result decides whether the default of 8 holds and whether the deployment docs recommend a lower `N` for small clusters.
- [Benchmark pass criteria](16-benchmark-pass-criteria.md) owns the workloads and thresholds.

Rejected alternative: leaving the smallest size implicit in the replication guard and keeping v2's benchmark order, which names no cluster size.

### Append linger

- The append linger defaults to adaptive, as the group coordinator's `group.coordinator.append.linger.ms` does with `-1`. A fixed linger stays available as a config. This decision removes "The append linger default" from v2's open items.
- v2 already depends on adaptive linger: the barrier row in [Leader change and reads](../../diskless-metadata-coordinator-v2/design.md#leader-change-and-reads), its seam test "Barrier rate under adaptive linger", and the rejected fixed linger in [Thread hold](../../diskless-metadata-coordinator-v2/design.md#thread-hold).
- A fixed linger saves little on a small cluster. Brokers' commit timers aren't aligned, so at `B=3` a shard receives about 12 parts per second, about 80 ms apart. A shorter linger merges almost nothing, and a linger near the 250 ms commit interval adds up to that much latency to every part. A full merge cuts flushes by at most a factor of `B`.

Rejected alternatives: a fixed linger as the default, and leaving the default to the implementer.

### Accepted risk: a cluster outgrows its partition count

- Effect: each coordinator partition has one writer, and its leader receives about `B` parts per round. With `N` fixed, the load on each leader grows linearly with `B`, so the `N` writers cap commit throughput. State per coordinator partition grows, which lengthens elections and snapshots. Correctness, the data-loss bar, and the blast radius of `1/N` don't change.
- Performance bar: the Postgres backend sends every broker's commits to one database server. At `N=8`, the cluster has 8 writers spread across brokers. v2's commit-throughput benchmark measures where the cap sits.
- Signal: `event-queue-time-ms` and `batch-flush-time-ms` from the coordinator runtime metrics on brokers that lead coordinator partitions, and P99 `FileCommitTime` on every broker. No new metric is needed.
- Procedure: no fix in place exists until splitting. At creation, the deployment docs say to choose `N` for the cluster's expected peak size. After creation, the operator reassigns coordinator leaders to less loaded brokers or brokers with faster disks, within the rack-spread rule of [Enforced replication factor and rack spread](11-replication-guard.md).

Rejected alternatives: pulling partition splitting into v3, which the map rules out of scope, and a dedicated saturation metric for each coordinator partition, which the runtime and commit metrics already cover.

### Relation to the gateway effort

Produce gateways cut the commit fan-out at every size from 2 brokers per rack up. Each leader receives parts only from its gateways, `n_racks × g` per round, so the cluster sends about `N × n_racks × g` parts and up to `R` times as many flushes per round, against `B × N` without gateways. At `N=8` with 3 racks, a file sends 4 parts at 6 brokers, 2 at 12, and 1 from 24 up, against 8. Only at 1 broker per rack do gateways change nothing. Under the gateway effort's split writer, a broker below `N` per rack uploads one single-coordinator object per gateway coordinator partition per tick, so the split multiplies object-storage PUTs by up to `⌈N / b_r⌉`. Produces that reach a non-gateway broker add parts on top. [Gateway benchmark](23-gateway-benchmark.md) corrects the earlier wording, which said gateways leave fan-out unchanged below `N × n_racks` brokers. Gateway routing is on by default once the coordinator feature is finalized, with no broker-count threshold, so the 3-broker benchmark runs with it on, where it routes exactly as with it off.

### Consequences for the rewrite

- `design.md` Topology: the default of 8 stays provisional, with the reason it doesn't depend on the broker count. The design states 3 brokers as the smallest supported cluster.
- `design.md` Flush policy: the flush count per file and per broker from this ticket replaces "one fsync per linger batch", and adaptive linger is the stated default.
- `design.md` Capacity and isolation: the accepted performance limit for a cluster that outgrows `N`, with its signal and procedure. Benchmarks start with the partition-count benchmark at `B=3`.
- `design.md` Open items: "The append linger default" leaves.
- The deployment docs say to choose `N` for the expected peak cluster size.
