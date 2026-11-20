# Gateway benchmark

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 16, 21, 22

## Question

Which benchmark shows what gateways save, and at which cluster sizes are they turned on?

Gateways reduce commit parts at every size from 2 brokers per rack up: a file sends about `N / b_r` parts below `N` brokers per rack and 1 from there up, against `N` with gateways off. The working notes correct the earlier wording, which said they save only at or above the saturation point. See [Gateway assignment](21-gateway-assignment.md) and [Benchmark pass criteria](16-benchmark-pass-criteria.md).

Input, which corrects [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md#relation-to-the-gateway-effort): the cluster-wide fan-out isn't flat as `B` grows. With `max(1, B / N / n_racks)` gateways per rack, each leader receives about `B / N` parts per round, against about `B` without gateways. The cost per file stays flat, and the cluster-wide count still grows with `B`. Gateways give `N` times more headroom before a cluster outgrows its `N`, and they don't remove that limit.

[Writer split against a gateway hint](../../diskless-produce-gateways/research/split-vs-hint.md) counts 2 coordinator partitions per broker at 12 brokers instead of 8, which disagrees with the claim that gateways leave fan-out unchanged below `N × n_racks` brokers. Settle which count holds. See [Routing produce by coordinator partition](20-gateway-routing.md). Settled in the working notes: split-vs-hint's count holds.

Settle these points:

- Workloads: `B` of 3, 12, 24, 45, and 48, at `N=8` with 3 racks, with gateways on and off, and clients that send `diskless_az` against clients that don't. 45 brokers, 15 per rack, is the worst case for peak gateway load: a simulation in [Gateway assignment](21-gateway-assignment.md) puts the busiest gateway at about 1.7 times today's busiest broker there, because each gateway still carries a whole coordinator partition.
- Metrics: commit parts per file, coordinator log flushes per broker per round from `LogFlushRateAndTimeMs`, P99 `FileCommitTime`, peak `BytesInPerSec` per broker against the rack mean, and the misroute meters from [Gateway assignment](21-gateway-assignment.md): routed and misrouted commit parts per second, and misrouted bytes per second.
- The threshold against gateways turned off that passes.
- Whether gateways are on by default, on above a broker count, or turned on by an operator config.

Input from [Tracking part under gateways](22-tracking-part-under-gateways.md): a broker's tracking partition is one of its gateway coordinator partitions, so a routed file sends no extra tracking part. Commit parts per file come from the per-broker commit-parts meter, whose values are `routed`, `misrouted`, and `empty-tracking`, divided by the file count. Switches show on the renewal counter's `assignment` reason. Expected values for routed files with no misroutes, averaged over 300 random clusters per size at `N=8`, `R=3`, and 3 racks:

| Brokers | Parts per file on a gateway | Coordinator log flushes per file, upper bound | Parts per coordinator leader per round | v2's tracking partition: parts per file and per leader |
| --- | --- | --- | --- | --- |
| 3 | 8 | 24 | 3 | 8 and 3 |
| 12 | 2.0 | 6 | 3 | 2.77 and 4.2 |
| 24 | 1.0 | 3 | 3 | 1.88 and 5.6 |
| 45 | 1.0, and spares upload nothing | 3 | 3 | 1.88 and 5.6 |
| 48 | 1.0 | 3 | 6 | 1.88 and 11.3 |

A rolling restart under gateways also exercises switches. Under the default `metadata.max.age.ms`, a restart that fits inside the stale window should cost almost no switches.

Input from [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md):

- The gateway switch is a cluster-level dynamic config that brokers read from `ConfigurationsImage`, so every broker flips at the same image offset. This ticket names it and decides its default and when to flip it. Gateway routing is on only when the switch is on, the coordinator feature is finalized, and `__diskless_metadata` exists. Turning the switch on costs each producer one retry, and turning it off costs none.
- A broker that isn't a gateway rejects a misrouted batch on the gateway path with `NOT_LEADER_OR_FOLLOWER` once per connection and partition per `metadata.max.age.ms`, then accepts it. A rolling restart therefore ends stale traffic within one retry instead of up to 5 minutes, which corrects the preceding paragraph: the switches from [Tracking part under gateways](22-tracking-part-under-gateways.md) follow the image, about 90 per rolling restart of 24 brokers. Without the rejection, the 24-broker example would send up to about 139,000 extra parts per rolling restart. Measure the rejection count, the retries, and parts per file during a rolling restart.
- The commit-parts meter gets a fourth value, `off-gateway`, for parts whose batches all belong to partitions kept off gateways, and the misrouted meters count accepted batches only, next to a new rejection count. Parts per file is the sum of all four values divided by the file count.
- In Inkless v3, consolidating, tiered, and phase-1 switched partitions keep replica routing. Add a workload with consolidation on, where every file sends about 8 parts at `N=8`, and one with 10% of traffic on consolidating topics, about 4.9 parts per file at 64 partitions per broker.
- Clients without `diskless_az` count as the rack-less group. When every broker has a rack, they reach a gateway in some rack and add no part, so the workload without `diskless_az` should show no misroutes, while about 2 in 3 of their bytes cross an AZ, as under today's hash.

## Working notes

Terms used in this ticket, as [Gateway assignment](21-gateway-assignment.md) defines them:

- Gateway: a broker that routing sends one rack's clients to for a coordinator partition's user partitions. It accepts their produce, uploads the WAL file, and sends a commit part to the coordinator partition's leader. It holds no coordinator state.
- Coordinator leader: the leader of a `__diskless_metadata` partition. It assigns offsets, appends and replicates the commit record, and is the only broker that can commit a part. Being a gateway and being a coordinator leader are independent.
- Round: one file from every broker, one commit tick.
- Parts per file: how many coordinator partitions one file sends a commit part to. Parts per leader per round: how many brokers send a part to one coordinator leader in one round.

The simulations are [22-tracking-sim.py](../research/22-tracking-sim.py) and [23-gateway-load-sim.py](../research/23-gateway-load-sim.py). The second one reuses the first one's murmur2 port and capped rule, and it reproduces the load figures in [Gateway assignment](21-gateway-assignment.md). Every figure from them is an estimate for equal user partitions, not a benchmark result.

Settled during grilling, before the resolution:

- Fan-out below saturation: [Writer split against a gateway hint](../../diskless-produce-gateways/research/split-vs-hint.md)'s count holds, and the claim that gateways leave fan-out unchanged below `N × n_racks` brokers is wrong, for this design and for the split variant. The claim took the bound for unrouted traffic, up to `N` parts per file, as the value below saturation. With gateways on, a broker is a gateway for about `N / b_r` coordinator partitions, so parts per file drop from 2 brokers per rack up, and the cluster sends `N × n_racks × g` parts per round at every size, against `B × N` with gateways off. Only at 1 broker per rack do gateways change nothing. In the base workload of [Benchmark pass criteria](16-benchmark-pass-criteria.md), every file with gateways off touches all 8 coordinator partitions. Ticket 22's 2.77 at 12 brokers is gateways on with v2's fixed tracking partition, not gateways off.
  - Numbers, averaged over 1000 random racks per size at `N=8` with 3 racks, for routed files with no misroutes:

    | Brokers (per rack) | Parts per file on a gateway, on | Parts per file, off | Parts per leader per round, on and off | Cluster parts per round, on and off |
    | --- | --- | --- | --- | --- |
    | 3 (1) | 8 | 8 | 3 and 3 | 24 and 24 |
    | 6 (2) | 4 | 8 | 3 and 6 | 24 and 48 |
    | 12 (4) | 2.0 | 8 | 3 and 12 | 24 and 96 |
    | 21 (7) | 1.48, with 1.6 brokers per rack holding none | 8 | 3 and 21 | 24 and 168 |
    | 24 (8) | 1.0 | 8 | 3 and 24 | 24 and 192 |
    | 45 (15) | 1.0, with 7 spares per rack | 8 | 3 and 45 | 24 and 360 |
    | 48 (16) | 1.0 | 8 | 6 and 48 | 48 and 384 |

  - Example: at 12 brokers with IDs 1 to 12 and rack `(id − 1) % 3`, rack 0 holds brokers 1, 4, 7, and 10, with `g = 1` and a cap of 2. Their gateway sets are {0, 3}, {2, 6}, {1, 5}, and {4, 7}. Broker 1's file sends 2 parts, and coordinator partition 4's leader gets 3 parts per round, one from broker 10 and one from a gateway in each other rack. With gateways off, each file sends 8 parts and each leader gets 12 per round: 96 parts and up to 288 flushes per round at `R=3`, against 24 and 72.
  - The split variant's extra PUTs below saturation stay true. The map's cost-bar line names only those, so it doesn't change. [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md#relation-to-the-gateway-effort) gets the corrected note.
- Sizes: gateway pairs run at 12, 21, 24, 45, and 48 brokers, 3 racks, `N=8`. A pair is the same workload on the coordinator backend with the gateway switch on and off, under the run protocol of [Benchmark pass criteria](16-benchmark-pass-criteria.md): 3 runs per side, 30 minutes each after a 10-minute warmup, median of 3. Five pairs take about 20 hours.
  - What each size tests: 12, savings below saturation at 2 parts per file against 8. 21, the worst peak load below saturation. 24, saturation at 1 part per file. 45, the worst peak load overall. 48, `g = 2`, where the pick between two gateways by user partition matters and each leader gets 6 parts per round.
  - 3 brokers needs no pair, because both settings route identically. Each rack has one broker. Today's routing, `selectLeaderLegacy` in `InklessTopicMetadataTransformer`, picks it for every client in the AZ, and the rule makes it the gateway of all 8 coordinator partitions. Clients without `diskless_az` pick another broker, but every broker is a gateway of every coordinator partition, so files still send 8 parts and nothing is rejected. Ticket 16's 3-broker runs exercise the code path at the switch's default.
  - Peak load, from [23-gateway-load-sim.py](../research/23-gateway-load-sim.py), with 512 equal user partitions per rack over 300 random racks per size. The produce figure applies the ratio to the rack mean of 16 MiB/s per broker:

    | Brokers (per rack) | Busiest gateway against today's busiest broker, mean and worst | Busiest gateway against the rack mean | Busiest gateway's produce |
    | --- | --- | --- | --- |
    | 12 (4) | 1.0 and 1.18 | 1.09 | 17 MiB/s |
    | 15 (5) | 1.21 and 1.44 | 1.34 | 21 MiB/s |
    | 18 (6) | 1.41 and 1.79 | 1.61 | 26 MiB/s |
    | 21 (7) | 1.60 and 1.90 | 1.87 | 30 MiB/s |
    | 24 (8) | 1.0 and 1.24 | 1.18 | 19 MiB/s |
    | 36 (12) | 1.41 and 1.73 | 1.77 | 28 MiB/s |
    | 45 (15) | 1.70 and 2.22 | 2.22 | 36 MiB/s |
    | 48 (16) | 1.0 and 1.42 | 1.32 | 21 MiB/s |

  - At 7 per rack, the cap `⌈8/7⌉ = 2` gives at least one broker two coordinator partitions, about a quarter of the rack's traffic, so 21 brokers comes close to 45. At 45, the busiest gateway passes 32 MiB/s, where an 8 MiB file closes on size before the 250 ms timer, so it commits more than 4 files per second. No other size reaches that point under the base workload.
- Variants: four, on top of the pairs.
  1. Clients without `diskless_az`, a pair at 24 brokers. With gateways on, they reach their coordinator partition's gateway in some rack, so parts per file drop as in the base pair, and about 2 in 3 of their bytes cross an AZ under both settings. Client bytes then dominate the inter-AZ metric of [Benchmark pass criteria](16-benchmark-pass-criteria.md#inter-az-traffic), so both sides are measured.
  2. Local-log mix, on-only runs at 24 brokers with 1%, 5%, and 10% of traffic on consolidating topics. With gateways off, every file sends 8 parts whatever the mix, so the 24-broker base off-run is the off side of all three.
  3. Rolling restart, pairs at 24 and 48 brokers, one restart per run at 60 seconds per broker. [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) counts about 130 stale moves per rolling restart at 24 brokers and 450 at 48, where fencing one of 16 brokers per rack drops `g` from 2 to 1 for every coordinator partition and the return raises it again. [Tracking part under gateways](22-tracking-part-under-gateways.md) expects about 90 `assignment` switches at 24.
  4. No consolidation-on run. Every partition keeps replica routing under both settings, so both sides route identically, and ticket 24's named test 5 checks that these partitions are never rejected and count as `off-gateway`.

  The variants take about 18 hours: 4 for the `diskless_az` pair, 6 for the three mix runs, and 8 for the two restart pairs.
  - Correction to ticket 24's 10%-mix figure: it estimated about 4.9 parts per file from 6 local-log partitions per file, 10% of the 64 partitions per broker. A broker carries its rack's traffic for `n_racks × 64 = 192` user partitions' worth, so about 19 consolidating partitions route to it, and a 256-batch file touches about 14. [23-gateway-load-sim.py](../research/23-gateway-load-sim.py) gives these parts per file, with equal traffic per partition and replica routing to the rack-local replica, over 40 random clusters each:

    | Local-log share of traffic | Parts per file at 24 brokers | Parts per file at 12 brokers |
    | --- | --- | --- |
    | 0% | 1.0 | 2.0 |
    | 1% | 2.1 | 3.1 |
    | 2% | 3.1 | 3.7 |
    | 5% | 5.1 | 5.5 |
    | 10% | 6.8 | 7.0 |
    | 100% | 8.0 | 8.0 |

    Ticket 24's decision stands, and its cost is larger than stated: 1% of traffic on consolidating topics doubles parts per file at saturation.
- Metrics: each run judges four metrics and records the rest. No new metric is needed.
  - Judged:
    1. Produce `RequestMetrics` `TotalTimeMs` for `request=Produce`, P50 and P99, cluster-wide and on the busiest gateway, the broker with the highest `BytesInPerSec`.
    2. In the rolling restart, client-side latency per record, with the producer's `record-retry-rate` and `record-error-rate`.
    3. Parts per file: the sum of the commit-parts meter's four values, `routed`, `misrouted`, `empty-tracking`, and `off-gateway`, divided by the file count from `FileCommitTime`'s count.
    4. Inter-AZ bytes per produced MB, measured as in [Benchmark pass criteria](16-benchmark-pass-criteria.md#inter-az-traffic).
  - Recorded: P99 `FileCommitTime`, `LogFlushRateAndTimeMs` per broker per round, parts per leader per round, peak `BytesInPerSec` against the rack mean with the busiest gateway's file rate, each commit-parts value, misrouted bytes, the rejection count, `assignment` renewals, `event-queue-time-ms` and `batch-flush-time-ms` on leaders, and CPU on gateways and leaders.
  - Why client-side latency in the restart: a broker rejects a misrouted batch before writing it, so the rejected request's `TotalTimeMs` is short, and the retry arrives as a new request after `retry.backoff.ms`, 100 ms by default, and a metadata refresh. Only the client sees the sum.
  - Why the busiest gateway on its own: each producer sends about one request per broker per linger, so request counts per broker stay similar while sizes differ. At 21 brokers, the broker with two coordinator partitions carries about 30 MiB/s but serves only about 1 in 6 of its rack's produce requests, so the cluster P99 dilutes it.
  - Parts per leader per round come from the commit-parts sum divided by `N` and by the file rate. Every part goes to some leader, and with gateways on the rule gives every leader `n_racks × g` parts per round, so the mean is the whole picture.
- Pass rules: each pair is judged against its own off side, in the 1.1 band of [Benchmark pass criteria](16-benchmark-pass-criteria.md), on the median of 3 runs. Ticket 16's comparisons against Postgres stay as they are, at the switch's default.
  - Pairs, at each size:
    1. On-side `TotalTimeMs` P50 and P99 are at most 1.1 times the off side's, cluster-wide and for the busiest gateway against the off side's busiest broker.
    2. Steady parts per file are at most 1.1 times the run's predicted value: `N × n_racks × g` divided by the number of brokers with a non-empty gateway set, from the assignment interface for the run's actual broker IDs. Above it is a defect, and the commit-parts values show where: `misrouted` points at the assignment or the transformer, `empty-tracking` at the tracking selection.
    3. On-side inter-AZ bytes per produced MB are at most 1.1 times the off side's, including the pair without `diskless_az`.
  - Rolling restart:
    1. `record-error-rate` is zero. A failed batch means the rejection guard looped, which breaks the rule that a produce reaching any broker commits, so it's a defect.
    2. Client-side P50 and P99 over the restart are at most 1.1 times the off-side restart's.
    3. Within 60 seconds of the last broker's return, parts per file are back within 1.1 times the predicted value.
  - Mix runs: `TotalTimeMs` P50 and P99 are at most 1.1 times the 24-broker off-run. Parts per file are recorded against the simulated curve, not judged, because the curve assumes equal traffic per partition.
  - Why the off side: the question is whether gateways help or hurt the coordinator, and ticket 16 already checks the bar against Postgres. Postgres sides at 21, 45, and 48 would cost about 6 more hours and would pass a setting that slowed the coordinator as long as Postgres stayed slower.
  - Why a per-run prediction: over 2000 random 21-broker clusters, parts per file has a median of 1.5, a 5th percentile of 1.33, a 95th of 1.71, and a range of 1.2 to 2.0. A fixed bound of 1.1 times the 1.48 average would fail correct clusters about 1 time in 10. In the cluster with IDs 1 to 21 and rack `(id − 1) % 3`, rack 0 has 6 gateways, with broker 13 at {2, 5}, broker 16 at {3, 4}, and broker 10 empty, and racks 1 and 2 have 5 each, so the prediction is 24 / 16 = 1.5.
  - Restart cost, estimated: at 24 brokers about 43 stale moves fall in each rack per restart, and each catches about one linger of a producer's in-flight batches to the moved coordinator partition, roughly 200 KiB. That's about 9 MiB retried of 23 GiB produced in 24 minutes, about 0.04% of records, so client P99 should barely move.
- Default and threshold: the gateway switch is on by default, with no broker-count threshold. Routing still needs the finalized coordinator feature and `__diskless_metadata`, so the default matters only on clusters that chose the coordinator backend. The benchmark result changes the default only through this rule:
  - Every pair passes: the default stays on.
  - The busiest-gateway latency rule fails only at 21 or 45 brokers, the load sizes: the default stays on. The deployment docs name the load sizes, 5 to 7 and 9 to 15 per rack at `N=8`, and the trade: up to 1.7 times today's peak on a few brokers against 7 to 15 times fewer parts. They give the levers: racks of up to 4 brokers or a multiple of `N`, or the switch off. The extra-gateways future work in [Gateway assignment](21-gateway-assignment.md) gets the result as its motivation.
  - Latency fails at 12, 24, or 48, where load is even: gateways hurt the coordinator in general. The default becomes off, routing ships as an operator opt-in, and ticket 16's runs at 12 and 24 run again at the new default.
  - A parts-per-file defect, a record error in the restart, or an inter-AZ failure in a pair with `diskless_az` clients: a defect that blocks release, and the default doesn't change. An inter-AZ gap in the pair without `diskless_az` goes into the deployment docs, as in [Benchmark pass criteria](16-benchmark-pass-criteria.md#what-a-failure-changes).
  - Why: gateways never send more parts than off at any size or local-log mix. The worst case is equal, 8 parts per file at 3 brokers or with consolidation on. Their only steady cost is load concentration at the load sizes, and those sizes sit between sizes that don't concentrate, 21 between 12 and 24 and 45 between 24 and 48, so no broker count separates them. At 1 per rack the rule is already a no-op. Estimates at the base workload, where on-side uploads exclude brokers with an empty gateway set:

    | Brokers | Parts per second, on and off | Log flushes per second, upper bound, on and off | WAL uploads per second, on and off | Busiest gateway against today's busiest broker |
    | --- | --- | --- | --- | --- |
    | 3 | 96 and 96 | 288 and 288 | 12 and 12 | 1.0 |
    | 12 | 96 and 384 | 288 and 1,152 | 48 and 48 | 1.0 |
    | 21 | 96 and 672 | 288 and 2,016 | about 64 and 84 | 1.6 |
    | 24 | 96 and 768 | 288 and 2,304 | 96 and 96 | 1.0 |
    | 45 | about 100 and 1,440 | about 300 and 4,320 | about 100 and 180 | 1.7 |
    | 48 | 192 and 1,536 | 576 and 4,608 | 192 and 192 | 1.0 |

  - Example, for the default: a cluster grows from 21 to 24 brokers. With the default on, rack 0's broker 13 carries coordinator partitions 2 and 5 at 21 brokers, about 28 MiB/s, and the cluster sends 96 parts per second. When one broker joins each rack, the cap drops to 1, broker 13 sheds a coordinator partition, and its producers pay one retry each. Parts stay at 96 per second. With a threshold at 24, the cluster sends 672 parts per second at 21, then every broker flips at the 24th registration, every producer in the cluster pays a retry, and a shrink to 23 flips them all back.
- Name and scope: the switch is `diskless.gateway.routing.enable`, a boolean that defaults to `true` and can be set only at the cluster default, the broker resource with an empty name.
  - Registration: in `ServerConfigs` and in `DynamicBrokerConfig.ALL_DYNAMIC_CONFIGS`, as `DISKLESS_CONSOLIDATION_FETCH_RATE_LIMIT_BYTES_PER_SECOND_CONFIG` already is.
  - Reading: brokers read it only from the cluster-default resource in `ConfigurationsImage`, with the built-in default when no record exists.
  - Enforcement: the controller's `ControllerConfigurationValidator` rejects a per-broker value with `INVALID_CONFIG`, next to the existing `InklessControlPlaneConnectionStringValidator` call in its `BROKER` branch. A broker refuses to start with a static value in `server.properties`, and the error names the `kafka-configs --entity-default` command.
  - Why: Kafka can restrict a key to per-broker values through `PER_BROKER_CONFIGS`, but has no opposite mechanism, and a per-broker override or a static value wins over the cluster default on that broker. If broker 7 alone routes by today's hash, clients that refresh from it land on non-gateways and get rejected, and clients that refresh elsewhere don't: the split that [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) rejected for a static per-broker switch.
  - Name: `diskless.` and `.enable` follow `diskless.managed.rf.enable` and `diskless.storage.system.enable`. The `diskless.gateway.` prefix gives the spec a family for the per-broker exclusion from [Gateway assignment](21-gateway-assignment.md). "Routing" names what turns off: produce and fetch routing together, and the rejection with them.
  - Example: `kafka-configs --entity-type brokers --entity-default --alter --add-config diskless.gateway.routing.enable=false` makes the controller write `ConfigRecord(BROKER, "", ...)`. Every broker applies it at the same offset, the transformer returns today's hash, and the rejection turns off. The same command with `--entity-name 7` gets `INVALID_CONFIG`.
- When to flip: the deployment docs say to leave the switch on, and not to flip it for rolling restarts or rolling upgrades, including one that changes the rule. An operator turns it off for a hot gateway that hurts produce latency at a load size, shown by `BytesInPerSec` against the rack mean and `TotalTimeMs` P99 on that broker, or for a suspected routing defect, shown by a steady nonzero `misrouted` or `empty-tracking` rate or a rejection count that keeps rising outside membership changes. The operator turns it back on once the cause is gone.
  - Flip run: one on-side run at 24 brokers, 3 times, about 2 hours. After 10 minutes of steady state the switch turns off, and at 20 minutes it turns back on. Pass: `record-error-rate` stays zero, parts per file reach the off side's value within `metadata.max.age.ms` plus 60 seconds after the flip off, parts per file return within 1.1 times the prediction within 60 seconds after the flip on, and the rejection count after the flip on is at most one per producer connection and user partition.
  - Costs, estimated at 24 brokers: a flip on moves about 7 in 8 user partitions, because today's hash lands on the gateway with probability `1/8` at 8 per rack. Each producer's in-flight batches to non-gateways get one rejection each, about 1.4 MiB per producer and 34 MiB cluster-wide, retried once. A flip off rejects nothing, and producers drift back to today's hash as their metadata expires, so parts per file climb from 1 to 8 over up to 5 minutes.
  - Why not for upgrades: turning the switch off for a 24-minute rolling upgrade costs 8 parts per file throughout and a retry round afterward. Leaving it on costs split traffic between old-rule and new-rule brokers, at most one retry per broker, connection, and partition per `metadata.max.age.ms`, as [Gateway assignment](21-gateway-assignment.md) accepts.
  - Example: at 45 brokers, rack 0's busiest gateway carries about 36 MiB/s. If its P99 climbs, the operator turns the switch off, the busiest broker falls to about 21 MiB/s over 5 minutes, and parts rise from about 100 to 1,440 per second. After a scale-out to 48, the operator turns it back on, each producer pays one retry round, and parts fall to 192 per second.

### Future work

The rewrite carries these into the design's Future work:

- A cluster-only scope for dynamic broker configs upstream: a set in `DynamicBrokerConfig` that mirrors `PER_BROKER_CONFIGS` and rejects per-broker values, so `diskless.gateway.routing.enable` wouldn't need its own check in `ControllerConfigurationValidator` and at startup. v3 keeps the check in Inkless code, because one key doesn't justify an upstream config-scope change.
- The extra-gateways future work in [Gateway assignment](21-gateway-assignment.md) gains a measured motivation if the busiest-gateway rule fails at 21 or 45 brokers. This ticket adds no design for it.

### Rejected alternatives

The rewrite carries these into the design's rejected alternatives. Each later decision in this ticket adds its own subsection.

#### Fan-out below saturation

- Treating gateways as saving nothing below `N × n_racks` brokers, as a conservative planning assumption: the capped rule contradicts it, since gateways cut parts per leader per round from 12 to 3 at 12 brokers. The benchmark would then skip the sizes where the savings and the load cost trade off.

#### Sizes

- The sizes 3, 12, 24, 45, and 48: they miss the worst peak load below saturation, about 1.6 times today's busiest broker at 21, and spend a pair at 3 brokers that can only come out equal.
- Only the sizes of [Benchmark pass criteria](16-benchmark-pass-criteria.md), 3, 12, and 24: peak load at 12 and 24 sits near today's, so the run never measures the load cost, and it never reaches `g = 2`.
- Pairs at 12 and at every step of ticket 16's broker sweep, 24 to 96: it reuses a ladder and covers 36 at 1.41 times, but it misses 21 and 45, the two worst sizes, and costs 8 pairs.

#### Variants

- Ticket 24's list as written, with pairs for consolidation on and a 10% mix: the consolidation-on pair routes identically on both sides, a single mix point sits where most of the saving is already gone, and a restart at 24 alone misses 48, with about 3.5 times the stale moves.
- Every variant at all five pair sizes: about 80 hours, while the mix and `diskless_az` results change little with size, as the 12- and 24-broker columns show.

#### Metrics

- A destination `partition` tag on the commit-parts meter, to measure parts per leader instead of deriving them: it multiplies the meter's series by `N` on every broker to answer a question the rule already answers, since every leader gets the same count. It doesn't find a hot coordinator partition either, because that's a byte problem.
- The ticket's first list, judged only on cluster-wide `TotalTimeMs`: the restart's retry cost never shows, and one hot broker at 21 brokers is diluted in the cluster percentile.

#### Pass rules

- Judging the on side against Postgres at every pair size: it fits ticket 16's frame literally, adds about 6 hours of Postgres runs, and passes a gateway setting that makes the coordinator slower than with gateways off.
- Judging parts per file against the averages over random clusters: a run's actual cluster can sit 35% off the average at 21 brokers, so correct clusters would fail as defects about 1 time in 10.

#### Default and threshold

- Off by default, with operators turning it on as the docs recommend: nobody gets a hot broker they didn't ask for, but every coordinator cluster pays 4 to 15 times the parts and flushes until someone sets a config, and the coordinator's commit-throughput sweep in [Benchmark pass criteria](16-benchmark-pass-criteria.md#commit-throughput) saturates sooner without gateways.
- On above a broker count, such as 24: 45, the worst load size, sits above any sensible threshold, the 4 times saving at 12 brokers goes, a second on/off rule sits next to the switch, and every scale event across the threshold costs every producer in the cluster a retry.
- On only at rack sizes that don't concentrate load, up to 4 per rack or a multiple of `N`: no hot broker, but nobody can predict the rule without the table, it gives up the 7 to 15 times saving at the load sizes, and most scale events flip routing.

#### Name and scope

- `diskless.metadata.gateways.enable`: it sits with the coordinator's configs, since routing needs the coordinator, but routing lives in the transformer and the produce path, not in coordinator state, and the per-broker exclusion would land in a coordinator family too.
- `diskless.produce.gateways.enable`, KIP-1164's term: fetches move with produce under [Routing produce by coordinator partition](20-gateway-routing.md), so the name hides half of what the switch turns off.
- Kafka's default precedence, where a per-broker override or a static value wins: no validator or startup edit, but any per-broker value brings back split traffic between brokers that disagree.

#### When to flip

- The same guidance with no flip run: [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md)'s named test 7 shows that a flip takes effect, but nothing measures the flip-on retry round across a cluster or the drift after a flip off, the two costs the docs quote.
- Turning the switch off for every rolling upgrade, as a safe mode: it costs 8 parts per file for the whole upgrade and a retry round afterward, more than the split traffic it avoids.

## Resolution

Resolved on 2026-11-20 in a grilling session.

Gateways cut commit parts at every size from 2 brokers per rack up, so the switch is on by default with no broker-count threshold, and the benchmark judges each gateway setting against the coordinator with gateways off. The working notes hold each decision's reasons and numbers, and the rejected alternatives and future work go into the rewrite. Every figure in this ticket is an estimate from [22-tracking-sim.py](../research/22-tracking-sim.py) and [23-gateway-load-sim.py](../research/23-gateway-load-sim.py) with equal traffic per user partition. Running the benchmarks stays out of scope.

- Fan-out: [Writer split against a gateway hint](../../diskless-produce-gateways/research/split-vs-hint.md)'s count holds. A file sends 4 parts at 6 brokers, 2 at 12, and 1 from 24 up, against 8 with gateways off, and the cluster sends `N × n_racks × g` parts per round against `B × N`. Only at 1 broker per rack do gateways change nothing. [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md#relation-to-the-gateway-effort) carries the corrected note.
- Sizes: gateway pairs, the coordinator with the switch on and off, run at 12, 21, 24, 45, and 48 brokers with 3 racks and `N=8`, under the run protocol of [Benchmark pass criteria](16-benchmark-pass-criteria.md). 3 brokers routes identically under both settings, so it has no pair. Peak gateway load is about 1.2, 1.4, and 1.6 times today's busiest broker at 5, 6, and 7 per rack, and 45 brokers stays the worst case at 1.7, where the busiest gateway passes the 32 MiB/s at which files close on size.
- Variants: a pair without `diskless_az` at 24 brokers, on-only runs at 24 with 1%, 5%, and 10% of traffic on consolidating topics, and rolling-restart pairs at 24 and 48. Consolidation on routes identically under both settings, so it has no run. The 10% mix sends about 6.8 parts per file, not the 4.9 that [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) estimated, and 1% already doubles parts per file at saturation.
- Metrics: the runs judge produce `TotalTimeMs` P50 and P99 cluster-wide and on the busiest gateway, client-side latency with `record-retry-rate` and `record-error-rate` in the restart, parts per file from the commit-parts meter's four values over the file count, and inter-AZ bytes per produced MB. They record flushes, P99 `FileCommitTime`, peak `BytesInPerSec` against the rack mean, the misroute and rejection meters, `assignment` renewals, and leader runtime metrics. No new metric: parts per leader per round come from the commit-parts sum.
- Pass rules: each pair passes when the on side's latency is within 1.1 times the off side's, cluster-wide and on the busiest gateway, its inter-AZ bytes are within 1.1 times the off side's, and its parts per file are within 1.1 times the run's predicted value from its own assignment. A breach of the parts rule is a defect. The restart passes with zero record errors, client P50 and P99 within 1.1 times the off-side restart, and parts per file back within 60 seconds. Mix runs judge latency only.
- Default: `diskless.gateway.routing.enable` is on by default. A latency failure only at 21 or 45 brokers keeps it on, and the deployment docs name the load sizes, 5 to 7 and 9 to 15 per rack at `N=8`, with the levers. A latency failure at 12, 24, or 48 turns the default off, and ticket 16's runs at 12 and 24 run again. A parts defect, a restart record error, or an inter-AZ failure with `diskless_az` clients blocks release.
- Name and scope: `diskless.gateway.routing.enable` is a boolean, settable only at the cluster default. It's registered in `ServerConfigs` and `ALL_DYNAMIC_CONFIGS`, and brokers read it only from the cluster-default resource in `ConfigurationsImage`. The controller rejects a per-broker value with `INVALID_CONFIG`, and a broker refuses a static value at startup.
- When to flip: the docs say to leave the switch on and not to flip it for rolling restarts or upgrades. An operator turns it off for a hot gateway that hurts produce latency or for a suspected routing defect, and back on once the cause is gone. A flip run at 24 brokers passes with zero record errors, convergence within `metadata.max.age.ms` plus 60 seconds after the flip off and within 60 seconds after the flip on, and at most one rejection per producer connection and user partition.

### Benchmark list

These runs join [Benchmark pass criteria](16-benchmark-pass-criteria.md)'s benchmarks, on its base workload of 16 MiB/s and 64 diskless partitions per broker, with 3 runs of 30 minutes after a 10-minute warmup per side:

| Run | Sizes | Sides | Hours |
| --- | --- | --- | --- |
| Gateway pair | 12, 21, 24, 45, and 48 | On and off | About 20 |
| Clients without `diskless_az` | 24 | On and off | About 4 |
| Local-log mix at 1%, 5%, and 10% | 24 | On, against the 24-broker off side | About 6 |
| Rolling restart, 60 seconds per broker | 24 and 48 | On and off | About 8 |
| Switch flip | 24 | On, flipped off at 10 minutes and on at 20 | About 2 |

### Failure rows

| Failure | Behavior | Test |
| --- | --- | --- |
| An operator sets the switch on one broker | The controller rejects the change with `INVALID_CONFIG`, so no broker diverges from the cluster default. | Named test 1 |
| A broker's `server.properties` sets the switch | The broker refuses to start, and the error names the `kafka-configs --entity-default` command. | Named test 1 |

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Cluster-default scope, a cluster test: a per-broker value gets `INVALID_CONFIG`, a static value stops startup, and a change to the cluster default flips routing on every broker at its config record, with no restart.
2. Predicted parts per file, a unit test over the assignment interface: the prediction is `N × n_racks × g` divided by the brokers with a non-empty gateway set, 1.5 for the 21-broker cluster with IDs 1 to 21 and rack `(id − 1) % 3`.

Follow-ups:

- [Benchmark pass criteria](16-benchmark-pass-criteria.md): its runs use the switch's default, and this ticket's runs and decision rule join its benchmark list.
- [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md): the corrected note, and gateways on by default with no threshold.
- [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md): the config, the two failure rows, the two tests, and the future work.
- [Gateway assignment](21-gateway-assignment.md): the peak load at 5 to 7 per rack, and the switch's name.
- [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md): the corrected 10%-mix figure.
