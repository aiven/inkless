# Gateway assignment

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 20

## Question

How does every broker compute the same gateways for each coordinator partition and rack, and how does the assignment behave when brokers join, leave, or run hot?

KIP-1164 gives each coordinator partition `max(1, B / N / n_racks)` gateways per rack, and the saturation point `B = N × n_racks`, 24 brokers at `N=8` with 3 racks. It doesn't define the algorithm, how the assignment reacts to broker changes, or how it balances load. See [`kip-1164-produce-gateways.md`](../../../docs/inkless/proposals/diskless-metadata-coordinator-v3/research/kip-1164-produce-gateways.md).

Settle these points:

- Inputs: the metadata image only, so every broker computes the same answer with no RPC. Say which brokers count: registered, unfenced, and not in controlled shutdown.
- Algorithm: for example, rendezvous hashing over the brokers of each rack, keyed by coordinator partition and broker ID. Below saturation, each broker is a gateway for about `N × n_racks / B` coordinator partitions. Say how evenly the algorithm spreads them.
- Leadership: whether a coordinator partition's leader is preferred as the gateway in its own rack, so that its commits are local, or whether gateways stay independent of leadership, so that an election doesn't move produce traffic.
- Stability: how many coordinator partitions change gateway when one broker joins or leaves.
- Fallback order: what the next choice is after the gateway, for example another gateway for the same coordinator partition, then any broker in the rack, then another rack.
- Uneven racks, a rack with no broker, and brokers with no rack.
- Load: whether the assignment counts only brokers or weighs load, and what an operator can do about a hot gateway.
- Below saturation, whether KIP-1164's co-location mirroring is worth its complexity.
- Rounding: with `B / N / n_racks` rounded down, 25 to 47 brokers at `N=8` with 3 racks leave some brokers with no coordinator partition. Say what those brokers do, and whether the choice among several gateways in one rack is keyed by user partition. See [the routing research](../research/gateway-routing.md#2-fetches-move-with-produce).
- Hot coordinator partition: produce and fetch share one `LeaderId`, so a hot coordinator partition makes one broker per AZ hot for both. Say whether that's acceptable.
- Inputs in code: the mapping and the assignment fit in `SharedState`, which `InklessTopicMetadataTransformer` and the produce path both reach. `KRaftMetadataCache.numPartitions` gives `N`. See [Routing produce by coordinator partition](20-gateway-routing.md).

Metric: the share of files that hold batches for a coordinator partition this broker isn't a gateway for, so that misroutes and their cost show up.

Named tests:

1. Every broker in a cluster test computes the same assignment from the same image.
2. A broker that joins or leaves moves only the coordinator partitions that the algorithm bounds.
3. A produce to a broker that isn't a gateway for any of its partitions commits. The existing tests already accept any mix of coordinator partitions in a file, so this test confirms them.

## Working notes

Terms used in this ticket:

- Gateway: a broker that routing sends the clients of one rack to for a coordinator partition's user partitions. It accepts their produce, uploads the WAL file, and sends a commit part to the coordinator partition's leader. It holds no coordinator state.
- Coordinator leader: the leader of a `__diskless_metadata` partition. It assigns offsets, appends and replicates the commit record, and is the only broker that can commit a part. Gateways lower the number of brokers that send it parts per round, not the RPC per part.

Settled during grilling, before the resolution:

- Candidates: the registered, unfenced brokers in the image, including brokers in controlled shutdown, with no listener filter, minus the brokers an operator excluded (see the operator levers). The controller fences a broker when it grants the shutdown (`SHUTDOWN_NOW` in `BrokerHeartbeatManager`), before the broker closes its sockets, so reconnecting clients already get the successor. Excluding the broker earlier would split its coordinator partitions between it and the successor while it moves its leaderships. A per-listener candidate list would give each listener its own assignment, so a gateway that lacks the client's listener is a fallback case in the transformer. The inputs come from one image through a new `MetadataView` method that returns unfenced brokers with racks, next to the partition-count method from [Routing produce by coordinator partition](20-gateway-routing.md). No public API changes.
- Leadership: gateways are independent of coordinator leadership. The assignment's only inputs are the candidates, their racks, and `N`. An election moves no client traffic, and the coordinator's work and a rack's client traffic for one coordinator partition stay on separate brokers. A leader preference would save one intra-AZ round trip per round in the leader's rack only, and the cross-AZ round trips from the other racks' gateways are the same either way. A gateway failure needs no election either: the controller fences the broker, and every broker recomputes the same assignment from the new image.
- Gateway count: KIP-1164's rounding, per rack. A coordinator partition gets `max(1, ⌊b_r / N⌋)` gateways in a rack, where `b_r` is the rack's candidate count. Below `N` candidates in a rack, every broker is a gateway for several coordinator partitions. From `N` up, leftover brokers are spare: they get no diskless produce or fetch through gateways, and the load and fallback questions decide their use. Making every broker a gateway would add files and commit parts under light traffic, because each broker that receives produce uploads a file per commit tick. At 10 per rack and `N=8`, that's 40 files per second against 32, and the busiest broker would still carry `1/N` of the rack. When a coordinator partition has several gateways in a rack, a hash of the user partition's topic ID and partition index picks one, so produce and fetch for a user partition reach one broker.
- Rule: rendezvous hashing with a cap, computed on every broker, per rack. Each pair of a coordinator partition and a candidate in the rack gets the score `toPositive(murmur2(bytes(cp) ++ bytes(brokerId)))`, with both as 4-byte big-endian integers. The rule walks the pairs from the highest score down, and assigns a pair when the coordinator partition still needs one of its `g` gateways and the broker holds fewer than `⌈N × g / b_r⌉` coordinator partitions. Equal scores go to the lower broker ID. A score depends only on its coordinator partition and broker, so a join or leave changes only one column of scores. In a simulation of 300 random clusters per size at `N=8`, the rule always met the cap, and it moved 2.7 gateway slots on average when a fourth broker joined a rack (ideal 2) and 1.1 when one of 10 left (ideal about 0.8). A gateway crash needs no extra mechanism. Until the controller fences the broker, after `broker.session.timeout.ms`, its coordinator partitions stall in its rack, as a classic partition does when its leader crashes. After the fence, the same rule on the remaining candidates picks the stand-ins. When the broker returns, its coordinator partitions move back, and the stand-ins keep clients with cached metadata for up to `metadata.max.age.ms`. [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) owns that window.
- Racks: the rule groups candidates by rack, and a missing or blank rack forms its own group that the rule handles like any other rack, as `StripedReplicaPlacer` treats rack-less brokers. A cluster with no racks is one group. In a mixed cluster, the rack-less group's gateways get only clients with an unknown AZ and fallback traffic, because no client AZ matches a missing rack. Uneven racks need nothing extra, because each rack uses its own `b_r`, cap, and `g`. A rack with no candidates has no gateways, and the other racks' assignments don't change, because no rack's computation reads another rack.
- Fallback order: for a client in rack `r` and a user partition of coordinator partition `p`, the transformer answers the first match of these steps. Every step skips brokers without the client's listener, and steps 2 and 4 skip excluded brokers unless no other broker matches:
  1. A gateway of `p` in `r`.
  2. Another broker in `r`, in the order of `p`'s scores from the rule, highest first.
  3. A gateway of `p` in another rack.
  4. Any broker.

  Each choice among several brokers takes the highest `hash(logKey, brokerId)`, so a broker that drops out moves only its own user partitions. The order keeps traffic in the client's AZ whenever a broker there can take it, at the cost of an extra commit part. When it can't, it crosses AZs only to a gateway that already sends a part for `p`. Clients with an unknown AZ, replica-only topics, and what a broker that isn't a gateway does with a produce belong to [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md). On the gateway path, the order drops today's preference for replicas in the client's AZ for managed partitions without remote storage. The resolution adds that point to the same ticket.
- Co-location mirroring: v3 leaves out KIP-1164's proposal that gateways of coordinator partitions led by one broker share a broker in every other rack. v2 sends one commit request per coordinator partition, even to the same leader broker, and each coordinator partition is its own log with its own flushes, so mirroring saves no requests and no flushes.
- Load: the assignment counts only brokers, with no traffic weighting. Every broker must compute the same assignment, and each measures only the traffic that reaches it, so weights would have to come from the image, which means a controller-written record or an operator config. A gateway carries whole coordinator partitions, so its peak exceeds today's per-user-partition routing. In a simulation with 512 equal user partitions at `N=8`, the busiest gateway carries about 1.1 times today's busiest broker at 3 per rack, the same at 8, 1.2 times at 10, 1.4 at 12, 1.7 at 15, and 0.9 at 16. The rejected option of the gateway count has the same peak: at 15 per rack, one coordinator partition still has a single gateway at `1/8` of the rack. What the chosen count trades for fewer files is how many brokers run at that peak, 8 against 1 at 15 per rack, plus 7 idle spares. Lowering the peak needs smaller units, which means a larger `N` or more commit parts. Diskless produce marks the standard per-broker `BytesInPerSec` in `ActiveFile`, so a hot gateway shows without a new metric. The resolution asks [Gateway benchmark](23-gateway-benchmark.md) to add 45 brokers, 15 per rack, as the worst case.
- Operator levers: two. The gateway switch that [Gateway benchmark](23-gateway-benchmark.md) owns returns routing to today's per-user-partition hash. A per-broker dynamic config, set through `AlterConfigs` on the broker's config resource, excludes that broker from gateway candidacy whatever its state, for a degraded broker or one about to be decommissioned. The controller stores dynamic configs in the metadata log, and `ConfigurationsImage` holds every resource's configs, so every broker reads the same exclusions. The excluded broker's coordinator partitions move by the rule, as on a fence, and the fallback order skips it unless no other broker can serve. No new RPC. The spec names the config. If the controller validates broker config keys against a known list, the config is registered in `KafkaConfig`.
- Stability test: named test 2 asserts provable invariants after every join and leave: each coordinator partition has exactly `g` gateways, no broker exceeds the cap, a departing broker holds nothing, and a new broker holds no more than the cap. It also asserts a measured bound over a fixed seeded sweep of rack sizes from 2 to 20: at most 5 extra moves per event at `N=8`. A move is one gateway slot changing broker. Forced moves are the departing broker's slots, the new broker's slots, and the slots added or removed when `g` changes. Extra moves are slots that change between two brokers that both stayed. No bound tighter than `N` is provable for the capped walk, because one coordinator partition taking its next choice can push later ones along. In 5000 random clusters per size, extra moves averaged under 1, except at the step from 7 to 8 brokers, where the cap changes from 2 to 1 and they averaged 2.2, with a worst case of 5. The design states that the bound is measured. The simulation used MD5, so the reference implementation re-measures the bound with murmur2 before the spec fixes it.
- Structure and cross-version agreement: the rule sits behind an internal interface in the style of `ReplicaPlacer`, which `QuorumController.Builder` creates as `StripedReplicaPlacer` by default and tests replace through `setReplicaPlacer`. The capped rule is the only implementation. Its input is the candidate snapshot from one image, with broker IDs and racks after exclusions, plus `N`. Its output is each rack's gateways per coordinator partition and the rack's brokers ranked by that coordinator partition's score, for step 2 of the fallback. The transformer applies the listener filter and the fallback order. The transformer and the produce path reach the interface through `SharedState`, and transformer tests inject a stub. Named test 1 stays a cluster test, which checks that the image delivers the same inputs to every broker. A second test pins the default implementation with fixed expected values for a few fixed clusters: the score encoding, the tiebreak, the cap, and the fallback ranking and pick. A deliberate change needs no version gate. During a rolling upgrade, old and new brokers then compute different gateways, which splits traffic and adds commit parts until the upgrade ends, and never affects correctness, because any broker commits any produce. The spec confirms where `SharedState` gets the implementation.
- Misroute metric: two per-broker meters at the point where the broker splits a file's commit by coordinator partition. The first is commit parts per second, split into parts for coordinator partitions this broker is a gateway of and misrouted parts. The second is misrouted bytes per second. A batch is misrouted when the receiving broker isn't a gateway of its coordinator partition in the broker's own rack, or own group for a rack-less broker. So a cross-AZ fallback to a gateway of the same coordinator partition doesn't count: it adds no commit part, and [Benchmark pass criteria](16-benchmark-pass-criteria.md#inter-az-traffic) measures its cross-AZ bytes. Both meters count only user partitions on the gateway path, so topics that [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) keeps off gateways don't hide real misroutes. The ratio of the parts meters is the misroute share, and the misrouted rate is the extra load on leaders. The spec names the meters and places them, because v2's splitting delegate doesn't exist in the code yet.
- Named tests, replacing the three in the question:
  1. Agreement, a cluster test: every broker computes the same assignment from the same image, including racks, exclusions, and `N`.
  2. Fixed values, a unit test: the default implementation gives hard-coded expected gateways and rankings for fixed clusters, including one with no racks, a mixed cluster, uneven racks, and one at `g = 2`.
  3. Stability, a unit test: the invariants after every join and leave, and at most 5 extra moves per event over the seeded sweep.
  4. Transformer routing, a unit test with a stubbed assignment: user partitions follow the assignment, which is the named test of [Routing produce by coordinator partition](20-gateway-routing.md), and one case per fallback step. The cases are the gateway in the client's rack, a gateway without the client's listener (step 2), a rack with no candidates (step 3), no gateway with the listener anywhere (step 4), an excluded broker that's skipped, and at `g = 2`, a pick by user partition that stays put when the other gateway leaves.
  5. A produce to a non-gateway, an integration test: it commits, and the misrouted meters count its part and bytes. A cross-AZ fallback to a gateway of the same coordinator partition and a topic kept off gateways aren't counted. The existing tests already accept any mix of coordinator partitions in a file.
  6. Exclusion, a cluster test: setting the per-broker config moves that broker's coordinator partitions on every broker, and clearing it moves them back.

### Future work

The rewrite carries these into the design's Future work:

- Extra gateways for a hot coordinator partition: a config that gives a named coordinator partition extra gateways from a rack's spares, splitting its user partitions across them. It's the only lever that relieves a hot coordinator partition, since moving one only moves the heat. It needs a per-coordinator-partition traffic metric, so operators know which one is hot: `BytesInPerSec` shows a hot broker, but not which coordinator partition causes it. It costs extra commit parts for that coordinator partition.

### Rejected alternatives

The rewrite carries these into the design's rejected alternatives. Each later decision in this ticket adds its own subsection.

#### Candidates

- The alive-broker list that `getAliveBrokerNodes` returns, filtered by the request's listener: each listener would get its own assignment, so during a listener rollout, internal and external clients of one coordinator partition reach different brokers, and the produce path, which has no listener, couldn't tell a misroute from a routed produce.
- Excluding brokers in controlled shutdown: the controller already fences a broker before it closes its sockets, so reconnecting clients get the successor either way. Excluding it earlier splits its coordinator partitions between it and the successor while it moves its leaderships, because clients with cached metadata keep sending to it.

#### Leadership

- Leader preference, which makes a coordinator partition's leader the gateway in its rack: it saves one intra-AZ round trip per round, in the leader's rack only. Every election then moves the gateway in two racks, and the old gateway keeps clients with cached metadata for up to `metadata.max.age.ms`. The leader carries its rack's client traffic on top of the coordinator work, and gateway balance follows leader placement, which the assignment doesn't control. The cross-AZ round trips from the other racks are the same either way.

#### Gateway count

- Every broker is a gateway for at least one coordinator partition, with the rack's brokers spread over the `N` coordinator partitions as evenly as possible: under light traffic, each broker that receives produce uploads a file per commit tick, so this adds files and commit parts, 40 files per second against 32 at 10 brokers per rack and `N=8`. With even traffic, the busiest broker still carries `1/N` of the rack, so it adds no capacity, and the assignment can't aim the extra gateways at hot coordinator partitions.

#### Rule

- Round-robin over sorted broker IDs: perfect balance, but one new ID shifts every later position. In the simulation, a fourth broker moved 6.6 slots on average where balance needed 2.
- Plain rendezvous hashing: the fewest moves, but balance is left to chance. At 3 brokers and `N=8`, about 1 cluster in 4 gets the 3, 3, 2 spread, and about 1 in 4 puts 5 or more coordinator partitions on one broker. From `N` brokers per rack up, two coordinator partitions can pick one broker while others stay spare.
- Consistent hashing on a ring: stability like plain rendezvous and the same chance balance at 8 coordinator partitions. With a cap, it becomes the chosen rule in another shape.
- Jump consistent hash: assumes buckets join and leave only at the end of a numbering. Any broker can leave, so a departure renumbers the brokers after it.
- Maglev hashing, as a draft in which brokers take turns picking their highest-scoring free coordinator partition: perfect balance, but the turn order depends on which brokers exist, so a join shifts later picks.
- Stable matching (Gale-Shapley) with capacities: with rankings from one symmetric score, the stable matching is what the capped walk produces, so it adds nothing.
- Assignment written by the controller into the metadata log: the controller knows the previous assignment, so it could move only what balance needs and could weigh load. It costs a new metadata record type behind `MetadataVersion` and controller code that rewrites the assignment at every fence and registration, a large Inkless edit to upstream controller code for about one move saved per membership change.
- Gateways that follow the coordinator partitions' replica placement, with the replica in each rack as that rack's gateway: no moves on a join, and operators can rebalance with the reassignment tool. It needs a second rule while a replica's broker is fenced and from `2N` brokers per rack up, where `g` reaches 2 but each rack holds one replica. A broker added in a scale-out gets no gateway until an operator reassigns `__diskless_metadata`.
- An operator-defined assignment in a dynamic config: deterministic and fully controllable, but every scale-out, failure, and rack change needs a manual step. An override on top of the rule belongs to the load question.
- An assignment that each broker remembers locally, for stickiness: a restarted broker has no memory and computes a different answer from its peers.
#### Racks

- Excluding rack-less brokers whenever some brokers have racks, so they become spares: the rule then needs a special case, one group of everyone when no broker has a rack, and it differs from how `StripedReplicaPlacer` treats rack-less brokers.
- KIP-1164's cluster-wide count, `B / N / n_racks`: uneven racks would get a count that fits none of them. A per-rack count lets each rack use its own `b_r`.

#### Fallback order

- A gateway of the same coordinator partition in another rack before another broker in the client's rack: it saves an extra commit part but moves every produced and fetched byte across AZs, which costs far more than commit metadata.
- Today's fallback, a hash of `"<topicId>-<partitionIndex>"` over the brokers in the client's AZ and then over all brokers: it ignores coordinator partitions, so every fallback is a misroute, including the cross-AZ case where a gateway of the same coordinator partition could take the traffic.
- A modulo over a sorted list to choose among several brokers: a change in the list reshuffles every user partition, not only the ones on the broker that changed.

#### Co-location mirroring

- KIP-1164's co-location mirroring, in which the gateways of coordinator partitions led by one broker share a broker in every other rack: it saves requests only if v3 adds combining of commit parts by destination broker, and never flushes. "Led by one broker" ties gateways to leadership, so every election that regroups leaders reshuffles gateway sets in every rack, and the groupings override the cap.
- Mirroring across racks alone, with the same gateway sets in every rack and no tie to leaders: each coordinator partition still gets one part per rack, so it saves nothing.

#### Load

- Weights from each broker's own traffic measurements: brokers see different traffic, so a small difference flips the outcome, and brokers compute different gateways for one coordinator partition.
- Weights from a controller-written record, with brokers reporting traffic and the controller aggregating it: the controller-written assignment that the rule's alternatives reject, with the same cost.

#### Operator levers

- Reusing KIP-1066's `cordoned.log.dirs`, so a broker whose log directories are all cordoned isn't a candidate: it couples two intents, "place no new replicas here" and "route no clients here", so an operator can't shed client traffic without blocking replica placement. A cordon acts only on future placements, while exclusion moves traffic at once. Gateway routing would change whenever upstream changes cordoning, and it would need a metadata version that supports cordoned directories.
- A cluster-wide list of excluded broker IDs: one place shows every exclusion, but IDs of departed brokers linger until someone removes them, and operators address brokers through their own config resource.
- A new admin RPC: public protocol, which the map rules out, for what a dynamic config does through `AlterConfigs`.

#### Stability test

- A proven bound on moves: none tighter than `N` exists for the capped walk.
- Invariants alone: they guard correctness but not stability, so a change that makes the rule move more slots would pass.

#### Structure and cross-version agreement

- The rule as an implementation detail, with no fixed-value test: a change goes unnoticed until split traffic shows during an upgrade.
- A `MetadataVersion` gate, so brokers keep the old rule until the whole cluster upgrades: version machinery for a cost that lasts one upgrade and touches only commit parts.
- A rule selected by config: the choice would have to be one cluster-wide value in the image, a switch splits traffic like a rule change, and the interface becomes a public extension point that has to stay compatible. Nobody needs alternative rules.

#### Misroute metric

- The share of files that hold a misrouted batch, as the ticket first worded it: it marks a whole file once, so a single stray batch looks the same as a misrouted stream carrying half the broker's traffic.
- Counting every diskless partition: topics kept off gateways by design land on non-gateways permanently and keep the metric high.

## Resolution

Amended by [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md#merged-tests): test 5 folds into test 1 of [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md), which keeps test 5's two cases that aren't counted. The per-broker exclusion is `diskless.gateway.candidate.enable`, and the busiest-gateway load is an accepted limit in Capacity and isolation.

Every broker computes the assignment from the image alone, per rack, with rendezvous hashing under a cap. The working notes hold each decision's reasons and numbers, and the rejected alternatives and future work go into the rewrite.

- Inputs: the registered, unfenced brokers, including brokers in controlled shutdown, with no listener filter, minus brokers excluded by a per-broker dynamic config. Racks group the candidates, and a missing rack is its own group. Leadership isn't an input, so an election moves no client traffic.
- Count: `max(1, ⌊b_r / N⌋)` gateways per coordinator partition in a rack, as KIP-1164 rounds. Leftover brokers are spare. Spares save files under light traffic. The busiest broker carries the same load either way, and more brokers run at that peak.
- Rule: rendezvous hashing with a cap of `⌈N × g / b_r⌉` coordinator partitions per broker, scored by `toPositive(murmur2(bytes(cp) ++ bytes(brokerId)))`, with ties to the lower broker ID. The rule sits behind an internal interface in the style of `ReplicaPlacer`, and a fixed-value test pins it. A deliberate change needs no version gate and costs one rolling upgrade of split traffic.
- Failure: until the controller fences a crashed gateway, its coordinator partitions stall in its rack, as a classic partition does. After the fence, the same rule picks the stand-ins. When the broker returns, the stand-ins keep cached clients for up to `metadata.max.age.ms`.
- Fallback order: the gateway in the client's rack, another broker in that rack by score, a gateway of the same coordinator partition in another rack, then any broker. Each choice among several brokers is a rendezvous on the user partition's log key. Brokers without the client's listener are skipped, and excluded brokers are skipped unless nothing else serves.
- Load: no traffic weights. At `N=8`, the busiest gateway carries about 1.1 times today's busiest broker at 3 per rack and up to 1.7 times at 15 per rack. The cost is accepted, and existing `BytesInPerSec` shows it. Co-location mirroring is left out, because v2 sends one request per coordinator partition and flushes each log separately.
- Operator levers: the gateway switch that [Gateway benchmark](23-gateway-benchmark.md) owns, and the per-broker exclusion. Extra gateways for a hot coordinator partition are future work.
- Metric: per broker, routed and misrouted commit parts per second, and misrouted bytes per second, on the gateway path only. A batch is misrouted when the receiving broker isn't a gateway of its coordinator partition in its own rack.
- Named tests: agreement, fixed values, stability with a measured bound of 5 extra moves per event, transformer routing with each fallback step, a produce to a non-gateway with the meters, and exclusion. The working notes give each test's cases.

Follow-ups:

- [Gateway benchmark](23-gateway-benchmark.md): adds 45 brokers, 15 per rack, as the worst case for peak gateway load, and uses the misroute meters.
- [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md): decides whether the gateway path drops today's replica preference for managed partitions without remote storage, and what a fenced broker that clients can still reach does with produce.
- [Tracking part under gateways](22-tracking-part-under-gateways.md): unblocked. Below saturation, a broker is a gateway for several coordinator partitions, and from `N` per rack up, a spare is a gateway for none.

### Update from ticket 24

[Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) settles the points this ticket left open:

- A client whose AZ resolves to `null` counts as a client of the rack-less group, and the fallback order runs unchanged.
- The gateway path drops the replica preference, so replica assignment isn't an input to the fallback order.
- Diskless produce gets no fencing check. A fenced broker that sees its own fence isn't in its own candidate set. A broker cut off from the controller keeps committing, as a cost-only accepted risk.
- A broker that isn't a gateway rejects a misrouted batch with `NOT_LEADER_OR_FOLLOWER` once per connection and partition per `metadata.max.age.ms`, so the stale window after a return ends within one retry. The misrouted meters count accepted batches only, next to a new rejection count, and the commit-parts meter gets an `off-gateway` value.
- The gateway switch is a cluster-level dynamic config read from `ConfigurationsImage`, like the per-broker exclusion.

### Correction from ticket 22

[Tracking part under gateways](22-tracking-part-under-gateways.md) found that the capped walk doesn't make every broker a gateway below `N` candidates per rack, as the gateway-count note says. The cap `⌈N × g / b_r⌉` lets one broker take two coordinator partitions while another gets none. Over 1000 random racks per size at `N=8`, the walk leaves 0.28 brokers per rack with an empty gateway set at 5 per rack, 0.92 at 6, and 1.62 at 7, and none at 2 to 4. The rule doesn't change. The rewrite words the gateway count as "below `N` per rack, most brokers are gateways for one or more coordinator partitions, and the cap can leave a few with none." The load figures in the working notes skip 5 to 7 per rack, so the reference implementation measures peak gateway load there too. Ticket 22 decides the tracking partition of a broker with an empty gateway set.

### Update from ticket 23

[Gateway benchmark](23-gateway-benchmark.md#resolution) measured the missing sizes with the method of this ticket's load note, in [23-gateway-load-sim.py](../research/23-gateway-load-sim.py), which reproduces this ticket's figures. The busiest gateway carries about 1.2 times today's busiest broker at 5 per rack, 1.4 at 6, and 1.6 at 7, where the cap of 2 gives one broker about a quarter of the rack. 15 per rack stays the worst case at 1.7. The gateway switch is `diskless.gateway.routing.enable`, on by default and settable only at the cluster default, so the spec can name the per-broker exclusion under the same `diskless.gateway.` prefix.
