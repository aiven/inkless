# Tracking part under gateways

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 21

## Question

At saturation, does a file keep a second commit part for the broker's tracking partition, or does the tracking partition follow the gateway assignment?

In v2's [Commit path](../../diskless-metadata-coordinator-v2/design.md#commit-path), every file sends a part to its tracking partition, `Utils.toPositive(Utils.murmur2(brokerId)) % N`, and the part is empty when the tracking partition holds none of the file's batches. With gateways at saturation, a broker's files touch about one coordinator partition, and it matches the tracking partition with a probability of `1 / N`. A file then costs about 2 parts and `2R` log flushes instead of 1 part and `R`. See [Fsync and checkpoint cost per commit](12-fsync-checkpoint-cost.md) for the flush count.

Settle these points:

- The options:
  - Accept the second part.
  - Make the broker's tracking partition one of its gateway coordinator partitions. Below saturation, a broker is a gateway for several coordinator partitions, so say which one it picks.
- For the second option, how a change in the assignment maps to the switch to a new partition and the lease release in [Lease lifecycle at the broker](02-lease-lifecycle.md). Say whether [Model of the fence, floor, and release protocol](17-protocol-model.md) must cover switches driven by the assignment, which occur more often than fallbacks.
- The parts and flushes per file at saturation for the chosen option, as input for [Gateway benchmark](23-gateway-benchmark.md).
- For the second option, the tracking partition of a spare. [Gateway assignment](21-gateway-assignment.md) leaves brokers beyond `g × N` per rack as spares that are gateways for no coordinator partition. The assignment also changes when an operator excludes a broker, and when a crashed gateway is fenced and later returns. The rule's ranking per coordinator partition is available from its interface.

## Working notes

Terms used in this ticket, as [Gateway assignment](21-gateway-assignment.md) defines them:

- Gateway: a broker that routing sends one rack's clients to for a coordinator partition's user partitions. It accepts their produce, uploads the WAL file, and sends a commit part to the coordinator partition's leader. It holds no coordinator state.
- Coordinator leader: the leader of a `__diskless_metadata` partition. It assigns offsets, appends and replicates the commit record, and is the only broker that can commit a part. Being a gateway and being a coordinator leader are independent.
- Tracking part: the part a file sends to its tracking partition. It carries the participant set and the finish report, and it's empty when the tracking partition holds none of the file's batches.
- Saturation: at least `N` candidates in a rack, where each gateway serves exactly one coordinator partition.

The simulation is [22-tracking-sim.py](../research/22-tracking-sim.py). It ports `Utils.murmur2`, checked against the vectors in `UtilsTest.testMurmur2`, and the capped rule from [Gateway assignment](21-gateway-assignment.md).

Settled during grilling, before the resolution:

- The tracking partition follows the assignment: a broker's tracking partition is one of its own gateway coordinator partitions, so a routed file sends no extra tracking part. v2's `Utils.toPositive(Utils.murmur2(brokerId)) % N` ignores the assignment, and at saturation it matches the broker's gateway coordinator partition with a probability of `1 / N`. In the example of 24 brokers, 8 per rack, IDs 1 to 24, and `N=8`, it matches for 3 brokers. Averaged over 300 random clusters per size at `N=8` with 3 racks, parts per file on a gateway drop from 2.77 to 2.0 at 12 brokers, and from 1.88 to 1.0 at 24, 45, and 48 brokers. At 3 brokers, every broker is a gateway for all 8 coordinator partitions, so both give 8. At 24 brokers with 4 files per second each, the cluster sends 96 parts per second instead of 180, and up to 288 coordinator log flushes instead of 540 at `R=3`. The change also makes the tracking partition a participant of every routed object, so its releases are local and send no release report, and it evens out tracking load: the busiest tracking partition serves 3.0 brokers on average at 24 brokers, against 5.8 with v2's hash. The empty tracking part adds no produce latency under [File completion and story 83](04-file-completion.md), so the saving is requests, records, and flushes.
  - Nothing outside the broker computes its tracking partition. Every part names it, release reports go to the partition the part names, and v2's fallback already lets it change within an epoch. So the choice needs no agreement across brokers.
  - The choice changes no WAL file or PUT count. Routing decides each file's contents, a broker uploads one file per commit tick either way, and the object key doesn't name the tracking partition. A switch abandons between `K/2` and `K` unused sequences at the old partition, and their keys, mostly never written, go to the deletion queue. `S3Storage` deletes up to 1000 keys per `DeleteObjects` request, and `GcsStorage` 50 per batch.
  - Cost: a broker switches its tracking partition when it loses that coordinator partition in the assignment. The simulation measures about 1 to 2 switches of other brokers per fence, return, or join in a rack near saturation, and fewer below it. Each switch is one lease request at the new partition and one lease release at the old one, with `N` lease-end range fences merged into pending requests as [Fence fan-out under correlated failure](06-fence-fan-out.md) batches them, about 18 records at `N=8`. A rolling restart of 24 brokers adds about 50 switches, a few seconds of the saved flushes. When a routed file's only part fails, its sequence becomes missed and costs `N` range fences instead of one exception fence, as a failed tracking part already does in v2.
- Selection: sticky, behind an interface. A broker keeps its current tracking partition while it stays in the broker's gateway set, the coordinator partitions the broker is a gateway for in its own rack. When the current one leaves the set, and at the start of an epoch, the broker takes the member with the highest score from [Gateway assignment](21-gateway-assignment.md), `toPositive(murmur2(bytes(cp) ++ bytes(brokerId)))`. At saturation the set has one member, so only below `N` per rack does the rule choose. With steady traffic every member gives the same parts per file, so the choice only sets how often the broker switches.
  - Numbers: in 2000 random racks per size at `N=8`, switches of other brokers per fence, return, and join average 0.02, 0.02, and 0.32 at 3 per rack, against 0.22, 0.22, and 0.40 for always taking the highest-scoring member. At 4 per rack they're 0.18, 0.12, and 0.55, against 0.40, 0.40, and 0.54. From 8 per rack up the two rules match. In the example of rack 0 at 12 brokers, brokers 1, 4, 7, and 10 with gateway sets {0, 3}, {2, 6}, {1, 5}, and {4, 7}, a fence of broker 1 gives broker 10 the set {0, 3, 7}. The sticky rule keeps 7, and the stateless rule switches to 0 and back on the return, two switches of about 18 records each for no saving.
  - A broker that only gains coordinator partitions never switches, which is the common case for a stand-in during a fence. A rolling restart doesn't bounce tracking partitions back and forth.
  - Interface: the selection sits behind an internal interface, so tests and later work can replace the strategy. Its inputs are `N`, the broker's gateway set with each member's score from the assignment interface in [Gateway assignment](21-gateway-assignment.md), and the current tracking partition, empty at the start of an epoch. Its output is the tracking partition the broker prefers, which the fallback decision widens to an ordered list. The coordinator backend's sequence source calls it at the start of each epoch and on each image that changes the broker's gateway set. Sticky is the only production implementation, and tests inject a stub, as for the assignment interface. No config selects the strategy. Unlike the gateway rule, a different strategy on one broker wouldn't split traffic, because the choice is local, but a config would make the interface a public extension point that nobody needs. The spec names the interface and where the backend gets it.
- Switch trigger: the first empty tracking part. When the preferred tracking partition differs from the current one, the broker switches once it closes the first file with no batch for the current tracking partition. That close starts a renewal at the preferred partition with a new reason tag, `assignment`, next to `threshold`, `keepalive`, and `failed-tracking-part` from [Lease lifecycle at the broker](02-lease-lifecycle.md). The renewal then follows 02 unchanged: the broker switches once the range is confirmed, and it releases the abandoned range once every file of the epoch at or below its end is finished and no planned sequence is at or below it, as [Model of the fence, floor, and release protocol](17-protocol-model.md#protocol-changes) adds. Only one renewal runs at a time, and every renewal, including threshold renewals and fallbacks, asks the preferred tracking partition first. The broker already splits each file by commit queue at close, so the check is local.
  - Why: while clients with cached metadata still send the lost coordinator partition's produce, the broker's files hold its batches, so the tracking part there isn't empty and a switch saves nothing. A diskless produce never answers `NOT_LEADER_OR_FOLLOWER`, so that stale traffic lasts up to `metadata.max.age.ms`, 5 minutes by default, unless [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) adds an error that makes clients refresh.
  - Example: in the 24-broker cluster, rack 0 holds brokers 1, 4, 7, 10, 13, 16, 19, and 22 with gateway sets {0}, {6}, {1}, {4}, {2}, {3}, {7}, and {5}. A rolling restart fences broker 19 at `t = 0` and unfences it at 60 seconds. While it's fenced, the cap rises from 1 to 2, broker 16 takes partition 4 as its second member, and broker 10 gets partition 7. Broker 10 loses partition 4 as one of the extra moves at the step between 7 and 8 brokers per rack, not because the fence forces it. About 80% of partition 4's producers still send to broker 10 at 60 seconds, so broker 10's files hold partition 4 throughout. The broker never switches, and the preference returns to 4 at 60 seconds. A switch on the image change would switch twice, about 36 records, 16 range fences, and 2 `DeleteObjects` requests, with the same 2 parts per file in between.
  - Numbers: the simulation finds about 2 brokers per fence near saturation that lose their tracking partition, and as many on the return. Over a rolling restart of 24 brokers, a switch on the image change costs about 90 switches, 1,600 records, and 720 range fences, against the 192 range fences that the 24 epoch ends already cost, as [Fence fan-out under correlated failure](06-fence-fan-out.md) counts. The chosen trigger avoids nearly all of them while restarts fit inside the stale window. With a refresh error from ticket 24, both triggers make the same switches, and this one adds 2 to 4 empty parts per change, up to 12 flushes. A permanent change, such as a join, costs one switch either way, later with this trigger.
  - The trigger's cost bound doesn't depend on ticket 24, and its savings do. The tracking partition can lag the assignment for as long as stale traffic lasts, which is harmless, because nothing outside the broker reads it. A broker that closes no files doesn't switch, and it sends no parts either. A broker that keeps getting misrouted produce for its old tracking partition never switches, and its tracking part stays non-empty.
  - Tests drive traffic to trigger the switch: change the image, then close a file without the old coordinator partition's batches.
- Empty gateway set: a broker that is a gateway for no coordinator partition keeps its current tracking partition. At the start of an epoch with an empty set, it takes the coordinator partition with the highest score over all `N`. This is the sticky rule with the whole partition range standing in for the empty set, so the interface needs no new input.
  - Empty sets aren't only spares. Over 1000 random racks per size at `N=8`, the capped walk leaves 0.28 brokers per rack with nothing at 5 per rack, 0.92 at 6, and 1.62 at 7, where the cap `⌈8/7⌉ = 2` lets one broker take two coordinator partitions while another gets none. From 9 to 15 per rack the count is `b_r − 8`, and at 8 and 16 it's 0. [Gateway assignment](21-gateway-assignment.md) says that below `N` every broker is a gateway for several coordinator partitions, and the follow-ups record the correction.
  - Such a broker gets produce only from stale traffic for a coordinator partition it lost, from a promotion, from step 2 of the fallback order, and from topics that [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) keeps off gateways. A broker that closes no files sends no parts, so its tracking partition matters only when it gets traffic.
  - Example: at 30 brokers, IDs 1 to 30 with rack `(id − 1) % 3`, rack 0 holds brokers 1, 4, 7, 10, 13, 16, 19, 22, 25, and 28 with gateway sets {0}, {6}, {1}, {5}, {2}, {4}, {7}, {}, {}, and {3}. Fencing each gateway in turn promotes spare 25 in 7 of 8 cases, to partition 5 in 5 of them, and spare 22 once, to partition 2. Spare 25 starts its epoch at partition 7, its highest score. Its first promotion to {5} makes a file with no batch at 7, so it switches to 5. Its demotion leaves the preference at 5, so it doesn't switch, and later promotions to 5 cost nothing. The cost is at most one switch, about 18 records, per spare and epoch.
- Fallback order and return: the interface returns an ordered list instead of one partition. The list holds the preferred tracking partition, then the other members of the gateway set by score, then every other coordinator partition by the broker's score. Every renewal, whether threshold, keepalive failure, failed tracking part, or `assignment`, walks the list from the top, which replaces v2's `t`, then `t + 1`, and onward in [Lease lifecycle at the broker](02-lease-lifecycle.md). The sticky rule's current input is the partition whose range is in use, so a fallback to another member of the set sticks. The `assignment` trigger fires only when the interface's first entry changes, not on every empty part while the broker is on a fallback. Otherwise, at saturation, every file close would send a lease request to a partition that can't answer, up to 4 per second per broker. A return after a fallback outside the set comes from the next renewal, as in 02.
  - Below saturation: at 12 brokers, broker 4 in rack 0 has the gateway set {2, 6} and tracks at 6. During a 60-second outage of partition 6, a fallback to 2 sends 2 parts per file, partition 2 with the tracking part and partition 6 waiting for its deadline. v2's order sends 3, adding an empty part at 7. After the recovery, 2 stays in the set, so the broker doesn't switch back. v2's order switches back at the next threshold renewal, after up to 512 more files with an empty part each.
  - At saturation: in the 24-broker cluster, partition 6's gateways are brokers 4, 17, and 3, one per rack. Every order gives 2 parts per file during an outage of 6. v2's order sends all three to partition 7, and the broker's score sends them to 0, 3, and 3. Each returns at its next threshold renewal, after up to 512 empty parts, about 128 seconds at 4 files per second.
- Metric: the per-broker commit-parts meter from [Gateway assignment](21-gateway-assignment.md) gets a third value, `empty-tracking`, next to `routed` and `misrouted`, and the spec words the tag as the part's kind. An empty tracking part holds no batch, so ticket 21's meters don't count it, and nothing else shows the cost this ticket removes. On a broker closing 4 files per second, the rate is about 0 on a routed gateway at saturation, apart from a few per switch and during a fallback outside the set. It's about 3.5 per second with v2's hash or a broken selection, and up to 4 per second on a spare with steady misrouted traffic, which is the signal for the traffic-following in Future work. The `assignment` reason on the renewal counter from [Lease lifecycle at the broker](02-lease-lifecycle.md) counts switches. [Gateway benchmark](23-gateway-benchmark.md) computes commit parts per file from this meter and the file count.
- Gateway switch off: the interface still reads the assignment, which [Gateway assignment](21-gateway-assignment.md) computes from the image either way. With routing back on today's per-user-partition hash, files hold batches for almost every coordinator partition, so the tracking part is rarely empty and the trigger rarely fires. Turning the switch on or off moves no tracking partition.
- Other backends: the Postgres and in-memory backends use the default sequence source, which has no tracking partition, so none of these rules apply to them.
- Model: [Model of the fence, floor, and release protocol](17-protocol-model.md) needs no new work for switches driven by the assignment. `RequestLease(q)` in `FencesFloors.tla` is enabled for any partition whenever no renewal runs and the broker isn't paused, with no failure precondition, so the model already explores every such switch. The trigger and the ordered list only narrow when and where the broker switches, and narrowing explored behavior can't break a safety invariant. Pinned scenarios cover the shapes this ticket makes common: D, a switch away and back with a late release, E, a tracking partition that is also a participant while a part under another range retries, and H, a tracking partition that is the only participant before a switch. The bound is 3 lease requests per epoch, from `MaxSeq = 6` and `R = 2`. A fourth switch repeats the per-partition release interactions, because each release is scoped to its own range end at its own partition. A note in ticket 17 records this.

### Future work

The rewrite carries these into the design's Future work.

- Traffic-following tracking for brokers with an empty gateway set: when such a broker gets steady misrouted produce for one coordinator partition, for example as a replica of a remote-storage topic on replica-only routing, its tracking part stays empty unless it tracks at that partition. Following traffic needs damping, because scattered misroutes would switch it on almost every file at about 18 records each. Whether steady misrouted traffic exists depends on [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md), and the misroute meters from [Gateway assignment](21-gateway-assignment.md) would show it in [Gateway benchmark](23-gateway-benchmark.md).

### Rejected alternatives

The rewrite carries these into the design's rejected alternatives. Each later decision in this ticket adds its own subsection.

#### Tracking partition under gateways

- Keeping v2's fixed tracking partition and its second part: at saturation, about 1.88 parts and up to `1.88 × R` coordinator log flushes per file instead of 1 and `R`, and every routed object needs a release report across partitions. Gateways would cut the parts each leader receives per round, but every file would still pay for a second partition.
- Choosing each file's tracking partition among its participants: the broker would need live leases at several partitions and would take sequences from them in increasing order. Ranges at different partitions are disjoint, so files can't alternate between them without rewriting the lease and range rules of [Lease lifecycle at the broker](02-lease-lifecycle.md) and [Fence fan-out under correlated failure](06-fence-fan-out.md).
- Dropping the empty tracking part, with participants registering commits at the tracking partition later: the tracking partition learns participant sets late, missed-sequence detection loses its basis in recorded tracking parts, and the records and flushes move instead of disappearing.
- Single-coordinator objects, the writer split from [Writer split against a gateway hint](../../diskless-produce-gateways/research/split-vs-hint.md): each broker keeps one active file per coordinator partition, so no object spans two coordinator partitions, and the tracking partition, fences, and release reports go away. Upstream rejected it for the PUT requests it adds. A broker uploads one object per coordinator partition it writes for in each commit tick, which is up to `⌈N × n_racks / B⌉` times the baseline below saturation, `N` times when clients don't route, and about `N` times on every broker where consolidation keeps partitions on replica routing. The chosen tracking partition already gets a routed file to 1 part with one object per tick.

#### Selection among several gateway coordinator partitions

- Always the highest-scoring member of the current gateway set: a pure function of the image, but it switches whenever a higher-scoring coordinator partition joins the set and switches back on the return, about twice the sticky rule's switches below saturation for the same parts.
- The member with the most local traffic: it lowers the chance of an empty tracking part when a member has no batch in a tick, but traffic shifts trigger switches of about 18 records each, so it needs hysteresis and a measurement window. With 512 user partitions over 8 coordinator partitions, every member has traffic in almost every tick.
- The lowest-numbered member: it switches as often as the stateless rule and piles tracking load onto low-numbered coordinator partitions.
- A strategy selected by config: the interface would become a public extension point that has to stay compatible, for a choice only tests need to vary.

#### Switch trigger

- A switch on the image change: simple and deterministic, but it switches while stale traffic makes the switch useless. A fence followed by a return inside the stale window costs two switches for nothing, about 90 switches, 1,600 records, and 720 range fences per rolling restart of 24 brokers.
- A switch at the next threshold renewal: no new trigger, but once stale traffic ends, up to `K/2 = 512` files send an empty tracking part before the renewal, up to 1,536 flushes per change.
- A switch after a grace timer such as `metadata.max.age.ms`: a new timer whose wait is too long when clients refresh early and too short when they don't, with the threshold renewal's empty parts in the meantime.
- Never switching within an epoch: no switches from the assignment, but after any change the broker pays a second part per file until it restarts, about 345,600 extra parts a day at 4 files per second.

#### Empty gateway set

- v2's hash, `Utils.toPositive(Utils.murmur2(brokerId)) % N`, for an empty set: the preference moves away on every demotion, so a fence followed by a return can cost a spare two switches, and its stray traffic doesn't favor the hash partition.
- The coordinator partition the broker would most likely stand in for, found by rerunning the capped walk without each gateway: it saves the first switch of a promotion, but it reruns the walk `N` times per image, and the answer moves with every membership change, all for one switch per spare and epoch.
- Following traffic, switching on an empty part to a participant of that file: scattered misroutes make it switch on almost every file. It's future work for the case of steady misrouted traffic.

#### Fallback order and return

- v2's order, `t + 1` and onward, for every partition after the preferred one: below saturation it adds an empty part per file during an outage and a switch back afterward, and at saturation it sends all the gateways of one coordinator partition to the next one.
- The other gateway-set members first, then v2's `t + 1` order: the same parts as the chosen order, but it keeps a second ordering rule and still concentrates fallbacks at saturation.
- A return on every empty part: it returns within one file after a recovery, but during the outage it sends up to 4 lease requests per second per broker to a partition that can't answer. A probe with backoff would add a timer to save at most 512 empty parts per broker and outage.

#### Metric

- A separate `EmptyTrackingPartRate` meter: clearer on its own, but a second place that emits parts, and the benchmark would add two meters to get parts per file.
- No new metric, deriving empty parts from parts per file minus participants: nothing measures participants per file.
- A gauge for whether the tracking partition is in the gateway set: it shows the lag during stale traffic, which is harmless, for one more series per broker.

#### Model

- A pinned scenario with 4 lease requests, `<<p1, p2, p1, p2>>`: it needs `MaxSeq = 8`, and pinned runs with 3 requests already reach the 1-hour limit, so it would add a bounded check of interactions the model already covers.
- Modeling the trigger with a preference variable: it restricts behavior the model already explores, so it can't find a safety violation that the unrestricted model misses.

## Resolution

Resolved on 2026-11-18 in a grilling session.

The tracking partition follows the gateway assignment, so a routed file sends no extra tracking part. The working notes hold each decision's reasons and numbers, and the rejected alternatives and future work go into the rewrite.

- Choice: a broker's tracking partition is one of its own gateway coordinator partitions. At `N=8` with 3 racks, parts per file on a gateway drop from 1.88 to 1 at saturation and from 2.77 to 2.0 at 12 brokers. Releases of routed objects become local, and tracking load evens out. WAL files and PUTs don't change. Nothing outside the broker computes its tracking partition, so the choice needs no agreement across brokers.
- Selection: sticky, behind an internal interface. A broker keeps its current tracking partition while it stays in its gateway set. Otherwise, and at the start of an epoch, it takes the member with the highest score from [Gateway assignment](21-gateway-assignment.md). A broker with an empty gateway set keeps its current tracking partition, and at epoch start it takes the highest-scoring coordinator partition over all `N`. Sticky is the only production implementation, tests inject a stub, and no config selects the strategy.
- Fallback order: the interface returns an ordered list. The list holds the preferred tracking partition, then the other gateway-set members by score, then every other coordinator partition by the broker's score. Every renewal walks it from the top, which replaces v2's `t`, then `t + 1`, and onward. The sticky rule's current input is the partition in use, so a fallback to another member sticks.
- Switch: when the list's first entry changes, the broker switches once it closes the first file with no batch for its current tracking partition. That close starts a renewal with the new reason `assignment`, and the switch and lease release then follow [Lease lifecycle at the broker](02-lease-lifecycle.md) unchanged. A broker on a fallback doesn't trigger on empty parts. It returns at its next renewal. The trigger skips useless switches while clients with cached metadata still send the lost coordinator partition's produce, and it costs a few empty parts per change when they don't.
- Metric: the commit-parts meter from [Gateway assignment](21-gateway-assignment.md) gets a third value, `empty-tracking`. The renewal counter's `assignment` reason counts switches.
- Model: [Model of the fence, floor, and release protocol](17-protocol-model.md) needs no new work. `RequestLease(q)` already explores every switch, and scenarios D, E, and H pin the shapes this ticket makes common.
- The gateway switch and the other backends: with routing off, the interface still reads the assignment, and turning the switch on or off moves no tracking partition. The Postgres and in-memory backends have no tracking partition.

### Parts and flushes per file

These are averages over 300 random clusters per size at `N=8`, `R=3`, and 3 racks, for routed files with no misroutes. A round is one file per gateway.

| Brokers | Parts per file on a gateway | Coordinator log flushes per file, upper bound | Parts per coordinator leader per round | v2's tracking partition: parts per file and per leader |
| --- | --- | --- | --- | --- |
| 3 | 8 | 24 | 3 | 8 and 3 |
| 12 | 2.0 | 6 | 3 | 2.77 and 4.2 |
| 24 | 1.0 | 3 | 3 | 1.88 and 5.6 |
| 45 | 1.0, and spares upload nothing | 3 | 3 | 1.88 and 5.6 |
| 48 | 1.0 | 3 | 6 | 1.88 and 11.3 |

### Failure rows

| Failure | Behavior | Test |
| --- | --- | --- |
| The preferred tracking partition is unavailable | The broker walks the ordered list. Below saturation, it stays inside its gateway set at no extra part. At saturation, it pays one empty part per file until its next renewal after the recovery. | Named test 5 |
| The assignment flaps during a rolling restart | The broker switches only once a file comes out empty, so a fence followed by a return costs nothing while stale traffic lasts. | Named test 4 |
| The selection picks a coordinator partition outside the gateway set, from a bug or a misroute | Cost only: one empty part per file. Any coordinator partition can track, so correctness doesn't change. `empty-tracking` shows it. | Named test 2 |

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Selection, a unit test with fixed values and a stubbed assignment: the sticky rule after a member is gained and after one is lost, an empty set, epoch start, and the ordered list.
2. One part per routed file, an integration test: at saturation, a gateway's file sends only its gateway coordinator partition's part, and `empty-tracking` stays at 0.
3. Switch on the first empty part: after an image change, files with stale batches don't switch. The first file without them starts an `assignment` renewal, the broker switches and releases, and no file fails.
4. A fence followed by a return inside the stale window: no switch.
5. Fallback inside the set, and no loop at saturation: below saturation, an outage of the preferred partition falls back to another member, and the broker doesn't switch back after the recovery. At saturation, no file close sends a lease request to the unavailable partition, and the broker returns at its next threshold renewal.
6. Spare promotion and demotion: the first promotion costs one switch, and a demotion costs none.
7. Gateway switch toggle: turning routing off and on moves no tracking partition.

Follow-ups:

- [Gateway assignment](21-gateway-assignment.md): a correction note. Below `N` per rack, the capped walk can leave brokers with an empty gateway set, 0.28 per rack at 5 brokers, 0.92 at 6, and 1.62 at 7.
- [Gateway benchmark](23-gateway-benchmark.md): the parts and flushes table as input, and commit parts per file from the commit-parts meter.
- [Model of the fence, floor, and release protocol](17-protocol-model.md): a note that switches driven by the assignment need no new model work.

### Update from ticket 24

[Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) adds the refresh error: a broker that isn't a gateway rejects a misrouted batch with `NOT_LEADER_OR_FOLLOWER` once per connection and partition per `metadata.max.age.ms`. Stale traffic now ends within one retry, so the first-empty-part trigger makes about the same switches as a switch on the image change, about 90 per rolling restart of 24 brokers, plus 2 to 4 empty parts per change. The trigger stays: its cost bound doesn't change, and it still skips switches while a client hasn't retried yet or a guarded batch is accepted. Named test 4 drives traffic through a broker whose rejection already fired for that connection, or runs with the gateway switch off, since a fence followed by a return no longer leaves stale traffic otherwise.

For the traffic-following future work, ticket 24 finds no source of steady misrouted traffic. Partitions kept off gateways spread over all coordinator partitions, and their parts count as `off-gateway`, so they never make one coordinator partition steady at a broker. The rejection ends stale traffic. The entry stays in Future work for the case the meters show otherwise.
