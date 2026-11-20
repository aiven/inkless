# Consequences for the cross-cutting sections

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24

## Question

What do v2's cross-cutting sections become in v3: failure modes, accepted risks, metrics, tests, user stories, and the section list for the rewrite?

v2's [Consequences for the cross-cutting sections](../../diskless-metadata-coordinator-v2/issues/11-cross-cutting-consequences.md) settled one set for v2. Each v3 ticket leaves rows, metrics, tests, and story changes for this pass, and the rewrite needs one decided set.

Settle these points:

- The failure-mode table and the accepted-risk table under the map's data-loss bar. Every accepted risk passes clauses 2 and 3, or becomes a fix row. Rewrite each row's reason from "A classic topic loses data the same way" where the blast radius differs. [Deletion bounds: soft-delete requirement and a deletion-rate cap](10-deletion-bounds.md#accepted-risks-in-v3) supplies six rows under clause 3's exception, written with that reason, and adds no row, metric, or test from its deferred design.
- The metric set, merging each ticket's signals into v2's two metrics groups.
- The tests, placing each ticket's named test, including Fable's five, in v2's four layers and seam tests, and the model from [Model of the fence, floor, and release protocol](17-protocol-model.md) in the testing decisions.
- The user stories: story 83 from [File completion and story 83](04-file-completion.md), and the stories that the replication guard and admission tickets imply. [Object footer and recovery after losing a replica set](09-object-footer.md) and [Deletion bounds: soft-delete requirement and a deletion-rate cap](10-deletion-bounds.md) each add a Future work entry, not a story.
- The Terms entries for "Kafka parity" and "blast radius", with the map's meanings.
- The section changes for the rewrite.

Input from [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md#resolution):

- Failure rows: a live broker that loses a gateway slot, brokers that disagree on the assignment, a fenced broker that sees its own fence, and a fenced broker that can't reach the coordinator leaders.
- Accepted risk: a gateway cut off from the controller but reachable by clients keeps committing its clients' produce. It's cost only, and the controller's `FencedBrokerCount` and the broker's `last-applied-record-lag-ms` are its signal.
- Metrics: the commit-parts meter's `off-gateway` value, a rejection count for misrouted batches, and the misrouted meters redefined on accepted batches only.
- Tests: seven named tests, covering misrouted produce, no loop under disagreement, unknown AZ, no replica preference, local-log partitions, the fenced broker, and when routing turns on.
- Future work: the per-read rule for consolidating and tiered partitions in Inkless, and cold-path fetches at brokers with an empty gateway set.

Input from [Model of the fence, floor, and release protocol](17-protocol-model.md#resolution):

- Failure rows: a lease renewal with no answer while the broker plans sequences in the requested range, a failed tracking part before a switch, a resent release report after a lease end, a batched release report with a group outside the leased ranges, and an upstream change to the controller's registration order.
- Tests: five named tests, covering the planned-sequence wait, the missed sequence from `finishedThrough`, the resent report, the batched report with one bad group, and the controller's registration order.
- Testing decisions: the three TLA+ specs and `run-all.sh` move into the repo as implementation work, with a fault-injection suite that drops the unflushed tail of a coordinator partition at each fsync point.
- Accepted risks: none new. The lost-tail runs find only the two paths that [Deletion bounds](10-deletion-bounds.md#accepted-risks-in-v3) already accepts.

Input from [Gateway benchmark](23-gateway-benchmark.md#resolution):

- Config: `diskless.gateway.routing.enable`, a boolean that defaults to `true` and can be set only at the cluster default. The controller rejects per-broker values, and a broker refuses a static value at startup.
- Failure rows: an operator sets the switch on one broker, and a broker's `server.properties` sets it.
- Metrics: none new. The benchmark derives parts per file and parts per leader per round from the commit-parts meter.
- Tests: cluster-default scope, a cluster test, and predicted parts per file, a unit test over the assignment interface.
- Future work: a cluster-only scope for dynamic broker configs upstream, and the measured motivation for extra gateways from [Gateway assignment](21-gateway-assignment.md) if the busiest-gateway rule fails at 21 or 45 brokers.

## Working notes

The inventory of every failure row, accepted risk, metric, test, story, Terms entry, Future work entry, and config, with its source, conflicts, and the review-finding check, is in [Inventory for the cross-cutting sections](../research/18-inventory.md). The decisions in this section settle the inventory's open conflicts and placement choices.

### Lost-tail reason and basis

The four lost-tail rows from [Deletion bounds](10-deletion-bounds.md#accepted-risks-in-v3) stay accepted risks under clause 3's exception, and the first row's reason becomes:

> A classic topic loses data to the same cause. The damage stays at the coordinator partition that lost its tail. A lost removal brings back batch rows whose object is gone, and a lost floor lets a late part commit into a deleted key. Reads of those offsets then fail, which a classic topic doesn't do. The protocol model finds no path to another partition within its bounds. The soft-delete bound is future work.

The other three rows keep "Same as the preceding row." The apply-bug and outside-delete rows keep ticket 10's reasons.

Reasons:

- Ticket 10's "the consequence crosses coordinator partitions" describes the mechanism: the tracking partition deletes, and the participant loses. Its own path list shows that both release inversion and resurrection damage only the partition that lost its tail, and the lost-tail runs in [Model of the fence, floor, and release protocol](17-protocol-model.md#lost-tail) find no path to another partition.
- The damage still fails clause 2's second condition: a classic topic that loses its tail never leaves offsets that can't be read. The rows need an exception either way.
- The model is bounded: one broker, no tracking leader change, whole-record drops, and no overlay. The clause 3 exception keeps covering a cross-partition path outside those bounds, and the map's bar stays as written.

### Failure table format and groups

Every failure-mode table uses v2's three columns: Failure, Behavior, and Test. Where a ticket wrote Effect and Recovery, the recovery becomes the closing sentence of Behavior. Every test cell uses v2's "Layer: test name" form, so a cell such as "Test 1" becomes the layer and name of that test. A sixth group, Gateway routing, holds the gateway rows: the switch set on one broker, the switch in `server.properties`, a live broker that loses its gateway slot, brokers that disagree on the assignment, a fenced broker that sees its own fence, a fenced broker that can't reach the coordinator leaders, and a gateway crash. Ticket 22's tracking-part rows go to Commits and leases, ticket 11's to Controller and admin, and ticket 13's to Replicas and snapshots.

Reasons:

- v2's 57 rows stay as written, and the KIP reader sees one shape.
- For most rows the recovery is automatic and the same, such as the delegate's retry, so a fourth column would mostly repeat itself.
- The routing failures share one mechanism, the assignment and the transformer, and a reader looks for them together.

### Placement of costs that aren't data loss

Each item goes where v2's definitions put its kind:

- Accepted risks holds what the design loses: data, reader availability, storage, or a duplicate. The held entry whose participant never reports stays there, next to v2's late-upload leak, with the reason "It leaks storage and never deletes data."
- A failure the design tolerates with a test and no loss is a failure-mode row. The gateway cut off from the controller but reachable by clients moves from ticket 24's accepted-risk table to the Gateway routing group, tested by the second half of ticket 24's test 6, with `FencedBrokerCount` and `last-applied-record-lag-ms` named in its Behavior.
- Performance limits go to a new "Accepted limits" subsection of Capacity and isolation, each with its signal and lever: a cluster that outgrows its partition count, from [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md#accepted-risk-a-cluster-outgrows-its-partition-count), and the busiest gateway at up to 1.7 times today's busiest broker, from [Gateway assignment](21-gateway-assignment.md#resolution). Accepted risks ends with one line: "Performance limits aren't risks to data, so Capacity and isolation states them as accepted limits."

Reasons:

- v2 defines Accepted risks as "the losses the design accepts" and Failure modes as "what the design fixes or tolerates, with the test that covers each row." The cut-off gateway loses nothing and has a test.
- v2 already moved its one performance cost, the barrier's thread hold, out of the table with a pointer.
- The data-loss bar check then applies only to rows that can lose something.

### Rows that close findings

The rewrite adds six failure-mode rows that no ticket wrote. The wording is in the inventory's "Rows no ticket wrote, needed to close a finding":

- Commits and leases: a participant fails over in the middle of a commit while the file's tracking part is empty, tested by ticket 01's named test. It closes Fable's defect 1.
- Deletion: a release report that arrives before the tracking part records, tested by ticket 05's named test, and a "referenced" fence answer after the release, tested by ticket 05's test 2. They close Fable's release-ordering finding.
- Deletion: a retention pass that releases many objects at one participant, tested by ticket 05's test 6. It closes Fable's batching suggestion.
- Commits and leases: a coordinator partition that recovers after an outage under load, tested by ticket 06's named test. It closes the council's fence-storm risk.
- Gateway routing: a gateway crash, from [Gateway assignment](21-gateway-assignment.md#resolution), with a new cluster test, "gateway crash". The test stops a gateway and checks that produce for its coordinator partitions in its rack fails until the controller fences it, then commits at the stand-in, and that other racks keep committing.

The council's benchmark finding and its co-tenancy and fsync-cost risk ask for criteria and measurement, not a fix. They're closed by the rows for a stuck coordinator partition (ticket 04), a leader under load (ticket 15), and the SQLite checkpoint (ticket 13), by the accepted limit for a cluster that outgrows its partition count, and by the criteria in [Benchmark pass criteria](16-benchmark-pass-criteria.md).

Reasons:

- The closing rule needs a failure-mode row and a named test for each fixed finding. Tickets 01, 05, and 06 fixed their findings and named tests, but wrote no rows.
- The gateway crash is a failure the design tolerates, so under Failure modes' definition it needs a row and a test. No ticket test crashes a gateway.

### Metrics recorded outside the backend

Each metric joins the existing group where its code records it:

- v2's backend group keeps every metric the coordinator backend records, including the commit-parts meter and the misrouted bytes meter, which [Gateway assignment](21-gateway-assignment.md#working-notes) places at the splitting delegate.
- `CoordinatorPartitionsBelowRackSpread` joins Kafka's `ControllerMetadataMetrics`, under `kafka.controller:type=KafkaController` next to `FencedBrokerCount` and `OfflinePartitionsCount`, computed from the image by `ControllerMetadataMetricsPublisher`. The class joins the interleaved entries in `INKLESS_OWNERSHIP`.
- The misrouted-batch rejection count joins `io.aiven.inkless.produce:type=WriterMetrics`, next to `RequestRate`, because the produce path rejects the batch before it reaches the backend.

The design's Metrics section lists these two in a second table with a Group column, after the backend table.

Reasons:

- v2 picks groups by neighbor, and the controller doesn't run the backend, so the gauge can't join the backend group in any option.
- The produce path needs no dependency on the backend's metrics class for one counter.

### Metric names

- `CommitPartRate`, tagged `kind` (`routed`, `misrouted`, `empty-tracking`, `off-gateway`), in the backend group, scope broker: commit parts sent, by kind. Parts per file is its total over `FileCommitRate`.
- `MisroutedBytesRate`, in the backend group, scope broker: bytes of accepted misrouted batches.
- `MisroutedBatchRejectRate`, in `WriterMetrics`, scope broker: misrouted batches rejected with `NOT_LEADER_OR_FOLLOWER`.
- `UnreconciledPartitionRejectRate` replaces `UnreconciledPartitionRejectCount` from [Image edge cases](03-image-edge-cases.md), and that ticket gets an "Amended by" pointer.

Reasons:

- v2 and every Inkless group end meters in `Rate` and rejection meters in `RejectRate`, and end gauges in `Count`, `Size`, or `AgeMs`.
- Ticket 03 reads its metric as a rate, so a `Count` suffix would make a meter look like a gauge next to five gauges that end in `Count`.
- `kind` is the tag word that [Tracking part under gateways](22-tracking-part-under-gateways.md#working-notes) asks for.

### Read pool gauges

The leader's read pool from [Admission cap](15-admission-cap.md) registers a `ThreadPoolMonitor` named `diskless-metadata-read`, so `queued-task-count-total` and `active-thread-count-total` under `io.aiven.inkless.thread-pool:type=diskless-metadata-read` are the queue-size and active-thread gauges that ticket 15 asks for. The backend group gets no read pool gauges.

Reasons:

- `ThreadPoolMonitor` already reports both values for five Inkless pools, the produce uploader and the fetch pools among them, so existing docs and dashboards cover the read pool.
- The read pool is a fixed `ThreadPoolExecutor`, the one type `ThreadPoolMonitor` supports.

### Transformer metrics on the gateway path

`ClientAzAwarenessMetrics` keeps its names, and the gateway path records them this way:

- `fallback-total` counts answers from steps 2 to 4 of the fallback order in [Gateway assignment](21-gateway-assignment.md#resolution), that is, answers that aren't the gateway in the client's rack. On replica routing it keeps today's meaning, a broker that isn't a replica. `metrics.rst` describes it as "Requests not routed to the first choice", with both paths spelled out.
- `offline-replicas-routed-around` isn't recorded on the gateway path, because replicas aren't an input there.
- `client-az-hit-rate`, `client-az-miss-rate`, `client-az-unaware-rate`, and `cross-az-routing-total` keep their meanings. Steps 1 and 2 count as hits, and steps 3 and 4 as misses, and as cross-AZ when the broker's rack is confirmed.

Reasons:

- `fallback-total` keeps its sense of "the transformer couldn't use its first choice", and it becomes the transformer-side signal for steps 2 and 4, which [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) names as a source of false misroutes.
- After switch-on, today's dashboards see a near-zero rate, not a flood of answers that only happen not to be replicas.

### Testing structure

- Fault injection gets a second hook next to v2's client-side RPC hook: a test-only coordinator-partition hook with three modes. Unavailable: the leader accepts no part. Delay: each append waits a set time. Lost tail: drop the unflushed records on every replica at a chosen fsync point, then restart.
- The lost-tail suite from [Model of the fence, floor, and release protocol](17-protocol-model.md#tests) runs the lost-tail mode at each fsync point. After each run it checks that no other coordinator partition deletes a referenced object or accepts a part into a deleted key.
- The benchmarks in [Benchmark pass criteria](16-benchmark-pass-criteria.md) use the unavailable and delay modes, with the verifying consumer for zero acknowledged loss.
- The seam tests get three groups: Routing, for the assignment and transformer unit tests (21-2, 21-3, 21-4, 22-1, 23-2, 24-3, 24-4, and 24-7); Produce path, for the `AppendCompleter` tests (04-3 and 04-4); and SQLite, for the checkpoint and version tests (13-1 and 13-2).
- A new Model check subsection: the three specs, the scenario module, and `run-all.sh` live in the repo with their bounds and last results. A change to a protocol rule updates the spec and reruns `run-all.sh`. CI doesn't run the model, because the full runs take hours.
- Every other test goes where the inventory's Tests table proposes.

Reasons:

- Ticket 17 places the lost-tail suite in the integration layer, and a real log exercises log recovery and replica truncation, the paths the lost-tail risks name.
- One hook serves both ticket 16 and ticket 17, and v2's client-side hook can't touch a coordinator partition's log.
- The thirteen tests in the new seam groups each test one internal boundary that none of v2's seam groups covers.

### Merged tests

- Misrouted produce, a cluster test: test 1 of [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md#tests) as written, plus two cases from test 5 of [Gateway assignment](21-gateway-assignment.md#working-notes): a cross-AZ fallback to a gateway of the same coordinator partition and a topic kept off gateways are neither rejected nor counted. Ticket 21's test 5 leaves.
- Batched report with one bad group, a shard seam test: test 4 of [Model of the fence, floor, and release protocol](17-protocol-model.md#tests) replaces test 5 of [Release reports: ordering, batching, and a leak signal](05-release-reports.md).
- Lost lease reply, a fault-injection test: the retry names the same range and is a no-op. The broker releases the unconfirmed range, scoped to `b`, only once no planned sequence is at or below `b`, and every part under that sequence commits. It folds v2's test, the row in [Lease lifecycle at the broker](02-lease-lifecycle.md), and ticket 17's test 1 into one.
- Not merged: ticket 20's test is already part of ticket 21's test 4, and tests 05-4 and 17-3 stay apart because they cover different classification checks.

Reason: each fault gets one test of the current rule, and no test asserts a rule that a later ticket replaced.

### Gateway stories

The spec adds these stories after story 96 from [Lease lifecycle at the broker](02-lease-lifecycle.md):

97. As an operator, I want clients routed to a gateway of their user partition's coordinator partition in their own rack, so that each file sends fewer commit parts and produce stays in the client's AZ.
98. As an operator, I want every broker to compute the same gateways from the metadata image, so that routing needs no new RPC and a crashed gateway's duty moves once the controller fences it.
99. As an operator, I want to exclude a broker from gateway duty with a dynamic broker config, so that I can drain a degraded broker or one about to be decommissioned.
100. As an operator, I want one cluster-wide dynamic switch that turns gateway routing off, so that I can return to per-partition routing without a restart.
101. As a Kafka client, I want a produce at a broker that isn't my partition's gateway to fail once with a retriable error and then succeed, so that stale metadata costs one retry, not a failed produce.
102. As an operator, I want a broker's tracking partition to be one of its gateway coordinator partitions, so that a routed file sends no extra commit part for tracking.
103. As a Kafka client, I want partitions whose fetches need a broker-local log to keep replica routing, so that consolidating, tiered, and switched partitions keep serving fetches.

The spec's Out of Scope and the design's Non-goals change "Produce gateways and preferred produce brokers" to "KIP-1163's `PreferredProduceBrokers` and any public protocol for gateways."

Reasons:

- The map's Notes put hint-only produce gateways in scope, and tickets 20 to 24 decide each behavior these stories state: [Gateway assignment](21-gateway-assignment.md) for 97 to 99, [Gateway benchmark](23-gateway-benchmark.md) for 100, [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) for 101 and 103, and [Tracking part under gateways](22-tracking-part-under-gateways.md) for 102.
- Each operator lever and client behavior that a test checks gets a stated user need, as every other v2 mechanism has.
- The public protocol stays out: [Routing produce by coordinator partition](20-gateway-routing.md) routes through a rewritten `LeaderId` because the map rules out public protocol.

### Other story edits

The spec rewords six v2 stories that closed tickets made stale:

3. As an operator, I want the coordinator topic to have a replication factor of at least 3, 3 by default, and a minimum ISR from `diskless.metadata.topic.min.insync.replicas` that defaults to `min(2, replication factor)` and is fixed at creation, so that one broker failure doesn't stop diskless writes.
5. As an operator, I want the controller to create the coordinator topic once enough unfenced brokers register for its replication factor, so that I don't create it by hand.
31. As a broker, I want the coordinator delegate to send each commit part to the current leader of its coordinator partition and retry a `NOT_COORDINATOR` or `COORDINATOR_LOAD_IN_PROGRESS` error with exponential backoff within a commit deadline, so that a leader change looks like a short delay, not a failure.
56. As a support engineer, I want the coordinator to report its state as metrics, so that I can see lag, snapshot age, log size, WAL pages and apply lag, the deletion queue, tracking entries, pending releases and fences, lease ends, barrier outcomes, stopped applies, commit parts by kind, and unknown-outcome commits.
78. As an operator, I want a broker to renew its lease at the next coordinator partition in its ordered fallback list when its tracking partition is unavailable, so that one unavailable coordinator partition doesn't fail whole objects.
79. As a developer, I want the user-partition mapping pinned by a test with fixed expected values, so that brokers of different versions always agree on it.

It adds two stories for [Admission cap](15-admission-cap.md):

104. As an operator, I want each coordinator leader's in-flight work bounded per commit queue and per coordinator partition, so that a slow leader degrades only its own user partitions.
105. As a Kafka client, I want reads from a slow coordinator leader to fail fast for its user partitions only, so that fetches on other partitions keep their latency.

v2's Mapping seam test drops the tracking partition, and v2's open item on the broker ID's byte encoding in the tracking hash leaves. Ticket 21's fixed-value test in the Routing seam group pins the assignment rule.

Reasons:

- Stories 3 and 5 follow [Enforced replication factor and rack spread](11-replication-guard.md), story 31 follows ticket 15's backoff and the retriable error from [Image edge cases](03-image-edge-cases.md), story 56 follows the metric decisions in this ticket, and story 78 follows the ordered list in [Tracking part under gateways](22-tracking-part-under-gateways.md).
- Ticket 22 makes the tracking partition a local choice, and [Gateway assignment](21-gateway-assignment.md) says a deliberate change to its rule needs no version gate, so only the user-partition mapping needs agreement across versions.
- The question asks for the stories that the admission ticket implies, and its fail-fast read bound is a promise to clients.

### Config names

- `diskless.gateway.candidate.enable`: the per-broker exclusion from [Gateway assignment](21-gateway-assignment.md). It's a boolean that defaults to `true`, and `false` excludes the broker from gateway candidacy. It's per-broker dynamic only: the controller rejects a cluster-default value with `INVALID_CONFIG`, and a broker refuses a static value at startup.
- `diskless.metadata.append.linger.ms`: the fixed linger that [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md) keeps available. It's a static broker config with default `-1`, adaptive.

Reasons:

- Inkless booleans end in `.enable`, "candidate" is ticket 21's term for the brokers the rule chooses from, and ticket 23 puts gateway configs under `diskless.gateway.`.
- Every broker must compute the same assignment, so the exclusion must come from the image: a static value reaches only one broker, and a cluster default of `false` removes every candidate.
- `group.coordinator.append.linger.ms` and `share.coordinator.append.linger.ms` use the same suffix and `-1` for adaptive.

### Kafka parity and blast radius

The design's Terms section adds:

- Kafka parity: a failure has Kafka parity when diskless topics survive it whenever a classic topic with the same replication factor and `min.insync.replicas`, written with `acks=all`, survives it, and lose no more than that classic topic loses when it doesn't.
- Blast radius: the user partitions whose data one failure can lose, delete, or resurrect. The data-loss bar limits a coordinator partition's failure to the user partitions that map to it, about 1/N of every diskless topic.

The accepted risk for retention and `LogAppendTime` under a skewed leader clock gets this reason: "Classic topics have the same exposure. The blast radius is the user partitions of the coordinator partitions the skewed broker leads, about the same share as the classic partitions it leads."

Reasons:

- The map's Notes ask for these two terms with the bar's meanings. Kafka parity is clause 1 with clause 2's "loses no more than a classic topic", and blast radius is clause 2's limit to the failed coordinator partition's user partitions.
- Without the classic topic's settings, a reader could compare against replication factor 1 or `acks=1`, which empties parity of meaning.
- v2's time-bounds table puts retention and `LogAppendTime` on the coordinator leader's wall clock, so a skewed broker affects the coordinator partitions it leads. The question asks to rewrite reasons that don't state their blast radius.

### Fan-out at the gateway default

- The base workload in [Benchmark pass criteria](16-benchmark-pass-criteria.md) becomes: "64 diskless partitions per broker, so with gateways off each file holds batches for all 8 coordinator partitions, the worst-case fan-out. At the switch's default, a file sends about 8 parts at 3 brokers, 2 at 12, and 1 at 24."
- The count of about `B × D / 0.25` exception fences in [Fence fan-out](06-fence-fan-out.md) is labeled the gateways-off worst case.
- A new check under Commit latency: at 12 and 24 brokers, the off sides of [Gateway benchmark](23-gateway-benchmark.md)'s pairs are compared against ticket 16's Postgres runs in the 1.1 band. A failure doesn't change the default and doesn't block a release. The deployment docs then say that turning gateways off at that size puts produce latency above the Postgres backend.

Reasons:

- Ticket 16's runs use the switch's default, so "each file holds batches for all 8 coordinator partitions" holds only with gateways off.
- Both runs already exist at 12 and 24 brokers, so the comparison costs no run, and the operator learns what the switch costs against the bar.
- Ticket 16 maps every result to an outcome, so a recorded but unjudged number would break its rule.

### Section list

The rewrite follows the Sections table in the inventory. `design.md`'s top level becomes, in v2's order: Changes from version 2, Goals, Non-goals, Topology, Flush policy, Coordinator partitions, Topic protection, Transport, Log records, Record compatibility, Speculative state, Write path, Commit and retries, Object tracking and deletion, Time bounds, Local state, Snapshots, Reads, Gateway routing, Capacity and isolation, Metrics, Leader change, Lifecycle, Maintenance, Consolidation and cross-tier offsets, Rollout, Private RPCs, Failure modes, Testing, Terms, Future work, and Open items.

- Added: Gateway routing, between Reads and Capacity and isolation. It holds the assignment rule and its candidates, the fallback order and the transformer, the routing policy, when routing turns on, the switch and the exclusion, and the tracking partition under gateways, from tickets 20 to 24.
- Removed: Thread hold, per [Admission cap](15-admission-cap.md).
- Replaced: Benchmarks, from tickets 16 and 23, and Metrics, Failure modes, and Testing, from this ticket.
- Capacity and isolation gains Load bounds, from ticket 15, and Accepted limits, from this ticket.
- Open items loses the append linger default, the overlay cap, the benchmark workloads and criteria, and the tracking hash's byte encoding. It gains the internal override's name, the SQLite driver floor and checkpoint trigger, the provisional defaults with the rules that settle them, the murmur2 re-measure of the stability bound, and where `SharedState` gets the assignment interface.
- `spec.md` follows the decisions in this ticket in its User Stories, Implementation Decisions, Testing Decisions, Out of Scope, and Further Notes.

Reasons:

- One section explains the routing that five tickets designed, next to the reads it changes and the capacity section that prices it.
- The failure group, the stories, and the tests for gateways all point into the design.

## Rejected alternatives

### Lost-tail reason and basis

- Keep ticket 10's reason. A KIP reader would conclude that a lost tail damages other partitions, which the protocol model's evidence contradicts.
- Move the lost-tail rows to a new exception under clause 2, leaving clause 3's exception to the apply bug and the outside delete. It states the evidence most precisely, but it edits the map's bar after the bar's tickets closed and rests a formal classification on a bounded model. A path outside the bounds would move the rows back.

### Failure table format and groups

- Four columns everywhere: Failure, Effect, Recovery, and Test. Operators would find recovery in a fixed place, but every v2 row needs splitting, and for most rows the Recovery cell repeats the delegate's retry.
- Three columns with the gateway rows spread over Commits and leases and Controller and admin. It avoids a new group, but Commits and leases already holds about 30 rows, and the routing failures would scatter.

### Placement of costs that aren't data loss

- Every accepted cost in the Accepted risks table. One place would list everything the design accepts, but the table's definition blurs, and a bar check on a performance cost says nothing.
- Each item where its ticket put it. Two gateway costs would land in different places, and the busiest-gateway cost would have no entry with its signal.

### Rows that close findings

- Only the five rows that close findings, with the gateway crash left as prose. A failure the design tolerates would have no row and no test.
- No derived rows, left to the rewrite. Four findings would stay open under the closing rule, and the rewrite would derive the rows without a record.

### Metrics recorded outside the backend

- The rejection count in the backend group, next to the gateway meters. It gives the produce path a dependency on the backend's metrics class for one counter, and the controller gauge still sits outside.
- A new gateway routing group for the two gateway meters and the rejection count. It adds a third coordinator-side group for three metrics, and the commit-parts meter's `empty-tracking` value counts tracking parts, not routing.

### Metric names

- Keep `UnreconciledPartitionRejectCount`. Ticket 03 stays as written, but a meter reads like a gauge.
- Kafka's `PerSec` suffix, as in `BytesInPerSec`. It matches Kafka's broker topic metrics but breaks the `Rate` pattern of v2 and every Inkless group.

### Read pool gauges

- Two new gauges in the backend group, such as `ReadPoolQueueSize` and `ReadPoolActiveThreads`. Every coordinator metric would sit in one group, but the gauges duplicate `ThreadPoolMonitor` and report this pool unlike every other Inkless pool.

### Transformer metrics on the gateway path

- Stop recording `fallback-total` on the gateway path and add `gateway-fallback-total`, tagged by step. Each metric keeps one definition, but `fallback-total` goes silent at switch-on, which reads as broken.
- Keep counting any answer whose broker isn't a replica. On the gateway path that's most answers, about 7 in 8 at 8 brokers per rack, so the metric no longer shows a fallback.

### Testing structure

- The lost-tail suite in the backend tests, with a test partition writer that drops its unflushed tail, and ticket 16's hook as benchmark tooling. It's cheaper and needs no hook in the real log, but it contradicts ticket 17's integration layer and skips log recovery and replica truncation.
- The model alone, with no lost-tail suite. It contradicts ticket 17, and the model leaves out several brokers, a tracking leader change, and the overlay.

### Merged tests

- Keep the old tests next to the new ones. Ticket 21's test 5 and ticket 05's test 5 would assert behavior that tickets 24 and 17 replaced.

### Gateway stories

- One umbrella story with the details in Implementation Decisions. The spec stays shorter, but the exclusion, the switch, and the one-retry promise to clients have no story behind their tests.
- Only the Out of Scope and Non-goals change. Five tickets' user-visible behavior would have no user need in the spec.

### Other story edits

- Edit only the stories the tickets wrote. Stories 3, 5, 31, 56, 78, and 79 would contradict the design, and story 79 would demand agreement on a value that no longer exists.
- The six edits without admission stories. The question's request goes unmet, and the fail-fast read promise has no story.

### Config names

- `diskless.gateway.exclude`, default `false`. The name says what it does, but it breaks the `.enable` pattern and inverts the boolean.
- Name only the exclusion and leave the linger name to the implementer. Ticket 14 promises the config, so the rewrite would carry a gap.

### Kafka parity and blast radius

- Parity as "survives what a classic topic survives", without the classic topic's settings. It's closer to the bar's words, but a comparison with replication factor 1 or `acks=1` would satisfy it.
- The terms without a new retention-skew reason. The one kept reason that states no blast radius would stay.

### Fan-out at the gateway default

- Record the off side against Postgres without judging it. It adds no rule but leaves a result to judge after the run, against ticket 16's rule.
- Correct the sentence only. The switch's cost against the bar stays unknown although the data exists.

### Section list

- Gateway routing spread across Commit path, Tracking partition, Reads, and Capacity and isolation. No new section, but the assignment rule, the fallback order, and the switch have no natural home.
- Gateway routing in its own document. The design stays near v2's size, but its failure group, stories, and tests would point outside the design.

## Future work

The rewrite's Future work section collects these entries:

- Grow the coordinator by splitting partitions. Source: v2's design. [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md) names splitting as the only fix for a cluster that outgrows its partition count.
- Merge WAL objects into extents. Source: v2's design. [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) notes that upstream's merge framework keeps every partition off a local log.
- Tiered metadata in the coordinator. Source: v2's design.
- Partitionless key ranges. Source: v2's design.
- A report-only orphan scanner. Source: v2's Non-goals. [Release reports: ordering, batching, and a leak signal](05-release-reports.md) cites it as the only detector for a held entry whose participant never reports.
- Object footer and recovery after losing a replica set. Source: [Object footer and recovery after losing a replica set](09-object-footer.md).
- Deletion bounds: a soft-delete requirement and a deletion-rate cap. Source: [Deletion bounds: soft-delete requirement and a deletion-rate cap](10-deletion-bounds.md).
- Empty restart of a coordinator partition. Source: [Empty restart of a coordinator partition](19-empty-restart.md).
- Extra gateways for a hot coordinator partition, with a per-coordinator-partition traffic metric. Source: [Gateway assignment](21-gateway-assignment.md), with the measured motivation from [Gateway benchmark](23-gateway-benchmark.md) if the busiest-gateway rule fails at 21 or 45 brokers.
- Traffic-following tracking for brokers with an empty gateway set. Source: [Tracking part under gateways](22-tracking-part-under-gateways.md). [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) found no source of steady misrouted traffic, which weakens its motivation.
- A cluster-only scope for dynamic broker configs upstream. Source: [Gateway benchmark](23-gateway-benchmark.md).
- The per-read rule in Inkless for consolidating and tiered partitions. Source: [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md).
- Cold-path fetches at brokers with an empty gateway set. Source: [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md).

Not future work: the "retire on proof" fallback in [Model of the fence, floor, and release protocol](17-protocol-model.md) applies only if an upstream sync breaks the registration-order test, and the AZ-aware leader placement in [Benchmark pass criteria](16-benchmark-pass-criteria.md) applies only if a benchmark fails.

## Resolution

Resolved on 2026-11-20 in a grilling session.

v3's cross-cutting sections are decided. Every review finding meets the map's closing rule, and every accepted risk passes the data-loss bar. [Inventory for the cross-cutting sections](../research/18-inventory.md) lists each row, risk, metric, test, story, Terms entry, Future work entry, and config with its source. The working notes hold each decision's reasons, and Rejected alternatives holds what each decision turned down.

- Accepted risks: ticket 10's six rows stay under clause 3's exception, and the lost-tail reason states that the damage stays at the partition that lost its tail. Ticket 11's lost replica set stays under clause 2's exception. The held-entry leak joins v2's kept rows, and the retention-skew reason states its blast radius.
- Failure modes: v2's three columns everywhere, with a sixth group, Gateway routing. Six derived rows close Fable's defect 1, the release-ordering finding, the batching suggestion, and the fence storm, and add a gateway crash. The cut-off gateway is a failure row, and performance limits go to Accepted limits in Capacity and isolation.
- Metrics: each metric joins the group whose code records it, with a second table for `CoordinatorPartitionsBelowRackSpread` and `MisroutedBatchRejectRate`. New names: `CommitPartRate{kind}`, `MisroutedBytesRate`, `MisroutedBatchRejectRate`, and `UnreconciledPartitionRejectRate`. The read pool uses `ThreadPoolMonitor`, and `fallback-total` counts fallback steps 2 to 4 on the gateway path.
- Tests: a coordinator-partition fault hook with unavailable, delay, and lost-tail modes, the lost-tail suite on it, three seam groups, a Model check subsection, and three merged tests. Every other test goes where the inventory places it.
- Stories: 97 to 103 for gateways and 104 and 105 for admission, with stories 3, 5, 31, 56, 78, and 79 reworded. Out of Scope and Non-goals narrow to the public protocol for gateways.
- Configs: `diskless.gateway.candidate.enable`, per-broker dynamic only, and `diskless.metadata.append.linger.ms`, default `-1`. The inventory's Configs table lists every config with its scope and default.
- Terms: Kafka parity and blast radius, plus the new and changed entries in the inventory.
- Benchmarks: ticket 16's worst-case fan-out applies with gateways off, and ticket 23's off sides at 12 and 24 brokers are judged against Postgres with a docs note as the only outcome.
- Section list: the inventory's Sections table, with Gateway routing between Reads and Capacity and isolation and Thread hold removed.
- Future work: the 13 entries in this ticket's Future work section.

Follow-ups:

- [Image edge cases](03-image-edge-cases.md), [Release reports](05-release-reports.md), [Fence fan-out under correlated failure](06-fence-fan-out.md), [Benchmark pass criteria](16-benchmark-pass-criteria.md), [Gateway assignment](21-gateway-assignment.md), and [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) carry "Amended by" pointers to this ticket.
- The rewrite copies v2's `design.md` and `spec.md` and applies the closed tickets, with this ticket's decisions for the cross-cutting sections.
