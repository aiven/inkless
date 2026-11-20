# Chair review of v3 (written before the council convened)

The chair read design.md end to end and checked two claims against the local Kafka checkout.

## C1. The follower fsync runs on the shared replica fetcher thread (high)

design.md:79 puts the follower's `flush.messages=1` fsync on the replica fetcher thread. `UnifiedLog.append` calls
`flush(false)` once `unflushedMessages() >= flushInterval` (UnifiedLog.java:1300), and the follower path appends
through it. `AbstractFetcherManager.getFetcherId` (line 113) hashes topic and partition over `num.replica.fetchers`,
which defaults to 1. Every classic partition that a broker follows from that leader shares the thread with the
coordinator partitions.

design.md:237 rejects applying SQLite on that thread for this exact reason: "SQLite writes then delay the next fetch
for every partition from that leader, classic ones included, and a slow fetcher drops the follower from the ISR." The
fsync is at 32 to 96 per second per broker at B=3 (design.md:83), so the same objection applies to the design as written.
The co-tenancy benchmark would see it, but the design neither acknowledges it nor names a lever, such as a dedicated
fetcher for `__diskless_metadata`.

## C2. The overlay cap of 4 × B ignores terms that grow with N (medium)

design.md:207 sizes the cap as "one commit part, one lease request, and the fence and release records of each broker".
Fences and release reports are bounded per tracking partition, not per broker: up to N − 1 fence batches and N − 1
release reports at a shard (design.md:589, 520). Add barriers, release-accepted, files-deleted, retention, and
broker-epoch-retired records. At the default B=3 and N=8, the in-flight bound is about 2B + 2(N − 1) = 20 against a cap
of 12. The design says the cap "trips only on a bug or a stalled high watermark" and that any nonzero
`OverlayCapRejectRate` "means a bug". On small clusters that isn't true during a recovery or a retention burst. It
costs liveness, not data, but the signal it relies on would false-alarm.

## C3. The coordinator runtime's thread count isn't specified (medium)

The leader fsyncs inside the log lock on the `CoordinatorRuntime` thread (design.md:79). Neither design.md nor
spec.md says how many runtime threads the diskless coordinator gets, or whether it shares the group coordinator's
pool. With fewer threads than led shards, one shard's fsync blocks another shard's commits. The isolation claims in
Load bounds ("none of them lets work stuck on one coordinator partition block another") then depend on an unstated
config.

## C4. An open item contradicts "none of them changes the design" (medium)

Open items (design.md:1936 on) says that none of them changes the design, then lists a leader that can't rebuild:
"The controller has no request that moves one partition's leadership off a broker, and the design needs one." A
leader with no covering snapshot keeps leadership, and an in-sync follower with an intact live database can't take
over. That's a new controller mechanism, or a resign path, that the design has to own. It's not an implementer's choice.

## C5. Scaling out in the band from N to 2N − 1 brokers per rack adds no diskless capacity (medium, product)

With g = max(1, ⌊b_r / N⌋), every broker beyond N in a rack is a spare until b_r reaches 2N. At N=8, a rack that grows
from 8 to 15 brokers routes no diskless produce or fetch to the 7 new brokers, and the busiest gateway carries 1.7
times today's peak. The design records this as an accepted limit with a signal and two levers: rack sizes, or turning
routing off. An operator who scales out for diskless throughput sees no relief, and nothing in the design warns them at
the moment they add brokers.

## C6. A fixed N plus a row limit per coordinator partition caps the cluster for its whole life (medium, product)

N never changes after creation. The state-size benchmark turns its largest passing step into "the stated limit on batch
rows per coordinator partition" (design.md:1216). Total diskless retention without consolidation is then capped at
N × limit for the life of the cluster. Each snapshot is a full SQLite copy every hour or every 1 GiB, so a large
database also costs disk I/O on every replica. The design should state the cap as a product limit next to the N choice
in the deployment docs, not only as a benchmark output.

## C7. Smaller points

- The misroute rejection can make an idempotent producer's next in-flight batch on the same connection fail with an
  out-of-order sequence error at the coordinator. The guard accepts that batch, and its predecessor bounced. That's
  correct, but it's one more retry per move than the design counts.
- Single provider (Claude only): model diversity across the council is low by construction.

## What v3 does well

- Every v2 finding maps to a decided ticket, and the map's data-loss bar is applied consistently in Accepted risks.
- The tracking-entry state machine (pending, fencing, held, and released, with released terminal) is explicit, and a
  TLA+ model reproduces the reviewed races against the old rules.
- Per-request completion fixes story 83 and matches the classic `DelayedProduce` isolation.
- Benchmarks have pass rules that map every result to an action.
