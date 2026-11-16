# Admission cap or retry budget

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: none

## Question

What bounds the load on a coordinator leader when it slows down and clients and the delegate retry into it?

All five council members ask for load shedding or a bound on retry amplification. See the council's [sixth status row](../reviews/council.md#status-of-the-earlier-findings) and [third fix](../reviews/council.md#what-would-close-the-gaps). In v2, the delegate retries each part until the commit deadline after `NOT_COORDINATOR` or a retriable error, per [Deadline and unknown outcomes](../../diskless-metadata-coordinator-v2/design.md#deadline-and-unknown-outcomes), and producers retry failed partitions on top. A slow leader also takes lease requests, release reports, fences, and cache-miss reads.

Settle these points, after finding what already bounds this in the Kafka and Inkless checkouts:

- Which bounds exist today: the overlay cap from v2's open items, `CoordinatorRuntime` queue limits, request quotas, the delegate's backoff, and the produce request timeout.
- Whether v3 adds an admission cap on the leader, a retry budget in the delegate, or neither, and which error a shed request gets.
- The priority between commit parts, fences, release reports, lease requests, and reads when the leader sheds load.
- The metric that shows shedding.

Constraint from [File completion and story 83](04-file-completion.md): an in-flight cap on the broker must not let files stuck on one commit queue block admission for requests that don't touch that queue. Otherwise one stuck coordinator partition delays unrelated produce requests, and story 83's bound breaks.

## Resolution

Resolved on 2026-11-13 in a grilling session.

v3 adds no admission cap on the leader and no retry budget in the delegate. In-flight limits on each broker bound a leader's load, so a slow leader receives at most about `B` commit parts and `8B` read parts. Retries after fast errors back off exponentially. Reads leave the shard thread on the leader and stop holding threads on the caller. Nothing sheds except the overlay cap, which only trips on a bug or a stalled high watermark.

### What bounds the load today

- Upstream `EventAccumulator` has no size limit. It rejects an event only when it's closed. Per shard, it runs one event at a time.
- Request quotas exempt `clusterAction` RPCs, so they don't throttle the private RPCs. `queued.max.requests`, 500 by default, applies to the whole broker, not to one shard.
- Upstream `scheduleReadOperation` enqueues reads last on the shard's queue, so in v2 reads wait in line with writes on the shard thread.
- v2's in-flight limits per broker at one leader:
  - Commit parts: one per commit queue, because a part runs only after the previous file's part on that queue finished. Producer retries land in later files, so they add batches, not parts.
  - Lease renewals: one at a time, per [Lease lifecycle at the broker: fallback ranges and idle ends](02-lease-lifecycle.md).
  - Release reports: one in flight per tracking partition, per [Release reports: ordering, batching, and a leak signal](05-release-reports.md).
  - Fences: one batched request in flight per participant, per [Fence fan-out under correlated failure](06-fence-fan-out.md).
- The gap: v2 retries a part after `NOT_COORDINATOR` or a retriable error with no wait. A leader that fails fast, such as one with a full overlay, a closed high-watermark gate, or an `acks=-1` append under the minimum ISR, gets a tight loop from every broker.

### Load bound

- v3 states the in-flight limits as requirements, each with a named test, and states the maximum for each leader: about `B` commit parts, `B` lease requests, one fence batch per participant, and `8B` read parts for each coordinator partition it leads.
- Every limit is per commit queue or per coordinator partition, so none of them lets work stuck on one coordinator partition block another. That meets the constraint from [File completion and story 83](04-file-completion.md).
- On the shard, commit parts, lease requests, release reports, and fences stay first in, first out. The limits keep that queue short, so the shard needs no priority between them.

Rejected alternatives:

- An admission cap on the leader, such as a queue-depth or queue-time limit that sheds with a retriable error. It duplicates the in-flight limits, and shedding commit parts turns slowness into failed produce requests sooner.
- A retry budget in the delegate, such as a token bucket per coordinator partition. A slow leader causes no retries, only fast errors do, and the backoff covers those.

### Backoff

- Every private RPC that the delegate retries waits between attempts with Kafka's `ExponentialBackoff`: 100 ms initial, multiplier 2, 1000 ms maximum, and 0.2 jitter, as constants that match the client defaults. That covers commit parts, lease renewals, release reports, and fences.
- The backoff resets after a success. A commit part still stops at its file's commit deadline, and the last wait ends at the deadline.
- Within a 30-second deadline, one part makes about 30 attempts per broker. The jitter keeps brokers from retrying in step after an election.

Rejected alternative: configs for the backoff. Nobody would tune them, as with `K` and the 500 ms barrier deadline.

### Overlay cap

- The cap is `4 × B` entries, where `B` is the number of registered brokers in the leader's metadata image, read at each check. It grows with the cluster, and the factor leaves room for one commit part, one lease request, and the fence and release records of each broker.
- An entry count is enough, because a part's record holds batch coordinates, and `inkless.produce.buffer.max.bytes` bounds the file behind it.
- A rejected request gets `COORDINATOR_NOT_AVAILABLE`, records nothing, and the delegate retries it with the backoff.
- "The cap on how far the overlay may run ahead of the high watermark" leaves v2's open items.

Rejected alternatives: a byte cap, as a config or a constant, and a fixed entry-count config. Either can be wrong for the cluster's size.

### Reads on the leader

- The leader answers `findBatches` and `listOffsets` from a read pool for the whole broker, `diskless.metadata.read.threads`, with a default of 4, provisional until the read-heavy and cache-storm benchmarks. Each read thread keeps one read-only SQLite connection per coordinator database that the broker leads, opened on first use and closed when the shard unloads.
- A read takes the applied offset from its own read transaction and answers from committed state, which is all a leader answers in v2. A reply goes out only after its apply commits, so a producer that got its answer can read its batches.
- Only a `findBatches` above the applied high watermark, the barrier case, enters the shard, because the barrier appends a record.
- The pool's queue has no limit of its own, because the read-slot limit bounds what arrives. A read for a coordinator partition the broker no longer leads gets `NOT_COORDINATOR`.
- Long read transactions hold back the `PASSIVE` checkpoint from [SQLite apply and checkpoint off the shard thread](13-sqlite-apply-checkpoint.md), and `SqliteWalPages` shows it.

Rejected alternatives:

- Reads first in, first out on the shard queue, as v2 implies. In a cache storm, a commit part waits behind about `8B` scans, about a second at `B=30` and 5 ms per scan.
- Commits and fences at the head of the queue with `enqueueFirst`. Upstream reserves it for the runtime's own events, and a steady commit flow starves reads.
- A default derived from the core count, which upstream coordinators don't do, and a pool per coordinator partition, which grows with `N` and idles on lightly loaded partitions.

### Reads on the calling broker

- `ControlPlane.findBatches` becomes asynchronous and returns a `CompletableFuture`, which was v2's alternative for the design review group. Postgres and in-memory wrap their calls in an executor, and `Reader`'s `FindBatchesJob` chain and `findBatchesNoWait` follow. A read part holds no shared thread, so v2's thread hold leaves, including for a parked barrier request.
- The delegate doesn't retry read parts, because the consumer fetches again. A coordinator partition with no leader, or a `NOT_COORDINATOR` answer, fails the part at once.
- A read part has a deadline of 1000 ms, a constant above the 500 ms barrier deadline. A part that fails or passes its deadline gives its user partitions `KAFKA_STORAGE_ERROR`, the retriable error `FetchCompleter` already returns, and the rest of the fetch returns normally.
- The delegate allows at most 8 read parts in flight per coordinator partition on each broker, a constant that matches today's `fetch.metadata.thread.pool.size` default. `listOffsets` parts share the slots and follow the same rules on `FetchOffsetHandler`'s pool. A part over the limit waits in that partition's queue on the calling broker, and its deadline counts from when it entered the queue.

Rejected alternatives:

- Keeping `findBatches` synchronous with only the deadline and fast-fail rules. A stuck partition still holds shared threads up to the deadline, and unrelated fetches queue behind them.
- No read-slot limit, with only the deadline. A cache storm sends every miss at once.
- One read limit for the whole broker. The constraint from [File completion and story 83](04-file-completion.md) rules it out.

### Metrics

| Name | Scope | Shows |
| --- | --- | --- |
| `OverlayCapRejectRate` | Leader, `partition` tag | Requests rejected at the overlay cap. Any nonzero rate means a bug or a stalled high watermark. |
| `PrivateRpcRetryRate`, tagged `rpc` (commit, lease, release, or fence) and `error` | Broker, `partition` tag | Retries after a retriable error. A sustained rate on one partition shows a leader that fails fast. |
| `ReadPartFailRate`, tagged `reason` (no leader, not coordinator, or deadline) | Broker, `partition` tag | Read parts that failed or passed their deadline, which consumers see as `KAFKA_STORAGE_ERROR`. |

The leader's read pool also reports queue-size and active-thread gauges. The shard needs nothing new: with reads off the shard, `event-queue-time-ms` measures write-side load alone.

Rejected alternatives: one rejection metric for every error, which hides whether the leader fails fast or the broker retries, and a saturation gauge for each coordinator partition, which [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md) already rejected.

### Failure-mode row

| Failure | Effect | Recovery | Test |
| --- | --- | --- | --- |
| A coordinator leader slows down or fails fast under load | In-flight work at that leader stays within about `B` commit parts and `8B` read parts. Retries follow the backoff, and producer retries add batches to later files, not parts. Read parts fail at 1000 ms with `KAFKA_STORAGE_ERROR` for its user partitions only. | Load falls as the leader recovers, with nothing to multiply it. | Tests 1 through 6 |

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Backoff: a leader answers `COORDINATOR_LOAD_IN_PROGRESS`. One broker's attempts for one part follow the backoff schedule, and the part ends with `KAFKA_STORAGE_ERROR` at the deadline.
2. Overlay cap: with the high watermark stalled, the leader rejects the entry past `4 × B` with `COORDINATOR_NOT_AVAILABLE`, appends nothing for it, and the delegate's next attempt follows the backoff.
3. Reads off the shard: a read pool full of slow scans leaves a commit part's latency on that leader unchanged.
4. Read isolation on the caller: fetches on a broker touch a stuck coordinator partition. Fetches for user partitions on healthy coordinator partitions keep their latency, and the stuck partition's user partitions get `KAFKA_STORAGE_ERROR` within 1000 ms.
5. Read slots: with a stuck partition's 8 slots full, a read to a healthy partition on the same broker goes out at once, and the stuck partition's queued part fails at its deadline.
6. In-flight limits: one test per limit in the load bound shows that a broker never has a second commit part per queue, lease renewal, release report per tracking partition, or fence batch per participant in flight.

### Consequences for the rewrite

- `design.md` Capacity and isolation: a new subsection on load bounds with the in-flight limits as requirements and the maximum for each leader. "A produce waits for its slowest part" is already replaced by [File completion and story 83](04-file-completion.md).
- `design.md` Deadline and unknown outcomes: retries use the backoff.
- `design.md` Speculative state: the overlay cap's size and error.
- `design.md` Reads and Thread hold: the leader's read pool, the asynchronous `findBatches`, the read deadline, the fast-fail rule, and the read slots. Thread hold leaves, and the asynchronous alternative becomes the decision.
- `design.md` Metrics: the three metrics and the read pool gauges.
- `design.md` Open items: "The cap on how far the overlay may run ahead of the high watermark" leaves.
- spec.md: the `findBatches` line about a synchronous call and the thread hold.
- Configs: `diskless.metadata.read.threads`.
- [Benchmark pass criteria](16-benchmark-pass-criteria.md): the read-heavy and cache-storm benchmarks judge the read pool default and commit latency during a storm.
