# Lease lifecycle at the broker: fallback ranges and idle ends

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: none

## Question

How does the broker's view of its leases stay consistent with the tracking partition's idle timer, after a lease fallback and after an idle period?

Two of Fable's findings come from the same gap: the idle timer ends a lease that the broker still counts on, and nothing tells the broker.

- Fallback: [Fences](../../diskless-metadata-coordinator-v2/design.md#fences) says an idle partition's ranges sit below the range in use. After a fallback in [Leases](../../diskless-metadata-coordinator-v2/design.md#leases), the fallback range sits above the remainder in use and gets no parts until the broker reaches it. Below about 0.6 files per second, its idle timer fires first, and the range fence raises every floor past the live range. Every file fails until the broker re-leases. See [Fable's second finding](../reviews/fable.md#a-fallback-leases-idle-timer-cuts-the-range-in-use).
- Idle end: after a lease-ended record from [Ending a lease](../../diskless-metadata-coordinator-v2/design.md#ending-a-lease), the broker's next file gets stale-sequence, and files closed before the new lease confirms fail with no lease. Any broker idle for 15 minutes hits it. Postgres has no such failure. See [Fable's third finding](../reviews/fable.md#the-first-file-after-an-idle-period-always-fails).

Settle these points:

- Fallback: switch to the fallback range as soon as it's confirmed and abandon the lower remainder, or start a range's idle timer only when the first part names it, or another rule.
- Idle end: the broker drops its lease after about half the idle timeout and re-leases on the first append, letting that file wait. Or the broker learns of the lease end some other way. v2 rejected "a file waits for a renewal" in [Leases](../../diskless-metadata-coordinator-v2/design.md#leases), so say what changed.
- Whether `StaleSequenceRejectRate` after an idle end stays an anomaly signal.
- How [Time bounds](../../diskless-metadata-coordinator-v2/design.md#time-bounds) and its failure rows change.
- Finish reports from [When the tracking partition fences silent participants](01-silent-participant-fences.md#finish-report): after a fallback, the old tracking partition's unsent finish reports wait for its lease end. The lease-end range fence assumes no file under the lease is still unfinished, which the fallback case can break. Say whether the idle-end fix also sends the pending finish report, for example on the request that drops the lease.

Named tests: a low-rate broker after a lease fallback, and a broker that resumes after an idle lease end.

## Resolution

Resolved on 2026-11-12 in a grilling session.

Amended by [Protocol model](17-protocol-model.md#protocol-changes): a lease release also waits until no planned sequence is at or below `b`. This adds a condition to [Lease release](#lease-release).

A live broker never relies on the idle timer. It keeps its lease with keepalives, switches at once to a range at a new partition after a fallback, and ends every range it abandons with a scoped lease release that carries its final finish report. The idle timer becomes a backstop for brokers that are cut off or paused.

### Terms

These terms go into the design's Terms section:

- Keepalive: a lease request for a range the broker already holds, sent to keep the lease from ending idle.
- Abandoned range: a range the broker will never use again, because it switched to a range at another partition or never got the range confirmed.
- Lease release: the request that ends the broker's abandoned ranges at one tracking partition, up to a range end `b`.
- Urgent finish report: a finish report that holds an exception range or covers a file whose tracking part the tracking partition didn't record.

### Idle end

- If the broker has had no answer from a tracking partition it uses for half the idle timeout, it sends a keepalive there. It measures the time on its monotonic clock, from the send time of the last request that the tracking partition's leader answered.
- v2 already treats the keepalive as a retry: the tracking partition appends nothing, confirms the range, and reschedules `lease-idle-<brokerId>`. A live broker's lease therefore never ends idle, and the first file after an idle period commits with no wait.
- The lease reply carries the leader's idle timeout, so a broker whose config value differs still keeps alive in time.
- If the keepalive gets no answer, the broker treats it as a failed renewal and falls back.
- v2 rejected idle heartbeats in [Ending a lease](../../diskless-metadata-coordinator-v2/design.md#ending-a-lease) because they would keep leftover keys and the duplicate window open. The fix in [When the tracking partition fences silent participants](01-silent-participant-fences.md) removes both reasons. Unused sequences were never uploaded. Uploaded objects whose commits failed are exceptions or missed sequences, and their fences follow the finish report, not the lease end. The unknown-outcome part is an exception, so its range fence closes the duplicate window. The rewrite moves heartbeats from the rejected alternatives into the design with this reason.
- v2's rejection of "a file waits for a renewal" in [Leases](../../diskless-metadata-coordinator-v2/design.md#leases) stands. The keepalive makes the wait unnecessary.

### Fallback

- When the broker confirms a range at a partition other than the one it uses, it switches to that range at once and abandons its ranges at the old partition. The keepalive goes only to the partition in use.
- A renewal at the same partition keeps v2's order and uses the remainder first. Abandoning it would turn up to `K/2` sequences into missed sequences on every renewal, which costs `N` range fences each time.
- When a later renewal reaches `t` again, the broker switches back the same way.
- A tracking part that finishes without a leader answer starts a renewal at once: `NOT_COORDINATOR` or retriable errors until the deadline, or an unknown outcome. The renewal asks `t` first, then `t + 1`, and onward. Only one renewal runs at a time. Renewing on the first `NOT_COORDINATOR` was rejected, because routine leader elections would cause fallbacks.
- v2's sentence in [Fences](../../diskless-metadata-coordinator-v2/design.md#fences), "an idle partition's ranges sit below the range in use," becomes: "The broker never sends a sequence from an abandoned range, so a lease end or a release never cuts a range in use."

### Finish reports

- A keepalive carries the current finish report for the ranges at that partition.
- The new finish-reported record holds the broker ID, the epoch, `finishedThrough`, and the exception ranges. The tracking partition appends it only when a keepalive brings a newer report than it has recorded, and it confirms once the record applies. Its apply triggers the same fences as a report in a file-committed record. A keepalive with no newer report appends nothing.
- The rule in [Finish report](01-silent-participant-fences.md#finish-report) becomes: the broker keeps a report until a recorded tracking part, a confirmed lease request, a recorded finish-reported record, or a confirmed lease release includes it.
- If an urgent report isn't carried by a tracking part within one `produce.commit.interval.ms` after it becomes urgent, the broker sends the keepalive early to carry it. Other reports ride the next tracking part or the regular keepalive. A busy broker never flushes early.
- This closes the idle-broker duplicate window about one commit interval plus one append after the deadline. The rewrite replaces v2's "Rejected alternatives for the idle-broker window" in [Deadline and unknown outcomes](../../diskless-metadata-coordinator-v2/design.md#deadline-and-unknown-outcomes) with this mechanism. The sequence bump cost a request per stuck part. The flush costs a request per quiet period after a failure.

### Lease release

- [Fence fan-out under correlated failure](06-fence-fan-out.md#broker-wide-finishedthrough) changes the next bullet's wait: the broker waits until every file of the epoch at or below `b` is finished, not only the files under the abandoned ranges.
- Once every file under its abandoned ranges at a partition is finished, the broker sends a lease release there. It names `b`, the highest range end it gives up, and carries the final finish report for those ranges. The broker retries it with backoff until the partition answers.
- The broker also releases ranges it requested and never got confirmed, scoped to the requested `b`.
- The tracking partition appends a lease-ended record with `b` and the report. Its apply raises the tracking partition's own fenced-through pair to `b`, fences ranges that end at or below `b`, and leaves any higher range live. The leader sends the lease-end range fences from the sequence after the reported `finishedThrough` to `b`.
- A repeated or late release is idempotent. Once the floor is at `b`, the tracking partition appends nothing and confirms. A new range at the same partition always starts above `b`, because the broker picks each range above every range it requested in the epoch. Ordering on the broker, by not renewing while a release is unconfirmed, was rejected: it blocks the return to `t`, and a request that waits in the leader's queue breaks it.
- The idle timer's lease-ended record and the epoch-end paths keep v2's meaning: they end every range of the epoch at that partition.
- The lease end is safe for the same reason as after an idle end: the broker never sends a sequence from a released range again. This settles the fallback case in [When the tracking partition fences silent participants](01-silent-participant-fences.md#fences). The old partition's pending finish report no longer waits for its lease end.

### Metrics

- `StaleSequenceRejectRate`: any nonzero rate from a registered broker is an anomaly: a broker pause, a clock jump, or a bug.
- `IdleLeaseEndRate`: any idle end is an anomaly: a broker cut off from its tracking partition, a broker paused past the timeout, or a clock jump.
- `LeaseFallbackRate` stays. The broker's renewal counter gets a reason tag: `threshold`, `keepalive`, or `failed-tracking-part`.
- New broker metric `FinishReportFlushRate` counts early flushes of urgent reports.
- Plain keepalives get no metric.

### Time bounds

The table in [Time bounds](../../diskless-metadata-coordinator-v2/design.md#time-bounds) changes:

| Decision | Whose clock | Role |
| --- | --- | --- |
| Idle lease timeout | Unchanged | A backstop. Ends the lease of a broker that is cut off from its tracking partition or paused past the timeout. It no longer closes the idle-broker duplicate window. |
| Keepalive interval, half the idle timeout from the lease reply | The broker's monotonic clock, from the send time of the last answered request | Liveness only |
| Urgent-report flush, one `produce.commit.interval.ms` | The broker's clock | Liveness only |

The skew bullets change:

- "The timer fires as a part arrives" and "the leader's monotonic clock jumps forward" stay. Both are now off the normal path.
- New bullet: a broker pauses longer than half the timeout. Its lease can end, the first file after the pause gets stale-sequence, and the broker renews above the floor. This was v2's behavior for every idle broker.
- "Leadership changes faster than the timeout" now affects only cut-off brokers.

### Cost

| Cost | v2 | v3 |
| --- | --- | --- |
| Quiet broker, per half idle timeout | Nothing, then a failed file after the lease end | One keepalive request, no append, no fsync |
| Quiet broker after a failure | Exceptions wait for the lease end | One early keepalive and one finish-reported record |
| Per fallback | Remainder used at the unavailable partition, each file waits for its deadline | One lease release and one lease-ended record at the old partition, plus its range fences |

The verdict against the Postgres backend is "Within." The normal path adds no wait and no fsync.

### Failure rows

These rows change under "Commits and leases" in [Failure modes](../../diskless-metadata-coordinator-v2/design.md#failure-modes):

| Failure | Behavior | Test |
| --- | --- | --- |
| A broker is idle longer than the idle timeout | The keepalive keeps the lease, and the first file commits. | Named: a broker that resumes after an idle period |
| A low-rate broker falls back | It switches at once and releases the abandoned range. No floor passes the range in use. | Named: a low-rate broker after a lease fallback |
| The tracking partition becomes unavailable mid-range | The failed tracking part triggers a renewal, and the broker falls back within one commit deadline. | Cluster: extend "tracking partition down" |
| A late release arrives after the broker returned to `t` | The release stops at `b`, and the new range stays live. | Shard: late release against a newer range |
| A failover loses a finish-reported record before it applies | The broker still holds the report and resends it. | Shard: failover before the record applies |
| A lease reply is lost | The broker releases the unconfirmed range, scoped to the requested `b`. | Fault injection: lost lease reply |
| A broker cut off from its tracking partition | Renamed from "a silent broker's lease goes idle." The mechanics are unchanged. | Cluster: idle lease end |

### User stories

- Story 95 becomes: "As an operator, I want the lease of a broker cut off from its tracking partition to end after `diskless.metadata.lease.idle.timeout.ms`, so that its unused keys are deleted without a restart."
- New story: "As a producer on a quiet broker, I want my first produce after an idle period to succeed, so that low traffic doesn't cause failed produces."

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Named test: a broker that resumes after an idle period. The broker stays quiet past the idle timeout, then produces. The keepalive kept the lease, the first file commits, and `StaleSequenceRejectRate` and `IdleLeaseEndRate` stay at zero.
2. Named test: a low-rate broker after a lease fallback. Its tracking partition is unavailable at renewal, and the broker produces below 0.6 files per second. It switches to the fallback range at once, and no file fails after the fallback. Once the old partition recovers, the release ends the abandoned range there, and its range fences queue the unused keys.
3. The tracking partition becomes unavailable early in a range. The first tracking part that finishes without a leader answer triggers one renewal, and the files after the fallback don't wait on the old partition.
4. A late release: the release to `t` is delayed until the broker has a new range at `t`. The new range stays live, and its files commit.
5. An urgent report on a quiet broker: one file's part ends with an unknown outcome, and the broker then goes quiet. The early keepalive appends a finish-reported record within one commit interval, and the exception fence closes the duplicate window.
6. A failover between the finish-reported append and its apply. The broker resends the report, and the new leader sends the fences once.
