# Fence fan-out under correlated failure

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 01

## Question

What bounds the fences that a tracking partition sends when many sequences are missed or many leases end at once?

[Fences](../../diskless-metadata-coordinator-v2/design.md#fences) fences each missed range at all `N` partitions, and each lease end sends a range fence to every partition. Taleb notes that a correlated failure, such as a tracking partition that is unavailable for a while or many brokers restarting, multiplies this into a fence storm on the coordinator partitions that are recovering. See the council's [tracking-partition risk](../reviews/council.md#new-risks-in-v2). The lease fallback adds a rarely exercised second path to the same machinery.

Settle these points, after [When the tracking partition fences silent participants](01-silent-participant-fences.md) fixes the normal-path fan-out:

- The worst-case fence count for one tracking partition outage and for a rolling restart of every broker, at `N=8`. [When the tracking partition fences silent participants](01-silent-participant-fences.md#fences) bounds exception fences to at most `N - 1` per finish report and merges consecutive missed sequences into one range fence per partition. The count includes both, plus lease ends, lease releases, and the fences that early flushes of urgent finish reports trigger, from [Lease lifecycle at the broker](02-lease-lifecycle.md).
- Whether fences coalesce per participant, for example one range fence per broker epoch instead of one per missed range, and whether they're rate-limited or queued behind normal commits.
- The metric that shows a storm, building on v2's `FenceRate`.

## Resolution

Resolved on 2026-11-12 in a grilling session.

Amended by [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md#fan-out-at-the-gateway-default): the count of about `B × D / 0.25` exception fences is the gateways-off worst case, because it assumes every file has a part at `t`.

Each tracking partition keeps at most one fence request in flight per participant, and fence needs that pile up meanwhile merge into the next request. A recovering partition therefore receives at most `N - 1` fence requests at once, whatever the broker count or the outage length. The session also found a race in the deferred fences, and `finishedThrough` now covers the whole broker epoch to close it.

### Cost of a fence

A fence's sequences-fenced record joins the leader's append linger batch, as v2's [Flush policy](../../diskless-metadata-coordinator-v2/design.md#flush-policy) describes, so a fence adds no fsync. It costs a request, a record and its SQLite apply at the participant, and an answer record at the tracking partition. A storm's harm is that work and the overlay slots it takes on the partition that is recovering. A full overlay rejects commits.

### Counts before batching

At `N=8`, with `B` brokers and the default `produce.commit.interval.ms` of 250 ms, the rules from [When the tracking partition fences silent participants](01-silent-participant-fences.md), [Lease lifecycle at the broker](02-lease-lifecycle.md), and [Release reports](05-release-reports.md) give these counts:

| Event | Fences |
| --- | --- |
| One partition `t` is unavailable for `D` seconds | Most files have a part at `t`, so every recorded finish report carries a new exception range for `t`: about `B × D / 0.25` range fences, all delivered when `t` recovers. At `B=30` and `D=60`, about 7,200. Each broker whose tracking partition is `t` also releases its abandoned range there, which adds `N` fences each, about `B` in total. The fallback leaves almost no missed sequences. |
| A rolling restart of every broker | Each epoch end costs `N` range fences, or `2N` with a fallback range: `B × N` in total, 240 at `B=30`, spread over the restart. |
| An availability zone goes down | A fenced broker keeps its epoch, so nothing fires until the idle timers end the leases: about `B/3 × N` range fences within seconds, 80 at `B=30`. |

Only the first row grows without bound. Busy brokers never flush urgent reports early, so early flushes add nothing to it.

### Batching

- Unit: one fence request per tracking partition and participant. It carries `(brokerId, brokerEpoch)` groups, and each group holds sequence ranges.
- Batching needs no timer: the tracking partition keeps at most one fence request in flight per participant. Fence needs that its records create while a request is in flight go into the next one. This mirrors the release reports in [Release reports](05-release-reports.md#batching).
- Within a group, two ranges merge into their hull, `[min a, max b]`. A fence's effect on the floor depends only on `b`, and every sequence in the hull is at or below a bound the broker has finished or inside an ended lease. The answers for the sequences in between are truthful and leave held or released entries unchanged, as [When the tracking partition fences silent participants](01-silent-participant-fences.md#finish-report) already allows.
- Records: the participant appends one sequences-fenced record per request, which raises each group's fenced-through pair, and the tracking partition appends one participant-state-recorded record per reply.
- Bound: the same internal constant as release reports caps a request's encoded size, for example 512 KiB. The tracking partition splits a larger set across consecutive requests. Nothing about batching is configurable.
- A new tracking leader rebuilds the pending set from recorded state and sends one batched request per participant.
- Fences aren't rate-limited and don't queue behind commits. The in-flight bound already caps a participant's fence work at `N - 1` requests. A rate limit would add a config and delay resolution.

Rejected alternatives:

- Coalescing per broker epoch only. A tracking partition still sends up to `B` requests to one participant.
- Accepting the storm because it costs no fsync. Its work still grows with the outage length and lands on the recovering partition's overlay.

### Counts after batching

| Event | Requests |
| --- | --- |
| One partition recovers after an outage of any length | At most `N - 1` arriving at once, one from each other tracking partition, plus `N - 1` it sends as a tracking partition |
| One broker restart in a rolling restart | `N` range fences, or `2N`, that merge into pending requests |
| An availability zone goes down | About `B/3 × N` ranges, carried in at most `N - 1` requests per tracking partition at a time |

The lease fallback uses the same batched path, so it adds no second fence mechanism.

### Broker-wide `finishedThrough`

A participant keeps one floor per broker, not one per tracking partition, so a range fence up to `b` makes every sequence of that broker at or below `b` stale there, whichever partition issued its range. [When the tracking partition fences silent participants](01-silent-participant-fences.md#terms) defined `finishedThrough` over the tracking partition's own ranges only. After a fallback or a switch back to `t`, a file under another partition's range can have a lower sequence and still be retrying:

- Exception fences are safe. An exception at participant `p` for `b` means `b`'s part at `p` finished, and the per-queue order finishes every lower part at `p` first.
- A missed sequence `b` at `t` can have no part at `p`. Its fence raised `p`'s floor to `b` while file `f` under `t + 1`, with a lower sequence, still retried at `p`. `f`'s part got stale-sequence, and its batches failed. A lease release up to `b` did the same when an earlier abandoned range at another partition still had an unfinished file.
- Neither lost data: `t + 1`'s finish report lists `p` as an exception, and its fence answers "released". Both failed a produce that could still commit.

The fix:

- `finishedThrough` becomes: every file of this broker epoch with a sequence at or below this value is finished, whichever tracking partition issued its range. The broker already knows it. Before the broker finishes any file in the epoch, it's one below the epoch's first sequence.
- Exception ranges stay scoped to the tracking partition's own ranges.
- A lease release waits until every file of the epoch at or below `b` is finished, not only the files under the abandoned ranges.
- Missed-sequence and lease-end fences then never raise a floor past a part that is still retrying.
- Cost: right after a switch, `finishedThrough` and the release can trail by one more commit deadline. Only fences wait.

Rejected alternatives: leaving the race to [Model of the fence, floor, and release protocol](17-protocol-model.md), which would close this ticket with a known hole in the fences it bounds, and a ticket of its own for a one-line definition change.

### Metrics

| Metric | Where | What it shows |
| --- | --- | --- |
| `FenceRate`, tagged `type` (exception, missed, or lease-end) and `answer` (referenced or released) | Leader of the tracking partition | Fence ranges issued, by cause. It counts ranges, not requests. The `sequence` type leaves with the silent-participant trigger. |
| `PendingFenceRangeCount` | Leader of the tracking partition | Recorded fence ranges without an answer, in flight or waiting for the next request. It grows with the broker count during a participant outage and drains within a few round trips after recovery. |

`TrackingEntryOldestAgeMs` with `state=fencing`, from [Release reports](05-release-reports.md#leak-signal), already shows a fence that never answers, so the ticket adds no age metric. A request-rate metric or a batch-size histogram would only confirm that batching works, and a `partition` tag on the gauge multiplies series by `N`.

### Tests

Placement in the test layers and the failure rows belong to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Named test: a participant outage under load. One coordinator partition is unavailable for 60 seconds at `N=8`. When it recovers, each tracking partition has at most one fence request in flight to it, `PendingFenceRangeCount` drains, and no commit is rejected for a full overlay.
2. Hull merge: two exception ranges for one broker epoch, with answered sequences between them, go out as one fence. The entries in between stay held.
3. Broker-wide `finishedThrough`: the broker switches back to `t` while a file under `t + 1` retries at `p`. A missed sequence at `t` has no part at `p`. Its fence waits until the `t + 1` file finishes, and that file's part commits.
4. A lease release after a switch waits for lower-sequence files under another partition's ranges.
5. A new tracking leader rebuilds the pending set and sends one batched request per participant.
6. A fence set above the size cap splits across consecutive requests, and the answers don't change.
