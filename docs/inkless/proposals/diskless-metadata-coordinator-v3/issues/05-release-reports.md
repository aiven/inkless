# Release reports: ordering, batching, and a leak signal

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 01

## Question

What is the per-participant state machine at the tracking partition, and how are release reports batched and made visible?

- Ordering: a release report can reach the tracking partition before the tracking part, which is common for all-fail parts. A late "referenced" fence answer can also arrive after the release. If the report is dropped, or "referenced" overwrites "released", the object is never queued. See [Fable's release-report finding](../reviews/fable.md#the-release-report-state-machine-is-under-specified). The council's lost-report case is covered by retries and the release-accepted record, as Fable notes.
- Batching: a retention pass releases many objects, and each costs a report request and two records. See [Fable's batching suggestion](../reviews/fable.md#release-traffic-isnt-batched).
- Signal: no metric shows a leaked release. See the council's [release-report risk](../reviews/council.md#new-risks-in-v2).

Settle these points, against [Releases](../../diskless-metadata-coordinator-v2/design.md#releases) and [Fences](../../diskless-metadata-coordinator-v2/design.md#fences) as [When the tracking partition fences silent participants](01-silent-participant-fences.md) changes them:

- Whether "released" is terminal per participant and sequence, and whether a report for a sequence the tracking partition hasn't recorded is stored.
- The order of the events that move a participant's entry, as [When the tracking partition fences silent participants](01-silent-participant-fences.md#what-replaces-released-in-the-apply-that-records-it) lists them. This includes a release report that arrives before the finish report that covers its sequence.
- The batch unit for reports and records, and its bound.
- The metric or gauge that shows releases or tracking entries that never complete.

Named test: a release report that arrives before the tracking part.

## Resolution

Resolved on 2026-11-12 in a grilling session.

"Released" is terminal for each participant and sequence, and the tracking partition stores a release report even when it hasn't recorded the sequence yet. Each entry moves forward through four states and never back. Participants batch their reports, one request in flight per tracking partition. Gauges on the states that must clear quickly show a stuck release.

### Released is terminal

Once a participant's entry for `(brokerId, brokerEpoch, sequence)` is released, every later event leaves it unchanged. This includes finish reports, fence answers, lease ends, and repeated release reports. The record still applies and still counts in metrics.

The rewrite states the safety argument as an invariant. After a participant releases a sequence, it can't come to reference that sequence again:

- Release by the emptiness check: the part committed, so the broker's producer state at the participant is at or above the sequence. A retry gets the stored result and doesn't commit again.
- An all-fail release: a retry gets its answer from the stored file-committed record.
- Release by a fence answer: the sequence is at or below the participant's fenced-through pair and can't commit there again.
- A "referenced" answer that races a release report: the participant applied the fence before the apply that removed its last batch. Its log already orders the two, and only the network reorders the messages.

### Reports for unrecorded sequences

An all-fail participant releases in the apply that records the part, and it reports at once, so its report often arrives before the tracking part. The tracking partition stores the report. If it dropped the report and accepted it, the participant would append release-accepted and never resend, and its entry would wait forever once the tracking part recorded.

When no entry exists for the sequence, the tracking partition classifies the report from the highest sequence it has recorded in that range. Any unrecorded sequence below that is a missed sequence by definition:

| Sequence | Action |
| --- | --- |
| Above the range's highest recorded sequence | Early report. The tracking partition stores the entry as released. When the tracking part records, its participant set merges with the stored entry. |
| A missed sequence with unanswered fences | The tracking partition stores the entry as released. It counts as that participant's answer, the same one a fence gets. |
| At or below the highest recorded sequence, with no entry | The object is already resolved. The tracking partition accepts the report as a no-op. This case is a duplicate after a participant leader change, before release-accepted applied. |
| Outside every range that this partition leased to the broker and epoch | The tracking partition rejects the report with a non-retriable error and counts it, because only a bug sends one. |

Early entries are bounded: at most `N - 1` per sequence, and only for sequences in leased ranges. The tracking part, the missed-sequence fences, or the lease end resolves each one.

Rejected alternative: a retriable rejection until the tracking part records. A tracking part that never records resolves through fences, its entry goes, and the participant retries forever unless the tracking partition can answer "already resolved". That needs the same classification as storing the report.

### Entry states

"Answered" from a finish report and "referenced" from a fence mean the same thing to the tracking partition, so they merge into one state, held:

| State | Meaning |
| --- | --- |
| Pending | The participant set is recorded, and no finish report, fence answer, or release report covers this participant yet. |
| Fencing | A range fence that covers the entry is out, with no recorded answer. |
| Held | The participant holds batches and owes a release report. |
| Released | Terminal. |

The entry moves only forward:

- Pending to held: a finish report says the participant answered, or a fence answers "referenced".
- Pending to fencing: a finish report lists the participant in an exception range, or the lease ends.
- Fencing to held: the fence answers "referenced".
- Any state except released, to released: the participant's release report arrives, or a fence answers "released".
- Held never goes back to pending or fencing. A finish report that lists a held entry as an exception leaves it held.

The object enters the deletion queue in the apply that moves its last entry to released, as in v2.

This changes the fences from [When the tracking partition fences silent participants](01-silent-participant-fences.md) in one way. The tracking partition doesn't send an exception-range or lease-end range fence to a participant whose entries in that range are all held or released. Those sequences can't commit there again, so the fence would cost a request and an fsync for nothing. The common case is an early release followed by a finish report that lists the same participant in a merged exception range.

A release report or a "referenced" answer from a partition outside the recorded participant set only comes from a bug, because a participant releases only a part it received. The tracking partition records the entry, leaves the participant set unchanged, and counts it.

### Batching

- Unit: one report request per participant and tracking partition. It carries `(brokerId, brokerEpoch)` groups, and each group holds sequence ranges. Retention removes old objects first, and one uploader's sequences rise over time, so a retention pass mostly releases consecutive sequences.
- Batching needs no timer: each participant keeps at most one report in flight per tracking partition. Releases that pile up while a report is in flight go into the next one. Under a trickle, each release goes out at once, as in v2. Under a retention pass, the batches grow by themselves.
- Records: the tracking partition appends one participant-state-recorded record per report and applies the whole report in one SQLite transaction. The participant appends one release-accepted record per accepted report, with the same groups and ranges. A retry resends the same report, and released entries are terminal, so the retry is a no-op.
- Bound: an internal constant caps a report's encoded size, for example 512 KiB, below the `max.message.bytes` of `__diskless_metadata`. The participant splits a larger set across consecutive reports. Nothing about batching is configurable.
- A tracking partition that is itself a participant still records its own releases in the apply that removes the batches.

Inter-AZ cost, with replication factor 3 across three AZs:

| Case | Cross-AZ bytes per released object and participant |
| --- | --- |
| v2, one report per object | About 570: about 170 for the request and reply, crossing AZs two times in three, and about 200 for each record's two follower copies. |
| v3, worst case with one 16-byte range per sequence | About 75 |
| v3, usual case with uniform retention | Close to 0: one report per participant and tracking partition per retention pass |

At `N=8`, a 9-broker cluster that closes a file every 250 ms per broker releases about 250 objects and participants per second. Under v2 that costs about 375 GB a month across AZs, or about $7.50 on AWS at list price. Release traffic isn't a cost driver either way. How coordinator traffic as a whole compares with the Postgres backend across AZs belongs to [Benchmark pass criteria](16-benchmark-pass-criteria.md).

### Leak signal

Held entries can live as long as topic retention, so their age isn't a signal. The signal comes from the states that must clear quickly:

| Metric | Where | What it shows |
| --- | --- | --- |
| `TrackingEntryCount`, tagged `state` (pending, fencing, or held) | Leader of the tracking partition | Entries in each state. |
| `TrackingEntryOldestAgeMs`, tagged `state` (pending or fencing) | Leader of the tracking partition | A stuck entry. A pending entry clears within one commit deadline plus the next tracking part or lease request, and at the latest when the lease idle timeout ends the lease. A fencing entry clears within a few round trips. |
| `PendingReleaseCount`, `PendingReleaseOldestAgeMs` | Leader of a participant | Releases without a release-accepted record. A high age means the tracking partition is unreachable or rejects reports. |
| `ReleaseReportRejectedRate` | Leader of the tracking partition | Reports outside the partition's leased ranges. Stays at 0 unless a bug sends them. |
| `ReleaseOutsideParticipantSetRate` | Leader of the tracking partition | Reports or "referenced" answers from a partition outside the recorded participant set. Stays at 0 unless a bug sends them. |

Each tracking entry stores a creation time: the leader's timestamp on the record that created it, as the deletion queue does for its mark time.

Accepted risk: a held entry whose participant no longer holds batches and never reports, after a broker wrongly claims "answered" or a participant skips its emptiness check. It leaks storage and never deletes data, so it passes the map's bar. The two counters and storage growth signal its causes. Only a cross-check against what participants hold would detect the leak itself, which is the report-only scanner that stays future work.

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Named test: a release report that arrives before the tracking part. An all-fail participant reports, then the tracking part records, the finish report arrives, and the other participants release. The tracking partition queues the object, and it sends no fence to the early participant.
2. A late "referenced" answer after a release: a fence answer of "referenced" arrives after the participant's release report. The entry stays released, and the object is queued.
3. An early report for a missed sequence: a participant reports a sequence whose tracking part never records. A later tracking part marks it missed, and the missed-sequence fences resolve it. The stored release counts as that participant's answer, and the tracking partition sends that participant no fence.
4. A duplicate after resolution: a new participant leader resends a release whose object is already deleted. The tracking partition accepts it as a no-op and stores no entry.
5. A report outside the leased ranges gets a non-retriable error and raises `ReleaseReportRejectedRate`.
6. Batching under retention: a retention pass releases thousands of objects at one participant. The participant sends one report per tracking partition with coalesced ranges, and each side appends one record per report. A lost reply resends the same report, and the result doesn't change.
7. A stuck fence: a participant doesn't answer a range fence, and `TrackingEntryOldestAgeMs` for fencing grows until the fence answers.
