# When the tracking partition fences silent participants

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: none

## Question

When may a tracking partition fence a participant it hasn't heard from, so that a file that can still commit is never fenced on the normal path?

v2's [Releases](../../diskless-metadata-coordinator-v2/design.md#releases) releases an empty part at the tracking partition in the apply that records it. [Fences](../../diskless-metadata-coordinator-v2/design.md#fences) fences every silent participant once the tracking partition and every participant it has heard from are released. With an empty tracking part, both conditions hold at once, so every participant is fenced while its part may still be in flight. A delayed part, after a leader election, a `NOT_COORDINATOR` retry, or a queue behind `s - 1`, gets a non-retriable stale-sequence, and a failover becomes failed produces. Even when the parts win the race, each such file costs one fence request and fsynced participant-state records per participant. A broker whose tracking partition holds none of its user partitions pays this on every file. See [Fable's first finding](../reviews/fable.md#an-empty-tracking-part-fences-in-flight-commits).

Settle these points:

- The condition that makes a silent participant safe to fence. Fable's candidates: the broker piggybacks a resolved-through sequence on later tracking parts and lease requests, and the tracking partition fences only at or below it or at the lease end. Or participants report committed sequences in batches.
- What replaces "released in the apply that records it" for an empty tracking part and an all-fail part.
- The normal-path cost per file after the fix, compared with the Postgres backend, and how the claim "fan-out runs only on failure" in [postgres-performance-bar.md](../../diskless-metadata-coordinator-v2/research/postgres-performance-bar.md) changes.
- Whether the orphan cases in [Orphan cases](../../diskless-metadata-coordinator-v2/design.md#orphan-cases) still each reach the deletion queue.

Named test: an empty tracking part with a participant failover in the middle of a commit, where the produce succeeds within the deadline.

## Resolution

Resolved on 2026-11-12 in a grilling session.

Amended by [Protocol model](17-protocol-model.md#protocol-changes): a sequence is missed once a recorded `finishedThrough` reaches it, and the tracking partition applies this rule before any lease end in the same record. This replaces "detects a missed sequence when it records a later one" in [Fences](#fences).

The tracking partition fences a participant only after the broker has finished the file. The broker tells it which files are finished, and which participants didn't answer, in a finish report. The tracking partition never fences a participant that answered. It waits for that participant's release report.

### Terms

These terms go into the design's Terms section:

- Finished part: a part that the broker stops sending. The broker stops when the part commits, when the leader rejects it with a non-retriable error, or when its file's commit deadline passes. v2's [Commit path](../../diskless-metadata-coordinator-v2/design.md#commit-path) already uses "finished" with this meaning.
- Finished file: a file whose every part is finished, the tracking part included. The broker never sends another attempt for it. A file whose upload failed is finished at once.
- `finishedThrough`: every sequence at or below this value in the tracking partition's ranges belongs to a finished file. It's an inclusive bound, as v2's fenced-through pair is. Before the broker finishes any file under its ranges there, it's one below the first range's start. Files finish out of order, so it trails the slowest unfinished file, by at most one commit deadline.
- Finish report: `finishedThrough` plus the exception ranges.

[Fence fan-out under correlated failure](06-fence-fan-out.md#broker-wide-finishedthrough) widens `finishedThrough` to every file of the broker epoch, whichever tracking partition issued its range, and batches the fences into one request in flight per participant.

The names `finishedBelow` and "high watermark" were rejected. `finishedBelow` was an exclusive bound, as Kafka's high watermark is, but the fences compare it with the fenced-through pair, which is inclusive, and one convention for both reads more easily. "High watermark" promises that everything below it is committed, and the commit path already uses the coordinator partition's own high watermark.

### Answered and exception

- A participant answered when its last reply came from a recorded file-committed record, pending or applied. This includes an all-fail commit and a retry answered from the stored result.
- Every other outcome is an exception. The exceptions are these:
  - A rejection before the producer checks, such as a stale epoch, a stale sequence, a mapping mismatch, or a full overlay.
  - `NOT_COORDINATOR` until the deadline.
  - An unknown outcome at the deadline.
  - A part that the broker never sent.
- The part reply gets a `Recorded` flag, set when the reply comes from a file-committed record. Per-batch error codes alone can't tell an all-fail commit from a rejection.
- A stale-sequence rejection stays an exception. Its fence appends nothing at the participant, because the sequence is below the floor. Taking the broker's word as "released" would let a bug in the broker delete data.
- The broker's claim decides only whether the tracking partition waits or fences. A participant is released only by its own release report or a fence answer, so a wrong "answered" claim leaks an object and never deletes one.

### Finish report

- A finish report holds `finishedThrough` and at most one exception range `[a, b]` per participant, covering its exceptions at or below `finishedThrough`. If a range covers sequences the participant answered, the fence answers "referenced" or "released" for them, which is harmless.
- The broker adds the current report to every tracking part and lease request. Because the file-committed record or the sequences-leased record of that request stores the report, it costs no append of its own and is still in SQLite for a new leader after a failover. A retry sends the same report as its first attempt.
- The broker keeps a report until a recorded tracking part or a confirmed lease request includes it, and merges it into newer reports until then. The payload is at most one `int64` plus `N - 1` ranges.
- The broker sends a tracking partition only the reports for its own ranges. After a lease fallback, the old tracking partition's unsent reports wait for its lease end.

### What replaces "released in the apply that records it"

- The tracking partition's own entry for an empty or all-fail tracking part is still released in the apply that records the part. Its release no longer triggers fences.
- Every other participant in the part's participant set starts waiting for a finish report. It moves only on these events:
  - A finish report marks it as answered. It then waits for the participant's release report.
  - A finish report lists it as an exception. The tracking partition sends the range fence.
  - Its release report arrives.
  - A fence answer arrives.
  - The lease ends.
- The object enters the deletion queue in the apply that releases its last participant, as in v2.
- The ordering of these events, and whether "released" is terminal, belong to [Release reports: ordering, batching, and a leak signal](05-release-reports.md).

### Fences

The tracking partition sends fences in the following cases. Each waits until the broker has finished the files it covers:

- Exceptions: one range fence per exception range in a recorded finish report. Raising the participant's floor to `b` is safe, because the broker never sends a sequence at or below `finishedThrough` again. A finish report triggers at most `N - 1` fences, however many files it covers.
- Missed sequences: the tracking partition still detects a missed sequence when it records a later one. It fences the missed range at all `N` partitions only once a recorded finish report has `finishedThrough` at or above the range's end, or the lease ends. Consecutive missed sequences merge into one range fence per partition.
  - The broker never supplies a missed sequence's participant set. A bug in the broker that left a participant out would delete an object that the participant still references.
  - v2's missed-sequence fence had the same race as Fable's finding. Parts of the missed sequence on other partitions can still be retrying when the later tracking part records.
- Lease ends: one range fence per partition, from the sequence after the recorded `finishedThrough` to the highest range end. The range covers these sequences:
  - Used sequences without a finish report.
  - Missed sequences.
  - Unused sequences, as in v2.

  For a sequence with a recorded participant set, each participant's answer moves its entry, and answers from other partitions don't change it. For a missed or unused sequence, the referenced answers build the participant set, as in v2. After an epoch ends, only late requests are in flight. After an idle end, every deadline has passed, because the commit deadline is at or below the produce request timeout. The fallback case where this isn't true belongs to [Lease lifecycle at the broker](02-lease-lifecycle.md).

v2's silent-participant trigger leaves the design. It fenced once the tracking partition and every participant it heard from were released. It also fenced on the retention path: once the tracking partition's own batches went, it fenced every participant that hadn't released yet. Each answered "referenced" and released later anyway. With mixed retention, the tracking partition could fence on day 1 and then wait 29 days.

A new tracking leader resends every fence that recorded state calls for and that has no recorded answer, as in v2. Finish reports are in records, so it derives the same fences.

### Orphan cases

Every orphan case in v2's [Orphan cases](../../diskless-metadata-coordinator-v2/design.md#orphan-cases) still reaches the deletion queue:

| Orphan case | Path in v3 |
| --- | --- |
| A broker stops before the upload | The lease end's range fence answers "released," and the key is queued. Unchanged. |
| A broker stops after the upload, before any part commits | If the broker is still running, its next tracking part marks the sequence missed, and the fence goes out once `finishedThrough` reaches it. If it goes idle or restarts, the lease end covers it. |
| A broker stops after some parts commit | No finish report covers the file. The lease end covers it instead. Committed participants answer "referenced" and release later, and the others answer "released." |
| A part at `s` has an unknown outcome, then `s + 1` commits there | `s` is an exception in a finish report. The range fence answers "referenced" or "released." |
| Leadership moves around the storage delete | Unchanged. The queue is in SQLite on every replica. |

### Cost

For a file whose parts all answer:

| Cost | v2 | v3 |
| --- | --- | --- |
| Tracking part, empty or not | One part, batched into the tracking partition's linger append | Unchanged |
| Finish report | None | One `int64` in the tracking part and its file-committed record. No request, record, or fsync. |
| Fences at commit time | One per participant when the tracking part is empty or all-fail | None |
| Fences at retention time | One per participant still silent when the tracking partition releases | None |
| Release reports | One per object and participant | Unchanged. Batching belongs to [Release reports: ordering, batching, and a leak signal](05-release-reports.md). |

The verdict against the Postgres backend is "Within," as for v2's ownership model, because the fix doesn't add a wait or an fsync. A broker whose tracking partition holds none of its user partitions pays only the empty tracking part, as every broker does.

In the rewrite's performance section, "Fan-out runs only on failure" from [postgres-performance-bar.md](../../diskless-metadata-coordinator-v2/research/postgres-performance-bar.md) becomes: "Fences run only on failure, and only after the broker finished the file. Each finish report triggers at most `N - 1` range fences, one per participant with exceptions. Missed ranges and lease ends each trigger one range fence per partition." The "Costs that stay off the path" entry changes to match. The worst case under correlated failure belongs to [Fence fan-out under correlated failure](06-fence-fan-out.md).

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Named test: an empty tracking part with a participant failover in the middle of a commit, where the produce succeeds within the deadline. The participant's part gets `NOT_COORDINATOR` and then commits at the new leader before the deadline. The test checks three outcomes:
   - The produce succeeds.
   - The tracking partition doesn't send a fence, before or after the finish report covers the sequence.
   - The tracking partition keeps the object's entry until the participants release it.
2. A missed sequence waits for the broker: the tracking part of `s` fails while another participant's part of `s` is still retrying. That part commits, and the missed-sequence fence goes to all `N` partitions only after `finishedThrough` reaches `s`.
3. Exceptions coalesce: a participant is unavailable across several files. The finish report includes one exception range, and the tracking partition sends that participant one range fence, which answers "referenced" for any late part that committed.
4. The `Recorded` flag: the tracking partition doesn't fence a participant whose part failed every batch. It fences a participant that rejected the part as stale, and that fence appends nothing at the participant.
5. The tracking part with a finish report fails, and the tracking partition doesn't record it. The next tracking part includes the merged report, and the fences follow it.
6. A lease end covers unreported sequences: the broker stops after some parts commit, before a finish report covers the file. Its restart ends the lease, the range fence covers the file, and the tracking partition queues the object only after the committed participants release it. This test replaces v2's "a broker that stops after some parts commit."
7. With no failed part, the shared behavioral suite at `N=8` doesn't send a fence, through commits and through retention.
