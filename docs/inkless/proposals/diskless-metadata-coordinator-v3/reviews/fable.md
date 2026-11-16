# Fable's review of v2

Verbatim copy of Fable's review of [v2](../../diskless-metadata-coordinator-v2/design.md) and its comparison with the [council review](council.md), as the user pasted them on 2026-11-10.

## Verdict

- Safety: I found no path that deletes a referenced object or lets a part commit into a deleted key. Every route into the deletion queue needs a durable floor at every partition that could still commit. That holds given flush.messages=1, no unclean election, and the storage-identity guard.
- Performance: the design misses the Postgres bar in one normal-path case (first finding), and three more cases turn delays into producer-visible failures.

## Correctness

### An empty tracking part fences in-flight commits

- Severity: Critical (performance and availability, not data loss)
- Location: design.md Releases and Fences ("Silent participants"); confirmed by issues/04-flush-policy.md:44
- Issue: An empty tracking part is released in the apply that records it, and no participant has been heard from yet. The tracking partition therefore fences every participant of the file at once.
  - A participant part that is delayed (leader election, NOT_COORDINATOR retry, queued behind s-1) gets a non-retriable stale-sequence, and so does a pending s-1. A coordinator failover becomes failed produces instead of a delay for these brokers.
  - Even when the part wins the race, every such file costs one fence RPC per participant plus fsynced participant-state records at the tracking partition. research/postgres-performance-bar.md:186 assumes "fan-out runs only on failure".
  - A broker whose tracking partition holds none of its user partitions hits this on every file.
- Recommendation: Don't fence silent participants while the file can still be committing. Have the broker piggyback a "resolved-through sequence" on later tracking parts and lease requests, and fence only at or below it, or at lease end. Alternatively, let participants report "committed" in a batch.

### A fallback lease's idle timer cuts the range in use

- Severity: Warning
- Location: design.md Leases, Fences ("Lease ends"), Ending a lease
- Issue: The claim that an idle partition's ranges sit below the range in use is false after a fallback. The fallback range sits above the range in use and receives no parts until the broker reaches it. If that takes longer than 15 minutes (under about 0.6 files per second), its lease ends and the range fence raises every floor past the live range. All in-flight and subsequent files fail until the broker re-leases.
- Recommendation: Switch to the fallback range as soon as it's confirmed and abandon the lower remainder. Or don't start the idle timer until the first part names the range.

### The first file after an idle period always fails

- Severity: Warning
- Location: design.md Ending a lease, Time bounds
- Issue: After the lease-ended record, nothing tells the broker. Its next file gets stale-sequence, and files closed before the new lease confirms fail with no lease. This is the normal behavior for any broker that is idle for 15 minutes, not only for clock jumps. Postgres has no such failure.
- Recommendation: Have the broker drop its lease when it's been idle longer than about half the timeout and re-lease on the first append, letting that one file wait. At minimum, document it and stop treating StaleSequenceRejectRate after an idle end as an anomaly.

### The retired marker rejects a re-registered broker ID forever

- Severity: Warning
- Location: design.md Acceptance rules against Ending a broker epoch
- Issue: The table says "row is retired → stale epoch" unconditionally. The text says a returning broker ID "commits as a new epoch". The marker carries no epoch, so the leader can't tell the two apart.
- Recommendation: Store a retired-through epoch in the record and reject only e <= that epoch.

### Topic deletion is inferred from absence in the image

- Severity: Warning (the outcome is irreversible)
- Location: design.md Lifecycle
- Issue: "SQLite has the topic and the image doesn't" appends a topic-deleted record, which purges and then deletes objects. This is safe only if no row can exist ahead of the leader's image. initDisklessLog and repairDisklessLog come from a partition leader whose image can be newer, and the design doesn't say they require a reconciled row.
- Recommendation: Reject every call for an unreconciled partition. Record the metadata offset in the partition-created record, and append topic-deleted only when the leader's image offset is at or past it.

### The release-report state machine is under-specified

- Severity: Warning (leak only)
- Location: design.md Releases, Fences
- Issue: A release report can reach the tracking partition before the tracking part does, which is common for all-fail parts. A delayed "referenced" fence answer can also arrive after the release. If the report is dropped, or "referenced" overwrites "released", the object is never queued.
- Recommendation: State that "released" is terminal per participant and sequence, and that reports for unknown sequences are recorded.

## Design and maintainability

### SQLite checkpoint fsync on the shard thread

- Severity: Suggestion
- Location: design.md Local state
- Issue: synchronous=NORMAL still fsyncs at WAL auto-checkpoint, on the thread that applies and replies. This is a periodic stall on the commit path.
- Recommendation: Turn off auto-checkpoint, checkpoint from a background thread, and apply one SQLite transaction per high-watermark advance instead of one per record.

### Fsync amplification on small clusters

- Severity: Suggestion
- Location: design.md Flush policy, Capacity and isolation
- Issue: One file costs up to (participants + 1) × replication factor fsyncs, against one for Postgres. On a 3-broker cluster with N=8, every broker fsyncs all 8 partitions on each commit round. The design defers this to benchmarks.
- Recommendation: Benchmark the smallest supported cluster first, and consider a lower default N there.

### Release traffic isn't batched

- Severity: Suggestion
- Location: design.md Releases
- Issue: A retention pass releases many objects, and each one costs a report RPC and two records.
- Recommendation: Batch the reports and records per tracking partition.

## Consistency, security

No findings.

## Tests

- Severity: Warning
- Location: design.md Testing
- Issue: No test covers the first three findings or the re-registration case.
- Recommendation: Add these tests:
  - An empty tracking part with a participant failover in the middle of a commit, where produce succeeds within the deadline.
  - A low-rate broker after a lease fallback.
  - A broker that resumes after an idle lease end.
  - A broker that is unregistered, re-registers with the same ID, and commits.
  - A release report that arrives before the tracking part.

## Fable's comparison with the council review

They overlap little and mostly complement each other: the council reviewed risk posture, while I traced the protocol for concrete defects. Neither contradicts the other on safety.

### Where we agree

- Safety as specified: neither review found a path where the protocol, working as written, deletes a referenced object. Both say it rests on flush.messages=1 and an honest fsync.
- Fsync cost: both flag it as the main Postgres-bar risk. The council says it's unmeasured; I added the count, up to (participants + 1) × replication factor fsyncs per file, which is worst on small clusters.
- Fence fan-out: the council worries about a fence storm from missed sequences under correlated failure. My first finding is the stronger version: the fan-out happens on the normal path whenever the tracking part is empty.
- Lease fallback: the council calls it a rarely exercised second path. I found a concrete bug in it, where the fallback range's idle timer cuts the range in use.
- Release reports: both say objects can leak quietly. The council's mechanism (a lost report) is actually covered by the retries and the release-accepted record. The leak I found comes from ordering: a report that arrives before the tracking part, or a late "referenced" answer that overwrites "released". Their point that no metric shows it stands.

### What the council has and I missed

- Story 83 overpromises: a file completes only when every part answers, so a stuck coordinator partition delays every produce in files that touch it, which is most files. This is valid, and I should have flagged it. It isn't worse than Postgres, but the story and the "delays only its own partitions" wording are wrong.
- Parity of cause, not consequence: a lost tail or a lying fsync here deletes or resurrects another partition's data, and losing a replica set costs 1/N of all diskless partitions. I noted the dependency but didn't challenge the accepted-risk rows. The argument is fair.
- Mitigations: object footers, soft-delete or versioning, a deletion-rate cap, benchmark pass criteria, load shedding, and enforcing replication factor 3. Most of these are things map.md puts out of scope, so they are a challenge to the scope rather than defects in the design.

### What I have and the council doesn't

The council found none of the five concrete protocol defects:

1. An empty tracking part fences in-flight commits.
2. A fallback lease's idle timer cuts the live range.
3. The first file after an idle period always fails.
4. The retired marker rejects a re-registered broker ID forever.
5. Topic deletion is inferred from absence in the image.

That fits its own caveat that most members didn't read all of the spec, tickets, or research.

### How I'd combine them

The council's call for a model check of the fence, floor, and release protocol is the recommendation my findings support most: defects 1, 2, 4, and the release ordering are the kind a TLA+ or property-test model would catch. I'd fix my defects 1 to 5 and story 83 in the design first, then decide whether to reopen the out-of-scope items (footers, soft-delete), which is your call on risk appetite.
