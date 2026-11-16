# File completion and story 83

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: none

## Question

Does a produce wait for every part of its file, and if so, what does v3 promise about a stuck coordinator partition?

v2's [Commit path](../../diskless-metadata-coordinator-v2/design.md#commit-path) completes a file once every part has an answer, and [Capacity and isolation](../../diskless-metadata-coordinator-v2/design.md#capacity-and-isolation) says a produce waits for its slowest part. Most files touch most coordinator partitions, so a stuck coordinator partition delays most produces until the commit deadline. Story 83 in [spec.md](../../diskless-metadata-coordinator-v2/spec.md) promises that a stuck coordinator partition delays only the partitions that map to it. Three council members flagged this, and Fable agrees: it isn't worse than Postgres, but the story and the wording are wrong. See the council's [fifth status row](../reviews/council.md#status-of-the-earlier-findings) and [fourth fix](../reviews/council.md#what-would-close-the-gaps).

Settle these points:

- Keep whole-file completion and correct story 83 and the "delays only its own partitions" wording, or complete the produce response per batch as each part answers.
- If per batch: what the request-level answer is when parts of one produce request finish at different times, and what Kafka's produce path allows. The map's Not yet specified section holds the follow-on detail.
- The bound on how long a stuck coordinator partition delays an unrelated produce, stated in terms of the commit deadline.

## Resolution

Resolved on 2026-11-12 in a grilling session.

The broker completes each produce request once every part that holds the request's batches has finished, instead of waiting for the whole file. Story 83 then holds, stated per request.

### Why per request, not per batch

- A produce request goes entirely into one file. `ActiveFile.add` registers one future per request, and `AppendCompleter` completes every future of the file at once.
- Kafka sends exactly one response per produce request. In `DelayedProduce`, a classic `acks=all` request waits until every one of its partitions is satisfied or the timeout passes. Partitions still pending at that point get `REQUEST_TIMED_OUT`, and the rest succeed in the same response. A per-batch answer isn't possible on this protocol, so the request is the finest granularity.
- A classic request also waits for its slowest partition, and no other request waits on it. Per-request completion gives diskless topics the same isolation.
- A client that writes to many partitions in one request still touches most coordinator partitions. The gain goes mostly to requests that write to a few partitions.

Rejected alternative: keeping whole-file completion and only rewording story 83. A stuck coordinator partition would then delay every request that shares a file with a request that touches it, by up to one commit deadline.

### Request completion and file completion

- A request waits only for the parts that hold its batches. It waits for the tracking part only when the tracking partition holds one of its batches. The tracking part's answer matters for deletion, and a participant's commit stands on its own, as [When the tracking partition fences silent participants](01-silent-participant-fences.md) records.
- A request completes when the last of its parts finishes. The broker sends no partial answer before that. The response mixes per-batch results: committed batches get their offsets, rejected batches their errors, and batches whose part didn't confirm by the deadline get `KAFKA_STORAGE_ERROR`.
- `KAFKA_STORAGE_ERROR` stays the error for an unknown outcome, the same error every backend returns for a failed commit. Rejected alternative: `REQUEST_TIMED_OUT`, which is closer to classic semantics. It's a client-visible change, and it would either differ between backends or change every backend.
- A file is still finished only when every part is finished, the tracking part included. `finishedThrough`, the finish report, and `FileCommitJob`'s cleanup when no part committed stay file-level events.

### `AppendCompleter` and the produce path

- `ClosedFile` records, for each request, the set of commit queues that hold its batches. Batches rejected at validation, in `invalidResponseByRequest`, belong to no queue. A request with only rejected batches completes at file close.
- When a part finishes, `AppendCompleter` records the per-batch results for that queue. Each request whose queues have all finished then completes, with results merged in request order as today.
- Each part fills the batch coordinate cache when it commits, not after the whole file finishes, so a consumer can read a request's batches as soon as the producer gets its answer.
- The Postgres and in-memory backends have one queue, so every request completes when the file does, as today.
- `ReplicaManager.appendRecords` doesn't change. It sends diskless entries to `AppendHandler.handle` and never puts them in `DelayedProduce` purgatory. For a mixed request, the classic entries wait in purgatory and the response callback joins them with the diskless future, which now completes sooner.
- `FileCommitTime` stays a per-file metric. It now measures the slowest part, not produce latency.

### Bound

- A request that holds no batch for a stuck coordinator partition `P` gets no added delay from `P`.
- A request that holds a batch for `P` completes within one commit deadline of its file's close. `P`'s batches get `KAFKA_STORAGE_ERROR`, and its other batches get their own results.
- The design states two invariants as requirements, because the bound depends on them:
  - Per-queue work never blocks a thread or a resource that other queues share.
  - An in-flight cap on the broker must not let files stuck on one queue block admission for requests that don't touch that queue. This invariant constrains [Admission cap or retry budget](15-admission-cap.md).
- Scope: the bound covers one stuck coordinator partition. A stuck leader broker stalls every coordinator partition it leads, so its blast radius is the user partitions of all of them. The failure-mode row says so, and the co-tenancy text in Capacity and isolation already describes that case.

Story 83 becomes: "As a Kafka client, I want a stuck coordinator partition to delay only the produce requests that write to its user partitions, and those by at most one commit deadline, so that one failure doesn't stall every diskless produce."

The rewrite replaces these v2 statements:

- [Commit path](../../diskless-metadata-coordinator-v2/design.md#commit-path): "A file completes once every part has an answer" becomes the request and file completion rules in this resolution.
- [Capacity and isolation](../../diskless-metadata-coordinator-v2/design.md#capacity-and-isolation): "A produce waits for its slowest part" becomes "A produce request waits for the slowest part that holds its batches."
- spec.md: story 83 and the matching file-completion line.

### Failure-mode row

| Failure | Effect | Recovery | Test |
| --- | --- | --- | --- |
| One coordinator partition is stuck: unavailable, slow, or rebuilding | Requests with a batch for it complete at their file's commit deadline, with `KAFKA_STORAGE_ERROR` for those batches. Other requests aren't delayed. A stuck leader broker extends this to every coordinator partition it leads. | Producers retry the failed partitions. The sequence rules resolve the unknown outcomes. | Test 1 |

### Benchmarks

[Benchmark pass criteria](16-benchmark-pass-criteria.md) judges produce latency with Kafka's per-request produce metrics, not `FileCommitTime`. The partial-failure benchmark passes only if requests that hold no batch for the offline coordinator partition don't wait for its deadline.

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Named test: one file holds request `A`, whose partitions map only to healthy coordinator partition `P1`, and request `B`, whose partitions map to `P1` and to stuck coordinator partition `P2`. `A` completes once `P1`'s part commits, before `P2`'s deadline. `B` completes at the deadline, with its own results for the `P1` batches and `KAFKA_STORAGE_ERROR` for the `P2` batches.
2. A stuck tracking partition: a request with no batch at the tracking partition completes once its parts commit. The file stays unfinished until the tracking part finishes, so `finishedThrough` doesn't pass that file's sequence until then.
3. A request with only rejected batches completes at file close, with no part to wait for.
4. The cache fills per part: a fetch for `A`'s batch finds the batch coordinate in the cache before `B`'s file finishes.
5. The single-queue backends don't change: the shared behavioral suite on Postgres and in-memory shows every request completing when the file does.
