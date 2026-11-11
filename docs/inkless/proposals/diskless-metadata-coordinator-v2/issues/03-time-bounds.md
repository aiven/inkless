# Time bounds in the chosen model

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: 01

## Question

For the ownership model that [Ownership model: intents or leases](01-ownership-model.md) picks, which decisions still depend on elapsed time, and how does each one stay safe under clock skew and long process pauses?

In v1, fences and tombstones expire on elapsed time, and the age check compares the uploader's first-send time with the leader's clock. See [Tombstones and fences](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#tombstones-and-fences). A clock skewed beyond the maximum commit age lets a late part commit pointers into a deleted object. A long pause, such as a VM freeze or a long GC, between the age check and the first-send stamp breaks the bound without any skew. The review proposed anchoring every age bound to the owner's intent append timestamp, carried by every part. Widening the expiry to about an hour only shrinks the window. In the lease model, time survives in the idle lease timeout and in `FileCleaner`'s grace period.

Settle these points:

- Every time-based decision in the chosen model, and whose clock each one reads.
- For each one, why neither skew nor a pause between a check and the action it guards can cause data loss, or the fix that removes the dependency.
- The tests: a skewed broker clock, and a pause injected between the age check and the send.
- The metrics that show skew or stuck requests before they matter.

## Resolution

Resolved in a grilling session on 2026-11-09, for the multi-partition lease model that [Ownership model: intents or leases](01-ownership-model.md) chose. No clock decides whether deleting a key is safe. A key enters a tracking partition's deletion queue only at a log position where a record makes it impossible to reference from then on. Clocks decide only when a queued key is deleted.

### Time-based decisions

| Decision | Whose clock | Role |
| --- | --- | --- |
| Idle lease timeout, `diskless.metadata.lease.idle.timeout.ms` | The tracking partition leader's monotonic clock. It persists nothing and restarts at each election. | Ends a silent broker's lease, which collects its leftover keys and closes the duplicate window for an idle broker. The fenced-through mark and the range fence keep data safe, not the timer. |
| `FileCleaner` grace period, `file.cleaner.retention.period.ms` | The marking leader's wall clock, written on the record, against the deleting leader's wall clock | Protects readers that hold cached coordinates, because `consume.batch.coordinate.cache.ttl.ms` is at most half the grace period. It also shrinks the late-upload leak. |
| Commit deadline | The uploading broker's clock | Decides when the broker gives up. The sequence rules resolve the unknown outcome, and [Broker-side deletion after a partial send](02-delete-after-partial-send.md) removes the broker's direct delete. |
| Retention (`retention.ms`) and `LogAppendTime` | The coordinator leader's wall clock against batch timestamps | A leader whose clock runs far ahead deletes early. A classic topic has the same exposure, so v2 records it as a Kafka-parity accepted risk. |

The snapshot interval, the snapshot transfer idle timeout, release-report backoff, the delegate's retry backoff, and the reconciler affect liveness only. Fences, floors, epoch retirement, and membership in the deletion queue use no clock.

### Idle lease timer

The timer is a `CoordinatorTimer` operation at the tracking partition, keyed per broker. Every part and lease request that names the partition reschedules the key, including one that the leader rejects or that a failed append reverts. That only delays a lease end. Its `generateRecords` appends the lease-ended record with no further idleness check: `CoordinatorTimerImpl` already drops a timer that fired and was overridden before its write event ran, and it ignores `NOT_COORDINATOR`. The group coordinator uses the same pattern for member session expiry.

Under skew, a pause, or a clock jump, the worst outcome is a lease that ends without cause or ends late:

- A part reaches the shard just as the timer fires. If the part runs first, it overrides the timer and the lease stays. If the timer runs first, the part to the tracking partition gets stale-sequence. A part of the same file that committed at a participant before the range fence stays readable, the fence answers that the participant holds the sequence, and the tracking partition tracks the object until the participant releases it.
- The leader pauses between the timer and the append. A short pause only orders the two events. If leadership moves, the new leader restarts the timer at its election, and the old leader's append fails at its old leader epoch.
- The leader's monotonic clock jumps forward. Leases end at the tracking partitions this broker leads, about 1/N of brokers. Their files in flight fail once with stale-sequence, the brokers renew above the floor, and producers retry.
- An uploading broker pauses past the timeout with files in flight. Its commits get stale-sequence. An upload that lands after the queue deleted its key is the accepted leak from [Remaining leak](../../diskless-metadata-coordinator/issues/05-sequence-leases.md#remaining-leak). The grace period prevents it for uploads that land within a minute of the mark.
- Leadership of a tracking partition changes more often than the timeout. Idle leases don't end, so leftover keys and the idle-broker duplicate window stay open until leadership holds for one timeout. This is a liveness cost only.

After a lease fallback, the previous tracking partition's lease ends on its idle timer as expected. The broker uses sequences in increasing order, so the range fence never cuts a live range.

### Grace period

The wall-clock mark time and the `markedBefore` comparison stay as [Object deletion without owners](../../diskless-metadata-coordinator/issues/06-object-deletion.md#filecleaner) decided. The Postgres backend already compares the marking broker's `arg_now` with the deleting broker's clock, so cross-broker skew in the grace period is the status quo.

- A deleting leader whose clock runs ahead deletes early. A reader with cached coordinates gets `KeyNotFoundException`, its fetch fails with a storage error, and the consumer retries with fresh coordinates. The batches were already gone from coordinator state, or for consolidation pruning, the data lives in tiered storage. v2 records skew beyond half the grace period as a reader-availability risk shared with the Postgres backend, not as data loss.
- A key that never committed and is deleted early widens the accepted leak window.
- A deleting leader whose clock runs behind holds storage longer.
- A `FileCleaner` that pauses after `getFilesToDelete` while leadership moves deletes from a stale list. Every key on that list is in committed queue state and can't be referenced again, so a second delete is harmless.

A monotonic grace period that restarts at each election was rejected. It delays every delete by one grace period after a failover, and it changes the `getFilesToDelete(markedBefore, …)` contract that the Postgres backend shares.

### Tests

Every test runs at `N ≥ 2`, so a file has a participant besides its tracking partition, and gives the uploader and each coordinator leader its own `MockTime`:

1. Skewed clocks: offset every uploader and leader by up to a day, then run commits, idle lease ends, epoch retirement, a failover, and `FileCleaner` cycles with `retention.ms=-1`. Assert that every batch row's object exists in `InMemoryStorage` and that no queued key matches a batch row. This lives in the coordinator backend's integration tests, because the shared suite runs on one clock.
2. Idle timer against a part: a part reschedules `lease-idle-<brokerId>`, and when the timer runs first, the participant's committed part stays readable and its key isn't queued. Advancing the leader's `MockTime` past the timeout between two parts covers a forward clock jump. This lives in unit tests on the tracking-partition shard with `MockCoordinatorTimer`. `MockCoordinatorTimer` runs an expired operation inside `poll()`, so it can't hold a fired timer. Kafka's `testTimerCancelledAfterExpiryButBeforeWriteOperation` and `testTimerOverridden` already cover the override rule.
3. Leader pause: queue the lease-ended write at the old leader, move leadership, and commit the part at the new leader. Assert that no lease-ended record exists. `testTimerIgnoredOnNotCoordinatorException` covers the runtime's side.
4. Uploader pause past the timeout: hold an upload until the lease ends, the range fence runs, and the grace period deletes the key, then release it and commit. Assert stale-sequence, no direct delete by the broker, and no batch row for the key. This extends the partial-send tests from [Broker-side deletion after a partial send](02-delete-after-partial-send.md).
5. Stale `FileCleaner` list: call `getFilesToDelete` on the old leader, move leadership, let the new leader delete and record its deletes, then let the old leader delete its list. Assert that every key the old leader deletes was in the committed queue and has no batch row.

### Metrics

No metric measures clock skew. Adding one would bring back a broker timestamp on every request for a gauge, and skew matters only for retention and `LogAppendTime`, which classic Kafka leaves to NTP. The retention accepted-risk row gets an operator procedure instead: monitor the NTP offset on every broker, and alert well below half the grace period.

These metrics show stuck requests and misfiring timers:

- Existing: `event-queue-time-ms` and `event-purgatory-time-ms` from `CoordinatorRuntimeMetricsImpl`, registered for this runtime, bound the duplicate window. The broker's `FileCommitTime` and `FileCommitErrorRate` count files that hit the commit deadline. Kafka's leader-election metrics show the churn that keeps idle leases open.
- Candidates from the earlier effort: idle lease ends per tracking partition, where a burst across many brokers points to a clock jump. Stale-sequence rejections, where a spike right after an idle lease end at an active broker means the lease ended without cause. The deletion queue's size and the age of its oldest row: a growing queue with a young oldest row points to a clock running behind, and an old oldest row points to stuck deletes.

Whether the Inkless broker already registers `CoordinatorRuntimeMetricsImpl` for a runtime, or the coordinator backend's runtime needs its own metrics group, is left to the rewrite.

### Consequences for other tickets

- The consequences pass in the map's Not yet specified list turns these candidates into final metric names and failure-mode rows. It also adds the retention and `LogAppendTime` accepted-risk row with the NTP procedure, and the reader-availability row for grace-period skew.
- The v1 sections that leave the design for time reasons are [Tombstones and fences](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#tombstones-and-fences), the age check, the age-check rejection metric, and the failure-mode row for broker clocks beyond the maximum commit age.
