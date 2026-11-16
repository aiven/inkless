# Image edge cases: re-registered broker IDs and unreconciled partitions

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: none

## Question

How does a coordinator leader tell a stale broker or topic from a new one when its state and its metadata image disagree?

Two of Fable's findings share this shape:

- Re-registered broker ID: [Acceptance rules](../../diskless-metadata-coordinator-v2/design.md#acceptance-rules) rejects every commit for a retired row as stale epoch. [Ending a broker epoch](../../diskless-metadata-coordinator-v2/design.md#ending-a-broker-epoch) says a broker that returns under the same ID commits as a new epoch. The retired marker carries no epoch, so the leader can't tell the two apart, and the ID is rejected forever. See [Fable's fourth finding](../reviews/fable.md#the-retired-marker-rejects-a-re-registered-broker-id-forever).
- Unreconciled partitions: [Lifecycle](../../diskless-metadata-coordinator-v2/design.md#lifecycle) appends a topic-deleted record when SQLite holds a topic ID that the image doesn't. That purges batches and then deletes objects, so it's irreversible. It's safe only if no row can exist ahead of the leader's image. `initDisklessLog` and `repairDisklessLog` come from a partition leader whose image can be newer, and v2 doesn't say they need a reconciled row. See [Fable's fifth finding](../reviews/fable.md#topic-deletion-is-inferred-from-absence-in-the-image).

Settle these points:

- Whether the broker-epoch-retired record stores a retired-through epoch, with commits rejected only at or below it, and what happens to the lease table at a tracking partition.
- Whether every call for an unreconciled user partition is rejected, and with which error.
- Whether the partition-created record stores the metadata offset it came from, and whether a leader appends topic-deleted only when its image offset is at or past that offset.
- Any other path that can write a row ahead of the leader's image.

Named test: a broker that is unregistered, re-registers with the same ID, and commits. The ticket names the unreconciled-partition test.

## Resolution

Resolved on 2026-11-12 in a grilling session.

Both findings come from a coordinator row that exists ahead of the leader's metadata image. The fix compares the image's last contained offset with an offset that the row carries. A KRaft broker epoch is the metadata offset of its registration: `QuorumController.registerBroker` passes `offsetControl.nextWriteOffset()` as the new epoch. An image at offset `X` has therefore seen every registration at an epoch at or below `X`.

v2 has a third case of the same shape. A leader whose image lags a new broker's registration sees no registration and appends the unregistered form of broker-epoch-retired, which retires a live broker forever.

### Broker epoch retirement

- The broker-epoch-retired record holds the broker ID and one value, `retiredThrough`. The retired marker and the unregistered flag leave the design.
- If the image shows a newer epoch `E`, the leader sets `retiredThrough` to `E - 1`. If the image shows no registration, it sets `retiredThrough` to the image's last contained offset. Any epoch at or below that offset that the image doesn't show belongs to a registration that is gone, including one this partition never saw.
- The leader appends the record only when `retiredThrough` is at or above the broker's newest epoch in its row, its lease table, and its overlay. An image that lags a new registration appends nothing.
- Apply raises the row's floor to `(retiredThrough + 1, none)`. A commit at `e <= retiredThrough` gets a stale-epoch rejection. A broker that returns under the same ID has a higher epoch and commits normally.
- At a tracking partition, the same apply ends every lease at an epoch at or below `retiredThrough`.
- The acceptance rule "the row is retired, or `e < e_r`" becomes "`e` is at or below the row's retired-through epoch, or `e < e_r`." The apply no-op "a broker-epoch-retired record at or below the row's epoch" keys on `retiredThrough`.

Rejected alternative: Fable's version, which keeps the unregistered form and stores the row's last epoch. It still retires a new broker that the image hasn't seen. It also misses an epoch that registered and was unregistered without this partition seeing it.

### Topic deletion

- The partition-created record gets a `metadataOffset` field: the image's last contained offset when the leader appends the record. It's at or above the topic's `TopicRecord` offset.
- The leader appends topic-deleted for a topic ID only when its image's last contained offset is at or past the highest `metadataOffset` among that topic's partitions in SQLite and the overlay. Below that, it skips the topic until the next metadata update. Topic IDs are never reused, so absence from an image past the creation offset proves the deletion. v2's diff stays, which covers a broker that loaded a snapshot and never saw the delete.
- The case that matters most is a leader change: the old leader appends partition-created at image offset `M`, and the new leader's image is at `M' < M`.

Rejected alternatives:

- Waiting until the image reaches the newest `metadataOffset` in all of SQLite before reconciling. It delays creations for every other topic.
- Deleting only on an explicit delete delta. v2 already rejected it, because of snapshot loads.

### Unreconciled partitions

- A user partition is unreconciled at a leader when a call names it and neither SQLite nor the overlay holds its partition-created record. The leader can't see the caller's image, so the definition uses only the leader's state.
- Every call that names an unreconciled partition gets `COORDINATOR_LOAD_IN_PROGRESS` on the private RPC and appends nothing. This covers commit parts, `initDisklessLog`, `repairDisklessLog`, `deleteRecords`, `pruneDisklessLogs`, `advanceCrossTierLogStartOffset`, and reads.
- Each rejection increments `UnreconciledPartitionRejectCount` on the leader. A rate that stays high signals a leader whose image is stuck.
- Callers map the error as follows:
  - The delegate retries a commit part until the commit deadline. After the deadline, the part gets `KAFKA_STORAGE_ERROR`, as any unconfirmed part does.
  - The `initDisklessLog` queue retries it with its existing backoff. `InitDisklessLogState` already retries every error except `NONE` and `INVALID_REQUEST`.
  - `repairDisklessLog` returns it to the admin client as retriable. `UNKNOWN_TOPIC_OR_PARTITION` keeps meaning "no log entry to repair."
  - `findBatches` and `listOffsets` answer `OFFSET_NOT_AVAILABLE`, as the barrier does when it times out.
- A partition whose topic is marked deleted still gets `UNKNOWN_TOPIC_OR_PARTITION`, so a caller with a stale image stops retrying.

Rejected alternatives:

- A new error code. It adds an entry to upstream `Errors.java`, where a fork-only number collides with upstream's next code at every sync. No caller would act on it differently, and the counter gives operators the distinction.
- `UNKNOWN_TOPIC_OR_PARTITION`. It would blur "not created yet," "deleted," and repair's "not found."
- `NOT_COORDINATOR`. It sends the delegate to look up a new leader for no reason.

### Paths that write a row ahead of the image

This inventory is complete:

| Row | Written by | Guard |
| --- | --- | --- |
| User partition, with its batches and producer state | partition-created, from the leader's own image | The `metadataOffset` gate on topic-deleted |
| Batches, producer state, and log starts | Commit parts, `initDisklessLog`, `repairDisklessLog`, `deleteRecords`, retention, prune, and cross-tier | Rejected without a partition-created record |
| Broker row at an epoch the image hasn't seen | A commit from a new registration | No retirement below the newest held epoch |
| Lease table entry at an epoch the image hasn't seen | A lease request | The lease table counts toward the newest held epoch |
| Broker row moved to `E` by an old leader | broker-epoch-retired | A new leader with a lagging image computes `retiredThrough < E` and appends nothing |
| Switched partition before the seal reaches the leader's image | `initDisklessLog` | partition-created follows only an image that shows the partition as diskless |

New partitions of an existing topic are safe, because the topic ID is still in the image. Kafka never deletes a single partition.

v2's [Apply rules](../../diskless-metadata-coordinator-v2/design.md#apply-rules) gain one unreachable case: a record that changes a user partition with no partition-created row. This covers file-committed, diskless-log-initialized, diskless-log-repaired, records-deleted, retention-applied, log-pruned, and cross-tier-start-advanced. A replica that meets one stops applying the coordinator partition and reports corruption. A leader bug that skipped the rejection then stops the partition instead of creating a row that a later topic-deleted could purge.

### Failure modes

| Failure | Behavior |
| --- | --- |
| A broker ID re-registers after an unregistration | Commits at the new epoch succeed. Epochs at or below `retiredThrough` get a stale-epoch rejection. |
| A leader's image lags a new registration | No broker-epoch-retired record. Commits proceed. |
| A new leader's image lags a partition-created record from the old leader | No topic-deleted until the image reaches the record's `metadataOffset`. |
| A call names an unreconciled partition | `COORDINATOR_LOAD_IN_PROGRESS`. The caller retries, and `UnreconciledPartitionRejectCount` rises. |
| A leader bug writes a row for an unreconciled partition | Apply stops at that record, and the replica reports corruption. |

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. Named test: a broker commits at `E1`, is unregistered, and re-registers under the same ID at `E2`. The leader appends broker-epoch-retired with `retiredThrough` at its image offset. A late part at `E1` gets a stale-epoch rejection, a part at `E2` commits, and the tracking partition has ended the `E1` leases.
2. A new broker ahead of the image: a broker registers at `E` and commits to a leader whose image is below `E`. The leader appends no broker-epoch-retired record, and the commit succeeds before and after the image catches up.
3. A middle epoch the partition never saw: a broker registers at `E1`, re-registers at `E2`, and is unregistered. The partition saw only `E1`. A late part at `E2` gets a stale-epoch rejection.
4. Named test, unreconciled partition across a leader change: the old leader, with its image at `M`, appends partition-created for a new topic. The new leader's image is at `M' < M` and lacks the topic. It appends no topic-deleted. After the topic is deleted and the image passes `M`, it appends topic-deleted.
5. A call before reconciliation: `initDisklessLog` and a commit part arrive before partition-created applies. Both get `COORDINATOR_LOAD_IN_PROGRESS`, append nothing, and increment `UnreconciledPartitionRejectCount`. Init succeeds on retry after reconciliation, and the produce succeeds within its deadline.
6. The apply rule: a file-committed record for a user partition with no partition-created row stops apply and reports corruption.
