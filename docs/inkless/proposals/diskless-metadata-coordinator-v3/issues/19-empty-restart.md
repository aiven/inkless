# Empty restart of a coordinator partition

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 11

## Question

After every replica of a coordinator partition `P` loses its storage, how does an operator bring `P` back with no state, and which rules keep that restart from harming other partitions' data?

[Enforced replication factor and rack spread](11-replication-guard.md) accepts the loss under clause 2's exception, with an empty restart as its procedure. Without it, `P` stays leaderless for good, and about 1/N of every diskless topic, present and future, stays unavailable. The trigger's precondition comes from [Object footer and recovery after losing a replica set](09-object-footer.md): the controller accepts it only for `__diskless_metadata`, and only when no ISR member, ELR member, or last known leader has intact storage.

Settle these points:

- The mechanism: a new admin request for one coordinator partition, or Kafka's unclean election allowed for `__diskless_metadata` under the precondition. Say how each one interacts with v2's storage-change guard, the ELR rules, and [Topic protection](../../diskless-metadata-coordinator-v2/design.md#topic-protection), which rejects an unclean election today.
- The user partitions that map to `P`: how they restart, through partition-created records from the leader's image and the `metadataOffset` rule in [Image edge cases](03-image-edge-cases.md). What consumers with committed offsets and idempotent producers see.
- Consolidation and cross-tier offsets: a user partition whose older data sits in a classic log through consolidation has offsets that an empty restart from 0 would collide with. Say where the restarted log starts. See v2's [Consolidation and cross-tier offsets](../../diskless-metadata-coordinator-v2/design.md#consolidation-and-cross-tier-offsets).
- `P` as a participant: fences that other tracking partitions resend to the empty `P` find no row. Confirm that the answer raises `P`'s floor so a late part from before the loss gets stale-sequence, and that no other partition's object is deleted while one of its batches still lives elsewhere.
- `P` as a tracking partition: brokers whose leases sit at `P` send tracking parts and finish reports that the empty `P` doesn't know. v2 answers an unleased sequence with `INVALID_REQUEST` as a broker bug. Say what the broker does instead, and how leases at `P` start again without reusing a key.
- The objects the old `P` tracked: they leak for good. Say whether a signal counts them, or whether the accepted-risk row only states it.
- The failure-mode rows and tests, and whether [Model of the fence, floor, and release protocol](17-protocol-model.md) covers an empty participant and an empty tracking partition.

## Resolution

Resolved on 2026-11-13 in a grilling session, and deferred to future work the same day, because whether the feature is needed is still debated.

### Status: future development idea

The session worked the whole design tree, and this resolution records it as the design for a later version. v3 includes none of it:

- v3 has no restart record, no `coordinatorRestartEpoch` tag, no participant-restarted record, no broker floor fence, no span release, no lazy adoption, and no restart initialization. `UNCLEAN` elections on `__diskless_metadata` stay rejected, as in v2's [Topic protection](../../diskless-metadata-coordinator-v2/design.md#topic-protection).
- No v3 component, record, request, or test depends on this design. The rewrite adds a Future work entry that summarizes it and links here.
- The amendments to other tickets in [Consequences if a later version builds it](#consequences-if-a-later-version-builds-it) apply only then. v3's fence, release, lease, and offset rules stay as their own tickets decided them.
- Without the restart, `P` stays leaderless after its replica set is lost, and about 1/N of every diskless topic stays unavailable. The map's Notes relax clause 2 to accept this in v3, and the accepted-risk row in [Enforced replication factor and rack spread](11-replication-guard.md) has no procedure. Its signal stays `OfflinePartitionsCount`, and clause 3 still holds there, because the objects `P` touched leak and nothing deletes them.

The rest of this resolution is the deferred design.

### Design summary

After every ISR and ELR candidate of `P` loses its storage, the operator runs an unclean election for `P`. The controller stores the election's leader epoch as `P`'s restart epoch. Every replica whose log predates that epoch truncates fully, and the leader appends a restart record that drops all of `P`'s state on every replica. The other tracking partitions turn `P` into an exception and fence it again. Each broker raises `P`'s floor for its epoch before `P` accepts its parts, abandons its ranges at `P`, and releases them over their full span. `P` adopts the objects it can find again, and each user partition restarts above whatever the classic prefix and the remote tier still hold.

### Trigger

- The operator sends `ElectLeaders` with type `UNCLEAN` for `P`, with `kafka-leader-election.sh --election-type unclean`. No new API or CLI is needed.
- `ReplicationControlManager.electLeader` accepts it for `__diskless_metadata` only when the partition's ISR, ELR, and `lastKnownElr` are all empty. Otherwise it answers `INVALID_REQUEST`, as v2's Topic protection does for every such request. Upstream's unclean election elects a live replica even when an offline ELR member keeps intact storage, so the precondition is stricter than upstream.
- The storage-change guard empties the ELR and `lastKnownElr` as replicas re-register with new storage.
- Unregistering a broker also removes it from `lastKnownElr` of every `__diskless_metadata` partition, in the same `O(N × RF)` walk as the storage-change guard. Upstream `handleBrokerUnregistered` removes a broker from the ISR and the ELR only, so a last known leader that never returns would block the restart forever.
- `unclean.leader.election.enable` stays fixed at `false`. Only the operator knows that an absent replica's disk isn't coming back.

Rejected alternatives:

- A new admin request. A fork-only API number collides with upstream's next number at every sync, for the reason [Image edge cases](03-image-edge-cases.md) rejected a fork-only error code.
- Ignoring unregistered members in the precondition. The controller would act on a state that differs from what it shows.

### Always empty

- `P` comes back empty even when a stale replica survives. A replica that lagged out of the ISR, and never joined the ELR, keeps an intact log that ends below the old high watermark. Its rows can point into objects deleted since, which makes fetches at those offsets fail with the retriable `KAFKA_STORAGE_ERROR` on every retry. Its floors, leases, and tracking entries are also from an old point.
- The restart record's apply drops every row of `P` on every replica, whatever the elected replica's log held.
- Clause 2's exception widens to: every ISR and ELR candidate of a coordinator partition loses its storage. A classic topic would keep the stale prefix, so this case needs the exception.

Rejected alternative: keeping the stale replica's state, as a classic unclean election does. It brings back reads that stay stuck on deleted objects, and the protocol model would need a stale `P` as a third state.

### Restart epoch

Kafka doesn't reliably bring followers into line with an unclean leader. `AbstractFetcherThread.fetchOffsetAndTruncate` states, "There is a potential for a mismatch between the logs of the two replicas here." A stale follower that asks for an epoch older than any the new leader has gets `UNDEFINED_EPOCH_OFFSET` and keeps its log up to its own high watermark. `RECOVERING` can't hold the restart open either: the controller rejects an ISR larger than 1 while the partition is `RECOVERING`, and `maybeIncrementLeaderHW` doesn't advance under the minimum ISR, so an `acks=-1` restart record would never commit.

- At the unclean election, the controller stores the new leader epoch `E_r` as `coordinatorRestartEpoch` on the `PartitionChangeRecord`, as an Inkless tagged field at 100 or above, as `classicToDisklessStartOffset` and the diskless leader epoch already are. `merge` keeps it, and nothing clears it.
- When a replica becomes leader or follower and its log holds no record at an epoch at or above `E_r`, it truncates the log fully, starts it at 0, and deletes its SQLite database and snapshot files. This covers a stale replica that wins the election and one that returns days later.
- A leader whose log holds no record at or above `E_r` appends the restart record before it serves anything. If it fails before the record commits, the next leader does the same.
- `RECOVERING` clears as usual, and the record commits once the ISR reaches the minimum.

Rejected alternatives: holding `RECOVERING` until the restart record commits, which can never commit. Starting the leader's log at a high offset so that stale followers truncate fully, because no value is safe against a log end the leader doesn't know.

### `P` as a participant

- When `T`'s leader sees a new `coordinatorRestartEpoch` for `P` in its image, it appends a participant-restarted record with `P` and `E_r`. It checks on each image change and after each election. Apply never reads the image.
- The apply moves `P` to fencing in every open entry that isn't released, and records `P` as an exception for every sequence in every range `T` holds, up to each broker epoch's highest range end.
- The existing rule fences `P` once `finishedThrough` covers each range. The empty `P` answers "released" and raises its floor. A part that commits at the new `P` is answered "referenced" and released later.
- This is the one exception to the forward-only entry states in [Release reports: ordering, batching, and a leak signal](05-release-reports.md): a held entry for the restarted participant moves back to fencing. Ticket 05's skip of a fence to a participant whose entries are all held or released doesn't apply to those entries.
- Older broker epochs need nothing new. The empty `P` has no rows, so its first image check appends broker-epoch-retired with `retiredThrough = E_b - 1` for every registered broker.

### Floor fence and gate

A paused broker can send a part for a finished file whose key a tracking partition deleted before the loss. The empty `P` has no floor to refuse it, and when `P` itself was the tracking partition, no surviving `T` knows that epoch.

- When a broker's image shows the restart, it sends `P` the existing fence-sequences request for its current epoch, up to `max(finishedThrough, b)`. `finishedThrough` is broker-wide, and `b` is the highest range end the broker requested at `P`. The request, the sequences-fenced record, and its apply are unchanged. Only the caller is new.
- Until `P` has recorded a floor at epoch `e` for broker `b`, it answers that broker's commit parts at `e` with `COORDINATOR_LOAD_IN_PROGRESS`.
- A deleted key belongs to a finished file, so its sequence is at or below the broker's current `finishedThrough`, and the late part gets stale-sequence.
- A broker whose image lags the restart sees its parts to `P` fail until the image catches up.

Rejected alternatives: accepting the window, which leaves the case where `P` was the tracking partition open. Gating on `T`'s restart fences, because `P` doesn't know which tracking partitions hold a broker epoch.

### `P` as a tracking partition

- When the broker's image shows the restart, it abandons every range it holds at `P` and renews, at `P` first and then `P + 1` onward. The new range starts above every range it requested in the epoch, so no key is reused. `P` rejects a lease whose start is at or below the broker's floor at `P`.
- The floor fence reaches `b`, so tracking parts under the old ranges get stale-sequence instead of `INVALID_REQUEST`, and they become exceptions. Parts of in-flight files with batches for `P`'s user partitions fail at `P`, and their producers retry.
- Once every file at or below `b` is finished, the broker sends the lease release from [Lease lifecycle at the broker](02-lease-lifecycle.md). For a restarted `P`, it also names `a`, the lowest range start the broker held at `P` in the epoch. `P` fences `[a, b]` at every partition. "Referenced" answers rebuild the tracking entries, and keys that no partition holds are queued.

Rejected alternative: a plain release from `finishedThrough + 1` to `b`. Every finished file that the old `P` tracked in a live epoch would leak.

### Lazy adoption

- At a restarted `P`, a release report for a sequence outside every recorded range is stored as a released entry.
- If the epoch is at or below the broker's `retiredThrough`, no part of it is in flight, so `P` fences that sequence at all `N` and adopts the object from the answers.
- If the epoch is live, the entry waits for the span release, which fences `[a, b]` and merges with it.
- Once the span release is recorded, a report outside every range gets ticket 05's non-retriable rejection again and counts in `ReleaseReportRejectedRate`.

### User partitions

- When a user partition's leader sees the restart in its image, it sends `P` a restart initialization. This extends `initDisklessLog` with the restart epoch and goes through the `InitDisklessLogManager` queue. `P` accepts one per restart epoch. It carries two values:
  - The diskless start: `max(0, seal, highestOffsetInRemoteStorage + 1)`.
  - The log start: the whole-log start, which is the cross-tier earliest offset where one exists.
- Until its restart initialization applies, each of `P`'s user partitions answers `COORDINATOR_LOAD_IN_PROGRESS` under the unreconciled rule in [Image edge cases](03-image-edge-cases.md). Every partition takes this path, so `P`'s apply doesn't read topic configs. A born-diskless partition that doesn't consolidate sends 0.
- A managed replica whose local consolidated log runs past the diskless start truncates down to it.
- The classic prefix and the remote tier stay readable at their offsets. Offsets from the diskless start up to the old high watermark are reused, as after an empty unclean election on a classic topic. A consumer whose committed offset is past the new end follows its reset policy.
- The partitions restart with no producer state. Diskless topics can't take part in transactions. An active idempotent producer gets one `OUT_OF_ORDER_SEQUENCE_NUMBER`. The Java client's `TransactionManager.canRetry` then bumps the epoch and retries without an application error, as on a classic partition with an empty log under KAFKA-15591.

Rejected alternatives: starting every partition at 0, which collides with the classic prefix and the remote tier. Starting at the leader's local log end, because managed replicas consolidate independently and their local tails aren't in the new WAL. Seeding producer state from the seal record or a remote producer snapshot, which are older than the lost writes.

### Failure-mode rows

| Failure | Behavior | Test |
| --- | --- | --- |
| Every ISR and ELR candidate of `P` loses its storage | The operator runs the unclean election. `P` comes back empty, and other coordinator partitions see no change. | 1 |
| `UNCLEAN` election while the ISR, ELR, or `lastKnownElr` isn't empty | `INVALID_REQUEST`. | 2 |
| A lost replica never returns | Unregistering it clears `lastKnownElr`, and the election is then accepted. | 2 |
| A stale replica wins the election, or returns days later | It has no record at or above `E_r`, so it truncates fully, with its SQLite and snapshots. | 3 |
| The restart leader fails before the restart record commits | The next leader finds no record at or above `E_r` and appends it. | 4 |
| A paused broker sends a part for a finished file | It's gated until the broker's floor fence applies, then gets stale-sequence. | 5 |
| `T` holds entries that wait on the old `P` | Participant-restarted turns them into exceptions. The fences answer "released," and the objects are queued. | 6 |
| Brokers with ranges at `P` | Tracking parts under the old ranges get stale-sequence. The broker renews, and the span release adopts the live epochs' objects. | 7 |
| A release report for a retired epoch | `P` stores it, fences the sequence at all `N`, and adopts the object. | 8 |
| A switched or consolidating partition restarts | The diskless start is `max(0, seal, highestOffsetInRemoteStorage + 1)`. The classic prefix and the remote tier stay readable, and a local log above the start truncates. | 9 |
| An idempotent producer writes after the restart | One `OUT_OF_ORDER_SEQUENCE_NUMBER`, then an epoch bump, with no application error. | 1 |
| A broker's image lags the restart | Its parts to `P` are gated until it catches up. Liveness only. | 5 |

### Accepted risk

This row would replace the one in [Enforced replication factor and rack spread](11-replication-guard.md):

| Risk | Why it's accepted | Signal or procedure |
| --- | --- | --- |
| Every ISR and ELR candidate of a coordinator partition loses its storage | Replication factor 3 and maximal spread are enforced, so clause 2's widened exception applies. `P`'s diskless data at or above `max(0, seal, highestOffsetInRemoteStorage + 1)` is lost, and offsets above that start are reused. Three classes of objects leak: objects whose only remaining batches were in `P`, objects whose other participants had all released to the old `P`, and keys in the old `P`'s deletion queue that weren't deleted. Clause 3 holds: the empty `P` answers fences truthfully, and a tracking partition deletes an object only when every participant answers "released." | Signal: `OfflinePartitionsCount` while `P` is down, `UncleanLeaderElectionsPerSec` on `__diskless_metadata` for the restart, and `UnreconciledPartitionRejectCount` for a restart that doesn't finish. Procedure: unregister each lost replica that won't return, then run `kafka-leader-election.sh --election-type unclean` for `P`. |

No metric counts the leaked objects. No component knows their keys, and the storage backends have no list operation. The report-only scanner stays future work, as in v2. Losing the KRaft metadata stays out of scope, and in combined mode the loss can take the whole cluster, as ticket 11 states.

### Tests

1. Named: end-to-end empty restart at `N=8`. Every replica of one coordinator partition is wiped and re-registers with new storage. The operator runs the election, and `P` serves again. Other partitions see no change, and an idempotent Java producer continues after one epoch bump.
2. `ReplicationControlManagerTest`: the election is rejected while the ISR, ELR, or `lastKnownElr` isn't empty, and accepted otherwise. Unregistering a broker clears it from `lastKnownElr`. The election writes `coordinatorRestartEpoch`, and `merge` keeps it.
3. A stale replica across a restart, both as the elected leader and as a replica that returns later. It truncates fully, and every replica applies the same state.
4. Fault injection: the restart leader fails before the restart record commits, and the next leader appends it once.
5. A late part across a restart. A part from a paused broker is gated, then stale after the floor fence. This includes the case where `P` was the tracking partition.
6. Objects tracked elsewhere: an object that waits on the old `P`'s release is queued after the participant-restarted fences.
7. A tracking partition restart: tracking parts under old ranges get stale-sequence, the broker renews above `b`, and the span release adopts the live epochs' objects.
8. Lazy adoption: a release report for a retired epoch triggers fences at all `N`, and the object is queued once every participant releases.
9. Switched and consolidated partitions: the classic prefix and the remote tier stay readable at their offsets after the restart, and a managed replica truncates a local log above the diskless start.

### Consequences if a later version builds it

- The map's clause 2 exception widens to every ISR and ELR candidate losing its storage.
- [Enforced replication factor and rack spread](11-replication-guard.md) takes the accepted-risk row in [Accepted risk](#accepted-risk).
- [Release reports: ordering, batching, and a leak signal](05-release-reports.md) gains the exception for a restarted participant's held entries, and its rejection of reports outside every range waits for the span release at a restarted `P`.
- [Lease lifecycle at the broker](02-lease-lifecycle.md) gains the range start `a` on a release to a restarted `P`.
- v2's Topic protection accepts `UNCLEAN` elections on `__diskless_metadata` under the precondition.
- [Model of the fence, floor, and release protocol](17-protocol-model.md) gains a restart action that drives participant-restarted, the floor fences and their gate, abandon and renew, the span release, and lazy adoption. It checks that no other partition's object is deleted while a batch references it, that no part commits into a queued or deleted key once its broker's floor fence applied, and the one exception to forward-only entry states.
- The design adds a section for the restart, the restart record and participant-restarted record to Log records, the controller rules to Topic protection and the election guards, and the terms restart epoch, restart record, and restart initialization to the Terms section. `ReplicationControlManager.java`, `ClusterControlManager.java`, `PartitionRegistration.java`, and `AbstractFetcherThread.scala` carry Inkless edits.
