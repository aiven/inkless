# Object footer and recovery after losing a replica set

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 07

## Question

What does the object footer contain, and is it enough to recover a coordinator partition after every replica of it is lost?

Clause 2 of the map's data-loss bar forbids accepting v2's row "Every replica of a coordinator partition is lost" as written, because it costs 1/N of all diskless partitions where a classic topic loses one. See v2's [Accepted risks](../../diskless-metadata-coordinator-v2/design.md#accepted-risks) and the council's [third status row](../reviews/council.md#status-of-the-earlier-findings). The map names the footer as the leading recovery path, and [WAL object format and footer feasibility](07-wal-object-format.md) holds the facts.

Settle these points:

- The footer's fields and encoding, and its version rule, consistent with [Record compatibility across broker versions](../../diskless-metadata-coordinator-v2/issues/08-record-compatibility.md).
- What a recovery rebuilds from footers, and what it can't: offsets the leader assigned, producer state, deleted and released batches, and leases. Whether recovery restores the user partitions' data, or only stops the objects from becoming orphans.
- Whether recovery needs a storage listing, and how that squares with v2's rejection of a deleting scanner. The storage backends have no list operation, and Azure can't read a blob's last bytes without first looking up its size. See [wal-object-format.md](../research/wal-object-format.md).
- If footers can't meet clause 2, the fallback the map recorded: relax clause 2 for this one case with an enforced replication factor and rack spread, decided with [Enforced replication factor and rack spread](11-replication-guard.md).
- The operator procedure, and whether a tool is needed. A restore tool is out of scope, so a needed tool reopens the destination.

## Resolution

Resolved on 2026-11-13 in a grilling session, and deferred to future work the same day.

### Status: future development idea

The session worked the whole design tree, and this resolution records it as the design for a later version. v3 doesn't include it:

- v3 doesn't write a footer and has no recovery path. The rewrite adds a Future work entry that summarizes this design and links here.
- Clause 2 of the map's data-loss bar takes the fallback: losing every replica of a coordinator partition is an accepted risk under replication factor 3 and rack spread enforced by a guard, which [Enforced replication factor and rack spread](11-replication-guard.md) decides.
- The failure-mode rows, accepted-risk row, metrics, tests, and user story in the following sections apply only when a later version builds the recovery. They don't go into v3's cross-cutting sections.
- `ObjectFormat` keeps its unused values, so a later version can add the footer with a new format value. Objects written before then have no footer and can't be recovered.

The rest of this resolution is the deferred design.

### Design summary

Every WAL object carries a footer that names its partition runs and an offset hint for each run. After every replica of a coordinator partition `P` loses its storage, the operator triggers a recovery. A new leader of `P` refuses all work, lists the cluster's keys in storage, reads every footer, and filters the runs by what the surviving tracking partitions know. It then serves `P`'s records again at new offsets above a floor, with producer state rebuilt from the batch headers. Clause 2 holds: the damage stays within `P`'s user partitions, and no acknowledged record is lost.

### Scope

- Recovery assumes the KRaft metadata survives. A footer names its runs by topic ID, and only the metadata log maps a topic ID to a topic. Broker epochs and `__diskless_metadata` itself also come from KRaft. When KRaft is lost, classic topics don't survive either, so clause 2 holds without coordinator work.
- The design recommends dedicated controllers, citing Kafka's guidance that combined mode "should be avoided in critical deployment environments" (`docs/operations/kraft.md:320` in the Kafka checkout). In combined mode, losing a replica set can mean losing the whole cluster, which this recovery doesn't cover.
- With KRaft intact, the batches of a deleted or recreated topic carry a topic ID that no longer exists, and the rebuild skips them.
- The upstream design doesn't assume consolidation. Consolidation is an Inkless extension, in [Inkless extension](#inkless-extension).

### What recovery gives back

- The records of `P`'s user partitions, at new offsets.
- Producer state, rebuilt from the batch headers: for each partition and producer, the highest epoch and the last 5 entries in sequence order, with their new offsets. A retry copy carries the same producer ID, epoch, and sequences as its original, so the rebuild keeps one. The Inkless commit requires base sequence 0 for a producer it has no state for, unlike Kafka's `ProducerAppendInfo.checkSequence`, so starting producer state over would fail every active idempotent producer with `OUT_OF_ORDER_SEQUENCE_NUMBER`.
- Retention applies again to the rebuilt partitions.
- Recovery doesn't give back the original offsets, the original commit outcomes, or removals that left no trace.

### Footer

Every object that this backend writes ends with a footer:

| Part | Fields | Size |
| --- | --- | --- |
| Body, once per object | Broker ID, broker epoch, sequence, tracking partition, run count | About 28 bytes |
| Body, once per partition run | Topic ID, partition, byte start, byte length, batch count, offset hint | 40 bytes |
| Trailer, last bytes of the object | Footer length, footer version, CRC32C over the body, magic number | 14 bytes |

- The offset hint is the highest last offset that the broker saw assigned for that partition in commit replies before the file closed, or −1 if none. The broker keeps a map from user partition to that offset, updated from commit replies, and drops an entry when it stops writing the partition.
- The footer leaves out what the bytes or the key already hold: producer fields and timestamps are in the batch headers, the cluster ID is in the key, and the participant set follows from the runs through the protocol mapping. Each batch header stores its length, so the rebuild walks the batches within a run.
- Size: about 4 KB for 100 partitions in an 8 MiB object, 0.05%. It grows with the partitions in a file, not with the batches.
- `BatchBuffer.close` appends the footer to the array it builds. The upload call, the readers, and the request count stay the same, as [WAL object format and footer feasibility](07-wal-object-format.md) found.

Encoding and versions follow [Record compatibility across broker versions](../../diskless-metadata-coordinator-v2/issues/08-record-compatibility.md):

- The body is a Kafka generated message from a JSON schema, for example `DisklessObjectFooter`. The trailer's version is the schema version. Upstream defines no tagged fields. Inkless extension tags go at 100 and up, checked against the same registry as records.
- At file close, the broker writes the highest footer version that the finalized coordinator feature level allows. The footer is part of level 1, so every object that this backend writes has one. Postgres clusters never write one.
- `ObjectFormat` gets a second value for a WAL object with a footer, stored in the file-committed record. Readers keep ignoring it.
- A rebuild that meets an unknown footer version or tag stops and reports the key, version, and tag. It never skips what it can't read. The write gate and the downgrade refusal make this reachable only through a bug or a manual binary downgrade.
- A missing magic number or a bad CRC can only mean a bug or storage corruption. The rebuild logs the key, counts it, and continues, because that object can never be read and stopping would block recovery for good.

### Bringing `P` back

Once every replica has lost its storage, the controller drops each one from the ELR at registration, and v2's guards rule out an unclean election, so `P` stays leaderless.

1. The operator sends a new admin request for one coordinator partition, through a CLI. The controller accepts it only for `__diskless_metadata`, and only when no ISR member, ELR member, or last known leader has intact storage. Otherwise it answers with an error. Recovery never starts by itself, because only the operator knows the disks aren't coming back.
2. The controller elects an empty replica and writes a recovery marker with the new leader epoch into the partition's KRaft record. The marker tells a recovering partition from a new one, which also starts with an empty log.
3. The new leader sees the marker and runs the rebuild. It appends the results as recovery records to `P`'s log, with `acks=-1`. Followers replicate and apply them as any other records, and apply still reads nothing outside the log.
4. A recovery-completed record lifts the gate when it applies, and the controller then clears the marker.

The restore logic lives in the broker. The operator only triggers it and may supply a floor. This is an operator procedure, not the restore tool that the map rules out of scope. The recovery records belong to coordinator feature level 1.

### Gate during the rebuild

The recovered `P` answers commits, fences, lease requests, release reports, and reads with `COORDINATOR_LOAD_IN_PROGRESS` until the recovery-completed record applies, as an unreconciled partition does in [Image edge cases: re-registered broker IDs and unreconciled partitions](03-image-edge-cases.md). Its share of `getFilesToDelete` stays empty. An empty `P` would otherwise answer "not held" to the fences that tracking partitions resend, and to its own exception fences, which would queue and delete the objects that the rebuild is about to index again.

The existing mechanisms carry everyone else through the wait. Brokers whose tracking partition is `P` fall back to `(t + 1) % N`. Tracking partitions keep resending fences. Participants keep retrying release reports. Produces to `P`'s user partitions keep failing, as they do while `P` is offline.

Losing `P` can't delete or resurrect another partition's data, so clause 3 holds:

- No key is reused. A broker picks each range above every range it requested in its epoch, and keeps that counter in memory. A restart is a new epoch.
- A tracking partition queues an object only when every participant answers released, so a wrong answer from `P` can't delete batches that another participant holds.
- The rebuild adds rows only for runs that map to `P`, and the existing mapping check rejects any other.

### Finding the objects

`StorageBackend` gets a read-only list operation that only recovery uses. The rebuild lists the prefix `<clusterId>.`, takes each object's size from the listing, reads the trailer at `size − 14` and then the body, and keeps the runs that map to `P`. The size also covers Azure, which has no suffix range. The listing sees current objects only, not soft-deleted versions.

v2 rejected a scanner as a deletion path and a prefix listing as a way to collect leftovers. A recovery listing deletes nothing and runs only during recovery. Its cost is one list request per 1,000 objects and one ranged read per object.

Rejected alternatives:

- Asking the surviving partitions for the keys whose participant set includes `P`. It misses objects whose only participant and tracking partition were both `P`, such as a low-traffic broker's files.
- That, plus probing the keys of `P`'s brokers by name. It can take millions of size lookups per broker epoch.

### Filter

A new private request asks each surviving tracking partition `T` for `P`'s state on the listed keys that it tracks, named by the footer's tracking partition. The rebuild applies this table:

| `T`'s state for `P` on object `X` | Meaning | Rebuild |
| --- | --- | --- |
| Released, or `X` is in `T`'s deletion queue | `P` removed its batches or never committed them | Skip `P`'s runs in `X` |
| Referenced | A fence found `P` holding them | Keep |
| No record, the file finished, and `P` not in an exception range | `P`'s part committed on the normal path | Keep |
| No record, and the file isn't finished | In flight at the loss | Keep |

Runs with no surviving signal stay: objects whose tracking partition was `P`, about 1/N of the objects with `P`'s batches, and in-flight files. Skipping them would lose about 1/N of `P`'s data plus its most recent writes. After the filter, the rebuild deduplicates by producer sequence and applies retention again.

### Offsets

For each user partition, the floor is the maximum of:

- The highest offset hint in any surviving footer, plus the total record count of the recovered batches. Every batch committed after the hinted one is newer than it, so retention hasn't removed its object, and the old high watermark is at most this sum.
- The highest offset committed in `__consumer_offsets` and `__share_group_state`.
- An optional floor that the operator supplies for a partition or for the whole recovery, for example the last `LogEndOffset` metric before the loss.

New offsets start at the floor plus 1:

- A partition with recovered batches keeps its log start at the bottom of the gap: 0 upstream. `find_batches` returns the next batch for an offset inside a gap, so a consumer whose position is in the gap re-reads the recovered records, with no `OFFSET_OUT_OF_RANGE` and no reliance on `auto.offset.reset`. A log start at the floor plus 1 would send those consumers to their reset policy, and `latest` would skip every recovered record.
- A partition with no recovered batches gets its log start and high watermark at the floor plus 1, with no gap. A gap at the end of a log returns empty fetches with no error, and the consumer would fetch the same offset forever.
- The coordinator allows an offset gap in a diskless log.
- Reusing old numbers was rejected: a consumer at committed offset `X` would silently skip the recovered records below `X`, which aren't the ones it read.

### Order within a partition

The rebuild orders each user partition's runs by offset hint, then by the batches' maximum timestamp, then by broker ID, epoch, and sequence. Each run keeps its batches in byte order. Each idempotent producer's batches are then checked against sequence order, and later sequences move after earlier ones where runs conflict. Only the leader computes the order, and the recovery records carry the assigned offsets.

Each broker's commits keep their relative order, and each idempotent producer's order is exact. Non-idempotent producers that wrote through several brokers get an order that is correct to within about one commit interval.

### Protocol roles

- Broker rows: for each broker, the fenced-through pair is the highest `(epoch, sequence)` among that broker's listed objects. A live broker's counter is above every key it uploaded, so its next commit passes. A part still in flight below the pair gets stale-sequence, and its producer retries. No stored result is restored, because every retry window from before the loss has ended.
- Tracking entries: for each object whose footer names `P` as tracking partition, the participant set comes from the runs. `P`'s own state is referenced if the rebuild kept runs there and released otherwise. Every other participant starts unknown.
- `P` fences unknown participants under the rule in [When the tracking partition fences silent participants](01-silent-participant-fences.md): only once the broker's epoch has retired, or once a finish report covers the sequence. Live brokers deliver that report on the lease release they retry for their abandoned ranges at `P`, under [Lease lifecycle at the broker: fallback ranges and idle ends](02-lease-lifecycle.md). Restarted brokers' epochs retire through the image. The batching in [Fence fan-out under correlated failure](06-fence-fan-out.md) applies.
- The lease table starts empty. Lease releases end the abandoned ranges at `P` as usual. The uploader's key prefix, which deleting never-committed keys needs, comes from that broker's listed keys.
- The deletion queue starts empty, and an object enters it when every participant has answered released.

### Resuming

- In the gather phase, the leader lists the bucket, reads the footers, queries the survivors, and reads committed offsets, all in memory.
- In the commit phase, it appends recovery records one user partition at a time, and each ends with a partition-recovered record. The broker rows and tracking entries come after every user partition, then the recovery-completed record.
- The rebuild's inputs stay stable during the gate: no new object can involve `P`, no object that `P` participates in can be deleted, and consumers can't advance on `P`'s partitions.
- After a leader change, the next leader sees the marker without a recovery-completed record. It gathers again and skips every user partition with a partition-recovered record. The role records are keyed by broker and object key, so writing them again is a no-op.
- There is no abort. Restarting from scratch would need records that replicas already applied to disappear, and a resumed rebuild reaches the same state.

### Inkless extension

Where a partition has a surviving copy at its original offsets, such as a consolidated log or the classic prefix below the seal, the extension raises the floor to that copy's end and keeps its offsets. How the extension drops WAL batches that consolidation already copied stays open for the version that builds the recovery.

### Rejected alternatives

- Safety only: `P` comes back empty, and its user partitions lose everything. Clause 2 fails without a relaxation.
- Safety now, with a footer for an offline salvage later. Kafka wouldn't serve the records again.
- A SQLite snapshot uploaded to object storage, with footers for the tail. It's the off-broker copy that the map rules out of scope, and a restore at an older offset brings back stale floors, stale leases, and coordinates into objects deleted since.
- Relaxing clause 2 with an enforced replication guard. The footer meets clause 2, so the fallback isn't needed.

### Failure-mode rows

| Failure | Behavior | Test |
| --- | --- | --- |
| Every replica of a coordinator partition loses its storage, and KRaft survives | The operator triggers recovery. `P` refuses all work, rebuilds from footers, and serves the records at new offsets above the floor, with producer state rebuilt. Other partitions are unaffected. | 1 |
| The leader changes during the rebuild | The next leader gathers again and skips the user partitions already recovered. | 4 |
| A footer has a missing magic number or a bad CRC | The rebuild logs the key, counts it, and continues. | 6 |
| A footer has an unknown version or tag | The rebuild stops and reports the key, version, and tag. | 6 |
| A fence, commit, or release reaches `P` during the rebuild | `COORDINATOR_LOAD_IN_PROGRESS`. Senders retry, and no object that `P` participates in is deleted. | 3 |

### Accepted risk

This row replaces v2's "Every replica of a coordinator partition is lost":

| Risk | Why it's accepted | Signal or procedure |
| --- | --- | --- |
| After a recovery, offsets change. In objects with no surviving signal, batches that `P` had removed or never committed come back. Non-idempotent producers can see duplicates. Consumers that store offsets outside Kafka can skip records unless the operator supplies a floor. | It stays within `P`'s user partitions, and it loses no acknowledged record. A classic partition that loses every replica loses all of them. | Signal: the recovery metrics. Procedure: the recovery runbook, which covers lag alerts, applications that assume contiguous offsets, and the operator floor. |

Losing the KRaft metadata goes to the map's Out of scope section.

### Metrics

On `P`'s leader:

- `RecoveryInProgress`.
- `RecoveryPartitionsRemaining`.
- `RecoveredRunCount`, tagged by signal: `referenced`, `committed`, `no-signal`, or `in-flight`.
- `RecoverySkippedRunCount`.
- `RecoveryUnreadableObjectCount`.

Offset-based lag counts the gap until a group reads past it, so lag alerts fire once after a recovery.

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. End-to-end recovery. With `N=8`, idempotent and non-idempotent producers write, every replica of one coordinator partition is wiped, and the operator triggers recovery. Every acknowledged record of `P`'s partitions reads back above the floor. Idempotent producers continue without `OUT_OF_ORDER_SEQUENCE_NUMBER`. Other partitions see no change.
2. Filter. Batches removed by `DeleteRecords` in an object that a surviving partition tracks don't come back.
3. Gate. During the rebuild, fences to `P` are refused, and no object that `P` participates in is deleted.
4. A leader change during the rebuild. Recovery resumes, and no user partition is applied twice.
5. Floor and gap. A consumer whose committed offset is inside the gap re-reads. A partition with nothing recovered has its log start equal to its high watermark, at the floor plus 1.
6. Footer. A checked-in golden footer decodes. The shared suite shows that every object has a valid footer that matches its commit requests, with readers unchanged. An unknown version stops the rebuild, and a bad CRC is counted.
7. Order. Runs from two brokers order by hint, and each producer's sequence order holds.
8. Controller, in `ReplicationControlManagerTest`. The recovery request is rejected while any eligible replica's storage is intact, and accepted otherwise. The marker clears after the recovery completes.

### User story

New story: "As an operator, I want to recover a coordinator partition's records from object storage after every replica loses its storage, so that the loss costs no more than a classic partition loss."

### Consequences for other tickets and the rewrite

Because the design is deferred:

- [Enforced replication factor and rack spread](11-replication-guard.md) owns the accepted-risk row "Every replica of a coordinator partition is lost", with its signal and procedure. The row states that the guard is what makes the loss acceptable under clause 2, and that a footer recovery is future work.
- The rewrite's Future work section gets an entry for the footer and the recovery, with the main decisions: footer fields, operator trigger, gate, bucket listing, filter, floor and gap, and the KRaft assumption. It links this ticket for the full tree.
- The design recommends dedicated controllers, citing Kafka's guidance on combined mode, independent of the recovery.

If a later version builds the recovery, it also takes these consequences:

- The recovery listing sees current objects only, not soft-deleted versions, which touches [Deletion bounds: soft-delete requirement and a deletion-rate cap](10-deletion-bounds.md).
- The replication guard stops being clause 2's fallback.
- The design adds a Recovery section, the footer to the write path, the list operation to `StorageBackend`, the recovery request to Private RPCs and the controller guards, the recovery records to Log records, and the terms floor, offset hint, and recovery marker to the Terms section.
- The Inkless extension for consolidated partitions still needs a decision on how it drops WAL batches that consolidation already copied.
