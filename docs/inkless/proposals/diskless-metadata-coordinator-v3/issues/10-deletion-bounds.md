# Deletion bounds: soft-delete requirement and a deletion-rate cap

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 08

## Question

How does v3 bound the damage when one coordinator partition deletes an object that another partition still references?

Clause 3 of the map's data-loss bar forbids accepting a failure that deletes or resurrects another partition's data. In v2, five accepted-risk rows can do that, with `flush.messages=1` and an honest fsync as the only defense: log recovery that truncates fsynced records, a device that acknowledges an fsync without persisting it, a log directory restored from an older copy, a stale leader-epoch entry that over-truncates an ELR member, and a deterministic apply bug on every replica. See v2's [Accepted risks](../../diskless-metadata-coordinator-v2/design.md#accepted-risks), [Flush policy](../../diskless-metadata-coordinator-v2/design.md#flush-policy), the council's [lost-tail risk](../reviews/council.md#new-risks-in-v2) and [second fix](../reviews/council.md#what-would-close-the-gaps). The provider facts are in [Object-storage soft-delete and versioning](08-object-storage-soft-delete.md).

Settle these points:

- Whether soft-delete or versioning is a deployment requirement, a startup check that fails or warns, or both, and the minimum retention. The research found that object-write credentials can't read the setting on any provider, but S3 and Azure delete responses report whether a delete was soft, which the backends ignore today. Azure versioning with a lifecycle rule ages from upload, so only Azure blob soft delete qualifies there. Only MinIO among the test emulators models a recoverable delete.
- The deletion-rate cap per tracking partition: its unit, its default, what happens to queue rows above it, and the alarm.
- For each of the five rows: whether the bound makes it acceptable under clause 3, and what an operator does to recover. The consequence "offsets permanently fail to read and consumers get stuck" needs an answer.
- Whether resurrection, a late part that commits into a queued or deleted key after a lowered floor, is bounded by the same means.

## Resolution

Resolved on 2026-11-13 in a grilling session, and deferred to future work the same day.

### Status: future development idea

The session worked the whole design tree, and this resolution records it as the design for a later version. v3 includes none of it:

- v3 doesn't require soft-delete, doesn't probe, has no deletion-rate cap, and adds no missing-key metric.
- Clause 3 of the map's data-loss bar is relaxed for the six rows in [Accepted risks in v3](#accepted-risks-in-v3). They stay accepted risks on v2's argument that a classic topic loses data to the same causes.
- The rewrite adds a Future work entry that summarizes this design and links here.
- The failure-mode rows, metrics, tests, and runbook in the deferred design apply only when a later version builds the bound. They don't go into v3's cross-cutting sections.

### Accepted risks in v3

These rows replace v2's five lost-tail and apply-bug rows, and add a sixth. Each row keeps v2's signal or procedure:

| Risk | Why it's accepted | Signal or procedure |
| --- | --- | --- |
| Log recovery truncates fsynced records after a read error or bit rot, under an unchanged directory ID | A classic topic loses data to the same cause. The consequence crosses coordinator partitions: a tracking partition can delete an object whose batch rows come back at the participant, or a late part can commit into a deleted key. The soft-delete bound is future work. | Procedure: alert on log recovery's invalid-message truncation warning for `__diskless_metadata`, and stop the broker for inspection. |
| A device acknowledges an fsync without persisting it | Same as the preceding row. | Procedure: the deployment docs require storage that honors fsync. |
| An operator restores a log directory, `meta.properties` included, from an older copy | Same as the preceding row. | Procedure: never restore a log directory from a copy. Reformat it and let the broker re-replicate. |
| A stale leader-epoch entry over-truncates an ELR member, and the leader fails before that member fetches again | Same as the preceding row. | Signal: `UncleanLeaderElectionsPerSec` and `ElectionFromEligibleLeaderReplicasPerSec`. |
| A deterministic apply bug corrupts every replica alike | Kafka's coordinators share this limitation. A wrong release or fence answer deletes an object that another coordinator partition still references, and the delete is permanent. | Signal: v2's sustained `PartitionStorageErrorRate`. Prevention: the shared suite, the golden-records test, and the apply-stop rule. |
| Something outside the coordinator deletes a WAL key, such as a bucket expiration rule or a manual delete | Inkless can't prevent it. A classic topic loses data the same way when someone deletes its segment files. | Procedure: the deployment docs forbid expiration rules and manual deletes on the WAL prefix. Signal: `PartitionStorageErrorRate`. |

### Paths to a deleted key with live references

Under the design as written, no path deletes a referenced object or lets a part commit into a deleted key, as [Fable's verdict](../reviews/fable.md#verdict) found. A key enters a deletion queue only after every participant holds a durable record that makes the key unreachable there: the removal of its batches, or a raised floor. A deleted key is referenced again only when that record disappears or was wrong from the start:

- An acknowledged record vanishes from every replica that can become leader. That is one of the first four rows. A lost removal brings batch rows back after the object is gone, which is release inversion. A lost floor lets a late part commit into a deleted key, which is resurrection.
- A deterministic apply bug releases held batches or answers "not held" by mistake.
- Something outside the coordinator deletes the key while batch rows reference it.

Object storage doesn't roll back with the coordinator log, so the delete is permanent, and every later read of those offsets fails. A recoverable delete undoes it: a restore brings the bytes back under the same key, and the batch rows that name the key read again with no metadata repair.

### Deferred design

#### Requirement

The bucket keeps deleted WAL objects recoverable for at least 7 days. Clause 3 holds as written under this bound. These setups count:

- S3: versioning `Enabled`, with a `NoncurrentVersionExpiration` rule of `NoncurrentDays` 7 or more that covers the prefix. `Suspended` doesn't count.
- GCS: soft delete with a retention of 7 days or more. It's the default and the minimum.
- Azure: blob soft delete with a retention of 7 days or more. Versioning alone doesn't count, because its lifecycle rule ages a version from the upload.

Seven days matches the GCS minimum and covers a weekend plus an operator's response. The design states the cost: the upload rate times the window, about double the WAL bytes for a 7-day-retention topic. The deployment docs also forbid expiration rules and manual deletes on the WAL prefix.

Rejected alternative: a long `FileCleaner` grace period that holds queued keys for days. It needs no bucket setting and keeps reads working, but it doesn't bound a bug in the coordinator's own delete path or a delete from outside the coordinator.

#### Enforcement

Object-write credentials can't read the bucket settings, so the broker checks the effect instead. [soft-delete-probe.md](../research/soft-delete-probe.md) holds the facts.

- Probe: before a tracking partition leader's first `FileCleaner` pass, and every hour after that, the broker uploads a small key `<prefix>/<clusterId>.probe.<brokerId>.<brokerEpoch>` and deletes it. The name can't match a WAL key.
  - S3: the delete response must report a delete marker. Object credentials can't read the lifecycle rule, so the 7-day window rests on the deployment docs.
  - GCS: the broker deletes the probe by generation, so that a bucket with Object Versioning still soft-deletes it, then reads it back with `objects.get` and `softDeleted=true`. A `null` result, or `hardDeleteTime − softDeleteTime` under 7 days, fails the check. This needs only `storage.objects.get`.
  - Azure: `x-ms-delete-type-permanent` must be `false`, and `List Blobs` with `include=deleted` must report `RemainingRetentionDays` of 7 or more for the probe.
- Per-delete check: on S3 and Azure, every real delete response is checked the same way. GCS returns an empty body, so the hourly probe is its only check.
- Reaction: a failed check or a permanent delete stops every delete on that broker, logs the reason, and sets the `PermanentDeleteDetected` gauge. Queue rows wait, and storage grows, but produce and fetch are unaffected. Deletes resume at the next probe that passes. At most one pass's keys are deleted permanently between probes, and those keys were rightly queued.
- Opt-out: the broker config `diskless.metadata.deletion.require.soft.delete`, true by default. False skips the check for development setups and emulators without versioning, and the design states that clause 3 doesn't hold then.

Rejected alternatives: warning only, which keeps deleting permanently, and failing startup, which nothing at startup can prove without a delete.

#### Detection and restore

- A fetch that gets `KeyNotFoundException` for a key that committed batch rows still reference raises the alarm. Outside the deposed-leader and grace-period windows, the broker counts it in `MissingReferencedObjectRate` and logs the key, the user partition, and the offset range.
- The operator restores the key by name with the provider's restore: copy the noncurrent S3 version onto the key, `objects.restore` on GCS, or `Undelete Blob` on Azure. The batch rows then read again.
- Residual risk: the soft-delete window expires on a wrongly deleted range that nobody read within 7 days, and its records are lost. The case that loses live data and can stay unread that long is the apply bug, for which the map already accepts read-time detection.

Rejected alternatives:

- Self-heal on read, where the broker re-checks with the leader and restores the key itself. It needs restore permissions beyond object write on S3, and every restored object leaks.
- A periodic audit that each referenced key exists. It costs one size lookup per object, or a tombstone list of deleted keys at each tracking partition for the whole window.

#### Deletion-rate cap

With soft-delete in place, the cap doesn't prevent loss. It makes a runaway visible and keeps a restore small.

- Unit: keys deleted per second by each tracking partition leader.
- Limit: `k` times the rate of sequences that the partition recorded over a trailing hour, with a floor, so a quiet partition still drains a topic deletion. The defaults are `k = 4` and 10 keys per second, both provisional and set by benchmarks. The leader keeps both values in memory and recomputes them after an election.
- Overflow: keys above the limit stay in the queue in mark-time order and go out in later `FileCleaner` passes. A delete only waits longer.
- Alarm: the `DeletionCapActiveTime` gauge, with the existing queue size and oldest-mark age. A sustained limit means a legitimate burst, such as a topic deletion or a retention cut, or a runaway. The runbook tells them apart by checking for a recent topic deletion or config change.

Normal deletes match uploads under the partition's leases, about 4 files per second per broker at the 250 ms commit interval. That is about 15 keys per second per tracking partition at 30 brokers and `N=8`, and about 150 at 300 brokers, so a fixed default would fit only one cluster size.

Rejected alternatives: a fixed keys-per-second config, which needs tuning per cluster size, and an alarm with no throttle, which lets a runaway delete at full speed.

#### Resurrection

Resurrection is bounded by the same means. In v3 it needs a cut-off zombie broker whose part arrives after an idle lease end, at a participant that lost its `sequences-fenced` record. A broker that gave up on a part doesn't resend it, and a participant's power loss resets its connections and pending entries. The new rows sit at the tail, so tail consumers fire the metric within seconds, and the operator restores the key.

A restored object leaks: the tracking partition already forgot it, and a later release report waits under the rule for unrecorded sequences in [Release reports: ordering, batching, and a leak signal](05-release-reports.md). The design accepts the leak, bounded by the restored keys, and the runbook says nothing collects them. No tombstones of deleted keys are added.

#### Rows and runbook

Each of the six rows in [Accepted risks in v3](#accepted-risks-in-v3) passes clause 3 under the bound. Each keeps its v2 signal or procedure, and gains `MissingReferencedObjectRate` and the restore within 7 days. The runbook answers stuck consumers by case:

- Release inversion after a retention removal: the next retention pass removes the rows again, with no action.
- Release inversion after `DeleteRecords`: the rows that came back were deleted by the user, so the operator runs the same `DeleteRecords` again.
- Resurrection, an apply bug, or an outside delete, inside the window: the operator restores the key.
- The same cases after the window: the range is lost. The operator resets the affected consumer groups past it with `kafka-consumer-groups --reset-offsets --to-offset`, and the rest of the log stays readable.

#### Failure-mode rows

| Failure | Behavior | Test |
| --- | --- | --- |
| The bucket deletes permanently, or keeps deleted objects for less than 7 days where the provider shows it | The probe or the per-delete check fails, deletes stop on that broker, and `PermanentDeleteDetected` is set. Deletes resume at the next passing probe. | 1 |
| A tracking partition queues far more keys than its upload rate | The cap holds deletes at `k` times the trailing rate, rows wait in mark-time order, and `DeletionCapActiveTime` rises. | 2 |
| A participant loses a removal record after the tracking partition deleted the object | A fetch fires `MissingReferencedObjectRate` with the key, and the operator restores it. | 3 |
| A participant loses its floor, and a zombie's part commits into a deleted key | The same detection and restore. The restored object leaks. | 4 |

#### Metrics

- `PermanentDeleteDetected`, on every broker.
- `DeletionCapActiveTime`, on each tracking partition leader.
- `MissingReferencedObjectRate`, on every broker, tagged by user partition.

#### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md). The model of a lost tail at every fsync point belongs to [Model of the fence, floor, and release protocol](17-protocol-model.md).

1. Probe. On MinIO with versioning enabled, deletes proceed. With versioning suspended, the probe reports a permanent delete, deletes stop, and the gauge is set. Re-enabling versioning resumes deletes at the next probe. Azure's header and `RemainingRetentionDays` parsing, and the GCS read-back, are unit tests on recorded responses, because Azurite and `fake-gcs-server` don't model soft delete.
2. Cap, in a shard test. A burst above `k` times the trailing rate is limited, rows go out in mark-time order, and the gauge rises. A quiet partition drains at the floor.
3. Release inversion restored, in a cluster test on versioned MinIO. After the tracking partition deletes an object, the participant's replicas lose the removal record by truncation below it. A fetch fires the metric with a log line that names the key, a restore of the noncurrent version follows, and the reads succeed.
4. Resurrection restored. The participant loses its `sequences-fenced` record, and a zombie's part commits into the deleted key. The same detection fires, and the restore works.
5. Metric precision, in a backend test. A deposed-leader read and a grace-period race don't count, and a key that committed rows still reference does.

### Consequences for other tickets and the rewrite

Because the design is deferred:

- [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md) takes the six rows in [Accepted risks in v3](#accepted-risks-in-v3) as written, with clause 3 relaxed for them, and adds no row, metric, test, or story from the deferred design.
- The rewrite's Future work section gets an entry for the soft-delete bound, with the main decisions: the 7-day requirement, the probe and per-delete check, the read-time detection and restore, the deletion-rate cap, and the restored-key leak. It links this ticket for the full tree.
- The map's clause 3 records the exception, as clause 2 does for the footer.
