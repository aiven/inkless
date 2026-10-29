# Classic to Diskless Switch Protocol

This document describes the implementation flow for switching a classic Kafka topic to diskless storage. The switch is driven by metadata records: the controller first marks each partition as switch-pending, the leader seals the local classic log and asks the controller to commit the final start offset, then the broker initializes the diskless log on the control plane from the committed metadata.

Internally, the switch is one of the three features under **TS unification**, together with [managed replicas](FEATURES.md#managed-replicas) and [TS consolidation](DISKLESS_CONSOLIDATION.md).

## Requirements and Guarantees

The switch protocol is designed with the following guarantees and constraints:
* **Online Switch:** The switch is performed online. Producers are blocked only while a partition is sealed but not yet initialized for diskless writes.
* **Zero Data Loss:** The leader waits until the High Watermark (HW) reaches the Log End Offset (LEO) before committing the diskless start offset.
* **Configuration Constraint:** Switching is only supported for topics where unclean leader election is **not enabled**.

The Kafka controller participates in the switch to fence stale leaders. The broker proposes the final start offset, but the controller persists it only if the request comes from the current leader epoch and the current partition leader.

## `classicToDisklessStartOffset` States

`classicToDisklessStartOffset` is a per-partition `long` field on the controller/broker in-memory metadata image (`PartitionRegistration`). It serves two purposes at once:

* As a **boundary**, it marks the first offset owned by diskless storage. The classic log owns every offset below it; diskless storage owns everything at or above it.
* As a **state machine**, it drives the switch flow. Two negative values are used as sentinels, and any non-negative value is the committed boundary:

| Value | Constant | Meaning |
| --- | --- | --- |
| `-1` | `NO_CLASSIC_TO_DISKLESS_START_OFFSET` | No classic-to-diskless switch offset has been committed (the default for classic and born-diskless partitions). |
| `-2` | `CLASSIC_TO_DISKLESS_SWITCH_PENDING` | The switch has started and the classic log must be sealed, but the final diskless start offset has not been committed yet. |
| `>= 0` | — | The committed seal offset. This is the first offset owned by diskless storage; the classic log owns offsets below it. |

### How it is stored

The switch state lives in the KRaft metadata log, but `classicToDisklessStartOffset` is **not** part of the upstream Apache Kafka record schemas (`PartitionRecord`, `PartitionChangeRecord`). To avoid forking those schemas, it is carried as an **unknown tagged field** (`RawTaggedField`) attached to the standard records:

* **High tag numbers (100+).** `classicToDisklessStartOffset` uses tag `100` (`CLASSIC_TO_DISKLESS_START_OFFSET_TAG`). The 100+ range is reserved to avoid colliding with upstream tagged fields. Because the tag is unknown to vanilla Kafka, it is **silently skipped** when read by an upstream broker, which keeps the on-disk metadata format compatible in both directions. Two sibling fields ride along on the same record using the same mechanism: producer states (tag `101`) and the diskless leader epoch (tag `102`).
* **Custom serde in `InitDisklessLogFields`.** The value is encoded by hand into the tagged field's raw byte payload: a single big-endian `int64` (8 bytes) written via `ByteBuffer.putLong`, and read back via `ByteBuffer.getLong`. There is no generated message class for it.
* **Sentinel is normally not serialized.** When `classicToDisklessStartOffset == -1`, automatic paths (`toRecord()` snapshots and ordinary change records) omit the tag entirely to save space; on read, an absent tag decodes back to `-1` (`NO_CLASSIC_TO_DISKLESS_START_OFFSET`). The one exception is the operator seal/abort path, which encodes an explicit `-1` to actively reset a sealed partition back to classic (see [Operator override](#operator-override-of-the-seal-offset)).
* **Replay and merge.** On load, `PartitionRegistration` decodes the tag from `PartitionRecord.unknownTaggedFields()`; `merge(PartitionChangeRecord)` applies updates from change records and carries the existing value forward when a change record **omits** the tag; `toRecord()` re-encodes it. Crucially, "tag omitted" and "tag explicitly set to `-1`" are distinguished at decode time (`decodeClassicToDisklessStartOffsetIfPresent` returns an empty `OptionalLong` only when the tag is absent): an omitted tag keeps the existing value, while a present tag — including an explicit `-1` — is applied verbatim. This is how the value survives metadata-log replay and propagates to brokers via metadata deltas, and how an operator can abort a switch.

## Protocol Steps

```mermaid
sequenceDiagram
    participant Operator
    participant Controller
    participant MetadataLog as KRaft metadata log
    participant Leader as Leader broker
    participant InitManager as InitDisklessLogManager
    participant ControlPlane as Diskless control plane

    Operator->>Controller: Set diskless.enable=true
    Controller->>MetadataLog: ConfigRecord + PartitionChangeRecord<br/>classicToDisklessStartOffset = -2
    MetadataLog-->>Leader: Metadata delta with switch pending

    Leader->>Leader: seal() local classic log
    Leader->>Leader: makeLeader() with bumped epoch
    Leader->>InitManager: registerPartition()

    InitManager->>Leader: Wait until HW == LEO
    Leader-->>InitManager: Classic prefix fully replicated
    InitManager->>Controller: InitDisklessLog(HW, leaderEpoch, producerStates)

    Controller->>Controller: Validate current leader and epoch
    Controller->>MetadataLog: PartitionChangeRecord<br/>classicToDisklessStartOffset = HW<br/>producerStates
    MetadataLog-->>Leader: Metadata delta with committed seal offset

    Leader->>Leader: Reconcile local classic log at seal
    Leader->>InitManager: initOnControlPlane(from metadata)
    InitManager->>ControlPlane: InitDisklessLog(topic, partition,<br/>classic log start, seal offset, producerStates)
    ControlPlane-->>InitManager: Success or already initialized
    InitManager-->>Leader: Allow diskless appends

    InitManager->>Leader: Wait until the classic prefix is in remote storage
    Leader-->>InitManager: logStartOffset >= seal or<br/>highestOffsetInRemoteStorage >= seal - 1
    InitManager->>Controller: SetClassicPrefixInRemoteStorage(seal, leaderEpoch)
    Controller->>MetadataLog: PartitionChangeRecord<br/>classicPrefixInRemoteStorage = true
```

### Step 1: mark the switch as pending

When `diskless.enable` changes from `false` to `true`, the controller emits the topic config update and one `PartitionChangeRecord` per partition in the same atomic write. Each partition change sets:

* `classicToDisklessStartOffset = -2`
* `leader = current leader`

Setting the leader to the current leader bumps the leader epoch, which forces the leader broker to process a `makeLeader` transition even when leadership did not otherwise change. Brokers rely on receiving the config change and the pending marker in the same metadata delta.

### Step 2: seal the classic log

When a leader broker applies the metadata delta for a diskless topic with `classicToDisklessStartOffset = -2`, it seals the existing local `Partition` before calling `makeLeader`.

Sealing happens under the `leaderIsrUpdateLock` write lock. If there are undecided transactions, the broker aborts them first; then it marks the partition sealed. Once sealed, `appendRecordsToLeader` rejects classic produce requests with `ReplicaNotAvailableException`, so the local LEO cannot increase.

If the metadata delta also elects a new leader for the partition, the new leader seals before it is placed in the leader role. This prevents the new leader from accepting classic writes during the switch.

### Step 3: propose the final start offset

After sealing, the leader registers the partition with `InitDisklessLogManager`. The manager waits in `WaitingForReplication` until:

* the partition is still sealed
* the local log exists
* `HW == LEO`

Once HW catches up to LEO, the partition moves to `SendingToController`. The broker batches ready partitions and sends `InitDisklessLog` to the controller with:

* `disklessStartOffset = HW`
* `leaderEpoch = current partition leader epoch`
* `producerStates = active producer state extracted from the local log`

At this point `HW == LEO`, so the proposed start offset is the sealed LEO and is safely replicated to the ISR.

### Step 4: persist the final start offset

The controller handles `InitDisklessLog` as a serialized write operation. For each partition, it validates that:

* the request leader epoch is not stale
* the requesting broker is the current leader
* the proposed `disklessStartOffset` is non-negative
* the partition has not already committed a final `classicToDisklessStartOffset`

If validation succeeds, the controller writes a `PartitionChangeRecord` containing the final `classicToDisklessStartOffset` and the producer states. These values are encoded as tagged fields on the partition change record.

If the broker is stale, the controller rejects the request. A lower leader epoch is a permanent failure for that broker's attempt; retriable controller or transport failures are retried by the broker.

### Step 5: initialize the diskless log

After the final offset is committed to the metadata log, brokers receive a new metadata delta with `classicToDisklessStartOffset >= 0`. The local leader then initializes the diskless log on the control plane using the authoritative data from metadata, not the earlier controller response.

The leader calls the control-plane `InitDisklessLog` API with:

* topic ID, topic name, and partition
* local classic log start offset
* committed `classicToDisklessStartOffset`
* committed producer states

Control-plane initialization is batched and retried on transient failures. An `INVALID_REQUEST` response from the control plane is treated as success because it means the diskless log is already initialized.

Once control-plane initialization succeeds, produce requests can use the diskless append path from the committed start offset onward. If `classicPrefixInRemoteStorage` is already `true`, `InitDisklessLogManager` removes the partition from tracking. Otherwise `InitDisklessLogManager` keeps tracking the partition for the next step.

### Step 6: request the `classicPrefixInRemoteStorage` flag

In phase 1, the leader tracks the partition in the `WaitingForUpload` state until the classic prefix is in remote storage. The remote log manager copies closed segments only, in offset order. The manager polls the log once per second to detect the upload. The classic prefix is in remote storage when either of these is true:

* `logStartOffset >= classicToDisklessStartOffset`: the prefix is empty, or retention or `DeleteRecords` already removed it.
* `highestOffsetInRemoteStorage >= classicToDisklessStartOffset - 1`: the remote log manager copied the whole prefix.

When the check passes, the partition moves to `SendingFlagToController`. The broker batches ready partitions and sends `SetClassicPrefixInRemoteStorage` to the controller with the seal offset and the current leader epoch. The controller sets `classicPrefixInRemoteStorage`, which starts phase 2, and `InitDisklessLogManager` removes the partition from tracking.

The broker handles the controller response per partition:

* `NONE`: the controller set the flag.
* `INVALID_REQUEST`, `UNKNOWN_TOPIC_ID`, or `UNKNOWN_TOPIC_OR_PARTITION`: the broker stops. The partition isn't in the state the request assumed, and a retry can't change that.
* Any other error, including `FENCED_LEADER_EPOCH` and `NOT_CONTROLLER`: the broker retries with backoff. An epoch bump that keeps the same leader fences the in-flight request. The retry sends the current epoch.

If an operator lowers the seal in phase 1, the leader restarts the watch with the new seal. The re-seal keeps the same leader and leader epoch. The broker detects the changed seal in the metadata delta. For more information, see [Operator override of the seal offset](#operator-override-of-the-seal-offset).

A partition in phase 1 counts toward the `DisklessSwitchedPartitionsPendingUploadCount` controller metric until the controller sets the flag. For more information, see [Controller Metrics](FEATURES.md#controller-metrics).

The leader reports the progress of this step under `kafka.server:type=InitDisklessLogManager`:

* `WaitingForUploadPartitions` and `SendingFlagToControllerPartitions`: the number of partitions in each state.
* `OldestWaitingForUploadAgeMs` and `OldestSendingFlagToControllerAgeMs`: how long the oldest partition has been in each state, or `0` if none is. A growing `OldestWaitingForUploadAgeMs` points to a prefix that the remote log manager doesn't copy.
* `FlagRequestCompletedPerSec`, `FlagRequestFailedPerSec`, and `FlagRequestRetriedPerSec`: per-partition outcomes of the flag request. `FlagRequestFailedPerSec` counts only controller rejections. A request that the broker skips after a leadership change or a topic deletion doesn't count.

`InFlightPartitions` counts only the partitions in steps 3 to 5, so it reaches `0` while partitions still wait for the upload.

## Additional Behavior

Followers also seal during the switch. While the switch is pending (`-2`), they keep using the classic fetcher to replicate the frozen classic prefix. After the final start offset is committed, followers continue until their local LEO reaches the committed seal offset, then hand off from the classic prefix.

When a broker applies a newly committed seal offset, it reconciles the local classic log:

* if local LEO is above the seal, it truncates to the seal because offsets at and above the seal belong to diskless storage
* if a leader's local LEO is below the seal, it marks the partition offline because the classic prefix is incomplete

If leadership changes while the switch is pending, the new leader seals and re-drives the `InitDisklessLog` controller request. If leadership changes after the controller commits the final offset, the new leader skips the controller step and initializes the control plane directly from metadata. If the flag is still unset, the new leader also rolls its active segment when that segment holds part of the classic prefix, so the remote log manager can copy it. The new leader then watches the upload itself, as in [Step 6](#step-6-request-the-classicprefixinremotestorage-flag).

## Switch phases

A switched partition runs in one of two phases. The `classicPrefixInRemoteStorage` partition flag marks the boundary. The controller applies different rules in each phase, because the classic prefix (the offsets below `classicToDisklessStartOffset`) lives in different places.

* Phase 1 starts when the controller marks the switch pending and ends when the controller sets `classicPrefixInRemoteStorage`. Only the replicas' local disks hold the classic prefix, and the replicas aren't interchangeable.
* Phase 2 starts when the controller sets `classicPrefixInRemoteStorage`. Remote storage holds the classic prefix, and the partition follows the same rules as a born-diskless partition.

| Controller behavior | Phase 1 | Phase 2 |
| --- | --- | --- |
| Leader election | Classic rules: the leader must be an active replica with an online log directory. | Born-diskless rules: any active replica can lead. |
| ISR expansion when a broker unfences | Skipped. As for a classic partition, only `AlterPartition` expands the ISR. | The controller re-adds the unfenced replica and completes an in-flight reassignment. |
| Reassignment | The controller stages the move as it does for a classic topic. New replicas are adding and dropped replicas are removing until the adding replicas join the ISR. | The target replicas replace the current replicas in one step. |
| `AlterDisklessSwitch` | Accepted. See [Operator override](#operator-override-of-the-seal-offset). | Rejected with `INVALID_REQUEST`. |

The controller sets the flag when it handles a `SetClassicPrefixInRemoteStorage` request from the current leader. In the same record, the controller sets the ISR, can complete a reassignment, and elects a leader if needed. The leader sends this request after the remote log manager uploads the classic prefix, or after retention or `DeleteRecords` removes it. For more information, see [Step 6](#step-6-request-the-classicprefixinremotestorage-flag).

## Operator override of the seal offset

The normal flow above is leader-driven: the broker proposes the seal offset and the controller validates it. For recovery and manual intervention, the `kafka-topic-switch.sh seal` operator command can override the seal state of a single partition directly, without being the partition leader. It is backed by the `AlterDisklessSwitch` controller RPC (admin → controller, authorized as `ALTER` on the cluster), which writes a `PartitionChangeRecord` carrying the requested `classicToDisklessStartOffset`. It is a recovery tool for a topic that is being switched, not a way to start a switch, so the controller rejects it unless the topic already has `diskless.enable=true`.

* **`>= 0`** forces (re-)sealing at that offset. The controller also captures the current leader epoch as the diskless leader epoch, exactly as the leader-driven commit does, so consolidation truncation stays correct. Unlike `InitDisklessLog`, this is accepted even when the partition has already committed a seal offset, so an operator can correct a bad seal — but only *downward*: a re-seal may not exceed the current committed seal, since that is the classic log end offset and offsets beyond it exist only in diskless storage (routing them to the classic log would break consumption). Committed producer states are left unchanged unless `--clear-producer-states` is passed, which writes an explicit empty producer-states tag to reset them.
* **`-1`** aborts the switch and reverts the partition to classic. The tag is written explicitly (see [How it is stored](#how-it-is-stored)) so that `merge` resets the value rather than treating the record as "unchanged".
* **`-2`** re-arms the switch as pending and re-sets the leader to bump the leader epoch, forcing the broker to seal again — the same mechanism as the initial switch-pending mark.

The negative (abort / re-arm) offsets are only accepted while the switch is still pending — i.e. the partition has not yet committed a seal (`classicToDisklessStartOffset < 0`). Once a seal is committed, diskless data may have been written past it, and reverting to classic would strand that data (and can move consumers backward), so the controller rejects `-1`/`-2` in that state.

The controller rejects every seal offset, including a downward correction, once the partition is in phase 2 (`classicPrefixInRemoteStorage` is `true`). At that point remote storage holds the classic prefix up to the committed seal, so a change would leave the two out of agreement. The request fails with `INVALID_REQUEST`.

Because a negative offset means the partition is not switched, `merge` also drops the dependent switch metadata (producer states + the diskless leader epoch) whenever an explicit negative `classicToDisklessStartOffset` is applied. This prevents stale state from a previous switch attempt surviving an abort or re-arm; the next `InitDisklessLog` flow re-captures both.

The command validates before committing: for a concrete (`>= 0`) seal it requires the seal offset to fall within the classic log range `[logStartOffset, logEndOffset]` (read via `listOffsets`), so the seal cannot point past the end of the log or below its start — in either case the classic log would be missing offsets it is supposed to own. Note the end offset is the leader's log end offset, not the replication high watermark, so this does not by itself prove every in-sync replica has replicated up to the seal. The `--dry-run` flag performs this validation and reports the action without writing anything.

The command also reads the partition when you omit `--offset` or pass an offset `>= 0`. If `classicPrefixInRemoteStorage` is `true` in that read, the command fails before it sends a request, including with `--dry-run`. If the partition already has a committed seal, an explicit offset that exceeds it fails the same way. The `-1` and `-2` offsets don't read the partition, which keeps them usable with `--bootstrap-controller`. For those offsets, the controller does all the checking and `--dry-run` doesn't validate them.

## Reconciling the control plane

The seal offset lives in two places: the KRaft metadata (`classicToDisklessStartOffset`) and the diskless control plane (the `diskless_start_offset` column on the control-plane log entry). The normal switch flow keeps them in sync — the broker initializes the control-plane entry from the committed metadata (Step 5). An operator `seal` override, however, only writes KRaft; the control plane is left untouched, so the two can diverge.

The `kafka-topic-switch.sh repair` command reconciles them. It reads the committed seal from KRaft (via `DescribeTopicPartitions`) and, for each switched partition (`classicToDisklessStartOffset >= 0`), sends a `RepairDisklessLog` request to the partition leader (which owns the control-plane connection). The leader updates `diskless_start_offset` on the existing control-plane entry. Repair only reconciles an entry that already exists — it never creates one; initialization always comes from the regular switch flow. Repairing a partition whose control-plane entry is missing fails with `UNKNOWN_TOPIC_OR_PARTITION` (a stuck switch must be unblocked through the switch flow, not force-initialized here). Partitions that are not switched are skipped.

`repair` targets a single partition with `--partition`, or all switched partitions of the topic when omitted. Unlike `seal` (an `ALTER`-authorized controller RPC), `RepairDisklessLog` is a broker RPC routed to the partition leader, since the control-plane write happens there.
