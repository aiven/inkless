# Record compatibility across broker versions

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

What does a replica do with a coordinator record whose type or version it doesn't know, and what stops a leader from writing one?

Kafka's coordinators skip an unknown record type: `CoordinatorLoaderImpl` logs it as "a left over from an aborted upgrade" and ignores it. A known type that fails to deserialize stops the load. For the group coordinator, skipping is safe because later records overwrite state. For this coordinator, a skipped record, such as a batch delete, makes one replica's SQLite diverge from the others. v1 already finalizes a coordinator feature at format time and rejects brokers that lack it, in [Rollout](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#rollout).

[Read consistency after a leader change](07-read-consistency.md#barrier-for-an-offset-above-the-high-watermark) adds a barrier record, whose apply only advances the applied offset.

Settle these points:

- Whether a replica skips an unknown type or version, or stops serving that coordinator partition.
- How writes are gated, for example on the finalized feature level, as KRaft gates records on `MetadataVersion`.
- How an upgrade, an aborted upgrade, and a downgrade behave.
- The tests.

## Resolution

Resolved in a grilling session on 2026-11-10. A replica never skips what it can't read: it stops applying that coordinator partition. Feature levels keep that state out of reach. The leader writes only what every registered broker supports, and the controller refuses every downgrade. The design targets upstream Kafka, so its records carry no tagged fields. Inkless extends them with raw tags through a registry that the upstream reader leaves empty. The SQLite schema has its own version, because a snapshot copies the database file, whose layout changes between binaries.

### Kafka baseline

- `CoordinatorLoaderImpl` skips an unknown record type with a warning. An `UnknownRecordVersionException` from `CoordinatorRecordSerde`, or any other deserialization failure, fails the load (`CoordinatorLoaderImpl.java:264-273`, `CoordinatorRecordSerde.java:80-82`). Followers of Kafka's coordinators don't apply records. Only a new leader replays the log.
- Skipping is safe there because group and offset records are keyed snapshots of state, and a later record or tombstone for the same key overwrites a missed one.
- KRaft treats a metadata loading fault as fatal on a controller. A broker-only node increments `metadataLoadErrorCount`, disables snapshots, and keeps running (`SharedServer.scala:203-210`).
- `FeatureControlManager.reasonNotSupported` rejects an upgrade unless every registered broker and controller supports the new level, and registration rejects a broker that lacks the finalized level.
- For any feature other than `metadata.version` and `kraft.version`, the controller accepts `SAFE_DOWNGRADE` and `UNSAFE_DOWNGRADE` with no further check (`FeatureControlManager.java:276-325`). `FeatureVersion.dependencies()` can require other features' levels, but nothing lets a feature refuse its own downgrade.
- `TransactionVersion.transactionLogValueVersion()` picks the transaction log's record version from the feature level.

### Unknown records on apply

A replica that meets an unknown record type, an unknown value version, or an unrecognized tagged field, at load or during live apply, stops applying that coordinator partition at that offset:

- Its SQLite stays at the last applied offset.
- It stops serving reads and commits for the partition, and a leader resigns.
- It keeps fetching bytes, because replication doesn't parse records, so it stays in the ISR.
- A gauge and an error log report it. See [Failure mode and metric](#failure-mode-and-metric).

No record type is ignorable. Every coordinator record is a delta that no later record overwrites. A skipped file deleted or retention applied record leaves rows that the other replicas dropped, a permanent and silent divergence. Even the barrier record from [Read consistency after a leader change](07-read-consistency.md#barrier-for-an-offset-above-the-high-watermark) advances the applied offset.

Leader epochs don't address this case. Below the high watermark, every batch was written by the leader of its epoch, because a follower truncates a divergent tail through `OffsetsForLeaderEpoch` before it fetches more. The unreadable record is valid. The reader's binary is older than the writer's.

### Write gate

One upstream feature, for example `diskless.coordinator.version`, has two roles:

- It selects the backend, as v1's format-time feature does. Level 0 means the coordinator is off.
- Its level caps the record set. Level 1 is the initial set, including the barrier record, and each later level names the types and versions it adds.

The leader reads the finalized level from its `MetadataImage` when it builds each record, and writes the highest version the level allows. A leader whose image lags writes an older version for a while, which every replica reads. The controller's support check makes the upgrade safe.

Rejected alternatives:

- Gating on `metadata.version`. Fork-specific levels would collide with upstream numbering at every sync.
- A second upstream feature only for the record format. It adds a pair of levels that operators must keep consistent, with no gain.

### Downgrade

The controller refuses every downgrade of the coordinator feature, `SAFE_DOWNGRADE` and `UNSAFE_DOWNGRADE` alike. A name-keyed branch in `FeatureControlManager.updateFeature`, next to the `kraft.version` branch, does this. Without it, a safe downgrade from level N to N-1 leaves level-N records in the log and snapshots, and brokers rolled back to an N-1 binary stop on them. A downgrade to 0 would also switch the backend back to Postgres, which v1 rules out.

Upgrade, aborted upgrade, and binary downgrade behave as follows:

- **Upgrade:** roll every broker to the new binary, then raise the level. Raising the level is the point of no return.
- **Aborted upgrade:** the binaries roll back before the level rises. No leader has written a newer record, so the log stays readable. A rolled-back replica can still find a newer SQLite schema, which [SQLite schema version](#sqlite-schema-version) covers.
- **Binary downgrade below the finalized level:** registration rejects the broker, so it never meets records it can't read.

A later level that changes behavior but adds no records could allow a downgrade, as `MetadataVersion.checkIfMetadataChanged` does. The design can add that per level when such a level ships.

### Tagged fields and Inkless extensions

The upstream design defines no tagged fields on coordinator records or private RPCs. Every schema change is a new version or a new type, gated by the coordinator feature's level. Generated readers collect an unknown tag into `unknownTaggedFields()` and read the rest, so a tag that an old reader skipped would change apply silently. For example, a record that names its log only through a new tag would apply to the wrong key.

Apply checks the `unknownTaggedFields()` of each record and each nested struct against a registry of recognized extension tags. The registry is empty upstream, so any tag stops the replica. The upstream design describes the hook generically.

Inkless registers its tags in the registry and follows the convention in [CLASSIC_TO_DISKLESS_SWITCH.md](../../../docs/inkless/CLASSIC_TO_DISKLESS_SWITCH.md): tags at 100 and up, encoded by hand as `RawTaggedField` values. The schema JSON stays identical to upstream, so syncs don't conflict. A vanilla build can't read an Inkless coordinator log that carries extension tags. It stops instead of dropping the extension's state. That rule is deliberately stricter than the KRaft tags, which vanilla skips, because apply can't safely ignore a delta.

An Inkless-owned feature, for example `inkless.diskless.coordinator.version`, gates the Inkless tags:

- Level 0 means no extension tags, and each later level names the tags it adds. The leader writes a tag only when the finalized level includes it.
- Each level declares the `diskless.coordinator.version` level it needs, through `FeatureVersion.dependencies()`.
- The controller's support check guards the upgrade, and the downgrade branch refuses every downgrade of this feature too.
- The feature doesn't need to be finalized at format time.

Rejected alternatives:

- Letting unrecognized tags in the extension range pass, so that vanilla can read an Inkless log. That only works if every extension is safe to drop, and nothing enforces it.
- Inkless levels in a high range of the upstream feature. A supported range is contiguous, so a broker that supports level 100 claims every level below it.
- Gating on the Inkless release version. The controller has no way to check it.

### SQLite schema version

A snapshot is a copy of the SQLite file, and its layout is the implementer's choice, so it changes between binaries independently of the feature level. The schema carries its own version:

- Each database stores its schema version, for example in `PRAGMA user_version`. Each snapshot carries the version in its file name and in the fetch-snapshot response.
- A binary opens and migrates forward any older schema it supports.
- A binary that finds a newer schema than it knows treats the database or snapshot as missing, as v1 treats a corrupt one. It rebuilds from a snapshot in a schema it reads, or from offset 0 while the log start is 0.
- The fetch-snapshot request names the highest schema the receiver reads. A source that holds no covering snapshot at or below it answers `SNAPSHOT_NOT_FOUND`, and the receiver tries the next replica in the follower-first order.

An aborted upgrade therefore costs a rebuild on each replica that rolled back after migrating, but it never costs correctness. During a roll, an old-binary replica that needs a snapshot uses a peer that hasn't been upgraded yet. If none remains, the replica stays out of the ISR until it's upgraded.

Gating the schema on the feature level was rejected. A rollback before the raise would be free, but every binary would have to read and write two SQLite schemas, which is far heavier than two record versions.

### Failure mode and metric

The failure-mode table gets this row:

| Failure | Behavior |
| --- | --- |
| A replica meets a record or snapshot it can't read | It stops applying that coordinator partition and doesn't diverge. A leader resigns. With the write gate and the downgrade refusal, this happens only through a bug or a manual binary downgrade. The operator rolls the broker to a binary that supports the finalized levels, and the replica resumes from its applied offset. |

Metric: a per-broker gauge of coordinator partitions whose apply has stopped, for example `ApplyStoppedPartitionCount`. The error log names the partition, offset, record type, version, and tag.

### Tests

1. Apply stops, in the shared suite. An unknown type, an unknown value version, and an unknown tag at the top level and in a nested struct each leave the applied offset unchanged, raise the gauge, stop serving, and make a leader resign. A tag in a test extension registry applies normally.
2. Write gate. For each record type, the leader writes the version that the finalized level allows. A leader with a lagging `MetadataImage` writes the lower version. An Inkless tag appears only at an Inkless feature level that includes it.
3. Controller, in `FeatureControlManagerTest`. Both features refuse `SAFE_DOWNGRADE` and `UNSAFE_DOWNGRADE`. An upgrade fails while a registered broker lacks support. The Inkless feature's dependency on the upstream level holds.
4. SQLite schema. Forward migration from each supported older schema succeeds. A newer schema counts as a missing database and triggers a rebuild. A snapshot fetch above the receiver's schema gets `SNAPSHOT_NOT_FOUND`, and the receiver moves to the next replica.
5. Golden records. A checked-in file holds every record type at every level, and the current binary decodes all of it. This pins compatibility as v1 pins the partition mapping with fixed expected values.
6. Cluster upgrade. Brokers roll to the new binary, the level rises, and a commit goes through the coordinator. A second variant rolls back before the raise, and the rolled-back replicas rebuild and rejoin the ISR.

### Consequences for other tickets and the rewrite

- v1's [Rollout](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#rollout) gains the upgrade procedure, the downgrade refusal, and the Inkless feature. Its "feature flag name and metadata version" open item becomes the two feature names.
- v1's [Log records](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#log-records), spec line 160, and the partitionless key ranges entry in Future work say a second log key encoding is a tagged field on the `Log` struct. They become a new record version.
- [Read consistency after a leader change](07-read-consistency.md#leader-epochs-on-private-rpcs) says that adding a leader epoch to the private RPCs later is a tagged field. Upstream, it becomes a new request version.
- [Snapshots](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#snapshots) gains the schema version, the forward-only migration, and the schema bound on the fetch-snapshot request.
- `Feature.java` and `FeatureControlManager.java` become `INTERLEAVED` entries in `INKLESS_OWNERSHIP`.
- The consequences pass in the map's Not yet specified list takes the failure-mode row, the gauge, and the tests.
