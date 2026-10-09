# Postgres remote log metadata manager

`PostgresRemoteLogMetadataManager` stores tiered-storage segment metadata in PostgreSQL
instead of the `__remote_log_metadata` topic. Use it in Inkless deployments that already
run the control-plane database and want one fewer Kafka-internal topic to operate.

## How it works

PostgreSQL is the source of truth. The manager doesn't keep broker-local metadata:
every read performs a round trip and every broker sees the same committed state. Mutations
run asynchronously on a dedicated thread pool. Each mutation commits in a
transaction that first upserts the partition row, which serializes concurrent writers per
partition through the row lock.

The tables in `inkless_rlmm` mirror `RemoteLogMetadataCache`:

- `rlmm_segments` stores one row per segment, including the original epoch map. Finished
  deletes leave tombstone rows so retried terminal updates complete as no-ops.
- `rlmm_segment_epochs` stores one row per segment and leader epoch. The `referenced`
  flag mirrors `offsetToId` membership: floor lookups only see referenced rows, while
  unreferenced rows stay listable for cleanup enumeration.
- `rlmm_epoch_state` stores the durable per-epoch high watermark, which never decreases.
- `rlmm_partitions` stores the partition delete state. A finished partition delete clears
  that partition's segments and epoch state, and further reads report not found.

The SQL model follows `RemoteLogMetadataCache` for overlap resolution, read-hole behavior,
valid segment transitions, and watermark handling. The differential suite replays scripted
and randomized operation streams against that cache. It compares catalog membership and
epoch watermarks after each randomized step, then compares every read method at scenario
boundaries.

## Configuration

Set the manager class and pass connection settings with the `rlmm.config.` prefix:

```properties
remote.log.metadata.manager.class.name=io.aiven.inkless.remote_log_metadata.postgres.PostgresRemoteLogMetadataManager
rlmm.config.connection.string=jdbc:postgresql://postgres:5432/inkless
rlmm.config.username=inkless
rlmm.config.password=secret
rlmm.config.max.connections=10
rlmm.config.mutation.threads=4
```

The broker always supplies `cluster.id`. Configuration fails broker startup when the
database is unreachable or migrations cannot apply, so a misconfigured broker never
serves reads from an empty store. Reads and mutations share the connection pool. Size
`max.connections` for the configured mutation threads and expected concurrent readers.
If `mutation.threads` exceeds `max.connections`, the manager logs a warning, and excess
operations wait up to `connection.pool.timeout.ms` for a connection.

Migrations run automatically on startup through an isolated Flyway stream
(`db/rlmm/migration`, schema `inkless_rlmm`, history table `flyway_schema_history_rlmm`),
separate from the control-plane stream even when both share one database server.

## Metrics

The manager exposes metrics through `Monitorable`. The broker registers its sensors under the
plugin group. Each operation exposes `AvgMs`, `MaxMs`, and `Count`:

- `AddSegment`, `UpdateSegment`, `PutPartitionDelete`
- `LookupSegment`, `NextTxnSegment`, `HighestOffset`, `ListSegments`, `RemoteLogSize`

Gauges expose `TransitionConflicts` (mutations dropped as invalid state transitions),
`OperationErrors` (unexpected failures, excluding not-found reads), `ActivePartitions`,
and pool stats (`PoolActiveConnections`, `PoolIdleConnections`, `PoolTotalConnections`,
`PoolPendingThreads`).

## Semantics notes

- Reads for a partition with no rows return empty results. Reads for a partition whose
  deletion finished throw `RemoteResourceNotFoundException`.
- Updates for unknown segments throw `RemoteResourceNotFoundException`. Invalid state
  transitions complete successfully without changing state, which keeps concurrent
  deleters and stale retries quiet.
- The manager doesn't age out terminal segment tombstones. They remain until partition
  deletion, so operators must monitor the `inkless_rlmm` schema's growth.
- `isReady` returns true for partitions the broker reported through leadership
  notifications. It resets on restart until the broker reports leadership again.

### Intentional differences

- Reads for an unknown partition return empty results or zero. The topic-based store can
  report not found until it initializes that partition's cache.
- A retried terminal segment delete completes as a no-op because PostgreSQL retains the
  tombstone. Stale updates after that terminal state also complete without changing data.
  The in-memory cache forgets the segment and logs it as missing.
- Re-adding an identical `COPY_SEGMENT_STARTED` event completes as a no-op. Reusing the
  segment ID with different metadata throws `IllegalArgumentException` instead of
  overwriting the first event.
- The manager validates partition-delete transitions and drops invalid transitions. The
  topic-based store records the latest partition-delete event without transition validation.

## Tests

The suites live in `io.aiven.inkless.remote_log_metadata.postgres` and use Testcontainers:

- `PostgresRemoteLogMetadataManagerTest`: contract, state machine, idempotency, and
  partition deletion.
- `PostgresRlmmDifferentialTest`: scripted and randomized replay against
  `RemoteLogMetadataCache`.
- `PostgresRlmmConcurrencyTest`: two managers racing on one database.
- `PostgresRlmmRestartTest`: durability across manager restarts.
- `PostgresRlmmMetricsTest` and `PostgresRlmmCoverageTest`: metric wiring and the
  consolidation coverage inputs.
- `InklessConsolidatedDisklessTopicsTest`: broker-level consolidation, remote copy,
  pruning, and reads with the PostgreSQL manager configured as the RLMM.
