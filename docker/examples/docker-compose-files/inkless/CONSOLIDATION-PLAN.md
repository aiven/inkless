# Consolidation Feature - Current State & Plan

## Status: Pipeline validated end-to-end; 2 production blockers identified

Date: 2026-06-02
Branch: `jeqo/ik-4.1` (synced with main)

---

## What works (validated end-to-end)

The full consolidation pipeline works for **pure diskless topics** created with `diskless.enable=true`:

```
produce → diskless (WAL in object storage)
       → consolidation fetcher → local log
       → segment roll → RLM copy → tiered storage (MinIO)
       → local retention deletes local segments
       → reads: tiered storage → local log → diskless
```

### Validated scenarios

| Scenario                              | Result   | Evidence                                   |
|---------------------------------------|----------|--------------------------------------------|
| Produce → immediate read (diskless)   | **PASS** | 500 msgs readable instantly                |
| Consolidation fetcher replicates      | **PASS** | Local log populated, segments roll         |
| RLM copies sealed segments            | **PASS** | `.log`, `.indexes`, `.rsm-manifest` in S3  |
| Read spanning tiered + local + diskless | **PASS** | 2700 msgs from beginning                  |
| Local retention deletes tiered copies | **PASS** | First segment deleted, still readable      |
| Offset seek into tiered range         | **PASS** | `--offset 500` returns correct data        |
| Broker restart preserves all data     | **PASS** | 2700 msgs after restart                    |
| Consumer groups (commit/resume)       | **PASS** | No duplicates on resume                    |
| PR #619: auto remote.storage.enable   | **PASS** | Set automatically on diskless topic create |

---

## Production blockers

### Blocker 1: ConsolidationFetcherThread dies on topic deletion (Gap 2)

**Severity**: Critical — permanently breaks consolidation for ALL topics on the broker.

**Reproduction**:
```
1. Create diskless topic → fetcher starts
2. Delete topic → UNKNOWN_TOPIC_OR_PARTITION
3. Fetcher thread dies permanently
4. New topics never get consolidated until broker restart
```

**Log evidence**:
```
WARN  Received UNKNOWN_TOPIC_OR_PARTITION from the leader for partition ephemeral-topic-0
ERROR [ConsolidationFetcherThread-0-1]: Error due to
      org.apache.kafka.common.errors.UnknownTopicOrPartitionException
INFO  [ConsolidationFetcherThread-0-1]: Stopped
```

**Root cause**: `ConsolidationFetcherThread` inherits from `AbstractFetcherThread` which treats `UnknownTopicOrPartitionException` as fatal during fetch. The partition is not evicted before the next fetch cycle.

**Fix**: Override error handling in `ConsolidationFetcherThread` to evict deleted partitions and continue, similar to how `ReplicaFetcherThread` handles this case for stopped replicas.

### Blocker 2: Classic→diskless switch stalls indefinitely (Gap 3)

**Severity**: Critical — topic is permanently stuck, produces fail with `REPLICA_NOT_AVAILABLE`.

**Reproduction**:
```
1. Create classic topic, produce data
2. kafka-configs --alter --add-config diskless.enable=true
3. Controller marks classicToDisklessStartOffset=-2 (PENDING)
4. Leader epoch NOT bumped → broker skips makeLeader
5. InitDisklessLog never triggers
6. Partition stuck in PENDING forever
```

**Log evidence**:
```
Skipped the become-leader state change for test-switched-consolidation-0
since it is already the leader with leader epoch 0.
```

**Root cause**: `ReplicationControlManager.markPartitionsForClassicToDisklessSwitch` bumps `partitionEpoch` but not `leaderEpoch`. The broker only re-evaluates leadership on leader epoch changes.

**Fix options**:
1. Controller bumps leaderEpoch when marking switch pending
2. Broker checks for pending seal state in `applyDelta` even when makeLeader is skipped

**Workaround**: Trigger preferred leader election after switch.

---

## Non-blocking issues

### WAL pruner fails when stuck partitions exist

The `ConsolidatedDisklessLogPruner` appears to fail the entire batch when any partition is in an error state (stuck switch partition). This prevents pruning of healthy consolidated partitions too.

**Evidence**:
```
WARN Got error during pruning consolidated diskless logs: broker is not the current leader
```

107 WAL objects remain unpruned despite segments being successfully tiered.

### Segment roll required for tiering

RLMCopyTask only copies sealed (inactive) segments. With default `segment.bytes=1048576`, small topics never get tiered. This is expected behavior but should be documented — production topics with reasonable throughput will roll naturally.

---

## Architecture (validated)

```
                                    ┌─────────────────────┐
                                    │  Object Storage     │
                                    │  (MinIO/S3)         │
                                    │                     │
                                    │  WAL objects        │──── diskless reads (offset >= local LEO)
                                    │  tiered-storage/    │──── tiered reads (offset < local start)
                                    └────────┬────────────┘
                                             │
                 ┌───────────────────────────┼─────────────────────────┐
                 │ Broker                    │                         │
                 │                           ▼                         │
                 │  ConsolidationFetcher ──► Local Log ──► RLMCopyTask │
                 │  (diskless → local)       (segments)   (local → TS) │
                 │                                                     │
                 │  ConsolidatedDisklessLogPruner                      │
                 │  (deletes WAL objects after tiering)                │
                 └─────────────────────────────────────────────────────┘
```

---

## Test results summary (2026-06-02)

| Test                                    | Result   |
|-----------------------------------------|----------|
| Pure diskless → consolidation → tiered  | **PASS** |
| Classic → diskless switch               | **FAIL** (Blocker 2) |
| Topic deletion kills fetcher            | **FAIL** (Blocker 1) |
| Read path (tiered + local + diskless)   | **PASS** |
| Consumer groups                         | **PASS** |
| Broker restart resilience               | **PASS** |
| WAL pruning                             | **PARTIAL** (fails with stuck partitions) |

---

## Next steps (prioritized)

### P0 — Production blockers

1. **Fix ConsolidationFetcherThread crash on topic deletion** (Blocker 1)
   - Location: `ConsolidationFetcherThread` error handling
   - Action: evict deleted partitions, don't kill the thread

2. **Fix classic→diskless switch stall** (Blocker 2)
   - Location: `ReplicationControlManager.markPartitionsForClassicToDisklessSwitch`
   - Action: bump leaderEpoch to trigger re-evaluation

### P1 — Reliability

3. **Fix WAL pruner batch failure** — isolate partition errors so healthy partitions still get pruned
4. **Add monitoring** — alert on dead ConsolidationFetcherThread, stalled switches

### P2 — Production readiness

5. **Load testing** — validate throughput under production traffic
6. **Document segment.bytes guidance** — ensure customers understand tiering latency

---

## How to reproduce

```bash
# Build local image (from repo root)
make docker_build

# Start cluster (from this directory)
KAFKA_VERSION=local docker compose -f docker-compose.yml -f docker-compose.s3-local.yml \
  -f docker-compose.consolidation.yml up -d

# Run tests
./test-consolidation.sh

# Manual validation of tiered storage
docker compose exec -T storage mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec -T storage mc ls --recursive local/inkless/tiered-storage/

# Trigger fetcher crash (topic deletion)
kafka-topics --create --topic ephemeral --config diskless.enable=true
kafka-topics --delete --topic ephemeral
# ConsolidationFetcherThread-0-N will die — check logs

# Verify fetcher death blocks new consolidation
kafka-topics --create --topic new-topic --config diskless.enable=true
# No local log will appear for new-topic on the affected broker
```
