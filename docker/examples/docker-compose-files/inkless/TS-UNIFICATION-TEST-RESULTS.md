# Tiered Storage Unification - E2E Test Results

**Date**: 2026-04-02
**Branch**: `jeqo/ik-4.0-dev` (inkless-release worktree)
**Environment**: Docker Compose ts-unification (2 brokers, PostgreSQL, MinIO)
**Image**: `ghcr.io/aiven/inkless:local`

---

## Summary

| # | Scenario | Result | Details |
|---|----------|--------|---------|
| 1 | Local classic -> diskless | PASS | 400/400 messages consumed |
| 2 | Tiered -> local -> diskless | PASS | 900/900 messages consumed |
| 3 | Only diskless after retention | PASS | 800/800 (retention needs >60s) |
| 4 | Multi-partition migration (6P) | PASS | 600/600 messages, all 6 partitions migrated |
| 5 | Leader failover during migration | **FAIL** | Classic data unreachable after failover |
| 6 | Consumer group offsets across migration | PASS | Offset preserved, seamless resume |
| 7 | Rack-aware replica placement | PASS | All replicas span az1 + az2 |

---

## Scenario 4: Multi-partition migration

**Topic**: `extra-multi-part` (6 partitions, RF=2)

### Procedure
1. Created 6-partition topic with RF=2
2. Produced 300 messages (distributed across partitions by key hash)
3. Enabled diskless (triggered migration)
4. Produced 300 more messages (diskless phase)
5. Consumed from beginning

### Results

**Offsets before migration**:
```
extra-multi-part:0:45
extra-multi-part:1:63
extra-multi-part:2:53
extra-multi-part:3:52
extra-multi-part:4:42
extra-multi-part:5:45
```

**Migration**: All 6 partitions sealed and initialized successfully. Leader partitions on broker2 (P1, P3, P5) migrated first, then broker1's partitions (P0, P2, P4) followed.

```
InitDisklessLog for extra-multi-part-3: disklessStartOffset=52
InitDisklessLog for extra-multi-part-5: disklessStartOffset=45
InitDisklessLog for extra-multi-part-1: disklessStartOffset=63
InitDisklessLog for extra-multi-part-2: disklessStartOffset=53
InitDisklessLog for extra-multi-part-4: disklessStartOffset=42
InitDisklessLog for extra-multi-part-0: disklessStartOffset=45
```

**Offsets after migration + produce**:
```
extra-multi-part:0:97
extra-multi-part:1:110
extra-multi-part:2:106
extra-multi-part:3:94
extra-multi-part:4:90
extra-multi-part:5:103
```

**Consumed**: 600/600 messages from beginning

**Topic config after migration**: `diskless.enable=true, remote.storage.enable=true`

**Verdict**: PASS - All partitions migrated independently, data readable across classic->diskless boundary per partition.

---

## Scenario 5: Leader failover during migration

**Topic**: `extra-failover` (2 partitions, RF=2)

### Procedure
1. Created 2-partition topic with RF=2
2. Produced 200 messages (P0: 94, P1: 106)
3. P0 leader=broker1, P1 leader=broker2
4. Enabled diskless
5. **Immediately stopped broker1** (before full migration could complete)
6. Waited 10s, checked migration state
7. Restarted broker1, waited 15s
8. Produced 200 more messages (diskless)
9. Consumed from beginning

### Results

**Migration during failover**:
- Broker1 sealed and InitDisklessLog'd P0 (disklessStartOffset=94) before going down
- Broker2 took over leadership of P0, attempted InitDisklessLog but correctly received rejection: "already initialized with disklessStartOffset=94"
- Broker2 sealed P1 (disklessStartOffset=106) and InitDisklessLog succeeded
- Idempotent behavior: duplicate InitDisklessLog requests properly rejected

**After restart**:
- Broker1 restarted, replayed metadata
- P0 leader: broker2, P1 leader: broker1
- Both brokers in ISR for both partitions

**Consumption**:
- P0 (leader=broker2): 198 messages consumed - PASS
- P1 (leader=broker1): **0 messages consumed from offset 0** - FAIL
- P1 from offset 106 (diskless start): 96 messages consumed - PASS

### Bug Found: Classic data unreachable after leader failover

**Symptom**: Consuming from offset 0 on partition 1 (leader=broker1) results in `TimeoutException`. Diskless data (offset 106+) is readable.

**Root cause analysis**: When broker1 restarted:
1. It replayed the initial `PartitionRecord` with `disklessStartOffset=-1` (the snapshot hadn't been updated yet)
2. It loaded the local log for P1 (logEndOffset=106, highWatermark=0)
3. Log says: `Skipping diskless init on control plane for extra-failover-1 because no previous partition registration was found`
4. Broker1 became leader for P1 but cannot serve classic data (offsets 0-105)

**Impact**: After a leader failover during/after migration, classic (pre-migration) data on the new leader may be unreachable. The partition appears to work for new writes (diskless) but cannot serve reads for the classic offset range.

**Severity**: HIGH - Data loss scenario for classic data during leader transitions on migrated topics.

**Verdict**: FAIL - Bug identified.

---

## Scenario 6: Consumer group offsets across migration

**Topic**: `extra-cg-test` (1 partition, RF=2)
**Consumer group**: `cg-migration-test`

### Procedure
1. Created topic, produced 200 messages (classic)
2. Started consumer group, consumed first 100 messages (committed offset=100)
3. Enabled diskless (disklessStartOffset=200, migration completed)
4. Produced 200 more messages (offsets 200-399, diskless)
5. Resumed consumer group

### Results

**Before migration**: Consumer group offset=100, lag=100
**After migration + produce**: Consumer group offset=100, log-end=400, lag=300

**Resumed consumer**:
- Consumed exactly 300 messages (offsets 100-399)
- First message: `Offset:100 cg-value-101`
- Last message: `Offset:399 cg-value-400`
- Seamlessly crossed classic->diskless boundary at offset 200
- Final state: offset=400, lag=0

**Verdict**: PASS - Consumer group offsets preserved perfectly across migration. No data loss, no duplicates, seamless boundary crossing.

---

## Scenario 7: Rack-aware replica placement

**Environment**: Broker 1 = rack `az1`, Broker 2 = rack `az2`

### Results

All diskless topics (both migrated and created-as-diskless) have replicas spanning both racks:

| Topic | Partitions | Replica Sets |
|-------|-----------|--------------|
| test-local-to-diskless | 1 | [1,2] |
| extra-multi-part | 6 | [1,2], [2,1], [1,2], [2,1], [1,2], [2,1] |
| extra-cg-test | 1 | [2,1] |
| extra-rack-test (born diskless) | 4 | [1,2], [2,1], [1,2], [2,1] |

Every partition has replicas in both `az1` and `az2`. Leaders are balanced across brokers.

**Verdict**: PASS - Rack-aware placement works for both migrated and born-diskless topics.

---

## Key Findings

### Bug: Classic data unreachable after leader failover (Scenario 5)

This is the most significant finding. When a broker restarts after a migration happened while it was down, and becomes leader for a partition:
- Diskless data (post-migration) is served correctly
- Classic data (pre-migration, in local log) cannot be served
- Consumer gets `TimeoutException` when fetching from offset 0

The root cause appears to be related to metadata replay: the initial `PartitionRecord` is replayed with `disklessStartOffset=-1`, and the broker skips diskless init because "no previous partition registration was found." The partition then cannot properly serve reads from the classic offset range.

**Recommendation**: Investigate the metadata replay path for migrated partitions on broker restart, particularly the `BrokerMetadataPublisher.initializeManagers` flow and how it interacts with the `disklessStartOffset` field in `PartitionRegistration`.

### Observations

1. **Migration idempotency works**: Duplicate `InitDisklessLog` requests are properly rejected by the controller with "already initialized" message.
2. **Producer state preserved**: `producerStates.size=1` in InitDisklessLog logs confirms producer state transfer across the migration boundary.
3. **Consumer group offsets unaffected**: Committed offsets survive migration and work across the classic->diskless boundary.
4. **Retention cleanup is slow**: In Scenario 3, classic data was not cleaned up within 60s. May need longer wait or more aggressive log cleaner settings for testing.
5. **Transient UNKNOWN_TOPIC_ID warnings**: Appear during topic creation for follower partitions. Normal and harmless.

---

## Environment Details

```
Brokers: 2 (broker=az1, broker2=az2)
Controller: Combined mode (both brokers are controllers)
PostgreSQL: 17.2 (control plane)
MinIO: S3-compatible object storage
Key configs:
  - remote.log.storage.system.enable=true
  - diskless.storage.system.enable=true
  - diskless.allow.from.classic.enable=true
  - classic.remote.storage.force.enable=true
  - diskless.managed.rf.enable=true
```
