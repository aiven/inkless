# Tiered Storage Unification — E2E Testing Guide

This guide walks through testing classic-to-diskless migration scenarios
using the local Docker Compose environment.

## Prerequisites

```bash
# Build local Docker image from the current branch
make docker_build

# Start the ts-unification environment
cd docker/examples/docker-compose-files/inkless
make ts-unification KAFKA_VERSION=local
```

This starts: 2 brokers (rack-aware), PostgreSQL (control plane), MinIO (object storage).

Key broker configs enabled:
- `remote.log.storage.system.enable=true` — classic tiered storage via Aiven TS plugin
- `diskless.storage.system.enable=true` — diskless (Inkless)
- `diskless.allow.from.classic.enable=true` — migration bridge
- `classic.remote.storage.force.enable=true` — all topics get `remote.storage.enable=true`
- `diskless.managed.rf.enable=true` — managed replicas

## Helper Commands

```bash
# Aliases for convenience (run from the inkless/ docker compose dir)
EXEC="docker compose exec -T broker"
KAFKA="$EXEC /opt/kafka/bin"

# Create a classic topic
$KAFKA/kafka-topics.sh --bootstrap-server broker:19092 \
  --create --topic my-topic --partitions 1 --replication-factor 2

# Describe topic (shows config including diskless.enable, remote.storage.enable)
$KAFKA/kafka-topics.sh --bootstrap-server broker:19092 --describe --topic my-topic

# Check offsets
$KAFKA/kafka-get-offsets.sh --bootstrap-server broker:19092 --topic my-topic

# Produce messages (small batches to avoid RecordBatchTooLargeException with small segments)
$EXEC bash -c '
  seq 1 100 | while read i; do echo "key-${i}:value-${i}"; done > /tmp/input.txt
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server broker:19092 --topic my-topic \
    --property parse.key=true --property key.separator=: \
    --producer-property batch.size=1024 < /tmp/input.txt
'

# Consume from beginning
$KAFKA/kafka-console-consumer.sh --bootstrap-server broker:19092 \
  --topic my-topic --from-beginning --timeout-ms 10000 \
  --property print.offset=true --property print.partition=true

# Enable diskless on a topic (triggers migration)
$KAFKA/kafka-configs.sh --bootstrap-server broker:19092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config "diskless.enable=true"

# Check broker logs for migration events
docker compose logs broker 2>/dev/null | grep -i 'my-topic' | grep -i 'seal\|init\|diskless'
```

## Automated Test Script

A script covering the three main scenarios is available:

```bash
bash test-ts-unification.sh
```

---

## Scenario 1: Local Classic Data → Diskless

**Goal**: Verify that data produced while the topic is classic (stored in local
UnifiedLog) remains readable after migrating to diskless, and new data lands in
diskless storage.

### Steps

1. **Create a classic topic** (RF=2, default 1MB segments):

```bash
$KAFKA/kafka-topics.sh --bootstrap-server broker:19092 \
  --create --topic s1-local-to-diskless --partitions 1 --replication-factor 2
```

2. **Verify config** — should show `remote.storage.enable=true, diskless.enable=false`:

```bash
$KAFKA/kafka-topics.sh --bootstrap-server broker:19092 --describe --topic s1-local-to-diskless
```

3. **Produce 200 messages** (classic phase):

```bash
$EXEC bash -c '
  seq 1 200 | while read i; do echo "classic-${i}:value-${i}"; done > /tmp/input.txt
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker:19092 \
    --topic s1-local-to-diskless --property parse.key=true --property key.separator=: \
    --producer-property batch.size=1024 < /tmp/input.txt
'
```

4. **Verify offset = 200**:

```bash
$KAFKA/kafka-get-offsets.sh --bootstrap-server broker:19092 --topic s1-local-to-diskless
# Expected: s1-local-to-diskless:0:200
```

5. **Enable diskless** (triggers migration):

```bash
$KAFKA/kafka-configs.sh --bootstrap-server broker:19092 \
  --alter --entity-type topics --entity-name s1-local-to-diskless \
  --add-config "diskless.enable=true"
```

6. **Watch migration in broker logs**:

```bash
docker compose logs --tail=50 broker 2>/dev/null | grep 's1-local-to-diskless'
```

   Expected log sequence:
   - `Topic s1-local-to-diskless transitioning from classic to diskless, sealing leader partitions`
   - `Sealed partition s1-local-to-diskless-0 for diskless migration with LEO 200`
   - `InitDisklessLog for s1-local-to-diskless-0: disklessStartOffset=200`
   - `InitDisklessLog succeeded for partition s1-local-to-diskless-0`

7. **Wait ~15s**, then produce 200 more messages (diskless phase):

```bash
$EXEC bash -c '
  seq 1 200 | while read i; do echo "diskless-${i}:value-${i}"; done > /tmp/input.txt
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker:19092 \
    --topic s1-local-to-diskless --property parse.key=true --property key.separator=: \
    --producer-property batch.size=1024 < /tmp/input.txt
'
```

8. **Verify offset = 400**:

```bash
$KAFKA/kafka-get-offsets.sh --bootstrap-server broker:19092 --topic s1-local-to-diskless
# Expected: s1-local-to-diskless:0:400
```

9. **Consume from beginning** — should read all 400 messages:

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server broker:19092 \
  --topic s1-local-to-diskless --from-beginning --timeout-ms 15000 \
  --property print.offset=true | wc -l
# Expected: 400
```

### What to verify

- [ ] `disklessStartOffset=200` in controller log (offset where diskless begins)
- [ ] Offsets 0-199 served from UnifiedLog (classic data)
- [ ] Offsets 200-399 served from diskless storage
- [ ] Consumer reads seamlessly across the boundary
- [ ] No errors in broker logs

---

## Scenario 2: Tiered → Local → Diskless

**Goal**: Verify that data spanning all three storage layers (remote tier, local
log, diskless) is readable end-to-end after migration.

### Steps

1. **Create topic with small segments** (to trigger tiering quickly):

```bash
$KAFKA/kafka-topics.sh --bootstrap-server broker:19092 \
  --create --topic s2-tiered-to-diskless --partitions 1 --replication-factor 2 \
  --config segment.bytes=102400 \
  --config local.retention.bytes=204800 \
  --config retention.bytes=1048576
```

2. **Produce 500 messages** (triggers segment rolling → tiered storage upload):

```bash
$EXEC bash -c '
  seq 1 500 | while read i; do echo "tiered-${i}:value-${i}"; done > /tmp/input.txt
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker:19092 \
    --topic s2-tiered-to-diskless --property parse.key=true --property key.separator=: \
    --producer-property batch.size=1024 < /tmp/input.txt
'
```

3. **Wait 30s** for tiered storage upload (RLM copy task runs every 5s):

```bash
sleep 30
```

4. **Verify tiering** — check broker logs for copy events:

```bash
docker compose logs broker 2>/dev/null | grep 's2-tiered-to-diskless' | grep -i 'copied\|remote'
```

5. **Produce 200 more messages** (still classic, in local log):

```bash
$EXEC bash -c '
  seq 1 200 | while read i; do echo "local-${i}:value-${i}"; done > /tmp/input.txt
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker:19092 \
    --topic s2-tiered-to-diskless --property parse.key=true --property key.separator=: \
    --producer-property batch.size=1024 < /tmp/input.txt
'
```

6. **Verify offset = 700**:

```bash
$KAFKA/kafka-get-offsets.sh --bootstrap-server broker:19092 --topic s2-tiered-to-diskless
# Expected: s2-tiered-to-diskless:0:700
```

7. **Enable diskless**:

```bash
$KAFKA/kafka-configs.sh --bootstrap-server broker:19092 \
  --alter --entity-type topics --entity-name s2-tiered-to-diskless \
  --add-config "diskless.enable=true"
```

8. **Wait ~15s**, produce 200 more (diskless):

```bash
sleep 15
$EXEC bash -c '
  seq 1 200 | while read i; do echo "diskless-${i}:value-${i}"; done > /tmp/input.txt
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker:19092 \
    --topic s2-tiered-to-diskless --property parse.key=true --property key.separator=: \
    --producer-property batch.size=1024 < /tmp/input.txt
'
```

9. **Consume from beginning** — should read all 900 messages:

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server broker:19092 \
  --topic s2-tiered-to-diskless --from-beginning --timeout-ms 30000 \
  --property print.offset=true | wc -l
# Expected: 900
```

### What to verify

- [ ] `disklessStartOffset=700` in controller log
- [ ] Early offsets served from remote tier (Aiven TS plugin → MinIO)
- [ ] Middle offsets served from local UnifiedLog
- [ ] Late offsets served from diskless (Inkless → MinIO)
- [ ] Consumer reads seamlessly across all three layers
- [ ] Check MinIO console (http://localhost:9001, minioadmin/minioadmin):
  - `tiered-storage/` prefix contains classic tiered segments
  - Root level contains inkless objects

---

## Scenario 3: Only Diskless After Retention Cleanup

**Goal**: After migration, set aggressive retention so classic data (both tiered
and local) is cleaned up. Verify that diskless data remains readable and the
topic's start offset advances.

### Steps

1. **Create topic, produce, migrate** (same as Scenario 2 steps 1-8)

2. **Set aggressive retention** to clean classic data:

```bash
$KAFKA/kafka-configs.sh --bootstrap-server broker:19092 \
  --alter --entity-type topics --entity-name s3-only-diskless \
  --add-config "retention.bytes=1024,local.retention.bytes=1024"
```

3. **Wait 60-120s** for retention cleanup (log cleaner + RLM expiration):

```bash
sleep 120
```

4. **Check offsets** — start offset should have advanced past classic data:

```bash
$KAFKA/kafka-get-offsets.sh --bootstrap-server broker:19092 --topic s3-only-diskless
```

5. **Consume from beginning** — should only get diskless messages:

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server broker:19092 \
  --topic s3-only-diskless --from-beginning --timeout-ms 15000 \
  --property print.offset=true
```

### What to verify

- [ ] Start offset advanced (classic data deleted by retention)
- [ ] Diskless messages remain readable
- [ ] No consumer errors when classic data is gone
- [ ] Tiered segments removed from MinIO `tiered-storage/` prefix

---

## Additional Test Scenarios

### Multi-partition migration

```bash
$KAFKA/kafka-topics.sh --bootstrap-server broker:19092 \
  --create --topic multi-part --partitions 6 --replication-factor 2
# Produce, then enable diskless — verify all partitions migrate
```

### Leader failover during migration

```bash
# Start migration, then stop the leader broker mid-migration
docker compose stop broker
sleep 10
docker compose start broker
# Verify migration completes after recovery
```

### Consumer group offsets across migration

```bash
# Start a consumer group, consume partial data, then migrate
# Verify the consumer group resumes correctly after migration
$KAFKA/kafka-console-consumer.sh --bootstrap-server broker:19092 \
  --topic my-topic --group test-group --max-messages 100
# Enable diskless, produce more
# Resume consumer — should continue from committed offset
```

### Idempotent producer across migration

```bash
# Verify that producer state (sequence numbers) is preserved
# across the classic→diskless boundary via InitDisklessLog's
# producerStates field
```

### Rack-aware replica placement

```bash
# Verify that after migration, managed replicas are placed
# across racks (az1, az2) as configured
$KAFKA/kafka-topics.sh --bootstrap-server broker:19092 --describe --topic my-topic
# Check Replicas field shows brokers from different racks
```

---

## Observability

### Broker logs to watch

```bash
# Migration events
docker compose logs broker 2>/dev/null | grep -E 'seal|InitDiskless|disklessStartOffset|transitioning'

# Errors
docker compose logs broker 2>/dev/null | grep -iE 'ERROR|WARN' | grep -v 'metadata.*recoverable'

# FetchOffsetHandler errors (fixed in #554)
docker compose logs broker 2>/dev/null | grep 'Error fetching offset for leader epoch'
```

### MinIO console

Browse http://localhost:9001 (minioadmin/minioadmin) to inspect:
- `inkless` bucket root: Inkless objects
- `inkless/tiered-storage/`: Classic tiered storage segments

### JMX metrics

```bash
# SealedPartitionsCount — number of partitions sealed for migration
# This is a one-way counter; sealed partitions stay sealed
```

---

## Teardown

```bash
docker compose down --remove-orphans -v
```
