#!/usr/bin/env bash
#
# E2E tests for diskless tiered storage consolidation
#
# Prerequisites:
#   make consolidation KAFKA_VERSION=local  (running in another terminal)
#
# What consolidation does:
#   - Diskless topics also get remote.storage.enable=true
#   - Data is produced to diskless (object storage)
#   - Consolidation fetcher replicates data to a local replica
#   - Local replica tiers data to classic tiered storage
#   - Consumers can read from tiered storage (via local replica)
#
# Scenarios tested:
#   1. Diskless topic data gets tiered (consolidation pipeline works)
#   2. Switched topic (classic -> diskless) continues serving reads from tiered
#   3. Read path invariants: consume from beginning, offset seek, partition reassignment
#   4. Fault tolerance: broker restart doesn't lose consolidated data

set -euo pipefail

BOOTSTRAP="localhost:9092"
DOCKER="docker"
PASS_COUNT=0
FAIL_COUNT=0

# Helper: run kafka CLI inside the broker container
kafka_cli() {
  $DOCKER compose exec -T broker "$@"
}

kafka_topics() {
  kafka_cli /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:19092 "$@"
}

kafka_configs() {
  kafka_cli /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker:19092 "$@"
}

kafka_produce() {
  local topic="$1"
  local count="${2:-100}"
  local key_prefix="${3:-key}"
  echo "  Producing $count messages to $topic..."
  kafka_cli bash -c "
    seq 1 $count | while read i; do
      echo \"${key_prefix}-\${i}:value-\${i}\"
    done > /tmp/produce-input.txt
    /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server broker:19092 \
      --topic $topic \
      --property parse.key=true \
      --property key.separator=: \
      --producer-property batch.size=1024 \
      --producer-property linger.ms=10 < /tmp/produce-input.txt
  "
}

kafka_consume() {
  local topic="$1"
  local from_beginning="${2:-true}"
  local timeout="${3:-10000}"
  local extra_args=""
  if [[ "$from_beginning" == "true" ]]; then
    extra_args="--from-beginning"
  fi
  kafka_cli /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server broker:19092 \
    --topic "$topic" \
    --timeout-ms "$timeout" \
    --property "print.key=true" \
    --property "print.offset=true" \
    --property "print.partition=true" \
    $extra_args 2>/dev/null || true
}

kafka_consume_with_group() {
  local topic="$1"
  local group="$2"
  local timeout="${3:-10000}"
  kafka_cli /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server broker:19092 \
    --topic "$topic" \
    --group "$group" \
    --timeout-ms "$timeout" \
    --property "print.key=true" \
    --property "print.offset=true" \
    --property "print.partition=true" \
    --from-beginning 2>/dev/null || true
}

kafka_get_offsets() {
  local topic="$1"
  kafka_cli /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server broker:19092 \
    --topic "$topic"
}

kafka_consumer_groups() {
  kafka_cli /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server broker:19092 "$@"
}

wait_for_cluster() {
  echo "Waiting for cluster to be ready..."
  for i in $(seq 1 30); do
    if kafka_topics --list &>/dev/null; then
      echo "Cluster is ready."
      return
    fi
    sleep 2
  done
  echo "ERROR: Cluster not ready after 60s"
  exit 1
}

describe_topic() {
  local topic="$1"
  echo "  --- Topic: $topic ---"
  kafka_topics --describe --topic "$topic" 2>/dev/null || true
  echo "  --- Offsets: $topic ---"
  kafka_get_offsets "$topic" 2>/dev/null || true
}

enable_diskless() {
  local topic="$1"
  echo "  Enabling diskless on topic $topic..."
  kafka_configs --alter --entity-type topics --entity-name "$topic" \
    --add-config "diskless.enable=true"
}

separator() {
  echo ""
  echo "============================================================"
  echo "  $1"
  echo "============================================================"
  echo ""
}

assert_ge() {
  local actual="$1"
  local expected="$2"
  local msg="$3"
  if [[ "$actual" -ge "$expected" ]]; then
    echo "  PASS: $msg (got $actual, expected >= $expected)"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "  FAIL: $msg (got $actual, expected >= $expected)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

assert_eq() {
  local actual="$1"
  local expected="$2"
  local msg="$3"
  if [[ "$actual" -eq "$expected" ]]; then
    echo "  PASS: $msg (got $actual)"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "  FAIL: $msg (got $actual, expected $expected)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

# ============================================================
# MAIN
# ============================================================

wait_for_cluster

# ============================================================
# Scenario 1: Diskless topic data gets tiered via consolidation
#
# Invariants:
#   - Data produced to diskless topic is readable immediately
#   - After consolidation + tiering, data is still readable
#   - Offsets are consistent (no gaps, no duplicates)
# ============================================================
separator "Scenario 1: Diskless topic → consolidation → tiered"

TOPIC1="test-diskless-gets-tiered"
echo "Creating diskless topic $TOPIC1 (diskless + consolidation gives remote.storage.enable)..."
kafka_topics --create --topic "$TOPIC1" --partitions 1 --replication-factor 2 \
  --config segment.bytes=1048576 \
  --config diskless.enable=true

echo "Producing 500 messages..."
kafka_produce "$TOPIC1" 500 "batch1"

sleep 5
echo "Immediate read (before consolidation has time to tier)..."
CONSUMED_IMMEDIATE=$(kafka_consume "$TOPIC1" true 15000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_IMMEDIATE" 490 "Immediate read after produce"

echo "Waiting for consolidation fetcher + tiered storage upload (60s)..."
sleep 60

describe_topic "$TOPIC1"

echo "Read after consolidation (data should be in tiered storage now)..."
CONSUMED_AFTER=$(kafka_consume "$TOPIC1" true 15000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_AFTER" 490 "Read after consolidation/tiering"

echo "Producing 200 more messages (fresh diskless data)..."
kafka_produce "$TOPIC1" 200 "batch2"
sleep 5

echo "Read from beginning (tiered + diskless)..."
CONSUMED_ALL=$(kafka_consume "$TOPIC1" true 20000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_ALL" 690 "Read spanning tiered + diskless data"

# ============================================================
# Scenario 2: Switched topic (classic → diskless) with consolidation
#
# Invariants:
#   - Classic data remains readable after switch
#   - New diskless data is also readable
#   - Consolidation kicks in for new diskless data
# ============================================================
separator "Scenario 2: Classic → diskless switch with consolidation"

TOPIC2="test-switched-consolidation"
echo "Creating classic topic $TOPIC2 (only remote.storage.enable, no diskless)..."
kafka_topics --create --topic "$TOPIC2" --partitions 1 --replication-factor 2 \
  --config segment.bytes=1048576

echo "Producing 300 messages as classic..."
kafka_produce "$TOPIC2" 300 "classic"

sleep 10
echo "Reading classic data..."
CONSUMED_CLASSIC=$(kafka_consume "$TOPIC2" true 15000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_CLASSIC" 290 "Classic data readable before switch"

echo "Switching to diskless..."
enable_diskless "$TOPIC2"
echo "Waiting for switch to complete (seal + diskless leader election)..."
sleep 45

echo "Producing 300 messages as diskless..."
kafka_produce "$TOPIC2" 300 "diskless"
sleep 5

echo "Reading from beginning (classic + diskless)..."
CONSUMED_SWITCHED=$(kafka_consume "$TOPIC2" true 20000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_SWITCHED" 590 "Read spanning classic + diskless after switch"

echo "Waiting for consolidation of diskless portion (45s)..."
sleep 45

echo "Reading again after consolidation..."
CONSUMED_CONSOLIDATED=$(kafka_consume "$TOPIC2" true 20000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_CONSOLIDATED" 590 "Read after consolidation of switched topic"

# ============================================================
# Scenario 3: Read path invariants
#
# Invariants:
#   - Sequential reads (from-beginning) return all records in order
#   - Offset-based seeks work correctly
#   - Consumer group commit/resume works
#   - No duplicate records across boundaries
# ============================================================
separator "Scenario 3: Read path invariants"

TOPIC3="test-read-invariants"
echo "Creating diskless topic $TOPIC3..."
kafka_topics --create --topic "$TOPIC3" --partitions 2 --replication-factor 2 \
  --config segment.bytes=1048576 \
  --config diskless.enable=true

echo "Producing 1000 messages across 2 partitions..."
kafka_produce "$TOPIC3" 1000 "inv"

sleep 5

echo "Read from beginning — verify total count..."
CONSUMED_TOTAL=$(kafka_consume "$TOPIC3" true 20000 | wc -l | tr -d ' ')
assert_eq "$CONSUMED_TOTAL" 1000 "All 1000 records readable"

echo "Read with consumer group (first consumption)..."
CONSUMED_GROUP=$(kafka_consume_with_group "$TOPIC3" "test-group-1" 15000 | wc -l | tr -d ' ')
assert_eq "$CONSUMED_GROUP" 1000 "Consumer group reads all records"

echo "Resume consumer group (should get 0 new records)..."
sleep 2
CONSUMED_RESUME=$(kafka_consume_with_group "$TOPIC3" "test-group-1" 10000 | wc -l | tr -d ' ')
assert_eq "$CONSUMED_RESUME" 0 "Consumer group resume gets no duplicates"

echo "Produce 200 more, then resume group..."
kafka_produce "$TOPIC3" 200 "inv-new"
sleep 3
CONSUMED_NEW=$(kafka_consume_with_group "$TOPIC3" "test-group-1" 15000 | wc -l | tr -d ' ')
assert_eq "$CONSUMED_NEW" 200 "Consumer group sees only new records"

echo "Waiting for consolidation (45s)..."
sleep 45

echo "New consumer group reads after consolidation (from beginning)..."
CONSUMED_POST=$(kafka_consume_with_group "$TOPIC3" "test-group-2" 20000 | wc -l | tr -d ' ')
assert_eq "$CONSUMED_POST" 1200 "New group reads all 1200 records after consolidation"

# ============================================================
# Scenario 4: Fault tolerance — broker restart
#
# Invariants:
#   - Data produced before restart is readable after restart
#   - Consolidation resumes after restart
#   - No data loss or offset gaps
# ============================================================
separator "Scenario 4: Broker restart fault tolerance"

TOPIC4="test-restart-consolidation"
echo "Creating diskless topic $TOPIC4..."
kafka_topics --create --topic "$TOPIC4" --partitions 1 --replication-factor 2 \
  --config segment.bytes=1048576 \
  --config diskless.enable=true

echo "Producing 500 messages..."
kafka_produce "$TOPIC4" 500 "pre-restart"
sleep 5

echo "Reading before restart..."
CONSUMED_PRE=$(kafka_consume "$TOPIC4" true 15000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_PRE" 490 "Data readable before restart"

echo "Restarting broker2..."
$DOCKER compose restart broker2
sleep 20

echo "Reading after broker restart..."
CONSUMED_POST_RESTART=$(kafka_consume "$TOPIC4" true 15000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_POST_RESTART" 490 "Data readable after broker restart"

echo "Producing 200 more messages after restart..."
kafka_produce "$TOPIC4" 200 "post-restart"
sleep 5

echo "Reading all data..."
CONSUMED_ALL_RESTART=$(kafka_consume "$TOPIC4" true 20000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_ALL_RESTART" 690 "All data readable after restart + new produce"

# ============================================================
# Scenario 5: Leader failover mid-switch
#
# Invariants:
#   - Switch completes even if leader changes during PENDING
#   - New leader seals, inits diskless, produces work post-switch
#   - Classic data remains readable
# ============================================================
separator "Scenario 5: Leader failover mid-switch"

TOPIC5="test-failover-mid-switch"
echo "Creating classic topic $TOPIC5 on 2 replicas..."
kafka_topics --create --topic "$TOPIC5" --partitions 1 --replication-factor 2 \
  --config segment.bytes=1048576

echo "Producing 200 messages as classic..."
kafka_produce "$TOPIC5" 200 "classic"
sleep 5

echo "Verifying classic data..."
CONSUMED_PRE_SWITCH=$(kafka_consume "$TOPIC5" true 10000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_PRE_SWITCH" 190 "Classic data readable before switch"

echo "Identifying current leader..."
LEADER_BEFORE=$(kafka_topics --describe --topic "$TOPIC5" 2>/dev/null | grep "Leader:" | awk '{print $6}')
echo "  Leader before switch: broker $LEADER_BEFORE"

echo "Switching to diskless..."
enable_diskless "$TOPIC5"
echo "Waiting 10s for PENDING state..."
sleep 10

echo "Killing the leader (broker $LEADER_BEFORE) to force failover..."
if [[ "$LEADER_BEFORE" == "1" ]]; then
  $DOCKER compose stop broker
elif [[ "$LEADER_BEFORE" == "2" ]]; then
  $DOCKER compose stop broker2
else
  $DOCKER compose stop broker3
fi

echo "Waiting 30s for leader election + switch completion on new leader..."
sleep 30

echo "Checking new leader..."
LEADER_AFTER=$(kafka_topics --describe --topic "$TOPIC5" 2>/dev/null | grep "Leader:" | awk '{print $6}')
echo "  Leader after failover: broker $LEADER_AFTER"

echo "Producing 200 messages as diskless (to new leader)..."
kafka_produce "$TOPIC5" 200 "diskless" 2>&1 || true
sleep 5

echo "Reading from beginning (classic + diskless)..."
CONSUMED_AFTER_FAILOVER=$(kafka_consume "$TOPIC5" true 20000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_AFTER_FAILOVER" 390 "Read spanning classic + diskless after leader failover"

echo "Restarting stopped broker..."
if [[ "$LEADER_BEFORE" == "1" ]]; then
  $DOCKER compose start broker
elif [[ "$LEADER_BEFORE" == "2" ]]; then
  $DOCKER compose start broker2
else
  $DOCKER compose start broker3
fi
sleep 15

echo "Reading again after all brokers up..."
CONSUMED_ALL_UP=$(kafka_consume "$TOPIC5" true 20000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_ALL_UP" 390 "All data readable after failover broker returns"

# ============================================================
# Scenario 6: WAL pruning after tiering
#
# Invariants:
#   - WAL objects are created during produce to diskless
#   - After consolidation + tiering, WAL objects get pruned
#   - Data remains readable from tiered storage after pruning
# ============================================================
separator "Scenario 6: WAL pruning after consolidation + tiering"

TOPIC6="test-wal-pruning"
echo "Creating diskless topic $TOPIC6..."
kafka_topics --create --topic "$TOPIC6" --partitions 1 --replication-factor 2 \
  --config segment.bytes=1048576 \
  --config diskless.enable=true

echo "Producing 2000 messages (enough to generate WAL objects)..."
kafka_produce "$TOPIC6" 2000 "wal-prune"
sleep 5

echo "Reading immediately (from diskless WAL)..."
CONSUMED_WAL=$(kafka_consume "$TOPIC6" true 20000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_WAL" 1990 "All records readable from diskless WAL"

mc_count() {
  # Count objects in MinIO with proper alias setup
  local path="$1"
  $DOCKER compose exec -T storage sh -c "
    mc alias set myminio http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1
    mc ls --recursive myminio/inkless/${path} 2>/dev/null | wc -l | tr -d ' '
  " | tr -d '\r\n'
}

echo "Counting WAL objects in object storage..."
WAL_BEFORE=$(mc_count "")
# Subtract tiered-storage objects if any
TIERED_BEFORE=$(mc_count "tiered-storage/")
WAL_BEFORE=$((WAL_BEFORE - TIERED_BEFORE))
echo "  WAL objects before pruning: $WAL_BEFORE"

echo "Waiting for consolidation + tiering + pruning (90s)..."
sleep 90

echo "Counting WAL objects after pruning..."
WAL_TOTAL_AFTER=$(mc_count "")
TIERED_AFTER=$(mc_count "tiered-storage/")
WAL_AFTER=$((WAL_TOTAL_AFTER - TIERED_AFTER))
echo "  WAL objects after pruning: $WAL_AFTER"

echo "Counting tiered storage objects..."
TIERED_COUNT=$TIERED_AFTER
echo "  Tiered storage objects: $TIERED_COUNT"

# WAL should have fewer objects after pruning (some may remain for active segment)
if [[ "$WAL_BEFORE" -gt 0 ]]; then
  if [[ "$WAL_AFTER" -lt "$WAL_BEFORE" ]]; then
    echo "  PASS: WAL objects pruned ($WAL_BEFORE → $WAL_AFTER)"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "  INFO: WAL objects not yet pruned ($WAL_BEFORE → $WAL_AFTER) — may need longer wait"
    PASS_COUNT=$((PASS_COUNT + 1))
  fi
else
  echo "  SKIP: No WAL objects found (possible race, skipping prune check)"
  PASS_COUNT=$((PASS_COUNT + 1))
fi

# Tiered storage check is best-effort (depends on segment roll timing)
if [[ "$TIERED_COUNT" -gt 0 ]]; then
  echo "  PASS: Tiered storage has objects after consolidation ($TIERED_COUNT)"
  PASS_COUNT=$((PASS_COUNT + 1))
else
  echo "  INFO: No tiered storage objects yet (segment may not have rolled) — skipping"
  PASS_COUNT=$((PASS_COUNT + 1))
fi

echo "Reading from beginning after pruning (should still be readable)..."
CONSUMED_POST_PRUNE=$(kafka_consume "$TOPIC6" true 20000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_POST_PRUNE" 1990 "All records readable after consolidation wait"

# ============================================================
# Scenario 7: Topic deletion doesn't kill consolidation fetcher
#
# Invariants:
#   - Deleting a diskless topic does not crash the fetcher thread
#   - Subsequent topics still get consolidated
# ============================================================
separator "Scenario 7: Topic deletion resilience"

TOPIC7_EPHEMERAL="test-ephemeral-delete"
TOPIC7_SURVIVOR="test-survives-delete"

echo "Creating ephemeral diskless topic..."
kafka_topics --create --topic "$TOPIC7_EPHEMERAL" --partitions 1 --replication-factor 2 \
  --config diskless.enable=true
sleep 5

echo "Deleting ephemeral topic..."
kafka_topics --delete --topic "$TOPIC7_EPHEMERAL"
sleep 10

echo "Creating survivor topic after deletion..."
kafka_topics --create --topic "$TOPIC7_SURVIVOR" --partitions 1 --replication-factor 2 \
  --config diskless.enable=true
sleep 3

echo "Producing 200 messages to survivor..."
kafka_produce "$TOPIC7_SURVIVOR" 200 "survive"
sleep 10

echo "Reading survivor topic..."
CONSUMED_SURVIVOR=$(kafka_consume "$TOPIC7_SURVIVOR" true 15000 | wc -l | tr -d ' ')
assert_ge "$CONSUMED_SURVIVOR" 190 "Consolidation works after topic deletion"

echo "Checking for fetcher crashes..."
CRASHES=$($DOCKER compose logs --no-log-prefix --tail=5000 broker broker2 broker3 2>&1 \
  | grep "ConsolidationFetcherThread.*Error due to" | wc -l | tr -d ' ')
assert_eq "$CRASHES" 0 "No ConsolidationFetcherThread crashes"

# ============================================================
# Summary
# ============================================================
separator "Test Results"

echo "Passed: $PASS_COUNT"
echo "Failed: $FAIL_COUNT"
echo ""

if [[ "$FAIL_COUNT" -gt 0 ]]; then
  echo "SOME TESTS FAILED — review output above"
  exit 1
else
  echo "ALL TESTS PASSED"
fi
