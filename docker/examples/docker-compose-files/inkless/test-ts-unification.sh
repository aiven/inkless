#!/usr/bin/env bash
#
# E2E test for classic-to-diskless migration (tiered storage unification)
#
# Prerequisites:
#   make ts-unification KAFKA_VERSION=local  (running in another terminal)
#
# Scenarios tested:
#   1. Local classic data -> diskless (read from UnifiedLog then diskless)
#   2. Tiered -> local -> diskless (data in remote tier + local + diskless)
#   3. Only diskless after migration (retention cleans tiered/local)

set -euo pipefail

BOOTSTRAP="localhost:9092"
BROKER_CONTAINER="inkless-broker-1"
DOCKER="docker"

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
  echo "Producing $count messages to $topic..."
  # Generate input inside container to avoid pipe truncation with docker exec -T
  # Use small batch.size to avoid RecordBatchTooLargeException with small segment.bytes
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

kafka_get_offsets() {
  local topic="$1"
  kafka_cli /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server broker:19092 \
    --topic "$topic"
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
  echo "--- Topic config: $topic ---"
  kafka_topics --describe --topic "$topic"
  echo "--- Topic offsets: $topic ---"
  kafka_get_offsets "$topic"
}

enable_diskless() {
  local topic="$1"
  echo "Enabling diskless on topic $topic..."
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

# ============================================================
# MAIN
# ============================================================

wait_for_cluster

# ============================================================
# Scenario 1: Local classic data -> diskless
# Data produced while classic stays in UnifiedLog, then migration
# makes new data go to diskless. Read from beginning should work.
# ============================================================
separator "Scenario 1: Local classic -> diskless"

TOPIC1="test-local-to-diskless"
echo "Creating classic topic $TOPIC1 (RF=2, 1 partition)..."
kafka_topics --create --topic "$TOPIC1" --partitions 1 --replication-factor 2

echo "Producing 200 messages while topic is classic..."
kafka_produce "$TOPIC1" 200 "classic"

echo "Waiting for segments to flush..."
sleep 5

describe_topic "$TOPIC1"

echo "Enabling diskless on $TOPIC1..."
enable_diskless "$TOPIC1"

echo "Waiting for migration to complete..."
sleep 15

echo "Producing 200 more messages (now diskless)..."
kafka_produce "$TOPIC1" 200 "diskless"

sleep 5
describe_topic "$TOPIC1"

echo "Consuming from beginning (should read classic + diskless data)..."
CONSUMED=$(kafka_consume "$TOPIC1" true 15000 | wc -l)
echo "Consumed $CONSUMED messages (expected ~400)"

if [[ "$CONSUMED" -ge 390 ]]; then
  echo "PASS: Scenario 1 - read all data across classic->diskless boundary"
else
  echo "FAIL: Scenario 1 - expected ~400 messages, got $CONSUMED"
fi

# ============================================================
# Scenario 2: Tiered -> local -> diskless
# Produce enough data to trigger tiered storage upload, then
# migrate to diskless. Read from beginning should traverse:
#   remote tier -> local log -> diskless
# ============================================================
separator "Scenario 2: Tiered -> local -> diskless"

TOPIC2="test-tiered-to-diskless"
echo "Creating classic topic $TOPIC2 with small segments for faster tiering..."
kafka_topics --create --topic "$TOPIC2" --partitions 1 --replication-factor 2 \
  --config segment.bytes=102400 \
  --config local.retention.bytes=204800 \
  --config retention.bytes=1048576

echo "Producing 500 messages to trigger segment rolling + tiering..."
kafka_produce "$TOPIC2" 500 "tiered"

echo "Waiting for tiered storage upload (30s)..."
sleep 30

describe_topic "$TOPIC2"

echo "Producing 200 more messages (still classic, local log)..."
kafka_produce "$TOPIC2" 200 "local"

sleep 5
describe_topic "$TOPIC2"

echo "Enabling diskless on $TOPIC2..."
enable_diskless "$TOPIC2"

echo "Waiting for migration to complete..."
sleep 15

echo "Producing 200 more messages (now diskless)..."
kafka_produce "$TOPIC2" 200 "diskless"

sleep 5
describe_topic "$TOPIC2"

echo "Consuming from beginning (should read tiered + local + diskless data)..."
CONSUMED=$(kafka_consume "$TOPIC2" true 30000 | wc -l)
echo "Consumed $CONSUMED messages (expected ~900)"

if [[ "$CONSUMED" -ge 880 ]]; then
  echo "PASS: Scenario 2 - read all data across tiered->local->diskless boundary"
else
  echo "FAIL: Scenario 2 - expected ~900 messages, got $CONSUMED"
fi

# ============================================================
# Scenario 3: Only diskless after retention cleanup
# After migration, set aggressive retention so that all classic
# data (both tiered and local) gets cleaned up. Only diskless
# data should remain. Read from beginning should still work.
# ============================================================
separator "Scenario 3: Only diskless after retention cleanup"

TOPIC3="test-only-diskless"
echo "Creating classic topic $TOPIC3 with small segments..."
kafka_topics --create --topic "$TOPIC3" --partitions 1 --replication-factor 2 \
  --config segment.bytes=102400 \
  --config local.retention.bytes=204800 \
  --config retention.bytes=1048576

echo "Producing 500 messages to trigger tiering..."
kafka_produce "$TOPIC3" 500 "old"

echo "Waiting for tiered storage upload (30s)..."
sleep 30

describe_topic "$TOPIC3"

echo "Enabling diskless on $TOPIC3..."
enable_diskless "$TOPIC3"

echo "Waiting for migration to complete..."
sleep 15

echo "Producing 300 messages (diskless)..."
kafka_produce "$TOPIC3" 300 "new"

sleep 5

echo "Setting aggressive retention to clean classic data..."
kafka_configs --alter --entity-type topics --entity-name "$TOPIC3" \
  --add-config "retention.bytes=1024,local.retention.bytes=1024"

echo "Waiting for retention cleanup (60s)..."
sleep 60

describe_topic "$TOPIC3"

echo "Consuming from beginning (should read whatever remains)..."
CONSUMED=$(kafka_consume "$TOPIC3" true 15000 | wc -l)
echo "Consumed $CONSUMED messages"

echo "NOTE: After retention cleanup, only diskless data (300 msgs) should remain."
echo "      If classic data is cleaned, start offset should have advanced."

if [[ "$CONSUMED" -ge 290 ]]; then
  echo "PASS: Scenario 3 - data still readable after retention cleanup"
else
  echo "WARN: Scenario 3 - got $CONSUMED messages, classic data may have been cleaned"
fi

# ============================================================
# Summary
# ============================================================
separator "Summary"

for topic in "$TOPIC1" "$TOPIC2" "$TOPIC3"; do
  describe_topic "$topic"
  echo ""
done

echo "Done. Review the output above for any issues."
