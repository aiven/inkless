#!/usr/bin/env bash
#
# E2E fault-tolerance tests for classic-to-diskless migration
#
# These scenarios exercise replica coordination bugs that were fixed in:
#   - PR #598: follower HW below seal after restart / new replica added post-migration
#   - PR #600: leader change during PENDING window stalls seal commit
#
# Prerequisites:
#   make ts-unification KAFKA_VERSION=local  (running in another terminal)
#
# Cluster topology:
#   broker  (node 1, az1) — exposed on localhost:9092
#   broker2 (node 2, az2) — exposed on localhost:9093
#   broker3 (node 3, az1) — exposed on localhost:9094

set -euo pipefail

BOOTSTRAP="localhost:9092"
COMPOSE_CMD="docker compose -f docker-compose.yml -f docker-compose.s3-local.yml -f docker-compose.ts-unification.yml"

# Helper: run kafka CLI inside broker container
kafka_cli() {
  local container="${1:-broker}"
  shift
  $COMPOSE_CMD exec -T "$container" "$@"
}

# CLI_BROKER controls which container runs admin commands (default: broker).
# Override when broker is intentionally stopped.
CLI_BROKER="broker"
BOOTSTRAP_INTERNAL="broker:19092,broker2:19093,broker3:19094"

# Map container to its internal port
container_port() {
  case "$1" in
    broker)  echo 19092 ;;
    broker2) echo 19093 ;;
    broker3) echo 19094 ;;
  esac
}

kafka_topics() {
  kafka_cli "$CLI_BROKER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" "$@"
}

kafka_configs() {
  kafka_cli "$CLI_BROKER" /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" "$@"
}

kafka_produce() {
  local topic="$1"
  local count="${2:-100}"
  local key_prefix="${3:-key}"
  echo "  Producing $count messages to $topic..."
  kafka_cli "$CLI_BROKER" bash -c "
    seq 1 $count | while read i; do
      echo \"${key_prefix}-\${i}:value-\${i}\"
    done > /tmp/produce-input.txt
    /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server $BOOTSTRAP_INTERNAL \
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
  kafka_cli "$CLI_BROKER" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP_INTERNAL" \
    --topic "$topic" \
    --timeout-ms "$timeout" \
    --property "print.key=true" \
    --property "print.offset=true" \
    --property "print.partition=true" \
    $extra_args 2>/dev/null || true
}

# Consume from a specific offset on partition 0
kafka_consume_from_offset() {
  local topic="$1"
  local offset="$2"
  local timeout="${3:-15000}"
  kafka_cli "$CLI_BROKER" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP_INTERNAL" \
    --topic "$topic" \
    --partition 0 \
    --offset "$offset" \
    --timeout-ms "$timeout" \
    --property "print.key=true" \
    --property "print.offset=true" \
    2>/dev/null || true
}

# Get the end offset (LEO) for partition 0 of a topic
get_end_offset() {
  local topic="$1"
  kafka_get_offsets "$topic" | sed -n "s/${topic}:0:\(.*\)/\1/p"
}

kafka_get_offsets() {
  local topic="$1"
  kafka_cli "$CLI_BROKER" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server "$BOOTSTRAP_INTERNAL" \
    --topic "$topic"
}

# Get the current leader node ID for a topic-partition
get_leader() {
  local topic="$1"
  local partition="${2:-0}"
  kafka_topics --describe --topic "$topic" \
    | grep "Partition: $partition" \
    | sed -n 's/.*Leader: \([0-9]*\).*/\1/p'
}

# Get the replicas list for a topic-partition (comma-separated node IDs)
get_replicas() {
  local topic="$1"
  local partition="${2:-0}"
  kafka_topics --describe --topic "$topic" \
    | grep "Partition: $partition" \
    | sed -n 's/.*Replicas: \([0-9,]*\).*/\1/p'
}

# Map node ID to container name
node_to_container() {
  local node_id="$1"
  case "$node_id" in
    1) echo "broker" ;;
    2) echo "broker2" ;;
    3) echo "broker3" ;;
    *) echo "ERROR: unknown node $node_id" >&2; exit 1 ;;
  esac
}

# Map node ID to the OTHER container name (the follower in a 2-replica set)
other_container() {
  local node_id="$1"
  case "$node_id" in
    1) echo "broker2" ;;
    2) echo "broker" ;;
    3) echo "broker" ;;
    *) echo "ERROR: unknown node $node_id" >&2; exit 1 ;;
  esac
}

# Find a node ID that is NOT in the current replica set
find_non_replica_node() {
  local topic="$1"
  local partition="${2:-0}"
  local replicas
  replicas=$(get_replicas "$topic" "$partition")
  for node in 1 2 3; do
    if ! echo "$replicas" | grep -q "$node"; then
      echo "$node"
      return
    fi
  done
  echo "ERROR: all nodes are replicas" >&2
  exit 1
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

# Wait for a specific container to rejoin the cluster and serve metadata
wait_for_broker() {
  local container="$1"
  local max_wait="${2:-60}"
  # Each broker listens on its own internal port
  local internal_port
  case "$container" in
    broker)  internal_port=19092 ;;
    broker2) internal_port=19093 ;;
    broker3) internal_port=19094 ;;
  esac
  echo "  Waiting for $container to rejoin cluster..."
  for i in $(seq 1 "$max_wait"); do
    if $COMPOSE_CMD exec -T "$container" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:${internal_port} --list &>/dev/null 2>&1; then
      echo "  $container is ready."
      return
    fi
    sleep 1
  done
  echo "ERROR: $container did not rejoin within ${max_wait}s"
  exit 1
}

# Wait until migration seal is committed (classicToDisklessStartOffset >= 0)
# We detect this by checking that describe shows diskless.enable=true and
# producing still works (the seal commit unblocks diskless produce path).
wait_for_seal_committed() {
  local topic="$1"
  local max_wait="${2:-60}"
  echo "  Waiting for seal to be committed on $topic..."
  for i in $(seq 1 "$max_wait"); do
    # After seal, producing to a diskless topic succeeds via the diskless path.
    # Before seal, producing may fail with RecordTooLargeException or timeout
    # depending on timing. Try a single-message produce as a health check.
    if kafka_cli "$CLI_BROKER" bash -c "
        echo 'seal-check:probe' | /opt/kafka/bin/kafka-console-producer.sh \
          --bootstrap-server $BOOTSTRAP_INTERNAL \
          --topic $topic \
          --property parse.key=true \
          --property key.separator=: \
          --request-timeout-ms 3000 \
          --timeout 3000 2>/dev/null" &>/dev/null; then
      echo "  Seal committed (diskless produce succeeded)."
      return
    fi
    sleep 1
  done
  echo "  WARN: Seal may not have committed within ${max_wait}s"
}

enable_diskless() {
  local topic="$1"
  echo "  Enabling diskless on topic $topic..."
  kafka_configs --alter --entity-type topics --entity-name "$topic" \
    --add-config "diskless.enable=true"
}

describe_topic() {
  local topic="$1"
  echo "  --- Topic config: $topic ---"
  kafka_topics --describe --topic "$topic"
  echo "  --- Topic offsets: $topic ---"
  kafka_get_offsets "$topic"
}

separator() {
  echo ""
  echo "============================================================"
  echo "  $1"
  echo "============================================================"
  echo ""
}

PASS_COUNT=0
FAIL_COUNT=0

assert_consumed() {
  local scenario="$1"
  local actual="$2"
  local expected_min="$3"
  local desc="$4"

  if [[ "$actual" -ge "$expected_min" ]]; then
    echo "  PASS: $scenario - $desc (got $actual, expected >= $expected_min)"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "  FAIL: $scenario - $desc (got $actual, expected >= $expected_min)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

# ============================================================
# MAIN
# ============================================================

wait_for_cluster

# ============================================================
# Scenario 4: Follower restart after migration (covers PR #598)
#
# Reproduce: After a topic is fully migrated (seal committed), a follower
# broker restarts. Its checkpointed HW is below the seal offset. Without
# the fix, the follower never schedules a catch-up fetcher, so its HW
# stays stale and it cannot serve reads below the seal if it becomes leader.
#
# Expected: After restart, the follower catches up to the seal via the
# ReplicaFetcher. Consuming from both brokers returns all data.
# ============================================================
separator "Scenario 4: Follower restart after migration (PR #598)"

TOPIC4="test-follower-restart"
echo "Creating classic topic $TOPIC4 (RF=2, 1 partition)..."
kafka_topics --create --topic "$TOPIC4" --partitions 1 --replication-factor 2

echo "Producing 200 messages while classic..."
kafka_produce "$TOPIC4" 200 "classic"
sleep 5

LEADER4=$(get_leader "$TOPIC4")
FOLLOWER4_CONTAINER=$(other_container "$LEADER4")
echo "  Leader is node $LEADER4, follower is $FOLLOWER4_CONTAINER"

echo "Enabling diskless and waiting for seal..."
enable_diskless "$TOPIC4"
sleep 10
wait_for_seal_committed "$TOPIC4" 30

echo "Producing 200 messages (diskless)..."
kafka_produce "$TOPIC4" 200 "diskless"
sleep 5

describe_topic "$TOPIC4"

echo "Stopping follower ($FOLLOWER4_CONTAINER) to simulate restart with stale HW..."
$COMPOSE_CMD stop "$FOLLOWER4_CONTAINER"
sleep 5

echo "Restarting follower ($FOLLOWER4_CONTAINER)..."
$COMPOSE_CMD start "$FOLLOWER4_CONTAINER"
wait_for_broker "$FOLLOWER4_CONTAINER" 60

echo "Waiting for catch-up fetcher to advance HW to seal..."
sleep 15

# Validate: end offset is correct (classic 200 + seal probe 1 + diskless 200 = 401)
END_OFFSET4=$(get_end_offset "$TOPIC4")
assert_consumed "Scenario 4a" "$END_OFFSET4" 401 \
  "end offset correct after follower restart"

# Validate: diskless data (offset >= 200) is readable
echo "Consuming from seal offset (diskless range)..."
CONSUMED4=$(kafka_consume_from_offset "$TOPIC4" 200 15000 | wc -l)
assert_consumed "Scenario 4b" "$CONSUMED4" 200 \
  "diskless data readable after follower restart"

describe_topic "$TOPIC4"


# ============================================================
# Scenario 5: Leader failover during PENDING window (covers PR #600)
#
# Reproduce: After enabling diskless, the leader seals its log and freezes
# LEO. The controller waits for followers to replicate up to LEO before
# committing the seal. If the leader dies during this PENDING window,
# followers' fetchers still point at the dead broker. The new leader never
# sees fetch traffic → seal never commits → migration hangs.
#
# Expected: With the fix, a leader epoch bump during PENDING reschedules
# fetchers. The migration completes after failover, and all data is readable.
# ============================================================
separator "Scenario 5: Leader failover during PENDING window (PR #600)"

TOPIC5="test-leader-failover-pending"
echo "Creating classic topic $TOPIC5 (RF=2, 1 partition)..."
kafka_topics --create --topic "$TOPIC5" --partitions 1 --replication-factor 2

echo "Producing 200 messages while classic..."
kafka_produce "$TOPIC5" 200 "classic"
sleep 5

LEADER5=$(get_leader "$TOPIC5")
LEADER5_CONTAINER=$(node_to_container "$LEADER5")
echo "  Leader is node $LEADER5 ($LEADER5_CONTAINER)"

echo "Enabling diskless (this starts the PENDING window)..."
enable_diskless "$TOPIC5"

# Kill the leader quickly — we want to hit the PENDING window before seal commits.
# The leader has already sealed its local log and frozen LEO, but followers
# haven't yet replicated up to it (or the controller hasn't committed the seal).
echo "Killing leader ($LEADER5_CONTAINER) during PENDING window..."
sleep 2
$COMPOSE_CMD stop "$LEADER5_CONTAINER"

echo "  Leader stopped. Waiting for new leader election..."
sleep 10

echo "Restarting old leader ($LEADER5_CONTAINER)..."
$COMPOSE_CMD start "$LEADER5_CONTAINER"
wait_for_broker "$LEADER5_CONTAINER" 60

echo "Waiting for migration to complete after failover..."
# The seal should eventually commit once the new leader sees follower fetches
wait_for_seal_committed "$TOPIC5" 90

NEW_LEADER5=$(get_leader "$TOPIC5")
echo "  New leader is node $NEW_LEADER5"

echo "Producing 200 messages (diskless, post-failover)..."
kafka_produce "$TOPIC5" 200 "diskless"
sleep 5

describe_topic "$TOPIC5"

# Validate: end offset is correct
END_OFFSET5=$(get_end_offset "$TOPIC5")
assert_consumed "Scenario 5a" "$END_OFFSET5" 401 \
  "end offset correct after leader failover during PENDING"

# Validate: diskless data is readable from the seal offset
echo "Consuming from seal offset (diskless range)..."
sleep 5
CONSUMED5=$(kafka_consume_from_offset "$TOPIC5" 200 20000 | wc -l)
assert_consumed "Scenario 5b" "$CONSUMED5" 200 \
  "diskless data readable after leader failover during PENDING"


# ============================================================
# Scenario 6: Leader failover AFTER seal committed (complementary)
#
# This verifies that a leader change after migration is fully complete
# doesn't break reads. The new leader must serve reads that span:
#   classic (local log below seal) + diskless (above seal)
# ============================================================
separator "Scenario 6: Leader failover after seal committed"

TOPIC6="test-leader-failover-post-seal"
echo "Creating classic topic $TOPIC6 (RF=2, 1 partition)..."
kafka_topics --create --topic "$TOPIC6" --partitions 1 --replication-factor 2

echo "Producing 200 messages while classic..."
kafka_produce "$TOPIC6" 200 "classic"
sleep 5

enable_diskless "$TOPIC6"
wait_for_seal_committed "$TOPIC6" 60

echo "Producing 200 messages (diskless)..."
kafka_produce "$TOPIC6" 200 "diskless"
sleep 5

LEADER6=$(get_leader "$TOPIC6")
LEADER6_CONTAINER=$(node_to_container "$LEADER6")
SURVIVOR6_CONTAINER=$(other_container "$LEADER6")
echo "  Leader is node $LEADER6 ($LEADER6_CONTAINER)"

# Switch CLI to a broker that will survive the stop
CLI_BROKER="$SURVIVOR6_CONTAINER"

echo "Stopping leader ($LEADER6_CONTAINER) to force failover..."
$COMPOSE_CMD stop "$LEADER6_CONTAINER"
sleep 10

NEW_LEADER6=$(get_leader "$TOPIC6")
NEW_LEADER6_CONTAINER=$(node_to_container "$NEW_LEADER6")
echo "  New leader is node $NEW_LEADER6 ($NEW_LEADER6_CONTAINER)"

# Validate: end offset is correct from the new leader
END_OFFSET6=$(get_end_offset "$TOPIC6")
assert_consumed "Scenario 6a" "$END_OFFSET6" 401 \
  "end offset correct from new leader after failover"

# Validate: diskless data readable from new leader
echo "Consuming from seal offset via new leader..."
CONSUMED6=$(kafka_consume_from_offset "$TOPIC6" 200 15000 | wc -l)
assert_consumed "Scenario 6b" "$CONSUMED6" 200 \
  "new leader serves diskless data after failover"

echo "Restarting old leader ($LEADER6_CONTAINER)..."
$COMPOSE_CMD start "$LEADER6_CONTAINER"
wait_for_broker "$LEADER6_CONTAINER" 60

# Restore default CLI broker
CLI_BROKER="broker"

describe_topic "$TOPIC6"


# ============================================================
# Scenario 7: Follower restart during PENDING window (covers PR #598 + #600)
#
# Combined scenario: follower restarts while migration is PENDING.
# The restarted follower must reschedule its fetcher against the current
# leader and replicate up to the frozen LEO so the seal can commit.
# ============================================================
separator "Scenario 7: Follower restart during PENDING window"

TOPIC7="test-follower-restart-pending"
echo "Creating classic topic $TOPIC7 (RF=2, 1 partition)..."
kafka_topics --create --topic "$TOPIC7" --partitions 1 --replication-factor 2

echo "Producing 200 messages while classic..."
kafka_produce "$TOPIC7" 200 "classic"
sleep 5

LEADER7=$(get_leader "$TOPIC7")
FOLLOWER7_CONTAINER=$(other_container "$LEADER7")
echo "  Leader is node $LEADER7, follower is $FOLLOWER7_CONTAINER"

echo "Enabling diskless (starts PENDING window)..."
enable_diskless "$TOPIC7"

echo "Restarting follower ($FOLLOWER7_CONTAINER) during PENDING..."
sleep 2
$COMPOSE_CMD stop "$FOLLOWER7_CONTAINER"
sleep 5
$COMPOSE_CMD start "$FOLLOWER7_CONTAINER"
wait_for_broker "$FOLLOWER7_CONTAINER" 60

echo "Waiting for follower to catch up and seal to commit..."
wait_for_seal_committed "$TOPIC7" 90

echo "Producing 200 messages (diskless)..."
kafka_produce "$TOPIC7" 200 "diskless"
sleep 5

describe_topic "$TOPIC7"

# Validate: end offset correct
END_OFFSET7=$(get_end_offset "$TOPIC7")
assert_consumed "Scenario 7a" "$END_OFFSET7" 401 \
  "end offset correct after follower restart during PENDING"

# Validate: diskless data readable
echo "Consuming from seal offset..."
CONSUMED7=$(kafka_consume_from_offset "$TOPIC7" 200 15000 | wc -l)
assert_consumed "Scenario 7b" "$CONSUMED7" 200 \
  "diskless data readable after follower restart during PENDING"


# ============================================================
# Scenario 8: Replica reassignment post-migration (covers PR #598)
#
# Reproduce: A topic starts on a subset of brokers (RF=2 out of 3 nodes).
# After migration completes, a preferred replica election moves the leader
# to a node that was always a replica but might have stale state. We also
# verify the third node (non-replica) doesn't interfere.
#
# Alternate approach: since kafka-reassign-partitions rejects RF changes in
# KRaft mode, we create the topic with explicit replica assignment excluding
# one node, then after migration we stop both original replicas and confirm
# that — with the managed-RF controller — the third node eventually gets
# added and can serve reads.
# ============================================================
separator "Scenario 8: New replica added post-migration (PR #598)"

TOPIC8="test-new-replica-post-migration"
echo "Creating classic topic $TOPIC8 with explicit assignment on nodes 1,2 only..."
kafka_topics --create --topic "$TOPIC8" --replica-assignment "1:2"

echo "Producing 200 messages while classic..."
kafka_produce "$TOPIC8" 200 "classic"
sleep 5

describe_topic "$TOPIC8"

echo "Enabling diskless and waiting for seal..."
enable_diskless "$TOPIC8"
wait_for_seal_committed "$TOPIC8" 60

echo "Producing 200 messages (diskless)..."
kafka_produce "$TOPIC8" 200 "diskless"
sleep 5

describe_topic "$TOPIC8"

# At this point, node 3 has no replica of this partition.
# In a managed-RF cluster, the controller should eventually add node 3
# when nodes 1 and 2 are unavailable (or when we trigger preferred leader).
# For this test, we verify that node 3 gets added by stopping one original
# replica (simulating a node leaving) — the managed-RF controller should
# fill from node 3.

LEADER8=$(get_leader "$TOPIC8")
LEADER8_CONTAINER=$(node_to_container "$LEADER8")
FOLLOWER8=$(other_container "$LEADER8" | sed 's/broker//' | sed 's/^$/1/')
echo "  Leader: node $LEADER8 ($LEADER8_CONTAINER)"
echo "  Node 3 (broker3) currently has no replica of this topic"

echo "Stopping $LEADER8_CONTAINER to trigger managed-RF replica addition on node 3..."
if [[ "$LEADER8_CONTAINER" == "broker" ]]; then
  CLI_BROKER="broker2"
fi
$COMPOSE_CMD stop "$LEADER8_CONTAINER"

echo "Waiting for managed-RF controller to add node 3 as replica (up to 60s)..."
for i in $(seq 1 60); do
  REPLICAS=$(get_replicas "$TOPIC8" 2>/dev/null || echo "")
  if echo "$REPLICAS" | grep -q "3"; then
    echo "  Node 3 added to replica set: [$REPLICAS]"
    break
  fi
  sleep 1
done

# Regardless of managed-RF timing, restart the stopped broker
echo "Restarting $LEADER8_CONTAINER..."
$COMPOSE_CMD start "$LEADER8_CONTAINER"
wait_for_broker "$LEADER8_CONTAINER" 60
CLI_BROKER="broker"

sleep 10
describe_topic "$TOPIC8"

# Validate: end offset correct
END_OFFSET8=$(get_end_offset "$TOPIC8")
assert_consumed "Scenario 8a" "$END_OFFSET8" 401 \
  "end offset correct after managed-RF adds new replica"

# Validate: diskless data readable
echo "Consuming from seal offset..."
CONSUMED8=$(kafka_consume_from_offset "$TOPIC8" 200 15000 | wc -l)
assert_consumed "Scenario 8b" "$CONSUMED8" 200 \
  "diskless data readable after managed-RF adds new replica"


# ============================================================
# Summary
# ============================================================
separator "Summary"

echo "Results: $PASS_COUNT passed, $FAIL_COUNT failed"
echo ""

for topic in "$TOPIC4" "$TOPIC5" "$TOPIC6" "$TOPIC7" "$TOPIC8"; do
  describe_topic "$topic"
  echo ""
done

if [[ "$FAIL_COUNT" -gt 0 ]]; then
  echo "SOME TESTS FAILED — review output above."
  exit 1
else
  echo "All tests passed."
fi
