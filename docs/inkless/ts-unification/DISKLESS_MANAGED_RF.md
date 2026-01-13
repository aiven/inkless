# Diskless-Managed Replication Factor

## Table of Contents

1. [Objectives](#objectives)
2. [Activation Model](#activation-model-binary-version)
3. [Placement Model](#placement-model)
4. [Controller Behavior](#controller-behavior)
   - [Topic Creation](#topic-creation)
   - [Add Partitions](#add-partitions)
   - [Standard Operations](#standard-operations-after-creation)
5. [Broker Behavior](#broker-behavior)
   - [No Replica Fetcher for Diskless Topics](#no-replica-fetcher-for-diskless-topics)
6. [Metadata Transformation](#metadata-transformation)
7. [Observability](#observability)
8. [Implementation Path](#implementation-path)
9. [Rejected Alternatives](#rejected-alternatives)
10. [Appendix: Topic Migration Interactions](#appendix-topic-migration-interactions)

---

## Objectives

Enable **rack-aware, dynamic, stable KRaft-managed replicas** for diskless topics:

- **KRaft-managed replicas**: Diskless topics will have actual KRaft-managed replicas, not the current "faked" RF=1 metadata. These replicas are tracked by the controller, participate in leader election, and appear in standard Kafka tooling. The key difference from classic replicas: **they don't replicate data** (object storage handles durability), but they do provide partition assignments for RLM integration and operational consistency. See [No Replica Fetcher for Diskless Topics](#no-replica-fetcher-for-diskless-topics) for implementation details.
- **Rack-aware at creation**: Topics are created with one replica per rack (RF = rack count)
- **Standard operations after creation**: Once created, diskless topics use standard Kafka replica management — no custom reconciliation or drift handling
- **Controller-managed RF**: Users don't specify RF at creation; the controller computes it from rack topology
- **Leader-agnostic produce**: Keep the diskless produce model where any broker can accept writes, while still having a leader for controller duties

**Non-goal:** 
Topic migration mechanics (Tiered Classic → Diskless), including sealing and offset boundary tracking.
Those are covered in [DESIGN.md](DESIGN.md).

---

## Activation Model (Binary Version)

Managed RF is activated by **deploying a new binary version**. No feature flags, no topic attributes, no server configs.

### Behavior Summary

**Old binary:**
- Diskless topics use legacy "RF=1 / faked metadata" behavior
- `InklessTopicMetadataTransformer` calculates synthetic placement via hashing

**New binary:**
- Diskless topics use KRaft-managed placement (one replica per rack at creation)
- Transformer filters KRaft placement by client AZ instead of calculating

### How It Works

1. Controller detects diskless topics via existing `diskless.enable=true` topic config
2. At topic creation, controller computes RF = rack count and places one replica per rack
3. No new KRaft records needed; existing topic and partition records are sufficient
4. After creation, standard Kafka replica management applies

### Rolling Upgrade

During rolling restart:
- Old brokers return synthetic RF=1 metadata
- New brokers return KRaft-based filtered metadata
- Clients handle inconsistency via standard metadata refresh/retry
- After full upgrade, all brokers return consistent KRaft-based metadata

### Existing Diskless Topics

- Managed RF applies **immediately** to all diskless topics on upgrade
- No data movement required (diskless data lives in object storage)
- One-time metadata update as controller sets proper replica assignments

### Rationale

Inkless is a new component with faster iteration cycles than upstream Kafka. 
We control deployments and can coordinate version upgrades. 
Complex feature gating adds overhead without proportional benefit for this internal enabler feature. 
See [Rejected Alternative: Feature Flag Activation](#rejected-alternative-feature-flag-activation) for the more complex approach we considered.

---

## Placement Model

### Rack-Aware Placement at Creation

When a diskless topic is created:
- Controller determines current rack count from registered brokers
- RF is set to rack count (e.g., 3 racks → RF=3)
- One broker is selected per rack for each partition
- Broker selection within a rack uses load balancing (least loaded broker)

### Brokers Without Rack

Brokers without an explicit `broker.rack` config are treated as belonging to `rack=unknown`.
- They are considered a separate "rack" for placement purposes
- If all brokers lack rack config, RF=1 (single "unknown" rack)

### Placement Is Static After Creation

Once a topic is created:
- RF does not automatically change if racks are added/removed
- Replica assignments don't automatically adjust
- Standard Kafka replica management applies

This keeps the design simple and predictable.

---

## Controller Behavior

### Topic Creation

When creating diskless topics (`diskless.enable=true`):

**RF Computation:**
- Controller counts distinct racks from registered brokers
- RF = rack count (e.g., 3 racks → RF=3)

**Placement:**
- One replica assigned per rack
- Within each rack, select least loaded broker
- Leader is typically the first replica (standard Kafka behavior)

**Validation:**
- `replicationFactor` in CreateTopics request must be `-1` or `1`
  - `-1`: Controller computes placement (recommended)
  - `1`: Accepted for compatibility with automation tools
  - Any value `> 1`: Rejected (RF is system-managed)
  - `0`: Rejected
- `replicaAssignments` must be empty (manual assignment not allowed at creation)

**Error messages:**
```
InvalidRequestException: replication factor is system-managed for diskless topics; 
use replicationFactor=-1 or replicationFactor=1
```

```
InvalidRequestException: replica assignments cannot be specified for diskless topics; 
placement is system-managed at creation time
```

### Add Partitions

When adding partitions to an existing diskless topic:

```bash
kafka-topics.sh --alter --topic foo --partitions 10
```

**Behavior:**
- New partitions are placed using the same one-per-rack logic as creation
- RF for new partitions = current rack count (may differ from original if racks changed)
- Existing partitions are not affected

This ensures consistency: all partitions of a diskless topic use rack-aware placement.

### Standard Operations (After Creation)

After topic creation, diskless topics behave like classic Kafka topics for all operations:

**Leader Election:**
- Standard Kafka leader election from ISR
- For diskless topics, all replicas are always in ISR (data is in object storage)
- `kafka-leader-election.sh` works normally

**Broker Failure:**
- Replicas on failed broker become offline
- Partition becomes under-replicated (reported in metrics)
- Leader fails over if leader was on failed broker
- When broker returns, replica immediately rejoins ISR (no catch-up needed)

**Broker Shutdown (Controlled):**
- Standard controlled shutdown
- Leaders moved away before shutdown
- No special handling for diskless

**Reassignment:**
- `kafka-reassign-partitions.sh` works normally
- Operator can reassign replicas to any brokers
- No data movement (just metadata update for diskless)
- ⚠️ Operator can break rack-awareness (see below)

**Rolling Upgrades:**
- Standard Kafka rolling restart behavior
- No special handling for diskless topics

### Operator Reassignment and Rack-Awareness

Operators can use standard reassignment tools on diskless topics:

```bash
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json \
  --execute
```

**Important:** The controller does **not** validate rack-awareness for reassignments.

If an operator reassigns replicas such that rack-awareness is broken (e.g., 2 replicas in same rack):
- Assignment is **accepted** (standard Kafka behavior)
- Warning logged: `WARN Diskless topic 'foo' partition 0 has non-rack-aware placement`
- Metric emitted: `kafka.controller.diskless.rack_aware{topic="foo",partition="0"} = 0`
- Operator is responsible for fixing if desired

**Rationale:** 
- Keeps implementation simple (no validation logic in reassignment path)
- Same behavior as classic topics (Kafka doesn't enforce rack-awareness on reassignment)
- Operator has full control for edge cases (intentional non-rack-aware placement)

### Topology Changes (Racks Added/Removed)

**New rack added:**
- Existing topics are NOT automatically updated
- RF stays at original value
- Operator can reassign to include new rack if desired
- New topics will include the new rack

**Rack removed (all brokers gone):**
- Partitions with replicas on that rack become under-replicated
- Standard Kafka under-replication handling
- Operator reassigns to remove references to gone rack

**Note:** These are edge cases that rarely happen in practice. The design prioritizes simplicity over automatic handling of rare topology changes.

---

## Broker Behavior

**Key point: Broker behavior is unchanged from today.** Diskless topics already work with existing Inkless produce/consume handlers. This design changes **metadata and placement**, not the data path.

### Produce Path

**No changes to produce handling.** The existing Inkless produce path continues to work:
- Any broker can accept writes (leader-agnostic)
- Writes go to object storage via `AppendHandler`
- Batch metadata stored in Control Plane (PostgreSQL)

The only difference is that KRaft now tracks replica assignments, but this doesn't affect how produces are processed.

### Consume Path

**No changes to consume handling.** The existing Inkless consume path continues to work:
- `FetchHandler` + `Reader` serve reads from object storage
- Data fetched via Control Plane batch lookups
- Object cache provides locality benefits

What changes is **which broker the client talks to** (determined by transformer filtering), not how that broker serves the request.

### ISR Semantics

For diskless topics, **all replicas are always considered in-sync** because:
- Source of truth (batch metadata + WAL objects) is shared via object storage
- All replicas have immediate access to the same committed data
- No replication lag in the classic sense

ISR membership is trivially "all assigned replicas" for diskless partitions.

**This is unchanged from today** — diskless topics already have this property. The difference is that now there are multiple replicas in the ISR (one per rack) instead of a single faked replica.

### No Replica Fetcher for Diskless Topics

**Diskless topics do not start replica fetcher threads.** This is a critical design invariant that must be preserved.

#### Current Implementation

In `ReplicaManager.scala`, diskless topics are explicitly skipped when applying metadata changes:

**`applyLocalLeadersDelta`** (line ~3118):
```scala
localLeaders.foreachEntry { (tp, info) =>
  if (!_inklessMetadataView.isDisklessTopic(tp.topic()))
    getOrCreatePartition(tp, delta, info.topicId).foreach { ... }
}
```

**`applyLocalFollowersDelta`** (line ~3154):
```scala
localFollowers.foreachEntry { (tp, info) =>
  if (!_inklessMetadataView.isDisklessTopic(tp.topic()))
    getOrCreatePartition(tp, delta, info.topicId).foreach { ... }
}
```

This means:
- No local `Partition` objects created for diskless topics
- No local log directories created
- No `ReplicaFetcherThread` started for followers
- No replication traffic between brokers for diskless data

#### Why This Must Be Preserved

1. **No data to replicate**: Diskless data lives in object storage, not local logs
2. **Performance**: Fetcher threads consume CPU and network; unnecessary for diskless
3. **Consistency**: All brokers read from the same object storage; no catch-up needed
4. **Simplicity**: Fewer moving parts means fewer failure modes

#### Regression Prevention

**What could cause a regression:**
- Code changes that don't check `isDisklessTopic()` before creating partitions
- New code paths that bypass the existing guards
- Feature flags that accidentally enable local storage for diskless topics

**How to prevent regressions:**

1. **Unit tests**: Ensure tests verify no fetcher is started for diskless topics
   ```scala
   // Example test assertion
   assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(disklessTopicPartition))
   ```

2. **Integration tests**: Verify no replication traffic for diskless topics
   - Create diskless topic with RF=3
   - Produce messages
   - Verify no inter-broker fetch requests for that topic

3. **Code review checklist**: Any changes to `ReplicaManager.applyLocalLeadersDelta` or `applyLocalFollowersDelta` must preserve the `isDisklessTopic()` guard

4. **Metrics**: Monitor `kafka.server:type=ReplicaFetcherManager,name=*` to detect unexpected fetcher activity

#### Managed RF Interaction

With managed RF, diskless topics will have multiple replicas (one per rack). However:
- These are **metadata-only replicas** in KRaft
- They exist for leader election and RLM coordination
- They do **not** trigger replica fetcher threads
- The `isDisklessTopic()` guards in `ReplicaManager` continue to apply

The managed RF design **does not change** this behavior — it only changes how replica assignments are computed and stored in KRaft.

---

## Metadata Transformation

### Current vs Target Behavior

**Today (legacy):**
- `InklessTopicMetadataTransformer` intercepts responses
- Forces diskless partitions to look like RF=1
- Calculates synthetic leader/ISR by hashing `(topicId, partition)` into alive brokers in client AZ

**Target (managed RF):**
- KRaft placement is source of truth
- Replica sets reflect controller placement (one per rack)
- Transformer **filters** by client AZ instead of **calculating**

### Transformer Implementation

**Input changes:**
- For each diskless partition: KRaft replica set (brokerIds), KRaft leader id
- For each broker: `broker.rack` (AZ) via `Node.rack()`

**Output (per request / per clientAZ):**
- Filtered `replicaNodes` / `isrNodes` for diskless partitions

**Filtering logic:**
- If partition has replica in `clientAZ`: return only that local replica
- If no replica in `clientAZ`: fallback to cross-AZ view (full replica set or deterministic subset)

**Detection:**
- New binary always uses KRaft-placement-based projection for diskless topics
- Check `diskless.enable=true` topic config to identify diskless topics

---

## Observability

### Metrics

**Controller metrics:**
- `kafka.controller.diskless.effective_rf{topic}` - RF assigned at creation
- `kafka.controller.diskless.rack_aware{topic,partition}` - 1 if one-per-rack, 0 if not
- `kafka.controller.diskless.rack_aware_total{topic}` - Count of rack-aware partitions
- `kafka.controller.diskless.non_rack_aware_total{topic}` - Count of non-rack-aware partitions

**Standard Kafka metrics (already exist):**
- `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`
- `kafka.controller:type=KafkaController,name=OfflinePartitionsCount`

### Admin Surfaces

**DescribeTopic shows actual replica assignments:**
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic foo

# Output:
# Topic: foo  TopicId: abc123  PartitionCount: 3  ReplicationFactor: 3
#   Partition: 0  Leader: 1  Replicas: 1,3,5  Isr: 1,3,5
#   Partition: 1  Leader: 3  Replicas: 3,5,1  Isr: 3,5,1
#   Partition: 2  Leader: 5  Replicas: 5,1,3  Isr: 5,1,3
```

### Logs

**Warning when rack-awareness is broken:**
```
WARN [Controller] Diskless topic 'foo' partition 0 has non-rack-aware placement: 
     replicas [1,2,5] map to racks [rack-A,rack-A,rack-C]. Expected one replica per rack.
```

This warning is logged:
- When reassignment results in non-rack-aware placement
- Periodically if non-rack-aware placement persists (configurable interval)

---

## Implementation Path

### Phase 1: Topic Creation with Rack-Aware Placement

1. Modify `ReplicationControlManager` to detect `diskless.enable=true` topics
2. Compute RF = rack count at creation time
3. Implement one-per-rack broker selection
4. Reject `replicationFactor > 1` and non-empty `replicaAssignments`

**Estimate:** 2 weeks

### Phase 2: Transformer Changes

1. Update `InklessTopicMetadataTransformer` to read KRaft placement
2. Implement AZ filtering logic
3. Remove synthetic hashing calculation

**Estimate:** 2 weeks

### Phase 3: Add Partitions Support

1. Apply same one-per-rack logic when adding partitions
2. Handle case where rack count changed since topic creation

**Estimate:** 1 week

### Phase 4: Observability

1. Add `rack_aware` metric
2. Add warning logs for non-rack-aware placement
3. Documentation

**Estimate:** 1 week

### Total Estimate

**6 weeks with 1 engineer, or 3-4 weeks with 2 engineers**

This is significantly simpler than the original design which included reconciliation loops and drift handling.

---

## Rejected Alternatives

### Why KRaft-Managed Replicas Are Required

All RF=1 alternatives fail because **RLM requires KRaft-managed partition leadership**:

```
Tiered storage integration (for PG scalability)
        │
        ▼
RLM expiration tasks (to clean up tiered segments)
        │
        ▼
onLeadershipChange() must fire (RLM's entry point)
        │
        ▼
KRaft-managed partition leadership (not faked/virtual)
        │
        ▼
RF > 1 with KRaft-managed replica assignments
```

The tiering pipeline's merge phase requires a partition leader to coordinate segment creation and RLM registration. There is no path to production-grade diskless topics without embracing KRaft-based replica placement.

See [DESIGN.md](DESIGN.md) Stream 7 for tiering pipeline details.

### Rejected Alternative: Feature Flag Activation

**Concept:** Follow upstream Kafka patterns with `DisklessVersion` feature enum, server config, and KRaft records.

**What it would have involved:**
- Policy flag (`disklessManagedRF`) as new KRaft record
- Feature enum (`DisklessVersion`) following `EligibleLeaderReplicasVersion` pattern
- Server config (`diskless.managed.rf.enable`) requiring cluster-wide coordination
- Manual activation via `kafka-features.sh --upgrade --feature diskless.version=1`

**Why we dropped it:**
- Not needed; `diskless.enable` config is sufficient
- Binary version is the gate
- Adds operational step without benefit
- Binary rollback is simpler

**When to reconsider:**
- If upstreaming to Apache Kafka (strict backward compatibility)
- If needing per-topic opt-in/opt-out
- If supporting long-lived mixed-version clusters

### Rejected Alternative: Dynamic Reconciliation

**Concept:** Controller continuously reconciles replica placement to maintain one-per-rack as topology changes.

**What it would have involved:**
- Reconciliation loop monitoring rack changes
- Automatic RF adjustment when racks added/removed
- Drift detection and auto-fix
- Rack liveness state machine (HEALTHY → DEGRADED → UNAVAILABLE)

**Why we dropped it:**
- Significant complexity in controller
- Divergent behavior from standard Kafka operations
- Topology changes are rare in practice
- Operator can handle edge cases manually
- Standard Kafka reassignment tools work fine

**When to reconsider:**
- If customers frequently add/remove racks
- If manual reassignment becomes burdensome

### Rejected Alternative A: Virtual Leader for RLM Tasks

**Concept:** Keep RF=1, designate deterministic "virtual leader" per partition for RLM tasks.

**Why it fails:**
- Two sources of leadership creates confusion
- Virtual leader failover requires new coordination mechanism
- RLM assumes real `Partition` objects with `UnifiedLog`
- Tiering pipeline merge phase needs `ReplicaManager` context

### Rejected Alternative B: Control Plane Manages Tiered Expiration

**Concept:** Extend PostgreSQL to track tiered segment metadata and run expiration directly.

**Why it fails:**
- Contradicts goal of reducing PG load
- Duplicates RLM retention logic
- Breaks RLMM integration and Kafka tooling
- Creates cross-system consistency problems

### Rejected Alternative C: Direct Tiered Read via FetchHandler

**Concept:** Extend `FetchHandler` to read tiered storage directly, bypassing RLM read path.

**Why it fails:**
- Only solves read path, not expiration
- Must combine with Alternative A or B
- Duplicates RLM index handling

### Rejected Alternative D: Treat Tiered Data as Read-Only Archival

**Concept:** Freeze tiered portion, use S3 lifecycle policies for expiration.

**Why it fails:**
- No programmatic retention (can't implement `retention.ms`/`retention.bytes`)
- Breaks topic deletion cleanup
- Doesn't address PG scalability for new diskless data

---

## Appendix: Topic Migration Interactions

This section describes how the managed RF design interacts with **topic migration (Tiered Classic → Diskless)**. For full migration mechanics, see [DESIGN.md](DESIGN.md).

### ISR Semantics During Topic Migration

When migrating a topic from Tiered Classic to Diskless, ISR semantics transition through phases:

**Phase 1: Pre-migration (Tiered Classic)**
- Standard Kafka ISR semantics apply
- Replicas must fetch from leader and stay within `replica.lag.time.max.ms`
- Leader manages replication to followers

**Phase 2: During Topic Migration (Sealing)**
- Active segment is rolled and copied to tiered storage
- ISR is maintained normally
- No new writes accepted until migration completes
- All replicas must be in-sync before proceeding

**Phase 3: Post-migration (Diskless)**
- New writes go to diskless storage (object storage)
- ISR for diskless portion is "all replicas" (trivially in-sync)
- Reads from tiered portion use existing RLM machinery
- Topic uses diskless ISR semantics going forward

### Replica Set Changes During Topic Migration

Topic migration (Tiered Classic → Diskless) may trigger replica set changes:

**Before migration:** RF=3 with classic placement (leader + 2 followers)

**After migration:** RF=rack_count with managed placement (one per rack)

If rack_count differs from original RF:
- Controller reconciles to target placement
- Uses standard add-then-remove approach
- Paced to avoid disruption

### Interaction with Tiering Pipeline

After topic migration to Diskless:
1. New writes accumulate in diskless storage
2. Aged batches become eligible for tiering (per `local.retention.ms`)
3. Tiering pipeline converts batches to tiered segments
4. Partition leader coordinates segment creation via RLM
5. Batch metadata deleted from PostgreSQL

The managed RF design ensures a KRaft-managed partition leader exists for step 4.

---

## Open Items

- Behavior when `broker.rack` config changes on a broker (rare edge case)
- Whether to add a tool to "re-rack-aware" a topic (reassign to restore one-per-rack)
