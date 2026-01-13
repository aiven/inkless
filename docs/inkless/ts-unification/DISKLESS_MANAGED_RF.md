# Diskless-Managed Replication Factor

## Table of Contents

1. [Objectives](#objectives)
2. [Activation Model](#activation-model-binary-version)
3. [Placement Model](#placement-model)
   - [Rack Cardinality](#rack-cardinality-and-missing-racks)
   - [Rack Liveness](#rack-liveness-and-transience)
4. [Controller Behavior](#controller-behavior)
   - [Placement Logic](#controller-placement-logic)
   - [Topic Creation](#topic-creation-semantics)
   - [Operator Override](#operator-override-and-reassignment)
   - [Reconciliation](#reconciliation-algorithm)
5. [Broker Behavior](#broker-behavior)
   - [Produce Path](#produce-path)
   - [Consume Path](#consume-path)
   - [ISR Semantics](#isr-semantics)
6. [Metadata Transformation](#metadata-transformation)
   - [Current vs Target](#current-vs-target-behavior)
   - [Transformer Changes](#transformer-implementation)
7. [Observability](#observability)
8. [Implementation Path](#implementation-path)
9. [Rejected Alternatives](#rejected-alternatives)
10. [Appendix: Migration Interactions](#appendix-migration-interactions)

---

## Objectives

Enable **rack-aware, dynamic, stable KRaft-managed replicas** for diskless topics:

- **KRaft-managed replicas**: Diskless topics will have actual KRaft-managed replicas, not the current "faked" RF=1 metadata. These replicas are tracked by the controller, participate in leader election, and appear in standard Kafka tooling. The key difference from classic replicas: they don't replicate data (object storage handles durability), but they do provide partition assignments for RLM integration and operational consistency.
- **Rack-aware**: Enforce one replica per rack/AZ, ensuring geographic distribution
- **Dynamic**: Effective RF = rack cardinality; adjusts automatically as cluster topology changes
- **Stable**: Minimize placement churn; prioritize leader stability during rack additions/removals
- **Controller-managed**: Users don't specify RF; the controller computes optimal placement
- **Leader-agnostic produce**: Keep the diskless produce model where any broker can accept writes, while still having a leader for controller duties

**Non-goal:** 
Tiered Storage migration/cutover mechanics (sealing, offset boundary tracking).
Those are covered in [DESIGN.md](DESIGN.md).
This document covers **why** tiered storage integration requires managed RF, not **how** to implement the migration.

---

## Activation Model (Binary Version)

Managed RF is activated by **deploying a new binary version**. No feature flags, no topic attributes, no server configs.

### Behavior Summary

**Old binary:**
- Diskless topics use legacy "RF=1 / faked metadata" behavior
- `InklessTopicMetadataTransformer` calculates synthetic placement via hashing

**New binary:**
- Diskless topics use KRaft-based placement (one replica per rack)
- Transformer filters KRaft placement by client AZ instead of calculating

### How It Works

1. Controller detects diskless topics via existing `diskless.enable=true` topic config
2. If diskless, controller applies managed RF placement (one replica per rack)
3. No new KRaft records needed; existing topic and partition records are sufficient
4. Transformer filters by client AZ instead of calculating synthetic placement

### Rolling Upgrade

During rolling restart:
- Old brokers return synthetic RF=1 metadata
- New brokers return KRaft-based filtered metadata
- Clients handle inconsistency via standard metadata refresh/retry
- After full upgrade, all brokers return consistent KRaft-based metadata

### Existing Diskless Topics

- Managed RF applies **immediately** to all diskless topics on upgrade
- No data movement required (diskless data lives in object storage)
- One-time metadata update as controller reconciles placement

### Rationale

Inkless is a new component with faster iteration cycles than upstream Kafka. 
We control deployments and can coordinate version upgrades. 
Complex feature gating adds overhead without proportional benefit for this internal enabler feature. 
See [Rejected Alternative: Feature Flag Activation](#rejected-alternative-feature-flag-activation) for the more complex approach we considered.

---

## Placement Model

### Rack Cardinality and Missing Racks

- **Effective RF** equals the number of racks (rack cardinality)
- Brokers without an explicit rack are treated as `rack=unknown` (a separate AZ for placement)

### Rack Liveness and Transience

**Problem:** The controller must distinguish between transient rack failures (network blip, rolling restart) and permanent rack removal to avoid:
- **Over-reacting:** Removing replicas during transient outages, causing unnecessary churn
- **Under-reacting:** Keeping replicas assigned to a permanently failed rack

**Solution: Threshold-based Liveness**

Config: `diskless.managed.rf.rack.unavailable.threshold.ms`
- Type: Long (milliseconds)
- Default: `300000` (5 minutes)
- Semantics: A rack is unavailable only after **all** brokers in that rack have been unreachable for longer than this threshold

**Rack State Machine:**

```
                  broker unregisters / heartbeat timeout
HEALTHY ─────────────────────────────────────────────────► DEGRADED
   ▲                                                           │
   │ broker re-registers                                       │ threshold exceeded
   │ (any broker in rack)                                      │ (all brokers in rack)
   │                                                           ▼
   └─────────────────────────────────────────────────────── UNAVAILABLE
                  broker re-registers (any)
```

**Controller Behavior by State:**

**HEALTHY:**
- Normal operation
- One replica assigned per rack
- Eligible brokers available

**DEGRADED:**
- Existing replicas retained
- No new replicas added to this rack
- `missingRackCapacity` exception surfaced
- Controller waits for recovery

**UNAVAILABLE:**
- Replicas removed from this rack
- Effective RF decreases
- Reconciliation triggered

**Rationale:**
- 5-minute default aligns with Kubernetes pod restart times and AWS AZ failover windows
- DEGRADED state prevents thrashing during rolling restarts
- Operators can tune threshold based on SLO requirements

---

## Controller Behavior

### Controller Placement Logic

**Components:** `ReplicationControlManager`, `PartitionChangeBuilder`, `PartitionRegistration`

**State tracked:**
- Topic: `effectiveRF` (derived from rack count)
- Partition: `placementState` (reconciling | steady | frozen)

**Reconciliation strategy:**
- Input: rack map, broker capacities, current assignment, throttles
- Target: one eligible broker per rack
- Approach: reuse existing by rack, minimize movement, preserve leader

### Topic Creation Semantics

When creating diskless topics (`diskless.enable=true`):

**Validation:**
- `replicationFactor` must be `-1` or `1`
  - `-1`: Controller computes placement (recommended)
  - `1`: Accepted for compatibility with automation tools that require an RF value
  - Any value `> 1`: Rejected
  - `0`: Rejected
- `replicaAssignments` must be empty (manual assignment not allowed)
- Controller computes actual placement as "one replica per rack"

**Error behavior:**
```
InvalidRequestException: replication factor is system-managed for diskless topics; 
use replicationFactor=-1 or replicationFactor=1
```

```
InvalidRequestException: replica assignments cannot be specified for diskless topics; 
placement is system-managed
```

**Rationale:** Many clients and automation tools always send an RF. Accepting `1` provides compatibility while still having the controller manage actual placement.

### Operator Override and Reassignment

Diskless topics use managed placement, but operators may still need to:
- Move a hot partition to a less loaded broker
- Drain a broker for maintenance
- Rebalance after adding brokers to a rack

**Guiding principle:** Allow all standard Kafka reassignment operations. Observe drift from rack-aware placement via metrics/alerts rather than blocking.

**Why not strict validation?**

We considered rejecting any assignment that violates one-per-rack, but this adds complexity:
- Validation logic needed in multiple code paths (create, reassign, alter)
- Edge cases when rack topology changes mid-operation
- Divergent behavior from standard Kafka (harder to reason about)
- Blocks legitimate operator actions (intentional temporary placement)

**Chosen approach: Observable Drift**

All standard Kafka assignment operations work unchanged:
- `kafka-reassign-partitions.sh` works normally
- Manual replica assignment accepted
- Leader election works as expected

When placement diverges from one-per-rack target:
- Partition flagged as having "placement drift"
- Metric: `kafka.controller.diskless.placement_drift{topic,partition}` = 1
- Alert recommendation: "Partition X has placement drift from rack-aware target"

**Reconciliation behavior:**

The controller reconciliation loop can be configured to:
- **Auto-fix drift** (default): Gradually move partitions back to one-per-rack placement
- **Observe only**: Report drift but don't auto-correct (for operators who want manual control)

Config: `diskless.managed.rf.auto.reconcile` (boolean, default: true)

**Operator workflow:**

1. Operator reassigns partition for operational reasons (e.g., drain broker)
2. Assignment accepted immediately
3. If placement violates one-per-rack:
   - Metric shows drift
   - Alert fires (if configured)
   - If auto-reconcile enabled: controller will eventually fix it
   - If auto-reconcile disabled: operator fixes manually or accepts drift

This keeps Kafka API compatibility, reduces implementation complexity, and gives operators flexibility while maintaining observability.

### Reconciliation Algorithm

**Inputs:** rack map, broker capacities/eligibility, current assignment, `maxMovesPerSec`

**Steps:**
1. Calculate required racks (all active racks)
2. Keep one replica per rack; evict extras from over-represented racks
3. For missing racks, add replica to least loaded eligible broker
4. Queue moves; enforce pacing and ISR health checks
5. If leader must move, ensure controlled election after catch-up

**Exception: No Capacity in Rack**

A rack has "no capacity" when all brokers in that rack are ineligible for replica assignment. This can happen when:
- All brokers in the rack are offline or unresponsive
- All brokers in the rack are in controlled shutdown
- All brokers in the rack have been fenced by the controller

When this occurs:
- Controller marks the partition as temporarily under-replicated for that rack
- Surfaces `missingRackCapacity` exception in metrics and admin APIs
- Does **not** assign a replica to a different rack (would violate one-per-rack invariant)
- Retries assignment when a broker in that rack becomes available again

This is distinct from "rack unavailable" (threshold exceeded, rack considered gone) where the replica is removed entirely.

**Exception: Rack Removed**

When a rack transitions to UNAVAILABLE (all brokers gone past threshold):
- Replicas in that rack are removed from partition assignments
- Effective RF decreases by 1
- No replacement replica is added (no other rack should have >1 replica)

**Guardrails:**
- Separate throttles for diskless vs classic reassignment
- Prefer "add then remove" to maintain availability
- Don't starve classic reassignment processing

---

## Broker Behavior

**Key point: Most broker behavior remains unchanged from today.** Diskless topics already work with the existing Inkless produce/consume handlers. This design changes **metadata and placement**, not the data path.

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
- `kafka.controller.diskless.effective_rf{topic}` - Current effective RF
- `kafka.controller.diskless.rack_coverage{topic,partition}` - Coverage percentage
- `kafka.controller.diskless.rack_state{rack}` - 0=HEALTHY, 1=DEGRADED, 2=UNAVAILABLE
- `kafka.controller.diskless.rack_degraded_since_ms{rack}` - Timestamp when degraded
- `kafka.controller.diskless.reassignment_backlog` - Pending reconciliation work
- `kafka.controller.diskless.placement_state{topic,partition}` - Current state
- `kafka.controller.diskless.placement_drift{topic,partition}` - 1 if placement differs from one-per-rack target
- `kafka.controller.diskless.exceptions{topic,partition}` - Active exceptions

**Broker metrics:**
- `kafka.broker.diskless.coordinator_lag{topic,partition}`
- `kafka.broker.diskless.cache_build_lag{topic,partition}`
- `kafka.broker.diskless.preferred_broker_hit_ratio{topic}`

### Admin Surfaces (Read-Only)

**DescribeTopic extensions:**
- `IsDiskless: boolean`
- `effectiveRF: int` (current rack cardinality)

**DescribePartition / Placement view:**
- `targetRacks: [rackId]`
- `assignedRacks: [rackId]`
- `rackCoverage: percent`
- `placementState: reconciling|steady|frozen`
- `exceptions: missingRackCapacity|none`

**Client metadata (Metadata v14):**
- `IsDiskless=true` for diskless topics

### Alert Recommendations

- Rack in DEGRADED state for >50% of threshold
- `rackCoverage` below 100% for extended period
- `reassignment_backlog` growing continuously
- `placement_drift` = 1 for any partition (operator override detected)

---

## Implementation Path

### Phase 1: Controller Placement

1. Modify `ReplicationControlManager` to detect `diskless.enable=true` topics
2. Compute rack-cardinality placement (one replica per rack)
3. Use existing partition records; no new KRaft record types
4. Implement rack liveness tracking with threshold-based state machine

### Phase 2: Transformer Changes

1. Update `InklessTopicMetadataTransformer` to filter by KRaft placement
2. Implement AZ filtering: local-AZ replica if present, else cross-AZ fallback
3. Remove synthetic placement calculation

### Phase 3: Steady-State Reconciliation

1. Implement reconciliation loop for rack topology changes
2. Add throttling and pacing controls
3. Add metrics and observability

**Key invariant:** Only change client-visible metadata when KRaft placement is already correct.

### Code References

**Controller:**
- `metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java`
- `metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java`
- `metadata/src/main/java/org/apache/kafka/metadata/PartitionRegistration.java`

**Inkless:**
- `storage/inkless/src/main/java/io/aiven/inkless/metadata/InklessTopicMetadataTransformer.java`
- `storage/inkless/src/main/java/io/aiven/inkless/consume/FetchHandler.java`
- `storage/inkless/src/main/java/io/aiven/inkless/control_plane/MetadataView.java`

---

## Rejected Alternatives

### Why RF > 1 Is Required

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

- New KRaft record type → Not needed; `diskless.enable` config is sufficient
- Feature enum + registration → Not needed; binary version is the gate
- Server config + propagation → Adds operational step without benefit
- Complex rollback semantics → Binary rollback is simpler

**When to reconsider:**
- If upstreaming to Apache Kafka (strict backward compatibility)
- If needing per-topic opt-in/opt-out
- If supporting long-lived mixed-version clusters

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

## Appendix: Migration Interactions

This section describes how the managed RF design interacts with topic migration from classic tiered storage to diskless mode. For full migration mechanics, see [DESIGN.md](DESIGN.md).

### ISR Semantics During Migration

When migrating a topic from classic tiered storage to diskless mode, ISR semantics transition through phases:

**Phase 1: Pre-migration (Classic Tiered)**
- Standard Kafka ISR semantics apply
- Replicas must fetch from leader and stay within `replica.lag.time.max.ms`
- Leader manages replication to followers

**Phase 2: During Migration (Sealing)**
- Active segment is rolled and copied to tiered storage
- ISR is maintained normally
- No new writes accepted until migration completes
- All replicas must be in-sync before proceeding

**Phase 3: Post-migration (Hybrid/Diskless)**
- New writes go to diskless storage (object storage)
- ISR for diskless portion is "all replicas" (trivially in-sync)
- Reads from tiered portion use existing RLM machinery
- Topic uses diskless ISR semantics going forward

### Replica Set Changes During Migration

Migration may trigger replica set changes:

**Before migration:** RF=3 with classic placement (leader + 2 followers)

**After migration:** RF=rack_count with managed placement (one per rack)

If rack_count differs from original RF:
- Controller reconciles to target placement
- Uses standard add-then-remove approach
- Paced to avoid disruption

### Interaction with Tiering Pipeline

After migration to diskless:
1. New writes accumulate in diskless storage
2. Aged batches become eligible for tiering (per `local.retention.ms`)
3. Tiering pipeline converts batches to tiered segments
4. Partition leader coordinates segment creation via RLM
5. Batch metadata deleted from PostgreSQL

The managed RF design ensures a KRaft-managed partition leader exists for step 4.

---

## Open Items

- Where to surface `effectiveRF` in admin tooling (prefer existing DescribeTopic extensions)
- Detailed timing/pacing parameters for reconciliation throttling
- Interaction with `kafka-reassign-partitions.sh` - should it be blocked for managed-RF topics?
