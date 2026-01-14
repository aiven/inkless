# Diskless-Managed Replication Factor

> **Alternative design under review:** [DISKLESS_MANAGED_RF_SIMPLIFIED.md](DISKLESS_MANAGED_RF_SIMPLIFIED.md) proposes a simpler approach using transformer-first availability instead of controller auto-reassignment. Review both before deciding.

## Table of Contents

1. [Purpose](#purpose)
2. [Objectives](#objectives)
3. [Activation Model](#activation-model-binary-version)
   - [Existing Diskless Topics](#existing-diskless-topics)
4. [Placement Model](#placement-model)
5. [Controller Behavior](#controller-behavior)
   - [Topic Creation](#topic-creation)
   - [Add Partitions](#add-partitions)
   - [Standard Operations](#standard-operations-after-creation)
6. [Broker Behavior](#broker-behavior)
   - [No Replica Fetcher for Diskless Topics](#no-replica-fetcher-for-diskless-topics)
7. [Metadata Transformation](#metadata-transformation)
8. [Observability](#observability)
9. [Implementation Path](#implementation-path) *(9 weeks / 1 eng, 5-6 weeks / 2 eng)*
   - [Phase 0: Research and Validation](#phase-0-research-and-validation)
   - [Phase 1: Topic Creation with Rack-Aware Placement](#phase-1-topic-creation-with-rack-aware-placement)
   - [Phase 2: Transformer Changes](#phase-2-transformer-changes)
   - [Phase 3: Add Partitions Support](#phase-3-add-partitions-support)
   - [Phase 4: Offline Replica Auto-Reassignment](#phase-4-offline-replica-auto-reassignment)
   - [Phase 5: Observability](#phase-5-observability)
   - [Testing Strategy](#testing-strategy)
   - [Future Work: RLM Integration](#future-work-rlm-integration)
10. [Rejected Alternatives](#rejected-alternatives)
11. [Appendix: Topic Migration Interactions](#appendix-topic-migration-interactions)
12. [Open Items](#open-items)

---

## Purpose

### Current State: RF=1 with Transformer Override

Current diskless topics use **RF=1 in KRaft** with the transformer routing requests to any alive broker. This was a deliberate decision that enabled:
- Fast iteration without controller changes
- Simple implementation
- Flexibility in routing

**This has worked well for pure diskless topics.**

### The Question: What Does Topic Migration Require?

We need to support **bidirectional topic migration**:
1. **Classic → Diskless** (immediate need)
2. **Diskless → Classic** (future need)

### Why Real Replicas Matter

| Capability                    | RF=1 (current)              | RF=rack_count (this design)  |
|-------------------------------|-----------------------------|-----------------------------|
| Topic migration coordination  | Custom coordinator needed   | Leader coordinates          |
| RLM integration               | Custom hooks (~11 weeks)    | Standard (~0 weeks)         |
| Background job assignment     | Racing/randomized           | Deterministic (leader)      |
| Diskless → Classic migration  | RF expansion + migration    | Migration only              |
| Operational debugging         | "Which broker ran this?"    | "Leader of partition X"     |

### What This Feature Enables

| Capability                | How                                    | Status         |
|---------------------------|----------------------------------------|----------------|
| Rack-aware placement      | One replica per AZ at creation         | ✅ This design |
| Topic migration           | Real replicas to migrate to            | ✅ This design |
| RLM integration           | Stable partition leader                | 🔜 Future      |
| Standard Kafka operations | Reassignment, election work normally   | ✅ This design |
| Deterministic job owner   | Leader owns partition operations       | ✅ This design |

### Related Documents

- [DESIGN.md](DESIGN.md) — Overall tiered storage unification design
- [DISKLESS_MANAGED_RF_SIMPLIFIED.md](DISKLESS_MANAGED_RF_SIMPLIFIED.md) — Simplified variant (transformer-first availability)

---

## Objectives

Enable **rack-aware, stable KRaft-managed replicas** for diskless topics:

- **KRaft-managed replicas**: Diskless topics will have actual KRaft-managed replicas, not the current "faked" RF=1 metadata. These replicas are tracked by the controller, participate in leader election, and appear in standard Kafka tooling. The key difference from classic replicas: **they don't replicate data** (object storage handles durability), but they do provide partition assignments for RLM integration and operational consistency. See [No Replica Fetcher for Diskless Topics](#no-replica-fetcher-for-diskless-topics) for implementation details.
- **Rack-aware at creation**: Topics are created with one replica per rack (RF = rack count)
- **Standard operations after creation**: Once created, diskless topics use standard Kafka replica management. The only automatic action is **offline replica reassignment** to preserve availability (see [Existing Diskless Topics](#existing-diskless-topics)).
- **Controller-managed RF**: Users don't specify RF at creation; the controller computes it from rack topology. Requests with `replicationFactor=-1` or `replicationFactor=1` are accepted for compatibility (both result in RF=rack_count).
- **Leader-agnostic produce and consume**: Keep the diskless model where any replica can accept writes and serve reads, while still having a KRaft leader for controller duties (e.g., future RLM coordination)

**Non-goal:** 
Topic migration mechanics (Tiered Classic → Diskless), including sealing and offset boundary tracking.
Those are covered in [DESIGN.md](DESIGN.md).

---

## Activation Model (Binary Version)

Managed RF is activated by **deploying a new binary version**. No feature flags, no topic attributes, no server configs. See [Rejected Alternative: Feature Flag Activation](#rejected-alternative-feature-flag-activation) for why we chose this simpler approach.

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

### Mixed Binary Compatibility

If a new controller updates partition metadata (e.g., RF=3 with replicas in multiple racks) while some brokers still run the old binary:

- **Data plane is unaffected**: Produce and consume APIs are not modified. Old brokers continue serving diskless requests via existing `AppendHandler`/`FetchHandler`.
- **Metadata plane differs**: Old brokers use transformer hashing, new brokers filter KRaft metadata. Clients may see different leader/replicas depending on which broker they query.
- **No correctness issues**: Diskless data lives in object storage. Any broker can serve any partition regardless of what metadata says.

This temporary inconsistency resolves once all brokers run the new binary. In our deployment model (new VMs), this period is brief or non-existent.

### Cluster Migration

In our deployment model, new binaries are deployed on **new VMs with new broker IDs** (not in-place rolling upgrade). This simplifies the transition:

**Before migration:**
- Old cluster with old broker IDs
- Diskless topics have RF=1 pointing to old broker IDs
- Transformer uses hash-based leader selection

**After migration:**
- New cluster with new broker IDs
- Controller detects orphaned replicas (old broker IDs not in cluster)
- Auto-migration expands RF to rack_count with new broker IDs
- Transformer filters KRaft metadata by client AZ

**Client behavior:**
- Clients reconnect to new cluster via bootstrap servers
- Metadata requests return new broker IDs with rack-aware placement
- No inconsistency period (unlike rolling upgrade)

### Existing Diskless Topics

Existing diskless topics created before this feature have RF=1 in KRaft. These need to be expanded to RF=rack_count.

**Migration approach: Automatic migration for orphaned replicas**

Legacy diskless topics were created with RF=1 pointing to a broker that existed at creation time. In our deployment model, **new binaries are deployed on new VMs with new broker IDs**. This means legacy topics will have broker IDs that no longer exist in the cluster, allowing safe automatic detection.

#### Deployment Model Assumption

```
Old cluster (legacy diskless topics created here):
  Brokers: 1, 2, 3, 4, 5, 6

New cluster (after infrastructure migration):
  Brokers: 101, 102, 103, 104, 105, 106  ← new VMs, new IDs
  
Legacy topic in KRaft:
  Topic: foo, Partition 0, Replicas=[3]  ← broker 3 no longer exists
```

#### Detection Logic

```
IF diskless.enable=true 
   AND ANY replica broker is offline
THEN:
   IF broker ID is not registered in cluster metadata:
      # Legacy topic from old cluster
      Reassign ALL replicas, expand to RF=rack_count
   ELSE:
      # Broker exists but offline (failure/maintenance)
      Reassign ONLY offline replica(s), preserve current RF
```

**Key distinction:**
- **Not registered:** Broker ID was never seen by this cluster (e.g., old cluster had brokers 1-6, new cluster has 101-106). This is a legacy topic that should be modernized.
- **Registered but offline:** Broker ID exists in cluster metadata but is currently unavailable. Operator chose this RF intentionally, so preserve it.

#### Why This Is Safe for Diskless

Auto-reassigning offline replicas is safe for diskless topics because:

| Property | Classic Topics | Diskless Topics |
|----------|----------------|-----------------|
| Data location | Local broker disk | Object storage (shared) |
| Reassignment cost | Data copy (slow) | Metadata only (instant) |
| Data loss risk | Yes (if out of sync) | No (all brokers see same data) |
| ISR meaning | Replication lag tracking | Meaningless (all always in sync) |

**This preserves "always available" semantics:**
- Current diskless: Transformer hashes to any alive broker → always available
- Managed RF diskless: Controller reassigns offline replicas to online brokers → always available

| Scenario | Broker Status | RF Change | Action |
|----------|---------------|-----------|--------|
| Legacy topic, cluster migration | Not registered | Expand to rack_count | Modernize to rack-aware |
| RF=3, one broker failed | Registered, offline | Keep RF=3 | Replace offline replica |
| RF=1, broker maintenance | Registered, offline | Keep RF=1 | Move to online broker |
| Classic topic | Any | N/A | No action (not diskless) |

#### Other Deployment Models (Broker IDs Overlap)

If the new cluster reuses broker IDs from the old cluster (e.g., in-place upgrade on same VMs):

**If old brokers are offline:** Auto-reassignment triggers (offline replica detection)
**If old brokers are online:** Topics work normally with existing placement

**For RF=1 topics that should be expanded to rack-aware:**
- Metric `kafka.controller.diskless.rf1_topics_total` tracks diskless topics with RF=1
- Log warning: `WARN Diskless topic 'foo' has RF=1; consider expanding to rack-aware placement`
- Operator can manually expand using `kafka-reassign-partitions.sh`

**Manual expansion to rack-aware:**

```bash
# Generate reassignment JSON (one replica per rack)
cat > reassignment.json << 'EOF'
{
  "version": 1,
  "partitions": [
    {"topic": "foo", "partition": 0, "replicas": [101, 103, 105]}
  ]
}
EOF

# Execute (instant for diskless - metadata only)
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json \
  --execute
```

#### Reassignment Process

When controller detects a diskless partition with offline replica(s):

**Case 1: Broker ID does not exist in cluster (e.g., cluster migration)**

This indicates the partition was created on an old cluster with different broker IDs. Expand to rack-aware placement.

1. Log: `INFO Detected diskless partition 'foo-0' with non-existent broker (broker 3 not registered)`
2. Compute target: one broker per rack (RF=rack_count)
3. Issue `AlterPartitionReassignments` to set new replicas
4. Log: `INFO Reassigned diskless partition 'foo-0' to RF=3, replicas=[101,103,105]`

**Case 2: Broker ID exists but is offline (e.g., broker failure, maintenance)**

This indicates a temporary or permanent broker outage. Preserve current RF, just replace offline replica.

1. Log: `INFO Detected diskless partition 'foo-0' with offline replica (broker 101)`
2. Find online broker in same rack as offline broker (preserve rack-awareness)
3. If no broker in same rack: pick any online broker (preserve RF, log warning about rack-awareness)
4. Issue `AlterPartitionReassignments` to replace offline replica
5. Log: `INFO Reassigned diskless partition 'foo-0' replica: broker 101 → 102`

**Summary:**

| Condition | RF Change | Rationale |
|-----------|-----------|-----------|
| Broker not registered in cluster | Expand to rack_count | Legacy topic from old cluster, modernize |
| Broker registered but offline | Keep current RF | Operator's RF choice respected |

**This is metadata-only** — no data movement required (data is in object storage).

#### Example

```
Legacy topic (created on old cluster):
  Topic: foo (diskless.enable=true)
  Partition 0: Replicas=[3], Leader=3, ISR=[3]
                        ↑
                        Broker 3 was valid on old cluster

New cluster brokers: [101, 102, 103, 104, 105, 106] in racks [A, A, B, B, C, C]
                      ↑
                      Broker 3 doesn't exist anymore

After auto-migration:
  Topic: foo (diskless.enable=true)  
  Partition 0: Replicas=[101,103,105], Leader=101, ISR=[101,103,105]
                        ↑    ↑    ↑
                        One broker per rack (A, B, C)
```

#### When Auto-Reassignment Runs

- **Trigger:** Controller startup, metadata change, or periodic scan
- **Pacing:** Batched to avoid overwhelming controller (configurable)
- **Idempotent:** Re-running when no offline replicas exist is a no-op

#### Observability

**Auto-reassignment of offline replicas:**
- Log: `INFO Detected diskless partition 'foo-0' with offline replica (broker 101)`
- Log: `INFO Reassigned diskless partition 'foo-0' replica: broker 101 → 102`
- Metric: `kafka.controller.diskless.replicas_reassigned_total` (counter)
- Metric: `kafka.controller.diskless.offline_replicas_total` (gauge) — pending reassignment

**RF=1 topics (informational):**
- Log: `WARN Diskless topic 'foo' has RF=1; consider expanding to rack-aware placement`
- Metric: `kafka.controller.diskless.rf1_topics_total` (gauge) — topics with RF=1

#### Rollback

If rolled back to old binary:
- Expanded RF is preserved in KRaft metadata
- Old brokers ignore KRaft RF and use transformer hashing
- No harm — metadata is unused until re-upgraded
- Re-upgrade will see RF > 1 and skip migration (idempotent)

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
- Manual replica assignments are rejected (same as topic creation)

This ensures consistency: all partitions of a diskless topic use rack-aware placement.

**Implementation note:** Currently `ReplicationControlManager.createPartitions()` uses the standard replica placer without diskless-specific handling. This needs to be updated to:
1. Detect diskless topics via `diskless.enable=true` config
2. Use rack-aware placement instead of standard placer
3. Reject manual replica assignments

### Standard Operations (After Creation)

After topic creation, diskless topics behave like classic Kafka topics for all operations:

**Leader Election:**
- Standard Kafka leader election from ISR
- For diskless topics, all replicas are always in ISR (data is in object storage)
- `kafka-leader-election.sh` works normally
- Leader can be any replica in ISR (shuffled on election)

**Important:** This is unchanged from current diskless behavior. For diskless topics with AZ-aware clients, leader election doesn't force cross-AZ traffic. The transformer returns the local replica to each client regardless of who the KRaft leader is. The KRaft leader is primarily for controller coordination (e.g., future RLM tasks), not for directing client traffic.

**Broker Failure:**
- Replicas on failed broker become offline
- Controller detects offline replica and **auto-reassigns** to online broker (preserving RF and rack-awareness when possible)
- Leader fails over if leader was on failed broker
- Partition remains available (unlike classic topics which would be under-replicated)
- If original broker returns later, no automatic action — new assignment is kept

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
- Replicas on that rack become offline
- Controller auto-reassigns to online brokers in other racks (preserving RF)
- Rack-awareness may be temporarily broken (logged as warning)
- Operator can manually rebalance to restore rack-awareness if desired

**Note:** These are edge cases that rarely happen in practice. Auto-reassignment ensures availability; rack-awareness restoration is optional.

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

**Implementation:** ISR is stored in KRaft as all assigned replicas. At topic creation, controller sets `ISR = replicas`. No `AlterPartition` requests are needed since there's no lag tracking — replicas don't fetch from each other.

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

#### No Local Partition Objects — Implications

**Current behavior:** `ReplicaManager` skips `getOrCreatePartition()` for diskless topics, meaning:
- No `Partition` objects on brokers
- No `UnifiedLog` objects
- No local log directories

**This raises questions:**

1. **How does leader election work without `Partition` objects?**
   
   Leader election is handled by the KRaft controller, not broker `Partition` objects. The controller updates partition metadata (leader, ISR) in KRaft records. Brokers observe these changes via metadata updates but don't need local `Partition` objects to "become" leader — they just serve requests for partitions where metadata says they're a replica.

2. **How do brokers know they should serve requests for a partition?**
   
   Currently: `InklessTopicMetadataTransformer` returns metadata pointing clients to brokers. Brokers check `isDisklessTopic()` and route to Inkless handlers (`AppendHandler`, `FetchHandler`) instead of local log.
   
   With managed RF: Same flow, but transformer filters KRaft metadata instead of computing synthetic placement.

3. **What about RLM integration?**
   
   RLM requires `Partition` and `UnifiedLog` objects to:
   - Track local vs. tiered segment boundaries
   - Coordinate segment uploads
   - Run expiration tasks
   
   **This is out of scope for this design.** RLM integration will be addressed separately when implementing the tiering pipeline (see [DESIGN.md](DESIGN.md)). At that point, we may need to create `Partition` objects for the leader, but this can be deferred.

4. **Can we avoid `Partition` objects entirely?**
   
   For the initial managed RF implementation: **yes**. We only need:
   - KRaft metadata (replicas, leader, ISR)
   - Transformer filtering
   - Inkless handlers (already working)
   
   `Partition` objects become necessary when:
   - Implementing local disk cache (future)
   - Integrating with RLM for tiering (future)
   
   **Recommendation:** Start without `Partition` integration. Add it when implementing local cache or RLM integration.

#### Research Needed

These items are covered in [Phase 0: Research and Validation](#phase-0-research-and-validation):
1. Leader election works correctly without broker `Partition` objects (test with existing diskless topics)
2. ISR updates in KRaft don't require broker-side `Partition` state
3. `DescribeTopics` / `ListOffsets` work correctly for diskless topics with RF > 1

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
1. Check if KRaft-assigned replicas exist in the cluster (are alive)
2. If replicas exist:
   - If partition has replica in `clientAZ`: return only that local replica as leader/replicas/ISR
   - If no replica in `clientAZ`: return full replica set (standard Kafka behavior — client talks to leader)
3. If replicas are orphaned (none exist in cluster): **fall back to legacy hash-based selection**

**Why the orphaned fallback is needed:**

During cluster migration, there's a brief window between:
1. New cluster starts with restored KRaft metadata (contains old broker IDs)
2. Controller completes auto-migration to new broker IDs

In this window, legacy topics have `Replicas=[3]` where broker 3 no longer exists. Without the fallback:
- New transformer would return empty/invalid metadata
- Partition would be effectively offline for clients

The fallback ensures partitions remain available during this migration window (typically seconds to minutes). Once auto-migration completes, the fallback is no longer triggered.

**Note:** This is consistent with current diskless behavior. Today, the transformer returns one replica based on hash as a "fake" leader. With managed RF, we have one real replica per AZ and return the local one. The orphaned fallback preserves availability only during the migration window.

**Detection:**
- New binary always uses KRaft-placement-based projection for diskless topics
- Check `diskless.enable=true` topic config to identify diskless topics

### Unclean Leader Election

For diskless topics, `unclean.leader.election.enable=true` can be safely enabled — there is no data loss risk since all data is in object storage.

However, **auto-reassignment of offline replicas** (not unclean election) is our primary availability mechanism. See [Rejected Alternative E: Unclean Leader Election for Availability](#rejected-alternative-e-unclean-leader-election-for-availability) for the full analysis.

---

## Observability

### Metrics

**Controller metrics (all prefixed `kafka.controller.diskless.`):**
- `effective_rf{topic}` - RF assigned at creation
- `rack_aware{topic,partition}` - 1 if one-per-rack, 0 if not
- `rack_aware_partitions_total{topic}` - Count of rack-aware partitions
- `non_rack_aware_partitions_total{topic}` - Count of non-rack-aware partitions
- `rf1_topics_total` - Count of diskless topics with RF=1
- `replicas_reassigned_total` - Count of replicas auto-reassigned due to offline broker
- `offline_replicas_total` - Count of replicas pending reassignment

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

**Total Estimate: 9 weeks with 1 engineer, or 5-6 weeks with 2 engineers**

### Phase 0: Research and Validation

Verify assumptions before implementation:

1. Leader election works correctly without broker `Partition` objects (test with existing diskless topics)
2. ISR updates in KRaft don't require broker-side `Partition` state
3. `DescribeTopics` / `ListOffsets` work correctly for diskless topics with RF > 1

**Estimate:** 1 week

### Phase 1: Topic Creation with Rack-Aware Placement

1. Modify `ReplicationControlManager` to detect `diskless.enable=true` topics
2. Compute RF = rack count at creation time
3. Implement one-per-rack broker selection
4. Reject `replicationFactor > 1` and non-empty `replicaAssignments`

**Estimate:** 2 weeks

### Phase 2: Transformer Changes

1. Update `InklessTopicMetadataTransformer` to read KRaft placement
2. Implement AZ filtering logic
3. **Add orphaned replica fallback** — if KRaft replicas don't exist, fall back to legacy hash-based selection
4. Remove synthetic hashing calculation (but keep as fallback path)

**Estimate:** 2 weeks

### Phase 3: Add Partitions Support

1. Apply same one-per-rack logic when adding partitions
2. Handle case where rack count changed since topic creation

**Estimate:** 1 week

### Phase 4: Offline Replica Auto-Reassignment

1. Implement offline replica detection (any diskless partition with offline broker)
2. Add auto-reassignment logic in controller:
   - Broker ID not registered in cluster → expand to RF=rack_count (legacy topic modernization)
   - Broker ID registered but offline → keep current RF, replace offline replica (same rack if possible)
3. Add pacing controls (batch size, interval)
4. Add metrics: `replicas_reassigned_total`, `offline_replicas_total`, `rf1_topics_total`

**Estimate:** 2 weeks (increased due to broader scope)

### Phase 5: Observability

1. Add `rack_aware`, `rack_aware_partitions_total`, `non_rack_aware_partitions_total` metrics
2. Add warning logs for non-rack-aware placement and RF=1 topics
3. Documentation

**Estimate:** 1 week

### Summary

| Phase | Scope | Estimate |
|-------|-------|----------|
| 0 | Research and Validation | 1 week |
| 1 | Topic Creation with Rack-Aware Placement | 2 weeks |
| 2 | Transformer Changes | 2 weeks |
| 3 | Add Partitions Support | 1 week |
| 4 | Offline Replica Auto-Reassignment | 2 weeks |
| 5 | Observability | 1 week |
| **Total** | | **9 weeks (1 eng) / 5-6 weeks (2 eng)** |

This includes the offline replica auto-reassignment which preserves "always available" semantics.

### Testing Strategy

**Unit Tests:**
- `ReplicationControlManager`: rack-aware placement logic, RF computation, validation
- `InklessTopicMetadataTransformer`: AZ filtering, cross-AZ fallback
- Migration detection: orphaned replica identification

**Integration Tests:**
- Topic creation with `diskless.enable=true` results in RF=rack_count
- Add partitions uses rack-aware placement
- Existing topics with orphaned replicas are auto-migrated
- Reassignment works and logs warnings for non-rack-aware placement
- No replica fetcher threads started for diskless topics with RF > 1
- **Orphaned replica fallback**: Transformer returns valid broker when KRaft replicas don't exist
- **Offline replica auto-reassignment**: Broker goes offline → replica reassigned to online broker
- **Legacy topic modernization**: Broker ID not in cluster → RF expanded to rack_count

**System Tests:**
- Multi-AZ cluster with diskless topics
- Client AZ awareness: verify clients talk to local replica
- Broker failure: verify leader election and continued availability
- Rolling restart: verify no disruption

**Existing Test Coverage:**
See [DISKLESS_INTEGRATION_TEST_COVERAGE.md](../../DISKLESS_INTEGRATION_TEST_COVERAGE.md) for current Inkless test coverage. Managed RF tests should extend this framework.

### Future Work: RLM Integration

This design enables RLM integration but doesn't implement it. Key considerations for future RLM work:

**Why RLM needs managed RF:**
- RLM's `onLeadershipChange()` is the entry point for tiering tasks
- Requires KRaft-managed partition leadership (not faked)
- Managed RF provides this foundation

**What RLM integration will require:**
1. **Partition objects on leader:** RLM uses `Partition` and `UnifiedLog` APIs. The leader broker may need to create lightweight `Partition` objects for diskless topics.
2. **Segment boundary tracking:** RLM needs to know which offsets are local vs. tiered. For diskless, "local" means Control Plane (PostgreSQL), "tiered" means S3 segments.
3. **Expiration coordination:** Leader runs RLM expiration tasks to clean up tiered segments.

**Design hints for RLM integration:**
- Create `Partition` objects only on the leader, not followers
- `UnifiedLog` can be a thin wrapper that delegates to Inkless handlers
- Alternatively, implement RLM hooks directly without full `Partition` — needs investigation

**This is out of scope for managed RF** but the decisions here (KRaft-managed replicas, stable leader election) provide the foundation. See [DESIGN.md](DESIGN.md) Stream 7 for tiering pipeline details.

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

**Why we initially considered it:**
- Upstream Kafka uses feature flags for backward-compatible feature rollout (e.g., `EligibleLeaderReplicasVersion`)
- Allows gradual activation: deploy new binary first, then enable feature cluster-wide
- Provides explicit opt-in, reducing risk of unexpected behavior changes
- Supports mixed-version clusters where some brokers have the feature and others don't

**What it would have involved:**
- Policy flag (`disklessManagedRF`) as new KRaft record
- Feature enum (`DisklessVersion`) following `EligibleLeaderReplicasVersion` pattern
- Server config (`diskless.managed.rf.enable`) requiring cluster-wide coordination
- Manual activation via `kafka-features.sh --upgrade --feature diskless.version=1`

**Why we dropped it:**
- **Inkless deployment model is different**: We deploy new binaries on new VMs, not rolling upgrades on existing clusters. There's no mixed-version period to manage.
- **`diskless.enable` already gates behavior**: Topics must explicitly opt into diskless mode. The feature flag would be redundant.
- **Operational overhead without benefit**: Adds a manual activation step that doesn't provide additional safety in our deployment model.
- **Simpler rollback**: Binary rollback is sufficient; no need to coordinate feature flag state.

**When to reconsider:**
- If upstreaming to Apache Kafka (strict backward compatibility requirements)
- If needing per-topic opt-in/opt-out for managed RF specifically
- If supporting long-lived mixed-version clusters (not our deployment model)

### Rejected Alternative: Dynamic Reconciliation

**Concept:** Controller continuously reconciles replica placement to maintain one-per-rack as topology changes.

**Why we initially considered it:**
- Ensures rack-awareness is always maintained, even after topology changes
- Automatically adapts RF when racks are added (more availability) or removed (avoid under-replication)
- Reduces operational burden — no manual reassignment needed
- Provides "self-healing" behavior similar to Kubernetes operators

**What it would have involved:**
- Reconciliation loop monitoring rack changes
- Automatic RF adjustment when racks added/removed
- Drift detection and auto-fix when operator breaks rack-awareness
- Rack liveness state machine (HEALTHY → DEGRADED → UNAVAILABLE) to distinguish transient failures from permanent rack loss

**Why we dropped it:**
- **Significant complexity**: Adds new controller component with its own state machine, edge cases, and failure modes
- **Divergent from Kafka norms**: Standard Kafka doesn't auto-adjust RF or auto-reassign. Operators expect explicit control.
- **Rare scenarios**: Topology changes (adding/removing racks) are infrequent in practice. Optimizing for rare cases adds constant complexity.
- **Existing tools work**: `kafka-reassign-partitions.sh` handles all cases. Operators already know this workflow.
- **Harder to reason about**: Auto-reconciliation can surprise operators. "Why did my partition move?" is a common complaint with auto-rebalancing systems.

**When to reconsider:**
- If customers frequently add/remove racks and manual reassignment becomes burdensome
- If we build a broader "Kafka operator" that manages cluster topology holistically

### Rejected Alternative A: Virtual Leader for RLM Tasks

**Concept:** Keep RF=1 (current faked metadata), but designate a deterministic "virtual leader" per partition for RLM tasks.

**Why we considered it:**
- Avoids changing the current RF=1 model that works for produce/consume
- RLM only needs *some* broker to run expiration tasks — could be a "virtual" designation
- Simpler than managing real replicas if we can make RLM work with virtual leadership

**Why it fails:**
- **Two sources of leadership**: KRaft has one leader (faked), RLM needs another (virtual). Confusing for operators and tooling.
- **Failover complexity**: Virtual leader failover requires new coordination mechanism outside KRaft. What happens when the virtual leader dies?
- **RLM assumptions**: RLM code assumes real `Partition` objects with `UnifiedLog`. Significant refactoring needed to work with virtual concept.
- **Tiering pipeline needs `ReplicaManager`**: The merge phase that creates tiered segments needs broker-side context that only exists with real partitions.

### Rejected Alternative B: Control Plane Manages Tiered Expiration

**Concept:** Extend PostgreSQL (Control Plane) to track tiered segment metadata and run expiration directly, bypassing RLM.

**Why we considered it:**
- Control Plane already tracks batch metadata — could extend to track tiered segments
- Avoids needing KRaft-managed replicas entirely
- Keeps all Inkless metadata in one place (PostgreSQL)

**Why it fails:**
- **Contradicts PG scalability goal**: The whole point of tiering is to *reduce* PG load. Adding more metadata to PG defeats the purpose.
- **Duplicates RLM logic**: Retention policies (`retention.ms`, `retention.bytes`) are already implemented in RLM. Reimplementing in Control Plane doubles the code and bugs.
- **Breaks tooling**: Kafka admin tools expect RLM for tiered storage management. Custom Control Plane expiration wouldn't integrate.
- **Cross-system consistency**: Tiered segments in S3, metadata in PG, Kafka expecting RLM — three systems to keep consistent. Recipe for orphaned data.

### Rejected Alternative C: Direct Tiered Read via FetchHandler

**Concept:** Extend `FetchHandler` to read tiered storage (S3 segments) directly, bypassing RLM read path.

**Why we considered it:**
- `FetchHandler` already serves diskless reads from object storage — could extend to tiered segments
- Avoids RLM dependency for reads
- Potentially simpler than full RLM integration

**Why it fails:**
- **Only solves reads**: Expiration (the main RLM value) still needs a solution. Must combine with Alternative A or B, inheriting their problems.
- **Duplicates index handling**: RLM maintains indexes for tiered segments. `FetchHandler` would need to duplicate this or depend on RLM indexes anyway.
- **Partial solution**: Doesn't address the core problem (needing KRaft leadership for RLM). Just moves complexity around.

### Rejected Alternative D: Treat Tiered Data as Read-Only Archival

**Concept:** Freeze tiered portion as read-only archive, use S3 lifecycle policies for expiration instead of RLM.

**Why we considered it:**
- S3 lifecycle policies are simple and battle-tested
- Avoids RLM complexity entirely for expiration
- "Archival" use case doesn't need sophisticated retention

**Why it fails:**
- **No programmatic retention**: S3 lifecycle can't implement `retention.ms` or `retention.bytes` based on Kafka semantics. Can only do "delete after N days" globally.
- **Topic deletion broken**: Deleting a Kafka topic should clean up tiered data. S3 lifecycle doesn't know about Kafka topics.
- **Doesn't solve PG scalability**: The goal is to tier *new* diskless data to reduce PG load. This alternative only addresses old/archived data.
- **User expectations**: Users expect Kafka retention semantics to work. "Your retention.ms doesn't apply to tiered data" is a poor user experience.

### Rejected Alternative E: Unclean Leader Election for Availability

**Concept:** Enable `unclean.leader.election.enable=true` for diskless topics to ensure availability when replicas go offline.

**Why we considered it:**
- For diskless topics, there is no data loss risk from unclean election (all data is in object storage)
- ISR membership is a metadata concept, not a data consistency concept
- Simple configuration change, no new code required
- Standard Kafka mechanism

**Why it's not the primary solution:**
- **Doesn't work for RF=1**: Unclean election needs multiple replicas to elect from. With RF=1, there's no alternative replica.
- **Reactive, not proactive**: Waits for election to happen rather than proactively ensuring availability
- **Doesn't preserve rack-awareness**: Elects from existing (possibly degraded) replica set

**What we do instead:**
Auto-reassignment of offline replicas is more powerful:
- Works for any RF (including RF=1)
- Proactively moves replica to online broker
- Preserves rack-awareness when possible
- Metadata-only operation (instant)

**Note:** Unclean leader election *can* be enabled for diskless topics (no downside), but auto-reassignment is the primary availability mechanism.

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
- Controller adjusts to target placement as part of the migration process
- Uses standard add-then-remove approach
- Paced to avoid disruption

**Note:** This is a one-time adjustment during topic migration, not ongoing reconciliation.

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

**Resolved in this design:**
- ~~Transformer fallback behavior~~ → Return full replica set (standard Kafka behavior)
- ~~ISR storage~~ → Stored in KRaft as all replicas
- ~~`broker.rack` config changes~~ → Use existing reassignment tooling to fix placement if needed (rare edge case)
- ~~Re-rack-aware tooling~~ → Use existing `kafka-reassign-partitions.sh` (no new tooling needed)

**Research (Phase 0):**
- See [Phase 0: Research and Validation](#phase-0-research-and-validation)
