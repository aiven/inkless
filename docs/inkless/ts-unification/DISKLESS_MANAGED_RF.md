# Diskless-Managed Replication Factor

## Table of Contents

1. [Purpose](#purpose)
2. [Objectives](#objectives)
3. [Design: Transformer-First Availability](#design-transformer-first-availability)
4. [Activation Model](#activation-model-binary-version)
5. [Placement Model](#placement-model)
6. [Controller Behavior](#controller-behavior)
7. [Broker Behavior](#broker-behavior)
8. [Metadata Transformation](#metadata-transformation)
9. [Observability](#observability)
10. [Implementation Path](#implementation-path) *(6 weeks / 1 eng, 4 weeks / 2 eng)*
11. [Rejected Alternatives](#rejected-alternatives)
12. [Appendix](#appendix)
    - [Ghost Replicas Problem](#ghost-replicas-problem)
    - [Operational Benefits Summary](#operational-benefits-summary)
    - [Leadership Benefits for Operations](#leadership-benefits-for-operations)

---

## Purpose

### Current State: RF=1 with Transformer Override

Current diskless topics use **RF=1 in KRaft** with the transformer routing requests to any alive broker. The single replica may point to an **offline or non-existent broker** — the transformer ignores KRaft metadata entirely and hashes to any alive broker.

```
Current diskless:
  KRaft: Partition 0 → Replicas=[1], Leader=1  ← may be offline/stale
  Transformer: Ignores KRaft, hashes to any alive broker
```

This was a deliberate decision that enabled:
- Fast iteration without controller changes
- Simple implementation
- "Always available" semantics (transformer routes around any failure)

**This has worked well for pure diskless topics.**

### The Question: What Does Topic Migration Require?

We need to support **bidirectional topic migration**:
1. **Classic → Diskless** (immediate need)
2. **Diskless → Classic** (future need)

The key question: **how does each approach handle both directions?**

We evaluate three approaches (A, B, C) below, starting with reverse migration requirements. The proposed design (Approach C) is then detailed in the rest of this document.

### Approach Comparison

**Terminology:**
- **Offline replica**: KRaft shows replica on broker X, but broker X doesn't exist or is down. No `Partition` object.
- **Ghost replica**: KRaft shows replica on broker X, broker X is alive, but `ReplicaManager` skips creating `Partition` object for diskless. Broker doesn't know it's a replica.
- **Real (metadata-only) replica**: KRaft shows replica on broker X, broker X recognizes it as a replica, but no local log (data in object storage).

#### Summary Table

| Aspect                  | A: Shrink to RF=1        | B: Keep RF=3, fake       | C: RF=rack_count (proposed) |
|-------------------------|--------------------------|--------------------------|------------------------------|
| KRaft RF after migrate  | 1                        | 3 (unchanged)            | 3 (managed, rack-aware)      |
| Transformer behavior    | Hash to any broker       | Hash to any broker       | Filter by AZ (KRaft-sourced) |
| Replica objects         | None (offline in KRaft)  | Ghost (no `Partition`)   | Real (metadata-only)         |
| Migration coordinator   | Custom (who?)            | Custom (who?)            | **Leader**                   |
| RLM integration         | Custom hooks (~11 wks)   | Custom hooks (~11 wks)   | **Standard (~0 wks)**        |
| Job assignment          | Racing                   | Racing                   | **Leader-based**             |
| Reverse migration ready | No (must expand RF)      | No (must fix ghosts)     | **Yes**                      |

#### Approach A: Shrink to RF=1 During Migration

```
Classic: Replicas=[1,2,3], Leader=1, data on all brokers
         ↓ migration starts
Shrink:  Replicas=[1], Leader=1  ← When? Who coordinates?
         ↓
Diskless: Replicas=[1], data in object storage
```

**Problems:**
- Timing of RF shrink is complex (before seal? during? after?)
- Single point of failure during migration
- Loses rack-awareness — original placement lost, must recreate for reverse migration
- **Reverse migration requires RF expansion first** (two operations)

#### Approach B: Keep Classic RF=3, Continue Faking

```
Classic:  Replicas=[1,2,3], Leader=1, data on all brokers
          ↓ migration starts (no RF change!)
Diskless: Replicas=[1,2,3], Leader=1, data in object storage
          ↑ KRaft unchanged, transformer ignores and hashes
```

What "ghost replica" means:
- KRaft says broker is a replica with ISR membership
- But `ReplicaManager.isDisklessTopic()` skips `getOrCreatePartition()`
- No `Partition` object, no fetcher, no local log
- Broker doesn't know it's a "replica" — just serves requests via Inkless handlers
- ISR never shrinks (no lag tracking for diskless)

**Benefit:** If users migrate back to classic, they keep the same partition assignments.

**Problems:**
- **RLM still blocked** — `onLeadershipChange()` needs `Partition` objects to run tiering tasks. Ghost replicas don't have them. Adding conditional `Partition` creation for diskless would be similar complexity to the proposed approach.
- **Reverse migration requires "un-ghosting"** — must create real `Partition` objects before migrating back
- Misleading tooling output (`describe topic` shows healthy replicas that aren't real) — operational burden on operators
- See [Ghost Replicas Problem](#ghost-replicas-problem)

#### Approach C: RF=rack_count with Real Replicas (Proposed)

```
Classic:  Replicas=[1,2,3], Leader=1, data on all brokers
          ↓ migration starts
Diskless: Replicas=[1,3,5], Leader=1, data in object storage
          ↑ rack-aware placement, real metadata-only replicas
          Leader coordinates migration, RLM tasks, background jobs
```

What "real (metadata-only) replica" means:
- KRaft says broker is a replica with ISR membership
- Broker recognizes it's a replica for this partition
- No local log (data in object storage) — but could have `Partition` object for coordination
- Leader can run RLM tasks, coordinate migration, own background jobs

**Benefits:**
- **Leader coordinates migration** — deterministic owner
- **Standard RLM integration** — `onLeadershipChange()` works
- **Deterministic job assignment** — leader owns partition operations
- **Accurate tooling output** — replicas are real
- **Reverse migration ready** — already has RF=3 with real replicas

### Diskless → Classic Migration Readiness

| Aspect            | A: RF=1               | B: RF=3 faked           | C: RF=rack_count        |
|-------------------|-----------------------|-------------------------|-------------------------|
| Starting RF       | 1                     | 3 (ghost replicas)      | 3 (real replicas)       |
| Before migration  | Must expand RF=1 → RF=3| Must "un-ghost" replicas| **Ready as-is**         |
| Replica state     | Create from scratch   | Create `Partition` objects| Already correct        |
| Complexity        | High (2 operations)   | Medium (fix ghosts)     | **Low (just migrate)**  |

**Key insight:** Only Approach C is ready for reverse migration without additional work.

### Cost Analysis

#### Cost of Keeping RF=1/Faked (Approaches A and B)

| Aspect                    | Custom Solution Required           | Effort    |
|---------------------------|-----------------------------------|-----------|
| Tiering entry point       | Bypass `onLeadershipChange()`     | ~2 weeks  |
| Leader coordination       | Custom leader election for tiering| ~2 weeks  |
| Segment merging           | Custom coordination               | ~3 weeks  |
| RLM API compatibility     | Fake `Partition` objects or fork  | ~2 weeks  |
| Expiration tasks          | Custom expiration outside RLM     | ~2 weeks  |
| **Total custom work**     |                                   | **~11 weeks** |

#### Cost of Proposed Design (Approach C: RF=rack_count)

| Aspect                    | Standard Solution                 | Effort    |
|---------------------------|-----------------------------------|-----------|
| Rack-aware placement      | Modify `ReplicationControlManager`| ~2 weeks  |
| Transformer filtering     | Update `InklessTopicMetadataTransformer` | ~2 weeks  |
| Add partitions            | Same logic as creation            | ~1 week   |
| RLM integration           | Standard `onLeadershipChange()`   | ~0 weeks* |
| **Total work**            |                                   | **~6 weeks** |

*RLM integration comes "for free" once we have real leadership.

#### Decision Framework

| Factor                   | A: Shrink to RF=1        | B: Keep RF=3, fake       | C: RF=rack_count (proposed) |
|--------------------------|--------------------------|--------------------------|------------------------------|
| Short-term cost          | Lower                    | Lower                    | ~6 weeks                     |
| Classic → Diskless       | Custom coordination      | Custom coordination      | Standard replica handoff     |
| RLM integration          | Custom hooks (~11 wks)   | Custom hooks (~11 wks)   | Standard (~0 wks)            |
| Diskless → Classic       | RF expansion + migration | Un-ghost + migration     | Migration only               |
| Long-term maintenance    | Two code paths           | Two code paths           | One code path                |
| Kafka alignment          | Divergent                | Divergent                | Aligned                      |

#### Summary

```
Approach A/B: 0 weeks now + ~11 weeks later = 11+ weeks total
Approach C:   6 weeks now + 0 weeks later   = 6 weeks total
                                              ─────────────
                                              Savings: 5+ weeks
```

Plus, deferring creates technical debt:
- Two code paths for leadership (KRaft vs. custom)
- Custom tiering pipeline that diverges from Kafka
- Every new feature asks "does this work with faked RF?"

### Recommendation

**Implement the proposed design (Approach C: RF=rack_count)** because:
1. Topic migration (both directions) benefits from real replicas
2. RLM integration becomes standard rather than custom
3. One-time 6-week investment vs. 11+ weeks of custom work
4. Avoids accumulating technical debt
5. Aligns diskless with Kafka's replica model

### Related Documents

- [DESIGN.md](DESIGN.md) — Overall tiered storage unification design

---

## Objectives

Enable **rack-aware, stable KRaft-managed replicas** for diskless topics:

- **KRaft-managed replicas**: Diskless topics will have actual KRaft-managed replicas (RF = rack count)
- **Rack-aware at creation**: Topics are created with one replica per rack
- **Standard operations after creation**: Once created, standard Kafka replica management applies
- **Controller-managed RF**: Users don't specify RF; controller computes from rack topology
- **Leader-agnostic produce and consume**: Any replica can accept writes and serve reads
- **Always available via transformer**: Transformer ensures availability by routing to alive brokers

---

## Design: Transformer-First Availability

### The Insight

For diskless topics, **availability** and **metadata accuracy** can be decoupled:

| Concern           | Priority     | Who Handles       | Speed      |
|-------------------|--------------|-------------------|------------|
| Availability      | Critical     | Transformer       | Instant    |
| Metadata accuracy | Nice-to-have | Controller (lazy) | Eventually |

### How It Works

**Current diskless behavior (RF=1, faked):**
- Transformer hashes to any alive broker
- Partition is always available
- KRaft metadata is ignored for routing

**Proposed behavior (RF=rack_count, real replicas):**
- KRaft stores RF=rack_count with real broker IDs
- Transformer filters by client AZ from KRaft replicas
- **If assigned replica is offline → fall back to any alive broker in AZ**
- Partition is always available (same as today!)
- KRaft metadata may be temporarily stale (shows offline broker)

### Why This Works for Diskless

```
Classic topic:
  Data on broker X → client MUST talk to broker X
  Broker X offline → partition unavailable (until reassigned)

Diskless topic:
  Data in object storage → ANY broker can serve
  Broker X offline → transformer routes to broker Y
  Partition stays available (no reassignment needed!)
```

The transformer already has all the information it needs:
1. KRaft replica assignments (preferred brokers)
2. Alive brokers in cluster
3. Client AZ

It can make instant routing decisions without waiting for controller.

---

## Activation Model (Binary Version)

Managed RF is activated by **deploying a new binary version**. No feature flags, no topic attributes, no server configs.

### Behavior Summary

**Old binary:**
- Diskless topics use legacy "RF=1 / faked metadata" behavior
- Transformer calculates synthetic placement via hashing

**New binary:**
- Diskless topics use KRaft-managed placement (one replica per rack at creation)
- Transformer filters KRaft placement by client AZ
- **Falls back to alive brokers if assigned replicas are offline**

### Existing Diskless Topics

Existing diskless topics (RF=1) continue to work:

**Immediate availability:**
- Transformer sees RF=1 with potentially offline broker
- Falls back to hash-based selection (same as today)
- No downtime

**Eventual modernization (optional):**
- Controller can lazily detect RF=1 diskless topics
- Background task expands to RF=rack_count when convenient
- Not urgent — availability is already handled by transformer

**Manual modernization (alternative):**
- Operator uses `kafka-reassign-partitions.sh` to expand RF
- Same tooling as classic topics

---

## Placement Model

### Rack-Aware Placement at Creation

When a diskless topic is created:
- Controller determines current rack count from registered brokers
- RF is set to rack count (e.g., 3 racks → RF=3)
- One broker is selected per rack for each partition
- Broker selection within a rack uses load balancing (least loaded broker)

### Placement Is Static After Creation

Once a topic is created:
- RF does not automatically change if racks are added/removed
- Replica assignments don't automatically adjust
- Standard Kafka replica management applies

---

## Controller Behavior

### Topic Creation

When creating diskless topics (`diskless.enable=true`):
- Controller counts distinct racks from registered brokers
- RF = rack count
- One replica assigned per rack
- Reject `replicationFactor > 1` and manual assignments

### Add Partitions

New partitions use same one-per-rack logic as creation.

### Standard Operations (After Creation)

**No auto-reassignment on broker failure.** Transformer handles availability.

**Leader Election:**
- Standard Kafka leader election from ISR
- For diskless topics, all replicas are always in ISR

**Broker Failure:**
- Replicas on failed broker become offline in KRaft metadata
- **Transformer routes around offline broker** (instant)
- Partition remains available
- KRaft shows under-replicated (informational only for diskless)
- When broker returns, it's back in ISR immediately

**Reassignment:**
- `kafka-reassign-partitions.sh` works normally
- Operator can reassign replicas if desired
- **Not required for availability** — transformer handles routing

---

## Broker Behavior

- No replica fetcher threads for diskless topics
- No local `Partition` objects (for now — may add for RLM coordination later)
- Inkless handlers (`AppendHandler`, `FetchHandler`) serve requests

---

## Metadata Transformation

### Filtering Logic

```
FOR each diskless partition:
  assigned_replicas = KRaft replica set
  alive_replicas = assigned_replicas ∩ alive_brokers
  
  IF alive_replicas is not empty:
    # Normal case: use KRaft placement
    IF any alive_replica in clientAZ:
      RETURN local replica (AZ-aware routing)
    ELSE:
      RETURN all alive_replicas (cross-AZ fallback)
  ELSE:
    # All assigned replicas offline: fall back to hash
    RETURN hash-based selection from all alive brokers in clientAZ
```

### Key Properties

1. **Instant availability**: No waiting for controller
2. **AZ-aware when possible**: Uses KRaft placement if alive
3. **Graceful degradation**: Falls back to hash if needed
4. **No metadata dependency for availability**: Stale metadata doesn't cause downtime

---

## Observability

### Metrics

**Controller metrics:**
- `kafka.controller.diskless.effective_rf{topic}` - RF assigned at creation
- `kafka.controller.diskless.rack_aware{topic,partition}` - 1 if one-per-rack, 0 if not
- `kafka.controller.diskless.rf1_topics_total` - Legacy topics not yet modernized

**Transformer metrics:**
- `kafka.server.diskless.transformer.fallback_total` - Count of hash fallbacks
- `kafka.server.diskless.transformer.offline_replicas_routed_around` - Routing decisions

**Standard Kafka metrics:**
- `UnderReplicatedPartitions` - Will show diskless partitions with offline brokers
- Note: For diskless, "under-replicated" is informational, not critical

### Logs

```
INFO [Transformer] Diskless partition 'foo-0' has offline replica (broker 101), 
     routing to alive replica (broker 103)
     
WARN [Transformer] Diskless partition 'foo-0' has ALL replicas offline, 
     using hash-based fallback to broker 105
```

### Operational Notes

**`kafka-topics.sh --describe` may show stale info:**
```
Topic: foo  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 101  Replicas: 101,103,105  Isr: 101,103,105
                       ↑
                       Broker 101 may be offline, but transformer routes to 103
```

This is acceptable because:
1. Availability is not affected
2. Operator can see actual state via broker metrics
3. Eventually consistent (when broker returns or is reassigned)

---

## Implementation Path

**Total Estimate: 6 weeks with 1 engineer, or 4 weeks with 2 engineers**

### Phase 0: Research and Validation (1 week)

1. Leader election works without broker `Partition` objects
2. ISR updates don't require broker-side `Partition` state
3. `DescribeTopics` / `ListOffsets` work with RF > 1

### Phase 1: Topic Creation with Rack-Aware Placement (2 weeks)

1. Modify `ReplicationControlManager` to detect diskless topics
2. Compute RF = rack count at creation
3. Implement one-per-rack broker selection
4. Reject `replicationFactor > 1` and manual assignments

### Phase 2: Transformer Changes (2 weeks)

1. Update `InklessTopicMetadataTransformer` to read KRaft placement
2. Implement AZ filtering logic
3. Add offline replica routing — if KRaft replica offline, route to alive replica
4. Add hash fallback — if all replicas offline, use legacy hash
5. Add metrics for fallback tracking

### Phase 3: Add Partitions Support (1 week)

1. Apply same one-per-rack logic when adding partitions
2. Handle case where rack count changed since topic creation

### Summary

| Phase     | Scope                                    | Estimate |
|-----------|------------------------------------------|----------|
| 0         | Research and Validation                  | 1 week   |
| 1         | Topic Creation with Rack-Aware Placement | 2 weeks  |
| 2         | Transformer Changes (with fallback)      | 2 weeks  |
| 3         | Add Partitions Support                   | 1 week   |
| **Total** |                                          | **6 weeks (1 eng) / 4 weeks (2 eng)** |

---

## Rejected Alternatives

### Alternative A: Shrink RF During Migration

**Concept:** When migrating Classic → Diskless, shrink RF=3 to RF=1, then use current diskless model.

**Why considered:**
- Maintains consistency with current RF=1 diskless model
- No need to change existing diskless implementation

**Why rejected:**
1. Timing complexity (when does RF shrink happen?)
2. Loses rack-awareness — must recreate for reverse migration
3. **Reverse migration requires RF expansion** (two operations)
4. RLM integration still blocked
5. Racing job assignment continues

See [Approach Comparison](#approach-comparison) for details.

---

### Alternative B: Keep Classic RF, Continue Faking Metadata

**Concept:** Keep existing RF (e.g., RF=3) in KRaft but continue using transformer to fake metadata.

**Why considered:**
- Zero controller changes for migration
- Simplest migration path
- If users migrate back, they keep same partition assignments

**Why rejected:**
1. **Ghost replicas** — KRaft shows healthy replicas that aren't real
2. **RLM blocked** — `onLeadershipChange()` needs `Partition` objects. Adding them would be similar complexity to proposed approach.
3. **Reverse migration requires "un-ghosting"** replicas
4. Misleading tooling output
5. Racing job assignment continues

See [Ghost Replicas Problem](#ghost-replicas-problem) for details.

---

### Alternative C: Controller Auto-Reassignment

**Concept:** Controller proactively detects offline replicas and reassigns to online brokers immediately.

**Why considered:**
- KRaft metadata always accurate
- Standard tooling shows correct state

**Why rejected:**
- **3 weeks additional complexity** for something transformer already handles
- **Instant vs eventually consistent** — transformer is faster
- **Consistent with today** — diskless already uses transformer for availability
- **Fewer failure modes** — no controller state machine to debug

**When to reconsider:**
- If KRaft metadata accuracy becomes critical requirement
- If operators require `describe topic` to always reflect reality

---

### Alternative D: Custom Job Coordinator

**Concept:** Keep faking metadata, add custom coordinator (outside KRaft) for background jobs.

**Why considered:**
- Gets deterministic job ownership without changing replica model
- Could use PG-based leader election

**Why rejected:**
1. **Two leadership systems** — KRaft vs. custom coordinator
2. **RLM still blocked** — RLM uses KRaft leadership
3. **More complexity** — three systems (KRaft + transformer + coordinator)
4. **Defeats purpose** — if adding coordinator, why not use KRaft's?

---

## Appendix

### Ghost Replicas Problem

If we keep RF=3 in KRaft but transformer ignores it, Kafka creates "ghost replicas":

**Current diskless behavior in `ReplicaManager.scala`:**

```scala
// applyLocalLeadersDelta - when broker becomes leader
localLeaders.foreachEntry { (tp, info) =>
  if (!_inklessMetadataView.isDisklessTopic(tp.topic()))
    getOrCreatePartition(tp, delta, info.topicId)  // SKIPPED for diskless!
}
```

**What this means:**

| Aspect                  | Classic Topic              | Diskless Topic (ghost)       |
|-------------------------|----------------------------|------------------------------|
| `Partition` objects     | Created on each replica    | **Never created**            |
| Replica fetcher threads | Started for followers      | **Never started**            |
| ISR tracking            | Leader tracks follower lag | **Skipped** (no lag)         |

**The problem:**

```bash
$ kafka-topics.sh --describe --topic diskless-foo

Topic: diskless-foo  Replicas: 1,2,3  ISR: 1,2,3
                                      ↑
                                      Looks healthy!
                                      But brokers 2,3 don't know they're replicas
```

**Operational risks:**

| Risk                         | Impact                                    |
|------------------------------|-------------------------------------------|
| False sense of redundancy    | Operator thinks RF=3 provides safety      |
| Confusing metrics            | Under-replicated = 0, but replicas unused |
| Tooling mismatch             | `describe` shows healthy, reality differs |
| Debugging difficulty         | "Why isn't replication working?"          |

This creates technical debt and operational confusion that compounds over time — essentially throwing problems at operators.

---

### Operational Benefits Summary

The proposed design (Approach C) provides clear operational benefits compared to alternatives:

| Aspect                    | Approaches A/B (faking)        | Approach C (proposed)          |
|---------------------------|--------------------------------|--------------------------------|
| Tooling accuracy          | `describe topic` misleading    | `describe topic` accurate      |
| Under-replicated alerts   | False positives                | Meaningful (informational)     |
| Job debugging             | "Which broker ran this?"       | "Leader of partition X"        |
| Incident response         | Check all broker logs          | Check leader broker            |
| Capacity planning         | Unpredictable load             | Proportional to leadership     |
| Standard Kafka ops        | May not work as expected       | Work normally                  |

---

### Leadership Benefits for Operations

Current diskless uses **racing/randomized** job assignment. Leadership provides:

| Benefit                 | Racing Model               | Leader Model                  |
|-------------------------|----------------------------|-------------------------------|
| Accountability          | "Which broker ran this?"   | "Leader of partition X"       |
| Load distribution       | Random, unpredictable      | Proportional to leadership    |
| Capacity planning       | Add brokers, hope it helps | Rebalance partition leaders   |
| Incident response       | Check all broker logs      | Check leader broker           |
| Tooling                 | Custom                     | Standard Kafka tooling        |

Leadership isn't just about RLM — it's about having a **deterministic owner** for partition-level operations.
