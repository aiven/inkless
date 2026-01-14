# Diskless-Managed Replication Factor (Simplified Design)

> **This is a draft alternative to [DISKLESS_MANAGED_RF.md](DISKLESS_MANAGED_RF.md)** that reduces complexity by using transformer-first availability instead of controller auto-reassignment.

## Table of Contents

1. [Purpose](#purpose)
2. [Objectives](#objectives)
3. [Key Simplification: Transformer-First Availability](#key-simplification-transformer-first-availability)
4. [Activation Model](#activation-model-binary-version)
   - [Existing Diskless Topics](#existing-diskless-topics)
5. [Placement Model](#placement-model)
6. [Controller Behavior](#controller-behavior)
   - [Topic Creation](#topic-creation)
   - [Add Partitions](#add-partitions)
   - [Standard Operations](#standard-operations-after-creation)
7. [Broker Behavior](#broker-behavior)
8. [Metadata Transformation](#metadata-transformation)
9. [Observability](#observability)
10. [Implementation Path](#implementation-path) *(6 weeks / 1 eng, 4 weeks / 2 eng)*
11. [Trade-offs vs Original Design](#trade-offs-vs-original-design)
12. [Rejected Alternatives](#rejected-alternatives)
    - [F: Keep Classic RF, Continue Faking](#rejected-alternative-f-keep-classic-rf-continue-faking-metadata)
    - [G: Controller Auto-Reassignment](#rejected-alternative-g-controller-auto-reassignment-from-original-design)
    - [H: Shrink RF During Migration](#rejected-alternative-h-shrink-rf-during-migration)
    - [I: Custom Job Coordinator](#rejected-alternative-i-custom-job-coordinator-without-kraft-leadership)

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

The key question: **how does each approach handle reverse migration?**

#### Diskless → Classic Migration (Future)

Before comparing approaches for Classic → Diskless, consider what's needed for the reverse:

| Aspect            | A: RF=1               | B: RF=3 faked           | C: RF=rack_count        |
|-------------------|-----------------------|-------------------------|-------------------------|
| Starting RF       | 1                     | 3 (ghost replicas)      | 3 (real replicas)       |
| Before migration  | Must expand RF=1 → RF=3| Must "un-ghost" replicas| **Ready as-is**         |
| Replica state     | Create from scratch   | Create `Partition` objects| Already correct        |
| Complexity        | High (2 operations)   | Medium (fix ghosts)     | **Low (just migrate)**  |

**Key insight:** Only approach C is ready for reverse migration without additional work.

#### How Critical Is Replica-Awareness for Topic Migration?

Now compare three approaches for Classic → Diskless migration:

| Aspect                  | A: Shrink to RF=1        | B: Keep RF=3, fake       | C: RF=rack_count (this)  |
|-------------------------|--------------------------|--------------------------|--------------------------|
| KRaft RF after migrate  | 1                        | 3 (unchanged)            | 3 (managed, rack-aware)  |
| Transformer behavior    | Hash to any broker       | Hash to any broker       | Filter by AZ (KRaft-sourced) |
| Replica objects         | None (offline in KRaft)  | Ghost (no `Partition`)   | Real (metadata-only)     |
| Migration coordinator   | Custom (who?)            | Custom (who?)            | **Leader**               |
| RLM integration         | Custom hooks             | Custom hooks             | **Standard**             |
| Job assignment          | Racing                   | Racing                   | **Leader-based**         |
| Reverse migration ready | No (must expand RF)      | No (must fix ghosts)     | **Yes**                  |

**Terminology:**
- **Offline replica**: KRaft shows replica on broker X, but broker X doesn't exist or is down. No `Partition` object.
- **Ghost replica**: KRaft shows replica on broker X, broker X is alive, but `ReplicaManager` skips creating `Partition` object for diskless. Broker doesn't know it's a replica.
- **Real (metadata-only) replica**: KRaft shows replica on broker X, broker X recognizes it as a replica, but no local log (data in object storage).

**Approach A: Shrink to RF=1 during migration**

```
Classic: Replicas=[1,2,3], Leader=1, data on all brokers
         ↓ migration starts
Shrink:  Replicas=[1], Leader=1  ← When does this happen?
         ↓                         Who removes replicas 2,3?
         ↓                         What if broker 1 fails mid-shrink?
Diskless: Replicas=[1], data in object storage
         Transformer hashes to any alive broker (ignores KRaft)
```

Problems:
- Timing of RF shrink is complex (before seal? during? after?)
- Single point of failure during migration
- Loses rack-awareness from classic topic
- **Reverse migration requires RF expansion first**

**Approach B: Keep classic RF=3, continue faking metadata**

```
Classic:  Replicas=[1,2,3], Leader=1, data on all brokers
          ↓ migration starts (no RF change!)
Diskless: Replicas=[1,2,3], Leader=1, data in object storage
          ↑ KRaft unchanged, transformer ignores and hashes
          Brokers 1,2,3 are "ghost replicas"
```

What "ghost replica" means:
- KRaft says broker is a replica with ISR membership
- But `ReplicaManager.isDisklessTopic()` skips `getOrCreatePartition()`
- No `Partition` object, no fetcher, no local log
- Broker doesn't know it's a "replica" — just serves requests via Inkless handlers
- ISR never shrinks (no lag tracking for diskless)

Problems:
- **RLM still blocked** — `onLeadershipChange()` needs `Partition` objects to run tiering tasks. Ghost replicas don't have them.
- **Reverse migration requires "un-ghosting"** — must create real `Partition` objects before migrating back
- Side effects: misleading tooling output (`describe topic` shows healthy replicas that aren't real)
- See [What Happens to "Ignored" Replicas?](#what-happens-to-ignored-replicas)

**Approach C: RF=rack_count with real replicas (this design)**

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

Benefits:
- **Leader coordinates migration** — deterministic owner
- **Standard RLM integration** — `onLeadershipChange()` works
- **Deterministic job assignment** — leader owns partition operations
- **Accurate tooling output** — replicas are real
- **Reverse migration ready** — already has RF=3 with real replicas

### Cost of Continuing with RF=1

If we keep RF=1 and use transformer override for topic migration:

**Challenges:**

1. **When does RF shrink happen?**
   - Classic topic has RF=3 with replicas [1,2,3]
   - Control Plane marks migration point, seals the topic
   - But when do we remove replicas 2 and 3?
   - Before seal? During? After data is in object storage?
   - What if broker 1 fails mid-transition?

2. **Migration coordination without real leadership:**
   - With RF=1, the single broker "owns" the migration
   - If it fails, how do we pick a new coordinator?
   - Custom leader election outside KRaft? Or use KRaft but ignore it elsewhere?

3. **RLM integration blocked:**
   - `onLeadershipChange()` won't fire without real KRaft leadership
   - Tiering pipeline needs a leader to coordinate segment merging
   - Custom hooks needed to bypass RLM's standard entry points
   - See [RLM Integration: Critical Decision](#rlm-integration-critical-decision) below

4. **Diskless → Classic complexity:**
   - Must expand RF=1 → RF=3 as part of migration
   - Two complex operations combined: RF expansion + data migration
   - More failure modes, harder to reason about

5. **Alternative: Keep existing placement, keep faking?**
   - Classic RF=3 becomes diskless RF=3, but transformer still fakes
   - Zero metadata changes, simplest migration
   - But: RLM still blocked, tooling still confused
   - Defers the problem, doesn't solve it
   - See [What Happens to "Ignored" Replicas?](#what-happens-to-ignored-replicas) below

**What RF=1 continuation requires:**
- Custom migration coordinator (not partition leader)
- Custom RLM integration hooks (bypass `onLeadershipChange()`)
- RF expansion logic embedded in Diskless → Classic migration
- Ongoing maintenance of parallel code paths

### Cost of Moving to RF=rack_count

**One-time investment:**
- Controller changes for rack-aware placement (~2 weeks)
- Transformer changes for AZ filtering (~2 weeks)
- Testing and validation (~2 weeks)

**What we get:**
- Topic migration uses standard replica handoff patterns
- RLM integration uses standard `onLeadershipChange()`
- Diskless → Classic migration starts from correct RF
- Single code path for replica management

### Decision Framework

| Factor                   | Keep RF=1                | Move to RF=rack_count      |
|--------------------------|--------------------------|----------------------------|
| Short-term cost          | Lower                    | ~6 weeks                   |
| Classic → Diskless       | Custom coordination      | Standard replica handoff   |
| RLM integration          | Custom hooks             | Standard leadership hook   |
| Diskless → Classic       | RF expansion + migration | Migration only             |
| Long-term maintenance    | Two code paths           | One code path              |
| Kafka alignment          | Divergent                | Aligned                    |

### What Happens to "Ignored" Replicas?

If we keep RF=3 in KRaft but the transformer ignores it, what does Kafka do with those replicas?

**Current diskless behavior in `ReplicaManager.scala`:**

```scala
// applyLocalLeadersDelta - when broker becomes leader
localLeaders.foreachEntry { (tp, info) =>
  if (!_inklessMetadataView.isDisklessTopic(tp.topic()))
    getOrCreatePartition(tp, delta, info.topicId)  // SKIPPED for diskless!
}

// applyLocalFollowersDelta - when broker becomes follower  
localFollowers.foreachEntry { (tp, info) =>
  if (!_inklessMetadataView.isDisklessTopic(tp.topic()))
    getOrCreatePartition(tp, delta, info.topicId)  // SKIPPED for diskless!
}
```

**What this means:**

| Aspect                  | Classic Topic              | Diskless Topic (current)     |
|-------------------------|----------------------------|------------------------------|
| `Partition` objects     | Created on each replica    | **Never created**            |
| Local log directories   | Created on each replica    | **Never created**            |
| Replica fetcher threads | Started for followers      | **Never started**            |
| ISR tracking            | Leader tracks follower lag | **Skipped** (no lag to track)|
| Leader election         | Uses `Partition` state     | KRaft metadata only          |

**The "ghost replica" problem:**

If KRaft says `Replicas=[1,2,3]` but diskless skips `Partition` creation:

```
KRaft metadata:
  Partition 0: Replicas=[1,2,3], Leader=1, ISR=[1,2,3]

Broker 1 (leader):
  - No Partition object for this topic
  - No local log
  - Serves requests via Inkless handlers

Broker 2 (follower):
  - No Partition object
  - No fetcher thread (nothing to fetch)
  - Doesn't know it's a "follower"

Broker 3 (follower):
  - Same as broker 2
```

**Will they become offline?**

| Scenario                     | What Happens                              |
|------------------------------|-------------------------------------------|
| Broker 2 restarts            | KRaft still shows it as replica, ISR=all  |
| Broker 2 decommissioned      | KRaft shows offline, but no data lost     |
| Leader election triggered    | KRaft picks new leader, but no `Partition`|
| ISR shrink check             | **Skipped** for diskless (no lag to check)|

**Key insight:** Replicas won't become "offline" in the Kafka sense because:
1. ISR shrink is skipped for diskless topics
2. No fetcher means no lag detection
3. KRaft ISR stays as "all replicas" forever

**But this creates confusion:**

```bash
$ kafka-topics.sh --describe --topic diskless-foo

Topic: diskless-foo  Replicas: 1,2,3  ISR: 1,2,3
                                      ↑
                                      Looks healthy!
                                      But brokers 2,3 aren't really "in sync"
                                      They don't even know they're replicas
```

**The operational risk:**

| Risk                         | Impact                                    |
|------------------------------|-------------------------------------------|
| False sense of redundancy    | Operator thinks RF=3 provides safety      |
| Confusing metrics            | Under-replicated = 0, but replicas unused |
| Tooling mismatch             | `describe` shows healthy, reality differs |
| Decommission confusion       | Remove broker 2, ISR still shows it       |
| Debugging difficulty         | "Why isn't replication working?"          |

**Summary:**

Keeping replicas but ignoring them creates **ghost replicas**:
- KRaft thinks they exist and are healthy
- Brokers don't know they're replicas
- ISR never shrinks (no lag tracking)
- Operators get misleading information
- No actual redundancy benefit

This is a form of **technical debt** that compounds over time.

---

### Current Diskless Job Assignment: Racing Brokers

**How diskless background jobs work today:**

Diskless has background jobs (merger, cleaner, etc.) that need to run periodically. Currently, these use a **racing/randomized** approach:

```
Current model (no leader):
  Broker 0: "I'll try to clean partition X" ──┐
  Broker 1: "I'll try to clean partition X" ──┼── Race! First one wins
  Broker 2: "I'll try to clean partition X" ──┘
  
  Coordination via: PostgreSQL locks, randomized delays, distributed claims
```

**Why this was chosen:**
- Simpler implementation (no leader election needed)
- Works without real KRaft replicas
- Good enough for initial diskless MVP

**The operational complexity:**

| Aspect                    | Racing Model                          |
|---------------------------|---------------------------------------|
| Job assignment            | Random/first-come-first-serve         |
| Duplicate work            | Possible (mitigated by PG locks)      |
| Debugging                 | "Which broker ran this job?"          |
| Monitoring                | Jobs scattered across brokers         |
| Failure handling          | Another broker eventually picks up    |
| Load distribution         | Probabilistic, not guaranteed         |

**What leadership enables:**

With real KRaft-managed leaders, background jobs have a **home**:

```
Leader-based model:
  Partition X leader = Broker 1
  
  Broker 0: "Not leader of X, skip"
  Broker 1: "I'm leader of X, I run jobs for X"  ← Deterministic!
  Broker 2: "Not leader of X, skip"
  
  Coordination via: KRaft leadership (standard Kafka)
```

| Aspect                    | Leader Model                          |
|---------------------------|---------------------------------------|
| Job assignment            | Deterministic (leader owns it)        |
| Duplicate work            | Impossible (single leader)            |
| Debugging                 | "Leader of partition X ran this"      |
| Monitoring                | Jobs grouped by leader                |
| Failure handling          | Leader election, new leader takes over|
| Load distribution         | Controlled by partition assignment    |

**Jobs that benefit from leadership:**

| Job                       | Current (racing)           | With Leader                     |
|---------------------------|----------------------------|---------------------------------|
| Batch merging             | Any broker can try         | Leader merges its partitions    |
| Segment cleanup           | Race to delete             | Leader cleans its partitions    |
| Tiering (diskless→TS)     | Complex coordination       | Leader uploads via RLM          |
| Expiration                | Distributed claims         | Leader runs RLM expiration      |
| Offset tracking           | Scattered                  | Leader maintains offsets        |

**The TS migration benefit:**

Tiered Storage migration requires uploading segments in TS format. With leadership:

```
Without leader (current):
  Who uploads segment for partition X?
  → Need custom coordinator
  → Race conditions possible
  → Custom logic to track progress

With leader:
  Partition X leader uploads segments
  → Reuse RLM upload logic directly
  → Standard Kafka pattern
  → Progress tracked via RLM APIs
```

**Key insight:**

Leadership isn't just about RLM integration — it's about having a **deterministic owner** for partition-level operations. This simplifies:
- Implementation (reuse Kafka patterns)
- Operations (clear ownership)
- Debugging (know who did what)
- Future features (upload, compaction, etc.)

### Operational Wins from Leadership

**1. Clear accountability:**
```
Racing model:
  Alert: "Partition X merger failed"
  Operator: "Which broker? Let me check all broker logs..."
  
Leader model:
  Alert: "Partition X merger failed on broker 2 (leader)"
  Operator: "Check broker 2 logs for partition X"
```

**2. Predictable load distribution:**
```
Racing model:
  Broker 0: 45% of merger jobs (got lucky)
  Broker 1: 35% of merger jobs
  Broker 2: 20% of merger jobs (got unlucky)
  
Leader model:
  Broker 0: Jobs for partitions it leads (balanced by assignment)
  Broker 1: Jobs for partitions it leads
  Broker 2: Jobs for partitions it leads
```

**3. Simpler capacity planning:**

| Aspect              | Racing                          | Leader                          |
|---------------------|--------------------------------|----------------------------------|
| Job count per broker| Unpredictable                  | = partitions led                 |
| Resource usage      | Spiky, varies by luck          | Proportional to leadership       |
| Scaling decision    | "Add brokers, hope it helps"   | "Rebalance partition leaders"    |

**4. Easier incident response:**

| Scenario                  | Racing                           | Leader                          |
|---------------------------|----------------------------------|----------------------------------|
| Job stuck                 | Which broker has it?             | Leader of partition X            |
| Job running slow          | Check all brokers                | Check leader broker              |
| Want to pause jobs        | Disable on all brokers           | Move leadership away             |
| Debug job history         | Correlate logs across brokers    | Single broker's logs             |

**5. Standard Kafka operations:**

```bash
# Racing model: No good way to move jobs
# "Jobs run wherever, hope for the best"

# Leader model: Standard Kafka tooling
kafka-leader-election.sh --topic foo --partition 0 --election-type preferred
# Jobs for partition 0 now run on the new leader
```

**6. Monitoring alignment with Kafka:**

| Metric                        | Racing                  | Leader                        |
|-------------------------------|-------------------------|-------------------------------|
| Jobs per broker               | Custom metric           | Partition leadership count    |
| Job failures                  | Custom alerting         | Under-replicated partitions   |
| Load imbalance                | Custom detection        | Leader skew metrics           |

**Summary: Operational benefits of leadership**

| Benefit                    | Impact                                         |
|----------------------------|------------------------------------------------|
| Single point of truth      | Leader owns partition, no ambiguity            |
| Standard tooling           | `kafka-leader-election`, `kafka-reassign`      |
| Predictable load           | Jobs proportional to leadership                |
| Simpler debugging          | One broker to check per partition              |
| Kafka-native monitoring    | Reuse existing metrics and alerts              |
| Incident response          | Move leadership to isolate/debug               |

---

### RLM Integration: Critical Decision

**Why RLM matters for diskless:**

The tiering pipeline (diskless batches → tiered segments) is critical for PostgreSQL scalability. Without it, all batch metadata stays in PG forever, which becomes the bottleneck.

```
Current diskless data flow:
  Produce → Object Storage (WAL) → PostgreSQL (batch metadata)
                                   ↑
                                   This grows unbounded!

Target diskless data flow:
  Produce → Object Storage (WAL) → PostgreSQL (recent batches)
                                   ↓ tiering pipeline
                                   Tiered Storage (aged batches)
                                   ↓ RLM expiration
                                   Deleted (per retention policy)
```

**How RLM works:**

```java
// RemoteLogManager.java - entry point
public void onLeadershipChange(Set<Partition> partitions, ...) {
    for (Partition partition : partitions) {
        // Start tiering tasks for this partition
        // Only runs on the LEADER
        scheduleTieringTasks(partition);
    }
}
```

RLM requires:
1. **Real KRaft leadership** — `onLeadershipChange()` must fire
2. **`Partition` objects** — RLM uses `Partition` and `UnifiedLog` APIs
3. **Leader coordination** — Only leader writes tiered segments

**Cost of deferring RLM integration (keeping RF=1/faked):**

| Aspect                    | Custom Solution Required           | Effort    |
|---------------------------|-----------------------------------|-----------|
| Tiering entry point       | Bypass `onLeadershipChange()`     | ~2 weeks  |
| Leader coordination       | Custom leader election for tiering| ~2 weeks  |
| Segment merging           | Who merges? How to coordinate?    | ~3 weeks  |
| RLM API compatibility     | Fake `Partition` objects or fork  | ~2 weeks  |
| Expiration tasks          | Custom expiration outside RLM     | ~2 weeks  |
| **Total custom work**     |                                   | **~11 weeks** |

**Cost of doing RF=rack_count now:**

| Aspect                    | Standard Solution                 | Effort    |
|---------------------------|-----------------------------------|-----------|
| Rack-aware placement      | Modify `ReplicationControlManager`| ~2 weeks  |
| Transformer filtering     | Update `InklessTopicMetadataTransformer` | ~2 weeks  |
| Add partitions            | Same logic as creation            | ~1 week   |
| RLM integration           | Standard `onLeadershipChange()`   | ~0 weeks* |
| **Total work**            |                                   | **~6 weeks** |

*RLM integration comes "for free" once we have real leadership.

**The math:**

```
Option A: Keep RF=1, defer RLM    = 0 weeks now + 11 weeks later = 11 weeks total
Option B: Do RF=rack_count now    = 6 weeks now + 0 weeks later  = 6 weeks total
                                    ─────────────────────────────
                                    Savings: 5 weeks
```

**But the real cost is worse:**

If we defer, we carry technical debt:
- Two code paths for leadership (KRaft vs. custom)
- Custom tiering pipeline that diverges from Kafka
- Harder to maintain, harder to reason about
- Every new feature asks "does this work with faked RF?"

### Recommendation

**Move to RF=rack_count now** because:
1. Topic migration (both directions) benefits from real replicas
2. RLM integration becomes standard rather than custom
3. One-time 6-week investment vs. 11+ weeks of custom work
4. Avoids accumulating technical debt
5. Aligns diskless with Kafka's replica model

### Related Documents

- [DESIGN.md](DESIGN.md) — Overall tiered storage unification design (includes topic migration, tiering pipeline details)

---

## Objectives

Enable **rack-aware, stable KRaft-managed replicas** for diskless topics:

- **KRaft-managed replicas**: Diskless topics will have actual KRaft-managed replicas (RF = rack count)
- **Rack-aware at creation**: Topics are created with one replica per rack
- **Standard operations after creation**: Once created, standard Kafka replica management applies
- **Controller-managed RF**: Users don't specify RF; controller computes from rack topology
- **Leader-agnostic produce and consume**: Any replica can accept writes and serve reads
- **Always available via transformer**: Transformer ensures availability by routing to alive brokers

**Key difference from original design:** Availability is handled by the transformer (instant), not controller auto-reassignment. Metadata accuracy is eventual.

---

## Key Simplification: Transformer-First Availability

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

**Simplified managed RF behavior:**
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

*Unchanged from original design.*

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

```bash
# Optional: manually expand legacy topic to rack-aware
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json \
  --execute
```

**Key difference from original:** No urgent auto-migration needed. Transformer handles availability immediately.

---

## Placement Model

*Unchanged from original design.*

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

*Unchanged from original design.*

When creating diskless topics (`diskless.enable=true`):
- Controller counts distinct racks from registered brokers
- RF = rack count
- One replica assigned per rack
- Reject `replicationFactor > 1` and manual assignments

### Add Partitions

*Unchanged from original design.*

New partitions use same one-per-rack logic as creation.

### Standard Operations (After Creation)

**Key difference: No auto-reassignment on broker failure.**

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

### What We Remove

No controller logic for:
- ❌ Proactive offline replica detection
- ❌ Immediate auto-reassignment
- ❌ "Not registered vs offline" distinction
- ❌ Pacing controls for reassignment
- ❌ Complex state machine

---

## Broker Behavior

*Unchanged from original design.*

- No replica fetcher threads for diskless topics
- No local `Partition` objects
- Inkless handlers (`AppendHandler`, `FetchHandler`) serve requests

---

## Metadata Transformation

### Updated Filtering Logic

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

### Comparison

| Scenario             | Original Design            | Simplified Design            |
|----------------------|----------------------------|------------------------------|
| Broker fails         | Wait for controller reassign| Instant transformer fallback |
| All replicas offline | Wait for controller        | Instant hash fallback        |
| Client routing       | After KRaft updated        | Immediate                    |

---

## Observability

### Metrics

**Controller metrics:**
- `kafka.controller.diskless.effective_rf{topic}` - RF assigned at creation
- `kafka.controller.diskless.rack_aware{topic,partition}` - 1 if one-per-rack, 0 if not
- `kafka.controller.diskless.rf1_topics_total` - Legacy topics not yet modernized

**Transformer metrics (new):**
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

### Admin Surfaces

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

*Same as original.*

1. Leader election works without broker `Partition` objects
2. ISR updates don't require broker-side `Partition` state
3. `DescribeTopics` / `ListOffsets` work with RF > 1

### Phase 1: Topic Creation with Rack-Aware Placement (2 weeks)

*Same as original.*

1. Modify `ReplicationControlManager` to detect diskless topics
2. Compute RF = rack count at creation
3. Implement one-per-rack broker selection
4. Reject `replicationFactor > 1` and manual assignments

### Phase 2: Transformer Changes (2 weeks)

Updated scope:

1. Update `InklessTopicMetadataTransformer` to read KRaft placement
2. Implement AZ filtering logic
3. **Add offline replica routing** — if KRaft replica offline, route to alive replica
4. **Add hash fallback** — if all replicas offline, use legacy hash
5. Add metrics for fallback tracking

### Phase 3: Add Partitions Support (1 week)

*Same as original.*

1. Apply same one-per-rack logic when adding partitions
2. Handle case where rack count changed since topic creation

### ~~Phase 4: Offline Replica Auto-Reassignment~~ (REMOVED)

**Not needed.** Transformer handles availability.

### ~~Phase 5: Observability~~ → Phase 4: Observability (included in Phase 2)

Simplified — mostly transformer metrics.

### Summary

| Phase     | Scope                                    | Estimate                       |
|-----------|------------------------------------------|--------------------------------|
| 0         | Research and Validation                  | 1 week                         |
| 1         | Topic Creation with Rack-Aware Placement | 2 weeks                        |
| 2         | Transformer Changes (with fallback)      | 2 weeks                        |
| 3         | Add Partitions Support                   | 1 week                         |
| **Total** |                                          | **6 weeks (1 eng) / 4 weeks (2 eng)** |

**Savings: 3 weeks** (removed Phase 4: Auto-Reassignment)

---

## Trade-offs vs Original Design

### What We Gain

| Benefit                       | Impact                                      |
|-------------------------------|---------------------------------------------|
| Simpler implementation        | 3 weeks saved, less code to maintain        |
| No controller complexity      | No state machine, pacing, edge cases        |
| Instant availability          | Transformer decides immediately             |
| Consistent with current       | Transformer fallback = what diskless does   |
| Fewer moving parts            | Less to debug, less to break                |

### What We Accept

| Trade-off                 | Impact                          | Mitigation                       |
|---------------------------|---------------------------------|----------------------------------|
| Stale KRaft metadata      | `describe` shows offline broker | Transformer metrics show reality |
| Under-replicated alerts   | Metrics fire for diskless       | Document as informational        |
| Manual legacy modernization| RF=1 stays until reassigned    | Provide runbook                  |
| No auto rack restoration  | Broken rack-awareness persists  | Same as original (operator fixes)|

### When Original Design Is Better

Choose original design if:
- KRaft metadata accuracy is critical for tooling/automation
- You want `describe topic` to always show current state
- Operators expect under-replicated alerts to always require action

Choose simplified design if:
- You prioritize simplicity and faster delivery
- You accept "eventually consistent" metadata
- Current diskless transformer behavior is acceptable baseline

---

## Rejected Alternatives

*Includes alternatives from original design, plus alternatives specific to the "keep faking" approach.*

### Rejected Alternative F: Keep Classic RF, Continue Faking Metadata

**Concept:** When migrating Classic → Diskless, keep the existing RF (e.g., RF=3) in KRaft but continue using the transformer to fake metadata. No replica management changes.

**Variations considered:**

1. **Keep RF=3, transformer ignores:** Classic RF=3 becomes diskless RF=3, transformer hashes to any alive broker
2. **Shrink to RF=1 during migration:** Classic RF=3 shrinks to RF=1, then uses current diskless model
3. **Keep placement, add custom coordination:** Keep RF=3, add custom job coordinator outside KRaft

**Why we considered it:**
- Zero controller changes for migration
- Simplest migration path (just flip `diskless.enable`)
- Works with existing diskless implementation
- Faster time to initial migration

**Why we rejected it:**

1. **Ghost replicas problem:**
   - KRaft shows `Replicas=[1,2,3], ISR=[1,2,3]`
   - But brokers 2,3 have no `Partition` objects, no fetchers
   - ISR never shrinks (no lag tracking for diskless)
   - Operators get misleading information
   - See [What Happens to "Ignored" Replicas?](#what-happens-to-ignored-replicas)

2. **RLM integration blocked:**
   - `onLeadershipChange()` won't fire without real leadership
   - Custom hooks needed (~11 weeks vs ~6 weeks for real replicas)
   - See [RLM Integration: Critical Decision](#rlm-integration-critical-decision)

3. **Racing job assignment continues:**
   - Background jobs (merger, cleaner) remain randomized
   - No deterministic owner for partition operations
   - See [Current Diskless Job Assignment: Racing Brokers](#current-diskless-job-assignment-racing-brokers)

4. **RF shrink complexity:**
   - If shrinking RF=3 → RF=1, when does this happen?
   - Who coordinates the shrink? What if coordinator fails?
   - See [Cost of Continuing with RF=1](#cost-of-continuing-with-rf1)

5. **Diskless → Classic migration harder:**
   - Must expand RF=1 → RF=3 as part of reverse migration
   - Two complex operations combined

6. **Technical debt compounds:**
   - Two systems: KRaft replicas (classic) vs transformer magic (diskless)
   - Every new feature asks "does this work with faked RF?"
   - Harder to maintain, harder to onboard

**Cost comparison:**

| Approach                | Now     | Later    | Total     |
|-------------------------|---------|----------|-----------|
| Keep faking + custom RLM| 0 weeks | ~11 weeks| 11+ weeks |
| RF=rack_count now       | 6 weeks | 0 weeks  | 6 weeks   |

**When this might make sense:**
- If RLM integration is not needed (PG can scale indefinitely)
- If bidirectional migration is not needed
- If operational confusion from ghost replicas is acceptable
- None of these apply to our use case

---

### Rejected Alternative G: Controller Auto-Reassignment (from original design)

**Concept:** Controller proactively detects offline replicas and reassigns to online brokers immediately.

**Why we considered it:**
- KRaft metadata always accurate
- Standard tooling shows correct state
- Consistent with future direction (KRaft as source of truth)

**Why we chose transformer-first instead:**
- **3 weeks additional complexity** for something transformer already handles
- **Instant vs eventually consistent** — transformer is faster
- **Consistent with today** — diskless already uses transformer for availability
- **Fewer failure modes** — no controller state machine to debug

**When to reconsider:**
- If KRaft metadata accuracy becomes critical requirement
- If RLM integration needs accurate replica state for leader selection
- If operators require `describe topic` to always reflect reality

---

### Rejected Alternative H: Shrink RF During Migration

**Concept:** When migrating Classic → Diskless, shrink RF=3 to RF=1 as part of the migration process, then use current diskless model.

**Why we considered it:**
- Maintains consistency with current RF=1 diskless model
- No need to change existing diskless implementation
- Single replica = simpler mental model

**Why we rejected it:**

1. **Timing complexity:**
   - When does RF shrink happen? Before seal? During? After?
   - Who removes replicas 2 and 3?
   - What if coordinator fails mid-shrink?

2. **Reverse migration harder:**
   - Diskless → Classic requires RF=1 → RF=3 expansion
   - Two operations: expand RF + migrate data

3. **Loses rack-awareness:**
   - Classic topics are often rack-aware (RF=3 across 3 racks)
   - Shrinking to RF=1 loses this property
   - Must re-establish rack-awareness later

4. **All problems of RF=1:**
   - Ghost replicas if we keep KRaft RF=3
   - RLM integration blocked
   - Racing job assignment

---

### Rejected Alternative I: Custom Job Coordinator (Without KRaft Leadership)

**Concept:** Keep faking metadata, but add a custom coordinator (outside KRaft) to assign background jobs deterministically.

**Why we considered it:**
- Gets deterministic job ownership without changing replica model
- Could use PG-based leader election
- Avoids KRaft changes

**Why we rejected it:**

1. **Two leadership systems:**
   - KRaft has its own leader concept
   - Custom coordinator has different leader
   - Which one is authoritative?

2. **RLM still blocked:**
   - RLM uses KRaft leadership, not custom coordinator
   - Still need custom hooks for tiering

3. **More complexity, not less:**
   - Now have KRaft + transformer + custom coordinator
   - Three systems to keep consistent

4. **Defeats the purpose:**
   - If we're adding a coordinator, why not use KRaft's?
   - KRaft leadership is battle-tested, custom is not

---

## Migration Path: Original → Simplified

If we start with simplified and later need original:

1. Simplified design is a **subset** of original
2. Can add controller auto-reassignment later without breaking changes
3. Transformer fallback remains as safety net

If we start with original and want to simplify:

1. Remove controller auto-reassignment code
2. Transformer already has fallback logic
3. Accept stale metadata

**Recommendation:** Start with simplified. Add controller complexity only if proven necessary.
