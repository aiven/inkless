# Diskless-Managed Replication Factor

## Overview

### Problem

Current diskless topics use **RF=1 with faked metadata** — the transformer routes to any broker, ignoring KRaft placement.
This works for pure diskless, but **blocks bidirectional topic migration** (Classic ↔ Diskless) and **RLM integration** for
the tiering pipeline.

### Key Factors Considered

| Factor | Constraint/Consideration |
|--------|--------------------------|
| **Topic migration** | Must support Classic → Diskless (immediate) and Diskless → Classic (future) |
| **Tiered Storage reads** | RLM requires `UnifiedLog` state — only replica brokers can serve tiered reads (or requires RLM extension; see [Decision Option](#decision-option-keep-alive-allow-tiered-storage-rlm-reads-from-any-broker)) |
| **Always-available semantics** | Diskless topics must remain available even when KRaft replicas are offline |
| **RLM/tiering pipeline** | Standard `onLeadershipChange()` integration requires real KRaft leadership |
| **Operational clarity** | Tooling (`describe topic`) should reflect reality; deterministic job ownership |
| **Implementation cost** | Minimize weeks-to-value while avoiding technical debt |

### Proposed Approach: RF=rack_count with Transformer-First Availability

**At topic creation:**
- Controller assigns **RF = rack_count** (one replica per rack/AZ)
- Real KRaft-managed replicas with rack-aware placement

**At runtime (routing):**
- **Hybrid/tiered partitions**: metadata returns replica brokers only (tiered reads require `UnifiedLog`)
- **Diskless-only partitions**: metadata prefers replicas but falls back to any alive broker if all are offline

**Key simplification:** No controller auto-reassignment. Availability is handled by the transformer at routing time, not by
moving replicas in KRaft. This keeps the design simple while preserving the "always available" property.

### Trade-offs Accepted

| Trade-off | Accepted | Rationale |
|-----------|----------|-----------|
| KRaft metadata may show offline brokers | Yes | Availability is not blocked; eventual consistency is acceptable |
| `describe topic` may be temporarily stale | Yes | Operator can see actual state via metrics; availability is real |
| Tiered reads require replica brokers | Yes | Necessary for RLM correctness; can revisit with "tiered reads from any broker" option |

### Estimate

**6-8 weeks** (1 engineer) — vs **18+ weeks** for alternatives that defer real replicas.

### Key Decisions for Review

1. **Tiered reads: replicas-only vs any broker** — Currently replicas-only; "any broker" kept as explicit decision option
2. **Post-switch RF normalization** — Eventual/best-effort, not forced prerequisite
3. **No controller auto-reassignment** — Transformer handles availability; simpler design

---

## Table of Contents

0. [Overview](#overview)
1. [Motivation](#motivation)
2. [Objectives](#objectives)
3. [Design: Transformer-First Availability](#design-transformer-first-availability)
4. [Activation Model](#activation-model-controller-server-config)
5. [Placement Model](#placement-model)
6. [Controller Behavior](#controller-behavior)
7. [Broker Behavior](#broker-behavior)
8. [Metadata Transformation](#metadata-transformation)
9. [Observability](#observability)
10. [Implementation Path](#implementation-path) *(6-8 weeks / 1 eng)*
11. [Rejected Alternatives](#rejected-alternatives)
    - [Decision Option: Tiered reads from any broker](#decision-option-keep-alive-allow-tiered-storage-rlm-reads-from-any-broker)
12. [Appendix](#appendix)
    - [Ghost Replicas Problem](#ghost-replicas-problem)
    - [Operational Benefits (Day-2)](#operational-benefits-day-2)
      - [Operational Benefits Summary](#operational-benefits-summary)
      - [Leadership Benefits for Operations](#leadership-benefits-for-operations)

---

## Motivation

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

We evaluate three approaches:
- **Approach A: Shrink to RF=1** — Reduce RF during migration, use current diskless model
- **Approach B: Keep RF=3, fake metadata** — Keep classic RF, but transformer ignores it
- **Approach C: RF=rack_count with real replicas** — Proposed design with KRaft-managed replicas

**Key decision (affects this whole design): do we require tiered reads to be served only by replica brokers?**

Kafka **Metadata** responses are offset-unaware, but Tiered Storage reads via `RemoteLogManager` require local `UnifiedLog`
state on the serving broker. This creates a hard constraint:
- When a partition can serve **tiered/local reads** (`DISKLESS_TIERED`), metadata transformation must return only brokers in
  the **KRaft replica set** (Kafka semantics).
- Only when a partition is **`DISKLESS_ONLY`** can we safely decouple “availability routing” from KRaft placement and route to
  any alive broker (transformer-first availability).

This is a deliberate trade-off: it keeps the “always available” property for diskless-only partitions, while preserving
correctness for tiered/hybrid reads.

> **Terminology**: `DISKLESS_TIERED` encompasses migration states where tiered data exists (`TIERED_ONLY`, `HYBRID` per
> `DESIGN.md`). Routing logic treats these identically: replicas-only.

If we later decide to make **tiered reads serveable from any broker**, then we can remove this constraint and strengthen the
decoupling (see [Decision Option: Tiered reads from any broker](#decision-option-keep-alive-allow-tiered-storage-rlm-reads-from-any-broker)).

**Scope/impact clarification:**
- **Approach C does not require this option** to reach a consistent architecture. Without it, we still get correctness by
  keeping hybrid/tiered reads on replica brokers and “always available” behavior for diskless-only partitions.
- **Approaches A/B benefit partially** if this option is implemented (it can remove the “custom TS fetch path” gap), but it
  does **not** fix the core A/B problems (ghost replicas, lack of real leadership/job ownership, RLM/tiering pipeline hooks).

The following terminology helps distinguish the key differences:

**Terminology:**
- **Offline replica**: KRaft shows replica on broker X, but broker X doesn't exist or is down. No `Partition` object.
- **Ghost replica**: KRaft shows replica on broker X, broker X is alive, but `ReplicaManager` skips creating `Partition` object for diskless. Broker doesn't know it's a replica.
- **Real (metadata-only) replica**: KRaft shows replica on broker X, broker X recognizes it as a replica, but no local log (data in object storage).

#### Summary Table

```
Aspect                 | A: Shrink RF=1      | B: Keep RF=3 fake   | C: RF=rack_count ✓
-----------------------|---------------------|---------------------|---------------------
KRaft RF after migrate | 1                   | 3 (unchanged)       | 3 (rack-aware)
Transformer behavior   | Hash to any broker  | Hash to any broker  | Filter by AZ
Hybrid/tiered reads    | Custom / risky      | Custom / risky      | Replicas only (safe)
Replica objects        | None (offline)      | Ghost (no Partition)| Real (metadata-only)
Local state after migr | Must delete (risk)  | Must delete (risk)  | Standard handoff
Migration coordinator  | Custom (who?)       | Custom (who?)       | Leader
RLM integration        | Custom (~11 wks)    | Custom (~11 wks)    | Standard (~0 wks)
Job assignment         | Racing              | Racing              | Leader-based
Reverse migration      | No (must expand RF) | No (must fix ghosts)| Yes
```

#### Migration RF Normalization (Classic → Diskless)

Another key decision: **if a classic topic’s RF is not aligned with `rack_count`, when do we “normalize” it?**

This matters because diskless-managed RF assumes replicas are meaningful for **leadership/job ownership** and for
**tiered/hybrid correctness** (replica brokers host `UnifiedLog` state used by RLM).

We have three options:

```
Option                     | What happens
---------------------------|----------------------------------------------------------
Pre-migration (operator)   | Operator increases RF before switching to diskless
Pre-migration (automatic)  | Migration pipeline expands RF while still classic
Post-switch (recommended)  | Switch to diskless first, expand RF eventually
```

| Option | Pros | Cons |
|--------|------|------|
| Pre-migration (operator) | Simplifies migration impl | Expensive/slow (replicates data we'll move anyway); ops burden |
| Pre-migration (automatic) | Fully automated | Same cost; adds risk during classic operation |
| Post-switch ✓ | Avoids wasting resources; single pipeline | Suboptimal placement until normalization |

**Encoding in the proposed algorithm:** After switching a partition to `DISKLESS_ONLY`, the controller may *eventually*
normalize RF/placement (background task) but does not block availability or correctness on completing that step.

**Recommendation:** Prefer **post-switch normalization**, but treat it as **eventual / best-effort**, not a forced prerequisite.

After the switch to `DISKLESS_ONLY`, correctness is not dependent on having `RF=rack_count` immediately:
- Metadata (and thus preferred routing) may temporarily return **fewer replicas**, and they may not be rack-aware.
- This can reduce locality and concentrate “leader/job ownership” on fewer brokers.
- However, diskless serving remains correct (data is in object storage), and availability can still be preserved via the
  transformer’s diskless-only fallback behavior.

Normalization is still **worth doing** for consistency and long-term operability (leadership distribution, rack-awareness,
future RLM-aligned work), but it can happen asynchronously (controller background task or operator-triggered reassignment).

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

```
Aspect           | A: RF=1             | B: RF=3 faked          | C: RF=rack_count ✓
-----------------|---------------------|------------------------|--------------------
Starting RF      | 1                   | 3 (ghost replicas)     | 3 (real replicas)
Before migration | Must expand RF=1→3  | Must "un-ghost"        | Ready as-is
Replica state    | Create from scratch | Create Partition objs  | Already correct
Complexity       | High (2 operations) | Medium (fix ghosts)    | Low (just migrate)
```

**Key insight:** Only Approach C is ready for reverse migration without additional work.

### Cost Analysis

#### Cost of Keeping RF=1/Faked (Approaches A and B)

Approaches A and B appear simpler but have **hidden upfront costs** related to local state management:

**The local state problem:**

When migrating Classic → Diskless, brokers still have local log directories with segment files. Diskless handlers ignore local state, but:

| Problem                     | Risk                                          | Severity     |
|-----------------------------|-----------------------------------------------|--------------|
| Stale local metadata        | Partition metadata files (LEO, etc.) diverge from object storage | **Critical** |
| Orphaned segment files      | Requires tiering to object storage + retention cleanup | Medium       |
| Log cleaner interaction     | May attempt to clean diskless partitions      | High         |
| Broker restart recovery     | Which is source of truth? Local metadata or object storage? | **Critical** |
| Replica fetcher state       | Followers may have stale fetch positions      | High         |

**Note:** Some of these issues (metadata sync, fetcher state) must be solved for *all* approaches. However, Approach C addresses them via standard replica handoff patterns, while A/B require custom one-off fixes.

**Required work for A/B to be safe:**

| Task                                | Effort    |
|-------------------------------------|-----------|
| Sync local metadata → object storage| ~1-2 weeks|
| Tier local segments to object storage| ~1 week   |
| Delete local logs after tiering     | ~0.5 week |
| Prevent log cleaner on diskless     | ~0.5 week |
| Block replica fetcher startup       | ~0.5 week |
| Handle broker restart (source of truth) | ~1 week |
| Custom TS fetch path (no `UnifiedLog`)  | ~2-3 weeks |
| **Subtotal (local state + TS)**     | **~6-8 weeks** |

This is **not** near-zero — it's ~6-8 weeks of custom work just to make migration safe, with ongoing risk if any edge case is missed.

**Additional deferred cost** (for future features):
- Diskless → Classic migration (reverse direction)
- RLM integration for tiering pipeline
- KIP-aligned features

The table below shows the deferred cost for RLM integration:

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

**Buffer for unknowns:** Add ~1-2 weeks for edge cases, testing, and unexpected issues.

**Realistic total: ~7-8 weeks**

**Why local state is simpler with Approach C:**

With real replicas, the migration can use standard Kafka replica handoff patterns:
- **Leader coordinates** data movement to object storage (deterministic owner)
- **Metadata sync** is part of standard replica protocol (LEO, offsets tracked by leader)
- **Local logs cleanup** follows standard tiering (RLM-style) — not custom deletion
- **Broker restart** has clear source of truth: leader + object storage
- **Fetcher state** managed via standard replica protocol (followers follow leader)

The same problems exist, but Approach C solves them via **standard Kafka patterns** rather than custom one-off fixes.

#### Decision Framework

```
Factor                | A: Shrink RF=1       | B: Keep RF=3 fake    | C: RF=rack_count ✓
----------------------|----------------------|----------------------|---------------------
Short-term cost       | ~7 wks (local + TS)  | ~7 wks (local + TS)  | ~6-8 wks (w/ buffer)
Local state risk      | High (must cleanup)  | High (must cleanup)  | Low (std handoff)
Classic → Diskless    | Custom coordination  | Custom coordination  | Std replica handoff
RLM integration       | Custom (~11 wks)     | Custom (~11 wks)     | Standard (~0 wks)
Diskless → Classic    | RF expand + migrate  | Un-ghost + migrate   | Migration only
Long-term maintenance | Two code paths       | Two code paths       | One code path
Kafka/KIP alignment   | Divergent            | Divergent            | Aligned
```

#### Summary

```
Approach A/B: ~7 weeks now (local state + TS) + ~11 weeks later (RLM) = 18+ weeks total
Approach C:   ~6 weeks now + ~2 weeks buffer + 0 weeks later          = 8 weeks total
                                                                        ─────────────
                                                                        Savings: 10+ weeks
```

**Plus ongoing risk with A/B:** Custom local state cleanup and TS fetch has edge cases — if any are missed, data corruption, metadata drift, or fetch failures can occur during/after migration.

Plus, deferring creates technical debt:
- Two code paths for leadership (KRaft vs. custom)
- Custom tiering pipeline that diverges from Kafka
- Every new feature asks "does this work with faked RF?"

**Operational cost of A/B:**
- Misleading tooling (`describe topic` shows ghost replicas)
- Debugging complexity ("which broker ran this job?")
- False alerts (under-replicated metrics for healthy topics)
- See [Operational Benefits Summary](#operational-benefits-summary)

### Recommendation

**Implement the proposed design (Approach C: RF=rack_count)** because:
1. Topic migration (both directions) benefits from real replicas
2. RLM integration becomes standard rather than custom
3. One-time 6-8 week investment vs. 18+ weeks with A/B
4. Avoids local state cleanup risks (data corruption, offset drift)
5. Avoids accumulating technical debt
6. Aligns diskless with Kafka's replica model

### Open Questions for Review

1. **Tiered reads: replicas-only vs any broker**
   - Current decision: replicas-only (RLM requires `UnifiedLog`)
   - Alternative kept alive as [Decision Option](#decision-option-keep-alive-allow-tiered-storage-rlm-reads-from-any-broker)
   - **Question:** Is this the right default? Should we invest in "tiered reads from any broker" sooner?

2. **Post-switch RF normalization: automatic vs operator-triggered**
   - Current decision: eventual/best-effort, controller may normalize in background
   - **Question:** Should normalization be fully automatic, or should operators trigger it explicitly?

3. **`Partition` objects for diskless**
   - Current: skipped (`ReplicaManager.isDisklessTopic()` check)
   - Future: may add for RLM coordination
   - **Question:** Should we add minimal `Partition` objects now (for RLM hooks), or defer?

4. **Existing RF=1 diskless topics modernization**
   - Current: eventual background task or manual `kafka-reassign-partitions.sh`
   - **Question:** Should we proactively modernize existing RF=1 topics, or leave as opt-in?

### Related Documents

- [DESIGN.md](DESIGN.md) — Overall tiered storage unification design

---

*The following sections detail the proposed design (Approach C: RF=rack_count with real replicas).*

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

### Diskless ISR Semantics

Diskless replicas never diverge because user data lives in remote object storage rather than broker-local logs. As a result,
ISR membership for `DISKLESS_ONLY` partitions is liveness-gated, not lag-gated:

- The controller drops a replica from ISR when it is fenced, unregistered, or explicitly removed (rolling restart or broker replacement).
- When the replica re-registers (rolling restart/upgrade completes), the controller can add it back immediately—no catch-up fetch or
`Partition` object is required, because the replica is “in sync” by construction.
- `min.insync.replicas` semantics remain intact: as long as the required number of replicas are alive/unfenced, they are considered in-sync.
  There is no risk of serving stale data since all reads/writes ultimately hit remote storage.
- Rolling maintenance therefore behaves predictably: ISR temporarily shrinks while a broker is fenced and expands as soon as it comes back,
  while availability is preserved by the transformer’s routing logic.

This weaker ISR definition is intentional for `DISKLESS_ONLY`. Hybrid/tiered states still rely on `UnifiedLog` state and classic
lag-based ISR semantics. If a future KIP introduces local logs as caches, that iteration can extend/restore traditional ISR behavior for
diskless replicas.

### The Insight

For diskless topics, **availability** and **metadata accuracy** can be decoupled.

Unlike classic topics where data lives on specific brokers (making KRaft metadata critical for routing), diskless data lives in object storage — any broker can serve any partition. This means:
- **Availability** can be handled instantly by the transformer (route to any alive broker)
- **Metadata accuracy** can be eventually consistent (controller updates KRaft when convenient)

**Scope note:** This decoupling applies to partitions in a **diskless-only** state. During tiered→diskless migration (hybrid/tiered state), reads may require a broker with `UnifiedLog`/RLM state, so routing must stay within the KRaft replica set (Kafka semantics).

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

## Activation Model (Controller Server Config)

Managed RF is activated by a **controller server configuration**. This allows incremental PRs to be merged without impacting
existing clusters, and enables controlled rollout per environment.

### Configuration

```properties
# Controller config (server.properties)
diskless.managed.rf.enabled=false   # default: disabled for backward compatibility
```

When `diskless.managed.rf.enabled=true`:
- New diskless topics use KRaft-managed placement (one replica per rack at creation)
- Transformer filters KRaft placement by client AZ
- **Falls back to alive brokers if assigned replicas are offline**

When `diskless.managed.rf.enabled=false` (default):
- Diskless topics use legacy "RF=1 / faked metadata" behavior
- Transformer calculates synthetic placement via hashing

### Rollout Strategy

This config-based activation enables:

1. **Safe incremental PRs**: Code changes can be merged without affecting existing clusters
2. **Per-environment enablement**: Enable in dev/staging first, then production
3. **Instant rollback**: Disable config without binary rollback if issues arise
4. **Mixed-state clusters**: Existing RF=1 topics continue working; new topics get RF=rack_count when enabled

### Behavior Summary

**Config disabled (default):**
- Diskless topics use legacy "RF=1 / faked metadata" behavior
- Transformer calculates synthetic placement via hashing

**Config enabled:**
- Diskless topics use KRaft-managed placement (one replica per rack at creation)
- Transformer filters KRaft placement by client AZ
- **Falls back to alive brokers if assigned replicas are offline**

### Existing Diskless Topics

Existing diskless topics (RF=1) continue to work:

**Immediate availability:**
- Transformer sees RF=1 with potentially offline broker
- Falls back to hash-based selection (same as today)
- No downtime

**Note on RF < rack_count:**

The same fallback logic applies to any RF < rack_count (including RF=1). This is intentional:

| Aspect | Design Decision | Rationale |
|--------|-----------------|-----------|
| RF=1 valid? | **Yes** | Fewer replicas = fewer brokers with jobs, but still available |
| Target RF | rack_count at creation | Default, not enforced after creation |
| RF < racks after ops | **Allowed** | Operator can reassign; transformer handles availability |
| Future configurability | Possible | Could allow `min.insync.replicas` style config later |

**Decision summary (routing vs tiered correctness):**

- **If a partition may require Tiered Storage reads** (`DISKLESS_TIERED`): metadata must return **replica brokers only**
  (brokers with `UnifiedLog`/RLM state). Availability follows Kafka semantics.
- **If a partition is `DISKLESS_ONLY`**: metadata can prioritize KRaft placement but may fall back to **any alive broker**
  (transformer-first availability).
- If we later choose [Tiered reads from any broker](#decision-option-keep-alive-allow-tiered-storage-rlm-reads-from-any-broker),
  we can remove the “replicas-only for tiered/hybrid” constraint and strengthen decoupling.

#### Tiered Storage (TS) reading — Important Consideration

TS fetching via `RemoteLogManager.read()` requires local state that current diskless doesn't have:

```
Requirement          | What RLM needs                 | Current diskless    | Approach C
---------------------|--------------------------------|---------------------|-------------------
UnifiedLog           | fetchLog.apply(tp) returns log | Missing             | Has it (real)
LeaderEpochFileCache | For segment lookup             | Missing             | Has it
Segment metadata     | To locate tiered data          | Missing             | Has it
```

**Therefore:** for migrated topics, “any broker can serve” is only safe once the partition is `DISKLESS_ONLY`, unless we
implement the “tiered reads from any broker” decision option.

**Impact on topic migration:**

During Classic → Diskless migration, the read path must handle tiered data:
```
Offset < disklessStartOffset → Read from Tiered Storage (via RLM)
Offset >= disklessStartOffset → Read from Diskless (object storage)
```

With Approaches A/B:
- Broker has no `UnifiedLog` for diskless partitions
- `RLM.read()` fails with `OffsetOutOfRangeException`
- **Custom TS fetch path required** (~2-3 weeks additional work)

With Approach C:
- Real replicas have `Partition` → `UnifiedLog` → works with RLM
- Standard TS fetch path works

**Implication for metadata transformation (offset-unaware):**

Kafka **Metadata** responses do not include fetch offsets, so the transformer cannot know whether a client will read:
- Tiered/local range (must be served by a broker with `UnifiedLog`/RLM state), or
- Diskless range (can be served by any broker).

Therefore the transformer must be conservative **per partition state**:
- **If tiered reads are possible** (`DISKLESS_TIERED`), the transformer must return only brokers in the **KRaft replica set** (so TS reads work).
- **Only when the partition is `DISKLESS_ONLY`** (no tiered reads), the transformer may route to any alive broker for availability.

**Note:** Pure diskless topics (never migrated) use Inkless `FetchHandler` which reads from object storage directly — no RLM dependency. The TS limitation matters during/around migration when a partition still has tiered/local data.

**Eventual modernization (optional):**
- Controller can lazily detect RF=1 diskless topics
- Background task expands to RF=rack_count when convenient
- Not urgent — availability is already handled by transformer

**But important for future tiering / migration work:**

If we later want Tiered Storage reads (RLM) to work for these partitions (for example during tiered→diskless migration, or
any design that reads via `UnifiedLog`/RLM), then RF=1 “orphan/synthetic placement” is **not sufficient**. We must first
modernize placement so that at least one **real replica broker** has the `UnifiedLog` state required by RLM.

In practice this means the “eventual modernization” step becomes a **prerequisite** for tiering-related work:
- migrate/replace an orphan single replica (broker id not present) to an alive broker
- and typically expand to RF=rack_count to align with Approach C and avoid re-introducing one-off tiered-read fixes

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
- Accept `replicationFactor=-1` (recommended) or `replicationFactor=1` (for compatibility)
- Reject `replicationFactor > 1` (RF is system-managed)
- Reject manual replica assignments

### Add Partitions

New partitions use same one-per-rack logic as creation.

### Standard Operations (After Creation)

**No auto-reassignment on broker failure.** Transformer handles availability.

**Why we do not auto-reassign (even within the same rack):**
- **Diskless-only partitions:** availability does not require reassignment; the transformer can route to any alive broker when
  all assigned replicas are offline. Auto-reassignment adds churn and new failure modes for marginal benefit.
- **Hybrid/tiered partitions:** tiered reads must be served by replica brokers (needs `UnifiedLog`/RLM state). If all replicas
  are offline, we prefer **Kafka semantics** (unavailable until a replica returns) over silently routing to non-replica
  brokers that cannot serve tiered reads.
- **Avoid thrash:** reassigning on every transient broker flap can cause repeated metadata churn and operational instability.

**Trade-off:** we prefer *temporary cross-AZ serving* (diskless-only fallback) over controller-driven reassignment, because
it preserves the “always available” property with fewer moving parts. If an operator wants rack-locality restored, standard
reassignment tooling remains available.

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

- No replica fetcher threads for diskless topics (invariant: must not regress)
- No local `Partition` objects (for now — may add for RLM coordination later)
- Inkless handlers (`AppendHandler`, `FetchHandler`) serve requests

**Regression guardrail:** diskless topics must not start follower fetchers or attempt log replication. Add tests/metrics to
ensure diskless partitions never create fetcher state.

---

## Metadata Transformation

### Filtering Logic

This is the concrete encoding of the “offset-unaware metadata” constraint described in
[Tiered Storage (TS) reading — Important Consideration](#tiered-storage-ts-reading--important-consideration) and the “key
decision” in [Approach Comparison](#approach-comparison).

```
FOR each diskless partition:
  # Important: Metadata responses are offset-unaware, so routing must be
  # conservative when tiered reads are possible.
  mode = migrationState(tp)   # DISKLESS_ONLY or DISKLESS_TIERED (has tiered data)

  assigned_replicas = KRaft replica set
  alive_replicas = assigned_replicas ∩ alive_brokers
  
  IF mode == DISKLESS_TIERED:
    # Tiered reads may be required -> must stay on replicas that have UnifiedLog/RLM state
    IF alive_replicas is not empty:
      RETURN alive_replicas (prefer clientAZ, else cross-AZ)
    ELSE:
      # Yes, this loses availability — but tiered reads REQUIRE replica brokers.
      # This is standard Kafka semantics: partition unavailable until replica returns.
      # Note: This scenario is rare (all replicas down) and temporary.
      # Rolling restarts or upgrades replacing nodes won't hit this path — at least one replica is alive.
      RETURN assigned_replicas (partition unavailable)
  ELSE:  # DISKLESS_ONLY
    # Diskless-only: any broker can serve, so we can preserve "always available"
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

1. **Instant availability (`DISKLESS_ONLY`)**: No waiting for controller; falls back to hash if all replicas offline
2. **AZ-aware when possible**: Uses KRaft placement if alive
3. **Graceful degradation (`DISKLESS_ONLY`)**: Falls back to hash-based selection if needed
4. **Tiered-safe routing (`DISKLESS_TIERED`)**: Only replica brokers are returned (RLM requires `UnifiedLog`)
5. **Availability trade-off (`DISKLESS_TIERED`)**: If all replicas offline, partition is unavailable (standard Kafka semantics)
6. **State-dependent semantics**: Hybrid/tiered behaves like Kafka; diskless-only is always available

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

**`DescribeTopicPartitions` path today:** brokers handle this RPC (controllers do not). `KafkaApis` responds from the local `MetadataCache`,
then applies the same diskless transformation as `MetadataResponse` (hashing/faking placement). Once KRaft metadata maintains diskless RF>1
state, this post-processing must be updated (or removed) to simply reflect controller placement.

**`ListOffsets` path today:** brokers call `ReplicaManager.fetchOffset`. For diskless topics this invokes
`ReplicaManager.disklessFetchOffset`, which fans out to the Inkless control plane to look up offsets in remote storage. This already bypasses
local logs, but once RF>1 diskless placement is real, the response still needs to prefer controller-provided replica IDs and no longer rely
on synthetic hashing.

---

## Implementation Path

**Total Estimate: 6-8 weeks with 1 engineer** (includes ~1-2 weeks buffer for unknowns)

### Phase 0: Research and Validation (1 week)

**Objective:** Validate that controller operations are metadata-driven and do not require broker-side `Partition` objects.

**Findings:**

1. **Leader election** *(validated)*
   - Controller (`ReplicationControlManager`) elects leaders purely from KRaft `PartitionInfo` + broker liveness/fencing
   - No dependency on broker-side `Partition` objects
   - `handleBrokerFenced()` removes broker from ISR and triggers re-election via `generateLeaderAndIsrUpdates()`

2. **ISR maintenance** *(validated)*
   - ISR updates are metadata-driven via `handleBrokerFenced()`, `handleBrokerUnregistered()`, `handleBrokerInControlledShutdown()`
   - Controller shrinks/expands ISR without broker `Partition`/fetcher state
   - For `DISKLESS_ONLY`, ISR is intentionally liveness-gated (no divergence risk; data is in remote storage)
   - `min.insync.replicas` remains satisfied as long as enough replicas are alive

3. **Broker heartbeats** *(validated)*
   - `BrokerHeartbeatManager` is broker-scoped (epochs, metadata offsets, fencing)
   - No per-partition payload, no dependency on `Partition` objects

4. **DescribeTopicPartitions path** *(documented)*
   - Served by brokers (controllers don't handle it)
   - Brokers answer from `MetadataCache` and currently apply diskless "fake placement" transformation
   - Once diskless RF>1 uses real KRaft placement, post-processing must be updated to reflect controller placement

5. **ListOffsets path** *(documented)*
   - Served by brokers via `ReplicaManager.fetchOffset`
   - Diskless topics use `disklessFetchOffset` to query Inkless control plane/remote storage (no local logs)
   - When RF>1 diskless lands, responses should rely on controller placement without synthetic hashing

6. **Staleness/consistency** *(documented)*
   - All broker-served metadata is as current as broker's `MetadataCache`
   - Clients see eventually consistent view until brokers apply latest KRaft deltas (matches classic Kafka)

**Remaining item:**
- Item 3 (`DescribeTopics`/`ListOffsets` with RF>1) cannot be exercised until diskless RF>1 is implemented
- It remains a future sanity check to ensure broker replies reflect KRaft placement plus control-plane offsets

**Test coverage:**
- See `ReplicationControlManagerTest.DisklessRfPhase0Tests` in `metadata/src/test/java/`
- Classic topic tests validate controller behavior (pass today)
- Diskless RF>1 tests are `@DisabledIf` until Phase 1 is implemented
- Run disabled tests: `DISKLESS_RF_FUTURE_TESTS_ENABLED=true ./gradlew :metadata:test --tests "...DisklessRfPhase0Tests.*" --rerun`

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
| Buffer    | Edge cases, testing, unknowns            | 1-2 weeks|
| **Total** |                                          | **6-8 weeks** |

---

## Rejected Alternatives

### Alternative A: Shrink RF During Migration

**Concept:** When migrating Classic → Diskless, shrink RF=3 to RF=1, then use current diskless model.

**Why considered:**
- Maintains consistency with current RF=1 diskless model
- No need to change existing diskless implementation

**Why rejected:**
1. **Local state cleanup required** — Must delete local logs on all replicas to avoid drift/corruption (~3-4 weeks)
2. Timing complexity (when does RF shrink happen?)
3. Loses rack-awareness — must recreate for reverse migration
4. **Reverse migration requires RF expansion** (two operations)
5. RLM integration still blocked
6. Racing job assignment continues

See [Approach Comparison](#approach-comparison) for details.

---

### Alternative B: Keep Classic RF, Continue Faking Metadata

**Concept:** Keep existing RF (e.g., RF=3) in KRaft but continue using transformer to fake metadata.

**Why considered:**
- Zero controller changes for migration
- Simplest migration path
- If users migrate back, they keep same partition assignments

**Why rejected:**
1. **Local state cleanup required** — Must delete local logs on all replicas to avoid drift/corruption (~3-4 weeks)
2. **Ghost replicas** — KRaft shows healthy replicas that aren't real
3. **RLM blocked** — `onLeadershipChange()` needs `Partition` objects. Adding them would be similar complexity to proposed approach.
4. **Reverse migration requires "un-ghosting"** replicas
5. Misleading tooling output
6. Racing job assignment continues

See [Ghost Replicas Problem](#ghost-replicas-problem) for details.

---

### Alternative C: Controller Auto-Reassignment

**Concept:** Controller proactively detects offline replicas and reassigns to online brokers immediately.

**Example:**

```
diskless topic foo-0 (RF=3, rack-aware)
  Replicas: [101(rack=a), 202(rack=b), 303(rack=c)]

Broker 101 goes offline.

Auto-reassign approach:
  Controller immediately replaces 101 with 111 (also rack=a):
    Replicas become [111, 202, 303]

If 101 flaps (comes back quickly), we may churn again unless we add damping/heuristics.
```

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
   - Hard to battle-proof: split-brain risk, conflicting ownership, retries/timeouts, and unclear source of truth during
     partial outages
4. **Defeats purpose** — if adding coordinator, why not use KRaft's?

---

### Decision Option (keep alive): Allow Tiered Storage (RLM) reads from any broker

**Concept:** Make Tiered Storage fetching work even when the broker serving the request does *not* host the partition’s
`UnifiedLog` (i.e., TS reads become “stateless” or “proxy-able”). This would preserve “any broker can serve” semantics even
for hybrid/tiered partitions.

**Why it matters to this design:** It directly affects whether metadata transformation must be migration-state-aware
(`DISKLESS_TIERED` → replicas-only) or can always return “best available” brokers without risking tiered-read failures.

#### Integration path if we choose this option (high level)

One plausible incremental path is **proxy-based TS reads** (least invasive to Kafka internals):

1. **Detect TS-needed reads at request time** (offset-aware):
   - In the fetch handling path, for partitions where `offset < disklessStartOffset`, mark them as “tiered/local path”.
2. **Route/proxy TS-needed reads to a replica broker**:
   - If the receiving broker is not in the replica set (no `UnifiedLog`), forward that partition’s fetch request to a chosen
     replica broker (prefer same AZ, fall back cross-AZ).
3. **Replica broker executes standard `UnifiedLog.read()`**:
   - This naturally covers local→tiered fallback via `RemoteLogManager` and returns data.
4. **Aggregate and respond**:
   - The front broker merges proxied results with diskless-range results and returns a single Fetch response.
5. **Hardening**:
   - Add timeouts/backpressure, metrics, and failure semantics (e.g., if no replica broker reachable, behave like Kafka: fail
     the tiered portion).

An alternative path is **making RLM independent of local `UnifiedLog`** (larger change / closer to KIP scope): move leader
epoch lineage + segment mapping into an external store so any broker can call RLM directly. This is likely larger than the
proxy approach.

#### How this would change A/B/C (weighting)

```
Impact area                            | A/B (faking)           | C (proposed)
---------------------------------------|------------------------|-------------------------
Remove "custom TS fetch path" work     | Yes (saves ~2-3 wks)   | Not needed either way
Fix ghost replicas / no Partition      | No                     | Already addressed
Enable std RLM integration             | No                     | Yes
Reduce need to modernize RF=1/orphans  | Partially              | Still recommended
New complexity introduced              | High (proxy, cross-AZ) | Medium (optional future)
```

**Bottom line:** This option can reduce *one* major A/B gap (tiered reads during migration) and could strengthen the
“availability routing” story, but it does **not** remove the main reasons we prefer C (real replicas, leadership, avoiding
ghost replicas, standard RLM integration).

**Status:** Keep as an explicit team decision. Not required for Approach C, but compatible as a future enhancement if “any
broker can serve even hybrid/tiered” becomes a hard requirement.

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

It also increases the risk of **unknown bugs**: tests/alerts may report “healthy ISR” while the data-plane is effectively
running a different system, masking real issues.

---

### Operational Benefits (Day-2)

The next two sections are related but distinct:
- **Operational Benefits Summary**: impact on tooling/alerts and operator mental model.
- **Leadership Benefits for Operations**: impact on partition-scoped job ownership and debugging.

#### Operational Benefits Summary

The proposed design (Approach C) provides clear operational benefits compared to alternatives:

| Aspect                    | Approaches A/B (faking)        | Approach C (proposed)          |
|---------------------------|--------------------------------|--------------------------------|
| Tooling accuracy          | `describe topic` misleading    | `describe topic` accurate      |
| Under-replicated alerts   | False positives                | Meaningful (informational)     |
| Job debugging             | "Which broker ran this?"       | "Leader of partition X"        |
| Incident response         | Check all broker logs          | Check leader broker            |
| Capacity planning         | Unpredictable load             | Proportional to leadership     |
| Standard Kafka ops        | May not work as expected       | Work normally                  |

#### Leadership Benefits for Operations

Current diskless uses **racing/randomized** job assignment. Leadership provides:

| Benefit                 | Racing Model               | Leader Model                  |
|-------------------------|----------------------------|-------------------------------|
| Accountability          | "Which broker ran this?"   | "Leader of partition X"       |
| Load distribution       | Random, unpredictable      | Proportional to leadership    |
| Capacity planning       | Add brokers, hope it helps | Rebalance partition leaders   |
| Incident response       | Check all broker logs      | Check leader broker           |
| Tooling                 | Custom                     | Standard Kafka tooling        |

**Observing progress / correctness:**
- **Leadership distribution**: standard `LeaderCount` / leader imbalance metrics per broker.
- **Job ownership**: leader-scoped logs/metrics (e.g., “job started/completed for partition X on leader Y”).
- **Rebalancing effectiveness**: changes in leader distribution after preferred leader election / reassignment.

Leadership isn't just about RLM — it's about having a **deterministic owner** for partition-level operations.
