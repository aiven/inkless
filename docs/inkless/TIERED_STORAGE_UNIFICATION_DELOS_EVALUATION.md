# Delos Virtual Consensus - Evaluation for Tiered Storage Unification

**Date:** December 16, 2025  
**Status:** Analysis / Discussion  
**Reference:** [Delos: Virtual Consensus in Delos (OSDI 2020)](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)

---

## Executive Summary

This document evaluates how Facebook's Delos virtual consensus architecture relates to our Tiered Storage Unification design and whether it offers an alternative or complementary path.

**Key Finding:** Delos doesn't provide a "totally different path" but rather offers **architectural patterns that our proposed implementation could evolve toward**. The core insight of Delos—decoupling the consensus API from its implementation via a virtualization layer—aligns well with our hybrid storage model.

---

## Delos Overview

### What is Delos?

Delos is a storage system built around **virtual consensus**: the idea that a distributed system's state machine layer can be decoupled from its underlying consensus engine through a virtualization abstraction called the **VirtualLog**.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DELOS ARCHITECTURE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                     Application (State Machine)                     │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                        │
│                                    ▼                                        │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                         VirtualLog API                               │  │
│   │                  (append, read, trim, seal)                          │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                        │
│                                    ▼                                        │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                      Virtualization Layer                           │  │
│   │                                                                      │  │
│   │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐             │  │
│   │   │  Loglet 1   │───►│  Loglet 2   │───►│  Loglet 3   │             │  │
│   │   │ (ZooKeeper) │    │   (NVMe)    │    │ (LogDevice) │             │  │
│   │   └─────────────┘    └─────────────┘    └─────────────┘             │  │
│   │        sealed             sealed            active                   │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   Key Insight: The VirtualLog chains multiple "loglets" (physical logs)    │
│   together, allowing live reconfiguration without stopping the system.     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Core Delos Concepts

| Concept | Description |
|---------|-------------|
| **VirtualLog** | A logical log abstraction that spans multiple physical logs |
| **Loglet** | A physical log implementation (can be ZooKeeper, custom, etc.) |
| **Sealing** | Marking a loglet as immutable, forcing writes to the next loglet |
| **MetaStore** | Stores the chain of loglets and their mappings |
| **Log Reconfiguration** | Ability to switch consensus implementations live |

---

## Mapping Delos to Our Design

### Conceptual Alignment

Our hybrid storage model shares key concepts with Delos:

| Delos Concept | Our Equivalent | Alignment |
|---------------|----------------|-----------|
| **VirtualLog** | Hybrid Topic (unified offset space) | ✓ Strong |
| **Loglet** | Storage tier (Diskless WAL, Tiered Segment) | ✓ Strong |
| **Sealing** | Migration switch (rotate segment, mark disklessStartOffset) | ✓ Strong |
| **MetaStore** | Control Plane (PostgreSQL) | ✓ Strong |
| **Log Reconfiguration** | Tiered → Hybrid migration | ✓ Partial |

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    OUR DESIGN vs. DELOS MAPPING                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   DELOS:                              OUR DESIGN:                           │
│                                                                             │
│   VirtualLog                          Hybrid Topic                          │
│   ┌─────────────────────────────┐     ┌─────────────────────────────┐      │
│   │ offset 0      offset N      │     │ offset 0      offset N      │      │
│   │    │             │          │     │    │             │          │      │
│   │    ▼             ▼          │     │    ▼             ▼          │      │
│   │ ┌──────┐ ┌──────┐ ┌──────┐  │     │ ┌──────┐ ┌──────┐ ┌──────┐  │      │
│   │ │Log 1 │→│Log 2 │→│Log 3 │  │     │ │Tiered│→│Local │→│Diskl.│  │      │
│   │ │sealed│ │sealed│ │active│  │     │ │(RLM) │ │(pend)│ │(WAL) │  │      │
│   │ └──────┘ └──────┘ └──────┘  │     │ └──────┘ └──────┘ └──────┘  │      │
│   └─────────────────────────────┘     └─────────────────────────────┘      │
│                                                                             │
│   MetaStore:                          Control Plane:                        │
│   - Loglet chain                      - Offset boundaries                   │
│   - Loglet endpoints                  - Migration state                     │
│   - Seal status                       - Tiering state                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Differences

| Aspect | Delos | Our Design | Impact |
|--------|-------|------------|--------|
| **Direction** | Append-only forward | Bidirectional (tiered↔diskless) | We need conversion, not just chaining |
| **Sealing** | Loglet sealed, never written again | Tiered data may be read, eventually deleted | Retention adds complexity |
| **Consensus** | Can swap ZK→Raft→custom | Fixed: Kafka replication + object storage | Less flexibility, but simpler |
| **MetaStore** | Dedicated consensus system | PostgreSQL | Different reliability model |
| **Active writers** | One active loglet | One active tier (diskless) | Similar |

---

## Does Delos Offer a Different Path?

### What Delos Suggests We Could Do Differently

#### 1. Explicit Loglet Abstraction

Delos uses a formal `Loglet` interface. We could formalize our storage tiers similarly:

```java
// Delos-inspired abstraction
interface StorageTier {
    // Core operations
    CompletableFuture<Long> append(TopicPartition tp, RecordBatch batch);
    CompletableFuture<RecordBatch> read(TopicPartition tp, long offset);
    
    // Lifecycle
    CompletableFuture<Void> seal(TopicPartition tp);  // No more writes
    boolean isSealed(TopicPartition tp);
    
    // Boundaries
    long startOffset(TopicPartition tp);
    long endOffset(TopicPartition tp);
}

class TieredStorageTier implements StorageTier { /* RLM-backed */ }
class DisklessStorageTier implements StorageTier { /* WAL-backed */ }
class LocalLogTier implements StorageTier { /* UnifiedLog-backed */ }
```

**Value:** Cleaner abstraction, easier to test, could enable future storage backends.

**Cost:** Additional abstraction layer, may not be worth it for our scope.

#### 2. Chain-Based Offset Mapping

Delos maintains an explicit chain of loglets with offset mappings. We could do similar:

```java
// Current approach: Implicit boundaries
offset < disklessStartOffset → tiered
offset >= disklessStartOffset → diskless

// Delos-inspired: Explicit chain
record TierChain(List<TierSegment> segments) {
    TierSegment findTier(long offset) {
        return segments.stream()
            .filter(s -> s.contains(offset))
            .findFirst()
            .orElseThrow();
    }
}

record TierSegment(
    StorageTier tier,
    long startOffset,
    long endOffset,  // -1 if active
    boolean sealed
) {}
```

**Value:** More flexible for complex scenarios (multiple migrations, rollbacks).

**Cost:** Adds complexity for our current use case (single migration direction).

#### 3. Live Reconfiguration Without Downtime

Delos's key innovation is **live reconfiguration**—switching consensus implementations without stopping writes. Our migration does this partially:

| Delos | Our Migration |
|-------|---------------|
| Seal old loglet | Rotate local segment |
| Start new loglet | Start diskless writes |
| No gap in offset space | Offset boundary at disklessStartOffset |
| Can switch back | Can switch back using same read path |

**Value:** We already achieve zero-downtime migration.

**Insight: Bidirectional Migration Without Downtime**

The same three-tier read path (Local → Diskless → Tiered) can mitigate downtime in **both** migration directions:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              BIDIRECTIONAL MIGRATION WITH SCALED READ PATH                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  TIERED → DISKLESS (Forward):                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Read Path: Local (pending copy) → Tiered (historical) → Diskless   │   │
│  │                                                                      │   │
│  │  offset ──► Check Local ──► Check Tiered ──► Check Diskless         │   │
│  │             (if in range)   (if < disklessStart)  (if >= disklessStart)│ │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  DISKLESS → TIERED (Reverse):                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Read Path: Local (new writes) → Tiered (converted) → Diskless      │   │
│  │                                                                      │   │
│  │  offset ──► Check Local ──► Check Tiered ──► Check Diskless         │   │
│  │             (new classic)   (converted from   (remaining hot         │   │
│  │                              diskless)         data)                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  KEY INSIGHT: The read path doesn't care about migration direction!        │
│  It simply routes based on offset boundaries, which shift as data          │
│  moves between tiers.                                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Reverse Migration Without Downtime:**

1. **Continue diskless writes** while tiering pipeline converts old batches
2. **Read path serves from all tiers** — no interruption to consumers
3. **Gradually shift boundary** — `disklessStartOffset` moves forward as batches tier
4. **Once fully tiered** — switch write path to classic local log
5. **Resume RLM copy tasks** — normal tiered topic operation

This is analogous to Delos's approach where the VirtualLog continues serving reads from sealed loglets while new writes go to the active loglet. The key difference is our "loglets" (storage tiers) can have data flowing between them via the tiering pipeline.

```java
// Reverse migration with zero-downtime read path
class ReverseMigrationHandler {
    
    CompletableFuture<Void> migrateDisklessToTiered(TopicPartition tp) {
        // 1. Force-tier all diskless batches up to current HWM
        //    (tiering pipeline already does this, just accelerate)
        long targetOffset = controlPlane.getHighWatermark(tp);
        tieringPipeline.forceConvertUpTo(tp, targetOffset);
        
        // 2. Wait for conversion to complete
        //    READ PATH CONTINUES WORKING - serves from diskless until tiered
        while (controlPlane.getDisklessStartOffset(tp) <= targetOffset) {
            Thread.sleep(100);
            // Consumers keep reading - no downtime
        }
        
        // 3. Now all historical data is in tiered storage
        //    Switch write path to classic
        partition.enableClassicWrites();
        partition.disableDisklessWrites();
        
        // 4. Update topic config
        topicConfig.set("diskless.enable", false);
        
        // 5. Resume RLM copy tasks
        remoteLogManager.resumeCopyTask(tp);
        
        // Done - topic is now classic tiered, zero consumer downtime
    }
}
```

**This validates the Delos approach**: by maintaining a unified read path across all storage tiers, migrations become non-disruptive regardless of direction.

---

## Could Our Implementation Evolve Toward Delos?

### Evolution Path

Our proposed implementation could evolve toward a Delos-like architecture in phases:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EVOLUTION PATH                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  PHASE 1 (Current Design)        PHASE 2 (Abstraction)      PHASE 3 (Delos)│
│                                                                             │
│  ┌───────────────────┐          ┌───────────────────┐      ┌─────────────┐ │
│  │ FetchHandler      │          │ FetchHandler      │      │ VirtualLog  │ │
│  │ (routing logic)   │    →     │ (simple dispatch) │  →   │ (generic)   │ │
│  └───────────────────┘          └───────────────────┘      └─────────────┘ │
│           │                              │                        │         │
│           ▼                              ▼                        ▼         │
│  ┌───────────────────┐          ┌───────────────────┐      ┌─────────────┐ │
│  │ if/else routing   │          │ StorageTier iface │      │ TierChain   │ │
│  │ - UnifiedLog      │    →     │ - TieredTier      │  →   │ - Loglet[]  │ │
│  │ - Reader          │          │ - DisklessTier    │      │ - seal()    │ │
│  └───────────────────┘          └───────────────────┘      └─────────────┘ │
│           │                              │                        │         │
│           ▼                              ▼                        ▼         │
│  ┌───────────────────┐          ┌───────────────────┐      ┌─────────────┐ │
│  │ Control Plane     │          │ Control Plane     │      │ MetaStore   │ │
│  │ (PG + offsets)    │    →     │ (TierChain state) │  →   │ (consensus) │ │
│  └───────────────────┘          └───────────────────┘      └─────────────┘ │
│                                                                             │
│  Effort: Current scope          Effort: +2-3 weeks         Effort: +months │
│  Value: Solves problem          Value: Cleaner code        Value: Future   │
│                                                             flexibility     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Recommended Approach

**Phase 1 (Current Design)** is the right choice for the 8-week scope:

1. **Solves the immediate problem** — Migration and PG scalability
2. **Pragmatic complexity** — No over-engineering
3. **Can evolve** — The concepts align, abstraction can be added later

**Phase 2 (Abstraction)** could be a follow-up if:
- We add more storage backends
- We need more complex migration scenarios
- Testing becomes difficult without interfaces

**Phase 3 (Full Delos)** would require:
- Replacing or augmenting the Control Plane with a proper MetaStore
- Implementing the full VirtualLog abstraction
- Likely a ground-up redesign

---

## Specific Delos Insights for Our Design

### 1. Sealing Protocol

Delos's sealing protocol ensures no writes can occur after a seal. We should ensure similar guarantees:

```java
// Our migration seal should guarantee:
// 1. All pending writes complete before seal
// 2. No new tiered writes after seal
// 3. disklessStartOffset is set atomically

class MigrationSealer {
    CompletableFuture<Long> seal(TopicPartition tp) {
        // 1. Stop accepting new classic writes
        partition.blockClassicWrites();
        
        // 2. Flush pending writes
        partition.flush();
        
        // 3. Wait for RLM to copy final segment
        remoteLogManager.waitForCopy(tp);
        
        // 4. Atomically set boundary
        long boundary = remoteLogManager.highestOffset(tp) + 1;
        controlPlane.setDisklessStartOffset(tp, boundary);
        
        // 5. Enable diskless writes
        partition.enableDisklessWrites();
        
        return CompletableFuture.completedFuture(boundary);
    }
}
```

### 2. Trim/Retention as First-Class Operation

Delos has explicit `trim` to remove log prefix. Our tiered retention does similar:

```java
// Delos: trim(offset) - remove all entries < offset
// Us: Retention deletes tiered segments, updates remoteLogStartOffset

// We could make this more explicit:
interface StorageTier {
    void trim(TopicPartition tp, long offset);  // Delete data < offset
}
```

### 3. MetaStore Reliability

Delos uses a separate consensus system for MetaStore. Our Control Plane uses PostgreSQL:

| Delos MetaStore | Our Control Plane |
|-----------------|-------------------|
| Consensus-based (Paxos/Raft) | PostgreSQL (single leader) |
| Highly available | Depends on PG HA setup |
| Stores loglet chain | Stores offset boundaries, batch metadata |

**Consideration:** If Control Plane becomes a bottleneck or reliability concern, we could:
- Use Kafka itself as a MetaStore (like RLMM uses `__remote_log_metadata`)
- Add a caching layer with eventual consistency
- Consider a ZooKeeper/etcd backend

---

## Summary: Delos Influence on Our Design

### What We Should Adopt (8-Week Scope)

| Delos Concept | Recommendation | Priority |
|---------------|----------------|----------|
| **Offset chain model** | Track offset boundaries between tiers | High (in scope) |
| **Trim as first-class** | Already have via retention, make more explicit | Low |

### Future Work: Bidirectional Migration (Delos-Inspired)

When bidirectional migration (Diskless → Tiered) is needed, we should implement:

| Delos Concept | Implementation | Why Needed |
|---------------|----------------|------------|
| **Sealing mechanism** | KRaft-based seal record to mark topic state transitions | Ensures atomic switch, prevents writes to sealed log |
| **MetaStore** | S3-based log chain metadata with conditional writes | Tracks which tier owns which offset range; enables rollback |
| **StorageTier interface** | Formal abstraction for tiered/local/diskless | Cleaner code, easier testing, enables future backends |

These components enable:
- **Zero-downtime reverse migration** — Same scaled read path works in both directions
- **Rollback capability** — If diskless has issues, can migrate back to tiered
- **Consistent state tracking** — MetaStore is source of truth for log chain

### What We Can Defer

| Delos Concept | Why Defer |
|---------------|-----------|
| Formal VirtualLog abstraction | Over-engineering for current scope |
| Pluggable consensus backends | We don't need to swap Kafka replication |
| MetaStore with consensus | PG/S3 is sufficient for now |

**Note:** Bidirectional reconfiguration (Diskless → Tiered) can reuse the same scaled read path, enabling zero-downtime reverse migration in a future phase. See the [Design Document Section 11](./TIERED_STORAGE_UNIFICATION_DESIGN.md) for details.

### Conclusion

**Delos validates our architectural direction** rather than suggesting a different path:

1. **Unified log abstraction** — Our hybrid topic model is conceptually similar to VirtualLog
2. **Chained storage tiers** — Our tiered + diskless model mirrors loglet chaining
3. **Seal-based migration** — Future sealing mechanism will use similar semantics
4. **Metadata-driven routing** — Both use a control plane for offset-to-tier mapping

**Our implementation can evolve toward Delos patterns** when bidirectional migration is needed. The current 8-week scope focuses on one-way migration (tiered → diskless) which doesn't require the full sealing/MetaStore infrastructure.

---

## References

1. [Delos: Virtual Consensus in Delos (OSDI 2020)](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)
2. [Tiered Storage Unification - Design Document](./TIERED_STORAGE_UNIFICATION_DESIGN.md)
3. [Tiered Storage Unification - Project Plan](./TIERED_STORAGE_UNIFICATION_PROJECT_PLAN.md)

