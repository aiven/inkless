# Tiered Storage Unification - Summary

**Date:** December 16, 2025  
**Status:** Design Ready for Review  
**Authors:** Engineering Team

---

## TL;DR

We're unifying Tiered Storage and Diskless (Inkless) so that:

1. **Existing tiered topics can migrate to diskless** — Read old data from tiered storage, write new data to diskless
2. **HYBRID becomes the stable end state** — Tiered (cold) + Diskless (hot) with a clear offset boundary
3. **Tiering pipeline (diskless → tiered) is deferred** — Focus the 8-week scope on migration; tiering conversion can follow later

---

## Why This Matters

### The Problem

| Issue | Impact |
|-------|--------|
| **No migration path** | Customers can't move tiered topics to diskless |
| **PG scalability (future)** | `inkless_batch` table will grow unbounded without tiering pipeline |
| **Backlog fetch performance** | S3 fetches for old data consume CPU, degrading write path |
| **Fragmented storage** | Two incompatible storage models with no interop |

### The Solution

```
┌─────────────────────────────────────────────────────────────────┐
│                    HYBRID TOPIC MODEL                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   TIERED STORAGE              DISKLESS STORAGE                  │
│   (Cold Data - Read Only)     (Hot Data - Active Writes)        │
│                                                                 │
│   ┌─────────────────┐         ┌─────────────────┐              │
│   │ Historical      │         │ New writes      │              │
│   │ segments        │         │ (after seal)    │              │
│   │ (RLM managed)   │         │                 │              │
│   └─────────────────┘         └─────────────────┘              │
│          │                           │                          │
│          │                           │                          │
│          ▼                           ▼                          │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │              UNIFIED READ PATH                          │  │
│   │   1. Try Diskless (hot data)                            │  │
│   │   2. Else try Local log (pending copy during migration) │  │
│   │   3. Else read from Tiered (via RLM)                    │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### 1. Read Path Priority: Diskless → Local → Tiered

The read path follows a fallback strategy:

1. **Try Diskless first** — Hot data, most recent writes
2. **Else try Local log** — During migration window, pending copy to tiered
3. **Else read from Tiered** — Historical data via RLM

This is guided by metadata that tracks the offset boundary between tiers.

### 2. RF=1 vs RF=3 (To Be Researched)

The team will evaluate both replication factor approaches:

**RF=1 (Current Diskless Model):**
- Simpler, no changes to existing diskless replication
- Question: Can tiered reads work with metadata transformer "faking" leader?

**RF=3 (Standard Kafka Semantics):**
- Tiered read path is replica-based, requires leader
- RLM job scheduling expects leader semantics
- More aligned with classic Kafka, but more changes required

**Research Output:** Determine which approach enables the read path with least friction, then proceed to migration implementation.

### 3. HYBRID as Stable End State

HYBRID (tiered tail + diskless head) is the permanent operating mode:
- Migrated topics: Tiered data (read-only) + new diskless writes
- The tiering pipeline (diskless → tiered) is deferred but acknowledged as needed for PG scalability

---

## Scope

### In Scope (8-Week Target) — Migration Focus

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| **RF research (RF=1 vs RF=3)** | Evaluate both replication factors for tiered read path compatibility; determine which enables RLM integration | Foundation |
| **Three-tier read path** | Diskless → Local → Tiered fallback based on offset boundaries | RF research |
| **Tiered → Hybrid migration** | Switch writes to diskless, preserve tiered read access | Read path |

**Research Phase (Weeks 1-2):** Validate whether RF=1 (current diskless model) can support tiered reads, or if RF=3 (standard Kafka semantics) is required. The decision will inform the migration implementation.

**Team Decision:** The 8-week scope focuses on **migration safety and effectiveness**. The tiering pipeline is deferred.

### Deferred (Follow-up)

| Feature | Reason | Effort |
|---------|--------|--------|
| **Tiering pipeline** | Complex; not needed for initial migration. Required later for PG scalability. | 4-6 weeks |
| **Sealing mechanism (KRaft)** | Needed for bidirectional migration; not required for one-way tiered→diskless | See Delos eval |
| **MetaStore (S3 log chain)** | Tracks offset boundaries for bidirectional migration; part of Delos-like architecture | See Delos eval |
| **Backlog fetch workaround** | If tiering is delayed, need interim solution for S3 CPU consumption | TBD |
| Full observability suite | Basic metrics sufficient for launch | 3-4 weeks |
| Admin APIs (pause/trigger) | Can follow after core functionality | Included above |
| **Diskless → Tiered migration** | Reverse direction for rollback; requires sealing + MetaStore | 4-5 weeks |

**Note:** Sealing and MetaStore are part of the Delos-inspired "virtual log" architecture. See [Delos Evaluation](./TIERED_STORAGE_UNIFICATION_DELOS_EVALUATION.md) for details on how these enable bidirectional migration.

---

## Timeline Overview

**Recommended: 8 weeks with 3 senior engineers — Migration Focus**

```
Week  1  2  3  4  5  6  7  8
      ├──┴──┼──┴──┼──┴──┼──┴──┤
Eng A │ P1  │ RF  │ P2  │ P3  │  Foundation, RF Research, Read Path
Eng B │ P1  │ RF  │ P2  │ P3  │  RF Research, Read Path, Migration
Eng C │ P1  │ P2  │ P3  │ E2E │  Read Path, Migration, Testing
      └─────┴─────┴─────┴─────┘

P1 = Foundation      P3 = Migration Switch
RF = RF Research     E2E = Integration Testing
P2 = Read Path       
```

**Critical Path:** P1 → RF Research (decide RF=1 or RF=3) → P2 (Read Path) → P3 (Migration) → E2E

### Milestones

| Week | Milestone | Demo |
|------|-----------|------|
| 2 | **RF decision made** | RF=1 or RF=3 validated for tiered reads |
| 4 | **Read path working** | Read from diskless + local + tiered |
| 6 | **Migration working** | Switch writes to diskless |
| 8 | **E2E validated** | Full migration flow, no data loss |

---

## Key Risks

| Risk | Mitigation |
|------|------------|
| RLM complexity | Deep-dive in Week 1, spike if needed |
| RF=1 may not work for tiered reads | Research phase will validate; fallback to RF=3 if needed |
| Backlog fetch CPU consumption | If tiering pipeline deferred, need interim workaround for S3 fetches |

---

## Open Questions for Discussion

1. **RF=1 vs RF=3 for tiered reads** — Can RF=1 with metadata transformer work, or is RF=3 required for RLM integration?
2. **Backlog fetch workaround** — If tiering pipeline is delayed, what interim solution for S3 CPU consumption?
3. **Tiered storage metadata topic** — Potential to use for diskless metadata (interesting but deeper investigation needed)

---

## Documents

| Document | Purpose |
|----------|---------|
| **[Design Document](./TIERED_STORAGE_UNIFICATION_DESIGN.md)** | Full technical design, code examples, data flows |
| **[Project Plan](./TIERED_STORAGE_UNIFICATION_PROJECT_PLAN.md)** | Timeline, task breakdown, risk assessment |

---

## Next Steps

1. **Review this summary** — Align on scope and approach
2. **Deep-dive on design doc** — Async review, collect questions
3. **Finalize timeline** — Confirm 8-week vs. longer
4. **Assign engineers** — Match skills to focus areas
5. **Kick off P1** — Foundation phase

---

## Discussion Points

### Decisions Made

- [x] **Focus on migration first** — 8 weeks dedicated to safe, effective migration
- [x] **Tiering pipeline deferred** — Complex, can follow later; need workaround for backlog fetches
- [x] **HYBRID is end state** — Tiered (read-only tail) + Diskless (active head)
- [x] **Sealing + MetaStore deferred** — Needed for bidirectional migration (see Delos eval)

### Still to Explore

- [ ] **RF=1 vs RF=3** — Research which replication factor works for tiered reads (Week 1-2)
- [ ] Tiered storage metadata topic for diskless metadata replication
- [ ] Backlog fetch performance workaround if tiering delayed

### For Follow-Up

- [ ] Tiering pipeline design refinement (when ready to implement)
- [ ] Diskless → Tiered reverse migration
- [ ] Full observability and admin APIs
