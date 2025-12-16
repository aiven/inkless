# Tiered Storage Unification - Summary

**Date:** December 16, 2025  
**Status:** Design Ready for Review  
**Authors:** Engineering Team

---

## TL;DR

We're unifying Tiered Storage and Diskless (Inkless) so that:

1. **Existing tiered topics can migrate to diskless** without data loss
2. **Diskless topics age their data into tiered format** to keep PostgreSQL bounded
3. **HYBRID becomes the stable end state** for all diskless topics

---

## Why This Matters

### The Problem

| Issue | Impact |
|-------|--------|
| **No migration path** | Customers can't move tiered topics to diskless |
| **PG scalability** | `inkless_batch` table grows unbounded → query degradation |
| **Fragmented storage** | Two incompatible storage models with no interop |

### The Solution

```
┌─────────────────────────────────────────────────────────────────┐
│                    HYBRID TOPIC MODEL                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   TIERED STORAGE              DISKLESS STORAGE                  │
│   (Cold Data)                 (Hot Data)                        │
│                                                                 │
│   ┌─────────────────┐         ┌─────────────────┐              │
│   │ Historical      │◄────────│ Aged batches    │              │
│   │ segments        │ convert │ (> local.       │              │
│   │ (RLM managed)   │         │  retention.ms)  │              │
│   └─────────────────┘         └─────────────────┘              │
│          │                           │                          │
│          │                           │                          │
│          ▼                           ▼                          │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │              UNIFIED READ PATH                          │  │
│   │   offset < disklessStart → Tiered (via UnifiedLog/RLM)  │  │
│   │   offset >= disklessStart → Diskless (via Reader)       │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### 1. Read Path: Use UnifiedLog for Local + Tiered

Instead of building custom tiered read logic, we leverage `UnifiedLog.read()` which already handles local → tiered fallback. Diskless only needs to route based on offset:

- `offset < disklessStartOffset` → Classic path (UnifiedLog handles local/tiered)
- `offset >= disklessStartOffset` → Diskless path (Reader)

### 2. Tiering Pipeline: Split, Merge, and Cleanup

The tiering pipeline converts aged diskless batches to tiered segments in two phases:

| Phase | Runs On | What It Does |
|-------|---------|--------------|
| **Split** | Any broker | Extract partition batches from shared WAL files (time-ordered, no coordination needed) |
| **Merge** | Partition leader | Combine split files into tiered segments, upload to RSM, register with RLMM, **delete batch metadata from PG** |

This achieves:
- **Horizontal scalability** — Any broker can split WAL files independently
- **RLM consistency** — Only leader writes tiered segments (standard Kafka semantics)
- **PG scalability** — Batch metadata deleted after tiering; table size bounded by `local.retention.ms`

### 3. HYBRID as Stable End State

HYBRID is not a transition—it's the **permanent operating mode**:
- Migrated topics: Start with existing tiered data + new diskless writes
- New diskless topics: Become HYBRID as aged data converts to tiered

---

## Scope

### In Scope (8-Week Target)

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| **RF=3 for diskless topics** | Standard Kafka replication semantics; enables leader election needed for RLM integration | Foundation |
| **Three-tier read path** | Read from local, tiered, or diskless seamlessly based on offset boundaries | RF=3 |
| **Tiered → Hybrid migration** | Existing tiered topics can switch to diskless writes while preserving access to historical tiered data | Read path, RF=3 |
| **Tiering pipeline** | Convert aged diskless batches to tiered segments (split → merge → cleanup PG metadata); merger runs on partition leader | RF=3 (leader required) |

**Note:** RF=3 is foundational—the tiering pipeline's merge phase requires a real partition leader to write segments to RLM. This must be implemented early.

### Deferred (Follow-up)

| Feature | Reason | Effort |
|---------|--------|--------|
| Full observability suite | Basic metrics sufficient for launch | 3-4 weeks |
| Admin APIs (pause/trigger tiering) | Can follow after core functionality | Included above |
| Extensive load testing | Basic validation in scope, deep perf tuning follows | Included above |
| **Diskless → Tiered migration** | Reverse direction for rollback scenarios; requires tiering pipeline stable first | 4-5 weeks |

---

## Timeline Overview

**Recommended: 8-10 weeks with 3 senior engineers**

```
Week  1  2  3  4  5  6  7  8  9  10
      ├──┴──┼──┴──┼──┴──┼──┴──┼──┴──┤
Eng A │ P1  │ P4  │ P2  │ P3  │ P7  │  Control Plane, RF=3, Migration
Eng B │ P1  │ P4  │ P2  │ P5  │ P7  │  RF=3, Read Path, RLM Integration
Eng C │ P1  │ P2b │ P2b │ P6  │ P6/7│  Tiering Pipeline (split, then merge)
      └─────┴─────┴─────┴─────┴─────┘

P1 = Foundation      P3 = Migration Switch    P6 = Segment Merging
P2 = Read Path       P4 = RF=3 (early!)       P7 = E2E Integration
P2b = Batch Split    P5 = RLM Integration
```

**Critical Path:** P1 → P4 (RF=3) → P2 (Read) → P3/P5 (Migration/RLM) → P6 (Merge) → P7 (E2E)

RF=3 is moved to Week 3-4 because the tiering pipeline's merge phase requires a real partition leader.

### Milestones

| Week | Milestone | Demo |
|------|-----------|------|
| 4 | **Read path working** | Read from local + tiered + diskless |
| 6 | **Migration working** | Tiered topic → Hybrid |
| 8 | **Tiering working** | Diskless batches → Tiered segments |
| 10 | **E2E validated** | Full pipeline, PG cleanup confirmed |

---

## Key Risks

| Risk | Mitigation |
|------|------------|
| RLM complexity | Deep-dive in Week 1, spike if needed |
| Index building | Reuse existing Kafka segment builder code |
| PG scale issues | Load test with production-like data early |

---

## Open Questions for Discussion

1. **Transactional records during migration** — Block, drain, or continue?
2. **Segment size for converted batches** — Use topic's `segment.bytes` or separate config?
3. **WAL file cleanup timing** — When is a WAL file safe to delete?

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

### For Today's Meeting

- [ ] Does the HYBRID model make sense as the end state?
- [ ] Is the split/merge job separation the right approach?
- [ ] Should we target 8 weeks or go longer for more features?
- [ ] Who owns which focus area (Control Plane / Read Path / Tiering)?

### For Follow-Up

- [ ] What's the migration strategy for existing production topics?
- [ ] Do we need backwards compatibility for any APIs?
- [ ] What observability is must-have vs. nice-to-have?
