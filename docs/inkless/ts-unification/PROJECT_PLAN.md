# Tiered Storage Unification - Project Plan

**Date:** December 16, 2025  
**Team:** 3 Full-Time Senior Engineers  
**Reference:** [Design Document](./TIERED_STORAGE_UNIFICATION_DESIGN.md)

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Total Duration** | 16 weeks (4 months) |
| **With Buffer** | 18 weeks (4.5 months) |
| **Critical Path** | 8 weeks to first value delivery |
| **Team Size** | 3 Senior Engineers |

---

## Team Structure

| Engineer | Focus Area | Key Skills Needed |
|----------|------------|-------------------|
| **Engineer A** | Control Plane, Database, Offset Tracking | PostgreSQL, Kafka metadata |
| **Engineer B** | Read/Write Path, RLM Integration | Kafka internals, UnifiedLog, RLM |
| **Engineer C** | Tiering Pipeline, Observability | Object storage, batch processing |

---

## Phase Dependency Analysis

### Critical Path (Blockers)

These phases are **sequential dependencies** - they block downstream work:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CRITICAL PATH                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐              │
│  │ P1:Foundation│─────►│ P4:RF=3      │─────►│ P2:Read Path │              │
│  │ (2 weeks)    │      │ (2 weeks)    │      │ (2 weeks)    │              │
│  └──────────────┘      └──────────────┘      └──────────────┘              │
│                               │                     │                       │
│                               │                     ▼                       │
│                               │              ┌──────────────┐              │
│                               │              │ P3:Switch    │              │
│                               │              │ (2 weeks)    │              │
│                               │              └──────┬───────┘              │
│                               │                     │                       │
│                               ▼                     ▼                       │
│                        ┌──────────────┐      ┌──────────────┐              │
│                        │ P6:Segment   │      │ P5:RLM       │              │
│                        │ Merging      │      │ Integration  │              │
│                        │ (leader req) │      │ (2 weeks)    │              │
│                        └──────────────┘      └──────────────┘              │
│                                                                             │
│  RF=3 is foundational: The tiering pipeline's merge phase requires a       │
│  real partition leader to write segments to RLM.                           │
│                                                                             │
│  Critical Path Duration: P1 → P4 → P2 → P3/P5 → P6 → P7 = ~10 weeks       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Non-Blocking Phases (Can Run in Parallel)

These phases have **independent dependencies** and can be parallelized:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        NON-BLOCKING PHASES                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  After P1 (Foundation):                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  P2b: Batch Splitting (3 weeks) - Engineer C                         │  │
│  │  • Only needs schema from P1                                          │  │
│  │  • No dependency on read path or migration                            │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  After P3 (Switch Mechanism):                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  P4: Multi-Replica (2 weeks) - REQUIRED for RLM                       │  │
│  │  • RF=3 needed for leader-based RLM integration                       │  │
│  │  • Can run parallel with P5/P6                                        │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  After P2b (Batch Splitting):                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  P6: Segment Merging (4 weeks) - Engineer C                           │  │
│  │  • Independent of migration path                                      │  │
│  │  • Only needs split files to exist                                    │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  Deferrable:                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  P8: Observability (4 weeks) - Can be reduced for MVP                 │  │
│  │  • Basic metrics in 1 week                                            │  │
│  │  • Admin APIs and load testing can follow                             │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Phased Delivery Options

### Option A: 4-Week MVP (1 Month) - Migration Only

**Goal:** Enable tiered topics to migrate to diskless (no tiering pipeline yet)

```
Week 1-2: Foundation (P1)
Week 3-4: Read Path (P2) + Switch Mechanism start (P3) + Multi-Replica (P4)
Week 5-6: Switch Mechanism complete (P3) + Basic RLM (P5 partial)
```

| What Works | What Doesn't |
|------------|--------------|
| ✓ Migrate tiered → hybrid | ✗ Diskless → tiered conversion |
| ✓ Read from tiered + diskless | ✗ PG metadata cleanup |
| ✓ New writes to diskless | ✗ Admin APIs |
| ✓ RF=3 with leader-based RLM | ✗ Full observability |

**Risk:** PG tables will grow for diskless data until tiering is added.

---

### Option B: 8-Week Release (2 Months) - Full Tiering Pipeline

**Goal:** Complete tiering pipeline, PG scalability solved

```
Week 1-2:   Foundation (P1) - All engineers
Week 3-4:   Read Path (P2) - A+B | Batch Splitting (P2b) - C
Week 5-6:   Switch Mechanism (P3) + Multi-Replica (P4) - A+B | Batch Splitting cont. - C
Week 7-8:   RLM Integration (P5) - A+B | Segment Merging (P6) - C
Week 9-10:  E2E Integration (P7) - All | Buffer
```

| What Works | What Doesn't |
|------------|--------------|
| ✓ Migrate tiered → hybrid | ✗ Full observability (basic only) |
| ✓ Diskless → tiered conversion | ✗ Admin APIs (deferred) |
| ✓ PG metadata cleanup | ✗ Extensive load testing |
| ✓ Read from all tiers | |
| ✓ RF=3 with leader-based RLM | |
| ✓ Distributed tiering pipeline | |

**Deferred to follow-up:**
- P8: Full Observability (3-4 weeks)

---

### Option C: 16-Week Full Release (4 Months)

**Goal:** Production-ready with all features

All phases as described in the design document.

---

## Recommended Approach: Option B (8 Weeks)

### Why 8 Weeks?

1. **Solves the scalability problem** - PG metadata is cleaned up
2. **Full tiering pipeline** - Both directions work (tiered→hybrid, diskless→tiered)
3. **Reasonable scope** - Defers nice-to-haves (RF=3, full observability)
4. **Parallelizable** - Engineer C works on tiering while A+B do migration

### 8-Week Detailed Timeline

```
Week  1  2  3  4  5  6  7  8  9  10
      ├──┴──┼──┴──┼──┴──┼──┴──┼──┴──┤
Eng A │ P1  │ P2  │ P3  │ P5  │ P7  │
Eng B │ P1  │ P2  │ P3  │ P5  │ P7  │
Eng C │ P1  │ P2b │ P2b │ P6  │ P6/7│
      └─────┴─────┴─────┴─────┴─────┘
       Found Read  Migr  RLM   E2E
             Path  ation Tier  Test
```

---

## Detailed 8-Week Plan

### Week 1-2: Foundation (P1)

**All Engineers**

| Task | Owner | Days |
|------|-------|------|
| Database schema (inkless_log extensions) | A | 3 |
| Migration state machine | A | 2 |
| Tiering state enum on batches | A | 1 |
| Control plane interface stubs | A | 2 |
| RLM internals deep-dive | B | 3 |
| FetchHandler routing prototype | B | 4 |
| Database schema (split_batch, claims) | C | 3 |
| Tiering eligibility checker | C | 3 |
| Integration test harness | All | 2 |

**Exit Criteria:**
- [ ] Schema migrations ready
- [ ] State enums defined
- [ ] Test infrastructure working

---

### Week 3-4: Read Path (P2) + Batch Splitting Start (P2b)

**Engineers A & B: Read Path**

| Task | Owner | Days |
|------|-------|------|
| `HybridTopicOffsets` queries | A | 3 |
| Offset boundary implementation | A | 3 |
| `HybridFetchRouter` | B | 4 |
| UnifiedLog integration | B | 3 |
| Three-tier read tests | A+B | 3 |

**Engineer C: Batch Splitting**

| Task | Owner | Days |
|------|-------|------|
| WAL file claiming mechanism | C | 4 |
| `BatchSplitterJob` skeleton | C | 3 |
| WAL parsing + batch grouping | C | 3 |

**Exit Criteria:**
- [ ] Can read from local, tiered, and diskless
- [ ] WAL files can be claimed by brokers
- [ ] Batch splitting works in isolation

---

### Week 5-6: Switch Mechanism (P3) + Batch Splitting Complete (P2b)

**Engineers A & B: Switch Mechanism**

| Task | Owner | Days |
|------|-------|------|
| `initializeHybridTopic()` | A | 3 |
| Controller `migrateToDiskless()` | A | 4 |
| `DisklessMigrationHandler` | B | 5 |
| Segment roll + boundary capture | B | 3 |
| Migration integration tests | A+B | 3 |

**Engineer C: Batch Splitting Complete**

| Task | Owner | Days |
|------|-------|------|
| `SplitBatchWriter` | C | 4 |
| `recordSplitBatch()` control plane | C | 2 |
| `markBatchesAsSplit()` | C | 2 |
| Splitting unit tests | C | 2 |

**Exit Criteria:**
- [ ] Tiered topic can migrate to hybrid
- [ ] Split files created from WAL files
- [ ] Batch state updated to SPLIT

---

### Week 7-8: RLM Integration (P5) + Segment Merging (P6)

**Engineers A & B: RLM Integration**

| Task | Owner | Days |
|------|-------|------|
| Skip copy task for hybrid | A | 3 |
| `RLMExpirationTask` hybrid mode | A | 4 |
| Register segments with RLMM | B | 4 |
| RLM integration tests | A+B | 3 |

**Engineer C: Segment Merging**

| Task | Owner | Days |
|------|-------|------|
| `SegmentMergerJob` skeleton | C | 3 |
| `LogSegmentBuilder` + indexes | C | 5 |
| RSM upload integration | C | 3 |
| `deleteTieredBatchMetadata()` | C | 2 |

**Exit Criteria:**
- [ ] Hybrid topics work with RLM
- [ ] Segments created from split files
- [ ] PG metadata cleaned up after tiering

---

### Week 9-10: E2E Integration + Buffer (P7)

**All Engineers**

| Task | Owner | Days |
|------|-------|------|
| `TieringScheduler` | C | 3 |
| E2E tiering test (diskless→hybrid) | C | 3 |
| E2E migration test (tiered→hybrid) | A+B | 3 |
| Offset boundary validation | A | 2 |
| PG cleanup validation | A | 2 |
| Basic metrics (critical only) | All | 3 |
| Bug fixes and edge cases | All | 4 |

**Exit Criteria:**
- [ ] Full pipeline works end-to-end
- [ ] PG table size bounded
- [ ] Basic operational metrics

---

## Follow-Up Work (Post 8-Week Release)

### Phase 8: Full Observability (3-4 weeks)
- Complete metrics suite
- Admin APIs (`getTieringStatus`, `triggerTiering`, etc.)
- Load testing and performance tuning
- Documentation and runbooks

**Total Follow-Up:** 3-4 weeks with 1-2 engineers

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| RLM complexity | Medium | +2 weeks | Deep-dive in Week 1 |
| Index building bugs | Medium | +1 week | Reuse Kafka code, extensive tests |
| PG performance at scale | Low | +1 week | Test with production-like data |
| Integration test flakiness | Medium | +1 week | Invest in test infra Week 1-2 |

**Recommended Buffer:** Week 9-10 includes 3-4 days buffer

---

## Success Metrics

### 8-Week Release

| Metric | Target |
|--------|--------|
| Tiered → Hybrid migration | Works for all topic configs |
| Diskless → Tiered conversion | Processes batches within 2x local.retention.ms |
| PG table growth | Bounded by local.retention.ms worth of data |
| Read latency | No regression from current diskless |
| Tiering throughput | 100 MB/s per broker minimum |

---

## Dependencies & Prerequisites

### Before Starting

- [ ] PostgreSQL test environment with production-like scale
- [ ] Object storage test buckets configured
- [ ] RLM test topics with tiered data
- [ ] Integration test framework ready
- [ ] Performance benchmarking baseline

### External Dependencies

| Dependency | Owner | Risk |
|------------|-------|------|
| RSM configuration | Ops team | Low |
| RLMM topic setup | Ops team | Low |
| PG schema migration approval | DBA | Medium |

---

## Communication Plan

### Weekly Sync

- **When:** Weekly, 30 min
- **Who:** All 3 engineers + PM
- **Agenda:** Progress, blockers, decisions needed

### Milestone Reviews

| Milestone | Week | Review |
|-----------|------|--------|
| Foundation complete | 2 | Demo schema, test harness |
| Read path working | 4 | Demo three-tier reads |
| Migration working | 6 | Demo tiered → hybrid |
| Tiering working | 8 | Demo full pipeline, PG cleanup |

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| TBD | RF=3 deferred | Not critical for initial value delivery |
| TBD | Full observability deferred | Basic metrics sufficient for launch |
| TBD | 8-week timeline selected | Balances scope vs. time to value |

