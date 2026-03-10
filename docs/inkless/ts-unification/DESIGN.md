# Tiered Storage Unification for Zero-copy Migration

## Design Document

**Authors:** Engineering Team  
**Date:** December 2025  
**Status:** Draft  
**Last Updated:** December 16, 2025 (post team discussion)

---

## 1. Executive Summary

This document outlines the design for enabling seamless migration from Apache Kafka's Tiered Storage to Inkless (Diskless) topics. The goal is to allow topics with existing tiered data to transition to the Diskless storage model while preserving read access to historical tiered data and enabling new writes to flow through the Diskless pipeline.

### Goals

1. **Migration from Tiered to Diskless** — Enable tiered topics to switch to diskless writes
2. **Hybrid read path** — Read new data from diskless, old data from tiered storage
3. **Managed RF (user-defined, RF=-1 → default)** — KRaft-managed replicas with rack-aware placement (see [DISKLESS_MANAGED_RF.md](DISKLESS_MANAGED_RF.md))
4. **Sealing mechanism** — KRaft-based topic sealing for safe migration
5. **MetaStore** — S3-based log chain metadata for offset boundaries
6. **Tiering pipeline (diskless → tiered)** — Converting aged diskless batches to tiered format for PG scalability

### Deferred (Future Phases)

1. **Reverse migration (diskless → tiered)** — Rollback capability
2. **Full observability** — Admin APIs, comprehensive metrics

### Team Decision (Dec 16, 2025)

The team agreed to focus on **migration safety and effectiveness**. The tiering pipeline is necessary for PG scalability and is included in scope. Managed RF approach has been decided (see [DISKLESS_MANAGED_RF.md](DISKLESS_MANAGED_RF.md)).

---

## 2. Background

### 2.1 Current Architecture

#### RemoteLogManager (RLM)

The `RemoteLogManager` is the central broker component that manages Tiered Storage integration:

- **Single instance per broker** - One RLM manages all tiered topics on a broker
- **Task-based scheduling** - Schedules copy, delete, and read tasks based on replica role
- **Key tasks:**
  - `RLMCopyTask`: Copies log segments to remote storage (leader only)
  - `RLMExpirationTask`: Handles retention-based cleanup (leader only)  
  - `RLMFollowerTask`: Tracks highest remote offset for followers
- **Offset tracking:**
  - `highestOffsetInRemoteStorage`: Last offset copied to tiered storage
  - `localLogStartOffset`: Start of local log
  - `remoteLogStartOffset`: Start of tiered data

Location: `core/src/main/java/kafka/log/remote/RemoteLogManager.java`

#### Inkless/Diskless Architecture

Diskless topics use a fundamentally different storage model:

- **Control Plane (PostgreSQL)** - Stores metadata, batch coordinates, and offset information
- **Object Storage** - Stores actual record data in object files
- **Single replica model** - Replication factor must be 1 (physically replicated by object storage)
- **Key components:**
  - `FetchHandler`/`Reader`: Handle reads from object storage
  - `AppendHandler`/`Writer`: Handle writes to object storage
  - `InklessTopicMetadataTransformer`: Makes any broker appear as leader to clients
  - `ControlPlane`: Tracks `logStartOffset` and `highWatermark`

Location: `storage/inkless/`

### 2.2 Current Gaps

| Capability | Tiered Topics | Diskless Topics |
|------------|---------------|-----------------|
| Read from tiered storage | ✓ | ✗ |
| Write to tiered storage | ✓ | ✗ |
| Read from object storage | ✗ | ✓ |
| Write to object storage | ✗ | ✓ |
| Multi-replica support | ✓ (3+) | ✗ (1 only) |
| Offset tracking | Local + Remote | Diskless only |

---

## 3. Architecture Overview

### 3.1 Hybrid Topic Model

The migration introduces a **Hybrid Topic** state where a topic can read from multiple storage tiers. During migration, there's a critical window where data may exist in **three** locations:

1. **Local Log** - Recently rotated segments not yet copied to Tiered Storage
2. **Tiered Storage** - Historical segments already copied to remote storage
3. **Diskless Storage** - New data written after migration starts

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        Hybrid Topic Offset Space (During Migration)              │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────────────┐ │
│  │   Tiered Storage    │ │   Local Log         │ │      Diskless Storage       │ │
│  │   (Read-only)       │ │   (Pending Copy)    │ │      (Read + Write)         │ │
│  ├─────────────────────┤ ├─────────────────────┤ ├─────────────────────────────┤ │
│  │ remoteLogStartOffset│ │ localLogStartOffset │ │ disklessStartOffset         │ │
│  │         │           │ │         │           │ │         │                   │ │
│  │         ▼           │ │         ▼           │ │         ▼                   │ │
│  │    [Segment 1]      │ │  [Rotated Segment]  │ │    [Batch 1]                │ │
│  │    [Segment 2]      │ │  (awaiting copy     │ │    [Batch 2]                │ │
│  │    [Segment N]      │ │   to tiered)        │ │    [Batch N]                │ │
│  │         │           │ │         │           │ │         │                   │ │
│  │         ▼           │ │         ▼           │ │         ▼                   │ │
│  │ tieredHighOffset    │ │ localHighOffset     │ │ disklessHighWatermark       │ │
│  └─────────────────────┘ └─────────────────────┘ └─────────────────────────────┘ │
│                                                                                  │
│  Offset ordering: tieredHighOffset < localLogStartOffset <= localHighOffset      │
│                   localHighOffset == disklessStartOffset - 1                     │
│                                                                                  │
│  Read priority: 1. Local Log (via UnifiedLog)                                   │
│                 2. Tiered Storage (via RLM)                                     │
│                 3. Diskless Storage (via Reader)                                │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**Post-Migration Steady State:**

Once all local segments are copied to Tiered Storage, the local log is empty and reads only go to Tiered or Diskless:

```
┌─────────────────────────────────────────────────────────────────────┐
│                  Hybrid Topic Offset Space (Steady State)            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────────┐  ┌──────────────────────────────────────┐ │
│  │   Tiered Storage     │  │         Diskless Storage              │ │
│  │   (Read-only)        │  │         (Read + Write)                │ │
│  ├──────────────────────┤  ├──────────────────────────────────────┤ │
│  │ remoteLogStartOffset │  │ disklessStartOffset                  │ │
│  │         │            │  │         │                            │ │
│  │         ▼            │  │         ▼                            │ │
│  │    [Segment 1]       │  │    [Batch 1]                         │ │
│  │    [Segment 2]       │  │    [Batch 2]                         │ │
│  │    [Segment N]       │  │    [Batch N]                         │ │
│  │         │            │  │         │                            │ │
│  │         ▼            │  │         ▼                            │ │
│  │ tieredHighOffset     │  │ disklessHighWatermark                │ │
│  └──────────────────────┘  └──────────────────────────────────────┘ │
│                                                                      │
│  tieredHighOffset == disklessStartOffset - 1                        │
│  (Local log is empty)                                               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.2 Migration State Machine

**For Classic Tiered → Hybrid Migration:**

```
┌─────────────┐     enable diskless     ┌────────────────┐
│   TIERED    │ ───────────────────────►│   MIGRATING    │
│   ONLY      │                         │                │
└─────────────┘                         └───────┬────────┘
                                                │
                                                │ final segment uploaded
                                                │ offset boundary recorded
                                                ▼
                                        ┌────────────────┐
                                        │    HYBRID      │  ◄── STABLE END STATE
                                        │                │
                                        │  Tiered + New  │
                                        │  Diskless data │
                                        └────────────────┘
```

**For Pure Diskless Topics (new topics created with diskless.enable=true):**

```
┌─────────────┐    local.retention      ┌────────────────┐
│  DISKLESS   │    elapsed, segments    │    HYBRID      │  ◄── STABLE END STATE
│   ONLY      │ ───────────────────────►│                │
│             │    converted to tiered  │  Tiered + New  │
│  (new topic)│                         │  Diskless data │
└─────────────┘                         └────────────────┘
```

**Key Insight:** HYBRID is the **default stable state** for all diskless topics over time:
- Migrated topics start as HYBRID (with existing tiered data)
- New diskless topics become HYBRID when their aged batches are converted to tiered format
- The tiered portion grows as diskless batches are converted, and shrinks as retention expires old segments

---

## 4. Work Streams

### 4.1 Stream 1: Unified Offset Model for Hybrid Topics

**Objective:** Extend the Inkless control plane to track both tiered and diskless offset boundaries.

#### 4.1.1 Control Plane Schema Changes

Extend `LogEntity` to include tiered storage offset tracking:

- **New fields:** `tieredLogStartOffset`, `tieredHighOffset`, `disklessStartOffset`, `migrationState`
- `logStartOffset` becomes the earliest readable offset across all tiers
- `migrationState` tracks the topic lifecycle: `TIERED_ONLY` → `MIGRATING` → `HYBRID` → `DISKLESS_ONLY`

#### 4.1.2 ControlPlane Interface Extensions

New methods on the `ControlPlane` interface:

- `initializeHybridTopic(...)` — Set up offset boundaries when migration begins
- `getHybridTopicOffsets(...)` — Query offset boundaries for read routing
- `updateMigrationState(...)` — Transition between migration states
- `getOffsetBoundary(partition, offset)` — Determine which storage tier holds a given offset (`TIERED`, `DISKLESS`, or `OUT_OF_RANGE`)

#### 4.1.3 Database Migration

New columns on `inkless_log` table:

- `tiered_log_start_offset` (BIGINT, default -1) — Start of tiered data
- `tiered_high_offset` (BIGINT, default -1) — Last offset in tiered storage
- `diskless_start_offset` (BIGINT, default 0) — First offset in diskless storage
- `migration_state` (VARCHAR, default 'DISKLESS_ONLY') — Current migration state
- Index on `migration_state` for efficient state queries

### 4.2 Stream 2: Hybrid Read Path (Local + Tiered + Diskless)

**Objective:** Enable `FetchHandler` to route read requests to Local Log, RLM, or Diskless storage based on offset availability.

#### 4.2.1 Architecture

The read path must check storage tiers in priority order:
1. **Local Log (UnifiedLog)** - For recently rotated segments not yet in tiered storage
2. **Tiered Storage (RLM)** - For historical data already copied to remote storage
3. **Diskless Storage (Reader)** - For new data written after migration

```
                                 ┌─────────────────┐
                                 │  FetchHandler   │
                                 │   (Entry Point) │
                                 └────────┬────────┘
                                          │
                                          ▼
                               ┌──────────────────────┐
                               │ HybridFetchRouter    │
                               │                      │
                               │ Determine storage    │
                               │ tier for offset      │
                               └──────────┬───────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │                           │                           │
              ▼                           ▼                           ▼
   ┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
   │   UnifiedLog     │        │ RemoteLogManager │        │  Reader          │
   │   .read()        │        │   .read()        │        │  (Diskless)      │
   │ (Local Segments) │        │ (Tiered Data)    │        │                  │
   └────────┬─────────┘        └────────┬─────────┘        └────────┬─────────┘
            │                           │                           │
            │ NOT_FOUND                 │                           │
            │ (segment copied)          │                           │
            └──────────┬────────────────┘                           │
                       │ fallback                                   │
                       ▼                                            │
            ┌──────────────────┐                                    │
            │ Try Tiered next  │                                    │
            └──────────────────┘                                    │
                                                                    │
                                         ┌──────────────────────────┘
                                         ▼
                               ┌──────────────────┐
                               │  CombinedReader  │
                               │  (Merge Results) │
                               └──────────────────┘
```

**Key Insight:** By checking Local Log first via `UnifiedLog`, we handle the migration window naturally. The `UnifiedLog` already knows which offsets are available locally vs. in tiered storage. If the offset is not in local log, `UnifiedLog.read()` will indicate the data is in remote storage, and we can delegate to RLM.

#### 4.2.2 FetchHandler Modifications

The key insight is to leverage `UnifiedLog` for the local + tiered read path, since it already handles the local-to-remote fallback seamlessly. We only need to add the diskless routing on top.

`FetchHandler` partitions each fetch request by storage location:

- **DISKLESS_ONLY topics** → route entirely to diskless Reader
- **Offset >= disklessStartOffset** → route to diskless Reader
- **Offset < disklessStartOffset** → route to classic path (UnifiedLog handles local → tiered fallback)
- Classic and diskless reads execute **in parallel**, results are merged

During migration, `UnifiedLog.read()` handles the local-to-tiered transition transparently:
- Rotated segment still in local log → served from local
- Once copied to tiered and local deleted → served from RLM

#### 4.2.3 Why UnifiedLog is the Right Abstraction

Using `UnifiedLog.read()` provides several benefits:

1. **Existing local-to-tiered fallback**: UnifiedLog already checks if the requested offset is in local segments or needs to be fetched from RLM
2. **Offset tracking is already in place**: `localLogStartOffset` and `highestOffsetInRemoteStorage` are maintained by UnifiedLog
3. **No duplicate logic**: We don't need to re-implement the local vs. tiered decision
4. **Handles edge cases**: Segment boundary conditions, concurrent copy operations, etc. are already handled

### 4.3 Stream 3: Topic Type Switch Mechanism

**Objective:** Implement the controller logic to enable switching a tiered topic to diskless mode.

#### 4.3.1 Switch Workflow

```
┌────────────────────────────────────────────────────────────────────┐
│                    Topic Switch Workflow                           │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  1. Validate prerequisites                                         │
│     ├── Topic has remote.storage.enable=true                      │
│     ├── Topic is not compacted                                    │
│     └── All replicas are in sync                                  │
│                                                                    │
│  2. Set migration state to MIGRATING                              │
│     └── Block new segment creation                                │
│                                                                    │
│  3. Force active segment roll                                      │
│     └── Current active segment becomes eligible for tiering       │
│                                                                    │
│  4. Wait for RLMCopyTask to complete                              │
│     └── All segments up to rolled position are in tiered storage  │
│                                                                    │
│  5. Record offset boundary                                        │
│     ├── tieredHighOffset = last offset in tiered storage          │
│     └── disklessStartOffset = tieredHighOffset + 1                │
│                                                                    │
│  6. Apply diskless.enable=true config                             │
│     └── New writes go to Diskless                                 │
│                                                                    │
│  7. Set migration state to HYBRID                                 │
│     └── Topic now reads from both, writes to diskless             │
│                                                                    │
│  8. Stop RLM copy tasks (copy complete, no more local segments)   │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

#### 4.3.2 Controller Responsibilities

The controller (`ReplicationControlManager`) handles:

- **Validation** — Verify topic has `remote.storage.enable=true`, is not compacted, and all replicas are in sync
- **State transition** — Write a `ConfigRecord` setting `diskless.migration.state=MIGRATING`
- **Error handling** — Return `INVALID_REQUEST` if prerequisites are not met

#### 4.3.3 Broker-side Migration Handler

A new `DisklessMigrationHandler` on each broker executes the migration steps:

1. **Roll active segment** — Force the current active segment to become eligible for tiering
2. **Wait for RLM copy** — Poll until `highestOffsetInRemoteStorage` reaches the rolled offset
3. **Record boundary** — Call `controlPlane.initializeHybridTopic()` with `disklessStartOffset = tieredHighOffset + 1`
4. **Transition state** — Update migration state to `HYBRID`

### 4.4 Stream 4: Multi-Replica Model for Diskless Topics

> **📄 Detailed Design:** See [DISKLESS_MANAGED_RF.md](DISKLESS_MANAGED_RF.md) for the comprehensive design of rack-aware,
> KRaft-managed replicas for diskless topics, including approach comparison, cost analysis, and implementation path.

**Objective:** Enable diskless topics to use real KRaft-managed replicas while preserving write-to-any semantics.

**Decision:** User-defined RF with transformer-first availability. Controlled by `diskless.managed.rf.enable` controller config
(default: `false`). When enabled, new diskless topics accept user-defined RF (RF=-1 → `default.replication.factor`); existing topics are unaffected.

#### 4.4.1 Current vs. Proposed Model

**Current Model:**
- Replication factor = 1 (enforced)
- Physical replication via object storage (3 zones)
- `InklessTopicMetadataTransformer` makes any broker appear as leader
- All writes handled by any broker

**Proposed Model:**
- Replication factor = 3 (standard Kafka)
- Physical replication still via object storage
- Leader election uses standard Kafka mechanics
- Writes still accepted by any replica (special handling)
- RLM integration becomes possible (uses replica semantics)

#### 4.4.2 Assignment Changes

- Use standard `ReplicaPlacer` for rack-aware placement (one replica per rack/AZ)
- Unlike classic topics, all replicas can accept writes (object storage handles replication)

#### 4.4.3 Write Path Modifications

- No fundamental change to write path — writes still go directly to object storage via `AppendHandler`
- No need to forward to leader or replicate locally
- All replicas see the same data via the control plane

#### 4.4.4 Metadata Transformer Updates

> **📄 See [DISKLESS_MANAGED_RF.md](DISKLESS_MANAGED_RF.md)** for the complete routing algorithm, including AZ-priority
> logic, mode derivation, and fallback behavior.

**Summary:** The transformer derives routing mode from topic config (`remote.storage.enable` + `diskless.enable`) and
routes requests with AZ-priority. Both modes prefer assigned replicas first; `DISKLESS_ONLY` can fall back to any broker
when replicas are unavailable, while `DISKLESS_TIERED` stays on replicas only (RLM requires `UnifiedLog` state).

### 4.5 Stream 5: RLM Integration for Hybrid Topics

**Objective:** Enable RLM to work with hybrid topics for read and delete operations.

#### 4.5.1 RLM Task Modifications

For hybrid topics, RLM behavior changes in `onLeadershipChange()`:

- **Skip copy tasks** — Filter out diskless/hybrid topics from `RLMCopyTask` scheduling (no more local segments to copy)
- **Schedule expiration tasks** — Create `RLMExpirationTask` in hybrid mode for hybrid leader partitions
- **Support read operations** — Continue serving tiered reads via standard RLM read path

#### 4.5.2 Expiration Task for Hybrid Topics

The `RLMExpirationTask` operates in hybrid mode:

- Applies topic's `retentionMs` and `retentionBytes` to tiered segments
- Deletes tiered segments beyond retention via standard RLM cleanup
- Once all tiered data is deleted (`deletedUpToOffset >= tieredHighOffset`), transitions migration state to `DISKLESS_ONLY`

### 4.7 Stream 7: Diskless-to-Tiered Conversion (Batch Splitting & Segment Merging)

**Objective:** Convert aged diskless batches into tiered-storage-compatible segments, making HYBRID the permanent state for all diskless topics.

#### 4.7.1 Motivation: Control Plane Scalability

The primary driver for diskless-to-tiered conversion is **PostgreSQL metadata scalability**. Without conversion, `inkless_batch` grows unboundedly. By converting aged batches to tiered format:

- **Batch metadata is deleted** from PostgreSQL after conversion
- **Tiered segments use RLMM** (Remote Log Metadata Manager) which stores metadata in `__remote_log_metadata` topic
- **Scalability is preserved**: Only "hot" diskless data remains in PostgreSQL

#### 4.7.2 Overview: Two-Phase Distributed Pipeline

Diskless topics store data in **shared WAL files** where batches from multiple partitions coexist. To convert this data to tiered format, we use a **two-phase distributed pipeline**:

| Phase | Who runs it | What it does |
|-------|-------------|--------------|
| **Batch Splitting** | Any broker | Claims WAL files via optimistic locking, extracts partition-specific batches, writes split files to object storage |
| **Segment Merging** | Partition leader only | Collects split files for its partitions, merges into tiered-storage-compatible log segments (.log, .index, .timeindex), uploads via RSM, registers with RLMM, deletes batch metadata from PG |

The split between phases enables **horizontal scalability**: splitting needs no coordination (WAL files are time-ordered per broker), while merging is consistent with how classic tiered storage works (leader-only).

#### 4.7.3 Eligibility Criteria

A diskless batch is eligible for tiered conversion when:

1. **Age threshold**: Batch timestamp > `local.retention.ms` (same semantics as classic tiered storage)
2. **Offset committed**: Batch offset < current `highWatermark`
3. **Not already converted**: Batch hasn't been marked as split or tiered

#### 4.7.4 Offset Boundary Management

With continuous diskless-to-tiered conversion, the offset boundaries shift over time:

```
Time T1:  Diskless covers offsets [0, 1000]
          tieredHighOffset = -1
          disklessStartOffset = 0

Time T2:  Conversion runs, offsets [0, 500] converted to tiered
          tieredHighOffset = 500
          disklessStartOffset = 501

Time T3:  More data written, more conversion
          tieredHighOffset = 800
          disklessStartOffset = 801
          highWatermark = 1500
```

The read path uses these boundaries:
- `offset < disklessStartOffset` → Read from tiered storage (RLM)
- `offset >= disklessStartOffset` → Read from diskless storage (Reader)

#### 4.7.5 Distributed Execution Summary

| Job | Runs On | Work Distribution | Coordination |
|-----|---------|-------------------|--------------|
| BatchSplitterJob | All brokers | WAL files claimed via optimistic locking | `inkless_wal_file_claim` table |
| SegmentMergerJob | All brokers | Only processes partitions where broker is leader | Standard Kafka leadership |

#### 4.7.6 Configuration

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `inkless.tiering.splitter.interval.ms` | long | 60000 | Interval between batch splitting runs |
| `inkless.tiering.splitter.max.files.per.run` | int | 10 | Max WAL files to claim per splitter run |
| `inkless.tiering.splitter.claim.duration.ms` | long | 300000 | How long a WAL file claim is held |
| `inkless.tiering.merger.interval.ms` | long | 300000 | Interval between segment merge runs |
| `inkless.tiering.min.segment.bytes` | long | 1048576 | Min bytes before creating tiered segment |
| `inkless.tiering.max.segment.bytes` | long | 1073741824 | Max bytes per tiered segment |
| `inkless.tiering.max.batches.per.run` | int | 1000 | Max batches to process per splitter run |

### 4.8 Stream 8: Migration State Tracking and Observability

**Objective:** Provide visibility into migration progress, tiering progress, and topic state.

#### 4.8.1 Metrics

| Category | Metric Name | Type | Description |
|----------|-------------|------|-------------|
| **Migration** | `inkless.migration.topics.state` | Gauge | Topics in each migration state |
| | `inkless.migration.topic.progress` | Gauge | Per-topic migration progress (0-100%) |
| **Read Path** | `inkless.hybrid.tiered.reads` | Counter | Reads served from tiered storage (RLM) |
| | `inkless.hybrid.local.reads` | Counter | Reads served from local log (migration window) |
| | `inkless.hybrid.diskless.reads` | Counter | Reads served from diskless storage |
| **Tiering Pipeline** | `inkless.tiering.batches.split` | Counter | Batches split from WAL files |
| | `inkless.tiering.segments.created` | Counter | Segments created from merged batches |
| | `inkless.tiering.batches.pending.split` | Gauge | Pending batches awaiting split |
| | `inkless.tiering.batches.pending.merge` | Gauge | Split batches awaiting merge |
| | `inkless.tiering.eligible.bytes` | Gauge | Bytes of diskless data eligible for tiering |
| | `inkless.tiering.split.latency` | Histogram | Time taken for batch splitting |
| | `inkless.tiering.merge.latency` | Histogram | Time taken for segment merging |
| **Offset Tracking** | `inkless.tiered.high.offset` | Gauge | Tiered high offset per partition |
| | `inkless.diskless.start.offset` | Gauge | Diskless start offset per partition |
| | `inkless.tiering.lag.bytes` | Gauge | Gap between diskless and tiered (unconverted data) |

#### 4.8.2 Admin API Extensions

| Category | Operation | Description |
|----------|-----------|-------------|
| **Migration** | `migrateToDiskless(topic)` | Start migration for a tiered topic |
| | `getMigrationStatus(topic)` | Get current migration state and progress |
| | `listTopicsByMigrationState()` | List all topics grouped by migration state |
| | `forceMigrationComplete(topic)` | Force completion for stuck migrations |
| **Tiering** | `getTieringStatus(topic)` | Get tiering progress (offsets, pending counts, eligible bytes) |
| | `triggerTiering(topic)` | Trigger immediate tiering (bypass scheduler) |
| | `pauseTiering(topic)` / `resumeTiering(topic)` | Pause/resume tiering for a topic |

---

## 5. Data Flow Diagrams

### 5.1 Read Path for Hybrid Topic

**Scenario 1: Offset in Local Log (during migration window)**

```
┌──────────┐     Fetch(offset=480)     ┌─────────────────┐
│  Client  │ ─────────────────────────►│   FetchHandler  │
└──────────┘                           └────────┬────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │ Check offset boundary │
                                    │ disklessStart = 500   │
                                    │ offset 480 < 500      │
                                    │ → Route to Classic    │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │    UnifiedLog.read()  │
                                    │                       │
                                    │ localLogStart = 450   │
                                    │ offset 480 >= 450     │
                                    │ → Read from local log │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  Local Log Segment    │
                                    │  (rotated, pending    │
                                    │   copy to tiered)     │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  Return RecordBatch   │
                                    └───────────────────────┘
```

**Scenario 2: Offset in Tiered Storage (historical data)**

```
┌──────────┐     Fetch(offset=100)     ┌─────────────────┐
│  Client  │ ─────────────────────────►│   FetchHandler  │
└──────────┘                           └────────┬────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │ Check offset boundary │
                                    │ disklessStart = 500   │
                                    │ offset 100 < 500      │
                                    │ → Route to Classic    │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │    UnifiedLog.read()  │
                                    │                       │
                                    │ localLogStart = 450   │
                                    │ offset 100 < 450      │
                                    │ → Delegate to RLM     │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  RemoteLogManager     │
                                    │  .read()              │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  RemoteStorageManager │
                                    │  (S3/GCS/Azure)       │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  Return RecordBatch   │
                                    └───────────────────────┘
```

**Scenario 3: Offset in Diskless Storage (new data)**

```
┌──────────┐     Fetch(offset=550)     ┌─────────────────┐
│  Client  │ ─────────────────────────►│   FetchHandler  │
└──────────┘                           └────────┬────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │ Check offset boundary │
                                    │ disklessStart = 500   │
                                    │ offset 550 >= 500     │
                                    │ → Route to Diskless   │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │   Reader (Diskless)   │
                                    │   findBatches()       │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │   Object Storage      │
                                    │   (Inkless format)    │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  Return RecordBatch   │
                                    └───────────────────────┘
```

---

## 6. Configuration

### 6.1 Topic Configurations

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `diskless.enable` | boolean | false | Enable diskless mode for topic |
| `diskless.migration.state` | enum | N/A | Current migration state (read-only) |

### 6.2 Broker Configurations

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `inkless.hybrid.read.enabled` | boolean | true | Enable reading tiered data from hybrid topics |
| `inkless.migration.segment.wait.ms` | long | 30000 | Max time to wait for final segment copy |
| `inkless.migration.parallel.topics` | int | 5 | Max concurrent topic migrations |

---

## 7. Compatibility and Rollback

### 7.1 Backward Compatibility

- Existing diskless topics continue to work unchanged
- Existing tiered topics continue to work unchanged
- Migration is opt-in per topic

### 7.2 Rollback Strategy

**Before migration completes (MIGRATING state):**
- Abort migration
- Continue as tiered topic
- No data loss

**After migration completes (HYBRID state):**
- Cannot rollback to TIERED_ONLY (writes are already in diskless)
- HYBRID is the stable operating state - no need to rollback
- Tiered portion grows as diskless batches are converted
- Oldest tiered segments expire per retention policy

**For new diskless topics:**
- Start as DISKLESS_ONLY
- Automatically become HYBRID when tiering kicks in
- No rollback needed - this is the intended progression

---

## 8. Testing Strategy

### 8.1 Unit Tests

- Offset boundary detection in FetchHandler
- Migration state machine transitions
- Control plane schema changes
- Batch eligibility checking for tiering
- Split file grouping logic

### 8.2 Integration Tests

- End-to-end migration flow (tiered → hybrid)
- End-to-end tiering flow (diskless → hybrid)
- Read path routing (local vs tiered vs diskless)
- Retention enforcement on hybrid topics
- Tiering pipeline (WAL split → segment merge → RLM upload)

### 8.3 Performance Tests

- Read latency for hybrid topics (all three tiers)
- Migration impact on produce latency
- Memory usage with hybrid offset tracking
- Tiering pipeline throughput (batches/sec, segments/sec)
- Object storage I/O during batch splitting

---

## 9. Implementation Phases

For detailed implementation timeline, task breakdown, and team assignments, see the separate **[Project Plan](./PROJECT_PLAN.md)**.

### High-Level Phases

| Phase | Description | Dependencies | Status |
|-------|-------------|--------------|--------|
| **P1: Foundation** | Schema, state machines, test infrastructure | — | |
| **P2: Read Path** | Three-tier read routing via UnifiedLog | P1 | |
| **P2b: Batch Splitting** | WAL file splitting (parallel with P2/P3) | P1 | |
| **P3: Switch Mechanism** | Tiered → Hybrid migration | P2 | |
| **P4: Multi-Replica** | Managed RF for diskless topics ([design](DISKLESS_MANAGED_RF.md)) | P3 | In progress |
| **P5: RLM Integration** | Hybrid topic support in RLM | P3 | |
| **P6: Segment Merging** | Split files → Tiered segments | P2b | |
| **P7: E2E Integration** | Full pipeline validation | P5, P6 | |
| **P8: Observability** | Metrics, admin APIs, docs | P7 | |

### Critical Path

The minimum path to deliver value:

```
P1 (Foundation) → P2 (Read Path) → P3 (Switch) → P5 (RLM) → P7 (E2E)
```

### Parallelizable Work

- **P4** (Multi-Replica / Managed RF) — In progress, can land independently
- **P2b + P6** (Tiering pipeline) — Can run in parallel with the migration path
- **P8** (Full observability) can be reduced for initial release

---

## 10. Open Questions

### Migration Questions

1. **Transactional record handling during migration?**
   - Option A: Block transactions during switch
   - Option B: Drain all in-flight transactions before switch
   - Option C: Continue transactions in hybrid mode with coordinator awareness
   - **Recommendation:** Option B - cleanest semantics

2. **Consumer group offset handling?**
   - Consumer offsets remain valid (logical offset space preserved)
   - `EARLIEST_LOCAL_TIMESTAMP` semantics need adjustment for hybrid topics
   - **Recommendation:** Document behavior, adjust offset reset logic

3. **RLM shared instance or separate?**
   - Option A: Share existing RLM instance
   - Option B: Create separate RLM for hybrid topics
   - **Recommendation:** Share existing instance, minimize resource overhead

### Tiering Pipeline Questions (to be resolved during implementation)

4. **Batch splitting granularity?** — One split file per partition per WAL file (initially), or accumulate across WAL files?

5. **Segment size for converted batches?** — Use topic's `segment.bytes` for consistency with RLM?

6. **Index building for converted segments?** — Full indexes (offset, time, transaction) for RLM compatibility?

7. **Handling gaps in converted offsets?** — Track deletions and exclude from conversion eligibility?

8. **Conversion failure handling?** — Retry with backoff + alerting after threshold?

9. **WAL file cleanup?** — Eligible for deletion when all its batches are either tiered or expired?

### Data Lifecycle Questions

10. **Retention policy for converted segments?** — Use topic's retention config, RLM expiration handles cleanup?

---

## 11. Future Work: Diskless → Tiered Migration (Reverse Direction)

While the initial scope focuses on **Tiered → Diskless** migration and the tiering pipeline, a future phase should enable the **reverse direction**: migrating diskless topics back to classic tiered storage.

### Use Cases

| Use Case | Description |
|----------|-------------|
| **Cost optimization** | Diskless may be more expensive for some workloads; allow switching back |
| **Feature requirements** | Classic topics support features diskless may not (e.g., compaction) |
| **Operational flexibility** | Avoid lock-in to diskless mode |
| **Disaster recovery** | Ability to fall back to proven classic storage |

### High-Level Approach

The reverse migration leverages the **same scaled read path** as forward migration, enabling zero-downtime transition:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│              DISKLESS → TIERED MIGRATION (ZERO DOWNTIME)                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   PHASE 1: Accelerated Tiering (reads continue from all tiers)                 │
│   ┌─────────────────────────────────────────────────────────────────────────┐  │
│   │ Tiered │◄─── converting ───│ Diskless (still active)                    │  │
│   │  data  │   tiering pipeline │  WAL writes continue                      │  │
│   └─────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
│   Read path: Local → Tiered (growing) → Diskless (shrinking)                   │
│   Consumers experience NO downtime                                             │
│                                                                                 │
│   PHASE 2: Switch Write Path (once diskless fully tiered)                      │
│   ┌─────────────────────────────────────────────────────────────────────────┐  │
│   │ Tiered (all historical) │ Local (new writes) │                          │  │
│   │  including converted    │  classic mode      │                          │  │
│   └─────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
│   Steps:                                                                        │
│   1. Force-tier diskless batches (accelerate tiering pipeline)                 │
│   2. Read path continues serving from diskless until conversion complete       │
│   3. Once fully tiered → switch write path to local log                        │
│   4. Update topic config: diskless.enable=false                                │
│   5. Resume RLM copy tasks for new local segments                              │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Key Insight:** The same three-tier read path (Local → Tiered → Diskless) that enables forward migration also enables zero-downtime reverse migration. The read path simply routes based on offset boundaries, which shift as the tiering pipeline converts data.

### Key Considerations

1. **Accelerate tiering** — Force the tiering pipeline to convert all diskless batches before switching write path
2. **Offset continuity** — Offset boundaries shift as data is converted; read path handles this transparently
3. **RF adjustment** — If diskless was RF=1 (legacy), may need partition reassignment for RF=3
4. **Config validation** — Ensure `remote.storage.enable=true` is set for the topic
5. **RLM task scheduling** — Resume copy tasks after migration completes

### Differences from Forward Migration

| Aspect | Tiered → Diskless | Diskless → Tiered |
|--------|-------------------|-------------------|
| Data conversion | Not needed (tiered stays tiered) | Required (diskless → tiered) |
| Complexity | Lower | Higher (tiering must complete first) |
| Downtime risk | Zero (scaled read path) | Zero (same scaled read path) |
| Use case frequency | High (new feature adoption) | Low (fallback/rollback) |

### Not In Scope for Initial Release

This reverse migration is explicitly **not in scope** for the 8-week initial release because:
- Primary use case is adoption (tiered → diskless), not rollback
- Adds significant complexity
- Can be implemented later using the same tiering pipeline infrastructure

Once the tiering pipeline is complete, enabling reverse migration becomes straightforward—it's essentially "forced tiering" followed by a config change.

---

## 12. Architecture Summary

### Final State: Hybrid Topic Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         HYBRID TOPIC DATA LIFECYCLE                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   WRITE PATH                    AGING                    TIERED STORAGE         │
│   (Hot Data)                    (Conversion)             (Cold Data)            │
│                                                                                 │
│   ┌─────────────┐              ┌─────────────┐          ┌─────────────┐        │
│   │   Producer  │              │  Tiering    │          │   Tiered    │        │
│   │             │              │  Pipeline   │          │   Segments  │        │
│   └──────┬──────┘              └──────┬──────┘          └──────┬──────┘        │
│          │                            │                        │               │
│          ▼                            │                        │               │
│   ┌─────────────┐                     │                        │               │
│   │   Diskless  │──── age > ─────────►│                        │               │
│   │   Storage   │  local.retention    │                        │               │
│   │  (WAL files)│                     │                        │               │
│   └─────────────┘                     │                        │               │
│          │                            │                        │               │
│          │                            ▼                        │               │
│          │                     ┌─────────────┐                 │               │
│          │                     │   Split &   │                 │               │
│          │                     │   Merge     │────────────────►│               │
│          │                     │             │                 │               │
│          │                     └─────────────┘                 │               │
│          │                                                     │               │
│          │                                                     ▼               │
│          │                                              ┌─────────────┐        │
│          │                                              │   Retention │        │
│          │                                              │   Cleanup   │        │
│          │                                              └──────┬──────┘        │
│          │                                                     │               │
│          │                                                     ▼               │
│          │                                              ┌─────────────┐        │
│          │                                              │   Deleted   │        │
│          │                                              │   (expired) │        │
│          │                                              └─────────────┘        │
│          │                                                                     │
│   ───────┴─────────────────────────────────────────────────────────────────    │
│                                                                                 │
│   READ PATH:   Consumer requests offset                                        │
│                                                                                 │
│   offset >= disklessStartOffset  →  Read from Diskless (Reader)                │
│   offset < disklessStartOffset   →  Read from Tiered (UnifiedLog → RLM)        │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **HYBRID is the stable end state** - Not a transition, but the permanent operating mode
2. **Continuous tiering** - Diskless batches are continuously converted to tiered format
3. **Unified read path** - Single entry point routes to appropriate storage tier
4. **RLM compatibility** - Converted segments are indistinguishable from classic tiered segments
5. **Seamless migration** - Topics can migrate from tiered to diskless without data loss

---

## 13. References

- [KIP-405: Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)
- [Diskless-Managed Replication Factor](./DISKLESS_MANAGED_RF.md) — Detailed design for rack-aware, KRaft-managed replicas
- [Project Plan](./PROJECT_PLAN.md) — Timeline, task breakdown, risk assessment
- [Delos Evaluation](./DELOS_EVALUATION.md) — Sealing and MetaStore evaluation

