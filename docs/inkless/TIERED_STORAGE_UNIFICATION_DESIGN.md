# Tiered Storage Unification for Zero-copy Migration

## Design Document

**Authors:** Engineering Team  
**Date:** December 2025  
**Status:** Draft

---

## 1. Executive Summary

This document outlines the design for enabling seamless migration from Apache Kafka's Tiered Storage to Inkless (Diskless) topics. The goal is to allow topics with existing tiered data to transition to the Diskless storage model while preserving read access to historical tiered data and enabling new writes to flow through the Diskless pipeline.

### Goals

1. Allow migration of Tiered Kafka topics into Diskless topics
2. Enable Diskless topics to read data from both Tiered Storage and Diskless storage
3. Track offsets across local/diskless and remote/tiered boundaries
4. Implement retention policy enforcement for migrated topics

### Non-Goals

1. Migration from Diskless to Tiered Kafka (reverse direction) for the initial phase
2. Support for running both storage modes simultaneously for **new** data (writes always go to one mode)

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

```java
// storage/inkless/src/main/java/io/aiven/inkless/control_plane/postgres/LogEntity.java
record LogEntity(
    Uuid topicId,
    int partition,
    String topicName,
    long logStartOffset,           // Earliest readable offset (min of tiered/diskless)
    long highWatermark,            // Current HWM in diskless
    // NEW FIELDS for hybrid support:
    long tieredLogStartOffset,     // Start of tiered data (-1 if none)
    long tieredHighOffset,         // Last offset in tiered storage (-1 if none)
    long disklessStartOffset,      // First offset in diskless storage
    MigrationState migrationState  // TIERED_ONLY, MIGRATING, HYBRID, DISKLESS_ONLY
) { ... }
```

#### 4.1.2 ControlPlane Interface Extensions

```java
// storage/inkless/src/main/java/io/aiven/inkless/control_plane/ControlPlane.java
public interface ControlPlane {
    // Existing methods...
    
    // NEW: Migration support
    void initializeHybridTopic(InitializeHybridTopicRequest request);
    
    HybridTopicOffsets getHybridTopicOffsets(TopicIdPartition partition);
    
    void updateMigrationState(TopicIdPartition partition, MigrationState state);
    
    // NEW: Query offset boundaries
    OffsetBoundary getOffsetBoundary(TopicIdPartition partition, long offset);
}

enum MigrationState {
    TIERED_ONLY,      // Topic uses only tiered storage
    MIGRATING,        // Switch in progress  
    HYBRID,           // Both tiered and diskless data exist
    DISKLESS_ONLY     // All tiered data expired, diskless only
}

enum OffsetBoundary {
    TIERED,           // Offset is in tiered storage
    DISKLESS,         // Offset is in diskless storage
    OUT_OF_RANGE      // Offset doesn't exist
}
```

#### 4.1.3 Database Migration

```sql
-- New columns for hybrid topic support
ALTER TABLE inkless_log ADD COLUMN tiered_log_start_offset BIGINT DEFAULT -1;
ALTER TABLE inkless_log ADD COLUMN tiered_high_offset BIGINT DEFAULT -1;
ALTER TABLE inkless_log ADD COLUMN diskless_start_offset BIGINT DEFAULT 0;
ALTER TABLE inkless_log ADD COLUMN migration_state VARCHAR(20) DEFAULT 'DISKLESS_ONLY';

-- Index for efficient migration state queries
CREATE INDEX idx_inkless_log_migration_state ON inkless_log(migration_state);
```

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

```java
// storage/inkless/src/main/java/io/aiven/inkless/consume/FetchHandler.java
public class FetchHandler implements Closeable {
    private final Reader disklessReader;
    private final ReplicaManager replicaManager;  // NEW: Access to UnifiedLog
    private final ControlPlane controlPlane;
    
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> handle(
        final FetchParams params,
        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos
    ) {
        // Partition requests by storage location
        Map<TopicIdPartition, FetchRequest.PartitionData> classicRequests = new HashMap<>();  // Local + Tiered
        Map<TopicIdPartition, FetchRequest.PartitionData> disklessRequests = new HashMap<>();
        
        for (var entry : fetchInfos.entrySet()) {
            TopicIdPartition tip = entry.getKey();
            FetchRequest.PartitionData partitionData = entry.getValue();
            
            HybridTopicOffsets offsets = controlPlane.getHybridTopicOffsets(tip);
            if (offsets.migrationState() == MigrationState.DISKLESS_ONLY) {
                // Pure diskless topic - read from Diskless storage only
                disklessRequests.put(tip, partitionData);
            } else if (partitionData.fetchOffset >= offsets.disklessStartOffset()) {
                // Offset is in diskless range
                disklessRequests.put(tip, partitionData);
            } else {
                // Offset is in local or tiered range - use UnifiedLog
                // UnifiedLog.read() handles local → tiered fallback automatically
                classicRequests.put(tip, partitionData);
            }
        }
        
        // Execute reads in parallel
        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> classicFuture = 
            fetchFromClassicPath(classicRequests, params);
        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> disklessFuture = 
            disklessReader.fetch(params, disklessRequests);
        
        return classicFuture.thenCombine(disklessFuture, this::mergeResults);
    }
    
    /**
     * Read from classic path (Local Log + Tiered Storage).
     * 
     * UnifiedLog.read() already handles the fallback:
     * 1. Check local log segments
     * 2. If offset < localLogStartOffset, delegate to RLM for tiered data
     * 
     * This means during migration:
     * - Rotated segment still in local log → served from local
     * - Once copied to tiered and local deleted → served from RLM
     */
    private CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetchFromClassicPath(
        Map<TopicIdPartition, FetchRequest.PartitionData> requests,
        FetchParams params
    ) {
        return CompletableFuture.supplyAsync(() -> {
            Map<TopicIdPartition, FetchPartitionData> results = new HashMap<>();
            
            for (var entry : requests.entrySet()) {
                TopicIdPartition tip = entry.getKey();
                FetchRequest.PartitionData partitionData = entry.getValue();
                
                // Get the partition's UnifiedLog
                Partition partition = replicaManager.getPartitionOrException(tip.topicPartition());
                UnifiedLog log = partition.log().get();
                
                // UnifiedLog.read() handles local vs tiered automatically
                FetchDataInfo fetchInfo = log.read(
                    partitionData.fetchOffset,
                    partitionData.maxBytes,
                    params.isolation(),
                    /* minOneMessage= */ true
                );
                
                results.put(tip, toFetchPartitionData(fetchInfo));
            }
            
            return results;
        });
    }
}
```

#### 4.2.3 Why UnifiedLog is the Right Abstraction

Using `UnifiedLog.read()` provides several benefits:

1. **Existing local-to-tiered fallback**: UnifiedLog already checks if the requested offset is in local segments or needs to be fetched from RLM
2. **Offset tracking is already in place**: `localLogStartOffset` and `highestOffsetInRemoteStorage` are maintained by UnifiedLog
3. **No duplicate logic**: We don't need to re-implement the local vs. tiered decision
4. **Handles edge cases**: Segment boundary conditions, concurrent copy operations, etc. are already handled

```java
// core/src/main/scala/kafka/log/UnifiedLog.scala
// Existing read method already handles this:
def read(startOffset: Long, ...): FetchDataInfo = {
  // If startOffset < localLogStartOffset, data is in tiered storage
  if (startOffset < localLogStartOffset) {
    // Delegate to RemoteLogManager
    return readFromRemoteStorage(startOffset, ...)
  }
  // Otherwise, read from local segments
  return readFromLocalLog(startOffset, ...)
}
```

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

#### 4.3.2 Controller Changes

```java
// metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java

/**
 * Initiate migration of a tiered topic to diskless mode.
 */
public ControllerResult<MigrateToDisklessResult> migrateToDiskless(
    MigrateToDisklessRequest request
) {
    String topicName = request.topicName();
    Uuid topicId = topicsByName.get(topicName);
    
    // Validate topic exists and is tiered
    TopicControlInfo topic = topics.get(topicId);
    if (!isRemoteStorageEnabled(topic)) {
        return ControllerResult.of(
            Collections.emptyList(),
            new MigrateToDisklessResult(Errors.INVALID_REQUEST, 
                "Topic must have remote.storage.enable=true")
        );
    }
    
    List<ApiMessageAndVersion> records = new ArrayList<>();
    
    // Update topic config to trigger migration
    records.add(new ApiMessageAndVersion(
        new ConfigRecord()
            .setResourceType(ResourceType.TOPIC.code())
            .setResourceName(topicName)
            .setName(DISKLESS_MIGRATION_STATE_CONFIG)
            .setValue(MigrationState.MIGRATING.name()),
        (short) 0
    ));
    
    return ControllerResult.of(records, 
        new MigrateToDisklessResult(Errors.NONE, null));
}
```

#### 4.3.3 Broker-side Migration Handler

```java
// NEW: core/src/main/java/kafka/server/DisklessMigrationHandler.java
public class DisklessMigrationHandler {
    private final ReplicaManager replicaManager;
    private final RemoteLogManager remoteLogManager;
    private final ControlPlane controlPlane;
    
    public CompletableFuture<Void> executeMigration(TopicPartition tp) {
        return CompletableFuture.runAsync(() -> {
            // 1. Roll the active segment
            Partition partition = replicaManager.getPartitionOrException(tp);
            UnifiedLog log = partition.log().get();
            log.roll();
            
            // 2. Wait for RLM to copy final segment
            waitForCopyCompletion(tp, log.logEndOffset());
            
            // 3. Record boundary in control plane
            long tieredHighOffset = remoteLogManager.highestOffsetInRemoteStorage(tp);
            controlPlane.initializeHybridTopic(new InitializeHybridTopicRequest(
                partition.topicId(),
                tp.partition(),
                tp.topic(),
                tieredHighOffset + 1  // disklessStartOffset
            ));
            
            // 4. Update migration state
            controlPlane.updateMigrationState(
                new TopicIdPartition(partition.topicId(), tp),
                MigrationState.HYBRID
            );
        });
    }
    
    private void waitForCopyCompletion(TopicPartition tp, long targetOffset) {
        // Poll until RLM has copied up to targetOffset
        while (remoteLogManager.highestOffsetInRemoteStorage(tp) < targetOffset - 1) {
            Thread.sleep(100);
        }
    }
}
```

### 4.4 Stream 4: Multi-Replica Model for Diskless Topics

**Objective:** Enable diskless topics to use 3 actual Kafka replicas while preserving write-to-any semantics.

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

```java
// metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java

/**
 * Create a topic assignment for a diskless topic with 3 replicas.
 * Unlike classic topics, all replicas can accept writes.
 */
private TopicAssignment createDisklessAssignment(int numPartitions, short replicationFactor) {
    // Use standard replica placer for rack-aware placement
    TopicAssignment assignment = clusterControl.replicaPlacer().place(
        new PlacementSpec(0, numPartitions, replicationFactor),
        clusterDescriber
    );
    
    return assignment;
}
```

#### 4.4.3 Write Path Modifications

With 3 replicas, we need to handle writes differently:

```java
// storage/inkless/src/main/java/io/aiven/inkless/produce/AppendHandler.java
public class AppendHandler {
    
    /**
     * For diskless topics with multiple replicas, any replica can accept writes.
     * The write goes directly to object storage, not to local log.
     * 
     * Unlike classic Kafka:
     * - No need to forward to leader
     * - No replication required (object storage is replicated)
     * - All replicas see same data via control plane
     */
    public CompletableFuture<ProduceResponse> handle(ProduceRequest request) {
        // Current logic remains - writes go to object storage
        // No change needed for write path itself
    }
}
```

#### 4.4.4 Metadata Transformer Updates

```java
// storage/inkless/src/main/java/io/aiven/inkless/metadata/InklessTopicMetadataTransformer.java
public class InklessTopicMetadataTransformer {
    
    public void transformClusterMetadata(
        final String clientId,
        final Iterable<MetadataResponseData.MetadataResponseTopic> topicMetadata
    ) {
        for (final var topic : topicMetadata) {
            if (!metadataView.isDisklessTopic(topic.name())) {
                continue;
            }
            
            for (final var partition : topic.partitions()) {
                if (metadataView.isHybridTopic(topic.name())) {
                    // Hybrid topic: use actual replica assignment for RLM compatibility
                    // Leader selection based on partition metadata
                    // No transformation needed - use real replicas
                } else {
                    // Pure diskless: current behavior - route to any broker
                    int leader = selectLeaderForInklessPartitions(clientId, topic.topicId(), 
                        partition.partitionIndex());
                    partition.setLeaderId(leader);
                    partition.setReplicaNodes(List.of(leader));
                    partition.setIsrNodes(List.of(leader));
                }
            }
        }
    }
}
```

### 4.5 Stream 5: RLM Integration for Hybrid Topics

**Objective:** Enable RLM to work with hybrid topics for read and delete operations.

#### 4.5.1 RLM Task Modifications

For hybrid topics, RLM needs to:
1. **Skip copy tasks** - No more local segments to copy
2. **Continue expiration tasks** - Clean up tiered data based on retention
3. **Support read operations** - Serve reads for tiered offsets

```java
// core/src/main/java/kafka/log/remote/RemoteLogManager.java

public void onLeadershipChange(
    Set<Partition> partitionsBecomeLeader,
    Set<Partition> partitionsBecomeFollower,
    Map<String, Uuid> topicIds
) {
    // Filter out hybrid/diskless topics from copy task scheduling
    Map<TopicIdPartition, Boolean> leaderPartitions = filterPartitions(partitionsBecomeLeader)
        .filter(p -> !isDisklessOrHybrid(p))  // NEW: Skip diskless/hybrid
        .collect(Collectors.toMap(...));
    
    // But still schedule expiration tasks for hybrid topics
    Map<TopicIdPartition, Boolean> hybridLeaderPartitions = filterPartitions(partitionsBecomeLeader)
        .filter(this::isHybridTopic)  // NEW: Only hybrid topics
        .collect(Collectors.toMap(...));
    
    // Schedule expiration tasks for hybrid topics
    hybridLeaderPartitions.forEach(this::scheduleHybridExpirationTask);
}

// NEW: Expiration task for hybrid topics
void scheduleHybridExpirationTask(TopicIdPartition tpId, boolean copyDisabled) {
    // Create expiration-only task that cleans tiered data
    // Does not expect any new segments to copy
    RLMExpirationTask task = new RLMExpirationTask(tpId, /* hybridMode= */ true);
    leaderExpirationRLMTasks.put(tpId, new RLMTaskState(task));
}
```

#### 4.5.2 Expiration Task for Hybrid Topics

```java
// core/src/main/java/kafka/log/remote/RemoteLogManager.java

class RLMExpirationTask extends RLMTask {
    private final boolean hybridMode;
    
    @Override
    protected void execute(UnifiedLog log) throws Exception {
        if (hybridMode) {
            // For hybrid topics, check if tiered data should be deleted
            executeHybridExpiration(log);
        } else {
            // Standard expiration logic
            executeStandardExpiration(log);
        }
    }
    
    private void executeHybridExpiration(UnifiedLog log) {
        // Get retention policy from topic config
        long retentionMs = log.config().retentionMs();
        long retentionBytes = log.config().retentionSize();
        
        // Delete tiered segments beyond retention
        // Once all tiered data is deleted, update migration state to DISKLESS_ONLY
        long deletedUpToOffset = cleanupTieredSegments(log, retentionMs, retentionBytes);
        
        if (deletedUpToOffset >= getTieredHighOffset()) {
            // All tiered data has been deleted
            notifyMigrationComplete(topicIdPartition);
        }
    }
}
```

### 4.7 Stream 7: Diskless-to-Tiered Conversion (Batch Splitting & Segment Merging)

**Objective:** Convert aged diskless batches into tiered-storage-compatible segments, making HYBRID the permanent state for all diskless topics.

#### 4.7.1 Motivation: Control Plane Scalability

The primary driver for diskless-to-tiered conversion is **PostgreSQL metadata scalability**. The Control Plane stores batch metadata in PostgreSQL, and without conversion:

- **Unbounded table growth**: Every batch written to diskless storage adds rows to `inkless_batch`
- **Query performance degradation**: Large tables slow down `findBatches()` queries during reads
- **Storage costs**: Metadata storage grows linearly with message count, not just data size

By converting aged batches to tiered storage format:
- **Batch metadata is deleted** from PostgreSQL after conversion
- **Tiered segments use RLMM** (Remote Log Metadata Manager) which stores metadata in `__remote_log_metadata` topic
- **Scalability is preserved**: Only "hot" diskless data remains in PostgreSQL

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      CONTROL PLANE SCALABILITY MODEL                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   WITHOUT TIERING:                         WITH TIERING:                        │
│                                                                                 │
│   PostgreSQL                               PostgreSQL                           │
│   ┌─────────────────────┐                  ┌─────────────────────┐              │
│   │ inkless_batch       │                  │ inkless_batch       │              │
│   │ ┌─────────────────┐ │                  │ ┌─────────────────┐ │              │
│   │ │ Batch 1 (old)   │ │                  │ │ Batch N (hot)   │ │              │
│   │ │ Batch 2         │ │                  │ │ Batch N+1       │ │  ← bounded   │
│   │ │ ...             │ │ ← unbounded      │ │ Batch N+2       │ │    by        │
│   │ │ Batch N-1       │ │   growth!        │ └─────────────────┘ │    local.    │
│   │ │ Batch N         │ │                  │                     │    retention │
│   │ │ Batch N+1       │ │                  └─────────────────────┘              │
│   │ └─────────────────┘ │                                                       │
│   └─────────────────────┘                  RLMM (__remote_log_metadata)         │
│                                            ┌─────────────────────┐              │
│                                            │ Segment 1 metadata  │              │
│                                            │ Segment 2 metadata  │  ← Kafka     │
│                                            │ ...                 │    native    │
│                                            └─────────────────────┘    storage   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### 4.7.2 Overview: Two-Phase Distributed Pipeline

Diskless topics store data in **shared WAL files** where batches from multiple partitions coexist. To convert this data to tiered format, we use a **two-phase distributed pipeline**:

1. **Batch Splitting (Any Broker)**: Extract partition-specific batches from shared WAL files
2. **Segment Merging (Partition Leader)**: Combine extracted batches into tiered-storage-compatible log segments

**Why separate jobs?**

The split between phases is intentional and enables **horizontal scalability**:

| Phase | Executed By | Reasoning |
|-------|-------------|-----------|
| **Splitting** | Any broker | WAL files are uploaded in time order per broker. Any broker can read a time range of WAL files and split them independently. No coordination needed. |
| **Merging** | Partition leader | Only the partition leader should write to RLM (consistent with classic tiered storage). With RF=3 diskless topics, the leader coordinates segment creation. |

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Distributed Tiering Pipeline                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   BROKER 0                    BROKER 1                    BROKER 2              │
│   ┌────────────────┐          ┌────────────────┐          ┌────────────────┐    │
│   │ BatchSplitter  │          │ BatchSplitter  │          │ BatchSplitter  │    │
│   │                │          │                │          │                │    │
│   │ Processes WAL  │          │ Processes WAL  │          │ Processes WAL  │    │
│   │ files from     │          │ files from     │          │ files from     │    │
│   │ time range     │          │ time range     │          │ time range     │    │
│   │ T0-T1          │          │ T1-T2          │          │ T2-T3          │    │
│   └───────┬────────┘          └───────┬────────┘          └───────┬────────┘    │
│           │                           │                           │             │
│           │  Split files for          │  Split files for          │             │
│           │  all partitions           │  all partitions           │             │
│           ▼                           ▼                           ▼             │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                     OBJECT STORAGE (Split Files)                        │   │
│   │   T0-P0-split-001, T0-P1-split-001, T1-P0-split-001, ...                │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│           │                           │                           │             │
│           │                           │                           │             │
│           ▼                           ▼                           ▼             │
│   ┌────────────────┐          ┌────────────────┐          ┌────────────────┐    │
│   │ SegmentMerger  │          │ SegmentMerger  │          │ SegmentMerger  │    │
│   │ (if leader of  │          │ (if leader of  │          │ (if leader of  │    │
│   │  partition)    │          │  partition)    │          │  partition)    │    │
│   │                │          │                │          │                │    │
│   │ Leader of P0   │          │ Leader of P1   │          │ Leader of P2   │    │
│   │ → merges P0    │          │ → merges P1    │          │ → merges P2    │    │
│   │   split files  │          │   split files  │          │   split files  │    │
│   └───────┬────────┘          └───────┬────────┘          └───────┬────────┘    │
│           │                           │                           │             │
│           ▼                           ▼                           ▼             │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                     TIERED STORAGE (Per-Partition Segments)              │   │
│   │   P0/segment-001.log      P1/segment-001.log      P2/segment-001.log    │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### 4.7.3 WAL File Time Ordering Property

The key insight enabling distributed splitting is that **WAL files are uploaded in time order per broker**:

```
Broker 0 WAL files:    wal-b0-t1.obj → wal-b0-t2.obj → wal-b0-t3.obj → ...
Broker 1 WAL files:    wal-b1-t1.obj → wal-b1-t2.obj → wal-b1-t3.obj → ...
Broker 2 WAL files:    wal-b2-t1.obj → wal-b2-t2.obj → wal-b2-t3.obj → ...
                       ─────────────────────────────────────────────────►
                                          Time
```

This means:
- **No coordination needed for splitting**: Each broker can independently claim a time range of WAL files to process
- **Parallelism is natural**: Brokers process different WAL files concurrently
- **Idempotency**: If a broker fails mid-split, another can retry the same WAL file

#### 4.7.4 Eligibility Criteria

A diskless batch is eligible for tiered conversion when:

1. **Age threshold**: Batch timestamp > `local.retention.ms` (same semantics as classic tiered storage)
2. **Offset committed**: Batch offset < current `highWatermark`
3. **Not already converted**: Batch hasn't been marked as split or tiered

```java
// storage/inkless/src/main/java/io/aiven/inkless/tiering/ConversionEligibilityChecker.java
public class ConversionEligibilityChecker {
    
    public boolean isEligibleForConversion(BatchMetadata batch, TopicConfig config) {
        long age = System.currentTimeMillis() - batch.timestamp();
        long localRetentionMs = config.localRetentionMs();
        
        return age > localRetentionMs 
            && batch.lastOffset() < getHighWatermark(batch.topicPartition())
            && batch.tieringState() == TieringState.DISKLESS;
    }
}
```

#### 4.7.5 Phase 1: Batch Splitting Job (Any Broker)

The batch splitter runs on **any broker** and processes WAL files within a claimed time range:

```java
// storage/inkless/src/main/java/io/aiven/inkless/tiering/BatchSplitterJob.java
public class BatchSplitterJob implements Runnable {
    
    private final int brokerId;
    private final ControlPlane controlPlane;
    private final ObjectFetcher objectFetcher;
    private final SplitBatchWriter splitBatchWriter;
    
    @Override
    public void run() {
        // 1. Claim a time range of WAL files to process
        //    This prevents multiple brokers from processing the same files
        Optional<WalFileClaimResult> claim = controlPlane.claimWalFilesForSplitting(
            brokerId,
            maxFilesPerClaim,
            claimDurationMs
        );
        
        if (claim.isEmpty()) {
            // No eligible WAL files or all claimed by other brokers
            return;
        }
        
        for (WalFileWithEligibleBatches walFile : claim.get().walFiles()) {
            try {
                processWalFile(walFile);
            } catch (Exception e) {
                // Release claim on failure, allow retry
                controlPlane.releaseWalFileClaim(walFile.objectKey());
                throw e;
            }
        }
    }
    
    private void processWalFile(WalFileWithEligibleBatches walFile) {
        // 2. Read the shared WAL file
        byte[] walData = objectFetcher.fetch(walFile.objectKey());
        
        // 3. Parse and group batches by topic-partition
        Map<TopicIdPartition, List<RecordBatch>> batchesByPartition = 
            parseBatchesByPartition(walData, walFile.eligibleBatches());
        
        // 4. Write split batches to intermediate storage
        for (var entry : batchesByPartition.entrySet()) {
            TopicIdPartition tip = entry.getKey();
            List<RecordBatch> batches = entry.getValue();
            
            SplitBatchFile splitFile = splitBatchWriter.write(tip, batches);
            
            // 5. Record split file in control plane
            controlPlane.recordSplitBatch(new SplitBatchRecord(
                tip,
                splitFile.objectKey(),
                splitFile.baseOffset(),
                splitFile.lastOffset(),
                splitFile.sizeBytes(),
                brokerId  // Track which broker created the split
            ));
        }
        
        // 6. Mark original batches as split (but not yet tiered)
        controlPlane.markBatchesAsSplit(walFile.batchIds());
    }
}
```

#### 4.7.6 Phase 2: Segment Merging Job (Partition Leader Only)

The segment merger runs on **partition leaders only**, consistent with how classic tiered storage works:

```java
// storage/inkless/src/main/java/io/aiven/inkless/tiering/SegmentMergerJob.java
public class SegmentMergerJob implements Runnable {
    
    private final int brokerId;
    private final ReplicaManager replicaManager;
    private final ControlPlane controlPlane;
    private final ObjectFetcher objectFetcher;
    private final RemoteLogMetadataManager remoteLogMetadataManager;
    private final RemoteStorageManager remoteStorageManager;
    
    @Override
    public void run() {
        // 1. Find partitions where this broker is the leader
        //    AND has enough split batches to form a segment
        List<PartitionWithPendingSplits> partitions = 
            controlPlane.findPartitionsReadyForSegmentMerge(
                brokerId,
                minSegmentBytes,
                maxSegmentBytes
            );
        
        for (PartitionWithPendingSplits partition : partitions) {
            // Verify we're still the leader (could have changed)
            if (!isLeaderForPartition(partition.topicIdPartition())) {
                continue;
            }
            
            mergePartitionSplits(partition);
        }
    }
    
    private boolean isLeaderForPartition(TopicIdPartition tip) {
        Partition partition = replicaManager.getPartition(tip.topicPartition());
        return partition != null && partition.isLeader();
    }
    
    private void mergePartitionSplits(PartitionWithPendingSplits partition) {
        // 2. Collect split batch files for this partition
        List<SplitBatchFile> splitFiles = 
            controlPlane.getSplitBatchFiles(partition.topicIdPartition());
        
        // 3. Merge into a single log segment
        LogSegmentData segmentData = mergeToLogSegment(
            partition.topicIdPartition(),
            splitFiles
        );
        
        // 4. Upload to tiered storage via RSM
        RemoteLogSegmentId segmentId = RemoteLogSegmentId.generateNew(
            partition.topicIdPartition()
        );
        
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(
            segmentId,
            segmentData.baseOffset(),
            segmentData.lastOffset(),
            segmentData.maxTimestamp(),
            brokerId,
            time.milliseconds(),
            segmentData.sizeBytes(),
            segmentData.leaderEpochs()
        );
        
        // 5. Upload segment to remote storage
        remoteStorageManager.copyLogSegmentData(metadata, segmentData);
        
        // 6. Register with RLMM
        remoteLogMetadataManager.addRemoteLogSegmentMetadata(metadata);
        
        // 7. Update control plane - mark batches as tiered
        controlPlane.markBatchesAsTiered(
            partition.topicIdPartition(),
            segmentData.baseOffset(),
            segmentData.lastOffset()
        );
        
        // 8. DELETE batch metadata from PostgreSQL (key for scalability!)
        controlPlane.deleteTieredBatchMetadata(
            partition.topicIdPartition(),
            segmentData.baseOffset(),
            segmentData.lastOffset()
        );
        
        // 9. Update offset tracking
        controlPlane.updateTieredHighOffset(
            partition.topicIdPartition(),
            segmentData.lastOffset()
        );
        
        // 10. Cleanup split batch files (now redundant)
        for (SplitBatchFile splitFile : splitFiles) {
            objectStorage.delete(splitFile.objectKey());
        }
        controlPlane.deleteSplitBatchRecords(splitFiles);
    }
    
    private LogSegmentData mergeToLogSegment(
        TopicIdPartition tip,
        List<SplitBatchFile> splitFiles
    ) {
        // Sort by base offset
        splitFiles.sort(Comparator.comparing(SplitBatchFile::baseOffset));
        
        // Build segment with indexes
        LogSegmentBuilder builder = new LogSegmentBuilder(tip);
        
        for (SplitBatchFile splitFile : splitFiles) {
            byte[] data = objectFetcher.fetch(splitFile.objectKey());
            builder.appendBatches(data);
        }
        
        return builder.build();  // Includes .log, .index, .timeindex, .txnindex
    }
}
```

#### 4.7.7 Control Plane Extensions for Tiering

```java
// storage/inkless/src/main/java/io/aiven/inkless/control_plane/ControlPlane.java
public interface ControlPlane {
    // ... existing methods ...
    
    // NEW: Tiering support - Splitting (any broker)
    
    /**
     * Claim a set of WAL files for splitting.
     * Uses optimistic locking to prevent multiple brokers from processing the same files.
     * Returns empty if no eligible files or all are already claimed.
     */
    Optional<WalFileClaimResult> claimWalFilesForSplitting(
        int brokerId,
        int maxFiles,
        long claimDurationMs
    );
    
    /**
     * Release a claim on a WAL file (on failure or timeout).
     */
    void releaseWalFileClaim(String objectKey);
    
    /**
     * Record a split batch file created from WAL extraction.
     */
    void recordSplitBatch(SplitBatchRecord record);
    
    /**
     * Mark batches as split (extracted from WAL, pending merge).
     */
    void markBatchesAsSplit(List<Long> batchIds);
    
    // NEW: Tiering support - Merging (partition leader only)
    
    /**
     * Find partitions where the given broker is leader
     * AND has enough pending splits to form a segment.
     */
    List<PartitionWithPendingSplits> findPartitionsReadyForSegmentMerge(
        int brokerId,
        long minSegmentBytes,
        long maxSegmentBytes
    );
    
    /**
     * Get split batch files for a partition, ordered by offset.
     */
    List<SplitBatchFile> getSplitBatchFiles(TopicIdPartition partition);
    
    /**
     * Mark batches as tiered (merged into remote segment).
     */
    void markBatchesAsTiered(TopicIdPartition tip, long baseOffset, long lastOffset);
    
    /**
     * DELETE batch metadata from PostgreSQL after tiering.
     * This is critical for control plane scalability.
     */
    void deleteTieredBatchMetadata(TopicIdPartition tip, long baseOffset, long lastOffset);
    
    /**
     * Update the highest tiered offset for a partition.
     */
    void updateTieredHighOffset(TopicIdPartition tip, long offset);
    
    /**
     * Delete split batch file records after successful merge.
     */
    void deleteSplitBatchRecords(List<SplitBatchFile> splitFiles);
}
```

#### 4.7.8 Database Schema for Tiering State

```sql
-- Track batch tiering state
ALTER TABLE inkless_batch ADD COLUMN tiering_state VARCHAR(20) DEFAULT 'DISKLESS';
-- Values: DISKLESS, SPLIT, TIERED

-- Track WAL file claims for distributed splitting
CREATE TABLE inkless_wal_file_claim (
    object_key VARCHAR(512) PRIMARY KEY,
    claimed_by_broker INT NOT NULL,
    claimed_at TIMESTAMP NOT NULL,
    claim_expires_at TIMESTAMP NOT NULL,
    split_completed BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_wal_claim_expiry ON inkless_wal_file_claim(claim_expires_at);

-- Track split batch files (intermediate state)
CREATE TABLE inkless_split_batch (
    id BIGSERIAL PRIMARY KEY,
    topic_id UUID NOT NULL,
    partition INT NOT NULL,
    object_key VARCHAR(512) NOT NULL,
    base_offset BIGINT NOT NULL,
    last_offset BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    created_by_broker INT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT fk_topic FOREIGN KEY (topic_id, partition) 
        REFERENCES inkless_log(topic_id, partition)
);

CREATE INDEX idx_split_batch_partition ON inkless_split_batch(topic_id, partition, base_offset);
```

#### 4.7.9 Offset Boundary Management

With continuous diskless-to-tiered conversion, the offset boundaries shift over time:

```
Time T1:  Diskless covers offsets [0, 1000]
          tieredHighOffset = -1
          disklessStartOffset = 0
          
Time T2:  Conversion runs, offsets [0, 500] converted to tiered
          tieredHighOffset = 500
          disklessStartOffset = 501  (logical, batches still in diskless but marked as tiered)
          
Time T3:  More data written, more conversion
          tieredHighOffset = 800
          disklessStartOffset = 801
          highWatermark = 1500
```

The read path uses these boundaries:
- `offset < disklessStartOffset` → Read from tiered storage (RLM)
- `offset >= disklessStartOffset` → Read from diskless storage (Reader)

#### 4.7.10 Scheduling and Distributed Execution

The tiering pipeline runs on all brokers, with different responsibilities:

```java
// storage/inkless/src/main/java/io/aiven/inkless/tiering/TieringScheduler.java
public class TieringScheduler {
    
    private final int brokerId;
    private final ScheduledExecutorService executor;
    private final BatchSplitterJob splitterJob;  // Runs on ALL brokers
    private final SegmentMergerJob mergerJob;    // Effective only on leaders
    
    public void start() {
        // Batch splitter runs on ALL brokers
        // Each broker claims and processes different WAL files
        executor.scheduleWithFixedDelay(
            splitterJob,
            randomInitialDelay(),  // Stagger starts to reduce contention
            config.batchSplitterIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        
        // Segment merger runs on ALL brokers, but only processes
        // partitions where this broker is the leader
        executor.scheduleWithFixedDelay(
            mergerJob,
            config.segmentMergerInitialDelayMs(),
            config.segmentMergerIntervalMs(),
            TimeUnit.MILLISECONDS
        );
    }
    
    private long randomInitialDelay() {
        // Random delay to prevent all brokers from starting simultaneously
        return ThreadLocalRandom.current().nextLong(0, config.batchSplitterIntervalMs());
    }
}
```

**Distributed Execution Summary:**

| Job | Runs On | Work Distribution | Coordination |
|-----|---------|-------------------|--------------|
| BatchSplitterJob | All brokers | WAL files claimed via optimistic locking | `inkless_wal_file_claim` table |
| SegmentMergerJob | All brokers | Only processes partitions where broker is leader | Standard Kafka leadership |

#### 4.7.11 Configuration

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

```java
// NEW: storage/inkless/src/main/java/io/aiven/inkless/metrics/HybridMetrics.java
public class HybridMetrics {
    
    // === Migration Metrics ===
    
    // Gauge: Number of topics in each migration state
    public static final String TOPICS_BY_STATE = "inkless.migration.topics.state";
    
    // Gauge: Per-topic migration progress (0-100%)
    public static final String TOPIC_MIGRATION_PROGRESS = "inkless.migration.topic.progress";
    
    // === Read Path Metrics ===
    
    // Counter: Reads from tiered storage (via RLM)
    public static final String HYBRID_TIERED_READS = "inkless.hybrid.tiered.reads";
    
    // Counter: Reads from local log (during migration window)
    public static final String HYBRID_LOCAL_READS = "inkless.hybrid.local.reads";
    
    // Counter: Reads from diskless storage
    public static final String HYBRID_DISKLESS_READS = "inkless.hybrid.diskless.reads";
    
    // === Tiering Pipeline Metrics ===
    
    // Counter: Batches split from WAL files
    public static final String TIERING_BATCHES_SPLIT = "inkless.tiering.batches.split";
    
    // Counter: Segments created from merged batches
    public static final String TIERING_SEGMENTS_CREATED = "inkless.tiering.segments.created";
    
    // Gauge: Pending batches awaiting split
    public static final String TIERING_BATCHES_PENDING_SPLIT = "inkless.tiering.batches.pending.split";
    
    // Gauge: Split batches awaiting merge
    public static final String TIERING_BATCHES_PENDING_MERGE = "inkless.tiering.batches.pending.merge";
    
    // Gauge: Bytes of diskless data eligible for tiering
    public static final String TIERING_ELIGIBLE_BYTES = "inkless.tiering.eligible.bytes";
    
    // Histogram: Time taken for batch splitting
    public static final String TIERING_SPLIT_LATENCY = "inkless.tiering.split.latency";
    
    // Histogram: Time taken for segment merging
    public static final String TIERING_MERGE_LATENCY = "inkless.tiering.merge.latency";
    
    // === Offset Tracking Metrics ===
    
    // Gauge: Tiered high offset per partition
    public static final String TIERED_HIGH_OFFSET = "inkless.tiered.high.offset";
    
    // Gauge: Diskless start offset per partition
    public static final String DISKLESS_START_OFFSET = "inkless.diskless.start.offset";
    
    // Gauge: Gap between diskless and tiered (unconverted data)
    public static final String TIERING_LAG_BYTES = "inkless.tiering.lag.bytes";
}
```

#### 4.8.2 Admin API Extensions

```java
// New admin API for migration and tiering management
public interface HybridAdmin {
    
    // === Migration APIs ===
    
    // Start migration for a topic (tiered → diskless)
    MigrateToDisklessResult migrateToDiskless(String topicName);
    
    // Get migration status
    MigrationStatus getMigrationStatus(String topicName);
    
    // List all topics by migration state
    Map<MigrationState, List<String>> listTopicsByMigrationState();
    
    // Force completion (for stuck migrations)
    void forceMigrationComplete(String topicName);
    
    // === Tiering APIs ===
    
    // Get tiering status for a topic
    TieringStatus getTieringStatus(String topicName);
    
    // Trigger immediate tiering for a topic (bypasses scheduler)
    void triggerTiering(String topicName);
    
    // Pause tiering for a topic
    void pauseTiering(String topicName);
    
    // Resume tiering for a topic
    void resumeTiering(String topicName);
}

record TieringStatus(
    String topicName,
    long tieredHighOffset,
    long disklessStartOffset,
    long highWatermark,
    long pendingBatchesCount,
    long pendingSplitFilesCount,
    long eligibleBytes,
    boolean isPaused
) {}
```

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

### 5.2 Write Path During Migration

```
┌──────────┐                           ┌─────────────────┐
│ Producer │ ─────Produce(msg)────────►│  KafkaApis      │
└──────────┘                           └────────┬────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │ Is diskless topic?    │
                                    │ state == HYBRID       │
                                    └───────────┬───────────┘
                                                │ Yes
                                                ▼
                                    ┌───────────────────────┐
                                    │   AppendHandler       │
                                    │   (Diskless write)    │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  Object Storage       │
                                    │  (Inkless format)     │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  ControlPlane         │
                                    │  Update HWM           │
                                    └───────────────────────┘
```

### 5.3 Diskless-to-Tiered Conversion Pipeline (Distributed)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         DISTRIBUTED TIERING PIPELINE                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  PHASE 1: BATCH SPLITTING (runs on ANY broker)                                  │
│                                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │   Scheduler (all brokers)│                                                   │
│  │   (periodic trigger)     │                                                   │
│  └────────────┬─────────────┘                                                   │
│               │                                                                 │
│               ▼                                                                 │
│  ┌──────────────────────────┐     ┌──────────────────────────────────────────┐ │
│  │   Control Plane          │     │   Object Storage (Diskless)              │ │
│  │                          │     │                                          │ │
│  │  claimWalFilesFor-       │     │   ┌──────────────────────────────────┐   │ │
│  │  Splitting(brokerId)     │────►│   │  WAL File                        │   │ │
│  │                          │     │   │  ┌────┐┌────┐┌────┐┌────┐┌────┐  │   │ │
│  │  Returns WAL files       │     │   │  │T0  ││T1  ││T0  ││T2  ││T1  │  │   │ │
│  │  claimed by this broker  │     │   │  │B1  ││B1  ││B2  ││B1  ││B2  │  │   │ │
│  │  (optimistic locking)    │     │   │  └────┘└────┘└────┘└────┘└────┘  │   │ │
│  └──────────────────────────┘     │   └──────────────────────────────────┘   │ │
│               │                   └──────────────────────────────────────────┘ │
│               │                                                                 │
│               ▼                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │   BatchSplitterJob       │                                                   │
│  │   (any broker)           │                                                   │
│  │                          │                                                   │
│  │  1. Claim WAL files      │                                                   │
│  │  2. Read WAL file        │                                                   │
│  │  3. Parse batches        │                                                   │
│  │  4. Group by partition   │                                                   │
│  │  5. Write split files    │                                                   │
│  └────────────┬─────────────┘                                                   │
│               │                                                                 │
│               ▼                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │   Object Storage (Split Files - Intermediate)                            │  │
│  │                                                                          │  │
│  │   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │  │
│  │   │  T0-split-001    │  │  T1-split-001    │  │  T2-split-001    │       │  │
│  │   │  [B1][B2]        │  │  [B1][B2]        │  │  [B1]            │       │  │
│  │   └──────────────────┘  └──────────────────┘  └──────────────────┘       │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│               │                                                                 │
│               │  markBatchesAsSplit()                                           │
│               ▼                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │   Control Plane          │                                                   │
│  │   inkless_split_batch    │                                                   │
│  │   table updated          │                                                   │
│  └──────────────────────────┘                                                   │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  PHASE 2: SEGMENT MERGING (runs on PARTITION LEADER only)                       │
│                                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │   Scheduler (all brokers)│                                                   │
│  │   (periodic trigger)     │                                                   │
│  └────────────┬─────────────┘                                                   │
│               │                                                                 │
│               ▼                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │   Control Plane          │                                                   │
│  │                          │                                                   │
│  │  findPartitionsReadyFor- │                                                   │
│  │  SegmentMerge(brokerId)  │                                                   │
│  │                          │                                                   │
│  │  Returns partitions      │                                                   │
│  │  WHERE broker is leader  │                                                   │
│  │  AND split files >= min  │                                                   │
│  │  segment size            │                                                   │
│  └────────────┬─────────────┘                                                   │
│               │                                                                 │
│               ▼                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │   SegmentMergerJob       │                                                   │
│  │   (partition leader)     │                                                   │
│  │                          │                                                   │
│  │  1. Verify still leader  │                                                   │
│  │  2. Collect split files  │                                                   │
│  │  3. Sort by offset       │                                                   │
│  │  4. Build log segment    │                                                   │
│  │     with indexes         │                                                   │
│  │  5. Upload to RSM        │                                                   │
│  │  6. Register with RLMM   │                                                   │
│  │  7. DELETE batch         │  ← Key for scalability!                           │
│  │     metadata from PG     │                                                   │
│  └────────────┬─────────────┘                                                   │
│               │                                                                 │
│               ▼                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │   Remote Storage (Tiered - RSM)                                          │  │
│  │                                                                          │  │
│  │   ┌───────────────────────────────────────────────────────────────────┐  │  │
│  │   │  topic-0/segment-00001                                            │  │  │
│  │   │                                                                   │  │  │
│  │   │  ┌─────────────────────────────────────────────────────────────┐  │  │  │
│  │   │  │  .log     │  .index  │  .timeindex  │  .txnindex (optional)│  │  │  │
│  │   │  │ [B1][B2]  │ offsets  │  timestamps  │  transactions        │  │  │  │
│  │   │  │ [B3]...   │          │              │                      │  │  │  │
│  │   │  └─────────────────────────────────────────────────────────────┘  │  │  │
│  │   └───────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│               │                                                                 │
│               │  markBatchesAsTiered(), updateTieredHighOffset()                │
│               ▼                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │   Control Plane          │                                                   │
│  │                          │                                                   │
│  │  tieredHighOffset ←──────│── updated                                         │
│  │  disklessStartOffset ←───│── updated                                         │
│  │                          │                                                   │
│  └──────────────────────────┘                                                   │
│               │                                                                 │
│               │  Cleanup: delete split files                                    │
│               ▼                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │   Object Storage         │                                                   │
│  │   (Split files deleted)  │                                                   │
│  └──────────────────────────┘                                                   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
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

For detailed implementation timeline, task breakdown, and team assignments, see the separate **[Project Plan](./TIERED_STORAGE_UNIFICATION_PROJECT_PLAN.md)**.

### High-Level Phases

| Phase | Description | Dependencies |
|-------|-------------|--------------|
| **P1: Foundation** | Schema, state machines, test infrastructure | — |
| **P2: Read Path** | Three-tier read routing via UnifiedLog | P1 |
| **P2b: Batch Splitting** | WAL file splitting (parallel with P2/P3) | P1 |
| **P3: Switch Mechanism** | Tiered → Hybrid migration | P2 |
| **P4: Multi-Replica** | RF=3 for diskless topics | P3 |
| **P5: RLM Integration** | Hybrid topic support in RLM | P3 |
| **P6: Segment Merging** | Split files → Tiered segments | P2b |
| **P7: E2E Integration** | Full pipeline validation | P5, P6 |
| **P8: Observability** | Metrics, admin APIs, docs | P7 |

### Critical Path

The minimum path to deliver value:

```
P1 (Foundation) → P2 (Read Path) → P3 (Switch) → P5 (RLM) → P7 (E2E)
     2 weeks         2 weeks        2 weeks       2 weeks     2 weeks
                                                          = 10 weeks
```

### Parallelizable Work

- **P2b + P6** (Tiering pipeline) can run in parallel with the migration path
- **P4** (Multi-Replica) can be deferred for MVP
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

### Tiering Pipeline Questions

4. **Batch splitting granularity?**
   - Option A: One split file per partition per WAL file
   - Option B: Accumulate batches across WAL files until min size
   - Option C: Time-based windows for grouping
   - **Recommendation:** Option A initially for simplicity, consider Option B for efficiency

5. **Segment size for converted batches?**
   - Should match classic segment.bytes config?
   - Should be configurable separately for tiered-from-diskless segments?
   - **Recommendation:** Use topic's segment.bytes config for consistency with RLM

6. **Index building for converted segments?**
   - Should we build full indexes (offset, time, transaction)?
   - Sparse vs dense indexing?
   - **Recommendation:** Full indexes for RLM compatibility, use existing index building code

7. **Handling gaps in converted offsets?**
   - If batches are deleted before conversion (compaction, delete records API)
   - Option A: Skip deleted offsets, create segments with gaps
   - Option B: Track deletions and exclude from conversion eligibility
   - **Recommendation:** Option B - cleaner segment boundaries

8. **Conversion failure handling?**
   - Option A: Retry indefinitely with backoff
   - Option B: Move to dead-letter queue after N failures
   - Option C: Alert and require manual intervention
   - **Recommendation:** Option A with alerting after threshold, Option C for persistent failures

9. **WAL file cleanup after all batches converted?**
   - When can a WAL file be safely deleted?
   - Need to track which batches are still referenced
   - **Recommendation:** WAL file eligible for deletion when all its batches are either tiered or expired

### Data Lifecycle Questions

10. **Retention policy for converted segments?**
    - Use topic's retention.ms/retention.bytes?
    - Should be consistent with existing tiered segments
    - **Recommendation:** Use topic's retention config, RLM expiration handles cleanup

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
- [Inkless Architecture Documentation](./architecture.md)
- [RemoteLogManager Implementation](../../core/src/main/java/kafka/log/remote/RemoteLogManager.java)
- [ControlPlane Interface](../../storage/inkless/src/main/java/io/aiven/inkless/control_plane/ControlPlane.java)

