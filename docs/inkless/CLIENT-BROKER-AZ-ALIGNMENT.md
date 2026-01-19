# Client-Broker AZ Alignment

This document describes how Inkless enables **client rack awareness** to route client requests to brokers within the same availability zone (AZ), minimizing cross-AZ data transfer costs in cloud deployments.

## Summary

### Current State (Inkless 0.32+)

- ✅ Client rack awareness implemented via `client.id` pattern
- ✅ Works with any Kafka client version
- ✅ Eliminates most cross-AZ costs for diskless topics
- ⚠️ Non-standard approach, requires user awareness

### Future State (KIP-1163)

- ⏳ KIP-1163: Adds `client.rack` to metadata requests (under discussion)
- ⏳ Standard `client.rack` configuration for both producers and consumers
- ⏳ Inkless will support both approaches for backward compatibility

### Migration

- Phase 1: Use `client.id` pattern (now)
- Phase 2: Support both mechanisms (when KIP-1163 is available)

**Recommendation:** Start using `client.id=app,diskless_az=<rack>` pattern now for immediate cost savings. When KIP-1163 becomes available in Kafka clients, consider migrating to `client.rack` for a standard configuration.

## Table of Contents

- [Summary](#summary)
- [Overview](#overview)
- [Relationship to Kafka Rack Awareness](#relationship-to-kafka-rack-awareness)
- [Current Implementation](#current-implementation)
- [Future Implementation (KIP-1163)](#future-implementation-kip-1163)
- [Migration Path](#migration-path)
- [Configuration Guide](#configuration-guide)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [References](#references)

---

## Overview

### The Problem

In cloud deployments (AWS, GCP, Azure), data transfer within the same availability zone is typically free or low-cost, while cross-AZ data transfer incurs significant charges. For high-throughput Kafka workloads, these costs can be substantial.

### The Solution

Inkless enables **client rack awareness** to route client requests to brokers within the same availability zone, eliminating most cross-AZ traffic for diskless topics:

1. **Producers** send produce requests to brokers in their local AZ
2. **Consumers** fetch data from brokers in their local AZ (benefiting from broker-local cache)
3. **Brokers** store data in shared object storage accessible from all AZs
4. **Caching** uses deterministic partition-to-broker assignment with local in-memory cache per broker (Caffeine), maximizing cache hit rates

### Benefits

- **Eliminate most cross-AZ data transfer costs** - Requests stay local to each AZ
- **Preserve durability** - Object storage provides cross-AZ replication
- **Maintain ordering guarantees** - Batch Coordinator ensures global ordering
- **Simple deployment** - Works with existing multi-AZ Kafka deployments

---

## Relationship to Kafka Rack Awareness

Apache Kafka has existing rack awareness features that Inkless builds upon. Understanding these is important for proper configuration.

### Broker Rack Configuration (`broker.rack`)

The `broker.rack` configuration is a standard Kafka broker setting that identifies the broker's physical location (rack, availability zone, or data center).

```properties
# server.properties
broker.rack=us-east-1a
```

**Used by:**
- **Replica placement**: Kafka distributes partition replicas across different racks for fault tolerance
- **Consumer follower fetching**: Consumers with `client.rack` can fetch from nearby replicas
- **Inkless client rack awareness**: Inkless uses this to match clients to brokers in the same AZ

### Consumer Rack Awareness (`client.rack`)

Standard Kafka consumers support `client.rack` configuration for **follower fetching** (KIP-392):

```properties
# Consumer configuration
client.rack=us-east-1a
```

This allows consumers to fetch from the closest replica instead of always fetching from the leader. However, this only works for **classic topics** with multiple replicas.

### Inkless Client Rack Awareness

For **diskless topics**, Inkless extends rack awareness to both producers and consumers:

| Feature                     | Classic Topics                     | Diskless Topics (Inkless)                              |
|-----------------------------|------------------------------------|---------------------------------------------------------|
| Broker rack config          | `broker.rack`                      | `broker.rack` (same)                                   |
| Consumer rack-aware fetch   | `client.rack` (follower fetching)  | `client.id` pattern (current) / `client.rack` (future) |
| Producer rack-aware routing | `client.rack` (KIP-1123, approved) | `client.id` pattern (current) / `client.rack` (future) |
| Mechanism                   | Fetch from nearest replica         | Route to broker in same AZ                             |

**Key difference**: In classic Kafka, rack awareness helps consumers fetch from nearby *replicas*. In Inkless, client rack awareness routes requests to nearby *brokers* since any broker can serve any diskless partition (data is in object storage).

**Note on related KIPs**:
- [KIP-1123](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1123%3A+Rack-aware+partitioning+for+Kafka+Producer) (approved, [PR #19850](https://github.com/apache/kafka/pull/19850)): Adds `client.rack` to the producer configuration for rack-aware *partition selection* of keyless records. However, this KIP does **not** add `client.rack` to the metadata request protocol.
- [KIP-1163: Diskless Core](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core) (under discussion): Adds `RackId` field to MetadataRequest and DescribeTopicPartitionsRequest, enabling brokers to return rack-aware metadata for diskless topics. **This is the KIP required for standard `client.rack` support in Inkless.**

---

## Current Implementation

The current Inkless implementation uses a **client.id pattern-based approach** for client rack awareness.

### Mechanism

Clients embed their rack/AZ information in the `client.id` configuration property using a specific pattern:

```
client.id=<application-name>,diskless_az=<rack-id>
```

**Components:**
- `<application-name>`: Your application identifier (existing client.id value)
- `diskless_az=`: Special marker that Inkless recognizes
- `<rack-id>`: The availability zone identifier, must match broker's `broker.rack` value

**Example:**
```properties
# Producer in us-east-1a
client.id=payment-producer,diskless_az=us-east-1a

# Consumer in us-west-2b
client.id=analytics-consumer,diskless_az=us-west-2b
```

### How It Works

#### Implementation Details

The Inkless broker parses the `client.id` field using a regex pattern:

```java
// From ClientAZExtractor.java
private static final Pattern CLIENT_AZ_PATTERN = 
    Pattern.compile("(^|[ ,])diskless_az=(?<az>[^=, ]*)");
```

This allows flexibility in placement:
- `diskless_az=us-east-1a` (at start)
- `my-app,diskless_az=us-east-1a` (after comma)
- `my-app diskless_az=us-east-1a` (after space)

#### Producer Flow

AZ matching happens during the **Metadata request**, not the Produce request:

1. Producer sends Metadata request with `client.id=my-producer,diskless_az=us-east-1a`
2. Broker extracts the client's AZ (`us-east-1a`) from the `client.id`
3. Broker returns metadata with partition leaders assigned to brokers in the client's AZ
4. Producer caches this metadata and sends Produce requests to the assigned brokers
5. Subsequent Produce requests go directly to the local AZ broker (no AZ extraction needed per request)

**Key Points:**
- The AZ-aware routing decision is made once during metadata fetch, not per produce request
- Producers automatically refresh metadata periodically, adapting to broker topology changes
- If no brokers exist in the client's rack, brokers from other racks are assigned as fallback

#### Consumer Flow

AZ matching happens during the **Metadata request**, similar to producers:

1. Consumer sends Metadata request with `client.id=my-consumer,diskless_az=us-east-1a`
2. Broker extracts the client's AZ and returns metadata with partition leaders in the client's AZ
3. Consumer fetches from the assigned broker, which serves data from its local cache or object storage
4. Deterministic partition assignment ensures the same broker handles the same partitions, improving cache hit rates

**Key Points:**
- If no brokers exist in the client's rack, brokers from other racks are used as fallback
- Each broker maintains its own local in-memory cache (Caffeine)

#### Caching Architecture

Inkless uses a **local in-memory cache (Caffeine) per broker** combined with **deterministic partition-to-broker assignment** and **AZ-aware metadata routing**. This architecture achieves cache locality properties similar to a distributed cache:

| Property         | Distributed Cache (e.g., Infinispan)       | Inkless Approach                                                                     |
|------------------|--------------------------------------------|---------------------------------------------------------------------------------------|
| Cache per AZ     | Shared distributed cache across AZ brokers | Each broker has local cache; AZ-aware routing directs clients to consistent brokers  |
| Data locality    | Entries replicated within AZ               | Deterministic assignment ensures same partition always served by same broker per AZ  |
| Cache efficiency | Network overhead for cache coordination    | No coordination overhead; hash-based assignment provides consistency                 |
| Complexity       | Requires distributed cache infrastructure  | Simple local cache with deterministic routing                                        |

##### How It Achieves Similar Results

- **Deterministic partition assignment**: A partition always maps to the same broker within an AZ (via MurmurHash2), so repeated fetches hit the same broker's cache
- **AZ-aware metadata**: Clients in the same AZ are directed to the same set of brokers, ensuring cache is warmed per-AZ
- **Result**: Within an AZ, cache entries are effectively "per-AZ" because all clients in that AZ hit the same broker for a given partition

#### Broker Selection Algorithm

The broker selection for diskless partitions uses a **deterministic hash-based algorithm**:

1. A hash is computed from the topic ID and partition index using MurmurHash2
2. The hash determines which broker (from the eligible set) handles each partition
3. This ensures consistent routing across metadata requests

```
broker_index = abs(murmur2(topicId + "-" + partitionIndex)) % eligible_brokers.size()
```

##### Benefits of Deterministic Selection

- **Consistency**: The same partition always routes to the same broker (within an AZ) across restarts
- **Cache efficiency**: Clients consistently connect to the same broker, improving cache hit rates
- **Load distribution**: Partitions are evenly distributed across brokers using the hash

##### Behavior During Broker Changes

- When a broker joins or leaves, some partitions may be reassigned to different brokers
- Clients will receive updated metadata and reconnect to the new broker
- Cache misses may occur temporarily until the new broker warms up

#### Diskless Topics vs Classic Topics: ISR Semantics

Diskless topics have fundamentally different ISR (In-Sync Replicas) semantics compared to classic topics:

| Aspect                      | Classic Topics                      | Diskless Topics                               |
|-----------------------------|-------------------------------------|-----------------------------------------------|
| Replication                 | Brokers replicate data to followers | No inter-broker replication; object storage   |
| ISR meaning                 | Replicas caught up with the leader  | All alive brokers are effectively "in-sync"   |
| Leader election             | Based on ISR membership             | Any broker can serve any partition            |
| Under-replicated partitions | Possible when followers fall behind | Not applicable; no replication to fall behind |

**Implication for client rack awareness:** Since any broker can serve any diskless partition, the client rack awareness mechanism simply selects a broker in the client's AZ. There's no concern about ISR membership or replica lag.

### Advantages

✅ **No protocol changes required** - Works with existing Kafka clients  
✅ **Backward compatible** - Clients without the pattern work normally (but incur cross-AZ costs)  
✅ **Simple to implement** - No changes to Kafka protocol  
✅ **Flexible placement** - Pattern can appear anywhere in client.id  

### Limitations

❌ **Non-standard** - Not part of official Kafka protocol  
❌ **Client.id parsing** - Adds complexity and potential for errors  
❌ **Not discoverable** - Clients don't know which AZs are available  
❌ **Limited validation** - Typos in AZ names won't cause errors, just inefficiency  
❌ **Tooling challenges** - Monitoring tools may not understand the pattern  
❌ **Client library support** - Requires documentation and user awareness  

---

## Future Implementation (KIP-1163)

[KIP-1163: Diskless Core](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core) is currently **under discussion**.

This KIP adds the `RackId` field to MetadataRequest and DescribeTopicPartitionsRequest, allowing clients to communicate their rack to brokers via the Kafka protocol. This enables brokers to return rack-aware metadata for diskless topics, directing clients to brokers in their local AZ.

**Note**: While [KIP-1123](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1123%3A+Rack-aware+partitioning+for+Kafka+Producer) adds `client.rack` to the producer configuration, it only uses this for partition selection (choosing which partition to send keyless records to). KIP-1163 is required to transmit the rack information in metadata requests, which is what Inkless needs for AZ-aware broker routing.

### Configuration

```properties
# Standard configuration for producers and consumers
client.rack=us-east-1a
```

### Benefits

- **Consistent configuration**: Producers and consumers use the same `client.rack` property
- **No workarounds**: No need to embed rack information in `client.id`
- **Standard approach**: Aligns with upstream Apache Kafka
- **Protocol-level support**: Rack information transmitted via MetadataRequest

### How It Will Work

1. Producer/Consumer configured with `client.rack=us-east-1a`
2. Client sends MetadataRequest with `RackId=us-east-1a`
3. Broker returns metadata with partition leaders assigned to brokers in the client's rack
4. Client sends Produce/Fetch requests to the assigned local broker
5. Falls back to other AZs if no local brokers are available

### Differences from Current Implementation

| Aspect                 | Current (client.id pattern)            | Future (KIP-1163)                   |
|------------------------|----------------------------------------|-------------------------------------|
| Configuration          | `client.id=app,diskless_az=us-east-1a` | `client.rack=us-east-1a`            |
| Protocol support       | Extracted from client.id string        | Native `RackId` in MetadataRequest  |
| Producer support       | Via `client.id` pattern                | Native via `client.rack`            |
| Consumer support       | Via `client.id` pattern                | Native via `client.rack`            |
| Validation             | No validation of rack IDs              | Potential for validation            |
| Monitoring             | Parse client.id in logs/metrics        | Standard property                   |
| Client support         | Requires user awareness                | Built into Kafka clients            |
| Backward compatibility | N/A                                    | Inkless will support both           |

---

## Migration Path

### Phase 1: Current Implementation (Now)

**Status:** Available in Inkless 0.32+

**Action:** Use `client.id` pattern for client rack awareness

```properties
# Producer
client.id=my-app,diskless_az=us-east-1a

# Consumer  
client.id=my-app,diskless_az=us-east-1a
```

This approach works with any Kafka client version and requires no protocol changes.

**Timeline:** Indefinite; will remain supported for backward compatibility with older clients.

---

### Phase 2: Dual Support (Future)

**Status:** Planned for when KIP-1163 is available in Kafka clients

**Action:** Support both mechanisms simultaneously

```properties
# Option A: Current approach (for older clients or backward compatibility)
client.id=my-app,diskless_az=us-east-1a

# Option B: Standard approach (recommended for new deployments)
client.rack=us-east-1a
```

**Behavior:**
- If `client.rack` is set and transmitted via MetadataRequest (KIP-1163), use it (preferred)
- If only `client.id` pattern is detected, use current mechanism
- If both are set, `client.rack` takes precedence

**Why support both:**
- Enables gradual migration without breaking existing clients
- Allows mixed deployments with clients on different Kafka versions
- Provides flexibility for environments where client upgrades are constrained

**Timeline:** When KIP-1163 is released in Apache Kafka

---

## Configuration Guide

### Broker Configuration

```properties
# Set broker's rack ID (required)
broker.rack=us-east-1a

# Enable diskless support
log.diskless.enable=true

# Configure object storage (example for S3)
inkless.storage.backend.class=io.aiven.inkless.storage_backend.s3.S3Storage
inkless.storage.s3.bucket.name=my-bucket
inkless.storage.s3.region=us-east-1

# Configure batch coordinator
inkless.control.plane.class=io.aiven.inkless.control_plane.PostgresControlPlane
inkless.control.plane.connection.string=jdbc:postgresql://db.example.com:5432/inkless
```

**Important:** All brokers in the same availability zone should have identical `broker.rack` values.

### Client Configuration (Current Implementation)

#### Producer

```properties
# Standard producer configs
bootstrap.servers=broker1:9092,broker2:9092,broker3:9092
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer

# Client rack awareness (current implementation)
client.id=payment-service,diskless_az=us-east-1a

# Tuning for diskless topics
linger.ms=100
batch.size=1000000
max.request.size=8000000
```

#### Consumer

```properties
# Standard consumer configs
bootstrap.servers=broker1:9092,broker2:9092,broker3:9092
group.id=analytics-group
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer

# Client rack awareness (current implementation)
client.id=analytics-consumer,diskless_az=us-west-2b

# Fetch settings
fetch.min.bytes=1048576
fetch.max.wait.ms=500
```

### Multi-AZ Deployment Example

#### AWS: 3 AZs, 6 Brokers

```
┌───────────────────────────────────────────────────────────────────────┐
│ AWS Region: us-east-1                                                 │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐  │
│  │ us-east-1a        │  │ us-east-1b        │  │ us-east-1c        │  │
│  │                   │  │                   │  │                   │  │
│  │ Broker 0          │  │ Broker 2          │  │ Broker 4          │  │
│  │ broker.rack=      │  │ broker.rack=      │  │ broker.rack=      │  │
│  │   us-east-1a      │  │   us-east-1b      │  │   us-east-1c      │  │
│  │ [Caffeine cache]  │  │ [Caffeine cache]  │  │ [Caffeine cache]  │  │
│  │                   │  │                   │  │                   │  │
│  │ Broker 1          │  │ Broker 3          │  │ Broker 5          │  │
│  │ broker.rack=      │  │ broker.rack=      │  │ broker.rack=      │  │
│  │   us-east-1a      │  │   us-east-1b      │  │   us-east-1c      │  │
│  │ [Caffeine cache]  │  │ [Caffeine cache]  │  │ [Caffeine cache]  │  │
│  │                   │  │                   │  │                   │  │
│  │ Producers         │  │ Producers         │  │ Producers         │  │
│  │ client.id=app,    │  │ client.id=app,    │  │ client.id=app,    │  │
│  │  diskless_az=     │  │  diskless_az=     │  │  diskless_az=     │  │
│  │  us-east-1a       │  │  us-east-1b       │  │  us-east-1c       │  │
│  │                   │  │                   │  │                   │  │
│  │ Consumers         │  │ Consumers         │  │ Consumers         │  │
│  │ client.id=app,    │  │ client.id=app,    │  │ client.id=app,    │  │
│  │  diskless_az=     │  │  diskless_az=     │  │  diskless_az=     │  │
│  │  us-east-1a       │  │  us-east-1b       │  │  us-east-1c       │  │
│  └───────────────────┘  └───────────────────┘  └───────────────────┘  │
│            │                      │                      │            │
│            └──────────────────────┼──────────────────────┘            │
│                                   │                                   │
│                                   ▼                                   │
│                        ┌──────────────────┐                           │
│                        │ S3 Bucket        │                           │
│                        │ (multi-AZ)       │                           │
│                        └──────────────────┘                           │
│                                   ▲                                   │
│                                   │                                   │
│                        ┌──────────┴───────┐                           │
│                        │ PostgreSQL       │                           │
│                        │ (Batch Coord.)   │                           │
│                        │ (multi-AZ)       │                           │
│                        └──────────────────┘                           │
└───────────────────────────────────────────────────────────────────────┘
```

##### Configuration Summary

- 6 brokers across 3 AZs (2 per AZ), each with its own local Caffeine cache
- Each broker has `broker.rack` set to its AZ
- Producers/consumers in each AZ use matching `diskless_az` in client.id
- Deterministic partition assignment ensures a partition maps to the same broker within each AZ
- Object storage (S3) is multi-AZ accessible
- PostgreSQL can be single-AZ or multi-AZ depending on setup

##### Cache Behavior

- Each broker maintains an independent local cache (Caffeine)
- Deterministic routing ensures clients in the same AZ always reach the same broker for a given partition
- This achieves per-AZ cache locality without distributed cache coordination overhead

---

## Monitoring

Inkless exposes JMX metrics to monitor client rack awareness effectiveness:

| Metric Name              | Description                                           |
|--------------------------|-------------------------------------------------------|
| `client-az-hit-rate`     | Requests with AZ matching a broker in same AZ         |
| `client-az-miss-rate`    | Requests with configured AZ but no brokers in that AZ |
| `client-az-unaware-rate` | Requests without AZ configuration in `client.id`      |

### Interpreting Metrics

- High `client-az-hit-rate` indicates clients are correctly routing to local brokers
- High `client-az-unaware-rate` indicates clients missing the `diskless_az` pattern
- High `client-az-miss-rate` indicates AZ mismatch between clients and brokers

---

## Troubleshooting

### Common Issues

**1. Mismatched rack IDs**
- Ensure `diskless_az` value in `client.id` matches `broker.rack` exactly
- Check broker config: `grep broker.rack config/server.properties`

**2. Typo in client.id pattern**
```properties
# Wrong
client.id=my-app,diskles_az=us-east-1a   # typo in 'diskless'
client.id=my-app:diskless_az=us-east-1a  # wrong separator

# Correct
client.id=my-app,diskless_az=us-east-1a
```

**3. Missing client rack awareness**
- Ensure ALL producers and consumers have the `diskless_az` pattern
- Non-configured clients route randomly across AZs

**4. Broker not configured with rack**
- Set `broker.rack=<az>` in server.properties for each broker

### Verification

Check broker configuration:
```bash
grep broker.rack config/server.properties
```

Test rack routing:
```bash
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --producer-property client.id=test,diskless_az=us-east-1a
```

---

## References

- [KIP-1163: Diskless Core](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core) - Adds `RackId` to MetadataRequest for rack-aware broker routing (under discussion)
- [KIP-1123: Rack-aware partitioning for Kafka Producer](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1123%3A+Rack-aware+partitioning+for+Kafka+Producer) - Adds `client.rack` to producer for partition selection ([PR #19850](https://github.com/apache/kafka/pull/19850))
- [Architecture](./ARCHITECTURE.md) - Caching and rack topology
- [Performance](./PERFORMANCE.md) - Performance tuning with client rack awareness
- [FAQ](./FAQ.md) - Common questions about client rack awareness
