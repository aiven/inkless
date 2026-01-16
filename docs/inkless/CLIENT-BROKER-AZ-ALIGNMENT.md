# Client-Broker AZ Alignment

This document describes how Inkless enables **client rack awareness** to route client requests to brokers within the same availability zone (AZ), minimizing cross-AZ data transfer costs in cloud deployments.

## Summary

### Current State (Inkless 0.32+)

- ✅ Client rack awareness implemented via `client.id` pattern
- ✅ Works with any Kafka client version
- ✅ Eliminates most cross-AZ costs for diskless topics
- ⚠️ Non-standard approach, requires user awareness

### Future State (KIP-1123)

- ⏳ KIP-1123: Adds `client.rack` to producer (approved)
- ⏳ Standard `client.rack` configuration for both producers and consumers
- ⏳ Inkless will support both approaches for backward compatibility

### Migration

- Phase 1: Use `client.id` pattern (now)
- Phase 2: Support both mechanisms (when KIP-1123 is available)

**Recommendation:** Start using `client.id=app,diskless_az=<rack>` pattern now for immediate cost savings. When KIP-1123 becomes available in Kafka clients, consider migrating to `client.rack` for a standard configuration.

## Table of Contents

- [Summary](#summary)
- [Overview](#overview)
- [Relationship to Kafka Rack Awareness](#relationship-to-kafka-rack-awareness)
- [Current Implementation](#current-implementation)
- [Future Implementation (KIP-1123)](#future-implementation-kip-1123)
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
2. **Consumers** fetch data from brokers in their local AZ (preferring local cached data)
3. **Brokers** store data in shared object storage accessible from all AZs
4. **Caching** is AZ-local, with each AZ maintaining its own cache

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

| Feature                    | Classic Topics                     | Diskless Topics (Inkless)                               |
|----------------------------|------------------------------------|---------------------------------------------------------|
| Broker rack config         | `broker.rack`                      | `broker.rack` (same)                                    |
| Consumer rack-aware fetch  | `client.rack` (follower fetching)  | `client.id` pattern (current) / `client.rack` (future)  |
| Producer rack-aware routing| `client.rack` (KIP-1123, approved) | `client.id` pattern (current) / `client.rack` (future)  |
| Mechanism                  | Fetch from nearest replica         | Route to broker in same AZ                              |

**Key difference**: In classic Kafka, rack awareness helps consumers fetch from nearby *replicas*. In Inkless, client rack awareness routes requests to nearby *brokers* since any broker can serve any diskless partition (data is in object storage).

**Note on KIP-1123**: [KIP-1123](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1123%3A+Rack-aware+partitioning+for+Kafka+Producer) has been approved ([PR #19850](https://github.com/apache/kafka/pull/19850)). This adds `client.rack` support to the producer, enabling rack-aware partition selection for keyless records in classic topics. Once available in Kafka clients, Inkless will support `client.rack` as an alternative to the current `client.id` pattern.

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

1. Producer sends Produce request with `client.id=my-producer,diskless_az=us-east-1a`
2. Broker extracts AZ: `us-east-1a`
3. Broker compares with its own `broker.rack` configuration
4. If match, broker accepts and processes the request locally
5. If no match, broker can still process but with cross-AZ costs

**Metadata Response Behavior:**
- When a client requests metadata, the broker identifies the client's AZ from the `client.id`
- The broker returns metadata with brokers in the same rack prioritized
- Producers route requests to brokers in the same rack

#### Consumer Flow

1. Consumer sends Fetch request with `client.id=my-consumer,diskless_az=us-east-1a`
2. Broker extracts AZ: `us-east-1a`
3. Broker uses existing follower fetching mechanism (PreferredReadReplica)
4. Broker returns metadata pointing to replicas in the consumer's rack
5. Consumer fetches from local rack, accessing AZ-local cache

**Key Points:**
- If no brokers exist in the client's rack, brokers from other racks are used as fallback
- Cache is AZ-local; each rack has its own distributed cache

#### Broker Selection Algorithm

The broker selection for diskless partitions uses a **deterministic hash-based algorithm**:

1. A hash is computed from the topic ID and partition index using MurmurHash2
2. The hash determines which broker (from the eligible set) handles each partition
3. This ensures consistent routing across metadata requests

```
broker_index = abs(murmur2(topicId + "-" + partitionIndex)) % eligible_brokers.size()
```

**Benefits of deterministic selection:**
- **Consistency**: The same partition always routes to the same broker (within an AZ) across restarts
- **Cache efficiency**: Clients consistently connect to the same broker, improving cache hit rates
- **Load distribution**: Partitions are evenly distributed across brokers using the hash

**Behavior during broker changes:**
- When a broker joins or leaves, some partitions may be reassigned to different brokers
- Clients will receive updated metadata and reconnect to the new broker
- Cache misses may occur temporarily until the new broker warms up

#### Diskless Topics vs Classic Topics: ISR Semantics

Diskless topics have fundamentally different ISR (In-Sync Replicas) semantics compared to classic topics:

| Aspect                     | Classic Topics                            | Diskless Topics                                |
|----------------------------|-------------------------------------------|------------------------------------------------|
| Replication                | Brokers replicate data to followers       | No inter-broker replication; object storage    |
| ISR meaning                | Replicas caught up with the leader        | All alive brokers are effectively "in-sync"    |
| Leader election            | Based on ISR membership                   | Any broker can serve any partition             |
| Under-replicated partitions| Possible when followers fall behind       | Not applicable; no replication to fall behind  |

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

## Future Implementation (KIP-1123)

[KIP-1123: Rack-aware partitioning for Kafka Producer](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1123%3A+Rack-aware+partitioning+for+Kafka+Producer) has been **approved** ([PR #19850](https://github.com/apache/kafka/pull/19850)).

This KIP adds the `client.rack` configuration to the Kafka producer, which was previously only available for consumers. Once available in Kafka clients, Inkless will support `client.rack` as an alternative to the current `client.id` pattern, enabling a standard configuration for client rack awareness.

### Configuration

```properties
# Standard configuration for producers and consumers
client.rack=us-east-1a
```

### Benefits

- **Consistent configuration**: Producers and consumers use the same `client.rack` property
- **No workarounds**: No need to embed rack information in `client.id`
- **Standard approach**: Aligns with upstream Apache Kafka

### How It Will Work

1. Producer/Consumer configured with `client.rack=us-east-1a`
2. Inkless broker receives the rack information via metadata requests
3. Broker routes requests to brokers in the same AZ (same as current implementation)
4. Falls back to other AZs if no local brokers are available

### Differences from Current Implementation

| Aspect                | Current (client.id pattern)              | Future (client.rack)                |
|-----------------------|------------------------------------------|-------------------------------------|
| Configuration         | `client.id=app,diskless_az=us-east-1a`   | `client.rack=us-east-1a`            |
| Producer support      | Via `client.id` pattern                  | Native via `client.rack` (KIP-1123) |
| Consumer support      | Via `client.id` pattern                  | Native via `client.rack`            |
| Validation            | No validation of rack IDs                | Potential for validation            |
| Monitoring            | Parse client.id in logs/metrics          | Standard property                   |
| Client support        | Requires user awareness                  | Built into Kafka clients            |
| Backward compatibility| N/A                                      | Inkless will support both           |

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

**Status:** Planned for when KIP-1123 is available in Kafka clients

**Action:** Support both mechanisms simultaneously

```properties
# Option A: Current approach (for older clients or backward compatibility)
client.id=my-app,diskless_az=us-east-1a

# Option B: Standard approach (recommended for new deployments)
client.rack=us-east-1a
```

**Behavior:**
- If `client.rack` is set, use it (preferred)
- If only `client.id` pattern is detected, use current mechanism
- If both are set, `client.rack` takes precedence

**Why support both:**
- Enables gradual migration without breaking existing clients
- Allows mixed deployments with clients on different Kafka versions
- Provides flexibility for environments where client upgrades are constrained

**Timeline:** When KIP-1123 is released in Apache Kafka

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
┌─────────────────────────────────────────────────────────────┐
│ AWS Region: us-east-1                                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────┐  │
│  │ us-east-1a       │  │ us-east-1b       │  │ us-east-1c│  │
│  │                  │  │                  │  │           │  │
│  │ Broker 0         │  │ Broker 2         │  │ Broker 4  │  │
│  │ broker.rack=     │  │ broker.rack=     │  │ broker.   │  │
│  │   us-east-1a     │  │   us-east-1b     │  │   rack=   │  │
│  │                  │  │                  │  │   us-east │  │
│  │ Broker 1         │  │ Broker 3         │  │   -1c     │  │
│  │ broker.rack=     │  │ broker.rack=     │  │           │  │
│  │   us-east-1a     │  │   us-east-1b     │  │ Broker 5  │  │
│  │                  │  │                  │  │ broker.   │  │
│  │ Producers        │  │ Producers        │  │   rack=   │  │
│  │ client.id=       │  │ client.id=       │  │   us-east │  │
│  │   app,diskless_  │  │   app,diskless_  │  │   -1c     │  │
│  │   az=us-east-1a  │  │   az=us-east-1b  │  │           │  │
│  │                  │  │                  │  │ Producers │  │
│  │ Consumers        │  │ Consumers        │  │ client.id │  │
│  │ client.id=       │  │ client.id=       │  │   =app,   │  │
│  │   app,diskless_  │  │   app,diskless_  │  │   diskles │  │
│  │   az=us-east-1a  │  │   az=us-east-1b  │  │   s_az=us │  │
│  │                  │  │                  │  │   -east-1c│  │
│  │                  │  │                  │  │           │  │
│  │ Cache (local)    │  │ Cache (local)    │  │ Cache     │  │
│  │                  │  │                  │  │ (local)   │  │
│  └──────────────────┘  └──────────────────┘  └───────────┘  │
│            │                      │                  │      │
│            └───────────┬──────────┴──────────────────┘      │
│                        │                                    │
│                        ▼                                    │
│              ┌──────────────────┐                           │
│              │ S3 Bucket        │                           │
│              │ (multi-AZ)       │                           │
│              └──────────────────┘                           │
│                        ▲                                    │
│                        │                                    │
│              ┌─────────┴─────────┐                          │
│              │ PostgreSQL        │                          │
│              │ (Batch Coord.)    │                          │
│              │ (multi-AZ)        │                          │
│              └───────────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

**Configuration summary:**
- 6 brokers across 3 AZs (2 per AZ)
- Each broker has `broker.rack` set to its AZ
- Producers/consumers in each AZ use matching `diskless_az` in client.id
- Object storage (S3) is multi-AZ accessible
- PostgreSQL can be single-AZ or multi-AZ depending on setup

---

## Monitoring

Inkless exposes JMX metrics to monitor client rack awareness effectiveness:

| Metric Name              | Description                                           |
|--------------------------|-------------------------------------------------------|
| `client-az-hit-rate`     | Requests with AZ matching a broker in same AZ         |
| `client-az-miss-rate`    | Requests with configured AZ but no brokers in that AZ |
| `client-az-unaware-rate` | Requests without AZ configuration in `client.id`      |

**Interpreting metrics:**
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

- [KIP-1123: Rack-aware partitioning for Kafka Producer](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1123%3A+Rack-aware+partitioning+for+Kafka+Producer) - Adds `client.rack` to producer ([PR #19850](https://github.com/apache/kafka/pull/19850))
- [Architecture](./ARCHITECTURE.md) - Caching and rack topology
- [Performance](./PERFORMANCE.md) - Performance tuning with client rack awareness
- [FAQ](./FAQ.md) - Common questions about client rack awareness
