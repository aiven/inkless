# Inkless Glossary

This glossary defines Inkless-specific terms and concepts. For general Apache Kafka terminology, see the [official Kafka documentation](https://kafka.apache.org/documentation/).

---

## A

**Availability Zone (AZ)**
A distinct location within a cloud region with independent infrastructure. Inkless uses AZ-aware routing to minimize cross-AZ data transfer costs. See also: [Rack](#rack).

---

## B

**Batch**
In Inkless, a batch refers to a group of producer records from the same topic partition that are written together as part of an object. Batches are the unit of coordination tracked by the Batch Coordinator.

**Batch Coordinator**
A centralized metadata service that manages batch coordinates and ensures total ordering of messages within partitions. Currently implemented using PostgreSQL.

**Batch Coordinates**
Metadata that describes the location of batches in object storage, including:
- Topic and partition
- Object ID and byte range
- Offset range
- Timestamp information

**Batch Coordinate Cache**
A local in-memory cache (Caffeine) on each broker that stores the metadata of recently produced batches. It serves as a metadata cache for improving the performance of fetches. Configured via `inkless.consume.batch.coordinate.cache.*` properties.

**Batch Index**
Another term for the Batch Coordinator's storage layer. In the PostgreSQL implementation, this refers to the database tables that store batch coordinates.

---

## C

**Classic Topics**
Standard Apache Kafka topics that store data on local broker disks with replication. Contrasted with diskless topics.

**Cold Path**
The fetch processing path for lagging consumer requests (data older than the threshold). Bypasses cache to avoid evicting hot data, uses a dedicated bounded executor pool with optional rate limiting, and a separate storage client for resource isolation. See also: [Hot Path](#hot-path).

**Control Plane**
The metadata management layer in Inkless, responsible for coordinating batch commits and lookups. See also: [Batch Coordinator](#batch-coordinator).

---

## D

**Diskless Topics**
Topics configured with `diskless.enable=true` that store data in object storage instead of on local broker disks. The core feature of Inkless.

**`diskless_az`**
A marker embedded in the `client.id` configuration to communicate the client's availability zone to brokers. Format: `client.id=<app>,diskless_az=<rack>` where `<rack>` matches the broker's `broker.rack` value. See [CLIENT-BROKER-AZ-ALIGNMENT.md](./CLIENT-BROKER-AZ-ALIGNMENT.md) for details.

---

## F

**File Merger**
A background process running on brokers that consolidates multiple small objects into larger, optimized objects. This improves read performance for lagging consumers by reducing the number of object storage requests and improving partition data locality.

---

## H

**Hot Path**
The fetch processing path for recent data requests. Uses the object cache for fast repeated access, with a dedicated executor pool and no rate limiting. See also: [Cold Path](#cold-path).

---

## I

**Inkless**
The name of this Apache Kafka fork implementing diskless topics. Named to reflect the reduced reliance on local disk storage ("ink" = write to disk).

---

## L

**Lagging Consumer** / **Trailing Consumer**
A consumer reading older data (based on batch timestamp age, not consumer lag). These terms are used interchangeably in Kafka literature; Inkless uses "lagging consumer" in configuration names (`inkless.fetch.lagging.consumer.*`).

**Leaderless**
Unlike traditional Kafka where a partition leader handles all writes and most reads (with followers only fetching for replication), Inkless diskless topics have no designated partition leader. Any broker can serve produce and fetch requests for any partition. Metadata coordination still requires the Batch Coordinator for ordering.

---

## M

**Mixed Cluster**
A Kafka cluster containing both classic topics (using local disk storage) and diskless topics (using object storage).

---

## O

**Object**
A unit of data stored in object storage (e.g., S3, GCS, Azure Blob). In Inkless, objects contain batches from multiple partitions.

**Object Cache**
A local in-memory cache (Caffeine) on each broker that stores recently accessed objects. Combined with deterministic partition-to-broker assignment and AZ-aware routing, this achieves per-AZ cache locality. Configured via `inkless.consume.cache.*` properties. See also: [Hot Path](#hot-path).

**Object Key**
The identifier used to locate an object in object storage. Includes a configurable prefix (`inkless.object.key.prefix`).

**Object Storage**
Cloud storage services like AWS S3, Google Cloud Storage, or Azure Blob Storage. Used by Inkless to store topic data durably and cost-effectively.

---

## P

**PostgreSQL Control Plane**
The production-ready batch coordinator implementation using PostgreSQL for durable metadata storage.

---

## R

**Rack** / **Broker Rack**
A logical grouping of brokers, typically corresponding to an availability zone. Configured via `broker.rack`. See also: [Rack Awareness](#rack-awareness) and [CLIENT-BROKER-AZ-ALIGNMENT.md](./CLIENT-BROKER-AZ-ALIGNMENT.md).

**Rack Awareness**
Configuration that enables clients (producers/consumers) and brokers to prefer operations within the same rack/AZ, minimizing cross-AZ data transfer costs. See [CLIENT-BROKER-AZ-ALIGNMENT.md](./CLIENT-BROKER-AZ-ALIGNMENT.md) for detailed implementation and configuration guide.

**Read Amplification**
Reading more data than you actually need. In Inkless, this happens because objects contain batches from multiple partitions—so fetching data for one partition may pull in data from other partitions stored in the same object.

**Read Replica**
A PostgreSQL read replica used to scale batch coordinate lookups. Configured via `inkless.control.plane.read.*` properties.

---

## S

**Storage Backend**
The object storage implementation (S3, GCS, or Azure Blob). Configured via `inkless.storage.backend.class`.

---

## T

**Tail Consumer**
A consumer reading recent data from near the "tail" (end) of the log. Tail consumers benefit from cache hits and the hot path. The opposite of a [Lagging Consumer](#lagging-consumer--trailing-consumer).

---

## W

**WAL** / **Write Ahead Log**
A temporary buffer where produced data accumulates before being written as an object to storage. Configured by:
- `inkless.produce.buffer.max.bytes` (default: 8 MiB)
- `inkless.produce.commit.interval.ms` (default: 250ms)

**WAL Segment**
A single write ahead log file that is rotated and uploaded to object storage when full or after the commit interval expires.

---

## Related Concepts

### Compared to Classic Kafka

| Classic Kafka                | Inkless Diskless Topics                                  |
|------------------------------|----------------------------------------------------------|
| Data on local disk           | Data in object storage                                   |
| Replication via ISR          | Replication via storage backend                          |
| Partition leaders            | Leaderless (any broker can serve)                        |
| Segments on disk             | Objects in storage                                       |
| ZooKeeper/KRaft for metadata | KRaft for cluster metadata + Batch Coordinator for message metadata and order |

### Compared to Tiered Storage

| Tiered Storage                                  | Inkless Diskless                        |
|-------------------------------------------------|-----------------------------------------|
| Recent data on disk, old data in object storage | All data in object storage from start   |
| Still requires local disk                       | Minimal local disk usage                |
| Replication still occurs                        | No inter-broker replication             |
| Topics are tiered over time                     | Topics are diskless from creation       |

---

## See Also

- [Architecture](./ARCHITECTURE.md) - Detailed system architecture
- [Features](./FEATURES.md) - Supported features and limitations
- [FAQ](./FAQ.md) - Frequently asked questions
- [Performance](./PERFORMANCE.md) - Performance tuning guide
- [Configs](./configs.rst) - Complete configuration reference
