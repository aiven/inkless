
# Inkless Architecture
```mermaid
---
title: High Level
---
flowchart LR
    subgraph ClientsA[Client Applications]
        Producer[Producer]
        Consumer[Consumer]
    end
    subgraph Service[Broker]
        Writer[Writer]
        Reader[Reader]
        Merger[FileMerger]
        ObjectCache[Object Cache]
    end
    subgraph Cloud[Object Storage]
        ObjectStorage[Object Storage]
    end
    subgraph Metadata[Batch Coordinate Storage]
        BatchIndex[SQL Database]
    end
    Producer == ProduceRequest ==> Writer ==> ObjectStorage & ObjectCache
    ObjectStorage <==> Merger
    Reader -- FindBatches --> BatchIndex
    ObjectStorage & ObjectCache ==> Reader == FetchRequest ==> Consumer
    Writer -- CommitBatches --> BatchIndex
    Merger -- CommitMerge --> BatchIndex 
```
Data from ProduceRequests is written to object storage without assigning offsets.
Multiple ProduceRequests from multiple clients may be combined together into a single object to reduce the number of object writes.
The batch metadata (topic, partition, etc.) and location of the data (object ID & extent) are sent to a SQL database to be committed in a linear order.
The data is opportunistically cached by the writer.

Consumer FetchRequests are served by querying the SQL database to find upcoming batch metadata for the requested partitions. 
Data is first queried from the Object Cache, and will be returned if it was requested recently by another Consumer.
If the cache misses, the object is read from object storage and then populated in the cache.
Offsets for the returned data are inserted from the batch metadata. 

A background job in the brokers processes recently added objects, and merges multiple objects together into larger objects.
This improves the locality of data in each partition and reduces read amplification caused when multiple partitions are stored together.

# Deployment Model & Data Flow
```mermaid
---
title: Deployment Model & Data Flow
---
flowchart LR
    subgraph AZ0[Zone 0]
        Producer[Producer]
        Broker00[Broker-0]
    end
    subgraph AZ1[Zone 1]
        Consumer[Consumer]
        Broker10[Broker-1]
    end
    ObjectStorage[Object Storage]
    BatchIndex[SQL Database]
    
    Producer ==> Broker00 == PUT ==> ObjectStorage
    Broker00 -- CommitBatches --> BatchIndex
    
    ObjectStorage == GET ==> Broker10 ==> Consumer
    BatchIndex -- FindBatches --> Broker10
```

Inkless is designed to be deployed in a non-uniform network environment, where there is a cost incentive for keeping data transfers local.
A single cluster which is deployed in multiple zones may produce in one zone and consume in another, while:

* Preserving global order consistency
* Durably storing data and metadata
* Avoiding cross-zone data transfers
* Serving multiple consumers at a low marginal cost

# Caching
```mermaid
---
title: Multi-Rack Zone Topology
---
flowchart TB
    subgraph AZ0[Zone 0]
        Broker00[Broker-0<br/>Local Cache]
        Broker01[Broker-1<br/>Local Cache]
        Clients0[Clients]
    end
    subgraph AZ1[Zone 1]
        Broker10[Broker-2<br/>Local Cache]
        Broker11[Broker-3<br/>Local Cache]
        Clients1[Clients]
    end
    subgraph AZ2[Zone 2]
        Broker20[Broker-4<br/>Local Cache]
        Broker21[Broker-5<br/>Local Cache]
        Clients2[Clients]
    end
    ObjectStorage[Object Storage]
    Broker00 & Broker01 <== Object Requests ==> ObjectStorage
    Broker10 & Broker11 <== Object Requests ==> ObjectStorage
    Broker20 & Broker21 <== Object Requests ==> ObjectStorage
    Broker00 & Broker01 <== Kafka Requests ==> Clients0
    Broker10 & Broker11 <== Kafka Requests ==> Clients1
    Broker20 & Broker21 <== Kafka Requests ==> Clients2
```

Each broker maintains its own **local in-memory cache (Caffeine)**. Cache locality is achieved through:

1. **AZ-aware metadata routing**: Clients configured with `diskless_az` in their `client.id` receive metadata directing them to brokers in their AZ
2. **Deterministic partition assignment**: A partition always maps to the same broker within an AZ (via hash), ensuring consistent cache hits

This approach provides per-AZ cache locality without inter-broker cache coordination overhead. Cached data is not shared between brokers or across racks.

```mermaid
---
title: Producer Write Caching
---
flowchart LR
    subgraph AZ0[Zone 0]
        Broker00[Broker-0<br/>Local Cache]
        Producer00[Producer-0]
        Consumer00[Consumer-0]
    end
    ObjectStorage[Object Storage]

    Producer00 == Produce ==> Broker00
    Broker00 == Fetch ==> Consumer00
    Broker00 == PUT =====> ObjectStorage
```

Data written by producers is stored in the broker's local cache.
Consumers routed to the same broker (via deterministic partition assignment) can fetch from this cache without requiring object storage reads.

```mermaid
---
title: Consumer Caching
---
flowchart LR
    subgraph AZ0[Zone 0]
        Broker00[Broker-0<br/>Local Cache]
        Consumer00[Consumer-0]
        Consumer01[Consumer-1]
    end
    ObjectStorage[Object Storage]

    Broker00 == Fetch ====> Consumer00
    Broker00 == Fetch ====> Consumer01
    ObjectStorage == GET ==> Broker00
```

When object data is necessary to serve a request, the broker first checks its local cache.
If a cache miss occurs (Consumer-0's first request for that data), the broker fetches from object storage and populates its local cache.
Subsequent requests for the same data (Consumer-1) are served from the local cache with lower latency.