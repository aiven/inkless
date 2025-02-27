
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
title: Multi-Rack Zone Toplogy
---
flowchart TB
    subgraph AZ0[Zone 0]
        subgraph C0[Cache 0]
            Broker00[Broker-0]
            Broker01[Broker-1]
        end
        Clients0[Clients]
    end
    subgraph AZ1[Zone 1]
        subgraph C1[Cache 1]
            Broker10[Broker-2]
            Broker11[Broker-3]
        end
        Clients1[Clients]
    end
    subgraph AZ2[Zone 2]
        subgraph C2[Cache 2]
            Broker20[Broker-4]
            Broker21[Broker-5]
        end
        Clients2[Clients]
    end
    ObjectStorage[Object Storage]
    Broker01 & Broker11 & Broker21 <== Object Requests ===> ObjectStorage
    Broker00 <== Cache Traffic ==> Broker01
    Broker10 <== Cache Traffic ==> Broker11
    Broker20 <== Cache Traffic ==> Broker21
    Broker01 <== Kafka Requests ==> Clients0
    Broker11 <== Kafka Requests ==> Clients1
    Broker21 <== Kafka Requests ==> Clients2
```

A single cluster may have a single cache if `broker.rack` is unset.
If `broker.rack` is set, brokers will join rack-specific caches, which are intended to align with network zones.
Brokers each contain an embedded distributed cache, and brokers become members of this cache in their local rack.
Each rack independently serves Kafka requests, and makes Object requests. Cached data is not shared between racks.

```mermaid
---
title: Producer Write Caching
---
flowchart LR
    subgraph AZ0[Zone 0]
        subgraph C0[Cache 0]
            Broker00[Broker-0]
            Broker01[Broker-1]
            Broker02[Broker-2]
        end
        Producer00[Producer-0]
        Consumer00[Consumer-0]
    end
    ObjectStorage[Object Storage]

    Producer00 == Produce ==> Broker00 == put ==> Broker01
    Broker01 == get ==> Broker02 == Fetch ==> Consumer00
    Broker00 == PUT =====> ObjectStorage
```

Data written by producers to object storage is also written to the cache.
Consumers in the local zone may fetch from this cache without requiring reading the object from storage.

```mermaid
---
title: Consumer Caching
---
flowchart LR
    subgraph AZ0[Zone 0]
        subgraph C0[Cache 0]
            Broker00[Broker-0]
            Broker01[Broker-1]
            Broker02[Broker-2]
        end
        Consumer00[Consumer-0]
        Consumer01[Consumer-1]
    end
    ObjectStorage[Object Storage]

    Broker00 == put ==> Broker01
    Broker01 == get ==> Broker02 == Fetch ==> Consumer01
    Broker00 == Fetch ====> Consumer00
    ObjectStorage == GET ==> Broker00
```

When object data is necessary to serve a request, the data is preferentially served from the rack's cache.
If a cache miss occurs (Consumer-0) data is fetched from the object storage.
After the cache miss, the data is populated in the cache in one member in the rack.
If a cache hit occurs (Consumer-1), the data is loaded remotely from another member of the cache.