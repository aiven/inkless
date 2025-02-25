
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

# Deployment Model
```mermaid
---
title: Deployment Model
---
flowchart LR
    subgraph Region[Local Region]
        direction LR
        subgraph AZ0[Zone 0]
            Producer[Producer]
            Broker00[Broker-0]
            Broker01[Broker-1]
        end
        subgraph AZ1[Zone 1]
            Consumer[Consumer]
            Broker10[Broker-2]
            Broker11[Broker-3]
        end
        ObjectStorage[Object Storage]
        BatchIndex[SQL Database]
    end
    Producer ==> Broker00 == PUT ==> ObjectStorage
    Broker00 --> BatchIndex
    Broker00 == Cache ==> Broker01
    
    ObjectStorage == GET ==> Broker10 ==> Consumer
    BatchIndex --> Broker10
    Broker10 == Cache ==> Broker11
```

Inkless is designed to be deployed in a non-uniform network environment, where there is a cost incentive for keeping data transfers local.
A single cluster which is deployed in multiple zones may produce in one zone and consume in another, while:

* Preserving global order consistency
* Durably storing data and metadata
* Avoiding cross-zone data transfers
* Serving multiple consumers at a low marginal cost
