```mermaid
---
title: Inkless Design B
---
flowchart LR
    subgraph ClientsA[Client Applications]
        Producer[Producer]
        Consumer[Consumer]
    end
    subgraph Cloud[Cloud Layer]
        ObjectStorage[Object Storage]
    end
    subgraph Service[Inkless Broker]
        Broker[Tweaked ReplicaManager]
        BatchProcessor["Batch Processor(*)"]
        CacheManager["Cache Manager(*)"]
    end
    subgraph Metadata["RemoteLogMetadataManager(*)"]
        PartitionOwnership[Partition Ownership]
        ObjectStorageCoordinates[Object Storage Coordinates]
    end
    subgraph ObjStorageOptimizer[Object Storage Optimizer]
        Optimizer[Optimizer]
    end
%% Produce Data
    Producer ==> Broker ==> BatchProcessor ==>CacheManager
    BatchProcessor <==> ObjectStorage
    BatchProcessor <==> ObjectStorageCoordinates
%% Consume Data
    Consumer -.-> Broker <-.-> PartitionOwnership 
    Broker <-.-> ObjectStorageCoordinates
    Broker <-.-> CacheManager
    Broker <-.-> ObjectStorage
%% Optimizer
    Optimizer <--> ObjectStorage
    Optimizer <--> ObjectStorageCoordinates
```

NOTE
> (*) modules may already exist from Tiered Storage components:
> - Batch Processor could be included within the RemoteLogManager
> - Cache Manager: There is some caching already on the TS plugin -- maybe this should be moved to within RemoteLogManager as well?
> - Metadata Store is already available from RemoteLogMetadataManager and could be extended for the new architecture.

This architecture relies on a modified broker and enhances it with a MetadataStore, a CacheManager and a BatchProcessor.
The goal of the CacheManager is to reduce the amount of object storage calls done. It's a limited sized circular cache.
The goal of the batch processor is to group messages to make the upload in object storage efficient in terms of price.
It stores the data in the right object storage and persists its coordinates to the Metadata Store.
This architecture relies on having soft leaders and replicas of partition. Only leaders write to object storage.
The cluster must direct producers to the right broker.

Consumers should be able to read from any broker. If data is in the CacheManager, then return it. If not, if the broker 
is a leader of the partition, it should fetch a batch from Object Storage and populate its cache. Then return the data.
If the broker is not the leader of the partition, it needs to query the MetadataStore and find out who the owner is, then
it can request a batch containing that offset. The leader broker follows the previously described flow and returns a batch.
The follower then updates its cache and returns the data.

If we need to optimize already stored data, this is something that can be done with a scheduler. Data can be read,
reorganized and then update the coordinates.

Brokers joining and leaving the cluster would talk to the MetadataStore to update the PartitionOwnership component.
This component should try to spread the load of writes evenly to maximize the use of the cluster.

```mermaid
---
title: Producing Data
---
sequenceDiagram
    autonumber
    participant P0 as Producer 0
    participant P1 as Producer 1
    participant B as Broker
    participant BP as BatchProcessor
    participant C as CacheManager
    participant O as Object Storage
    participant M as MetaDataStore
    P0->>B: Produce partition 0
    P1->>B: Produce partition 1
    B->>BP: Add to Batch
    activate BP
    BP->>C: Add to Cache
    Note over C: Keeps track of the lowest and highest offset per topic/partition
    alt batch is full
    BP-->>B: full
    else batch is not full
    BP-->>B: not-full
    B-->>B: collect Futures for requests
    end
    deactivate BP
    B->>BP: push batch
    activate BP
    BP->>O: Write data A
    activate O
    O-->>BP: Ack data A
    deactivate O
    BP->>+M: Write metadata A
    M-->>-BP: Ack metadata A
    BP-->>B: Metadata A
    deactivate BP
    B->>B: complete Futures for requests
    activate B
    B-->>P0: Ack partition 0
    B-->>P1: Ack partition 1
    deactivate B
```
```mermaid
---
title: Consuming Data from "leader"
---
sequenceDiagram
    autonumber
    participant C0 as Consumer 0
    participant B as Broker
    participant CM as CacheManager
    participant O as Object Storage
    participant M as MetaDataStore
    C0->>B: Consume partition 0
    activate B
    Note over B: Checks is leader for this partition
    B->>+CM: Get data by offset
    CM-->>-B: Optional[Data]
    alt Data is there
    B-->>C0: Msg
    else Data is not there
    B->>+M: Where is my data?
    M-->>-B: Object Storage coordinates
    B-->+O: fetch
    O-->>-B: Data A
    B->>CM: Add Data A to Rotation
    B-->>C0: Msg
    end
    deactivate B    
```
```mermaid
---
title: Consuming Data from "replica"
---
sequenceDiagram
    autonumber
    participant C0 as Consumer 0
    participant B as ReplicaBroker
    participant OB as OwnerBroker
    participant CM as CacheManager
    participant M as MetaDataStore
    C0->>B: Consume partition 0
    activate B
    Note over B: Checks is not leader for this partition
    B->>+CM: Get data by offset
    CM-->>-B: Optional[Data]
    alt Data is there
    B-->>C0: Msg
    else Data is not there
    B->>+M: Who owns this partition?
    M-->>-B: BrokerID
    B->>+OB: Fetch batch (topic, partition, offset)
    OB-->>-B: Data A
    B->>CM: Add Data A to Rotation
    B-->>C0: Msg
    end
    deactivate B
```
