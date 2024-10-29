```mermaid
---
title: Inkless Design J
---
flowchart LR
    subgraph ClientsA[Client Applications]
        Producer[Producer]
        Consumer[Consumer]
    end
    subgraph Cloud[Cloud Layer]
        ObjectStorage[Object Storage]
        BatchCoordinates[Batch Coordinate Storage]
    end
    subgraph Service[Inkless Broker]
        Broker[Modified Kafka Broker]
        ObjectCache[Object Cache]
        LocalRLMM[Forwarding RLMM]
        LocalRSM[RemoteStorageManager]
        Compaction[Compaction Jobs]
    end
    subgraph Metadata[Metadata Kafka]
        Controller[Controller]
        RemoteRLMM[Terminating RLMM]
        LocalRSM[RemoteStorageManager]
        ConsumerMetadata[Group Coordinator/Offset Store]
    end
%% Data
    Producer ==> Broker <==> LocalRSM <==> ObjectStorage
    Broker ==> Consumer
    Compaction <==> LocalRSM
    Broker <==> ObjectCache
%% Batch Metadata
    Broker <--> LocalRLMM <--> RemoteRLMM <--> BatchCoordinates
    Compaction <--> LocalRLMM
%% Consumer Offsets
    Consumer --> ConsumerMetadata
%% Cluster Membership
    Broker --> Controller
```

This architecture makes use of request forwarding from the pluggable RLMM on the Inkless Broker to another pluggable RLMM on the metadata Kafka cluster.

The RemoteLogMetadataManager and RemoteStorageManager interfaces are extended or replaced with new interfaces allowing for write-before-commit and multi-partition batching.

The RemoteStorageManager calls are always completed on the local machine.
Requests must be made or redirected to the intended machine before calling.

```mermaid
---
title: Produce Flow
---
sequenceDiagram
    participant Producer
    box Inkless Broker 0
        participant InklessBroker0 as Inkless Broker
        participant RSM as RemoteStorageManager
        participant ObjectStorage as Object Storage
        participant InklessPlugin0 as Forwarding RLMM
    end
    box Metadata Broker
        participant MetadataBroker as Metadata Broker
        participant MetadataPlugin as Terminating RLMM
        participant CoordinateStorage as Coordinate Storage
    end
    Producer->>InklessBroker0: Produce Request
    InklessBroker0->>RSM: upload(batches)
    RSM->>ObjectStorage: put bytes
    ObjectStorage->>RSM: success
    RSM->>InklessBroker0: batch coordinates
    InklessBroker0->>InklessPlugin0: commit(batch coordinates)
    InklessPlugin0-->>InklessBroker0: must forward
    InklessBroker0->>MetadataBroker: Commit
    MetadataBroker->>MetadataPlugin: commit(batch coordinates)
    MetadataPlugin->>CoordinateStorage: CAS assign offsets
    CoordinateStorage->>MetadataPlugin: success
    MetadataPlugin->>MetadataBroker: batch offsets
    MetadataBroker->>InklessBroker0: batch offsets
    InklessBroker0->>Producer: Produce Response
```

The produce flow is the most straightforward, because it doesn't involve multiple inkless brokers.

```mermaid
---
title: Fetch Flow
---
sequenceDiagram
    participant Consumer
    box Inkless Broker 0
        participant InklessBroker0 as Inkless Broker
        participant InklessPlugin0 as Forwarding RLMM
    end
    box Inkless Broker 1
        participant InklessBroker1 as Replica Broker
        participant InklessPlugin1 as Forwarding RLMM
        participant RSM as RemoteStorageManager
        participant ObjectStorage as Object Storage
        participant ObjectCache as ObjectCache
    end
    box Metadata Broker
        participant MetadataBroker as Metadata Broker
        participant MetadataPlugin as Terminating RLMM
        participant CoordinateStorage as Coordinate Storage
    end
    Consumer->>InklessBroker0: Fetch Request
    InklessBroker0->>InklessPlugin0: find(partition, offset)
    InklessPlugin0-->>InklessBroker0: must forward
    InklessBroker0->>MetadataBroker: Find
    MetadataBroker->>MetadataPlugin: find(partition, offset)
    MetadataPlugin->>CoordinateStorage: query
    CoordinateStorage->>MetadataPlugin: result
    MetadataPlugin->>MetadataBroker: batch coordinates
    MetadataBroker->>InklessBroker0: batch coordinates
    alt Vanilla Fetch Request
        InklessBroker0->>InklessBroker1: Fetch Request
        InklessBroker1->>InklessPlugin1: find(partition, offset)
        InklessPlugin1-->>InklessBroker1: must forward
        InklessBroker1->>MetadataBroker: Find
        MetadataBroker->>MetadataPlugin: find(partition, offset)
        MetadataPlugin->>CoordinateStorage: query
        CoordinateStorage->>MetadataPlugin: result
        MetadataPlugin->>MetadataBroker: batch coordinates
        MetadataBroker->>InklessBroker1: batch coordinates
    else Extended Fetch Request
        InklessBroker0->>InklessBroker1: Fetch Request + batch coordinates
    end
    InklessBroker1->>ObjectCache: cache get
    alt 
        ObjectCache-->>InklessBroker1: cache miss
        InklessBroker1->>RSM: get(coordinates)
        RSM->>ObjectStorage: get objectid
        ObjectStorage->>RSM: bytes
        RSM->>InklessBroker1: batches
        InklessBroker1->>ObjectCache: cache put
    else
        ObjectCache-->>InklessBroker1: cache miss
        InklessBroker1->>InklessBroker1: active request in purgatory
        InklessBroker1->>ObjectCache: cache get
        ObjectCache->>InklessBroker1: cache hit
    else
        ObjectCache->>InklessBroker1: cache hit
    end
    InklessBroker1-->InklessBroker0: batches
    InklessBroker0->>Consumer: Fetch Response
```

Because a unique broker in each zone is given responsibility to perform the GET request for an object, other brokers must know what object contains the data for their fetch request.
Because multiple topic-partitions/offsets may be present in a single object, finding the object ID for a fetch request requires contacting the coordinate storage.
This means that Fetch requests will always need to query the coordinate storage to find the responsible broker.

If we use the vanilla Fetch request to perform cross-broker data requests, then the target broker will need to make a second request to find the object coordinates.
This may be good from a security/correctness perspective: each broker will get the freshest object coordinates from their assumed-safe RLMM.
But when the requests are forwarded, this will add latency to fetch requests for additional round trip to the metadata cluster.
This can be avoided if we augment the Fetch request to include the already-looked-up batch coordinates as a hint.

Each block of object storage is also placed in a local in-memory cache.
Requests for the same block of object data are deduplicated and subsequent requests are placed in purgatory.
Requests after the cache is populated can serve the cached data directly.

```mermaid
---
title: Compaction Flow
---
sequenceDiagram
    box Inkless Broker 0
        participant InklessBroker0 as Inkless Broker
        participant InklessPlugin0 as Forwarding RLMM
        participant RSM as RemoteStorageManager
        participant ObjectStorage as Object Storage
    end
    box Metadata Broker
        participant MetadataBroker as Metadata Broker
        participant MetadataPlugin as Terminating RLMM
        participant CoordinateStorage as Coordinate Storage
    end
    InklessBroker0->>InklessPlugin0: claimWork()
    InklessPlugin0-->>InklessBroker0: must forward
    InklessBroker0->>MetadataBroker: ClaimWork
    MetadataBroker->>MetadataPlugin: claimWork()
    MetadataPlugin->>CoordinateStorage: CAS mark as claimed
    activate CoordinateStorage
    CoordinateStorage->>MetadataPlugin: success
    MetadataPlugin->>MetadataBroker: work description
    MetadataBroker->>InklessBroker0: work description
    alt Compaction
        loop each input file
            InklessBroker0->>RSM: open(objectId)
            RSM->>ObjectStorage: get objectId
            ObjectStorage->>RSM: byte stream
            RSM->>InklessBroker0: batch iterator
        end
        loop each output file
            InklessBroker0->>RSM: open(objectId)
            RSM->>InklessBroker0: writable stream
        end
        loop first pass
            RSM->>InklessBroker0: Read input
        end
        loop second pass
            RSM->>InklessBroker0: Read input
            InklessBroker0->>RSM: Append to output file
        end
        loop each output file
            InklessBroker0->>RSM: commit(objectId)
            RSM->>ObjectStorage: put objectId
            ObjectStorage->>RSM: success
            RSM->>InklessBroker0: success
        end
    else Deletion
        loop each input file
            InklessBroker0->>RSM: delete(objectId)
            RSM->>ObjectStorage: delete objectId
            ObjectStorage->>RSM: success
            RSM->>InklessBroker0: success
        end
    end
    InklessBroker0->>InklessPlugin0: finishWork(batches)
    InklessPlugin0-->>InklessBroker0: must forward
    InklessBroker0->>MetadataBroker: FinishWork
    MetadataBroker->>MetadataPlugin: finishWork(batches)
    MetadataPlugin->>CoordinateStorage: update batch coordinates
    CoordinateStorage->>MetadataPlugin: success
    MetadataPlugin->>CoordinateStorage: CAS mark as finished
    deactivate CoordinateStorage
    CoordinateStorage->>MetadataPlugin: success
    MetadataPlugin->>MetadataBroker: success
    MetadataBroker->>InklessBroker0: success
```

Abstractions for compaction are delicate: The file needs to be structured to allow easy iteration over the records, but without potentially having to share a Java representation like MemoryRecords/FileRecords/etc.
Compaction could also take full responsibility for the data format, and the RSM could provide byte-level data and iterators.
