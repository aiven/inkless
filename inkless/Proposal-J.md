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
