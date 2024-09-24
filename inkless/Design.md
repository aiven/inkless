```mermaid
---
title: Warpstream Logical Architecture
---
flowchart LR
    subgraph ClientsA[Client Applications]
        Producer[Producer]
        Admin[Admin]
    end
    subgraph ClientsB[Client Applications]
        Consumer[Consumer]
    end
    subgraph Cloud[Cloud Layer]
        Environment[Environment]
        WriteStorage[Write-Optimized Storage]
        ReadStorage[Read-Optimized Storage]
    end
    subgraph Service[Service Layer]
        AdminProxy([Admin Proxy])
        ProducerProxy[Producer Proxy]
        ConsumerProxy[Consumer Proxy]
        ObjectCache[Object Cache]
        Compaction[Compaction Jobs]
        Retention([Retention Jobs])
    end
    subgraph Metadata[Metadata Layer]
        ClientMetadata([Client Metadata])
        BatchIndex([Batch Order Consensus & Index])
        ConsumerMetadata([Group Coordinator/Offset Store])
    end
    subgraph ControlPlane[Control Plane]
        ConfigStore([Configuration Store])
    end
    %% Data
    Producer ==> ProducerProxy ==> WriteStorage ==> Compaction ==> ReadStorage
    WriteStorage == Low-Latency ==> ObjectCache
    ReadStorage == High-Latency ==> ObjectCache
    ObjectCache ==> ConsumerProxy ==> Consumer
    %% Batch Metadata
    ProducerProxy --> BatchIndex --> ConsumerProxy
    AdminProxy <-.-> BatchIndex
    BatchIndex <--> Retention & Compaction
    Retention --> WriteStorage & ReadStorage
    %% Consumer Offsets
    AdminProxy <-.-> ConsumerMetadata 
    ConsumerMetadata <--> ConsumerProxy
    %% Client Metadata
    ClientMetadata -.-> Service
    %% Configuration
    Admin <-.-> AdminProxy
    AdminProxy <-.-> ConfigStore
    ConfigStore -.-> Service & Metadata
    Environment -.-> Service
    %% Layout
    Compaction ~~~ ConsumerMetadata
```
* Thick arrows are data flow, proportional to total throughput
* Thin arrows are batch metadata, proportional to producer/consumer batches
* Dotted arrows are control-plane operations, proportional to infrastructure changes
* Rectangular nodes are explicitly mentioned in public documentation
* Rounded nodes are inferred to exist but are not mentioned in public documentation

```mermaid
---
title: Producing Data
---
sequenceDiagram
    autonumber
    participant P0 as Producer 0
    participant P1 as Producer 1
    participant B as Broker
    participant O as Object Storage
    participant M as Metastore
    P0->>B: Produce partition 0
    P1->>B: Produce partition 1
    B->>O: Write data A
    O-->>B: Ack data A
    B->>M: Write metadata A
    M-->>B: Ack metadata A
    B-->>P0: Ack partition 0
    B-->>P1: Ack partition 1
```
```mermaid
---
title: Consuming low-latency data
---
sequenceDiagram
    autonumber
    participant C0 as Consumer 0
    participant B as Broker
    participant O as Object Storage
    participant M as Metastore
    C0->>B: Fetch partition 0
    B->>M: Query metadata
    M-->>B: Return metadata A,B
    B->>O: Query data A
    B->>O: Query data B
    O-->>B: Return data A
    O-->>B: Return data B
    B-->>C0: Data partition 0
```

```mermaid
---
title: Consuming read-optimized data
---
sequenceDiagram
    autonumber
    participant C0 as Consumer 0
    participant B as Broker
    participant O as Object Storage
    participant M as Metastore
    C0->>B: Fetch partition 0
    B->>M: Query metadata
    M-->>B: Return metadata
    B->>O: Query data 0
    O-->>B: Return data 0
    B-->>C0: Data partition 0
    C0->>B: Fetch partition 0
    B-->>C0: Data partition 0
```
