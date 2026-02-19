
# Inkless Architecture
```mermaid
---
title: High Level
---
flowchart LR
    Producer[Producer]
    Consumer[Consumer]
    subgraph Service[Broker]
        Diskless[Diskless]
        Local[Local Segments]
        RemoteStorageManager[Remote Storage Manager]
    end
    subgraph Cloud[Object Storage]
        ObjectStorage[WAL Segments]
        TieredStorage[Tiered Segments]
    end
    Producer == ProduceRequest ==> Diskless
    Diskless <==> ObjectStorage
    Diskless == Append ==> Local 
    Diskless & Local & RemoteStorageManager == FetchRequest ==> Consumer
    Local == Copy To Remote ==> RemoteStorageManager 
    RemoteStorageManager <==> TieredStorage
```

Diskless topics primarily change how new data is written to Kafka.
Existing mechanisms for serving data are largely unaffected, including the use of Tiered Storage.

```mermaid
---
title: Comparison with Classic Topic
---
flowchart LR
    Producer[Producer]
    Consumer[Consumer]
    subgraph Broker0[Leader]
        Broker0Handler[Produce Handler]
        Broker0Local[Local Segments]
    end
    subgraph Broker1[Follower]
        ReplicaFetcher[Replica Fetcher]
        Broker1Local[Local Segments]
    end
    subgraph Broker2[Broker]
        Broker2Diskless[Diskless Writer]
    end
    subgraph Broker3[Replica]
        Broker3Diskless[Diskless Reader]
        Broker3Local[Local Segments]
    end
    subgraph Cloud[Object Storage]
        ObjectStorage[WAL Segments]
    end
    Producer == Classic Produce ==> Broker0Handler == Append ==> Broker0Local
    Broker0Local == FetchResponse ==> ReplicaFetcher == Append ==> Broker1Local
    Producer == Diskless Produce ==> Broker2Diskless
    Broker2Diskless == PUT ==> ObjectStorage == GET ==> Broker3Diskless
    Broker3Diskless == Append ==> Broker3Local
    Broker1Local & Broker3Local == FetchResponse ==> Consumer
```
Diskless topics change how ProduceRequests are translated to appends on each replica.

For Classic topics, Produce requests trigger an append to the leader's local segments.
Followers issue FetchRequests to the leader, which are served from the leader's local segments.
Followers then append this data to their local segments.

For Diskless topics, Produce requests trigger a PUT to Object Storage.
Replicas issue  GETs to the Object Storage to retrieve batch data.
Diskless then appends batches to the local segments on the replica.

# Offsets Management

```mermaid
---
title: Offsets Management
---
flowchart LR
    Producer[Producer]
    Consumer[Consumer]
    subgraph Broker1[Broker]
        BatchIndex[Diskless Coordinator<br><i>Assign Offsets</i>]
    end
    subgraph Broker2[Broker]
        Broker2Diskless[Diskless Writer<br><i>Validate Data</i>]
    end
    subgraph Broker3[Replica]
        Broker3Diskless[Diskless Reader<br><i>Inject Offsets</i>]
        Broker3Local[Local Segments]
    end
    subgraph Cloud[Object Storage]
        ObjectStorage[WAL Segments]
    end
    Producer == ProduceRequest ==> Broker2Diskless
    Broker2Diskless == Unordered<br>Batch Data ==> ObjectStorage == Unordered<br>Batch Data ==> Broker3Diskless
    Broker2Diskless -- Locally Ordered<br>Batch Coordinates --> BatchIndex -- Globally Ordered<br>Batch Coordinates --> Broker3Diskless
    Broker3Diskless == Append ==> Broker3Local
    Broker3Local == FetchResponse ==> Consumer
```

Data from ProduceRequests is written to object storage without assigning offsets.
Multiple ProduceRequests from multiple clients may be combined together into a single object to reduce the number of object writes.
The batch metadata (topic, partition, etc.) and location of the data (object ID & extent) are sent to the diskless coordinator to be committed in a linear order.

Appending to replicas is done by querying the Diskless Coordinator to find upcoming batch metadata for the requested partitions. 
For each object containing needed data, the object is read from object storage and then split by partition.
Offsets for the returned data are inserted from the batch metadata, and the data is appended to the local segments. 
Once the data lands in the local segments, it is available for consumers to read.

# Zone Topology
```mermaid
---
title: Multi-Rack Zone Toplogy
---
flowchart TB
    subgraph AZ0[Zone 0]
        Broker00[Broker-0]
        Clients0[Clients]
    end
    subgraph AZ1[Zone 1]
        Broker10[Broker-1]
        Clients1[Clients]
    end
    subgraph AZ2[Zone 2]
        Broker20[Broker-2]
        Clients2[Clients]
    end
    ObjectStorage[Object Storage]
    Broker00 & Broker10 & Broker20 <== Object Requests ===> ObjectStorage
    Broker00 <== Kafka Requests ==> Clients0
    Broker10 <== Kafka Requests ==> Clients1
    Broker20 <== Kafka Requests ==> Clients2
```

Inkless is designed to be deployed in a non-uniform network environment, where there is a cost incentive for keeping data transfers local.
A single cluster which is deployed in multiple zones may produce in one zone and consume in another, while:

* Preserving global order consistency
* Durably storing data and metadata
* Avoiding cross-zone data transfers
* Serving multiple consumers at a low marginal cost

When a cluster is deployed in multiple racks/zones, one replica should be placed in each zone for each Diskless topic.
This will permit all Kafka requests from rack-aware clients to be directed to the local zone replica, avoiding cross-zone charges for client traffic.
Each broker will request objects from object storage independently, and these requests for object data will also not incur cross-zone charges

# WAL Segment Layout
```mermaid
block-beta
    columns 1
    block
        columns 1
        Title["WAL Segment Header"]
        columns 1
        T0P0B0["Topic 0 Partition 0 Batch 0"]
        space
        T0P0BN["Topic 0 Partition 0 Batch N_0"]
        T0P0B0 --> T0P0BN
        T0PPB0["Topic 0 Partition 1 Batch 0"]
        space
        T0PPBN["Topic 0 Partition 1 Batch N_1"]
        T0PPB0 --> T0PPBN
        TMPPB0["Topic M Partition 0 Batch 0"]
        space
        TMPPBN["Topic M Partition P Batch N_2"]
        TMPPB0 --> TMPPBN
    end
```

WAL Segments consist of a 1-byte header (version byte = 0), followed by batches from multiple partitions.
Batches are sorted lexicographically by topic, partition, and batch processing time.
Batches from the same partition are always stored consecutively.

# Produce Path
```mermaid
---
title: Produce Path
---
sequenceDiagram
    autonumber
    participant P0 as Producer 1
    participant P1 as Producer 2
    participant B as Broker
    participant O as Remote Storage
    participant M0 as Diskless Coordinator 1
    participant M1 as Diskless Coordinator 2
    Note over P0, P1: Multiple producers send requests <br/> concurrently for 250ms or 4MiB
    par
        P0->>B: Produce partition 0
        activate B
    and
        P1->>B: Produce partition 1
    end
    Note over B, O: Multiple brokers upload <br/> files independently
    B->>O: Upload file
    activate O
    O-->>B: Upload finished
    deactivate O
    Note over M0, M1: Multiple independent coordinators <br/> may be represented in a single file
    par
        B->>+M0: Commit file
        M0-->>-B: Offsets assigned
    and
        B->>+M1: Commit file
        M1-->>-B: Offsets assigned
    end
    par
        B-->>P0: Ack partition 0
    and
        B-->>P1: Ack partition 1
    end
    deactivate B
```

# Consume Path
```mermaid
---
title: Consume Path
---
sequenceDiagram
    autonumber
    participant C0 as Consumer 0
    participant B as Broker
    participant O as Object Storage
    participant M as Diskless Coordinator
    participant T as Tiered Storage
    C0->>B: Consume multiple partitions
    activate B
    B->>B: Authn/Authz checks
    alt Data in Local segments
        B->>B: Fetch data
    else Broker in ISR and fetch is at tail
        B->>B: Enter purgatory
        Note over B, M: Replica fetcher thread polling <br/> for new batches in background
        par
            B->>+M: Find Batches
            M-->>-B: Batch Coordinates
            B->>+O: GET Object (id, range)
            O-->>-B: WAL Segment Data
            B->>B: Append to local segments
        end
        B->>B: Exit purgatory
        B->>B: Fetch data
    else Broker is replica and data moved to Tiered Storage
        B->>B: Fetch Remote Segment Metadata (RLMM)
        B->>B: Fetch Remote Segment Data (RSM)
        B->>+T: GET Object
        T-->>-B: Remote Segment
    else Broker not in ISR or fetch not at tail
        B->>+M: Find Batches
        M-->>-B: Batch Coordinates
        B->>O: GET Object (id, range)
        O-->>B: WAL Segment Data
        B->>B: Inject Offsets
    end
    B-->>C0: Batches
    deactivate B
```

# Offset Regions
```mermaid
block-beta
    columns 6
    0 1000 2000 3000 4000 5000
    
    space:1 P["WAL Segments"]:5
    
    T["Tiered Segments"]:2 space:4
    
    space:2 U["On Disk for tiered upload"] space:3
    
    space:4 L["Inactive Segments"] A["Active Segment"]
```

Above is a diagram representing an example layout of 6 segments for a single partition.

* Data present in the local segments (4000-5500) is served in the same manner as classic topics. 
* Fetches for the end of the topic (>5500) while the replica is in-sync wait for a background thread to populate the active segment.
* Fetches for data in tiered segments (0-2000) is served from Tiered Storage.
* Fetches for data not in the local segments or tiered storage (2000-4000) is served directly from WAL Segments
* Nodes that are elected the leader of the partition will begin assembling segments at the earliest non-tiered segment (2000). This data is not served to consumers.

```mermaid
---
title: Relationship with Tiered Storage
---
flowchart LR
    Producer --> Classic["Classic Topics<br><i>Low Latency</i>"]
    
    Producer --> Diskless["Diskless Topics<br><i>Low Replication Cost</i>"]
    Classic --> Tiered["Tiered Storage<br><i>Low Storage Cost</i>"]
    Diskless --> Tiered
    Classic --> Consumer
    Tiered --> Consumer
    Diskless --> Consumer
```

Producers can produce data for both classic and diskless topics.
After data is ordered, it will be asynchronously copied to Tiered Storage and then cleaned out from the ingest engines.
This permits the Classic storage engine to be write-latency-optimized and the Diskless storage engine to be replication-cost-optimized.
Once the data is moved to Tiered Storage, it will be storage-cost-optimized both for archival and access.
Data will remain visible to consumers between the time it is given an order by the ingest engine, and when it is finally cleaned up due to retention in tiered storage. 

