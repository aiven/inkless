
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
    Local & RemoteStorageManager == FetchRequest ==> Consumer
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
        Broker2Diskless[Diskless]
    end
    subgraph Broker3[Replica]
        Broker3Diskless[Diskless]
        Broker3Local[Local Segments]
    end
    subgraph Cloud[Object Storage]
        ObjectStorage[WAL Segments]
    end
    subgraph Broker4[Broker]
        BatchIndex[Batch Coordinator]
    end
    Producer == Classic Produce ==> Broker0Handler == Append ==> Broker0Local
    Broker0Local == FetchResponse ==> ReplicaFetcher == Append ==> Broker1Local
    Producer == Diskless Produce ==> Broker2Diskless
    Broker2Diskless == PUT ==> ObjectStorage == GET ==> Broker3Diskless
    Broker2Diskless -- CommitBatches --> BatchIndex -- FindBatches --> Broker3Diskless
    Broker3Diskless == Append ==> Broker3Local
    Broker1Local & Broker3Local == FetchResponse ==> Consumer
```
Diskless topics change how ProduceRequests are translated to appends on each replica.

For Classic topics, Produce requests trigger an append to the leader's local segments.
Followers issue FetchRequests to the leader, which are served from the leader's local segments.
Followers then append this data to their local segments.

For Diskless topics, Produce requests trigger a PUT to Object Storage, and a CommitBatches on the Batch Coordinator.
Replicas issue FindBatches requests to the BatchCoordinator, and GETs to the Object Storage to retrieve batch data.
Diskless then appends batches to the local segments on the replica.

# Offsets Management

Data from ProduceRequests is written to object storage without assigning offsets.
Multiple ProduceRequests from multiple clients may be combined together into a single object to reduce the number of object writes.
The batch metadata (topic, partition, etc.) and location of the data (object ID & extent) are sent to a SQL database to be committed in a linear order.

Appending to replicas is done by querying the Batch Coordinator to find upcoming batch metadata for the requested partitions. 
For each object containing needed data, the object is read from object storage and then split by partition.
Offsets for the returned data are inserted from the batch metadata, and the data is appended to the local segments. 

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
    BatchIndex[Batch Coordinator]
    
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

When a cluster is deployed in multiple racks/zones, one replica should be placed in each zone for each Diskless topic.
This will permit all Kafka requests from rack-aware clients to be directed to the local zone replica, avoiding cross-zone charges for client traffic.
Each broker will request objects from object storage independently, and these requests for object data will also not incur cross-zone charges
