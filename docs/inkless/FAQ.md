**Inkless FAQ \- Expanded Responses**

**What is Inkless?**

* **Q: What is Inkless?**  
  * A: Inkless is an implementation of [KIP-1150: Diskless Topics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Diskless+Topics), which essentially provides a new data path for producers and consumers. This data path leverages object storage for storing Kafka topic data, making it a drop-in replacement for Apache Kafka. It's designed to use object storage like AWS S3, Google Cloud Storage (GCS), or Azure Blob Storage instead of relying solely on local disks.  
* **Q: Are the APIs compatible with Apache Kafka?**  
  * A: Yes, Inkless is a fork of Kafka that includes new features, but the core APIs are designed to be highly compatible with Apache Kafka. This means that applications written for standard Apache Kafka should, in most cases, work with Inkless with minimal to no changes. The aim is to maintain compatibility to facilitate easy adoption.  
* **Q: What is the main difference between Inkless and Kafka?**  
  * A: The key difference lies in the storage layer. Classic Kafka stores data on local disks and relies on replication for durability and fault tolerance. Inkless, on the other hand, offloads data storage to object storage, simplifying the broker's role and leveraging the built-in redundancy and durability of the object storage system. This change also introduces a new type of topic that utilizes this object storage mechanism.

**Inkless Architecture and Concepts**

* **Q: Are you really leaderless?**  
  * A: Yes, at the data layer for diskless topics, there is no designated leader for each partition. This means that any broker can serve read requests for any partition data stored in object storage. However, there is still a central coordinator, specifically the "Batch Coordinator," for metadata management, which is currently backed by PostgreSQL. So, it's more accurate to say we are "leaderless" for the data path but not entirely leaderless for metadata management.  
* **Q: Are there partitions in diskless topics?**  
  * A: Yes, diskless topics still use partitions. Partitions are fundamental to how Kafka distributes and parallelizes data, and this concept remains in Inkless. Even though the data storage is different, partitions provide the necessary structure for consumers and producers to work efficiently.  
* **Q: Are diskless partitions the same as classic Kafka partitions?**  
  * A: The core concept is the same, and they maintain API compatibility. They still represent a sequential, ordered log of messages. The primary difference is in how the data within those partitions is physically stored. In classic Kafka, partitions reside on local disks and are replicated. In diskless topics, partitions are stored in object storage.  
* **Q: Why do you still use partitions in diskless topics?**  
  * A: Partitions are used for distributing the load of producers and consumers. They allow for parallelism, meaning multiple consumers can read from different partitions simultaneously, and multiple producers can write to them concurrently. This parallelism significantly improves the throughput and scalability of the system. Without partitions, the system would be less efficient and harder to scale.  
* **Q: Should I still use more than one partition?**  
  * A: Yes, consumer parallelism is still limited by the partition count, and so multiple partitions will permit multiple consumers to process data in parallel. Each consumer consumes from one partition, so having more partitions than consumers will not work. This ensures that multiple consumers can work concurrently without stepping on each other's toes.  
* **Q: Is there a partition leader in diskless topics?**  
  * A: No, for diskless topics, there is no leader for each partition in the traditional Kafka sense. This is one of the key aspects of being "leaderless" at the data layer. Any broker can serve data from any partition, which simplifies the architecture and improves availability.  
* **Q: What about replications in Inkless?**  
  * A: Replication for diskless topics is delegated to the storage layer, meaning the object storage system handles data redundancy and durability. Services like AWS S3, GCS, or Azure Blob Storage have built-in replication mechanisms, which Inkless leverages instead of managing replication at the Kafka broker level.  
* **Q: Are there under-replicated partitions in Inkless?**  
  * A: No, there are no under-replicated partitions in Inkless by design because the storage layer handles data replication. Object storage services are designed to be highly durable and redundant, typically providing multiple copies of data across different storage units or even different physical locations. This eliminates the traditional Kafka problem of under-replicated partitions.  
* **Q: What is the storage backed with?**  
  * A: The storage is backed by cloud object storage services such as AWS S3, Google Cloud Storage (GCS), and Azure Blob Storage. Inkless offloads data persistence to these services, taking advantage of their scalability, reliability, and cost-effectiveness.  
* **Q: Do you have segments?**  
  * A: Yes, but they're different from traditional Kafka segments. In Inkless, instead of segments on local disk, we aggregate multiple messages into "objects" that are stored in object storage. These objects serve a similar purpose to segments by chunking data, but they have different characteristics and are stored remotely.  
* **Q: What is the difference between classic Kafka segments and these objects in Inkless?**  
  * A: Here are the key distinctions:  
    * In classic Kafka, a segment is a totally ordered sequence of messages specific to a single partition and stored locally.  
    * In Inkless, objects contain messages from several partitions and are not ordered internally within the object itself. Ordering is maintained at the partition level by the Batch Coordinator.  
    * Classic Kafka segments are stored and managed directly by Kafka brokers, whereas Inkless objects are stored and managed by object storage services.  
* **Q: When are these aggregated messages (objects) written?**  
  * A: These objects are written to object storage in batches. A batch is created either when a specific time interval has elapsed (e.g., 250ms by default) or when a certain number of bytes have been accumulated (e.g., 8MB by default). This batching helps optimize write operations to object storage, as it's more efficient to write larger chunks of data less frequently than many small chunks.  
* **Q: Why do we have objects?**  
  * A: The main reason for using objects is to optimize for the economics of object storage. Object storage is designed for storing large amounts of data cost-effectively but has a higher overhead for each individual write operation. Batching messages into objects reduces the number of writes and makes data storage and retrieval more efficient and cost-effective.  
* **Q: Why using object storage?**  
  * A: Object storage is used to align Kafka with cloud-native architectures. It provides several benefits, including:  
    * Scalability: Object storage can scale to handle vast amounts of data.  
    * Durability: Object storage services offer high durability and data redundancy.  
    * Cost-effectiveness: It can be more cost-effective for large volumes of data, especially for infrequent access patterns.  
    * Simplicity: Offloading storage simplifies the Kafka broker architecture.  
* **Q: What do I get by using object storage?**  
  * A: You gain several key benefits:  
    * Reliability: High durability and availability are guaranteed by the object storage service provider.  
    * Cost-effectiveness: Lower storage costs, particularly for large datasets and infrequent access.  
    * Simplified Architecture: Reduced operational complexity for Kafka brokers.  
    * Scalability: Easily handle growing data volumes.  
* **Q: Why not using Tiered Storage?**  
  * A: There are several reasons why Inkless uses a different approach than tiered storage:  
    * Cost: Tiered storage often involves replicating data between brokers, as with classic Kafka topics, leading to higher costs and network overhead.  
    * Durability: Inkless leverages the inherent durability of object storage, removing the need for replication at the Kafka broker level.  
* **Q: What is the batch coordinator?**  
  * A: The Batch Coordinator is a key component responsible for managing message batches and ensuring the total order of messages within partitions. It essentially manages the metadata about which batch is in which object and their order.  
    * Currently, the batch coordinator is implemented using a PostgreSQL database per cluster.  
    * Future plans include transitioning to a topic-based coordinator as outlined in [KIP-1164: Topic Based Batch Coordinator](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1164%3A+Topic+Based+Batch+Coordinator).  
* **Q: What is the relationship between Inkless and PostgreSQL?**  
  * A: Currently, PostgreSQL is used as the backing store for the Batch Coordinator. It ensures the total order of messages within partitions. This means that messages are written and read in the correct sequence, which is critical for Kafka's guarantees.  
* **Q: Is there a coordinator leader in Inkless?**  
  * A: Yes, for the Batch Coordinator (currently based on PostgreSQL), there is a leader that manages metadata and ensures ordering. While the data path is designed to be leaderless, the metadata management does rely on a leader for the Batch Coordinator. This leader is responsible for ensuring consistency and ordering of metadata, which indirectly guarantees message ordering within partitions.  

**Data Handling and Guarantees**

* **Q: Do you have the same guarantees as classic topics?**  
  * A: Yes, diskless topics aims to provide the same core guarantees as classic Kafka topics:  
    * **Ordering:** Message ordering within partitions is preserved. Messages are delivered in the same order they were produced.  
    * **Consistency:** Data consistency is maintained. Consumers will see a consistent view of the data.  
    * **Deliveries:** Message delivery is assured, meaning that messages will not be lost (assuming the object storage system is functioning correctly).  
    * **Durability:** Data durability is provided by the underlying object storage, which is designed for high data durability and redundancy.  
    * **Integrity:** Data integrity is guaranteed. Messages are delivered without corruption.  
* **Q: How do we guarantee these things?**  
  * A: The guarantees are achieved through a combination of mechanisms:  
    * **Batch Coordinator:** The Batch Coordinator ensures a total order of messages within partitions. It tracks which messages are in which objects and maintains their sequence.  
    * **Object Storage Guarantees:** The inherent durability and redundancy of object storage systems ensure data availability and integrity.  
    * **Metadata Management:** Robust metadata management ensures that consumers can find and retrieve messages correctly and in the right order.  
* **Q: Is it still an append-only distributed log?**  
  * A: Yes, Inkless retains the fundamental characteristic of being an append-only distributed log. Messages are appended to the end of the log and are not modified or deleted. This ensures consistency and makes it suitable for event streaming use cases.

**Usage and Operations**

* **Q: Can I change the topic type from classic to diskless?**  
  * A: Currently, the topic type (classic or diskless) is set at topic creation and cannot be changed afterward. This is because the storage mechanism is fundamentally different between the two types. In the future, there might be mechanisms to facilitate data migration between classic and diskless topics, but direct conversion is not supported right now.  
* **Q: Do you have local disks for Inkless data?**  
  * A: No, Inkless data is stored in object storage, not on local disks. The Kafka brokers in an Inkless setup primarily manage metadata and coordinate data transfer. The data itself is persisted in external object storage.  
* **Q: What data is on the local disks in a mixed cluster?**  
  * A: In a mixed cluster (with both classic and diskless topics), local disks are used to store data for classic Kafka topics. Metadata for all topics (both classic and diskless) is also stored locally.  
* **Q: Can we have both classic and diskless topics in the same cluster/broker?**  
  * A: Yes, a Kafka cluster can contain both classic and diskless topics. This allows for a gradual transition to diskless or the flexibility to use the best storage mechanism for different use cases.  
* **Q: Can we have diskless topics only?**  
  * A: Yes, you can have a cluster consisting entirely of diskless topics. In this case, all data is stored in object storage, and the brokers primarily handle metadata. This provides a cleaner, simpler architecture.  
* **Q: What data is stored in diskless-only clusters?**  
  * A: In an Inkless-only cluster, Kafka brokers primarily store metadata. This includes:  
    * Topic configuration.  
    * Partition information.  
    * Batch coordinates (pointers to the objects in object storage that contain the message data).  
    * Consumer group offsets.  
* **Q: How do you migrate topics to diskless?**  
  * A: Direct topic migration from classic to Inkless is not currently possible. Common methods for moving data from classic to diskless topics include:  
    * **MirrorMaker:** Using Kafka MirrorMaker to replicate data from the classic topic to a new diskless topic.  
    * **Dual Writes/Reads:** Implementing a strategy where applications write to both the old and new topics, then transition to reading only from the new diskless topic.  
* **Q: Can you shrink your cluster?**  
  * A: Yes, you can shrink a cluster containing only diskless topics, as data storage is independent of the number of brokers. Shrinking a mixed cluster is more complex and has similar challenges to shrinking a standard Kafka cluster, as data for classic topics resides on the brokers.  
* **Q: Can you shrink partition numbers?**  
  * A: No, you cannot shrink partition numbers for existing topics, regardless of whether they are classic or Inkless. This is a fundamental limitation of Kafka's design. Once a topic is created with a certain number of partitions, that number remains fixed.  
* **Q: Can I upgrade from version 2.x to Inkless?**  
  * A: Upgrading to a version that supports Inkless involves a multi-step process. Typically, you would need to upgrade to intermediate versions that support newer features and metadata formats before finally upgrading to the version that includes Inkless (or Kafka 4.0 or later, depending on the terminology used). The exact steps may vary depending on the specific versions involved.  
* **Q: Is adding a new broker in the cluster faster with diskless topics?**  
  * A: Yes, adding a new fresh cluster tend to be faster with diskless topics because the data is not stored on the brokers. Adding new brokers is simpler and faster as they don't have to synchronize data.  
* **Q: Can we have multiple buckets within a cluster and region?**  
  * A: Currently, the design focuses on using a single object storage bucket per cluster. Using multiple buckets is not supported.  
* **Q: Is there a community behind Inkless?**  
  * A: Inkless is an implementation of [KIP-1150: Diskless Topics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Diskless+Topics), and the aim is to upstream these features to Apache Kafka. Therefore, the primary community support and development will eventually come from within the broader Apache Kafka community.  
* **Q: Can Inkless be used on an Aiven Tenant?**  
  * A: Offering Inkless as a service on Aiven requires changes to our service provisioning and sales models. Currently, Inkless is not directly available as a standard offering on Aiven Tenants.  
* **Q: Are we using Inkless ourselves?**  
  * A: We are currently testing and validating Inkless internally. It is being used to ingest logs from some of our services as an internal trial and evaluation.

**Performance and Cost**

* **Q: Are diskless topics as performant as classic Kafka topics?**  
  * A: No, there is typically a latency overhead with diskless topics compared to classic Kafka topics. This is because of the batching mechanism and the extra network hop to the object storage and batch coordinator. Classic Kafka topics have lower latency for individual messages but might face other bottlenecks with high throughput.  
* **Q: When to use diskless topics?**  
  * A: Diskless topics are well-suited for use cases where:  
    * **Latency tolerance:** Applications that can tolerate slightly higher latency (e.g., logs aggregation, analytics, or data warehousing).  
    * **Batching is already done:** Workloads that naturally batch data before sending it to Kafka.  
    * **Cost is a significant factor:** When lower storage costs and reduced operational overhead are priorities.  
* **Q: What is the round trip?**  
  * A: The round-trip time depends on whether it's a producer or consumer and other factors:  
    * **Producer:** The round trip for the Inkless producer includes the batching delay (e.g., 250ms by default) plus the time to write to object storage and commit data to batch coordinator.  
    * **Consumer:** The round trip for a consumer depends on whether the data is cached or needs to be fetched from object storage. Subsequent reads from cached data will be much faster than the initial fetch.  
    * **Classic Producer:** The round trip for a classic Kafka producer is generally much lower, often just the network latency to the broker.  
* **Q: How do you achieve performant consumers?**  
  * A: Performant consumers are achieved through caching. When a consumer fetches data from object storage, the fetched object is cached on a broker within the same availability zone (AZ). Subsequent reads of data from that same object are served from the cache, resulting in much lower latency.  
* **Q: I’m producing a lot of data, does it scale?**  
  * A: Yes, Inkless scales well, particularly with high data throughput. The higher the overall throughput across all diskless topics, the lower the average latency tends to be. This is because larger batches can be formed more quickly, making the object storage writes more efficient. High volume production aligns well with the design of Inkless.  
* **Q: Can I tune it for my use case?**  
  * A: Yes, you can tune some parameters, primarily the batching interval and the batch size. However, we have provided sensible default values (e.g., 250ms for the batching interval and 8MB for the batch size) that are designed to work well for most common use cases. Tuning should be done carefully based on your specific requirements and after thorough testing.  
* **Q: Are there downsides to tuning it?**  
  * A: Yes, at extreme values (low latency \<10ms and small objects \<100kb) there is very strong diminishing returns. Cost grows exponentially, and can exceed the cost of Classic topics. It is important to test your configuration against real traffic and load before doing a full deployment.  
* **Q: Where does the cost come from?**  
  * A: The cost components in Inkless include:  
    * **Fixed Costs:** These include the cost of running the Kafka brokers and the Batch Coordinator (currently PostgreSQL). These costs remain relatively stable regardless of the volume of data stored.  
    * **Object Storage Costs:** These costs are tied to the amount of data stored and retrieved from object storage, as well as the number of write operations. Costs depend on the pricing model of the chosen object storage provider.  
    * **Network Costs:** Network costs can significantly decrease with Inkless because inter-broker data transfer is minimized. Data is not replicated between brokers, and traffic stays largely within the same availability zone (AZ).  
    * **Total Cost of Ownership (TCO):** Overall, the TCO for Inkless can be lower than classic Kafka due to reduced storage costs, lower network costs, and simplified broker operations, especially at large scale.
* **Q: Why am I seeing seconds-high produce latencies when `inkless.produce.commit.interval.ms` is significantly lower?**  
  * A: Common problems could be
    * **Single Producer and Single Partition:** The most simple tests may opt for a single client producing to a single partition. In such cases, the client may hit the bottleneck of sequential request processing. Per broker-connection, the client maintains at most `max.requests.per.connection` requests in flight. On the server, future queued requests (by client-connection) have to wait for all preceding requests (by the same client) to be completed before being processed. The latency on the client-side can therefore grow to be the equivalent of `max.requests.per.connection` or more requests. To alleviate this with a single client - larger and more aggressive batching must be configured on the producer. It is generally recommended to run with multiple producers. See (the performance tuning guide)[./PERFORMANCE.md]

**Security and Disaster Recovery**

* **Q: Do you support HTTPS?**  
  * A: Kafka, in general, handles security concerns independently of HTTPS at the application level. Kafka uses its own mechanisms for authentication and authorization, such as SASL and SSL/TLS, which operate at the transport layer. Thus, security is handled within the Kafka protocol itself, not through HTTPS.  
* **Q: What is the security model for Inkless?**  
  * A: The security model follows Kafka's standard security model, which includes:  
    * **Authentication:** Using SASL (Simple Authentication and Security Layer) mechanisms.  
    * **Authorization:** Using Access Control Lists (ACLs) to control who can produce and consume from topics.  
    * **Encryption:** Using SSL/TLS for encrypting communication between clients and brokers and between brokers.  
  * In addition, object storage access controls will be used to control who can access the data stored in object storage.  
* **Q: What does my Disaster Recovery (DR) setup look like?**  
  * A: For DR with Inkless, you still need at least three Availability Zones (AZs) for the Kafka brokers and Batch Coordinator. Even though the data is in object storage, the metadata and control plane still require redundancy. A common DR strategy involves replicating the metadata (topics, ACLs, etc.) across multiple regions.  
* **Q: Why do you still replicate?**  
  * A: While the message data is stored in object storage, several other components still require replication:  
    * **Cluster Metadata:** Topic definitions, ACLs, and other configuration data.  
    * **Consumer Group State:** Offsets and other information about consumer groups.  
    * **Classic Topics Data:** Data for any classic topics that exist in a mixed cluster.  
    * **Batch Coordinates:** Metadata tracking which messages are in which objects and their location in object storage.  
* **Q: Do I need Mirror-Maker to replicate between region specific clusters?**  
  * A: MirrorMaker 2 (MM2) can be used to replicate data between region-specific clusters. MM2 will replicate data with relatively low latency, similar to the replication used for Classic Kafka topics. Importantly, the batch coordinates in the destination cluster will be stored separately from the main coordinates of the source cluster. This separation ensures that the replication process doesn't interfere with the operations of the original data.  
* **Q: How does Inkless handle object loss in storage?**  
  * A: Inkless relies on the durability and redundancy of the object storage service. If an object is lost due to storage system failure, Inkless will be unable to deliver the messages within that object. Object storage services are generally designed to prevent data loss, but in the unlikely event that it happens, data would indeed be lost.  
* **Q: Can Diskless enable topic sharing between clusters without inter-region costs?**  
  * A: In the future, this is a planned feature but is not currently implemented. The idea is that data in one region might be replicated in the background by the object storage service, and the batch coordinates could be stored centrally within one cluster that spans across regions. This approach would potentially enable topic sharing between clusters without incurring inter-region costs, as the data replication would be handled by the storage layer.

**Technical Details and Specific Use Cases**

* **Q: Are you compatible with librdkafka?**  
  * A: Yes, Inkless should be compatible with librdkafka to the extent that standard Apache Kafka is compatible. If your librdkafka client works with a regular Kafka broker, it should generally work with an Inkless-enabled broker, as long as the client configuration is correct and the underlying dependencies of the broker are taken care of.  
* **Q: What are the limitations of Inkless?**  
  * A: Current limitations include:  
    * **No Transactions:** Inkless does not currently support transactions.  
    * **Limited Retention Policies:** Retention policies based on bytes or time are not yet fully supported in the same way as classic Kafka topics.
    * **No Compacted Topics:** diskless topics can't be also compacted topics. This follows a similar limitation as with Tiered Storage.
    * **No Share Groups:** Queues for Kafka are currently not supported in diskless topics.
* **Q: How does the producer work with Inkless?**  
  * A: The Inkless producer collects messages and buffers them according to the batching parameters. Once a batch is full (either by time or size), it writes the batch as an object to object storage. The producer then receives confirmation of the successful write and continues sending more data.  
* **Q: How is producer rack awareness configured?**  
  * A: Producers should configure rack awareness by setting the `client.id` to a specific format. This format is `<custom_id>,inkless_az=<rack>`, where `<custom_id>` is a unique identifier for the user's application or their existing `client.id`, and `<rack>` corresponds to the `broker.rack` configuration for brokers. This allows Inkless to route producer requests to the appropriate brokers within the same availability zone.  
* **Q: How does producer rack awareness avoid inter-zone costs?**  
  * A: When a producer is configured with a specific rack ID, it will primarily produce data to brokers that have the same rack ID. This ensures that data transmission stays within the same availability zone. Producers that are not configured with a rack ID might send data to brokers in other zones, which can result in inter-zone data transfer costs. Therefore, properly configured producer rack awareness significantly reduces the risk of incurring these costs.
* **Q: How is consumer rack awareness configured?**
  * A: Consumers should configure rack awareness by setting the `client.id` to a specific format. This format is `<custom_id>,inkless_az=<rack>`, where `<custom_id>` is a unique identifier for the user's application or their existing `client.id`, and `<rack>` corresponds to the `broker.rack` configuration for brokers. This allows Inkless to route consumer requests to the appropriate brokers within the same availability zone.
* **Q: How does consumer rack awareness avoid inter-zone costs?**
  * A: When a consumer is configured with a specific rack ID, it will primarily consume data from brokers that have the same rack ID. This ensures that data transmission stays within the same availability zone. Consumers that are not configured with a rack ID might receive data from brokers in other zones, which can result in inter-zone data transfer costs. Therefore, properly configured consumer rack awareness significantly reduces the risk of incurring these costs.
* **Q: Is there producer rack awareness for Classic Topics?**  
  * A: No but for keyless data, [KIP-1123: Rack-aware partitioning for Kafka Producer](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1123:%20Rack-aware%20partitioning%20for%20Kafka%20Producer) will send keyless data to a leader in the local rack. This is a proposal that will address this in the future, but it is not implemented in the same way as the producer rack awareness with diskless topics.  
* **Q: How is data from multiple producers distributed to multiple consumers?**  
  * A: In Inkless, each object containing batched messages is cached by one broker per availability zone. When consumers make fetch requests, these cached objects are then distributed by inter-broker requests to other brokers that are serving the fetch requests. This way, data produced by multiple producers gets distributed across different brokers and made available to multiple consumers in different parts of the system. This caching and distribution mechanism ensures that:  
    * Consumers can access data with low latency, particularly for frequently accessed or recently produced data.  
    * The load is balanced across multiple brokers as they cooperate to serve consumer requests.  
    * The overall system scales well as the number of producers and consumers increases.  
* **Q: Do inter-broker connections cross availability zones?**  
  * A: No, generally, inter-broker connections for diskless topics stay within the same availability zone (AZ). This is a key design principle to reduce cross-AZ traffic and associated costs. The distribution of cached objects between brokers occurs within the same AZ.  
* **Q: How does Read-ahead work in Inkless?**  
  * A: Read-ahead behavior depends on whether the data being read is part of the "hot set" (frequently accessed) or trailing data (older data).  
    * **Hot Set Data:** For hot set data, read-ahead is relatively straightforward. The data is likely to be cached on a broker within the same AZ, making reads fast.  
    * **Trailing Reads:** For trailing reads, efficiency depends on file merging. If file merging has been performed, retrieving older data is more efficient. Otherwise, multiple object reads may be needed, potentially increasing latency.  
* **Q: How does file merging work?**  
  * A: A broker will read from objects written by several other brokers, and work to reorganize the data into output files. These files will be optimized for retention and read access. Data is currently merged into single output files, but this may be improved in the future to merge data in smaller groups or with a more complex method. File merging is primarily aimed at improving the efficiency of reading older, trailing data by combining many smaller objects into larger, more consolidated files.  
* **Q: What metrics are available for Inkless?**  
  * A: You get the standard Kafka metrics (e.g., throughput, latency, consumer lag) plus new metrics specific to Inkless. These additional metrics typically provide insights into:  
    * Object storage operations (writes, reads, latency, errors).  
    * Batching efficiency (batch sizes, time intervals).  
    * Cache hit rates.  
    * Batch Coordinator performance.  
* **Q: Does Inkless work with S3 Express?**  
  * A: Support for S3 Express is not currently implemented but is in the backlog. The architecture should support integration with high-performance object storage services, but specific testing and integration work needs to be done.  
* **Q: How do I avoid all inter-availability-zone traffic in Inkless?**  
  * A: To avoid inter-AZ traffic:  
    * **Use Producer Rack Awareness (PRA):** Configure producers with the `client.id` format to ensure they write primarily to brokers within the same rack/AZ.  
    * **Use Consumer Rack Awareness (CRA):** Configure consumers with the `client.id` format to ensure they receive primarily from brokers within the same rack/AZ.  
    * **Strategic Replication:** Although data replication is handled by object storage, ensure the object storage configuration minimizes cross-AZ data transfer costs (e.g., ensure object storage replicas are within the same region or AZ as much as possible).  
* **Q: Is KRaft stable?**  
  * A: Yes, KRaft (Kafka Raft metadata mode) is considered stable, and there's a plan to move to using KRaft General Availability (GA) soon. KRaft replaces ZooKeeper for metadata management and provides a more robust and integrated metadata management system.  
* **Q: What are some commercial alternatives to Inkless?**  
  * A: Several commercial alternatives offer similar concepts to Inkless, leveraging object storage for Kafka-like workloads. These include:  
    * WarpStream  
    * AutoMQ  
    * FreightClusters  
    * BuffStream  
    * S2  
    * Northguard  
    * RedPanda R1  
* **Q: Who is the target audience of this help document?**  
  * A: This document is intended for developers, system administrators, architects, and developer managers who are interested in understanding Inkless. The target audience includes anyone who is considering or actively working with Inkless and wants to learn how it differs from traditional Kafka and how it can be used effectively.

