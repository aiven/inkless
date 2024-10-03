# Known Challenges

This is the current list of known challenges for Inkless.

## User facing challenges / Feature parity

### What cluster metadata is exposed to clients?

How can we tweak partition ownership given that brokers should be stateless?
How do we spread load of consumers and producers?
Which type of metadata?

### Replication

We need to understand how to effectively deactivate replication factor for the new type of topics.
Is setting replication factor to 1 enough?
Should this setting be ignored for the new type of topics?

#### Answer

Replication factor is a configuration value that can set to 1 meaning effectively no replication occurs.
We could piggyback on this setting and have it forcefully set to 1 for inkless topics.
The way replication and being "in sync" is handled goes as following:
- Leader keeps track of the replicas and their state of replication.
- Uses the high watermark concept to understand if all needed followers (replication factor) have seen the messages so it can reply to the producer
- Followers actively fetch latest messages from leader

We should be able to use the fetch mechanism (with some modifications) in Inkless for brokers that are not partition owners
to fetch data from the owner and store it in their cache.

### Integration with existing Tiered Storage data

Can we read existing tiered storage data into an Inkless broker?

### Consumer Groups

How do consumer groups perform coordination and persist offsets?
Can we use the existing (modernized) consumer group coordinator & new consumer rebalance protocol directly?

#### Answer
Consumer Groups will be managed by an unmodified Group Coordinator and __consumer_offsets topic.
If __consumer_offsets is a traditional topic, this will require a traditional broker to host it.
If the Group Coordinator is in a different cluster, Inkless brokers can proxy consumer requests to the coordinator.

### Idempotent producers

How do we support idempotent producers & producer IDs?
How do we "deduplicate" batches that have already been persisted?

### Transactional Producers/Exactly Once Semantics

How can we support exactly once semantics? Where is the transaction coordinator and it's state?

### Authorization

How can we maintain feature parity with existing authorization mechanisms and policies? 

#### Answer

We will leave existing authorization implementations in-place, with the exact same semantics as the upstream.
Authorization should be performed at the edge of the cluster by inkless brokers, to distribute load.

### Partition creation & deletion

Are partitions created upon first write, or explicitly via control plane/AdminClient?
Can partitions be deleted?
How do we communicate creation and deletion to brokers?
How do we communicate ACLs and other metadata to brokers?

### Compatibility with legacy clients

Should we support all API versions that Apache Kafka supports?
Should we accept legacy data formats and convert them upon writing?
Can we keep existing data-safety (checksums, order verification) in place?

#### Answer

We will inherit all legacy client handling from the upstream repository.
We can enforce all available validations for fully implementation parity.

### Sources of Networking costs

Networking costs come from 3 different sources: Replication (fixed), Incoming/Producer (variable), Outgoing/Consumer (variable).
Both Producer and Consumer are variable as they depend on the placement of the clients vs the replica to write/read.
Their requests may come all from the same AZ, all from different AZ, or spread across AZs.
Replication costs are assumed to be fixed, as replication factor flows across 3 AZs.

#### Answer

We will approach a solution that first solves the Replication costs, and based on that model defines solutions for the other 2.
Outgoing costs have already a solution on today's Kafka with Follower fetching. Our solution should find a compatible or similar approach.
Incoming costs may require a solution that is not within the model of how Kafka works.

## Developer & Operator facing challenges / Implementation

### New type of Topic

We need to create a new type of topic, in order to have Inkless topics working alongside traditional topics.
What would we need? can we piggyback on TS infrastructure?

#### Answer

Kafka doesn't have the concept of topic types at the moment. There are configuration per topic that we could use.

org.apache.kafka.coordinator.group.modern.TopicMetadata seems to be a reasonable place to store this type.
However, Kafka doesn't really operate at topic level within a broker, but at log level, this is why many of the metadata
elements of a topic get pushed down (like configs).
We can follow the same path tiered storage uses by setting the topic type as another topic level configuration value.

We will likely need to modify the ReplicaPlacer interface to ensure topics are assigned to brokers which can accommodate them.

### New type of broker

Should brokers without log directories join the cluster as full members?
How would brokers in the same membership pool communicate their disk capacity to host traditional topics, tiered topics, etc)
Could brokers without log directories use group coordination to isolate membership from the rest of the cluster?

### How to deal with Object Storage writing

Where do we need to "inject" ourselves to enable Inkless writing on an Object Storage as primary storage.
Ordering of batches?
How to persist metadata in the metadata store?

### Cloud-First Layout of Local Storage

Could the local "segment" system be replaced with the same layout as the object storage to unify the stateful & stateless broker implementations?
Maybe S3 Express 1Z actually makes sense for low-latency topics once we can use the object-storage file layout?

### How to deal with batch coordinates?

How can we record where is the data stored?

### What is an acceptable size for batch coordinates? 

Data storage required for batch coordinates will scale with time and the amount of data in an inkless topic-partition.
Is this relationship linear O(n) or logarithmic O(log n)?
If linear, what is the constant factor (e.g. metadata bytes per record, per batch, per data byte?)
Can we use compression to optimize this ratio?

### What's our take on partition ownership?

Given brokers are stateless and Kafka has the concept of leader of a partition, how do we reconcile these two concepts? How can we guarantee ordering within partition?

#### Answer

For each partition, there will be a leader in control of a single linear history of the batch coordinates.
This allows for good data producer data transfer locality when possible. 
Multiple brokers can accept writes for a partition, but must contact the leader of the batch coordinates to obtain a total order.

### Data format of our Object Storage data

How does the data look like? Which metadata (topic, partition...) do we need to store and how?
How can we combine multiple files of a segment together for fewer Object API calls?

### Upgrades to Data Format

How can we ensure that Object Storage data can be read after a data-plane/metadata-plane upgrade?
How can we evolve the data format after some data has already been written?

#### Answer

We should include necessary magic/version numbers in any custom data formats, similar to existing formats.

### Upgrades to wire protocols

How will we keep pace with Kafka wire protocol changes? Does every version of Apache Kafka trigger a new version of the stateless brokers?
How will we upgrade the broker <-> metadata wire protocol? How will we perform upgrades of brokers & metadata layer?
How out-of-date can the data layer be from the metadata layer?

#### Answer

We will support all API calls with the exact same version as present in the corresponding Apache version.
Kafka has supported very wide upgrade windows (many intermediate versions skipped) so users will expect the same from Inkless brokers.
We should tolerate heterogeneous data & metadata layers, allowing for rolling upgrades for both in any order.
We should tolerate any released version of the data layer against any released version of the metadata layer.
When/if we want to deprecate or remove functionality, we can revise this policy.

### Async Jobs

How should asynchronous deletion & compaction jobs be coordinated?
How do we avoid corrupting data that is in-use by other brokers?

### What sort of external dependencies (libraries, services, cloud services) could/should we bring in?

Some caching solutions could be solved with an existing library or prebuilt service/cloud offering.
AK tends to avoid dependencies whenever possible, and builds services on top of log storage (e.g. group coordination, transaction coordination, kafka connect)
AK has brought in dependencies in the past and later phased them out (ZooKeeper), or kept them long-term (RocksDB).

### What latency is acceptable?

* Producer-ack latency
* Producer-consumer data latency
* Producer-consumer-commit latency
* Producer-service latency
* Consumer-service latency
* Object storage latency
* Metadata layer latency
* Intra-AZ latency
* Inter-AZ latency

#### Answer (partial)

The published latency for competitors is 400ms p99 producer-ack latency, and 1000ms p99 producer-consumer data latency.

Our first released version should be <800ms p99 producer-ack latency and 2000ms p99 producer-consumer data latency.
We should conduct this test with data + metadata layers in the same S3 region for best-case benchmarking.
We should evaluate the viability to running a single metadata layer for multiple regions.

### What throughput is acceptable?

* Per-broker throughput bytes/sec
* Per-broker throughput records/sec

#### Answer

Due to the different latency and topology characteristics of Inkless, different tuning may be required for producers and consumers to reach optimal performance.
Inkless brokers should target ultimate throughput performance parity with Apache Kafka, while not guaranteeing performance parity with any particular configuration.

For fair comparisons, we should do performance tests on equivalently-specified hardware and find pairs of configurations that:
* Use Replication Factor > 1 for Apache Kafka
* Saturate the network bandwidth of brokers
* Use very large records (>1MB) to test bytes/sec
* Use very small records (~0b) to test records/sec

We can also use recommended producer settings from competitors as a baseline for our testing.

### What topology is acceptable?

* Number of brokers per AZ
* Number of AZs per metadata plane
* Number of metadata planes per Operator
* Number of metadata planes per Customer

### Answer

< 100 brokers per AZ
< 300 brokers per metadata plane
< 10000 metadata planes per Operator
< 100 metadata planes per Customer

### Could there be more than one bucket per cluster, or more than one cluster per bucket? 

Replication/mirroring/migrations between Kafka clusters may be cheaper when sharing underlying storage.
How could we share metadata between clusters to access the same underlying data?

It may be a requirement to have different inkless partitions in a cluster backed by different buckets.
Either permissions/compliance reasons, reducing single points of failure, or to allow simple deletion

### Could there be other readers/writers to the bucket other than Inkless?

What if there are rogue writers? What if someone wants to pull data directly for analytics/data laking (e.g. iceberg)

#### Answer

There are numerous Kafka-based requirements for the data format that will constrain the design.
Well-intentioned external uses of the data are poorly defined currently, and not part of the official requirements.
To keep the design scope manageable, we should assume that there are no external dependencies on our data format or bucket contents.

Additionally, we will design around cloud providers' durability guarantees, and treat data loss as an exceptional situation. 
Users are expected to have tight controls over the data in their object storage, and prevent unintended access to the stored data.

## Business facing challenges / Cost management

### What is the right batch size for our Object Storage writing

We need to calculate the right size to minimize costs and latency (which probably are at odds)

### How to reduce inter-AZ costs?

Intra-AZ traffic is usually free, while inter-AZ traffic is expensive.
How to have clusters that span multiple AZs but reduce inter-AZ traffic to a minimum?

Is setting independent clusters with same object storage enough? How do we deal with data linearization?
What is communicated between brokers in different AZs?
What sorts of communication is there between brokers in the same AZ?

### How to deal with the active segment?

How can we make use of the active segment as a cache for fresh data, but at the same time, limiting the disk to the active segment

### How can we utilize ephemeral local storage for an out-of-memory cache?

If we can use ephemeral disks with no reliability guarantees as caches, we could use hardware with less memory requirements.
Ephemeral disk cache may also save enough GET requests to justify their ongoing cost

### Object Read Optimization

How can we minimize the number of reads from object storage?
How could we do this without harming scalability & stability while scaling?

### How can we optimize same-AZ consumers & producers?

We could avoid reading from S3 in the same if we dual-write to S3 and a cache.
We could also intentionally co-locate consumers and producers for the same partition on a single broker.
How can we offer this optimization while preserving load-balancing & scalability?
A single partition may have very high ultimate throughput requirements (multiple producers, consumer share groups) and very high fan-out (>1000 consumers)
We need to explore the idea of having other consumers request messages to any broker, and if the broker doesn't have them instead of going to S3, go to the partition leader and ask for those. In a sense we could use the existing flows of replicas to ask for not yet seen messages to the leader.


### Compression

How can we maintain or improve compression when writing to object storage?

### Multitenant metadata-plane + control-plane

Should we design the metadata APIs with multitenancy as a first-class abstraction?
Can we amortize costs of non-data-plane components by sharing them among multiple customers?
