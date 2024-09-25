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

### Integration with existing Tiered Storage data

Can we read existing tiered storage data into an Inkless broker?

### Consumer Groups

How do consumer groups perform coordination and persist offsets?
Can we use the existing (modernized) consumer group coordinator & new consumer rebalance protocol directly?

### Idempotent producers

How do we support idempotent producers & producer IDs?
How do we "deduplicate" batches that have already been persisted?

### Transactional Producers/Exactly Once Semantics

How can we support exactly once semantics? Where is the transaction coordinator and it's state?

## Developer & Operator facing challenges / Implementation

### New type of Topic

We need to create a new type of topic, in order to have Inkless topics working alongside traditional topics.
What would we need? can we piggyback on TS infrastructure?

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

### How to deal with batch metadata?

How can we record where is the data stored?

### What's our take on partition ownership?

Given brokers are stateless and Kafka has the concept of leader of a partition, how do we reconcile these two concepts? How can we guarantee ordering within partition?

### Data format of our Object Storage data

How does the data look like? Which metadata (topic, partition...) do we need to store and how?
How can we combine multiple files of a segment together for fewer Object API calls?

### Upgrades to Data Format

How can we ensure that Object Storage data can be read after a data-plane/metadata-plane upgrade?
How can we evolve the data format after some data has already been written?

### Upgrades to wire protocols

How will we keep pace with Kafka wire protocol changes? Does every version of Apache Kafka trigger a new version of the stateless brokers?
How will we upgrade the broker <-> metadata wire protocol? How will we perform upgrades of brokers & metadata layer?
How out-of-date can the data layer be from the metadata layer?

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

### What throughput is acceptable?

* Per-broker throughput bytes/sec
* Per-broker throughput records/sec

### What topology is acceptable?

* Number of brokers per AZ
* Number of AZs per metadata plane
* Number of metadata planes per Operator
* Number of metadata planes per Customer

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

### Compression

How can we maintain or improve compression when writing to object storage?

### Multitenant metadata-plane + control-plane

Should we design the metadata APIs with multitenancy as a first-class abstraction?
Can we amortize costs of non-data-plane components by sharing them among multiple customers?