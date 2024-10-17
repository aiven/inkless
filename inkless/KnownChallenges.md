# Known Challenges

This is the current list of known challenges for Inkless.

## User facing challenges / Feature parity

### What cluster metadata is exposed to clients?

How can we tweak partition ownership given that brokers should be stateless?
How do we spread load of consumers and producers?
Which type of metadata?

#### Answer

We should rely on KRaft to ensure we have some sort of partition ownership.
Using some sort of load balancing algorithm (round robin, least connection, least response time, ...) we can try to ensure
load is distributed evenly within all the brokers. We need some ADR to find out what would be the best for this. Maybe even make it configurable.
This means we will fully utilize the current cluster metadata infrastructure.

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

Replication factor could also be used to manage high-fan-out topics: ones with many more consumers than producers.
Perhaps rather than having a single broker per AZ responsible for caching data, more than one copy could be kept loaded.
In Infinispan for example, there can be multiple "owners" for a key which can all serve requests.

### Integration with existing Tiered Storage data

Can we read existing tiered storage data into an Inkless broker?

#### Answer

Once Inkless Followers (followers which are not part of the ISR and their main goal is to serve consumer requests in different AZs) are in place,
they could in a similar way leverage Remote Log Metadata already available to source consumer requests.

We could also have Inkless brokers write compacted data into tiered storage.
This comes with a caveat that small segment sizes could cause wasteful API calls if minimums are not enforced.
It may also not be efficient to compact from a multi-segment format into a single-segment format.
Utilizing the object cache may make this shuffle efficient if the cache is large enough.

Compaction can combine multi-segment files into large multi-segment files until the size of an individual partition reaches a minimum threshold.
Then that individual partition can be moved into a tiered storage segment.
Small partitions that never reach the minimum segment size would always remain merged with other small partitions.
Large partitions could be compacted directly into a single-segment format after 1 pass.

Compaction instead of being an N:1 operation could be an N:2 operation:
N mixed input files could be selected which have a segment's worth of contiguous data for a single partition.
The remaining partitions could be emitted into a new mixed segment, but with one fewer partition.

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

#### Answer

The metadata layer will need to be aware of producer IDs and deduplicate batches.
Duplicate data in objects may be deleted after compaction occurs and live data is persisted to another object.

### Transactional Producers/Exactly Once Semantics

How can we support exactly once semantics? Where is the transaction coordinator and it's state?

#### Answer

Exactly once transaction markers will be persisted to object storage similar to normal data.
Transaction managers can periodically write blocks of commit messages to object storage, and contact the batch metadata system to persist their order.

This may cause thundering-herd problems for the broker that is arbitrarily assigned to cache these commit messages.
This may be replaced by special batch coordinates that encode retired transactions directly.

### Authorization

How can we maintain feature parity with existing authorization mechanisms and policies? 

#### Answer

We will leave existing authorization implementations in-place, with the exact same semantics as the upstream.
Authorization should be performed at the edge of the cluster by inkless brokers, to distribute load.

When customer-hosted inkless brokers contact an operator-hosted backend, they should be using secured and authorized connections.
Authorization here should be to mediate the customer/operator relationship, not enforcing intra-cluster resources.
Customers using an "untrusted inkless broker" implementation will be allowed to circumvent topic topology limits.

### Partition creation & deletion

Are partitions created upon first write, or explicitly via control plane/AdminClient?
Can partitions be deleted?
How do we communicate creation and deletion to brokers?
How do we communicate ACLs and other metadata to brokers?

#### Answer

For full protocol compatibility, Inkless brokers should accept AdminClient metadata operations.
We can optionally provide AdminClient access directly from the control plane if desirable.

Deletion of topics could be immediate from the metadata layer, and asynchronous for the data layer.

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

Warpstream solves this by encoding a "rack ID" in the producer client ID, which the metadata layer parses
The upstream could add native rack-awareness/multi-leader partitions to remove this hack.

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

#### Answer

Brokers could announce themselves as being in an "inkless" rack, with special meaning.
Or the inkless brokers could get the real rack IDs, and traditional brokers given internal rack IDs.

### How to deal with Object Storage writing

Where do we need to "inject" ourselves to enable Inkless writing on an Object Storage as primary storage.
Ordering of batches?
How to persist metadata in the metadata store?

#### Answer

Multiple Produce requests to the same broker will be batched together.
This "multi-batch" will be serialized according to some format, and written to an object.
The coordinates for the multi-batch will be sent to an offsets coordinator, which will determine:
* Which produce requests succeed and fail
* What order/offset the produce requests appear in the log
* What timestamp the produce request was finalized with

### Cloud-First Layout of Local Storage

Could the local "segment" system be replaced with the same layout as the object storage to unify the stateful & stateless broker implementations?
Maybe S3 Express 1Z actually makes sense for low-latency topics once we can use the object-storage file layout?

#### Answer

Probably not. The existing layout is purpose designed to optimize for consumer locality at the time of writing.
Requiring an additional compaction step to get consumer locality would generate wasteful disk load.
Modern filesystems already provide performant in-kernel lookup from file -> extent -> block, and userspace solutions would be strictly slower.

### How to deal with batch coordinates?

How can we record where is the data stored?

#### Answer (partial)

We need to vote on an ADR to choose the batch coordinate storage.
Requirements for this storage are:
* Strong consistency per-partition
* Query by nearby offset (and timestamp?) for fetching
* Query by object ID for compaction

### What is an acceptable size for batch coordinates? 

Data storage required for batch coordinates will scale with time and the amount of data in an inkless topic-partition.
Is this relationship linear O(n) or logarithmic O(log n)?
If linear, what is the constant factor (e.g. metadata bytes per record, per batch, per data byte?)
Can we use compression to optimize this ratio?

#### Answer (partial)

This depends on the design chosen for the batch coordinates.
We should expect on the order of ~100 bytes per producer batch, but variable per data byte.
This will be more efficient for larger records and larger batch sizes.
During compaction batches may be combined, and may allow batch coordinates to get more efficient for the same amount of data stored.

### What's our take on partition ownership?

Given brokers are stateless and Kafka has the concept of leader of a partition, how do we reconcile these two concepts? How can we guarantee ordering within partition?

#### Answer

For each partition, there will be a leader in control of a single linear history of the batch coordinates.
Multiple brokers can accept writes for a partition, but must contact the leader of the batch coordinates to obtain a total order.

### Data format of our Object Storage data

How does the data look like? Which metadata (topic, partition...) do we need to store and how?
How can we combine multiple files of a segment together for fewer Object API calls?

#### Answer (partial)

Our data format is a necessary component for any solution, but with relatively little impact on the architecture.
We should expect to spend time in the development phase designing the object storage format, after the architecture has been defined and the requirements are better understood.

### Upgrades to Data Format

How can we ensure that Object Storage data can be read after a data-plane/metadata-plane upgrade?
How can we evolve the data format after some data has already been written?

#### Answer

We should include necessary magic/version numbers in any custom data formats, similar to existing formats.

### What components are concerned with the data format?

There are a few components that _must_ know the in-object data format in order to operate:
* Producing records
* Compaction jobs planning & execution
* Fetching records

Components that don't need to know the in-object data format in order to operate:
* Object reads and writes
* Cache system

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

#### Answer

Async jobs should contact the metadata service to obtain work assignments, as they should avoid performing duplicate work.
The metadata service should reference count objects & extents of objects to determine if an object should remain live.

The metadata service should include a TTL for batch coordinates, before which it will not delete the referenced data.
Deletion of a file should take place if all data in a file is redundant/unused, and after all TTLs for emitted batch coordinates have expired.
Reads from object storage completing within the TTL are expected to succeed, and reads after the TTL are undefined.

### What sort of external dependencies (libraries, services, cloud services) could/should we bring in?

Some caching solutions could be solved with an existing library or prebuilt service/cloud offering.
AK tends to avoid dependencies whenever possible, and builds services on top of log storage (e.g. group coordination, transaction coordination, kafka connect)
AK has brought in dependencies in the past and later phased them out (ZooKeeper), or kept them long-term (RocksDB).

#### Answer

For commodity dependencies (cloud services) we should find a unifying abstraction that can accept plugins.
Architectural dependencies should be libraries and not services, so that there is less deployment burden on operators.
Architectural dependencies should also be open-source and well-supported.
The lifetime of these dependencies will be discussed on a case-by-case basis.

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

#### Answer (partial)

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

#### Answer

* < 100 brokers per AZ
* < 300 brokers per metadata plane
* < 10000 metadata planes per Operator
* < 100 metadata planes per Customer

### Could there be more than one bucket per cluster, or more than one cluster per bucket? 

Replication/mirroring/migrations between Kafka clusters may be cheaper when sharing underlying storage.
How could we share metadata between clusters to access the same underlying data?

It may be a requirement to have different inkless partitions in a cluster backed by different buckets.
Either permissions/compliance reasons, reducing single points of failure, or to allow simple deletion

#### Answer

S3 Express 1Z support will require writing to multiple buckets cross-zone in parallel.
We should plan to support a single cluster with multiple backing buckets, and encode the bucket in appropriate places.
Buckets may also be regional, so we could also expect multiple buckets to be attached to one cluster, but without the requirement for dual writes.

At this time there is not a strategy for sharing metadata between clusters, which would be necessary to make sharing buckets meaningful. 
We will design as if a bucket will only be attached to a single cluster at a time.

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

#### Answer (partial)

We will need to perform scale testing and optimization to determine a recommended default value for this.
It will be one of the primary tuning parameters, and should be configurable by users and by operators.

### How to reduce inter-AZ costs?

Intra-AZ traffic is usually free, while inter-AZ traffic is expensive.
How to have clusters that span multiple AZs but reduce inter-AZ traffic to a minimum?

Is setting independent clusters with same object storage enough? How do we deal with data linearization?
What is communicated between brokers in different AZs?
What sorts of communication is there between brokers in the same AZ?

#### Answer

Data transfer (producer -> broker, broker <-> object storage, broker -> consumer) should be zone-local in happy-path cases.
Data consists of any record keys, values, or headers written by user-controlled producers.
In cases of misconfiguration or infrastructure failure, data is allowed to cross AZ boundaries.

Metadata is always allowed to cross AZ boundaries, and must be kept to a minimum size necessary.
Metadata consists of everything that isn't data: Membership, leadership, topic topology, ACLs, batch coordinates, client service discovery, transactions, and consumer offsets.

### How to deal with the active segment?

How can we make use of the active segment as a cache for fresh data, but at the same time, limiting the disk to the active segment

#### Answer

Inkless topics may tune the **local** retention and log rotation configs to limit the size of the active segment (e.g. rotate by 100MiB, 1h with minimal local retention).
Similar to Tiered Storage, rotated segments should be removed as long as metadata confirms that records are in Remote Storage.

Note: The active segment may need to be completely empty for the lowest latency operation. 

### How can we utilize ephemeral local storage for an out-of-memory cache?

If we can use ephemeral disks with no reliability guarantees as caches, we could use hardware with less memory requirements.
Ephemeral disk cache may also save enough GET requests to justify their ongoing cost

#### Answer (partial)

Infinispan has support for multiple tiers of cache locality (memory, disk) and could make use of ephemeral storage.
We would need to performance-test the implementation to discover how a larger working set could impact costs.

### Object Read Optimization

How can we minimize the number of reads from object storage?
How could we do this without harming scalability & stability while scaling?

#### Answer

Object storage pricing fixes our priority on optimizing more writes than reads.
e.g. in AWS S3, write operations are 10x more expensive than reads.

This problem would also benefit from separating between global WAL (live data, i.e. active segment) vs. rotated segments.
If consumers are reading from the leader, records could be served from a local cache (many batches per request, no remote storage latency).
If reading from a follower, they will have to read from the global WAL, which means quite fragmented data (as multiple topic partition may be in the same object, optimizing writes) 1 batch per request.
If reading from older, rotated segments which should be compacted, per-partition, then consumption should be more performant (many batches per request + remote storage latency per request)

This problem should be part of the Follower fetching support for Inkless Topics.
Strategies could be applied:
- Price follower fetching, so customers pay a bit more to account for the burst in fetch request when reading from local data.
- Make the follower fetching lag behind a few global WAL batches, so we could apply some optimization (eager compaction) before serving data.

### How can we optimize same-AZ consumers & producers?

We could avoid reading from S3 in the same if we dual-write to S3 and a cache.
We could also intentionally co-locate consumers and producers for the same partition on a single broker.
How can we offer this optimization while preserving load-balancing & scalability?
A single partition may have very high ultimate throughput requirements (multiple producers, consumer share groups) and very high fan-out (>1000 consumers)
We need to explore the idea of having other consumers request messages to any broker, and if the broker doesn't have them instead of going to S3, go to the partition leader and ask for those.
In a sense we could use the existing flows of replicas to ask for not yet seen messages to the leader.

#### Answer

Eager cache warming on produce is wasteful if a partition has no consumers.
However, if other partitions in the same object have consumers, the data is very likely to be read back in immediately.
In a typical deployment, eager cache warming should reduce the number of GET requests for low-latency data by 1/3 in a 3-zone deployment.
It may increase "fetch jitter", because data in the local AZ will be ready before data produced in other AZs, reduced by the latency of 1 GET request.

We cannot influence the AZs that clients are created in, and we can't influence which partitions that they read/write to.
We should prioritize balancing of clients across the brokers in a zone, and not try to implement intra-zone locality.
Caches should manage distributing data between brokers in a cluster.

### Compression

How can we maintain or improve compression when writing to object storage?

#### Answer

This is more of a challenge for _rotated_ segments than the most recent data,
so it could be solved when records in global WAL (live data) get materialized/compacted into a log-per-partition format.

If we opt for a proposal where the rotated segments aim to be compatible with tiered storage format,
then the same approach from the Tiered Storage plugin can be valid:
There is a compression checker that validates if the first batch is compressed.
If it is not, then a _compression per chunk_ (where a chunk is configurable size, e.g. 8MiB).

### Zero Copying & Cache Ownership

How can we minimize data copying (and possibly enable zero-copying?)

What component is responsible for caching?

#### Answer

Producing records in Kafka currently involves copying and modification to assign offsets.
Since we will continue to provide the same validation frontend, there isn't an opportunity to remove a copy operation.

However, it may be possible to directly send from the in-memory buffers to object storage.
This is an optimization that we should attempt to include in the API design.
Whatever component is responsible for the data format will also be responsible for managing memory buffers.
If the data format is plugin-controlled, this may lead to ownership complications and potential for resource leaks.

Fetching will be strictly less efficient than the existing sendfile system, which can serve directly from the OS cache.
Populating an in-memory cache will always involve a socket-to-memory copy, and serving from that cache will involve a memory-to-socket copy.
If a distributed zonal cache is used, there will be additional memory-to-socket and socket-to-memory copies.

Compaction will always require copying into user-memory buffers, and is not an opportunity for zero copy optimizations.
Compaction reads could either be backed by the object cache, or be performed by dedicated direct reads.
Writes from compaction will likely be larger files, and require streaming/multipart uploads.

We should place responsibility for caching and zero-copy-optimization within Kafka.
Operations across plugin interfaces should be best-effort zero-copy when available, but without expecting the plugin to perform caching.

If caching was performed on ephemeral disks instead of memory, this should incur just one additional copy, as sending from the file could be zero-copy.

### Multitenant metadata-plane + control-plane

Should we design the metadata APIs with multitenancy as a first-class abstraction?
Can we amortize costs of non-data-plane components by sharing them among multiple customers?

#### Answer

We expect that building a multitenant metadata-plane/control-plane will have poor ROI, and that we should not pursue it.
The market segment of users that benefit from both a multitenant control-plane and from Inkless is minimal.
Users of this feature that operate at a scale to be sensitive to the cost of cross-az-traffic are likely to find a fixed cost associated with the metadata plane reasonable, and are likely to desire dedicated infrastructure anyway.

Building such a multitenant control plane also diverges us further from the upstream, requiring us to reimplement large parts of Kraft.
We may undertake this in the future as part of a separate effort when the benefits become more clear.
