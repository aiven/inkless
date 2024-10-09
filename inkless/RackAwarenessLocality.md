
# Rack Awareness / Locality

Rack Awareness is the abstraction that Kafka has for discussing data locality.
Data locality is very influential on costs, and as a cost-oriented project, Inkless should use rack awareness generally.

## Types of existing Rack Awareness

### Replica Rack Awareness

[KIP-36](https://cwiki.apache.org/confluence/display/KAFKA/KIP-36+Rack+aware+replica+assignment) added this to Apache Kafka.
Brokers can be assigned a rack id via their configuration.
Replicas hosted by a broker inherit its rack id.

The rack-aware process is replica placement: given a replica for some topic-partition, choose a broker to host it.
The process minimizes or prevents replicas for a single partition being assigned to brokers with the same rack id.
This is termed "rack anti-affinity".

This is implemented by the StripedReplicaPlacer.

### Consumer Rack Awareness

[KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica) added this to Apache Kafka.
Consumers can be assigned a rack id via their configuration.

The rack-aware process is fetching data: given multiple replicas for a topic-partition, choose a broker to fetch from.
The process minimizes or prevents consumers from fetching data from replicas in a different rack.
This is termed "rack affinity".

This is partially implemented by the StickyAssignor and CooperativeStickyAssignor for the classic group protocol, and the UniformAssignor in the consumer protocol.
The broker enforces fetch-from-follower with the RackAwareReplicaSelector, giving the consumer a preferred replica to read from.

### Producer Rack Awareness

[Hacking the Kafka Protocol](https://www.warpstream.com/blog/hacking-the-kafka-protocol) describes this feature in WarpStream.
Producers can be assigned a rack id, embedded in their client id.

The rack-aware process is producing data: given multiple brokers which can write to a partition, choose a broker to produce to.
This is implemented by the broker's client metadata distribution system, manipulating the vanilla producer's logic.

## Proposed Rack Awareness

### Object Storage Rack Awareness

Without the concept of "replicas" in Inkless, replica rack awareness is non-functional.
Instead, the backing object storage should have locality information tracked and used for optimization.

For example: S3 Express 1Z would require "rack anti-affinity" to ensure durability.
S3 Cross-Region-Replication would require "rack affinity" to request objects in the local region to minimize costs.

Brokers handling writes should contact the metadata layer during initialization to discover required object stores.
If an object store is "required", data uploaded from that broker needs to be present in that store prior to committing offsets.
Brokers can then transmit coordinates for each of these object stores back to the metadata layer to commit & obtain offsets.

Brokers handling reads should contact the metadata layer when data is requested.
The metadata layer should respond with its preferred read replica, or balk at requests that have no suitable read replica.

The metadata layer should assign object storage according to configured storage and the brokers' configured zones.
A multi-level zone system may be necessary, to abstract both zone and region for clouds.

### Cache Rack Awareness

The architecture of Inkless also appears to need distributed caching for cost-effectiveness.
These caches should have good locality for good performance, and to also avoid network transfer costs.

Standalone caches should have some mechanism of specifying zone information, and communicating that to brokers.
Embedded caches should inherit their zone from the hosting broker.

Caches should not require anti-affinity, as they are not expected to be durable stores.