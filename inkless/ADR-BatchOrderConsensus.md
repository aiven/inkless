## Decision statement

Storage of batch coordinates should be delegated to a pluggable interface instantiable on every broker.

Batch coordinate plugins should have an opt-in mechanism for forwarding requests to the partition leader chosen by Kraft.

## Rationale

To motivate the decision, consider all other alternatives:

Alternative A: Don't offer pluggable durable storage

The scalability of metadata coordinate consensus and storage is critical to the success of Inkless brokers.
For this reason, it may be necessary to change both the implementation and architecture of the metadata coordinate system.
This can happen over time, as new scalability concerns arise, or per-environment, where certain design choices are better suited.

For this reason, it is not reasonable to assert a single durable storage implementation for all use-cases, and instead we should make it pluggable.
This is in-line with the arguments and tradeoffs made for the Tiered Storage RemoteLogMetadataManager interface.

Alternative B: Offer pluggable durable storage, but require Kafka leadership/forwarding mechanisms

Because coordination of batch coordinates is critical to the correctness of the Kafka protocol, we could potentially use Kraft to choose leaders for topic partitions.
Inkless brokers producing data for a topic-partition would then forward requests to the partition leader, which could then delegate to pluggable durable storage.

However, this is wasteful when the underlying durable storage has consensus mechanisms available.
For example, suppose the underlying durable storage was a distributed NoSQL database with suitable consistency guarantees.
Choosing a leader and forwarding requests would concentrate load on leaders, when instead connections could be made directly to the NoSQL database.
This has the potential to have worse latency, resource utilization, and reliability, while not improving consistency guarantees. 

Alternative C: Offer pluggable durable storage, but require plugins to implement their own leadership/forwarding mechanisms.

Some implementations could be substantially worse without the ability to use the forwarding mechanism. For example:
* Plugins backed by a SQL database may suffer from excess connections on the backing database.
* Plugins backed by the broker filesystem may need to forward requests to brokers which are guaranteed to have persistent storage. 
* Deployments with a large version difference between nodes may want requests serviced by brokers with the most up-to-date version.
* Deployments with different network/credentials context may need to forward requests to a machine with sufficient privilege.

If the durable storage plugins were not made aware of Kafka's leadership but still wanted to make use of leaders, they would need some mechanism internally of discovering leaders.
This would involve implementing an unofficial/internal network API that mirrors the pluggable interface.
The Kafka project can offer this with minimal complication, taking this burden off of plugin developers.
This would also potentially allow interoperation of multiple plugin implementations via the common network API.

## Expected Outcome

We will define a pluggable interface and accompanying network protocol for batch coordinate requests.

The pluggable interface will allow swapping implementations over time, and/or to cloud-specific storage offerings.

Implementations on Inkless brokers will be able to forward requests to leaders, which can always be placed on brokers with access to durable storage and the latest version.

## Implications of the decision

We may be expected to provide an equivalent of the TopicBasedRemoteLogMetadataManager, as a batteries-included implementation.
It may or may not be natural to implement this in the prototype stage, as we may skip to using a NoSQL database directly.

We may also be expected to provide an "always forward" implementation that also cannot be a leader of any partition.
Brokers may need a mechanism for approving or rejecting a replica placement to support this.

Metadata storage plugins should include mechanisms for gaining and losing leadership of topic partitions, similar to the ReplicaManager.

## Status

Under Discussion
