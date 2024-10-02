## Decision statement

Inkless brokers should use upstream KRaft cluster metadata distribution and controller quorum mechanisms.

Inkless clusters should also include traditional brokers to manage traditional topics.

## Rationale

Inkless brokers have many of the same cluster membership concerns as traditional Kafka brokers, because both should: 
* Form a cluster which provides access to multiple resources in a shared namespace
* Control authorization and quotas for these resources to enforce user-defined policies
* Support CRUD operations on these resources with eventual consistency

The KRaft effort in Apache Kafka has invested 5 years to implement a scalable, reliable system for distributing cluster metadata.
We should utilize that investment the project has already made and is dedicated to supporting, rather than replacing it with a bespoke metadata quorum.
This will minimize divergence from the upstream, one of the priorities established in our [ADR for Inkless Implementation](ADR-InklessImplementation.md).
Building a replacement may delay the project, require additional resources, or introduce bugs/inconsistencies.

The benefits of a bespoke metadata quorum are not significant, or necessarily unique to a bespoke solution:
* Optimize/remove metadata operations which are not necessary for inkless brokers (leadership, replicas, etc.) 
* Offer multitenancy and better cost amortization

Many of the internal mechanisms of Apache Kafka are implemented by an internal topic, and directly depend on partition leadership and low-latency writes:
* Consumer Offsets management
* Transactional producers
* Share Groups

We should expect that future features will also utilize this pattern of storage, and require additional internal topics.
Including traditional brokers in the same cluster as Inkless brokers will allow these features to operate seamlessly, as they do in the upstream.

Alternatively, traditional brokers could utilize a distinct metadata quorum, and Inkless brokers could proxy their requests to the traditional brokers.
This is undesirable because:
* Adds complexity to the implementation, as Kafka generally redirects requests to the leader/group coordinator rather than forwarding them.
* Adds a controller quorum to deploy and manage
* Adds load to Inkless brokers, both in throughput and connection overhead
* Risks the metadata of the two controller quorums getting out-of-sync in unpredictable ways.

As a byproduct of the above rationale, user-facing traditional topics can also be added without any additional work.

## Expected Outcome

Inkless brokers are first-class members in a Kafka cluster with traditional brokers.

Inkless brokers contact a controller during startup to download the latest metadata snapshot.
Inkless brokers maintain ongoing heartbeats to the controller quorum to ensure membership.
Disconnection from the controller quorum or loss of authorization will disable an unhealthy broker.

Clients for Inkless clusters can connect directly to traditional brokers and/or controllers when necessary.
Clients for Inkless clusters can also manipulate traditional topics, which are provisioned on traditional brokers.

## Implications of the decision

Running a controller quorum and traditional brokers requires persistent disks, and all the same operational concerns as traditional Kafka.
Adding and removing Inkless brokers should be efficient, but adding and removing traditional brokers will have the same overhead as they do currently.

As a minimum number of controllers is required to form a cluster, this prevents scaling-to-zero.
Controllers are currently dedicated per-kafka-cluster, this prevents amortizing their cost among multiple clusters without further investment.
Traditional brokers for hosting internal topics are dedicated per-kafka-cluster, and cannot be amortized among multiple clusters.

The improved scalability of the Inkless design may stress the existing controller implementation, which may require optimization.
These optimizations will also benefit non-Inkless clusters, so they should be easy to contribute upstream.

Deployments where Inkless brokers are separate from the controller quorum may have challenges that may be unusual within a Kafka deployment:
* Higher latency for metadata operations
* Large ongoing version differences
* Coordination of maintenance operations between operators
* Encryption of intra-cluster communications

Inkless brokers could be an "add-on" to traditional clusters to offer expansion and migration from traditional/tiered topics to fully inkless topics.
We should discuss the migration path for existing traditional/tiered topics to inkless during the design phase.

## Status

Status: Under Discussion
