# Traditional Topics

Inkless is an extension of Apache Kafka, and so all existing functionality in Apache Kafka is still present for Traditional topics.
No extra steps are necessary to make use of this functionality with an Inkless enabled cluster.

# Inkless Topics

The Inkless feature is enabled on a per-broker basis by passing appropriate configurations and credentials to reach both object storage and batch coordinate storage.
Once Inkless is enabled on brokers, it can be enabled for individual topics.
Inkless topics have a restricted set of features available, as not all functionality has been implemented and tested.

Currently Inkless topics support:
* Non-Idempotent Produce
* Idempotent Produce
* Fetch
* ListOffsets
* Access restriction via ACLs
* Committing offsets via traditional Group Coordinators

The following are notable unsupported features:
* cleanup.policy=delete
* cleanup.policy=compact
* Adding Inkless topics to transactions
* read_committed consumers reading Inkless topics
* Producing to both inkless and traditional topics simultaneously

If not specified above, features are untested and assumed to be inoperable.

# Roadmap

In addition to full feature parity with traditional topics, we may add inkless-topic specific features
Listed in no particular order, here are some features that may be added Inkless in the future:
* Broker roles
* Heterogeneous broker capacities
* Batch coalescing/recompression
* Parallel produce request handling
* Zero-Copy cross-region sharing
* Out-of-process cross-region replication
* Cross-Cluster topic sharing
* Column-oriented object formats