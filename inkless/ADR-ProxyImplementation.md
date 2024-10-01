## Decision statement

We should use Apache Kafka as a base for our inkless proxy implementation.
We will make modifications to Apache Kafka to add our desired new functionality that meets the design requirements.
We will release it as an alternative distribution of Apache Kafka on a similar cadence to the upstream.
If/when merged into the upstream, we may deprecate and remove our fork.

## Rationale

Apache Kafka is the reference implementation of the Kafka Protocol.
By building upon the reference implementation, we make it faster and easier to:

* Incorporate existing features (e.g. Consumer Offsets, Producer Idempotency, Exactly Once, Authorization)
* Maintain ongoing parity with bug fixes and features
* Upstream the new functionality if/when the community desires

The costs of modifying Apache Kafka seem reasonable:
* Modification appears to be necessary for 10s of classes, a significant minority of the overall codebase.
* We have moderate expertise in the Apache Kafka codebase already

The benefits of building from scratch seem minimal:
* Using a different language could avoid some of the downsides of the JVM or Garbage Collection
* We could avoid tech debt, legacy features, deprecations, etc already present in the upstream

The costs of building from scratch (the best known alternative) are substantial:
* We are faced with re-implementing the Kafka Protocol from scratch, or depending on a third-party reimplementation.
* Third-party implementations of the Kafka protocol are second-class citizens and receive features later than the upstream.
* The voting requirements for upstreaming a from-scratch solution are stricter than an opt-in feature.

## Expected Outcome

We should be able to reach feature parity with the upstream and make it to market faster.
We should be able to keep feature parity with the upstream and with non-open-source competitors.

## Implications of the decision

We will need to maintain a fork of Apache Kafka which has diverged in substantial ways from the upstream.
This will require an ongoing maintenance burden on our developers to merge the upstream into our fork. We should:
* Choose our design to minimize this divergence
* Influence the upstream to control the divergence and provide unofficial-APIs where possible
* Hire additional Java developers to address the burden in the short-term
* Discuss upstreaming this feature with the community for the long-term

