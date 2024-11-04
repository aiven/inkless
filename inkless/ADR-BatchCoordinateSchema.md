## Decision statement

To coordinate batch metadata between Inkless brokers and stateful storage, we should use Kafka schemas to define request/response pairs.

However, we should use a different transport mechanism than Kafka, not relying on the existing Kafka wire protocol for transport.

## Rationale

Other upstream features that require coordination between Kafka brokers and/or controllers have used the Kafka API.
In the form of this feature that may be upstreamed, coordination will almost certainly use the Kafka API.
In order to prepare for upstreaming this feature, we should restrict ourselves to the schema expressiveness of the Kafka API.

We could attempt a seamless transition: Define the APIs, choose API IDs that don't conflict with the upstream, and merge them as-is.
However, we should expect that the upstream will add new API calls for other features before this feature. 
Therefore, we would choose IDs that will not conflict in the near future, and request that the upstream "skip" integer IDs when this feature is merged.
This is an unhealthy practice that the community would disapprove of, and may make it harder to gain approvals.

The community may also desire changes to the proposed protocol before merging upstream, requiring a transition from the internal API to the published API.
Since the community may not support a seamless transition, and may in fact prevent it, we should be prepared for one in the future.
Rather than modifying the Kafka network layer and incurring merge conflicts, only to require rework later during the transition phase, we should avoid modifying the network layer completely.

## Expected Outcome

Requests regarding batch coordinates will be managed via request-response pairs, rather than push-based or filesystem-based.

## Implications of the decision

Inkless brokers will generate request traffic, and must have an accompanying service component to respond.
This component may or may not be Kafka based in the internal implementation, but could be replaced with a Kafka-based implementation in the future.

## Status

Status: In Discussion
