# Routing mechanism

Type: research
Status: open
Assignee: unassigned
Blocked by: none

## Question

How do producers reach the gateways for a partition's coordinator partition, and what does each mechanism do to fetches, tiered topics, and clients that send no rack?

There are two candidates. KIP-1163 adds `PreferredProduceBrokers` to `MetadataResponse` version 14 and lets newer producers send `client.rack`. Older clients that put `diskless_rack_id` in their client ID get a rewritten `LeaderId` instead. Inkless already rewrites `LeaderId` for diskless partitions in `InklessTopicMetadataTransformer` (`storage/inkless/src/main/java/io/aiven/inkless/metadata/InklessTopicMetadataTransformer.java`). It selects a broker in the client's AZ by a murmur2 hash of the topic ID and partition index. [`kip-1164-produce-gateways.md`](../research/kip-1164-produce-gateways.md) quotes both KIPs.

Find, in the Inkless checkout and the local Kafka checkout:

- Whether the transformer can select a gateway for the partition's coordinator partition instead of a hash-selected broker, and what changes in it.
- What a rewritten `LeaderId` does to fetches. Kafka clients send produce and fetch to the same leader, so gateway routing also moves consumer traffic. Say how that interacts with the per-AZ cache locality that [CLIENT-BROKER-AZ-ALIGNMENT.md](../../../CLIENT-BROKER-AZ-ALIGNMENT.md) describes, and whether a consumer's `PreferredReadReplica` can separate the two.
- Tiered diskless topics: with remote storage enabled, the transformer only routes to assigned replicas, because remote log reads need a `UnifiedLog`. Say whether gateway routing then requires replica placement aligned with coordinator partitions, and what that means for managed replicas and consolidation.
- `ShareFetch`, which only the real leader serves, and clients that send no rack and get the true metadata.
- What `PreferredProduceBrokers` adds over a rewritten `LeaderId`: separate produce and fetch routing, an ordered fallback list, and no client ID convention. Say what it costs: public protocol, client support, and non-Java clients.
- How often a correctly configured client still reaches a non-gateway broker: metadata refresh lag, a gateway that fails, a rack outage, and cross-AZ fallback.

## Resolution
