# Routing produce by coordinator partition

Type: research
Status: open
Assignee:
Blocked by: none

## Question

How does the broker route a diskless partition's produce to a gateway of its coordinator partition without public protocol, and what does that do to fetches, tiered topics, and `ShareFetch`?

v3 adds hint-only produce gateways as a cost optimization. Correctness never depends on them: a produce that reaches any broker commits as it does today, and only the number of commit parts per file changes. KIP-1164 defines gateways only as a recommendation, in its section "Reducing multi-DC commit operations." The map rules out public protocol, so KIP-1163's `PreferredProduceBrokers` isn't available, and routing goes through a rewritten `LeaderId`. See [`kip-1164-produce-gateways.md`](../../../docs/inkless/proposals/diskless-metadata-coordinator-v3/research/kip-1164-produce-gateways.md) and [Design weaknesses and mitigations](../../diskless-produce-gateways/research/design-weaknesses.md).

`InklessTopicMetadataTransformer` (`storage/inkless/src/main/java/io/aiven/inkless/metadata/InklessTopicMetadataTransformer.java`) rewrites `LeaderId` for diskless partitions when the client ID carries `diskless_az`. It selects a broker in the client's AZ by `murmur2("<topicId>-<partitionIndex>")` over the sorted candidates. With managed replicas and remote storage, it routes only to assigned replicas.

Find, in the Inkless checkout and the local Kafka checkout:

- Where the selection key changes from the user partition to its coordinator partition, and how the transformer reads `N` and the assignment from [Gateway assignment](21-gateway-assignment.md).
- Fetches: a rewritten `LeaderId` moves fetches with produce. Say what that does to the per-AZ alignment in [CLIENT-BROKER-AZ-ALIGNMENT.md](../../../docs/inkless/CLIENT-BROKER-AZ-ALIGNMENT.md), and to tail reads that hit the uploading broker's file cache from `CacheStoreJob`.
- Managed replicas with remote storage: whether a gateway must be an assigned replica, and whether those topics stay on today's routing or need placement aligned with coordinator partitions.
- `ShareFetch`, which only the real leader serves, and clients that send no `diskless_az` and get the true metadata. Say whether the broker can route them too.
- How long a client keeps sending to an old gateway. A diskless produce never answers `NOT_LEADER_OR_FOLLOWER`, so the client may not refresh until `metadata.max.age.ms`, 5 minutes by default. Count the other sources of misroutes: a gateway that fails, a rack outage, and the cross-AZ fallback.

Named test: two user partitions that map to the same coordinator partition get the same broker in each AZ, and two that map to different coordinator partitions follow the assignment.

## Resolution
