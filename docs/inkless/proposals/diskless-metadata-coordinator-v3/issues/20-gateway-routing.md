# Routing produce by coordinator partition

Type: research
Status: closed
Assignee: viktor.somogyi-vass
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

The findings, with citations, are in [Routing produce by coordinator partition](../research/gateway-routing.md).

- Selection key: `selectByHash` and `selectLeaderLegacy` in `InklessTopicMetadataTransformer` hash `"<topicId>-<partitionIndex>"`. v2's coordinator mapping hashes the 20-byte log key, so routing needs that mapping, not today's string. The transformer sees only `MetadataView` and the listener-to-AZ map. `MetadataView` has no partition count, but `KRaftMetadataCache.numPartitions` has one. The mapping and the assignment fit in `SharedState`, which the transformer and the produce path both reach.
- Fetches: one `LeaderId` serves produce and fetch, so per-AZ alignment holds. A tail read hits the uploader's file cache when producer and consumer share an AZ, as today. Load now spreads by whole coordinator partitions, 3/3/2 at `N=8` with 3 brokers per rack. With `B / N / n_racks` rounded down, 25 to 47 brokers leave some brokers with no routed traffic.
- Managed replicas with remote storage: diskless produce has no leader or replica check, so a gateway needn't be a replica for produce. Fetch is the constraint: remote-storage topics and switched partitions with a local classic prefix route only to replicas. Replica placement ignores coordinator partitions, so these topics stay on today's routing unless placement is aligned.
- `ShareFetch`: diskless topics don't support share groups, and `ShareFetch` reads only the leader's local log, so gateways change nothing. Clients without `diskless_az` don't get the true metadata. `KafkaApis` rewrites `LeaderId` for every client, and a null AZ hashes over all brokers, so the broker can route these clients too.
- Stale gateways: `metadata.max.age.ms` defaults to 5 minutes. `KAFKA_STORAGE_ERROR` and a disconnect refresh at once, so the 5-minute window applies only while the old gateway stays alive, as when a broker joins or the assignment moves. A crashed gateway stalls produce until the broker is fenced, about 9 seconds, and isn't a misroute. Other misroutes come from a rack outage, the cross-AZ fallback, replica-only topics, brokers whose metadata briefly differs, and listener gaps.
- Named test: implementable as a unit test with the mapping and assignment, or a stub. "The same broker in each AZ" holds only at one gateway per coordinator partition per rack, which ends at 48 brokers with `N=8` and 3 racks. "Follow the assignment" must check that each partition gets its assigned broker, not that two partitions get different brokers.

The linked KIP-1164 note was deleted from `docs/inkless/proposals/` in `9187910153`, and the research read it from `8a86a2c404`. `split-vs-hint.md` counts 2 coordinator partitions per broker at 12 brokers instead of 8, which disagrees with the map's claim that fan-out is unchanged below `N × n_racks`. [Gateway benchmark](23-gateway-benchmark.md) owns that discrepancy.

The policy questions this research raised go to [Routing policy for clients a gateway can't serve](24-gateway-routing-policy.md) and [Gateway assignment](21-gateway-assignment.md).
