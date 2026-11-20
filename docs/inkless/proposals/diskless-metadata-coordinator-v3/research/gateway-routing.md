# Routing produce by coordinator partition

Research for ticket [20-gateway-routing](../issues/20-gateway-routing.md), read on 2026-11-16. It gives [Gateway assignment](../issues/21-gateway-assignment.md), [Tracking part under gateways](../issues/22-tracking-part-under-gateways.md), and [Gateway benchmark](../issues/23-gateway-benchmark.md) the facts they need. It doesn't propose a design.

## Summary

- Two private helpers of `InklessTopicMetadataTransformer` compute the selection key: `selectByHash` for managed and tiered partitions, and `selectLeaderLegacy` for untiered partitions with one replica. Both hash the string `"<topicId>-<partitionIndex>"`. Gateway routing changes the key in both, and in the candidate list each helper picks from. The transformer sees only `MetadataView` and the listener-to-AZ map. `MetadataView` has no method for a topic's partition count. `InklessMetadataView` wraps `KRaftMetadataCache`, which has one. No gateway assignment and no coordinator-partition mapping exist in the code yet.
- One `LeaderId` serves produce and fetch, so fetches move with produce. Per-AZ alignment holds, and the uploader's file cache still serves tail reads in the same AZ, because the producer and the consumer of a partition in one AZ still reach the same broker. The spread changes. Brokers in an AZ share load per coordinator partition instead of per user partition. A hot coordinator partition makes one broker per AZ hot for both produce and fetch.
- Diskless produce has no leader or replica check, so a gateway needn't be a replica to accept produce. Topics with remote storage, and switched partitions whose classic prefix is only on local disks, route only to replicas, because their fetches read a local log. Placement uses the standard rack-aware `ReplicaPlacer`, which ignores coordinator partitions. These topics stay on today's routing unless placement aligns with coordinator partitions.
- `ShareFetch` doesn't serve diskless data today on any broker. It requires `Partition.isLeader()` and reads only the local log, and the FAQ lists share groups as unsupported for diskless topics. Gateways don't change that. The ticket's premise that clients without `diskless_az` get the true metadata is wrong: the transformer rewrites `LeaderId` for every client. Without an AZ, it hashes over all alive brokers, or over the alive replicas. The broker can route these clients to a gateway too.
- `metadata.max.age.ms` defaults to 300000 ms (5 minutes) for both the producer and the consumer. Diskless produce answers `KAFKA_STORAGE_ERROR` on a commit failure, and that error does trigger an immediate metadata refresh. A dropped connection does too. The 5-minute window applies while the old gateway is alive and healthy, which is the case when a broker joins or the assignment moves for any other reason.
- The ticket's test is implementable as a unit test, with two caveats. "The same broker in each AZ" is true only at one gateway per coordinator partition per rack, and only on the branch that routes to gateways. The test also needs the assignment and the coordinator mapping, and neither exists in the code yet.

## Sources

- Inkless checkout: `/Users/viktor.somogyi-vass/dev/github/aiven/workspace-inkless/diskless-metadata`, branch `svv/diskless-metadata`. Paths are relative to it.
- Kafka checkout: `/Users/viktor.somogyi-vass/dev/github/apache/kafka`, commit `27d8bbc35e`. Paths prefixed with `kafka:` are relative to it.
- v2 design: [`design.md`](../../diskless-metadata-coordinator-v2/design.md), cited by section.
- The ticket links `docs/inkless/proposals/diskless-metadata-coordinator-v3/research/kip-1164-produce-gateways.md`. Commit `9187910153` deleted that file when it replaced the gateway effort's folder with this map's copy. The findings read it from commit `8a86a2c404`, with `git show 8a86a2c404:docs/inkless/proposals/diskless-metadata-coordinator-v3/research/kip-1164-produce-gateways.md`. The links in this map to "the gateway effort" in `docs/inkless/proposals/diskless-metadata-coordinator-v3/` now resolve to this map's own copy.

## 1. Selection key and transformer inputs

### Facts

Callers:

- `KafkaApis` builds one transformer per broker from `SharedState.metadata()` and `config().clientAzListenerMap()` (`core/src/main/scala/kafka/server/KafkaApis.scala:130`).
- It transforms every `Metadata` response (`KafkaApis.scala:998`) and every `DescribeTopicPartitions` response (`KafkaApis.scala:1026`), for every client. No condition on the client ID guards either call.

The selection:

- Both public methods skip non-diskless topics and build one `AliveBrokerSnapshot` per response from `MetadataView.getAliveBrokerNodes(listenerName)` (`storage/inkless/src/main/java/io/aiven/inkless/metadata/InklessTopicMetadataTransformer.java:92-100`, `:527-539`).
- A partition routes to replicas when it has more than one replica, its topic has remote storage, or its classic prefix lives only on local disks (`InklessTopicMetadataTransformer.java:106-110`). Otherwise it takes the legacy path (`:119-122`). `transformDescribeTopicResponse` duplicates this logic (`:144-176`).
- The managed path picks with `selectByHash(topicId, partitionIndex, candidates)` (`:514-519`). The legacy path picks inside `selectLeaderLegacy` (`:419-452`). Both sort the candidates by broker ID, hash `String.format("%s-%s", topicId, partitionIndex)` as UTF-8 with `Utils.murmur2`, and index with `Utils.toPositive(hash) % size` (`:448-451`, `:515-518`).
- The candidate lists are the branches of the fallback order: the replicas in the client AZ, the brokers in the client AZ, the replicas in any AZ, then any broker (`:335-410`). With remote storage, the last two branches don't exist, and the partition becomes unavailable (`:356-363`).
- The transformer keeps the controller's leader epoch (`:207-208`).

Transformer inputs:

- Its only inputs are `MetadataView` and `clientAzListenerMap` (`InklessTopicMetadataTransformer.java:61-73`). It doesn't see the control plane, `inkless.control.plane.class`, or any other config.
- `MetadataView` offers alive brokers, broker count, topic ID, topic flags, topic config, and the diskless partition sets (`storage/inkless/src/main/java/io/aiven/inkless/control_plane/MetadataView.java:29-56`). It has no partition count for a topic and no partition registration.
- `InklessMetadataView` wraps `KRaftMetadataCache` and reads `currentImage()` for partition registrations (`core/src/main/scala/kafka/server/metadata/InklessMetadataView.scala:35`, `:64`, `:141-144`). `KRaftMetadataCache.numPartitions(topicName)` reads the partition count from the image (`metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java:426-428`). Upstream `BrokerMetadataPublisher` passes `__consumer_offsets`'s count to the group coordinator with the same call (`kafka:core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala:359`), the pattern v2 names for `N` (`design.md`, Coordinator partitions).
- `getAliveBrokerNodes` drops only fenced brokers. A broker in controlled shutdown stays in the list (`KRaftMetadataCache.java:398-402`). `BrokerRegistration` exposes `inControlledShutdown()` and `rack()` (`metadata/src/main/java/org/apache/kafka/metadata/BrokerRegistration.java:260-270`). The list also contains only brokers that expose the request's listener.
- The client AZ comes from `diskless_az=` in the client ID, then from `clientAzListenerMap` for the request's listener (`InklessTopicMetadataTransformer.java:461-475`). `ClientAZExtractor` returns `null` for an AZ that matches no known rack, after it tries stripping trailing `-` segments (`storage/inkless/src/main/java/io/aiven/inkless/metadata/ClientAZExtractor.java:42-76`).
- A search for `__diskless_metadata` in Java and Scala sources finds nothing. The code doesn't define the topic, a coordinator-partition mapping, or a gateway assignment.

Coordinator-partition mapping:

- v2 maps a user partition with `Utils.toPositive(Utils.murmur2(logKey)) % N`. The log key is the 16 bytes of the topic ID, most significant bits first, then the partition index as 4 big-endian bytes. The mapping is part of the protocol, and a fixed-value test pins it (`design.md`, Coordinator partitions).
- The transformer's key, the UTF-8 string `"<topicId>-<partitionIndex>"`, uses the same hash function on a different input. Reusing it doesn't produce the coordinator partition.

### Inferences

- The change touches both helpers and both public methods, or one shared helper that both call. Each branch of the managed path, and the legacy path, needs a rule for where gateways come in.
- Reading `N` needs one new `MetadataView` method backed by `KRaftMetadataCache.numPartitions`. The assignment from ticket 21 is a function of the image only. The misroute metric that ticket 21 asks for runs on the produce path in `storage/inkless`, which doesn't hold the transformer. Both paths hold `SharedState`. So the assignment fits a component that `SharedState` exposes, which the transformer and the produce path both read.
- Gateway routing must stay off for the Postgres and in-memory backends, and while `__diskless_metadata` doesn't exist. The transformer has no input that names the backend.
- Each metadata response calls `getAliveBrokerNodes` and the topic lookups separately. A response can mix two images. That's harmless today, but an assignment computed per request can then disagree with the broker list in the same response.

## 2. Fetches move with produce

### Facts

- The producer sends to `cluster.leaderFor(partition)` (`kafka:clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java:783`, `:822`). The consumer fetches from the leader or a preferred read replica (`kafka:clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractFetch.java:371`, `:583`). Both read the same rewritten `LeaderId`.
- Today the hash maps a partition to one broker per AZ, and all clients in that AZ send to it. [CLIENT-BROKER-AZ-ALIGNMENT.md](../../../docs/inkless/CLIENT-BROKER-AZ-ALIGNMENT.md) relies on this for cache locality in "Caching Architecture" and "Broker Selection Algorithm."
- The uploading broker caches the whole file after the upload succeeds. `FileCommitter` submits a `CacheStoreJob` per file (`storage/inkless/src/main/java/io/aiven/inkless/produce/FileCommitter.java:237-245`), and the job stores aligned extents under the object key (`storage/inkless/src/main/java/io/aiven/inkless/produce/CacheStoreJob.java:53-84`).
- The cache is a local `CaffeineCache` per broker (`storage/inkless/src/main/java/io/aiven/inkless/common/SharedState.java:151-156`). Entries live 60 seconds by default (`storage/inkless/src/main/java/io/aiven/inkless/config/InklessConfig.java:95-97`).
- A hot fetch reads through the cache with `computeIfAbsent` on the same aligned keys (`storage/inkless/src/main/java/io/aiven/inkless/consume/FetchPlanner.java:255-271`). A consolidation fetch peeks into the cache and doesn't fill it on a miss (`FetchPlanner.java:303-320`).
- `AppendCompleter` puts each committed batch's coordinates into the broker's local `BatchCoordinateCache` (`storage/inkless/src/main/java/io/aiven/inkless/produce/AppendCompleter.java:83-95`). `DelayedFetch` decides early completion of a long poll from that cache alone (`core/src/main/scala/kafka/server/DelayedFetch.scala:186-199`). It contains only batches this broker produced (`core/src/main/scala/kafka/server/ReplicaManager.scala:2437-2465`).

### Inferences

- Per-AZ alignment holds. The key changes, but produce and fetch still read one `LeaderId` per partition and AZ.
- Tail reads still hit the uploader's cache when the producer and the consumer of a partition are in the same AZ. Both reach the gateway that uploaded the file. Data produced in another AZ misses, as it does today. Each user partition still has its data in about one file per AZ per tick, so the number of objects a tail read touches stays the same.
- Long-poll wakeups depend on the same colocation, and they keep it.
- The spread changes. Today brokers in an AZ share the user partitions by hash. With gateways, they share `N` coordinator partitions. At `N=8` with 3 brokers per rack, the brokers hold 3, 3, and 2 coordinator partitions: 1.125 and 0.75 times the mean. A hot coordinator partition makes one broker per AZ hot for produce and fetch together.
- With `max(1, B / N / n_racks)` gateways per rack, rounded down, `B` from 25 to 47 at `N=8` and 3 racks gives one gateway per coordinator partition per rack. Some brokers in each rack then aren't a gateway for any coordinator partition, and routed clients don't send them produce or fetch. At `B=30`, that's 2 of 10 brokers per rack. Ticket 21 has to settle this.
- At two or more gateways per rack, the choice among a coordinator partition's gateways needs a second key. Keyed by user partition, it keeps produce and fetch together, because both still read one `LeaderId`.
- A producer and a consumer with different metadata ages can reach different brokers for one partition until both refresh. That costs cache hits, not correctness.

## 3. Managed replicas with remote storage

### Facts

- Diskless produce has no leader or replica check. `ReplicaManager.appendRecords` sends diskless entries to `AppendHandler` on any broker (`core/src/main/scala/kafka/server/ReplicaManager.scala:1003-1020`). Only a pending switch answers `REPLICA_NOT_AVAILABLE` (`:1005-1011`).
- With remote storage, the transformer routes only to replicas. It prefers a same-AZ replica, falls back to a cross-AZ replica, and otherwise reports the partition unavailable (`InklessTopicMetadataTransformer.java:335-363`). The code comment gives the reason: tiered reads need a replica with a `UnifiedLog`. A tiered partition with one replica also takes this path (`:46-53`, `:110`).
- A switched partition whose classic prefix lives only on local disks routes only to alive ISR members (`:56-59`, `:284-299`).
- A consolidating partition serves fetches from the local log through `getPartitionOrError` (`ReplicaManager.scala:2718-2729`).
- Without remote storage, a managed partition can fall back to any broker in the client's AZ (`InklessTopicMetadataTransformer.java:376-384`). The non-replica path already exists there.
- Managed replicas use the standard rack-aware `ReplicaPlacer` ([FEATURES.md](../../../docs/inkless/FEATURES.md), Managed Replicas, Behavior). The placer doesn't read coordinator partitions.
- [Design weaknesses and mitigations](../../diskless-produce-gateways/research/design-weaknesses.md) recommends aligning replica placement with coordinator partitions as cells. Its section on routing conflicts says the north-star page plans a cellular placement KIP.

### Inferences

- A gateway needn't be a replica to accept produce. The constraint comes from fetch. Produce and fetch share `LeaderId`, so a gateway that isn't a replica sends fetches for a tiered or locally prefixed partition to a broker without the local log.
- These topics stay on today's routing unless placement aligns with coordinator partitions. With independent placement and one replica per rack, the replica in a rack equals the gateway of its coordinator partition about `1 / b_r` of the time, where `b_r` is the rack's broker count. At `B = n_racks`, every broker is both, so nothing changes on a 3-broker cluster.
- A third option keeps a replica only when a gateway is a replica, and otherwise keeps today's replica choice. It's correct, and the misroute share for these topics is about `1 - 1 / b_r`.
- Managed partitions without remote storage can route to a gateway that isn't a replica. The cost is the replica preference that today's code uses for deterministic routing.
- At consolidation, replicas read WAL objects with a cache peek. A replica that is also the gateway hits its own upload cache. Aligned placement improves that hit rate, and unaligned placement keeps today's.

## 4. `ShareFetch` and clients without `diskless_az`

### Facts

`ShareFetch`:

- The share consumer sends to `metadata.currentLeader(partition)` (`kafka:clients/src/main/java/org/apache/kafka/clients/consumer/internals/ShareConsumeRequestManager.java:186-188`).
- On the broker, `ShareFetchUtils.partition` throws `NotLeaderOrFollowerException` unless `Partition.isLeader()` holds (`core/src/main/java/kafka/server/share/ShareFetchUtils.java:191-197`). `DelayedShareFetch` reads through it and then through `ReplicaManager.readFromLog` (`core/src/main/java/kafka/server/share/DelayedShareFetch.java:517-518`, `:531-550`).
- `readFromLog` has no diskless branch and reads only the local log (`ReplicaManager.scala:3286` onward). Diskless fetches go through `fetchMessages` instead (`ReplicaManager.scala:2686-2715`).
- The FAQ says "Queues for Kafka are currently not supported in diskless topics" ([FAQ.md](../../../docs/inkless/FAQ.md):207). [FEATURES.md](../../../docs/inkless/FEATURES.md) lists `SHARE_FETCH` as not tested.
- For diskless topics, `KafkaApis.getCurrentLeader` returns leader `-1`, so its error responses contain no leader hint (`KafkaApis.scala:387-391`).

Clients without `diskless_az`:

- With no `diskless_az` and no listener mapping, `resolveClientAZ` returns `null` (`InklessTopicMetadataTransformer.java:461-475`). The transformer still rewrites `LeaderId`, because `KafkaApis` calls it unconditionally.
- Legacy path: the transformer hashes over all alive brokers (`:429-439`).
- Managed path: `filterByAZ` returns an empty list for a `null` AZ (`:501-512`). The selection then picks from all alive replicas (`:347-355`, `:385-393`).
- Tests pin this behavior for a `null` client ID (`storage/inkless/src/test/java/io/aiven/inkless/metadata/InklessTopicMetadataTransformerTest.java:467-545`), for a client ID with no marker on an unmapped listener (`:355-369`), and for an AZ that matches no rack (`:130-135`).

### Inferences

- Gateway routing doesn't break `ShareFetch` further. Today a `ShareFetch` for a diskless partition gets `NOT_LEADER_OR_FOLLOWER` from any broker that isn't the KRaft leader, and the refreshed metadata names the same rewritten broker again. On the KRaft leader, `readFromLog` returns only the local log, which doesn't contain the diskless data. Adding `ShareFetch` support later needs its own routing answer, and KIP-1163 keeps `ShareFetch` on the real leader.
- The broker already routes clients without an AZ, so it can route them to a gateway. Picking among the gateways of the coordinator partition in all racks keeps commit parts low at the same cross-AZ cost as today's hash over all brokers. The ticket's phrase about clients that get the true metadata doesn't match the code, and the follow-up tickets shouldn't count those clients as unroutable.
- The code has no path that bypasses the rewrite, so every client gets a routed `LeaderId`. Administration tools see the rewritten `LeaderId` too.

## 5. How long a client keeps sending to an old gateway

### Facts

Client defaults and triggers in the Kafka checkout:

- `metadata.max.age.ms` defaults to `5 * 60 * 1000` for the producer (`kafka:clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java:494`) and the consumer (`kafka:clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java:483-488`).
- The producer requests a metadata update on any produce error that is an `InvalidMetadataException` (`kafka:clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java:713-730`). `KafkaStorageException`, `NotLeaderOrFollowerException`, `ReplicaNotAvailableException`, and `UnknownTopicOrPartitionException` all extend it (`kafka:clients/src/main/java/org/apache/kafka/common/errors/KafkaStorageException.java:35` and siblings).
- The KIP-951 leader hint in an error response updates the client only for a strictly newer leader epoch (`kafka:clients/src/main/java/org/apache/kafka/clients/Metadata.java:400`). A metadata response with an equal epoch and a new `LeaderId` replaces the cached leader (`Metadata.java:584-588`).
- Any server disconnect, including a request timeout, requests a metadata update (`kafka:clients/src/main/java/org/apache/kafka/clients/NetworkClient.java:967-1009`, `:1497-1518`).
- The producer also refreshes for partitions with an unknown leader (`Sender.java:385-395`). The consumer refreshes on fetch errors such as `NOT_LEADER_OR_FOLLOWER` and `KAFKA_STORAGE_ERROR` (`kafka:clients/src/main/java/org/apache/kafka/clients/consumer/internals/FetchCollector.java:325-340`).
- `broker.session.timeout.ms` defaults to 9000 and `broker.heartbeat.interval.ms` to 2000 (`kafka:raft/src/main/java/org/apache/kafka/raft/KRaftConfigs.java:40-44`).

Broker errors on diskless produce:

- The diskless path never answers `NOT_LEADER_OR_FOLLOWER`. It answers `INVALID_REQUEST` for transactional produce, `KAFKA_STORAGE_ERROR` when `controlPlaneAvailability` rejects the call or a commit fails, and `REPLICA_NOT_AVAILABLE` during a pending switch (`storage/inkless/src/main/java/io/aiven/inkless/produce/AppendHandler.java:100-118`, `storage/inkless/src/main/java/io/aiven/inkless/produce/AppendCompleter.java:117-125`, `ReplicaManager.scala:1005-1011`). The last two trigger a refresh. v2's empty sequence source also fails a file with `KAFKA_STORAGE_ERROR` (`design.md`, Sequence source).

### Inferences

The window depends on why the gateway changed:

| Cause | Old gateway | First refresh trigger | Window |
| --- | --- | --- | --- |
| Broker joins, or the assignment moves for load or leadership | Alive and healthy | `metadata.max.age.ms` | Up to 5 minutes by default |
| Gateway crashes | Unreachable | Disconnect, immediately | Produce doesn't misroute; it stalls until the controller fences the broker, up to about 9 seconds, because unfenced brokers stay in the broker list. Today's hash routing behaves the same. |
| Controlled shutdown | Alive, then gone | Disconnect at shutdown | Until the connection closes. The broker remains a candidate during controlled shutdown, unless ticket 21 excludes it. |
| Commit failure on the gateway | Alive, failing | `KAFKA_STORAGE_ERROR` | One retry. The new metadata is the same unless the assignment moved. |

Sources of misroutes, where a file holds batches for a coordinator partition the broker isn't a gateway for:

1. Stale metadata after an assignment change while the old gateway is alive: up to `metadata.max.age.ms` per client. With a stable assignment such as rendezvous hashing, a join moves about `1 / (b_r + 1)` of the coordinator partitions in the joining rack.
2. A failed gateway: no misroute window. After fencing, the next refresh returns the next gateway, and the fallback order from ticket 21 decides whether that broker is a gateway too.
3. Clients in a lost rack fall back cross-AZ. The fallback is a misroute unless it picks a gateway of the same coordinator partition in another rack. Clients in other racks keep their gateways if the assignment works per rack.
4. The cross-AZ fallback: a client AZ with no alive broker, or an AZ string that matches no rack and resolves to `null`. Each is a misroute unless the fallback picks a gateway.
5. Structural misroutes: topics with remote storage and switched partitions with a local classic prefix, unless placement aligns (section 3).
6. Image skew: brokers that answer metadata from different image offsets compute different gateways for a moment. This source is short-lived, and the findings don't measure it.
7. A gateway that isn't on the client's listener drops out of that client's candidate list (section 1).

The broker can shorten source 1 with existing protocol. It can reject misrouted produce with an error that extends `InvalidMetadataException`, which makes the client refresh at once. That trades a retry per misrouted request for the 5-minute window. The KIP-951 hint can't name the gateway, because the epoch doesn't change.

## Ticket test

The ticket asks for this test: "two user partitions that map to the same coordinator partition get the same broker in each AZ, and two that map to different coordinator partitions follow the assignment."

Facts:

- `InklessTopicMetadataTransformerTest` mocks `MetadataView` under `Strictness.STRICT_STUBS` and builds responses by hand (`InklessTopicMetadataTransformerTest.java:60-70`). A new `MetadataView` method for `N`, or an injected assignment, fits that pattern.
- `transformClusterMetadata` and `transformDescribeTopicResponse` duplicate the selection (`InklessTopicMetadataTransformer.java:80-125`, `:132-177`). The existing tests cover both.
- The code defines neither the coordinator mapping nor the assignment (section 1).

Inferences:

- The test is implementable as a unit test once the mapping and the assignment exist, or with a stubbed assignment. It has to pick user partitions by computing the pinned mapping, or use `N=1` and `N=2` where the cases are easy to construct.
- "The same broker in each AZ" is true only at one gateway per coordinator partition per rack. At `B ≥ 2 × N × n_racks`, 48 brokers at `N=8` with 3 racks, a second key chooses among the gateways, and two user partitions of one coordinator partition can get different brokers. The test needs to say "a gateway of that coordinator partition in that AZ" or fix one gateway per rack.
- The test only holds on the gateway branch. Topics with remote storage, partitions with a local classic prefix, a `null` client AZ, and the cross-AZ fallback follow other rules, and the test needs a case for each to show the rule that applies.
- "Follow the assignment" must assert that each partition gets `assignment(p, az)`. It can't assert that the brokers differ, because below saturation one broker is a gateway for several coordinator partitions.
- Ticket 21's test that every broker computes the same assignment from one image covers the cross-broker half. This test covers only one transformer.

## Unverified

- The 9-second stall after a crash assumes the controller fences the broker only after `broker.session.timeout.ms`. The findings didn't trace the controller's fencing path.
- Whether a fenced broker that keeps its process and network still accepts diskless produce. `AppendHandler` checks control-plane availability, not fencing. A fenced broker keeps its registration, so v2's coordinator wouldn't retire its epoch. That's an edge case for ticket 21's candidate rule.
- How long image propagation skew lasts across brokers.
- Non-Java clients: the refresh triggers come from the Java client only. librdkafka and others may refresh on different errors or intervals.
- The map says that below `N × n_racks` brokers, gateways leave the commit fan-out unchanged. [Writer split against a gateway hint](../../diskless-produce-gateways/research/split-vs-hint.md) gives `k = min(N, ceil(N × n_racks / B))` coordinator partitions per broker below saturation: 2 at `B=12` and `N=8`, against 8 without gateways. Ticket 23 should check which statement holds.
