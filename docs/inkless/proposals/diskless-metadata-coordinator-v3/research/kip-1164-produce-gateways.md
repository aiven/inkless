# Produce gateways in KIP-1164 and KIP-1163

Input for the [v3 map](../map.md), read on 2026-11-11.

Sources:

- [KIP-1164: Diskless Coordinator](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783984/KIP-1164+Diskless+Coordinator), sections "WAL file management" and "Reducing multi-DC commit operations."
- [KIP-1163: Diskless Core](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core), the section on client routing and the `MetadataResponse` schema.

## Summary

- KIP-1164 treats produce gateways as an optimization that lowers the number of commit calls per WAL object. Correctness doesn't depend on them.
- Routing is a recommendation. KIP-1163 adds `PreferredProduceBrokers` to `MetadataResponse` version 14, and producers send to those brokers "in order and subject to availability."
- KIP-1164 keeps its shared-ownership list, its handoff, and its orphan scanner alongside gateways. Any broker can still receive a produce request for any partition, so a WAL object can still span coordinators.
- The benefit reaches one commit call per object only at the saturation point, `n_brokers = n_dcs × n_racks`.

## Why the KIP wants gateways

KIP-1164 states the problem:

> Any broker can handle a Produce request for any Diskless partition. As mentioned above, this opens the possibility for a WAL file to contain data from partitions belonging to different Diskless coordinators. In this case, the broker will have to commit this file against multiple DCs. This situation is undesirable because in the worst case committing one WAL file will result in `n_dcs` (number of DCs) outbound network commit calls, which increases the chances for partial failure and higher tail latencies.

Co-location of coordinators on one broker bounds the calls by `n_brokers`, which the KIP still calls "bad enough."

## The gateway mechanism

> For each Diskless coordinator, one or several brokers in each rack will be selected as "produce gateways". This means, when a producer needs to send a Produce request to a Diskless partition managed by this DC, it'll be encouraged to send it to particular brokers depending on the rack, not just to any broker.

The number of gateways per coordinator per rack is `max(1, n_brokers / n_dcs / n_racks)`. The KIP's scenarios, with 3 racks and 12 coordinators:

| Brokers | Gateways per DC per rack | DCs per broker | Commit calls per object, upper bound |
| ------- | ------------------------ | -------------- | ------------------------------------ |
| 3       | 1                        | 12             | `n_brokers`                          |
| 6       | 1                        | 6              | `n_brokers`                          |
| 12      | 1                        | 3              | `n_brokers`                          |
| 24      | 1                        | 1.5, so 2      | 2                                    |
| 36      | 1                        | 1              | 1                                    |
| 108     | 3                        | 1              | 1                                    |

The KIP's arithmetic for the 108-broker scenario divides by 18 instead of 108. The result of one coordinator per broker is still correct.

The KIP lists two further optimizations:

- Below the saturation point, gateways mirror coordinator co-location: "if DCs dc1, dc2, dc3 are co-located on some broker, their produce gateways should also be co-located on some brokers in other racks." That lowers the upper bound from `n_brokers` to `n_racks`.
- A commit to a coordinator on the same broker isn't a network call, and "DC can be its own produce gateway in its own rack."

The KIP doesn't define how brokers compute the assignment, how it reacts to broker changes, or how it balances load.

## What the KIP keeps despite gateways

WAL file management assumes that an object can span coordinators:

> When a broker is about to commit a file, it gathers the IDs of all the DCs it's going to send the commit requests to into the list of owners. The list of owners is randomly permuted.

The first owner holds the object, hands it to the next owner once its last batch is gone, and the last owner deletes it. An orphan scanner deletes objects that no coordinator knows, after a grace period. v2 replaces both with per-broker sequence leases at a tracking partition, and it rejects a scanner that deletes objects.

## Routing in KIP-1163

KIP-1163 adds version 14 of `MetadataRequest` and `MetadataResponse`. Clients send `client.rack`, and the response marks a topic `IsDiskless` and adds a partition field:

```json
{ "name": "PreferredProduceBrokers", "type": "[]int32", "versions": "14+", "ignorable": true, "entityType": "brokerId",
  "about": "The ordered list of brokers to which the client is recommended to send Produce requests." }
```

Newer clients send `Produce` to the preferred brokers, `Fetch` to the leader or the closest replica, and `ShareFetch` to the leader. Older and third-party clients can append `diskless_rack_id=<rack_id>` to their client ID. The broker then rewrites `LeaderId` to a replica in that rack, which breaks `ShareFetch`, because only the real leader serves it.

## What Inkless has today

`InklessTopicMetadataTransformer` (`storage/inkless/src/main/java/io/aiven/inkless/metadata/InklessTopicMetadataTransformer.java`) implements the client ID fallback with the `diskless_az` key:

- It rewrites `LeaderId` for every diskless partition in `Metadata` and `DescribeTopicPartitions` responses.
- It selects a broker in the client's AZ by `murmur2("<topicId>-<partitionIndex>")` over the sorted candidates, so routing is deterministic per partition and AZ.
- With managed replicas and remote storage enabled, it routes only to assigned replicas, because remote log reads need a `UnifiedLog`. Without remote storage, it falls back to any broker in the client's AZ, then to cross-AZ brokers.

The transformer has no notion of coordinator partitions. Gateway routing through it means changing the selection key from the user partition to its coordinator partition. Because clients use `LeaderId` for both produce and fetch, that change also moves fetches.

## Position in v1 and v2

v1 and v2 both list "Produce gateways and `PreferredProduceBrokers`" as out of scope, with no recorded reason. v2's request splitting and per-queue `FileCommitter` already tolerate objects that span coordinator partitions, so gateways would only lower the number of parts per file.
