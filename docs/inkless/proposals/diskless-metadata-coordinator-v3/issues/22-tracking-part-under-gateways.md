# Tracking part under gateways

Type: grilling
Status: open
Assignee:
Blocked by: 21

## Question

At saturation, does a file keep a second commit part for the broker's tracking partition, or does the tracking partition follow the gateway assignment?

In v2's [Commit path](../../diskless-metadata-coordinator-v2/design.md#commit-path), every file sends a part to its tracking partition, `Utils.toPositive(Utils.murmur2(brokerId)) % N`, and the part is empty when the tracking partition holds none of the file's batches. With gateways at saturation, a broker's files touch about one coordinator partition, and it matches the tracking partition with a probability of `1 / N`. A file then costs about 2 parts and `2R` log flushes instead of 1 part and `R`. See [Fsync and checkpoint cost per commit](12-fsync-checkpoint-cost.md) for the flush count.

Settle these points:

- The options:
  - Accept the second part.
  - Make the broker's tracking partition one of its gateway coordinator partitions. Below saturation, a broker is a gateway for several coordinator partitions, so say which one it picks.
- For the second option, how a change in the assignment maps to the switch to a new partition and the lease release in [Lease lifecycle at the broker](02-lease-lifecycle.md). Say whether [Model of the fence, floor, and release protocol](17-protocol-model.md) must cover switches driven by the assignment, which occur more often than fallbacks.
- The parts and flushes per file at saturation for the chosen option, as input for [Gateway benchmark](23-gateway-benchmark.md).

## Resolution
