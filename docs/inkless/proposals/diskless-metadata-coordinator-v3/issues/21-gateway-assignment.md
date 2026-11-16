# Gateway assignment

Type: grilling
Status: open
Assignee:
Blocked by: 20

## Question

How does every broker compute the same gateways for each coordinator partition and rack, and how does the assignment behave when brokers join, leave, or run hot?

KIP-1164 gives each coordinator partition `max(1, B / N / n_racks)` gateways per rack, and the saturation point `B = N × n_racks`, 24 brokers at `N=8` with 3 racks. It doesn't define the algorithm, how the assignment reacts to broker changes, or how it balances load. See [`kip-1164-produce-gateways.md`](../../../docs/inkless/proposals/diskless-metadata-coordinator-v3/research/kip-1164-produce-gateways.md).

Settle these points:

- Inputs: the metadata image only, so every broker computes the same answer with no RPC. Say which brokers count: registered, unfenced, and not in controlled shutdown.
- Algorithm: for example, rendezvous hashing over the brokers of each rack, keyed by coordinator partition and broker ID. Below saturation, each broker is a gateway for about `N × n_racks / B` coordinator partitions. Say how evenly the algorithm spreads them.
- Leadership: whether a coordinator partition's leader is preferred as the gateway in its own rack, so that its commits are local, or whether gateways stay independent of leadership, so that an election doesn't move produce traffic.
- Stability: how many coordinator partitions change gateway when one broker joins or leaves.
- Fallback order: what the next choice is after the gateway, for example another gateway for the same coordinator partition, then any broker in the rack, then another rack.
- Uneven racks, a rack with no broker, and brokers with no rack.
- Load: whether the assignment counts only brokers or weighs load, and what an operator can do about a hot gateway.
- Below saturation, whether KIP-1164's co-location mirroring is worth its complexity.

Metric: the share of files that hold batches for a coordinator partition this broker isn't a gateway for, so that misroutes and their cost show up.

Named tests:

1. Every broker in a cluster test computes the same assignment from the same image.
2. A broker that joins or leaves moves only the coordinator partitions that the algorithm bounds.
3. A produce to a broker that isn't a gateway for any of its partitions commits. The existing tests already accept any mix of coordinator partitions in a file, so this test confirms them.

## Resolution
