# Gateway assignment

Type: grilling
Status: open
Assignee: unassigned
Blocked by: [03](03-routing-mechanism.md)

## Question

How does each broker compute which brokers are gateways for which coordinator partition, and how does that assignment behave when brokers join, leave, or run hot?

KIP-1164 picks `max(1, n_brokers / N / n_racks)` gateways per coordinator partition per rack. It suggests that coordinator partitions co-located on one broker get co-located gateways in the other racks, which lowers the upper bound on commit calls per file from `n_brokers` to `n_racks`. It also notes that a coordinator partition's leader can be its own gateway, which makes the commit local. The KIP doesn't define the algorithm.

Settle these points:

- The inputs: the metadata image only, so every broker computes the same answer without an RPC, or something that coordinator leaders publish.
- Whether gateways follow coordinator partition leadership, so the gateway in the leader's rack commits locally, or stay independent of it so a leader change doesn't move produce traffic.
- Stability: how many partitions change gateway when one broker joins or leaves, and whether rendezvous or consistent hashing keeps that small.
- Load: a coordinator partition with hot user partitions makes its gateways hot. Say whether the assignment weighs load or only counts, and what an operator can do about a hot gateway.
- Below the saturation point: how the assignment degrades, and whether co-location mirroring is worth its complexity.
- Racks with uneven broker counts, and a rack with no broker.

## Resolution
