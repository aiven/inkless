# Diskless metadata coordinator v3: map

## Destination

An exploration of one alternative to v2's object tracking: single-coordinator objects, kept affordable by produce gateways. The map is done when every ticket is closed and the map records one verdict with the numbers behind it. The verdict is one of these: the variant replaces v2's tracking partition, it stays a documented alternative for the design review group, or it's rejected. Only a replace verdict leads to a v3 `design.md` and `spec.md`.

## Notes

- Base: [v2](../diskless-metadata-coordinator-v2/spec.md) as written, with its [design record](../diskless-metadata-coordinator-v2/design.md) and [decision log](../diskless-metadata-coordinator-v2/map.md). The v2 folder is read-only. Read it, never edit it.
- Terms used in this map and its tickets:
  - Coordinator partition: a partition of `__diskless_metadata`, as in v2. KIP-1164 calls it a Diskless coordinator (DC).
  - Single-coordinator object: a WAL object whose batches all map to one coordinator partition. The broker's writer guarantees it by construction, whatever the client does.
  - Produce gateway: a broker that routing sends producers to for the user partitions of one coordinator partition. Each coordinator partition has one or more gateways per rack.
  - Saturation point: the cluster size at which every broker is a gateway for at most one coordinator partition, `n_brokers = N × n_racks` in KIP-1164's model.
- Correctness never depends on routing. A produce that reaches a non-gateway broker can cost more objects or more commit calls, but it can't break an invariant. A ticket that needs routing for safety reopens this note.
- Data-loss bar and performance bar: the same as in v2's [Notes](../diskless-metadata-coordinator-v2/map.md#notes).
- Cost bar: every variant states its object storage requests per second and its cross-AZ bytes per second at the cluster sizes in [Cost model](issues/05-cost-model.md). The variant doesn't have to win on cost, but the verdict has to state what it costs.
- Public protocol: none, as in v2, unless [Routing mechanism](issues/03-routing-mechanism.md) argues that the variant needs KIP-1163's `PreferredProduceBrokers`.
- Kafka references: look them up in the local Kafka checkout at `/Users/viktor.somogyi-vass/dev/github/apache/kafka`, not on the web. The KIP texts are on the Apache wiki. [`kip-1164-produce-gateways.md`](research/kip-1164-produce-gateways.md) keeps the quotes this map relies on.
- Domain: [GLOSSARY.md](../../GLOSSARY.md). Style: [AGENTS.md](../../../../AGENTS.md), engineering rules and style section.
- Tracker: local markdown, as in v2. Tickets live in [issues/](issues/), one file per ticket, and a ticket's `Assignee` line is its claim. Research findings live in [research/](research/).
- Grilling tickets call the `grilling` and `domain-modeling` skills and ask one question at a time.

## Starting point

These findings come from a first pass over KIP-1164 and v2, before any ticket ran. They're inputs, not decisions. [`kip-1164-produce-gateways.md`](research/kip-1164-produce-gateways.md) and [`cost-first-pass.md`](research/cost-first-pass.md) hold the detail.

- In KIP-1164, produce gateways are a routing hint, not a correctness mechanism. Clients are "encouraged" to produce to them through KIP-1163's `PreferredProduceBrokers` Metadata field. The KIP keeps its owner list, its handoff, and its orphan scanner, because any broker can still receive a produce request for any coordinator partition.
- As a hint alone, gateways don't simplify v2. Multi-coordinator objects still happen, so every rule in v2's [Object tracking and deletion](../diskless-metadata-coordinator-v2/spec.md#implementation-decisions) must stay. Gateways only reduce the commit parts per file, and v2 still sends a separate part to the broker's tracking partition.
- Gateways become a simplification when the writer also splits its buffer by coordinator partition. Every object then has one participant, so the tracking partition, participant sets, release reports, silent-participant fences, the missed-sequence fence across all `N` partitions, and partial file outcomes could all leave the design. Each coordinator partition behaves like v2 at `N=1`. [Delta against v2](issues/07-delta-against-v2.md) checks this claim.
- Topic protection, `flush.messages=1`, snapshots and their distribution, the ELR storage-identity guard, record compatibility, the read barrier, the overlay on `CoordinatorRuntime`, lifecycle reconciliation, maintenance, consolidation, and rollout look unaffected.
- Cost moves from coordinator complexity to object storage requests, not from inter-broker traffic. v2 never moves record data between brokers. A broker that writes for `k` coordinator partitions uploads `k` objects per commit interval instead of one. Gateways keep `k` near 1, but only at or above the saturation point, which is 24 brokers for `N=8` and 3 racks.

## Tickets

- [01: Single-coordinator objects in the writer](issues/01-writer-split.md): how the broker guarantees that every object maps to one coordinator partition.
- [02: Leases, keys, and deletion for single-coordinator objects](issues/02-lease-and-deletion-model.md): what remains of v2's lease, fence, and release rules.
- [03: Routing mechanism](issues/03-routing-mechanism.md): how clients reach gateways, with or without public protocol.
- [04: Gateway assignment](issues/04-gateway-assignment.md): how brokers agree on which broker is a gateway for which coordinator partition.
- [05: Cost model](issues/05-cost-model.md): object storage requests, commit calls, and cross-AZ bytes per variant and cluster size.
- [06: Coordinator partition count](issues/06-partition-count.md): how `N` relates to cluster size when gateways only pay off at saturation.
- [07: Delta against v2](issues/07-delta-against-v2.md): which v2 records, RPCs, metrics, failure modes, and tests leave, change, or stay.
- [08: Verdict](issues/08-verdict.md): replace, keep as an alternative, or reject.

## Decisions so far

None yet.

## Not yet specified

## Out of scope

- Every v2 decision that the variant doesn't touch. [Delta against v2](issues/07-delta-against-v2.md) confirms the list.
- Benchmarks. The cost model is arithmetic over list prices and the writer's triggers, not a measurement.
- Changing `N` after creation, beyond what [Coordinator partition count](issues/06-partition-count.md) needs to state the dependency.
- Everything v2 lists as out of scope, other than produce gateways and preferred produce brokers.
