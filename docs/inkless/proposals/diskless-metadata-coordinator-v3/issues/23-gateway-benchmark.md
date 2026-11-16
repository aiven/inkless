# Gateway benchmark

Type: grilling
Status: open
Assignee:
Blocked by: 16, 21, 22

## Question

Which benchmark shows what gateways save, and at which cluster sizes are they turned on?

Gateways reduce commit parts only at or above the saturation point. Below it, every broker stays a gateway for most coordinator partitions. See [Gateway assignment](21-gateway-assignment.md) and [Benchmark pass criteria](16-benchmark-pass-criteria.md).

Input, which corrects [Fsync amplification and default N on small clusters](14-small-cluster-fsync.md#relation-to-the-gateway-effort): the cluster-wide fan-out isn't flat as `B` grows. With `max(1, B / N / n_racks)` gateways per rack, each leader receives about `B / N` parts per round, against about `B` without gateways. The cost per file stays flat, and the cluster-wide count still grows with `B`. Gateways give `N` times more headroom before a cluster outgrows its `N`, and they don't remove that limit.

Settle these points:

- Workloads: `B` of 3, 12, 24, and 48, at `N=8` with 3 racks, with gateways on and off, and clients that send `diskless_az` against clients that don't.
- Metrics: commit parts per file, coordinator log flushes per broker per round from `LogFlushRateAndTimeMs`, P99 `FileCommitTime`, and the misroute share from [Gateway assignment](21-gateway-assignment.md).
- The threshold against gateways turned off that passes.
- Whether gateways are on by default, on above a broker count, or turned on by an operator config.

## Resolution
