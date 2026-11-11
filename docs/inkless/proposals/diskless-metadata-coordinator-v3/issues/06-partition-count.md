# Coordinator partition count

Type: grilling
Status: open
Assignee: unassigned
Blocked by: [05](05-cost-model.md)

## Question

How should `N` relate to cluster size, when gateways only cut objects per tick at `n_brokers ≥ N × n_racks` and v2 fixes `N` at creation?

v2 defaults `N` to 8, accepts only a power of two, and leaves splitting a coordinator partition as future work. Below the saturation point, single-coordinator objects multiply PUT requests by up to the number of coordinator partitions each broker writes for. At `N=1`, every object is a single-coordinator object already, with no gateways.

Settle these points:

- Whether the operator picks `N` at format time from the expected cluster size, and what the default is for a 3-broker or 6-broker cluster.
- Whether the variant only makes sense above some cluster size, with `N=1` below it.
- What happens when a cluster grows past its saturation point, or shrinks below it, with `N` fixed.
- Splitting. A split of coordinator partition `p` into `p` and `p + N` turns existing single-coordinator objects into objects that span two coordinator partitions. Say whether the variant then needs a handoff or a tracking rule for pre-split objects, and whether that brings back the machinery it removed.

## Resolution
