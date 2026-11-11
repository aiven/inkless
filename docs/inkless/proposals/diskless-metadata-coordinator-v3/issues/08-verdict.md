# Verdict

Type: grilling
Status: open
Assignee: unassigned
Blocked by: [04](04-gateway-assignment.md), [05](05-cost-model.md), [06](06-partition-count.md), [07](07-delta-against-v2.md)

## Question

Does single-coordinator objects with gateways replace v2's tracking partition, stay a documented alternative for the design review group, or get rejected?

Weigh:

- The design removed, from [Delta against v2](07-delta-against-v2.md), against the design added in the writer, routing, and gateway assignment.
- The cost at each cluster size, from [Cost model](05-cost-model.md), and the cluster sizes Inkless expects to run.
- The constraint on `N` from [Coordinator partition count](06-partition-count.md), including what splitting does to the variant.
- The map's data-loss, performance, and cost bars.

Record the verdict and its reasons in the map's Decisions so far. If the verdict is replace, list what the v3 `design.md` and `spec.md` take from v2 unchanged.

## Resolution
