# Delta against v2

Type: research
Status: open
Assignee: unassigned
Blocked by: [01](01-writer-split.md), [02](02-lease-and-deletion-model.md)

## Question

Which parts of v2 leave, change, or stay under single-coordinator objects with gateways?

The map's [Starting point](../map.md#starting-point) claims that the cross-partition object tracking leaves and most other sections stay. This ticket checks that claim item by item against v2's [spec](../../diskless-metadata-coordinator-v2/spec.md) and [design record](../../diskless-metadata-coordinator-v2/design.md).

Produce:

- The record types, private requests, and metrics that leave, change, or stay.
- The failure-mode rows and accepted risks that leave or change.
- The user stories and tests that leave, change, or get added. Include the routing and gateway-assignment tests that v2 doesn't have.
- A recheck of v2's [Flush policy under Kafka parity](../../diskless-metadata-coordinator-v2/issues/04-flush-policy.md). Its cases 1 to 3 span partitions and may leave. Case 4, offset reuse after a lost commit record, doesn't depend on objects spanning partitions. Say whether `flush.messages=1` stays required.
- A recheck of v2's [Check against the Postgres performance bar](../../diskless-metadata-coordinator-v2/issues/12-postgres-performance-bar.md) for the writer split from [01](01-writer-split.md).
- The changes to shared broker code: v2 changes only `FileCommitter`, and the variant also changes the writer and possibly the metadata transformer.

## Resolution
