# Cost model

Type: research
Status: open
Assignee: unassigned
Blocked by: none

## Question

What does each variant cost in object storage requests, commit calls, and cross-AZ bytes, across the cluster sizes and loads Inkless runs at?

[`cost-first-pass.md`](../research/cost-first-pass.md) gives back-of-envelope numbers for one point. The verdict needs a model that covers the range, with its assumptions stated.

Model four variants:

- v2 as written, with no routing change.
- v2 with gateways as a hint, and with the tracking partition set to the broker's gateway coordinator partition.
- Single-coordinator objects without gateways.
- Single-coordinator objects with gateways.

Over these inputs:

- Brokers: 3, 6, 12, 24, and 48, with 3 racks.
- `N`: 1, 4, 8, and 16.
- Throughput per broker, from light load, where the commit tick triggers every upload, to heavy load, where `produce.buffer.max.bytes` triggers them. With the defaults of 250 ms and 8 MiB, the crossover is 32 MiB/s per file.
- The routing miss rate from [Routing mechanism](03-routing-mechanism.md), once known.

Report for each point:

- PUT requests per second and object size.
- GET requests per second for a consumer reading partitions across coordinator partitions, with the broker's object cache and without it.
- Commit parts per file, and the parts that cross brokers.
- Cross-AZ bytes per second for commit parts, `__diskless_metadata` replication, and cache-miss lookups.
- The produce latency effect of waiting for the slowest of several parts or several files.

Use current S3 and GCS list prices, and record the date you read them.

## Resolution
