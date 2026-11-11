# Cost first pass

Input for the [v3 map](../map.md) and the starting point for [Cost model](../issues/05-cost-model.md). These numbers are back-of-envelope arithmetic, not measurements.

## Summary

- Single-coordinator objects trade coordinator complexity for object storage PUT requests. They don't trade inter-broker traffic, because no variant moves record data between brokers.
- The PUT penalty applies to uploads triggered by the commit tick. Uploads triggered by a full buffer cost few extra PUT requests.
- Cross-AZ metadata traffic is two orders of magnitude cheaper than the PUT penalty in the example that follows.

## Assumptions

- Writer defaults: `produce.commit.interval.ms=250` and `produce.buffer.max.bytes=8388608` (8 MiB), from [`configs.rst`](../../../configs.rst).
- S3 Standard list prices, to verify in [Cost model](../issues/05-cost-model.md): $0.005 per 1,000 PUT requests, $0.0004 per 1,000 GET requests, and $0.01 per GB for cross-AZ transfer in each direction.
- A broker under steady load uploads one object per commit tick, which is 4 objects per second.
- A commit part carries about 100 bytes per batch, and a file holds 1,000 batches.

## Where the traffic goes

In every variant, a producer sends records to a broker in its own AZ, and the broker uploads them to object storage. Inter-broker traffic is coordinator metadata only:

- Commit parts from the uploading broker to coordinator partition leaders.
- `__diskless_metadata` replication, which carries the same records in every variant.
- `findBatches` lookups that miss the batch coordinate cache.

## PUT requests

A broker that writes for `k` coordinator partitions uploads `k` objects per commit tick instead of one.

- One extra object per tick is 4 PUT requests per second, or 345,600 per day. That costs $1.73 per day, about $630 per year, per broker and per extra coordinator partition.
- Example: 3 brokers with `N=8`, below the saturation point, so each broker writes for all 8 coordinator partitions. Each broker uploads 7 extra objects per tick, which costs about $13,200 per year on top of about $1,900 per year for one object per tick.
- At or above the saturation point, gateways keep `k` near 1, and the penalty comes only from misrouted produce requests.

The buffer limit changes this. A file for one coordinator partition fills 8 MiB within one 250 ms tick once that partition's traffic on the broker exceeds 32 MiB/s. Above that rate, the buffer triggers the uploads in every variant, and splitting by coordinator partition adds few PUT requests. The penalty lands on light and moderate load.

## Commit calls and cross-AZ bytes

v2 sends one commit part per coordinator partition that the object touches, plus one part to the tracking partition. Single-coordinator objects send one part per object. The number of parts is about the same without gateways, because v2's parts already split by coordinator partition. It drops to one per object with gateways at saturation.

With the assumptions here, commit parts carry about 100 KB per file. At 4 files per second, that's about 400 KB/s, or 34.6 GB per day, per broker. With 3 racks, about two thirds cross an AZ, which is about 23 GB per day. Billed in both directions, that costs about $0.46 per day per broker.

## GET requests

Not estimated yet. A consumer that reads partitions across coordinator partitions reads more objects when objects don't mix coordinator partitions, so GET requests rise. GET requests cost about a twelfth of a PUT request, and the broker's object cache absorbs many of them. [Cost model](../issues/05-cost-model.md) covers them.
