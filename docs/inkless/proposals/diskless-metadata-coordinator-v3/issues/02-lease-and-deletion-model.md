# Leases, keys, and deletion for single-coordinator objects

Type: grilling
Status: open
Assignee: unassigned
Blocked by: [01](01-writer-split.md)

## Question

With every object mapped to one coordinator partition, what remains of v2's sequence leases, fences, releases, and deletion queue?

v2 tracks each object at the uploading broker's tracking partition because an object can hold batches for several coordinator partitions. See v2's [Ownership model: intents or leases](../../diskless-metadata-coordinator-v2/issues/01-ownership-model.md) and the Object tracking and deletion section of its [spec](../../diskless-metadata-coordinator-v2/spec.md#implementation-decisions). With single-coordinator objects, the object's own coordinator partition sees every commit and every batch removal for it.

Settle these points:

- Where a broker leases sequences: one sequence space per broker and coordinator partition, leased at that partition, or one per-broker space as in v2. Say which one keeps the floor and the lease check on the same partition, and what the object key looks like.
- Which fences survive. v2 has silent-participant fences, missed-sequence fences at all `N` partitions, and range fences at every partition after a lease ends. Say which ones become local, and which ones disappear.
- Deletion: whether an object enters the deletion queue in the same apply that removes its last batch, as Postgres does, and whether the participant set, release reports, and the release-accepted record leave the design.
- Lease fallback: v2 renews at the next partition when the tracking partition is unavailable. Say whether that rule still has a purpose when an unavailable coordinator partition can't commit its files anyway.
- Whether v2's rule from [Broker-side deletion after a partial send](../../diskless-metadata-coordinator-v2/issues/02-delete-after-partial-send.md) still holds. A single-coordinator object can't be partially committed, but a commit with an unknown outcome still exists.
- Which time-based decisions remain, as input for a recheck of v2's [Time bounds in the chosen model](../../diskless-metadata-coordinator-v2/issues/03-time-bounds.md).
- What happens to a produce that reaches a non-gateway broker. Correctness must not depend on it, so confirm that the rules above hold for any mix of coordinator partitions on one broker.

## Resolution
