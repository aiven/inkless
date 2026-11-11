# Ownership model: intents or leases

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

Does v2 keep v1's per-object intent at an owner coordinator partition, or move to per-broker sequence leases at a tracking partition?

v1 stores an intent at the owner before every upload. That's a serial, replicated append on the produce path, with `acks=-1` and an fsync under `flush.messages=1`. The Postgres backend has no step before the upload, so the intent breaks the performance bar in the map's [Notes](../map.md#notes). The earlier effort's multi-partition variant removes the intent: a broker leases sequence ranges ahead of time, and its tracking partition decides deletion.

Inputs:

- v1: [File references](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#file-references) and [Commit and retries](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#commit-and-retries).
- The lease model's multi-partition variant: [Tracking partition](../../diskless-metadata-coordinator/issues/03-single-partition-scope.md#tracking-partition), [Fences in the multi-partition variant](../../diskless-metadata-coordinator/issues/04-broker-sequence-model.md#fences-in-the-multi-partition-variant), [Leases in the multi-partition variant](../../diskless-metadata-coordinator/issues/05-sequence-leases.md#leases-in-the-multi-partition-variant), and [Deletion in the multi-partition variant](../../diskless-metadata-coordinator/issues/06-object-deletion.md#deletion-in-the-multi-partition-variant).

Settle these points:

- How each model meets the data-loss bar: every path that deletes an object, and what guarantees no committed batch points into it.
- How each model meets the performance bar: blocking round trips on the produce path, and per-operation cost. For the lease model this includes the missed-sequence fence across all `N` partitions, range fences, and lease fallback. For the intent model, it's the intent round trip and owner fallback.
- How many time-based decisions each model leaves, as input for [Time bounds in the chosen model](03-time-bounds.md).
- Whether the rule from [Broker-side deletion after a partial send](02-delete-after-partial-send.md) fits both models unchanged.
- Which v1 records, RPCs, metrics, and tests each choice adds or drops.

## Resolution

Resolved in a grilling session on 2026-11-09. v2 uses per-broker sequence leases at a tracking partition, and it drops the per-object intent at an owner.

Each broker has one tracking partition. That partition holds the broker's sequence leases, tracks which partitions reference each of the broker's objects, and decides when each object is deleted. The lease model's detail comes from the earlier effort's resolutions in [What a single coordinator partition keeps and drops](../../diskless-metadata-coordinator/issues/03-single-partition-scope.md#multi-partition-variant), [Broker sequence model](../../diskless-metadata-coordinator/issues/04-broker-sequence-model.md#fences-in-the-multi-partition-variant), [Sequence leases and deterministic object keys](../../diskless-metadata-coordinator/issues/05-sequence-leases.md#leases-in-the-multi-partition-variant), and [Object deletion without owners](../../diskless-metadata-coordinator/issues/06-object-deletion.md#deletion-in-the-multi-partition-variant).

Why:

- Performance: the intent is a serial, replicated, fsynced append before every upload, and the Postgres backend has no such step. The lease model adds no round trip on the normal path. Its extra fan-out runs only on failure paths: a fence for a missed sequence and a range fence when a lease ends.
- Data loss: safety comes from ordered sequences and a fenced-through floor, not from broker clocks. The time-based decisions that remain are the idle lease timeout, which collects garbage without guarding safety, and `FileCleaner`'s grace period.
- Records and RPCs: the intent record, the preferred-owner hash, owner fallback, stale-owner handling, predecessor fences, per-key tombstones and fences, the keys-expired record, and the maximum commit age leave the design. The lease table, the sequences-fenced record, the `files` index on broker, epoch, and sequence, the range list in the sequence source, and per-partition idle timers join it.

Consequences for other tickets:

- [Time bounds in the chosen model](03-time-bounds.md) starts from the two remaining time-based decisions above.
- [Broker-side deletion after a partial send](02-delete-after-partial-send.md) keeps its open question. Under leases, every upload already has a sequence that the coordinator resolves, so the backend's `isSafeToDeleteFile` returns false.
- The record, RPC, metric, and test inventory for the lease model goes into the rewrite, as the earlier effort's resolutions list it.

This decision holds at any partition count `N`. At `N=1`, the tracking partition is partition 0 for every broker. The choice of `N` isn't settled here.
