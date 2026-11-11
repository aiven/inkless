# Diskless metadata coordinator v2: map

## Destination

A v2 of the multi-coordinator design, written in this folder as `design.md` and `spec.md`, that closes the gaps a design review found in [v1](../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md). The user copies the folder to `docs/inkless/proposals/v2/` later. The map is done when every gap has a decided fix or a recorded accepted risk, and nothing is left to decide before the rewrite.

## Notes

- Base: v1 as written, with per-object owners, intents, and clock-bounded fences and tombstones. `docs/inkless/proposals/v1/` is a read-only historical copy. Read it, never edit it.
- The single-partition effort in [../diskless-metadata-coordinator/](../diskless-metadata-coordinator/map.md) is an input, not the base. Its multi-partition variant, with sequence leases and a tracking partition, is a candidate design that tickets weigh. Its tickets 03 to 06 hold the detail.
- Earlier v1 decisions can be rewritten, and interfaces such as `ControlPlane`, `FileCommitter`, and `FileCommitJob` can change when that fits the algorithm better.
- Data-loss bar: Kafka parity. The design must survive every failure that a classic topic survives. A failure that a classic topic doesn't survive can be an accepted risk.
- Performance bar: no worse than the Postgres backend. No unnecessary blocking on the produce, commit, or read path, and no change that adds high per-operation complexity.
- A gap is closed by one of two things: a fix, with a failure-mode row and a named test, or an accepted risk, with a metric or an operator procedure. An accepted risk can't lose data beyond what Kafka parity allows.
- Domain: [GLOSSARY.md](../../docs/inkless/GLOSSARY.md). Kafka references: look them up in the local Kafka checkout at `/Users/viktor.somogyi-vass/dev/github/apache/kafka`, not on the web.
- Style for the design and spec: [AGENTS.md](../../AGENTS.md), engineering rules and style section.
- Tracker: local markdown. Tickets live in [issues/](issues/), one file per ticket. A ticket's `Assignee` line is its claim. Research findings live in [research/](research/), because `.scratch/` is ignored by git.
- Grilling tickets call the `grilling` and `domain-modeling` skills and ask one question at a time.
- Don't write `design.md` or `spec.md` until every ticket is closed. The rewrite after the map copies v1's files here and applies the closed tickets.

## Decisions so far

- [Ownership model: intents or leases](issues/01-ownership-model.md): per-broker sequence leases at a tracking partition replace the per-object intent at an owner. Deletion safety no longer depends on broker clocks.
- [What Kafka's internal-topic status protects](issues/05-internal-topic-protections.md): only produce, with an `__admin_client` bypass, and `DeleteRecords` are guarded. Every admin request, including `DeleteTopics` and config alters, has no internal-topic check, so v2 needs its own guards whether or not the topic joins `Topic.INTERNAL_TOPICS`.
- [Broker-side deletion after a partial send](issues/02-delete-after-partial-send.md): the coordinator backend turns off the broker's direct delete with a capability that the check runs before the `ControlPlaneUnavailableException` branch. The tracking partition's queue is the only deletion path there. Tests for both cases go in the shared suite.
- [Time bounds in the chosen model](issues/03-time-bounds.md): no clock decides whether a key is deleted, only when. The idle lease timer becomes a per-broker `CoordinatorTimer` at the tracking partition, the grace period keeps its wall-clock comparison as Postgres does, and retention skew is a Kafka-parity risk with an NTP procedure instead of a skew metric.
- [Flush policy under Kafka parity](issues/04-flush-policy.md): `__diskless_metadata` requires `flush.messages=1`, and v2 drops the weaker-flush fallback without an unsafe mode. In the multi-partition lease model, a lost tail on a participant or a tracking partition can resurrect batch references to an object that the tracking partition's queue already deleted or queued for deletion.
- [Admin protection for `__diskless_metadata`](issues/06-admin-protection.md): the topic joins `Topic.INTERNAL_TOPICS`, and name-keyed guards with no bypass reject `DeleteTopics`, client `CreateTopics`, `CreatePartitions`, unclean `ElectLeaders`, and non-coordinator produce. They also fix five safety configs. [Guard for last-known-leader election on `__diskless_metadata`](issues/10-last-known-leader-guard.md) replaces its `min.insync.replicas` and reassignment rules.
- [Read consistency after a leader change](issues/07-read-consistency.md): a deposed leader serves committed but older state, as in classic Kafka, with no lease and no epoch on private RPCs. Every stale read is lower or unknown, except `findBatches` above the high watermark, which confirms with a barrier record before it answers `OFFSET_OUT_OF_RANGE` and answers `OFFSET_NOT_AVAILABLE` after 500 ms.
- [Record compatibility across broker versions](issues/08-record-compatibility.md): a replica stops applying a coordinator partition at any record type, version, or tag it can't read, and never skips it. The coordinator feature's level gates writes, and the controller refuses every downgrade. Upstream records carry no tagged fields. Inkless adds raw tags at 100 and up through an extension registry, gated by its own feature. The SQLite schema has its own version, so a rollback rebuilds instead of opening a newer file.
- [Last-known-leader election on `__diskless_metadata`](issues/09-last-known-leader-election.md): yes. ELR is on by default, the last-known-leader path is always on and ignores `unclean.leader.election.enable`, and an unclean shutdown never clears `lastKnownElr`. Intact storage loses nothing, but a broker that returns under the same ID with a wiped disk is elected with an empty log. Turning ELR off doesn't help.
- [Guard for last-known-leader election on `__diskless_metadata`](issues/10-last-known-leader-guard.md): storage identity decides who may lead. The controller drops a broker from the ELR and `lastKnownElr` when its replica's directory isn't in its registration, and keeps it in the ELR across an unclean shutdown. The replication factor and `min.insync.replicas` are fixed at creation, the latter from a new controller config, the cluster-wide ELR clear skips the topic, and the broker fsyncs the log directory after it creates a partition directory.
- [Check against the Postgres performance bar](issues/12-postgres-performance-bar.md): no decision adds blocking or a high per-operation cost to the normal path, but ticket 07's 500 ms barrier deadline needs a per-operation timeout instead of the runtime-wide write timeout, the barrier wait holds a synchronous fetch metadata thread, and ticket 10's registration guard must scan only the topic's partitions.
- [Consequences for the cross-cutting sections](issues/11-cross-cutting-consequences.md): each parked `findBatches` gets its own 500 ms deadline from arrival with no upstream change, `findBatches` stays synchronous with an asynchronous call as the review group's alternative, and the delayed-fetch probe never runs the barrier. Failure modes become a fix table with named tests and an accepted-risk table with signals, metrics split into a runtime group and a backend group, tests take four layers, and the owner model's rows, metrics, tests, and stories leave or become lease-model ones.

## Not yet specified

## Out of scope

- An off-broker copy of coordinator state, a restore procedure, and versioned buckets or soft-delete as cover for losing every replica of a coordinator partition. A classic topic loses data the same way, so v2 records this as an accepted risk, with its blast radius of 1/N of diskless partitions and their orphaned WAL objects.
- A replay checker and an inspect-and-repair tool for a deterministic apply bug that corrupts every replica alike. Kafka's coordinators share this limitation, so v2 records it as an accepted risk. The record compatibility gate stays in scope, in [Record compatibility across broker versions](issues/08-record-compatibility.md).
- The benchmark question and the capacity risk in v1's [Capacity and isolation](../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#capacity-and-isolation).
- The single-partition direction of the earlier effort.
