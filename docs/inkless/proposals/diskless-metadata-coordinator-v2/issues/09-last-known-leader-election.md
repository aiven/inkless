# Last-known-leader election on `__diskless_metadata`

Type: research
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

Can the controller's automatic eligible leader replicas (ELR) recovery elect a leader for a `__diskless_metadata` partition whose log is missing acknowledged records?

[Admin protection for `__diskless_metadata`](06-admin-protection.md) rejects admin `UNCLEAN` elections and fixes `unclean.leader.election.enable=false`. `PartitionChangeBuilder` has a separate path: when the ISR and ELR are both empty, `canElectLastKnownLeader` elects `lastKnownElr[0]` and marks the result unclean, on the online election path as well as the unclean one (`metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java:257`, `:280`, `:297-325`).

Find, in the Inkless checkout and the local Kafka checkout at `/Users/viktor.somogyi-vass/dev/github/apache/kafka`:

- When `eligibleLeaderReplicasEnabled` and `useLastKnownLeaderInBalancedRecovery` are true in the fork, and which feature level or config controls them.
- Which callers reach `canElectLastKnownLeader`, and whether any of them reads `unclean.leader.election.enable`.
- Whether the last known leader can hold fewer records than the high watermark it acknowledged, given `flush.messages=1`: for example after an unclean shutdown, a lost or replaced log directory, or a directory ID change. Cite how `uncleanShutdownReplicas` and directory assignments clear ELR and `lastKnownElr`.
- What a per-topic guard would look like, if one is needed, such as `setUseLastKnownLeaderInBalancedRecovery(false)` for `__diskless_metadata`.

## Resolution

Resolved by research on 2026-11-10. The findings, with citations, are in [last-known-leader-election.md](../research/last-known-leader-election.md).

Yes, it can. On a cluster formatted at the fork's latest production metadata version, `eligible.leader.replicas.version` is 1. `useLastKnownLeaderInBalancedRecovery` is always `true`, because no production code calls its setter and no config or feature controls it. No caller reads `unclean.leader.election.enable` before the last-known-leader check, and the check runs on the `ONLINE` and `PREFERRED` paths. Broker unfencing rescans every leaderless partition, so the election is automatic.

- With `flush.messages=1` and intact storage, the last known leader holds every acknowledged record on disk, and the election loses nothing. This is the recovery that brings a coordinator partition back after a correlated power loss.
- An unclean shutdown removes a broker from the ELR but never from `lastKnownElr`. Unregistration, an offline directory, and `AssignReplicasToDirs` don't clear it either. A broker that returns under the same ID with a wiped or replaced disk gets a new directory ID, reassigns the replica by itself, and is then elected with an empty log. The surviving replicas truncate to match it.
- Turning ELR off doesn't close the gap. The last ISR member stays in the ISR and wins a clean election, with nothing marking it unsafe.
- Guard options: turn the path off for `__diskless_metadata` at the seven classic `PartitionChangeBuilder` sites, or keep it and drop a broker from ELR and `lastKnownElr` when its replica's assigned directory isn't in its new registration.

The choice between them is a decision, in [Guard for last-known-leader election on `__diskless_metadata`](10-last-known-leader-guard.md).
