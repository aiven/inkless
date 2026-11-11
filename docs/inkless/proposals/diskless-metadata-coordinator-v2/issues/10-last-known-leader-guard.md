# Guard for last-known-leader election on `__diskless_metadata`

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: 09

## Question

Does v2 guard the controller's automatic last-known-leader election for `__diskless_metadata`, and if so, how?

[Last-known-leader election on `__diskless_metadata`](09-last-known-leader-election.md) found that the election is live, automatic, and ignores `unclean.leader.election.enable`. With intact storage and `flush.messages=1`, it loses nothing and is the only way a coordinator partition comes back after every replica restarts uncleanly. A broker that returns under the same ID with a wiped or replaced disk is elected with an empty log, and the surviving replicas truncate to match it. Classic topics have the same exposure, with or without ELR.

Settle these points:

- Whether the wiped-disk election is a Kafka-parity accepted risk or a gap that needs a fix.
- If it's a gap, which guard: turning the path off for the topic, invalidating `lastKnownElr` and ELR on a directory ID change, or something else, and what recovers a partition that the guard leaves leaderless.
- Whether the JBOD case, where a broker stays in the ELR after a clean restart with a removed directory, is in scope.
- The tests, and the failure-mode row or accepted-risk entry.

## Resolution

Resolved in a grilling session on 2026-11-10. The wiped-disk election is a gap, not a Kafka-parity risk. v2 keeps automatic recovery and makes storage identity, not shutdown cleanliness, decide whether a replica may lead. Every rule keys on the name `__diskless_metadata`, so other topics keep upstream behavior. The facts behind each rule, with citations, are in [last-known-leader-election.md](../research/last-known-leader-election.md).

### Why it's a gap

A classic topic loses data the same way, so the data-loss bar alone would allow an accepted risk. Two reasons rule that out, as in [Flush policy under Kafka parity](04-flush-policy.md):

- The damage differs in kind. A leader with an empty log makes every follower truncate to offset 0, so the coordinator partition loses its whole batch index, lease table, and deletion queue, and object storage doesn't roll back with it.
- The election destroys copies that survived. The map's accepted risk covers losing every replica, and here another replica can still hold every acknowledged record.

### Decisions

1. Storage change guard. In `ClusterControlManager.registerBroker`, next to the shutdown handler, the controller removes the broker from the ELR and `lastKnownElr` of every `__diskless_metadata` partition whose assigned directory for that replica isn't in the new registration. An `UNASSIGNED` directory counts as changed. `handleAssignReplicasToDirs` applies the same rule when a replica moves off a directory the broker no longer registers. Registration precedes unfencing, so the guard runs before the unfence scan can elect the broker. Formatting always creates a new directory ID, so the guard catches a wiped, replaced, or reformatted disk, and the multi-directory case where a broker restarts cleanly without the replica's directory.
2. ELR across an unclean shutdown. `handleBrokerShutdown` handles `__diskless_metadata` partitions as it handles a clean shutdown: the broker leaves the ISR and joins the ELR if the ISR drops below the minimum. With `flush.messages=1`, every ELR member holds every acknowledged record on disk, and the storage change guard removes the members whose storage didn't survive. The first returning replica wins a clean ELR election. The last-known-leader election stays on as a fallback.
3. Fixed replication factor and minimum ISR.
   - `rfChangeAllowedForTopic` in `ReplicationControlManager.alterPartitionReassignments` is false for `__diskless_metadata`. A reassignment that changes the replica count gets `INVALID_REPLICATION_FACTOR`. One that keeps it passes.
   - A new controller config, `diskless.metadata.topic.min.insync.replicas`, sets the topic-level `min.insync.replicas` at creation. It defaults to `min(2, diskless.metadata.topic.replication.factor)`, and startup validation requires it to be between that default and the replication factor. Changing it after creation has no effect, as with `diskless.metadata.topic.num.partitions`.
   - `ConfigurationControlManager.validateAlterConfig` rejects any change to the topic's `min.insync.replicas` with `INVALID_CONFIG`, and setting its current value passes. The topic-level `ClearElrRecord` therefore never fires for this topic.
4. Cluster-wide ELR clear. A cluster-level or broker-level `min.insync.replicas` change never reaches the topic-level override, so the cluster-wide `ClearElrRecord` skips `__diskless_metadata`. The skip lives in both replays: `ReplicationControlManager.replayClearAllElrs` on the controller and `TopicsDelta` and `TopicDelta.replay()` in the metadata image. Every controller and broker applies it alike, because the topic exists only once the coordinator feature is finalized.
5. Partition directory fsync. After `LogManager.createLogDirectory` creates a `__diskless_metadata` partition directory, the broker calls `Utils.flushDir` on the log directory. A failed fsync takes the log directory offline, as a failed create does. Renames already fsync the parent through `Utils.atomicMoveWithFallback`. The cost is one fsync per partition directory creation, none on the produce path.

Turning the last-known-leader election off for the topic was rejected: after a correlated power loss, every coordinator partition would stay offline with intact disks. Turning ELR off was rejected because the returning broker then wins a clean election. An operator procedure alone was rejected as fragile in the middle of an outage.

### Failure-mode rows

| Failure | Outcome |
| --- | --- |
| The last known leader returns under the same ID with a wiped, replaced, or reformatted disk | The controller removes it from the ELR and `lastKnownElr` at registration. The partition waits for a replica whose storage survived. |
| A multi-directory broker restarts cleanly without the directory that held the replica | Same as the preceding row. |
| Every replica restarts uncleanly with intact storage | They stay in the ELR, and the first one to return wins a clean ELR election with no acknowledged record lost. |
| A broker crashes shortly after it creates a coordinator partition directory | The parent-directory fsync keeps the directory, and recovery finds the log. |
| An operator changes the cluster default `min.insync.replicas` while a coordinator partition has no leader | The cluster-wide ELR clear skips `__diskless_metadata`, and the partition keeps its candidates. |
| A reassignment changes the replica count, or an alter changes `min.insync.replicas` | The controller rejects it with `INVALID_REPLICATION_FACTOR` or `INVALID_CONFIG`. |

### Accepted risks

Each one loses data that a classic topic loses the same way:

- Log recovery truncates fsynced records after a read error or bit rot, under an unchanged directory ID. Procedure: alert on log recovery's invalid-message truncation warning for `__diskless_metadata`, and stop the broker for inspection instead of letting it rejoin.
- A device acknowledges an fsync without persisting it. Procedure: the deployment docs require storage that honors fsync, with no volatile write cache that lacks power-loss protection.
- An operator restores a log directory, `meta.properties` included, from an older copy. Procedure: never restore a broker's log directory from a copy. Reformat it and let the broker re-replicate.
- A stale leader-epoch entry, left by a crash while a truncation's checkpoint write is queued, over-truncates an ELR member, and the leader fails before that member fetches again. Signal: `kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec`, which counts last-known-leader elections, and the controller's ELR election rate. No signal catches the over-truncation itself.

### Tests

- `ReplicationControlManagerTest`:
  - In the shape of `testEligibleLeaderReplicas_UncleanShutdown`, the last known leader re-registers with a new directory ID and unfencing doesn't elect it. With its old directory ID, unfencing elects it.
  - A multi-directory broker that restarts cleanly without the replica's directory leaves the ELR.
  - An unclean shutdown keeps the broker in the ELR for `__diskless_metadata` and removes it for a classic topic.
  - A replica whose assigned directory is `UNASSIGNED` leaves the ELR and `lastKnownElr` at registration.
  - After a storage change, a periodic and an admin `PREFERRED` election on the leaderless partition elect nobody.
  - A reassignment that changes the replica count gets `INVALID_REPLICATION_FACTOR`. One that keeps it passes.
  - A cluster-wide `ClearElrRecord` keeps the ELR of `__diskless_metadata` and clears a classic topic's.
- `TopicsDeltaTest`: the same cluster-wide `ClearElrRecord` case on the image side.
- `ConfigurationControlManagerTest`: changing `min.insync.replicas` gets `INVALID_CONFIG`, and its current value passes.
- `LogManagerTest`: creating a `__diskless_metadata` partition flushes the log directory, and a failed flush takes the log directory offline.
- Controller config validation: `diskless.metadata.topic.min.insync.replicas` outside its range fails startup.
- One `KafkaClusterTestKit` test kills every replica of a coordinator partition uncleanly, restarts them with intact disks, and checks that the partition recovers with every acknowledged record.

### Consequences for other tickets and the rewrite

- [Admin protection for `__diskless_metadata`](06-admin-protection.md): its `min.insync.replicas` rule changes from "at least the bound" to "fixed at creation", and its controller-create bound comes from `diskless.metadata.topic.min.insync.replicas`. Its note that `AlterPartitionReassignments` stays unchanged becomes: a reassignment that keeps the replica count passes. Its `ConfigurationControlManagerTest` cases for at, below, and above the bound give way to the fixed-value cases in this ticket. Its accepted consequence that a partition with a lost ISR stays offline now reads: it stays offline only once every ELR member and the last known leader lost their storage.
- `design.md` and `spec.md`: `Topology` gains the minimum ISR config, the fixed replication factor, and the directory fsync. `Leader change` gains the storage change guard and the ELR rule for unclean shutdowns. Rollout gains the new controller config.
- `INKLESS_OWNERSHIP`: `ClusterControlManager.java`, `ReplicationControlManager.java`, `ConfigurationControlManager.java`, `TopicsDelta.java`, `TopicDelta.java`, and `LogManager.scala` carry Inkless edits when the change lands, as `INTERLEAVED` entries where they aren't already.
