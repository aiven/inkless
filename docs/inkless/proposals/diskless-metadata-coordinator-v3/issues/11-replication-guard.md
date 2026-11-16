# Enforced replication factor and rack spread

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: none

## Question

Does v3 enforce replication factor 3 and rack spread for `__diskless_metadata` with a guard instead of a deployment-docs procedure?

v2's [Topology](../../diskless-metadata-coordinator-v2/design.md#topology) makes the replication factor a config with a default of 3, fixed at creation, and places replicas across racks "where possible". The accepted-risk row for losing every replica relies on the deployment docs requiring 3 replicas across racks. Ada says parity holds only if that is enforced. See the council's [third status row](../reviews/council.md#status-of-the-earlier-findings) and [sixth fix](../reviews/council.md#what-would-close-the-gaps).

Settle these points:

- Whether the controller refuses to create the topic, or the feature refuses to finalize, below replication factor 3, and how a single-broker test or development cluster still runs.
- Whether the controller requires distinct racks for each coordinator partition's replicas when `broker.rack` is set, what it does when racks are fewer than the replication factor, and what a reassignment that breaks the spread gets.
- How the guard fits with the reassignment and `min.insync.replicas` rules from v2's [Guard for last-known-leader election](../../diskless-metadata-coordinator-v2/issues/10-last-known-leader-guard.md).
- The accepted-risk row "Every replica of a coordinator partition is lost", with its signal and procedure. [Object footer and recovery after losing a replica set](09-object-footer.md) deferred the footer recovery to future work, so clause 2's exception for this loss holds only under this ticket's guard. The row also states that losing the KRaft metadata is out of scope, and that in combined mode, losing the replica set can mean losing the whole cluster.

## Resolution

Resolved on 2026-11-13 in a grilling session.

v3 enforces both requirements in the controller. Startup validation sets a minimum replication factor of 3, and the controller checks rack spread at creation and on every reassignment. A gauge covers the spread that decays without a request. Clause 2's exception for losing a replica set therefore holds. v3 has no procedure that brings the coordinator partition back. [Empty restart of a coordinator partition](19-empty-restart.md) designs one and defers it to future work, and the map's Notes relax clause 2 for the partition that stays leaderless.

Facts from the Kafka checkout:

- `StripedReplicaPlacer` puts replicas on racks round-robin from a random starting rack. With fewer racks than the replication factor, it puts more than one replica on a rack but still uses every rack. A broker without `broker.rack` counts as one more rack, and KRaft accepts a mix of brokers with and without a rack.
- `StripedReplicaPlacer` throws `InvalidReplicationFactorException` when fewer brokers are registered than the replication factor, or when no broker is unfenced.
- `ReplicationControlManager` never checks racks on a reassignment. A broker's rack comes from each registration, so a restart can change it.
- Upstream's `offsets.topic.replication.factor` defaults to 3 and accepts 1, and enforces no minimum. Kafka defines test-only switches with `defineInternal`, for example `unstable.feature.versions.enable` in `ServerConfigs`.

### Minimum replication factor

- Controller startup validation rejects a `diskless.metadata.topic.replication.factor` below 3.
- A new internal controller config, defined with `defineInternal` and left out of the docs, lowers the minimum to 1. Tests and local development use it. When the controller creates the topic below 3, it logs a warning, and clause 2's exception doesn't apply to that cluster.
- The rest of v2's [Topology](../../diskless-metadata-coordinator-v2/design.md#topology) stays: the replication factor is fixed at creation, and changing the config afterward has no effect.

Rejected alternatives:

- No override, with development clusters on the in-memory or Postgres backend. It drops v2's single-broker shared-suite runs at `N=1` and `N=8` and slows every coordinator test.
- A warning and a gauge without a minimum. That's a signal, not a guard, so clause 2's exception wouldn't hold.
- A check when the feature is finalized. The feature is finalized at format time, so the check would duplicate startup validation in a second place.

### Rack spread

- Term: maximal spread. A coordinator partition's replicas have maximal spread when they span `min(replication factor, number of distinct racks among registered brokers)` racks. With 3 or more racks, that's one replica per rack. With 2 racks, both are used. With no rack declared, the rule reduces to distinct brokers. The rewrite defines the term in the design's Terms section.
- Creation goes ahead with any number of racks. The placer already produces maximal spread, so creation needs no new placement logic. When there are fewer racks than the replication factor, the controller logs a warning.
- `alterPartitionReassignments` rejects a reassignment of a `__diskless_metadata` partition with `INVALID_REPLICA_ASSIGNMENT` when the target replica set spans fewer racks than the current set and fewer than maximal spread. A move that keeps or widens the spread passes, including a move within a rack to decommission a broker. The check runs after `rfChangeAllowedForTopic`, so a change in replica count still gets `INVALID_REPLICATION_FACTOR` first.
- A broker that re-registers with another rack, and a rack that registers after creation, can leave a partition below maximal spread without any request to reject. A new controller gauge, `CoordinatorPartitionsBelowRackSpread`, counts the partitions below maximal spread. The procedure is a reassignment that restores it.

Rejected alternatives:

- Strict spread, one rack per replica or no creation. It blocks two-zone deployments and single-zone on-premises clusters, where a classic topic has the same spread.
- Rejecting a mix of brokers with and without a rack, as ZooKeeper-era Kafka did. It checks only the brokers present at creation, and it differs from upstream KRaft.
- No gauge, only a warning log. Nothing alerts on log lines, and the accepted-risk row needs a signal.
- Requiring every reassignment to reach maximal spread. Once a new rack registers, it blocks unrelated maintenance.

### Creation

- The active controller tries to create `__diskless_metadata` when it sees at least as many registered, unfenced brokers as the replication factor. It checks on each broker registration and unfencing, and once after it becomes active, until creation succeeds. Coordinator calls before then get v2's retriable error.
- If the first brokers to register share a rack, creation places every replica on that rack, and `CoordinatorPartitionsBelowRackSpread` rises once the other racks register. The deployment docs say to start the first brokers across racks.

Rejected alternatives: a quiet period after the last registration before creation, which adds a startup delay and still guesses. An automatic reassignment once new racks register, which adds a mechanism for a one-time startup case and takes reassignment of the coordinator topic out of operator control.

### Fit with the v2 guards

v2's [Guard for last-known-leader election](../../diskless-metadata-coordinator-v2/issues/10-last-known-leader-guard.md) stays as written:

- `diskless.metadata.topic.min.insync.replicas` keeps its range from `min(2, replication factor)` to the replication factor. At the minimum of 3, its lower bound is always 2, and under the internal override at a replication factor of 1, it's 1.
- The storage-change guard and the ELR rules decide which replica may lead, not where replicas sit, so the rack rules don't touch them.
- KRaft doesn't reassign partitions on its own, and `handleAssignReplicasToDirs` moves a replica only between directories of one broker. The reassignment API is the only hook the rack check needs.
- On a 3-broker cluster, an operator can't decommission a broker without adding a replacement first, because the replica count is fixed. v2 already has this constraint, and the deployment docs state it.

### Failure-mode rows

| Failure | Effect | Recovery | Test |
| --- | --- | --- | --- |
| `diskless.metadata.topic.replication.factor` is below 3 without the internal override | The controller fails startup. | Set the replication factor to 3 or more. | Controller config validation |
| Fewer registered, unfenced brokers than the replication factor | The topic isn't created, and coordinator calls get the retriable error. | The controller creates the topic at the registration or unfencing that reaches the replication factor. | `ReplicationControlManagerTest` |
| Fewer racks than the replication factor | Creation places replicas across every rack and logs a warning. | None needed. | `ReplicationControlManagerTest` |
| A reassignment would lower a partition's spread | The controller rejects it with `INVALID_REPLICA_ASSIGNMENT`. | Pick a target that keeps the spread. | `ReplicationControlManagerTest`, `KafkaClusterTestKit` |
| A broker re-registers with another rack, or a new rack registers | `CoordinatorPartitionsBelowRackSpread` rises. Nothing is rejected. | A reassignment restores maximal spread. | Controller metrics test |

### Accepted risk: every replica of a coordinator partition is lost

- With replication factor 3 and maximal spread enforced, clause 2's exception applies.
- Effect: the storage-change guard drops each returning replica from the ELR, and unclean election stays off, so the coordinator partition `P` stays leaderless. Its user partitions, about 1/N of the diskless partitions, can't be written or read. Objects that `P` tracked leak.
- Clause 3 holds: fences to `P` go unanswered and releases from `P` never arrive, so the objects `P` touched leak rather than get deleted. No other partition's data is deleted or resurrected.
- Signal: `OfflinePartitionsCount` for `__diskless_metadata`.
- Procedure: none in v3. `P` stays leaderless, and about 1/N of every diskless topic, present and future, stays unavailable. The map's Notes accept this under clause 2's exception. [Empty restart of a coordinator partition](19-empty-restart.md) records a procedure that brings `P` back empty, as future work.
- Losing the KRaft metadata is out of scope, because classic topics don't survive it either.
- In combined mode, losing a coordinator partition's replica set can also lose a majority of the controller quorum, and with it the whole cluster. Classic topics go down with it, so clause 2 holds without the exception. The deployment docs recommend dedicated controllers, citing Kafka's guidance that combined mode "should be avoided in critical deployment environments" (`docs/operations/kraft.md:320` in the Kafka checkout). The controller doesn't refuse combined mode, because Kafka itself allows it in production.

Rejected alternative: leaving `P` offline for good, with a new cluster as the procedure. Recreating an affected topic gives it a new topic ID, and about 1/N of its partitions map to `P` again. So 1/N of every diskless topic, present and future, would stay unavailable. A classic topic that loses its replica set can be deleted and recreated, or brought back empty through an unclean election, so leaving `P` offline would fail clause 2 even with the exception. After this ticket closed, [Empty restart of a coordinator partition](19-empty-restart.md) deferred the restart, and the map's Notes relax clause 2 to accept `P` staying offline in v3.

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

- Controller config validation: a replication factor below 3 fails startup without the internal override and passes with it.
- `ReplicationControlManagerTest`:
  - The controller attempts creation at each registration, at each unfencing, and on activation, and it succeeds once the registered, unfenced brokers reach the replication factor.
  - Placement spans `min(replication factor, number of racks)` racks with 1, 2, 3, and 4 racks, and with no rack declared.
  - A reassignment that narrows the spread gets `INVALID_REPLICA_ASSIGNMENT`. A move within a rack passes, and a move that widens the spread passes.
  - A reassignment that changes the replica count still gets `INVALID_REPLICATION_FACTOR`.
- Controller metrics: `CoordinatorPartitionsBelowRackSpread` counts the partitions below maximal spread after a broker re-registers with another rack and after a new rack registers.
- `KafkaClusterTestKit`: on a 3-rack cluster, the controller creates the topic with one replica per rack, and a reassignment that narrows the spread fails through `Admin`.

### Consequences for the rewrite

- `design.md` Topology: the minimum replication factor, the internal override, maximal spread, the reassignment check, and the creation trigger replace "places replicas across racks where possible". Metrics gains `CoordinatorPartitionsBelowRackSpread`. Accepted risks replaces the row for losing every replica, and the Terms section defines maximal spread.
- `spec.md`: story 4's development cluster with one replica goes through the internal override. Story 6 becomes: "As an operator, I want each coordinator partition's replicas spread across as many racks as the replication factor allows, enforced by the controller, so that losing one rack doesn't take out every copy."
- The deployment docs state the 3-broker decommission constraint, starting the first brokers across racks, and the recommendation for dedicated controllers.
- Open items for the implementer gain the name of the internal override config.
- `INKLESS_OWNERSHIP`: `ReplicationControlManager.java` and the controller metrics class carry Inkless edits, as `INTERLEAVED` entries where they aren't already.
