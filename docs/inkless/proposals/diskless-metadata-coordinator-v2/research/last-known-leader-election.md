# Last-known-leader election on `__diskless_metadata`

Research for ticket [09-last-known-leader-election](../issues/09-last-known-leader-election.md).

## Summary

Yes, it can. On a cluster formatted at the fork's latest production metadata version,
ELR is on. `useLastKnownLeaderInBalancedRecovery` is always `true`, because no
production code calls its setter. The last-known-leader path is live for every classic
topic. No caller reads `unclean.leader.election.enable` before it takes that path. The
most common trigger is automatic: any broker unfencing rescans every leaderless
partition.

With `flush.messages=1` and intact storage, the last known leader holds every
acknowledged record, so the election loses nothing. The gap is storage that the
controller can't see change. The controller removes a broker from the ELR when the
broker restarts uncleanly, but it never removes that broker from `lastKnownElr`.
Unregistration, a log directory failure, and a directory reassignment don't remove it
either. A broker that comes back with the same ID and a wiped or replaced disk is
elected with an empty log. The other replicas then truncate to match it.

A per-topic `setUseLastKnownLeaderInBalancedRecovery(false)` closes this path. Its cost
is that a coordinator partition stays offline, with no recovery path, after every ISR
and ELR member restarts uncleanly at once. A correlated power loss is that case, and
it's the one `flush.messages=1` exists to survive. Turning ELR off doesn't close the
gap. Without ELR, the returning broker stays in the ISR and wins a clean election.

## Sources

The research uses two local checkouts. Citations use paths relative to each root.

- Inkless: `/Users/viktor.somogyi-vass/dev/github/aiven/workspace-inkless/diskless-metadata`
  at commit `2a412af730`.
- Kafka: `/Users/viktor.somogyi-vass/dev/github/apache/kafka` at commit `27d8bbc35e`.

Unless a citation says Kafka, it refers to the Inkless checkout. Upstream has the same
last-known-leader logic, at different line numbers. Each section notes where it differs.

## When the two flags are true

### `eligibleLeaderReplicasEnabled`

- Every `PartitionChangeBuilder` construction site in `ReplicationControlManager` passes
  `featureControl.isElrFeatureEnabled()` as the last constructor argument
  (`metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:1498`,
  `:1834`, `:2233`, `:2456`, `:2628`, `:2945`, `:3081`, `:3155`, `:3289`). The builder stores
  it at `metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java:126`.
- `FeatureControlManager.isElrFeatureEnabled` returns true when the finalized
  `eligible.leader.replicas.version` is at least `ELRV_1`
  (`metadata/src/main/java/org/apache/kafka/controller/FeatureControlManager.java:477-480`).
  `FeaturesImage.isElrEnabled` is the image-side twin
  (`metadata/src/main/java/org/apache/kafka/image/FeaturesImage.java:80-83`).
- `ELRV_1` has bootstrap metadata version `IBP_4_1_IV0` and depends on `IBP_4_0_IV1`.
  Its `LATEST_PRODUCTION` is `ELRV_1`
  (`server-common/src/main/java/org/apache/kafka/server/common/EligibleLeaderReplicasVersion.java:24-31`).
  Upstream is identical (Kafka `EligibleLeaderReplicasVersion.java:27`, `:31`).
- The formatter's release version defaults to `MetadataVersion.latestProduction()`
  (`metadata/src/main/java/org/apache/kafka/metadata/storage/Formatter.java:268-282`). Each
  feature it isn't given explicitly gets `defaultLevel(releaseVersion)`
  (`Formatter.java:317-319`). That level is the highest feature version whose bootstrap
  metadata version is at or below the release version
  (`server-common/src/main/java/org/apache/kafka/server/common/Feature.java:190-204`).
- The fork's `LATEST_PRODUCTION` metadata version is `IBP_4_3_IV0`
  (`server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java:125`, `:154`).
  Upstream's is `IBP_4_4_IV2` (Kafka `MetadataVersion.java:163`). Both are past
  `IBP_4_1_IV0`.
- A newly formatted fork cluster with default flags therefore has
  `eligible.leader.replicas.version=1`. On activation, the controller also writes a
  cluster-level `min.insync.replicas` record when ELR is on
  (`metadata/src/main/java/org/apache/kafka/controller/ActivationRecordsGenerator.java:97-104`).
- An upgraded cluster keeps level 0 until an operator raises the feature. Raising it
  generates the ELR safety config records
  (`metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java:803-816`, `:750-775`).

### `useLastKnownLeaderInBalancedRecovery`

- The field defaults to `true` (`PartitionChangeBuilder.java:103-105`). Its only setter is
  `setUseLastKnownLeaderInBalancedRecovery` (`PartitionChangeBuilder.java:186-189`).
- No production code in either checkout calls the setter. The only hits outside tests are
  the declarations (`PartitionChangeBuilder.java:186`; Kafka `PartitionChangeBuilder.java:180`).
- No feature level, topic config, or broker config controls it. The last-known-leader
  path is on wherever ELR is on.

## Callers that reach `canElectLastKnownLeader`

### Where the path sits in the builder

- `build()` runs `maybePopulateTargetElr`, then `tryElection`, then `maybeUpdateRecordElr`,
  then sets the ISR, then `maybeUpdateLastKnownLeader` (`PartitionChangeBuilder.java:437-480`).
- `PREFERRED` calls `electPreferredLeader`, which tries the last known leader after the
  ISR and ELR candidates (`PartitionChangeBuilder.java:238-262`, `:257-259`).
- `ONLINE` and `UNCLEAN` call `electAnyLeader`, which tries the last known leader before
  the `UNCLEAN` branch (`PartitionChangeBuilder.java:267-295`, `:280-282`, `:284-292`).
  The `ONLINE` type reaches it as well.
- `canElectLastKnownLeader` requires four things:
  - Both flags are true (`PartitionChangeBuilder.java:298-302`).
  - The target ISR and target ELR are empty (`:303-307`).
  - `partition.lastKnownElr` has exactly one member (`:317-320`).
  - `isAcceptableLeader` accepts that member (`:321-325`).
- The result is marked unclean (`:258`, `:281`). `tryElection` then sets the ISR to the new
  leader alone and sets `LeaderRecoveryState.RECOVERING` (`:355-362`).
  `maybeUpdateRecordElr` clears the ELR (`:533-536`).
- Upstream has the same code (Kafka `PartitionChangeBuilder.java:246-248`, `:269-271`, `:286-316`).

### Construction sites

The fork constructs `PartitionChangeBuilder` at nine sites, all in
`ReplicationControlManager`. Seven of them can see a classic `__diskless_metadata`
partition:

| Site | Trigger | Election type | Reaches the path for a leaderless partition |
| ---- | ------- | ------------- | ------------------------------------------- |
| `generateLeaderAndIsrUpdates` (`:2938`) | Fencing, unfencing, unregistration, controlled shutdown, registration shutdown handling, directory offline | `ONLINE` | Yes; the main automatic path |
| `maybeTriggerLeaderChangeForPartitionsWithoutPreferredLeader` (`:2621`) | Periodic `electPreferred` task | `PREFERRED` | Yes |
| `electLeader` (`:2449`) | Admin `ElectLeaders`; periodic `electUnclean` task | `PREFERRED` or `UNCLEAN` | Yes for admin `PREFERRED` |
| `changePartitionReassignment` (`:3148`) | Admin `AlterPartitionReassignments` | `ONLINE` | Yes |
| `cancelPartitionReassignment` (`:3074`) | Admin reassignment cancel | `ONLINE` | Yes |
| `handleAssignReplicasToDirs` (`:3282`) | Broker `AssignReplicasToDirs` RPC | `ONLINE` | Yes |
| `alterPartition` (`:1491`) | Leader's `AlterPartition` | `ONLINE` | No in practice |

The other two sites only handle diskless partitions:

- `SetClassicPrefixInRemoteStorage` (`:1825`) runs only for a switching topic, and only
  when the requester is the current leader (`:1800-1808`).
- `expandIsrForDisklessManagedPartitions` (`:2226`) skips every non-diskless topic (`:2208`).

Upstream has the seven classic sites (Kafka `ReplicationControlManager.java:1146`, `:1696`,
`:1863`, `:2146`, `:2266`, `:2332`, `:2415`). The two diskless sites are Inkless additions.

The following details apply to each site:

- `generateLeaderAndIsrUpdates` has these callers:
  - `handleBrokerFenced` (`:2145-2156`).
  - `handleBrokerUnregistered` (`:2168-2176`).
  - `handleBrokerUnfenced`, which scans `brokersToIsrs.partitionsWithNoLeader()` (`:2190-2196`).
  - `handleBrokerInControlledShutdown` (`:2261-2270`).
  - `handleBrokerShutdown` (`:2279-2291`).
  - `handleDirectoriesOffline` (`:2305-2330`).

  The leader acceptor excludes `brokerToRemove` and `brokerWithUncleanShutdown`. It
  accepts `brokerToAdd` even while that broker is still fenced (`:2922-2924`).
  `BrokersToIsrs` indexes every leaderless partition under `NO_LEADER`, whatever its ISR
  (`metadata/src/main/java/org/apache/kafka/controller/BrokersToIsrs.java:103`, `:128-141`,
  `:277-278`). As a result, unfencing any broker re-evaluates every leaderless partition.
- The periodic `electPreferred` task runs when `auto.leader.rebalance.enable` is true
  (`core/src/main/scala/kafka/server/ControllerServer.scala:225-229`;
  `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java:1696-1698`, `:1765-1770`).
  That config defaults to true, every 300 seconds
  (`server/src/main/java/org/apache/kafka/server/config/ReplicationConfigs.java:119`, `:148`).
  A leaderless partition never has its preferred leader, so it stays in
  `imbalancedPartitions` (`ReplicationControlManager.java:611-616`, `:659-665`;
  `metadata/src/main/java/org/apache/kafka/metadata/PartitionRegistration.java:462-464`).
- `electLeader` returns `ELECTION_NOT_NEEDED` only when the partition already has its
  preferred leader (`PREFERRED`) or any leader (`UNCLEAN`) (`ReplicationControlManager.java:2434-2437`).
  An admin `PREFERRED` election on a leaderless partition therefore reaches
  `electPreferredLeader`. The planned `UNCLEAN` rejection for `__diskless_metadata` sits
  before the builder, next to the pending-switch check (`:2438-2443`), so it closes the
  admin `UNCLEAN` route but not the `PREFERRED` one.
- `handleAssignReplicasToDirs` builds with the leader acceptor for the partition
  registration before the directory change (`:3282-3293`). It can still elect when the
  replica's old directory is online.
- `alterPartition` is reachable in code only. `AlterPartition` comes from the current
  leader, and `electAnyLeader` keeps a valid current leader (`PartitionChangeBuilder.java:268-271`).
  `lastKnownElr` is also empty whenever the partition has a leader, because any record
  that elects a leader clears it (`PartitionChangeBuilder.java:523-526`).

### Reads of `unclean.leader.election.enable`

The controller reads the config through
`ConfigurationControlManager.uncleanLeaderElectionEnabledForTopic`. That method resolves
the topic value, then the cluster default, then the static default
(`ConfigurationControlManager.java:690-695`, `:825-831`). Its callers use the result in
these ways:

- `alterPartition` (`ReplicationControlManager.java:1500-1506`),
  `generateLeaderAndIsrUpdates` (`:2947-2953`), and `cancelPartitionReassignment`
  (`:3083-3085`) use it only to pick `Election.UNCLEAN`.
- `cancelPartitionReassignment` also uses it to reject a revert that needs an unclean
  election (`:3067-3072`).
- `maybeTriggerUncleanLeaderElectionForLeaderlessPartitions` is the only caller that
  skips the election entirely when the config is false (`:2671-2685`).

No caller consults the config before the last-known-leader check. That check runs on
the `ONLINE` and `PREFERRED` paths, which don't depend on the config. On the `UNCLEAN`
path, it runs before the unclean branch. `unclean.leader.election.enable=false` on
`__diskless_metadata` doesn't stop it.

## Whether the last known leader can lack acknowledged records

### How `lastKnownElr` is set and cleared

With the two flags on, `lastKnownElr` holds only the last leader. The builder manages it
as follows:

- Set: When a record makes the partition leaderless and `lastKnownElr` is empty, the
  builder stores `[partition.leader]`, the leader before the change
  (`PartitionChangeBuilder.java:518-522`).
- Kept: While the partition stays leaderless, `maybeUpdateRecordElr` keeps the existing
  single entry (`:543-548`) and returns before writing any other value (`:550-552`).
- Cleared: A record that elects a leader clears it (`:523-526`).
- Cleared: `ClearElrRecord` clears ELR and `lastKnownElr` for one topic or for all topics
  (`ReplicationControlManager.java:724-772`). The controller writes one when
  `min.insync.replicas` changes (`ConfigurationControlManager.java:226-246`).

Nothing else clears it. In particular, none of these events touch it:

- Unclean shutdown: `uncleanShutdownReplicas` filters only the target ELR
  (`PartitionChangeBuilder.java:578-581`). The upstream test asserts that an uncleanly
  shut down last ISR member becomes the last known leader
  (`metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java:1386-1390`).
- Unregistration: `handleBrokerUnregistered` iterates only partitions where the broker is
  in the ISR or ELR (`ReplicationControlManager.java:2168-2173`). A partition with an
  empty ISR and ELR isn't visited.
- Directory offline: `handleDirectoriesOffline` handles the broker as `brokerToRemove` for
  partitions on that directory (`:2318-2320`). For a leaderless partition with an empty
  ISR, that doesn't change `lastKnownElr`.
- Directory reassignment: `handleAssignReplicasToDirs` only calls `setDirectory`
  (`:3291`).

### Unclean shutdown detection

- Every broker process generates a new incarnation ID
  (`server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java:101`).
  Registration with a new incarnation always runs the shutdown handler
  (`metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java:438-442`).
- While the old incarnation's session is valid, a new registration fails with
  `DuplicateBrokerRegistrationException` (`ClusterControlManager.java:370-375`). The broker is
  usually fenced before it can register again.
- The controller treats a shutdown as clean only when the request's
  `previousBrokerEpoch` equals the stored broker epoch, and only for request version 3
  or later (`ClusterControlManager.java:440-441`; `QuorumController.java:2157-2160`).
- The broker reads `previousBrokerEpoch` from the clean shutdown file in every
  configured log directory. It reports `-1` when any directory is offline, has no file,
  or disagrees with the others
  (`core/src/main/scala/kafka/log/LogManager.scala:1528-1548`; `BrokerLifecycleManager.java:528`).
  `LogManager` writes the file only during a clean shutdown
  (`LogManager.scala:712-721`). A crash, `kill -9`, power loss, or wiped disk all count
  as unclean.
- With ELR on and an unclean shutdown, `handleBrokerShutdown` passes the broker as
  `brokerWithUncleanShutdown` for its ISR and ELR partitions
  (`ReplicationControlManager.java:2280-2285`). The broker leaves the ISR (`:2960-2961`)
  and the ELR (`PartitionChangeBuilder.java:580`), and it isn't an acceptable leader in that
  pass (`ReplicationControlManager.java:2922-2924`).
- With a clean shutdown, or with ELR off, the controller handles the broker as fenced
  (`:2286-2290`). It leaves the ISR and, if the ISR drops below the minimum, enters the
  ELR (`PartitionChangeBuilder.java:569-581`).
- Upstream matches (Kafka `ReplicationControlManager.java:1532`, `:2159`).

### Directory assignments

- For a classic topic, the leader acceptor requires the replica's assigned directory to
  be online (`ReplicationControlManager.java:3851-3861`, `:3878-3885`;
  `ClusterControlManager.java:811-814`). A directory ID that isn't in the broker's
  registration counts as offline (`server-common/src/main/java/org/apache/kafka/common/DirectoryId.java:125-142`).
- A wiped log directory has no `meta.properties`, so the broker refuses to start until
  someone formats it (`core/src/main/scala/kafka/server/KafkaRaftServer.scala:160-162`).
  Formatting generates a new random directory ID (`Formatter.java:425-433`). Registration
  accepts new directory IDs and checks only that no other broker owns them
  (`ClusterControlManager.java:382-400`).
- Until the broker reassigns the replica, the old directory ID blocks it as a leader. The
  broker reassigns it without operator action:
  - The broker creates a local follower replica even when the partition has no leader
    (`core/src/main/scala/kafka/server/ReplicaManager.scala:4436-4441`).
  - `createLogInAssignedDirectoryId` creates the log when no directory is offline
    (`core/src/main/scala/kafka/cluster/Partition.scala:880-889`). `LogManager` picks a
    directory itself when the target ID is unknown (`LogManager.scala:1036-1056`).
  - The broker sends `AssignReplicasToDirs` when the actual directory differs from the
    assigned one (`ReplicaManager.scala:3935`, `:4552-4562`).
  - The controller then records the new directory (`ReplicationControlManager.java:3282-3293`).
- A directory ID change therefore delays the election but doesn't prevent it, and it
  doesn't clear ELR or `lastKnownElr`.

### Wiped-disk walkthrough

Assume RF 3, `min.insync.replicas=2`, and brokers 1, 2, and 3, with 1 as leader. The
following sequence ends with broker 1 leading on an empty log:

1. Broker 3 is fenced. The ISR becomes `[1, 2]`, which meets the minimum, so the ELR
   stays empty.
2. Broker 2 is fenced. The ISR becomes `[1]` and the ELR becomes `[2]`
   (`PartitionChangeBuilder.java:562-581`).
3. Broker 1 is fenced. The ISR becomes empty, the ELR becomes `[1, 2]`, the partition has
   no acceptable leader, and `lastKnownElr` becomes `[1]`. `ReplicationControlManagerTest`
   shows the same shapes (`ReplicationControlManagerTest.java:1229-1244`, `:1360-1391`).
4. Broker 2 restarts uncleanly, for example after a power loss, and leaves the ELR.
   Broker 1 restarts with a replaced disk. Its restart is unclean too, so it leaves the
   ELR. The ISR and ELR are empty, and `lastKnownElr` stays `[1]`.
5. Broker 1 recreates an empty log and sends `AssignReplicasToDirs`. The next unfence,
   preferred-election pass, admin `PREFERRED` election, or reassignment passes
   `canElectLastKnownLeader` and elects broker 1 with ISR `[1]` and `RECOVERING`.
6. Brokers 2 and 3 become followers of an empty leader at a higher epoch. Follower
   truncation reconciles them with the leader's log
   (`core/src/main/scala/kafka/server/AbstractFetcherThread.scala:237`, `:368-369`).
   They drop the records they kept. This research didn't trace the exact truncation
   offset for a leader whose epoch cache is empty.

If broker 1's disk is intact in step 4, it holds every acknowledged record, and the
same election restores the partition without loss. That case is the one KIP-966's
balanced recovery is for.

A related ELR case doesn't need `lastKnownElr`. On a multi-directory (JBOD) broker,
suppose an operator removes a failed directory from `log.dirs` and restarts the broker
cleanly. Every configured directory then has a clean shutdown file, so the registration
counts as clean (`LogManager.scala:1528-1548`) and the broker stays in the ELR. After it
recreates the replica on another directory, it's a valid ELR leader with an empty log
(`PartitionChangeBuilder.java:329-333`). This research didn't run that scenario.

### What `flush.messages=1` guarantees

The flush setting covers both the leader and the followers:

- Leader: `UnifiedLog.append` holds `lock` from the record append through the flush
  (`storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java:1137`,
  `:1262`, `:1286`). With `flush.messages=1`, `unflushedMessages()` is at least 1 after
  every append, so every append flushes (`storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java:251-253`).
  The flush syncs the segments, syncs the parent directory when the flush includes a new
  segment, and moves the recovery point (`LocalLog.java:261-274`; `UnifiedLog.java:2254-2270`).
- Leader high watermark: `maybeIncrementHighWatermark` takes the same `lock`
  (`UnifiedLog.java:563-580`). The high watermark can't pass a record until the leader's
  flush of it completes.
- Follower: `ReplicaFetcherThread.processPartitionData` appends through the same `append`
  path, so the follower flushes each fetched batch
  (`core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:155`). The fetcher computes
  the next fetch offset after that call returns (`AbstractFetcherThread.scala:377-395`). The
  leader learns a follower's log end offset only from that next fetch, so every offset a
  follower reports is already on its disk.
- High watermark rule: The leader advances the high watermark only when the ISR is at
  least `min.insync.replicas` (`core/src/main/scala/kafka/cluster/Partition.scala:1146-1149`).
  It never moves past the smallest log end offset in the maximal ISR (`Partition.scala:1150-1168`).
  An `acks=all` produce completes only when the high watermark reaches it
  (`Partition.scala:1103-1112`), and the leader rejects `acks=all` appends below the
  minimum ISR (`Partition.scala:1374-1379`).

Together, these points guarantee the following, assuming the coordinator's appends
complete only at the high watermark and the device honors fsync:

- Every acknowledged record is on disk on the leader and on every ISR member at the time
  the high watermark passed it.
- An ELR member was in the ISR when the ISR dropped below the minimum, and the high
  watermark doesn't advance below the minimum. So an ELR member holds every acknowledged
  record on disk.
- `lastKnownElr[0]` was the leader, so it holds every acknowledged record on disk.
- An unclean shutdown with intact storage loses no acknowledged record on any of these
  replicas.

`flush.messages=1` doesn't guarantee the following:

- The controller trusts a crash with intact storage. ELR removal on unclean shutdown
  happens regardless of the flush setting.
- A replica keeps its data when its storage changes outside Kafka. A wiped, replaced,
  or reformatted disk, a restore from an older copy of the log directory, and a device
  or hypervisor that acknowledges fsync without persisting all lose data the controller
  can't see.
- This research didn't trace log recovery after an unclean restart, or the durability
  of the leader-epoch checkpoint.

## A per-topic guard

### Existing per-topic hooks

- Every classic construction site already calls two per-topic helpers:
  - `leaderAcceptorFor(topicName, partition)` (`ReplicationControlManager.java:3851-3861`).
  - `getTopicEffectiveMinIsr(topicName)` (`:3350-3356`).
- The fork already special-cases elections per partition. A pending classic-to-diskless
  switch disables `Election.UNCLEAN` at `:1500-1506` and `:2947-2953`, and rejects or skips
  unclean elections at `:2438-2443` and `:2666-2669`.
- `isSystemTopic` is a name check: `Topic.isInternal` plus `__cluster_metadata` and
  `__remote_log_metadata` (`:172-179`, `:1094-1096`). Its only uses keep a topic classic.
- No non-test source file references `__diskless_metadata` yet. A search outside
  `.scratch` finds the name only in `docs/inkless/proposals/`.

### Option A: turn off the last-known-leader path for the topic

Set `setUseLastKnownLeaderInBalancedRecovery(false)` for `__diskless_metadata` at the seven
classic sites: `:1491`, `:2449`, `:2621`, `:2938`, `:3074`, `:3148`, and `:3282`. A
single helper keeps this consistent, for example a method that takes the topic name and
returns a configured builder. The guard has these effects:

- `canElectLastKnownLeader` returns false (`PartitionChangeBuilder.java:298-302`).
- `maybeUpdateLastKnownLeader` does nothing (`:519`).
- `lastKnownElr` instead holds the real last-known ELR members (`:552-556`, `:583-589`).
  No KRaft election reads that list when the flag is false.
- The builder warns that the field's meaning changes when the flag flips (`:309-316`).
  It ignores a multi-member list if the flag returns to true (`:317-320`). Setting the
  flag from topic creation avoids a flip. On an upgraded cluster, a partition that
  already has a single-entry `lastKnownElr` still can't use it, because the flag check
  runs first.

With the guard, a partition whose ISR and ELR are empty stays leaderless:

- The topic has `unclean.leader.election.enable=false`, so the periodic unclean task
  skips it (`ReplicationControlManager.java:2671-2685`).
- The admin `UNCLEAN` election is rejected.
- A `PREFERRED` election, a reassignment, or an unfence finds no candidate.
- Changing `min.insync.replicas` writes `ClearElrRecord`, which empties `lastKnownElr`
  but elects nobody (`ConfigurationControlManager.java:226-246`).
- No path to a leader exists without a code change or a new operator tool.

That outcome matches the accepted consequence in
[Admin protection for `__diskless_metadata`](../issues/06-admin-protection.md): a coordinator
partition whose whole ISR is lost stays offline. It widens "lost" to "every ISR and ELR
member restarted uncleanly," including when every disk is intact.

### Option B: keep the path, and invalidate it on a storage identity change

For `__diskless_metadata` partitions, drop a broker from ELR and `lastKnownElr` when
its replica's assigned directory isn't in its new registration. Two places can do this:

- `ClusterControlManager.registerBroker`, next to the shutdown handler
  (`ClusterControlManager.java:382-400`, `:438-442`).
- `handleAssignReplicasToDirs`, when the old directory isn't one of the broker's
  registered directories (`ReplicationControlManager.java:3282-3293`).

This option relies on formatting generating a new directory ID (`Formatter.java:425-433`).
It keeps balanced recovery after a correlated crash with intact disks. It also covers
the JBOD case, where a broker stays in the ELR. It doesn't cover a restored copy of
`meta.properties` or an fsync the device didn't persist. It adds Inkless logic to
`INTERLEAVED` controller files. The invalidation also has to happen before the next
unfence scan can elect the broker.

### Option C: turn off ELR

Setting `eligible.leader.replicas.version=0`, or passing `false` for this topic's
builders, doesn't close the gap:

- Without ELR, the builder never empties the ISR, so the last ISR member stays in it
  (`PartitionChangeBuilder.java:455-463`).
- An unclean shutdown is handled as a fence (`ReplicationControlManager.java:2286-2290`).
- When the broker returns, `isValidNewLeader` accepts it from the ISR (`PartitionChangeBuilder.java:329-333`).
  The election is clean, and the controller doesn't mark the partition `RECOVERING`.
- A wiped broker is elected the same way, and nothing records that the election was
  unsafe.
- The cluster-wide version also changes every classic topic.

## Inkless history

`git log` on `PartitionChangeBuilder.java` shows one Inkless commit:
`ae2d2d49fb feat(inkless:controller): add the SetClassicPrefixInRemoteStorage RPC`. It adds
the `classicPrefixInRemoteStorage` flag (`PartitionChangeBuilder.java:107-110`, `:201-204`,
`:450-451`, `:482-493`) and doesn't change ELR or election logic. The ELR and
last-known-leader code comes from upstream commits:

- `505e5b3eaa KAFKA-15584: Leader election with ELR`, the only commit that touches
  `setUseLastKnownLeaderInBalancedRecovery`.
- `c162d2eb14 KAFKA-19522: avoid electing fenced lastKnownLeader`.
- `162db130f6 MINOR: simplify last known elr update`.
- `04a77120c9 MINOR: Move the ELR enabled check to the PartitionChangeBuilder constructor`.

Two Inkless commits touch `lastKnownElr` elsewhere:

- `00701557d1` threads it through `PartitionRegistration` changes.
- `7830390e21` makes the classic-to-diskless switch reject a partition with a non-empty
  ELR or `lastKnownElr` (`ReplicationControlManager.java:3554-3564`).

Neither commit changes when the controller elects the last known leader.

## Implications for the design

The decision waits on these facts and choices:

- Whether a coordinator partition may go permanently offline after a correlated unclean
  shutdown of every replica. Option A turns that event into an outage with no recovery
  path. `flush.messages=1` exists so that this event loses nothing
  ([Flush policy under Kafka parity](../issues/04-flush-policy.md)). Without a guard, the
  last-known-leader path is what brings the partition back.
- Whether an operator reusing a broker ID with a wiped or replaced disk is a supported
  procedure in the target deployments. If it is, the topic needs Option A or Option B.
  Without a guard, that procedure can elect an empty leader and truncate the surviving
  replicas. This is a cross-partition invariant break for the coordinator, not only a
  lost tail.
- Whether a directory ID is an acceptable stand-in for storage identity. Option B depends
  on it and misses restored `meta.properties` files.
- Whether v2 adds an operator escape hatch for a leaderless coordinator partition. With
  Option A and the `UNCLEAN` rejection, the topic has none. An explicit, audited
  last-known-leader election could fill that role. Its cost is new admin surface.
- Whether the JBOD case, where a broker stays in the ELR after a clean restart with a
  removed directory, is in scope. Option A doesn't cover it. Option B does.
- Option C (turning ELR off) doesn't address any of these cases and makes the wiped-disk
  election look clean.

Whichever option the design takes, these tests follow from the findings:

- A `ReplicationControlManagerTest` case in the style of
  `testEligibleLeaderReplicas_UncleanShutdown` (`ReplicationControlManagerTest.java:1360-1391`).
  It ends with the last known leader returning with a new directory ID and checks
  whether unfencing elects it.
- A case that runs the periodic `PREFERRED` pass and an admin `PREFERRED` `ElectLeaders`
  request on a leaderless `__diskless_metadata` partition.

## Durability across an unclean restart

A proposed design keeps a replica in the ELR across an unclean shutdown when its log
directory ID doesn't change. It depends on two facts. Log recovery must keep every fsynced
record, and the leader-epoch cache must match the log after a crash. This section checks
both facts and the follower fsync order. Citations use the checkouts in [Sources](#sources).
Upstream has the same logic. Each subsection notes the upstream line numbers.

### Log recovery keeps fsynced batches

A broker loads a log after a crash in these steps:

- `LogManager` treats a log directory as uncleanly shut down when it has no clean shutdown
  file. It deletes the file before it loads any log, so a crash during recovery triggers
  recovery again (`core/src/main/scala/kafka/log/LogManager.scala:450-457`).
- It reads `recovery-point-offset-checkpoint` for each directory. An unreadable file resets
  every recovery point to 0, and a partition missing from the file gets 0
  (`LogManager.scala:459-466`, `:345`). Both cases only widen the scan.
- `LogLoader.recoverLog` recovers every segment from the one that contains the recovery
  point to the end of the log
  (`storage/src/main/java/org/apache/kafka/storage/internals/log/LogLoader.java:456-492`).
  `LogSegments.values(from, to)` starts at the segment that includes `from`
  (`LogSegments.java:178-194`), so recovery always rescans the active segment.
- `LogSegment.recover` resets the offset, time, and transaction indexes, then walks the
  `.log` batches. Each batch must pass `ensureValid`, which checks the CRC
  (`LogSegment.java:478-509`). The walk stops at the first `CorruptRecordException` or
  `InvalidRecordException`, and the method truncates the file to the valid prefix (`:510-518`).
- `FileLogInputStream.nextBatch` returns no batch when fewer bytes remain than a header or
  the declared batch size. A torn tail ends the walk without an exception, and `recover`
  truncates it the same way. Zero-filled bytes, such as a preallocated tail, fail the
  minimum-size check and throw `CorruptRecordException`
  (`clients/src/main/java/org/apache/kafka/common/record/internal/FileLogInputStream.java:64-94`).

Recovery keeps and drops data by these rules:

- It keeps every complete, CRC-valid batch before the first invalid one. It doesn't drop a
  segment because the segment starts past the recovery point. It scans that segment.
- After the first segment that it truncates, `recoverLog` deletes every later segment in the
  scanned range, valid or not (`LogLoader.java:478-486`).
- If `recoverSegment` throws `IOException` or `InvalidOffsetException`, `recoverLog`
  truncates that segment to its base offset. It then deletes every later segment as well
  (`:473-486`). `InvalidOffsetException` comes from a non-monotonic index append
  (`OffsetIndex.java:157`, `TimeIndex.java:194`), so valid data doesn't reach it. An
  `IOException`, such as `EIO` from a failing sector during the scan, does.
- The new log end offset is the end of the last valid batch. The new recovery point is
  `min(checkpoint, logEndOffset)` (`:506-511`). The epoch cache and producer state then
  follow that log end offset (`:220-245`).

With `flush.messages=1`, every acknowledged record is on disk before the high watermark
passes it, as [What `flush.messages=1` guarantees](#what-flushmessages1-guarantees) shows.
Only unacknowledged bytes follow the last fsync, so the first invalid batch starts at or
after it. The truncation and the later-segment deletion remove only
unacknowledged data, as long as the fsynced bytes read back unchanged.

The specific risks behave as follows:

- Directory entry on a segment roll: `LocalLog.flush` fsyncs the partition directory when it
  flushes a segment whose base offset is at or past the recovery point
  (`LocalLog.java:261-274`). With `flush.messages=1`, the recovery point equals the log end
  offset after each append. A rolled segment's base offset then equals the recovery point.
  The first append into it flushes the segment and the directory before `append` returns
  (`UnifiedLog.java:1286`, `:2254-2271`). The asynchronous flush that `roll` schedules for
  the old segment (`:2218-2221`) has no unflushed data left. Upstream renames the call to
  `Utils.flushPathIfExists` (Kafka `LocalLog.java:273`).
- Recovery point cadence: The in-memory recovery point advances on every flush, after the
  fsync returns (`UnifiedLog.java:2264-2267`; `LocalLog.java:240-246`). The
  `kafka-recovery-point-checkpoint` task writes that value to the file every
  `log.flush.offset.checkpoint.interval.ms` (`LogManager.scala:635-638`, `:845-851`). The
  default is 60 seconds
  (`server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java:116`).
  The file can lag the fsynced data but can't lead it. `LogLoader` keeps the same order on
  purpose (`LogLoader.java:500-505`).
- Stale recovery point: A stale value widens the rescan and costs recovery time. It doesn't
  drop valid data by itself. It does widen the range where a read error or a corrupt older
  batch triggers the truncate-and-delete rules.
- Indexes: `LogSegment.sanityCheck` requires the `.index` file and validates the
  transaction index. It skips the offset and time index checks (`LogSegment.java:180-191`).
  A missing offset index below the recovery point makes `loadSegmentFiles` recover that
  segment (`LogLoader.java:377-386`). Recovery rebuilds indexes from the `.log`, and the
  log end offset comes from reading batches (`LogSegment.java:609-617`). Index files never
  decide which records survive. `loadSegmentFiles` ignores the truncated byte count, so
  unlike `recoverLog`, it doesn't delete later segments after a truncation.
- Producer state: `rebuildProducerState` and `ProducerStateManager.truncateAndReload` read
  segments and delete out-of-range or corrupt snapshot files (`UnifiedLog.java:2534-2603`;
  `ProducerStateManager.java:296-320`, `:354-374`). They never truncate the log. A lost
  snapshot costs a longer scan, or `UNKNOWN_PRODUCER_ID` for an idempotent producer. It
  doesn't cost records.
- Partition directory creation: `LogManager.createLogDirectory` calls
  `Files.createDirectories` and doesn't fsync the parent log directory
  (`LogManager.scala:1116-1122`; Kafka
  `storage/src/main/java/org/apache/kafka/storage/internals/log/LogManager.java:1377-1383`).
  `LocalLog.flush` fsyncs the partition directory, not its entry in the log directory. A file
  system that loses that entry on power loss comes back with no log for the partition, and
  the log directory ID stays the same. The window is the time right after the broker creates
  the replica. This research didn't check file-system journaling behavior.

Upstream `LogLoader`, `LogSegment.recover`, and `LocalLog.flush` have the same logic. The
diffs are imports and renames (Kafka `LogLoader.java:456`, `:476`, `:483`; Kafka
`LogSegment.java:482`).

### Leader-epoch checkpoint durability

The broker writes the checkpoint in these places:

- `CheckpointFile.write` writes a temporary file, fsyncs it, and renames it over the
  checkpoint
  (`server-common/src/main/java/org/apache/kafka/server/common/CheckpointFile.java:74-86`).
  The rename fsyncs the parent directory
  (`clients/src/main/java/org/apache/kafka/common/utils/Utils.java:937-953`). Upstream is
  identical.
- `LeaderEpochFileCache.assign` writes the checkpoint synchronously whenever it adds an entry
  (`storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java:103-118`,
  `:516-523`). For appends, it runs under `UnifiedLog.lock`.
- A new leader assigns its epoch at its log end offset (`Partition.scala:770-784`). Every
  append, on a leader or a follower, assigns each batch's epoch before it writes the batch
  (`UnifiedLog.java:1213-1216`, `:1262`). So the checkpoint records each epoch before the log
  writes any of its batches.
- `truncateFromEndAsyncFlush` and `truncateFromStartAsyncFlush` change memory at once and
  queue the checkpoint write on the scheduler (`LeaderEpochFileCache.java:335-355`,
  `:367-388`). KAFKA-15046 (`d71d0639d9`, in both checkouts) made these two writes
  asynchronous to keep fsync off fetcher threads and out of `UnifiedLog.lock`. Its comment
  states the invariant: the checkpoint never misses an entry and can only hold extra ones
  (`:340-347`). KAFKA-16541 (`3835515fea`) added `writeIfDirExists` for the queued write.
- A checkpoint that fails to parse throws, and the log directory goes offline
  (`CheckpointFile.java:145-178`;
  `storage/src/main/java/org/apache/kafka/storage/internals/checkpoint/CheckpointFileWithFailureHandler.java:73-81`).
  A corrupt file doesn't load as an empty cache.

After an unclean restart, the broker loads the cache in these steps:

- `UnifiedLog.create` builds the cache from the checkpoint (`UnifiedLog.java:331-339`;
  `LeaderEpochFileCache.java:68-75`).
- `LogSegment.recover` assigns an epoch only when a batch's epoch is higher than the latest
  cached one (`LogSegment.java:503-507`). Recovery adds missing higher epochs. It doesn't
  rebuild the cache from the log, and it doesn't remove wrong entries.
- After recovery, `LogLoader` drops entries at or past the recovered log end offset and
  trims entries below the log start offset (`LogLoader.java:220`, `:226`).

The cache on disk can't be behind the log, because every add is synchronous. It can be ahead
of the log, with extra entries from a queued truncation write that didn't reach disk.
Recovery removes an extra entry only when its start offset is at or past the log end
offset. This sequence leaves an extra entry below the log end offset:

1. A follower truncates to offset `T`. Memory drops the entries at or after `T`, and the
   cache queues the checkpoint write.
2. The follower appends batches past the start offset of a dropped entry, all in the latest
   epoch still cached. `assign` doesn't add an entry, so it doesn't rewrite the file
   (`LeaderEpochFileCache.java:120-122`, `:131`).
3. The broker crashes before the queued write runs.

After the restart, the cache records an epoch boundary that the log doesn't contain at that
offset. Step 1 needs divergent history: the replica held batches from an epoch that
the current leader's log doesn't have. An unclean or last-known-leader election can create
that history. Steps 2 and 3 have to fit between queuing the write and the scheduler running
it.

If this replica becomes leader, the extra entry has these effects:

- `endOffsetFor` returns the next entry's start offset as the end of the requested epoch
  (`LeaderEpochFileCache.java:284-327`). An extra boundary makes some epochs end too early.
- A follower whose latest epoch precedes the extra one truncates below records the leader
  still has (`Partition.scala:1530-1560`; `AbstractFetcherThread.scala:643-645`). It then
  fetches them again from the leader. The partition keeps every record while the leader
  stays up.
- A follower that holds real batches from the extra epoch gets an end offset that is too
  late. It keeps those batches at offsets where the leader has different batches. The logs
  diverge, and neither side detects it.

Under the design, the over-truncation matters for the ELR. An election from the ELR sets the
ISR to the new leader alone and keeps the other members in the ELR
(`metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java:341-344`).
A member that over-truncated remains eligible until it fetches again. If the leader fails in
that window, the controller can elect that member without the acknowledged records it
dropped. An over-truncated follower can't rejoin the ISR before it reaches the high
watermark (`Partition.scala:1042-1046`), so the ISR path is safe.

With correct epoch caches, a follower truncates only where its log diverges from the
leader's. The leader and the ISR and ELR members share the same log below the high
watermark, so that point is at or past the high watermark. Followers then drop only
unacknowledged tails.

#### Followers of a leader with an empty log

The [wiped-disk walkthrough](#wiped-disk-walkthrough) left the truncation offset open. A
replica with an empty log that becomes leader assigns `(newEpoch, 0)`
(`Partition.scala:770-784`). Its followers then do the following:

- A follower fetches with `lastFetchedEpoch` set to its latest epoch `e`. `endOffsetFor(e)`
  doesn't find an entry at or below `e` and returns `(e, 0)`
  (`LeaderEpochFileCache.java:305-312`).
- `readRecords` returns a diverging epoch, because `0` is below the fetch offset
  (`Partition.scala:1548-1559`).
- The follower truncates to the lower of its own end offset for `e` and `0`, which is `0`
  (`AbstractFetcherThread.scala:360-369`, `:643-645`).
- If the leader's cache had no entry at all, `readRecords` throws `OffsetOutOfRangeException`
  (`Partition.scala:1537-1540`). The follower then truncates to the leader's log end offset,
  which is also `0` (`AbstractFetcherThread.scala:664-692`).

Either way, every follower drops its whole log and fetches from the empty leader. New records
reuse offsets that earlier records held.

Upstream `LeaderEpochFileCache` differs only in an import and a comment typo, and the
checkpoint classes are identical (Kafka `LeaderEpochFileCache.java:103`, `:335`, `:516`;
Kafka `Partition.scala:651`).

### Follower fsync before the next fetch

The follower path runs in this order:

1. `AbstractFetcherThread` calls `processPartitionData` and computes the next fetch offset
   from its result after it returns (`AbstractFetcherThread.scala:377-397`).
2. `ReplicaFetcherThread.processPartitionData` appends through
   `Partition.appendRecordsToFollowerOrFutureReplica` and `UnifiedLog.appendAsFollower`
   (`ReplicaFetcherThread.scala:155`; `Partition.scala:1319`; `UnifiedLog.java:1080`). That
   path ends in `append`, which flushes inside the call under `flush.messages=1`
   (`UnifiedLog.java:1286`).
3. The next request sends the new offset
   (`core/src/main/scala/kafka/server/RemoteLeaderEndPoint.scala:192`). The leader records it
   as the follower's log end offset (`Partition.scala:1461-1463`).

A failed fsync throws `KafkaStorageException` and takes the log directory offline
(`LocalLog.java:808-822`). The fetcher then marks the partition failed without advancing the
fetch offset (`AbstractFetcherThread.scala:412-415`). A follower reports only offsets it has
fsynced. Upstream has the same order (Kafka `UnifiedLog.java:1229`, `:1276`, `:1300`).

### Verdict

Each fact holds as follows:

- Log recovery is safe with a caveat. With intact storage that honors fsync, recovery keeps
  every acknowledged record. A stale recovery point or a missing index only makes recovery
  slower. Producer state doesn't drop records, and a roll fsyncs the directory. Two rules
  delete valid fsynced data: an `IOException` during the scan truncates that segment to its
  base offset, and any truncation deletes every later segment in the scanned range. A read
  error or bit rot in fsynced data turns either rule into a silent loss of acknowledged
  records. The controller can't see that loss, and the directory ID doesn't change.
- The leader-epoch checkpoint is safe with a caveat. `assign` fsyncs each new entry before
  its batches land, so the cache is never behind the log. Recovery doesn't rebuild the cache.
  It removes only extra entries at or past the log end offset. An extra entry below the log
  end offset remains after a crash only after divergent history and inside the queued-write
  window. That entry makes the leader report early epoch ends. An ELR member can then
  over-truncate, and the controller can elect it before it fetches again. A leader with an empty log makes every follower truncate
  to `0`.
- The follower order holds. A follower fsyncs each batch before its next fetch reports the
  new offset.

Keeping an uncleanly shut down replica in the ELR by directory ID loses acknowledged records
in these cases:

- A read error or corruption during recovery, which truncates fsynced data.
- A lost partition directory entry after replica creation, which loads as an empty log with
  the same directory ID.
- A device that acknowledges fsync without persisting the data.
- An extra epoch entry, followed by a leader failure before the over-truncated ELR member
  fetches again.

The directory ID can't signal the first two cases. Only the broker knows that recovery
truncated below the recovery point checkpoint or didn't find a log for the partition.
