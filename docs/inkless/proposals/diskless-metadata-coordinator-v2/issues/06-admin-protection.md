# Admin protection for `__diskless_metadata`

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: 05

## Question

How does v2 stop admin requests from damaging `__diskless_metadata`, and which requests and configs does it guard?

v1 rejects only `CreatePartitions` for the topic. `DeleteTopics`, `DeleteRecords`, and config alters aren't covered, and any of them can destroy or weaken coordinator state. [What Kafka's internal-topic status protects](05-internal-topic-protections.md) supplies the facts.

Settle these points:

- Whether `__diskless_metadata` joins `Topic.INTERNAL_TOPICS`, an upstream file edit, or gets Inkless-side checks instead.
- Which requests v2 rejects, and with which errors.
- Which config alters v2 rejects or restricts. Candidates are `min.insync.replicas`, `unclean.leader.election.enable`, `retention.ms`, `retention.bytes`, `cleanup.policy`, and `flush.messages`. The flush setting interacts with [Flush policy under Kafka parity](04-flush-policy.md).
- The tests for each rejection.

## Resolution

[Guard for last-known-leader election on `__diskless_metadata`](10-last-known-leader-guard.md#consequences-for-other-tickets-and-the-rewrite) replaces this ticket's `min.insync.replicas` rule, its reassignment note, and its offline consequence. Read that section before you apply this one.

Resolved in a grilling session on 2026-11-09. `__diskless_metadata` joins `Topic.INTERNAL_TOPICS`, and explicit controller and broker guards cover every request and config that `Topic.isInternal` doesn't guard. No guard has a bypass. The guards key on the name `__diskless_metadata` only, so the three upstream internal topics keep upstream behavior.

### Internal-topic status

`Topic.java` in `clients` adds `__diskless_metadata` to `Topic.INTERNAL_TOPICS`. The topic then inherits the produce and `DeleteRecords` rejections in `ReplicaManager`, the `isInternal` flag in metadata responses, exclusion from `Admin.listTopics()` and client-side regex subscriptions by default, no tiered storage, and system-topic status in `ReplicationControlManager.isSystemTopic`, which keeps it classic. The edit makes `Topic.java` an `INTERLEAVED` entry in `INKLESS_OWNERSHIP`.

### Requests

| Request | Where the check lives | Error |
| --- | --- | --- |
| `DeleteTopics` | `ReplicationControlManager.deleteTopic`, after `ControllerApis` resolves names and IDs and checks authorization | `INVALID_TOPIC_EXCEPTION` |
| Produce | `ReplicaManager.appendToLocalLog`, next to the internal-topic check: reject unless `origin` is `AppendOrigin.COORDINATOR` | `INVALID_TOPIC_EXCEPTION` |
| `CreateTopics` from a client, including broker auto-create | `ControllerApis`, in the same check as `__cluster_metadata` | `INVALID_REQUEST` |
| `CreatePartitions` | `ReplicationControlManager.createPartitions`, before the count checks | `INVALID_PARTITIONS` |
| `ElectLeaders` with type `UNCLEAN` | `ReplicationControlManager.electLeader`, next to the pending-switch check | `INVALID_REQUEST` |
| `DeleteRecords` from a client | Inherited from `Topic.isInternal` in `ReplicaManager.deleteRecordsOnLocalLog` | `INVALID_TOPIC_EXCEPTION` |

Notes on the requests:

- `DeleteTopics` departs from upstream, which lets a principal with `DELETE` remove `__consumer_offsets`. Deleting `__diskless_metadata` loses the batch index for every diskless partition, every lease, and every deletion queue, and no rollback to Postgres exists. `design.md` states this reason. The check fails only that topic, so other topics in the same request proceed, and authorization runs first, so an unauthorized caller learns nothing new.
- The produce check closes the `__admin_client` bypass in `KafkaApis.handleProduceRequest`, which any client can use. A foreign record would apply on every replica and take an offset the leader's overlay doesn't expect. The bypass stays for the three upstream topics. `WriteTxnMarkers` also appends with `COORDINATOR` origin. It isn't covered, because it needs `CLUSTER_ACTION`, which is broker-level trust.
- The `CreateTopics` check stops a client or auto-create from creating the topic first with broker defaults and the wrong partition count. The controller's own creation calls `ReplicationControlManager` directly. The rewrite confirms that path against the code.
- The admin `UNCLEAN` election path never reads `unclean.leader.election.enable`, so the fixed config alone doesn't close it. `PREFERRED` elections and `AlterPartitionReassignments` stay unchanged: both keep acknowledged records, and a reassignment below `min.insync.replicas` only stops writes.

### Configs

`ConfigurationControlManager.validateAlterConfig` checks `allConfigs`, the topic's config after the alter, next to the `diskless.enable` system-topic check. Reading the result covers `SET`, `DELETE`, `APPEND`, `SUBTRACT`, and the implicit deletes of legacy `AlterConfigs`. Unlike the `diskless.enable` check, it also runs for a newly created resource, so the controller's own create passes the same rules. A violation gets `INVALID_CONFIG`.

| Config | Rule | Why |
| --- | --- | --- |
| `flush.messages` | Must be `1` | A lost unflushed tail deletes objects that live batches reference, as [Flush policy under Kafka parity](04-flush-policy.md) shows. Deleting the override would fall back to the broker default of `Long.MAX_VALUE`. |
| `unclean.leader.election.enable` | Must be `false` | An unclean leader truncates acknowledged records. |
| `retention.ms` | Must be `-1` | Segment deletion would move the log start past the snapshots. |
| `retention.bytes` | Must be `-1` | Same as `retention.ms`. |
| `cleanup.policy` | Must be `delete` | Compaction removes records from a log that replicas replay in full. |
| `min.insync.replicas` | At least `min(2, diskless.metadata.topic.replication.factor)` | At 1, `acks=-1` acknowledges records that only the leader holds, and the log start advances on the leader's snapshot alone. |

Setting a key to its required value passes. Other topic configs stay alterable. Because the values are topic-level overrides, broker and cluster default changes don't reach the topic.

The controller creates the topic with `min.insync.replicas` at that same bound. This fixes a v1 gap: a fixed 2 fails every append at a replication factor of 1. The bound reads the static controller config, so it doesn't follow a later reassignment, and controllers that disagree on the config disagree on the bound, as they do for `diskless.metadata.topic.num.partitions`.

### Accepted consequences

- Operators lose two incident workarounds that classic topics have: lowering `min.insync.replicas`, and an unclean election. With two of three replicas down, a coordinator partition stops until one returns. Once its whole ISR is lost, it stays offline. This matches the map's accepted risk for losing every replica of a coordinator partition.
- The failure-mode table in the rewrite gets a row for a coordinator partition whose whole ISR is lost: it stays offline, and no unclean election is possible.

### Tests

Unit tests sit next to each guard. None go in `AbstractControlPlaneTest`, because the guards live in the controller and broker, not behind `ControlPlane`.

- `TopicTest`: `Topic.isInternal("__diskless_metadata")` is true.
- `ControllerApisTest`: `CreateTopics` gets `INVALID_REQUEST`. `DeleteTopics` by name and by ID gets `INVALID_TOPIC_EXCEPTION`, an unauthorized caller still gets `TOPIC_AUTHORIZATION_FAILED`, and other topics in the same request are deleted.
- `ReplicationControlManagerTest`: `CreatePartitions` gets `INVALID_PARTITIONS`. `UNCLEAN` `ElectLeaders` gets `INVALID_REQUEST`. A `PREFERRED` election and a reassignment succeed.
- `ConfigurationControlManagerTest`: each fixed config is rejected under `SET`, `DELETE`, `APPEND` and `SUBTRACT` on `cleanup.policy`, and a legacy `AlterConfigs` that omits it. Its required value passes. `min.insync.replicas` at replication factor 1 and 3, at, below, and above the bound. An unrelated config such as `segment.bytes` alters.
- `ReplicaManagerTest`: a client produce with client ID `__admin_client` gets `INVALID_TOPIC_EXCEPTION`, an append with `AppendOrigin.COORDINATOR` succeeds, and client `DeleteRecords` gets `INVALID_TOPIC_EXCEPTION`. If `ReplicaManagerTest` can't drive a `COORDINATOR` append without the coordinator runtime, that case moves to the cluster test.

One `KafkaClusterTestKit` test on a cluster formatted with the feature replaces v1's `CreatePartitions` cluster test. It checks that the controller creates the topic with the fixed configs and the bounded `min.insync.replicas`, that `Admin.listTopics()` hides it by default, and that every rejection holds end to end through `Admin`: delete, create, `CreatePartitions`, altering `flush.messages`, an unclean election, and an `__admin_client` produce. A `Metadata` request with auto-create on doesn't create the topic.

### Consequences for other tickets and the rewrite

- `design.md` and `spec.md` replace v1's single `CreatePartitions` rejection in `Coordinator partitions` and `Rollout` with this guard set, and `Topology` takes the `min.insync.replicas` bound.
- `INKLESS_OWNERSHIP` gains `clients/src/main/java/org/apache/kafka/common/internals/Topic.java` as `INTERLEAVED` when the change lands.
- Surfaced: `PartitionChangeBuilder` elects the last known leader when the ISR and ELR are both empty, on the online path as well as the unclean one (`metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java:257`, `:280`). It's automatic recovery, not an admin request, so [Last-known-leader election on `__diskless_metadata`](09-last-known-leader-election.md) covers it.
