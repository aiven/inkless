# What Kafka's internal-topic status protects

Research for ticket [05-internal-topic-protections](../issues/05-internal-topic-protections.md).

## Summary

`Topic.isInternal` guards very little on the request path. In KRaft mode, only two
client requests are rejected for an internal topic by name, and both checks run in the
broker's `ReplicaManager`:

- Produce: The broker rejects an append to an internal topic with
  `INVALID_TOPIC_EXCEPTION`. A produce request bypasses the check when its client ID
  is exactly `__admin_client`.
- `DeleteRecords`: The broker rejects it with `INVALID_TOPIC_EXCEPTION`. No client ID
  or request flag bypasses it. Only in-process coordinator code passes
  `allowInternalTopicDeletion = true`.

Two more requests treat internal topics specially:

- `AddPartitionsToTxn` versions 0 to 3: The broker drops internal topics from the
  authorized set, so they fail with `TOPIC_AUTHORIZATION_FAILED`.
- `StreamsGroupHeartbeat`: The broker rejects a topology that names an internal topic
  with `STREAMS_INVALID_TOPOLOGY`.

`DeleteTopics`, `CreatePartitions`, `AlterConfigs`, `IncrementalAlterConfigs`,
`AlterPartitionReassignments`, and `ElectLeaders` have no internal-topic check on the
broker forwarding path or on the controller (`ControllerApis`,
`ReplicationControlManager`, `ConfigurationControlManager`). No topic config is
restricted for internal topics in upstream Kafka.

The remaining effects of `Topic.isInternal` are informational. It sets the `isInternal`
flag in `Metadata` and `DescribeTopicPartitions` responses. Clients use that flag to
exclude internal topics from client-side regex subscriptions (`exclude.internal.topics`,
default `true`) and from `Admin.listTopics` unless `listInternal(true)` is set. It also
turns off tiered storage for the topic.

A few guards use a name other than `Topic.isInternal`. The controller refuses to create
`__cluster_metadata`. `UnifiedLog.isRemoteLogEnabled` also excludes
`__remote_log_metadata` and `__cluster_metadata`. The Inkless fork adds
`ReplicationControlManager.isSystemTopic`, which is `Topic.isInternal` plus
`__cluster_metadata` and `__remote_log_metadata`. That check only stops a system topic
from becoming diskless. The fork has no code that references `__diskless_metadata`, so
the v1 design's `CreatePartitions` rejection isn't implemented yet.

## Sources

The research uses two local checkouts. Citations use paths relative to each root.

- Kafka: `/Users/viktor.somogyi-vass/dev/github/apache/kafka` at commit `27d8bbc35e`.
- Inkless: `/Users/viktor.somogyi-vass/dev/github/aiven/workspace-inkless/diskless-metadata`
  at commit `2a412af730`. Its upstream base is older, so line numbers differ from Kafka.

Unless a citation says Inkless, it refers to Kafka. ZooKeeper-mode code isn't present in
either checkout's request paths.

## How `Topic.isInternal` is defined

`Topic.INTERNAL_TOPICS` is a private, hard-coded set of `__consumer_offsets`,
`__transaction_state`, and `__share_group_state` (`clients/src/main/java/org/apache/kafka/common/internals/Topic.java:27-29`,
`:37`). `Topic.isInternal` is a set lookup (`Topic.java:74-76`). The set has no
extension point. `__cluster_metadata` has a constant in the same class but isn't in the
set (`Topic.java:30`, `:37`). `Topic.validate`, which the controller's config validator
calls, only checks name syntax and length; it has no internal-topic logic
(`Topic.java:41-60`).

The same set and lookup exist in the Inkless fork at the same lines
(Inkless `clients/src/main/java/org/apache/kafka/common/internals/Topic.java:37`, `:74-75`).

## Request-by-request findings

### `DeleteTopics`

There's no internal-topic check.

- The broker forwards `DeleteTopics` to the controller without inspecting it
  (`core/src/main/scala/kafka/server/KafkaApis.scala:191`).
- `ControllerApis.deleteTopics` checks only `delete.topic.enable`, duplicate names or
  IDs, and `DELETE` or `DESCRIBE` authorization
  (`core/src/main/scala/kafka/server/ControllerApis.scala:223-360`). The only global
  switch is `delete.topic.enable`, which returns `TOPIC_DELETION_DISABLED` for every
  topic (`ControllerApis.scala:231-238`).
- `ReplicationControlManager.deleteTopic` checks only that the topic ID exists and the
  controller mutation quota (`metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:1040-1058`).
- A principal with `DELETE` on the topic, or `DELETE` on the cluster, can delete
  `__consumer_offsets`.
- `kafka-topics.sh --delete` applies no client-side protection either. `--exclude-internal`
  is accepted only with `--list` and `--describe`, so delete resolves its regex
  against a listing that includes internal topics
  (`tools/src/main/java/org/apache/kafka/tools/TopicCommand.java:624-640`, `:964`).

The Inkless fork adds no check here (Inkless `ControllerApis.scala` has no
`Topic.isInternal` or `isSystemTopic` reference; its only `Topic` checks are the
`__cluster_metadata` create checks at `:436` and `:461-465`).

### `DeleteRecords`

A `Topic.isInternal` check guards it.

- Location: `ReplicaManager.deleteRecordsOnLocalLog`
  (`core/src/main/scala/kafka/server/ReplicaManager.scala:1082-1088`; Inkless
  `ReplicaManager.scala:1421-1426`).
- Error: `InvalidTopicException("Cannot delete records of internal topic <name>")`,
  which `LogDeleteRecordsResult.error` maps to `INVALID_TOPIC_EXCEPTION` (code 17)
  (`server/src/main/java/org/apache/kafka/server/LogDeleteRecordsResult.java:24-29`;
  `clients/src/main/java/org/apache/kafka/common/protocol/Errors.java:219-220`).
- Bypass: `ReplicaManager.deleteRecords` takes `allowInternalTopicDeletion` with a
  default of `false` (`ReplicaManager.scala:1296-1301`). The `DeleteRecords` request
  handler doesn't pass it, so client requests always get the default
  (`KafkaApis.scala:1640-1643`). No client ID or request field changes this. The only
  caller that passes `true` is the coordinator runtime's partition writer, which
  group and share coordinators use to prune their own logs
  (`core/src/main/scala/kafka/coordinator/group/CoordinatorPartitionWriter.scala:195-213`).
- The check runs after `DELETE` authorization and the existence check in the handler
  (`KafkaApis.scala:1590-1606`).

In the Inkless fork, the client handler still calls `deleteRecords` without the flag
(Inkless `KafkaApis.scala:1638`, `:1659`). `ReplicaManager.deleteRecords` splits
diskless partitions out first. Only the local-log portion reaches
`deleteRecordsOnLocalLog`, so the internal-topic check applies only to classic partitions
and to the classic leg of a switched partition (Inkless `ReplicaManager.scala:1640-1730`,
`:1770`). `DisklessDeleteRecordsForwarder` forwards only diskless partitions with a local
leg to another broker as a new `DeleteRecords` request
(Inkless `core/src/main/scala/kafka/server/DisklessDeleteRecordsForwarder.scala:94-110`, `:185-191`).

### `CreatePartitions`

There's no internal-topic check.

- The broker forwards it to the controller (`KafkaApis.scala:208`).
- `ControllerApis.createPartitions` checks duplicates and `ALTER` authorization only
  (`ControllerApis.scala:822-888`).
- `ReplicationControlManager.createPartitions` checks the topic exists, the count is an
  increase, and the manual assignment size (`ReplicationControlManager.java:1932-1981`).
  It has no name-based check.

The Inkless fork adds diskless-topic checks to this path, not internal-topic checks. It
rejects a manual assignment for a diskless topic when managed replicas are off, with
`InvalidReplicaAssignmentException` (Inkless `ReplicationControlManager.java:2765-2768`).
No code rejects `CreatePartitions` for `__diskless_metadata`; no non-test source file in
the fork contains that name.

### `AlterConfigs` and `IncrementalAlterConfigs`

There's no internal-topic check, and no config is restricted for internal topics.

- Broker side: `handleAlterConfigsRequest` and `handleIncrementalAlterConfigsRequest`
  run `ConfigAdminManager.preprocess`, then forward the remainder to the controller
  (`KafkaApis.scala:2234-2283`). `ConfigAdminManager.scala` has no reference to
  `Topic` or internal topics.
- Controller side: `ControllerApis.handleLegacyAlterConfigs` and
  `handleIncrementalAlterConfigs` check duplicates and authorization, then call the
  controller (`ControllerApis.scala:498-555`, `:741-820`).
- `ConfigurationControlManager.validateAlterConfig` checks min ISR transitions,
  cordoned log dirs, and value size, then runs the validator and any `AlterConfigPolicy`
  (`metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java:355-390`).
- `ControllerConfigurationValidator.validate` for a topic calls `Topic.validate` (syntax
  only) and `LogConfig.validate` (`core/src/main/scala/kafka/server/ControllerConfigurationValidator.scala:47-54`,
  `:120-130`). `LogConfig.java` has no internal-topic or per-name logic.
- A user can set `cleanup.policy=delete` or `retention.ms` on `__consumer_offsets` and
  the controller accepts it, subject to `ALTER_CONFIGS` authorization and any
  configured `AlterConfigPolicy`.

The one per-topic config effect in upstream Kafka is passive. `remote.storage.enable=true`
is accepted on an internal topic, but the broker never enables tiered storage for it
(see [Guards that don't use `Topic.isInternal`](#guards-that-dont-use-topicisinternal)).

The Inkless fork adds one system-topic config check. On alter, the controller rejects a
resulting `diskless.enable=true` for a system topic with
`InvalidConfigurationException("System topics cannot be diskless topics.")`, which maps
to `INVALID_CONFIG` (Inkless `ConfigurationControlManager.java:424-430`;
`Errors.java:264-265`). The check skips newly created resources.

### Produce

A `Topic.isInternal` check guards it.

- Location: `ReplicaManager.appendToLocalLog`
  (`ReplicaManager.scala:1406-1411`; Inkless `ReplicaManager.scala:2095-2100`).
- Error: `InvalidTopicException("Cannot append to internal topic <name>")`, which
  `LogAppendResult.error` maps to `INVALID_TOPIC_EXCEPTION`
  (`server/src/main/java/org/apache/kafka/server/LogAppendResult.java:67-71`).
- Bypass: The produce handler sets `internalTopicsAllowed` to true when
  `request.header.clientId == "__admin_client"` (`KafkaApis.scala:545`, `:551`; Inkless
  `KafkaApis.scala:554`, `:560`). Any client can send that ID because it comes from the
  `client.id` config (`clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java:85`).
  No other code in the Kafka tree sets or references `__admin_client`. The request still
  needs `WRITE` on the topic (`KafkaApis.scala:436`, `:443-444`).
- In-process callers pass `internalTopicsAllowed = true`: `WriteTxnMarkers`
  (`KafkaApis.scala:1876-1879`), the coordinator partition writer
  (`CoordinatorPartitionWriter.scala:171-173`), and the transaction state manager
  (`core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala:289-292`, `:801-804`).

In the Inkless fork, `ReplicaManager.appendRecords` splits diskless entries off before
`appendRecordsToLeader` and sends them to the Inkless `AppendHandler`. Only classic
entries reach `appendToLocalLog` and the internal-topic check
(Inkless `ReplicaManager.scala:953`, `:963-964`, `:990-999`).

### `AlterPartitionReassignments` and `ElectLeaders`

There's no internal-topic check for either.

- The broker forwards both to the controller (`KafkaApis.scala:216`, `:218`).
- `ControllerApis` requires `ALTER` on the cluster for both, then calls the controller
  (`ControllerApis.scala:577-594`, `:904-914`).
- `ReplicationControlManager.java` uses `Topic` only for collision-character and syntax
  checks (`ReplicationControlManager.java:440-441`, `:556-557`, `:914-920`). It has no
  other internal-topic logic.

The Inkless fork adds a diskless-topic check to reassignments. It only allows a
replication factor change for a diskless topic when managed replicas are on
(Inkless `ReplicationControlManager.java:2993-3005`).

### Other requests with an internal-topic check

`AddPartitionsToTxn`:

- For versions 0 to 3, the broker filters internal topics out before `WRITE`
  authorization, so they never enter the authorized set and fail with
  `TOPIC_AUTHORIZATION_FAILED` (`KafkaApis.scala:1956-1964`; Inkless
  `KafkaApis.scala:1982`). Any failed partition fails the whole transaction in that
  request.
- Version 4 and later requires `CLUSTER_ACTION` and skips the filter
  (`KafkaApis.scala:1910-1911`, `:1960-1961`).

`StreamsGroupHeartbeat`:

- The broker rejects a topology whose source, repartition, or changelog topics include
  an internal topic, with `STREAMS_INVALID_TOPOLOGY` and the message "Use of Kafka
  internal topics ... in a Kafka Streams topology is prohibited."
  (`KafkaApis.scala:2829-2849`; Inkless `KafkaApis.scala:2836-2840`).

`Metadata` and `DescribeTopicPartitions` don't reject anything. They set the `isInternal`
flag from `Topic.isInternal` for existing topics
(`metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java:272`, `:300`,
`:322`), for missing topics (`KafkaApis.scala:875-881`), for create-unauthorized topics
(`KafkaApis.scala:943`), and for auto-created topics
(`server/src/main/java/org/apache/kafka/server/DefaultAutoTopicCreationManager.java:123`,
`:128`, `:203`). Describe-unauthorized topics always get `isInternal = false`
(`KafkaApis.scala:953`, `:957`; `core/src/main/java/kafka/server/handlers/DescribeTopicPartitionsRequestHandler.java:98`).

Fetch, `ListOffsets`, `OffsetCommit`, `TxnOffsetCommit`, `DescribeConfigs`, and
`CreateTopics` have no `Topic.isInternal` check. `Topic.isInternal` appears in `core`
only in `KafkaApis.scala` and `ReplicaManager.scala`, and none of those call sites belong
to these handlers.

## Guards that don't use `Topic.isInternal`

Name checks on other topics:

- `CreateTopics` for `__cluster_metadata`: The controller removes it from the request
  and returns `INVALID_REQUEST` with "Creation of internal topic __cluster_metadata is
  prohibited." (`ControllerApis.scala:410-411`, `:436-440`). No bypass exists.
- Tiered storage: `UnifiedLog.isRemoteLogEnabled` returns false for compacted topics,
  `Topic.isInternal` topics, `__remote_log_metadata`, and `__cluster_metadata`, even when
  `remote.storage.enable=true` (`storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java:2706-2713`;
  Inkless `UnifiedLog.java:2690`). The controller accepts the topic config, but the log doesn't tier.

Per-name coordinator wiring, which isn't a guard but is keyed on the three constant names:

- `FindCoordinator` maps the coordinator type to its topic name and auto-creates that
  topic if it's missing (`KafkaApis.scala:1277-1295`).
- Auto-creation applies per-name partition count, replication factor, and configs for
  the three names (`DefaultAutoTopicCreationManager.java:237-262`). Other names get the
  broker defaults (`DefaultAutoTopicCreationManager.java:263-274`).
- `WriteTxnMarkers` sends `__consumer_offsets` markers to the group coordinator and
  appends the rest directly (`KafkaApis.scala:1840-1872`).

Consumers of the `isInternal` flag in responses, which the broker derives from
`Topic.isInternal`:

- Consumer regex subscriptions: `ConsumerMetadata.retainTopic` drops an internal topic
  unless it's explicitly subscribed, transient, or `exclude.internal.topics=false`
  (`clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerMetadata.java:59`,
  `:120-127`; `clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java:353-356`).
  This applies to client-side `Pattern` subscriptions. Explicit subscription to an
  internal topic still works.
- Broker-side RE2J regex subscriptions (KIP-848): `TopicRegexResolver` matches against
  every topic name with no internal-topic filter
  (`group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/consumer/TopicRegexResolver.java:99-104`).
  The consumer then retains any topic ID assigned from RE2J regardless of the flag
  (`ConsumerMetadata.java:141-142`).
- Client `Cluster.internalTopics()`: `MetadataResponse.buildCluster` and `Metadata` fill
  it from the flag (`clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java:159-161`;
  `clients/src/main/java/org/apache/kafka/clients/Metadata.java:512-516`).
- Admin client: `listTopics` omits internal topics unless `listInternal(true)` is set
  (`clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java:2242-2244`).
  `describeTopics` copies the flag into `TopicDescription.isInternal`
  (`KafkaAdminClient.java:2555`, `:2560`).
- Producer and share consumer: `ProducerMetadata.retainTopic` and
  `ShareConsumerMetadata.retainTopic` ignore the flag
  (`clients/src/main/java/org/apache/kafka/clients/producer/internals/ProducerMetadata.java:131-136`;
  `clients/src/main/java/org/apache/kafka/clients/consumer/internals/ShareConsumerMetadata.java:75-77`).

Consumers that call `Topic.isInternal` locally, so they use the set compiled into the
client or tool jar rather than the broker's flag:

- `kafka-topics.sh --exclude-internal` (`TopicCommand.java:211-221`) and the tools
  `IncludeList` filter (`tools/src/main/java/org/apache/kafka/tools/filter/TopicFilter.java:63-64`).
- The broker's client quota callback: `MetadataCache.toCluster` fills
  `Cluster.internalTopics()` from `Topic.isInternal` and passes it to the
  `ClientQuotaCallback` plugin (`metadata/src/main/java/org/apache/kafka/metadata/MetadataCache.java:174-176`;
  `metadata/src/main/java/org/apache/kafka/metadata/publisher/DynamicTopicClusterQuotaPublisher.java:64-67`).

## Inkless fork additions

`ReplicationControlManager.isSystemTopic` returns true for `Topic.isInternal` topics,
`__cluster_metadata`, and `__remote_log_metadata`
(Inkless `metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:172-179`,
`:1094-1096`). Every use of this check keeps a system topic classic:

- `CreateTopics` with an explicit `diskless.enable=true` on a system topic: The
  controller returns `INVALID_REQUEST` with "System topics cannot be diskless topics."
  (Inkless `ReplicationControlManager.java:906-910`).
- A system topic is never diskless at creation, even when the broker default is
  diskless, and doesn't get the forced `remote.storage.enable=true` for diskless
  consolidation (Inkless `ReplicationControlManager.java:891-897`, `:912`).
  `validConfigRecords` persists `diskless.enable=false` for it when the default is
  diskless (Inkless `ReplicationControlManager.java:1121-1143`), and the
  `CreateTopics` response reports `diskless.enable=false` (Inkless
  `ReplicationControlManager.java:1182-1185`).
- Alter configs: The controller rejects `diskless.enable=true` for a system topic with
  `INVALID_CONFIG`, as described in
  [`AlterConfigs` and `IncrementalAlterConfigs`](#alterconfigs-and-incrementalalterconfigs).
- `CREATE_TOPIC` interceptors skip system topics:
  `DisklessDisabledFallbackCreateTopicInterceptor`
  (Inkless `metadata/src/main/java/org/apache/kafka/controller/DisklessDisabledFallbackCreateTopicInterceptor.java:69-71`),
  `DisklessForceCreateTopicInterceptor`
  (Inkless `metadata/src/main/java/org/apache/kafka/controller/DisklessForceCreateTopicInterceptor.java:98-100`),
  and `ClassicTopicRemoteStorageForceCreateTopicInterceptor`
  (Inkless `metadata/src/main/java/org/apache/kafka/controller/ClassicTopicRemoteStorageForceCreateTopicInterceptor.java:72-78`).
  `DisklessForceCreateTopicInterceptor` also skips any name that starts with `__`
  (Inkless `DisklessForceCreateTopicInterceptor.java:95-97`).

The fork adds diskless-topic checks that key on whether the topic is diskless, not on
internal status:

- `WriteTxnMarkers` returns `INVALID_TOPIC_EXCEPTION` for a diskless partition
  (Inkless `core/src/main/scala/kafka/server/KafkaApis.scala:1822-1824`).
- `AddPartitionsToTxn` returns `INVALID_TOPIC_EXCEPTION` for a diskless partition
  (Inkless `KafkaApis.scala:1990-1992`).
- `CreatePartitions` and `AlterPartitionReassignments` have the managed-replica checks
  described in their sections.

`storage/inkless/src/main/java` has no reference to `isInternal`, `isSystemTopic`, or
internal-topic names. `InklessTopicMetadataTransformer` doesn't change the `isInternal`
flag; no `setIsInternal` call exists under `storage/inkless/src/main`.

## What joining `Topic.INTERNAL_TOPICS` would give a topic

Adding a name to `Topic.INTERNAL_TOPICS` means editing `Topic.java` in the `clients`
module, because the set is a private constant (`Topic.java:37`). The topic would then
get these guards:

- Produce rejection with `INVALID_TOPIC_EXCEPTION` unless the client ID is
  `__admin_client` (`ReplicaManager.scala:1406-1411`). Any in-process writer of the
  topic has to pass `internalTopicsAllowed = true` or its appends fail the same way.
- `DeleteRecords` rejection with `INVALID_TOPIC_EXCEPTION`, with no client bypass
  (`ReplicaManager.scala:1082-1088`). Any in-process pruning has to pass
  `allowInternalTopicDeletion = true`.
- `AddPartitionsToTxn` versions 0 to 3 rejection with `TOPIC_AUTHORIZATION_FAILED`
  (`KafkaApis.scala:1959`).
- `StreamsGroupHeartbeat` topology rejection with `STREAMS_INVALID_TOPOLOGY`
  (`KafkaApis.scala:2842-2848`).

It would also get these side effects:

- `isInternal = true` in `Metadata` and `DescribeTopicPartitions` responses
  (`KRaftMetadataCache.java:272`, `:300`, `:322`; `KafkaApis.scala:879`).
- Exclusion from client-side regex subscriptions by default
  (`ConsumerMetadata.java:124-125`) and from `Admin.listTopics` by default
  (`KafkaAdminClient.java:2243`). `TopicDescription.isInternal` becomes true.
- Exclusion by `kafka-topics.sh --exclude-internal`, and membership in the quota
  callback's `Cluster.internalTopics()`. Both depend on the `Topic.java` compiled into
  the tool or broker, not on the response flag (`TopicCommand.java:219`;
  `MetadataCache.java:174-176`).
- Tiered storage stays off even with `remote.storage.enable=true`
  (`UnifiedLog.java:2706-2713`).
- In the Inkless fork, the topic becomes a system topic, so it can't be diskless and
  the create-topic interceptors skip it (Inkless `ReplicationControlManager.java:1094-1096`).

It wouldn't get any protection for `DeleteTopics`, `CreatePartitions`, `AlterConfigs`,
`IncrementalAlterConfigs`, `AlterPartitionReassignments`, or `ElectLeaders`, because none
of those paths consult `Topic.isInternal` in KRaft mode. Membership also doesn't change
auto-creation or `FindCoordinator`, which use per-name switches on the three existing
constants (`DefaultAutoTopicCreationManager.java:237-274`; `KafkaApis.scala:1277-1287`).
