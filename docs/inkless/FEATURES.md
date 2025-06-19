# Traditional Topics

Inkless is an extension of Apache Kafka, and so all existing functionality in Apache Kafka is still present for Traditional topics.
No extra steps are necessary to make use of this functionality with an Inkless enabled cluster.

# Diskless Topics

The Inkless feature is enabled on a per-broker basis by passing appropriate configurations and credentials to reach both object storage and batch coordinate storage.
Once Inkless is enabled on brokers, it can be enabled for individual topics.
Diskless topics have a restricted set of features available, as not all functionality has been implemented and tested.

Currently Diskless topics support:
* Non-Idempotent Produce
* Idempotent Produce
* Fetch
* ListOffsets
* Access restriction via ACLs
* Committing offsets via traditional Group Coordinators

The following are notable unsupported features:
* cleanup.policy=delete
* cleanup.policy=compact
* Adding Diskless topics to transactions
* read_committed consumers reading Diskless topics
* Producing to both inkless and traditional topics simultaneously

If not specified above, features are untested and assumed to be inoperable.

## API support

### Diskless topics supported (possibly with limitations)
- `PRODUCE`
    - upload parallelism is underutilized;
    - Diskless topics can’t participate in transactions.
- `FETCH`
    - can't fetch from Inkless and classic topics in the same request.
- `LIST_OFFSETS`
- `METADATA`
    - the output is modified according to client and broker racks.
- `DESCRIBE_TOPIC_PARTITIONS`
    - the output is modified according to client and broker racks.
- `CREATE_TOPICS`
    - Diskless topics cannot be created with the remote storage enabled;
    - the replication factor must be `1` or `-1`;
    - the initial partition assignment provided by the user is ignored.
- `DELETE_TOPICS`
- `DELETE_RECORDS`
- `OFFSET_FOR_LEADER_EPOCH`
- `DESCRIBE_CONFIGS`
- `ALTER_CONFIGS`
    - the remote storage cannot be enabled for Diskless topics.
- `CREATE_PARTITIONS`
- `INCREMENTAL_ALTER_CONFIGS`
    - the remote storage cannot be enabled for Diskless topics.
- `ALTER_PARTITION_REASSIGNMENTS`
    - the replication factor can't be changed for Diskless topics.

### Diskless topics are excluded
- `ADD_PARTITIONS_TO_TXN`
- `WRITE_TXN_MARKERS`
- `TXN_OFFSET_COMMIT`

### Not supported for Diskless topics (WIP)
- `DESCRIBE_PRODUCERS`
- `ASSIGN_REPLICAS_TO_DIRS`

### Not affected
- `LEADER_AND_ISR`
- `STOP_REPLICA`
- `UPDATE_METADATA`
- `CONTROLLED_SHUTDOWN`
- `OFFSET_COMMIT`
- `OFFSET_FETCH`
- `FIND_COORDINATOR`
- `JOIN_GROUP`
- `HEARTBEAT`
- `LEAVE_GROUP`
- `SYNC_GROUP`
- `DESCRIBE_GROUPS`
- `LIST_GROUPS`
- `SASL_HANDSHAKE`
- `API_VERSIONS`
- `INIT_PRODUCER_ID`
- `ADD_OFFSETS_TO_TXN`
- `END_TXN`
- `DESCRIBE_ACLS`
- `CREATE_ACLS`
- `DELETE_ACLS`
- `SASL_AUTHENTICATE`
- `ALTER_REPLICA_LOG_DIRS`
- `DESCRIBE_LOG_DIRS`
- `CREATE_DELEGATION_TOKEN`
- `RENEW_DELEGATION_TOKEN`
- `EXPIRE_DELEGATION_TOKEN`
- `DESCRIBE_DELEGATION_TOKEN`
- `DELETE_GROUPS`
- `ELECT_LEADERS`
    - despite it doesn't make much sense, it's possible to trigger leader for Inkless partitions.
- `OFFSET_DELETE`
- `DESCRIBE_CLIENT_QUOTAS`
- `ALTER_CLIENT_QUOTAS`
- `DESCRIBE_USER_SCRAM_CREDENTIALS`
- `ALTER_USER_SCRAM_CREDENTIALS`
- `ALTER_PARTITION`
- `CONSUMER_GROUP_HEARTBEAT`
- `CONSUMER_GROUP_DESCRIBE`
- `VOTE`
- `BEGIN_QUORUM_EPOCH`
- `END_QUORUM_EPOCH`
- `DESCRIBE_QUORUM`
- `UPDATE_FEATURES`
- `ENVELOPE`
- `FETCH_SNAPSHOT`
- `DESCRIBE_CLUSTER`
- `BROKER_REGISTRATION`
- `BROKER_HEARTBEAT`
- `UNREGISTER_BROKER`
- `DESCRIBE_TRANSACTIONS`
- `LIST_TRANSACTIONS`
- `CONTROLLER_REGISTRATION`
- `GET_TELEMETRY_SUBSCRIPTIONS`
- `PUSH_TELEMETRY`
- `REMOVE_RAFT_VOTER`
- `UPDATE_RAFT_VOTER`
- `ADD_RAFT_VOTER`
- `LIST_PARTITION_REASSIGNMENTS`
- `ALLOCATE_PRODUCER_IDS`
- `LIST_CLIENT_METRICS_RESOURCES`
- `SHARE_GROUP_HEARTBEAT`
- `SHARE_GROUP_DESCRIBE`

### Not tested
- `SHARE_FETCH`
- `SHARE_ACKNOWLEDGE`
- `INITIALIZE_SHARE_GROUP_STATE`
- `READ_SHARE_GROUP_STATE`
- `WRITE_SHARE_GROUP_STATE`
- `DELETE_SHARE_GROUP_STATE`
- `READ_SHARE_GROUP_STATE_SUMMARY`
- `STREAMS_GROUP_HEARTBEAT`
- `STREAMS_GROUP_DESCRIBE`
- `DESCRIBE_SHARE_GROUP_OFFSETS`
- `ALTER_SHARE_GROUP_OFFSETS`
- `DELETE_SHARE_GROUP_OFFSETS`

# Roadmap

In addition to full feature parity with traditional topics, we may add inkless-topic specific features
Listed in no particular order, here are some features that may be added Inkless in the future:
* Broker roles
* Heterogeneous broker capacities
* Batch coalescing/recompression
* Parallel produce request handling
* Zero-Copy cross-region sharing
* Out-of-process cross-region replication
* Cross-Cluster topic sharing
* Column-oriented object formats