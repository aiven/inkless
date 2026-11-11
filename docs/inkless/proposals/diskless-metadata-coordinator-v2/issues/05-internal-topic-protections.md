# What Kafka's internal-topic status protects

Type: research
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

Which requests does Kafka guard for internal topics, and how does each guard behave?

`Topic.isInternal` in `clients` checks a hard-coded set, `Topic.INTERNAL_TOPICS`, that holds only `__consumer_offsets`, `__transaction_state`, and `__share_group_state`. `__diskless_metadata` gets none of those guards today. v1 rejects only `CreatePartitions` for it, in [Coordinator partitions](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#coordinator-partitions).

Find, in the local Kafka checkout at `/Users/viktor.somogyi-vass/dev/github/apache/kafka`, for each of these requests: whether an internal-topic check guards it, where the check is, what error it returns, and whether a client ID or a flag bypasses it.

- `DeleteTopics`
- `DeleteRecords`
- `CreatePartitions`
- `AlterConfigs` and `IncrementalAlterConfigs`, including which configs, if any, are restricted for internal topics
- Produce
- `AlterPartitionReassignments` and `ElectLeaders`
- Any other request with an internal-topic check

Also record the guards that rely on something other than `Topic.isInternal`, such as the `isInternal` flag in topic metadata, and which of them a new internal topic would inherit by joining `Topic.INTERNAL_TOPICS`.

## Resolution

Findings, with a file and line for every claim: [internal-topic-protections.md](../research/internal-topic-protections.md).

- In KRaft, internal-topic status guards only two client requests, both in `ReplicaManager`, and both return `INVALID_TOPIC_EXCEPTION`. Produce is bypassed by the client ID `__admin_client`, which any client can set. `DeleteRecords` has no client bypass.
- `DeleteTopics`, `CreatePartitions`, `AlterConfigs`, `IncrementalAlterConfigs`, `AlterPartitionReassignments`, and `ElectLeaders` have no internal-topic check on the broker or the controller. No topic config is restricted for internal topics.
- Joining `Topic.INTERNAL_TOPICS` also marks the topic internal in responses, hides it from client-side regex subscriptions and `Admin.listTopics` by default, and makes its own in-process writers pass `internalTopicsAllowed = true`.
- The fork's `isSystemTopic` only keeps system topics from becoming diskless. Diskless partitions skip both `ReplicaManager` checks. No code references `__diskless_metadata` yet, so v1's `CreatePartitions` rejection is design only.
