# CREATE_TOPICS Config Interceptors

This document describes the interceptor framework that the controller applies to `CREATE_TOPICS` requests. Interceptors give the controller a generic hook to **mutate the topic configs** of a topic being created (for example, force `diskless.enable=true` or `remote.storage.enable=true`) and to run **additional validations** (for example, reject a request that explicitly opts out of a forced config).

## Motivation

Operators often want cluster-wide policies over how topics are stored — "all matching topics must be diskless", "all classic topics must be tiered" — without requiring every client to set the right configs. Rather than scattering that logic through `ReplicationControlManager`, the interceptor framework lets each policy live in its own small, testable class, and chains them with clear priority ordering.

## Concepts

### The interceptor interface

`CreateTopicConfigInterceptor` has multiple `intercept` methods and they can all be invoked during a single topic creation (see [Where it runs](#where-it-runs)), so an interceptor must be careful to apply the same decision through all variants to keep the persisted records and the response view consistent.

The two responsibilities of an interceptor are:

- **Mutation** — add or change entries in the target map (e.g. set `diskless.enable=true`).
- **Validation** — throw an `ApiException` (such as `InvalidRequestException`) to reject the request.

### The chain: first-match-wins

`CreateTopicConfigInterceptors` holds an ordered list of interceptors and applies them with **first-match-wins** semantics: it walks the chain and stops at the first interceptor whose `intercept` returns `true`. Subsequent interceptors are skipped. This lets multiple policies coexist as long as their matching rules don't overlap; when they might, the chain order decides the winner.

The chain is built once in `ReplicationControlManager`'s constructor via `CreateTopicConfigInterceptors.create(...)`, from controller configs.

## Where it runs

Interceptors run inside `ReplicationControlManager.createTopics`, per topic, in two places:

- When computing the config records to persist, the **records variant** is applied to the `keyToOps` map. Any `ApiException` thrown here is recorded as that topic's error and the topic is skipped.
- Inside `createTopic`, the **view variant** is re-applied to a fresh `creationConfigs` map. This ensures forced configs participate in topic-policy validation and are reflected in the effective-config view returned to the client.

## Built-in interceptors

- [DisklessForceCreateTopicInterceptor](../../metadata/src/main/java/org/apache/kafka/controller/DisklessForceCreateTopicInterceptor.java) - Forces `diskless.enable=true` for some topics based on `diskless.force.include.topic.regexes`.
- [ClassicTopicRemoteStorageForceCreateTopicInterceptor](../../metadata/src/main/java/org/apache/kafka/controller/ClassicTopicRemoteStorageForceCreateTopicInterceptor.java) - Forces `remote.storage.enable=true` on classic topics.
- [DisklessDisabledFallbackCreateTopicInterceptor](../../metadata/src/main/java/org/apache/kafka/controller/DisklessDisabledFallbackCreateTopicInterceptor.java) - Allow `diskless.enable=false` to be passed even when diskless storage is disabled.

## Adding a new interceptor

1. Implement `CreateTopicConfigInterceptor`, providing all `intercept` overloads.
2. Add any new controller configs to `ServerConfigs` and surface them through `KafkaConfig`.
3. Insert the interceptor into `CreateTopicConfigInterceptors.create(...)` at the appropriate position in the chain (remember first-match-wins).
4. Throw an `ApiException` from `intercept` for validation failures; the controller converts it into a per-topic error.
