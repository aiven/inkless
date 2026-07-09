# Repository Guide for Agents

Guide for agentic coding agents working in the `inkless` repository.

The following requirement levels are used throughout this document to indicate
the importance of a given rule or guideline:

| Keyword            | How to treat it          |
| ------------------ | ------------------------ |
| MUST/REQUIRED      | mandatory                |
| SHOULD/RECOMMENDED | deviate only with reason |
| MAY/OPTIONAL       | use judgment             |

## Repository Structure

`inkless` is a fork of Apache Kafka that adds Diskless Topics (KIP-1150). Most
of the tree is upstream Kafka. Which paths are inkless-owned — and which are
upstream files carrying inkless edits — is recorded in
[`INKLESS_OWNERSHIP`](INKLESS_OWNERSHIP). When a task is Inkless-related, work in the
**OWNED** paths first (`@aiven/inkless`).

Note that some diskless logic is **interleaved** into upstream broker classes rather
than confined to an inkless package — e.g. `DisklessFetchOffsetRouter.scala` and
`InitDisklessLog*.scala` are net-new under `core/src/main/scala/kafka/server/`, and
`ReplicaManager.scala`/`KafkaConfig.scala`/`BrokerServer.scala` carry heavy inkless
edits (the dual-owner INTERLEAVED entries in the manifest). Don't assume all inkless
Scala lives under `io/aiven/inkless/`.

Everything else is upstream Kafka. Follow upstream conventions when editing those and
keep Inkless changes minimal and isolated.

## Documentation (Load On-Demand)

### Inkless Documentation

Read these only when a task touches the relevant area. All docs are under
[`docs/inkless/`](docs/inkless/).

| Doc                                               | Read it when you need…                                                       |
| ------------------------------------------------- | ---------------------------------------------------------------------------- |
| `README.md`                                       | The documentation index and entry point.                                     |
| `ARCHITECTURE.md`                                 | The system design and how components fit together.                           |
| `GLOSSARY.md`                                     | Definitions of Inkless-specific terms.                                       |
| `FEATURES.md`                                     | Supported features, API compatibility, managed replicas, controller metrics. |
| `QUICKSTART.md`                                   | To run Inkless via Docker or locally.                                        |
| `PERFORMANCE.md`                                  | Producer/consumer tuning guidance.                                           |
| `CLASSIC_TO_DISKLESS_SWITCH.md`                   | To work on switching classic topics to diskless.                             |
| `CLIENT-BROKER-AZ-ALIGNMENT.md`                   | Multi-AZ deployment and cost optimization.                                   |
| `CREATE-TOPICS-INTERCEPTORS.md`                   | The interceptors on the `CREATE_TOPIC` path.                                 |
| `VERSIONING-STRATEGY.md`                          | The version format and release workflow.                                     |
| `RELEASES.md`                                     | Released artifacts (binaries, Docker images).                                |
| `SYSTEM_TESTS.md`                                 | To write or run system tests.                                                |
| `FAQ.md`                                          | Common questions.                                                            |
| `configs.rst`, `topic_configs.rst`, `metrics.rst` | Auto-generated config/metrics reference.                                     |

Upstream Kafka build, test, and tooling commands are in the root
[`README.md`](README.md). Upstream sync procedures live in
[`inkless-sync/`](inkless-sync/).

### Engineering Rules

**MUST**-follow defaults. All docs are under
[`.ai-agents/rules/`](.ai-agents/rules/).

| Topic    | Doc           | When to Load               |
| -------- | ------------- | -------------------------- |
| Comments | `comments.md` | Writing or reviewing code. |
