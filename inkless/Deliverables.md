
# Development Phases & Deliverables

Status: Under Discussion

## Phase 0: Discovery

Goal: Research existing solutions, reusable code, known challenges, and requirements for the project. 

Acceptance criteria:
* Questions raised during research, and initial answers to all questions
* Architectural Design Records for decisions made by engineering
* Final Architecture Diagram and description for later phases.
* Interfaces for components within the architecture
* This document of deliverables for later phases

## Phase 1: Prototype

Goal: Show viability of architecture & interfaces by assembling a single-node implementation.
Temporary in-memory (v0) implementations of the interfaces should be used to accelerate development.

Allowances:
* Clusters are limited to a single node
* Records & consumer offsets are lost after a restart
* Broker only needs to handle Produce and Fetch requests for one partition.
* All allowances from the next phase

Acceptance criteria:
* Broker should accept connections from non-idempotent Producers and read_uncommitted Consumers. 
* Latency between producer send & consumer poll is <30s
* Consumers reading >30s after writing receive same data with same ordering
* Can sustain continuous operation for >10 minutes

## Phase 2: Deployment Parity

Goal: Stretch interfaces over multiple homogeneous nodes to support available and durable deployments.
Temporary (v0) implementations are replaced with early (v1) implementations that can operate across the network, and backed by durable storage.

Allowances:
* Object compaction may be unimplemented/inactive
* Broker can have attached persistent storage
* Object API requests can be cost-unoptimized
* All allowances from the next phase

Acceptance criteria:
* All acceptance criteria from the previous phase
* Records & consumer offsets should be persisted durably
* Cluster must support multiple nodes
* Cluster must support multiple topics and multiple partitions

## Phase 3: Cost Parity

Goal: Allow differentiating nodes to support disk-free operation & zone transfer cost-optimization.
Early (v1) implementations should be replaced with advanced (v2) versions that support node differentiation

Allowances:
* Maximum throughput may be very low
* Maximum number of partitions may be very low
* Producer/Consumer latency may be very high
* Object storage growth may be unbounded
* Concurrency may be limited
* All allowances from the next phase

Acceptance criteria:
* All acceptance criteria from the previous phase
* Cluster must support multiple AZs
* Object compaction should reduce API requests for lagging reads by >50%
* At least one node in each AZ must have no persistent storage attached
* Object writes in a single broker should be batched together.
* Object reads in a single AZ should be cached for later fetches.
* Latency for inkless node join/leave should be <10m

## Phase 4: Scale Parity

Goal: Improve and characterize performance of the solution by implementing basic scale testing.
Advanced implementations (v2) may be swapped out (v3) or modified (v2.1) to resolve bottlenecks when discovered.
Interfaces may be modified to include performance optimizations

Allowances:
* Cluster may be unstable, experiencing errors or race conditions regularly.
* Records & consumer offsets may be lost in known circumstances
* All allowances from the next phase

Acceptance criteria:
* All acceptance criteria from the previous phase
* Latency between producer send & consumer poll is <2s
* Latency for inkless node join/leave should be <1m
* Can support 100k topic-partitions
* Can support 10k records/sec per-partition
* Can support 1Mbps per-partition
* Can support 100 nodes/AZ
* Can support 100 producers per-partition
* Retention should eliminate unused/expired data faster than producer throughput
* Can sustain continuous operation for >1h

## Phase 5: Reliability Parity

Goal: Establish testing & operations practices to ensure reliable operation

Allowances:
* May be missing feature parity with competitors
* All allowances from the next phase

Acceptance criteria:
* All acceptance criteria from the previous phase
* Can sustain continuous operation for >1000h
* Does not lose data or consumer offsets in any known failure circumstance
* Metrics and Alerts for observability
* Tooling for operations
* Written documentation for operations
* Reliability review with SREs

## Phase 6: Competition Feature Parity

Goal: Establish support for functionality that is already implemented by other alternatives

Allowances:
* May be missing feature parity with upstream

Acceptance criteria:
* All acceptance criteria from the previous phase
* Support for Low-Latency/Traditional topics
* Support for Compacted topics
* Support for Idempotent producers
* Support for Kafka Connect

## Phase 7: Apache Kafka Feature Parity

Goal: Establish support for functionality already implemented upstream

Acceptance criteria:
* All acceptance criteria from the previous phase
* Support for Transactional producers
* Support for read_committed consumers
* Support for Share groups