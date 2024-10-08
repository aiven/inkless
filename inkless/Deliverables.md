
# Development Phases & Deliverables

Status: Under Discussion

## Phase 0: Discovery

Goal: Research existing solutions, reusable code, known challenges, and requirements for the project. 

Acceptance criteria:
* Questions raised during research, and initial answers to all questions
* Architectural Design Records for decisions made by engineering
* Final Architecture Diagram and description for later phases.
* This document of deliverables for later phases

## Phase 1: Prototype

Goal: Show viability of architecture & interfaces by assembling a single-node implementation.
Temporary in-memory (v0) implementations of the interfaces should be used to accelerate development.

Allowances:
* Clusters are limited to a single node
* Records & consumer offsets are lost after a restart
* Broker can have attached persistent storage
* Broker only needs to handle Produce and Fetch requests for one partition.
* Excessive API call costs for unoptimized requests

Acceptance criteria:
* Broker should accept connections from non-idempotent Producers and read_uncommitted Consumers. 
* Latency between producer send & consumer poll is <30s
* Consumers reading >30s after writing receive same data with same ordering
* Can sustain continuous operation for >10 minutes

## Phase 2: Deployment Parity

Goal: Stretch interfaces over multiple nodes to support multi-node deployment requirements.
Temporary implementations are replaced with early (v1) implementations that can operate across the network, but may have performance problems.

Allowances:
* Maximum throughput may be very low
* Maximum number of partitions may be very low
* Producer/Consumer latency may be very high
* Records & consumer offsets may be lost in known circumstances
* Compaction may be unimplemented/inactive
* Object storage growth may be unbounded
* Concurrency may be limited

Acceptance criteria:
* Records & consumer offsets should be persisted durably
* Cluster must support multiple nodes
* Cluster must support multiple AZs
* At least one node in each AZ must have no persistent storage attached
* Object writes in a single broker should be batched together.
* Object reads in a single AZ should be cached for later fetches.
* Latency for inkless node join/leave should be <10m

## Phase 3: Scale Parity

Goal: Improve and characterize performance of the solution by implementing basic scale testing.
Early implementations (v1) may be swapped out (v2) or modified (v1.1) to resolve bottlenecks when discovered.
Interfaces may be modified to include performance optimizations

Allowances:
* Cluster may be unstable, experiencing errors or race conditions regularly.
* Records & consumer offsets may be lost in known circumstances

Acceptance criteria:
* Latency between producer send & consumer poll is <2s
* Latency for inkless node join/leave should be <1m
* Can support 100k topic-partitions
* Can support 10k records/sec per-partition
* Can support 1Mbps per-partition
* Can support 100 nodes/AZ
* Can support 100 producers per-partition
* Compaction should reduce API requests by >50%
* Compaction should eliminate unused/expired data faster than producer throughput
* Can sustain continuous operation for >1h

## Phase 4: Reliability Parity

Goal: Establish testing & operations practices to ensure reliable operation

Allowances:
* May be missing feature parity with traditional Kafka clusters

Acceptance criteria:
* Can sustain continuous operation for >1000h
* Does not lose data or consumer offsets in any known failure circumstance
* Metrics and Alerts for observability
* Tooling for operations
* Written documentation for operations
* Reliability review with SREs

## Phase 5: Apache Kafka Feature Parity

Goal: Establish support for existing Kafka functionality

Acceptance criteria: Include support for
* Low-Latency/Traditional topics
* Idempotent producers
* Transactional producers
* read_committed consumers
* Share groups
* Kafka Connect