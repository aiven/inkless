
# Design Matrix

There are some common logical roles present in the proposed designs, this is a summary and some tradeoffs to consider.

Rows in the below tables are not mutually exclusive (a design may use multiple rows.)
Choosing rows in one table may conflict with another (not all cross-products are viable designs.)

## Proxy Language

| Instance | Description                                                             |
|----------|-------------------------------------------------------------------------|
| Java     | Used by Kafka<br/> +first class client support<br/> +easier to upstream |
| Go       | Used by WarpStream<br/> -no experience                                  |
| Cpp      | +no garbage collection<br/> -limited experience                         |
| Python   | -untyped<br/> +easier for internal developers                           |

## Proxy Implementation

| Instance        | Description                                                |
|-----------------|------------------------------------------------------------|
| Bespoke Service | Used by WarpStream<br/>-new boilerplate                    |
| Modified Kafka  | +easier to upstream<br/> +no boilerplate                   |
| Kroxylicious    | +prebuilt<br/> -immature<br/> -limited concurrency control |

## Object Storage

| Instance      | Description                               |
|---------------|-------------------------------------------|
| MinIO         | Used by WarpStream<br/>Testing only       |
| S3            | Used by WarpStream<br/>Supported from V1+ |
| S3 Express 1Z | -cost<br/>+latency                        |
| GCS           | Used by WarpStream<br/>Supported from V2+ |
| Azure Blob    | Used by WarpStream<br/>upported from V2+  |

## Object Cache

| Instance            | Description                                                          |
|---------------------|----------------------------------------------------------------------|
| Bespoke distributed | Used by WarpStream<br/> -design work & implementation                |
| Infinispan          | +Mature<br/> -dependency<br/>-learning & configuring, tuning         | 
| Ephemeral Volumes   | -fixed infrastructure cost<br/> +lower object GET cost<br/> -latency | 

## Object Format

| Instance         | Description                                           |
|------------------|-------------------------------------------------------|
| Bespoke Format   | Used by WarpStream<br/> -design work & implementation |
| Kafka Segment    | +no dependency<br/> -higher object store PUT cost     | 
| Data Lake format | -dependency<br/>-learning                             | 

## Batch Coordinate Format
| Instance              | Description                                     |
|-----------------------|-------------------------------------------------|
| Unknown               | Used by WarpStream                              |
| Bespoke binary format | -boilerplate<br/> -design work & implementation |
| Bespoke text format   | +readable                                       |
| Kafka binary format   | -design work                                    |
| SQL table schema      | -design work                                    |

## Order Consensus

| Instance        | Description                                                                                  |
|-----------------|----------------------------------------------------------------------------------------------|
| Bespoke service | Used by WarpStream<br/> -api design work & implementation<br/>                               |
| Kafka Topic     | -Needs multiple traditional brokers<br/> +no dependency                                      |
| SQL Database    | +existing wire protocol<br/> -dependency<br/> +well understood concurrency<br/> -scalability |
| NoSQL Database  | +existing wire protocol<br/> -dependency                                                     |

## Consumer Offsets

| Instance                | Description                                                                          |
|-------------------------|--------------------------------------------------------------------------------------|
| Bespoke Service         | Used by WarpStream<br/>-new boilerplate<br/> -need to port & maintain implementation |
| Kafka Group Coordinator | +prebuilt<br/> -requires controller/stateful kafka                                   |
