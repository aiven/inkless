## Decision Statement

Inkless brokers should use a new batch block format for storing records on remote storage.
A batch block will be a collection of record batches:
- from multiple partitions,
- produced by the same broker,
- that happened within the same time window (e.g. 250ms.)
- and have a maximum size (e.g. 8MiB)

A batch block will be stored as a single object on remote storage.
The metadata describing its structure will be stored in a separate metadata repository.

## Rationale

To achieve the desired performance and cost characteristics of Inkless, we need to optimize the storage format for records.
Storing record batches per topic/partition or per individual batch would result in a large number of small objects which will be inefficient to write and read.

By buffering multiple record batches from multiple topic/partitions into a single object, we can reduce the number of objects written to remote storage into a predictable number, amortizing its cost.

## Expected outcome

By separating the metadata from the data, we can optimize the storage format for each.
The metadata can be stored in a highly available, low-latency, low-throughput storage system.
The data can be stored in a highly durable, high-throughput, high-latency storage system.

### Data Format

```yaml
- batches: # sequence of batches
    - batch: [RecordBatch] # as https://kafka.apache.org/documentation/#messageformat
```

Record Batch: 
```
baseOffset: int64
batchLength: int32
partitionLeaderEpoch: int32
magic: int8 (current magic value is 2)
crc: uint32
attributes: # int16
  bit 0~2:
    0: no compression
    1: gzip
    2: snappy
    3: lz4
    4: zstd
  bit 3: timestampType
  bit 4: isTransactional (0 means not transactional)
  bit 5: isControlBatch (0 means not a control batch)
  bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
  bit 7~15: unused
lastOffsetDelta: int32
baseTimestamp: int64
maxTimestamp: int64
producerId: int64
producerEpoch: int16
baseSequence: int32
records: [Record]

```
Record
```
length: varint
attributes: int8
    bit 0~7: unused
timestampDelta: varlong
offsetDelta: varint
keyLength: varint
key: byte[]
valueLen: varint
value: byte[]
Headers => [Header]
```
Record Header
```
headerKeyLength: varint
headerKey: String
headerValueLength: varint
Value: byte[]
```

### Metadata Format

```yaml
- block_header:
    - id: UUID
    - broker: int
    - eventTimestamp: long
    - path: string
    - flags: int # e.g. compressed, encrypted
- topic_partitions:
    - topic_partition:  
        id: UUID
        name: string
        partition: int
        batches:
          - batch:
              - byte_offset: int
              - size: int
              - number_of_records: int
```

## Implications of the decision

// TODO 

## Status

Status: In discussion
