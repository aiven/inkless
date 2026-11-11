# Read consistency after a leader change

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

What read guarantee does v2 give while a deposed coordinator leader hasn't yet learned that it lost leadership, and can a stale read cause harm beyond staleness?

v1 has only the leader answer reads, and user story 25 in the v1 spec promises strict `ListOffsets` results. See [Reads](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#reads) and [Leader change](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#leader-change). A deposed leader still answers `findBatches` and `listOffsets` from its own SQLite, and it can miss commits the new leader has acknowledged. Classic Kafka has the same window: KIP-320's `currentLeaderEpoch` fences clients with stale metadata, not a stale leader. A read barrier would close the window, but it adds blocking that the performance bar rules out.

Settle these points:

- The guarantee: matching classic Kafka, with leader epochs on the private read RPCs checked as KIP-320 does, or a leader lease that bounds the window by time.
- Whether a stale read can return batch coordinates for an object that the new leadership has since deleted, and what a consumer then sees.
- The new wording of user story 25 and story 45.
- The tests.

## Resolution

Resolved in a grilling session on 2026-11-09. v2 keeps Kafka's model: a deposed coordinator leader serves committed but older state until a metadata update reaches it. v2 adds no leader lease and no read barrier on the normal read path. One stale answer can do more than show older data, and v2 fixes that one on its error path with a barrier record.

### What a deposed leader serves

A KRaft broker doesn't step down when it loses the controller. `BrokerLifecycleManager` acts on the fenced flag only at the first unfence, so the broker keeps answering from its own SQLite until it applies the metadata record for the new leader. Unclean election is off, so that state is a committed prefix of the new leader's log. It lacks the new leader's commits and still holds rows that the new leader has since removed.

Kafka's coordinators have the same window. A deposed group coordinator answers `OffsetFetch` from its old state, and the consumer re-reads.

A leader lease was rejected. The deposed leader would stop serving once its followers stopped fetching, and each new leader would wait one lease before serving. That bounds the window, but it adds a wait to every failover and makes safety depend on clock drift rates. [Time bounds in the chosen model](03-time-bounds.md) removed clocks from safety decisions, and Kafka has nothing similar.

### The stale-read rule

Every coordinator read that a deposed leader answers is lower than or equal to the current answer, or unknown. A lower answer makes a consumer re-read records, and an unknown answer makes it retry. Neither makes it skip a record. `findBatches` above the high watermark is the only exception, and [Barrier for an offset above the high watermark](#barrier-for-an-offset-above-the-high-watermark) fixes it. A new read RPC that breaks this rule needs the barrier too.

| Read | Stale answer | Effect |
| --- | --- | --- |
| `findBatches` below the high watermark | Batches from the older state, possibly in objects that the new leader's deletion queue has since deleted | A missing object surfaces as the retriable `KAFKA_STORAGE_ERROR` in `FetchCompleter`. Object keys are never reused, so a reader never gets another object's bytes. Records that the new leader deleted can still be read, as from a classic deposed leader. |
| `findBatches` below the log start | The stale log start is at or below the true one | The out-of-range answer is always genuine. |
| `listOffsets` with `LATEST` or `EARLIEST` | Lower | Re-read. A position below the true log start gets a genuine out-of-range at a current leader. |
| `listOffsets` with a timestamp, including `by_duration` resets | Lower, or `UNKNOWN_OFFSET` when the older state holds no record at or after the timestamp | The candidate records in the older state include every current candidate below its high watermark, so the first match can only be lower. The consumer leaves an `UNKNOWN_OFFSET` partition unreset and tries again (`OffsetFetcherUtils.handleListOffsetResponse`). |
| `listOffsets` with `MAX_TIMESTAMP` | An older maximum | Same as a classic deposed leader. |
| `getCrossTierLogStart` | Lower or empty | The remote log manager reclaims less, or defers. The value only advances, so reporting it back does nothing. |
| `getFilesToDelete` | Keys in committed queue state | Those keys can't be referenced again. [Time bounds in the chosen model](03-time-bounds.md#grace-period) already covers a second delete. |

### Barrier for an offset above the high watermark

`findBatches` answers `OFFSET_OUT_OF_RANGE` for an offset above the high watermark. From a deposed leader, that answer can be false. A consumer that starts from an offset committed under the new leader then resets by `auto.offset.reset`: `earliest` rewinds it to the log start, and `latest` answered by a current leader skips records. Classic topics are protected from this by KIP-320: the consumer takes the leader epoch from its committed offset and refuses older metadata, through `Metadata.updateLastSeenEpochIfNewer`. For diskless, a coordinator leader change doesn't bump the user partition's epoch, and clients never see coordinator epochs. Without a fix, v2 falls below Kafka parity.

The rule:

- When `findBatches` gets an offset above the high watermark, the leader parks the request and appends a barrier record after the request arrives. Every such request on the coordinator partition within one linger interval shares one barrier.
- Once the high watermark passes the barrier, the leader checks the offset again. If the offset is still above the high watermark, the answer is `OFFSET_OUT_OF_RANGE`. If the high watermark passed the offset in the meantime, the fetch returns batches.
- If the barrier doesn't commit within 500 ms, the answer is `OFFSET_NOT_AVAILABLE`. The consumer refreshes its metadata and retries (`FetchCollector`). If the shard unloads, the parked request gets `NOT_COORDINATOR`. The 500 ms is a constant, not a config, and it's passed as the `CoordinatorRuntime` write-operation timeout. It equals the client default `fetch.max.wait.ms`, so a fetch on this path waits about as long as a long poll.
- A barrier from a deposed leader never commits. The append uses `acks=-1`, and the new leader came from the deposed leader's ISR and stops fetching from it once elected. A committed barrier therefore shows that no other leader took over before the request arrived, so the leader's state holds every record acknowledged before it.
- The barrier must be a new record. `CoordinatorRuntime` completes a write with no records once the pending writes complete (`CoordinatorRuntime.append`). On an idle deposed leader nothing is pending, so the write proves nothing. An earlier pending write doesn't prove it either, because a follower fetch sent before the new election can still commit it.
- Out-of-range below the log start and `listOffsets` keep their current answers.

The barrier adds nothing to the normal read path. A consumer that keeps up fetches at or below the high watermark. The cost is one small replicated append only for an offset above the high watermark, which means a stale read or a genuine seek past the end. A client that keeps fetching past the end costs at most one append per linger interval per coordinator partition.

Rejected alternatives:

- Always answering the retriable `OFFSET_NOT_AVAILABLE` above the high watermark. A consumer that genuinely seeks past the end would wait for the log to grow instead of resetting, which changes classic API behavior.
- Passing the fetch's `FetchParams.maxWaitMs` through `findBatches` as the barrier deadline. It changes `ControlPlane` for every backend, and a consumer with `fetch.max.wait.ms=0` would never see a genuine out-of-range.
- The request timeout or the commit deadline as the barrier deadline. A deposed leader would hold every fetch that touches it for about 30 seconds.

### Leader epochs on private RPCs

The private RPCs carry no leader epoch. They rely on `NOT_COORDINATOR`, as the group and share coordinators do. KIP-320's check exists so that a client can detect log truncation, and a broker that calls the coordinator keeps no position that a truncation invalidates. Rejecting a caller whose epoch is lower than the receiver's would only force a needless metadata refresh. Rejecting a caller whose epoch is higher only speeds up failure in a race that the stale-read rule already makes safe: a re-elected leader that hasn't yet applied its own election. Adding the epoch later is a tagged field, so no request version changes.

### User stories

- Story 24 becomes: "As a Kafka client, I want a fetch that the current coordinator leader answers to see every record committed before it, and a fetch that a deposed leader answers never to skip a record, so that a consumer doesn't miss data after a failover."
- Story 25 becomes: "As a Kafka client, I want `ListOffsets` from the current coordinator leader to include every acknowledged record, and a deposed leader's answer to be no later than the current one, so that a stale answer makes me re-read records but never skip them."
- Story 45 becomes: "As a leader, I want to serve only after my high watermark reaches the log end offset I saw at election, so that my replies include every record committed under earlier leaders." The term "strict read" leaves the spec.
- A new story, next to story 46: "As a Kafka client, I want a fetch above the high watermark to get `OFFSET_OUT_OF_RANGE` only after the coordinator leader confirms its leadership with a barrier record, and a retriable `OFFSET_NOT_AVAILABLE` otherwise, so that a deposed leader never resets my committed offset."

Story 33 stays, because stale state is still committed state.

### Failure mode and metric

The failure-mode table gets this row:

| Failure | Behavior |
| --- | --- |
| A deposed coordinator leader answers reads before it learns of the new leader | It serves committed but older state. `listOffsets` answers are lower or unknown. A coordinate into an object deleted since gives a retriable `KAFKA_STORAGE_ERROR`. A `findBatches` above the high watermark gets `OFFSET_NOT_AVAILABLE` once the barrier times out. This matches a classic deposed leader. |

One metric: barrier outcomes per coordinator partition, as one counter split into out-of-range, in-range, and timed out. A nonzero timed-out rate on a broker means it answers as a deposed leader. No other metric shows that.

### Tests

1. Barrier on the shard, in unit tests with `MockPartitionWriter`. The barrier commits with the offset still above the high watermark, and the answer is `OFFSET_OUT_OF_RANGE`. The high watermark passes the offset while the barrier waits, and the fetch returns batches. The high watermark never advances, and the answer after 500 ms is `OFFSET_NOT_AVAILABLE`. The shard unloads, and the answer is `NOT_COORDINATOR`. Two requests within one linger interval append one barrier record.
2. The stale-read rule, as a backend test. Build an older state and a newer one, with commits, retention, and `DeleteRecords` between them. For `EARLIEST`, `LATEST`, `MAX_TIMESTAMP`, and timestamp lookups, assert that the older answer is lower than or equal to the newer one, or unknown. A future read that breaks the rule fails this test.
3. Deposed leader, as a cluster test. Cut B1 off from the controller and its followers, commit through the new leader up to offset 1500, then consume `orders-0` from committed offset 1400 through B1 under `auto.offset.reset=earliest` and under `latest`. Assert that no reset moves the position, and that the consumer reads from 1400 once B1 learns it was deposed.
4. A genuine seek past the end, as a cluster test: `seek(highWatermark + 1000)` resets by the consumer's policy.

### Consequences for other tickets

- [Record compatibility across broker versions](08-record-compatibility.md) covers the barrier record like any other record type. Apply does nothing for it but advance the applied offset.
- The consequences pass in the map's Not yet specified list takes the failure-mode row, the barrier metric, and the story changes.
