# Diskless metadata coordinator

Status: draft for handoff.

Source: [KIP-1164: Diskless Coordinator](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783984/KIP-1164+Diskless+Coordinator), scoped down to a third `ControlPlane` implementation. Map and decision log: [map.md](map.md). The comparison of owner-based and scanner deletion: [deletion-comparison.md](deletion-comparison.md).

## Goals

- Add a `ControlPlane` implementation, `DisklessMetadataControlPlane`, selected with `inkless.control.plane.class`.
- Change the shared commit path in `FileCommitter` so that a broker orders its commits per coordinator partition, not across all its files. The Postgres and in-memory backends keep today's behavior. [Commit path](#commit-path) describes the change.
- Store the coordinator state in the internal topic `__diskless_metadata`, split across a fixed number of partitions. Each diskless user partition maps to one coordinator partition by a hash.
- Keep every replica's state in a local SQLite database, rebuilt from the log and from snapshots.
- Delete every WAL object that the design can orphan, through the object's owner coordinator partition.
- Pass the shared `AbstractControlPlaneTest` suite, and the cluster tests in [Testing](#testing).
- Add no public Kafka protocol. The new requests use private ApiKeys in the 504+ range.

## Non-goals

These are out of scope for this spec. [map.md](map.md#out-of-scope) lists them with their reasons.

- Changing the partition count of `__diskless_metadata` after the controller creates it. The slot layer keeps it possible, as [Future work](#future-work) describes.
- Moving an object's owner to another coordinator partition after the commit.
- Orphan-object scanning. A scanner that only reports suspected orphans stays possible as future work.
- Produce gateways and `PreferredProduceBrokers`.
- Moving a user partition between coordinators.
- Transaction state.
- The KIP's public APIs, keys 95 to 101.
- Follower reads.
- Migrating existing Postgres-backed clusters. New clusters only.
- Raft for the metadata quorum.

`__consumer_offsets` and `__transaction_state` split their state by key, because each request touches one group ID or one transactional ID. A WAL object is different: it carries batches for many user partitions, often across topics, and offsets for each user partition must come from one writer. The coordinator splits its state by user partition, so each user partition still has exactly one writer, and a commit goes to every coordinator partition its batches map to. That split needs no two-phase commit. Kafka doesn't make a non-transactional produce atomic across partitions, and `commitFile` already returns one result per batch, so a commit can succeed on one coordinator partition and fail on another. The only state that spans coordinator partitions is the object itself: whether any batch still references it, and whether it was ever committed at all. [File references](#file-references) gives each object one owner coordinator partition that tracks this state.

## Topology

- The internal topic `__diskless_metadata` has `diskless.metadata.topic.num.partitions` partitions, 8 by default, with replication factor 3, `min.insync.replicas=2`, and `unclean.leader.election.enable=false`. The replication factor key is `diskless.metadata.topic.replication.factor`. The controller creates the topic on first enable and places replicas across racks where possible. The partition-count default is provisional, and the benchmarks in [Capacity and isolation](#capacity-and-isolation) confirm it.
- The leader of each coordinator partition is the single writer for the diskless user partitions that map to it. [Coordinator partitions](#coordinator-partitions) defines the mapping. In the rest of this document, "the leader" means the leader of the coordinator partition that the user partition, call, or object under discussion maps to.
- Every replica fetches its coordinator partitions with normal replica fetching. Replicas do not vote.
- The coordinator appends to the topic with `acks=-1`. `Partition.appendRecordsToLeader` then rejects the append with `NOT_ENOUGH_REPLICAS` whenever the ISR is below `min.insync.replicas`, before the leader writes the record to its log. The high watermark doesn't advance under the minimum ISR either, so an acknowledged record is always on at least `min.insync.replicas` replicas. [Write path](#write-path) names the writer that does this.
- Unclean leader election stays off, so a replica outside the ISR never becomes leader and truncates acknowledged records.
- The topic is created with `flush.messages=1`, unlike other Kafka logs. A replica fsyncs every batch it appends, so the record a committed reply points to survives a power loss on that replica. The leader's append linger keeps this to one fsync per batch, not one per commit.
- Replication alone covers a single broker failure, because another in-sync replica still holds every acknowledged record. It does not cover a correlated power loss on every replica that holds a record before the operating system writes it back. For a coordinator partition, that loss reaches every user partition and object it serves: the next commit reuses the lost offsets, a lost fence lets a late commit point at a deleted object, and a lost intent leaves an object with no owner. The KRaft metadata log fsyncs every batch for the same reason. The commit-throughput benchmark in [Capacity and isolation](#capacity-and-isolation) measures the fsync cost.
- The topic is created with `retention.ms=-1` and `retention.bytes=-1`. Time and size retention never delete a segment, so the snapshot rule in [Snapshots](#snapshots) is the only thing that moves the log start.

## Coordinator partitions

- A diskless user partition maps to a slot, and the slot maps to a coordinator partition. The slot is `Utils.toPositive(Utils.murmur2(logKey)) % S`, where `S` is 4096, a protocol constant. The coordinator partition is `slot % N`, where `N` is the partition count of `__diskless_metadata`.
- The log key names the log a state change belongs to. The log key of a Kafka partition is the 16 bytes of the topic ID, most significant bits first, followed by the partition index as 4 big-endian bytes. That is the only encoding this spec defines.
- `diskless.metadata.topic.num.partitions` accepts only a power of two from 1 to `S`. With such an `N`, the two steps give the same result as `Utils.toPositive(Utils.murmur2(logKey)) % N`, so the slot layer adds no runtime cost and no stored state.
- The slot layer exists so that `N` can grow later without remapping every user partition. When `N` doubles, coordinator partition `p` keeps half of its slots, and the new partition `p + N` takes the other half. [Future work](#future-work) describes what that growth still needs.
- The log key includes the partition index, so the partitions of one topic spread across coordinator partitions, and a busy topic doesn't load a single leader.
- Every broker reads `N` from the metadata image, as `BrokerMetadataPublisher` passes the `__consumer_offsets` partition count from the metadata cache to the group coordinator. The config sets `N` only when the controller creates the topic. A call made before the topic exists gets a retriable error.
- `N` never changes after creation. A different `N` would map user partitions to other coordinator partitions, and moving a user partition between coordinators is out of scope. The controller rejects `CreatePartitions` for `__diskless_metadata`.
- The mapping function, the log key encoding, and `S` are part of the protocol. Every broker version must compute the same coordinator partition for a user partition, so the spec fixes them and a test pins them with fixed expected values.
- Each leader recomputes the mapping for every user partition in a request it receives. If a user partition doesn't map to that coordinator partition, the leader rejects the request with `INVALID_REQUEST` and counts the mismatch. A broker that maps differently would otherwise give a user partition a second writer.
- Each coordinator partition is an independent coordinator, with its own log, its own SQLite database on each replica, its own snapshots, its own overlay, and its own leader. Kafka's normal leader placement spreads the leaders across brokers.
- With `N=1`, every user partition and every object maps to partition 0, and the design reduces to a single coordinator.

## Transport

- Each broker holds a `DisklessMetadataControlPlane` delegate. For each call, the delegate maps every user partition in the request to its coordinator partition, splits the request by coordinator partition, and sends the parts in parallel. A part runs locally when the broker leads that coordinator partition. Otherwise the broker sends a private RPC to that partition's leader, which it finds from the metadata cache.
- The delegate merges the results of the parts back into the order of the original request, as each `ControlPlane` method requires.
- A broker that receives a request for a coordinator partition it does not lead returns `NOT_COORDINATOR`.
- The RPCs carry the arguments of the `ControlPlane` methods that have remote callers, plus the requests that [File references](#file-references) adds. The table in [Private RPCs](#private-rpcs) lists them.
- The Produce response cannot carry the offsets that a commit assigns, so the commit needs its own response. That is why the RPCs exist.

## Log records

The leader appends one outcome record per committed call, or per part of a call that spans coordinator partitions. Every replica applies records to SQLite in log order, and only up to the high watermark. Each record carries the state change and the assigned values, so every replica reaches the same state without recomputing anything.

The record types follow the KIP's record set, and the implementation may merge or split them:

| Record | Written by | Content |
| --- | --- | --- |
| Intent registered | Owner, before the upload | Object key, participant set, owner, uploader broker ID, and uploader broker epoch |
| File committed | Commit part, one per participant | Object key, format, size, uploader broker ID, uploader broker epoch, participant set, owner, first-send time, this partition's committed batches with their assigned offsets and producer fields, and the outcome of each other batch in the part |
| Partition created | create-partitions | Topic ID, name, partition index |
| Topic deleted | delete-topics | Topic ID |
| Records deleted | `deleteRecords` | Topic partition, new log start offset |
| Diskless log initialized | `initDisklessLog` | Topic partition, log start, diskless start, producer states |
| Diskless log repaired | `repairDisklessLog` | Topic partition, diskless start |
| Participant state recorded | Owner, on a release report or a fence answer | Object key, participant, state (referenced or released) |
| Owner accepted | Participant, after the owner accepts its release report | Object key |
| File fenced | Participant, on a fence from an owner or a predecessor fence in a commit part | Object key, the fencing owner or uploader broker, the answer: referenced with the recorded owner, or released |
| Owner entry dropped | Owner, when a fence answer names another owner | Object key |
| File delete marked | Owner, once the object can be deleted | Object key, marked-for-deletion time |
| File deleted | File cleanup on the owner | Object key |
| Keys expired | Leader, periodically | Object keys whose tombstones, fences, or stored batch outcomes have expired |
| Retention applied | `enforceRetention` | Topic partition, batches deleted, new log start |
| Log pruned | `pruneDisklessLogs` | Topic partition, new diskless log start |
| Cross-tier start advanced | `advanceCrossTierLogStartOffset` | Topic partition, remote log start offset |
| Deleted log purged | topic purge | Topic partition, optional boundary offset, whether the partition is now empty |

The records use the message-JSON conventions that Kafka uses for its other records, so their schemas can be versioned.

Every record names the log it changes with a `Log` struct that holds `TopicId` and `PartitionIndex`, not with a bare topic-partition field. The struct is the record form of the log key. A second log key encoding would add a tagged field to the struct, which readers that don't know it skip, so no record version changes.

Every change to a diskless partition's coordinator state is a record, including its creation and deletion. KRaft decides that a topic exists. The coordinator record fixes the position of the coordinator's own state change among the commits. Apply reads nothing outside its own coordinator partition's log, and it never reads a clock. An owner's view of other participants comes from records in its own log, and every time-based decision is a record the leader appends, such as keys expired. The same log therefore produces the same state on every replica, and a snapshot's tag fully describes its content. If a replica instead removed a topic's rows when its own `MetadataImage` dropped the topic, each replica would remove them at a different log position.

## Speculative state

The leader keeps an in-memory overlay of records it has appended and not yet applied. The overlay runs from the offset after the applied offset through the last written offset. Followers keep no overlay.

- An overlay entry holds the object key, the assigned offsets, the outcome of each batch that didn't commit, the producer epoch and sequence numbers, and a pending fence or intent when the entry is one.
- The leader checks and reserves on one thread. A commit reads SQLite plus every earlier overlay entry, so two commits cannot receive the same offsets.
- The leader collects reservations for one append linger interval, then appends them as one log batch. One replication round trip covers the batch.
- When the high watermark passes an entry, the leader applies it to SQLite, removes it from the overlay, and replies.
- If an append fails, the leader resigns, or the log truncates an entry, the leader removes that entry and every entry after it. It fails their parked replies with a retriable error. A later entry took its offsets and sequence numbers from an earlier entry, so the leader keeps no suffix past a broken entry.
- The overlay has a cap. Past that cap, the leader rejects a new commit with a retriable error.
- A new leader starts with an empty overlay. The overlay fills from the first commit after the high-watermark gate opens.

## Write path

The leader runs this loop on `CoordinatorRuntime`, the runtime the group coordinator and the share coordinator already use. The runtime supplies the single writer thread, the append linger, the revert to the last committed snapshot after a failed append, the parked replies that complete when the high watermark moves, and the `NOT_COORDINATOR` failure on resignation.

`CoordinatorRuntime` already runs one shard per partition of a coordinator topic, as the group coordinator does for `__consumer_offsets`. Each coordinator partition is one shard, loaded and unloaded with its leadership.

The runtime writes through a `PartitionWriter` that Inkless owns, not through the upstream `CoordinatorPartitionWriter`. The upstream writer appends with `acks=1`, and the group and share coordinators keep using it unchanged. With `acks=1`, a leader under its minimum ISR would still write the record to its log and park the reply until the operation times out. The Inkless writer appends with `acks=-1`, so the append fails at once, the runtime reverts the batch, and the caller gets a retriable error it can act on, such as falling back to another owner for an intent.

The shard stores the overlay in timeline structures backed by a `SnapshotRegistry`. Committed state stays in SQLite. Two pieces stay outside the runtime:

- A custom `CoordinatorLoader` replays from the applied offset stored in SQLite. The stock loader starts at the log start and would replay records SQLite already holds.
- A follower applier writes fetched records to SQLite up to the high watermark. The runtime runs on the leader only.

Rejected alternative: a write loop written for this coordinator. It would reimplement batching, revert, parked replies, and resignation handling.

Rejected alternative: the full state in `CoordinatorRuntime` timeline maps. Every election would replay the whole log, and `findBatches` and `listOffsets` would scan indexes held on the heap.

## Commit and retries

A commit starts before the upload. The broker closes the file, computes the participants, generates the object key, and stores an intent at the owner, as [File references](#file-references) describes. Only after the intent is stored does it upload the object under that key and send the commit. The key moves out of `FileUploadJob`, which generates it today, so that the intent can name it. Every upload attempt reuses the key.

### Commit path

`FileCommitter` commits on a single thread, which keeps the files of a broker in commit order. Only the order within a user partition matters, and on this backend that means the order within a coordinator partition. With one thread, a part that waits on an unavailable or slow coordinator partition, or on a new leader that rebuilds its database, holds back every later file of the broker. Most files hold batches for most coordinator partitions, so one coordinator partition would stall all diskless produce on every broker.

This spec changes the caller as well as adding a backend. `FileCommitter` and `ControlPlane` change as follows:

- `ControlPlane` maps each user partition to a commit queue. The new backend maps it to its coordinator partition. The Postgres and in-memory backends map every user partition to one queue, which keeps today's order.
- `ControlPlane` gets an intent hook that the broker calls with the object key and the participant set before the upload. The Postgres and in-memory backends do nothing in it.
- `ControlPlane` gets a per-part commit call that commits the batches of one file that map to one queue. The Postgres and in-memory backends call `commitFile` for it, because their one queue contains the whole file.
- `FileCommitter` keeps one ordered queue per commit queue, in file rotation order. The part of file `F2` on a queue runs once `F2` is uploaded and the part of the previous file on the same queue has finished: committed, failed, or past its deadline. It doesn't wait for parts on other queues.
- Every part of a file shares the file's commit deadline, measured from the moment the file closes. The deadline doesn't exceed the produce request timeout. Parts queued behind a stuck part each expire at their own file's deadline, so a stuck coordinator partition delays each queued part by at most one deadline, not one deadline per earlier file.
- A file completes once every part has an answer. `AppendCompleter` merges the per-batch results in request order, as it does today.

### Commit on the leader

The delegate sends each part of a file to the leader of its coordinator partition. Every part carries the object key, the participant set, the owner that stored the intent, the uploader's broker ID and broker epoch, the first-send time, and any predecessor keys. The first-send time is the broker's wall-clock time when the delegate first sent that part. [Predecessor fences](#predecessor-fences) defines the predecessor keys. Each leader runs its part in this order:

1. Reject the part when its first-send time is older than `diskless.metadata.commit.max.age.ms`, 5 minutes by default, measured on the leader's clock. Also reject it when the leader's metadata image holds a newer broker epoch for the uploader, or no registration for it. A part whose broker epoch is newer than the image's is accepted, because the image can lag behind the controller. A fenced uploader isn't rejected, so a short heartbeat gap doesn't fail commits in flight.
2. Fence each predecessor key that has no pending or applied commit on this coordinator partition. The fences go into the current batch even when the part itself records nothing.
3. Look up the object key in the overlay and in SQLite. When a commit for that key is pending or applied, the leader appends nothing and assigns no offsets. It returns the stored result of every batch in the part: the assigned offsets of the committed batches, and the stored outcome of each other batch. It waits for the pending record to apply when the key is still in the overlay. When the key is fenced or tombstoned, the leader rejects the part.
4. Check the producer epoch and sequence rules against SQLite plus the overlay. The Postgres `commit_file` functions enforce the same rules against committed state.
5. Reserve the key and assign the offsets in the overlay, then add one file-committed record to the current batch. The record stores the committed batches with their offsets, and the outcome of each other batch in the part: the error code of a failed batch, or the original batch's offset and timestamp for a duplicate. A retry sends the same batches in the same order, so a batch's position in the part identifies it.
6. When the high watermark passes the record, apply it to SQLite in the same transaction as the applied offset, remove the overlay entry, and reply with the per-batch results.

Every reply takes `logStartOffset` from current state, whether it answers the first attempt or a retry. The stored outcome contains only what the leader decided.

A part whose batches all fail their checks records nothing. Its object already has an intent at the owner, and the owner resolves that participant by fencing it. A retry of such a part runs the checks again. Commits are ordered per coordinator partition, so no later part from the same broker commits to that coordinator partition in between, and the answer is normally the same. The client has seen no answer for the first attempt either way.

Retries are safe because the object key identifies the upload. The broker generates a new key for every upload. Deduplication runs per coordinator partition, against the overlay and against SQLite. The same object key appears in every participant, each time with that participant's own batches. A `files` row exists only after apply, so a retry that arrives during the high-watermark wait would miss a check of that table alone. The check covers non-idempotent producers too, because the identity is the file, not the batch.

The stored outcomes are needed only while a retry can still pass the age check. Once the part's first-send time is older than one maximum commit age, a keys-expired record removes them.

### Deadline and unknown outcomes

The delegate retries each part on its own against the current leader of that coordinator partition, after `NOT_COORDINATOR` or a retriable error, reusing the same object key and the same first-send time. If the first attempt is still pending on the leader, the retry waits for its result and appends no second record. The delegate stops at the file's commit deadline. When the deadline passes:

- Batches on parts that confirmed get their per-batch results.
- Batches on parts that did not confirm get `KAFKA_STORAGE_ERROR`, as a whole failed commit does today. Their outcome is unknown: the part's last request can still reach the leader and commit later. That window closes when this broker's next part on the same coordinator partition fences the key, when the broker restarts, or when the part's first-send time passes the maximum commit age.
- If at least one part committed, the file returns these per-batch results. The committed batches are already visible to consumers, so the delegate must not fail them. A producer retries only the failed partitions, as with any partial produce failure.
- If no part committed, the file fails as a whole, and `FileCommitJob` runs its existing cleanup path.

That cleanup calls `isSafeToDeleteFile`. On the new backend it returns false without a request, so the broker never deletes the object itself. The owner deletes it once every participant is released. A `ControlPlaneUnavailableException` still makes `FileCommitJob` delete the object directly. That is safe, because no part was sent, and the owner's later delete of a missing object counts as success.

An unknown outcome differs from an `acks=all` timeout on a classic topic. There, the timed-out batch already sits at its offset before the producer retries, so a retry can duplicate it but never land ahead of it. Here, a late part appends at a later offset, possibly after the producer's retry has committed:

- An idempotent producer's retry is caught by its sequence numbers.
- A non-idempotent producer that retries through the same broker is covered by the predecessor fence. Its retry lands in a later file whose part on that coordinator partition fences the late key first, so the data appears once. If the late part commits before the fence, the data appears twice, in the original order.
- A non-idempotent producer that retries through another broker can see its data twice, with the late copy after the retried one. The maximum commit age bounds that window. Its default is 5 minutes, not one hour, to keep the window short.

### Predecessor fences

- A part that reaches its deadline unconfirmed would break ordering if its late request committed after a later part on the same coordinator partition. So the delegate remembers each unconfirmed key per coordinator partition, and the next part it sends to that coordinator partition carries those keys as predecessor keys.
- The leader fences each predecessor key that has no pending or applied commit, before it reserves the new part. For a key that did commit, the leader leaves the commit in place and reports it. Either way the delegate drops the key from its list once that part confirms.
- A predecessor fence is an ordinary fence at that participant. The fenced key rejects later commits and counts as released. A later fence from the owner gets the answer released.
- The delegate keeps the list in memory only. A broker restart loses it, but the restarted broker has a new broker epoch, and the leaders reject every part from the old one. In both cases the owner's fences resolve the object.
- A key that no later part follows stays unfenced until the owner fences it. No later batch from this broker exists on that coordinator partition, so nothing can be reordered behind it.

### Apply rules

The `files` insert in the Postgres implementation is a plain `INSERT`, so a repeated object key fails there. This spec changes that for the new implementation only.

Apply rules for file records are strict. The leader's checks make the cases below unreachable, so a replica that meets one treats it as a bug:

- A file-committed record for a key that is already applied with identical offsets and content is a no-op for file state. The applied offset still advances.
- A file-committed record for a key that is already applied with different content, or for a fenced or tombstoned key, is an invariant violation. The replica stops serving that coordinator partition and reports corruption.
- A file-fenced record for a key that is already fenced is a no-op. A file-fenced record for a key with an applied commit leaves the commit in place, and its answer is referenced with the owner named in that commit. These rules hold for predecessor fences too.

Rejected alternative: keeping the single commit thread. One stuck coordinator partition would then stall every broker's diskless produce, which defeats splitting the state across coordinator partitions.

Rejected alternative: answering a retry with a fixed error for the batches of a committed part that didn't commit. The error changes what the client does. `INVALID_PRODUCER_EPOCH` is fatal for a producer, while `KAFKA_STORAGE_ERROR` makes it retry, and a duplicate reported as an error is retried until it falls out of the producer's sequence window. Running the checks again on a retry can't commit those batches either, because the key already has a record.

## File references

Every object has one owner, a coordinator partition that records the object before it is uploaded and decides when it can be deleted. The other participants report to it. This section is the only path that deletes an object. [deletion-comparison.md](deletion-comparison.md) compares this design with a scanner that deletes objects no coordinator partition knows.

### Owner and participants

- The participants of an object are the coordinator partitions that its batches map to.
- The preferred owner is the participant at index `Utils.toPositive(Utils.murmur2(objectKeyBytes)) % participants.size()` in the sorted participant list, over the UTF-8 bytes of the object key. The hash spreads ownership across coordinator partitions.
- If the preferred owner can't store the intent, because of `NOT_COORDINATOR`, a retriable error, or a timeout, the broker tries the next participants in sorted order. Only the user partitions of an unavailable coordinator partition then fail, not the whole object.
- The owner is whichever participant stored the intent. Every commit part names it, and every record stores it, so no replica recomputes it. The fixed-value test pins only the preferred owner.
- The owner never changes after the commit, so no handoff between owners is needed.

### Intent

- The broker stores the intent before the upload starts. The intent is a record in the owner's log with the object key, the participant set, the owner, and the uploader's broker ID and broker epoch. Every object in storage therefore has an intent recorded first. The owner checks the broker epoch as a commit part's leader does, so it rejects an intent from an earlier incarnation of the broker.
- If no participant can store the intent, the broker uploads nothing, and the produce fails with `KAFKA_STORAGE_ERROR`.
- The broker gives up on the upload and the commit once the intent is older than the maximum commit age. A slow upload can't finish after the owner has deleted the object, short of storage completing a request the broker already gave up on.
- The intent creates the owner's entry for the object. The entry holds the participant set and each participant's state: silent, referenced, or released. A participant is silent until the owner hears from it, through a release report or a fence answer. A participant's state only moves forward, so a delayed answer for a participant that is already released is a no-op.
- The owner tracks its own state locally. It is referenced while its own part holds batches, and released once it holds none. If its own part has not committed when the intent passes the maximum commit age, the owner fences the key on itself and counts as released.

### Releases

- A participant releases an object when it holds no batch of it anymore. Apply moves the state to released when it removes the participant's last batch of the object, through retention, delete records, topic purge, or consolidation pruning.
- The participant leader reports each release to the owner named in its part, on a private request. The broker's scheduler sends and retries the report with Kafka's `ExponentialBackoff` client defaults, as for snapshot reports. The report is idempotent, so a repeat after a lost reply is a no-op at the owner. When the participant is the owner, it records the release locally and sends nothing.
- When the owner accepts the report, the participant appends an owner-accepted record. A new participant leader resends only the releases without one.
- When the owner-accepted record applies, the participant deletes its row for the object if the part's first-send time is older than the maximum commit age, measured against that record's append time. Otherwise it keeps a tombstone: the object key, marked closed. A tombstone rejects any later commit for the key, so a delayed retry can't re-add batches that retention or `DeleteRecords` removed.
- Participants report only releases. The owner learns everything else through its fences.

### Resolving silent participants

- The owner fences only when a fence can unblock deletion. Once the intent is older than the maximum commit age, and the owner and every participant it has heard from are released, the owner leader fences the participants that are still silent. The age is measured from the intent's append time, so a new owner leader needs no extra state to restart the timer.
- A silent participant either still holds batches and hasn't released yet, or never committed its part. Only a fence tells the two apart. The owner fences each participant at most once per object, and only at that point. An object needs no fence when every other participant reports its release before the owner releases.
- By then the broker has stopped retrying, because it gave up at the same age. A part that still arrives later reaches a fenced partition and is rejected.
- A fence is final. A participant that holds a commit for the key answers referenced, with the owner named in its part. The owner marks it referenced and waits for its release report without fencing it again. Any other participant records the fence, rejects any later commit for the key, and counts as released.
- A fence answer that names a different owner means the fencing partition holds a stale intent: its intent was stored, but the reply was lost, and the broker fell back to another owner. The fencing partition appends an owner-entry-dropped record and never deletes the object. A stale owner that is itself a participant drops its entry the same way when its own committed part names another owner.

### Deletion

- The owner appends a file-delete-marked record once every participant is released and the intent is older than the maximum commit age.
- `FileCleaner` runs on owner leaders only. It deletes the object after the deletion grace period, as it does today. Deleting a missing object counts as success. S3 already answers a missing key with success, and the GCS and Azure backends map their not-found answer to success.
- Object storage needs no transactions and no atomic writes for this rule. It needs a delete by key that removes whatever is stored there. Readers reach an object only through committed batches, so a partial object is never read. S3 writes each WAL object with one `PutObject`. GCS uses a single-request upload or a resumable session. Azure commits a block blob's blocks at once and drops uncommitted blocks on its own.
- An object leaks only when storage completes an upload after the owner deleted it, or when a bug loses a record. Every object that the design itself can orphan has an intent, so its owner resolves it.

### Tombstones and fences

- A tombstone or fence matters only while a commit for its key can still pass the age check. Each one is created after the commit it guards was first sent, so it can go once it is older than twice the maximum commit age. At the 5-minute default, that is 10 minutes.
- That bound assumes broker clocks differ by less than one maximum commit age. The age check compares the uploader's first-send time with the leader's clock, and expiry uses the leader's clock. If the skew is larger, a late part can pass the age check after its fence or tombstone expired. It then commits batches that point into a deleted object, or re-adds batches after a release. Deletion safety depends on broker clocks within that bound, though not on object-storage timestamps. The broker epoch check removes the risk for parts from a broker that has restarted since, but not for a live broker with a wrong clock.
- The leader appends a keys-expired record for tombstones and fences past that age. Apply removes them, so SQLite holds keys only for objects released or fenced within the last two maximum commit ages, however much data the cluster has deleted.

### Rejected alternatives

Rejected alternative: a two-phase commit across participants. A non-transactional produce has no cross-partition atomicity to preserve, and the extra round trip would block every commit.

Rejected alternative: one upload per coordinator partition per commit interval. It removes every cross-partition reference, but it multiplies object-storage writes by up to `N` and shrinks each object.

Rejected alternative: routing produce so each broker writes for one coordinator partition. Clients could no longer produce to any broker, and every availability zone would need a broker for each coordinator partition.

Rejected alternative: a scanner as the deletion path. It deletes objects that no coordinator partition knows, so a stale snapshot, a restore, or a shared bucket prefix deletes live data. Its safety also depends on clocks in more places, including the storage timestamp of each object. [deletion-comparison.md](deletion-comparison.md) has the full comparison.

Rejected alternative: a fence that the delegate sends on its own request at the commit deadline. The fence can fail like the part it guards, and it adds a request for every stuck part. The [predecessor fence](#predecessor-fences) rides on the next part to the same coordinator partition instead. It costs no extra request, and it lands before that next part commits, which is the only point where ordering is at stake. Neither form shortens deletion, which waits the maximum commit age anyway.

Rejected alternative: fencing at the commit deadline from the owner. The owner only learns when the commit started through its own part, and an owner fence doesn't order anything on the other participants.

## Local state

- Each replica of a coordinator partition stores that partition's state in its own SQLite database. A broker that hosts several coordinator partitions keeps one database for each. The database is a cache. The log is the source of truth.
- Each apply stores the applied offset and the leader epoch of the record, in the same transaction as the state change. On restart, the replica replays from that offset.
- On open, a replica checks its stored offset and epoch against the log: the log must still hold that offset, and the record at that offset must carry that epoch. A log flushed with `flush.messages=1` can still be truncated and rewritten by a later leader, so the offset alone does not prove the applied row still matches the log. This is the same check KRaft makes for a snapshot's `OffsetAndEpoch`.
- Invariant: the applied offset is always at or below the high watermark. A replica learns the high watermark from each fetch response. A record above the high watermark stays in the log buffer, unapplied, until the watermark passes it.
- SQLite holds only records at or below the high watermark. The leader drops unreplicated work from the overlay, so a replica never reverses an applied row.
- SQLite runs in WAL journal mode with `synchronous=NORMAL`. In WAL mode a reader does not block a writer, so a snapshot backup and a `findBatches` read do not stall applies. The WAL file grows during a long backup, because a checkpoint cannot pass an open reader, and it shrinks after the backup ends.
- With `synchronous=NORMAL`, a power loss can roll back the most recent applies but never corrupts the database. Each rolled-back apply takes its applied offset with it, so the replica reopens at an earlier offset and replays from the log, which `flush.messages=1` keeps durable. SQLite applies only records already in the local log, so it can fall behind the log but never get ahead of it. `synchronous=FULL` would add a second fsync to the leader's reply path.
- The schema has tables for logs, files, batches, the stored outcomes of batches that didn't commit, producer state, owner entries, participant release states, tombstones and fences, and the applied offset. The outcome table holds rows only for failed and duplicate batches of the last maximum commit age, so it stays small. The layout is the implementer's choice, but it must support indexed lookup by partition and offset, which `findBatches` and `listOffsets` need, and by object key.
- A coordinate row must be able to describe an extent: an offset range mapped to an object and a byte range, with the producer fields optional. A WAL batch row always carries its producer fields, because commits check idempotence against them. This spec writes only WAL batch rows. The requirement keeps merged and consolidated objects possible without one row per batch. The `ControlPlane` interface and the `findBatches` response don't change.
- The SQLite driver is a JDBC driver with a bundled native library. Its license and packaging are to be checked against the repo's dependency rules before the choice is final.

## Snapshots

Each coordinator partition runs the rules in this section on its own, with its own log start, snapshots, and reports. The snapshot requests name the coordinator partition.

A replica keeping up with the log never needs another replica's snapshot. It replays the log into SQLite and takes its own snapshots from that state. Snapshots exist so the leader can delete the log prefix, and a transfer exists for the replica that needs records the prefix deletion removed.

### Log start

- The log start is Kafka's own log start offset of the coordinator partition. Only the leader advances it, through `maybeIncrementLogStartOffset`, the call the `DeleteRecords` path uses. Followers adopt it from fetch responses, as `ReplicaFetcherThread` already does. Kafka refuses a log start above the high watermark, so the leader never deletes a record the ISR has not replicated.
- The leader advances the log start only to an offset that at least `min.insync.replicas` in-sync replicas, the leader included, hold a covering snapshot for. The new log start is the lower of two offsets: the offset of the leader's own older snapshot, and the highest offset that at least `min.insync.replicas - 1` other in-sync replicas have reported a snapshot at or beyond. At the default of 2, that is the leader and one other replica. A third in-sync replica that has not snapshotted yet does not hold the log start back. It fetches a snapshot if it later needs one.
- The leader uses its older snapshot, not its newest, so the snapshot it keeps after the next rotation still covers the log start.
- When the topic has a single replica, there is no other replica to confirm, and the leader advances the log start from its own older snapshot alone. A development cluster with `diskless.metadata.topic.replication.factor=1` still bounds its log.
- The wait delays log-start advancement only. Produce and fetch do not wait on a follower's snapshot report.
- The log start stays at offset 0 until the first advancement. A replica that loses its database before then replays the log from offset 0 and needs no snapshot. A new cluster therefore has no unavailability window before its first snapshot.

### Taking a snapshot

- Every replica takes its own snapshot from its own applied state, when either `diskless.metadata.snapshot.interval.ms` elapses, 1 hour by default, or `diskless.metadata.snapshot.max.bytes.between.snapshots` of log has been applied since its last snapshot, 1 GiB by default. Whichever comes first triggers it. This follows KRaft's `metadata.log.max.snapshot.interval.ms` and `metadata.log.max.record.bytes.between.snapshots`. The trigger counts bytes rather than records, because a file-committed record can carry hundreds of batches, and replay time follows bytes.
- Both defaults are provisional. KRaft's 20 MiB does not carry over, because a coordinator snapshot is a full copy of the SQLite database and the log grows with every broker's commit. With 1 GiB, a rebuild replays at most about 1 GiB after its snapshot, and each replica's log stays near 2 GiB. Consolidation keeps pruning batches as data moves to tiered storage, so the database is expected to stay small, and 1 GiB may be large next to it. The state-size and failover benchmarks confirm or lower the defaults, and they can show that the byte trigger should scale with the database size.
- The SQLite backup API writes a point-in-time copy to a temporary file in the snapshot directory. The replica reads the snapshot's offset and epoch from the applied-offset row inside the finished copy, never from the live database. The applied offset commits in the same transaction as the state it describes, so the copy's row is the exact position of the copy's content. A tag read from the live database before the backup can be too low, which replays records the copy already holds. A tag read after the backup can be too high, which lets the leader delete records the copy does not contain.
- The replica fsyncs the temporary file, renames it to a final name built from the offset, the epoch, and a checksum, and fsyncs the directory. Only then does it delete the snapshot that falls out of the retained pair. A crash at any step leaves the previous pair intact, and a partial file never carries a final name.
- On startup, the replica deletes any leftover temporary file. It checks a snapshot's checksum before it loads or serves that snapshot.
- Each replica keeps the newest snapshot and one older snapshot of its own. The count is a constant, not a config, because the log-start rule depends on that pair: with one snapshot, the leader's only covering snapshot would disappear at its next rotation, and a third would only use more disk. Snapshots are written to the broker's disk. They never go to object storage.

### Reporting snapshots

- A replica reports its newest snapshot offset to the leader on a private request after it finishes a snapshot, when it sees a new leader, and when its metadata shows it back in the ISR. The report is not a field on the public fetch request. Steady-state followers do not already send a private request, so this report is its own request.
- The broker's scheduler sends and retries the report with Kafka's `ExponentialBackoff`, using the client retry defaults: 100 ms initial, 1 second maximum, multiplier 2, and 20% jitter. These values are constants, not configs. A leader change needs no retry, because the replica reports again to the new leader. The replica fetcher thread never sends the report. A report blocked on an unresponsive leader would otherwise stall fetching and push the follower out of the ISR. A missed report only delays log-start advancement.
- The leader records the newest snapshot offset reported by each replica. When it considers advancing the log start, a report counts only if its replica is in the leader's committed ISR and has no pending removal. This is the ISR view the leader already uses for the high watermark, and the leader does not ask the controller.
- When a replica leaves the ISR, the leader drops that replica's report. The replica may have lost its snapshot files while it was out, and its report counts again only after it reports again.

### Fetching a snapshot

- A replica fetches a peer's snapshot only when it needs records below the log start. That happens when it starts empty, for example on a replaced broker or after a reassignment. It also happens when its database is lost or corrupt and its own snapshots do not reach the log start, or when it was out of the ISR while the log start moved past its newest snapshot.
- The replica fetches the newest snapshot that covers the log start from an in-sync follower that holds one. It falls back to the leader only when no follower has one. The leader already carries every commit and every cache-miss read for its coordinator partition.
- Every fetch-snapshot request names the snapshot ID, its offset and epoch, and a byte position. Rotation does not delete a file that has an active transfer. A transfer stays active until it finishes or until no chunk request has arrived for `diskless.metadata.snapshot.transfer.idle.timeout.ms`, 60 seconds by default. Each chunk request restarts that timer, so a large snapshot is never cut off for its total duration. Active transfers live in memory, so a source restart ends them all.
- A broker serves at most `diskless.metadata.snapshot.max.concurrent.transfers` active transfers across all the coordinator partitions it hosts, 1 by default. A request beyond that limit gets a retriable busy error, and the receiver tries the next replica with a covering snapshot, in the same follower-first order. A replaced broker therefore rebuilds its coordinator partitions one after another, and the brokers that serve the snapshots keep their disk and network for their own partitions. The snapshot-transfer benchmark confirms or raises the default.
- The receiver writes chunks to a temporary file under that snapshot ID, and a resume continues from its last fsynced byte. If the source answers that the ID is gone, the receiver discards its partial file and asks for the newest covering snapshot again, with the same follower-first order. The ID in every request means a resume never splices bytes from two snapshots, even across a source restart or a leader change.
- The request and response follow KRaft's `FetchSnapshotRequest` and `FetchSnapshotResponse` schemas. The request carries the receiver's replica ID, a maximum byte count, and either a snapshot ID with a byte position or, on the first request, a minimum end offset that asks for the source's newest snapshot covering the log start. The response carries an error code, the snapshot ID the source picked or confirmed, the total file size, the position of the chunk, and the bytes in a `records`-typed field.
- A chunk is at most 8 MiB, a constant equal to KRaft's `MAX_FETCH_SIZE_BYTES`. The `records` type lets the source send the chunk from the file with zero copy, as KRaft does with `UnalignedRecords`, so a transfer never copies the snapshot through the source's heap.
- `SNAPSHOT_NOT_FOUND` answers a snapshot ID that is gone, or no covering snapshot. `POSITION_OUT_OF_RANGE` answers a position outside the file. Both codes exist in Kafka already.
- When the file is complete, the receiver checks its checksum, replaces its database, takes its applied offset from the snapshot, and replays the log from there.
- A replica serves its finished snapshot files whatever the state of its live database. It stops serving a file only when that file fails its checksum. Two replicas that both need a rebuild can then still serve each other.
- A replica treats its database as missing or corrupt when it fails the open check, or when its applied offset is ahead of its log. Such a replica rebuilds before it serves: from a peer's snapshot once the log start has moved, and from offset 0 before then. This includes a newly elected leader: the high-watermark gate in [Leader change](#leader-change) stays closed until the rebuild and the replay finish.

This is the KIP-630 mechanism in outline, applied to a normal topic. It is the one part of the design that has no equivalent in the KIP's text. Snapshot distribution is a private RPC because a normal topic has no `FetchSnapshot`.

## Reads

- Only the leader answers reads. `findBatches`, `listOffsets`, and every other call go to the leader of each user partition's coordinator partition, and the leader serves them from state that includes every committed record.
- The delegate splits `findBatches` by coordinator partition. `fetchMaxBytes` is one budget across the whole request, so the delegate sends the full budget with each part, merges the results in request order, and trims them so the response stays within that budget. The results are batch coordinates, not data, so the extra rows a part returns cost little.
- Brokers absorb most fetch-side lookups in the existing batch coordinate cache. A cache miss goes to the leader.
- The KIP's stale `DisklessFindBatches` path is an optimization. It is not part of this spec.

## Capacity and isolation

Status: open. The severity of this risk is unknown until the benchmarks in this section run against the Postgres backend.

Each coordinator partition's leader serves every commit part, intent, release report, cache-miss `findBatches` and `listOffsets` call, and maintenance job for its user partitions and owned objects, on top of the broker's own partitions. `N` leaders share the cluster's load, and Kafka's leader placement spreads them across brokers.

Splitting reduces the work per batch, not the number of commit calls. An object usually holds batches for most coordinator partitions, so each leader still receives about one commit part per broker per commit interval. A produce waits for the intent and then for its slowest part, so the commit-latency tail grows with `N`.

The load differs from the Postgres backend in two directions:

- Lower cost per call: SQLite runs in the broker process, so a call has no network round trip, connection pool, or remote query planning. The Postgres backend spends part of its commit latency on those.
- Co-tenancy: SQLite shares the leader broker's heap, page cache, disk, and garbage collector with that broker's own partitions. A large `findBatches` scan can evict the broker's log pages. A garbage collection pause on that broker stalls every commit that has a part on the coordinator partitions it leads, and with most objects spanning most coordinator partitions, that is most commits. The Postgres backend runs on its own host and has none of this contention.

A cluster-wide consumer restart empties the batch coordinate cache on every broker at once. Every miss then reaches the leaders together.

### Benchmarks

Each benchmark runs the same workload against the Postgres backend and against this backend, at `N=1` and at `N=8`, and compares the broker metrics both backends already report: `FileCommitTime`, `FileCommitWaitTime`, `FindBatchesTime`, and `FileCommitErrorRate`. The benchmarks extend the producer setup in `inkless-benchmarks/`.

- Commit throughput: increase the number of brokers and producers until commit latency rises. Record the commit rate at which each backend's P99 `FileCommitTime` doubles. This backend runs with `flush.messages=1`, so the result includes the fsync cost on the leader and on the followers.
- Partition count: run a fixed commit rate at `N` of 1, 4, 8, and 16. Record P99 `FileCommitTime`, the commit parts each leader receives, and per-leader CPU and fsync rate. The result confirms or replaces the default of 8.
- Intent cost: measure the latency the intent adds to each produce, and the intent records each owner appends.
- Partial failure: take one coordinator partition offline under load. Measure the share of produces that fail and the latency of parts on the healthy coordinator partitions, including parts of files that also hold batches for the offline one, with owner fallback on and off. Parts on healthy coordinator partitions must not wait for the offline partition's deadline.
- Read-heavy load: run lagging consumers that miss the batch coordinate cache, alongside steady produce. Compare `FindBatchesTime` and the leader's produce latency for its own partitions.
- Cache storm: restart every consumer at once and measure how long `FindBatchesTime` stays above its steady-state P99.
- Co-tenancy: load a leader broker's own partitions heavily during the commit and read benchmarks. Compare that broker's produce latency with that of a broker that leads no coordinator partition.
- State size: grow the file and batch row count across several orders of magnitude, and record `findBatches` latency, election time, SQLite file size, snapshot backup duration, and log growth between snapshots at each step. Run it with consolidated topics, which is the expected production workload.
- Failover: kill a leader under load and record the time until commits succeed again.
- Snapshot transfer: rebuild one replica, then two at once, while commits and reads run at steady load. Record the leader's `FileCommitTime` and `FindBatchesTime`, the source replica's disk and network use, and the time each rebuild takes. A transfer streams a file that can be large off the source's disk, which is the same co-tenancy risk as a large `findBatches` scan.

The workloads, scales, and pass criteria need more design before these run. Results decide whether the coordinator needs a separate disk for SQLite, a cap on `findBatches` result size, a higher `diskless.metadata.snapshot.max.concurrent.transfers` default, a different default partition count, a different default maximum commit age, or none of these. The 5-minute default maximum commit age stays well above upload and commit time on NTP-synced brokers, and the benchmarks confirm it. If the fsync cost is too high, the fallback is Kafka's default flush, with the correlated power-loss exposure in [Topology](#topology) recorded as accepted.

## Metrics

These metrics show whether the owner mechanism resolves every object and keeps state bounded:

- Unknown-outcome parts: commit parts that reached the commit deadline without a confirmation, per coordinator partition.
- Open owner entries: the count per owner and the age of the oldest. An entry whose fences stay unanswered points to a stuck resolution.
- Fences: the fences issued, by answer: referenced, released, or owned elsewhere.
- Tombstones and fences held: the count per coordinator partition.
- Parts rejected by the age check: a nonzero rate points to a stuck request path or clock skew.
- Parts and intents rejected by the broker epoch check: a nonzero rate shows requests that a broker sent before it restarted.
- Predecessor fences: the unconfirmed keys that a later part fenced, and those that turned out to have committed.
- Mapping mismatches: requests rejected because a user partition doesn't map to the receiving coordinator partition.

## Leader change

- Each coordinator partition changes leader on its own. A leader change stalls only the commit parts, intents, reads, and owned objects of that coordinator partition.
- A new owner leader restarts its fence timers from the append times of the intents, and a new participant leader resends the releases that have no owner-accepted record.
- A new leader starts with an empty overlay. It rejects requests with a retriable error until its high watermark reaches the log end offset it saw at election.
- Records that were not committed under the old leader are discarded by the log. The leader drops those entries from its overlay and fails their parked replies with a retriable error. Their requests were never acknowledged, so the retry rule in [Commit and retries](#commit-and-retries) covers them.
- The delegate retries the commit part against the new leader after a `NOT_COORDINATOR` or retriable error, using the same object key and first-send time, until the commit deadline passes.

## Lifecycle

- The controller never calls the coordinator. A diskless topic's creation or deletion commits to cluster metadata only. `ControllerApis.createTopicsDiskless` and the diskless delete path stop calling the control plane, which removes the ten-second metadata wait on the request thread.
- Each coordinator partition's leader reconciles its state against the metadata image. `BrokerMetadataPublisher.onMetadataUpdate` already delivers that image to every broker, and it is where the group and share coordinators learn about topic changes. On election and on each metadata update, the leader diffs the user partitions in its SQLite against the diskless user partitions in `MetadataImage` that map to its coordinator partition. A delta's `deletedTopicIds` is not enough: a broker that loads a snapshot never sees a delete for a topic the process did not hold. For each mapped diskless user partition in the image that SQLite does not hold, the leader appends a partition-created record. For each topic ID SQLite holds that the image no longer has, it appends a topic-deleted record. The partitions of one topic map to several coordinator partitions, so each of those coordinator partitions creates and deletes its own share of the topic, at slightly different times. KRaft already stops produce to a deleted topic at the broker. A leader that was down catches up when it returns, and the metadata log is the retry record, so the coordinator keeps no separate queue. The diff is by topic ID, so a topic recreated under the same name is a new ID and does not collide with the one being deleted.
- The diff counts pending overlay entries as well as SQLite rows, so two metadata updates that arrive before the first partition-created record applies do not append it twice. Apply is idempotent as a second guard. A partition-created record for a row that exists, or a topic-deleted record for a topic already marked deleted, is a no-op, and the applied offset still advances. The Postgres insert does the same with `onConflictDoNothing`.
- A produce to a diskless partition that the leader has not reconciled yet gets a retriable error. Reconciliation finishes once the high-watermark gate opens.
- The admin client stops retrying a delete once the controller reports success, which is when the topic leaves the metadata log. It does not retry coordinator cleanup. Today's `ControllerApis` deletes from the control plane before the KRaft delete for that reason. Reconciliation removes the need for that order: a topic ID still in SQLite and absent from the image is retried on the next metadata update and on the next election.
- The leader of a switched partition calls `initDisklessLog` after the controller has committed the classic-to-diskless seal. `ControllerApis.handleInitDisklessLogRequest` writes that seal into cluster metadata. The control plane call happens later, from `InitDisklessLogManager` on the partition leader, through `AwaitingMetadata.sendBatch` in `InitDisklessLogState`.
- That same partition leader calls `repairDisklessLog` from `ReplicaManager`. `KafkaApis.handleRepairDisklessLogRequest` invokes `ReplicaManager`, which calls the control plane only while this broker leads the partition.
- Each call appends its record and replies once the high watermark passes it. A user partition's create, delete, initialization, and repair go to the same coordinator partition as its commits, so they are ordered with every commit to that user partition in one log. Ordering across user partitions is not guaranteed, and nothing depends on it.

## Maintenance

- Retention and topic purge run on each coordinator partition's leader, for its own user partitions. Each run appends its outcome records to the log.
- Topic purge works through deleted partitions in capped passes, as `purge_deleted_logs_v1` does in Postgres. Each pass appends one deleted-log-purged record per partition it touched, in one log batch. Applying a deleted-log-purged record removes that partition's batches whose last offset is below the boundary, or all of them when the record has no boundary. When the record says the partition is empty, the apply also drops its `logs` and `producer_state` rows. The objects whose last local batch the pass removed become released, and the participant reports them to their owners.
- File deletion marking and object deletion run on owner leaders, as [File references](#file-references) describes. Object deletes wait for the deletion grace period, as `FileCleaner` does today.
- Expiry of tombstones and fences runs on every leader, through keys-expired records.

## Consolidation and cross-tier offsets

- The broker that hosts a partition's local log decides the safe prune offset, from `highestOffsetInRemoteStorage` and the classic-to-diskless seal.
- The broker sends `pruneDisklessLogs` or `advanceCrossTierLogStartOffset` to the leader. The leader appends the outcome record.
- The broker advances its own prune floor from the response.
- The same pattern covers `CrossTierLogStartReporter` and the delete-records path.

The leader does not need a local log for any partition, because it never reads one.

## Rollout

- A new cluster finalizes the coordinator feature at format time, with `kafka-storage format --feature`. The coordinator is active from the first startup.
- The controller creates `__diskless_metadata` with `diskless.metadata.topic.num.partitions` partitions. Changing that config afterward has no effect, and the controller rejects `CreatePartitions` for the topic.
- The controller rejects registration from a broker that does not support the finalized feature.
- `inkless.control.plane.class` must name the new class. If it names the in-memory or Postgres class while the feature is finalized, the broker refuses to start.
- Diskless topics still need the existing Inkless storage configuration and `diskless.enable=true`.
- There is no rollback onto Postgres. A feature downgrade would orphan the files and batches in `__diskless_metadata`, and no migration exists. Clusters already on Postgres are never formatted with the feature.

## Private RPCs

Numbers are assigned in the 504+ range at implementation time. Each request type and its handler is listed here. Every request names the coordinator partition it targets, and "Leader" means that partition's leader.

| Request | Caller | Handled by |
| --- | --- | --- |
| Register intent | Broker, before the upload, to the preferred owner and then to the fallback owners | Leader of the owner |
| Commit file | Broker, from `FileCommitJob`, one request per participant | Leader |
| Find batches | Broker, on cache miss | Leader |
| List offsets | Broker | Leader |
| Delete records | Broker, from the delete-records path | Leader |
| Init diskless log | Partition leader, from `InitDisklessLogManager` after the controller commits the seal | Leader |
| Repair diskless log | Partition leader, from `ReplicaManager` | Leader |
| Enforce retention | Local only, on the leader | Not an RPC |
| Prune diskless logs | Broker, from `ConsolidatedDisklessLogPruner` | Leader |
| Advance cross-tier log start | Broker, from `CrossTierLogStartReporter` | Leader |
| Report release | Participant leader's scheduler | Leader of the owner |
| Fence file | Owner leader, for a silent participant | Leader of the participant |
| Fetch snapshot | Replica, when it needs records below the log start | An in-sync follower that holds a covering snapshot, or the leader when no follower does |
| Report snapshot offset | Replica's scheduler, after a snapshot, on a new leader, and on rejoining the ISR | Leader |

Each request carries the coordinator partition and the topic partition or file it acts on, and it returns the same error codes as the `ControlPlane` method it wraps.

## Failure modes

| Failure | Behavior |
| --- | --- |
| Leader crashes during a commit | The part was not acknowledged. The delegate retries against the new leader within the commit deadline. The object-key rule returns the stored offsets, or assigns them on first commit. |
| A commit part misses the deadline, and its record commits later | Its batches get `KAFKA_STORAGE_ERROR`. The late record makes them visible anyway, unless this broker's next part on that coordinator partition fenced the key first, the broker restarted, or the part's first-send time passed the maximum commit age. Idempotent producers deduplicate their retries. The owner deletes the object only once that participant is released. |
| A coordinator partition is unavailable, slow, or rebuilding its database | Parts on it wait until their file's deadline, then get `KAFKA_STORAGE_ERROR`. Parts on other coordinator partitions keep committing, because every broker keeps one commit queue per coordinator partition. |
| The ISR of a coordinator partition drops below `min.insync.replicas` | Appends fail at once with `NOT_ENOUGH_REPLICAS`, and the runtime reverts the batch. The delegate retries parts until the deadline, and the broker falls back to another owner for an intent. |
| Some parts of a commit succeed and others fail | The committed batches return their offsets. The others get `KAFKA_STORAGE_ERROR`, and the producer retries only those partitions. |
| A retry arrives for a part where some batches failed or were duplicates | The leader returns the stored outcome of every batch, also after a failover. |
| The uploader broker restarts with parts or intents in flight | Leaders reject the requests that carry the old broker epoch. The owner fences the participants that never committed. |
| Retry arrives while the first attempt is pending on the leader | The retry waits for the pending record's result. Only one record and one set of offsets exist. |
| The preferred owner can't store the intent | The broker tries the next participants in sorted order. If none can, nothing is uploaded, and the produce fails with `KAFKA_STORAGE_ERROR`. |
| An intent is stored, but its reply is lost | The broker falls back to another owner. The stale owner fences the participants later, learns the real owner from a fence answer, and drops its entry without deleting the object. |
| Broker stops after the intent and before the upload | The owner fences every participant once the intent is older than the maximum commit age, then deletes the missing object, which counts as success. |
| Broker stops after the upload and before any part commits | The owner fences every participant once the intent is older than the maximum commit age, then deletes the object. |
| Broker stops after some parts commit | Each committed participant reports its release to the owner later. Once the owner and every participant it has heard from are released, the owner fences the silent ones, and the ones that never committed count as released. |
| Storage completes an upload after the broker gave up on it | If the owner already deleted the object, the object leaks. The broker gives up only once the intent is older than the maximum commit age, so the window is small. |
| Owner leader is unreachable for a release report | The participant retries. Only that object's deletion waits. Commits and reads continue. |
| Broker clocks differ by more than the maximum commit age | The age check and the expiry of tombstones and fences lose their guarantee. A delayed part from a live broker can then commit after its fence or tombstone expired, pointing into a deleted object or re-adding batches after a release. Parts from a restarted broker are still rejected by the broker epoch check. The age-check rejection metric shows clock skew before it reaches that size. |
| Leader loses the partition | The new leader serves once its high watermark reaches its election-time log end offset. |
| Coordinator leader is down when a topic is created or deleted | The metadata change commits without it. The new leader reconciles on election, so the partition is created or soft-deleted then. Produces to the new topic get a retriable error until reconciliation finishes. |
| Append fails, or the leader resigns, before the high watermark | The leader drops the failed overlay entry and every entry after it, and fails their parked replies with a retriable error. SQLite is unchanged. |
| Leader change truncates unreplicated records | The leader drops the truncated entries from its overlay and fails their parked replies with a retriable error. A replica discards fetched records above its applied offset that the new leader does not have. Because the applied offset never exceeds the high watermark, the truncation never reaches applied state, and no SQLite row is undone. |
| Replica falls behind the log start | It fetches the newest snapshot that covers the log start, from an in-sync follower or, when no follower has one, from the leader, then replays. |
| Replica's SQLite is corrupt or missing, or its stored offset and epoch no longer match the log | It rebuilds from a peer's snapshot that covers the log start, then replays the log. Before the log start has first moved, it replays from offset 0 instead. A replica that is the elected leader rejects requests until the rebuild finishes. |
| Power loss on a replica | `flush.messages=1` keeps every appended record. SQLite may roll back its most recent applies, and the replica replays them from the log. |
| Crash while a snapshot is written | Only the temporary file can be incomplete. Startup deletes it, and the previous pair of snapshots stays intact. |
| Snapshot source rotates or restarts during a transfer | Rotation keeps a file while its transfer is active. If the transfer idled out or the source restarted and the snapshot ID is gone, the receiver discards its partial file and fetches the newest covering snapshot. |
| Every replica with a covering snapshot is already serving its limit of transfers | Each answers with a retriable busy error. The receiver retries in follower-first order until a source frees up. |
| Two replicas need a rebuild at once | Each serves its finished snapshot files to the other, whatever the state of its live database. |
| A replica leaves the ISR after reporting a snapshot | The leader drops its report. The log start does not advance on that report until the replica rejoins and reports again. |
| Fewer than `min.insync.replicas` in-sync replicas, the leader included, hold a snapshot at the leader's older-snapshot offset | The leader does not advance the log start. Produce and fetch continue. |
| The leader is unreachable when a replica sends a snapshot report | The scheduler retries with backoff. Replica fetching continues, and only log-start advancement waits. |
| Leader dies after the log start has moved | Another in-sync replica has reported a snapshot that covers the log start. A new leader whose database is missing rebuilds from that snapshot. |
| The confirming replica later loses that snapshot, and the new leader's database is also missing | The leader cannot rebuild and must not serve. Diskless writes for that coordinator partition's user partitions stay unavailable until a covering snapshot exists. How the leader gives up leadership is an open item. |
| The topic has one replica | The leader advances the log start from its own older snapshot. There is no other replica to confirm. |
| A replica is down | With three replicas and `min.insync.replicas=2`, writes continue. |
| Two replicas of a coordinator partition are down | Its ISR is below the minimum, so its appends fail at once. Commit parts, intents, and reads for that coordinator partition stop until a replica returns. Other coordinator partitions keep committing, because their commit queues don't wait on it, and owner fallback keeps objects uploading. |

## Testing

Three layers, in this order:

1. The shared `AbstractControlPlaneTest` suite runs against `DisklessMetadataControlPlane`, as it runs against the in-memory and Postgres backends. It runs at `N=1` and at `N=8`. One broker leads every coordinator partition, so the second run covers request splitting, result merging, intents, and releases.
2. A multi-broker `KafkaClusterTestKit` test covers these cases:
   - Leader failover in the middle of a commit, followed by the retry path.
   - A leader change that truncates unreplicated records. The follower's state matches the new leader's state, and no applied record was undone.
   - A retry that arrives while its first attempt is pending on the leader. The client sees one record and one set of offsets, for idempotent and non-idempotent producers.
   - A commit whose parts land on several coordinator partitions, where one part fails. The committed partitions return offsets, the failed ones get `KAFKA_STORAGE_ERROR`, and the object is deleted only after every participant is released.
   - A preferred owner that is down. The broker falls back to the next participant, and only the down coordinator partition's user partitions fail.
   - An intent stored at the preferred owner whose reply is lost. The stale owner drops its entry after a fence answer names the real owner, and it never deletes the object.
   - A broker that stops after the intent and before the upload, and one that stops after the upload and before any part commits. The owner fences every participant once the intent passes the maximum commit age, then deletes the object.
   - A broker that stops after some parts commit. The owner fences the participant that never committed, and it deletes the object only after the committed participants release it.
   - An object whose batches expire on one coordinator partition while another still holds batches. The object stays until both are released.
   - A part whose first-send time is older than the maximum commit age. The leader rejects it.
   - One coordinator partition offline while produce runs to user partitions on every coordinator partition. Parts on the other coordinator partitions commit without waiting for the offline partition's deadline.
   - A part that reaches its deadline, followed by the same broker's next part on that coordinator partition. The leader fences the earlier key, a late request for it is rejected, and the later batches commit.
   - A coordinator partition whose ISR drops below `min.insync.replicas`. Commit parts fail at once rather than at the deadline, and intents fall back to another owner.
   - A retried part where some batches failed and some were duplicates, sent to a new leader after a failover. The retry returns the same per-batch results as the first attempt.
   - A broker that restarts with a part in flight. The leader rejects the part's old broker epoch.
   - A release that happens within the maximum commit age of its commit. The participant keeps a tombstone, a delayed retry of the commit is rejected, and the tombstone expires after twice the maximum commit age.
   - An owner leader change with open entries. The new leader restarts its fence timers, and the participants resend their unaccepted releases.
   - A `CreatePartitions` request for `__diskless_metadata`. The controller rejects it.
   - A part that names a user partition the receiving coordinator partition doesn't own. The leader rejects it with `INVALID_REQUEST`.
   - A replica that catches up from a snapshot after the log start has moved.
   - A newly elected leader whose database is missing. It rebuilds from a follower's snapshot, and it serves no request before the rebuild finishes.
   - A leader whose older snapshot is ahead of every other in-sync replica's reported snapshot. The log start stays put. Once a follower reports a snapshot at that offset, the log start advances, and that follower's snapshot still covers it after the leader stops.
   - A follower that reports a snapshot and then leaves the ISR. The log start does not advance on its report until it rejoins and reports again.
   - A snapshot source that rotates or restarts during a transfer. The receiver never loads a file spliced from two snapshots, and it finishes from the newest covering snapshot.
   - A rebuild that a follower serves while the leader is under commit load. The leader serves no transfer while an in-sync follower holds a covering snapshot.
   - A replica that loses its database before the log start has first moved. It replays from offset 0 and needs no snapshot.
   - A single-replica topic. The leader advances the log start from its own older snapshot.
   - A topic deleted and purged. Every replica drops the same batches and rows as the leader, and the owners delete the emptied objects.
   - A topic created while a coordinator leader is down. The leader reconciles on return, and a produce before reconciliation gets a retriable error. The same holds for a topic deleted in that window.
   - A cluster formatted with the feature. A conflicting control plane class refuses to start, and a broker that lacks the feature cannot register.
3. Fault injection covers the leader's high-watermark gate after an election, the object-key retry rule, a lost intent reply, a part whose request reaches the leader after its deadline, and a broker stop between intent, upload, and commit, so these behaviors are deterministic to test.

The mapping function and the preferred-owner choice get a unit test with fixed expected values, so a change to either fails the build. The same test checks that the slot mapping equals `Utils.toPositive(Utils.murmur2(logKey)) % N` for every allowed `N`, and that the config rejects an `N` that isn't a power of two.

Snapshot writing, rotation, loading, and chunk serving get unit tests on a temporary directory, with a fault hook at each step of rotation and transfer. They cover a tag read from inside the copy while applies run during the backup, a crash at each rotation step, a transfer whose source rotates or restarts before the receiver resumes, and a checksum mismatch on load and on serve. No public seam reproduces these cases deterministically. The cluster tests still cover snapshot behavior end to end.

Apply rules for file records, owner entries, releases, tombstones, and expiry get unit tests at the replica state layer, with records appended directly to a test log. No public call can produce a duplicate or conflicting record, or control the append times that the tombstone rule compares, so this is the one internal seam, and it is a deliberate exception.

## Future work

None of these items is part of this spec. Each one depends on a decision this spec makes, and the spec must keep that decision intact. The longer-term direction behind them is in the [Diskless north star RFC](https://app.notion.com/p/40eb1ae1752781c399c3d711f92bb209).

### Grow the coordinator by splitting partitions

- With `slot % N` and a power-of-two `N`, doubling `N` moves exactly half of the slots of each coordinator partition `p` to the new partition `p + N`. The IDs of the existing coordinator partitions don't change.
- The growth needs a handoff that moves a slot's state from `p` to `p + N`: its logs, batches, and producer state. It also needs owner entries that stay correct for objects whose batches move, and a stored slot table once a slot's owner is no longer `slot % N`. Until that exists, the controller keeps rejecting `CreatePartitions` for `__diskless_metadata`.

### Merge WAL objects into extents

- A merged object is a new object with its own intent and participants. Each participant appends a record that replaces a log's batch rows with extent rows pointing at the merged object, then releases the old objects through the normal release path. [deletion-comparison.md](deletion-comparison.md#merged-and-multi-partition-objects) explains why owner-based deletion doesn't need to change for this.
- This needs a record type for the swap, and a way to turn extent rows into the `BatchInfo` objects that `findBatches` returns. The extent requirement in [Local state](#local-state) keeps the schema ready for it.

### Tiered metadata in the coordinator

- The remote log segment metadata of a diskless user partition would live in the coordinator partition that owns its batches. A composite `RemoteLogMetadataManager` would route diskless partitions to the coordinator and classic partitions to the topic-based implementation, because a broker loads one class from `remote.log.metadata.manager.class.name`.
- WAL pruning and segment registration then become records in one log for that user partition. A lookup goes to the coordinator leader on demand, so brokers don't consume every metadata partition, and the [Snapshots](#snapshots) mechanism bounds the metadata log. These are the RLMM problems that Jun Rao raised in the KIP-1163 discussion (JR1 and JR11).
- This spec must keep two constraints for this to work:
  - A user partition's batches and its remote segments map to the same coordinator partition, by the same log key.
  - Pruning and cross-tier offsets stay decided on the broker that hosts the local log and recorded by the leader, as [Consolidation and cross-tier offsets](#consolidation-and-cross-tier-offsets) describes.

### Partitionless key ranges

- A log that covers a key range instead of a fixed partition would get a second log key encoding, carried as a tagged field on the `Log` struct.
- The input that encoding hashes decides whether a range split moves a child range to another coordinator partition. If a parent range and its children map to one coordinator partition, the split is ordered with their commits in one log.

## Open items

The implementer decides these. None of them changes the design above.

- Names and numbers of the private request types, the number of each ApiKey, the error code for a busy snapshot source, and the error code for a part rejected by the age check.
- Feature flag name and its metadata version.
- Deletion grace period default for `FileCleaner`.
- SQLite driver choice, including its license and packaging check.
- How a leader that cannot rebuild its state gives up leadership of that one coordinator partition. The controller has no request that moves one partition's leadership off a broker, and the design needs one. Until then the leader marks only that partition offline and stops serving it. The directory stays online, and the broker does not halt.
- The commit deadline default, which must not exceed the produce request timeout.
- How often leaders append keys-expired records.
- The cap on how far the overlay may run ahead of the high watermark.
- The append linger default.

This item can change the design:

- The workloads, scales, and pass criteria for the benchmarks in [Capacity and isolation](#capacity-and-isolation). Their results can require a separate SQLite disk, a `findBatches` result cap, a different default partition count, a different default maximum commit age, or a return to Kafka's default flush.
